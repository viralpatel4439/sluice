import { type Engine, createEngine } from "@viralpatel4439/sluice";
import { consoleLogger } from "@viralpatel4439/sluice";
import { remotePool } from "@viralpatel4439/sluice/remote";
import { afterAll, describe, expect, inject, it } from "vitest";
import { type RunningServer, startServer } from "../src/index.js";

const redisUrl = inject("redisUrl" as never) as string;
const silent = consoleLogger("silent");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const ADMIN = "admin-secret";
const cleanup: (() => Promise<unknown>)[] = [];

afterAll(async () => {
  for (const c of cleanup.reverse()) await c().catch(() => {});
});

async function server(extra: Parameters<typeof startServer>[0] = {}): Promise<RunningServer> {
  const s = await startServer(
    {
      redisUrl,
      grpcPort: 0,
      httpPort: 0,
      httpHost: "127.0.0.1",
      grpcHost: "127.0.0.1",
      adminToken: ADMIN,
      leaseMs: 3000,
      renewMs: 500,
      ...extra,
    },
    silent,
  );
  cleanup.push(() => s.close());
  return s;
}

async function engine(
  url: string,
  namespace: string,
  caller: string,
  opts: { token?: string } = {},
): Promise<Engine> {
  const instanceId = `${caller}-${Math.random().toString(36).slice(2, 8)}`;
  const coordination = await remotePool({
    url,
    instanceId,
    caller,
    token: opts.token,
    logger: silent,
    leaseMs: 3000,
    safetyMs: 800,
  });
  const e = createEngine({ namespace, caller, instanceId, coordination, logger: silent });
  cleanup.push(() => e.close());
  return e;
}

function service(latencyMs = 10) {
  const s = {
    inFlight: 0,
    peak: 0,
    send: async () => {
      s.inFlight++;
      s.peak = Math.max(s.peak, s.inFlight);
      await sleep(latencyMs);
      s.inFlight--;
      return { status: 200 };
    },
  };
  return s;
}

const ns = () => `srv${Date.now()}${Math.floor(Math.random() * 1000)}`;

describe("sluice-server", () => {
  it("coordinates remote engines under one global capacity", async () => {
    const s = await server();
    const url = `127.0.0.1:${s.grpcPort}`;
    const namespace = ns();
    const svc = service();
    const engines = await Promise.all([
      engine(url, namespace, "orders"),
      engine(url, namespace, "orders"),
      engine(url, namespace, "billing"),
    ]);
    const ts = engines.map((e) => e.target("inventory", { capacity: 5, send: svc.send }));
    const results = await Promise.all(ts.flatMap((t) => Array.from({ length: 60 }, (_, i) => t.submit(i))));
    expect(results).toHaveLength(180);
    expect(svc.peak).toBeLessThanOrEqual(5);
    expect(svc.peak).toBeGreaterThanOrEqual(4);
  });

  it("returns a disconnected instance's tokens immediately", async () => {
    const s = await server();
    const url = `127.0.0.1:${s.grpcPort}`;
    const namespace = ns();
    const a = await engine(url, namespace, "a");
    const ta = a.target("hold", { capacity: 3, send: () => new Promise(() => {}) });
    for (let i = 0; i < 3; i++) void ta.submit(i).catch(() => {});
    await sleep(300);
    const pool = await s.reader.read(`${namespace}/hold`);
    expect(pool?.held).toBe(3);
    // Drop the connection without leaving the pool.
    (a.coordinator as unknown as { client: { close(): void }; closed: boolean }).closed = true;
    (a.coordinator as unknown as { client: { close(): void } }).client.close();
    (a.coordinator as unknown as { stream?: { cancel(): void } }).stream?.cancel();
    await sleep(400);
    const after = await s.reader.read(`${namespace}/hold`);
    expect(after?.held ?? 0).toBe(0);
  });

  it("keeps working across a server restart without exceeding capacity", async () => {
    const first = await server();
    const port = first.grpcPort;
    const url = `127.0.0.1:${port}`;
    const namespace = ns();
    const svc = service(20);
    const e = await engine(url, namespace, "survivor");
    const t = e.target("restart", { capacity: 4, send: svc.send });
    const work = Array.from({ length: 150 }, (_, i) => t.submit(i));
    await sleep(300);
    await first.close();
    await sleep(300);
    await server({ grpcPort: port });
    const results = await Promise.all(work);
    expect(results).toHaveLength(150);
    expect(svc.peak).toBeLessThanOrEqual(4);
  });

  it("rejects instances without a valid token when tokens are configured", async () => {
    const s = await server({ tokens: new Map([["orders", "t-orders"]]) });
    const url = `127.0.0.1:${s.grpcPort}`;
    const namespace = ns();
    const good = await engine(url, namespace, "orders", { token: "t-orders" });
    await good.target("auth", { capacity: 2, send: service().send }).submit(1);
    const coordination = await remotePool({
      url,
      instanceId: "intruder",
      caller: "orders",
      token: "wrong",
      logger: silent,
    });
    await expect(coordination.join(`${namespace}/auth`, { caller: "orders", capacity: 2 })).rejects.toThrow();
    await coordination.close();
  });

  it("lets an operator pin capacity, which every engine follows, and audits it", async () => {
    const s = await server();
    const url = `127.0.0.1:${s.grpcPort}`;
    const base = `http://127.0.0.1:${s.httpPort}`;
    const namespace = ns();
    const svc = service(15);
    const engines = await Promise.all([engine(url, namespace, "x"), engine(url, namespace, "y")]);
    const ts = engines.map((e) => e.target("pinned", { capacity: 8, send: svc.send }));
    await Promise.all(ts.map((t) => t.submit(0)));
    const pool = encodeURIComponent(`${namespace}/pinned`);

    const denied = await fetch(`${base}/api/pools/${pool}/capacity`, {
      method: "POST",
      body: JSON.stringify({ capacity: 2 }),
    });
    expect(denied.status).toBe(401);
    const ok = await fetch(`${base}/api/pools/${pool}/capacity`, {
      method: "POST",
      headers: { authorization: `Bearer ${ADMIN}`, "x-sluice-actor": "test-operator" },
      body: JSON.stringify({ capacity: 2 }),
    });
    expect(ok.status).toBe(200);
    await sleep(800);
    svc.peak = 0;
    await Promise.all(ts.flatMap((t) => Array.from({ length: 40 }, (_, i) => t.submit(i))));
    expect(svc.peak).toBeLessThanOrEqual(2);

    const log = (await (await fetch(`${base}/api/pools/${pool}/log`)).json()) as {
      type: string;
      actor?: string;
    }[];
    expect(log.some((e) => e.type === "admin:capacity" && e.actor === "test-operator")).toBe(true);
    const metrics = await (await fetch(`${base}/metrics`)).text();
    expect(metrics).toMatch(
      new RegExp(`sluice_pool_capacity\\{namespace="${namespace}",target="pinned"\\} 2`),
    );
    const page = await (await fetch(`${base}/`)).text();
    expect(page).toContain("sluice control panel");
  });
});
