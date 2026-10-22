import { Redis } from "ioredis";
import { createClient } from "redis";
import { afterAll, beforeEach, describe, expect, inject, it } from "vitest";
import { type Engine, createEngine } from "../src/core/engine.js";
import { consoleLogger } from "../src/core/logger.js";
import {
  RedisCoordinator,
  collectPool,
  inspectPool,
  overrideCapacity,
  redisPool,
} from "../src/redis/index.js";

const url = inject("redisUrl");
const admin = new Redis(url);
const silent = consoleLogger("silent");
const fast = { leaseMs: 1500, renewMs: 300, safetyMs: 400 };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let ns = 0;
let namespace = "";
const engines: Engine[] = [];
const clients: { disconnect?: () => unknown; destroy?: () => unknown }[] = [];

beforeEach(() => {
  namespace = `t${Date.now()}-${++ns}`;
});

afterAll(async () => {
  for (const e of engines) await e.close().catch(() => {});
  for (const c of clients) {
    try {
      if (c instanceof Redis) c.disconnect();
      else c.destroy?.();
    } catch {}
  }
  admin.disconnect();
});

async function engine(
  caller: string,
  extra: { weight?: number; instanceId?: string; client?: "ioredis" | "node-redis" } = {},
) {
  const instanceId = extra.instanceId ?? `${caller}-${Math.random().toString(36).slice(2, 8)}`;
  // Each engine gets its own client, so tests can cut one instance's connection.
  let client: Redis | ReturnType<typeof createClient>;
  if (extra.client === "node-redis") {
    client = createClient({ url });
    client.on("error", () => {});
    await client.connect();
  } else {
    client = new Redis(url, { commandTimeout: 1000, maxRetriesPerRequest: 1, enableOfflineQueue: false });
    client.on("error", () => {});
  }
  clients.push(client);
  const coordination = await redisPool({ client, instanceId, caller, logger: silent, ...fast });
  const e = createEngine({
    namespace,
    caller,
    instanceId,
    weight: extra.weight,
    coordination,
    logger: silent,
  });
  engines.push(e);
  return { e, coordination, client };
}

/** A fake target shared by every engine; records global concurrency. */
function service(latencyMs = 15) {
  const s = {
    inFlight: 0,
    peak: 0,
    byCaller: new Map<string, number>(),
    send: async (item: { headers: Record<string, string> }) => {
      s.inFlight++;
      s.peak = Math.max(s.peak, s.inFlight);
      const caller = item.headers["x-sluice-caller"]!;
      s.byCaller.set(caller, (s.byCaller.get(caller) ?? 0) + 1);
      await sleep(latencyMs);
      s.inFlight--;
      return { status: 200 };
    },
  };
  return s;
}

async function pool(name: string) {
  return (await inspectPool(admin, `${namespace}/${name}`)) as {
    cap: number;
    free: number;
    debt: number;
    owner: string | false;
    instances: { id: string; held: number; caller: string }[] | Record<string, never>;
  };
}

function heldSum(p: Awaited<ReturnType<typeof pool>>) {
  return (Array.isArray(p.instances) ? p.instances : []).reduce((a, i) => a + i.held, 0);
}

describe("redis coordination", () => {
  it("keeps global in-flight <= N across instances and completes all work", async () => {
    const svc = service();
    const nodes = await Promise.all([engine("orders"), engine("orders"), engine("billing")]);
    const ts = nodes.map(({ e }) => e.target("inventory", { capacity: 6, send: svc.send }));
    const work = ts.flatMap((t) => Array.from({ length: 80 }, (_, i) => t.submit(i)));
    const results = await Promise.all(work);
    expect(results).toHaveLength(240);
    expect(svc.peak).toBeLessThanOrEqual(6);
    expect(svc.peak).toBeGreaterThanOrEqual(5);
    await sleep(500);
    const p = await pool("inventory");
    expect(p.free + heldSum(p)).toBe(p.cap + p.debt);
  });

  it("shares a contended target by caller weight", async () => {
    const svc = service(20);
    const a = await engine("heavy", { weight: 3 });
    const b = await engine("light", { weight: 1 });
    const ta = a.e.target("shared", { capacity: 8, send: svc.send });
    const tb = b.e.target("shared", { capacity: 8, send: svc.send });
    // Both stay backlogged for the whole measurement window.
    const work = [
      ...Array.from({ length: 1000 }, (_, i) => ta.submit(i)),
      ...Array.from({ length: 1000 }, (_, i) => tb.submit(i)),
    ];
    await sleep(300); // let shares settle
    const h0 = svc.byCaller.get("heavy") ?? 0;
    const l0 = svc.byCaller.get("light") ?? 0;
    await sleep(800);
    const heavy = (svc.byCaller.get("heavy") ?? 0) - h0;
    const light = (svc.byCaller.get("light") ?? 0) - l0;
    expect(ta.snapshot().queued).toBeGreaterThan(0);
    expect(tb.snapshot().queued).toBeGreaterThan(0);
    expect(heavy / light).toBeGreaterThan(2.2);
    expect(heavy / light).toBeLessThan(4);
    await Promise.all(work);
    expect(svc.peak).toBeLessThanOrEqual(8);
  });

  it("reclaims tokens from a crashed instance after its lease", async () => {
    const svc = service(5);
    const crash = await engine("crashy");
    const t = crash.e.target("reclaim", { capacity: 4, send: async () => new Promise(() => {}) });
    for (let i = 0; i < 4; i++) void t.submit(i).catch(() => {});
    await sleep(300);
    expect(heldSum(await pool("reclaim"))).toBe(4);
    // Simulate a crash: the process vanishes without leaving the pool.
    (crash.client as Redis).disconnect();
    clearInterval((crash.coordination as unknown as { renewTimer: NodeJS.Timeout }).renewTimer);
    const survivor = await engine("survivor");
    const ts = survivor.e.target("reclaim", { capacity: 4, send: svc.send });
    const started = Date.now();
    await ts.submit("after-crash");
    expect(Date.now() - started).toBeGreaterThan(500);
    expect(Date.now() - started).toBeLessThan(4000);
  });

  it("deduplicates a retried sync (same seq) so tokens are never released twice", async () => {
    const c = await RedisCoordinator.create({
      url,
      instanceId: "dedupe",
      caller: "x",
      logger: silent,
      ...fast,
    });
    const name = `${namespace}/dedupe`;
    c.attach({ grant: () => {}, state: () => {}, fenced: () => {}, restored: () => {} });
    await c.join(name, { caller: "x", capacity: 5 });
    const call = (
      c as unknown as {
        call: (s: string, e: unknown, a: string[]) => Promise<{ granted?: number; free: number }>;
      }
    ).call.bind(c);
    const entry = (c as unknown as { pools: Map<string, unknown> }).pools.get(name);
    const grant = await call("ssync", entry, ["1", "0", "3", "1", "3", "1500", "0"]);
    expect(grant.granted).toBe(3);
    const rel1 = await call("ssync", entry, ["2", "3", "0", "1", "0", "1500", "3"]);
    const rel2 = await call("ssync", entry, ["2", "3", "0", "1", "0", "1500", "3"]);
    expect(rel1.free).toBe(5);
    expect(rel2.free).toBe(5);
    const p = (await inspectPool(admin, name)) as { free: number };
    expect(p.free).toBe(5);
    await c.close();
  });

  it("shrinks capacity through debt without cancelling in-flight work", async () => {
    const { e, coordination } = await engine("shrink");
    let inFlight = 0;
    let peakAfter = 0;
    let shrunk = false;
    let capacity = 6;
    const t = e.target("shrink", {
      // The capacity source is the authority; the owner pushes what it reports.
      capacity: { type: "callback", fn: () => capacity, intervalMs: 50, fallback: 6 },
      send: async () => {
        inFlight++;
        if (shrunk) peakAfter = Math.max(peakAfter, inFlight);
        await sleep(30);
        inFlight--;
        return { status: 200 };
      },
    });
    const work = Array.from({ length: 120 }, (_, i) => t.submit(i));
    await sleep(700); // become cap owner
    expect(coordination.ownsCapacity(`${namespace}/shrink`)).toBe(true);
    capacity = 2;
    // Requests already in flight finish; the pool retires tokens as they come back.
    await sleep(250);
    shrunk = true;
    await Promise.all(work);
    expect(peakAfter).toBeLessThanOrEqual(2);
    const p = await pool("shrink");
    expect(p.cap).toBe(2);
    expect(p.free + heldSum(p)).toBe(p.cap + p.debt);
  });

  it("rejects capacity writes from a stale owner (fencing token)", async () => {
    const name = `${namespace}/fence`;
    const c = await RedisCoordinator.create({
      url,
      instanceId: "owner1",
      caller: "x",
      logger: silent,
      ...fast,
    });
    c.attach({ grant: () => {}, state: () => {}, fenced: () => {}, restored: () => {} });
    await c.join(name, { caller: "x", capacity: 5 });
    await sleep(500);
    expect(c.ownsCapacity(name)).toBe(true);
    // Another instance takes ownership after ours "expires"; our fence token is now stale.
    await admin.hset(`sluice:{${name}}:meta`, "owner", "someone-else", "oexp", "0");
    await admin.hincrby(`sluice:{${name}}:meta`, "fence", 1);
    c.setCapacity(name, 99);
    await sleep(200);
    expect(((await inspectPool(admin, name)) as { cap: number }).cap).toBe(5);
    await c.close();
  });

  it("fences itself when Redis is unreachable and recovers afterwards", async () => {
    const { e, client } = await engine("fenced");
    const events: string[] = [];
    e.on("fallback:on", () => events.push("on"));
    e.on("fallback:off", () => events.push("off"));
    const t = e.target("fence2", {
      capacity: 4,
      send: async () => {
        await sleep(10);
        return { status: 200 };
      },
    });
    await t.submit(1);
    const cmd = client as Redis;
    cmd.disconnect();
    await sleep(1600);
    expect(events).toContain("on");
    expect(t.snapshot().fallback).toBe(true);
    // Still works in fallback (local share).
    await t.submit(2);
    await cmd.connect();
    await sleep(1200);
    expect(events).toContain("off");
    expect(t.snapshot().fallback).toBe(false);
    await t.submit(3);
  });

  it("lets ioredis and node-redis engines share one pool", async () => {
    const svc = service(10);
    const nodes = await Promise.all([
      engine("io", { client: "ioredis" }),
      engine("nr", { client: "node-redis" }),
      engine("nr", { client: "node-redis" }),
    ]);
    const ts = nodes.map(({ e }) => e.target("mixed", { capacity: 5, send: svc.send }));
    const work = ts.flatMap((t) => Array.from({ length: 60 }, (_, i) => t.submit(i)));
    expect(await Promise.all(work)).toHaveLength(180);
    expect(svc.peak).toBeLessThanOrEqual(5);
    expect(svc.byCaller.get("nr")).toBeGreaterThan(0);
    await sleep(400);
    const p = await pool("mixed");
    expect(p.free + heldSum(p)).toBe(p.cap + p.debt);
  });

  it("garbage-collects only pools that nobody uses and nobody pinned", async () => {
    const { e } = await engine("gc");
    await e.target("busy", { capacity: 2, send: async () => ({ status: 200 }) }).submit(1);
    expect(await collectPool(admin, `${namespace}/busy`)).toBe(false); // an instance is registered
    await e.close();
    expect(await collectPool(admin, `${namespace}/busy`)).toBe(true);
    expect(await admin.exists(`sluice:{${namespace}/busy}:meta`)).toBe(0);
    expect(await admin.sismember("sluice:pools", `${namespace}/busy`)).toBe(0);

    const { e: e2 } = await engine("gc2");
    await e2.target("pinned", { capacity: 2, send: async () => ({ status: 200 }) }).submit(1);
    await overrideCapacity(admin, `${namespace}/pinned`, 1, "test");
    await e2.close();
    expect(await collectPool(admin, `${namespace}/pinned`)).toBe(false); // operator pin survives
  });

  it("connects from a URL", async () => {
    const c = await redisPool({ url, instanceId: "from-url", caller: "x", logger: silent, ...fast });
    const e = createEngine({ namespace, caller: "x", coordination: c, logger: silent });
    await e.target("url", { capacity: 2, send: async () => ({ status: 200 }) }).submit(1);
    await e.close();
  });

  it("keeps the conservation invariant under random concurrent churn", async () => {
    const svc = service(3);
    const nodes = await Promise.all(Array.from({ length: 4 }, (_, i) => engine(`churn${i % 2}`)));
    const ts = nodes.map(({ e }) =>
      e.target("churn", { capacity: 5, retry: { max: 1, baseMs: 1, maxMs: 5 }, send: svc.send }),
    );
    const work: Promise<unknown>[] = [];
    for (let round = 0; round < 10; round++) {
      for (const t of ts)
        for (let i = 0; i < Math.floor(Math.random() * 15); i++)
          work.push(t.submit(i, { priority: Math.random() < 0.3 ? "bulk" : "standard" }));
      await sleep(40);
      const p = await pool("churn");
      expect(p.free + heldSum(p)).toBe(p.cap + p.debt);
      expect(p.free).toBeGreaterThanOrEqual(0);
    }
    await Promise.all(work);
    expect(svc.peak).toBeLessThanOrEqual(5);
  });
});
