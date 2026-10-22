import {
  type CreatedProxy,
  type StartedToxiProxyContainer,
  ToxiProxyContainer,
} from "@testcontainers/toxiproxy";
import { Redis } from "ioredis";
import { GenericContainer, Network, type StartedNetwork, type StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { type Engine, createEngine } from "../src/core/engine.js";
import { consoleLogger } from "../src/core/logger.js";
import { inspectPool, redisPool } from "../src/redis/index.js";

/**
 * Chaos on real components: every engine reaches Redis through its own
 * Toxiproxy proxy, so one instance's link can be slowed or cut while the
 * fleet keeps working. The fake target checks, at every request, that requests
 * sent under pool tokens (excluding orphans of a fenced instance) never exceed N.
 */

const silent = consoleLogger("silent");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let network: StartedNetwork;
let redisC: StartedTestContainer;
let toxi: StartedToxiProxyContainer;
let admin: Redis;
const cleanup: (() => Promise<unknown>)[] = [];

beforeAll(async () => {
  network = await new Network().start();
  redisC = await new GenericContainer("redis:7-alpine")
    .withNetwork(network)
    .withNetworkAliases("redis")
    .withExposedPorts(6379)
    .start();
  toxi = await new ToxiProxyContainer("ghcr.io/shopify/toxiproxy:2.9.0").withNetwork(network).start();
  admin = new Redis(redisC.getMappedPort(6379), redisC.getHost());
}, 180_000);

afterAll(async () => {
  for (const c of cleanup.reverse()) await c().catch(() => {});
  admin?.disconnect();
  await toxi?.stop();
  await redisC?.stop();
  await network?.stop();
});

let proxies = 0;
async function instance(namespace: string, fallback: "local-share" | "pause") {
  const proxy: CreatedProxy = await toxi.createProxy({ name: `redis-${++proxies}`, upstream: "redis:6379" });
  const id = `chaos-${proxies}`;
  const coordination = await redisPool({
    url: `redis://${proxy.host}:${proxy.port}`,
    instanceId: id,
    caller: "chaos",
    logger: silent,
    leaseMs: 1500,
    renewMs: 250,
    safetyMs: 500,
    fallback,
  });
  const engine = createEngine({ namespace, caller: "chaos", instanceId: id, coordination, logger: silent });
  cleanup.push(() => engine.close());
  return { engine, proxy };
}

function referee(capacity: number) {
  type Req = { who: number; pooled: boolean; orphan: boolean };
  const running = new Set<Req>();
  const r = {
    violations: [] as string[],
    peakAll: 0,
    fallbackSends: 0,
    sends: 0,
    fenced: new Set<number>(),
    orphan(who: number) {
      for (const q of running) if (q.who === who) q.orphan = true;
    },
    send: (who: number) => async () => {
      const q: Req = { who, pooled: !r.fenced.has(who), orphan: false };
      running.add(q);
      r.sends++;
      if (!q.pooled) r.fallbackSends++;
      r.peakAll = Math.max(r.peakAll, running.size);
      const pooled = [...running].filter((x) => x.pooled && !x.orphan).length;
      if (pooled > capacity) {
        const byWho: Record<number, number> = {};
        for (const x of running) if (x.pooled && !x.orphan) byWho[x.who] = (byWho[x.who] ?? 0) + 1;
        r.violations.push(
          `t=${Date.now() % 100000} pooled ${pooled} > ${capacity} by ${JSON.stringify(byWho)} fenced=${[...r.fenced]}`,
        );
      }
      await sleep(10 + Math.random() * 20);
      running.delete(q);
      return { status: 200 };
    },
  };
  return r;
}

function watch(engine: Engine, who: number, r: ReturnType<typeof referee>) {
  engine.on("fallback:on", () => {
    r.orphan(who);
    r.fenced.add(who);
  });
  engine.on("fallback:off", () => {
    r.orphan(who);
    r.fenced.delete(who);
  });
}

async function conserved(pool: string) {
  const p = (await inspectPool(admin, pool)) as {
    cap: number;
    free: number;
    debt: number;
    instances: { held: number }[] | object;
  };
  const held = (Array.isArray(p.instances) ? p.instances : []).reduce((a, i) => a + i.held, 0);
  return p.free + held === p.cap + p.debt;
}

describe("chaos: real Redis behind Toxiproxy", () => {
  it("survives a slow link, a cut link and recovery without exceeding N", async () => {
    const namespace = `chaos${Date.now()}`;
    const N = 8;
    const r = referee(N);
    const nodes = [];
    for (let i = 0; i < 4; i++) nodes.push(await instance(namespace, "local-share")); // proxies must be created one by one
    nodes.forEach((n, i) => watch(n.engine, i, r));
    const ts = nodes.map((n, i) => n.engine.target("victim", { capacity: N, send: r.send(i) }));

    let stop = false;
    const load = ts.map(async (t) => {
      const inflight: Promise<unknown>[] = [];
      while (!stop) {
        inflight.push(t.submit(1));
        if (inflight.length > 40) await Promise.all(inflight.splice(0, 20));
        await sleep(2);
      }
      await Promise.all(inflight);
    });

    await sleep(700);
    // 1. Slow link: 150 ms of latency on instance 0's Redis connection.
    const slow = await nodes[0]!.proxy.instance.addToxic({
      name: "slow",
      type: "latency",
      stream: "downstream",
      toxicity: 1,
      attributes: { latency: 150, jitter: 50 },
    });
    await sleep(1500);
    await slow.remove();

    // 2. Cut instance 1 off from Redis entirely; it must fence and fall back.
    await nodes[1]!.proxy.setEnabled(false);
    await sleep(2500);
    expect(r.fenced.has(1)).toBe(true);
    // 3. Restore it; it must rejoin.
    await nodes[1]!.proxy.setEnabled(true);
    await sleep(2500);
    expect(r.fenced.has(1)).toBe(false);

    stop = true;
    await Promise.all(load);
    expect(r.violations).toEqual([]);
    expect(r.sends).toBeGreaterThan(500);
    expect(r.fallbackSends).toBeGreaterThan(0);
    await sleep(600);
    expect(await conserved(`${namespace}/victim`)).toBe(true);
  }, 60_000);

  it("in pause mode a cut-off instance sends nothing, and the target never sees more than N", async () => {
    const namespace = `chaosp${Date.now()}`;
    const N = 5;
    const r = referee(N);
    const nodes = [];
    for (let i = 0; i < 3; i++) nodes.push(await instance(namespace, "pause"));
    nodes.forEach((n, i) => watch(n.engine, i, r));
    const ts = nodes.map((n, i) => n.engine.target("paused", { capacity: N, send: r.send(i) }));
    let stop = false;
    const load = ts.map(async (t) => {
      const inflight: Promise<unknown>[] = [];
      while (!stop) {
        inflight.push(t.submit(1));
        if (inflight.length > 30) await Promise.all(inflight.splice(0, 15));
        await sleep(2);
      }
      await Promise.all(inflight);
    });
    await sleep(600);
    await nodes[2]!.proxy.setEnabled(false);
    await sleep(2500);
    expect(r.fenced.has(2)).toBe(true);
    await nodes[2]!.proxy.setEnabled(true);
    await sleep(2000);
    stop = true;
    await Promise.all(load);
    expect(r.fallbackSends).toBe(0);
    expect(r.violations).toEqual([]);
    // Requests are short compared with the fencing margin, so even counting orphans the target stays at N.
    expect(r.peakAll).toBeLessThanOrEqual(N);
  }, 60_000);
});
