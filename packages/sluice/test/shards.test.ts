import { Redis } from "ioredis";
import { afterAll, describe, expect, inject, it } from "vitest";
import { type Engine, createEngine } from "../src/core/engine.js";
import { consoleLogger } from "../src/core/logger.js";
import { inspectPool, redisPool } from "../src/redis/index.js";

const url = inject("redisUrl");
const admin = new Redis(url);
const silent = consoleLogger("silent");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const engines: Engine[] = [];
afterAll(async () => {
  for (const e of engines) await e.close().catch(() => {});
  admin.disconnect();
});

async function shardCaps(pool: string, k: number) {
  const views = await Promise.all(
    Array.from(
      { length: k },
      (_, i) => inspectPool(admin, `${pool}%${i}`) as Promise<{ cap?: number; err?: string }>,
    ),
  );
  return views.map((v) => v.cap ?? 0);
}

async function node(namespace: string, id: string, shards: number) {
  const coordination = await redisPool({
    url,
    instanceId: id,
    caller: "c",
    logger: silent,
    leaseMs: 1500,
    renewMs: 250,
    safetyMs: 400,
    shards,
  });
  const e = createEngine({ namespace, caller: "c", instanceId: id, coordination, logger: silent });
  engines.push(e);
  return e;
}

describe("sharded pools", () => {
  it("keeps the fleet at or below N across shards and moves capacity to where the work is", async () => {
    const namespace = `sh${Date.now()}`;
    const pool = `${namespace}/hot`;
    let inFlight = 0;
    let peak = 0;
    const send = async () => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await sleep(15);
      inFlight--;
      return { status: 200 };
    };
    const ids = Array.from({ length: 6 }, (_, i) => `inst-${i}`);
    const nodes = await Promise.all(ids.map((id) => node(namespace, id, 3)));
    const ts = nodes.map((e) => e.target("hot", { capacity: 9, send }));

    // Everyone busy: total N is respected.
    await Promise.all(ts.flatMap((t) => Array.from({ length: 40 }, (_, i) => t.submit(i))));
    expect(peak).toBeLessThanOrEqual(9);
    await sleep(800);
    expect((await shardCaps(pool, 3)).reduce((a, b) => a + b, 0)).toBe(9);

    // Only one instance busy: capacity drifts to its shard, total stays 9.
    peak = 0;
    const busy = ts[0]!;
    const work = Array.from({ length: 600 }, (_, i) => busy.submit(i));
    await sleep(2000);
    const caps = await shardCaps(pool, 3);
    expect(caps.reduce((a, b) => a + b, 0)).toBe(9);
    expect(Math.max(...caps)).toBeGreaterThanOrEqual(6);
    await Promise.all(work);
    expect(peak).toBeLessThanOrEqual(9);
    expect(busy.snapshot().capacity).toBe(9);
  });

  it("adopts the shard count chosen by the first instance", async () => {
    const namespace = `sh2${Date.now()}`;
    const a = await node(namespace, "first", 2);
    await a.target("adopt", { capacity: 4, send: async () => ({ status: 200 }) }).submit(1);
    const b = await node(namespace, "second", 5);
    await b.target("adopt", { capacity: 4, send: async () => ({ status: 200 }) }).submit(1);
    expect(await admin.hget("sluice:shards", `${namespace}/adopt`)).toBe("2");
    expect(await admin.exists(`sluice:{${namespace}/adopt%4}:meta`)).toBe(0);
  });
});
