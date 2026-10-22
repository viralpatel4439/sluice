import { Cluster, Redis } from "ioredis";
import {
  GenericContainer,
  Network,
  type StartedNetwork,
  type StartedTestContainer,
  Wait,
} from "testcontainers";
import { afterAll, describe, expect, it } from "vitest";
import { type Engine, createEngine } from "../src/core/engine.js";
import { consoleLogger } from "../src/core/logger.js";
import { inspectPool, redisPool } from "../src/redis/index.js";

/**
 * Redis Cluster and Sentinel. Both run in a Docker network; the host-side
 * clients reach them through ioredis natMap (container address → mapped port).
 */

const silent = consoleLogger("silent");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const stops: (() => Promise<unknown>)[] = [];
afterAll(async () => {
  for (const s of stops.reverse()) await s().catch(() => {});
});

function service(capacity: number) {
  const s = {
    inFlight: 0,
    peak: 0,
    over: 0,
    send: async () => {
      s.inFlight++;
      s.peak = Math.max(s.peak, s.inFlight);
      if (s.inFlight > capacity) s.over++;
      await sleep(10);
      s.inFlight--;
      return { status: 200 };
    },
  };
  return s;
}

async function fleet(make: () => Promise<unknown>, namespace: string, n: number) {
  const engines: Engine[] = [];
  for (let i = 0; i < n; i++) {
    const client = await make();
    const coordination = await redisPool({
      client,
      instanceId: `${namespace}-${i}`,
      caller: `c${i % 2}`,
      logger: silent,
      leaseMs: 2000,
      renewMs: 300,
      safetyMs: 600,
    });
    const e = createEngine({
      namespace,
      caller: `c${i % 2}`,
      instanceId: `${namespace}-${i}`,
      coordination,
      logger: silent,
    });
    engines.push(e);
    stops.push(() => e.close());
  }
  return engines;
}

describe("Redis Cluster", () => {
  let network: StartedNetwork;
  const nodes: StartedTestContainer[] = [];

  it("runs pools across a 3-master cluster: many targets on different slots, one global N each", async () => {
    network = await new Network().start();
    stops.push(() => network.stop());
    for (let i = 0; i < 3; i++) {
      const c = await new GenericContainer("redis:7-alpine")
        .withNetwork(network)
        .withCommand([
          "redis-server",
          "--cluster-enabled",
          "yes",
          "--cluster-config-file",
          "nodes.conf",
          "--appendonly",
          "no",
        ])
        .withExposedPorts(6379)
        .withWaitStrategy(Wait.forLogMessage("Ready to accept connections"))
        .start();
      nodes.push(c);
      stops.push(() => c.stop());
    }
    const ips = nodes.map((n) => n.getIpAddress(network.getName()));
    const create = await nodes[0]!.exec([
      "redis-cli",
      "--cluster",
      "create",
      ...ips.map((ip) => `${ip}:6379`),
      "--cluster-yes",
    ]);
    expect(create.exitCode).toBe(0);
    const natMap = Object.fromEntries(
      nodes.map((n, i) => [`${ips[i]}:6379`, { host: n.getHost(), port: n.getMappedPort(6379) }]),
    );
    const make = async () => {
      const c = new Cluster([{ host: nodes[0]!.getHost(), port: nodes[0]!.getMappedPort(6379) }], {
        natMap,
        redisOptions: { maxRetriesPerRequest: 1 },
        enableOfflineQueue: true,
      });
      c.on("error", () => {});
      stops.push(async () => c.disconnect());
      return c;
    };
    // Wait until the cluster reports ok.
    const probe = await make();
    for (let i = 0; i < 50; i++) {
      const info = String(await (probe as Cluster).cluster("INFO").catch(() => ""));
      if (info.includes("cluster_state:ok")) break;
      await sleep(200);
    }

    const namespace = `cl${Date.now()}`;
    const engines = await fleet(make, namespace, 4);
    // Several targets so their pools land on different slots and masters.
    const names = ["alpha", "bravo", "charlie", "delta", "echo"];
    const services = Object.fromEntries(names.map((n) => [n, service(4)]));
    const work: Promise<unknown>[] = [];
    for (const e of engines) {
      for (const n of names) {
        const t = e.target(n, { capacity: 4, send: services[n]!.send });
        for (let i = 0; i < 25; i++) work.push(t.submit(i));
      }
    }
    expect(await Promise.all(work)).toHaveLength(4 * 5 * 25);
    for (const n of names) {
      expect(services[n]!.over).toBe(0);
      expect(services[n]!.peak).toBeGreaterThanOrEqual(3);
    }
    // Pools really are spread over several masters.
    const slots = new Set(
      names.map((n) => (probe as Cluster).slots.length && keySlot(`sluice:{${namespace}/${n}}:meta`)),
    );
    expect(slots.size).toBeGreaterThan(1);
    await sleep(500);
    for (const n of names) {
      const p = (await inspectPool(probe, `${namespace}/${n}`)) as {
        cap: number;
        free: number;
        debt: number;
        instances: { held: number }[];
      };
      expect(p.free + p.instances.reduce((a, i) => a + i.held, 0)).toBe(p.cap + p.debt);
    }
  }, 120_000);
});

describe("Redis Sentinel", () => {
  it("coordinates through Sentinel and keeps going after a failover", async () => {
    const network = await new Network().start();
    stops.push(() => network.stop());
    const master = await new GenericContainer("redis:7-alpine")
      .withNetwork(network)
      .withNetworkAliases("redis-master")
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage("Ready to accept connections"))
      .start();
    stops.push(() => master.stop());
    const replica = await new GenericContainer("redis:7-alpine")
      .withNetwork(network)
      .withNetworkAliases("redis-replica")
      .withCommand([
        "redis-server",
        "--replicaof",
        "redis-master",
        "6379",
        "--replica-announce-ip",
        "redis-replica",
      ])
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage("MASTER <-> REPLICA sync: Finished with success"))
      .start();
    stops.push(() => replica.stop());
    const conf = [
      "port 26379",
      "sentinel resolve-hostnames yes",
      "sentinel announce-hostnames yes",
      "sentinel monitor main redis-master 6379 1",
      "sentinel down-after-milliseconds main 1000",
      "sentinel failover-timeout main 5000",
    ].join("\n");
    const sentinel = await new GenericContainer("redis:7-alpine")
      .withNetwork(network)
      .withExposedPorts(26379)
      .withCopyContentToContainer([{ content: conf, target: "/data/sentinel.conf", mode: 0o666 }])
      .withCommand(["redis-sentinel", "/data/sentinel.conf"])
      .withWaitStrategy(Wait.forLogMessage("+monitor master main"))
      .start();
    stops.push(() => sentinel.stop());

    const natMap = {
      "redis-master:6379": { host: master.getHost(), port: master.getMappedPort(6379) },
      "redis-replica:6379": { host: replica.getHost(), port: replica.getMappedPort(6379) },
    };
    const make = async () => {
      const c = new Redis({
        sentinels: [{ host: sentinel.getHost(), port: sentinel.getMappedPort(26379) }],
        name: "main",
        natMap,
        maxRetriesPerRequest: 1,
        enableOfflineQueue: false,
        commandTimeout: 1000,
      });
      c.on("error", () => {});
      stops.push(async () => c.disconnect());
      return c;
    };
    const namespace = `sn${Date.now()}`;
    const svc = service(5);
    const engines = await fleet(make, namespace, 3);
    const ts = engines.map((e) => e.target("sentinel", { capacity: 5, send: svc.send }));
    expect(
      await Promise.all(ts.flatMap((t) => Array.from({ length: 30 }, (_, i) => t.submit(i)))),
    ).toHaveLength(90);
    expect(svc.over).toBe(0);

    // Fail over: stop the master; Sentinel promotes the replica; engines follow it.
    const events: string[] = [];
    for (const e of engines) e.on("fallback:on", () => events.push("fenced"));
    const during = ts.flatMap((t) => Array.from({ length: 60 }, (_, i) => t.submit(i)));
    await master.stop();
    const all = await Promise.all(during);
    expect(all).toHaveLength(180);
    // Work continued (possibly through a short fallback) and the pool is usable on the new master.
    await sleep(1500);
    const after = await Promise.all(ts.map((t) => t.submit("after-failover")));
    expect(after).toHaveLength(3);
    const admin = await make();
    const p = (await inspectPool(admin, `${namespace}/sentinel`)) as {
      cap: number;
      free: number;
      debt: number;
      instances: { held: number }[];
    };
    expect(p.free + p.instances.reduce((a, i) => a + i.held, 0)).toBe(p.cap + p.debt);
  }, 120_000);
});

/** CRC16 key slot, as Redis Cluster computes it (honouring {hash tags}). */
function keySlot(fullKey: string): number {
  let key = fullKey;
  const s = key.indexOf("{");
  if (s !== -1) {
    const e = key.indexOf("}", s + 1);
    if (e > s + 1) key = key.slice(s + 1, e);
  }
  let crc = 0;
  for (const b of Buffer.from(key)) {
    crc ^= b << 8;
    for (let i = 0; i < 8; i++) crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
  }
  return crc % 16384;
}
