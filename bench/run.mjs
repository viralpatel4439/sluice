// Performance budgets for sluice. Run: npm run bench  (needs `npm run build`, and Redis for the handoff check).
// Prints results as JSON; exits 1 if a budget is missed.
import { consoleLogger, createEngine } from "@viralpatel4439/sluice";

const silent = consoleLogger("silent");
const results = {};
const budgets = {
  overheadP99Us: { max: 50, label: "submit→send overhead p99 (µs)" },
  throughputPerSec: { min: 100_000, label: "items/s through one instance" },
  // A pending Promise plus its two resolver functions costs ~250 bytes in V8 by itself (measured);
  // the rest is the item's own fields. 512 leaves headroom while catching regressions.
  bytesPerQueuedItem: {
    max: 512,
    label: "engine memory per queued item (bytes, excl. payload, incl. the caller's promise)",
  },
  // A handoff is two network legs (release script, then pub/sub push to the waiter). Absolute latency
  // depends on the network and on CPU wake-up after idle, so the budget is sluice's own overhead:
  // handoff p50 minus two Redis round trips measured with the same idle pattern.
  handoffOverheadMs: { max: 0.3, label: "cross-instance handoff overhead beyond 2 RTT, p50 (ms)" },
};

function quantile(xs, q) {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(q * s.length))];
}

// 1. Overhead: time from submit() to send() while tokens are free.
{
  const engine = createEngine({ caller: "bench", logger: silent });
  let started = 0;
  const samples = [];
  const t = engine.target("overhead", {
    capacity: 1000,
    send: async () => {
      samples.push((performance.now() - started) * 1000);
      return 1;
    },
  });
  await t.submit(0);
  for (let i = 0; i < 20_000; i++) {
    started = performance.now();
    await t.submit(i);
  }
  results.overheadP50Us = +quantile(samples.slice(1000), 0.5).toFixed(2);
  results.overheadP99Us = +quantile(samples.slice(1000), 0.99).toFixed(2);
  await engine.close();
}

// 2. Throughput: many concurrent submits with an instant send.
{
  const engine = createEngine({ caller: "bench", logger: silent });
  const t = engine.target("throughput", {
    capacity: 256,
    queue: { maxItems: 2_000_000 },
    send: async () => 1,
  });
  const N = 300_000;
  const t0 = performance.now();
  const batch = 10_000;
  for (let i = 0; i < N; i += batch) {
    const ps = new Array(batch);
    for (let j = 0; j < batch; j++) ps[j] = t.submit(j);
    await Promise.all(ps);
  }
  results.throughputPerSec = Math.round(N / ((performance.now() - t0) / 1000));
  await engine.close();
}

// 3. Memory per queued item: queue 200k items behind a closed gate.
{
  global.gc?.();
  const engine = createEngine({ caller: "bench", logger: silent });
  const t = engine.target("memory", { capacity: 0, queue: { maxItems: 1_000_000 }, send: async () => 1 });
  const payload = {};
  await new Promise((r) => setTimeout(r, 50));
  global.gc?.();
  const before = process.memoryUsage().heapUsed;
  const N = 200_000;
  const ps = [];
  for (let i = 0; i < N; i++) ps.push(t.submit(payload, { key: "k", size: 0 }));
  global.gc?.();
  const after = process.memoryUsage().heapUsed;
  // The caller's array slot (8 bytes) is not engine memory; the promise the caller holds is counted.
  results.bytesPerQueuedItem = Math.round((after - before) / N - 8);
  for (const p of ps) p.catch(() => {});
  await engine.drain({ timeoutMs: 1 });
  await Promise.allSettled(ps);
}

// 4. Handoff latency through Redis: instance B is waiting; A finishes a request; how long until B sends?
const redisUrl = process.env.SLUICE_BENCH_REDIS_URL;
if (redisUrl) {
  const { redisPool } = await import("@viralpatel4439/sluice/redis");
  const ns = `bench${Date.now()}`;
  const mk = async (id) =>
    createEngine({
      namespace: ns,
      caller: "bench",
      instanceId: id,
      logger: silent,
      coordination: await redisPool({ url: redisUrl, instanceId: id, caller: "bench", logger: silent }),
    });
  const a = await mk("a");
  const b = await mk("b");
  const samples = [];
  let releasedAt = 0;
  let releaseA;
  const ta = a.target("handoff", {
    capacity: 1,
    send: () =>
      new Promise((r) => {
        releaseA = () => r(1);
      }),
  });
  const tb = b.target("handoff", {
    capacity: 1,
    send: async () => {
      samples.push(performance.now() - releasedAt);
      return 1;
    },
  });
  for (let i = 0; i < 200; i++) {
    const pa = ta.submit(i); // A takes the only token
    await new Promise((r) => setTimeout(r, 15));
    const pb = tb.submit(i); // B waits
    await new Promise((r) => setTimeout(r, 15));
    releasedAt = performance.now();
    releaseA(); // A finishes and has no more work: the token goes to B
    await pa;
    await pb;
  }
  results.handoffP50Ms = +quantile(samples.slice(10), 0.5).toFixed(3);
  results.handoffP99Ms = +quantile(samples.slice(10), 0.99).toFixed(3);
  // Baseline: a bare PING after the same idle gap.
  const { Redis } = await import("ioredis");
  const r = new Redis(redisUrl);
  const rtts = [];
  for (let i = 0; i < 150; i++) {
    await new Promise((x) => setTimeout(x, 15));
    const t0 = performance.now();
    await r.ping();
    rtts.push(performance.now() - t0);
  }
  r.disconnect();
  results.redisRttP50Ms = +quantile(rtts, 0.5).toFixed(3);
  results.handoffOverheadMs = +Math.max(0, results.handoffP50Ms - 2 * results.redisRttP50Ms).toFixed(3);
  await a.close();
  await b.close();
}

let failed = false;
for (const [k, b] of Object.entries(budgets)) {
  if (!(k in results)) continue;
  const v = results[k];
  const ok = b.max !== undefined ? v <= b.max : v >= b.min;
  if (!ok) failed = true;
  console.log(
    `${ok ? "PASS" : "FAIL"}  ${b.label}: ${v} (budget ${b.max !== undefined ? `≤ ${b.max}` : `≥ ${b.min}`})`,
  );
}
console.log(JSON.stringify(results));
process.exit(failed ? 1 : 0);
