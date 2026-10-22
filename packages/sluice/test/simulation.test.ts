import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { ManualClock } from "../src/core/clock.js";
import type { Coordinator, CoordinatorHandlers } from "../src/core/coordinator.js";
import { LocalHub } from "../src/core/coordinator.js";
import { createEngine } from "../src/core/engine.js";
import { consoleLogger } from "../src/core/logger.js";

/**
 * Deterministic fleet simulation with fault injection.
 *
 * Several engines share one in-process pool. A FaultyLink sits between each
 * engine and the pool and, driven by a seeded schedule on simulated time:
 *  - delays and reorders grant/state delivery,
 *  - cuts the instance off (fence) and later restores it; while cut off the
 *    pool reclaims its tokens, as Redis does after the lease,
 *  - crashes instances (their tokens are reclaimed, their requests keep running).
 *
 * Invariant checked at every send: requests sent under pool tokens that are not
 * orphans of a fenced/crashed instance never exceed N. Fallback sends are bounded
 * separately by each fenced instance's fallback limit.
 */

interface Faults {
  maxDelayMs: number;
  /** Fence windows per instance: [start, end) in simulated ms. */
  fences: [number, number][][];
  /** Crash time per instance, if any. */
  crashes: (number | null | undefined)[];
}

class FaultyLink implements Coordinator {
  readonly kind = "local" as const;
  private h: CoordinatorHandlers | undefined;
  fenced = false;
  crashed = false;
  private seq = 0;
  private lastAt = 0;
  private readonly pools = new Map<string, { caller: string; capacity: number }>();

  constructor(
    private readonly inner: Coordinator,
    private readonly clock: ManualClock,
    private readonly rand: () => number,
    private readonly maxDelayMs: number,
    readonly fallback: number,
  ) {
    inner.attach({
      grant: (pool, n) =>
        this.later(() =>
          this.fenced || this.crashed ? this.inner.release(pool, n) : this.h?.grant(pool, n),
        ),
      state: (pool, s) => this.later(() => !this.crashed && this.h?.state(pool, s)),
      fenced: () => {},
      restored: () => {},
    });
  }

  /** Delay delivery by a random amount; occasionally reorders relative to other messages. */
  private later(fn: () => void): void {
    if (this.maxDelayMs === 0) return void queueMicrotask(fn);
    const delay = Math.floor(this.rand() * this.maxDelayMs);
    // Keep grant/state order per link mostly FIFO, but allow ties to reorder.
    const at = Math.max(this.lastAt, this.clock.now() + delay);
    this.lastAt = at;
    this.clock.setTimer(at - this.clock.now(), fn);
    this.seq++;
  }

  attach(h: CoordinatorHandlers): void {
    this.h = h;
  }
  async join(pool: string, opts: { caller: string; capacity: number; held?: number }) {
    this.pools.set(pool, opts);
    return this.inner.join(pool, opts);
  }
  demand(pool: string, want: number, hint: { priority: "critical" | "standard" | "bulk"; backlog: number }) {
    if (!this.fenced && !this.crashed) this.inner.demand(pool, want, hint);
  }
  release(pool: string, n: number) {
    if (!this.fenced && !this.crashed) this.inner.release(pool, n);
  }
  report(pool: string, load: { held: number; inFlight: number; backlog: number }) {
    if (!this.fenced && !this.crashed) this.inner.report(pool, load);
  }
  setCapacity(pool: string, n: number) {
    if (!this.fenced && !this.crashed) this.inner.setCapacity(pool, n);
  }
  ownsCapacity(pool: string) {
    return !this.fenced && !this.crashed && this.inner.ownsCapacity(pool);
  }
  async leave(pool: string) {
    if (!this.crashed) await this.inner.leave(pool);
  }
  async close() {
    if (!this.crashed) await this.inner.close();
  }

  /** Cut off: the pool reclaims everything we hold (worst case: immediately), engine falls back. */
  async fence(): Promise<void> {
    if (this.fenced || this.crashed) return;
    this.fenced = true;
    for (const pool of this.pools.keys()) await this.inner.leave(pool);
    for (const pool of this.pools.keys()) this.h?.fenced(pool, this.fallback);
  }

  async restore(): Promise<void> {
    if (!this.fenced || this.crashed) return;
    for (const [pool, opts] of this.pools) await this.inner.join(pool, { ...opts, held: 0 });
    this.fenced = false;
    for (const pool of this.pools.keys()) this.h?.restored(pool);
  }

  async crash(): Promise<void> {
    if (this.crashed) return;
    if (!this.fenced) for (const pool of this.pools.keys()) await this.inner.leave(pool);
    this.crashed = true;
  }
}

async function runFleet(
  seed: number,
  n: number,
  capacity: number,
  faults: Faults,
  jobs: { at: number; who: number; ms: number }[],
) {
  let s = seed >>> 0 || 1;
  const rand = () => {
    s ^= s << 13;
    s ^= s >>> 17;
    s ^= s << 5;
    return ((s >>> 0) % 1_000_000) / 1_000_000;
  };
  const clock = new ManualClock();
  const hub = new LocalHub();
  const fallback = Math.max(1, Math.floor(capacity / n));
  const links = Array.from(
    { length: n },
    () => new FaultyLink(hub.client(), clock, rand, faults.maxDelayMs, fallback),
  );

  // Accounting of what the target actually sees.
  type Req = { who: number; pooled: boolean; orphan: boolean };
  const running = new Set<Req>();
  const rejections: string[] = [];
  let sends = 0;
  let fallbackSends = 0;
  const violations: string[] = [];
  const check = () => {
    const pooled = [...running].filter((r) => r.pooled && !r.orphan).length;
    if (pooled > capacity) violations.push(`t=${clock.now()} pooled in flight ${pooled} > N=${capacity}`);
    for (let i = 0; i < n; i++) {
      const fb = [...running].filter((r) => r.who === i && !r.pooled && !r.orphan).length;
      if (fb > fallback)
        violations.push(`t=${clock.now()} instance ${i} fallback in flight ${fb} > ${fallback}`);
    }
  };
  const orphanAll = (who: number) => {
    for (const r of running) if (r.who === who) r.orphan = true;
  };

  const engines = links.map((link, i) =>
    createEngine({
      clock,
      coordination: link,
      logger: consoleLogger("silent"),
      random: rand,
      caller: `c${i % 2}`,
      instanceId: `i${i}`,
    }),
  );
  const targets = engines.map((e, i) =>
    e.target("t", {
      capacity,
      timeoutMs: 1_000_000,
      retry: { max: 0 },
      send: async (item) => {
        // A crashed process sends nothing; the engine object lingers only because this is a simulation.
        if (links[i]!.crashed) return { status: 200 };
        const req: Req = { who: i, pooled: !links[i]!.fenced, orphan: false };
        running.add(req);
        sends++;
        if (!req.pooled) fallbackSends++;
        check();
        await new Promise<void>((r) => clock.setTimer((item.payload as { ms: number }).ms, r));
        running.delete(req);
        return { status: 200 };
      },
    }),
  );

  for (const [i, windows] of faults.fences.entries()) {
    for (const [start, end] of windows) {
      clock.setTimer(start, () => {
        orphanAll(i);
        void links[i]!.fence();
      });
      clock.setTimer(end, () => void links[i]!.restore());
    }
  }
  for (const [i, at] of faults.crashes.entries()) {
    if (at !== undefined && at !== null) {
      clock.setTimer(at, () => {
        orphanAll(i);
        void links[i]!.crash();
      });
    }
  }

  const settled = new Set<number>();
  jobs.forEach((job, k) => {
    clock.setTimer(job.at, () => {
      if (links[job.who]!.crashed) {
        settled.add(k);
        return;
      }
      targets[job.who]!.submit({ ms: job.ms }).then(
        () => settled.add(k),
        (error: unknown) => {
          rejections.push(String((error as Error)?.message ?? error));
          settled.add(k);
        },
      );
    });
  });

  // Work queued on a crashed process dies with it; every other job must finish.
  const mustSettle = jobs
    .map((j, k) => [j, k] as const)
    .filter(([j]) => faults.crashes[j.who] == null)
    .map(([, k]) => k);
  const done = () => mustSettle.every((k) => settled.has(k));
  for (let t = 0; t < 20_000 && !done(); t += 20) await clock.advance(20);
  return {
    violations,
    settled: mustSettle.filter((k) => settled.has(k)).length,
    total: mustSettle.length,
    links,
    engines,
    hub,
    sends,
    fallbackSends,
    rejections,
  };
}

const faultsArb = (n: number) =>
  fc.record({
    maxDelayMs: fc.constantFrom(0, 1, 5, 20),
    fences: fc.array(
      fc.array(
        fc
          .tuple(fc.integer({ min: 0, max: 800 }), fc.integer({ min: 20, max: 400 }))
          .map(([a, d]) => [a, a + d] as [number, number]),
        { maxLength: 2 },
      ),
      { minLength: n, maxLength: n },
    ),
    crashes: fc.array(
      fc.oneof(
        { arbitrary: fc.constant(null), weight: 4 },
        { arbitrary: fc.integer({ min: 0, max: 1000 }), weight: 1 },
      ),
      { minLength: n, maxLength: n },
    ),
  });

describe("deterministic fleet simulation with fault injection", () => {
  it("never exceeds N under delays, partitions and crashes, and live work always completes", async () => {
    const stats = { runs: 0, sends: 0, fallbackSends: 0, fences: 0, sample: [] as string[] };
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 2 ** 31 - 1 }),
        fc.integer({ min: 2, max: 4 }).chain((n) =>
          fc.tuple(
            fc.constant(n),
            fc.integer({ min: 2, max: 8 }),
            faultsArb(n),
            fc.array(
              fc.record({
                at: fc.integer({ min: 0, max: 1200 }),
                who: fc.integer({ min: 0, max: n - 1 }),
                ms: fc.integer({ min: 1, max: 60 }),
              }),
              { minLength: 20, maxLength: 120, size: "max" },
            ),
          ),
        ),
        async (seed, [n, capacity, faults, jobs]) => {
          // Non-overlapping fence windows per instance.
          faults.fences = faults.fences.map((w) =>
            w.sort((a, b) => a[0] - b[0]).filter((x, i, arr) => i === 0 || x[0] >= arr[i - 1]![1]),
          );
          const r = await runFleet(seed, n, capacity, faults, jobs);
          stats.runs++;
          if (r.rejections.length && stats.sample.length < 5) stats.sample.push(...r.rejections.slice(0, 2));
          stats.sends += r.sends;
          stats.fallbackSends += r.fallbackSends;
          stats.fences += faults.fences.flat().length;
          if (r.violations.length) console.log("VIOL", r.violations.slice(0, 5).join(" | "));
          expect(r.violations).toEqual([]);
          expect(r.settled).toBe(r.total);
          for (const e of r.engines) await e.close().catch(() => {});
        },
      ),
      { numRuns: 200 },
    );
    console.log("SIMSTATS", JSON.stringify(stats));
    // The simulation must actually exercise the system, not pass vacuously.
    expect(stats.runs).toBe(200);
    expect(stats.sends).toBeGreaterThan(2000);
    expect(stats.fallbackSends).toBeGreaterThan(0);
  }, 120_000);
});
