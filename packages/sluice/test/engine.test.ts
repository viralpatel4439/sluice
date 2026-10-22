import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { ManualClock } from "../src/core/clock.js";
import { LocalHub } from "../src/core/coordinator.js";
import { DeadlineExceededError, QueueFullError, ShutdownError, TimeoutError } from "../src/core/errors.js";
import { MockService, settle, testEngine } from "./helpers.js";

describe("engine: core guarantee", () => {
  it("never exceeds capacity and resolves every submit with the real response", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 10);
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 5, send: svc.send });
    const results = Array.from({ length: 100 }, () => t.submit({ x: 1 }));
    await settle(clock, 400);
    const all = await Promise.all(results);
    expect(all.every((r) => (r as { status: number }).status === 200)).toBe(true);
    expect(svc.peak).toBe(5);
    expect(svc.calls).toBe(100);
    await engine.close();
  });

  it("keeps waiting items pending (no timeout by default)", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 1_000_000);
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 1, send: svc.send, timeoutMs: 10_000_000 });
    let settled = false;
    t.submit(1);
    t.submit(2).finally(() => {
      settled = true;
    });
    await settle(clock, 100_000, 10_000);
    expect(settled).toBe(false);
    expect(svc.calls).toBe(1);
    await engine.close();
  });

  it("returns the token when send throws synchronously", async () => {
    const clock = new ManualClock();
    const engine = testEngine(clock);
    let calls = 0;
    const t = engine.target("svc", {
      capacity: 1,
      retry: { max: 0 },
      send: () => {
        calls++;
        throw new Error("boom");
      },
    });
    await expect(t.submit(1)).rejects.toThrow("boom");
    await expect(t.submit(2)).rejects.toThrow("boom");
    expect(calls).toBe(2);
    expect(t.snapshot().inFlight).toBe(0);
    await engine.close();
  });
});

describe("engine: retries and failures", () => {
  it("retries 503 and resolves with the eventual success", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 5, (n) => (n <= 2 ? "503" : "ok"));
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 2, send: svc.send });
    const p = t.submit(1);
    await settle(clock, 2000);
    expect(((await p) as { status: number }).status).toBe(200);
    expect(t.snapshot().counters.retried).toBe(2);
    await engine.close();
  });

  it("honours Retry-After", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 1, (n) => (n === 1 ? "429" : "ok"));
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 1, send: svc.send });
    const p = t.submit(1);
    await settle(clock, 500);
    expect(svc.calls).toBe(1);
    await settle(clock, 700);
    expect(svc.calls).toBe(2);
    await p;
    await engine.close();
  });

  it("hands back the last response when retries are exhausted", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 1, () => "503");
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 1, retry: { max: 2, baseMs: 1, maxMs: 2 }, send: svc.send });
    const p = t.submit(1);
    await settle(clock, 200);
    expect(((await p) as { status: number }).status).toBe(503);
    expect(svc.calls).toBe(3);
    await engine.close();
  });

  it("aborts a hung request at timeoutMs, returns the token and retries", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 1, (n) => (n === 1 ? "hang" : "ok"));
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 1, timeoutMs: 100, send: svc.send });
    const p = t.submit(1);
    await settle(clock, 400);
    expect(((await p) as { status: number }).status).toBe(200);
    expect(svc.calls).toBe(2);
    await engine.close();
  });

  it("rejects with TimeoutError when retryOnTimeout is off", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 1, () => "hang");
    const engine = testEngine(clock);
    const t = engine.target("svc", {
      capacity: 1,
      timeoutMs: 50,
      retry: { retryOnTimeout: false },
      send: svc.send,
    });
    const p = t.submit(1);
    const check = expect(p).rejects.toBeInstanceOf(TimeoutError);
    await settle(clock, 100);
    await check;
    await engine.close();
  });

  it("caps retries in flight with the retry budget", async () => {
    const clock = new ManualClock();
    let retryInFlight = 0;
    let peakRetry = 0;
    const engine = testEngine(clock);
    const t = engine.target("svc", {
      capacity: 20,
      retry: { budget: 0.1, baseMs: 1, maxMs: 1 },
      send: async (item) => {
        const isRetry = item.attempt > 1;
        if (isRetry) peakRetry = Math.max(peakRetry, ++retryInFlight);
        await new Promise<void>((r) => clock.setTimer(10, r));
        if (isRetry) retryInFlight--;
        return item.attempt === 1
          ? { status: 503, headers: { get: () => null } }
          : { status: 200, headers: { get: () => null } };
      },
    });
    const all = Promise.all(Array.from({ length: 40 }, (_, i) => t.submit(i)));
    await settle(clock, 2000);
    await all;
    expect(peakRetry).toBeLessThanOrEqual(2);
    await engine.close();
  });

  it("opens the circuit on connection errors and recovers with probes", async () => {
    const clock = new ManualClock();
    let down = true;
    const svc = new MockService(clock, 1, () => (down ? "error" : "ok"));
    const engine = testEngine(clock);
    const t = engine.target("svc", {
      capacity: 10,
      circuit: { threshold: 3, openMs: 1000, probes: 1 },
      retry: { max: 100, baseMs: 1, maxMs: 5 },
      send: svc.send,
    });
    const all = Promise.all(Array.from({ length: 10 }, (_, i) => t.submit(i)));
    await settle(clock, 300);
    expect(t.snapshot().circuit).toBe("open");
    const callsWhileOpen = svc.calls;
    await settle(clock, 500);
    expect(svc.calls).toBe(callsWhileOpen);
    down = false;
    await settle(clock, 2000);
    await all;
    expect(t.snapshot().circuit).toBe("closed");
    await engine.close();
  });
});

describe("engine: queue policies", () => {
  it("drops expired items before sending, without spending a token", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 100);
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 1, send: svc.send });
    const first = t.submit(1);
    const late = t.submit(2, { deadlineMs: 50 });
    const check = expect(late).rejects.toBeInstanceOf(DeadlineExceededError);
    await settle(clock, 300);
    await check;
    await first;
    expect(svc.calls).toBe(1);
    await engine.close();
  });

  it("reject overflow throws QueueFullError", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 100);
    const engine = testEngine(clock);
    const t = engine.target("svc", {
      capacity: 1,
      queue: { maxItems: 2, overflow: "reject" },
      send: svc.send,
    });
    const ok = [t.submit(1)];
    await clock.advance(1);
    ok.push(t.submit(2), t.submit(3));
    await expect(t.submit(4)).rejects.toBeInstanceOf(QueueFullError);
    await settle(clock, 500);
    await Promise.all(ok);
    await engine.close();
  });

  it("block overflow makes the caller wait for room", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 10);
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 1, queue: { maxItems: 2 }, send: svc.send });
    const all = Array.from({ length: 10 }, (_, i) => t.submit(i));
    await clock.advance(1);
    expect(t.snapshot().queued).toBeLessThanOrEqual(2);
    await settle(clock, 300);
    await Promise.all(all);
    expect(svc.calls).toBe(10);
    await engine.close();
  });

  it("drop-lowest evicts bulk work for critical work", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 100);
    const engine = testEngine(clock);
    const t = engine.target("svc", {
      capacity: 1,
      queue: { maxItems: 1, overflow: "drop-lowest" },
      send: svc.send,
    });
    const running = t.submit(0);
    await clock.advance(1);
    const bulk = t.submit(1, { priority: "bulk" });
    const crit = t.submit(2, { priority: "critical" });
    await expect(bulk).rejects.toThrow(/dropped/);
    await settle(clock, 300);
    await Promise.all([running, crit]);
    await engine.close();
  });

  it("coalesces duplicate keys", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 10);
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 1, dedupe: "coalesce", send: svc.send });
    const a = t.submit(1, { key: "k" });
    const b = t.submit(1, { key: "k" });
    expect(a).toBe(b);
    await settle(clock, 50);
    await a;
    expect(svc.calls).toBe(1);
    await engine.close();
  });

  it("enforces route sub-limits inside the target", async () => {
    const clock = new ManualClock();
    let bulkInFlight = 0;
    let peakBulk = 0;
    const engine = testEngine(clock);
    const t = engine.target("svc", {
      capacity: 10,
      routes: { "bulk-write": { maxShare: 0.3 } },
      send: async (item) => {
        const isBulk = item.route === "bulk-write";
        if (isBulk) peakBulk = Math.max(peakBulk, ++bulkInFlight);
        await new Promise<void>((r) => clock.setTimer(10, r));
        if (isBulk) bulkInFlight--;
        return { status: 200 };
      },
    });
    const all = [
      ...Array.from({ length: 30 }, (_, i) => t.submit(i, { route: "bulk-write" })),
      ...Array.from({ length: 30 }, (_, i) => t.submit(i)),
    ];
    await settle(clock, 500);
    await Promise.all(all);
    expect(peakBulk).toBe(3);
    await engine.close();
  });
});

describe("engine: lifecycle", () => {
  it("drain finishes queued work, then rejects the rest and reports it", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 100);
    const engine = testEngine(clock);
    const t = engine.target("svc", { capacity: 1, send: svc.send });
    const ps = Array.from({ length: 5 }, (_, i) => t.submit(i).catch((e) => e));
    let unsent: unknown[] = [];
    const drained = engine.drain({
      timeoutMs: 250,
      onUnsent: (u) => {
        unsent = u;
      },
    });
    await expect(t.submit(99)).rejects.toBeInstanceOf(ShutdownError);
    await settle(clock, 400);
    await drained;
    const results = await Promise.all(ps);
    expect(results.filter((r) => r instanceof ShutdownError).length).toBe(unsent.length);
    expect(unsent.length).toBeGreaterThan(0);
    expect(engine.health().ready).toBe(false);
  });

  it("creates targets lazily from templates and evicts idle ones", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 1);
    const engine = testEngine(clock, { idleTtlMs: 1000, reportIntervalMs: 500 });
    engine.define("partner-*", { capacity: 2, send: svc.send });
    const p = engine.submit(1, { target: "partner-acme" });
    await settle(clock, 20);
    await p;
    expect(engine.get("partner-acme")).toBeDefined();
    await settle(clock, 2000, 100);
    expect(engine.get("partner-acme")).toBeUndefined();
    expect(() => engine.submit(1, { target: "nobody" })).toThrow(/unknown target/);
    await engine.close();
  });

  it("validates config", () => {
    const engine = testEngine(new ManualClock());
    expect(() => engine.target("bad name!", { send: async () => 1 })).toThrow(/must match/);
    expect(() => engine.target("x", { send: async () => 1, retry: { budget: 2 } })).toThrow(/budget/);
  });
});

describe("engine: several instances sharing one pool", () => {
  it("keeps the global in-flight count at N and shares fairly by weight", async () => {
    const clock = new ManualClock();
    const hub = new LocalHub();
    const svc = new MockService(clock, 10);
    const a = testEngine(clock, { coordination: hub.client(), caller: "orders", weight: 3 });
    const b = testEngine(clock, { coordination: hub.client(), caller: "billing", weight: 1 });
    const ta = a.target("inv", { capacity: 8, send: svc.send });
    const tb = b.target("inv", { capacity: 8, send: svc.send });
    const work = [
      ...Array.from({ length: 400 }, (_, i) => ta.submit(i)),
      ...Array.from({ length: 400 }, (_, i) => tb.submit(i)),
    ];
    let samples = 0;
    let aHeld = 0;
    let bHeld = 0;
    for (let i = 0; i < 100; i++) {
      await clock.advance(5);
      if (ta.snapshot().queued > 50 && tb.snapshot().queued > 50) {
        samples++;
        aHeld += ta.snapshot().inFlight;
        bHeld += tb.snapshot().inFlight;
      }
    }
    await settle(clock, 6000);
    await Promise.all(work);
    expect(svc.peak).toBeLessThanOrEqual(8);
    expect(samples).toBeGreaterThan(10);
    expect(aHeld / bHeld).toBeGreaterThan(2);
    await a.close();
    await b.close();
  });
});

describe("property: random workloads", () => {
  it("never exceeds N, settles every call exactly once, and returns all tokens", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 8 }),
        fc.array(
          fc.record({
            at: fc.integer({ min: 0, max: 200 }),
            latency: fc.integer({ min: 1, max: 50 }),
            kind: fc.constantFrom("ok", "ok", "ok", "503", "error"),
          }),
          { minLength: 1, maxLength: 60 },
        ),
        fc.array(fc.integer({ min: 1, max: 10 }), { maxLength: 3 }),
        async (cap, jobs, capChanges) => {
          const clock = new ManualClock();
          const hub = new LocalHub();
          const engines = [
            testEngine(clock, { coordination: hub.client() }),
            testEngine(clock, { coordination: hub.client() }),
          ];
          let inFlight = 0;
          let peak = 0;
          let maxCap = cap;
          const targets = engines.map((e) =>
            e.target("t", {
              capacity: cap,
              retry: { max: 2, baseMs: 1, maxMs: 10 },
              send: async (item) => {
                const job = item.payload as (typeof jobs)[number];
                inFlight++;
                peak = Math.max(peak, inFlight);
                await new Promise<void>((r) => clock.setTimer(job.latency, r));
                inFlight--;
                if (job.kind === "error" && item.attempt === 1) throw new TypeError("fetch failed");
                return {
                  status: job.kind === "503" && item.attempt === 1 ? 503 : 200,
                  headers: { get: () => null },
                };
              },
            }),
          );
          const settledCount = new Map<number, number>();
          const ps = jobs.map((job, i) =>
            new Promise<void>((r) => clock.setTimer(job.at, r)).then(() =>
              targets[i % 2]!.submit(job).then(
                () => settledCount.set(i, (settledCount.get(i) ?? 0) + 1),
                () => settledCount.set(i, (settledCount.get(i) ?? 0) + 1),
              ),
            ),
          );
          for (const [k, c] of capChanges.entries()) {
            clock.setTimer(50 * (k + 1), () => engines[0]!.coordinator.setCapacity("default/t", c));
            maxCap = Math.max(maxCap, c);
          }
          await settle(clock, 3000, 10);
          await Promise.all(ps);
          expect(peak).toBeLessThanOrEqual(maxCap);
          expect([...settledCount.values()].every((c) => c === 1)).toBe(true);
          expect(settledCount.size).toBe(jobs.length);
          for (const t of targets) expect(t.snapshot().inFlight).toBe(0);
          const pool = hub.inspect("default/t")!;
          const held = Object.values(pool.held).reduce((a, b) => a + b, 0);
          expect(pool.free + held).toBe(pool.capacity + pool.debt);
          for (const e of engines) await e.close();
        },
      ),
      { numRuns: 150 },
    );
  });
});
