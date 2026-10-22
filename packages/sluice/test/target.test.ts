import { describe, expect, it } from "vitest";
import { ManualClock } from "../src/core/clock.js";
import type { Coordinator, CoordinatorHandlers, JoinOptions, PoolState } from "../src/core/coordinator.js";
import { createEngine } from "../src/core/engine.js";
import { DeadlineExceededError, TimeoutError } from "../src/core/errors.js";
import type { EngineEvents, EventName } from "../src/core/events.js";
import { consoleLogger } from "../src/core/logger.js";
import type { Priority } from "../src/core/types.js";
import { MockService, response, settle, testEngine } from "./helpers.js";

/** A coordinator the test drives by hand: it records every call and grants only when told to. */
class FakeCoordinator implements Coordinator {
  readonly kind = "local" as const;
  h!: CoordinatorHandlers;
  owner = true;
  failJoin = false;
  joins: [string, JoinOptions][] = [];
  demands: [string, number, { priority: Priority; backlog: number }][] = [];
  releases: [string, number][] = [];
  reports: [string, { held: number; inFlight: number; backlog: number }][] = [];
  capacities: [string, number][] = [];
  leaves: string[] = [];
  constructor(private capacity = 4) {}
  attach(h: CoordinatorHandlers) {
    this.h = h;
  }
  async join(pool: string, opts: JoinOptions): Promise<PoolState> {
    this.joins.push([pool, opts]);
    if (this.failJoin) throw new Error("coordinator down");
    return { capacity: this.capacity, contended: false, fairShare: this.capacity };
  }
  demand(pool: string, want: number, hint: { priority: Priority; backlog: number }) {
    this.demands.push([pool, want, hint]);
  }
  release(pool: string, n: number) {
    this.releases.push([pool, n]);
  }
  report(pool: string, load: { held: number; inFlight: number; backlog: number }) {
    this.reports.push([pool, load]);
  }
  setCapacity(pool: string, n: number) {
    this.capacities.push([pool, n]);
  }
  ownsCapacity() {
    return this.owner;
  }
  async leave(pool: string) {
    this.leaves.push(pool);
  }
  async close() {}
}

function record(engine: ReturnType<typeof createEngine>, names: EventName[]) {
  const events: { name: EventName; e: unknown }[] = [];
  for (const name of names) engine.on(name, (e) => events.push({ name, e }));
  const of = <K extends EventName>(n: K) =>
    events.filter((x) => x.name === n).map((x) => x.e as EngineEvents[K]);
  return { events, of };
}

const flush = () => new Promise((r) => setImmediate(r));

describe("target: events, counters and headers", () => {
  it("emits exact item events and counters through retries, success and failure", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 5, (n) => (n === 1 ? "503" : n === 3 ? "error" : "ok"));
    const engine = testEngine(clock, { caller: "orders" });
    const rec = record(engine, ["item:sent", "item:done", "item:retry", "item:dead"]);
    const t = engine.target("inv", { capacity: 1, retry: { max: 1, baseMs: 1, maxMs: 2 }, send: svc.send });
    const ok = t.submit("a", { key: "k1", priority: "critical" });
    await settle(clock, 200);
    expect(((await ok) as { status: number }).status).toBe(200);
    const bad = t.submit("b", { key: "k2" });
    const badCheck = expect(bad).rejects.toThrow("fetch failed");
    svc.behave = () => "error";
    await settle(clock, 200);
    await badCheck;
    expect(rec.of("item:sent").map((e) => [e.key, e.attempt, e.priority, e.target])).toEqual([
      ["k1", 1, "critical", "inv"],
      ["k1", 2, "critical", "inv"],
      ["k2", 1, "standard", "inv"],
      ["k2", 2, "standard", "inv"],
    ]);
    expect(rec.of("item:retry").map((e) => [e.key, e.reason])).toEqual([
      ["k1", "status 503"],
      ["k2", "TypeError"],
    ]);
    expect(rec.of("item:done").map((e) => e.key)).toEqual(["k1"]);
    expect(rec.of("item:done")[0]!.durationMs).toBeGreaterThan(0);
    expect(rec.of("item:dead").map((e) => e.key)).toEqual(["k2"]);
    expect(t.snapshot().counters).toEqual({
      submitted: 2,
      sent: 4,
      done: 1,
      failed: 1,
      retried: 2,
      expired: 0,
      dead: 1,
      dropped: 0,
      rejected: 0,
    });
    await engine.close();
  });

  it("sends a stable idempotency key, the caller, user headers and the attempt number", async () => {
    const clock = new ManualClock();
    const seen: Record<string, string>[] = [];
    const engine = testEngine(clock, { caller: "billing" });
    let n = 0;
    const t = engine.target("pay", {
      capacity: 1,
      retry: { baseMs: 1, maxMs: 1 },
      send: async (item) => {
        seen.push({ ...item.headers });
        return response(++n < 3 ? 503 : 200);
      },
    });
    const p = t.submit({}, { headers: { "x-trace": "abc" } });
    await settle(clock, 100);
    await p;
    expect(seen).toHaveLength(3);
    expect(seen[0]!["idempotency-key"]).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/);
    expect(new Set(seen.map((h) => h["idempotency-key"])).size).toBe(1);
    expect(seen.map((h) => h["x-sluice-attempt"])).toEqual(["1", "2", "3"]);
    expect(seen.every((h) => h["x-sluice-caller"] === "billing" && h["x-trace"] === "abc")).toBe(true);
    const own = t.submit({}, { key: "mine" });
    n = 10;
    await settle(clock, 20);
    await own;
    expect(seen.at(-1)!["idempotency-key"]).toBe("mine");
    await engine.close();
  });

  it("counts payload bytes for strings, buffers, objects and odd values", async () => {
    const clock = new ManualClock();
    const engine = testEngine(clock);
    const t = engine.target("bytes", { capacity: 0, send: async () => 1 });
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const payloads: unknown[] = [
      "héllo",
      new Uint8Array(7),
      new ArrayBuffer(5),
      { a: 1 },
      null,
      undefined,
      circular,
    ];
    const ps = payloads.map((p) => t.submit(p).catch(() => {}));
    await flush();
    // "héllo" = 6 bytes UTF-8, 7, 5, '{"a":1}' = 7, null 0, undefined 0, circular 0
    expect(t.snapshot().queuedBytes).toBe(6 + 7 + 5 + 7);
    expect(t.snapshot().queued).toBe(7);
    const drained = engine.drain({ timeoutMs: 1 });
    await settle(clock, 20);
    await drained;
    await Promise.all(ps);
  });

  it("requires a key when asked, and coalesces only keyed duplicates while in flight", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 10);
    const engine = testEngine(clock);
    const strict = engine.target("strict", { capacity: 1, requireKey: true, send: svc.send });
    await expect(strict.submit(1)).rejects.toThrow(/requires a key/);
    const t = engine.target("co", { capacity: 1, dedupe: "coalesce", send: svc.send });
    const a = t.submit(1);
    const b = t.submit(1);
    expect(a).not.toBe(b); // no key: never coalesced
    const k1 = t.submit(1, { key: "same" });
    expect(t.submit(1, { key: "same" })).toBe(k1);
    await settle(clock, 100);
    await Promise.all([a, b, k1]);
    const again = t.submit(1, { key: "same" }); // the first one finished: a fresh send
    expect(again).not.toBe(k1);
    await settle(clock, 50);
    await again;
    expect(svc.calls).toBe(4);
    const plain = engine.target("plain", { capacity: 1, send: svc.send });
    expect(plain.submit(1, { key: "x" })).not.toBe(plain.submit(1, { key: "x" }));
    await settle(clock, 50);
    await engine.close();
  });
});

describe("target: cancellation, deadlines and timeouts", () => {
  it("rejects a pre-aborted signal at once and removes items aborted while waiting", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 50);
    const engine = testEngine(clock);
    const t = engine.target("abort", { capacity: 1, send: svc.send });
    const pre = new AbortController();
    pre.abort(new Error("already"));
    await expect(t.submit(1, { signal: pre.signal })).rejects.toThrow("already");
    const first = t.submit(1);
    const ac = new AbortController();
    const waiting = t.submit(2, { signal: ac.signal });
    ac.abort(new Error("changed my mind"));
    await expect(waiting).rejects.toThrow("changed my mind");
    expect(t.snapshot().queued).toBe(0);
    await settle(clock, 100);
    await first;
    expect(svc.calls).toBe(1);
    expect(t.snapshot().counters.expired).toBe(0);
    await engine.close();
  });

  it("aborting during a retry delay cancels the retry; aborting mid-send is not retried", async () => {
    const clock = new ManualClock();
    let calls = 0;
    const engine = testEngine(clock);
    const t = engine.target("abort2", {
      capacity: 2,
      retry: { baseMs: 1000, maxMs: 1000 },
      send: async (item, signal) => {
        calls++;
        if (item.payload === "mid") {
          await new Promise<void>((r, j) => signal.addEventListener("abort", () => j(signal.reason)));
        }
        return response(503);
      },
    });
    const ac = new AbortController();
    const delayed = t.submit("delay", { signal: ac.signal });
    await settle(clock, 10); // first attempt 503 → waiting in retry delay
    ac.abort(new Error("stop"));
    await expect(delayed).rejects.toThrow("stop");
    const ac2 = new AbortController();
    const mid = t.submit("mid", { signal: ac2.signal });
    await settle(clock, 10);
    ac2.abort(new Error("cut"));
    await expect(mid).rejects.toThrow("cut");
    await settle(clock, 3000);
    expect(calls).toBe(2);
    expect(t.snapshot().retryQueued).toBe(0);
    await engine.close();
  });

  it("expires an item whose deadline passes during its retry delay", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 1, () => "503");
    const engine = testEngine(clock);
    const rec = record(engine, ["item:expired"]);
    const t = engine.target("dl", { capacity: 1, retry: { baseMs: 1000, maxMs: 1000 }, send: svc.send });
    const p = t.submit(1, { deadlineMs: 300, key: "late" });
    const check = expect(p).rejects.toBeInstanceOf(DeadlineExceededError);
    await settle(clock, 2000);
    await check;
    expect(svc.calls).toBe(1);
    expect(rec.of("item:expired").map((e) => e.key)).toEqual(["late"]);
    expect(t.snapshot().counters.expired).toBe(1);
    await engine.close();
  });

  it("times out a hung send with TimeoutError, aborting its signal", async () => {
    const clock = new ManualClock();
    let aborted: unknown;
    const engine = testEngine(clock);
    const t = engine.target("to", {
      capacity: 1,
      timeoutMs: 100,
      retry: { max: 0 },
      send: (_i, signal) =>
        new Promise((_, reject) =>
          signal.addEventListener("abort", () => {
            aborted = signal.reason;
            reject(signal.reason);
          }),
        ),
    });
    const p = t.submit(1);
    const check = expect(p).rejects.toBeInstanceOf(TimeoutError);
    await settle(clock, 99);
    expect(aborted).toBeUndefined();
    await settle(clock, 10);
    await check;
    expect(aborted).toBeInstanceOf(TimeoutError);
    expect(t.snapshot().inFlight).toBe(0);
    await engine.close();
  });
});

describe("target: robustness against user code", () => {
  it("turns throwing hooks into error events and keeps working", async () => {
    const clock = new ManualClock();
    const engine = testEngine(clock);
    const rec = record(engine, ["error"]);
    engine.setTracingHooks({
      capture: () => {
        throw new Error("capture boom");
      },
      send: (_c, _i, run) => run(),
    });
    let n = 0;
    const t = engine.target("hooks", {
      capacity: 1,
      retry: {
        max: 1,
        baseMs: 1,
        maxMs: 1,
        classify: () => {
          if (++n === 1) throw new Error("classify boom");
          return { kind: "retry" };
        },
      },
      onDead: () => {
        throw new Error("onDead boom");
      },
      send: async () => {
        throw new TypeError("fetch failed");
      },
    });
    engine.on("item:sent", () => {
      throw new Error("listener boom");
    });
    await expect(t.submit(1)).rejects.toThrow("fetch failed"); // classify threw on an error → fail
    const second = t.submit(2);
    const check = expect(second).rejects.toThrow("fetch failed");
    await settle(clock, 50);
    await check;
    const where = rec.of("error").map((e) => e.where);
    expect(where).toContain("hooks.capture");
    expect(where).toContain("retry.classify");
    expect(where).toContain("onDead");
    expect(where).toContain("listener:item:sent");
    expect(t.snapshot().counters.dead).toBe(1);
    expect(t.snapshot().counters.failed).toBe(2);
    await engine.close();
  });

  it("a classify that throws on a result resolves with the result", async () => {
    const clock = new ManualClock();
    const engine = testEngine(clock);
    const t = engine.target("cls", {
      capacity: 1,
      retry: {
        classify: () => {
          throw new Error("nope");
        },
      },
      send: async () => response(503),
    });
    expect(((await t.submit(1)) as { status: number }).status).toBe(503);
    await engine.close();
  });

  it("only counts real connection failures towards the circuit", async () => {
    const clock = new ManualClock();
    const engine = testEngine(clock);
    const kinds: unknown[] = [
      Object.assign(new Error("x"), { code: "ECONNREFUSED" }),
      Object.assign(new Error("y"), { cause: { code: "UND_ERR_SOCKET" } }),
      new TypeError("fetch failed"),
    ];
    let i = 0;
    const t = engine.target("circ", {
      capacity: 1,
      retry: { max: 0 },
      circuit: { threshold: 3, openMs: 1000, probes: 1 },
      send: async () => {
        throw kinds[i++ % kinds.length];
      },
    });
    for (let k = 0; k < 3; k++) await t.submit(k).catch(() => {});
    expect(t.snapshot().circuit).toBe("open");
    const other = engine.target("circ2", {
      capacity: 1,
      retry: { max: 0 },
      circuit: { threshold: 2, openMs: 1000, probes: 1 },
      send: async () => {
        // Application errors and non-Error throws are not connection failures.
        throw i++ % 2 ? new RangeError("bad input") : "a string";
      },
    });
    for (let k = 0; k < 6; k++) await other.submit(k).catch(() => {});
    expect(other.snapshot().circuit).toBe("closed");
    await engine.close();
  });

  it("cancels an unread response body before retrying", async () => {
    const clock = new ManualClock();
    let cancelled = 0;
    let n = 0;
    const engine = testEngine(clock);
    const t = engine.target("body", {
      capacity: 1,
      retry: { baseMs: 1, maxMs: 1 },
      send: async () => ({
        ...response(++n === 1 ? 503 : 200),
        body: {
          cancel: async () => {
            cancelled++;
          },
        },
      }),
    });
    const p = t.submit(1);
    await settle(clock, 20);
    await p;
    expect(cancelled).toBe(1);
    await engine.close();
  });
});

describe("target: coordinator interaction", () => {
  it("asks for tokens with the right want, priority and backlog, and releases what it cannot use", async () => {
    const clock = new ManualClock();
    const fake = new FakeCoordinator(4);
    const engine = createEngine({
      clock,
      coordination: fake,
      logger: consoleLogger("silent"),
      caller: "c",
      namespace: "ns",
    });
    const pending: (() => void)[] = [];
    const t = engine.target("t", {
      capacity: 4,
      send: () =>
        new Promise((r) => {
          pending.push(() => r(1));
        }),
    });
    await flush();
    expect(fake.joins[0]).toEqual(["ns/t", { caller: "c", capacity: 4, weight: 1 }]);
    const ps = [
      t.submit(1, { priority: "bulk" }),
      t.submit(2, { priority: "critical" }),
      t.submit(3, { priority: "standard" }),
    ];
    await flush();
    expect(fake.demands.at(-1)).toEqual(["ns/t", 3, { priority: "critical", backlog: 3 }]);
    fake.h.grant("ns/t", 5); // two more than needed
    await flush();
    expect(t.snapshot().inFlight).toBe(3);
    expect(fake.releases.reduce((s, [, n]) => s + n, 0)).toBe(2);
    for (const done of pending.splice(0)) done();
    await Promise.all(ps);
    await settle(clock, 10);
    expect(t.snapshot().held).toBe(0); // idle: every token returned
    clock.advance(2000);
    await settle(clock, 2100, 100);
    expect(fake.reports.some(([p]) => p === "ns/t")).toBe(true);
    await engine.close();
    expect(fake.leaves).toContain("ns/t");
  });

  it("gives back tokens above its fair share when another instance is starved", async () => {
    const clock = new ManualClock();
    const fake = new FakeCoordinator(4);
    const engine = createEngine({ clock, coordination: fake, logger: consoleLogger("silent"), caller: "c" });
    const svc = new MockService(clock, 10);
    const t = engine.target("fs", { capacity: 4, send: svc.send });
    await flush();
    const ps = Array.from({ length: 20 }, (_, i) => t.submit(i));
    await flush();
    fake.h.grant("default/fs", 4);
    await flush();
    fake.releases.length = 0;
    fake.h.state("default/fs", { capacity: 4, contended: true, fairShare: 1 });
    await settle(clock, 30);
    expect(fake.releases.reduce((s, [, n]) => s + n, 0)).toBe(3);
    expect(t.snapshot().held).toBe(1);
    fake.h.state("default/fs", { capacity: 4, contended: false, fairShare: 1 });
    fake.h.grant("default/fs", 3);
    await settle(clock, 300);
    await Promise.all(ps);
    await engine.close();
  });

  it("falls back to a local limit on fence, keeps orphans separate, and returns grants that arrive while fenced", async () => {
    const clock = new ManualClock();
    const fake = new FakeCoordinator(6);
    const engine = createEngine({ clock, coordination: fake, logger: consoleLogger("silent"), caller: "c" });
    const rec = record(engine, ["fallback:on", "fallback:off"]);
    let inFlight = 0;
    let peakFallback = 0;
    let fenced = false;
    const t = engine.target("f", {
      capacity: 6,
      send: async () => {
        inFlight++;
        if (fenced) peakFallback = Math.max(peakFallback, inFlight);
        await new Promise<void>((r) => clock.setTimer(50, r));
        inFlight--;
        return 1;
      },
    });
    await flush();
    const ps = Array.from({ length: 30 }, (_, i) => t.submit(i));
    await flush();
    fake.h.grant("default/f", 6);
    await settle(clock, 10);
    expect(inFlight).toBe(6);
    fenced = true;
    fake.h.fenced("default/f", 2);
    fake.h.fenced("default/f", 2); // idempotent
    expect(t.snapshot().fallback).toBe(true);
    expect(rec.of("fallback:on")).toEqual([{ target: "f", capacity: 2 }]);
    fake.releases.length = 0;
    fake.h.grant("default/f", 3); // arrives while fenced: handed straight back
    expect(fake.releases).toEqual([["default/f", 3]]);
    await settle(clock, 60); // orphans finish
    await settle(clock, 200);
    expect(peakFallback).toBeLessThanOrEqual(6); // the 6 orphans, then never more than 2 new
    const sent = t.snapshot().counters.sent;
    expect(sent).toBeGreaterThan(6);
    fenced = false;
    fake.h.restored("default/f");
    fake.h.restored("default/f"); // idempotent
    expect(rec.of("fallback:off")).toEqual([{ target: "f" }]);
    expect(t.snapshot().fallback).toBe(false);
    fake.h.grant("default/f", 6);
    await settle(clock, 600);
    await Promise.all(ps);
    expect(t.snapshot().inFlight).toBe(0);
    await engine.close();
  });

  it("starts in fallback when it cannot join the pool", async () => {
    const clock = new ManualClock();
    const fake = new FakeCoordinator(8);
    fake.failJoin = true;
    const engine = createEngine({ clock, coordination: fake, logger: consoleLogger("silent"), caller: "c" });
    const svc = new MockService(clock, 10);
    const t = engine.target("j", { capacity: 8, send: svc.send });
    const ps = Array.from({ length: 12 }, (_, i) => t.submit(i));
    await settle(clock, 200);
    await Promise.all(ps);
    expect(t.snapshot().fallback).toBe(true);
    expect(svc.peak).toBe(4); // half of the configured capacity
    await engine.close();
  });

  it("publishes desired capacity only as owner, updates route caps, and emits capacity:changed", async () => {
    const clock = new ManualClock();
    const fake = new FakeCoordinator(10);
    fake.owner = false;
    const engine = createEngine({ clock, coordination: fake, logger: consoleLogger("silent"), caller: "c" });
    const rec = record(engine, ["capacity:changed"]);
    const t = engine.target("own", {
      capacity: { type: "callback", fn: () => 20, intervalMs: 50, fallback: 10 },
      routes: { r: { maxShare: 0.5 } },
      send: async () => 1,
    });
    await settle(clock, 100);
    expect(fake.capacities).toEqual([]);
    fake.owner = true;
    fake.h.state("default/own", { capacity: 10, contended: false, fairShare: 10 });
    await flush();
    expect(fake.capacities).toEqual([
      ["default/own", 20],
      ["default/own#r", 10],
    ]);
    fake.h.state("default/own", { capacity: 20, contended: false, fairShare: 20 });
    await flush();
    expect(rec.of("capacity:changed")).toEqual([
      { target: "own", capacity: 20, previous: 10, source: "pool" },
    ]);
    expect(t.snapshot().routes.r!.capacity).toBe(10);
    expect(t.snapshot().capacitySource).toBe("callback");
    await engine.close();
  });

  it("ignores grants and state for pools it does not know, releasing stray grants", async () => {
    const clock = new ManualClock();
    const fake = new FakeCoordinator(2);
    const engine = createEngine({ clock, coordination: fake, logger: consoleLogger("silent"), caller: "c" });
    const t = engine.target("k", { capacity: 2, send: async () => 1 });
    await flush();
    t.onGrant("default/k#unknown", 2);
    expect(fake.releases).toEqual([["default/k#unknown", 2]]);
    t.onState("default/other", { capacity: 99, contended: true, fairShare: 0 });
    t.onFenced("default/other", 1);
    t.onRestored("default/other");
    expect(t.snapshot().capacity).toBe(2);
    expect(t.snapshot().fallback).toBe(false);
    await engine.close();
  });
});
