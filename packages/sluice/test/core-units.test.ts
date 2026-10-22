import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { Circuit } from "../src/core/circuit.js";
import { ManualClock } from "../src/core/clock.js";
import { LocalHub, apportion } from "../src/core/coordinator.js";
import { Deque } from "../src/core/deque.js";
import { LaneQueue, type Queued } from "../src/core/queue.js";
import {
  DEFAULT_RETRY,
  backoffMs,
  defaultClassify,
  isOverloadSignal,
  isRetrySafe,
  parseRetryAfter,
} from "../src/core/retry.js";
import { TimerHeap } from "../src/core/timers.js";
import type { Priority } from "../src/core/types.js";
import { response } from "./helpers.js";

// ---------------------------------------------------------------- circuit

describe("Circuit", () => {
  const make = (threshold = 3, openMs = 100, probes = 2) => {
    const clock = new ManualClock();
    const changes: string[] = [];
    const c = new Circuit({ threshold, openMs, probes }, clock, (s) => changes.push(s));
    return { clock, c, changes };
  };

  it("opens exactly at the threshold and a success resets the count", () => {
    const { c, changes } = make(3);
    expect(c.state).toBe("closed");
    expect(c.canSend()).toBe(true);
    c.onResult(true, false);
    c.onResult(true, false);
    c.onResult(false, false); // success resets
    c.onResult(true, false);
    c.onResult(true, false);
    expect(c.state).toBe("closed");
    c.onResult(true, false);
    expect(c.state).toBe("open");
    expect(changes).toEqual(["open"]);
  });

  it("stays open for exactly openMs, reporting the remaining time", async () => {
    const { c, clock } = make(1, 100);
    c.onResult(true, false);
    expect(c.retryInMs()).toBe(100);
    await clock.advance(40);
    expect(c.retryInMs()).toBe(60);
    expect(c.canSend()).toBe(false);
    await clock.advance(59);
    expect(c.canSend()).toBe(false);
    await clock.advance(1);
    expect(c.canSend()).toBe(true);
    expect(c.state).toBe("half-open");
    expect(c.retryInMs()).toBe(0);
  });

  it("allows exactly `probes` concurrent probes while half-open", async () => {
    const { c, clock } = make(1, 10, 2);
    c.onResult(true, false);
    await clock.advance(10);
    expect(c.canSend()).toBe(true);
    c.onSend();
    expect(c.canSend()).toBe(true);
    c.onSend();
    expect(c.canSend()).toBe(false);
    c.onResult(true, true); // a probe fails: open again
    expect(c.state).toBe("open");
    await clock.advance(10);
    expect(c.canSend()).toBe(true);
    c.onSend();
    c.onResult(false, true); // a probe succeeds: closed
    expect(c.state).toBe("closed");
    expect(c.canSend()).toBe(true);
  });

  it("frees a probe slot when a probe finishes, and closed sends do not count as probes", async () => {
    const { c, clock, changes } = make(1, 10, 1);
    c.onSend(); // closed: not a probe
    c.onResult(true, false);
    await clock.advance(10);
    expect(c.canSend()).toBe(true);
    c.onSend();
    expect(c.canSend()).toBe(false);
    c.onResult(false, true);
    expect(changes).toEqual(["open", "half-open", "closed"]);
    // After closing, probe accounting starts from zero again.
    c.onResult(true, false);
    await clock.advance(10);
    expect(c.canSend()).toBe(true);
  });
});

// ---------------------------------------------------------------- retry

describe("retry helpers", () => {
  it("parses Retry-After strictly", () => {
    expect(parseRetryAfter("  3  ")).toBe(3000);
    expect(parseRetryAfter("2.5")).toBe(2500);
    expect(parseRetryAfter("0")).toBe(0);
    expect(parseRetryAfter("a2")).toBeUndefined();
    expect(parseRetryAfter("2a")).toBeUndefined();
    expect(parseRetryAfter("1.")).toBeUndefined();
    expect(parseRetryAfter("12.34")).toBe(12340);
    expect(parseRetryAfter("10.x")).toBeUndefined();
    expect(parseRetryAfter("")).toBeUndefined();
    expect(parseRetryAfter(null)).toBeUndefined();
    expect(parseRetryAfter(new Date(1000).toUTCString(), 5000)).toBe(0);
  });

  it("only treats real Response-like values as responses", () => {
    const item = {} as never;
    expect(defaultClassify({ result: { status: 503 }, item })).toEqual({ kind: "ok" }); // no headers.get
    expect(defaultClassify({ result: { status: "503", headers: { get: () => null } }, item })).toEqual({
      kind: "ok",
    });
    expect(defaultClassify({ result: null, item })).toEqual({ kind: "ok" });
    expect(defaultClassify({ result: 503, item })).toEqual({ kind: "ok" });
    expect(defaultClassify({ result: { status: 503, headers: {} }, item })).toEqual({ kind: "ok" });
    expect(defaultClassify({ result: response(503), item })).toEqual({ kind: "retry" });
    expect(defaultClassify({ result: response(502), item })).toEqual({ kind: "retry" });
    expect(defaultClassify({ result: response(504), item })).toEqual({ kind: "retry" });
    expect(defaultClassify({ result: response(500), item })).toEqual({ kind: "ok" });
    expect(defaultClassify({ error: undefined, result: response(200), item })).toEqual({ kind: "ok" });
    expect(defaultClassify({ error: new Error("x"), item })).toEqual({ kind: "retry" });
  });

  it("computes full-jitter backoff exactly", () => {
    const half = () => 0.5;
    expect(backoffMs(0, DEFAULT_RETRY, half)).toBe(100);
    expect(backoffMs(1, DEFAULT_RETRY, half)).toBe(100);
    expect(backoffMs(2, DEFAULT_RETRY, half)).toBe(200);
    expect(backoffMs(3, DEFAULT_RETRY, half)).toBe(400);
    expect(backoffMs(30, DEFAULT_RETRY, half)).toBe(15_000);
    expect(backoffMs(3, DEFAULT_RETRY, () => 0)).toBe(0);
  });

  it("recognises overload responses and safe retries", () => {
    expect(isOverloadSignal(response(429))).toBe(true);
    expect(isOverloadSignal(response(503))).toBe(true);
    expect(isOverloadSignal(response(502))).toBe(false);
    expect(isOverloadSignal(response(200))).toBe(false);
    expect(isOverloadSignal({ status: 429 })).toBe(false);
    expect(isRetrySafe("get", undefined)).toBe(true);
    expect(isRetrySafe(undefined, undefined)).toBe(true);
    expect(isRetrySafe("POST", undefined)).toBe(false);
    expect(isRetrySafe("POST", { "content-type": "x" })).toBe(false);
    expect(isRetrySafe("PATCH", { "IDEMPOTENCY-KEY": "k" })).toBe(true);
  });
});

// ---------------------------------------------------------------- timers

describe("TimerHeap", () => {
  it("fires any set of timers in time order, ties in scheduling order", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.tuple(fc.integer({ min: 0, max: 50 }), fc.boolean()), { minLength: 1, maxLength: 60 }),
        async (spec) => {
          const clock = new ManualClock();
          const heap = new TimerHeap(clock);
          const fired: number[] = [];
          const handles = spec.map(([at], i) => heap.schedule(at, () => fired.push(i)));
          expect(heap.size).toBe(spec.length);
          spec.forEach(([, cancel], i) => cancel && handles[i]!.cancel());
          await clock.advance(100);
          const expected = spec
            .map(([at, cancel], i) => ({ at, cancel, i }))
            .filter((x) => !x.cancel)
            .sort((a, b) => a.at - b.at || a.i - b.i)
            .map((x) => x.i);
          expect(fired).toEqual(expected);
          expect(heap.size).toBe(0);
        },
      ),
      { numRuns: 200 },
    );
  });

  it("re-arms for an earlier timer and clear() drops everything", async () => {
    const clock = new ManualClock();
    const heap = new TimerHeap(clock);
    const fired: string[] = [];
    heap.schedule(50, () => fired.push("late"));
    await clock.advance(10);
    heap.schedule(20, () => fired.push("early"));
    await clock.advance(15);
    expect(fired).toEqual(["early"]);
    heap.clear();
    expect(heap.size).toBe(0);
    await clock.advance(100);
    expect(fired).toEqual(["early"]);
  });

  it("schedules timers added from inside a firing callback", async () => {
    const clock = new ManualClock();
    const heap = new TimerHeap(clock);
    const fired: number[] = [];
    heap.schedule(10, () => {
      fired.push(10);
      heap.schedule(10, () => fired.push(11)); // already due: fires in the same sweep
      heap.schedule(30, () => fired.push(30));
    });
    await clock.advance(50);
    expect(fired).toEqual([10, 11, 30]);
  });
});

// ---------------------------------------------------------------- deque

describe("Deque", () => {
  it("behaves exactly like an array under any sequence of operations", () => {
    type Op = ["push", number] | ["unshift", number] | ["shift"] | ["pop"] | ["remove", number] | ["peek"];
    const op: fc.Arbitrary<Op> = fc.oneof(
      fc.tuple(fc.constant("push" as const), fc.integer({ min: 0, max: 9 })),
      fc.tuple(fc.constant("unshift" as const), fc.integer({ min: 0, max: 9 })),
      fc.tuple(fc.constant("shift" as const)),
      fc.tuple(fc.constant("pop" as const)),
      fc.tuple(fc.constant("remove" as const), fc.integer({ min: 0, max: 9 })),
      fc.tuple(fc.constant("peek" as const)),
    );
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 4 }), fc.array(op, { maxLength: 200 }), (cap, ops) => {
        const d = new Deque<number>(cap);
        const a: number[] = [];
        for (const o of ops) {
          if (o[0] === "push") {
            d.push(o[1]);
            a.push(o[1]);
          } else if (o[0] === "unshift") {
            d.unshift(o[1]);
            a.unshift(o[1]);
          } else if (o[0] === "shift") expect(d.shift()).toBe(a.shift());
          else if (o[0] === "pop") expect(d.pop()).toBe(a.pop());
          else if (o[0] === "peek") expect(d.peek()).toBe(a[0]);
          else {
            const idx = a.indexOf(o[1]);
            const got = d.remove((v) => v === o[1]);
            if (idx === -1) expect(got).toBeUndefined();
            else {
              expect(got).toBe(o[1]);
              a.splice(idx, 1);
            }
          }
          expect(d.length).toBe(a.length);
          expect([...d]).toEqual(a);
        }
      }),
      { numRuns: 300 },
    );
  });
});

// ---------------------------------------------------------------- lane queue

const q = (priority: Priority, lane?: string, size = 1): Queued & { id: number } => ({
  priority,
  lane,
  size,
  dead: false,
  id: Math.random(),
});

describe("LaneQueue", () => {
  it("accounts bytes and items through every operation", () => {
    const lq = new LaneQueue<ReturnType<typeof q>>({ maxItems: 100, maxBytes: 1000, overflow: "block" });
    const a = q("standard", undefined, 10);
    const b = q("bulk", undefined, 20);
    const c = q("critical", "r", 30);
    lq.push(a);
    lq.push(b);
    lq.push(c);
    expect([lq.items, lq.byteSize]).toEqual([3, 60]);
    lq.kill(a);
    lq.kill(a); // idempotent
    expect([lq.items, lq.byteSize]).toEqual([2, 50]);
    expect(lq.shift()).toBe(c);
    expect([lq.items, lq.byteSize, lq.countRoute("r")]).toEqual([1, 20, 0]);
    expect(lq.evictLowest("critical")).toBe(b);
    expect([lq.items, lq.byteSize]).toEqual([0, 0]);
    lq.push(q("bulk", undefined, 5));
    expect(lq.drainAll()).toHaveLength(1);
    expect([lq.items, lq.byteSize]).toEqual([0, 0]);
  });

  it("enforces the byte limit inclusively and always admits a first item", () => {
    const lq = new LaneQueue<ReturnType<typeof q>>({ maxItems: 10, maxBytes: 100, overflow: "block" });
    expect(lq.hasRoom(1000)).toBe(true); // empty queue: always one
    lq.push(q("standard", undefined, 60));
    expect(lq.hasRoom(40)).toBe(true);
    expect(lq.hasRoom(41)).toBe(false);
    const lim = new LaneQueue<ReturnType<typeof q>>({ maxItems: 2, maxBytes: 100, overflow: "block" });
    lim.push(q("standard"));
    expect(lim.hasRoom(1)).toBe(true);
    lim.push(q("standard"));
    expect(lim.hasRoom(0)).toBe(false);
  });

  it("returns a copy of the priority counts and supports pushing to the front", () => {
    const lq = new LaneQueue<ReturnType<typeof q>>({ maxItems: 10, maxBytes: 100, overflow: "block" });
    const first = q("standard");
    const second = q("standard");
    lq.push(first);
    lq.push(second, true);
    const counts = lq.countByPriority();
    counts.standard = 99;
    expect(lq.countByPriority()).toEqual({ critical: 0, standard: 2, bulk: 0 });
    expect(lq.shift()).toBe(second);
  });

  it("evicts only live entries and drains only live ones", () => {
    const lq = new LaneQueue<ReturnType<typeof q>>({ maxItems: 10, maxBytes: 100, overflow: "block" });
    const older = q("bulk");
    const newer = q("bulk");
    lq.push(older);
    lq.push(newer);
    lq.kill(newer);
    expect(lq.evictLowest("standard")).toBe(older);
    expect(lq.evictLowest("standard")).toBeUndefined();
    const dead = q("critical");
    lq.push(dead);
    lq.push(q("critical"));
    lq.kill(dead);
    expect(lq.drainAll()).toHaveLength(1);
  });

  it("round-robins between routes of the same priority and skips closed ones", () => {
    const lq = new LaneQueue<ReturnType<typeof q>>({ maxItems: 100, maxBytes: 1000, overflow: "block" });
    for (let i = 0; i < 3; i++) {
      lq.push(q("standard", "a"));
      lq.push(q("standard", "b"));
    }
    const order = Array.from({ length: 6 }, () => lq.shift()!.lane);
    expect(order).toEqual(["a", "b", "a", "b", "a", "b"]);
    lq.push(q("standard", "a"));
    lq.push(q("standard", "b"));
    expect(lq.shift((r) => r !== "a")!.lane).toBe("b");
    expect(lq.shift((r) => r !== "a")).toBeUndefined();
    expect(lq.shift()!.lane).toBe("a");
    lq.push(q("standard"));
    expect(lq.shift((r) => r === undefined)!.lane).toBeUndefined();
  });

  it("gives the most starved lane priority, measured over a sliding window", () => {
    const lq = new LaneQueue<ReturnType<typeof q>>(
      { maxItems: 10_000, maxBytes: 1e9, overflow: "block" },
      { critical: 0.5, standard: 0.3, bulk: 0.2 },
      10,
    );
    for (let i = 0; i < 100; i++) {
      lq.push(q("critical"));
      lq.push(q("standard"));
      lq.push(q("bulk"));
    }
    const served: Priority[] = Array.from({ length: 50 }, () => lq.shift()!.priority);
    // Over every full window of 10, each lane gets at least its minimum share (rounded down).
    for (let start = 10; start + 10 <= served.length; start++) {
      const w = served.slice(start, start + 10);
      expect(w.filter((p) => p === "standard").length).toBeGreaterThanOrEqual(2);
      expect(w.filter((p) => p === "bulk").length).toBeGreaterThanOrEqual(1);
      expect(w.filter((p) => p === "critical").length).toBeGreaterThanOrEqual(4);
    }
    // With only one lane queued, it gets everything.
    const only = new LaneQueue<ReturnType<typeof q>>({ maxItems: 100, maxBytes: 1e9, overflow: "block" });
    for (let i = 0; i < 5; i++) only.push(q("bulk"));
    expect(Array.from({ length: 5 }, () => only.shift()!.priority)).toEqual([
      "bulk",
      "bulk",
      "bulk",
      "bulk",
      "bulk",
    ]);
  });
});

// ---------------------------------------------------------------- apportion and LocalHub

describe("apportion", () => {
  const ap = (total: number, w: Record<string, number>) =>
    Object.fromEntries(apportion(total, new Map(Object.entries(w))));
  it("splits in proportion with largest remainders and stable ties", () => {
    expect(ap(8, { a: 3, b: 1 })).toEqual({ a: 6, b: 2 });
    expect(ap(7, { a: 1, b: 1, c: 1 })).toEqual({ a: 3, b: 2, c: 2 });
    expect(ap(2, { b: 1, a: 1, c: 1 })).toEqual({ a: 1, b: 1, c: 0 });
    expect(ap(10, { a: 0, b: 5 })).toEqual({ a: 0, b: 10 });
    expect(ap(0, { a: 1 })).toEqual({ a: 0 });
    expect(ap(5, { a: 0, b: 0 })).toEqual({ a: 0, b: 0 });
  });
  it("gives everyone with weight at least one token when there are enough", () => {
    expect(ap(3, { a: 100, b: 1, c: 1 })).toEqual({ a: 1, b: 1, c: 1 });
    expect(ap(10, { a: 1000, b: 1 })).toEqual({ a: 9, b: 1 });
  });
  it("always sums to the total", () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 50 }),
        fc.array(fc.integer({ min: 0, max: 20 }), { minLength: 1, maxLength: 8 }),
        (total, ws) => {
          const m = apportion(total, new Map(ws.map((w, i) => [`k${i}`, w])));
          const sum = [...m.values()].reduce((a, b) => a + b, 0);
          expect(sum).toBe(ws.some((w) => w > 0) && total > 0 ? total : 0);
          for (const v of m.values()) expect(v).toBeGreaterThanOrEqual(0);
        },
      ),
    );
  });
});

describe("LocalHub", () => {
  const handlers = () => {
    const h = {
      grants: [] as number[],
      states: [] as { capacity: number; contended: boolean; fairShare: number }[],
    };
    return {
      h,
      impl: {
        grant: (_p: string, n: number) => h.grants.push(n),
        state: (_p: string, s: { capacity: number; contended: boolean; fairShare: number }) =>
          h.states.push(s),
        fenced: () => {},
        restored: () => {},
      },
    };
  };
  const tick = () => new Promise((r) => setImmediate(r));

  it("serves waiters by priority, then by who waited first, one token at a time", async () => {
    const hub = new LocalHub();
    const [a, b, c] = [hub.client(), hub.client(), hub.client()];
    const ha = handlers();
    const hb = handlers();
    const hc = handlers();
    a.attach(ha.impl);
    b.attach(hb.impl);
    c.attach(hc.impl);
    for (const x of [a, b, c]) await x.join("p", { caller: "x", capacity: 0 });
    b.demand("p", 2, { priority: "bulk", backlog: 2 });
    c.demand("p", 2, { priority: "standard", backlog: 2 });
    a.demand("p", 2, { priority: "critical", backlog: 2 });
    a.setCapacity("p", 1);
    await tick();
    expect([ha.h.grants, hb.h.grants, hc.h.grants]).toEqual([[1], [], []]);
    a.setCapacity("p", 3); // +2: fair share first — a already has its share (1 of 3), so c and b get one each
    await tick();
    expect(ha.h.grants.reduce((s, n) => s + n, 0)).toBe(1);
    expect(hc.h.grants.reduce((s, n) => s + n, 0)).toBe(1);
    expect(hb.h.grants.reduce((s, n) => s + n, 0)).toBe(1);
    a.setCapacity("p", 4); // everyone at share: the extra goes by priority, critical first
    await tick();
    expect(ha.h.grants.reduce((s, n) => s + n, 0)).toBe(2);
  });

  it("grows by cancelling debt first and shrinks from free first", async () => {
    const hub = new LocalHub();
    const a = hub.client();
    const ha = handlers();
    a.attach(ha.impl);
    await a.join("p", { caller: "x", capacity: 4 });
    a.demand("p", 3, { priority: "standard", backlog: 3 });
    await tick();
    expect(hub.inspect("p")).toMatchObject({ capacity: 4, free: 1, debt: 0 });
    a.setCapacity("p", 1); // free 1 → 0, then 2 debt
    expect(hub.inspect("p")).toMatchObject({ capacity: 1, free: 0, debt: 2 });
    a.release("p", 1); // pays debt
    expect(hub.inspect("p")).toMatchObject({ free: 0, debt: 1 });
    a.setCapacity("p", 3); // +2: cancels 1 debt, 1 free
    expect(hub.inspect("p")).toMatchObject({ capacity: 3, free: 1, debt: 0 });
    a.release("p", 99); // clamps to what is held
    expect(hub.inspect("p")).toMatchObject({ free: 3, debt: 0, held: { 1: 0 } });
  });

  it("returns held tokens on leave, ignores unknown pools and reports contention only for starved others", async () => {
    const hub = new LocalHub();
    const a = hub.client();
    const b = hub.client();
    const ha = handlers();
    const hb = handlers();
    a.attach(ha.impl);
    b.attach(hb.impl);
    await a.join("p", { caller: "x", capacity: 2 });
    await b.join("p", { caller: "y", capacity: 2 });
    expect(a.ownsCapacity("p")).toBe(true);
    expect(b.ownsCapacity("p")).toBe(false);
    expect(a.ownsCapacity("nope")).toBe(false);
    a.demand("nope", 1, { priority: "standard", backlog: 1 });
    a.release("nope", 1);
    a.report("nope", { held: 0, inFlight: 0, backlog: 0 });
    a.demand("p", 2, { priority: "standard", backlog: 2 });
    await tick();
    b.demand("p", 1, { priority: "standard", backlog: 5 });
    await tick();
    expect(ha.h.states.at(-1)!.contended).toBe(true); // b waits with no tokens: starved
    expect(hb.h.states.at(-1)!.contended).toBe(false); // a is not waiting
    await a.leave("p");
    await tick();
    expect(hb.h.grants.reduce((s, n) => s + n, 0)).toBe(1);
    expect(b.ownsCapacity("p")).toBe(true);
    await b.close();
    expect(hub.inspect("p")).toMatchObject({ free: 2, held: {} });
    expect(hub.inspect("missing")).toBeUndefined();
  });

  it("weights fair shares by reported backlog", async () => {
    const hub = new LocalHub();
    const a = hub.client();
    const b = hub.client();
    const ha = handlers();
    const hb = handlers();
    a.attach(ha.impl);
    b.attach(hb.impl);
    await a.join("p", { caller: "x", capacity: 10, weight: 1 });
    await b.join("p", { caller: "y", capacity: 10, weight: 1 });
    a.report("p", { held: 0, inFlight: 0, backlog: 30 });
    b.report("p", { held: 0, inFlight: 0, backlog: 10 });
    a.demand("p", 1, { priority: "standard", backlog: 30 });
    await tick();
    expect(ha.h.states.at(-1)!.fairShare).toBe(8);
    expect(hb.h.states.at(-1)!.fairShare).toBe(2);
  });
});
