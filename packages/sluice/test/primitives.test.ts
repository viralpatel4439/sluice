import { describe, expect, it } from "vitest";
import { ManualClock } from "../src/core/clock.js";
import { Deque } from "../src/core/deque.js";
import { LaneQueue, type Queued } from "../src/core/queue.js";
import {
  DEFAULT_RETRY,
  backoffMs,
  defaultClassify,
  isRetrySafe,
  parseRetryAfter,
  retryBudget,
} from "../src/core/retry.js";
import { TimerHeap } from "../src/core/timers.js";
import type { Priority } from "../src/core/types.js";
import { response } from "./helpers.js";

const q = (priority: Priority, route?: string, size = 1): Queued & { n: number } => ({
  priority,
  lane: route,
  size,
  dead: false,
  n: Math.random(),
});

describe("Deque", () => {
  it("is FIFO across growth and supports both ends", () => {
    const d = new Deque<number>(2);
    for (let i = 0; i < 100; i++) d.push(i);
    d.unshift(-1);
    expect(d.shift()).toBe(-1);
    for (let i = 0; i < 99; i++) expect(d.shift()).toBe(i);
    expect(d.pop()).toBe(99);
    expect(d.length).toBe(0);
  });
  it("removes a matching element", () => {
    const d = new Deque<number>();
    for (const n of [1, 2, 3, 4]) d.push(n);
    expect(d.remove((n) => n === 3)).toBe(3);
    expect([...d]).toEqual([1, 2, 4]);
  });
});

describe("LaneQueue", () => {
  const opts = { maxItems: 10, maxBytes: 100, overflow: "block" as const };
  it("serves higher priority first", () => {
    const lq = new LaneQueue(opts);
    lq.push(q("bulk"));
    lq.push(q("critical"));
    lq.push(q("standard"));
    expect(lq.shift()!.priority).toBe("critical");
    expect(lq.shift()!.priority).toBe("standard");
    expect(lq.shift()!.priority).toBe("bulk");
  });
  it("guarantees bulk its minimum share under constant critical load", () => {
    const lq = new LaneQueue({ ...opts, maxItems: 10_000 });
    for (let i = 0; i < 1000; i++) lq.push(q("critical"));
    for (let i = 0; i < 1000; i++) lq.push(q("bulk"));
    const served: Priority[] = [];
    for (let i = 0; i < 200; i++) served.push(lq.shift()!.priority);
    const bulk = served.filter((p) => p === "bulk").length;
    expect(bulk).toBeGreaterThanOrEqual(15);
    expect(bulk).toBeLessThanOrEqual(60);
  });
  it("enforces limits and lazily skips dead entries", () => {
    const lq = new LaneQueue({ ...opts, maxItems: 2 });
    const a = q("standard");
    lq.push(a);
    lq.push(q("standard"));
    expect(lq.hasRoom(1)).toBe(false);
    lq.kill(a);
    expect(lq.items).toBe(1);
    expect(lq.hasRoom(1)).toBe(true);
    expect(lq.shift()).not.toBe(a);
  });
  it("skips closed routes without blocking others", () => {
    const lq = new LaneQueue(opts);
    lq.push(q("standard", "slow"));
    lq.push(q("standard", "fast"));
    const e = lq.shift((r) => r !== "slow");
    expect(e!.lane).toBe("fast");
    expect(lq.countRoute("slow")).toBe(1);
  });
  it("evicts the newest lowest-priority entry", () => {
    const lq = new LaneQueue(opts);
    lq.push(q("bulk"));
    const newest = q("bulk");
    lq.push(newest);
    lq.push(q("critical"));
    expect(lq.evictLowest("critical")).toBe(newest);
    expect(lq.evictLowest("bulk")).toBeUndefined();
  });
});

describe("TimerHeap", () => {
  it("fires in order with a single underlying timer", async () => {
    const clock = new ManualClock();
    const heap = new TimerHeap(clock);
    const fired: number[] = [];
    heap.schedule(30, () => fired.push(30));
    heap.schedule(10, () => fired.push(10));
    const h = heap.schedule(20, () => fired.push(20));
    h.cancel();
    await clock.advance(50);
    expect(fired).toEqual([10, 30]);
  });
});

describe("retry", () => {
  it("parses Retry-After seconds and dates", () => {
    expect(parseRetryAfter("2")).toBe(2000);
    expect(parseRetryAfter(new Date(10_000).toUTCString(), 4000)).toBe(6000);
    expect(parseRetryAfter("soon")).toBeUndefined();
  });
  it("classifies responses and errors", () => {
    const item = {} as never;
    expect(defaultClassify({ result: response(200), item }).kind).toBe("ok");
    expect(defaultClassify({ result: response(404), item }).kind).toBe("ok");
    expect(defaultClassify({ result: response(429, { "retry-after": "3" }), item })).toEqual({
      kind: "retry",
      afterMs: 3000,
    });
    expect(defaultClassify({ error: new Error("x"), item }).kind).toBe("retry");
  });
  it("only retries safe requests", () => {
    expect(isRetrySafe("GET", {})).toBe(true);
    expect(isRetrySafe("POST", {})).toBe(false);
    expect(isRetrySafe("POST", { "Idempotency-Key": "k" })).toBe(true);
  });
  it("uses capped full-jitter backoff and a minimum retry budget of one", () => {
    expect(backoffMs(1, DEFAULT_RETRY, () => 0.999)).toBeLessThan(200);
    expect(backoffMs(20, DEFAULT_RETRY, () => 0.999)).toBeLessThan(30_000);
    expect(retryBudget(3, 0.1)).toBe(1);
    expect(retryBudget(100, 0.1)).toBe(10);
  });
});
