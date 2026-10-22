import { Deque } from "./deque.js";
import { PRIORITIES, type Priority, type QueueOptions } from "./types.js";

export const DEFAULT_MIN_SHARES: Record<Priority, number> = { critical: 0.6, standard: 0.3, bulk: 0.1 };

/** Anything stored in the queue. `dead` entries are skipped lazily (expired or cancelled). */
export interface Queued {
  readonly priority: Priority;
  /** The capped route whose lane this entry waits in (undefined: the target's main lane). */
  readonly lane: string | undefined;
  readonly size: number;
  dead: boolean;
}

const NO_ROUTE = "\u0000";

/**
 * Bounded multi-lane queue: one FIFO per (route, priority).
 * Picks the next entry by strict priority, except that a lane below its
 * minimum share of recent dispatches goes first, so bulk work never starves.
 */
export class LaneQueue<E extends Queued> {
  private lanes = new Map<string, Deque<E>[]>();
  private routeOrder: string[] = [];
  private rr = 0;
  private window: Priority[] = [];
  private windowCounts: Record<Priority, number> = { critical: 0, standard: 0, bulk: 0 };
  private liveCount: Record<Priority, number> = { critical: 0, standard: 0, bulk: 0 };
  private liveByRoute = new Map<string, number>();
  private bytes = 0;

  constructor(
    private readonly opts: QueueOptions,
    private readonly minShares: Record<Priority, number> = DEFAULT_MIN_SHARES,
    private readonly windowSize = 100,
  ) {}

  get items(): number {
    return this.liveCount.critical + this.liveCount.standard + this.liveCount.bulk;
  }

  get overflow(): QueueOptions["overflow"] {
    return this.opts.overflow;
  }

  /** Live entries queued on one route. */
  countRoute(route: string | undefined): number {
    return this.liveByRoute.get(route ?? NO_ROUTE) ?? 0;
  }

  get byteSize(): number {
    return this.bytes;
  }

  countByPriority(): Record<Priority, number> {
    return { ...this.liveCount };
  }

  /** True if one more entry of this size fits under both limits. */
  hasRoom(size: number): boolean {
    if (this.items === 0) return true; // always admit one item, even if it alone exceeds maxBytes
    return this.items + 1 <= this.opts.maxItems && this.bytes + size <= this.opts.maxBytes;
  }

  push(entry: E, front = false): void {
    const lanes = this.lanesFor(entry.lane);
    const lane = lanes[PRIORITIES.indexOf(entry.priority)]!;
    if (front) lane.unshift(entry);
    else lane.push(entry);
    this.liveCount[entry.priority]++;
    this.bump(entry.lane, 1);
    this.bytes += entry.size;
  }

  private bump(route: string | undefined, delta: number): void {
    const key = route ?? NO_ROUTE;
    this.liveByRoute.set(key, (this.liveByRoute.get(key) ?? 0) + delta);
  }

  /** Mark an entry dead (expired/cancelled). Storage is reclaimed lazily. */
  kill(entry: E): void {
    if (entry.dead) return;
    entry.dead = true;
    this.liveCount[entry.priority]--;
    this.bump(entry.lane, -1);
    this.bytes -= entry.size;
  }

  /**
   * Remove and return the next entry to dispatch, or undefined.
   * `routeOpen` lets the caller skip routes that are at their sub-limit.
   */
  shift(routeOpen: (route: string | undefined) => boolean = () => true): E | undefined {
    const order = this.priorityOrder();
    for (const priority of order) {
      const entry = this.shiftPriority(priority, routeOpen);
      if (entry) {
        this.record(priority);
        return entry;
      }
    }
    return undefined;
  }

  /** Evict the newest live entry with priority strictly lower than `than`. */
  evictLowest(than: Priority): E | undefined {
    const limit = PRIORITIES.indexOf(than);
    for (let p = PRIORITIES.length - 1; p > limit; p--) {
      for (const route of this.routeOrder) {
        const lane = this.lanes.get(route)![p]!;
        while (lane.length > 0) {
          const entry = lane.pop()!;
          if (entry.dead) continue;
          this.kill(entry);
          return entry;
        }
      }
    }
    return undefined;
  }

  /** Remove every live entry (used by drain). */
  drainAll(): E[] {
    const out: E[] = [];
    for (const lanes of this.lanes.values()) {
      for (const lane of lanes) {
        for (let e = lane.shift(); e; e = lane.shift()) {
          if (!e.dead) {
            this.kill(e);
            out.push(e);
          }
        }
      }
    }
    return out;
  }

  private priorityOrder(): Priority[] {
    const total = this.window.length;
    const starving: Priority[] = [];
    if (total > 0) {
      for (const p of PRIORITIES) {
        if (this.liveCount[p] > 0 && this.windowCounts[p] / total < this.minShares[p]) starving.push(p);
      }
    }
    // Among starving lanes, the one furthest below its share goes first.
    starving.sort(
      (a, b) =>
        this.windowCounts[a] / total - this.minShares[a] - (this.windowCounts[b] / total - this.minShares[b]),
    );
    const rest = PRIORITIES.filter((p) => !starving.includes(p));
    return [...starving, ...rest];
  }

  private shiftPriority(priority: Priority, routeOpen: (r: string | undefined) => boolean): E | undefined {
    if (this.liveCount[priority] === 0) return undefined;
    const p = PRIORITIES.indexOf(priority);
    const n = this.routeOrder.length;
    for (let i = 0; i < n; i++) {
      const route = this.routeOrder[(this.rr + i) % n]!;
      const lane = this.lanes.get(route)![p]!;
      while (lane.length > 0 && lane.peek()!.dead) lane.shift();
      if (lane.length === 0) continue;
      const realRoute = route === NO_ROUTE ? undefined : route;
      if (!routeOpen(realRoute)) continue;
      const entry = lane.shift()!;
      this.rr = (this.rr + i + 1) % n;
      this.liveCount[priority]--;
      this.bump(entry.lane, -1);
      this.bytes -= entry.size;
      return entry;
    }
    return undefined;
  }

  private record(priority: Priority): void {
    this.window.push(priority);
    this.windowCounts[priority]++;
    if (this.window.length > this.windowSize) {
      const old = this.window.shift()!;
      this.windowCounts[old]--;
    }
  }

  private lanesFor(route: string | undefined): Deque<E>[] {
    const key = route ?? NO_ROUTE;
    let lanes = this.lanes.get(key);
    if (!lanes) {
      lanes = PRIORITIES.map(() => new Deque<E>());
      this.lanes.set(key, lanes);
      this.routeOrder.push(key);
    }
    return lanes;
  }
}
