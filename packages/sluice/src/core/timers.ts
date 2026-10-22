import type { Clock, TimerHandle } from "./clock.js";

interface Entry {
  at: number;
  seq: number;
  fn: () => void;
  cancelled: boolean;
}

/**
 * One min-heap per engine for deadlines and retry delays, backed by a single
 * underlying clock timer. Scales to hundreds of thousands of pending timers.
 */
export class TimerHeap {
  private heap: Entry[] = [];
  private seq = 0;
  private armed: { at: number; handle: TimerHandle } | undefined;

  constructor(private readonly clock: Clock) {}

  get size(): number {
    return this.heap.length;
  }

  schedule(at: number, fn: () => void): TimerHandle {
    const entry: Entry = { at, seq: ++this.seq, fn, cancelled: false };
    this.push(entry);
    this.arm();
    return {
      cancel: () => {
        entry.cancelled = true;
      },
    };
  }

  clear(): void {
    this.heap = [];
    this.armed?.handle.cancel();
    this.armed = undefined;
  }

  private arm(): void {
    while (this.heap.length > 0 && this.heap[0]!.cancelled) this.pop();
    const top = this.heap[0];
    if (!top) {
      this.armed?.handle.cancel();
      this.armed = undefined;
      return;
    }
    if (this.armed && this.armed.at <= top.at) return;
    this.armed?.handle.cancel();
    const at = top.at;
    this.armed = { at, handle: this.clock.setTimer(at - this.clock.now(), () => this.fire()) };
  }

  private fire(): void {
    this.armed = undefined;
    const now = this.clock.now();
    while (this.heap.length > 0 && this.heap[0]!.at <= now) {
      const entry = this.pop()!;
      if (!entry.cancelled) entry.fn();
    }
    this.arm();
  }

  private less(a: Entry, b: Entry): boolean {
    return a.at < b.at || (a.at === b.at && a.seq < b.seq);
  }

  private push(entry: Entry): void {
    const h = this.heap;
    h.push(entry);
    let i = h.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (!this.less(h[i]!, h[p]!)) break;
      [h[i], h[p]] = [h[p]!, h[i]!];
      i = p;
    }
  }

  private pop(): Entry | undefined {
    const h = this.heap;
    const top = h[0];
    const last = h.pop();
    if (h.length > 0 && last) {
      h[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let m = i;
        if (l < h.length && this.less(h[l]!, h[m]!)) m = l;
        if (r < h.length && this.less(h[r]!, h[m]!)) m = r;
        if (m === i) break;
        [h[i], h[m]] = [h[m]!, h[i]!];
        i = m;
      }
    }
    return top;
  }
}
