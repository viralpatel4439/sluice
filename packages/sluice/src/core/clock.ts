export interface TimerHandle {
  cancel(): void;
}

/** All time access in the engine goes through a Clock, so tests can run on simulated time. */
export interface Clock {
  /** Monotonic milliseconds. Never wall-clock time. */
  now(): number;
  setTimer(delayMs: number, fn: () => void): TimerHandle;
}

export const systemClock: Clock = {
  now: () => performance.now(),
  setTimer(delayMs, fn) {
    const t = setTimeout(fn, Math.max(0, delayMs));
    t.unref?.();
    return { cancel: () => clearTimeout(t) };
  },
};

/** Deterministic clock for tests and simulations. */
export class ManualClock implements Clock {
  private t = 0;
  private seq = 0;
  private timers = new Map<number, { at: number; fn: () => void }>();

  now(): number {
    return this.t;
  }

  setTimer(delayMs: number, fn: () => void): TimerHandle {
    const id = ++this.seq;
    this.timers.set(id, { at: this.t + Math.max(0, delayMs), fn });
    return { cancel: () => this.timers.delete(id) };
  }

  /** Advance time, firing due timers in order. */
  async advance(ms: number): Promise<void> {
    const end = this.t + ms;
    for (;;) {
      let nextId = -1;
      let nextAt = Number.POSITIVE_INFINITY;
      for (const [id, timer] of this.timers) {
        if (timer.at < nextAt || (timer.at === nextAt && id < nextId)) {
          nextAt = timer.at;
          nextId = id;
        }
      }
      if (nextId === -1 || nextAt > end) break;
      const timer = this.timers.get(nextId)!;
      this.timers.delete(nextId);
      this.t = nextAt;
      timer.fn();
      await flushMicrotasks();
    }
    this.t = end;
    await flushMicrotasks();
  }
}

export async function flushMicrotasks(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}
