/** Fixed log-scale histogram in milliseconds; cheap enough for the hot path. */
export class Histogram {
  static readonly BOUNDS = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000];
  readonly counts = new Array<number>(Histogram.BOUNDS.length + 1).fill(0);
  sum = 0;
  count = 0;

  observe(ms: number): void {
    let i = 0;
    while (i < Histogram.BOUNDS.length && ms > Histogram.BOUNDS[i]!) i++;
    this.counts[i]!++;
    this.sum += ms;
    this.count++;
  }

  quantile(q: number): number {
    if (this.count === 0) return 0;
    const rank = q * this.count;
    let acc = 0;
    for (let i = 0; i < this.counts.length; i++) {
      acc += this.counts[i]!;
      if (acc >= rank) return Histogram.BOUNDS[i] ?? Histogram.BOUNDS[Histogram.BOUNDS.length - 1]! * 2;
    }
    return 0;
  }

  reset(): void {
    this.counts.fill(0);
    this.sum = 0;
    this.count = 0;
  }
}

export interface TargetCounters {
  submitted: number;
  sent: number;
  done: number;
  failed: number;
  retried: number;
  expired: number;
  dead: number;
  dropped: number;
  rejected: number;
}

export function emptyCounters(): TargetCounters {
  return {
    submitted: 0,
    sent: 0,
    done: 0,
    failed: 0,
    retried: 0,
    expired: 0,
    dead: 0,
    dropped: 0,
    rejected: 0,
  };
}

export interface TargetSnapshot {
  target: string;
  pool: string;
  capacity: number;
  capacitySource: string;
  held: number;
  inFlight: number;
  queued: number;
  queuedBytes: number;
  retryQueued: number;
  contended: boolean;
  fairShare: number;
  fallback: boolean;
  circuit: "closed" | "open" | "half-open";
  counters: TargetCounters;
  waitMs: { p50: number; p99: number };
  sendMs: { p50: number; p99: number };
  routes: Record<string, { capacity: number; held: number; inFlight: number }>;
}
