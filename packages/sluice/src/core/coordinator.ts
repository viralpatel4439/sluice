import type { Priority } from "./types.js";

export interface PoolState {
  /** Total capacity N of the pool across the whole fleet. */
  capacity: number;
  /** True while another instance waits for tokens and is below its fair share. */
  contended: boolean;
  /** Tokens this instance may keep while contended (weighted fair share). */
  fairShare: number;
}

export interface CoordinatorHandlers {
  /** n tokens were granted to this instance for `pool`. */
  grant(pool: string, n: number): void;
  /** Fresh pool-wide state (capacity, contention, fair share). */
  state(pool: string, state: PoolState): void;
  /**
   * The coordinator can no longer vouch for held tokens (lost contact past the
   * safety margin). The instance must stop using them and fall back.
   */
  fenced(pool: string, fallbackCapacity: number): void;
  /** Contact restored after a fence; held tokens are reset to zero by the coordinator. */
  restored(pool: string): void;
}

export interface JoinOptions {
  caller: string;
  /** Capacity to seed the pool with if it does not exist yet. */
  capacity: number;
  /** Fair-share weight of this caller (default 1). */
  weight?: number;
  /** Tokens this instance is still using from before (reconnect). Default 0. */
  held?: number;
}

/**
 * The seam between the local engine and whatever keeps the shared token pool:
 * in-process (local), Redis, or a remote sluice-server.
 *
 * Tokens are permissions for one request in flight at the target. Grants are
 * always delivered through `handlers.grant`, never as return values, so all
 * implementations behave the same.
 */
export interface Coordinator {
  readonly kind: "local" | "redis" | "remote";
  attach(handlers: CoordinatorHandlers): void;
  join(pool: string, opts: JoinOptions): Promise<PoolState>;
  /** Ask for up to `want` more tokens than currently held. 0 withdraws from the waiters list. */
  demand(pool: string, want: number, hint: { priority: Priority; backlog: number }): void;
  /** Give `n` held tokens back to the pool. */
  release(pool: string, n: number): void;
  /** Report local load; implementations use it for leases, fair share and drift correction. */
  report(pool: string, load: { held: number; inFlight: number; backlog: number }): void;
  /** Set pool capacity. Only effective when this instance owns capacity for the pool. */
  setCapacity(pool: string, n: number): void;
  /** Whether this instance should read target capacity and run AIMD for the pool. */
  ownsCapacity(pool: string): boolean;
  leave(pool: string): Promise<void>;
  close(): Promise<void>;
}

interface Holder {
  id: number;
  handlers: CoordinatorHandlers | undefined;
  held: number;
  weight: number;
  backlog: number;
  want: number;
  priority: Priority;
  waitingSince: number;
}

interface LocalPool {
  capacity: number;
  free: number;
  debt: number;
  holders: Map<number, Holder>;
}

const PRIORITY_RANK: Record<Priority, number> = { critical: 0, standard: 1, bulk: 2 };

/**
 * In-process token pool. One hub can serve several engines in the same process,
 * which is how single-instance mode works and how multi-instance behaviour is
 * simulated in tests.
 */
export class LocalHub {
  private pools = new Map<string, LocalPool>();
  private seq = 0;
  private waitSeq = 0;

  client(): Coordinator {
    const id = ++this.seq;
    const hub = this;
    let handlers: CoordinatorHandlers | undefined;
    const joined = new Set<string>();
    return {
      kind: "local",
      attach(h) {
        handlers = h;
      },
      async join(name, opts) {
        const pool = hub.pool(name, opts.capacity);
        pool.holders.set(id, {
          id,
          handlers,
          held: 0,
          weight: opts.weight ?? 1,
          backlog: 0,
          want: 0,
          priority: "standard",
          waitingSince: 0,
        });
        joined.add(name);
        return hub.stateFor(pool, id);
      },
      demand(name, want, hint) {
        const pool = hub.pools.get(name);
        const holder = pool?.holders.get(id);
        if (!pool || !holder) return;
        if (want > 0 && holder.want === 0) holder.waitingSince = ++hub.waitSeq;
        holder.want = Math.max(0, want);
        holder.priority = hint.priority;
        holder.backlog = hint.backlog;
        hub.distribute(name, pool);
      },
      release(name, n) {
        const pool = hub.pools.get(name);
        const holder = pool?.holders.get(id);
        if (!pool || !holder || n <= 0) return;
        const give = Math.min(n, holder.held);
        holder.held -= give;
        hub.returnTokens(pool, give);
        hub.distribute(name, pool);
      },
      report(name, load) {
        const holder = hub.pools.get(name)?.holders.get(id);
        if (holder) holder.backlog = load.backlog;
      },
      setCapacity(name, n) {
        const pool = hub.pools.get(name);
        if (pool) hub.resize(name, pool, n);
      },
      ownsCapacity(name) {
        const pool = hub.pools.get(name);
        if (!pool) return false;
        return Math.min(...pool.holders.keys()) === id;
      },
      async leave(name) {
        const pool = hub.pools.get(name);
        const holder = pool?.holders.get(id);
        if (!pool || !holder) return;
        pool.holders.delete(id);
        joined.delete(name);
        hub.returnTokens(pool, holder.held);
        hub.distribute(name, pool);
      },
      async close() {
        for (const name of [...joined]) await this.leave(name);
      },
    };
  }

  /** Snapshot for tests and the in-process admin API. */
  inspect(
    name: string,
  ): { capacity: number; free: number; debt: number; held: Record<number, number> } | undefined {
    const pool = this.pools.get(name);
    if (!pool) return undefined;
    const held: Record<number, number> = {};
    for (const h of pool.holders.values()) held[h.id] = h.held;
    return { capacity: pool.capacity, free: pool.free, debt: pool.debt, held };
  }

  private pool(name: string, capacity: number): LocalPool {
    let pool = this.pools.get(name);
    if (!pool) {
      pool = { capacity, free: capacity, debt: 0, holders: new Map() };
      this.pools.set(name, pool);
    }
    return pool;
  }

  private returnTokens(pool: LocalPool, n: number): void {
    const pay = Math.min(n, pool.debt);
    pool.debt -= pay;
    pool.free += n - pay;
  }

  private resize(name: string, pool: LocalPool, n: number): void {
    const target = Math.max(0, Math.floor(n));
    const delta = target - pool.capacity;
    pool.capacity = target;
    if (delta > 0) {
      const cancel = Math.min(delta, pool.debt);
      pool.debt -= cancel;
      pool.free += delta - cancel;
    } else if (delta < 0) {
      const fromFree = Math.min(-delta, pool.free);
      pool.free -= fromFree;
      pool.debt += -delta - fromFree;
    }
    this.distribute(name, pool);
  }

  private fairShares(pool: LocalPool): Map<number, number> {
    const weights = new Map<number, number>();
    for (const h of pool.holders.values())
      weights.set(h.id, h.weight * Math.max(h.backlog, h.held > 0 ? 1 : 0));
    return apportion(pool.capacity, weights);
  }

  private stateFor(pool: LocalPool, id: number): PoolState {
    const shares = this.fairShares(pool);
    // Contended means another instance is waiting *below its fair share* (starved). Waiters that
    // already have their share do not trigger give-backs; otherwise spare tokens would ping-pong.
    const contended = [...pool.holders.values()].some(
      (h) => h.id !== id && h.want > 0 && h.held < (shares.get(h.id) ?? 0),
    );
    return { capacity: pool.capacity, contended, fairShare: shares.get(id) ?? 0 };
  }

  private distribute(name: string, pool: LocalPool): void {
    const shares = this.fairShares(pool);
    const waiters = [...pool.holders.values()]
      .filter((h) => h.want > 0)
      .sort(
        (a, b) => PRIORITY_RANK[a.priority] - PRIORITY_RANK[b.priority] || a.waitingSince - b.waitingSince,
      );
    const grants = new Map<Holder, number>();
    // Pass 1 serves waiters below their fair share, pass 2 anyone still waiting.
    // Round-robin one token at a time so one large waiter cannot take everything.
    for (const belowShareOnly of [true, false]) {
      let progress = true;
      while (pool.free > 0 && progress) {
        progress = false;
        for (const h of waiters) {
          if (pool.free === 0) break;
          if (h.want === 0) continue;
          if (belowShareOnly && h.held >= (shares.get(h.id) ?? 0)) continue;
          pool.free--;
          h.held++;
          h.want--;
          progress = true;
          grants.set(h, (grants.get(h) ?? 0) + 1);
        }
      }
    }
    // Deliver asynchronously, like a networked coordinator, so callers never re-enter.
    const states = [...pool.holders.values()].map((h) => [h, this.stateFor(pool, h.id)] as const);
    // State first, so a grant is never judged against a stale fair share.
    queueMicrotask(() => {
      for (const [h, st] of states) h.handlers?.state(name, st);
      for (const [h, n] of grants) h.handlers?.grant(name, n);
    });
  }
}

/**
 * Split `total` tokens in proportion to `weights` with largest-remainder rounding,
 * so the shares always add up to `total`. Everyone with weight gets at least 1 when
 * there are enough tokens to go round.
 */
export function apportion<K>(total: number, weights: Map<K, number>): Map<K, number> {
  const out = new Map<K, number>();
  let sum = 0;
  for (const w of weights.values()) sum += w;
  if (sum === 0 || total <= 0) {
    for (const k of weights.keys()) out.set(k, 0);
    return out;
  }
  const rest: [K, number][] = [];
  let given = 0;
  for (const [k, w] of weights) {
    const exact = (total * w) / sum;
    const base = Math.floor(exact);
    out.set(k, base);
    given += base;
    rest.push([k, exact - base]);
  }
  // Ties broken by key so every node computes the same shares.
  rest.sort(
    (a, b) => b[1] - a[1] || (String(a[0]) < String(b[0]) ? -1 : String(a[0]) > String(b[0]) ? 1 : 0),
  );
  for (let i = 0; given < total && i < rest.length; i++, given++)
    out.set(rest[i]![0], out.get(rest[i]![0])! + 1);
  const active = [...weights].filter(([, w]) => w > 0).map(([k]) => k);
  if (total >= active.length) {
    for (const k of active) {
      if (out.get(k)! > 0) continue;
      let maxK = k;
      for (const [kk, v] of out) if (v > out.get(maxK)!) maxK = kk;
      out.set(maxK, out.get(maxK)! - 1);
      out.set(k, 1);
    }
  }
  return out;
}
