import type { Coordinator, CoordinatorHandlers, JoinOptions, PoolState } from "../core/coordinator.js";
import { apportion } from "../core/coordinator.js";
import { randomId } from "../core/ids.js";
import { type Logger, consoleLogger } from "../core/logger.js";
import type { Priority } from "../core/types.js";
import { type RedisLike, type Script, connect, script, toRedisLike } from "./adapters.js";
import { CLAIM, GC, INSPECT, JOIN, LEAVE, PROTOCOL_VERSION, RENEW, SETCAP, SYNC } from "./scripts.js";
import { SubscriptionHub } from "./subscriptions.js";

const SCRIPTS: Record<string, Script> = {
  sjoin: script("join", JOIN),
  ssync: script("sync", SYNC),
  srenew: script("renew", RENEW),
  ssetcap: script("setcap", SETCAP),
  sleave: script("leave", LEAVE),
  sinspect: script("inspect", INSPECT),
  sclaim: script("claim", CLAIM),
  sgc: script("gc", GC),
};

export interface RedisPoolOptions {
  /** Connection URL, e.g. redis://localhost:6379. Ignored when `client` is given. */
  url?: string;
  /**
   * An existing client: ioredis (single node, Sentinel or Cluster), node-redis
   * (`createClient()` from "redis"), or any RedisLike. sluice opens one extra
   * connection from it for pub/sub.
   */
  client?: unknown;
  /** Share one subscriber connection between many coordinators (used by sluice-server). */
  subscriptions?: SubscriptionHub;
  /** Extra ioredis options when connecting from `url`. */
  redisOptions?: Record<string, unknown>;
  instanceId: string;
  caller?: string;
  keyPrefix?: string;
  /** Lease length; an instance silent for this long loses its tokens. */
  leaseMs?: number;
  /** How often leases are renewed and load is reported. */
  renewMs?: number;
  /** Stop using held tokens this long before the lease could expire on the Redis side. */
  safetyMs?: number;
  /** What to do when Redis is unreachable: share N evenly, or pause entirely. */
  fallback?: "local-share" | "pause";
  /** Calls slower than this emit a warning. */
  slowMs?: number;
  /**
   * Split hot pools into this many sub-pools on different Redis slots. Each
   * instance uses one home shard; a logical owner keeps the shards' capacities
   * summing to N and moves capacity towards backlog. A function decides per pool.
   */
  shards?: number | ((pool: string) => number);
  logger?: Logger;
  onSlow?: (op: string, ms: number) => void;
}

interface RawState {
  err?: string;
  cap: number;
  free: number;
  debt: number;
  epoch: number;
  inc: string;
  contended: boolean;
  share: number;
  instances: number;
  held: number;
  fence: number;
  owner: string | false;
  granted?: number;
  /** Pool-wide N of a sharded pool (0 when not sharded). */
  total?: number;
  /** Sequence number of the grant returned in this reply. */
  gseq?: number;
  /** Highest grant number ever issued to this instance. */
  gall?: number;
  /** After a drift correction: every grant up to this number is accounted for. */
  gack?: number | false;
}

interface PoolEntry {
  /** Logical pool name, as the engine knows it. */
  name: string;
  /** Physical pool (the home shard, or the pool itself when not sharded). */
  phys: string;
  key: string;
  shards: number;
  /** Sharded pools only: whether we are the logical capacity owner, and with which fence. */
  logicalOwner: boolean;
  lfence: number;
  /** Sharded pools only: the N the logical owner keeps the shards summing to. */
  desiredTotal: number;
  opts: JoinOptions;
  joined: boolean;
  /** Tokens this client believes it holds (grants seen minus releases sent). */
  heldKnown: number;
  /** Every grant numbered up to this has been applied (or written off by a drift correction). */
  glow: number;
  /** Grants applied above `glow` (they can arrive out of order across connections). */
  gapplied: Set<number>;
  pendingRelease: number;
  want: number;
  priority: Priority;
  backlog: number;
  seq: number;
  /** Serialises operations on this pool. */
  chain: Promise<void>;
  flushQueued: boolean;
  lastOk: number;
  fenced: boolean;
  capacity: number;
  instances: number;
  owner: boolean;
  fence: number;
  load: { held: number; inFlight: number; backlog: number };
  renewSoon: boolean;
}

const RANK: Record<Priority, number> = { critical: 0, standard: 1, bulk: 2 };

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms).unref?.());
}

/**
 * Coordinator backed by Redis. Every instance of every caller service that
 * uses the same Redis and namespace shares one token pool per target.
 */
export class RedisCoordinator implements Coordinator {
  readonly kind = "redis" as const;
  private handlers: CoordinatorHandlers | undefined;
  private readonly pools = new Map<string, PoolEntry>();
  private readonly byPhys = new Map<string, PoolEntry>();
  private readonly shardsFor: (pool: string) => number;
  private readonly opts: Required<
    Omit<
      RedisPoolOptions,
      "client" | "url" | "redisOptions" | "onSlow" | "caller" | "subscriptions" | "shards"
    >
  > &
    Pick<RedisPoolOptions, "onSlow">;
  private readonly prefix: string;
  private renewTimer: NodeJS.Timeout | undefined;
  private fenceTimer: NodeJS.Timeout | undefined;
  private closed = false;
  private incarnation = randomId(8);

  private readonly onChannel = (channel: string, message: string) => this.onMessage(channel, message);

  private constructor(
    private readonly cmd: RedisLike,
    private readonly sub: SubscriptionHub,
    opts: RedisPoolOptions,
    private readonly ownsClients: boolean,
  ) {
    this.opts = {
      instanceId: opts.instanceId,
      keyPrefix: opts.keyPrefix ?? "sluice",
      leaseMs: opts.leaseMs ?? 6_000,
      renewMs: opts.renewMs ?? 2_000,
      safetyMs: opts.safetyMs ?? 1_500,
      fallback: opts.fallback ?? "local-share",
      slowMs: opts.slowMs ?? 50,
      logger: opts.logger ?? consoleLogger("warn"),
      onSlow: opts.onSlow,
    };
    this.prefix = this.opts.keyPrefix;
    const shards = opts.shards ?? 1;
    this.shardsFor = (pool) => Math.max(1, Math.floor(typeof shards === "function" ? shards(pool) : shards));
  }

  private homeShard(shards: number): number {
    let h = 0;
    for (const ch of this.opts.instanceId) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
    return h % shards;
  }

  static async create(opts: RedisPoolOptions): Promise<RedisCoordinator> {
    const owns = !opts.client;
    const cmd = opts.client
      ? toRedisLike(opts.client)
      : await connect(opts.url ?? "redis://localhost:6379", opts.redisOptions);
    await cmd.ready();
    const sub = opts.subscriptions ?? (await SubscriptionHub.create(cmd));
    const coordinator = new RedisCoordinator(cmd, sub, opts, owns);
    await coordinator.start();
    return coordinator;
  }

  private async start(): Promise<void> {
    await this.sub.subscribe(this.instanceChannel(), this.onChannel);
    try {
      const policy = await this.cmd.configGet("maxmemory-policy");
      if (policy && policy !== "noeviction") {
        this.opts.logger.warn(
          { policy },
          "redis maxmemory-policy is not noeviction; pool keys could be evicted",
        );
      }
    } catch {
      // CONFIG may be disabled on managed Redis.
    }
    this.renewTimer = setInterval(() => void this.renewAll(), this.opts.renewMs);
    this.renewTimer.unref?.();
    this.fenceTimer = setInterval(() => this.checkFences(), Math.max(100, Math.floor(this.opts.renewMs / 4)));
    this.fenceTimer.unref?.();
  }

  private instanceChannel(): string {
    return `sluice:i:${this.opts.instanceId}`;
  }

  private keyFor(pool: string): string {
    return `${this.prefix}:{${pool}}:`;
  }

  attach(handlers: CoordinatorHandlers): void {
    this.handlers = handlers;
  }

  // ---- Coordinator API -----------------------------------------------------------

  async join(pool: string, opts: JoinOptions): Promise<PoolState> {
    // The first instance to join decides the shard count; everyone else adopts it, so the
    // whole fleet always agrees on which physical pools exist.
    const wanted = this.shardsFor(pool);
    const stored = Number(await this.cmd.hsetnxGet(`${this.prefix}:shards`, pool, String(wanted)));
    const shards = Number.isInteger(stored) && stored > 0 ? stored : wanted;
    if (shards !== wanted)
      this.opts.logger.warn(
        { pool, wanted, shards },
        "pool already uses a different shard count; adopting it",
      );
    const home = shards > 1 ? this.homeShard(shards) : 0;
    const phys = shards > 1 ? `${pool}%${home}` : pool;
    const entry: PoolEntry = {
      name: pool,
      phys,
      key: this.keyFor(phys),
      shards,
      logicalOwner: false,
      lfence: 0,
      desiredTotal: opts.capacity,
      opts,
      joined: false,
      heldKnown: 0,
      glow: 0,
      gapplied: new Set(),
      pendingRelease: 0,
      want: 0,
      priority: "standard",
      backlog: 0,
      seq: 0,
      chain: Promise.resolve(),
      flushQueued: false,
      lastOk: performance.now(),
      fenced: false,
      capacity: opts.capacity,
      instances: 1,
      owner: false,
      fence: 0,
      load: { held: 0, inFlight: 0, backlog: 0 },
      renewSoon: false,
    };
    this.pools.set(pool, entry);
    this.byPhys.set(phys, entry);
    await this.sub.subscribe(`${entry.key}ev`, this.onChannel);
    this.cmd.sadd(`${this.prefix}:pools`, phys).catch(() => {});
    return this.doJoin(entry, opts.held ?? 0);
  }

  private async doJoin(entry: PoolEntry, heldNow: number): Promise<PoolState> {
    // A new shard starts with an even slice of N; the logical owner rebalances from there.
    const seed =
      entry.shards > 1
        ? (apportion(
            entry.opts.capacity,
            new Map(Array.from({ length: entry.shards }, (_, i) => [i, 1])),
          ).get(Number(entry.phys.slice(entry.phys.lastIndexOf("%") + 1))) ?? 0)
        : entry.opts.capacity;
    const raw = await this.call<RawState>("sjoin", entry, [
      entry.opts.caller,
      String(entry.opts.weight ?? 1),
      String(seed),
      String(this.opts.leaseMs),
      this.incarnation,
      String(heldNow),
      String(PROTOCOL_VERSION),
    ]);
    if (raw.err) throw new Error(`sluice: cannot join pool ${entry.name}: ${raw.err}`);
    entry.joined = true;
    entry.heldKnown = heldNow;
    // Grants numbered up to now belong to an earlier registration: never apply them.
    entry.glow = raw.gall ?? 0;
    entry.gapplied.clear();
    entry.pendingRelease = 0;
    entry.seq = 0;
    entry.lastOk = performance.now();
    this.absorb(entry, raw);
    return this.toState(raw);
  }

  demand(pool: string, want: number, hint: { priority: Priority; backlog: number }): void {
    const entry = this.pools.get(pool);
    if (!entry) return;
    entry.want = want;
    entry.priority = hint.priority;
    entry.backlog = hint.backlog;
    this.scheduleFlush(entry);
  }

  release(pool: string, n: number): void {
    const entry = this.pools.get(pool);
    if (!entry || n <= 0) return;
    entry.heldKnown = Math.max(0, entry.heldKnown - n);
    if (entry.fenced) return;
    entry.pendingRelease += n;
    this.scheduleFlush(entry);
  }

  report(pool: string, load: { held: number; inFlight: number; backlog: number }): void {
    const entry = this.pools.get(pool);
    if (entry) entry.load = load;
  }

  setCapacity(pool: string, n: number): void {
    const entry = this.pools.get(pool);
    if (!entry) return;
    if (entry.shards > 1) {
      if (!entry.logicalOwner || entry.desiredTotal === n) return;
      entry.desiredTotal = n;
      entry.chain = entry.chain.then(() => this.rebalance(entry)).catch(() => {});
      return;
    }
    if (!entry.owner) return;
    entry.chain = entry.chain.then(async () => {
      try {
        const raw = await this.call<RawState>("ssetcap", entry, [String(n), String(entry.fence)]);
        if (raw.err === "OVERRIDDEN") return;
        if (raw.err === "STALE_FENCE") {
          entry.owner = false;
          return;
        }
        if (!raw.err) this.absorb(entry, raw);
      } catch (error) {
        this.opts.logger.warn({ pool, err: error }, "setCapacity failed");
      }
    });
  }

  ownsCapacity(pool: string): boolean {
    const e = this.pools.get(pool);
    if (!e) return false;
    return e.shards > 1 ? e.logicalOwner : e.owner;
  }

  /** Fleet facts for a joined pool (used by sluice-server to relay state). */
  info(pool: string): { instances: number; owner: boolean; capacity: number } | undefined {
    const e = this.pools.get(pool);
    return e && { instances: e.instances, owner: this.ownsCapacity(pool), capacity: e.capacity };
  }

  async leave(pool: string): Promise<void> {
    const entry = this.pools.get(pool);
    if (!entry) return;
    this.pools.delete(pool);
    this.byPhys.delete(entry.phys);
    await entry.chain.catch(() => {});
    try {
      await this.sub.unsubscribe(`${entry.key}ev`, this.onChannel);
      await this.call("sleave", entry, []);
    } catch {
      // Lease expiry reclaims the tokens if Redis is unreachable.
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    clearInterval(this.renewTimer);
    clearInterval(this.fenceTimer);
    for (const pool of [...this.pools.keys()]) await this.leave(pool);
    await this.sub.unsubscribe(this.instanceChannel(), this.onChannel);
    this.sub.release();
    if (this.ownsClients) this.cmd.close();
  }

  /** Full pool view for the control panel. */
  async inspect(pool: string): Promise<unknown> {
    return inspectPool(this.cmd, pool, this.prefix);
  }

  // ---- internals -----------------------------------------------------------------

  /** Coalesce releases and demand into one SYNC call per pool, serialised and idempotent. */
  private scheduleFlush(entry: PoolEntry): void {
    if (entry.flushQueued || entry.fenced || !entry.joined) return;
    entry.flushQueued = true;
    entry.chain = entry.chain.then(async () => {
      entry.flushQueued = false;
      if (entry.fenced || !this.pools.has(entry.name)) return;
      const release = entry.pendingRelease;
      entry.pendingRelease = 0;
      const seq = ++entry.seq;
      const args = [
        String(seq),
        String(release),
        String(entry.want),
        String(RANK[entry.priority]),
        String(entry.backlog),
        String(this.opts.leaseMs),
        String(entry.heldKnown),
      ];
      // Retry the identical call (same seq) until it lands or we are fenced: the script deduplicates.
      for (let attempt = 0; ; attempt++) {
        try {
          const raw = await this.call<RawState>("ssync", entry, args);
          if (raw.err === "NOPOOL") {
            await this.rejoin(entry);
            return;
          }
          entry.lastOk = performance.now();
          this.absorb(entry, raw);
          return;
        } catch (error) {
          if (entry.fenced || this.closed || !this.pools.has(entry.name)) return;
          this.opts.logger.debug({ pool: entry.name, attempt, err: error }, "sync failed; retrying");
          await sleep(Math.min(1000, 20 * 2 ** attempt));
        }
      }
    });
  }

  private absorb(entry: PoolEntry, raw: RawState): void {
    entry.capacity = raw.cap;
    entry.instances = Math.max(1, raw.instances);
    entry.fence = raw.fence;
    entry.owner = raw.owner === this.opts.instanceId;
    if (typeof raw.gack === "number") this.writeOff(entry, raw.gack);
    this.handlers?.state(entry.name, this.toState(raw));
    if (raw.granted && raw.granted > 0) this.deliver(entry, raw.granted, raw.gseq);
  }

  /** Grants up to `g` are settled by a drift correction; late messages for them are ignored. */
  private writeOff(entry: PoolEntry, g: number): void {
    if (g <= entry.glow) return;
    entry.glow = g;
    for (const x of entry.gapplied) if (x <= g) entry.gapplied.delete(x);
  }

  private deliver(entry: PoolEntry, n: number, gseq?: number): void {
    if (entry.fenced) return;
    if (gseq !== undefined) {
      // Each grant is applied exactly once; stale or duplicate ones are dropped.
      if (gseq <= entry.glow || entry.gapplied.has(gseq)) return;
      entry.gapplied.add(gseq);
      while (entry.gapplied.has(entry.glow + 1)) entry.gapplied.delete(++entry.glow);
    }
    entry.heldKnown += n;
    entry.want = Math.max(0, entry.want - n);
    this.handlers?.grant(entry.name, n);
  }

  private toState(raw: RawState): PoolState {
    // For a sharded pool the engine sees the pool-wide N; fair shares stay within the home shard.
    return {
      capacity: raw.total && raw.total > 0 ? raw.total : raw.cap,
      contended: raw.contended,
      fairShare: raw.share,
    };
  }

  private onMessage(channel: string, message: string): void {
    if (channel === this.instanceChannel()) {
      // "g <pool> <n>": tokens granted to us by another instance's release.
      const [kind, pool, n, g] = message.split(" ");
      const entry = pool ? this.byPhys.get(pool) : undefined;
      if (kind === "g" && entry && n) this.deliver(entry, Number(n), g === undefined ? undefined : Number(g));
      return;
    }
    for (const entry of this.pools.values()) {
      if (channel === `${entry.key}ev`) {
        // contended / cleared / capacity: refresh our share promptly instead of waiting for the next tick.
        if (!entry.renewSoon) {
          entry.renewSoon = true;
          setTimeout(() => {
            entry.renewSoon = false;
            void this.renew(entry);
          }, 100).unref?.();
        }
      }
    }
  }

  private async renewAll(): Promise<void> {
    await Promise.all([...this.pools.values()].map((e) => this.renew(e)));
  }

  /** Renews run on the pool's operation queue, so they never interleave with a sync. */
  private renew(entry: PoolEntry): Promise<void> {
    const run = entry.chain.then(() => this.renewNow(entry));
    entry.chain = run.catch(() => {});
    return run.catch(() => {});
  }

  private async renewNow(entry: PoolEntry): Promise<void> {
    if (this.closed || !entry.joined) return;
    const started = performance.now();
    try {
      if (entry.fenced) {
        await this.rejoin(entry);
        return;
      }
      const stats = JSON.stringify({ ...entry.load, caller: entry.opts.caller, at: Date.now() });
      const raw = await this.call<RawState>("srenew", entry, [
        String(this.opts.leaseMs),
        // Releases not yet sent are still held as far as Redis knows.
        String(entry.heldKnown + entry.pendingRelease),
        String(entry.load.backlog),
        String(this.opts.leaseMs),
        stats,
        String(entry.glow),
      ]);
      if (raw.err === "NOPOOL") {
        await this.rejoin(entry);
        return;
      }
      entry.lastOk = started;
      this.absorb(entry, raw);
      if (entry.shards > 1) await this.claimAndRebalance(entry);
    } catch (error) {
      this.opts.logger.debug({ pool: entry.name, err: error }, "renew failed");
    }
  }

  /** Sharded pools: renew the logical-owner claim on shard 0 and, if ours, rebalance. */
  private async claimAndRebalance(entry: PoolEntry): Promise<void> {
    const shard0 = `${entry.name}%0`;
    const raw = await this.cmd.evalScript(SCRIPTS.sclaim!, this.keyFor(shard0), [
      shard0,
      this.opts.instanceId,
      String(this.opts.leaseMs),
    ]);
    const claim = JSON.parse(raw) as { owner: string; fence: number };
    const was = entry.logicalOwner;
    entry.logicalOwner = claim.owner === this.opts.instanceId;
    entry.lfence = claim.fence;
    if (entry.logicalOwner && !was)
      this.opts.logger.info({ pool: entry.name }, "logical capacity owner of sharded pool");
    if (entry.logicalOwner) await this.rebalance(entry);
  }

  /**
   * Keep the shards' caps summing to N, weighted by where the work is
   * (backlog + held). Capacity only moves out of a shard's free tokens, so a
   * transfer can never push the pool above N; a real cut of N uses debt.
   */
  private async rebalance(entry: PoolEntry): Promise<void> {
    const n = entry.shards;
    const names = Array.from({ length: n }, (_, i) => `${entry.name}%${i}`);
    type View = {
      err?: string;
      cap: number;
      free: number;
      instances?: { held: number; backlog: number }[] | Record<string, never>;
    };
    const views = await Promise.all(names.map((p) => inspectPool(this.cmd, p, this.prefix) as Promise<View>));
    const caps = views.map((v) => (v.err ? 0 : v.cap));
    const weights = new Map<number, number>();
    views.forEach((v, i) => {
      const inst = Array.isArray(v.instances) ? v.instances : [];
      weights.set(i, inst.reduce((a, x) => a + x.backlog + x.held, 0) + (inst.length > 0 ? 1 : 0));
    });
    const total = entry.desiredTotal;
    const target = apportion(total, weights);
    const sum = caps.reduce((a, b) => a + b, 0);
    const set = async (i: number, cap: number, mode: "shard" | "shard-free") => {
      const res = JSON.parse(
        await this.cmd.evalScript(SCRIPTS.ssetcap!, this.keyFor(names[i]!), [
          names[i]!,
          this.opts.instanceId,
          String(cap),
          String(entry.lfence),
          mode,
          String(total),
        ]),
      ) as RawState;
      if (res.err === "STALE_FENCE") {
        entry.logicalOwner = false;
        throw new Error("stale logical fence");
      }
      return res.err ? caps[i]! : res.cap;
    };
    // 1. Take free capacity from shards above target.
    for (let i = 0; i < n; i++) {
      if (caps[i]! > target.get(i)!)
        caps[i] = await set(i, target.get(i)!, sum > total ? "shard" : "shard-free");
    }
    // 2. Hand out what is now unassigned to shards below target.
    let spare = total - caps.reduce((a, b) => a + b, 0);
    for (let i = 0; i < n && spare > 0; i++) {
      const need = target.get(i)! - caps[i]!;
      if (need <= 0) continue;
      const give = Math.min(need, spare);
      caps[i] = await set(i, caps[i]! + give, "shard");
      spare -= give;
    }
    // 3. Keep every shard's view of the pool-wide N current.
    for (let i = 0; i < n; i++) if (caps[i] === target.get(i)) await set(i, caps[i]!, "shard");
  }

  /** Re-register after a fence, a reclaim, or lost Redis data, carrying our real in-flight count. */
  private async rejoin(entry: PoolEntry): Promise<void> {
    const wasFenced = entry.fenced;
    // After a fence our in-flight requests are orphans the engine no longer counts as held.
    const heldNow = wasFenced ? 0 : entry.heldKnown;
    const state = await this.doJoin(entry, heldNow);
    if (wasFenced) {
      entry.fenced = false;
      this.handlers?.restored(entry.name);
      this.opts.logger.info({ pool: entry.name }, "rejoined pool after fence");
    }
    this.handlers?.state(entry.name, state);
    if (entry.want > 0) this.scheduleFlush(entry);
  }

  /** Stop trusting held tokens before Redis can hand them to someone else. */
  private checkFences(): void {
    const now = performance.now();
    for (const entry of this.pools.values()) {
      if (entry.fenced || !entry.joined) continue;
      if (now - entry.lastOk > this.opts.leaseMs - this.opts.safetyMs) {
        entry.fenced = true;
        entry.heldKnown = 0;
        entry.pendingRelease = 0;
        entry.want = 0;
        const fallback =
          this.opts.fallback === "pause"
            ? 0
            : Math.max(1, Math.floor(entry.capacity / Math.max(1, entry.instances)));
        this.opts.logger.warn({ pool: entry.name, fallback }, "lost contact with redis; fenced held tokens");
        this.handlers?.fenced(entry.name, fallback);
      }
    }
  }

  private async call<T>(script: string, entry: PoolEntry, args: string[]): Promise<T> {
    const started = performance.now();
    const raw = await this.cmd.evalScript(SCRIPTS[script]!, entry.key, [
      entry.phys,
      this.opts.instanceId,
      ...args,
    ]);
    const ms = performance.now() - started;
    if (ms > this.opts.slowMs) this.opts.onSlow?.(script, ms);
    return (raw === "OK" ? {} : JSON.parse(raw)) as T;
  }
}

/** Wait until a client (ioredis, node-redis or RedisLike) can take commands. */
export function waitReady(client: unknown, timeoutMs = 10_000): Promise<void> {
  return toRedisLike(client).ready(timeoutMs);
}

/** Operator capacity override (wins over the cap-owner). Pass "clear" to hand control back. */
export async function overrideCapacity(
  client: unknown,
  pool: string,
  capacity: number | "clear",
  actor: string,
  keyPrefix = "sluice",
): Promise<unknown> {
  const r = toRedisLike(client);
  await r.ready();
  const raw = await r.evalScript(SCRIPTS.ssetcap!, `${keyPrefix}:{${pool}}:`, [
    pool,
    `admin:${actor}`,
    String(capacity),
    "admin",
  ]);
  return JSON.parse(raw);
}

/**
 * Delete a pool nobody uses any more (no registered instance, no operator pin) and
 * drop it from the registry. Returns true if it was deleted.
 */
export async function collectPool(client: unknown, pool: string, keyPrefix = "sluice"): Promise<boolean> {
  const r = toRedisLike(client);
  await r.ready();
  const deleted = (await r.evalScript(SCRIPTS.sgc!, `${keyPrefix}:{${pool}}:`, [pool, ""])) === "1";
  if (deleted) await r.srem(`${keyPrefix}:pools`, pool);
  return deleted;
}

/** Read a pool's full state without joining it (control panel). */
export async function inspectPool(client: unknown, pool: string, keyPrefix = "sluice"): Promise<unknown> {
  const r = toRedisLike(client);
  await r.ready();
  const raw = await r.evalScript(SCRIPTS.sinspect!, `${keyPrefix}:{${pool}}:`, [pool, ""]);
  return JSON.parse(raw);
}

/** Create a Redis-backed coordinator. Pass it as `coordination` to createEngine. */
export function redisPool(opts: RedisPoolOptions): Promise<RedisCoordinator> {
  return RedisCoordinator.create(opts);
}
