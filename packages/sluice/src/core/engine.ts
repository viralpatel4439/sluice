import { type Clock, type TimerHandle, systemClock } from "./clock.js";
import { type Coordinator, LocalHub } from "./coordinator.js";
import { ConfigError, SluiceError } from "./errors.js";
import { Emitter, type EngineEvents, type EventName } from "./events.js";
import { randomId } from "./ids.js";
import { type Logger, consoleLogger } from "./logger.js";
import type { TargetSnapshot } from "./stats.js";
import { Target, type TargetOptions, type TracingHooks } from "./target.js";
import { TimerHeap } from "./timers.js";
import type { SubmitOptions, UnsentItem } from "./types.js";

// Targets are heterogeneous (each has its own payload/result types), so the registry erases them.
// biome-ignore lint/suspicious/noExplicitAny: see above
export type AnyTarget = Target<any, any>;
// biome-ignore lint/suspicious/noExplicitAny: see above
export type AnyTargetOptions = TargetOptions<any, any>;

export interface EngineOptions {
  /** Isolates fleets sharing one coordinator (e.g. "prod-eu"). */
  namespace?: string;
  /** Name of the calling service. */
  caller?: string;
  /** Fair-share weight of this caller when several services share a target. */
  weight?: number;
  instanceId?: string;
  /** Omit for a single instance. */
  coordination?: Coordinator;
  logger?: Logger;
  clock?: Clock;
  random?: () => number;
  /** Lazily created targets unused for this long are evicted. */
  idleTtlMs?: number;
  maxTargets?: number;
  /** Interval of load reports to the coordinator. */
  reportIntervalMs?: number;
}

export interface Health {
  live: boolean;
  ready: boolean;
  draining: boolean;
  targets: { name: string; fallback: boolean; circuit: string; queued: number }[];
}

interface Template {
  pattern: RegExp;
  source: string;
  opts: AnyTargetOptions;
}

function globToRegExp(glob: string): RegExp {
  const escaped = glob
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*/g, ".*")
    .replace(/\?/g, ".");
  return new RegExp(`^${escaped}$`);
}

const NAME_RE = /^[A-Za-z0-9._:\-]{1,200}$/;

export class Engine {
  readonly namespace: string;
  readonly caller: string;
  readonly instanceId: string;
  readonly coordinator: Coordinator;
  readonly logger: Logger;
  private readonly clock: Clock;
  private readonly timers: TimerHeap;
  private readonly emitter = new Emitter();
  private readonly targets = new Map<string, AnyTarget>();
  private readonly templates: Template[] = [];
  private readonly opts: EngineOptions;
  private reportTimer: TimerHandle | undefined;
  private tracing: TracingHooks | undefined;
  private draining = false;
  private closed = false;

  constructor(opts: EngineOptions = {}) {
    this.opts = opts;
    this.namespace = opts.namespace ?? "default";
    this.caller = opts.caller ?? "unknown";
    this.instanceId = opts.instanceId ?? `${this.caller}-${randomId(4)}`;
    for (const [field, value] of [
      ["namespace", this.namespace],
      ["caller", this.caller],
    ] as const) {
      if (!NAME_RE.test(value)) throw new ConfigError(`${field} "${value}" must match ${NAME_RE}`);
    }
    this.clock = opts.clock ?? systemClock;
    this.timers = new TimerHeap(this.clock);
    this.logger = opts.logger ?? consoleLogger("warn", { caller: this.caller, instanceId: this.instanceId });
    this.coordinator = opts.coordination ?? new LocalHub().client();
    this.coordinator.attach({
      grant: (pool, n) => this.byPool(pool)?.onGrant(pool, n),
      state: (pool, s) => this.byPool(pool)?.onState(pool, s),
      fenced: (pool, cap) => this.byPool(pool)?.onFenced(pool, cap),
      restored: (pool) => this.byPool(pool)?.onRestored(pool),
    });
    this.scheduleReport();
  }

  /** Install tracing hooks (used by sluice/otel). Pass undefined to remove them. */
  setTracingHooks(hooks: TracingHooks | undefined): void {
    this.tracing = hooks;
  }

  on<K extends EventName>(event: K, fn: (payload: EngineEvents[K]) => void): () => void {
    return this.emitter.on(event, fn);
  }

  /** Declare a target (a downstream service). Returns the existing one if already declared. */
  target<P = unknown, R = unknown>(name: string, opts: TargetOptions<P, R>): Target<P, R> {
    const existing = this.targets.get(name);
    if (existing) return existing as Target<P, R>;
    if (this.closed || this.draining)
      throw new SluiceError("ERR_SHUTDOWN", "sluice: engine is draining or closed");
    if (!NAME_RE.test(name)) throw new ConfigError(`target name "${name}" must match ${NAME_RE}`);
    if (typeof opts.send !== "function") throw new ConfigError(`target "${name}" needs a send function`);
    validateTargetOptions(name, opts);
    const maxTargets = this.opts.maxTargets ?? 1000;
    if (this.targets.size >= maxTargets) this.evictIdle(0);
    if (this.targets.size >= maxTargets) {
      throw new SluiceError(
        "ERR_TOO_MANY_TARGETS",
        `sluice: more than ${maxTargets} targets; raise maxTargets`,
      );
    }
    const t = new Target<P, R>(
      {
        name,
        pool: `${this.namespace}/${name}`,
        caller: this.caller,
        weight: this.opts.weight ?? 1,
        clock: this.clock,
        timers: this.timers,
        coordinator: this.coordinator,
        emitter: this.emitter,
        logger: this.logger,
        random: this.opts.random ?? Math.random,
        hooks: () => this.tracing,
      },
      opts,
    );
    this.targets.set(name, t);
    this.emitter.emit("target:created", { target: name });
    return t;
  }

  /** Register options for targets created lazily on first submit, matched by glob ("partner-*"). */
  define<P = unknown, R = unknown>(pattern: string, opts: TargetOptions<P, R>): void {
    this.templates.push({ pattern: globToRegExp(pattern), source: pattern, opts });
  }

  /** Submit to a target by name, creating it from a template if needed. */
  submit<R = unknown>(payload: unknown, opts: SubmitOptions & { target: string }): Promise<R> {
    const t = this.resolve(opts.target);
    return t.submit(payload, opts) as Promise<R>;
  }

  /** Find or lazily create a target. */
  resolve(name: string, fallback?: AnyTargetOptions): AnyTarget {
    const existing = this.targets.get(name);
    if (existing) return existing;
    const template = this.templates.find((t) => t.pattern.test(name));
    const opts = template?.opts ?? fallback;
    if (!opts) throw new ConfigError(`unknown target "${name}" and no matching define() template`);
    return this.target(name, opts);
  }

  get(name: string): AnyTarget | undefined {
    return this.targets.get(name);
  }

  list(): AnyTarget[] {
    return [...this.targets.values()];
  }

  snapshot(): TargetSnapshot[] {
    return this.list().map((t) => t.snapshot());
  }

  health(): Health {
    const targets = this.list().map((t) => {
      const s = t.snapshot();
      return { name: t.name, fallback: s.fallback, circuit: s.circuit, queued: s.queued };
    });
    return { live: !this.closed, ready: !this.closed && !this.draining, draining: this.draining, targets };
  }

  /**
   * Graceful shutdown: stop intake, let queued work finish for up to `timeoutMs`,
   * then reject what is left and pass it to `onUnsent`.
   */
  async drain(
    opts: { timeoutMs?: number; onUnsent?: (items: UnsentItem[]) => void } = {},
  ): Promise<UnsentItem[]> {
    if (this.closed) return [];
    this.draining = true;
    this.logger.info({ targets: this.targets.size }, "draining");
    const results = await Promise.all(this.list().map((t) => t.drain(opts.timeoutMs ?? 25_000)));
    const unsent = results.flat();
    if (unsent.length > 0) {
      this.logger.warn({ unsent: unsent.length }, "drain finished with unsent items");
      try {
        opts.onUnsent?.(unsent);
      } catch (error) {
        this.emitter.emit("error", { error, where: "onUnsent" });
      }
    }
    await this.close();
    return unsent;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.draining = true;
    this.reportTimer?.cancel();
    await Promise.all(this.list().map((t) => t.close()));
    this.timers.clear();
    await this.coordinator.close();
  }

  private byPool(pool: string): AnyTarget | undefined {
    const prefix = `${this.namespace}/`;
    if (!pool.startsWith(prefix)) return undefined;
    const rest = pool.slice(prefix.length);
    const hash = rest.indexOf("#");
    return this.targets.get(hash === -1 ? rest : rest.slice(0, hash));
  }

  private scheduleReport(): void {
    const interval = this.opts.reportIntervalMs ?? 2_000;
    this.reportTimer = this.clock.setTimer(interval, () => {
      for (const t of this.targets.values()) t.report();
      this.evictIdle(this.opts.idleTtlMs ?? 10 * 60_000);
      if (!this.closed) this.scheduleReport();
    });
  }

  private evictIdle(ttlMs: number): void {
    const now = this.clock.now();
    for (const [name, t] of this.targets) {
      if (t.idle && now - t.lastUsed >= ttlMs && this.templates.some((tpl) => tpl.pattern.test(name))) {
        this.targets.delete(name);
        void t.close();
        this.emitter.emit("target:evicted", { target: name });
      }
    }
  }
}

function validateTargetOptions(name: string, opts: AnyTargetOptions): void {
  const q = opts.queue;
  if (q?.maxItems !== undefined && !(q.maxItems > 0))
    throw new ConfigError(`${name}: queue.maxItems must be > 0`);
  if (q?.maxBytes !== undefined && !(q.maxBytes > 0))
    throw new ConfigError(`${name}: queue.maxBytes must be > 0`);
  const r = opts.retry;
  if (r?.budget !== undefined && !(r.budget > 0 && r.budget <= 1))
    throw new ConfigError(`${name}: retry.budget must be in (0, 1]`);
  if (r?.max !== undefined && !(r.max >= 0)) throw new ConfigError(`${name}: retry.max must be >= 0`);
  if (opts.timeoutMs !== undefined && !(opts.timeoutMs > 0))
    throw new ConfigError(`${name}: timeoutMs must be > 0`);
  for (const [route, ro] of Object.entries(opts.routes ?? {})) {
    if (ro.maxShare !== undefined && !(ro.maxShare > 0 && ro.maxShare <= 1)) {
      throw new ConfigError(`${name}: routes.${route}.maxShare must be in (0, 1]`);
    }
  }
  if (typeof opts.capacity === "number" && !(opts.capacity >= 0))
    throw new ConfigError(`${name}: capacity must be >= 0`);
}

export function createEngine(opts: EngineOptions = {}): Engine {
  return new Engine(opts);
}
