import {
  type CapacityConfig,
  type CapacitySource,
  type Observation,
  createCapacitySource,
} from "./capacity.js";
import { Circuit, type CircuitOptions, DEFAULT_CIRCUIT } from "./circuit.js";
import type { Clock, TimerHandle } from "./clock.js";
import type { Coordinator, PoolState } from "./coordinator.js";
import { Deque } from "./deque.js";
import {
  DeadlineExceededError,
  DroppedError,
  QueueFullError,
  ShutdownError,
  TimeoutError,
} from "./errors.js";
import type { Emitter, ItemEvent } from "./events.js";
import { ulid } from "./ids.js";
import type { Logger } from "./logger.js";
import { LaneQueue, type Queued } from "./queue.js";
import { DEFAULT_RETRY, backoffMs, defaultClassify, isOverloadSignal, retryBudget } from "./retry.js";
import { Histogram, type TargetCounters, type TargetSnapshot, emptyCounters } from "./stats.js";
import type { TimerHeap } from "./timers.js";
import type {
  Item,
  Priority,
  QueueOptions,
  RetryOptions,
  RouteOptions,
  SendFn,
  SubmitOptions,
  UnsentItem,
} from "./types.js";

export interface TargetOptions<P = unknown, R = unknown> {
  send: SendFn<P, R>;
  capacity?: CapacityConfig | number;
  queue?: Partial<QueueOptions>;
  retry?: Partial<RetryOptions>;
  /** Per-request timeout. The token is returned when it fires even if `send` never settles. */
  timeoutMs?: number;
  routes?: Record<string, RouteOptions>;
  circuit?: Partial<CircuitOptions>;
  /** "coalesce": a submit with a key already queued or in flight returns the same promise. */
  dedupe?: "none" | "coalesce";
  requireKey?: boolean;
  /** Estimate payload size for byte limits. Default: JSON length for objects, byteLength for strings/buffers. */
  sizeOf?: (payload: P) => number;
  /** Called when an item exhausts its retries. */
  onDead?: (item: UnsentItem, error: unknown) => void;
  /** When false (default true), a retryable outcome is never retried for this target. */
  retrySafe?: (item: Item<P>) => boolean;
}

export interface TargetDeps {
  name: string;
  pool: string;
  caller: string;
  weight: number;
  clock: Clock;
  timers: TimerHeap;
  coordinator: Coordinator;
  emitter: Emitter;
  logger: Logger;
  random: () => number;
  /** Tracing hooks, read at call time so they can be installed after targets exist. */
  hooks: () => TracingHooks | undefined;
}

/** Lets a tracer capture the caller's context at submit and run each send inside it. */
export interface TracingHooks {
  /** Called synchronously in submit(); the result is handed back to `send`. */
  capture(): unknown;
  /** Wrap one send attempt. `waitMs` is the time the item queued before its first attempt. */
  send<T>(
    captured: unknown,
    info: { target: string; item: Item; waitMs: number },
    run: () => Promise<T>,
  ): Promise<T>;
}

/** One queued call: the Item handed to `send`, plus the engine's bookkeeping. One object per call. */
interface Entry extends Queued, Item {
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
  isRetry: boolean;
  deadlineTimer: TimerHandle | undefined;
  /** Set while the entry is waiting for its retry delay. */
  delayed: boolean;
  /** Set while a send for this entry is running. */
  sending: boolean;
  waitMs: number;
  /** Tracing context captured at submit. */
  traced: unknown;
}

/** Tokens leased from one pool (the target, or a capped route inside it). */
class Wallet {
  /** Tokens leased from the coordinator in the current epoch. */
  held = 0;
  /** Requests in flight that hold a token of the current epoch. */
  inFlight = 0;
  /** Requests in flight from older epochs (before a fence/restore). They no longer hold tokens. */
  orphans = 0;
  epoch = 0;
  fallback = false;
  fallbackLimit = 0;
  capacity: number;
  contended = false;
  fairShare = 0;
  wanted = 0;
  joined = false;

  constructor(
    readonly pool: string,
    capacity: number,
  ) {
    this.capacity = capacity;
  }

  free(): number {
    if (this.fallback) return Math.max(0, this.fallbackLimit - this.inFlight - this.orphans);
    return this.held - this.inFlight;
  }
}

const NETWORK_ERROR_NAMES = new Set([
  "TypeError",
  "TimeoutError",
  "FetchError",
  "ConnectTimeoutError",
  "SocketError",
]);

function isConnectionFailure(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const code = (error as { code?: string }).code ?? (error.cause as { code?: string } | undefined)?.code;
  if (
    code &&
    /^(ECONNREFUSED|ECONNRESET|ENOTFOUND|ETIMEDOUT|EHOSTUNREACH|EPIPE|UND_ERR|ERR_TIMEOUT)/.test(code)
  )
    return true;
  return NETWORK_ERROR_NAMES.has(error.name);
}

function defaultSize(payload: unknown): number {
  if (payload === undefined || payload === null) return 0;
  if (typeof payload === "string") return Buffer.byteLength(payload);
  if (payload instanceof Uint8Array) return payload.byteLength;
  if (payload instanceof ArrayBuffer) return payload.byteLength;
  try {
    return Buffer.byteLength(JSON.stringify(payload) ?? "");
  } catch {
    return 0;
  }
}

/** Runtime for one target: its queue, wallets, dispatcher, retries and capacity source. */
export class Target<P = unknown, R = unknown> {
  readonly name: string;
  readonly pool: string;
  private readonly opts: TargetOptions<P, R>;
  private readonly retry: RetryOptions;
  private readonly queue: LaneQueue<Entry>;
  private readonly retryQueue = new Deque<Entry>();
  private readonly blocked = new Deque<{ size: number; wake: () => void }>();
  private readonly coalesce = new Map<string, Promise<unknown>>();
  private readonly wallet: Wallet;
  private readonly routes = new Map<string, { opts: RouteOptions; wallet: Wallet }>();
  private readonly circuit: Circuit;
  private capacitySource: CapacitySource;
  private capacitySourceName: string;
  private retriesInFlight = 0;
  private delayedRetries = 0;
  private seq = 0;
  private pumping = false;
  private repump = false;
  private closed = false;
  private joined: Promise<void>;
  private drainWaiters: (() => void)[] = [];
  private circuitTimer: TimerHandle | undefined;
  lastUsed: number;
  readonly counters: TargetCounters = emptyCounters();
  readonly waitHist = new Histogram();
  readonly sendHist = new Histogram();

  constructor(
    private readonly deps: TargetDeps,
    opts: TargetOptions<P, R>,
  ) {
    this.name = deps.name;
    this.pool = deps.pool;
    this.opts = opts;
    this.retry = { ...DEFAULT_RETRY, ...opts.retry };
    this.queue = new LaneQueue<Entry>({
      maxItems: opts.queue?.maxItems ?? 100_000,
      maxBytes: opts.queue?.maxBytes ?? 256 * 1024 * 1024,
      overflow: opts.queue?.overflow ?? "block",
    });
    this.capacitySource = createCapacitySource(opts.capacity);
    this.capacitySourceName = this.capacitySource.name;
    this.wallet = new Wallet(deps.pool, this.capacitySource.initial);
    for (const [route, ropts] of Object.entries(opts.routes ?? {})) {
      this.routes.set(route, {
        opts: ropts,
        wallet: new Wallet(`${deps.pool}#${route}`, this.routeCapacity(ropts, this.wallet.capacity)),
      });
    }
    this.circuit = new Circuit({ ...DEFAULT_CIRCUIT, ...opts.circuit }, deps.clock, (state) => {
      this.deps.emitter.emit(
        state === "open" ? "circuit:open" : state === "closed" ? "circuit:close" : "circuit:open",
        { target: this.name },
      );
      if (state === "open") this.deps.logger.warn({ target: this.name }, "circuit open: target unreachable");
      if (state === "closed") this.deps.logger.info({ target: this.name }, "circuit closed");
    });
    this.lastUsed = deps.clock.now();
    this.joined = this.join();
  }

  private routeCapacity(r: RouteOptions, total: number): number {
    const share =
      r.maxShare !== undefined ? Math.max(1, Math.floor(total * r.maxShare)) : Number.POSITIVE_INFINITY;
    return Math.min(share, r.max ?? Number.POSITIVE_INFINITY);
  }

  private async join(): Promise<void> {
    const { coordinator, caller, weight } = this.deps;
    try {
      const state = await coordinator.join(this.pool, { caller, capacity: this.wallet.capacity, weight });
      this.applyState(this.wallet, state);
      this.wallet.joined = true;
      for (const r of this.routes.values()) {
        const rs = await coordinator.join(r.wallet.pool, { caller, capacity: r.wallet.capacity, weight });
        this.applyState(r.wallet, rs);
        r.wallet.joined = true;
      }
    } catch (error) {
      this.deps.logger.error({ target: this.name, err: error }, "failed to join pool; using fallback");
      this.onFenced(this.pool, Math.max(1, Math.floor(this.wallet.capacity / 2)));
    }
    this.capacitySource.start({
      clock: this.deps.clock,
      logger: this.deps.logger,
      target: this.name,
      set: (n, source) => this.setCapacity(n, source),
      inFlight: () => this.wallet.inFlight + this.wallet.orphans,
      current: () => this.wallet.capacity,
    });
    this.pump();
  }

  // ---- coordinator callbacks -------------------------------------------------

  onGrant(pool: string, n: number): void {
    const w = this.walletFor(pool);
    if (!w) {
      this.deps.coordinator.release(pool, n);
      return;
    }
    if (w.fallback) {
      this.deps.coordinator.release(pool, n);
      return;
    }
    w.held += n;
    w.wanted = Math.max(0, w.wanted - n);
    this.emitTokens();
    this.pump();
  }

  onState(pool: string, state: PoolState): void {
    const w = this.walletFor(pool);
    if (!w) return;
    const prev = w.capacity;
    this.applyState(w, state);
    if (pool === this.pool && prev !== state.capacity) {
      for (const r of this.routes.values()) r.wallet.capacity = this.routeCapacity(r.opts, state.capacity);
      this.deps.emitter.emit("capacity:changed", {
        target: this.name,
        capacity: state.capacity,
        previous: prev,
        source: "pool",
      });
    }
    if (pool === this.pool) this.applyDesiredCapacity();
    this.rebalance();
    this.pump();
  }

  onFenced(pool: string, fallbackCapacity: number): void {
    const w = this.walletFor(pool);
    if (!w || w.fallback) return;
    // Held tokens are no longer ours. In-flight requests keep running but become orphans.
    w.orphans += w.inFlight;
    w.inFlight = 0;
    w.held = 0;
    w.wanted = 0;
    w.epoch++;
    w.fallback = true;
    w.fallbackLimit = Math.max(0, fallbackCapacity);
    if (pool === this.pool) {
      this.deps.emitter.emit("fallback:on", { target: this.name, capacity: w.fallbackLimit });
      this.deps.logger.warn(
        { target: this.name, fallbackCapacity },
        "lost coordinator; using local fallback limit",
      );
    }
    this.pump();
  }

  onRestored(pool: string): void {
    const w = this.walletFor(pool);
    if (!w || !w.fallback) return;
    w.orphans += w.inFlight;
    w.inFlight = 0;
    w.held = 0;
    w.wanted = 0;
    w.epoch++;
    w.fallback = false;
    if (pool === this.pool) {
      this.deps.emitter.emit("fallback:off", { target: this.name });
      this.deps.logger.info({ target: this.name }, "coordinator restored");
    }
    this.pump();
  }

  private walletFor(pool: string): Wallet | undefined {
    if (pool === this.pool) return this.wallet;
    for (const r of this.routes.values()) if (r.wallet.pool === pool) return r.wallet;
    return undefined;
  }

  private applyState(w: Wallet, state: PoolState): void {
    w.capacity = state.capacity;
    w.contended = state.contended;
    w.fairShare = state.fairShare;
  }

  /** What our capacity source last asked for; applied whenever this instance owns capacity. */
  private desiredCapacity: number | undefined;

  private setCapacity(n: number, source: string): void {
    this.capacitySourceName = source;
    this.desiredCapacity = n;
    this.applyDesiredCapacity();
  }

  /** Push the desired capacity to the pool if we are its capacity owner and it differs. */
  private applyDesiredCapacity(): void {
    const n = this.desiredCapacity;
    const { coordinator } = this.deps;
    if (n === undefined || !coordinator.ownsCapacity(this.pool)) return;
    const prev = this.wallet.capacity;
    if (prev === n) return;
    coordinator.setCapacity(this.pool, n);
    for (const r of this.routes.values())
      coordinator.setCapacity(r.wallet.pool, this.routeCapacity(r.opts, n));
    this.deps.logger.info(
      { target: this.name, capacity: n, previous: prev, source: this.capacitySourceName },
      "capacity changed",
    );
  }

  // ---- submit ------------------------------------------------------------------

  submit(payload: P, opts: SubmitOptions = {}): Promise<R> {
    if (this.closed) return Promise.reject(new ShutdownError());
    if (this.opts.requireKey && !opts.key)
      return Promise.reject(new TypeError(`sluice: target "${this.name}" requires a key`));
    this.lastUsed = this.deps.clock.now();
    if (this.opts.dedupe === "coalesce" && opts.key) {
      const existing = this.coalesce.get(opts.key);
      if (existing) return existing as Promise<R>;
    }
    const promise = this.enqueueNew(payload, opts);
    if (this.opts.dedupe === "coalesce" && opts.key) {
      const key = opts.key;
      this.coalesce.set(key, promise);
      const cleanup = () => this.coalesce.delete(key);
      promise.then(cleanup, cleanup);
    }
    return promise;
  }

  /** Fast path: room in the queue means one promise and one entry, nothing else. */
  private enqueueNew(payload: P, opts: SubmitOptions): Promise<R> {
    const size = opts.size ?? (this.opts.sizeOf ? this.opts.sizeOf(payload) : defaultSize(payload));
    const priority: Priority = opts.priority ?? "standard";
    this.counters.submitted++;
    if (!this.queue.hasRoom(size)) return this.enqueueOverflow(payload, opts, size, priority);
    return this.createEntry(payload, opts, size, priority);
  }

  private async enqueueOverflow(
    payload: P,
    opts: SubmitOptions,
    size: number,
    priority: Priority,
  ): Promise<R> {
    const policy = this.queue.overflow;
    if (policy === "reject") {
      this.counters.rejected++;
      throw new QueueFullError(this.name);
    }
    if (policy === "drop-lowest") {
      const victim = this.queue.evictLowest(priority);
      if (!victim) {
        this.counters.rejected++;
        throw new QueueFullError(this.name);
      }
      victim.deadlineTimer?.cancel();
      this.counters.dropped++;
      this.deps.emitter.emit("item:dropped", this.itemEvent(victim));
      victim.reject(new DroppedError(this.name));
    } else {
      // block: wait for room. The caller's promise simply stays pending longer.
      while (!this.queue.hasRoom(size)) {
        await new Promise<void>((wake) => this.blocked.push({ size, wake }));
        if (this.closed) throw new ShutdownError();
      }
    }
    return this.createEntry(payload, opts, size, priority);
  }

  private createEntry(payload: P, opts: SubmitOptions, size: number, priority: Priority): Promise<R> {
    const now = this.deps.clock.now();
    // Headers and a generated key are only materialised when the item is first sent,
    // so a queued item costs as little memory as possible.
    let captured: unknown;
    const hooks = this.deps.hooks();
    if (hooks) {
      try {
        captured = hooks.capture();
      } catch (error) {
        this.deps.emitter.emit("error", { error, where: "hooks.capture" });
      }
    }
    return new Promise<R>((resolve, reject) => {
      const entry: Entry = {
        id: ++this.seq,
        key: opts.key ?? "",
        payload,
        priority,
        route: opts.route,
        lane: opts.route !== undefined && this.routes.has(opts.route) ? opts.route : undefined,
        size,
        enqueuedAt: now,
        deadlineAt: opts.deadlineMs !== undefined ? now + opts.deadlineMs : undefined,
        attempt: 0,
        signal: opts.signal,
        headers: opts.headers as Record<string, string>,
        dead: false,
        resolve: resolve as (v: unknown) => void,
        reject,
        isRetry: false,
        deadlineTimer: undefined,
        delayed: false,
        sending: false,
        waitMs: 0,
        traced: captured,
      };
      if (opts.signal) {
        if (opts.signal.aborted) {
          reject(opts.signal.reason);
          return;
        }
        opts.signal.addEventListener("abort", () => this.cancel(entry, opts.signal!.reason), { once: true });
      }
      if (entry.deadlineAt !== undefined) {
        entry.deadlineTimer = this.deps.timers.schedule(entry.deadlineAt, () => this.expire(entry));
      }
      this.queue.push(entry);
      this.pump();
    });
  }

  /** The item's idempotency key, generated on first use if the caller gave none. */
  private keyOf(item: Item): string {
    if (item.key === "") (item as { key: string }).key = ulid();
    return item.key;
  }

  private expire(entry: Entry): void {
    this.removeWaiting(entry, new DeadlineExceededError(this.name), true);
  }

  /** The caller aborted. Waiting entries are removed; a running send sees the same signal. */
  private cancel(entry: Entry, reason: unknown): void {
    if (entry.sending) return;
    this.removeWaiting(entry, reason, false);
  }

  private removeWaiting(entry: Entry, error: unknown, expired: boolean): void {
    if (entry.dead || entry.sending) return;
    if (entry.isRetry) {
      if (entry.delayed) {
        entry.delayed = false;
        this.delayedRetries--;
      } else {
        this.retryQueue.remove((e) => e === entry);
      }
      entry.dead = true;
    } else {
      this.queue.kill(entry);
      this.wakeBlocked();
    }
    entry.deadlineTimer?.cancel();
    if (expired) {
      this.counters.expired++;
      this.deps.emitter.emit("item:expired", this.itemEvent(entry));
    }
    entry.reject(error);
    this.updateDemand();
    this.checkDrained();
  }

  // ---- dispatch ----------------------------------------------------------------

  /** Send as many items as free tokens allow. Re-entrant safe. */
  pump(): void {
    if (this.pumping) {
      this.repump = true;
      return;
    }
    this.pumping = true;
    try {
      do {
        this.repump = false;
        // Fairness first: tokens above our share go back before the fast path can reuse them.
        this.rebalance();
        this.pumpOnce();
      } while (this.repump);
    } finally {
      this.pumping = false;
    }
    this.updateDemand();
    this.rebalance();
    this.checkDrained();
  }

  private pumpOnce(): void {
    while (this.wallet.free() > 0) {
      if (!this.circuit.canSend()) {
        this.scheduleCircuitRetry();
        return;
      }
      const entry = this.nextEntry();
      if (!entry) return;
      this.dispatch(entry);
    }
  }

  private scheduleCircuitRetry(): void {
    if (this.circuitTimer) return;
    const wait = Math.max(10, this.circuit.retryInMs());
    this.circuitTimer = this.deps.clock.setTimer(wait, () => {
      this.circuitTimer = undefined;
      this.pump();
    });
  }

  private routeOpen = (route: string | undefined): boolean => {
    if (route === undefined) return true;
    const r = this.routes.get(route);
    return !r || r.wallet.free() > 0;
  };

  private nextEntry(): Entry | undefined {
    const budget = retryBudget(
      this.wallet.fallback ? this.wallet.fallbackLimit : this.wallet.held,
      this.retry.budget,
    );
    if (this.retryQueue.length > 0 && this.retriesInFlight < budget) {
      for (let i = 0; i < this.retryQueue.length; i++) {
        const head = this.retryQueue.shift()!;
        if (this.routeOpen(head.lane)) return head;
        this.retryQueue.push(head);
      }
    }
    const entry = this.queue.shift(this.routeOpen);
    if (entry) this.wakeBlocked();
    return entry;
  }

  private wakeBlocked(): void {
    while (this.blocked.length > 0 && this.queue.hasRoom(this.blocked.peek()!.size))
      this.blocked.shift()!.wake();
  }

  private dispatch(entry: Entry): void {
    const item: Item = entry;
    entry.deadlineTimer?.cancel();
    entry.deadlineTimer = undefined;
    const now = this.deps.clock.now();
    if (item.deadlineAt !== undefined && now >= item.deadlineAt) {
      // Expired while waiting: never spend a token on it.
      entry.dead = true;
      this.counters.expired++;
      this.deps.emitter.emit("item:expired", this.itemEvent(item));
      entry.reject(new DeadlineExceededError(this.name));
      return;
    }
    const wallet = this.wallet;
    const route = entry.lane !== undefined ? this.routes.get(entry.lane) : undefined;
    const epoch = wallet.epoch;
    const routeEpoch = route?.wallet.epoch;
    wallet.inFlight++;
    if (route) route.wallet.inFlight++;
    entry.sending = true;
    if (entry.isRetry) this.retriesInFlight++;
    const probe = this.circuit.state === "half-open";
    this.circuit.onSend();
    if (item.attempt === 0) {
      (item as { headers: Record<string, string> }).headers = {
        "idempotency-key": this.keyOf(item),
        "x-sluice-caller": this.deps.caller,
        ...item.headers,
      };
    }
    item.attempt++;
    item.headers["x-sluice-attempt"] = String(item.attempt);
    if (item.attempt === 1) {
      entry.waitMs = now - item.enqueuedAt;
      this.waitHist.observe(entry.waitMs);
    }
    this.counters.sent++;
    this.deps.emitter.emit("item:sent", this.itemEvent(item));
    this.emitTokens();

    const controller = new AbortController();
    const signal = item.signal ? AbortSignal.any([controller.signal, item.signal]) : controller.signal;
    const timeoutMs = this.opts.timeoutMs ?? 30_000;
    let settled = false;
    const finish = (outcome: { result?: unknown; error?: unknown }) => {
      if (settled) return;
      settled = true;
      timer.cancel();
      entry.sending = false;
      // Return the token first: a thrown or hung send never leaks capacity.
      this.returnToken(wallet, epoch);
      if (route) this.returnToken(route.wallet, routeEpoch!);
      if (entry.isRetry) this.retriesInFlight--;
      this.complete(entry, outcome, this.deps.clock.now() - now, probe);
    };
    const timer = this.deps.clock.setTimer(timeoutMs, () => {
      controller.abort(new TimeoutError(this.name, timeoutMs));
      finish({ error: new TimeoutError(this.name, timeoutMs) });
    });
    let promise: Promise<R>;
    try {
      const hooks = this.deps.hooks();
      const run = () => this.opts.send(item as Item<P>, signal);
      promise = hooks
        ? hooks.send(entry.traced, { target: this.name, item, waitMs: entry.waitMs }, run)
        : run();
    } catch (error) {
      promise = Promise.reject(error);
    }
    Promise.resolve(promise).then(
      (result) => finish({ result }),
      (error) => finish({ error }),
    );
  }

  private returnToken(w: Wallet, epoch: number): void {
    if (w.epoch === epoch) w.inFlight--;
    else w.orphans = Math.max(0, w.orphans - 1);
  }

  private complete(
    entry: Entry,
    outcome: { result?: unknown; error?: unknown },
    durationMs: number,
    probe: boolean,
  ): void {
    const item: Item = entry;
    const isError = "error" in outcome;
    const connectionFailure = isError && isConnectionFailure(outcome.error);
    this.circuit.onResult(connectionFailure, probe);
    this.sendHist.observe(durationMs);
    const obs: Observation = {
      latencyMs: durationMs,
      error: connectionFailure,
      overload: !isError && isOverloadSignal(outcome.result),
      result: outcome.result,
    };
    try {
      this.capacitySource.observe?.(obs);
    } catch (error) {
      this.deps.emitter.emit("error", { error, where: "capacity.observe" });
    }

    let verdict: ReturnType<typeof defaultClassify>;
    try {
      verdict = (this.retry.classify ?? defaultClassify)({ ...outcome, item });
    } catch (error) {
      this.deps.emitter.emit("error", { error, where: "retry.classify" });
      verdict = isError ? { kind: "fail" } : { kind: "ok" };
    }
    const isTimeout = isError && outcome.error instanceof TimeoutError;
    const safe = this.opts.retrySafe ? this.opts.retrySafe(item as Item<P>) : true;
    const canRetry =
      verdict.kind === "retry" &&
      item.attempt <= this.retry.max &&
      safe &&
      !(isTimeout && !this.retry.retryOnTimeout) &&
      !item.signal?.aborted &&
      !this.closed;

    if (canRetry) {
      const delay = Math.max(
        (verdict.kind === "retry" && verdict.afterMs) || 0,
        backoffMs(item.attempt, this.retry, this.deps.random),
      );
      this.counters.retried++;
      this.deps.emitter.emit("item:retry", {
        ...this.itemEvent(item),
        delayMs: delay,
        reason: isError
          ? String((outcome.error as Error)?.name ?? "error")
          : `status ${(outcome.result as { status?: number })?.status}`,
      });
      this.releaseResultBody(outcome.result);
      this.scheduleRetry(entry, delay);
    } else if (!isError) {
      // Success, or retries exhausted/unsafe: either way the caller gets the target's real response.
      entry.dead = true;
      this.counters.done++;
      this.deps.emitter.emit("item:done", { ...this.itemEvent(item), durationMs, waitMs: entry.waitMs });
      entry.resolve(outcome.result);
    } else {
      entry.dead = true;
      this.counters.failed++;
      if (verdict.kind === "retry") {
        this.counters.dead++;
        this.deps.emitter.emit("item:dead", { ...this.itemEvent(item), error: outcome.error });
        try {
          this.opts.onDead?.(this.unsent(entry), outcome.error);
        } catch (error) {
          this.deps.emitter.emit("error", { error, where: "onDead" });
        }
      }
      entry.reject(outcome.error);
    }
    this.pump();
  }

  /** Drain an unread fetch body so the connection is reused before retrying. */
  private releaseResultBody(result: unknown): void {
    const body = (result as { body?: { cancel?: () => Promise<void> } | null })?.body;
    if (body && typeof body.cancel === "function") body.cancel().catch(() => {});
  }

  private scheduleRetry(entry: Entry, delayMs: number): void {
    entry.isRetry = true;
    entry.delayed = true;
    this.delayedRetries++;
    const at = this.deps.clock.now() + delayMs;
    if (entry.deadlineAt !== undefined) {
      entry.deadlineTimer = this.deps.timers.schedule(entry.deadlineAt, () => this.expire(entry));
    }
    this.deps.timers.schedule(at, () => {
      if (entry.dead || !entry.delayed) return;
      entry.delayed = false;
      this.delayedRetries--;
      this.retryQueue.push(entry);
      this.pump();
    });
  }

  // ---- token demand and surplus ------------------------------------------------

  private backlog(): number {
    return this.queue.items + this.retryQueue.length;
  }

  private topPriority(): Priority {
    const counts = this.queue.countByPriority();
    if (counts.critical > 0) return "critical";
    if (counts.standard > 0) return "standard";
    return counts.bulk > 0 ? "bulk" : "standard";
  }

  private updateDemand(): void {
    const backlog = this.backlog();
    const hint = { priority: this.topPriority(), backlog };
    this.demandFor(this.wallet, backlog, hint);
    for (const r of this.routes.values()) this.demandFor(r.wallet, this.backlogFor(r.wallet), hint);
  }

  private backlogFor(w: Wallet): number {
    if (w === this.wallet) return this.backlog();
    for (const [name, r] of this.routes) {
      if (r.wallet !== w) continue;
      let n = this.queue.countRoute(name);
      for (const e of this.retryQueue) if (e.lane === name) n++;
      return n;
    }
    return 0;
  }

  private demandFor(w: Wallet, backlog: number, hint: { priority: Priority; backlog: number }): void {
    if (w.fallback || !w.joined) return;
    const want = Math.max(0, Math.min(backlog - w.free(), w.capacity));
    if (want !== w.wanted) {
      w.wanted = want;
      this.deps.coordinator.demand(w.pool, want, hint);
    }
  }

  /** Give back tokens we hold but cannot use, and tokens above our fair share while others wait. */
  private rebalance(): void {
    for (const w of [this.wallet, ...[...this.routes.values()].map((r) => r.wallet)]) {
      if (w.fallback || !w.joined) continue;
      const free = w.free();
      if (free <= 0) continue;
      const backlog = this.backlogFor(w);
      let surplus = Math.max(0, free - backlog);
      if (w.contended && w.held > w.fairShare)
        surplus = Math.max(surplus, Math.min(free, w.held - w.fairShare));
      if (surplus > 0) {
        w.held -= surplus;
        this.deps.coordinator.release(w.pool, surplus);
        this.emitTokens();
      }
    }
  }

  private emitTokens(): void {
    if (this.deps.emitter.has("tokens:changed")) {
      this.deps.emitter.emit("tokens:changed", {
        target: this.name,
        held: this.wallet.fallback ? this.wallet.fallbackLimit : this.wallet.held,
        inFlight: this.wallet.inFlight + this.wallet.orphans,
      });
    }
  }

  /** Load report for leases/fair share; called by the engine on each renew tick. */
  report(): void {
    this.applyDesiredCapacity();
    const backlog = this.backlog();
    const { coordinator } = this.deps;
    coordinator.report(this.pool, { held: this.wallet.held, inFlight: this.wallet.inFlight, backlog });
    for (const r of this.routes.values()) {
      coordinator.report(r.wallet.pool, { held: r.wallet.held, inFlight: r.wallet.inFlight, backlog });
    }
  }

  // ---- drain / close -------------------------------------------------------------

  get idle(): boolean {
    return (
      this.backlog() === 0 &&
      this.delayedRetries === 0 &&
      this.wallet.inFlight + this.wallet.orphans === 0 &&
      this.blocked.length === 0
    );
  }

  private checkDrained(): void {
    if (this.drainWaiters.length > 0 && this.idle) {
      for (const w of this.drainWaiters.splice(0)) w();
    }
  }

  /** Stop intake, let queued work finish until `deadlineAt`, then reject the rest. */
  async drain(timeoutMs: number): Promise<UnsentItem[]> {
    this.closed = true;
    for (let b = this.blocked.shift(); b; b = this.blocked.shift()) b.wake();
    await this.joined;
    if (!this.idle) {
      // A draining process must not exit before the drain finishes, even if nothing else is pending.
      const keepAlive = setInterval(() => {}, 1 << 30);
      await new Promise<void>((resolve) => {
        const timer = this.deps.clock.setTimer(timeoutMs, resolve);
        this.drainWaiters.push(() => {
          timer.cancel();
          resolve();
        });
      }).finally(() => clearInterval(keepAlive));
    }
    const unsent: UnsentItem[] = [];
    const leftovers = [...this.queue.drainAll()];
    for (let e = this.retryQueue.shift(); e; e = this.retryQueue.shift()) leftovers.push(e);
    for (const e of leftovers) {
      e.deadlineTimer?.cancel();
      e.dead = true;
      unsent.push(this.unsent(e));
      e.reject(new ShutdownError());
    }
    return unsent;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.capacitySource.stop();
    this.circuitTimer?.cancel();
    await this.joined;
    await this.deps.coordinator.leave(this.pool);
    for (const r of this.routes.values()) await this.deps.coordinator.leave(r.wallet.pool);
  }

  private unsent(e: Entry): UnsentItem {
    return {
      target: this.name,
      key: this.keyOf(e),
      payload: e.payload,
      attempt: e.attempt,
      priority: e.priority,
    };
  }

  private itemEvent(item: Item): ItemEvent {
    return {
      target: this.name,
      key: this.keyOf(item),
      priority: item.priority,
      route: item.route,
      attempt: item.attempt,
    };
  }

  snapshot(): TargetSnapshot {
    const routes: TargetSnapshot["routes"] = {};
    for (const [name, r] of this.routes) {
      routes[name] = {
        capacity: r.wallet.capacity,
        held: r.wallet.held,
        inFlight: r.wallet.inFlight + r.wallet.orphans,
      };
    }
    return {
      target: this.name,
      pool: this.pool,
      capacity: this.wallet.capacity,
      capacitySource: this.capacitySourceName,
      held: this.wallet.fallback ? this.wallet.fallbackLimit : this.wallet.held,
      inFlight: this.wallet.inFlight + this.wallet.orphans,
      queued: this.queue.items,
      queuedBytes: this.queue.byteSize,
      retryQueued: this.retryQueue.length + this.delayedRetries,
      contended: this.wallet.contended,
      fairShare: this.wallet.fairShare,
      fallback: this.wallet.fallback,
      circuit: this.circuit.state,
      counters: { ...this.counters },
      waitMs: { p50: this.waitHist.quantile(0.5), p99: this.waitHist.quantile(0.99) },
      sendMs: { p50: this.sendHist.quantile(0.5), p99: this.sendHist.quantile(0.99) },
      routes,
    };
  }
}
