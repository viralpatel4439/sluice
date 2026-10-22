import type { Clock, TimerHandle } from "./clock.js";
import type { Logger } from "./logger.js";

export interface CapacityContext {
  clock: Clock;
  logger: Logger;
  target: string;
  /** Publish a new capacity value. */
  set(n: number, source: string): void;
  /** Current in-flight count at this instance (adaptive uses it to detect saturation). */
  inFlight(): number;
  /** Current capacity in effect. */
  current(): number;
}

export interface Observation {
  latencyMs: number;
  /** Connection error or timeout. */
  error: boolean;
  /** 429/503 or equivalent. */
  overload: boolean;
  /** Raw result, so header-reading sources can inspect it. */
  result?: unknown;
}

export interface CapacitySource {
  readonly name: string;
  /** Value to start with before the source has produced anything. */
  readonly initial: number;
  start(ctx: CapacityContext): void;
  observe?(o: Observation): void;
  stop(): void;
}

export type CapacityConfig =
  | { type: "static"; limit: number }
  | { type: "callback"; fn: () => number | Promise<number>; intervalMs?: number; fallback?: number }
  | {
      type: "http";
      url?: string;
      /** Fetch each replica and sum maxConcurrency. */
      replicas?: () => string[] | Promise<string[]>;
      intervalMs?: number;
      fallback?: number;
      staleMs?: number;
    }
  | {
      type: "adaptive";
      start?: number;
      min?: number;
      max?: number;
      /** Latency SLO. When omitted, 3x the best observed p50 is used. */
      latencyMs?: number;
      tickMs?: number;
    }
  | { type: "auto"; start?: number; url?: string; min?: number; max?: number; latencyMs?: number };

export interface CapacityDoc {
  maxConcurrency: number;
  inFlight?: number;
  health?: "ok" | "degraded" | "down";
}

/** fetch that is never gated by sluice's own interceptor. Bound at module load, before any install. */
export const rawFetch: typeof fetch = globalThis.fetch.bind(globalThis);

export function createCapacitySource(cfg: CapacityConfig | number | undefined): CapacitySource {
  if (cfg === undefined) return new AutoCapacity({ type: "auto" });
  if (typeof cfg === "number") return new StaticCapacity(cfg);
  switch (cfg.type) {
    case "static":
      return new StaticCapacity(cfg.limit);
    case "callback":
      return new CallbackCapacity(cfg);
    case "http":
      return new HttpCapacity(cfg);
    case "adaptive":
      return new AdaptiveCapacity(cfg);
    case "auto":
      return new AutoCapacity(cfg);
  }
}

class StaticCapacity implements CapacitySource {
  readonly name = "static";
  constructor(readonly initial: number) {}
  start(ctx: CapacityContext): void {
    ctx.set(this.initial, this.name);
  }
  stop(): void {}
}

/** Polls a function. Keeps the last good value; after repeated failures uses `fallback`. */
class CallbackCapacity implements CapacitySource {
  readonly name = "callback";
  readonly initial: number;
  private timer: TimerHandle | undefined;
  private stopped = false;
  constructor(private readonly cfg: Extract<CapacityConfig, { type: "callback" }>) {
    this.initial = cfg.fallback ?? 10;
  }
  start(ctx: CapacityContext): void {
    const tick = async () => {
      try {
        const n = await this.cfg.fn();
        if (Number.isFinite(n) && n >= 0) ctx.set(Math.floor(n), this.name);
      } catch (error) {
        ctx.logger.warn({ target: ctx.target, err: error }, "capacity callback failed");
      }
      if (!this.stopped) this.timer = ctx.clock.setTimer(this.cfg.intervalMs ?? 5_000, tick);
    };
    void tick();
  }
  stop(): void {
    this.stopped = true;
    this.timer?.cancel();
  }
}

export async function fetchCapacityDoc(url: string, timeoutMs = 2_000): Promise<CapacityDoc> {
  const res = await rawFetch(url, {
    signal: AbortSignal.timeout(timeoutMs),
    headers: { accept: "application/json" },
  });
  if (!res.ok) throw new Error(`capacity endpoint ${url} returned ${res.status}`);
  const body = (await res.json()) as Partial<CapacityDoc>;
  if (typeof body.maxConcurrency !== "number" || body.maxConcurrency < 0) {
    throw new Error(`capacity endpoint ${url} returned no maxConcurrency`);
  }
  return { maxConcurrency: body.maxConcurrency, inFlight: body.inFlight, health: body.health ?? "ok" };
}

/** Reads `GET /_capacity` → {maxConcurrency, inFlight, health}, aggregate or summed over replicas. */
class HttpCapacity implements CapacitySource {
  readonly name = "http";
  readonly initial: number;
  private timer: TimerHandle | undefined;
  private stopped = false;
  private lastGoodAt = 0;
  constructor(private readonly cfg: Extract<CapacityConfig, { type: "http" }>) {
    this.initial = cfg.fallback ?? 10;
    if (!cfg.url && !cfg.replicas) throw new Error("http capacity needs url or replicas");
  }
  start(ctx: CapacityContext): void {
    this.lastGoodAt = ctx.clock.now();
    const tick = async () => {
      try {
        let total = 0;
        if (this.cfg.replicas) {
          const urls = await this.cfg.replicas();
          const docs = await Promise.allSettled(urls.map((u) => fetchCapacityDoc(u)));
          for (const d of docs)
            if (d.status === "fulfilled" && d.value.health !== "down") total += d.value.maxConcurrency;
          if (docs.every((d) => d.status === "rejected")) throw new Error("no replica answered");
        } else {
          const doc = await fetchCapacityDoc(this.cfg.url!);
          total = doc.health === "down" ? 0 : doc.maxConcurrency;
        }
        this.lastGoodAt = ctx.clock.now();
        ctx.set(total, this.name);
      } catch (error) {
        const stale = ctx.clock.now() - this.lastGoodAt > (this.cfg.staleMs ?? 5_000);
        if (stale && this.cfg.fallback !== undefined) ctx.set(this.cfg.fallback, `${this.name}:fallback`);
        ctx.logger.warn({ target: ctx.target, err: error, stale }, "capacity endpoint unreachable");
      }
      if (!this.stopped) this.timer = ctx.clock.setTimer(this.cfg.intervalMs ?? 2_000, tick);
    };
    void tick();
  }
  stop(): void {
    this.stopped = true;
    this.timer?.cancel();
  }
}

/**
 * AIMD on latency and errors. Each tick: if any overload signal, error rate
 * above 10% or p95 latency above the SLO → cut 30%. Otherwise, if the limit
 * was actually reached during the tick → add 5% (at least 1).
 */
export class AdaptiveCapacity implements CapacitySource {
  readonly name = "adaptive";
  readonly initial: number;
  private readonly min: number;
  private readonly max: number;
  private limit: number;
  private timer: TimerHandle | undefined;
  private stopped = false;
  private samples: number[] = [];
  private errors = 0;
  private overloads = 0;
  private saturated = false;
  private bestP50 = Number.POSITIVE_INFINITY;
  private ctx: CapacityContext | undefined;

  constructor(
    private readonly cfg:
      | Extract<CapacityConfig, { type: "adaptive" }>
      | { start?: number; min?: number; max?: number; latencyMs?: number; tickMs?: number },
  ) {
    this.initial = cfg.start ?? 10;
    this.min = cfg.min ?? 1;
    this.max = cfg.max ?? 10_000;
    this.limit = this.initial;
  }

  start(ctx: CapacityContext): void {
    this.ctx = ctx;
    this.limit = Math.max(this.min, Math.min(this.max, ctx.current() || this.initial));
    ctx.set(this.limit, this.name);
    const tick = () => {
      this.evaluate();
      if (!this.stopped) this.timer = ctx.clock.setTimer(this.cfg.tickMs ?? 1_000, tick);
    };
    this.timer = ctx.clock.setTimer(this.cfg.tickMs ?? 1_000, tick);
  }

  observe(o: Observation): void {
    this.samples.push(o.latencyMs);
    if (o.error) this.errors++;
    if (o.overload) this.overloads++;
    if (this.ctx && this.ctx.inFlight() + 1 >= this.ctx.current()) this.saturated = true;
  }

  /** Adopt a value set from elsewhere (e.g. fleet-wide capacity) as the new base. */
  sync(n: number): void {
    this.limit = n;
  }

  private evaluate(): void {
    const ctx = this.ctx!;
    const n = this.samples.length;
    if (n === 0) {
      this.saturated = false;
      return;
    }
    const sorted = [...this.samples].sort((a, b) => a - b);
    const p50 = sorted[Math.floor(n * 0.5)]!;
    const p95 = sorted[Math.min(n - 1, Math.floor(n * 0.95))]!;
    if (n >= 5) this.bestP50 = Math.min(this.bestP50, p50);
    const slo =
      this.cfg.latencyMs ??
      (Number.isFinite(this.bestP50) ? Math.max(this.bestP50 * 3, 50) : Number.POSITIVE_INFINITY);
    const errorRate = this.errors / n;
    let next = this.limit;
    if (this.overloads > 0 || errorRate > 0.1 || p95 > slo) {
      next = Math.max(this.min, Math.floor(this.limit * 0.7));
    } else if (this.saturated) {
      next = Math.min(this.max, this.limit + Math.max(1, Math.floor(this.limit * 0.05)));
    }
    this.samples = [];
    this.errors = 0;
    this.overloads = 0;
    this.saturated = false;
    if (next !== this.limit) {
      this.limit = next;
      ctx.set(next, this.name);
    }
  }

  stop(): void {
    this.stopped = true;
    this.timer?.cancel();
  }
}

/** Reads capacity advertised in response headers, if the target sends any. */
export function capacityFromHeaders(result: unknown): number | undefined {
  const headers = (result as { headers?: { get?: (n: string) => string | null } })?.headers;
  if (!headers || typeof headers.get !== "function") return undefined;
  const sluice = headers.get("x-sluice-capacity");
  if (sluice && /^\d+$/.test(sluice.trim())) return Number(sluice);
  // IETF RateLimit draft: "RateLimit-Limit" (older) or "RateLimit-Policy: 100;w=1" style concurrency hints.
  const limit = headers.get("ratelimit-limit");
  if (limit) {
    const first = Number.parseInt(limit.split(/[,;]/)[0]!.trim(), 10);
    if (Number.isFinite(first) && first > 0) return first;
  }
  return undefined;
}

/**
 * Zero-config capacity: response headers win; otherwise probe `/_capacity`
 * once and keep polling it if it answers; otherwise adaptive AIMD.
 */
class AutoCapacity implements CapacitySource {
  name = "auto";
  readonly initial: number;
  private adaptive: AdaptiveCapacity;
  private http: HttpCapacity | undefined;
  private mode: "adaptive" | "http" | "headers" = "adaptive";
  private ctx: CapacityContext | undefined;
  private lastHeaderValue: number | undefined;

  constructor(private readonly cfg: Extract<CapacityConfig, { type: "auto" }>) {
    this.initial = cfg.start ?? 10;
    this.adaptive = new AdaptiveCapacity({
      start: this.initial,
      min: cfg.min,
      max: cfg.max,
      latencyMs: cfg.latencyMs,
    });
  }

  start(ctx: CapacityContext): void {
    this.ctx = ctx;
    this.adaptive.start(ctx);
    if (this.cfg.url) {
      const url = this.cfg.url;
      fetchCapacityDoc(url)
        .then(() => {
          if (this.mode === "headers") return;
          this.adaptive.stop();
          this.mode = "http";
          this.name = "auto:http";
          this.http = new HttpCapacity({ type: "http", url, fallback: ctx.current() });
          this.http.start(ctx);
        })
        .catch(() => ctx.logger.debug({ target: ctx.target, url }, "no capacity endpoint; using adaptive"));
    }
  }

  observe(o: Observation): void {
    const fromHeaders = capacityFromHeaders(o.result);
    if (fromHeaders !== undefined) {
      if (this.mode !== "headers") {
        this.adaptive.stop();
        this.http?.stop();
        this.mode = "headers";
        this.name = "auto:headers";
      }
      if (fromHeaders !== this.lastHeaderValue) {
        this.lastHeaderValue = fromHeaders;
        this.ctx?.set(fromHeaders, this.name);
      }
      return;
    }
    if (this.mode === "adaptive") this.adaptive.observe(o);
  }

  stop(): void {
    this.adaptive.stop();
    this.http?.stop();
  }
}
