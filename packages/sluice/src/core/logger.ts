export type LogLevel = "debug" | "info" | "warn" | "error" | "silent";

export interface Logger {
  debug(obj: Record<string, unknown>, msg: string): void;
  info(obj: Record<string, unknown>, msg: string): void;
  warn(obj: Record<string, unknown>, msg: string): void;
  error(obj: Record<string, unknown>, msg: string): void;
  isLevelEnabled?(level: LogLevel): boolean;
}

const RANK: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

/** JSON-lines logger to stderr. Quiet by default (warn). */
export function consoleLogger(level: LogLevel = "warn", base: Record<string, unknown> = {}): Logger {
  const min = RANK[level];
  const write = (lvl: Exclude<LogLevel, "silent">) => (obj: Record<string, unknown>, msg: string) => {
    if (RANK[lvl] < min) return;
    const line = JSON.stringify(
      { level: lvl, time: new Date().toISOString(), lib: "sluice", ...base, ...obj, msg },
      replacer,
    );
    process.stderr.write(`${line}\n`);
  };
  return {
    debug: write("debug"),
    info: write("info"),
    warn: write("warn"),
    error: write("error"),
    isLevelEnabled: (l) => RANK[l] >= min,
  };
}

function replacer(_key: string, value: unknown): unknown {
  if (value instanceof Error)
    return { name: value.name, message: value.message, code: (value as { code?: unknown }).code };
  return value;
}

/** Adapts pino, winston or any logger with (obj, msg) or (msg, obj) methods. */
export function adaptLogger(
  target: {
    debug: (...a: unknown[]) => void;
    info: (...a: unknown[]) => void;
    warn: (...a: unknown[]) => void;
    error: (...a: unknown[]) => void;
  },
  style: "pino" | "winston" = "pino",
): Logger {
  const call = (fn: (...a: unknown[]) => void) => (obj: Record<string, unknown>, msg: string) =>
    style === "pino"
      ? fn.call(target, { lib: "sluice", ...obj }, msg)
      : fn.call(target, msg, { lib: "sluice", ...obj });
  return {
    debug: call(target.debug),
    info: call(target.info),
    warn: call(target.warn),
    error: call(target.error),
  };
}

/**
 * Wraps a logger so repeated warnings with the same key are collapsed:
 * the first is logged, the rest are counted and summarised once per window.
 */
export class RateLimitedLog {
  private seen = new Map<string, { at: number; suppressed: number }>();

  constructor(
    private readonly logger: Logger,
    private readonly windowMs = 10_000,
    private readonly now: () => number = () => performance.now(),
  ) {}

  warn(key: string, obj: Record<string, unknown>, msg: string): void {
    const t = this.now();
    const entry = this.seen.get(key);
    if (entry && t - entry.at < this.windowMs) {
      entry.suppressed++;
      return;
    }
    const suppressed = entry?.suppressed ?? 0;
    this.seen.set(key, { at: t, suppressed: 0 });
    if (this.seen.size > 1000) this.seen.delete(this.seen.keys().next().value!);
    this.logger.warn(suppressed > 0 ? { ...obj, suppressed } : obj, msg);
  }
}
