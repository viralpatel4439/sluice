import type { Item, RetryOptions, Verdict } from "./types.js";

export const DEFAULT_RETRY: RetryOptions = {
  max: 5,
  baseMs: 200,
  maxMs: 30_000,
  budget: 0.1,
  retryOnTimeout: true,
};

const RETRYABLE_STATUS = new Set([429, 502, 503, 504]);
const IDEMPOTENT_METHODS = new Set(["GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"]);

interface ResponseLike {
  status: number;
  headers: { get(name: string): string | null };
}

function isResponseLike(value: unknown): value is ResponseLike {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as ResponseLike).status === "number" &&
    typeof (value as ResponseLike).headers?.get === "function"
  );
}

/** Parse Retry-After (delta-seconds or HTTP date) into milliseconds. */
export function parseRetryAfter(value: string | null | undefined, nowMs = Date.now()): number | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  if (/^\d+(\.\d+)?$/.test(trimmed)) return Math.round(Number(trimmed) * 1000);
  // Only real HTTP dates (IMF-fixdate, RFC 850, asctime) — Date.parse alone accepts junk like "1.".
  if (!/^[A-Za-z]{3},? .*\d{1,2}:\d{2}:\d{2}/.test(trimmed)) return undefined;
  const date = Date.parse(trimmed);
  if (Number.isNaN(date)) return undefined;
  return Math.max(0, date - nowMs);
}

/** True when it is safe to send the same request again. */
export function isRetrySafe(
  method: string | undefined,
  headers: Record<string, string> | undefined,
): boolean {
  if (!method || IDEMPOTENT_METHODS.has(method.toUpperCase())) return true;
  if (!headers) return false;
  return Object.keys(headers).some((h) => h.toLowerCase() === "idempotency-key");
}

/**
 * Default classifier: a Response-like result with 429/502/503/504 is retried
 * (honouring Retry-After); any other result is ok. Thrown errors are retried.
 */
export function defaultClassify(outcome: { result?: unknown; error?: unknown; item: Item }): Verdict {
  if ("error" in outcome && outcome.error !== undefined) return { kind: "retry" };
  const result = outcome.result;
  if (isResponseLike(result)) {
    if (RETRYABLE_STATUS.has(result.status)) {
      const afterMs = parseRetryAfter(result.headers.get("retry-after"));
      return afterMs === undefined ? { kind: "retry" } : { kind: "retry", afterMs };
    }
  }
  return { kind: "ok" };
}

/** Full-jitter exponential backoff. */
export function backoffMs(attempt: number, opts: RetryOptions, random: () => number = Math.random): number {
  const ceiling = Math.min(opts.maxMs, opts.baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.floor(random() * ceiling);
}

/** How many retries may be in flight at once for a given number of held tokens. */
export function retryBudget(held: number, budget: number): number {
  return Math.max(1, Math.floor(held * budget));
}

export function isOverloadSignal(result: unknown): boolean {
  return isResponseLike(result) && (result.status === 429 || result.status === 503);
}
