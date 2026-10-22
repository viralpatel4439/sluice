export type Priority = "critical" | "standard" | "bulk";

export const PRIORITIES: readonly Priority[] = ["critical", "standard", "bulk"];

export type OverflowPolicy = "block" | "reject" | "drop-lowest";

/** One unit of work waiting for capacity at a target. */
export interface Item<P = unknown> {
  readonly id: number;
  readonly key: string;
  readonly payload: P;
  readonly priority: Priority;
  readonly route: string | undefined;
  readonly size: number;
  readonly enqueuedAt: number;
  readonly deadlineAt: number | undefined;
  attempt: number;
  /** Extra headers the engine wants attached to the outgoing request. */
  readonly headers: Record<string, string>;
  /** The caller's own abort signal, if any. */
  readonly signal: AbortSignal | undefined;
}

export type SendFn<P = unknown, R = unknown> = (item: Item<P>, signal: AbortSignal) => Promise<R>;

export type Verdict = { kind: "ok" } | { kind: "retry"; afterMs?: number } | { kind: "fail" };

export interface RetryOptions {
  /** Maximum attempts after the first one. */
  max: number;
  baseMs: number;
  maxMs: number;
  /** Max share of held tokens that retries may occupy at once. */
  budget: number;
  retryOnTimeout: boolean;
  classify?: (outcome: { result?: unknown; error?: unknown; item: Item }) => Verdict;
}

export interface QueueOptions {
  maxItems: number;
  maxBytes: number;
  overflow: OverflowPolicy;
}

export interface RouteOptions {
  /** Hard cap as a share of target capacity (0..1]. */
  maxShare?: number;
  /** Hard cap as an absolute number of in-flight requests. */
  max?: number;
}

export interface CallerPolicy {
  weight?: number;
  minShare?: number;
  maxShare?: number;
}

export interface SubmitOptions {
  key?: string;
  priority?: Priority;
  route?: string;
  deadlineMs?: number;
  headers?: Record<string, string>;
  size?: number;
  /** Cancels the item while it is still waiting; also forwarded to `send` once dispatched. */
  signal?: AbortSignal;
}

export interface UnsentItem {
  target: string;
  key: string;
  payload: unknown;
  attempt: number;
  priority: Priority;
}
