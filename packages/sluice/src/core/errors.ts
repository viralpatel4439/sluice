export type SluiceErrorCode =
  | "ERR_QUEUE_FULL"
  | "ERR_DROPPED"
  | "ERR_DEADLINE"
  | "ERR_SHUTDOWN"
  | "ERR_RETRIES_EXHAUSTED"
  | "ERR_CONFIG"
  | "ERR_TIMEOUT"
  | "ERR_CIRCUIT_OPEN"
  | "ERR_TOO_MANY_TARGETS";

export class SluiceError extends Error {
  constructor(
    readonly code: SluiceErrorCode,
    message: string,
    options?: { cause?: unknown },
  ) {
    super(message, options);
    this.name = "SluiceError";
  }
}

export class QueueFullError extends SluiceError {
  constructor(target: string) {
    super("ERR_QUEUE_FULL", `sluice: queue for target "${target}" is full`);
    this.name = "QueueFullError";
  }
}

export class DroppedError extends SluiceError {
  constructor(target: string) {
    super("ERR_DROPPED", `sluice: item dropped from "${target}" to make room for higher priority work`);
    this.name = "DroppedError";
  }
}

/** Raised when an item's deadline passes before it is sent. Named AbortError so fetch callers handle it as usual. */
export class DeadlineExceededError extends SluiceError {
  constructor(target: string) {
    super("ERR_DEADLINE", `sluice: deadline passed while waiting for "${target}"`);
    this.name = "AbortError";
  }
}

export class ShutdownError extends SluiceError {
  constructor() {
    super("ERR_SHUTDOWN", "sluice: engine is draining or closed");
    this.name = "ShutdownError";
  }
}

export class TimeoutError extends SluiceError {
  constructor(target: string, ms: number) {
    super("ERR_TIMEOUT", `sluice: request to "${target}" timed out after ${ms}ms`);
    this.name = "TimeoutError";
  }
}

export class ConfigError extends SluiceError {
  constructor(message: string) {
    super("ERR_CONFIG", `sluice: invalid config: ${message}`);
    this.name = "ConfigError";
  }
}
