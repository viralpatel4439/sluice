import type { Clock } from "./clock.js";

export interface CircuitOptions {
  /** Consecutive connection failures that open the circuit. */
  threshold: number;
  /** How long the circuit stays open before probing. */
  openMs: number;
  /** Probe requests allowed while half-open. */
  probes: number;
}

export const DEFAULT_CIRCUIT: CircuitOptions = { threshold: 5, openMs: 5_000, probes: 2 };

export type CircuitState = "closed" | "open" | "half-open";

/** Opens on consecutive connection errors (not on HTTP status codes), then probes. */
export class Circuit {
  state: CircuitState = "closed";
  private failures = 0;
  private openedAt = 0;
  private probesInFlight = 0;

  constructor(
    private readonly opts: CircuitOptions,
    private readonly clock: Clock,
    private readonly onChange: (state: CircuitState) => void,
  ) {}

  /** May one more request be sent right now? Call `onSend` if it is. */
  canSend(): boolean {
    if (this.state === "closed") return true;
    if (this.state === "open") {
      if (this.clock.now() - this.openedAt < this.opts.openMs) return false;
      this.set("half-open");
    }
    return this.probesInFlight < this.opts.probes;
  }

  /** Milliseconds until the circuit may allow a probe (0 when it already can). */
  retryInMs(): number {
    return this.state === "open" ? Math.max(0, this.opts.openMs - (this.clock.now() - this.openedAt)) : 0;
  }

  onSend(): void {
    if (this.state === "half-open") this.probesInFlight++;
  }

  onResult(connectionFailure: boolean, wasProbe: boolean): void {
    if (wasProbe) this.probesInFlight = Math.max(0, this.probesInFlight - 1);
    if (!connectionFailure) {
      this.failures = 0;
      if (this.state !== "closed") this.set("closed");
      return;
    }
    this.failures++;
    if (this.state === "half-open" || this.failures >= this.opts.threshold) {
      this.openedAt = this.clock.now();
      if (this.state !== "open") this.set("open");
    }
  }

  private set(state: CircuitState): void {
    this.state = state;
    if (state !== "half-open") this.probesInFlight = 0;
    this.onChange(state);
  }
}
