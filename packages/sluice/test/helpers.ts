import type { ManualClock } from "../src/core/clock.js";
import { type EngineOptions, createEngine } from "../src/core/engine.js";
import { consoleLogger } from "../src/core/logger.js";

export interface MockResponse {
  status: number;
  headers: { get(name: string): string | null };
  body: null;
}

export function response(status = 200, headers: Record<string, string> = {}): MockResponse {
  const lower = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]));
  return { status, headers: { get: (n) => lower[n.toLowerCase()] ?? null }, body: null };
}

/** A fake downstream service that records its peak concurrency. */
export class MockService {
  inFlight = 0;
  peak = 0;
  calls = 0;
  constructor(
    private readonly clock: ManualClock,
    public latencyMs: number | (() => number) = 10,
    public behave: (call: number) => "ok" | "503" | "429" | "error" | "hang" = () => "ok",
  ) {}

  send = async (): Promise<MockResponse> => {
    const call = ++this.calls;
    this.inFlight++;
    this.peak = Math.max(this.peak, this.inFlight);
    const kind = this.behave(call);
    try {
      if (kind === "hang") await new Promise(() => {});
      const ms = typeof this.latencyMs === "function" ? this.latencyMs() : this.latencyMs;
      await new Promise<void>((r) => this.clock.setTimer(ms, r));
      if (kind === "error")
        throw Object.assign(new TypeError("fetch failed"), { cause: { code: "ECONNRESET" } });
      if (kind === "503") return response(503);
      if (kind === "429") return response(429, { "retry-after": "1" });
      return response(200);
    } finally {
      this.inFlight--;
    }
  };
}

export function testEngine(clock: ManualClock, opts: Partial<EngineOptions> = {}) {
  return createEngine({ clock, logger: consoleLogger("silent"), random: () => 0.5, caller: "test", ...opts });
}

export async function settle(clock: ManualClock, ms: number, step = 5): Promise<void> {
  for (let t = 0; t < ms; t += step) await clock.advance(step);
}
