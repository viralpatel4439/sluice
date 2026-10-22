import { context, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { afterAll, describe, expect, it } from "vitest";
import { createEngine } from "../src/core/engine.js";
import { consoleLogger } from "../src/core/logger.js";
import { instrumentOtel, traceOtel } from "../src/otel.js";

const ctxManager = new AsyncLocalStorageContextManager().enable();
context.setGlobalContextManager(ctxManager);
afterAll(() => ctxManager.disable());

describe("OpenTelemetry", () => {
  it("records wait and send spans in the caller's trace", async () => {
    const exporter = new InMemorySpanExporter();
    const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
    const tracer = provider.getTracer("test");
    const engine = createEngine({ caller: "orders", logger: consoleLogger("silent") });
    await traceOtel(engine, { tracer });
    let childParent: string | undefined;
    const t = engine.target("inventory", {
      capacity: 1,
      send: async () => {
        // Work done inside send (e.g. the HTTP client span) sees sluice.send as its parent.
        childParent = trace.getActiveSpan()?.spanContext().spanId;
        await new Promise((r) => setTimeout(r, 5));
        return { status: 200 };
      },
    });
    const root = tracer.startSpan("handle-job");
    await context.with(trace.setSpan(context.active(), root), () => Promise.all([t.submit(1), t.submit(2)]));
    root.end();
    const spans = exporter.getFinishedSpans();
    const sends = spans.filter((s) => s.name === "sluice.send");
    const waits = spans.filter((s) => s.name === "sluice.wait");
    expect(sends).toHaveLength(2);
    expect(waits).toHaveLength(2);
    for (const s of [...sends, ...waits]) {
      expect(s.spanContext().traceId).toBe(root.spanContext().traceId);
      const parent = (s as unknown as { parentSpanContext?: { spanId: string } }).parentSpanContext?.spanId;
      expect(parent).toBe(root.spanContext().spanId);
      expect(s.attributes["sluice.target"]).toBe("inventory");
    }
    expect(sends.map((s) => s.spanContext().spanId)).toContain(childParent);
    expect(sends[0]!.attributes["http.response.status_code"]).toBe(200);
    await engine.close();
  });

  it("exports gauges, counters and histograms", async () => {
    const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
    const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 60_000 });
    const provider = new MeterProvider({ readers: [reader] });
    const engine = createEngine({ caller: "orders", logger: consoleLogger("silent") });
    await instrumentOtel(engine, { meter: provider.getMeter("test") });
    const t = engine.target("inventory", { capacity: 3, send: async () => ({ status: 200 }) });
    await Promise.all([1, 2, 3, 4].map((i) => t.submit(i)));
    await reader.forceFlush();
    const names = exporter
      .getMetrics()
      .flatMap((rm) => rm.scopeMetrics.flatMap((sm) => sm.metrics.map((m) => m.descriptor.name)));
    for (const n of [
      "sluice.capacity",
      "sluice.inflight",
      "sluice.items",
      "sluice.queue.wait",
      "sluice.send.duration",
    ]) {
      expect(names).toContain(n);
    }
    await provider.shutdown();
    await engine.close();
  });
});
