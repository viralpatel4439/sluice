import type { Engine } from "./core/engine.js";

type Api = typeof import("@opentelemetry/api");

/**
 * Export engine metrics through OpenTelemetry. Uses the global MeterProvider
 * unless a meter is given. Names match the Prometheus exporter.
 */
export async function instrumentOtel(
  engine: Engine,
  opts: { meter?: import("@opentelemetry/api").Meter } = {},
): Promise<() => void> {
  let api: Api;
  try {
    api = await import("@opentelemetry/api");
  } catch {
    throw new Error('sluice/otel needs "@opentelemetry/api": npm i @opentelemetry/api');
  }
  const meter = opts.meter ?? api.metrics.getMeter("sluice");
  const attrs = (target: string) => ({
    "sluice.namespace": engine.namespace,
    "sluice.caller": engine.caller,
    "sluice.target": target,
  });
  const gauges = {
    capacity: meter.createObservableGauge("sluice.capacity", {
      description: "Capacity N of the target (fleet-wide).",
    }),
    held: meter.createObservableGauge("sluice.tokens.held", { description: "Tokens held by this instance." }),
    inflight: meter.createObservableGauge("sluice.inflight", {
      description: "Requests in flight from this instance.",
    }),
    queued: meter.createObservableGauge("sluice.queue.items", {
      description: "Items waiting in the local queue.",
    }),
    bytes: meter.createObservableGauge("sluice.queue.bytes", {
      description: "Bytes waiting in the local queue.",
      unit: "By",
    }),
    fallback: meter.createObservableGauge("sluice.fallback.active", {
      description: "1 while a local fallback limit applies.",
    }),
    circuit: meter.createObservableGauge("sluice.circuit.open", {
      description: "1 while the circuit is open.",
    }),
  };
  const observe = (r: import("@opentelemetry/api").BatchObservableResult) => {
    for (const s of engine.snapshot()) {
      const a = attrs(s.target);
      r.observe(gauges.capacity, s.capacity, a);
      r.observe(gauges.held, s.held, a);
      r.observe(gauges.inflight, s.inFlight, a);
      r.observe(gauges.queued, s.queued + s.retryQueued, a);
      r.observe(gauges.bytes, s.queuedBytes, a);
      r.observe(gauges.fallback, s.fallback ? 1 : 0, a);
      r.observe(gauges.circuit, s.circuit === "closed" ? 0 : 1, a);
    }
  };
  meter.addBatchObservableCallback(observe, Object.values(gauges));
  const items = meter.createCounter("sluice.items", { description: "Items by outcome." });
  const wait = meter.createHistogram("sluice.queue.wait", {
    description: "Time items waited for a token.",
    unit: "ms",
  });
  const duration = meter.createHistogram("sluice.send.duration", {
    description: "Time from send to response.",
    unit: "ms",
  });
  const offs = [
    engine.on("item:done", (e) => {
      items.add(1, { ...attrs(e.target), "sluice.outcome": "done" });
      wait.record(e.waitMs, attrs(e.target));
      duration.record(e.durationMs, attrs(e.target));
    }),
    engine.on("item:retry", (e) => items.add(1, { ...attrs(e.target), "sluice.outcome": "retry" })),
    engine.on("item:expired", (e) => items.add(1, { ...attrs(e.target), "sluice.outcome": "expired" })),
    engine.on("item:dead", (e) => items.add(1, { ...attrs(e.target), "sluice.outcome": "dead" })),
    engine.on("item:dropped", (e) => items.add(1, { ...attrs(e.target), "sluice.outcome": "dropped" })),
  ];
  return () => {
    meter.removeBatchObservableCallback(observe, Object.values(gauges));
    for (const off of offs) off();
  };
}

/**
 * Trace sluice through OpenTelemetry. For each call it records `sluice.wait`
 * (time queued for a token) and one `sluice.send` span per attempt, created in
 * the caller's context. HTTP client spans from your HTTP instrumentation become
 * children of `sluice.send`, so traceparent flows from caller to target as usual.
 */
export async function traceOtel(
  engine: Engine,
  opts: { tracer?: import("@opentelemetry/api").Tracer } = {},
): Promise<() => void> {
  let api: Api;
  try {
    api = await import("@opentelemetry/api");
  } catch {
    throw new Error('sluice/otel needs "@opentelemetry/api": npm i @opentelemetry/api');
  }
  const tracer = opts.tracer ?? api.trace.getTracer("sluice");
  engine.setTracingHooks({
    capture: () => ({ ctx: api.context.active(), at: Date.now() }),
    send: (captured, info, run) => {
      const { ctx, at } = (captured as
        | { ctx: import("@opentelemetry/api").Context; at: number }
        | undefined) ?? {
        ctx: api.context.active(),
        at: Date.now(),
      };
      const attributes = {
        "sluice.namespace": engine.namespace,
        "sluice.caller": engine.caller,
        "sluice.target": info.target,
        "sluice.priority": info.item.priority,
        "sluice.attempt": info.item.attempt,
        "sluice.key": info.item.key,
      };
      if (info.item.attempt === 1) {
        tracer
          .startSpan(
            "sluice.wait",
            { startTime: at, attributes: { ...attributes, "sluice.wait_ms": info.waitMs } },
            ctx,
          )
          .end();
      }
      const span = tracer.startSpan("sluice.send", { kind: api.SpanKind.INTERNAL, attributes }, ctx);
      return api.context.with(api.trace.setSpan(ctx, span), () =>
        run().then(
          (result) => {
            const status = (result as { status?: number } | undefined)?.status;
            if (typeof status === "number") span.setAttribute("http.response.status_code", status);
            if (typeof status === "number" && status >= 500)
              span.setStatus({ code: api.SpanStatusCode.ERROR });
            span.end();
            return result;
          },
          (error: unknown) => {
            span.recordException(error instanceof Error ? error : String(error));
            span.setStatus({
              code: api.SpanStatusCode.ERROR,
              message: error instanceof Error ? error.message : String(error),
            });
            span.end();
            throw error;
          },
        ),
      );
    },
  });
  return () => engine.setTracingHooks(undefined);
}
