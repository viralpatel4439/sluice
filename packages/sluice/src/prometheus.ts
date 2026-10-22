import { type Server, createServer } from "node:http";
import type { Engine } from "./core/engine.js";
import { Histogram } from "./core/stats.js";

function esc(v: string): string {
  return v.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");
}

/**
 * Prometheus text for one engine. Labels: namespace, caller, target
 * (instance is left to Prometheus' own `instance` label to keep cardinality low).
 */
export function renderPrometheus(engine: Engine): string {
  const out: string[] = [];
  const snaps = engine.snapshot();
  const base = (target: string) =>
    `namespace="${esc(engine.namespace)}",caller="${esc(engine.caller)}",target="${esc(target)}"`;
  const metric = (name: string, type: "gauge" | "counter", help: string, rows: [string, number][]) => {
    out.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`);
    for (const [labels, v] of rows) out.push(`${name}{${labels}} ${v}`);
  };
  metric(
    "sluice_capacity",
    "gauge",
    "Capacity N of the target (fleet-wide).",
    snaps.map((s) => [base(s.target), s.capacity]),
  );
  metric(
    "sluice_tokens_held",
    "gauge",
    "Tokens held by this instance.",
    snaps.map((s) => [base(s.target), s.held]),
  );
  metric(
    "sluice_inflight",
    "gauge",
    "Requests in flight from this instance.",
    snaps.map((s) => [base(s.target), s.inFlight]),
  );
  metric(
    "sluice_queue_items",
    "gauge",
    "Items waiting in the local queue.",
    snaps.map((s) => [base(s.target), s.queued + s.retryQueued]),
  );
  metric(
    "sluice_queue_bytes",
    "gauge",
    "Bytes waiting in the local queue.",
    snaps.map((s) => [base(s.target), s.queuedBytes]),
  );
  metric(
    "sluice_fallback_active",
    "gauge",
    "1 while coordination is lost and a local limit applies.",
    snaps.map((s) => [base(s.target), s.fallback ? 1 : 0]),
  );
  metric(
    "sluice_circuit_open",
    "gauge",
    "1 while the circuit to the target is open.",
    snaps.map((s) => [base(s.target), s.circuit === "closed" ? 0 : 1]),
  );
  const counters = [
    "submitted",
    "sent",
    "done",
    "failed",
    "retried",
    "expired",
    "dead",
    "dropped",
    "rejected",
  ] as const;
  for (const c of counters) {
    metric(
      `sluice_items_${c}_total`,
      "counter",
      `Items ${c}.`,
      snaps.map((s) => [base(s.target), s.counters[c]]),
    );
  }
  for (const [name, help, pick] of [
    [
      "sluice_queue_wait_seconds",
      "Time items waited for a token.",
      (t: ReturnType<Engine["list"]>[number]) => t.waitHist,
    ],
    [
      "sluice_send_duration_seconds",
      "Time from send to response.",
      (t: ReturnType<Engine["list"]>[number]) => t.sendHist,
    ],
  ] as const) {
    out.push(`# HELP ${name} ${help}`, `# TYPE ${name} histogram`);
    for (const t of engine.list()) {
      const h = pick(t);
      let acc = 0;
      Histogram.BOUNDS.forEach((b, i) => {
        acc += h.counts[i]!;
        out.push(`${name}_bucket{${base(t.name)},le="${b / 1000}"} ${acc}`);
      });
      out.push(`${name}_bucket{${base(t.name)},le="+Inf"} ${h.count}`);
      out.push(`${name}_sum{${base(t.name)}} ${h.sum / 1000}`);
      out.push(`${name}_count{${base(t.name)}} ${h.count}`);
    }
  }
  return `${out.join("\n")}\n`;
}

/** Serve /metrics, /healthz and /readyz for an engine (Kubernetes probes, Prometheus scrape). */
export function serveMetrics(
  engine: Engine | Promise<Engine>,
  opts: { port: number; host?: string },
): Server {
  const server = createServer(async (req, res) => {
    const e = await engine;
    const path = (req.url ?? "/").split("?")[0];
    if (path === "/metrics") {
      res.writeHead(200, { "content-type": "text/plain; version=0.0.4" });
      return res.end(renderPrometheus(e));
    }
    if (path === "/healthz" || path === "/readyz") {
      const h = e.health();
      const ok = path === "/healthz" ? h.live : h.ready;
      res.writeHead(ok ? 200 : 503, { "content-type": "application/json" });
      return res.end(JSON.stringify(h));
    }
    res.writeHead(404).end();
  });
  server.listen(opts.port, opts.host ?? "0.0.0.0");
  server.unref();
  return server;
}
