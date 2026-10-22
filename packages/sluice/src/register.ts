/**
 * Zero-code entry point:
 *   NODE_OPTIONS="--import @viralpatel4439/sluice/register" node app.js
 * Configured entirely through SLUICE_* environment variables.
 */
import { serveMetrics } from "./prometheus.js";
import { sluice } from "./sluice.js";

if (process.env.SLUICE_DISABLED !== "1") {
  const engine = sluice.install();
  // Optional /metrics, /healthz, /readyz for Prometheus and Kubernetes probes.
  const port = Number(process.env.SLUICE_METRICS_PORT);
  if (Number.isInteger(port) && port > 0) serveMetrics(engine, { port });
  // Optional OpenTelemetry metrics and spans through the app's global OTel providers.
  if (process.env.SLUICE_OTEL === "1") {
    void engine
      .then(async (e) => {
        const { instrumentOtel, traceOtel } = await import("./otel.js");
        await instrumentOtel(e);
        await traceOtel(e);
      })
      .catch((error) => process.stderr.write(`sluice: OpenTelemetry setup failed: ${error}\n`));
  }
}
