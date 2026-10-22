import { describe, expect, it } from "vitest";
import { ManualClock } from "../src/core/clock.js";
import { renderPrometheus } from "../src/prometheus.js";
import { MockService, settle, testEngine } from "./helpers.js";

describe("prometheus exporter", () => {
  it("exposes gauges, counters and histograms per target", async () => {
    const clock = new ManualClock();
    const svc = new MockService(clock, 10);
    const engine = testEngine(clock, { namespace: "prod", caller: "orders" });
    const t = engine.target("inventory", { capacity: 2, send: svc.send });
    const all = Array.from({ length: 5 }, (_, i) => t.submit(i));
    await settle(clock, 100);
    await Promise.all(all);
    const text = renderPrometheus(engine);
    expect(text).toContain('sluice_capacity{namespace="prod",caller="orders",target="inventory"} 2');
    expect(text).toContain('sluice_items_done_total{namespace="prod",caller="orders",target="inventory"} 5');
    expect(text).toMatch(/sluice_send_duration_seconds_count\{[^}]+\} 5/);
    expect(text).toMatch(/sluice_queue_wait_seconds_bucket\{[^}]+le="\+Inf"\} 5/);
    await engine.close();
  });
});
