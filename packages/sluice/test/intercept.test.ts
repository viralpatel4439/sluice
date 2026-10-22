import { type Server, createServer } from "node:http";
import type { AddressInfo } from "node:net";
import axios from "axios";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { exclusionMatcher, isInternalHost, targetNameFor } from "../src/intercept/hosts.js";
import { sluice } from "../src/sluice.js";

/** A real HTTP target that records concurrency, headers and can misbehave on demand. */
function target(opts: { latencyMs?: number; fail?: (n: number) => number | undefined } = {}) {
  const t = {
    inFlight: 0,
    peak: 0,
    calls: 0,
    headers: [] as Record<string, string | string[] | undefined>[],
    server: undefined as unknown as Server,
    url: "",
  };
  t.server = createServer((req, res) => {
    t.calls++;
    t.inFlight++;
    t.peak = Math.max(t.peak, t.inFlight);
    t.headers.push(req.headers);
    const n = t.calls;
    req.resume();
    setTimeout(() => {
      t.inFlight--;
      const status = opts.fail?.(n);
      res.writeHead(status ?? 200, {
        "content-type": "text/plain",
        ...(status === 503 ? { "retry-after": "0" } : {}),
      });
      res.end(`ok ${n}`);
    }, opts.latencyMs ?? 20);
  });
  return t;
}

async function listen(t: ReturnType<typeof target>) {
  await new Promise<void>((r) => t.server.listen(0, "127.0.0.1", () => r()));
  t.url = `http://127.0.0.1:${(t.server.address() as AddressInfo).port}`;
  return t;
}

const servers: Server[] = [];
afterAll(() => {
  for (const s of servers) s.close();
});

describe("host helpers", () => {
  it("derives target names and recognises internal hosts", () => {
    expect(targetNameFor(new URL("http://Inventory/records"))).toBe("inventory");
    expect(targetNameFor(new URL("https://api.example.com:443/x"))).toBe("api.example.com");
    expect(targetNameFor(new URL("http://10.0.0.5:8080/"))).toBe("10.0.0.5_8080");
    expect(isInternalHost("inventory")).toBe(true);
    expect(isInternalHost("orders.default.svc.cluster.local")).toBe(true);
    expect(isInternalHost("10.1.2.3")).toBe(true);
    expect(isInternalHost("api.stripe.com")).toBe(false);
  });
  it("never gates telemetry, metadata or excluded hosts", () => {
    const excluded = exclusionMatcher(["*.health.local"], {
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4318",
    });
    expect(excluded(new URL("http://collector:4318/v1/traces"))).toBe(true);
    expect(excluded(new URL("http://169.254.169.254/latest"))).toBe(true);
    expect(excluded(new URL("http://a.health.local/"))).toBe(true);
    expect(excluded(new URL("http://inventory/_capacity"))).toBe(true);
    expect(excluded(new URL("http://inventory/records"))).toBe(false);
  });
});

describe("zero-code interception", () => {
  const svc = target({ latencyMs: 25 });
  beforeAll(async () => {
    await listen(svc);
    servers.push(svc.server);
    await sluice.install({
      caller: "intercept-test",
      handleSignals: false,
      logLevel: "silent",
      targets: { "127.0.0.1_*": { capacity: 3 } },
    });
  });
  afterAll(async () => {
    await sluice.uninstall();
  });
  afterEach(() => {
    svc.peak = 0;
  });

  it("gates plain fetch and returns the real Response", async () => {
    const res = await Promise.all(Array.from({ length: 20 }, () => fetch(`${svc.url}/records`)));
    expect(res.every((r) => r.status === 200)).toBe(true);
    expect(await res[0]!.text()).toMatch(/^ok/);
    expect(svc.peak).toBeLessThanOrEqual(3);
    expect(svc.headers.at(-1)?.["idempotency-key"]).toBeTruthy();
    expect(svc.headers.at(-1)?.["x-sluice-caller"]).toBe("intercept-test");
  });

  it("gates axios (http.Agent) with the same pool", async () => {
    const both = await Promise.all([
      ...Array.from({ length: 10 }, () => axios.get(`${svc.url}/a`)),
      ...Array.from({ length: 10 }, () => fetch(`${svc.url}/f`)),
    ]);
    expect(both).toHaveLength(20);
    expect(svc.peak).toBeLessThanOrEqual(3);
  });

  it("adds idempotency and caller headers on the axios path too", async () => {
    await axios.post(`${svc.url}/a`, { x: 1 });
    const h = svc.headers.at(-1)!;
    expect(h["idempotency-key"]).toMatch(/^[0-9A-Z]{26}$/);
    expect(h["x-sluice-caller"]).toBe("intercept-test");
    await axios.post(`${svc.url}/a`, { x: 1 }, { headers: { "Idempotency-Key": "mine" } });
    expect(svc.headers.at(-1)!["idempotency-key"]).toBe("mine");
  });

  it("passes through bypassed calls", async () => {
    const calls = Array.from({ length: 8 }, () => sluice.bypass(() => fetch(`${svc.url}/health`)));
    await Promise.all(calls);
    expect(svc.peak).toBeGreaterThan(3);
  });

  it("applies deadlines as AbortError and honours the caller's AbortSignal", async () => {
    const slow = Array.from({ length: 6 }, () => fetch(`${svc.url}/fill`));
    const late = sluice.with({ deadlineMs: 5 }, () => fetch(`${svc.url}/late`));
    await expect(late).rejects.toMatchObject({ name: "AbortError" });
    const ac = new AbortController();
    const cancelled = fetch(`${svc.url}/cancel`, { signal: ac.signal });
    ac.abort(new Error("user cancelled"));
    await expect(cancelled).rejects.toThrow("user cancelled");
    await Promise.all(slow);
  });

  it("wraps arbitrary async functions behind a named target", async () => {
    let inFlight = 0;
    let peak = 0;
    const charge = sluice.wrap(
      "payments-sdk",
      async (amount: number) => {
        inFlight++;
        peak = Math.max(peak, inFlight);
        await new Promise((r) => setTimeout(r, 10));
        inFlight--;
        return amount * 2;
      },
      { capacity: 2 },
    );
    const out = await Promise.all(Array.from({ length: 10 }, (_, i) => charge(i)));
    expect(out[3]).toBe(6);
    expect(peak).toBeLessThanOrEqual(2);
  });
});

describe("retries through fetch", () => {
  it("retries idempotent requests on 503 and never retries unkeyed POSTs", async () => {
    const flaky = await listen(target({ latencyMs: 2, fail: (n) => (n % 2 === 1 ? 503 : undefined) }));
    servers.push(flaky.server);
    await sluice.install({ caller: "retry-test", handleSignals: false, logLevel: "silent" });
    try {
      const get = await fetch(`${flaky.url}/x`);
      expect(get.status).toBe(200);
      expect(flaky.calls).toBe(2);
      const post = await fetch(`${flaky.url}/x`, { method: "POST", body: "a" });
      expect(post.status).toBe(503);
      expect(flaky.calls).toBe(3);
      const keyed = await fetch(`${flaky.url}/x`, {
        method: "POST",
        body: "b",
        headers: { "Idempotency-Key": "k1" },
      });
      expect(keyed.status).toBe(200);
      expect(flaky.headers.at(-1)?.["idempotency-key"]).toBe("k1");
    } finally {
      await sluice.uninstall();
    }
  });
});
