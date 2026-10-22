import { consoleLogger, createEngine } from "@viralpatel4439/sluice";
import { remotePool } from "@viralpatel4439/sluice/remote";
import { type Browser, chromium } from "playwright";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { type RunningServer, startServer } from "../src/index.js";

/** Drives the real control panel in headless Chromium against a live server and fleet. */

const redisUrl = inject("redisUrl" as never) as string;
const silent = consoleLogger("silent");
const ADMIN = "panel-admin";
const cleanup: (() => Promise<unknown>)[] = [];
let server: RunningServer;
let browser: Browser;
let base: string;
const namespace = `panel${Date.now()}`;

beforeAll(async () => {
  server = await startServer(
    {
      redisUrl,
      grpcPort: 0,
      httpPort: 0,
      httpHost: "127.0.0.1",
      grpcHost: "127.0.0.1",
      adminToken: ADMIN,
      leaseMs: 3000,
      renewMs: 500,
    },
    silent,
  );
  cleanup.push(() => server.close());
  base = `http://127.0.0.1:${server.httpPort}`;
  // A small fleet: two caller services sharing one target.
  for (const caller of ["orders", "billing"]) {
    const instanceId = `${caller}-panel`;
    const coordination = await remotePool({
      url: `127.0.0.1:${server.grpcPort}`,
      instanceId,
      caller,
      logger: silent,
    });
    const e = createEngine({ namespace, caller, instanceId, coordination, logger: silent });
    cleanup.push(() => e.close());
    const t = e.target("inventory", { capacity: 6, send: async () => ({ status: 200 }) });
    await Promise.all(Array.from({ length: 10 }, (_, i) => t.submit(i)));
  }
  browser = await chromium.launch();
  cleanup.push(() => browser.close());
}, 120_000);

afterAll(async () => {
  for (const c of cleanup.reverse()) await c().catch(() => {});
});

describe("control panel (browser)", () => {
  it("lists targets, shows callers and instances, and pins capacity through the UI", async () => {
    const page = await browser.newPage();
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
    await page.goto(base);

    const card = page.locator(".pool", { hasText: "inventory" }).filter({ hasText: namespace });
    await card.waitFor({ timeout: 10_000 });
    await expect(card.locator(".badge").textContent()).resolves.toBeTruthy();
    await card.click();

    const detail = page.locator("#detail");
    await detail.getByText(`${namespace} / inventory`).waitFor();
    // Both caller services appear with their instances.
    await detail.locator("table").first().getByText("orders").waitFor();
    await detail.locator("table").first().getByText("billing").waitFor();
    expect(await detail.getByText("orders-panel").count()).toBeGreaterThan(0);

    // Actions are disabled until a token is saved.
    await expect(detail.locator("#setCap").isDisabled()).resolves.toBe(true);
    await page.fill("#token", ADMIN);
    await page.click("#saveToken");
    await expect(page.locator("#setCap").isDisabled()).resolves.toBe(false);

    // Invalid input is rejected client-side.
    await page.fill("#cap", "-3");
    await page.click("#setCap");
    await expect(page.locator("#actErr").textContent()).resolves.toContain("whole number");

    // Pin capacity to 3.
    await page.fill("#cap", "3");
    await page.click("#setCap");
    await page.waitForFunction(
      async ([b, pool]) =>
        ((await (await fetch(`${b}/api/pools/${encodeURIComponent(pool)}`)).json()) as { cap: number })
          .cap === 3,
      [base, `${namespace}/inventory`] as const,
      { timeout: 10_000 },
    );
    const view = (await (
      await fetch(`${base}/api/pools/${encodeURIComponent(`${namespace}/inventory`)}`)
    ).json()) as { cap: number; override: string | false };
    expect(view.cap).toBe(3);
    expect(view.override).toBe("3");
    // The event log in the page shows the audited action.
    await page.locator(".log", { hasText: "admin:capacity" }).waitFor({ timeout: 10_000 });

    // Unpin hands control back.
    await page.click("#clearCap");
    await page.waitForFunction(
      async ([b, pool]) =>
        ((await (await fetch(`${b}/api/pools/${encodeURIComponent(pool)}`)).json()) as { override: unknown })
          .override === false,
      [base, `${namespace}/inventory`] as const,
      { timeout: 10_000 },
    );

    // Filtering hides other targets.
    await page.fill("#filter", "does-not-exist");
    await page.locator("#pools", { hasText: "No matching targets" }).waitFor();

    expect(errors).toEqual([]);
    await page.close();
  }, 60_000);

  it("escapes untrusted names (no script injection through pool or caller names)", async () => {
    const evil = `x"><img src=x onerror=window.__pwned=1>`;
    const coordination = await remotePool({
      url: `127.0.0.1:${server.grpcPort}`,
      instanceId: "evil-1",
      caller: evil.replace(/[^A-Za-z0-9]/g, "") || "evil",
      logger: silent,
    });
    // Pool names are validated by the server; caller names reach the panel via Redis, so inject one directly.
    const e = createEngine({ namespace, caller: "evil", instanceId: "evil-1", coordination, logger: silent });
    cleanup.push(() => e.close());
    await e.target("inventory", { capacity: 6, send: async () => ({ status: 200 }) }).submit(1);
    const { Redis } = await import("ioredis");
    const r = new Redis(redisUrl);
    await r.hset(`sluice:{${namespace}/inventory}:caller`, "evil-1", evil);
    r.disconnect();
    const page = await browser.newPage();
    await page.goto(base);
    await page.locator(".pool", { hasText: namespace }).first().click();
    await page.locator("#detail table").first().waitFor();
    await page.waitForTimeout(2500);
    expect(
      await page.evaluate(() => (globalThis as unknown as { __pwned?: number }).__pwned),
    ).toBeUndefined();
    expect(await page.locator("#detail").innerText()).toContain("onerror");
    await page.close();
  }, 60_000);
});
