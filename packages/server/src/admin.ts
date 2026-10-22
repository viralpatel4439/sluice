import { timingSafeEqual } from "node:crypto";
import { type IncomingMessage, type Server, type ServerResponse, createServer } from "node:http";
import type { Logger } from "@viralpatel4439/sluice";
import { collectPool, inspectPool, overrideCapacity } from "@viralpatel4439/sluice/redis";
import type { Cluster, Redis } from "ioredis";
import type { ServerConfig } from "./config.js";
import type { SessionInfo } from "./sessions.js";
import { PANEL_HTML } from "./ui.js";

export interface InstanceView {
  id: string;
  caller: string;
  held: number;
  want: number;
  backlog: number;
  weight: number;
  share: number;
  waiting: boolean;
  leaseMs: number;
  stats: { held: number; inFlight: number; backlog: number; at: number } | null;
}

export interface PoolView {
  pool: string;
  namespace: string;
  target: string;
  cap: number;
  free: number;
  debt: number;
  waiters: number;
  owner: string | false;
  override: string | false;
  instances: InstanceView[];
  held: number;
  inFlight: number;
  backlog: number;
}

interface HistoryPoint {
  t: number;
  cap: number;
  held: number;
  inFlight: number;
  backlog: number;
}

export interface AdminDeps {
  config: ServerConfig;
  redis: Redis | Cluster;
  logger: Logger;
  sessions: () => SessionInfo[];
  ready: () => boolean;
}

function same(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  return ab.length === bb.length && timingSafeEqual(ab, bb);
}

function tokenOf(req: IncomingMessage, url: URL): string {
  const h = req.headers.authorization ?? "";
  if (h.startsWith("Bearer ")) return h.slice(7);
  // EventSource cannot set headers; allow a query token for reads only.
  return url.searchParams.get("token") ?? "";
}

function send(res: ServerResponse, status: number, body: unknown, type = "application/json"): void {
  const text = type === "application/json" ? JSON.stringify(body) : String(body);
  res.writeHead(status, {
    "content-type": `${type}; charset=utf-8`,
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
  });
  res.end(text);
}

async function readJson(req: IncomingMessage, limit = 16 * 1024): Promise<Record<string, unknown>> {
  let size = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > limit) throw new Error("body too large");
    chunks.push(chunk as Buffer);
  }
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
}

/** Reads every known pool from Redis and keeps a short in-memory history for charts. */
export class PoolReader {
  private readonly history = new Map<string, HistoryPoint[]>();
  private timer: NodeJS.Timeout | undefined;
  last: PoolView[] = [];

  /** When each pool was first seen with no instances. */
  private readonly emptySince = new Map<string, number>();

  constructor(
    private readonly redis: Redis | Cluster,
    private readonly keyPrefix: string,
    private readonly gcMs = 10 * 60_000,
  ) {}

  start(intervalMs = 2_000): void {
    const tick = async () => {
      try {
        this.last = await this.readAll();
        const t = Date.now();
        for (const p of this.last) {
          const h = this.history.get(p.pool) ?? [];
          h.push({ t, cap: p.cap, held: p.held, inFlight: p.inFlight, backlog: p.backlog });
          if (h.length > 900) h.shift(); // 30 min at 2 s
          this.history.set(p.pool, h);
        }
      } catch {
        // Redis briefly unavailable; keep the last view.
      }
    };
    void tick();
    this.timer = setInterval(tick, intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    clearInterval(this.timer);
  }

  historyOf(pool: string): HistoryPoint[] {
    return this.history.get(pool) ?? [];
  }

  async readAll(): Promise<PoolView[]> {
    const names = (await this.redis.smembers(`${this.keyPrefix}:pools`)).sort();
    const views: PoolView[] = [];
    const now = Date.now();
    for (const name of names) {
      const v = await this.read(name);
      if (!v) {
        await this.redis.srem(`${this.keyPrefix}:pools`, name);
        continue;
      }
      if (v.instances.length > 0) this.emptySince.delete(name);
      else if (!this.emptySince.has(name)) this.emptySince.set(name, now);
      else if (
        now - this.emptySince.get(name)! >= this.gcMs &&
        (await collectPool(this.redis, name, this.keyPrefix))
      ) {
        this.emptySince.delete(name);
        continue;
      }
      views.push(v);
    }
    return views;
  }

  async read(pool: string): Promise<PoolView | undefined> {
    const raw = (await inspectPool(this.redis, pool, this.keyPrefix)) as Record<string, unknown> & {
      err?: string;
    };
    if (raw.err) return undefined;
    const instances = (Array.isArray(raw.instances) ? raw.instances : []).map(
      (i: Record<string, unknown>) => ({
        ...(i as unknown as InstanceView),
        stats: typeof i.stats === "string" ? (JSON.parse(i.stats) as InstanceView["stats"]) : null,
      }),
    );
    const slash = pool.indexOf("/");
    return {
      pool,
      namespace: pool.slice(0, slash),
      target: pool.slice(slash + 1),
      cap: Number(raw.cap),
      free: Number(raw.free),
      debt: Number(raw.debt),
      waiters: Number(raw.waiters),
      owner: (raw.owner as string | false) ?? false,
      override: (raw.override as string | false) ?? false,
      instances,
      held: instances.reduce((a, i) => a + i.held, 0),
      inFlight: instances.reduce((a, i) => a + (i.stats?.inFlight ?? 0), 0),
      backlog: instances.reduce((a, i) => a + i.backlog, 0),
    };
  }
}

function esc(v: string): string {
  return v.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");
}

/** Prometheus text exposition of fleet-wide pool state. */
export function renderMetrics(pools: PoolView[], sessions: number): string {
  const out: string[] = [];
  const gauge = (name: string, help: string, rows: [Record<string, string>, number][]) => {
    out.push(`# HELP ${name} ${help}`, `# TYPE ${name} gauge`);
    for (const [labels, v] of rows) {
      const l = Object.entries(labels)
        .map(([k, val]) => `${k}="${esc(val)}"`)
        .join(",");
      out.push(`${name}{${l}} ${v}`);
    }
  };
  const base = (p: PoolView) => ({ namespace: p.namespace, target: p.target });
  gauge("sluice_server_sessions", "Engine instances connected to this server replica.", [[{}, sessions]]);
  gauge(
    "sluice_pool_capacity",
    "Capacity N of the target across the fleet.",
    pools.map((p) => [base(p), p.cap]),
  );
  gauge(
    "sluice_pool_tokens_free",
    "Tokens not held by any instance.",
    pools.map((p) => [base(p), p.free]),
  );
  gauge(
    "sluice_pool_tokens_held",
    "Tokens held by all instances.",
    pools.map((p) => [base(p), p.held]),
  );
  gauge(
    "sluice_pool_debt",
    "Tokens to retire after a capacity cut.",
    pools.map((p) => [base(p), p.debt]),
  );
  gauge(
    "sluice_pool_waiters",
    "Instances waiting for tokens.",
    pools.map((p) => [base(p), p.waiters]),
  );
  gauge(
    "sluice_pool_instances",
    "Instances registered on the pool.",
    pools.map((p) => [base(p), p.instances.length]),
  );
  gauge(
    "sluice_pool_backlog",
    "Queued items across the fleet (use for KEDA/HPA).",
    pools.map((p) => [base(p), p.backlog]),
  );
  gauge(
    "sluice_pool_override",
    "1 when an operator override pins capacity.",
    pools.map((p) => [base(p), p.override === false ? 0 : 1]),
  );
  const byCaller = new Map<string, { labels: Record<string, string>; held: number; backlog: number }>();
  for (const p of pools) {
    for (const i of p.instances) {
      const key = `${p.pool}|${i.caller}`;
      const row = byCaller.get(key) ?? { labels: { ...base(p), caller: i.caller }, held: 0, backlog: 0 };
      row.held += i.held;
      row.backlog += i.backlog;
      byCaller.set(key, row);
    }
  }
  gauge(
    "sluice_caller_tokens_held",
    "Tokens held per caller service.",
    [...byCaller.values()].map((r) => [r.labels, r.held]),
  );
  gauge(
    "sluice_caller_backlog",
    "Queued items per caller service.",
    [...byCaller.values()].map((r) => [r.labels, r.backlog]),
  );
  return `${out.join("\n")}\n`;
}

export function startAdmin(deps: AdminDeps): { server: Server; reader: PoolReader } {
  const { config, redis, logger } = deps;
  const reader = new PoolReader(redis, config.keyPrefix, config.poolGcMs);
  reader.start();
  const clients = new Set<ServerResponse>();
  const push = setInterval(() => {
    if (clients.size === 0) return;
    const data = `data: ${JSON.stringify({ pools: reader.last, sessions: deps.sessions().length, at: Date.now() })}\n\n`;
    for (const c of clients) c.write(data);
  }, 2_000);
  push.unref?.();

  const canRead = (req: IncomingMessage, url: URL) =>
    !config.viewToken ||
    same(tokenOf(req, url), config.viewToken) ||
    (!!config.adminToken && same(tokenOf(req, url), config.adminToken));
  const canWrite = (req: IncomingMessage) => {
    const h = req.headers.authorization ?? "";
    return !!config.adminToken && h.startsWith("Bearer ") && same(h.slice(7), config.adminToken);
  };

  const audit = async (
    pool: string,
    action: string,
    detail: Record<string, unknown>,
    req: IncomingMessage,
  ) => {
    const actor = String(req.headers["x-sluice-actor"] ?? "admin").slice(0, 64);
    await redis.xadd(
      `${config.keyPrefix}:{${pool}}:log`,
      "MAXLEN",
      "~",
      1000,
      "*",
      "type",
      `admin:${action}`,
      "actor",
      actor,
      "detail",
      JSON.stringify(detail),
    );
    logger.info({ pool, action, actor, ...detail }, "admin action");
    return actor;
  };

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const path = url.pathname;
    try {
      if (path === "/healthz") return send(res, 200, { ok: true });
      if (path === "/readyz") return send(res, deps.ready() ? 200 : 503, { ready: deps.ready() });
      if (path === "/metrics") {
        if (!canRead(req, url)) return send(res, 401, { error: "unauthorized" });
        const pools = await reader.readAll().catch(() => reader.last);
        return send(res, 200, renderMetrics(pools, deps.sessions().length), "text/plain; version=0.0.4");
      }
      if (path === "/" || path === "/index.html") {
        res.writeHead(200, {
          "content-type": "text/html; charset=utf-8",
          "content-security-policy":
            "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'",
          "x-frame-options": "DENY",
        });
        return res.end(PANEL_HTML);
      }
      if (!path.startsWith("/api/")) return send(res, 404, { error: "not found" });
      if (!canRead(req, url)) return send(res, 401, { error: "unauthorized" });

      if (path === "/api/overview" && req.method === "GET") {
        return send(res, 200, {
          pools: reader.last,
          sessions: deps.sessions(),
          server: { id: config.serverId, actions: !!config.adminToken },
        });
      }
      if (path === "/api/events" && req.method === "GET") {
        res.writeHead(200, {
          "content-type": "text/event-stream",
          "cache-control": "no-store",
          connection: "keep-alive",
        });
        res.write(
          `data: ${JSON.stringify({ pools: reader.last, sessions: deps.sessions().length, at: Date.now() })}\n\n`,
        );
        clients.add(res);
        req.on("close", () => clients.delete(res));
        return;
      }
      const m = path.match(/^\/api\/pools\/([^/]+)(?:\/(capacity|pause|resume|log|history))?$/);
      if (!m) return send(res, 404, { error: "not found" });
      const pool = decodeURIComponent(m[1]!);
      const action = m[2];
      if (!action && req.method === "GET") {
        const view = await reader.read(pool);
        return view ? send(res, 200, view) : send(res, 404, { error: "unknown pool" });
      }
      if (action === "history" && req.method === "GET") return send(res, 200, reader.historyOf(pool));
      if (action === "log" && req.method === "GET") {
        const entries = await redis.xrevrange(`${config.keyPrefix}:{${pool}}:log`, "+", "-", "COUNT", 100);
        return send(
          res,
          200,
          entries.map(([id, fields]) => {
            const o: Record<string, string> = { id, at: id.split("-")[0]! };
            for (let i = 0; i < fields.length; i += 2) o[fields[i]!] = fields[i + 1]!;
            return o;
          }),
        );
      }
      // Writes below.
      if (req.method !== "POST" && req.method !== "DELETE")
        return send(res, 405, { error: "method not allowed" });
      if (!config.adminToken)
        return send(res, 403, { error: "actions are disabled: set SLUICE_ADMIN_TOKEN" });
      if (!canWrite(req)) return send(res, 401, { error: "admin token required" });
      if (!(await reader.read(pool))) return send(res, 404, { error: "unknown pool" });

      if (action === "capacity" && req.method === "POST") {
        const body = await readJson(req);
        const capacity = Number(body.capacity);
        if (!Number.isInteger(capacity) || capacity < 0 || capacity > 1_000_000) {
          return send(res, 400, { error: "capacity must be an integer between 0 and 1000000" });
        }
        const actor = await audit(pool, "capacity", { capacity }, req);
        return send(res, 200, await overrideCapacity(redis, pool, capacity, actor, config.keyPrefix));
      }
      if (action === "capacity" && req.method === "DELETE") {
        const actor = await audit(pool, "override-clear", {}, req);
        return send(res, 200, await overrideCapacity(redis, pool, "clear", actor, config.keyPrefix));
      }
      if (action === "pause" && req.method === "POST") {
        const view = (await reader.read(pool))!;
        if (view.override === false || view.cap > 0)
          await redis.hset(`${config.keyPrefix}:{${pool}}:meta`, "paused_cap", view.cap);
        const actor = await audit(pool, "pause", { previous: view.cap }, req);
        return send(res, 200, await overrideCapacity(redis, pool, 0, actor, config.keyPrefix));
      }
      if (action === "resume" && req.method === "POST") {
        const prev = Number(await redis.hget(`${config.keyPrefix}:{${pool}}:meta`, "paused_cap"));
        const actor = await audit(pool, "resume", { capacity: prev }, req);
        if (Number.isFinite(prev) && prev > 0)
          await overrideCapacity(redis, pool, prev, actor, config.keyPrefix);
        return send(res, 200, await overrideCapacity(redis, pool, "clear", actor, config.keyPrefix));
      }
      return send(res, 404, { error: "not found" });
    } catch (error) {
      logger.error({ err: error, path }, "admin request failed");
      if (!res.headersSent) send(res, 500, { error: "internal error" });
    }
  });
  server.on("close", () => {
    clearInterval(push);
    reader.stop();
    for (const c of clients) c.end();
  });
  return { server, reader };
}
