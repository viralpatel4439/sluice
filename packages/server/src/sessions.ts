import { timingSafeEqual } from "node:crypto";
import type { Metadata, ServerDuplexStream } from "@grpc/grpc-js";
import type { Logger } from "@viralpatel4439/sluice";
import { RedisCoordinator, type SubscriptionHub } from "@viralpatel4439/sluice/redis";
import { type ClientMessage, REMOTE_PROTOCOL, type ServerMessage } from "@viralpatel4439/sluice/remote";
import type { Cluster, Redis } from "ioredis";
import type { ServerConfig } from "./config.js";

const RANKS = ["critical", "standard", "bulk"] as const;
const POOL_RE = /^[A-Za-z0-9._:-]{1,200}\/[A-Za-z0-9._:-]{1,200}(#[A-Za-z0-9._:-]{1,200})?$/;

export interface SessionDeps {
  config: ServerConfig;
  redis: Redis | Cluster;
  subscriptions: SubscriptionHub;
  logger: Logger;
}

export interface SessionInfo {
  instanceId: string;
  caller: string;
  pools: string[];
  connectedAt: number;
  peer: string;
}

function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  return ab.length === bb.length && timingSafeEqual(ab, bb);
}

/** Checks the bearer token against SLUICE_TOKENS. Returns an error message, or undefined if allowed. */
export function authorize(
  tokens: Map<string, string>,
  metadata: Metadata,
  caller: string,
): string | undefined {
  if (tokens.size === 0) return undefined;
  const header = String(metadata.get("authorization")[0] ?? "");
  const token = header.startsWith("Bearer ") ? header.slice(7) : "";
  if (!token) return "missing bearer token";
  const own = tokens.get(caller);
  if (own !== undefined && safeEqual(own, token)) return undefined;
  const any = tokens.get("*");
  if (any !== undefined && safeEqual(any, token)) return undefined;
  return `token not valid for caller "${caller}"`;
}

/**
 * One connected engine instance. Its pool operations run through a
 * RedisCoordinator acting on its behalf; when the stream ends its tokens are
 * returned at once instead of waiting for the lease to expire.
 */
export class Session {
  info: SessionInfo | undefined;
  private coordinator: RedisCoordinator | undefined;
  private chain: Promise<void> = Promise.resolve();
  private ended = false;
  private lastSeen = Date.now();
  private watchdog: NodeJS.Timeout;

  constructor(
    private readonly call: ServerDuplexStream<ClientMessage, ServerMessage>,
    private readonly deps: SessionDeps,
    private readonly onClose: (s: Session) => void,
  ) {
    call.on("data", (m: ClientMessage) => {
      this.lastSeen = Date.now();
      this.chain = this.chain.then(() => this.handle(m)).catch((error) => this.fail("INTERNAL", error));
    });
    call.on("end", () => this.end("client ended stream"));
    call.on("error", (e: Error) => this.end(e.message));
    call.on("cancelled", () => this.end("cancelled"));
    // Clients ping every second; a silent client is treated as gone.
    this.watchdog = setInterval(() => {
      if (Date.now() - this.lastSeen > deps.config.leaseMs) this.end("client silent past lease");
    }, 1_000);
    this.watchdog.unref?.();
  }

  private write(m: ServerMessage): void {
    if (this.ended) return;
    try {
      this.call.write(m);
    } catch {
      this.end("write failed");
    }
  }

  private fail(code: string, error: unknown): void {
    const message = error instanceof Error ? error.message : String(error);
    this.deps.logger.warn({ code, err: message, instance: this.info?.instanceId }, "session error");
    this.write({ error: { code, message } });
  }

  private async handle(m: ClientMessage): Promise<void> {
    if (this.ended) return;
    if ("hello" in m) return this.hello(m.hello);
    if ("ping" in m) return this.write({ pong: { at: m.ping.at ?? 0 } });
    const c = this.coordinator;
    if (!c || !this.info) return this.fail("NO_HELLO", "first message must be hello");
    if ("join" in m) {
      const { req = 0, pool = "", weight, capacity = 0, held = 0 } = m.join;
      if (!POOL_RE.test(pool)) {
        this.write({ joined: { req, pool, error: `invalid pool name "${pool}"` } });
        return;
      }
      try {
        const state = await c.join(pool, { caller: this.info.caller, weight: weight || 1, capacity, held });
        if (!this.info.pools.includes(pool)) this.info.pools.push(pool);
        this.write({ joined: { req, pool, state: this.stateMsg(pool, state), error: "" } });
      } catch (error) {
        this.write({ joined: { req, pool, error: error instanceof Error ? error.message : String(error) } });
      }
      return;
    }
    if ("demand" in m) {
      const { pool = "", want = 0, rank = 1, backlog = 0 } = m.demand;
      c.demand(pool, want, { priority: RANKS[rank] ?? "standard", backlog });
      return;
    }
    if ("release" in m) return c.release(m.release.pool ?? "", m.release.n ?? 0);
    if ("report" in m) {
      const { pool = "", held = 0, inFlight = 0, backlog = 0 } = m.report;
      return c.report(pool, { held, inFlight, backlog });
    }
    if ("setCapacity" in m) return c.setCapacity(m.setCapacity.pool ?? "", m.setCapacity.capacity ?? 0);
    if ("leave" in m) {
      const { req = 0, pool = "" } = m.leave;
      await c.leave(pool);
      this.info.pools = this.info.pools.filter((p) => p !== pool);
      this.write({ left: { req } });
    }
  }

  private async hello(h: { instanceId?: string; caller?: string; protocol?: number }): Promise<void> {
    if (this.coordinator) return this.fail("DUPLICATE_HELLO", "hello already received");
    const instanceId = h.instanceId ?? "";
    const caller = h.caller ?? "unknown";
    if (!instanceId) return this.fail("BAD_HELLO", "instanceId is required");
    if ((h.protocol ?? 0) > REMOTE_PROTOCOL)
      return this.fail("PROTOCOL", `server speaks protocol ${REMOTE_PROTOCOL}`);
    const denied = authorize(this.deps.config.tokens, this.call.metadata, caller);
    if (denied) {
      this.fail("UNAUTHENTICATED", denied);
      this.call.end();
      return this.end("unauthenticated");
    }
    const { config, redis, subscriptions, logger } = this.deps;
    this.coordinator = await RedisCoordinator.create({
      client: redis,
      subscriptions: subscriptions.retain(),
      instanceId,
      caller,
      keyPrefix: config.keyPrefix,
      leaseMs: config.leaseMs,
      renewMs: config.renewMs,
      shards: (pool) => {
        const target = pool.slice(pool.indexOf("/") + 1);
        return config.shards.find(([re]) => re.test(target))?.[1] ?? 1;
      },
      logger,
    });
    this.coordinator.attach({
      grant: (pool, n) => this.write({ grant: { pool, n } }),
      state: (pool, state) => this.write({ state: { pool, state: this.stateMsg(pool, state) } }),
      fenced: (pool, fallback) => this.write({ fenced: { pool, fallback } }),
      restored: (pool) => this.write({ restored: { pool } }),
    });
    this.info = { instanceId, caller, pools: [], connectedAt: Date.now(), peer: this.call.getPeer() };
    logger.info({ instanceId, caller, peer: this.info.peer }, "instance connected");
    this.write({
      welcome: { serverId: config.serverId, protocol: REMOTE_PROTOCOL, leaseMs: config.leaseMs },
    });
  }

  private stateMsg(pool: string, s: { capacity: number; contended: boolean; fairShare: number }) {
    const info = this.coordinator?.info(pool);
    return {
      capacity: s.capacity,
      contended: s.contended,
      fairShare: s.fairShare,
      owner: info?.owner ?? false,
      instances: info?.instances ?? 1,
    };
  }

  /** Close the session and give its tokens back right away. */
  end(reason: string): void {
    if (this.ended) return;
    this.ended = true;
    clearInterval(this.watchdog);
    if (this.info)
      this.deps.logger.info({ instanceId: this.info.instanceId, reason }, "instance disconnected");
    const c = this.coordinator;
    this.coordinator = undefined;
    void this.chain.finally(() => c?.close()).catch(() => {});
    try {
      this.call.end();
    } catch {}
    this.onClose(this);
  }
}
