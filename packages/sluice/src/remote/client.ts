import type { ClientDuplexStream } from "@grpc/grpc-js";
import type { Coordinator, CoordinatorHandlers, JoinOptions, PoolState } from "../core/coordinator.js";
import { type Logger, consoleLogger } from "../core/logger.js";
import type { Priority } from "../core/types.js";
import {
  type ClientMessage,
  type PoolStateMsg,
  REMOTE_PROTOCOL,
  type ServerMessage,
  loadGrpc,
  poolState,
  serviceDefinition,
} from "./protocol.js";

export interface RemotePoolOptions {
  /** "host:port" (plaintext) or "grpcs://host:port" (TLS). */
  url: string;
  instanceId: string;
  caller?: string;
  /** Sent as "authorization: Bearer <token>". */
  token?: string;
  /** Lease the server applies; fencing triggers `safetyMs` before it would expire. */
  leaseMs?: number;
  safetyMs?: number;
  fallback?: "local-share" | "pause";
  logger?: Logger;
  /** PEM root certificates for grpcs:// with a private CA. */
  rootCerts?: Buffer;
}

interface RemotePool {
  name: string;
  opts: JoinOptions;
  heldKnown: number;
  want: number;
  priority: Priority;
  backlog: number;
  fenced: boolean;
  joined: boolean;
  state: PoolStateMsg;
}

const RANK: Record<Priority, number> = { critical: 0, standard: 1, bulk: 2 };

/**
 * Coordinator that talks to a sluice-server over one gRPC stream. Survives
 * server restarts: it reconnects and re-registers the tokens it still holds.
 * If it stays disconnected for too long it fences itself like the Redis client.
 */
export class RemoteCoordinator implements Coordinator {
  readonly kind = "remote" as const;
  private handlers: CoordinatorHandlers | undefined;
  private readonly pools = new Map<string, RemotePool>();
  private stream: ClientDuplexStream<ClientMessage, ServerMessage> | undefined;
  private client:
    | { close(): void; Session(metadata: unknown): ClientDuplexStream<ClientMessage, ServerMessage> }
    | undefined;
  private metadata: unknown;
  private connected = false;
  private lastContact = performance.now();
  private leaseMs: number;
  private reqSeq = 0;
  private readonly pending = new Map<number, (m: ServerMessage) => void>();
  private pingTimer: NodeJS.Timeout | undefined;
  private fenceTimer: NodeJS.Timeout | undefined;
  private reconnectDelay = 100;
  private closed = false;
  private readyWaiters: { resolve: () => void; reject: (e: Error) => void }[] = [];
  /** Set when the server refused us (bad token, protocol); joins fail fast instead of waiting. */
  private refused: Error | undefined;
  private readonly logger: Logger;

  private constructor(private readonly opts: RemotePoolOptions) {
    this.leaseMs = opts.leaseMs ?? 6_000;
    this.logger = opts.logger ?? consoleLogger("warn");
  }

  static async create(opts: RemotePoolOptions): Promise<RemoteCoordinator> {
    const c = new RemoteCoordinator(opts);
    await c.init();
    return c;
  }

  private async init(): Promise<void> {
    const grpc = await loadGrpc();
    const def = await serviceDefinition();
    const Ctor = grpc.makeGenericClientConstructor(def, "Coordinator");
    const tls = this.opts.url.startsWith("grpcs://");
    const address = this.opts.url.replace(/^grpcs?:\/\//, "");
    const creds = tls ? grpc.credentials.createSsl(this.opts.rootCerts) : grpc.credentials.createInsecure();
    this.client = new Ctor(address, creds, {
      "grpc.keepalive_time_ms": 10_000,
      "grpc.keepalive_timeout_ms": 5_000,
      "grpc.keepalive_permit_without_calls": 1,
    }) as unknown as RemoteCoordinator["client"];
    const md = new grpc.Metadata();
    if (this.opts.token) md.set("authorization", `Bearer ${this.opts.token}`);
    this.metadata = md;
    this.fenceTimer = setInterval(() => this.checkFences(), 250);
    this.fenceTimer.unref?.();
    this.connect();
    await this.waitReady(5_000).catch(() => {
      this.logger.warn({ url: this.opts.url }, "sluice-server not reachable yet; will keep retrying");
    });
  }

  attach(handlers: CoordinatorHandlers): void {
    this.handlers = handlers;
  }

  private connect(): void {
    if (this.closed || !this.client) return;
    const stream = this.client.Session(this.metadata);
    this.stream = stream;
    stream.on("data", (m: ServerMessage) => this.onMessage(m));
    const drop = (why: string) => {
      if (this.stream !== stream) return;
      this.stream = undefined;
      if (this.connected) this.logger.warn({ why }, "lost connection to sluice-server");
      this.connected = false;
      for (const p of this.pools.values()) p.joined = false;
      clearInterval(this.pingTimer);
      if (!this.closed) {
        setTimeout(() => this.connect(), this.reconnectDelay).unref?.();
        this.reconnectDelay = this.refused ? 30_000 : Math.min(5_000, this.reconnectDelay * 2);
      }
    };
    stream.on("error", (e: Error) => drop(e.message));
    stream.on("end", () => drop("stream ended"));
    this.send({
      hello: {
        instanceId: this.opts.instanceId,
        caller: this.opts.caller ?? "unknown",
        protocol: REMOTE_PROTOCOL,
      },
    });
  }

  private send(m: ClientMessage): boolean {
    if (!this.stream) return false;
    try {
      this.stream.write(m);
      return true;
    } catch {
      return false;
    }
  }

  private async onWelcome(leaseMs: number): Promise<void> {
    this.connected = true;
    this.reconnectDelay = 100;
    if (leaseMs) this.leaseMs = leaseMs;
    clearInterval(this.pingTimer);
    this.pingTimer = setInterval(() => this.send({ ping: { at: Date.now() } }), 1_000);
    this.pingTimer.unref?.();
    this.refused = undefined;
    for (const w of this.readyWaiters.splice(0)) w.resolve();
    // Re-register every pool, carrying the tokens still in use (0 if we fenced meanwhile).
    for (const p of this.pools.values()) void this.rejoin(p);
  }

  private async rejoin(p: RemotePool): Promise<void> {
    const wasFenced = p.fenced;
    try {
      const state = await this.request(p, p.fenced ? 0 : p.heldKnown);
      p.joined = true;
      if (wasFenced) {
        p.fenced = false;
        p.heldKnown = 0;
        this.handlers?.restored(p.name);
      }
      this.handlers?.state(p.name, state);
      if (p.want > 0) this.sendDemand(p);
    } catch (error) {
      this.logger.warn({ pool: p.name, err: error }, "rejoin failed");
    }
  }

  private waitReady(timeoutMs: number): Promise<void> {
    if (this.connected) return Promise.resolve();
    if (this.refused) return Promise.reject(this.refused);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("timeout")), timeoutMs);
      t.unref?.();
      this.readyWaiters.push({
        resolve: () => {
          clearTimeout(t);
          resolve();
        },
        reject: (e) => {
          clearTimeout(t);
          reject(e);
        },
      });
    });
  }

  private onMessage(m: ServerMessage): void {
    this.lastContact = performance.now();
    if ("welcome" in m) return void this.onWelcome(m.welcome.leaseMs ?? 0);
    if ("joined" in m) {
      this.pending.get(m.joined.req)?.(m);
      return;
    }
    if ("left" in m) {
      this.pending.get(m.left.req)?.(m);
      return;
    }
    if ("grant" in m) {
      const p = this.pools.get(m.grant.pool);
      if (!p || p.fenced) return;
      const n = m.grant.n ?? 0;
      p.heldKnown += n;
      p.want = Math.max(0, p.want - n);
      this.handlers?.grant(p.name, n);
      return;
    }
    if ("state" in m) {
      const p = this.pools.get(m.state.pool);
      if (!p) return;
      p.state = poolState(m.state.state);
      this.handlers?.state(p.name, this.toState(p.state));
      return;
    }
    if ("fenced" in m) {
      const p = this.pools.get(m.fenced.pool);
      if (p) this.fence(p, m.fenced.fallback ?? 0);
      return;
    }
    if ("restored" in m) {
      const p = this.pools.get(m.restored.pool);
      if (p?.fenced) {
        p.fenced = false;
        p.heldKnown = 0;
        this.handlers?.restored(p.name);
      }
      return;
    }
    if ("error" in m) {
      this.logger.error({ code: m.error.code }, `sluice-server: ${m.error.message}`);
      if (m.error.code === "UNAUTHENTICATED" || m.error.code === "PROTOCOL") {
        this.refused = new Error(`sluice-server refused this instance: ${m.error.message}`);
        for (const w of this.readyWaiters.splice(0)) w.reject(this.refused);
        // Back off hard; a rotated token or upgraded server may accept us later.
        this.reconnectDelay = 30_000;
      }
    }
  }

  private toState(s: PoolStateMsg): PoolState {
    return { capacity: s.capacity, contended: s.contended, fairShare: s.fairShare };
  }

  private request(p: RemotePool, held: number): Promise<PoolState> {
    const req = ++this.reqSeq;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(req);
        reject(new Error("join timed out"));
      }, 5_000);
      timer.unref?.();
      this.pending.set(req, (m) => {
        clearTimeout(timer);
        this.pending.delete(req);
        if (!("joined" in m)) return;
        if (m.joined.error) return reject(new Error(m.joined.error));
        p.state = poolState(m.joined.state);
        resolve(this.toState(p.state));
      });
      const sent = this.send({
        join: {
          req,
          pool: p.name,
          caller: p.opts.caller,
          weight: p.opts.weight ?? 1,
          capacity: p.opts.capacity,
          held,
        },
      });
      if (!sent) {
        clearTimeout(timer);
        this.pending.delete(req);
        reject(new Error("not connected"));
      }
    });
  }

  async join(pool: string, opts: JoinOptions): Promise<PoolState> {
    const p: RemotePool = {
      name: pool,
      opts,
      heldKnown: opts.held ?? 0,
      want: 0,
      priority: "standard",
      backlog: 0,
      fenced: false,
      joined: false,
      state: { capacity: opts.capacity, contended: false, fairShare: 0, owner: false, instances: 1 },
    };
    this.pools.set(pool, p);
    if (!this.connected) await this.waitReady(5_000);
    const state = await this.request(p, p.heldKnown);
    p.joined = true;
    return state;
  }

  private sendDemand(p: RemotePool): void {
    if (!p.joined || p.fenced) return;
    this.send({ demand: { pool: p.name, want: p.want, rank: RANK[p.priority], backlog: p.backlog } });
  }

  demand(pool: string, want: number, hint: { priority: Priority; backlog: number }): void {
    const p = this.pools.get(pool);
    if (!p) return;
    p.want = want;
    p.priority = hint.priority;
    p.backlog = hint.backlog;
    this.sendDemand(p);
  }

  release(pool: string, n: number): void {
    const p = this.pools.get(pool);
    if (!p || n <= 0) return;
    p.heldKnown = Math.max(0, p.heldKnown - n);
    // While disconnected the release is implicit: rejoin re-registers only what we still hold.
    if (p.joined && !p.fenced) this.send({ release: { pool, n } });
  }

  report(pool: string, load: { held: number; inFlight: number; backlog: number }): void {
    const p = this.pools.get(pool);
    if (p?.joined)
      this.send({ report: { pool, held: load.held, inFlight: load.inFlight, backlog: load.backlog } });
  }

  setCapacity(pool: string, n: number): void {
    this.send({ setCapacity: { pool, capacity: n } });
  }

  ownsCapacity(pool: string): boolean {
    return this.pools.get(pool)?.state.owner ?? false;
  }

  async leave(pool: string): Promise<void> {
    const p = this.pools.get(pool);
    if (!p) return;
    this.pools.delete(pool);
    const req = ++this.reqSeq;
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, 1_000);
      timer.unref?.();
      this.pending.set(req, () => {
        clearTimeout(timer);
        this.pending.delete(req);
        resolve();
      });
      if (!this.send({ leave: { req, pool } })) resolve();
    });
  }

  async close(): Promise<void> {
    if (this.closed) return;
    for (const pool of [...this.pools.keys()]) await this.leave(pool);
    this.closed = true;
    clearInterval(this.pingTimer);
    clearInterval(this.fenceTimer);
    this.stream?.end();
    this.client?.close();
  }

  private fence(p: RemotePool, fallback: number): void {
    if (p.fenced) return;
    p.fenced = true;
    p.heldKnown = 0;
    p.want = 0;
    this.handlers?.fenced(p.name, fallback);
  }

  private checkFences(): void {
    if (this.connected) return;
    if (performance.now() - this.lastContact <= this.leaseMs - (this.opts.safetyMs ?? 1_500)) return;
    for (const p of this.pools.values()) {
      const fallback =
        this.opts.fallback === "pause"
          ? 0
          : Math.max(1, Math.floor(p.state.capacity / Math.max(1, p.state.instances)));
      if (!p.fenced)
        this.logger.warn({ pool: p.name, fallback }, "sluice-server unreachable; fenced held tokens");
      this.fence(p, fallback);
    }
  }
}

/** Connect to a sluice-server. Pass the result as `coordination` to createEngine. */
export function remotePool(opts: RemotePoolOptions): Promise<RemoteCoordinator> {
  return RemoteCoordinator.create(opts);
}
