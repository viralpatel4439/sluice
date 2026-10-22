import { createHash } from "node:crypto";

/** A Lua script with its SHA1, so it can be run with EVALSHA and loaded on demand. */
export interface Script {
  readonly name: string;
  readonly lua: string;
  readonly sha: string;
}

export function script(name: string, lua: string): Script {
  return { name, lua, sha: createHash("sha1").update(lua).digest("hex") };
}

/** A pub/sub connection. */
export interface SubscriberConn {
  subscribe(channel: string): Promise<void>;
  unsubscribe(channel: string): Promise<void>;
  onMessage(fn: (channel: string, message: string) => void): void;
  /** Called after a reconnect, when subscriptions must be restored (if the client does not do it itself). */
  onReconnect(fn: () => void): void;
  close(): void;
}

/**
 * The few Redis operations sluice needs. Adapters exist for ioredis and
 * node-redis; implement this interface to use any other client.
 */
export interface RedisLike {
  readonly kind: string;
  /** Resolves once commands can be sent. */
  ready(timeoutMs?: number): Promise<void>;
  /** EVALSHA with a fallback to EVAL when the script is not cached yet. */
  evalScript(script: Script, key: string, args: string[]): Promise<string>;
  sadd(key: string, member: string): Promise<unknown>;
  srem(key: string, member: string): Promise<unknown>;
  /** HSETNX then HGET: the stored value wins. */
  hsetnxGet(key: string, field: string, value: string): Promise<string>;
  configGet(param: string): Promise<string | undefined>;
  /** Open a separate connection for pub/sub. */
  subscriber(): Promise<SubscriberConn>;
  close(): void;
}

function isNoScript(error: unknown): boolean {
  return error instanceof Error && error.message.includes("NOSCRIPT");
}

function waitFor(
  emitter: { once(e: string, fn: () => void): unknown },
  event: string,
  timeoutMs: number,
  what: string,
) {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`sluice: ${what} not ready after ${timeoutMs}ms`)),
      timeoutMs,
    );
    emitter.once(event, () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

// ---- ioredis --------------------------------------------------------------------

interface IoredisLike {
  status: string;
  evalsha(sha: string, numKeys: number, ...args: string[]): Promise<unknown>;
  eval(lua: string, numKeys: number, ...args: string[]): Promise<unknown>;
  sadd(key: string, member: string): Promise<unknown>;
  srem(key: string, member: string): Promise<unknown>;
  hsetnx(key: string, field: string, value: string): Promise<unknown>;
  hget(key: string, field: string): Promise<string | null>;
  config(op: "GET", param: string): Promise<unknown>;
  duplicate(): IoredisLike;
  subscribe(...channels: string[]): Promise<unknown>;
  unsubscribe(...channels: string[]): Promise<unknown>;
  on(event: string, fn: (...a: never[]) => void): unknown;
  once(event: string, fn: () => void): unknown;
  disconnect(): void;
  defineCommand: unknown;
}

/** Wrap an ioredis client (single node, Sentinel or Cluster). */
export function fromIoredis(client: unknown): RedisLike {
  const c = client as IoredisLike;
  return {
    kind: "ioredis",
    ready: (timeoutMs = 10_000) =>
      c.status === "ready" ? Promise.resolve() : waitFor(c, "ready", timeoutMs, "redis"),
    async evalScript(s, key, args) {
      try {
        return String(await c.evalsha(s.sha, 1, key, ...args));
      } catch (error) {
        if (!isNoScript(error)) throw error;
        return String(await c.eval(s.lua, 1, key, ...args));
      }
    },
    sadd: (key, member) => c.sadd(key, member),
    srem: (key, member) => c.srem(key, member),
    async hsetnxGet(key, field, value) {
      await c.hsetnx(key, field, value);
      return (await c.hget(key, field)) ?? value;
    },
    async configGet(param) {
      const reply = (await c.config("GET", param)) as string[];
      return reply[1];
    },
    async subscriber() {
      const sub = c.duplicate();
      sub.on("error", () => {});
      if (sub.status !== "ready") await waitFor(sub, "ready", 10_000, "redis subscriber");
      return {
        subscribe: async (ch) => void (await sub.subscribe(ch)),
        unsubscribe: async (ch) => void (await sub.unsubscribe(ch)),
        onMessage: (fn) => void sub.on("message", fn as (...a: never[]) => void),
        // Registered after the first "ready", so every later "ready" is a reconnect.
        onReconnect: (fn) => void sub.on("ready", fn),
        close: () => sub.disconnect(),
      };
    },
    close: () => c.disconnect(),
  };
}

// ---- node-redis (redis@4/5) ---------------------------------------------------------

interface NodeRedisLike {
  isReady: boolean;
  isOpen: boolean;
  evalSha(sha: string, opts: { keys: string[]; arguments: string[] }): Promise<unknown>;
  eval(lua: string, opts: { keys: string[]; arguments: string[] }): Promise<unknown>;
  sAdd(key: string, member: string): Promise<unknown>;
  sRem(key: string, member: string): Promise<unknown>;
  hSetNX(key: string, field: string, value: string): Promise<unknown>;
  hGet(key: string, field: string): Promise<string | null | undefined>;
  configGet(param: string): Promise<Record<string, string>>;
  duplicate(): NodeRedisLike;
  connect(): Promise<unknown>;
  subscribe(channel: string, listener: (message: string, channel: string) => void): Promise<unknown>;
  unsubscribe(channel: string): Promise<unknown>;
  on(event: string, fn: (...a: never[]) => void): unknown;
  once(event: string, fn: () => void): unknown;
  disconnect?(): Promise<unknown>;
  destroy?(): unknown;
  quit?(): Promise<unknown>;
}

function closeNodeRedis(c: NodeRedisLike): void {
  if (typeof c.destroy === "function") c.destroy();
  else if (typeof c.disconnect === "function") void c.disconnect().catch(() => {});
  else void c.quit?.().catch(() => {});
}

/** Wrap a node-redis client (`createClient()` from the "redis" package, v4 or v5). */
export function fromNodeRedis(client: unknown): RedisLike {
  const c = client as NodeRedisLike;
  return {
    kind: "node-redis",
    async ready(timeoutMs = 10_000) {
      if (c.isReady) return;
      if (!c.isOpen) await c.connect();
      if (!c.isReady) await waitFor(c, "ready", timeoutMs, "redis");
    },
    async evalScript(s, key, args) {
      const opts = { keys: [key], arguments: args };
      try {
        return String(await c.evalSha(s.sha, opts));
      } catch (error) {
        if (!isNoScript(error)) throw error;
        return String(await c.eval(s.lua, opts));
      }
    },
    sadd: (key, member) => c.sAdd(key, member),
    srem: (key, member) => c.sRem(key, member),
    async hsetnxGet(key, field, value) {
      await c.hSetNX(key, field, value);
      return (await c.hGet(key, field)) ?? value;
    },
    async configGet(param) {
      return (await c.configGet(param))[param];
    },
    async subscriber() {
      const sub = c.duplicate();
      sub.on("error", () => {});
      await sub.connect();
      let handler: (channel: string, message: string) => void = () => {};
      return {
        // node-redis restores subscriptions after reconnecting by itself.
        subscribe: async (ch) =>
          void (await sub.subscribe(ch, (message, channel) => handler(channel, message))),
        unsubscribe: async (ch) => void (await sub.unsubscribe(ch)),
        onMessage: (fn) => {
          handler = fn;
        },
        onReconnect: () => {},
        close: () => closeNodeRedis(sub),
      };
    },
    close: () => closeNodeRedis(c),
  };
}

/** Accepts an ioredis client, a node-redis client, or a ready-made RedisLike. */
export function toRedisLike(client: unknown): RedisLike {
  const c = client as Record<string, unknown>;
  if (typeof c.evalScript === "function" && typeof c.subscriber === "function") return client as RedisLike;
  if (typeof c.defineCommand === "function" && typeof c.evalsha === "function") return fromIoredis(client);
  if (typeof c.evalSha === "function" && typeof c.sAdd === "function") return fromNodeRedis(client);
  throw new Error(
    "sluice: unsupported Redis client; pass an ioredis or node-redis client, or a RedisLike adapter",
  );
}

/** Open a connection from a URL with whichever client is installed (ioredis first, then node-redis). */
export async function connect(url: string, ioredisOptions: Record<string, unknown> = {}): Promise<RedisLike> {
  try {
    const { Redis } = (await import("ioredis")) as unknown as {
      Redis: new (url: string, o: object) => unknown;
    };
    const client = new Redis(url, {
      commandTimeout: 1_000,
      maxRetriesPerRequest: 1,
      enableOfflineQueue: false,
      ...ioredisOptions,
    }) as IoredisLike;
    client.on("error", () => {});
    return fromIoredis(client);
  } catch (error) {
    if (!(error instanceof Error) || !/Cannot find (package|module)/.test(error.message)) throw error;
  }
  try {
    const { createClient } = (await import("redis" as string)) as { createClient: (o: object) => unknown };
    const client = createClient({ url, socket: { connectTimeout: 2_000 } }) as NodeRedisLike;
    client.on("error", () => {});
    await client.connect();
    return fromNodeRedis(client);
  } catch (error) {
    if (error instanceof Error && /Cannot find (package|module)/.test(error.message)) {
      throw new Error("sluice/redis needs a Redis client: npm i ioredis (or npm i redis)");
    }
    throw error;
  }
}
