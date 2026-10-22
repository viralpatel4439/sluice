import { hostname } from "node:os";

export interface ServerConfig {
  redisUrl: string;
  keyPrefix: string;
  grpcHost: string;
  grpcPort: number;
  httpHost: string;
  httpPort: number;
  serverId: string;
  /** Lease applied to each connected instance. */
  leaseMs: number;
  renewMs: number;
  /** caller → token. Empty: no auth on gRPC. "*" maps a token any caller may use. */
  tokens: Map<string, string>;
  /** Required for write actions in the control panel/API. Unset: actions disabled. */
  adminToken: string | undefined;
  /** Required for reads when set. */
  viewToken: string | undefined;
  logLevel: "debug" | "info" | "warn" | "error";
  tlsCert: string | undefined;
  tlsKey: string | undefined;
  /** Shard counts by target glob, e.g. "inventory=4,payments-*=2". */
  shards: [RegExp, number][];
  /** Pools with no instance for this long are deleted (unless pinned by an operator). */
  poolGcMs: number;
}

function parseTokens(raw: string | undefined): Map<string, string> {
  const map = new Map<string, string>();
  for (const pair of (raw ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)) {
    const i = pair.indexOf(":");
    if (i === -1) map.set("*", pair);
    else map.set(pair.slice(0, i), pair.slice(i + 1));
  }
  return map;
}

function parseShards(raw: string | undefined): [RegExp, number][] {
  const out: [RegExp, number][] = [];
  for (const pair of (raw ?? "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean)) {
    const [glob, n] = pair.split("=");
    const k = Number(n);
    if (!glob || !Number.isInteger(k) || k < 1) continue;
    out.push([new RegExp(`^${glob.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*")}$`), k]);
  }
  return out;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): ServerConfig {
  const num = (v: string | undefined, d: number) => (v && Number.isFinite(Number(v)) ? Number(v) : d);
  return {
    redisUrl: env.SLUICE_REDIS_URL ?? env.REDIS_URL ?? "redis://localhost:6379",
    keyPrefix: env.SLUICE_KEY_PREFIX ?? "sluice",
    grpcHost: env.SLUICE_GRPC_HOST ?? "0.0.0.0",
    grpcPort: num(env.SLUICE_GRPC_PORT, 7070),
    httpHost: env.SLUICE_HTTP_HOST ?? "0.0.0.0",
    httpPort: num(env.SLUICE_HTTP_PORT, 8080),
    serverId: env.SLUICE_SERVER_ID ?? env.HOSTNAME ?? hostname(),
    leaseMs: num(env.SLUICE_LEASE_MS, 6_000),
    renewMs: num(env.SLUICE_RENEW_MS, 2_000),
    tokens: parseTokens(env.SLUICE_TOKENS),
    adminToken: env.SLUICE_ADMIN_TOKEN || undefined,
    viewToken: env.SLUICE_VIEW_TOKEN || undefined,
    logLevel: (env.SLUICE_LOG_LEVEL as ServerConfig["logLevel"]) ?? "info",
    tlsCert: env.SLUICE_TLS_CERT,
    tlsKey: env.SLUICE_TLS_KEY,
    shards: parseShards(env.SLUICE_SHARDS),
    poolGcMs: num(env.SLUICE_POOL_GC_MS, 10 * 60_000),
  };
}
