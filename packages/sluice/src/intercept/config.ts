import { readFileSync } from "node:fs";
import { hostname } from "node:os";
import { join } from "node:path";
import type { LogLevel } from "../core/logger.js";
import type { RuntimeOptions } from "./runtime.js";

export interface InstallOptions extends Partial<RuntimeOptions> {
  namespace?: string;
  caller?: string;
  instanceId?: string;
  weight?: number;
  /** Connect to a sluice-server (gRPC), e.g. "sluice-server:7070". Takes precedence over redisUrl. */
  serverUrl?: string;
  /** Coordinate directly through Redis. */
  redisUrl?: string;
  /** Direct-Redis mode: split every pool into this many shards (first instance's value wins fleet-wide). */
  shards?: number;
  /** Bearer token for sluice-server. */
  token?: string;
  logLevel?: LogLevel;
  /** Patch global fetch (default true). */
  fetch?: boolean;
  /** Patch http/https agents (default true). */
  agents?: boolean;
  /** Drain on SIGTERM/SIGINT (default true). */
  handleSignals?: boolean;
  drainTimeoutMs?: number;
}

function sanitize(name: string): string {
  return (
    name
      .replace(/^@/, "")
      .replace(/[^A-Za-z0-9._:-]/g, "-")
      .slice(0, 200) || "unknown"
  );
}

function packageName(cwd: string): string | undefined {
  try {
    return (JSON.parse(readFileSync(join(cwd, "package.json"), "utf8")) as { name?: string }).name;
  } catch {
    return undefined;
  }
}

function list(v: string | undefined): string[] {
  return v
    ? v
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : [];
}

/** Reads every setting from the environment, so `--import …/register` needs no code at all. */
export function optionsFromEnv(env: NodeJS.ProcessEnv = process.env, cwd = process.cwd()): InstallOptions {
  let targets: RuntimeOptions["targets"] = {};
  if (env.SLUICE_CONFIG) {
    const file = JSON.parse(readFileSync(env.SLUICE_CONFIG, "utf8")) as {
      targets?: RuntimeOptions["targets"];
    };
    targets = file.targets ?? {};
  } else if (env.SLUICE_TARGETS) {
    targets = JSON.parse(env.SLUICE_TARGETS) as RuntimeOptions["targets"];
  }
  const mode = env.SLUICE_MODE === "context" ? "context" : "all";
  const headers =
    env.SLUICE_HEADERS === "all" || env.SLUICE_HEADERS === "none" ? env.SLUICE_HEADERS : "internal";
  return {
    namespace: env.SLUICE_NAMESPACE,
    caller: env.SLUICE_CALLER ?? env.OTEL_SERVICE_NAME ?? packageName(cwd),
    instanceId: env.SLUICE_INSTANCE_ID ?? env.HOSTNAME,
    weight: env.SLUICE_WEIGHT ? Number(env.SLUICE_WEIGHT) : undefined,
    serverUrl: env.SLUICE_SERVER_URL,
    redisUrl: env.SLUICE_REDIS_URL,
    shards: env.SLUICE_SHARDS && /^\d+$/.test(env.SLUICE_SHARDS) ? Number(env.SLUICE_SHARDS) : undefined,
    token: env.SLUICE_TOKEN,
    logLevel: (env.SLUICE_LOG_LEVEL as LogLevel | undefined) ?? undefined,
    mode,
    headers,
    exclude: list(env.SLUICE_EXCLUDE),
    defaultCapacity: env.SLUICE_DEFAULT_CAPACITY ? Number(env.SLUICE_DEFAULT_CAPACITY) : undefined,
    targets,
    drainTimeoutMs: env.SLUICE_DRAIN_TIMEOUT_MS ? Number(env.SLUICE_DRAIN_TIMEOUT_MS) : undefined,
  };
}

export function resolveIdentity(o: InstallOptions): {
  namespace: string;
  caller: string;
  instanceId: string;
} {
  const caller = sanitize(o.caller ?? "unknown");
  return {
    namespace: sanitize(o.namespace ?? "default"),
    caller,
    instanceId: sanitize(o.instanceId ?? `${hostname()}-${process.pid}`),
  };
}
