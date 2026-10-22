import type { Coordinator } from "./core/coordinator.js";
import { type Engine, createEngine } from "./core/engine.js";
import { consoleLogger } from "./core/logger.js";
import type { TargetOptions } from "./core/target.js";
import type { Item } from "./core/types.js";
import { type InstallOptions, optionsFromEnv, resolveIdentity } from "./intercept/config.js";
import { type Hints, bypass, withHints } from "./intercept/context.js";
import {
  type Call,
  InterceptRuntime,
  createFetch,
  isGated,
  patchAgents,
  unpatchAgents,
} from "./intercept/runtime.js";

let installed:
  | {
      engine: Promise<Engine>;
      runtime: InterceptRuntime;
      originalFetch: typeof fetch;
      removeSignals: () => void;
    }
  | undefined;

async function makeCoordinator(
  o: InstallOptions,
  id: ReturnType<typeof resolveIdentity>,
): Promise<Coordinator | undefined> {
  if (o.serverUrl) {
    const { remotePool } = await import("./remote/index.js");
    return remotePool({ url: o.serverUrl, token: o.token, instanceId: id.instanceId, caller: id.caller });
  }
  if (o.redisUrl) {
    const { redisPool } = await import("./redis/index.js");
    return redisPool({ url: o.redisUrl, instanceId: id.instanceId, caller: id.caller, shards: o.shards });
  }
  return undefined;
}

/**
 * Gate outgoing HTTP calls of this process. Patches global fetch and http/https
 * agents; existing code keeps calling them exactly as before. Settings come
 * from the environment (SLUICE_*) and may be overridden here.
 */
export function install(options: InstallOptions = {}): Promise<Engine> {
  if (installed) return installed.engine;
  const o: InstallOptions = { ...optionsFromEnv(), ...stripUndefined(options) };
  const id = resolveIdentity(o);
  const logger = consoleLogger(o.logLevel ?? "warn", { caller: id.caller, instanceId: id.instanceId });
  const engine = makeCoordinator(o, id).then((coordination) =>
    createEngine({ ...id, weight: o.weight, coordination, logger }),
  );
  engine.catch((error) => logger.error({ err: error }, "sluice failed to start; calls pass through ungated"));
  const runtime = new InterceptRuntime(engine, {
    caller: id.caller,
    mode: o.mode ?? "all",
    exclude: o.exclude ?? [],
    headers: o.headers ?? "internal",
    defaultCapacity: o.defaultCapacity ?? 10,
    targets: o.targets ?? {},
  });
  const originalFetch = globalThis.fetch;
  if (o.fetch !== false && !isGated(originalFetch)) globalThis.fetch = createFetch(runtime, originalFetch);
  if (o.agents !== false) patchAgents(runtime);
  const removeSignals =
    o.handleSignals === false ? () => {} : hookSignals(engine, o.drainTimeoutMs ?? 25_000);
  installed = { engine, runtime, originalFetch, removeSignals };
  return engine;
}

function hookSignals(engine: Promise<Engine>, timeoutMs: number): () => void {
  let draining = false;
  const handler = (signal: NodeJS.Signals) => {
    if (draining) return;
    draining = true;
    void engine
      .then((e) => e.drain({ timeoutMs }))
      .finally(() => {
        // Re-raise for the app's own handlers, or exit if there are none.
        process.off(signal, handler);
        if (process.listenerCount(signal) === 0) process.exit(signal === "SIGINT" ? 130 : 143);
        process.emit(signal, signal);
      });
  };
  process.on("SIGTERM", handler);
  process.on("SIGINT", handler);
  return () => {
    process.off("SIGTERM", handler);
    process.off("SIGINT", handler);
  };
}

/** install() with defaults if nothing is installed yet. */
function ensureInstalled(): NonNullable<typeof installed> {
  if (!installed) install();
  return installed!;
}

/** Undo install(): restore fetch and agents, then close the engine. */
export async function uninstall(): Promise<void> {
  if (!installed) return;
  const { engine, originalFetch, removeSignals } = installed;
  installed = undefined;
  if (isGated(globalThis.fetch)) globalThis.fetch = originalFetch;
  unpatchAgents();
  removeSignals();
  await (await engine.catch(() => undefined))?.close();
}

/**
 * Gate any async function (gRPC client, SDK call) behind a named target.
 * Not retried unless `retry` is set, because sluice cannot know it is safe.
 */
export function wrap<A extends unknown[], R>(
  target: string,
  fn: (...args: A) => Promise<R>,
  opts: Partial<Omit<TargetOptions<Call>, "send" | "retrySafe">> & { retry?: TargetOptions["retry"] } = {},
): (...args: A) => Promise<R> {
  const retrySafe = opts.retry !== undefined && (opts.retry.max ?? 1) > 0;
  return async (...args: A) => {
    const rt = ensureInstalled().runtime;
    const engine = await rt.engine;
    const t = engine.resolve(target, {
      ...rt.optionsFor(target, undefined),
      ...opts,
      retry: opts.retry ?? { max: 0 },
    });
    const call: Call = { retrySafe, run: () => fn(...args) };
    return (await withHints({}, () => t.submit(call))) as R;
  };
}

function stripUndefined<T extends object>(o: T): Partial<T> {
  return Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined)) as Partial<T>;
}

export const sluice = {
  install,
  uninstall,
  /** Run `fn` with hints (priority, deadline, route, target) applied to every call it makes. */
  with: <T>(hints: Hints, fn: () => T): T => withHints(hints, fn),
  /** Run `fn` with gating switched off (health checks, user-facing paths). */
  bypass: <T>(fn: () => T): T => bypass(fn),
  wrap,
  /** The engine behind install(), for stats, health and events. */
  engine: (): Promise<Engine> => ensureInstalled().engine,
};

export type { Hints, InstallOptions, Call, Item };
