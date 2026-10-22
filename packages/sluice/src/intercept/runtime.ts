import http from "node:http";
import https from "node:https";
import type { CapacityConfig } from "../core/capacity.js";
import type { Engine } from "../core/engine.js";
import { DeadlineExceededError } from "../core/errors.js";
import { ulid } from "../core/ids.js";
import { isRetrySafe } from "../core/retry.js";
import type { TargetOptions } from "../core/target.js";
import type { Item, SubmitOptions } from "../core/types.js";
import { currentContext } from "./context.js";
import { exclusionMatcher, isInternalHost, targetNameFor } from "./hosts.js";

/** What every intercepted call carries as its payload: how to (re)send it and whether that is safe. */
export interface Call {
  run(item: Item<Call>, signal: AbortSignal): Promise<unknown>;
  retrySafe: boolean;
}

export interface RuntimeOptions {
  /** This service's caller name, sent as X-Sluice-Caller. */
  caller: string;
  /** "all": gate every outbound call. "context": only calls inside `sluice.with()`. */
  mode: "all" | "context";
  exclude: string[];
  /** Which hosts receive Idempotency-Key and X-Sluice-* headers. */
  headers: "internal" | "all" | "none";
  /** Starting capacity for auto-discovered targets. */
  defaultCapacity: number;
  /** Per-target overrides by glob, e.g. {"inventory": {capacity: 50}}. */
  targets: Record<
    string,
    Partial<Pick<TargetOptions, "capacity" | "timeoutMs" | "retry" | "queue" | "routes">>
  >;
}

const GATED = Symbol.for("sluice.gated");

function globToRegExp(glob: string): RegExp {
  return new RegExp(`^${glob.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*")}$`, "i");
}

/** Shared by the fetch and http.Agent interceptors. */
export class InterceptRuntime {
  private readonly excluded: (url: URL) => boolean;
  private readonly overrides: { re: RegExp; opts: RuntimeOptions["targets"][string] }[];

  constructor(
    readonly engine: Promise<Engine>,
    readonly opts: RuntimeOptions,
  ) {
    this.excluded = exclusionMatcher(opts.exclude);
    this.overrides = Object.entries(opts.targets).map(([glob, o]) => ({ re: globToRegExp(glob), opts: o }));
  }

  shouldGate(url: URL): boolean {
    if (url.protocol !== "http:" && url.protocol !== "https:") return false;
    const ctx = currentContext();
    if (ctx?.bypass) return false;
    if (this.opts.mode === "context" && !ctx?.gated) return false;
    return !this.excluded(url);
  }

  sendHeaders(url: URL): boolean {
    return this.opts.headers === "all" || (this.opts.headers === "internal" && isInternalHost(url.hostname));
  }

  /** Submit a call for `url`, creating the target on first use. */
  async submit(url: URL, call: Call, extra: SubmitOptions = {}): Promise<unknown> {
    const engine = await this.engine;
    const ctx = currentContext();
    const name = ctx?.target ?? targetNameFor(url);
    const target = engine.resolve(name, this.optionsFor(name, url));
    return target.submit(call, {
      priority: ctx?.priority,
      deadlineMs: ctx?.deadlineMs,
      route: ctx?.route,
      key: ctx?.key,
      ...extra,
    });
  }

  optionsFor(name: string, url: URL | undefined): TargetOptions<Call> {
    const override = this.overrides.find((o) => o.re.test(name))?.opts ?? {};
    const internal = url ? isInternalHost(url.hostname) : false;
    const capacity: CapacityConfig | number = override.capacity ?? {
      type: "auto",
      start: this.opts.defaultCapacity,
      url: internal && url ? `${url.origin}/_capacity` : undefined,
    };
    return {
      ...override,
      capacity,
      send: (item, signal) => item.payload.run(item, signal),
      retrySafe: (item) => item.payload.retrySafe,
    };
  }
}

// ---- fetch -------------------------------------------------------------------

function isReplayable(body: unknown): boolean {
  if (body === undefined || body === null) return true;
  return (
    !(body instanceof ReadableStream) &&
    typeof (body as AsyncIterable<unknown>)[Symbol.asyncIterator] !== "function"
  );
}

function bodySize(init: RequestInit | undefined, headers: Headers): number {
  const len = headers.get("content-length");
  if (len && /^\d+$/.test(len)) return Number(len);
  const body = init?.body;
  if (typeof body === "string") return Buffer.byteLength(body);
  if (body instanceof ArrayBuffer || ArrayBuffer.isView(body)) return body.byteLength;
  if (body instanceof Blob) return body.size;
  return 0;
}

/** A drop-in `fetch` that waits for a token before each request and returns the target's real Response. */
export function createFetch(rt: InterceptRuntime, original: typeof fetch): typeof fetch {
  const gatedFetch = async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    let url: URL;
    try {
      url = new URL(input instanceof Request ? input.url : String(input));
    } catch {
      return original(input, init);
    }
    if (!rt.shouldGate(url)) return original(input, init);

    const base = new Request(input, init);
    const userSignal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
    const replayable =
      isReplayable(init?.body ?? (input instanceof Request ? input.body : undefined)) || base.body === null;
    const userHeaders = Object.fromEntries(base.headers);
    const addHeaders = rt.sendHeaders(url);
    const call: Call = {
      retrySafe: replayable && isRetrySafe(base.method, userHeaders),
      run: (item, signal) => {
        const headers = new Headers(base.headers);
        if (addHeaders) {
          for (const [k, v] of Object.entries(item.headers)) if (!headers.has(k)) headers.set(k, v);
        }
        const attempt = new Request(replayable ? base.clone() : base, {
          headers,
          signal,
          duplex: "half",
        } as RequestInit);
        return original(attempt);
      },
    };
    const key = base.headers.get("idempotency-key") ?? undefined;
    try {
      return (await rt.submit(url, call, {
        key,
        signal: userSignal ?? undefined,
        size: bodySize(init, base.headers),
      })) as Response;
    } catch (error) {
      throw toFetchError(error);
    }
  };
  Object.defineProperty(gatedFetch, GATED, { value: true });
  return gatedFetch as typeof fetch;
}

/** Keep fetch's error contract: deadline → AbortError DOMException. */
function toFetchError(error: unknown): unknown {
  if (error instanceof DeadlineExceededError) {
    // DOMException.code is read-only; the sluice code travels as `sluiceCode`, the original as `cause`.
    const abort = new DOMException(error.message, "AbortError");
    Object.defineProperties(abort, { cause: { value: error }, sluiceCode: { value: error.code } });
    return abort;
  }
  return error;
}

export function isGated(fn: unknown): boolean {
  return typeof fn === "function" && (fn as unknown as Record<symbol, unknown>)[GATED] === true;
}

// ---- http / https agents (axios, got, node-fetch, request…) ----------------------

interface AgentRequestOptions {
  protocol?: string;
  host?: string;
  hostname?: string;
  port?: number | string;
  path?: string;
}

type AddRequest = (this: http.Agent, req: http.ClientRequest, options: AgentRequestOptions) => void;

let originalAddRequest: AddRequest | undefined;

/**
 * Gate every http/https request by wrapping Agent#addRequest: the request waits
 * for a token before it is handed a socket. Bodies are streamed, so these
 * requests are never retried by sluice; the token covers the time until
 * response headers arrive.
 */
export function patchAgents(rt: InterceptRuntime): void {
  if (originalAddRequest) return;
  const proto = http.Agent.prototype as unknown as { addRequest: AddRequest };
  const original = proto.addRequest;
  originalAddRequest = original;
  proto.addRequest = function (this: http.Agent, req, options) {
    const marked = req as http.ClientRequest & { [GATED]?: boolean };
    if (marked[GATED]) return original.call(this, req, options);
    const agentProtocol =
      (this as unknown as { protocol?: string }).protocol ??
      (this instanceof https.Agent ? "https:" : "http:");
    const protocol = options.protocol ?? agentProtocol;
    const host = options.hostname ?? options.host ?? "localhost";
    let url: URL;
    try {
      url = new URL(
        `${protocol}//${host.includes(":") && !host.startsWith("[") ? `[${host}]` : host}${options.port ? `:${options.port}` : ""}${options.path ?? "/"}`,
      );
    } catch {
      return original.call(this, req, options);
    }
    if (!rt.shouldGate(url)) return original.call(this, req, options);
    const controller = new AbortController();
    const onEarlyClose = () => controller.abort(new Error("request closed before it was sent"));
    req.once("close", onEarlyClose);
    // addRequest runs inside the ClientRequest constructor, before the caller writes a body,
    // so headers can still be set here. The key is fixed now because these requests are sent once.
    const existingKey = req.getHeader("idempotency-key");
    const key = typeof existingKey === "string" ? existingKey : ulid();
    if (rt.sendHeaders(url) && !req.headersSent) {
      if (existingKey === undefined) req.setHeader("idempotency-key", key);
      if (!req.hasHeader("x-sluice-caller")) req.setHeader("x-sluice-caller", rt.opts.caller);
      req.setHeader("x-sluice-attempt", "1");
    }
    const call: Call = {
      retrySafe: false,
      run: (item, signal) =>
        new Promise((resolve, reject) => {
          req.off("close", onEarlyClose);
          if (req.destroyed) return reject(new Error("request destroyed before it was sent"));
          const onAbort = () =>
            req.destroy(signal.reason instanceof Error ? signal.reason : new Error("aborted"));
          const onResponse = (res: http.IncomingMessage) => {
            cleanup();
            resolve({
              status: res.statusCode ?? 0,
              headers: { get: (n: string) => headerValue(res.headers[n.toLowerCase()]) },
              body: null,
            });
          };
          const onError = (err: Error) => {
            cleanup();
            reject(err);
          };
          const onClose = () => {
            cleanup();
            reject(new Error("request closed before a response"));
          };
          const cleanup = () => {
            signal.removeEventListener("abort", onAbort);
            req.off("response", onResponse);
            req.off("error", onError);
            req.off("close", onClose);
          };
          signal.addEventListener("abort", onAbort, { once: true });
          req.on("response", onResponse);
          req.on("error", onError);
          req.on("close", onClose);
          marked[GATED] = true;
          original.call(this, req, options);
        }),
    };
    rt.submit(url, call, { signal: controller.signal, key }).catch((error) => {
      if (!req.destroyed && !marked[GATED])
        req.destroy(error instanceof Error ? error : new Error(String(error)));
    });
  };
}

export function unpatchAgents(): void {
  if (!originalAddRequest) return;
  (http.Agent.prototype as unknown as { addRequest: AddRequest }).addRequest = originalAddRequest;
  originalAddRequest = undefined;
}

function headerValue(v: string | string[] | undefined): string | null {
  if (v === undefined) return null;
  return Array.isArray(v) ? v.join(", ") : v;
}
