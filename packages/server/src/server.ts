import { readFileSync } from "node:fs";
import type { Server as HttpServer } from "node:http";
import * as grpc from "@grpc/grpc-js";
import { type Logger, consoleLogger } from "@viralpatel4439/sluice";
import { SubscriptionHub, fromIoredis } from "@viralpatel4439/sluice/redis";
import { type ClientMessage, type ServerMessage, serviceDefinition } from "@viralpatel4439/sluice/remote";
import { Redis } from "ioredis";
import { type PoolReader, startAdmin } from "./admin.js";
import { type ServerConfig, loadConfig } from "./config.js";
import { Session } from "./sessions.js";

export interface RunningServer {
  config: ServerConfig;
  grpcPort: number;
  httpPort: number;
  reader: PoolReader;
  sessions: () => Session[];
  close(): Promise<void>;
}

/** Start the gRPC coordinator and the admin/metrics HTTP server. */
export async function startServer(
  overrides: Partial<ServerConfig> = {},
  logger?: Logger,
): Promise<RunningServer> {
  const config = { ...loadConfig(), ...overrides };
  const log =
    logger ?? consoleLogger(config.logLevel, { component: "sluice-server", serverId: config.serverId });
  const redis = new Redis(config.redisUrl, {
    commandTimeout: 1_000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
  });
  redis.on("error", (err) => log.debug({ err }, "redis error"));
  const adapter = fromIoredis(redis);
  await adapter.ready(30_000);
  const subscriptions = await SubscriptionHub.create(adapter);

  const sessions = new Set<Session>();
  const server = new grpc.Server({
    "grpc.keepalive_time_ms": 10_000,
    "grpc.keepalive_timeout_ms": 5_000,
    "grpc.keepalive_permit_without_calls": 1,
    "grpc.http2.min_ping_interval_without_data_ms": 5_000,
    "grpc.max_receive_message_length": 64 * 1024,
  });
  server.addService(await serviceDefinition(), {
    Session: (call: grpc.ServerDuplexStream<ClientMessage, ServerMessage>) => {
      const s = new Session(call, { config, redis, subscriptions, logger: log }, (done) =>
        sessions.delete(done),
      );
      sessions.add(s);
    },
  });
  const creds =
    config.tlsCert && config.tlsKey
      ? grpc.ServerCredentials.createSsl(null, [
          { cert_chain: readFileSync(config.tlsCert), private_key: readFileSync(config.tlsKey) },
        ])
      : grpc.ServerCredentials.createInsecure();
  const grpcPort = await new Promise<number>((resolve, reject) =>
    server.bindAsync(`${config.grpcHost}:${config.grpcPort}`, creds, (err, port) =>
      err ? reject(err) : resolve(port),
    ),
  );

  const { server: http, reader } = startAdmin({
    config,
    redis,
    logger: log,
    sessions: () => [...sessions].flatMap((s) => (s.info ? [s.info] : [])),
    ready: () => redis.status === "ready",
  });
  const httpPort = await new Promise<number>((resolve) =>
    http.listen(config.httpPort, config.httpHost, () => resolve((http.address() as { port: number }).port)),
  );
  log.info(
    { grpcPort, httpPort, redis: config.redisUrl.replace(/\/\/[^@]*@/, "//***@") },
    "sluice-server listening",
  );

  let closing: Promise<void> | undefined;
  const shutdown = async () => {
    for (const s of [...sessions]) s.end("server shutting down");
    await new Promise<void>((r) => server.tryShutdown(() => r()));
    await new Promise<void>((r) => (http as HttpServer).close(() => r()));
    http.closeAllConnections?.();
    subscriptions.release();
    redis.disconnect();
  };
  const close = () => {
    closing ??= shutdown();
    return closing;
  };
  return { config, grpcPort, httpPort, reader, sessions: () => [...sessions], close };
}
