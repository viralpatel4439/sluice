export { RemoteCoordinator, remotePool, type RemotePoolOptions } from "./client.js";
export {
  serviceDefinition,
  poolState,
  REMOTE_PROTOCOL,
  type ClientMessage,
  type ServerMessage,
  type PoolStateMsg,
} from "./protocol.js";
export type { Coordinator, CoordinatorHandlers, JoinOptions, PoolState } from "../core/coordinator.js";
export type { Logger, LogLevel } from "../core/logger.js";
export type { Priority } from "../core/types.js";
