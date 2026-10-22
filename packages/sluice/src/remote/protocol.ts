import type { ServiceDefinition } from "@grpc/grpc-js";
import { PROTO_TEXT } from "./proto-text.js";

export const REMOTE_PROTOCOL = 1;

export interface PoolStateMsg {
  capacity: number;
  contended: boolean;
  fairShare: number;
  owner: boolean;
  instances: number;
}

export type ClientMessage =
  | { hello: { instanceId: string; caller: string; protocol: number } }
  | { join: { req: number; pool: string; caller: string; weight: number; capacity: number; held: number } }
  | { demand: { pool: string; want: number; rank: number; backlog: number } }
  | { release: { pool: string; n: number } }
  | { report: { pool: string; held: number; inFlight: number; backlog: number } }
  | { setCapacity: { pool: string; capacity: number } }
  | { leave: { req: number; pool: string } }
  | { ping: { at: number } };

export type ServerMessage =
  | { welcome: { serverId: string; protocol: number; leaseMs: number } }
  | { joined: { req: number; pool: string; state?: PoolStateMsg; error: string } }
  | { grant: { pool: string; n: number } }
  | { state: { pool: string; state?: PoolStateMsg } }
  | { fenced: { pool: string; fallback: number } }
  | { restored: { pool: string } }
  | { pong: { at: number } }
  | { error: { code: string; message: string } }
  | { left: { req: number } };

async function loadProtobuf(): Promise<typeof import("protobufjs")> {
  try {
    const mod = (await import("protobufjs")) as typeof import("protobufjs") & {
      default?: typeof import("protobufjs");
    };
    return mod.default ?? mod;
  } catch {
    throw new Error(
      'sluice remote coordination needs "protobufjs" and "@grpc/grpc-js": npm i protobufjs @grpc/grpc-js',
    );
  }
}

/** Builds the gRPC service definition straight from the embedded .proto text. */
export async function serviceDefinition(): Promise<ServiceDefinition> {
  const protobuf = await loadProtobuf();
  const root = protobuf.parse(PROTO_TEXT, { keepCase: false }).root;
  const Client = root.lookupType("sluice.v1.ClientMessage");
  const Server = root.lookupType("sluice.v1.ServerMessage");
  const toObject = { longs: Number, defaults: true, oneofs: false } as const;
  const strip = (o: Record<string, unknown>) => {
    // Keep only the populated oneof member so messages read as { kind: {...} }.
    for (const key of Object.keys(o)) if (o[key] === null || o[key] === undefined) delete o[key];
    return o;
  };
  return {
    Session: {
      path: "/sluice.v1.Coordinator/Session",
      requestStream: true,
      responseStream: true,
      requestSerialize: (m: ClientMessage) => Buffer.from(Client.encode(Client.fromObject(m)).finish()),
      requestDeserialize: (b: Buffer) =>
        strip(Client.toObject(Client.decode(b), { ...toObject, defaults: false }) as Record<string, unknown>),
      responseSerialize: (m: ServerMessage) => Buffer.from(Server.encode(Server.fromObject(m)).finish()),
      responseDeserialize: (b: Buffer) =>
        strip(Server.toObject(Server.decode(b), { ...toObject, defaults: false }) as Record<string, unknown>),
    },
  } as unknown as ServiceDefinition;
}

/** Fill proto3 defaults that toObject({defaults:false}) leaves out. */
export function poolState(s: Partial<PoolStateMsg> | undefined): PoolStateMsg {
  return {
    capacity: s?.capacity ?? 0,
    contended: s?.contended ?? false,
    fairShare: s?.fairShare ?? 0,
    owner: s?.owner ?? false,
    instances: s?.instances ?? 1,
  };
}

export async function loadGrpc(): Promise<typeof import("@grpc/grpc-js")> {
  try {
    return await import("@grpc/grpc-js");
  } catch {
    throw new Error('sluice remote coordination needs "@grpc/grpc-js": npm i @grpc/grpc-js protobufjs');
  }
}
