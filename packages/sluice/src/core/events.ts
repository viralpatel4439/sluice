import type { Priority } from "./types.js";

export interface ItemEvent {
  target: string;
  key: string;
  priority: Priority;
  route: string | undefined;
  attempt: number;
}

export interface EngineEvents {
  "item:sent": ItemEvent;
  "item:done": ItemEvent & { durationMs: number; waitMs: number };
  "item:retry": ItemEvent & { delayMs: number; reason: string };
  "item:expired": ItemEvent;
  "item:dead": ItemEvent & { error: unknown };
  "item:dropped": ItemEvent;
  "tokens:changed": { target: string; held: number; inFlight: number };
  "capacity:changed": { target: string; capacity: number; previous: number; source: string };
  "fallback:on": { target: string; capacity: number };
  "fallback:off": { target: string };
  "circuit:open": { target: string };
  "circuit:close": { target: string };
  "coordinator:slow": { op: string; durationMs: number };
  "config:mismatch": { target: string; field: string };
  "target:created": { target: string };
  "target:evicted": { target: string };
  error: { error: unknown; where: string };
}

export type EventName = keyof EngineEvents;
type Listener<K extends EventName> = (payload: EngineEvents[K]) => void;

/** Minimal typed emitter. A throwing listener never breaks the engine. */
export class Emitter {
  private listeners = new Map<EventName, Set<Listener<EventName>>>();

  on<K extends EventName>(event: K, fn: Listener<K>): () => void {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    set.add(fn as Listener<EventName>);
    return () => set.delete(fn as Listener<EventName>);
  }

  emit<K extends EventName>(event: K, payload: EngineEvents[K]): void {
    const set = this.listeners.get(event);
    if (!set) return;
    for (const fn of set) {
      try {
        fn(payload);
      } catch (error) {
        if (event !== "error") this.emit("error", { error, where: `listener:${event}` });
      }
    }
  }

  has(event: EventName): boolean {
    return (this.listeners.get(event)?.size ?? 0) > 0;
  }
}
