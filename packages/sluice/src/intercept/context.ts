import { AsyncLocalStorage } from "node:async_hooks";
import type { Priority } from "../core/types.js";

/** Per-call hints, inherited by every call made inside `sluice.with()`. */
export interface Hints {
  priority?: Priority;
  deadlineMs?: number;
  route?: string;
  /** Force a target name instead of deriving it from the host. */
  target?: string;
  key?: string;
}

interface Store extends Hints {
  bypass?: boolean;
  /** Set inside `with()`; in "context" mode only these calls are gated. */
  gated?: boolean;
}

const storage = new AsyncLocalStorage<Store>();

export function currentContext(): Store | undefined {
  return storage.getStore();
}

export function withHints<T>(hints: Hints, fn: () => T): T {
  return storage.run({ ...storage.getStore(), ...hints, gated: true, bypass: false }, fn);
}

export function bypass<T>(fn: () => T): T {
  return storage.run({ ...storage.getStore(), bypass: true }, fn);
}
