import fc from "fast-check";
import { Redis } from "ioredis";
import { afterAll, describe, expect, inject, it } from "vitest";
import { apportion } from "../src/core/coordinator.js";
import { type Script, fromIoredis } from "../src/redis/adapters.js";
import { script } from "../src/redis/adapters.js";
import { JOIN, LEAVE, RENEW, SETCAP, SYNC } from "../src/redis/scripts.js";

/**
 * Model-based test of the Lua token pool. A small TypeScript reference model
 * states what each script is supposed to do; random operation sequences run
 * against both the model and real Redis, and their full state is compared after
 * every step (counters, every instance's held/want/backlog, the waiters order,
 * and what each script returns).
 */

const url = inject("redisUrl");
const redis = new Redis(url);
const client = fromIoredis(redis);
afterAll(() => redis.disconnect());

const S: Record<string, Script> = {
  join: script("join", JOIN),
  sync: script("sync", SYNC),
  renew: script("renew", RENEW),
  setcap: script("setcap", SETCAP),
  leave: script("leave", LEAVE),
};
const LEASE = 1e9; // no lease expires during a test
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Inst {
  held: number;
  want: number;
  backlog: number;
  weight: number;
  rank: number;
  since: number | undefined;
  lease: number;
  seq: number | undefined;
  res: string | undefined;
  drift: number;
  dgs: number | undefined;
  dmiss: number;
  waiting: number | undefined; // waiter score
}

/** Reference model of scripts.ts (no lease expiry, no owner claims). */
class Model {
  cap = 0;
  free = 0;
  debt = 0;
  exists = false;
  t = 0;
  inst = new Map<string, Inst>();
  /** Per-instance grant counter; survives leave/rejoin like the Lua 'gseq' hash. */
  gseq = new Map<string, number>();

  private leaseOrder(): string[] {
    return [...this.inst.entries()]
      .sort((a, b) => a[1].lease - b[1].lease || (a[0] < b[0] ? -1 : 1))
      .map(([id]) => id);
  }
  private waiters(): string[] {
    return [...this.inst.entries()]
      .filter(([, i]) => i.waiting !== undefined)
      .sort((a, b) => a[1].waiting! - b[1].waiting! || (a[0] < b[0] ? -1 : 1))
      .map(([id]) => id);
  }
  private returnTokens(n: number) {
    if (n <= 0) return;
    const pay = Math.min(n, this.debt);
    this.debt -= pay;
    this.free += n - pay;
  }
  private take(n: number) {
    if (n <= 0) return;
    const f = Math.min(n, this.free);
    this.free -= f;
    this.debt += n - f;
  }
  shares(): Map<string, number> {
    const ids = this.leaseOrder();
    const w = new Map<string, number>();
    for (const id of ids) {
      const i = this.inst.get(id)!;
      const b = i.backlog === 0 && i.held > 0 ? 1 : i.backlog;
      w.set(id, i.weight * b);
    }
    return apportion(this.cap, w);
  }
  private distribute(me: string | undefined): number {
    if (this.free <= 0) return 0;
    const ws = this.waiters();
    if (ws.length === 0) return 0;
    const shares = this.shares();
    const grant = new Map<string, number>(ws.map((id) => [id, 0]));
    for (const pass of [1, 2]) {
      let progress = true;
      while (this.free > 0 && progress) {
        progress = false;
        for (const id of ws) {
          if (this.free === 0) break;
          const i = this.inst.get(id)!;
          if (i.want > 0 && (pass === 2 || i.held < (shares.get(id) ?? 0))) {
            this.free--;
            i.want--;
            i.held++;
            grant.set(id, grant.get(id)! + 1);
            progress = true;
          }
        }
      }
    }
    let mine = 0;
    for (const id of ws) {
      const i = this.inst.get(id)!;
      if (grant.get(id)! > 0) {
        if (i.want <= 0) i.waiting = undefined;
        this.gseq.set(id, (this.gseq.get(id) ?? 0) + 1);
        if (id === me) mine = grant.get(id)!;
      }
    }
    return mine;
  }
  state(me: string) {
    const shares = this.shares();
    const contended = this.waiters().some((id) => {
      const i = this.inst.get(id)!;
      return id !== me && i.want > 0 && i.held < (shares.get(id) ?? 0);
    });
    return {
      cap: this.cap,
      free: this.free,
      debt: this.debt,
      contended,
      share: shares.get(me) ?? 0,
      instances: this.inst.size,
      held: this.inst.get(me)?.held ?? 0,
    };
  }
  join(id: string, initCap: number, weight: number, heldNow: number) {
    this.t += 1;
    if (!this.exists) {
      this.exists = true;
      this.cap = initCap;
      this.free = initCap;
      this.debt = 0;
    }
    const old = this.inst.get(id);
    if (old) {
      this.inst.delete(id);
      this.returnTokens(old.held);
    }
    this.inst.set(id, {
      held: heldNow,
      want: 0,
      backlog: 0,
      weight,
      rank: 1,
      since: undefined,
      lease: this.t + LEASE,
      seq: undefined,
      res: undefined,
      drift: 0,
      dgs: undefined,
      dmiss: 0,
      waiting: undefined,
    });
    this.take(heldNow);
    return this.state(id);
  }
  sync(
    id: string,
    seq: number,
    release: number,
    wantIn: number,
    rank: number,
    backlog: number,
    heldKnown: number,
  ) {
    this.t += 1;
    const i = this.inst.get(id);
    if (!i) return { err: "NOPOOL" };
    if (i.seq !== undefined && seq <= i.seq) return JSON.parse(i.res!);
    i.lease = this.t + LEASE;
    if (release > 0) {
      const r = Math.min(release, i.held);
      i.held -= r;
      this.returnTokens(r);
    }
    const inTransit = Math.max(0, i.held - heldKnown);
    const want = Math.max(0, wantIn - inTransit);
    i.want = want;
    i.backlog = backlog;
    i.rank = rank;
    if (want > 0) {
      if (i.waiting === undefined || i.since === undefined) i.since = this.t;
      i.waiting = rank * 1e13 + i.since;
    } else {
      i.waiting = undefined;
      i.since = undefined;
    }
    const granted = this.distribute(id);
    const res = { ...this.state(id), granted, seq };
    i.seq = seq;
    i.res = JSON.stringify(res);
    return res;
  }
  /** cg: highest grant number the client has applied. Drift is only trusted once every grant was seen. */
  renew(id: string, heldLocal: number, backlog: number, cg: number) {
    this.t += 1;
    const i = this.inst.get(id);
    if (!i) return { err: "NOPOOL" };
    i.lease = this.t + LEASE;
    i.backlog = backlog;
    const sg = this.gseq.get(id) ?? 0;
    let settled = cg >= sg;
    if (!settled) {
      if (i.dgs === sg) {
        i.dmiss++;
        if (i.dmiss >= 3) settled = true;
      } else {
        i.dgs = sg;
        i.dmiss = 0;
      }
    } else i.dmiss = 0;
    let drift = 0;
    let gack: number | false = false;
    if (settled && i.held !== heldLocal) {
      i.drift++;
      if (i.drift >= 2) {
        if (i.held > heldLocal) this.returnTokens(i.held - heldLocal);
        else this.take(heldLocal - i.held);
        drift = i.held - heldLocal;
        i.held = heldLocal;
        i.drift = 0;
        i.dmiss = 0;
        gack = sg;
      }
    } else i.drift = 0;
    const granted = this.distribute(id);
    return { ...this.state(id), granted, drift, gack };
  }
  setcap(n: number) {
    this.t += 1;
    const delta = n - this.cap;
    if (delta === 0) return this.state("admin");
    this.cap = n;
    if (delta > 0) {
      const cancel = Math.min(delta, this.debt);
      this.debt -= cancel;
      this.free += delta - cancel;
    } else {
      const fromFree = Math.min(-delta, this.free);
      this.free -= fromFree;
      this.debt += -delta - fromFree;
    }
    const granted = this.distribute("admin");
    return { ...this.state("admin"), granted };
  }
  leave(id: string) {
    this.t += 1;
    const i = this.inst.get(id);
    if (i) {
      this.inst.delete(id);
      this.returnTokens(i.held);
    }
    this.distribute(undefined);
  }
}

async function redisState(prefix: string) {
  const [meta, held, want, backlog, waiters] = await Promise.all([
    redis.hgetall(`${prefix}meta`),
    redis.hgetall(`${prefix}held`),
    redis.hgetall(`${prefix}want`),
    redis.hgetall(`${prefix}backlog`),
    redis.zrange(`${prefix}waiters`, 0, -1),
  ]);
  // Absent and 0 mean the same thing; compare only non-zero entries.
  const num = (o: Record<string, string>) =>
    Object.fromEntries(
      Object.entries(o)
        .map(([k, v]) => [k, Number(v)])
        .filter(([, v]) => v !== 0),
    );
  return {
    cap: Number(meta.cap),
    free: Number(meta.free),
    debt: Number(meta.debt),
    held: num(held),
    want: num(want),
    backlog: num(backlog),
    waiters,
  };
}

function modelState(m: Model) {
  const pick = (f: (i: Inst) => number) =>
    Object.fromEntries([...m.inst.entries()].map(([id, i]) => [id, f(i)]).filter(([, v]) => v !== 0));
  const waiters = [...m.inst.entries()]
    .filter(([, i]) => i.waiting !== undefined)
    .sort((a, b) => a[1].waiting! - b[1].waiting! || (a[0] < b[0] ? -1 : 1))
    .map(([id]) => id);
  return {
    cap: m.cap,
    free: m.free,
    debt: m.debt,
    held: pick((i) => i.held),
    want: pick((i) => i.want),
    backlog: pick((i) => i.backlog),
    waiters,
  };
}

type Op =
  | { k: "join"; who: number; held: number; weight: number }
  | {
      k: "sync";
      who: number;
      release: number;
      want: number;
      rank: number;
      backlog: number;
      transit: number;
      replay: boolean;
    }
  | { k: "renew"; who: number; delta: number; backlog: number; lag: number }
  | { k: "setcap"; n: number }
  | { k: "leave"; who: number };

const who = fc.integer({ min: 0, max: 3 });
const opArb: fc.Arbitrary<Op> = fc.oneof(
  {
    arbitrary: fc.record({
      k: fc.constant("join" as const),
      who,
      held: fc.integer({ min: 0, max: 3 }),
      weight: fc.integer({ min: 1, max: 3 }),
    }),
    weight: 2,
  },
  {
    arbitrary: fc.record({
      k: fc.constant("sync" as const),
      who,
      release: fc.integer({ min: 0, max: 4 }),
      want: fc.integer({ min: 0, max: 6 }),
      rank: fc.integer({ min: 0, max: 2 }),
      backlog: fc.integer({ min: 0, max: 20 }),
      transit: fc.integer({ min: 0, max: 2 }),
      replay: fc.boolean(),
    }),
    weight: 6,
  },
  {
    arbitrary: fc.record({
      k: fc.constant("renew" as const),
      who,
      delta: fc.constantFrom(0, 0, 0, -1, 1),
      backlog: fc.integer({ min: 0, max: 20 }),
      lag: fc.constantFrom(0, 0, 0, 1),
    }),
    weight: 2,
  },
  {
    arbitrary: fc.record({ k: fc.constant("setcap" as const), n: fc.integer({ min: 0, max: 12 }) }),
    weight: 1,
  },
  { arbitrary: fc.record({ k: fc.constant("leave" as const), who }), weight: 1 },
);

describe("Lua token pool vs reference model", () => {
  it("matches the model step by step on random operation sequences", async () => {
    let run = 0;
    let steps = 0;
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 10 }),
        fc.array(opArb, { minLength: 5, maxLength: 40 }),
        async (initCap, ops) => {
          const pool = `model-${Date.now()}-${++run}`;
          const prefix = `sluice:{${pool}}:`;
          const m = new Model();
          const seqs = new Map<string, number>();
          const call = (s: Script, id: string, args: (string | number)[]) =>
            client.evalScript(s, prefix, [pool, id, ...args.map(String)]);
          for (const op of ops) {
            await sleep(2); // keep Redis TIME strictly increasing between ops, as the model's clock is
            steps++;
            if (op.k === "join") {
              const id = `i${op.who}`;
              const got = JSON.parse(
                await call(S.join!, id, ["c", op.weight, initCap, LEASE, "inc", op.held, 1]),
              );
              const exp = m.join(id, initCap, op.weight, op.held);
              seqs.set(id, 0);
              expect({ share: got.share, contended: got.contended, held: got.held }).toEqual({
                share: exp.share,
                contended: exp.contended,
                held: exp.held,
              });
            } else if (op.k === "sync") {
              const id = `i${op.who}`;
              if (!m.inst.has(id)) continue;
              const held = m.inst.get(id)!.held;
              const release = Math.min(op.release, held);
              const heldKnown = Math.max(0, held - release - op.transit);
              let seq = seqs.get(id)!;
              if (!(op.replay && seq > 0)) seq++;
              seqs.set(id, seq);
              const args = [seq, release, op.want, op.rank, op.backlog, LEASE, heldKnown];
              const got = JSON.parse(await call(S.sync!, id, args));
              const exp = m.sync(id, seq, release, op.want, op.rank, op.backlog, heldKnown);
              expect({
                granted: got.granted,
                share: got.share,
                contended: got.contended,
                held: got.held,
              }).toEqual({
                granted: exp.granted,
                share: exp.share,
                contended: exp.contended,
                held: exp.held,
              });
            } else if (op.k === "renew") {
              const id = `i${op.who}`;
              if (!m.inst.has(id)) continue;
              const heldLocal = Math.max(0, m.inst.get(id)!.held + op.delta);
              // lag 1: one grant still in transit to the client.
              const cg = Math.max(0, (m.gseq.get(id) ?? 0) - op.lag);
              const got = JSON.parse(await call(S.renew!, id, [LEASE, heldLocal, op.backlog, 0, "", cg]));
              const exp = m.renew(id, heldLocal, op.backlog, cg) as {
                granted: number;
                drift: number;
                share: number;
                gack: number | false;
              };
              expect({ granted: got.granted, drift: got.drift, share: got.share, gack: got.gack }).toEqual({
                granted: exp.granted,
                drift: exp.drift,
                share: exp.share,
                gack: exp.gack,
              });
            } else if (op.k === "setcap") {
              if (!m.exists) continue;
              const got = JSON.parse(await call(S.setcap!, "admin", [op.n, "admin"]));
              const exp = m.setcap(op.n);
              expect({ cap: got.cap, free: got.free, debt: got.debt }).toEqual({
                cap: exp.cap,
                free: exp.free,
                debt: exp.debt,
              });
            } else {
              const id = `i${op.who}`;
              await call(S.leave!, id, []);
              m.leave(id);
            }
            if (m.exists) {
              const r = await redisState(prefix);
              expect(r).toEqual(modelState(m));
              const totalHeld = (Object.values(r.held) as number[]).reduce((a, b) => a + b, 0);
              expect(r.free + totalHeld).toBe(r.cap + r.debt);
              expect(r.free).toBeGreaterThanOrEqual(0);
              expect(r.debt).toBeGreaterThanOrEqual(0);
            }
          }
          const keys = await redis.keys(`${prefix}*`);
          if (keys.length > 0) await redis.del(...keys);
        },
      ),
      { numRuns: 200 },
    );
    expect(steps).toBeGreaterThan(1500);
  }, 300_000);
});
