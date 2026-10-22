# Architecture

This document is kept current with every change. If you change behaviour described here, update it in the same PR.

## Goals and non-goals

**Goal:** across all instances of all caller services, never have more than N requests in flight at a target, where N is the target's capacity. Every call is still answered with the target's real response, and no work is dropped by default.

**Non-goals:** user-facing request paths; serverless producers; storing payloads; exactly-once delivery; deciding which instance processes which work (partitioning).

## Identity model

```
namespace  "prod-eu"      isolates fleets sharing one server/Redis
 └ target  "inventory"    owns capacity N  ← the pool belongs to the target
    └ route "bulk-write"  optional sub-limit inside N (its own pool, capped at maxShare × N)
 caller    "orders"       the calling service; has a fair-share weight
 instance  pod/VM id      holds a lease and leased tokens
```

The pool for a target is named `namespace/target`; a capped route's pool is `namespace/target#route`. Every caller service that uses a target shares that target's pool.

## Components

```
packages/sluice/src/
  core/        engine, per-target runtime, queue, scheduler, retry, timers, capacity sources, circuit, LocalHub
  intercept/   fetch and http.Agent interceptors, AsyncLocalStorage hints, env config, host rules
  redis/       Lua scripts and the Redis coordinator (also used inside sluice-server)
  remote/      gRPC client coordinator + protocol (proto/sluice.proto)
  prometheus.ts, otel.ts, register.ts, sluice.ts
packages/server/src/
  sessions.ts  one gRPC stream per engine instance → a RedisCoordinator acting for it
  admin.ts     HTTP: control panel, JSON API, SSE, /metrics, operator actions
```

The **Coordinator** port separates the local engine from whatever holds the shared pool. There are three implementations:

| Implementation | Where the pool lives |
|---|---|
| `LocalHub` | in-process: single instance, and the simulation tests |
| `RedisCoordinator` | Redis, through Lua scripts |
| `RemoteCoordinator` | sluice-server over gRPC; the server runs a `RedisCoordinator` for each connected instance |

Every implementation behaves the same way: grants are always delivered through a callback, and never as a return value.

## The local engine

For each target, `Target` holds:

| Component | Role |
|---|---|
| **LaneQueue** | One FIFO per (route, priority), bounded by items and by bytes. It picks strict priority first, except that a lane below its minimum share of recent dispatches goes first (critical 0.6, standard 0.3, bulk 0.1), so no lane starves. Expired or cancelled entries are skipped lazily, which keeps every operation O(1). |
| **Wallets** | One for the target and one for each capped route. Each tracks the tokens held, requests in flight, *orphans* (requests from before a fence), fallback mode, capacity, fair share and contention. |
| **Dispatcher** (`pump`) | First gives back tokens above its fair share if another instance is starved. Then, while a token is free, it takes the next entry whose route has room and sends it. The token is returned when the send settles **or when the per-request timeout fires**, so a hung `send` can never leak capacity. |
| **Retry** | `classify` decides `ok`, `retry(afterMs)` or `fail`. Backoff uses full jitter and honours Retry-After. A retry budget caps retries in flight at `max(1, budget × held)`. Retries only happen when `retrySafe` says so. |
| **TimerHeap** | One min-heap that drives every deadline and retry delay through a single clock timer. |
| **Capacity source** | `static`, `callback`, `http` (one URL, or the sum over replicas), `adaptive` (AIMD), or `auto` (headers, then `/_capacity`, then adaptive). Only the capacity owner publishes. The value the source wants is remembered and re-applied whenever this instance becomes owner. |
| **Circuit** | Opens after consecutive connection failures, then lets 1–3 probes through per interval. |

All time goes through a `Clock`, so tests run on simulated time.

## The shared pool (Redis)

All keys of a pool share the hash tag `{namespace/target}`, so each pool sits on one Cluster slot and every script is atomic.

| Key | Holds |
|---|---|
| `meta` | `cap`, `free`, `debt`, `epoch`, `inc` (incarnation), `v` (protocol), `fence`, `owner`, `oexp`, `override` |
| `held`, `want`, `backlog`, `weight`, `caller`, `rank`, `since`, `seq`, `res`, `drift` | per-instance hashes |
| `waiters` | zset, score = priority rank × 1e13 + time it started waiting |
| `lease` | zset, instance → expiry in Redis server time |
| `log` | capped stream of events and audit entries |
| `stats:{id}` | the instance's last load report, with a TTL |
| `sluice:pools` | set of known pools (for the control panel) |

**Invariant, kept by every script:** `free + Σ held = cap + debt` (debt counts tokens still out above a lowered cap; it is paid off as they come back).

Scripts: `join`, `sync` (release + demand in a single call), `renew`, `setcap`, `leave`, `inspect`. Every script first reclaims expired leases.

### Granting

`distribute` runs in two passes over the waiters, ordered by priority and then by wait time:
1. Waiters **below their fair share**.
2. Anyone still waiting.

Tokens go out one per waiter per round. A grant counts as held immediately. The caller's own grant comes back in the script reply; grants for other instances are pushed over pub/sub (`sluice:i:<instance>`). A lost message is repaired by drift correction in `renew`: if the server's and the client's counts disagree on two renews in a row, the client's count wins. Grants carry a per-instance sequence number, and correction only happens once the client has seen every grant (or one has been missing for 3 renews, meaning it was lost). That way a grant still in transit is never corrected away; see [protocol.md](protocol.md#grant-numbering). The chaos test caught this race on a slow link.

### Fair share

`share_i = apportion(cap, weight_i × max(backlog_i, held_i > 0 ? 1 : 0))` with largest-remainder rounding, so the shares always add up to `cap`. An instance gives tokens back only when another waiting instance is **below** its share (starved). Without that rule, spare tokens would ping-pong between instances that already have their share.

### Idempotency

Each client sends a `seq` with every `sync`. The script stores the last `seq` and its result per instance, so a retried call returns the cached result instead of releasing tokens twice. A retry after a timeout is therefore always safe.

### Capacity owner and fencing tokens

`renew` makes the caller owner if there is no owner or the owner has expired, and increments `fence` whenever ownership changes. `setcap` rejects a stale fence. An operator override (`fence = "admin"`) pins capacity until it is cleared.

### Shrinking

Lowering `cap` takes tokens from `free` first and records the remainder as `debt`. Each later release pays off debt before it frees a token. Requests in flight are never cancelled.

### Sharded pools

A hot target can be split into `k` physical pools `namespace/target%0..k-1`, each with its own hash tag and so its own slot.
- **Agreement on k:** the first instance to join records `k` in `sluice:shards`, and everyone adopts it.
- **Home shard:** each instance joins one shard, chosen by a hash of its id. Leases, fairness, the fast path and handoff all work inside that shard exactly as for an unsharded pool.
- **Logical owner:** claimed on shard 0 with its own fencing counter (`lfence`), it runs `rebalance` every renew. It computes each shard's target cap by apportioning N by `backlog + held`, then:
  1. lowers caps on shards above target, taking only free tokens (`setcap … shard-free`);
  2. hands the now-unassigned capacity to shards below target.

  Because capacity only moves out of free tokens, the shard caps sum to N and a move never adds debt. A real cut of N uses debt as usual.
- **Engine view:** engines see the pool-wide N (`ltotal`), while fair shares are computed within the home shard.

### Pool garbage collection

The `gc` script deletes a pool's keys only when no instance is registered and no operator override is pinned. sluice-server runs it on pools that have been empty for `SLUICE_POOL_GC_MS`.

## Redis clients

The coordinator talks to Redis through `RedisLike` (EVALSHA with an EVAL fallback, pub/sub, SADD/SREM, HSETNX, CONFIG GET). Adapters exist for ioredis (single node, Sentinel, Cluster) and node-redis.

## Failure handling

| Failure | Behaviour |
|---|---|
| Instance crashes | Its lease expires (6 s by default) and the next script call reclaims its tokens and hands them to waiters. Through sluice-server this is immediate when the stream drops. |
| Instance loses Redis or the server | **Self-fencing:** at `lastOk + lease − safety` (monotonic clock) the instance stops using held tokens, which become orphans, and falls back to `floor(N / instances)`, or to 0 with `fallback: "pause"`. This always happens *before* the Redis side can hand those tokens to someone else. On reconnect it re-joins and resumes. |
| Redis reply lost | The retry carries the same `seq` and gets the cached result. |
| Redis data lost or failover | Scripts return `NOPOOL`, and clients re-join carrying the tokens they still hold (`held`), so the pool is rebuilt without double counting. |
| sluice-server replica dies | Clients reconnect to another replica, which is stateless, and re-register the tokens they still hold. If they're away past the fencing window, they fence. |
| Target slow or overloaded | AIMD cuts capacity by 30%. 429 and 503 responses are retried after `Retry-After` and count as overload. |
| Target down | The circuit opens and probe requests go through until it recovers. |
| Capacity endpoint down | The last value is kept for 5 s, then `fallback` is used. |
| Hung request | Aborted at `timeoutMs`; the token is returned and the request retried if that's safe. |
| Retry storm | The retry budget holds extra retries in the queue. |
| Process shutdown | `drain()`: stop intake, finish until the timeout, return the rest through `onUnsent`. |

**Documented bound:** while an instance is fenced in `local-share` mode, the target can briefly see more than N requests: at most N ÷ instances extra for each instance that is cut off, plus its orphaned in-flight requests. Use `pause` mode if that is not acceptable.

## Interception

- `register` and `install()` replace `globalThis.fetch` and wrap `http.Agent.prototype.addRequest`, which covers `https.Agent` and every client built on it.
- Each intercepted call becomes an item whose payload knows how to re-send itself: fetch clones a replayable `Request`; the agent path sends exactly once, because its bodies are streamed.
- The target is derived from the host.
- Headers (`Idempotency-Key`, `X-Sluice-Caller`, `X-Sluice-Attempt`) are added only for internal-looking hosts, unless configured otherwise.
- The token is held until response headers arrive. Response bodies stream after that.
