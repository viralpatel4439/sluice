# Coordination protocol

There are two wire formats, and both carry the same operations:

- **gRPC** (`packages/sluice/proto/sluice.proto`, package `sluice.v1`): between an engine and sluice-server. Use it for clients in other languages.
- **Redis Lua** (`packages/sluice/src/redis/scripts.ts`): between an engine, or a server session, and Redis.

Payloads never travel over either one. Only token counts do.

## gRPC session

One bidirectional stream per engine instance: `Coordinator.Session(stream ClientMessage) returns (stream ServerMessage)`.

1. **Client → `Hello{instance_id, caller, protocol}`**. The server checks `authorization: Bearer <token>` against `SLUICE_TOKENS`, then answers `Welcome{server_id, protocol, lease_ms}`, or `Error{UNAUTHENTICATED|PROTOCOL}` followed by end of stream.
2. **`Join{req, pool, caller, weight, capacity, held}` → `Joined{req, pool, state, error}`.**
   - `capacity` seeds a pool that does not exist yet.
   - `held` re-registers tokens still in use after a reconnect.
3. **Flow messages (fire and forget):**
   - `Demand{pool, want, rank, backlog}`: ask for up to `want` tokens beyond those held. `want = 0` withdraws.
   - `Release{pool, n}`.
   - `Report{pool, held, in_flight, backlog}`: load, used for leases and fair share.
   - `SetCapacity{pool, capacity}`: honoured only when the instance is the capacity owner.
4. **Server pushes:**
   - `Grant{pool, n}`.
   - `State{pool, PoolState{capacity, contended, fair_share, owner, instances}}`.
   - `Fenced{pool, fallback}`: stop using held tokens and limit locally to `fallback`.
   - `Restored{pool}`.
5. **Keepalive:** `Ping{at}` → `Pong{at}` every second.
   - A client that is silent past the lease is disconnected, and its tokens are returned immediately.
   - A client that cannot reach any server for `lease − safety` fences itself.
6. **`Leave{req, pool}` → `Left{req}`.**

`rank`: 0 = critical, 1 = standard, 2 = bulk. `contended` means another instance is waiting *below its fair share*.

### Reconnect rules (client)

- **On stream loss:** keep tokens and keep using them on the fast path, then reconnect with exponential back-off (100 ms → 5 s).
- **On `Welcome`:** re-join every pool with `held` = the tokens it still holds (0 if it fenced meanwhile), then re-send its demand.

## Redis keys and scripts

Every key of pool `P` starts with `sluice:{P}:` (configurable prefix). The full key list is in [architecture.md](architecture.md).

All scripts take `KEYS[1] = "sluice:{P}:"`, `ARGV[1] = P` and `ARGV[2] = instance id`. Each one reclaims expired leases first.

| Script | Remaining ARGV | Returns |
|---|---|---|
| `join` | caller, weight, initCap, leaseMs, incarnation, heldNow, protocol | state |
| `sync` | seq, release, want, rank, backlog, leaseMs, heldKnown | state + `granted`, `gseq` (cached per `seq`) |
| `renew` | leaseMs, heldLocal (incl. unsent releases), backlog, ownerTtlMs, statsJson, appliedGrantSeq | state + `granted`, `gseq`, `drift`, `gack` |
| `setcap` | capacity or `clear`, fence or `admin` | state, or `{err: STALE_FENCE \| OVERRIDDEN \| NOPOOL}` |
| `leave` | – | `OK` |
| `inspect` | – | full pool view (read-only) |

`state` = `{cap, free, debt, epoch, inc, v, contended, share, instances, held, gall, fence, owner, total}`.

**Pub/sub**
- `sluice:i:<instance>`: carries `g <pool> <n> <gseq>` (tokens granted to that instance, with the grant's per-instance sequence number).
- `sluice:{P}:ev`: carries `contended`, `cleared` and `capacity <n>`. These are hints only. Correctness never depends on them, because every reply carries the same state.

### Grant numbering

Every grant gets a per-instance sequence number (`gseq`) that only ever increases.
- **Applying grants:** the client applies each number once, and records the highest number below which it has seen everything.
- **Renew reports:** that number, together with the client's held count *including releases it has queued but not sent*.
- **When Redis corrects drift:**
  - only when the client has seen every grant;
  - or when a grant has been missing for 3 renews in a row, meaning its message was lost. The reply then carries `gack`, and the client ignores any late message numbered up to it.

Without this, a grant still in transit, or a release not yet sent, could be "corrected" away, and the same token would then exist twice. Renews also run on the same per-pool queue as syncs, so they never interleave.

## Compatibility

- `PROTOCOL_VERSION` (Redis) and `REMOTE_PROTOCOL` (gRPC) are both 1.
- A server accepts clients with protocol ≤ its own.
- A breaking change needs a new major version and a new key prefix.
- The `.proto` is the source of truth. `packages/sluice/scripts/gen-proto.mjs` embeds it into the build.
