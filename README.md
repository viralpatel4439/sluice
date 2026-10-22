# sluice

**Release bursts to your downstream services only as fast as they can take them, across every instance of every caller, without changing your code.**

Background work (batch syncs, migrations, queue consumers, fan-out, webhooks, backfills) tends to arrive in bursts. When it does, the service on the other end gets overloaded, times out and starts failing, and the failures cascade. sluice sits inside each calling process and holds requests in a local queue until the target has room. It then sends each one and hands the caller the target's real response.

```
 orders ×3 ─┐                                       ┌─▶ inventory replica 1
 billing ×3 ┼─ sluice (in-process) ── HTTP ─▶ LB ───┼─▶ inventory replica 2   never more than N in flight,
 jobs ×10  ─┘        │                              └─▶ inventory replica 3   fleet-wide
                     ▼
          sluice-server / Redis  (tokens only; payloads never leave the A→B path)
```

**The guarantee:** a call is never sent to the target early and never answered early. It waits, with its promise pending, until the target has a free slot and it is that call's turn. Then it is sent, and the caller gets the target's real response. By default nothing times out, nothing is rejected and nothing is dropped while waiting.

## 30-second start

No code changes. Attach sluice when you start your service:

```bash
# sluice is not on npm yet: build it from this repo and install the packed file
git clone https://github.com/viralpatel4439/sluice && cd sluice
npm install && npm run build && npm pack -w @viralpatel4439/sluice
# in your service:
npm i /path/to/sluice/viralpatel4439-sluice-0.0.0.tgz
NODE_OPTIONS="--import @viralpatel4439/sluice/register" node app.js
```

From then on, every outgoing `fetch` and `http`/`https` request (axios, got, node-fetch…) is gated per target host:

- **Target:** derived from the URL host. All replicas behind `http://inventory` form one target.
- **Capacity:** learned automatically. sluice reads the `X-Sluice-Capacity` or `RateLimit-Limit` headers, falls back to `GET /_capacity` on internal hosts, and otherwise adapts using AIMD on latency and errors.
- **Retries:** applied only when safe. That means idempotent methods, or requests that carry an `Idempotency-Key`. Retries happen on network errors and 429/502/503/504, and honour `Retry-After`.
- **Errors:** your code sees the same `Response` objects and errors it always did.

To coordinate many instances, point them at a sluice-server (or straight at Redis):

```bash
docker compose up -d                       # Redis + sluice-server, control panel on http://localhost:8080
SLUICE_SERVER_URL=localhost:7070 SLUICE_CALLER=orders \
NODE_OPTIONS="--import @viralpatel4439/sluice/register" node app.js
```

### See it working

```bash
npm run demo     # = docker compose --profile demo up --build
```

The demo starts:
- a target with three replicas behind nginx, which can take 20 requests at once in total;
- two caller services that **have no sluice code in them**: `orders` uses `fetch` and `billing` uses `axios`, each with three replicas;
- sluice-server, Redis, Prometheus and Grafana.

Where to look:
- `curl localhost:3000/stats`: the target's own fleet-wide scoreboard. Watch `peak` stay at or below 20, and `rejected` stay at 0.
- http://localhost:8080: the sluice control panel.
- http://localhost:3001: the Grafana dashboard.

## Three ways to use it

| | How | When |
|---|---|---|
| **Zero code** | `--import @viralpatel4439/sluice/register` + `SLUICE_*` env vars | Most services |
| **One line** | `import { sluice } from "@viralpatel4439/sluice"; sluice.install()` | Configure in code |
| **Explicit** | `createEngine()` + `engine.target(name, { send })` | Libraries, custom transports, full control |

Optional hints use AsyncLocalStorage, so your call sites stay plain:

```ts
import { sluice } from "@viralpatel4439/sluice";

await sluice.with({ priority: "bulk", deadlineMs: 60_000 }, () => runBackfill()); // every call inside inherits it
await sluice.bypass(() => fetch(healthCheckUrl));                                   // never gated
const charge = sluice.wrap("payments", (req) => grpcClient.charge(req));            // gate any async function
```

The explicit API:

```ts
import { createEngine } from "@viralpatel4439/sluice";
import { remotePool } from "@viralpatel4439/sluice/remote";

const engine = createEngine({
  namespace: "prod",
  caller: "orders",
  coordination: await remotePool({ url: "sluice-server:7070", instanceId: process.env.HOSTNAME!, caller: "orders" }),
});

const inventory = engine.target("inventory", {
  capacity: { type: "http", url: "http://inventory/_capacity", fallback: 50 },
  routes: { "bulk-write": { maxShare: 0.3 } },     // sub-limit inside the target's capacity
  send: (item, signal) =>
    fetch("http://inventory/records", { method: "POST", body: JSON.stringify(item.payload), signal, headers: item.headers }),
});

const res = await inventory.submit(record, { key: record.id, priority: "bulk", route: "bulk-write" });
process.on("SIGTERM", () => engine.drain({ timeoutMs: 25_000, onUnsent: (items) => requeue(items) }));
```

## How it works

- **Tokens.** Each target has a pool of N tokens, where N is the target's capacity. One token is permission for one request in flight. The pool belongs to the *target*, so every caller service that uses `inventory` shares the same N.
- **Fast path.** When a request finishes and the instance has more work queued, the same token sends the next item at once, with no network call. Tokens only move between instances when one runs out of work or when fairness requires it.
- **Fair sharing.** When several instances want more tokens than exist, each gets a share in proportion to `weight × backlog`. An instance above its share gives tokens back as soon as another instance is starved.
- **Capacity.** One instance per target is the *capacity owner*. It reads the target's capacity (from headers, `/_capacity`, or AIMD) and publishes it. When capacity grows, the new tokens go to waiters first. When it shrinks, tokens are retired as they come back, and requests already in flight are never cancelled.
- **Failures.** Instances hold *leases*, so a crashed instance's tokens are reclaimed within about 6 s, or at once when it was connected through sluice-server. An instance that loses contact **fences itself** before its lease could expire: it stops using its tokens and falls back to a local share of N.

The full design is in [docs/architecture.md](docs/architecture.md) and the wire protocol is in [docs/protocol.md](docs/protocol.md).

## Coordination modes

| Mode | Set up | Use when |
|---|---|---|
| Single instance | nothing | One process, or per-process limits are enough |
| **sluice-server** | `SLUICE_SERVER_URL` | Fleets. Gives you a central control panel, auth tokens per caller, and one place to run |
| Direct Redis | `SLUICE_REDIS_URL` | You'd rather not run the server. Every instance talks Lua to Redis directly |

Server mode and Redis mode share the same pools, so they can be mixed. The server is stateless and scales horizontally, because all state lives in Redis.

**Redis topologies:**
- Redis 7+, Redis 8 and Valkey 8, on a single node, Sentinel (including failover) or Cluster. Every topology is covered by tests.
- Clients: ioredis or node-redis (`redis` v4/v5). Both can share one pool, and any other client can plug in through the small `RedisLike` adapter.

**Hot targets:** a pool can be split into shards on different Redis slots with `SLUICE_SHARDS` on the server (`inventory=4`) or `redisPool({ shards })`.
- Each instance uses one home shard.
- A logical owner keeps the shard capacities summing to N and moves capacity toward the shards with backlog. It only takes free tokens when moving capacity, so the pool never exceeds N.
- The first instance's shard count is stored in Redis and adopted by everyone else.

## Delivery semantics

- **At-least-once** to the target when retries are on, which is the default for safe requests. Each logical call keeps one `Idempotency-Key` across all of its attempts, so the target can deduplicate. Retries are off for non-idempotent requests that have no key.
- **At-most-once** with `retry: { max: 0 }`, or with `retryOnTimeout: false` for timeouts.
- **Not exactly-once.** That needs the target's cooperation, which is what the key is for.
- **Crash:** the in-memory queue is lost with the process. Your work source (database, queue, log) must be able to replay. `engine.drain()` on SIGTERM finishes what it can and hands back the rest.

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `SLUICE_SERVER_URL` | – | sluice-server address (`host:7070`, or `grpcs://` for TLS) |
| `SLUICE_REDIS_URL` | – | Coordinate through Redis directly instead |
| `SLUICE_TOKEN` | – | Bearer token for the server |
| `SLUICE_NAMESPACE` | `default` | Isolates fleets that share one server or Redis |
| `SLUICE_CALLER` | `OTEL_SERVICE_NAME` or package name | This service's name |
| `SLUICE_WEIGHT` | `1` | Fair-share weight of this caller |
| `SLUICE_MODE` | `all` | `context`: only gate calls made inside `sluice.with()`, for services that also serve users |
| `SLUICE_EXCLUDE` | – | Comma-separated host globs never to gate |
| `SLUICE_HEADERS` | `internal` | Which hosts get `Idempotency-Key`/`X-Sluice-*` headers: `internal`, `all`, `none` |
| `SLUICE_TARGETS` / `SLUICE_CONFIG` | – | Per-target overrides as JSON, e.g. `{"inventory":{"capacity":50}}` |
| `SLUICE_DEFAULT_CAPACITY` | `10` | Starting capacity for newly discovered targets |
| `SLUICE_METRICS_PORT` | – | Serve `/metrics`, `/healthz` and `/readyz` |
| `SLUICE_LOG_LEVEL` | `warn` | `debug`, `info`, `warn`, `error` or `silent` |
| `SLUICE_OTEL` | – | `1` exports OpenTelemetry metrics and spans through your global OTel providers |
| `SLUICE_SHARDS` | `1` | Direct-Redis mode: shard every pool into this many sub-pools |
| `SLUICE_DISABLED` | – | `1` turns `register` into a no-op |

These are never gated: telemetry exporters (`OTEL_EXPORTER_*_ENDPOINT`), cloud metadata endpoints, and sluice's own server and Redis.

## When not to use it

- **User-facing request paths.** A user can't wait in a queue. Use `SLUICE_MODE=context` so only background work is gated, and keep gateway rate limits for user traffic.
- **Serverless producers.** Waiting is billed per second, and a function can't keep a local queue.
- **Exactly-once processing.** sluice controls *rate*, not *delivery*. Pair it with idempotent targets.

## Observability

- **Engines:**
  - Prometheus metrics (`SLUICE_METRICS_PORT`, or `@viralpatel4439/sluice/prometheus`).
  - OpenTelemetry metrics *and traces* (`SLUICE_OTEL=1`, or `instrumentOtel` / `traceOtel`). Every call gets a `sluice.wait` span (time queued) and a `sluice.send` span per attempt in the caller's trace, and your HTTP client span nests under `sluice.send`.
  - Typed events (`engine.on("capacity:changed", …)`) and structured JSON logs that are quiet by default.
- **Server:** fleet-wide `/metrics` (capacity, held, free, debt, waiters, backlog per target and per caller), a control panel, and a JSON API with live updates.
- **Monitoring assets:** a Grafana dashboard ([monitoring/grafana](monitoring/grafana)) and Prometheus alert rules with runbooks ([monitoring/prometheus/alerts.yml](monitoring/prometheus/alerts.yml), [docs/runbooks](docs/runbooks)).

## Performance

Measured with `npm run bench`, which fails if a budget is missed:

| Measure | Result | Budget |
|---|---|---|
| submit → send overhead | p50 2.2 µs, p99 6 µs | p99 ≤ 50 µs |
| Throughput through one instance | ~600,000 items/s | ≥ 100,000 |
| Memory per queued item | 444 B (of which ~250 B is the caller's own promise) | ≤ 512 B |
| Cross-instance handoff | two network legs, no measurable sluice overhead | ≤ 2 RTT + 0.3 ms |
| Redis calls per request when busy | 0 | 0 |

## How it is tested

| Layer | What it proves |
|---|---|
| Unit and property tests | Queue, scheduler, retry and engine behaviour on simulated time |
| **Deterministic fleet simulation** | 200 seeded runs inject delays, reordering, partitions and crashes. Requests under pool tokens never exceed N, and every live job finishes |
| **Model-based Lua tests** | Random operation sequences run against real Redis *and* a reference model, compared field by field at every step |
| Redis integration | Weighted fairness, crash reclaim, idempotent retries, debt-based shrink, stale-owner fencing, self-fencing, sharding, mixed ioredis and node-redis |
| **Chaos** | Toxiproxy slows and cuts individual instances' Redis links under load |
| **Topologies** | A real 3-master Redis Cluster, and Sentinel with a live failover |
| Server and browser | gRPC sessions, restart, auth, operator actions, and the control panel driven in headless Chromium (including an XSS check) |
| **Mutation testing** | Stryker mutates the core (`npm run test:mutation`): 77% of mutants killed; the run fails below 75% |
| API lock | API Extractor reports; `npm run api:check` fails on an unreviewed public-API change |

114 tests in total; coverage is 90.5% of lines and 82.8% of branches (`npm run test:coverage` requires 85% / 80%). The suite has been run on Node 22 and 24 against Redis 7.2, Redis 8 and Valkey 8.

## Running the server

- Docker: build it with `docker build --target server -t sluice-server .`
- Helm chart: [deploy/helm/sluice-server](deploy/helm/sluice-server)
- From source: `npm run build && node packages/server/dist/main.js`

See [packages/server/README.md](packages/server/README.md).

## Repository

| Path | What |
|---|---|
| [packages/sluice](packages/sluice) | The library: engine, interceptors, Redis and remote coordinators, exporters |
| [packages/server](packages/server) | sluice-server: gRPC sessions, control panel, metrics |
| [examples](examples) | Demo target and callers, and an explicit-API data migration |
| [deploy](deploy), [monitoring](monitoring) | Helm chart, nginx, Prometheus, Grafana |
| [docs](docs) | Architecture, protocol, decisions (ADRs), runbooks |

Contributions are welcome: see [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [Apache-2.0](LICENSE).
