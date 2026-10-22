# @viralpatel4439/sluice-server

The coordination server for [sluice](https://github.com/viralpatel4439/sluice). Engines connect over gRPC. The server keeps each target's token pool in Redis, serves the control panel and JSON API, and exports fleet-wide Prometheus metrics.

It is stateless: run as many replicas as you like behind a load balancer.

```bash
docker build --target server -t sluice-server .   # from the repo root
docker run -p 7070:7070 -p 8080:8080 -e SLUICE_REDIS_URL=redis://redis:6379 -e SLUICE_ADMIN_TOKEN=... sluice-server
# or
npm run build && node packages/server/dist/main.js   # from the repo root
# or, on Kubernetes
helm install sluice deploy/helm/sluice-server --set redis.url=redis://redis:6379
```

| Variable | Default | Meaning |
|---|---|---|
| `SLUICE_REDIS_URL` | `redis://localhost:6379` | Where pools live (use `maxmemory-policy noeviction`) |
| `SLUICE_GRPC_PORT` / `SLUICE_HTTP_PORT` | `7070` / `8080` | Engines / panel, API, metrics |
| `SLUICE_TOKENS` | – | `caller:token,...` or one shared token. Unset means no auth |
| `SLUICE_ADMIN_TOKEN` | – | Enables control-panel actions (pin capacity, pause, resume) |
| `SLUICE_VIEW_TOKEN` | – | Required for reads when set |
| `SLUICE_LEASE_MS` / `SLUICE_RENEW_MS` | `6000` / `2000` | Instance lease and renew interval |
| `SLUICE_TLS_CERT` / `SLUICE_TLS_KEY` | – | Serve gRPC over TLS |
| `SLUICE_KEY_PREFIX` | `sluice` | Redis key prefix |
| `SLUICE_SHARDS` | – | Shard hot targets, e.g. `inventory=4,payments-*=2` |
| `SLUICE_POOL_GC_MS` | `600000` | Delete pools nobody has used for this long (operator-pinned pools are kept) |
| `SLUICE_LOG_LEVEL` | `info` | JSON logs |

**HTTP endpoints**

| Endpoint | Purpose |
|---|---|
| `/` | Control panel |
| `/api/overview` | All pools and connected instances |
| `/api/events` | Live updates (SSE) |
| `/api/pools/<pool>` | One pool |
| `…/history`, `…/log` | That pool's history and event log |
| `POST …/capacity` / `DELETE …/capacity` | Pin capacity / unpin it |
| `POST …/pause`, `…/resume` | Pause or resume the target |
| `/metrics` | Prometheus |
| `/healthz`, `/readyz` | Liveness and readiness probes |

All write actions need `Authorization: Bearer $SLUICE_ADMIN_TOKEN` and are recorded in the pool's audit log.
