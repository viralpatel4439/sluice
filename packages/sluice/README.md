# @viralpatel4439/sluice

Release bursts to downstream services only as fast as they can take them, across every instance of every caller, with no code changes.

```bash
# not on npm yet: from the repo root, `npm run build && npm pack -w @viralpatel4439/sluice`, then install the .tgz
NODE_OPTIONS="--import @viralpatel4439/sluice/register" node app.js
```

Every outgoing `fetch` and `http`/`https` request (axios, got, node-fetch) is now gated per target host:
- capacity is learned from response headers, `/_capacity`, or adaptively;
- retries happen only when safe;
- your code gets the same responses and errors it always did.

Add `SLUICE_SERVER_URL` (sluice-server) or `SLUICE_REDIS_URL` to share each target's capacity across a whole fleet.

| Import | Purpose |
|---|---|
| `@viralpatel4439/sluice` | `sluice.install/with/bypass/wrap`, and `createEngine` for the explicit API |
| `@viralpatel4439/sluice/register` | Zero-code entry for `--import` |
| `@viralpatel4439/sluice/remote` | `remotePool()`: coordinate through sluice-server (needs `@grpc/grpc-js`, `protobufjs`) |
| `@viralpatel4439/sluice/redis` | `redisPool()`: coordinate through Redis (needs `ioredis`) |
| `@viralpatel4439/sluice/prometheus` | `renderPrometheus`, `serveMetrics` |
| `@viralpatel4439/sluice/otel` | `instrumentOtel` (needs `@opentelemetry/api`) |

Full documentation: https://github.com/viralpatel4439/sluice
