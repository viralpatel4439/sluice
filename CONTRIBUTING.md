# Contributing

Thanks for helping. Bug reports, docs fixes and code are all welcome.

## Setup

Requirements: Node.js 22+ and Docker. The tests start a Redis container; to use your own Redis instead, set `SLUICE_TEST_REDIS_URL`.

```bash
npm ci
npm run lint        # Biome
npm run typecheck
npm test                 # everything: unit, property, simulation, model-based Lua, Redis, chaos,
                         # Cluster/Sentinel, server and browser tests (needs Docker)
npm run test:unit        # fast tests, no Docker
npm run test:mutation    # Stryker on the core (slow)
npm run bench            # performance budgets (set SLUICE_BENCH_REDIS_URL for the handoff check)
npm run api:update       # refresh the locked API reports after an intended API change
npm run docs:dev         # documentation site
npm run demo             # full stack: callers, target, server, Prometheus, Grafana
```

Browser tests need Chromium once: `npx playwright install chromium`.

## Ground rules

- **The core package has no runtime dependencies.** Redis, gRPC and OpenTelemetry are optional peer dependencies, loaded only by the subpaths that need them.
- **Coordination changes need tests at the right level:**
  - unit or property tests for engine logic (on `ManualClock`);
  - Redis integration tests for Lua changes, and the conservation invariant `free + Σheld = cap + debt` must hold;
  - server tests for anything that touches the protocol.
- **Keep docs current:** `docs/architecture.md` and `docs/protocol.md` change in the same PR as the behaviour. Record significant decisions as ADRs in `docs/adr/`.
- **Protocol:** `proto/sluice.proto` is the source of truth. Run `node packages/sluice/scripts/gen-proto.mjs` after editing it.
- **Public API:** API Extractor reports in `packages/sluice/api/` record the public API. Run `npm run api:check` before committing; if you change the API on purpose, run `npm run api:update` and commit the reports.
- **Commits** use Conventional Commits.

## Design proposals

For larger changes (new coordination backends, protocol changes, new public API), open an issue labelled `rfc` first, describing the problem, the proposal and the alternatives you considered.
