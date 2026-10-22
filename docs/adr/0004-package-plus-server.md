# ADR 0004: Ship a server alongside the library

**Status:** accepted

**Context.** The library alone can coordinate through Redis. Teams also asked for a central control panel, per-caller authentication, fewer Redis connections, and a path for non-Node clients.

**Decision.** Ship `sluice-server`, a stateless gRPC service. For each connected engine instance it runs the same `RedisCoordinator` the library uses. Redis stays the single source of truth, and server replicas scale horizontally.

**Consequences.**
- One code path for pool logic.
- Direct-Redis mode and server mode can be mixed.
- A dropped stream returns that instance's tokens immediately instead of after the lease.
- Moving a token between instances costs one extra hop.
