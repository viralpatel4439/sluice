# ADR 0002: Counters in Redis, a local fast path, and grants pushed over pub/sub

**Status:** accepted

**Context.** A per-request round trip to a coordinator would add latency to every call and turn the coordinator into the bottleneck.

**Decision.**
- **Tokens are leased to instances.** A finished request hands its token straight to the next queued item on the same instance, with no network call. Tokens move between instances only when one runs out of work or when another is starved below its fair share.
- **Redis stores counters, not token IDs.** Grants count as held the moment they are made and are pushed over pub/sub in one hop. Lost messages are repaired by drift correction in `renew`.

**Consequences.**
- Zero Redis calls per request under steady load.
- Cluster-friendly: there are no blocking pops across slots.
- A lost grant costs at most two renew intervals of idle capacity.
