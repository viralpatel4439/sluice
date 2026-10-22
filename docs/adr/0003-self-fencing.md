# ADR 0003: Instances fence themselves before their lease can expire

**Status:** accepted

**Context.** If an instance is cut off from Redis but can still reach the target, Redis will reclaim its tokens after the lease and give them to others. If the cut-off instance kept using them, the target would see more than N requests.

**Decision.** Each instance records, on its monotonic clock, when it last heard from the coordinator. At `lease − safety` it stops trusting its held tokens:
- Requests still in flight become orphans and finish normally.
- New work runs under a local fallback limit: `N ÷ instances`, or 0 in `pause` mode.

**Consequences.** The overshoot during a partition is bounded and documented, and `pause` mode removes it entirely. Clock skew between machines is irrelevant, because leases use Redis server time and fencing uses local monotonic time.
