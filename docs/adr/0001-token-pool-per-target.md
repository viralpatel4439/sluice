# ADR 0001: One token pool per target, shared by every caller

**Status:** accepted

**Context.** Capacity is a property of the downstream service (the target), not of whoever calls it. If each caller service had its own budget, three services calling `inventory` could together send three times what it can take.

**Decision.**
- Pools are keyed by `namespace/target`. Every caller service and every instance that uses the target draws from the same N.
- Callers get weighted fair shares *within* the pool.
- Routes are optional sub-pools, capped at a share of N.

**Consequences.** One place to set and observe a target's limit. Fairness between callers is explicit (weights) rather than accidental (who has more pods).
