# Runbook: SluiceFallbackActive

**Meaning:** instances lost contact with sluice-server or Redis and fenced themselves. They now limit locally to `N ÷ instances`, or to 0 in `pause` mode.

**Impact:** throughput may drop, and in `local-share` mode the target can briefly see more than N requests (bounded, see docs/architecture.md).

**Check:**
1. Is sluice-server ready? Look at `kubectl get pods`, `/readyz`, and the server logs for "redis error".
2. Is Redis healthy? Check memory and `maxmemory-policy noeviction`, check for failover, check network policies.
3. Look at the engine logs for "lost contact" or "fenced".

**Fix:** restore connectivity. Instances re-join automatically and log "rejoined pool after fence". Nothing needs restarting.
