# Runbook: SluiceDeadItems

**Meaning:** items exhausted their retries (network errors or timeouts on every attempt).

**Check:** engine logs for `item:dead`, and the target's error rate.

**Fix:** fix the target. Dead items are passed to `onDead` with their key and attempt count. Replay them from your work source.
