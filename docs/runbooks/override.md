# Runbook: SluiceCapacityPinned

**Meaning:** an operator pinned capacity in the control panel more than 6 hours ago. The capacity source is ignored until the pin is removed.

**Check:** the pool's event log in the panel (`admin:capacity` entries show who pinned it and when).

**Fix:** if the pin is no longer needed, click **Unpin**, or `DELETE /api/pools/<pool>/capacity`.
