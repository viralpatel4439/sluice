# Runbook: SluiceBacklogGrowing / SluiceQueueWaitHigh

**Meaning:** work arrives faster than the target's capacity N allows.

**Check:** the control panel → the target.
- Is N lower than expected? Look for an operator pin (📌), a low AIMD value, or an unhealthy target.
- Is one caller hogging the target? Look at the caller shares.

**Fix:** any of these:
- Raise the target's real capacity (scale it), and let `/_capacity` or the headers report it.
- Adjust caller weights.
- Accept the longer wait.

Scaling the *callers* will not help: sluice never lets them exceed N.
