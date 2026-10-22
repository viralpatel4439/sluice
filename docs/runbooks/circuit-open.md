# Runbook: SluiceCircuitOpen

**Meaning:** consecutive connection failures to the target. sluice now only lets probe requests through.

**Check:** is the target up? DNS, network policies, TLS.

**Fix:** restore the target. The circuit closes after the first successful probe, and queued work then resumes on its own.
