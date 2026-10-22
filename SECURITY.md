# Security policy

## Reporting a vulnerability

Please **do not** open a public issue. Use GitHub's private vulnerability reporting instead: *Security → Report a vulnerability* on this repository. You will get an acknowledgement within 3 working days, and a fix or mitigation plan within 30 days for confirmed issues.

## Supported versions

Security fixes go to the latest minor release.

## Deployment guidance

- Set `SLUICE_TOKENS` on sluice-server so only your services can join pools, and `SLUICE_ADMIN_TOKEN` to enable control-panel actions. Without an admin token, actions are disabled.
- Use TLS for gRPC (`SLUICE_TLS_CERT` and `SLUICE_TLS_KEY` on the server, `grpcs://` on clients), or run inside a service mesh.
- Give sluice its own Redis (or Redis ACL user and key prefix) with `maxmemory-policy noeviction`.
- sluice never stores or logs request payloads. Pool names, caller names and instance IDs are visible in the control panel.
