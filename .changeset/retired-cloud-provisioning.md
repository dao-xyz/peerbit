---
"@peerbit/server": major
---

Retire provider-managed VM and DNS provisioning together with the hosted test-domain flow. Add a provider-neutral `peerbit domain configure` command for domains users control, retain cleanup of legacy Hetzner remotes, and report actionable manual cleanup details for legacy AWS remotes.

Make domain reconfiguration transactional: serialize concurrent replacements, preserve and recover the previous certificate container, verify stable Docker and HTTPS readiness before activation, and retain bounded configuration history for rollback diagnostics.

Add an invite-gated `peerbit domain lease` lifecycle for managed `nodes.peerchecker.com` names, including direct-IP ownership challenges, crash-safe local credentials, explicit renew/release/status commands, and background renewal while a configured node is running.
