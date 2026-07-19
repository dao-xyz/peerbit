---
"@peerbit/server": major
---

Replace the legacy replayable HTTP administration signature with strict
signed-request v2. Requests are now framed unambiguously, bound to a pinned
server identity and per-process boot ID, freshness checked, protected by
single-use nonces, and verified against the exact request target and raw body
digest. Remote records persist the server identity used for audience pinning.
Existing remote names retain that pin until explicitly removed, so routine
re-enrollment cannot silently accept a substituted server identity.

Legacy signed requests are intentionally rejected. Upgrade remote servers
before their administrator clients, then reconnect with the server peer ID
pinned.
