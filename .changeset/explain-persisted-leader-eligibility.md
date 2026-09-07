---
"@peerbit/shared-log": patch
---

Clarify persisted delivery's initial empty/self-only leader-plan error without changing its fail-fast behavior or durability checks. Document the distinction between per-log replication maturity, receipt readiness, and exact-entry leader eligibility; committed-write errors still retain their exact hashes and unsafe-retry indication.
