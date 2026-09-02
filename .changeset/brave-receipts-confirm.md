---
"@peerbit/shared-log": patch
---

Require each persisted-receipt target's exact active peer generation to confirm applying the writer's latest V2 replication-role revision before the receipt-carrying transfer, bypass stale leader-selection context for receipt plans, and retain revision-fenced background dissemination to fresh entry leaders, full replicas, and cross-GID owners. Only entry leaders remain eligible for persisted quorum evidence; receipt-capable peers without V2 application confirmation fail closed.
