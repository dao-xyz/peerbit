---
"@peerbit/shared-log": patch
---

Serialize replication-range mutations, validate bounded owner snapshots, and reconcile ambiguous durable writes before publishing ownership changes. Fence append planning, pruning, repair, warmup, and announcements across ownership failure and close or reopen boundaries.
