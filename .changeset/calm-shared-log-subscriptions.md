---
"@peerbit/shared-log": patch
---

Drain admitted pubsub subscription callbacks and abort generation-owned replication-info sends before terminal teardown. Old request snapshots can no longer publish after reopen, and subscription plus legacy-role replies reuse one replication snapshot.
