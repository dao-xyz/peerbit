---
"@peerbit/shared-log": patch
---

Recover replication capability exchange after a one-sided topic-session reopening, including a fresh instance whose departure announcement was lost, without requiring an application readiness waiter. Reuse the authenticated, bounded rearm handshake and require a fresh Full sequence before an old stream can confirm readiness again.
