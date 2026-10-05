---
"@peerbit/stream-interface": patch
"@peerbit/stream": patch
"@peerbit/shared-log": patch
---

Wake pending replication-info recovery when a peer's new transport stream becomes writable. Keep retry work bound to its current peer session and receive generation, and ignore callbacks from superseded retry timers. Writable readiness remains a scheduling hint, not replication confirmation or a persisted delivery receipt.
