---
"@peerbit/shared-log": patch
"@peerbit/time": patch
---

Retry timed-out replication announcements from an authoritative current-state snapshot while preserving explicit caller errors, adaptive scheduling, and shutdown ordering. Make fixed-interval debouncer shutdown terminal so queued work cannot restart after close.
