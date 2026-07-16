---
"@peerbit/program": patch
"@peerbit/shared-log": patch
"@peerbit/native-backbone": patch
"peerbit": patch
---

Make program graph open, close, drop, and handler stop race-safe and retryable
after partial failures; preserve parent/child ownership through rollback; fence
concurrent initialization and teardown; and retain cleanup ownership until all
terminal work completes. Direct lifecycle reentry into the owning handler stop
now rejects instead of deadlocking; lifecycle code must schedule stop from its
external owner rather than await its own teardown. Interrupted native persistence
drops can now resume their durable tombstone on the same adapter generation.
