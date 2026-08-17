---
"@peerbit/shared-log": patch
"@peerbit/native-backbone": patch
---

Add an internal, unwired Node custody-record persistence adapter backed by bounded SQLite point reads and a crash-released namespace lock. The adapter binds records to the exact log, local key, role, profile, and durable writer domain, while deliberately exposing no transfer, pin, release, deletion, or prune authority.
