---
"@peerbit/shared-log": patch
---

Keep bounded capability and V2 confirmation recovery active during persisted delivery, including incomplete receive state and replacement sessions. Reuse the existing readiness watchdog for fresh leader candidates without weakening exact-entry, session, ownership, or durable receipt checks. Recovery does not occupy transfer slots, and authenticated key lookups and advisory waits are coalesced in a bounded per-delivery pool.
