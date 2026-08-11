---
"@peerbit/shared-log": patch
---

Escalate the persistent V2 recovery unpark delay against silent-but-subscribed peers (doubling per fruitless cycle, capped at 5 minutes, reset on any applied V2 progress), stop rotating a still-current capability grant on unpark so in-flight Fulls answering the pre-park challenge still apply, and coalesce session-less capability-triggered subscriber snapshot requests to one in flight per peer.
