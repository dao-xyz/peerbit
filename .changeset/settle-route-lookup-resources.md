---
"@peerbit/pubsub": patch
---

Release route-lookup timers and abort listeners when a lookup settles or its fanout channel closes or detaches. Keep late local callbacks from completing replacement proxy searches, while preserving independent deadlines for coalesced callers.
