---
"@peerbit/stream": patch
---

Release cancelled or failed ACK waits' route-health checks without cancelling other active waits or pruning replacement routes. Keep cleanup bound to the original wait when a later message reuses its ID.
