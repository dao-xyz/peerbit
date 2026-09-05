---
"@peerbit/stream": patch
---

Drain stream shutdown during libp2p's before-stop phase, before connection shutdown begins. Await existing peer closes and pending outbound opens while preserving subclass cleanup in the normal stop phase.
