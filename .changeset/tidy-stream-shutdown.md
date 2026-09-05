---
"@peerbit/stream": patch
---

Cancel inbound pruning when peer streams close and reject late inbound or outbound attachments, disposing their raw streams so shutdown cannot retain timers or revive a closed peer.

Drain stream shutdown during libp2p's before-stop phase, before connection shutdown begins. Await existing peer closes and pending outbound opens while preserving subclass cleanup in the normal stop phase. Abort rejected, previously pruned connections before removing their peer-store state so concurrent protocol negotiation cannot race a graceful TCP shutdown.

Recover transient outbound negotiation resets within the existing attempt budget, and discard definitively closed connections or multiplexers so subsequent dials can reconnect.
