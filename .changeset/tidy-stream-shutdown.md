---
"@peerbit/stream": patch
---

Cancel inbound pruning when peer streams close and reject late inbound or outbound attachments, disposing their raw streams so shutdown cannot retain timers or revive a closed peer.
