---
"@peerbit/pubsub": patch
---

Reset peer connections when fanout KICK control delivery fails so removed children cannot retain a stale parent.
