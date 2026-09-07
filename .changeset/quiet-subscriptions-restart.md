---
"@peerbit/pubsub": patch
---

Renew subscription batching after a completed stop/start of the same service. Preserve queued reference counts on repeated start, and prevent already-flushing subscription announcements from publishing to or closing a replacement lifecycle's fanout channel.
