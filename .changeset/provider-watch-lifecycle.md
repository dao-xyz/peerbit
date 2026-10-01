---
"@peerbit/pubsub": patch
---

Cancel fanout control sends when message creation outlives the sending service, preventing provider-watch teardown from publishing after shutdown. Do not renew closed provider watches after pending bootstrap discovery completes, or let obsolete cleanup unsubscribe a replacement watch after restart. Active-service errors and awaited cancellation remain observable.
