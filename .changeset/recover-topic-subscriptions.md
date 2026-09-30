---
"@peerbit/pubsub": patch
---

Re-announce local subscriptions after shard attachment or child attachment, coalesce recovery work, and apply signed root-claim batches before remapping shards. Announce successful shard subscriptions independently when another shard join fails. Exchange signed subscription state with direct neighbours so bootstrap/dial root-policy differences do not prevent direct discovery, while preserving configured roots and lifecycle fencing.

Retain bounded unsubscribe timestamps so delayed Subscribe messages from the other delivery path cannot immediately restore a departed subscription. Clear these timestamps when the local topic is removed or the service stops.
