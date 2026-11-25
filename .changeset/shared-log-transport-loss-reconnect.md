---
"@peerbit/shared-log": patch
---

A replica that drops off the network and comes back with the same replication ranges no longer costs two full rebalance passes on each side. When pubsub reports the peer as unreachable and every remaining replica still leads every entry, the peer's removal is held for up to ten seconds. If the peer's next full replication announcement brings back exactly the same ranges, both passes are skipped: the entries written or received while the peer was away, and those it had not confirmed when it left, are sent to it straight away (the most recent 1,024; entries imported through `SharedLog.join` are left to the repair), and the targeted repair for that peer runs instead of the passes. A different return, a pass started for any other reason, or the end of the wait runs the usual passes.
