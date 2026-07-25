---
"@peerbit/pubsub": patch
---

Converge auto-discovered shard roots across late peer joins and bind direct root-control messages to their verified transport peer.

Leaves that use a queried gateway's internal shard roots must disable automatic candidates with `setTopicRootCandidates([])`; locally configured roots, resolvers, and trackers remain authoritative.
