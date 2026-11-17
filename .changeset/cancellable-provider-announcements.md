---
"@peerbit/pubsub": patch
---

`FanoutTree.announceProvider` and `announceProviders` accept a `signal` that cancels the announcement, including a pending bootstrap dial and a control send that waits for queue capacity. Stopping the service still cancels it as well.
