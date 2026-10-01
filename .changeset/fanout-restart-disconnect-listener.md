---
"@peerbit/pubsub": patch
---

Re-register the fanout tree's underlay `peer:disconnect` listener when the service starts again after `stop()`, so a restarted instance handles peer disconnects again.
