---
"@peerbit/pubsub": patch
---

Re-register the fanout tree's underlay `peer:disconnect` listener when the service starts again after `stop()`. A restarted instance reacts to disconnects again: it detaches from departed parents, prunes departed children, and drops departed peers' provider watches.
