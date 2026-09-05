---
"@peerbit/stream": patch
"@peerbit/pubsub": patch
---

Keep same-identity stream replacements independent of obsolete close, reader, and
disconnect callbacks. Preserve negotiated session state and reattach pubsub
readiness listeners to the replacement. Pending retired closes remain bounded
and owned by the network shutdown barrier.
