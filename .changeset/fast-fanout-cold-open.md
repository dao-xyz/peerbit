---
"@peerbit/pubsub": patch
"@peerbit/shared-log": patch
---

Prefer live fanout join candidates, bound cold-open dial, join, and topic-root
query waits by overall deadlines, keep proactive shard-host scans local and
bounded, suppress a directly departed origin's current signed auto-root claim,
and expose aggregate fanout-open profile diagnostics.
