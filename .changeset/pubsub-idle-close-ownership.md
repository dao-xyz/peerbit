---
"@peerbit/pubsub": patch
---

Keep ephemeral fanout idle timers owned by their exact channel and lifecycle. Prevent pending publish completions from rearming timers during shutdown or touching replacement channels after restart, and ignore stale timer callbacks without changing idle deadlines or delivery behavior.
