---
"@peerbit/pubsub": patch
---

Stop closing a healthy connection after an event-loop stall. When several fanout channels were joining through the same peer, one stall fired all of their join-request timers in the same tick. Each timeout counted toward that peer's reset threshold, so the join loop closed the connection to a peer that was still answering. A join timeout now counts only if nothing arrived from the peer after the request went out, and at most once per request window, however many channels were waiting. Anything the peer sends starts the count over. A peer that stays silent for three consecutive request windows is still reset as before.
