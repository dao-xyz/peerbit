---
"@peerbit/pubsub": patch
---

Stop closing a healthy connection after an event-loop stall. When several fanout channels were joining through the same peer, one stall fired all of their join-request timers in the same tick. Each timeout counted toward that peer's reset threshold, so the join loop closed the connection to a peer that was still answering. Join timeouts now count once per request window, however many channels were waiting, and an answer that arrives before its request times out starts the count over. The connection is reset after three consecutive windows in which nothing arrived from the peer, or after six consecutive windows with no answer in time while the peer keeps sending other frames (a connection that loses or badly delays only our frames).

Resets of an unresponsive peer take longer. With the default join timing, a silent peer is reset after about 14 s however many channels join through it (before, three channels timing out together reset it after about 2 s). A peer that keeps sending frames but answers no join in time is reset after about 32 s (before, about 14 s with one channel joining and about 2 s with three). That now includes a peer whose answers all come after the join timeout: before, with one channel joining, such a peer was never reset if its answers came less than about 14 s late.
