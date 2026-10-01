---
"@peerbit/stream": patch
---

Notify all active protocols when a shared route becomes unreachable, so a disconnect cannot leave stale subscriber state in another service. Preserve alternate routes and replacement sessions, and finish route cleanup when closing an empty peer stream re-enters removal.
