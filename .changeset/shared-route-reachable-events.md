---
"@peerbit/stream": patch
---

Notify every active protocol sharing local routes when a route or session change makes a peer reachable. Wake existing waiters when a route becomes distance-zero eligible without repeating notifications for equivalent additional paths. Preserve protocol-local isolation and ignore obsolete notifications after synchronous route removal or service shutdown.
