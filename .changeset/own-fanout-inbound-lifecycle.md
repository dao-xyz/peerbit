---
"@peerbit/pubsub": patch
"@peerbit/stream": patch
---

Fence fanout payload verification, dispatch and reply fallbacks to their exact channel and lifecycle. Stale verification cannot update peer sessions or consume the replacement channel's dedupe slot. Observe asynchronous payload failures while suppressing only expected not-started errors after their owner becomes stale; active and unexpected failures remain logged.
