---
"@peerbit/pubsub": patch
---

Preserve provider-cache expiry when discovery returns no fresh evidence. Repeated cache reads and empty tracker replies no longer renew stale providers indefinitely; fresh replies retain the existing cache TTL behavior.
