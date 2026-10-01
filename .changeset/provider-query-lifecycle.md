---
"@peerbit/pubsub": patch
---

Retire provider-query requests and cancel pending control sends on timeout, cancellation, or service stop. Report send failures through the awaited query and cancel its remaining tracker attempts without waiting for blocked signing.
