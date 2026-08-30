---
"@peerbit/trusted-network": patch
---

Require trust-relation deletes to be signed by the relation owner and add an
idempotent `TrustedNetwork.revoke()` API for removing the caller's outgoing
trust edge.
