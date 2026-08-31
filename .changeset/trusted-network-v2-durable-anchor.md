---
"@peerbit/trusted-network": patch
---

Add an internal crash-safe durable anchor for the non-activatable
TrustedNetwork v2 policy reducer, using generic storage barriers and preserving
fail-closed unavailable and fork evidence across reopen.
