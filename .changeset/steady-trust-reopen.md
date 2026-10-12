---
"@peerbit/trusted-network": patch
---

Reinitialize a closed document child when reopening the same TrustedNetwork or IdentityGraph instance. Already-live shared children remain reused without resetting their runtime or authorization.
