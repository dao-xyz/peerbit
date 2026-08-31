---
"@peerbit/trusted-network": patch
---

Bound the internal TrustedNetwork v2 durable fork evidence and admission
windows, commit to the complete fork-proof set, and make a durably published
authority fork a constant-time policy-admission fail-stop. Validate and copy
policy bytes from their intrinsic typed-array extent so caller hooks cannot
bypass those bounds.
