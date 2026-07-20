---
"@peerbit/program": patch
---

Harden terminal lifecycle proof and failed-open ownership cleanup so forged
close or drop overrides cannot bypass base cleanup, while legacy programs still
reconcile released owner edges exactly.
