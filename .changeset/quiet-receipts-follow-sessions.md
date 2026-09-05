---
"@peerbit/shared-log": patch
---

Keep persisted receipt confirmation progressing across delayed replication-state commits and replacement sessions. Retry outstanding application queries without continuously replacing their sequence, recover missing sender state during persisted delivery, and recheck the current session and replicator eligibility when readiness transition events are missed. Existing authenticated session, exact entry, leader eligibility, cancellation, and durability checks remain required.
