---
"@peerbit/shared-log": patch
---

Bound Simple sync response authorization and active delivery, capacity waiters, coordinate resolution, incoming claim and resolver work, retry scans, and per-target retry fanout; give admitted new claimants a prompt request, propagate cancellation through delivery, and use indexed expiry and cleanup paths for abandoned work. Response authorization now follows cross-caller conflicts through request delivery and payload success or failure, retains non-abortable transport work across lifecycle rollover, and starts its response deadline only after a request is sent. Coordinate request frames are now limited to 1,024 symbols, and larger custom sender chunk settings are clamped to that receiver work limit.
