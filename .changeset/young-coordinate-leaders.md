---
"@peerbit/shared-log": patch
"@peerbit/shared-log-rust": patch
"@peerbit/native-backbone": patch
---

Keep coordinate-selected young replicas in leader plans when a mature-only full-replica shortcut is incomplete. Preserve existing mature fallback owners, strict-range policy, peer filters, and maturity thresholds. Apply the same routing rule to TypeScript, Rust range planning, and the native backbone's embedded planner, including batch and local-leader checks. Persisted receipts remain the durability proof; this routing correction does not itself establish the cause of intermittent delivery timeouts.
