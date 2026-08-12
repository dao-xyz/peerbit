---
"@peerbit/shared-log": major
---

Delete the dead legacy inbound replication-info machinery (B12 stage 4): the ResponseRole/All/Added/Stopped dispatch arms below the unconditional default-mode drop, the legacy apply handlers, the per-peer ordering watermark and the legacy-cutover probe. Default-mode nodes have dropped these frames before any side effect since the V2 migration, and no compatibility mode can open. The wire tombstones stay registered and exported so old frames still decode; the only surface change is the removal of the deprecated `toReplicationInfoMessage()` helper from the exported `ResponseRoleMessage` tombstone (its class, variant and fields are unchanged; the sole caller was the deleted dispatch conversion). Folds into the B12 major release train.
