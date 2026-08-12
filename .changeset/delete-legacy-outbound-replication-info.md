---
"@peerbit/shared-log": patch
---

Delete the dead legacy outbound replication-info machinery (B12 stage 3): the legacy announcement broadcast tail with its retry/repair workers, the startup All/ResponseRole sends and request polling on subscription change, the legacy waitForReplicator request arm, and the close/drop empty-All broadcasts. Default-mode nodes never executed these paths since compatibility opens were retired. Internal-only: no exported symbol changes; the wire tombstones stay registered and exported, the V2 announcement/recovery stack is unchanged.
