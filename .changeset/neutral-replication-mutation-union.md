---
"@peerbit/shared-log": patch
---

Internal replication announcements now flow as a neutral full/added/stopped mutation union instead of the retired legacy wire classes. The advanced `announce` callback option on `replicate()`/`startAnnounceReplicating()` receives the tagged mutation objects (`{ full: { segments } }` / `{ added: { segments } }`) instead of `AllReplicatingSegmentsMessage`/`AddedReplicationSegmentMessage` instances, and the internal `LegacyReplicationInfoMessage` type alias is no longer exported from the (non re-exported) `replication-info-v2-send` module. Wire bytes, sequencing and coalescing are unchanged; the legacy classes remain exported decode tombstones.
