---
"@peerbit/native-backbone": patch
"@peerbit/shared-log": patch
"@peerbit/shared-log-rust": patch
---

Route new entries to strict range replicators when they intersect the entry coordinates, even when strict ranges are excluded from the full-replica fallback. This restores live document-stream delivery without broadcasting each append to every peer.
