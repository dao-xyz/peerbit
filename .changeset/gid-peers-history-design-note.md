---
"@peerbit/shared-log": patch
---

Document why the `_gidPeersHistory` map cannot be released from the trim path. Comment only; no behavior change. The note records the growth shape (one row per distinct gid ever held, released only by prune, last-peer-drop, an explicit cache-clearing rebalance, and close/reset), why copying prune's whole-row delete onto trim would drop a live suppression memo and pay for the freed memory in redundant delivery and repair traffic, and what a correct bound would require.
