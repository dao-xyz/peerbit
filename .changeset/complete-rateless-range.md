---
"@peerbit/riblt": patch
"@peerbit/shared-log": patch
"@peerbit/shared-log-rust": patch
"@peerbit/native-backbone": patch
---

Use exclusive circular range endpoints consistently in rateless synchronization so a receiver that already holds the sender's entries does not report the last entry as missing. Include the maximum hash coordinate in wrapped ranges in both indexed and native resolution, preserving receive limits and collision multiplicity. Hash-number range resolvers now interpret a nonzero start with end zero as the high segment through the ring maximum; `(0, 0)` remains empty. Update custom range resolvers and deploy the coherent JavaScript/WASM cohort together.
