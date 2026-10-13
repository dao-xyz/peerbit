---
"@peerbit/document": minor
"@peerbit/shared-log": patch
"@peerbit/log": patch
---

Add the persisted `owner-proofs-v1` format to the existing Documents class. New
fixed-owner stores authenticate complete bounded same-document delete evidence,
preserve independent replacements and cross-document boundaries, and recover
interrupted retirement before serving queries or issuing persisted receipts.
The first profile requires strict disk durability and full replication; oversized
proofs and unsupported policies or batching reject before mutation. Existing
store bytes and deletion semantics are unchanged; no in-place migration or
mutable trust-graph revocation support is implied.
