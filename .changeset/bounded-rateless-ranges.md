---
"@peerbit/shared-log-rust": patch
"@peerbit/native-backbone": patch
"@peerbit/shared-log": patch
---

Bound receive-side rateless range materialization in JavaScript and both native
backends through an explicit limited-range capability, fall back to paged index
iteration when an older native package lacks that capability, fall back to
simple sync on overflow, and page simple coordinate lookups without using
`iterator.all()`.
