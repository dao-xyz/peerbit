---
"@peerbit/shared-log-rust": patch
"@peerbit/native-backbone": patch
"@peerbit/shared-log": patch
---

Bound receive-side rateless range materialization in JavaScript and both native
backends, fall back to simple sync on overflow, and page simple coordinate
lookups without using `iterator.all()`.
