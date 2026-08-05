---
"@peerbit/document": patch
"@peerbit/log": patch
---

Normalize log entries from duplicate package installations across both sides of
remote indexed-query responses, preventing valid responses from timing out or
being rejected when the same `@peerbit/log` version has multiple runtime class
identities.
