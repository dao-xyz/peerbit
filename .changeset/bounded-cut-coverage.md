---
"@peerbit/log-rust": patch
"@peerbit/native-backbone": patch
---

Use existing reverse-edge and head membership indexes to avoid unrelated CUT-head scans during native log join planning. Retain the amortized batch scan when estimated relevant adjacency work is larger. Preserve exact tombstone coverage and join decisions without removing history or changing storage or wire formats.
