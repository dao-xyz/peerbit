---
"@peerbit/indexer-rust": patch
---

Reuse checksum-validated V1 snapshot and journal bytes when rebuilding native-compatible indexes to avoid serializing every restored document a second time.
