---
"@peerbit/native-backbone": patch
---

Open existing coordinate files and atomic-replacement temporary files with non-truncating writable handles before syncing. This fixes Windows permission failures during durable native-coordinate reopen while preserving missing-file and real sync failures.
