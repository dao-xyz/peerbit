---
"@peerbit/log": patch
---

Close the log's indexer when `Log.fromEntry` replay fails before returning ownership to its caller. Preserve the replay error, and report both errors if cleanup also fails, without deleting the caller's blocks.
