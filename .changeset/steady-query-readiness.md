---
"@peerbit/document": patch
---

Make the blocking remote-query wait (`remote.wait.behavior: "block"`) see every replication event. An event that arrived while the wait read the cover was lost, so the wait ran until a later event or its deadline although a candidate was available. The wait now listens before its first read and reads again after events that arrive during a read. An abort before or during the first read now ends the wait at once; before, the wait ignored it and ran to its deadline, or with a zero timeout kept its listeners for good. The deadline now also bounds the first read. Closing or aborting a blocking iterator before its first `next()`, or letting an `onTimeout: "error"` deadline pass first, no longer leaves an unhandled promise rejection, which ends a Node.js process by default; a later `next()` still rejects with the timeout. Timeout defaults are unchanged, and readiness still only means that a non-self cover candidate exists.
