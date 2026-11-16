---
"@peerbit/document": patch
---

A delete that arrived out of order could remove a newer version of the same document, including a re-granted trust relation. A received or local delete now removes a document only while its indexed version is the one the delete names, or descends from it. Deletes applied because entries were trimmed or pruned still remove by key. Upgrading does not restore a version that was already lost; `docs.recover()` or a new put does. The head check and the removal run as one index call (`DocumentIndex.delIfHead`): a native backbone document index checks the head when it removes the document, after any write queued ahead of it, and other indexes run one delete that matches both the id and the head.
