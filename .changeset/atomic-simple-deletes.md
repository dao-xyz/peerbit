---
"@peerbit/indexer-simple": patch
---

Make deletes with several predicates atomic. A put that replaced a row while a delete was matching it could be deleted even though it no longer matched. A delete now removes a row only while it is the stored value its predicates matched, and matches a replacement again.
