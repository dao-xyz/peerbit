---
"@peerbit/indexer-interface": patch
"@peerbit/indexer-simple": patch
---

Add an explicit iterator consistency option. In-memory index iteration keeps
snapshot consistency by default, while unsorted callers can opt into lazy,
bounded paging with `consistency: "weak"`. Weak cursors remain finite while
allowing unvisited deletes and replacements to be observed.
