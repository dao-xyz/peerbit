---
"@peerbit/pubsub": patch
---

Stop provider queries from renewing cached provider hints that no tracker reported again. When the cache held fewer candidates than requested, `queryProviderCandidates` stored every merged candidate, cached hints included, with a new expiry, so repeated queries kept a stopped provider listed indefinitely. A tracker's directory is the same cache, so a tracker that queried one of its own namespaces kept announced providers past their announce TTL and served them to other peers. Now only providers in a fresh tracker reply are renewed, with the requested `cacheTtlMs` as before; other cached hints keep their expiry. The cache also keeps the addresses from a fresh reply instead of overwriting them with the older cached copy.
