---
"@peerbit/shared-log": patch
"@peerbit/shared-log-rust": patch
---

Bound native shared-log fallback sampling to linear endpoint-index work per
cursor while preserving deterministic owner selection. Replace the synthetic
maximum metadata sentinel with coordinate-only partitions so maximum-timestamp,
high-Unicode identifiers remain in their correct fallback segment, and apply
metadata tie-breaking across circularly aliased zero/MAX endpoints. Split the
TypeScript directional fallback into disjoint monotone phases, treat exact
directional equality as zero distance, and exclude the current range from
same-owner adjacency queries. Keep prefetched join rows visible to iterator
completion and pending counts so batched fallback scans cannot truncate them,
drain `all()` through the ordered merge, and release buffered rows on close.
