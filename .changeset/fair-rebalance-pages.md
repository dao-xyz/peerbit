---
"@peerbit/shared-log": patch
---

Yield to transport and lifecycle work between rebalance scan pages instead of draining synchronously resolved SQLite pages in one event-loop turn. This does not change iterator ordering or replace OFFSET paging.
