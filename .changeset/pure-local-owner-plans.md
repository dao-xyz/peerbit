---
"@peerbit/shared-log": patch
"@peerbit/shared-log-rust": patch
---

Add an internal, issued-view-only owner planner that hydrates the native range
planner once and returns canonical bounded decisions without querying an Index.
Give standalone native range planners an explicit idempotent close/free
lifecycle and reject every operation after release.
