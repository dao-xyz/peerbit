---
"@peerbit/shared-log": patch
---

Make checked pruning use request-correlated, acknowledged handoffs with monotonic capability repair, exact-generation completion and resends, and responder ordering so stale responses, lost grants, and circular handoffs cannot authorize deletion. Revalidate block presence, responder leadership, requester ownership, and exact confirmation quorums at the final grant and deletion boundaries; use non-recursive removal and fail closed for legacy peers. Bound inbound correlated-prune vectors and hash lengths, retained missing-entry and retry state, and concurrent request/grant delivery; cap per-peer and global handler/callback work, keeping deadline-abandoned non-cancellable reads charged to their original lifecycle admission until they settle. Retire exhausted retries and debounce-only candidates instead of retaining entry state.
