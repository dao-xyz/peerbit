---
"@peerbit/shared-log": patch
---

Correlate checked-prune grants with exact per-generation IDs and authenticated peers, then revalidate ownership and quorum at the serialized delete boundary. Revoke stale receipts across peer churn, retain expired background handoffs in a bounded low-rate audit, and prune newly exposed parents through independent nonrecursive generations.

This is a fail-closed protocol cutover: legacy uncorrelated prune messages no longer authorize deletion. During a mixed-version rollout, peers retain extra copies until every participant in a handoff has upgraded.
