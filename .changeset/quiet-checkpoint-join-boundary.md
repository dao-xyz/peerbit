---
"@peerbit/log": minor
"@peerbit/shared-log": minor
"@peerbit/document": minor
"@peerbit/trusted-network": patch
"@peerbit/blocks": patch
---

Add an optional reject-only `canJoin` preflight before recursive join dependency resolution, including native receive fallback. Documents exposes it as `log.canJoin` with detached callback entries. Existing signature and application admission checks still apply; this does not enable checkpoint import or history retirement.

Keep JavaScript recursive CUT cleanup within locally indexed graph metadata, matching the native planner. Cleanup no longer loads entry payload blocks or crosses an unindexed predecessor to delete older indexed entries.

Reuse TrustedNetwork's canonical public EntryV0 scanner from `@peerbit/log`, preserving the existing TrustedNetwork aliases. The scanner checks framing before decoding; callers still provide byte limits, CID checks, signature verification, and application policy.

Defer Documents query RPC startup until lower-log opening, local log recovery and document backend validation succeed. Preserve custom index open overrides and standalone DocumentIndex opening. This is not a checkpoint publication barrier: it does not gate local reads, replication or later Program/parent lifecycle callbacks.

Add the versioned `CheckpointDocuments` resource for fixed Ed25519 writers, bounded public JSON documents and an identity index. Its checkpoint and signed epoch admission are mandatory. Use retained APPEND tombstones, deterministic causal-frontier projection, a crash-safe checkpoint floor, signed frozen-writer reconciliation and unanimous approvals before activating a successor epoch. Recovery completes before query or replication startup. Seals omit deleted-only keys while preserving all concurrent live/delete tips. Existing Documents descriptors and operation bytes do not change. All fixed writers must participate in a seal; no automatic migration, physical history deletion, remote delivery guarantee, confidentiality or adaptive replication is implied.

Forward the block service's existing local crash-safe durability capability without synthesizing support for unsupported stores.
