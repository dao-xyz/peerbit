---
"@peerbit/log": minor
"@peerbit/shared-log": minor
"@peerbit/document": minor
"@peerbit/trusted-network": patch
---

Add an optional reject-only `canJoin` preflight before recursive join dependency resolution, including native receive fallback. Documents exposes it as `log.canJoin` with detached callback entries. Existing signature and application admission checks still apply; this does not enable checkpoint import or history retirement.

Keep JavaScript recursive CUT cleanup within locally indexed graph metadata, matching the native planner. Cleanup no longer loads entry payload blocks or crosses an unindexed predecessor to delete older indexed entries.

Reuse TrustedNetwork's canonical public EntryV0 scanner from `@peerbit/log`, preserving the existing TrustedNetwork aliases. The scanner checks framing before decoding; callers still provide byte limits, CID checks, signature verification, and application policy.
