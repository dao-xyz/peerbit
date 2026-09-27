---
"@peerbit/log": minor
"@peerbit/shared-log": minor
"@peerbit/document": minor
---

Add an optional reject-only `canJoin` preflight before recursive join dependency resolution, including native receive fallback. Documents exposes it as `log.canJoin` with detached callback entries. Existing signature and application admission checks still apply; this does not enable checkpoint import or history retirement.
