---
"@peerbit/log": patch
"@peerbit/shared-log": patch
---

Allow callers to cancel log joins without cancelling another caller's shared join, while draining already-started writes and callbacks. Cancel missing-parent receives when their SharedLog receive generation closes so shutdown does not wait for the remote block timeout.
