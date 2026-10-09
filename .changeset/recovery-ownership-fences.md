---
"@peerbit/log": patch
"@peerbit/rpc": patch
---

Expose non-waiting lower-log mutation fences and optional RPC ownership checks for bounded recovery work. RPC callers can track physical setup/publish settlement after logical cancellation without treating it as a remote delivery acknowledgment.
