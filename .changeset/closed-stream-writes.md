---
"@peerbit/stream": patch
---

Reject direct and queued writes to closed peer streams, including writes that were waiting for outbound queue capacity when the stream closed.
