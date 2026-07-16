---
"@peerbit/rpc": patch
---

Reject RPC requests with their original abort reason when the signal is already
aborted, aborts while key/envelope setup is still pending, or carries a
`TimeoutError`. Canceled setup no longer registers an interceptor/resolver or
invokes the transport, while synchronous and asynchronous transport timeout
failures retain their existing best-effort behavior.
