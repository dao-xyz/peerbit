---
"@peerbit/libp2p-test-utils": patch
---

Support `abort()` on in-memory connections, which the stream layer calls when it rejects a recently pruned peer. Aborting closes both ends at once: their streams are aborted, the local end reports "aborted" and the remote end "reset", both connection managers drop the connection immediately, and topologies hear the disconnect as they do after `close()`.
