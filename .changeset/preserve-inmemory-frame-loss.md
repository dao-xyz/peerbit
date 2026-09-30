---
"@peerbit/libp2p-test-utils": patch
---

Apply simulated packet loss to complete length-prefixed frames, including fragmented headers and payloads or coalesced writes. Preserve following control messages, per-frame metrics, and backpressure, and discard incomplete frames when a stream closes.
