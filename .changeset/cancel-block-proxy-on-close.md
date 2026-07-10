---
"@peerbit/blocks": patch
---

Cancel active relay proxy lookups when the block transport stops, and reject late provider results before they can start a timeout-backed remote read, so shutdown does not wait for the request timeout.
