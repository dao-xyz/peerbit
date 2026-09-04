---
"@peerbit/trusted-network": patch
---

Add an internal bounded lease that authenticates a policy entry by exact CID and runs a callback only while its sequence and body digest match an already-published durable current TrustedNetwork v2 policy head. This precondition slice does not ingest or checkpoint a newly fetched head.
