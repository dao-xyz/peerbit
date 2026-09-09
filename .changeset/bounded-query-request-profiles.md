---
"@peerbit/document": patch
"@peerbit/rpc": minor
"@peerbit/diagnostics": minor
---

Add bounded, opt-in document-query and RPC-request diagnostics for local lookup, cover selection, sampled target identities, setup, publishing, accepted responses, deadlines, and result introduction. Documents uses its existing sync profile callback; RPC also accepts a request profile callback. Observer failures are isolated and traces reserve a terminal event while suppressing late events. These diagnostics do not alter query coverage, timeouts, authorization, delivery receipts, or wire/storage formats.
