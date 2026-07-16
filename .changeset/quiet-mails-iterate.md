---
"@peerbit/document": patch
---

Honor `throwOnMissing` and bounded missing-response retries when a remote
document iterator responder disappears between pages. Track default-cover and
prefetched remote iterator identities before responses settle, close them from
implicit consumers and aborted requests, detach completed lifecycle listeners,
retain every superseded prefetch ID across bounded retries, avoid redundant
closes for drained peers, and forward nested remote RPC options to follow-up page
requests.
