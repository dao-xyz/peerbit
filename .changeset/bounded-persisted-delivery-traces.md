---
"@peerbit/shared-log": patch
---

Add opt-in bounded persisted-delivery traces to the existing `sync.profile`
callback. Report exact-entry leader/session selection, confirmation and receipt
phases, provisional receipt progress, and terminal outcomes without changing
durability checks, retries, or deadlines. Traces omit entry hashes and payloads,
cap detail events and entry sampling, and isolate callback failures.
