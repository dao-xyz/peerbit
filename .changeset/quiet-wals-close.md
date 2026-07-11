---
"@peerbit/any-store-rust": patch
"@peerbit/shared-log": patch
---

Avoid rewriting the complete native durable block snapshot on every program close. Rust-backed sublevels can now defer close-time compaction below an explicit journal threshold while preserving crash-safe WAL recovery, generic store defaults, and immutable cached-sublevel policies.
