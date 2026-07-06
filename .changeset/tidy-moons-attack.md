---
"@peerbit/log-rust": patch
---

Refactor the crate into native-safe cores with a thin wasm surface: core logic now returns a real `LogError` type instead of `Result<_, JsValue>`, so error paths (malformed entries, CID mismatches, bad signatures) return catchable errors on native targets instead of aborting the process. The published wasm API and all error messages reaching JS are byte-identical.
