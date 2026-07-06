---
"@peerbit/shared-log-rust": patch
---

Serialize wasm init to fix a double-init race under concurrent loads (browser use-after-free).
