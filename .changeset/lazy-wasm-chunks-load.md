---
"@peerbit/shared-log-rust": patch
---

Let browser bundlers emit the lazy wasm-bindgen glue chunk so the native shared-log planner loads in Vite applications instead of requesting a missing `/wasm/shared_log_rust.js` path.
