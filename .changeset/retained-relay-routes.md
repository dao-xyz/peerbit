---
"@peerbit/stream": patch
"@peerbit/network-rust": patch
---

Preserve valid relay routes and the remote session when only a direct connection closes. Report unreachable peers only after their last route is lost, and avoid sending a departure hint while an alternative path remains available. Applies to both JavaScript and Rust-backed routing.
