---
"@peerbit/stream": patch
"@peerbit/network-rust": patch
---

Keep unexpired relay routes and the remote session when only a direct connection closes, instead of dropping the peer. A peer is reported unreachable when its last route is lost. If the peer itself went away while a relay route to it was still recorded, that now happens when the liveness probe sent on the disconnect times out (`seekTimeout`, 10 s by default), not at the disconnect. The departure hint (Goodbye) to peers that reached the lost peer through this node is held back while another path remains, and is sent if that path then fails the probe, so their routes cannot keep looping back through this node. A peer that learns of the departure only from that Goodbye stops treating the lost peer as reachable when it arrives and reports it unreachable after its own probe, about twice `seekTimeout` after the peer left. Applies to both JavaScript and Rust-backed routing.
