---
"peerbit": patch
"@peerbit/react": patch
---

Negotiate a Peerbit Yamux profile with a 4 MiB initial stream window between
updated peers, while retaining standard Yamux fallback for older peers. This
removes repeated flow-control stalls during concurrent large block responses
without breaking mixed-version connections.
