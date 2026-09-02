---
"@peerbit/blocks-interface": patch
"@peerbit/blocks": patch
"peerbit": patch
---

Expose immutable local block-store ownership and reclamation metadata through
`peer.services.blocks`. Built-in stores identify their node-wide block-service
reference domain, while custom stores fail closed as unknown unless their
caller supplies an explicit truthful declaration.
