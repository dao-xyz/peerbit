---
"@peerbit/blocks-interface": minor
"@peerbit/blocks": minor
"peerbit": minor
---

Add an opt-in, crash-safe scoped-reference namespace for physical block
reclamation on Peerbit's built-in persistent Node store. Managed blocks remain
isolated from legacy raw blocks, unsupported stores fail closed, and public
health and limit metadata let callers verify the capability before use.
