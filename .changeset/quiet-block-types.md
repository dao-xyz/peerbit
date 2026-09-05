---
"@peerbit/blocks-interface": patch
---

Erase type-only imports from the runtime entrypoint so importing block helpers does not unnecessarily evaluate the stream message and crypto module graph. This avoids an accidental nested-module initialization path in hoisted installations; it does not make arbitrary duplicated Peerbit cohorts interoperable.
