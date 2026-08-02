---
"@peerbit/document": patch
---

Allow index transforms to explicitly preserve Borsh getter/setter fields when Documents adds its local context wrapper, enabling compatible persisted-index migrations that discard large derived payloads.
