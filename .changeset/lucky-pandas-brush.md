---
"@peerbit/native-backbone": patch
---

Use non-literal specifiers for the node-only fs/path dynamic imports so browser bundlers (esbuild `--platform=browser`) no longer fail resolving `node:fs/promises` and `node:path` when bundling the package
