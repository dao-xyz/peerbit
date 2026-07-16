---
"@peerbit/crypto": patch
---

Declare `multiformats` and `uint8arrays` as runtime dependencies so clean and nested package installs can import the published crypto package without relying on dependency hoisting. Remove the unused runtime dependency on `@peerbit/cache`.
