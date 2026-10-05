---
"@peerbit/vite": patch
---

Replace the glob-watching static-copy dependency with literal asset routes and the existing copy helper, removing the production chokidar 3/braces dependency chain. Preserve public-file precedence, custom asset additions, proxy/base routing, build output with `copyPublicDir: false`, and the legacy SQLite Wasm alias without creating another watcher.
