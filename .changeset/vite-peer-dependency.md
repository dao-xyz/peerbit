---
"@peerbit/vite": major
---

Vite is now a required peer dependency (`^7.3.8 || ^8.3.7`) instead of a dependency, so the plugin runs on the application's own Vite and no longer installs a Vite of its own; it imports only Vite's types. Install `vite` next to the plugin, at 7.3.8 or newer on Vite 7 or 8.3.7 or newer on Vite 8, the first releases with the fixes for Vite's published dev-server security advisories, and keep `@peerbit/vite` with `vite` in `devDependencies`, because npm counts the peer of a production dependency as production. When the application's `vite` cannot resolve inside the peer range, for example on Vite 6 or with an exact pin below a floor, npm stops with `ERESOLVE` and pnpm and Yarn report an unmet peer. Plugin options, behaviour and the supported Node.js range are unchanged.
