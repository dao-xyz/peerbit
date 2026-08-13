---
"@peerbit/libp2p-test-utils": patch
"@peerbit/react": patch
"peerbit": patch
---

Move `@libp2p/webrtc` to `^6.0.29`, which depends on `node-datachannel ^0.32.3` instead of `^0.29.0`.

`node-datachannel@0.29.0` publishes no prebuilt binaries at all, so every install compiled it from source. That source build fails on Node 24 — `prebuild`/`prebuild-install` are deprecated and their `napi-build-utils` cannot resolve NAPI v10, aborting with "does not support N-API version undefined" (upstream: https://github.com/murat-dogan/node-datachannel/issues/428). `0.32.3` ships napi-v8 prebuilds for every supported platform, and N-API is ABI-stable, so installs no longer need a compiler or the broken build tooling.
