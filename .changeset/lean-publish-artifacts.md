---
"peerbit": patch
"@peerbit/test-utils": patch
"@peerbit/log": patch
"@peerbit/log-rust": patch
"@peerbit/document": patch
"@peerbit/shared-log": patch
"@peerbit/shared-log-rust": patch
"@peerbit/blocks": patch
"@peerbit/network-rust": patch
"@peerbit/pubsub": patch
"@peerbit/stream": patch
"@peerbit/any-store-rust": patch
"@peerbit/crypto": patch
"@peerbit/indexer-rust": patch
"@peerbit/indexer-sqlite3": patch
---

Stop shipping compiled benchmark suites in npm tarballs, and slim @peerbit/indexer-sqlite3 by 65% (drop an unreferenced bundle, unused sqlite main-thread loaders and worker1 helpers, and the broken ./sqlite.org export whose target was never published).
