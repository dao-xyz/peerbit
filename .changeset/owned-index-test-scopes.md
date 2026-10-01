---
"@peerbit/indexer-tests": patch
---

Keep polymorphism setup inside its suite, register scope-test fixtures with per-test cleanup, and await the iterator-completion assertion. This prevents indexer conformance tests from leaking resources or completing before their assertions settle.
