---
"@peerbit/document": patch
---

Add an opt-in `[default, native]` conformance matrix leg for the document layer,
a mechanical clone of the merged shared-log leg. It re-runs an allowlist of the
existing document suites against the native (Rust) data plane via the shared-log
env switch (`PEERBIT_SHARED_LOG_RUST_CORE=1`), under which a store opened in the
default `mode:"auto"` builds its generic index on `@peerbit/indexer-rust` — so
`docs.index.index` is a `RustIndex` on the switch peer where it is a
`SQLiteIndex` on the default backend. A hard in-suite guard asserts this so the
leg cannot false-green as JS.

Test-only: no `@peerbit/document` `src/` product code changes. The single
test-helper fix makes the `iterate > sort` sync-suppression HACK backend-agnostic
(it drops both `ExchangeHeadsMessage` and `RawExchangeHeadsMessage`), a no-op for
the default (JS) suite. Wire-up mirrors the shared-log leg: a
`test:document-rust-core` script (grep anchored to the confirmed native-green
comparator/sort/paging/query describes) and a `continue-on-error` step in the
`test_native` job. Native and default each run 188 passing / 0 failing under the
allowlist grep. The excluded set (del-path block-store read-back, native-internal
append-path assertions, remote-indexed `resolve:false` fetch) is documented in
`test/NATIVE_CONFORMANCE.md`; none is an index comparator/sort/paging divergence.
