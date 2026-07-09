---
"@peerbit/shared-log": patch
---

Make the `getReceivedHeads` test helper backend-agnostic so the native
`[default, native]` conformance leg (`PEERBIT_SHARED_LOG_RUST_CORE=1`) folds in
the `redundancy > only sends entries once` family (4 variants × 2 setups). The
helper now counts heads from both `ExchangeHeadsMessage` (JS wire) and
`RawExchangeHeadsMessage` (native raw exchange) at the same per-entry
granularity, and the companion repair-hint exclusion filter these tests use was
made backend-agnostic (`isRepairHintExchangeHeadsMessage`) so native repair
hints do not leak into the no-redundancy count. Both changes are no-ops for the
default (JS) count; the native leg goes from 139 to 147 passing, 0 failing.
Test-only.
