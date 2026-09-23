# @peerbit/diagnostics

## 0.1.0

### Minor Changes

- [#1463](https://github.com/dao-xyz/peerbit/pull/1463) [`a86014b`](https://github.com/dao-xyz/peerbit/commit/a86014bfdbfc1fb4a7a3c71c07a21321a425ebe9) Thanks [@peerbit-org](https://github.com/peerbit-org)! - Add bounded, opt-in document-query and RPC-request diagnostics for local lookup, cover selection, sampled target identities, setup, publishing, accepted responses, deadlines, and result introduction. Documents uses its existing sync profile callback; RPC also accepts a request profile callback. Observer failures are isolated and traces reserve a terminal event while suppressing late events. These diagnostics do not alter query coverage, timeouts, authorization, delivery receipts, or wire/storage formats.
