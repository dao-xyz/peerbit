---
"@peerbit/network-rust": minor
"@peerbit/native-backbone": minor
"@peerbit/shared-log": minor
"@peerbit/pubsub": minor
"@peerbit/stream": minor
"@peerbit/blocks": minor
"@peerbit/rpc": minor
"peerbit": minor
"@peerbit/log": patch
---

Native network plane: Rust/WASM-backed stream, pubsub, and block-exchange path.

Adds an optional native execution path that offloads the hot network code
paths (direct-stream routing, fanout-tree/topic control, block exchange, and
fused send/receive) to a Rust/WASM backbone while keeping the existing
JavaScript implementations as the default. The native path is opt-in and
defaults off; with it disabled every flag-off TS path, wire format, protocol
id, and observable behavior is byte- and semantics-identical, so JS-path and
native-path nodes remain interoperable.

New package:

- `@peerbit/network-rust` — Rust/WASM network backbone (`peerbit_wire` crate):
  native direct-stream, fanout-tree, topic-control, and block-exchange kernels
  with their TS bindings, consumed via the new `rustCore` option on the
  transport packages.

Additive public API on existing packages:

- `@peerbit/native-backbone` — new `NativeBackboneWireSyncSession` receive-fusion
  session plus wire-sync counter/meta types and native raw-receive / sync-send
  wire helpers.
- `@peerbit/shared-log` — new fused send/receive path and sync capability
  handshake: exported `SyncCapabilitiesMessage` and `RawExchangeHeadsMessage`,
  a `SharedLogNativeDefaults` type, and additive `nativeBackbone` / `nativeGraph`
  / raw-exchange `sync` options. Also routes `onChange` dispatch through the
  fused receive path.
- `@peerbit/pubsub` — rust-core `TopicControlPlane` and `FanoutTree` via a new
  `FanoutWireCodec` seam (`fanoutWire` / `fanoutParentUpgrade` exports); the
  native directory adoption and `subscribeShouldReplace` route through the
  native core when `rustCore` is enabled.
- `@peerbit/stream` — new `rustCore` option and exported `RustCoreStream` /
  `PushableLanes` types, plus public `routes` and `wireCounters` surface for the
  native lane scheduler and out-of-band envelope verification.
- `@peerbit/blocks` — `DirectBlock` gains an optional `rustCore` option and a
  `getBlockResponsePayload` hook so block exchange can run through the native
  core.
- `@peerbit/rpc` — new optional `resolveRequest` hook that lets a caller supply
  a pre-resolved request (used by the native path), falling back to decode when
  it is absent or throws.
- `peerbit` — new `NativeNetworkRuntime` client wiring (`nativeNetwork` option)
  for selecting the native network preset.

Internal changes:

- `@peerbit/log` — lazily materializes prepared entry-block bytes so the native
  commit path does not force stash-backed heads to build block bytes it never
  reads (no public API change).
