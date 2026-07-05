# @peerbit/transport-rust

The **native rust-libp2p node transport** for Peerbit. Phase 1: the standalone,
interop-proven connection layer. Opt-in and additive — the default node path
stays js-libp2p and nothing here runs unless explicitly used.

## What this is

A Rust crate (`peerbit_transport`) that owns the socket end of a Peerbit **node**
peer so it can speak the unchanged `/peerbit/*` wire to the rest of the
(js-libp2p) fleet:

- **Connection layer** (`src/swarm.rs`) — a tokio [rust-libp2p 0.56] Swarm
  matching Peerbit's node interop contract: **TCP + WebSocket(+DNS) + Noise +
  Yamux + identify + circuit-relay-v2 client**, Yamux as the only muxer, Noise
  as the only encrypter, multistream-select 1.0. This is the exact feature set
  the feasibility spike proved live against js-libp2p 3.3.4.
- **Ed25519 identity bridge** (`src/identity.rs`) — one raw 32-byte key drives
  **both** the libp2p peerId Peerbit derives *and* the DirectStream
  message-signing key. `identity::Keypair::ed25519_from_bytes` over the same raw
  secret produces a byte-identical peerId; the same bytes seed the wire signer.
- **The three frozen `/peerbit/*` multicodecs** (`src/protocol.rs`) —
  `/peerbit/direct-block/1.0.0`, `/peerbit/topic-control-plane/2.0.0`,
  `/peerbit/fanout-tree/0.5.0` — each mounted over `libp2p-stream`.
- **Native framing** (`src/framing.rs`) — it-length-prefixed unsigned-varint
  (caps 15 MB in / 10 MB out) wrapping the Borsh envelope, calling the
  **`peerbit_wire` codec directly** on socket slices.

## Byte-parity by construction (reuse, not re-implementation)

The crate depends on the already-merged `@peerbit/network-rust` crate
(`peerbit_wire`) as a Cargo path dependency — the same pattern
`packages/utils/native-backbone` uses:

```toml
peerbit_wire = { path = "../network-rust" }
```

`peerbit_wire`'s `wire` and `direct_stream::{lanes,routes,seen_cache,decisions}`
modules are `JsValue`-free, so this native crate calls `encode_frame` /
`decode_frame` / `decode_and_verify_frames` / `LaneScheduler` **directly**. The
codec, framing, and signing are **not** re-implemented — byte parity with the js
fleet is inherited from the crate whose parity tests already guard it. The
`framing` module reading `decode_and_verify_frames(&[&[u8]])` on borrowed socket
slices is exactly the "socket→wasm ingress copy disappears" path the design
targets (`array.to_vec()` never happens on the native transport).

## Live interop test — the Phase-1 deliverable

`src/bin/interop_dial_js.rs` + `js/listener.mjs` prove the real thing: a rust
`peerbit_transport` node dials a js-libp2p 3.3.4 node configured like a Peerbit
node peer, negotiates TCP+Noise+Yamux+identify cross-implementation, opens each
`/peerbit/*` stream, and round-trips a signed `DataMessage` envelope produced by
the real `peerbit_wire` codec. The js side decodes it with the **real**
`@peerbit/stream-interface` `DataMessage.from(...)` and re-serializes with
`.bytes()`; the rust side asserts the echoed envelope is **byte-identical** in
both directions — production codecs on both stacks agreeing on the same bytes.

Run it:

```sh
bash packages/transport/transport-rust/scripts/run-interop.sh
```

or manually:

```sh
node js/listener.mjs                          # prints DIAL_ME=/ip4/.../tcp/<port>/p2p/<peerId>
cargo run --bin interop_dial_js -- <DIAL_ME>
```

## CI strategy — zero cost to every default job

This is the **only** place the heavy libp2p 0.56 tree compiles:

- **NOT in the wasm pipeline.** The crate is `crate-type = ["rlib"]` (plus a
  `[[bin]]`), never `cdylib`; the package `build` script is `aegir build`
  (tsc-only) and never invokes `wasm-pack`/`cargo`. So `build_workspace`
  (`pnpm run build`) never compiles libp2p.
- **NOT in any `test_push` partition.** There is no root Cargo workspace (each
  crate is standalone), so `cargo` never reaches this crate from a sibling. The
  package has no `test:cov` script and an empty `test`, so `test:ci:part-3`
  (`aegir run test:cov --roots ./packages/transport/**`) discovers the package
  but runs zero Rust.
- **Compiled in exactly one existing job.** `test_native` (the declared home for
  pure-Rust crates) gains one opt-in `cargo test --manifest-path …` step, one
  live-interop step, and one `actions/cache` on the crate `target/` keyed on its
  `Cargo.lock`. `build_workspace` and all 14 `test_push` partitions are
  unchanged.

## Phase boundary — the deferred maintainer decision

Phase 1 deliberately does **not** wire this crate into the JS Peerbit runtime.
How a native tokio swarm binds into the wasm-based node runtime — given the swarm
needs native tokio sockets that wasm-on-node cannot open — is a load-bearing
decision that touches the maintainer's "wasm-pack only, no napi" rule
(napi native addon vs. sidecar/IPC vs. feature-gated native build vs. revisiting
the rule). Everything built here is independent of that decision. Deferred to
Phase 2+: the TS `adapter.ts`/`node.ts` wiring into `createLibp2pExtended`,
circuit-relay-v2 relayed-connection hardening, WebSocket live interop, and
removing js-libp2p from the node profile. Browser stays js-libp2p permanently
(no rust browser↔browser `/webrtc` upstream) — the target is a hybrid fleet.

[rust-libp2p 0.56]: https://github.com/libp2p/rust-libp2p
