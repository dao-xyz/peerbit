# `peerbit_node_spike` — fully-native Peerbit node feasibility spike

**Verdict: FEASIBLE — proven by running it, not just reasoned.**

A single native process where rust-libp2p (`peerbit_transport`, rust-libp2p 0.56)
binds **directly** to the JsValue-free engine cores (`peerbit_wire`'s wire codec
+ `direct_stream` routing/seen-cache/decisions + `topic_control` plane codec) —
**NO JS, NO wasm boundary, NO napi, NO sidecar** anywhere on the message path.

## What this crate proves

1. **The engine cores run native as-is.** `peerbit_wire`'s pure modules
   (`wire`, `direct_stream::{seen_cache,decisions,lanes}`, `topic_control`)
   compile and run as an ordinary native rlib. Its `lib.rs` wasm surface still
   *compiles* (js-sys/wasm-bindgen are ordinary crates), but it is **dead code
   on the native path**: `nm` on the built binary shows **0** `__wbindgen`
   symbols — the linker stripped all wasm glue — while the core symbols
   (`peerbit_wire::direct_stream::seen_cache::SeenCache::{new,modify,...}`) are
   linked as native code.

2. **A native node binds transport → cores with no boundary.** The binary
   `native_node_spike` stands up **two** `peerbit_transport` swarms in one
   process, connects them over TCP+Noise+Yamux, opens the frozen
   `/peerbit/topic-control-plane/2.0.0` stream, and pushes one **real signed**
   `PubSubData` `DataMessage` through the full native receive engine:

   ```
   read_frame (socket → Rust mem, no copy)
     → peerbit_wire::decode_and_verify_frames   → VerifyStatus::Verified   (native Ed25519)
     → SeenCache.modify                          → dedup counter            (native)
     → decisions::should_ignore_data             → relay/ignore decision    (native)
     → topic_control::decode_pubsub_message      → topics + payload         (native)
     → decisions::should_acknowledge             → ack decision             (native)
     → LaneScheduler.push/shift                  → outbound WRR ordering     (native)
     → build_signed_ack (peerbit_wire encode+sign) → signed AckMessage       (native)
   ```
   The dialer then reads the ACK and **verifies it natively** (`VerifyStatus::Verified`,
   variant `1` = AckMessage). The listener's acked id equals the dialer's message id.

3. **Interop with the js-libp2p fleet holds.** The pre-existing Phase-1 harness
   (`packages/transport/transport-rust/scripts/run-interop.sh`) — a native rust
   node dialing a real js-libp2p Peerbit-config node — passes byte-parity on all
   three `/peerbit/*` protocols (rust `peerbit_wire` encode → js
   `@peerbit/stream-interface` `DataMessage.from`/`.bytes()` → rust decode).

## Run it

```bash
# Pure native engine (no network): 5 tests
cargo test  --manifest-path spikes/native-node/Cargo.toml --lib

# Two native nodes, one process, full native stack + signed message + ACK
cargo run   --manifest-path spikes/native-node/Cargo.toml --bin native_node_spike

# Native ↔ js-libp2p fleet interop (needs `pnpm install` + stream-interface build first)
bash packages/transport/transport-rust/scripts/run-interop.sh
```

## Isolation (maintainer hard rule)

- Lives under `spikes/` — **not** a pnpm workspace member (see
  `pnpm-workspace.yaml`, which does not glob `spikes/`), and has **no**
  `package.json`, so `wasm-pack`/pnpm never see it.
- There is **no root Cargo workspace** in this repo (every rust crate is
  standalone), so `cargo` against a sibling crate never compiles this one.
- `crate-type = ["rlib"]` + a bin, no `cdylib` — the heavy libp2p tree never
  enters the wasm pipeline or any default CI partition.
- Depends on `peerbit_transport` and `peerbit_wire` as **path rlibs** — the
  exact pattern `peerbit_transport` already uses for `peerbit_wire`. No shared
  source is modified.

## Scope / what is out

This is a feasibility spike (learning over polish). The data-plane fusion
(feeding a `sync_payload`-recognized `RawExchangeHeadsMessage` into
`peerbit_log_rust` verify) and native persistence are **not** built here — they
are the largest unbuilt pieces for a standalone native node and are analyzed,
not implemented. Multi-hop relay via `Routes` is likewise left as a stretch;
the base proof is point-to-point.
