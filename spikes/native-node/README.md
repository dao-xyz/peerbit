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

4. **The DATA-PLANE layer above the engine runs native too** (`src/data_plane.rs`).
   A native node takes a **received, verified** entry and appends it to a native
   log + commits it to a native index — no JS, no wasm — reusing the two
   data-plane primitive crates as **path rlibs**: `peerbit_log_rust`
   (`NativeLogBlockStore`, the raw-entry ingest/verify primitive, `LogGraphIndex`)
   and `peerbit_indexer_core` (`NativeQueryIndex`, pure Rust — borsh+indexmap+
   roaring, **zero** wasm refs). The glue (`NativeDataPlane::recognize_and_commit`)
   is the native equivalent of native-backbone's `raw_receive` + `append_tx`:

   ```
   verified PubSubData payload (from process_inbound_frame → outcome.pubsub)
     → sync_payload::parse_pubsub_data + parse_raw_exchange_rpc_request  (recognize heads)
     → prepare_raw_entry_v0_..._verify   → CID match + Ed25519 author sig (native)
     → NativeLogBlockStore.put(cid, storage_bytes)   → block persisted     (native)
     → LogGraphIndex.put(log_index_entry(head=true)) → heads/graph updated (native)
     → NativeQueryIndex.put(cid, DocumentFields{…})  → document indexed    (native)
     → NativeQueryIndex.search(Exact GID) == [cid]   → queryable natively  (native)
   ```

   Proven two ways: `data_plane_demo` (single node is handed a received entry,
   commits it, a native index query returns it) and — the **stretch** —
   `data_plane_network_demo`, where an entry **appended on node A** flows over the
   real TCP+Noise+Yamux transport through node B's native receive engine and
   lands in node B's native log + index; a native index query on B returns A's
   entry. Two independent native Ed25519 verifications run on B (transport
   envelope + inner EntryV0 author signature).

## Run it

```bash
# Pure native engine + data plane (no network): 11 tests
cargo test  --manifest-path spikes/native-node/Cargo.toml --lib

# Two native nodes, one process, full native stack + signed message + ACK
cargo run   --manifest-path spikes/native-node/Cargo.toml --bin native_node_spike

# DATA-PLANE slice: a received signed entry → native log append + index commit,
# then a native index query returns it (single node, in-process)
cargo run   --manifest-path spikes/native-node/Cargo.toml --bin data_plane_demo

# DATA-PLANE over the transport (stretch): append on node A → commit on node B
cargo run   --manifest-path spikes/native-node/Cargo.toml --bin data_plane_network_demo

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
- Depends on `peerbit_transport`, `peerbit_wire`, `peerbit_log_rust` and
  `peerbit_indexer_core` as **path rlibs** — the exact pattern `peerbit_transport`
  already uses for `peerbit_wire`. No shared source is modified; the two log/
  indexer crates are reused as-is, never re-implemented.

## Data-plane error surface (a native-hardening note)

`peerbit_log_rust`'s public functions return `Result<_, JsValue>`, and building a
`JsValue` calls a `wasm-bindgen` intrinsic that **aborts** on a non-wasm target.
So a block that fails to *parse* (or an `expected_cid` that mismatches) would
abort rather than return a catchable error. `recognize_and_commit` therefore
stays on the primitive's **Ok** path — it passes `expected_cids = None` (so the
primitive *computes* the CID, never the mismatch-abort branch), then re-checks
`cid == head.hash` and `signature_verified` in plain Rust (both catchable). The
remaining abort surface is a malformed borsh block; here blocks arrive inside an
envelope the network engine already Ed25519-verified (i.e. from an authenticated
peer). A durable node needs the primitive to grow a native `Result<_, LogError>`
— deferred, and the biggest single native-hardening item this slice surfaced.

## Scope / what is out

This is a spike (proving the layer over polish). The data-plane **slice** — a
received, verified entry appended to a native log + committed to a native index,
queryable natively, in-process and over the transport — **is** built and proven
(point 4). Deferred, exactly as native-backbone still does them across ~478
JsValue-woven refs, none needed to prove the layer:

- **Leader / replication coordinates** — the slice commits an entry
  unconditionally; who-should-replicate (`EntryCoordinateCommit`/`GidLeaderPlan`)
  is skipped.
- **Journal / flush / WAL ordering, trim / prune, head-demotion-on-join,
  batch head-selection dedup, onChange dispatch.**
- **Full Document projection** via schema IR / signer — the slice commits a raw
  index row keyed by CID with a handful of scalar fields (hash/gid/wall/logical/
  head/size), enough to assert "received entry is indexed and queryable".
- **Durable native store** — in-memory for the slice (block store `HashMap`,
  graph indexmap, index indexmap+roaring); `peerbit_indexer_core` already ships
  the `storage::ByteStorage` + `persistence.rs` seam for a crash-consistent
  write/load path.
- **The `JsValue`→native error type** on `peerbit_log_rust` (see above).

Multi-hop relay via `Routes` is likewise still a stretch; the base proof is
point-to-point.
