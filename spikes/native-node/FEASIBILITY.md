# Fully-native Peerbit node — feasibility

**Verdict: FEASIBLE for the transport + network-engine layer — proven by running it.
The remaining work to a *complete* native node is the data-plane orchestration and a
native persistence binding; the Rust-program layer then sits on top as a thin trait.**

This is the foundation for Rust-defined Peerbit programs: a node where rust-libp2p +
the engine cores + (eventually) programs run in **one native process, no JS, no wasm
boundary** — which is why the napi-vs-sidecar question (how to bridge a native
transport into a *wasm* engine) simply dissolves: there is no wasm engine to bridge to.

## What is proven (see README + `cargo run --bin native_node_spike`)

- **The engine cores run native as-is.** `peerbit_wire`'s pure modules (`wire`,
  `direct_stream::{routes,seen_cache,lanes,decisions}`, `topic_control`,
  `fanout_tree`, `block_exchange`) are JsValue-free (std + ed25519-dalek + sha2 +
  indexmap only; `web-sys` appears nowhere; the sole wasm file is `lib.rs`, a thin
  marshalling surface). Built into a native binary, `nm` shows **0 `__wbindgen`
  symbols** — the linker strips the wasm glue; the cores link as native code.
- **A native node binds transport → cores with no boundary.** Two `peerbit_transport`
  swarms in one process connect over TCP+Noise+Yamux, open the frozen
  `/peerbit/topic-control-plane/2.0.0` stream, and push a **real signed** `PubSubData`
  `DataMessage` through the full native receive engine: `read_frame` → native
  `decode_and_verify_frames` (`Verified`) → `SeenCache` dedup → `should_ignore_data`
  → `topic_control` decode → `should_acknowledge` → `LaneScheduler` → native signed
  `AckMessage` back, which the dialer verifies natively. Independently re-run: PASS.
- **Interop with the js fleet holds.** The Phase-1 harness (native node ↔ live
  js-libp2p 3.3.4 Peerbit-config node) passes byte-parity on all three `/peerbit/*`
  protocols. Authorship language is invisible on the wire.

## The Rust-program layer (what this unlocks)

A native program is a Rust trait impl — roughly
`trait Program { fn open(&mut self, ctx: &NodeContext); fn can_append(&self, e: &LogIndexEntry) -> bool; fn on_change(&mut self, c: &Change); fn query(&self, q: &Query) -> QueryResult; }`
— handed a `NodeContext` exposing the identity, a native log+index handle
(`peerbit_log_rust` + `peerbit_indexer_core`, both JsValue-free and native-tested),
and a publish handle (build envelope → `LaneScheduler` → transport). The mapping is
direct: `canAppend` becomes a Rust fn the append core calls in-process (today it's a
JS callback across the wasm boundary); `onChange` fires from the receive-fusion append
facts natively instead of surfacing to JS; `query` goes straight to
`peerbit_indexer_core`'s planner.

**Native and TS programs coexist over one frozen wire.** A native-authored program and
a TS-authored program with the same address + schema exchange heads and converge
exactly as two TS peers do — the wire is byte-frozen (golden vectors + the live js
interop). So "moving higher up the stack" is incremental: as more logic becomes a
native trait impl, the JS boundary recedes from per-entry (today) to per-program-open.

## Honest unknowns — what is NOT yet native-clean

- **The data plane is only half-native.** The data-plane *primitives*
  (`peerbit_indexer_core`, `peerbit_log_rust`) are JsValue-free and native-tested. But
  `native-backbone`'s own orchestration (`raw_receive`/`documents`/`shared_log_plan`/
  `graph_blocks`, ~478 JsValue refs) is woven around the JS caller — its
  journal/flush/leader-plan-selection logic is **real logic, not just plumbing**, and
  would have to be **re-authored native**. A native node binds the primitives directly
  and owns that orchestration itself. This is the largest engineering piece.
- **Persistence is unbuilt.** The wasm build persists via the JS-side store; a native
  node needs a native store binding (filesystem / embedded KV). Biggest unbuilt piece
  for a *standalone* node. Node/server only — browser is permanently out of scope
  (wasm never owns sockets).
- **Behavioral mixed-fleet parity is unproven at the node level.** The cores port the
  TS constants (routing redundancy, ACK timing, seen-cache TTLs, lane weights,
  backpressure) with 93 unit tests, but full-topology js/rust convergence hasn't been
  run end-to-end on a native node.
- `libp2p-stream` 0.4.0-alpha (hand-rolled `NetworkBehaviour` fallback budgeted); relay
  reachability / stream-promotion off limited relayed connections deferred.

## Phased plan

1. **(done, this spike)** Prove the native network engine + transport bind with no
   boundary and interop with the js fleet.
2. **Native data plane** — bind `peerbit_log_rust` + `peerbit_indexer_core` directly and
   re-author the `native-backbone` orchestration (journal/flush/selection) in native
   Rust; add a native persistence store. *(largest slice)*
3. **`Program` trait + `NodeContext`** — the authorship API; port one real program
   (e.g. a document store) as a native trait impl; prove it converges with the same
   program authored in TS on another peer.
4. **Mixed-fleet + hardening** — full-topology js/rust parity, relay/NAT reachability,
   ops (config, lifecycle, supervision).

The hard, uncertain part (native network engine + no-boundary binding + js interop) is
proven. The remaining work is substantial but is *engineering of known shape*, not
open research.
