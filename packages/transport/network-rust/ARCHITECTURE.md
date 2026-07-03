# Native network plane — as-built architecture

This documents the system that landed with the native network plane
(`@peerbit/network-rust` and its integrations), as built — not as planned.
Where the pre-implementation plan and this document disagree, this document
and the code win.

Scope: the `peerbit_wire` crate and its two wasm instantiations, the
receive-fusion stash in `@peerbit/native-backbone`, the `rustCore` /
`nativeWire` seams in `@peerbit/stream`, `@peerbit/pubsub` and
`@peerbit/blocks`, the shared-log fused receive, and the `peerbit/rust`
client preset that composes all of it. The base it builds on (native codec /
batching / sync planner / prepared raw receive) is the native-log-append
kernel and is documented by its own package sources.

Guarantees that hold everywhere below:

- Wire bytes, protocol ids (`/peerbit/topic-control-plane/2.0.0`,
  `/peerbit/fanout-tree/0.5.0`, `/peerbit/direct-block/1.0.0`) and observable
  protocol behavior are unchanged.
- Every native path is opt-in and defaults **off**. With flags off, the TS
  paths are byte-for-byte unchanged.
- Mixed fleets (js-only peers next to rust-enabled peers) are a supported,
  tested configuration at every layer.

---

## 1. Module topology: one crate, two wasm modules

The `peerbit_wire` crate lives in this package and is shipped **twice**:

```
packages/transport/network-rust            packages/utils/native-backbone
┌────────────────────────────────┐         ┌──────────────────────────────────┐
│ wasm module: peerbit_wire      │         │ wasm module: native_backbone     │
│ (standalone wasm-pack build)   │         │                                  │
│                                │  crate  │  peerbit_wire  ◄─ embedded via   │
│  wire.rs        envelope codec │  dep    │  (same code)      Cargo path dep │
│  direct_stream/ routes, seen-  │ ──────► │  wire_sync.rs  stash + fused     │
│    cache, lanes, decisions     │         │    decode (this module only)     │
│  topic_control.rs              │         │  raw_receive.rs, append_tx, ...  │
│  fanout_tree.rs                │         │  (the #939 kernel)               │
│  block_exchange.rs             │         │                                  │
│  sync_payload.rs recognizer    │         │  peerbit_log_rust also links     │
│    (+ encoder groundwork)      │         │  peerbit_wire for                │
│  lib.rs         wasm bindings  │         │  block_response_payload          │
└────────────────────────────────┘         └──────────────────────────────────┘
        │                                            │
        ▼ consumed as                                ▼ consumed as
  @peerbit/network-rust                       @peerbit/native-backbone
  createRustCoreStream()                      createNativeWireSyncSession()
  → DirectStreamOptions.rustCore              → DirectStreamOptions.nativeWire
    (stream/pubsub/fanout/blocks)               on the pubsub stream +
                                                SharedLog sync.nativeWireSync
```

**Why two modules.** wasm modules do not share linear memory. The fused
receive requires the wire decoder and the prepared-raw-receive pipeline to
read the *same* bytes: when `decode_and_verify_batch` copies an inbound frame
batch into wasm linear memory (the one unavoidable JS→wasm ingress write),
that copy must be the memory `prepare_raw_receive_*` later reads from. If the
codec lived only in the standalone `peerbit_wire` module, every stashed
payload would need a second wasm→JS→wasm round trip into native-backbone —
exactly the copy the fusion exists to remove. So the codec core is compiled
into the native-backbone module (`peerbit_wire = { path =
"../../transport/network-rust" }` in its `Cargo.toml`) for the fused path,
and shipped standalone for everything that does not need to touch the
raw-receive pipeline.

**Where the single ingress copy lives.** In `wire_sync.rs`
(native-backbone module): `NativeWireSyncSession.decodeAndVerifyBatch` writes
the frame batch into linear memory once; recognized shared-log frames are
*kept* there ("stashed") and their entry block bytes are later consumed as
in-memory slices. No JS materialization of entries, no second boundary copy.

**Under the full preset both instantiations are loaded.** The pubsub
DirectStream decodes inbound frames on the native-backbone instance (the
wire-sync session is passed as the explicit `nativeWire` option, which takes
precedence over `rustCore.nativeWire` — see
`packages/transport/stream/src/index.ts`), because shared-log sync rides
pubsub and only that stream needs stashing. The fanout and blocks
DirectStreams decode on the standalone `peerbit_wire` instance via their
shared `rustCore`. The codec logic is written once (one crate), but the
compiled code ships twice; unifying the modules is deferred work
(section 9).

**JsValue-free cores.** All decode/verify/decision logic is written
`JsValue`-free so host `cargo test` covers it; `lib.rs` (and `wire_sync.rs`
on the native-backbone side) only translate across the wasm boundary. The
batch decode returns a flat `Uint32Array` — 4 u32 words per frame: decode
flag, top-level variant, verify status (failed / verified / unsupported→TS
fallback), signature count, header priority, payload byte offset and length
(`RECORD_*` in `lib.rs`, mirrored by `NATIVE_WIRE_*` in
`@peerbit/stream`). Receive-fusion decoders additionally set
`RECORD_FLAG_SYNC_STASHED`.

---

## 2. Message flow

### 2.1 Default path (all flags off — byte-for-byte the pre-existing code)

```
socket (js-libp2p) ──varint-framed frames──► DirectStream.processRpc (TS)
    │ per frame:
    ├─ TS borsh envelope decode (Message.from)        wireCounters.tsFrames++,
    │                                                  .tsEnvelopeDecodes++
    ├─ TS signature verify (libsodium, per message)    .tsSignatureVerifies++
    ├─ TS routing / dedup / relay / ACK decisions
    └─► TopicControlPlane / FanoutTree / DirectBlock handlers (TS)
          └─► RPC borsh decode ─► SharedLog.onMessage (TS)
                └─ per-entry Entry decode + verify in JS; block bytes copied
                   into wasm only if the native backbone data plane is on
```

### 2.2 `nativeWire` path (batched native decode + verify)

```
socket (js-libp2p)
    │  same-tick inbound frames are collected
    ▼
nativeWire.decodeAndVerifyBatch(frames, now)      ── ONE JS→wasm write per batch
    │  wasm: borsh envelope decode (byte-identical re-encode is pinned by
    │  parity tests) + sha256-prehashed Ed25519 verification, per-signature
    │  ed25519_dalek::verify_strict (same accept set as libsodium)
    ▼
flat u32 records ──► TS builds one header-only envelope object per frame
    │                (payload = zero-copy subarray view; verification result
    │                memoized so verify(true) short-circuits)
    │                                              wireCounters.nativeFrames++
    ├─ decode failure / unsupported scheme (secp256k1, non-sha256 prehash)
    │      └─► full TS fallback for that frame     .nativeFallbackFrames++
    ▼
TS routing state machine + app events (or native decisions under rustCore)
    └─► protocol handlers as in 2.1
```

Envelopes that arrive outside the inbound stream path (fanout-delivered
shard frames with a nested DataMessage) are verified natively out-of-band via
`seedNativeWireVerification`; on any native failure the TS verification stays
authoritative.

### 2.3 `rustCore` path (protocol decisions in wasm)

Same ingress as 2.2 (`rustCore` implies `nativeWire`). In addition:

- **DirectStream**: routing table (`Routes` port incl. session semantics and
  LRU bounds), seen-cache dedup (message-id / sha256 keying), relay and
  redundancy decisions, seek routing, ACK trace back-routing, flood/silent
  relay target selection, and the 4-lane WRR outbound scheduler with
  byte-budget backpressure all execute in wasm. Byte buffers never cross the
  boundary for scheduling: the core orders `(sequence, byteLength, lane)`
  records and JS keeps the chunks. JS remains the socket owner, byte pump,
  timer/promise host and event emitter; the class contract and constants are
  identical.
- **TopicControlPlane**: `PubSubMessage` codec (variants 0–7), shard mapping
  with the bit-exact `topicHash32` (including the f64 rounding of the
  unchecked JS multiplication above 2^53), root directory, auto-candidate
  normalization, subscribe-state convergence rules (watermark + session
  replacement). The observable subscription maps stay authoritative
  host-side state (public API); the core supplies every decision feeding
  them.
- **FanoutTree**: the complete hand-written big-endian frame codec for
  `MSG_JOIN_REQ(1)`…`MSG_PARENT_PROBE_REPLY(41)` — byte-identical encode
  including JS numeric coercions (`>>> 0`, `| 0`, clamps) and the
  skip/truncate tolerance rules — plus the parent-upgrade policy/gate
  decisions (PR #911). All frame parsing flows through a single
  `FanoutWireCodec` seam; the TS decoders (extracted verbatim into
  `fanout-tree-codec.ts`) are the default implementation, the native codec
  replaces them in rust-core mode. Channel state machine, timers and
  tracker/provider directories stay host-side.
- **DirectBlock / RemoteBlocks**: `BlockMessage` codec, default provider
  resolution (negotiated→connected, capped 32), provider-hint cache and
  eager-block index (FIFO cache port with the lazy-delete semantics and
  verbatim constants of `@peerbit/cache`). Natively stored blocks are served
  via `block_response_payload` (in `peerbit_log_rust`): the borsh
  `BlockResponse` is serialized inside wasm, so served block bytes never
  materialize as a JS value.

### 2.4 Fused receive path (`wireSync` + shared-log `sync.nativeWireSync`)

```
pubsub DirectStream ──frames──► wireSync.decodeAndVerifyBatch   (native-backbone module)
    │ wasm: decode + verify as in 2.2, then per DataMessage:
    │   is the delivery mode locally addressed? (Silent/Ack: `to` includes
    │   self; AnyWhere modes: yes; Traced/none: no)
    │   does the payload parse EXACTLY as
    │     PubSubData(0, registered topic)
    │       └ RPCMessage/RequestV0 [0,0], respondTo absent
    │           └ MaybeEncrypted/DecryptedThing [0,0]        (plaintext only)
    │               └ TransportMessage RawExchangeHeadsMessage [0]+[0,7] ?
    │   ── yes → STASH the frame in wasm linear memory, keyed by the 32-byte
    │            message id; record head hash/refs/byte-ranges as offsets
    │            (record flag RECORD_FLAG_SYNC_STASHED)
    │   ── no  → plain record; TS path proceeds normally
    ▼
TS: header-only envelope → TopicControlPlane dispatch → RPC controller
    │ resolveRequest(message) hook fires BEFORE the request payload decode:
    │ shared-log calls wireSync.stashedMeta(id)
    │   → pins the stash entry, returns hashes / gidRefrences / byteLengths
    │     (metadata only — block bytes stay in wasm)
    ▼
SharedLog.onMessage(StashBackedRawExchangeHeadsMessage)
    │ nativeBackbone.prepareStashedRawReceive*(handle, id, indexes)
    │   wasm: entry block bytes read as slices of the stashed frame
    │   → section parse, batch signature verify, digests/CIDs,
    │     index columns, graph/coordinates/document-index commit
    ▼
wireSync.release(id) when onMessage finishes
    (profile events sharedLog.rawReceive.wireStashResolve / wireStashRelease
     report fused entries and lazily-materialized byte counts)

miss (never stashed / evicted): resolveRequest returns undefined
    → normal RPC borsh decode; behavior identical to the unfused path.
    A throwing resolveRequest hook also falls back to the decode path.
```

---

## 3. The stash handoff: design and bounds

`packages/utils/native-backbone/src/wire_sync.rs`. One
`NativeWireSyncSession` belongs to one node, keyed by the node's public key
hash (needed for the local-delivery check). Programs register/unregister
their RPC topic (refcounted) on open/close.

**Bounds.** The stash is FIFO-bounded to
`WIRE_SYNC_MAX_STASHED_MESSAGES = 512` messages and
`WIRE_SYNC_MAX_STASHED_BYTES = 64 MiB`. The bounds exist for
never-consumed entries: a message can be stashed at the wire level and then
dropped before program dispatch (e.g. by the seen cache), so unconsumed
entries must not accumulate. In a healthy 2-node run the inbound wire stays
in lockstep with the consumer and the caps are unreachable.

**Eviction is loss-free.** FIFO-evicting a never-resolved entry only costs
the fast path: the RPC layer has not skipped anything yet, `resolveRequest`
misses, and the TS borsh decode processes the message normally. The
stash-pressure benchmark leg pushes past the caps deliberately and asserts
convergence on every run (evicted entries recover through the TS fallback
and the synchronizer's retries, at ~4–5× the commit-phase cost).

**Pinning makes consumption safe.** Consumption is two-phase with an async
gap: `stashedMeta(id)` resolves the message (metadata out, bytes stay in
wasm), and the block bytes are read later across the many awaits of
`SharedLog.onMessage`. After resolve there is **no** TS fallback — the RPC
controller skipped the borsh decode — so eviction of an in-flight entry
would drop the message's heads. Therefore `stashedMeta` pins the entry:
pinned entries are excluded from FIFO eviction until `release(id)`, and a
duplicate delivery of a pinned message keeps the pinned entry rather than
replacing it (replacement would reset the pin). Pinned entries are bounded
by the message-processing concurrency of their consumers, not by the caps.
Cargo tests pin all of this (survival across a flood past the 512-message
cap, duplicate-of-pinned semantics, byte accounting).

**Counters.** The session exposes `stashed / evicted / metaReads /
blockCopyOuts / released`; `blockCopyOuts` counts stash bytes that had to be
materialized into JS (normally zero — the lazy `bytes` getters on the
stash-backed message and prepared join facts exist so validation probes
`byteLength` without pulling bytes).

---

## 4. What runs where (native preset)

| subsystem | Rust (wasm) | JS | wasm module |
|---|---|---|---|
| envelope decode + verify | batch codec + `verify_strict` Ed25519 | header-only envelope object (routing state, app events); payload is a zero-copy view | native-backbone (pubsub stream), peerbit_wire (fanout/blocks streams) |
| dedup / routing / relay / ACK / seek | all decisions, routing table, seen cache | timers, promises, sockets, events | peerbit_wire |
| outbound scheduling | 4-lane WRR + byte-budget backpressure accounting | byte buffers, socket writes | peerbit_wire |
| pubsub control plane | codec, subscribe convergence rules, shard hashing, root directory | subscription maps (public API), events | peerbit_wire |
| fanout tree | frame codec, parent-upgrade policy/gate decisions | channel state machine, timers, tracker/provider directories | peerbit_wire |
| block exchange | codec, provider rules, hint/eager caches, native-store-served responses | store chain fallback, publish plumbing | peerbit_wire (+ peerbit_log_rust for serving) |
| shared-log receive | wire stash → prepared raw receive → batch verify → index/graph/coordinates commit | RPC shell, high-level events only | native-backbone |
| shared-log send | — (encoder groundwork only, see section 9) | TS serialization, one message per ≤ 512 KB batch | — |
| transports | — | js-libp2p (noise/yamux/tcp/ws/webrtc) | — |

---

## 5. Flag and option matrix

Every native path defaults **off** at its own layer; the preset is what
turns them on together.

| option | layer | default | effect |
|---|---|---|---|
| `nativeWire` | `DirectStreamOptions` (`@peerbit/stream`) | off | batched native decode+verify of inbound frames (section 2.2); per-frame TS fallback on decode failure / unsupported schemes |
| `rustCore` | `DirectStreamOptions`; picked up by pubsub/fanout/blocks | off | native protocol cores (section 2.3); implies `nativeWire` via `rustCore.nativeWire` unless an explicit `nativeWire` is given (explicit wins); `false` additionally opts out of the test injection |
| `PEERBIT_STREAM_RUST_CORE=1` | env, test-only | off | `resolveInjectedRustCore()` lets the unmodified stream/pubsub/blocks suites pick up a `globalThis`-installed core (the "both modes" re-runs) |
| `sync.rawExchangeHeads` | `@peerbit/shared-log` open args | off | sender ships raw entry block bytes (`RawExchangeHeadsMessage [0,7]`, batched ≤ 512 KB) with no TS re-serialization |
| `sync.nativeWireSync` | `@peerbit/shared-log` open args | off | receive fusion (section 2.4); requires `nativeBackbone`; registers the program topic with the session, resolves via the RPC `resolveRequest` hook |
| `network` | `Peerbit.create` (`CreateInstanceOptions`) | off | native network plane: `rustCore` factory (one core shared by pubsub/fanout/blocks), `wireSync` factory (per-node session keyed by public key hash, installed as the pubsub inbound decoder), `sharedLogDefaults`. Requires client-built services — rejected early (before any resource is acquired) when combined with an external libp2p instance |
| `network.sharedLogDefaults` | `Peerbit.create` | true when `network` is present | advertises `nativeBackbone: {}`, `nativeGraph: { optional: true }` (degrades instead of aborting program open when the optional native module is absent) and `sync: { rawExchangeHeads: true, nativeWireSync }` to programs opened on the client. Explicit per-open options — including `false` — always win. Programs with a program-level `onChange` consumer (e.g. document stores) receive change events from the raw path as lazy entry views: entry bytes/decodes materialize only when the consumer reads them, and every materialization is counted (`sharedLog.rawReceive.jsEntryDecode`, stash `blockCopyOuts`). Programs with a `canAppend` hook run the lower-log batch join (hook fires per entry) instead of the native-validated commit |
| `network` in `createRustPeerbitOptions()` | `peerbit/rust` preset | **on** inside the preset; `network: false` opts out | composes the chain: `rustCore → createRustCoreStream()` (`@peerbit/network-rust`), `wireSync → createNativeWireSyncSession({ selfHash })` (`@peerbit/native-backbone`), `sharedLogDefaults`; each individually toggleable (`rustCore` / `wireSync` / `sharedLogDefaults`, default true) |
| `storage.nativeLogBlocks` | `peerbit/rust` preset (predates this plane) | off | native log block store for blocks; with `rustCore` set, stored blocks are served via `getBlockResponsePayload` straight from wasm. Memory-only: an error is logged when a `directory` is configured alongside it, since blocks (including program manifests) do not survive a restart |

Composition summary for `Peerbit.create(createRustPeerbitOptions())`: native
wire decode+verify on all three DirectStreams, rust-core protocol ports,
wire-sync receive fusion on the pubsub stream, native backbone data plane and
shared-log native defaults — while the app still defines and interacts with
programs in TS.

---

## 6. Interop guarantees and how they are enforced

| guarantee | enforcement |
|---|---|
| envelope bytes identical, both directions | golden-vector parity suite (`test/parity.spec.ts`): TS→Rust and Rust→TS decode, byte-identical re-encode both directions, equal signable bytes (mode + signatures excluded), matching verification outcomes incl. tampered signatures/payloads, in-transit mode rewrites, expired headers; a deterministic Rust-authored golden corpus; a TS-authored golden payload pins the sync-payload recognizer to the TS borsh layout |
| accept-set parity of signature verification | per-signature `ed25519_dalek::verify_strict` (scalar canonicality + small-order checks, matching libsodium); a cargo test constructs a weak-key signature that batch/non-strict verification accepts and the native path must reject. Unsupported schemes (secp256k1, non-sha256 prehash) always fall back to TS |
| protocol codec parity (pubsub, fanout, blocks) | golden parity specs per protocol (`topic-control.spec.ts`, `fanout-codec.spec.ts` — byte-identical encode for every message kind incl. JS numeric coercions and truncation-tolerance, decoder agreement on every prefix of every frame and on random garbage —, `block-exchange.spec.ts`); `topicHash32` parity over a UTF-16 fuzz corpus incl. the f64-rounding overflow |
| behavioral parity of the ported state machines | the existing `@peerbit/stream`, `@peerbit/pubsub` and `@peerbit/blocks` behavioral suites re-run unmodified against the native cores (`PEERBIT_STREAM_RUST_CORE=1` via `test:stream-rust-core`; 427 tests at merge time, wired into CI) in addition to the default-mode runs |
| mixed js/rust fleets converge | dedicated mixed-topology specs at every layer: direct-stream (delivery both directions, ACK-trace route learning, exactly-once discovery dispatch, native lane backpressure), pubsub (subscription convergence, unsubscribe, peer-unavailable eviction, shard/root agreement across a default relay), fanout (join handshake with reject/redirect, downstream forwarding, publish-proxy), blocks; plus mixed legs in the shared-log and document E2E specs (pure-native peer ↔ all-default peer, both directions) |
| flag-off paths byte-for-byte unchanged | every option is checked at its seam; with the option unset the pre-existing code path runs (no shims). The default-mode suite runs stay in CI unchanged |
| the hot path is actually JS-free | always-on `wireCounters` on DirectStream (`nativeFrames`, `nativeFallbackFrames`, `tsFrames`, `tsSignatureVerifies`, `tsEnvelopeDecodes`), the `sharedLog.rawReceive.jsEntryDecode` sync-profile event, and the stash counters — asserted mechanically by the E2E specs (section 7) |
| adversarial parity review | two independent reviews: DirectStream (behavioral parity of the state-machine port) — no blockers; FanoutTree (incl. ~30k hostile differential fuzz cases and >6M decoder comparisons) — no blockers, two minor findings fixed on-branch with differential specs (falsy-zero `remoteSession` coercion; NaN-as-unset marshalling sentinel) |

---

## 7. The E2E proof

`packages/programs/data/shared-log/test/network-e2e-native.spec.ts` runs two
pure-native peers (real `Peerbit.create` with the preset) over real TCP,
syncing a TS-defined store; `packages/programs/data/document/document/test/
native-network-e2e.spec.ts` does the same for a document store in strict
native mode (additionally pinning zero JS document decodes and zero generic
JS index puts). A 200-entry cold sync arrives as one wire batch and commits
entirely inside wasm:

| counter | value |
|---|---|
| `sharedLog.rawReceive.jsEntryDecode` | **0** |
| `tsFrames` / `nativeFallbackFrames` | **0** / **0** |
| `tsSignatureVerifies` | **0** |
| stash `blockCopyOuts` / bytes materialized | **0** / **0** |
| `nativeFrames` (pubsub + fanout) | 257 + 13 |

Both specs also run a mixed leg (pure-native ↔ all-default, both directions)
proving fleet interop with the default peer running the unchanged TS path.

---

## 8. Enumerated hot-path exceptions

Counted JS work per message — exceptions, not flip-flops (bytes never bounce
back and forth):

1. One header-only TS envelope parse per frame (`tsEnvelopeDecodes`),
   feeding the TS routing state machine and app events; the payload bytes
   enter wasm once and never come back.
2. The socket→wasm ingress copy per frame batch — inherent to a wasm engine;
   removable only by native transports (section 9).
3. The small fixed RPC shell decode per message; the per-entry inner payload
   resolves from the stash with zero JS decode.
4. Outbound send is not fused: shared-log still TS-serializes the exchange
   message — one serialization per ≤ 512 KB batch
   (`MAX_RAW_EXCHANGE_MESSAGE_SIZE`), not per entry, asserted in the E2E.

---

## 9. Measured performance

Micro (same corpus, TS vs native batch): same-tick inbound frames decode +
verify in one native call at 2.3–2.4× at realistic payloads
(`benchmark/index.ts` in this package).

Receive path (`benchmark/receive-prune.ts`, wire legs; 1000 entries, 5 runs):
72.6 ms → 61.0 ms mean per message batch (13.8k → 16.6k entries/s) for the
fused leg vs the unfused wire leg — the profiled `inputCopy` phase disappears
and materialize/prepareFacts/plan shrink, while the dominant native verified
commit (~28 ms) is identical in both legs. The 100-entry smoke drops
37.6 ms → 16.3 ms.

End-to-end (`benchmark/network-preset-e2e.ts`, two real nodes over TCP;
indicative, one dev machine — see `packages/programs/data/shared-log/
benchmark/README.md` for methodology and caveats):

- cold-sync: ~2.4× entries/s for the native preset; the sender-side send
  loop is ~2% of wall on both legs, so the unfused outbound path does not
  hide the receive win at these sizes.
- live-puts (sustained singleton puts): currently ~0.4× throughput with
  higher visibility lag on the native preset — the per-message fixed cost of
  the fused receive dominates for one-entry messages. This is the workload
  send fusion / outbound batching is expected to address.
- stash-pressure: above the stash caps the commit phase pays ~4–5× in MB/s
  (eviction fallback + synchronizer retry duplicates) while converging on
  every run.

Never run these benchmarks concurrently with builds or tests.

---

## 10. Known deferred work

- **Send fusion.** Outbound `RawExchangeHeadsMessage` construction still
  TS-serializes (one message per ≤ 512 KB batch). The encoder groundwork
  exists in `sync_payload.rs`; fusing it (serialize from the native log
  block store straight into a `DataMessage` buffer) is the identified fix
  for the live-puts regression above.
- **Native document projection in raw receive.** The raw-receive path now
  dispatches program-level `onChange` with lazy entry views (see the flag
  matrix), so document stores take the raw path — but their per-entry
  consumer materializes payload bytes and decodes in JS. Committing the
  document index inside the wasm raw-receive pipeline (composing
  `documents.rs` with the prepared join commit) and synthesizing the change
  set from prepared-join facts would remove that per-entry JS work; the
  strict-native document append path already commits natively and is the
  starting point.
- **Module unification.** `peerbit_wire` ships compiled into two wasm
  modules (section 1). Collapsing to one module — or sharing a memory —
  would drop the duplicate code weight and let the fanout/blocks streams
  share the fused ingress.
- **Native transports.** JS keeps sockets everywhere (js-libp2p noise/yamux/
  tcp/ws/webrtc). A rust-libp2p swarm on node, and any browser transport
  work (blocked upstream on browser↔browser `/webrtc`), remain later phases
  per the original plan; they only become worthwhile once profiling shows
  the transport layer itself is material — exception 2 in section 8 is the
  cost they would remove.
