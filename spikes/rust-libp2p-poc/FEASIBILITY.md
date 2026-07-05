# FEASIBILITY — rust-libp2p node swarm interoperating with the Peerbit js-libp2p fleet

> Decision-grade synthesis of the `spike/rust-libp2p-feasibility` spike. Throwaway
> feasibility artifact (see `../README.md`), grounded against rust-libp2p 0.56.0 /
> js-libp2p 3.x, the libp2p test-plans interop matrix, PR #988's rust-core engine,
> and Peerbit's live config. Every factual claim cites a file/line or an upstream
> source. July 2026.

---

## 1. VERDICT — FEASIBLE-WITH-CAVEATS (node only)

**Replacing js-libp2p with a rust-libp2p swarm on NODE peers, while staying
interoperable with the existing js-libp2p fleet, is FEASIBLE.** The entire shared
interop path Peerbit depends on — TCP + WebSocket + Noise + Yamux +
multistream-select 1.0 + identify — is in libp2p's continuously cross-tested
known-good set (rust-v0.56 ↔ js-v3.x), and this spike *ran it live*: a rust-libp2p
0.56.0 peer dialed a js-libp2p 3.3.4 node configured like a Peerbit node peer,
negotiated TCP+Noise+Yamux+identify cross-implementation, opened a `/peerbit/*`
protocol stream via `libp2p-stream`, and exchanged a byte-identical
varint-length-prefixed frame round-trip (GOAL 2, `src/goal2_dial_js.rs` ↔
`js/listener.mjs`). The **caveats** are what keep this from an unqualified "yes":
(a) the transport swap is the *easy* half — the load-bearing engineering is
re-implementing Peerbit's DirectStream wire (Borsh schema, Ed25519/SHA-256 signing,
seen-cache, 4-lane scheduler, routing) natively over **three** frozen multicodecs,
which the spike deliberately did not attempt; (b) `libp2p-stream` (the natural
binding point) is `0.4.0-alpha` with real API-churn risk; (c) Circuit-Relay-v2
*relayed* connections interop but rust↔js **DCUtR hole-punch is unreliable** — NAT'd
node peers must stay on relayed connections, not hole-punched direct ones; and
(d) **browser peers cannot move** and never will (wasm never owns sockets;
rust-libp2p has no browser↔browser `/webrtc`), so the only realistic end state is a
**hybrid fleet** — rust-libp2p node peers speaking the unchanged `/peerbit/*` wire to
permanently-js-libp2p browser peers. The "no JS networking" dream is achievable on
node and structurally impossible in the browser.

---

## 2. Interop matrix (per component, rust ↔ js)

Status source: libp2p `test-plans` `versionsInput.json` declares, for **rust-v0.56**:
transports `ws, tcp, quic-v1, webrtc-direct`; secureChannels `tls, noise`; muxers
`mplex, yamux`. For **js-v3.x (Node)**: transports `tcp, ws, wss(dial-only)`;
secureChannels `noise`; muxers `mplex, yamux` — **no quic-v1, no webtransport, no
webrtc-direct on js Node**. ~1700 tests run across implementations on every PR. Every
"known-good" row below is a green cell in that live matrix; the two ✔ (ran) rows were
additionally exercised in this spike.

| Component | rust crate (~ver, in libp2p 0.56.0) | rust ↔ js status | Notes / source |
|---|---|---|---|
| **TCP** | `libp2p-tcp` | **known-good ✔ (ran)** | Both declare `tcp`; safest pair. Exercised GOAL 1+2. |
| **WebSocket** | `libp2p-websocket` | **known-good** (one caveat) | Both declare `ws`; cross-tested. js declares `wss` **dial-only** — a js Node peer will not *listen* on secure-ws. Build gotcha: in 0.56.0, `websocket` needs `dns` or fails to compile (`E0599 no variant Dns`, `SwarmBuilder::with_websocket`), documented in `Cargo.toml:32-37`. Peerbit's `/ws` listen addr (`transports.ts:27`) makes this mandatory. |
| **Noise** | `libp2p-noise` | **known-good ✔ (ran)** | The *only* secure channel js supports (`libp2p.ts:90` `noise()`); both declare `noise`. rust also offers `tls` but js Node does not — Noise is the interop path. Exercised GOAL 1+2. |
| **Yamux** | `libp2p-yamux` | **known-good ✔ (ran)** | Both declare `yamux` (`libp2p.ts:91`); cross-tested. Historical rust↔js yamux/WS bugs (#2461) long fixed and regression-guarded by the matrix. Exercised GOAL 1+2. |
| **multistream-select 1.0** | in `libp2p-swarm` core | **known-good ✔ (ran)** | Every green matrix cell exercises mss 1.0. Peerbit pins mss 1.0; per-protocol negotiation of `/peerbit/direct-stream/2.0.0` succeeded cross-impl in GOAL 2. |
| **identify** | `libp2p-identify` | **known-good ✔ (ran)** | Standard/interoperable; Peerbit mounts `identify()` (`libp2p.ts:67-69`) and topology registration depends on it. GOAL 2 saw the js node's `identify` advertise the mounted protocol. *Caveat:* rust issue #5646 shows identify timing out specifically on the immature **webrtc-websys** path — not on TCP/WS. |
| **Circuit-Relay v2** | `libp2p-relay` (+ `libp2p-dcutr`) | **relayed conns / reservations: known-good** · **DCUtR hole-punch: rough** | Spec-shared `/libp2p/circuit/relay/0.2.0/hop`+`/stop`, identical RESERVE/STATUS flow — relayed traffic interops. But js-libp2p DCUtR is limited (maintainer, Feb 2024: "mainly there to enable the unilateral connection attempt… really needs QUIC… still missing in Node.js"). **Use relayed connections for NAT'd node peers; do not rely on rust↔js hole-punch to direct.** Not exercised in this spike (needs a 3rd relay process); the `relay` feature compiles (`Cargo.toml:41`). |
| **WebRTC / webrtc-direct** | `libp2p-webrtc` (native listen+dial), `libp2p-webrtc-websys 0.4.0` (browser, **dial-only**) | **browser→server webrtc-direct: rough/partial** · **browser↔browser `/webrtc`: unproven / effectively BLOCKED** | rust-libp2p does **not** implement `/webrtc-signaling/0.0.1` (browser↔browser private-to-private). Tracked in issue **#4389** (open since Aug 2023) + PR **#5978** (open, blocked, still unmerged as of Jun 2026: WASM-incompatible tokio time, relay-shutdown-disconnect). Even webrtc-direct rust↔js is reported non-interoperable. **This is the hard boundary (§5).** Not attempted. |

**Bottom line of the matrix:** everything a *node* peer needs (TCP/WS + Noise +
Yamux + mss + identify + relay-v2 relayed conns) is green. Everything a *browser*
peer uniquely needs (`/webrtc` browser↔browser) is red on the rust side.

---

## 3. The concrete interop contract a rust-libp2p node must satisfy

Grounded against Peerbit's live config (js-libp2p v3.x majors: `libp2p ^3.1.7`,
`@libp2p/tcp ^11`, `@libp2p/websockets ^10`, `@libp2p/circuit-relay-v2 ^4.1.7`,
`@chainsafe/libp2p-noise ^17`, `@chainsafe/libp2p-yamux ^8`, `@libp2p/identify ^4`).

**Transports.** Node peer listens on TCP + WS + circuit
(`transports.ts:8-18,25-30`: `webSockets()`, `circuitRelayTransport({reservationCompletionTimeout:5000})`,
`tcp()`; listen `/ip4/127.0.0.1/tcp/0`, `/ip4/127.0.0.1/tcp/0/ws`, `/p2p-circuit`;
webRTC-direct commented out). A rust node needs **tcp + websocket** transports and a
**circuit-relay-v2 CLIENT** to be reachable via `/p2p-circuit`. To act as a
rendezvous it needs the v2 **SERVER** with unlimited reservations
(`circuitRelayServer({reservations:{applyDefaultLimit:false,maxReservations:1000}})`,
`transports.ts:19-23`; `applyDefaultLimit:false` is deliberate, js-libp2p #2622).

**Muxer + encrypter (identical node/browser).** `streamMuxers:[yamux()]` — Yamux
only, no mplex; `connectionEncrypters:[noise()]` — Noise only (`libp2p.ts:90-91`). A
rust node MUST offer `/yamux/1.0.0` and `/noise`.

**Identity — Ed25519 peerId (hard requirement).** Peerbit throws on any non-Ed25519
peerId/keypair (`peer.ts:140` "Only Ed25519 peerIds are supported"; `peer.ts:383`
"Only Ed25519 keypairs are supported"). The peerId is the standard libp2p Ed25519
identity-multihash over the raw 32-byte pubkey (`crypto/src/ed25519.ts:39-41`), so a
rust `identity::Keypair::ed25519_from_bytes` produces a **byte-identical peerId**.
**One Ed25519 raw key must drive both** the rust swarm peerId *and* the DirectStream
message-signing/publicKeyHash used by the routing tables (plan item 3, plan risk 3).

**Standard protocols for fleet membership.** `identify` (`libp2p.ts:67-69`, on by
default) + circuit-relay-v2 hop/stop. **No ping, no kad-DHT, no mdns, no gossipsub,
no autonat/dcutr** are configured — discovery is explicit dial/bootstrap + the
FanoutTree overlay.

**Behavioral tunables (not wire-breaking, but match them).** From `libp2p.ts:73-87`:
`inboundStreamProtocolNegotiationTimeout:1e4`, `inboundUpgradeTimeout:1e4`,
`outboundStreamProtocolNegotiationTimeout:1e4`, `reconnectRetries:0` (#3289),
`connectionMonitor.abortConnectionOnPingFailure:false`.

**THE core contract — the four custom `/peerbit/*` protocol IDs.** These are *not*
gossipsub/kad; each is a `registrar.handle()` mount in the DirectStream base class
(`stream/src/index.ts:1529-1550`) with
`{maxInboundStreams,maxOutboundStreams,runOnLimitedConnection:false}` and topology
`{notifyOnLimitedConnection:false}` — i.e. these streams do **not** run over limited
(relayed) connections directly; they need a full/direct or promoted connection.
Verified in the repo:

1. `/peerbit/direct-block/1.0.0` — DirectBlock exchange (`blocks/src/libp2p.ts:52`)
2. `/peerbit/topic-control-plane/2.0.0` — pubsub TopicControlPlane (`pubsub/src/index.ts:317`)
3. `/peerbit/fanout-tree/0.5.0` — FanoutTree overlay (`pubsub/src/fanout-tree.ts:873,1365`)
4. `bench/0.0.0` — benchmark only, NOT part of the fleet.

**There is no single `/peerbit/direct-stream/*` id.** "DirectStream" is the *shared*
routing/relay/ACK/lane engine; each consumer registers its own multicodec and runs
the same framing underneath. **A rust node must implement the DirectStream wire three
times over three distinct multicodec strings.** (The spike uses one representative
`/peerbit/direct-stream/2.0.0` to prove the negotiation mechanics, which are
identical for all three; a real port calls `Control::open_stream`/`accept` once per
id.)

**DirectStream wire format (per multicodec).**
- Negotiation: `connection.newStream(multicodecs, {negotiateFully:true})`
  (`stream/src/index.ts:1802-1806`) — standard mss, full negotiation.
- Framing: `it-length-prefixed` unsigned-varint length prefix; inbound cap
  `MAX_DATA_LENGTH_IN ≈ 15 MB`, outbound `MAX_DATA_LENGTH_OUT ≈ 10 MB`
  (`index.ts:195-196`).
- Body: **Borsh** (`@dao-xyz/borsh`), dispatched by a leading **1-byte variant tag**
  (`messages.ts:487-500`): `DataMessage=0, ACK=1, Hello=2, Goodbye=3`. DeliveryMode
  sub-variants Silent=0, Acknowledge=1, Traced=3, AnyWhere=4, AcknowledgeAnyWhere=5.
- Signatures: `Signatures` vec of `SignatureWithKey`; DirectStream signs with
  **Ed25519 over `PreHash.SHA_256`** (`stream/src/index.ts` `signPreparedSha256`).

So the rust node must reproduce **Peerbit's Borsh message schema + Ed25519/SHA-256
signing**, not merely the libp2p transport. Transport (a–c above) is off-the-shelf
rust-libp2p; this wire re-implementation (d) is the load-bearing work.

---

## 4. Engine binding — feeding the #988 rust-core DirectStream engine directly

**As built today (JS is the byte pump).** PR #988 ported the DirectStream engine
(routing/relay/ACK/lanes/seen-cache) to Rust/WASM, but JS still owns the socket. The
engine's real inbound contract is a *pure function over already-deframed frames*, and
its outbound contract is a *byte-free ordering scheduler*:
- **Inbound:** js-libp2p socket → noise/yamux (JS) → `lp.decode` deframes varint
  frames (`stream/src/index.ts:807-809`) → batched → the WASM entrypoint
  `decode_and_verify_batch(frames: Array, now_ms)` iterates the JS Array and does
  **`array.to_vec()` per frame** — a copy from the JS heap into a wasm-owned
  `Vec<u8>` (`network-rust/src/lib.rs`, verified: `.map(|array| array.to_vec())`) —
  then calls `wire::decode_and_verify_frames(&[&[u8]], u64) -> Vec<FrameRecord>` and
  returns a flat `Vec<u32>` (4 words/frame). *Frames in, records out — nothing
  socket-shaped.*
- **Outbound:** bytes never enter WASM — the scheduler is fed only `(lane,
  byte_length)`; the WASM `DirectStreamLanes` returns a monotonic `sequence`, the TS
  `records` Map holds the actual chunk, and `scheduler.shift()` says which sequence to
  emit next. The chunk then flows through `lp.encode` → `raw.send` to the js stream.

**The two residual per-message costs a native transport targets** (ARCHITECTURE.md
§8/§10, verified):
- **Exception 2** — "The socket→wasm ingress copy per frame batch — inherent to a
  wasm engine; **removable only by native transports**" (ARCHITECTURE.md:388-389).
- The mirror **egress copy** on the fused-send path (ARCHITECTURE.md:394).

**What a rust-libp2p swarm on node changes (the "no JS hop" binding).** The swarm's
per-peer `libp2p-stream` events feed the engine **in-process, native `&[u8]`**:
- Inbound: the swarm negotiates `/peerbit/*`, reads the stream, does its own
  unsigned-varint deframing **in Rust**, and calls
  `decode_and_verify_frames(&[&[u8]], now)` on slices that came off the socket into
  Rust memory — **`array.to_vec()` (the ingress copy) disappears; the engine reads
  socket bytes in place.** This is exactly the cost ARCHITECTURE.md:450-453 names:
  "A rust-libp2p swarm on node… only become worthwhile once profiling shows the
  transport layer itself is material — exception 2 in section 8 is the [cost they
  would remove]."
- Outbound: the same `LaneScheduler` (`lanes.rs`, already `JsValue`-free and
  host-cargo-tested) orders sequences, but the chunk store and the `AsyncWrite` sink
  both live in Rust — no `records` Map, no `lp.encode` in JS, no `raw.send`.

**Shape of the change (what moves TS → Rust on the node path):** the `lp.decode`/
`lp.encode` framing, the `pendingNativeWireFrames` batching + microtask flush, the
`records` Map, the outbound pump/`raw.send` loop, stream lifecycle (open/close/prune),
and socket-drain backpressure (`waitForDrain`). The wasm-bindgen glue
(`Array→to_vec`, `Vec<u32>` return) becomes direct Rust calls; the TS adapter's
promise/timer machinery becomes tokio timers/notifies. **The routing/seen-cache/
decision cores are unchanged — already pure and `JsValue`-free** (`mod.rs`: "never
owns sockets"). One Ed25519 key must bridge the swarm peerId and the signing key (§3).

**Honest sizing of the payoff.** The binding removes exception 2 (+ the egress copy)
and the JS backpressure/event-loop mediation, buys connection-scaling headroom, and
eventually removes js-libp2p from the node profile. It does **not** remove
exception 1 (the header-only TS `Message.from` parse that still drives the host-side
routing state machine / app events) unless the host-side subscription maps, channel
state machines, timers, and event emission also move to Rust — explicitly kept
host-side today. **Magnitude is unproven:** the plan gates all transport work behind
Phase-0 profiling and repeats "only worthwhile once profiling shows the transport
layer is material." Every measured #988 win to date is decode/verify/receive-fusion —
**none is transport.** Do not fund the transport swap on speculative copy-elimination;
fund it on a profile that shows transport is the bottleneck, or on the strategic goal
of removing js-libp2p from node.

---

## 5. The hard browser boundary → a HYBRID fleet is the only realistic target

The browser cannot go pure-rust, for reasons stated identically in the plan (§6) and
the as-built doc (ARCHITECTURE.md §10):

1. **WASM cannot own transport primitives.** `RTCPeerConnection`, `WebSocket`, and
   therefore dialing/listening are browser-runtime objects. Even a full wasm rust
   swarm (`webrtc-websys`/`websocket-websys` via web-sys, `Swarm::with_wasm_executor`)
   is just Rust driving the same browser APIs through JS bindings — **there is no
   transport-copy escape hatch in the browser, only a code-ownership choice.**
   `libp2p-wasm-ext` (delegating transport to JS) reaches the same conclusion from the
   other side.
2. **A hard functional blocker, not merely a performance wash.** Peerbit browsers
   listen on `/webrtc` for **browser↔browser WebRTC with circuit-relay signaling**
   (`transports.browser.ts:6-21`, verified: `webRTC({})` + listen `/webrtc`,
   `/p2p-circuit`) — a load-bearing feature. This uses the libp2p spec
   `/webrtc-signaling/0.0.1` (varint-prefixed protobuf SDP/ICE over a relayed conn).
   **rust-libp2p does not implement it.** `libp2p-webrtc-websys 0.4.0` is
   webrtc-direct **dial-only** (cannot listen, no relay-signaling). Browser↔browser
   via rust is issue **#4389** (open) + PR **#5978** (open, blocked, unmerged Jun
   2026). So a wasm rust swarm today would **lose browser↔browser connectivity or
   force permanent relaying** — a capability regression independent of speed.
3. **Binary-size cost.** A full wasm swarm adds ~1.5–3 MB pre-gzip on top of the
   already-shipped native/indexer wasm (plan risk 2).

**The decision (plan §6 / as-built).** Delegate transport to JS in the browser; run
the Rust protocol layer (L0–L2) in WASM. js-libp2p keeps
webSockets/webRTC/circuitRelay + mss/noise/yamux; negotiated `/peerbit/*` streams are
piped as byte streams into the **same** wasm engine as node (the IO-seam is
identical). Wasm owns everything above the socket — "this is where the CPU is." The
as-built browser sub-variant already ships this (ARCHITECTURE.md transports row =
"js-libp2p (noise/yamux/tcp/ws/webrtc)"; a browser rust-core e2e smoke exists).

**Why this forces a hybrid fleet.** Because browsers keep js-libp2p transport
*permanently*, any deployment that includes browsers is inherently mixed. The design
is built to make that mixed fleet first-class: interop is a hard requirement across
the whole migration ("every phase must leave a mixed js/rust network fully
functional"); protocol ids and wire bytes are **frozen and byte-identical**; and the
critical enabler is that **a rust-libp2p node and a js-libp2p browser peer speak the
same `/peerbit/*` protocols with byte-identical envelopes** — so a node swapping its
transport to rust-libp2p (keeping WS/TCP listeners, since JS peers can't dial QUIC) is
*invisible* to browser peers. Mixed-topology specs at every layer and pure-native ↔
all-default E2E legs (both directions) already enforce this.

**What this means for the "no JS networking" dream.**
- **On node: achievable.** A rust-libp2p swarm can own the socket end-to-end; js-libp2p
  can be removed from the node profile once the DirectStream wire is ported natively.
  This spike demonstrates the transport prerequisite works.
- **In the browser: structurally impossible.** Not "hard" — impossible: wasm never
  owns the socket, and even a pure-rust wasm swarm bottoms out on JS-exposed browser
  APIs, *and* would regress browser↔browser `/webrtc` until #4389/#5978 land and enter
  the interop matrix. The JS↔wasm socket boundary in the browser is permanent by
  construction.
- **Net end state:** rust-libp2p on node peers ⇄ js-libp2p on browser peers, over an
  unchanged `/peerbit/*` wire. The hybrid is not a compromise — it is the ceiling.

---

## 6. What the prototype demonstrated vs what remains unproven

**Demonstrated (ran live on this machine; isolation verified three ways — no root
`Cargo.toml [workspace]`, absent from `pnpm-workspace.yaml`, absent from root
`package.json` workspaces; `git status` showed only `spikes/` added):**
- **GOAL 1** (`goal1_two_peers.rs`): two in-process rust-libp2p 0.56.0 peers over
  TCP+Noise+Yamux, negotiated `identify`, opened `/peerbit/direct-stream/2.0.0` via
  `libp2p-stream` `Control::open_stream`/`accept`, exchanged a varint-length-prefixed
  frame with the `DataMessage=0` tag, full echo round-trip. identify even advertised
  the mounted `/peerbit/*` protocol — exactly as a Peerbit registrar mount would.
- **GOAL 2** (the real interop question): rust dialer ↔ **js-libp2p 3.3.4** node
  (`listener.mjs`) configured like a Peerbit node peer. Resolved js versions match the
  spec: libp2p 3.3.4, noise 17.0.0, yamux 8.0.1, tcp 11.0.22, identify 4.1.8, Ed25519
  identity. **TCP+Noise+Yamux+multistream-select negotiated cross-implementation;**
  rust opened `/peerbit/direct-stream/2.0.0` on the js node; rust sent a varint-framed
  DataMessage; js decoded `tag=0 payload=hello-from-rust`; js echoed; rust received
  it. **Byte-identical framing across rust and js confirmed.** Both stacks ran
  concurrently with full crates.io + npm network access.
- **Build findings** (resolved, carry-forward): `websocket` requires `dns` in 0.56.0
  or fails to compile (documented `Cargo.toml:32-37`); `libp2p-stream 0.4.0-alpha`
  compiled and ran unmodified but is alpha; js-libp2p v2→v3 changed the stream API
  (MessageStream + `.send()`, positional handler `(stream, connection)`) — a js-side
  concern that does **not** touch the rust↔js wire.

**Not proven (out of scope by design / environment):**
- **The DirectStream engine port itself** — Borsh message schema, Ed25519/SHA-256
  signing, seen-cache, 4-lane scheduler, routing tables, all three multicodecs. The
  spike proves *transport + protocol-negotiation + framing* only — the prerequisite
  everything gates behind — not the load-bearing wire re-implementation (§3d, §4).
- **Circuit-Relay-v2 relayed connections** for NAT'd node peers (needs a 3rd relay
  process; `relay` feature compiles). The research says relayed conns/reservations
  interop but rust↔js DCUtR hole-punch is unreliable — unverified locally.
- **WebSocket rust↔js live** (matrix known-good, but this spike ran TCP end-to-end;
  WS was compile-checked only).
- **Scale / throughput / connection-count** — no perf numbers; the transport payoff
  (§4) remains a Phase-0-profiling question.
- **Browser anything** — deliberately not attempted (§5).

---

## 7. Phased plan, cost/risk (t-shirt), and top open questions

### Phased plan

| Phase | Work | Size | Gate |
|---|---|---|---|
| **0. Profile** | Measure whether the transport layer (js-libp2p socket + copy) is actually material on node under real load. | **S** | **Blocks everything.** If transport isn't the bottleneck, stop — the #988 wasm cores already capture the measured wins (decode/verify/fusion). |
| **1. Rust node swarm shell** | `peerbit_swarm` crate: tokio rust-libp2p 0.56.0 Swarm (tcp+websocket+noise+yamux+identify+relay-v2 client, `dns` on), Ed25519 identity bridged to peerId. Mount `/peerbit/*` via `libp2p-stream` Control (or hand-rolled behaviour). | **M** | Spike already proves the mechanics. |
| **2. DirectStream native wire** | Port the wire per multicodec: unsigned-varint framing, Borsh schema + 1-byte tags, Ed25519/SHA-256 signing, feed `decode_and_verify_frames(&[&[u8]])` natively (drop `array.to_vec`), native LaneScheduler chunk store + AsyncWrite sink, stream lifecycle/backpressure. ×3 multicodecs. | **L** | **The load-bearing work.** This is where the risk and cost live. |
| **3. Mixed-fleet interop CI** | rust node ⇄ js node ⇄ js browser E2E, both directions, per layer (blocks/pubsub/fanout). Byte-parity assertions on every envelope. | **M** | Interop is a hard requirement every phase must preserve. |
| **4. Relay-v2 hardening** | Relayed connections for NAT'd node peers; explicitly *avoid* relying on rust↔js DCUtR. Optionally run a rust relay server (unlimited reservations). | **M** | Sharp open question (below). |
| **5. Remove js-libp2p from node profile** | Once 1–4 are green under load, drop js-libp2p on node. Node achieves "no JS networking." | **S–M** | Browser stays js forever. |
| **B. Browser wasm swarm** | **Deferred / blocked upstream.** Revisit only if rust #4389/#5978 land *and* enter the interop matrix *and* profiling shows browser JS transport is material. | **XL / blocked** | Not on the critical path. |

### Cost/risk t-shirt summary
- **Transport swap (Phases 0–1):** **S–M**, low risk — proven by this spike, off-the-shelf crates.
- **Native DirectStream wire (Phase 2):** **L**, medium-high risk — three multicodecs, Borsh + signing parity, byte-exact framing; a subtle divergence silently breaks mixed-fleet interop.
- **Interop CI (Phase 3):** **M**, low-medium — the design already has mixed-topology specs to extend.
- **Relay-v2 (Phase 4):** **M**, **medium-high / sharp** — relayed conns known-good but not locally verified; DCUtR unreliable.
- **Browser (Phase B):** **XL / blocked upstream** — do not schedule.

### Top open questions / risks (sharpest first)
1. **Circuit-Relay-v2 rust↔js under Peerbit's exact config — the sharpest node risk.**
   The matrix says relayed conns/reservations interop, but Peerbit uses
   `applyDefaultLimit:false` + `maxReservations:1000` and mounts `/peerbit/*` with
   `runOnLimitedConnection:false` (streams refuse limited/relayed conns and need a
   full/promoted one). **Unverified:** does a rust node correctly reserve on a js relay
   *and* get its `/peerbit/*` streams onto a full connection, given rust↔js DCUtR
   hole-punch is unreliable? If NAT'd node peers can't promote off the relay, they
   can't run the app protocols. **Must be spiked before committing (Phase 4).**
2. **WebRTC / browser boundary — the sharpest strategic risk (permanent, not a bug).**
   Browser peers are stuck on js-libp2p; #4389/#5978 are open and not interop-tested.
   Any roadmap that assumes "eventually all-rust" is wrong. Plan for a permanent
   hybrid; do not let it become an implicit blocker.
3. **`libp2p-stream 0.4.0-alpha` API churn.** The binding point is alpha ("streams
   dropped if your application falls behind" — backpressure to watch when porting the
   4-lane scheduler). Ran unmodified here, but budget for a hand-rolled
   `NetworkBehaviour` fallback (same substream open/accept over the three multicodecs).
4. **Wire byte-parity across the port.** Borsh schema + variant tags + Ed25519/SHA-256
   signing + varint framing must be byte-identical to js, ×3 multicodecs. A silent
   divergence breaks mixed-fleet interop with no loud failure. Byte-parity fixtures in
   CI are mandatory (Phase 3).
5. **Payoff is unproven.** No perf data shows transport is the node bottleneck; every
   measured #988 win is CPU-side (decode/verify/fusion), not transport. Phase 0 must
   justify the whole track, or this is a strategic (remove-js) rather than a
   performance investment.

---

### Reproduce
```sh
cd spikes/rust-libp2p-poc
cargo run --bin goal1_two_peers                 # GOAL 1, self-contained
cd js && npm install && node listener.mjs       # prints DIAL_ME=/ip4/.../tcp/<port>/p2p/<peerId>
cargo run --bin goal2_dial_js -- <DIAL_ME>       # GOAL 2, rust dials js
# RUST_LOG=info for the negotiation trace. Toolchain: cargo/rustc 1.94.1, node v24.14.1.
```
