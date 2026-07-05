# rust-libp2p feasibility spike

**Question.** PR #988 ported Peerbit's application protocols (DirectStream
routing/relay/ACK/lanes, pubsub, block exchange) to Rust/WASM, but the *transport*
stays js-libp2p. Could a **rust-libp2p node swarm replace js-libp2p on node peers**
while still interoperating with the existing js-libp2p fleet (browsers must stay
js-libp2p — WASM never owns sockets)?

**Verdict from this spike: YES for node peers.** Both goals passed *live* on this
machine — a `rust-libp2p 0.56.0` peer negotiates TCP + Noise + Yamux + identify and
opens a Peerbit-style protocol stream against a `js-libp2p 3.3.4` node configured
like a Peerbit node peer, and the two exchange a byte-identical varint-framed
message round-trip.

> Throwaway spike. Not a workspace member; not built by CI. See `../README.md`.

---

## What was proven (ran live)

### GOAL 1 — two in-process rust peers (`src/goal1_two_peers.rs`) — **PASS (ran)**

Two `rust-libp2p` peers over **TCP + Noise + Yamux**:
- negotiated Noise + Yamux + `identify` (`/ipfs/id/1.0.0`)
- opened `/peerbit/direct-stream/2.0.0` via `libp2p-stream 0.4.0-alpha`
  (`Control::open_stream` / `Control::accept` → `IncomingStreams`)
- exchanged an **unsigned-varint length-prefixed** frame with Peerbit's 1-byte
  variant tag (`DataMessage = 0`), full echo round-trip
- `identify` even advertised `/peerbit/direct-stream/2.0.0` in the peer's protocol
  list, exactly as a Peerbit registrar mount would.

### GOAL 2 — rust peer dials a js-libp2p Peerbit-style node — **PASS (ran, the real interop question)**

`src/goal2_dial_js.rs` (rust dialer) against `js/listener.mjs` (js-libp2p node):
- **TCP + Noise + Yamux negotiated cross-implementation** (rust ⇄ js)
- rust **opened `/peerbit/direct-stream/2.0.0` on the js node** — multistream-select
  1.0 protocol negotiation succeeded across implementations
- rust sent a varint-framed DataMessage; js decoded `tag=0 payload=hello-from-rust`;
  js echoed; rust received `echo:hello-from-rust`.
- **The framing is byte-identical across rust and js** — both hand-roll the same
  unsigned-varint length prefix + tag, proving the Peerbit wire envelope is
  reproducible on the Rust side.

js versions resolved (match `packages/transport/stream/package.json`):
`libp2p 3.3.4`, `@chainsafe/libp2p-noise 17.0.0`, `@chainsafe/libp2p-yamux 8.0.1`,
`@libp2p/tcp 11.0.22`, `@libp2p/identify 4.1.8`, Ed25519 identity.

This directly exercises the shared interop path the INTEROP research identified as
libp2p-test-plans continuously-cross-tested known-good: **tcp/ws + noise + yamux +
multistream-select 1.0 + identify**.

---

## How to reproduce

```sh
# Rust (needs network for the first `cargo fetch`)
cd spikes/rust-libp2p-poc
cargo run --bin goal1_two_peers            # GOAL 1, self-contained

# GOAL 2 — two terminals:
cd js && npm install && node listener.mjs  # prints DIAL_ME=/ip4/127.0.0.1/tcp/<port>/p2p/<peerId>
# then, back in spikes/rust-libp2p-poc:
cargo run --bin goal2_dial_js -- <the DIAL_ME multiaddr>
```

Set `RUST_LOG=info` for the connection/negotiation trace.

---

## Findings worth carrying forward

1. **The shared interop path works, live.** rust-libp2p 0.56.0 ⇄ js-libp2p 3.3.4
   over TCP+Noise+Yamux+multistream-select+identify — connect *and* per-protocol
   stream open *and* app-level frame round-trip — all succeeded with no shims on
   the transport. This is the load-bearing claim for "rust node swarm replaces js
   node swarm."

2. **`libp2p-stream` is the natural DirectStream binding point and it works, but it
   is `0.4.0-alpha`.** `Control::open_stream(peer, StreamProtocol)` and
   `Control::accept(StreamProtocol) -> IncomingStreams` map 1:1 onto Peerbit's
   "each consumer registers its own multicodec" model — you call them once per
   `/peerbit/*` id (`direct-block/1.0.0`, `topic-control-plane/2.0.0`,
   `fanout-tree/0.5.0`). The alpha status (API-churn risk) is real; the documented
   fallback is a hand-rolled `NetworkBehaviour` doing the same substream
   open/accept. For this spike the alpha API compiled and ran unmodified.

3. **Build gotcha (documented in `Cargo.toml`): `websocket` requires `dns`.** In
   libp2p 0.56.0 the `SwarmBuilder::with_websocket` phase references a DNS-wrapped
   TCP + `WebsocketErrorInner::Dns` that are `#[cfg(feature = "dns")]`-gated.
   Enabling `websocket` without `dns` fails to compile (`E0599 no variant Dns`). A
   Peerbit node needs the `/ws` listen addr, so a real port must enable
   `dns + websocket` together.

4. **js-libp2p v2→v3 changed the stream API (this is a js concern, NOT interop).**
   The listener had to be written against v3's `MessageStream`
   (`AsyncIterable<Uint8Array|Uint8ArrayList>` + `.send()`), and the stream-handler
   callback signature is now positional `(stream, connection)` — not the v2
   `{ stream }` object. `it-length-prefixed-stream` v2 (which wants the old
   `.sink` duplex) does **not** work against a v3 stream. None of this touches the
   rust⇄js wire; it only affects how the *js* side reads/writes bytes. The Peerbit
   codebase already targets libp2p 3.x so it uses the correct APIs; this spike's
   listener is a standalone reimplementation and had to catch up.

5. **Scope boundary held.** This spike does not attempt the browser transport,
   which the research shows cannot move to rust (rust-libp2p has no browser↔browser
   `/webrtc` listen; webrtc-websys is webrtc-direct dial-only; upstream #4389/#5978
   still open). The realistic target remains a **hybrid fleet**: js-libp2p keeps the
   browser transport permanently (Rust protocol cores in WASM over JS-owned
   `/peerbit/*` byte streams), while **node peers can swap to a rust-libp2p
   transport** — which is exactly what GOAL 2 demonstrates end to end.

## Not covered here (by design / environment)

- NAT'd node peers over **Circuit-Relay-v2**: the research says relayed
  connections/reservations interop (test-plans known-good) but rust↔js **DCUtR
  hole-punch to direct is not reliable** — use relayed connections for NAT'd node
  peers. Not exercised in this spike (would need a third relay process); the
  transport features (`relay`) are enabled in `Cargo.toml` and compile.
- The actual DirectStream engine port (Borsh schema, seen-cache, lanes,
  routing) — out of scope; this spike only proves the transport + protocol
  negotiation + framing, which is the prerequisite the plan gates everything else
  behind.
