/**
 * `@peerbit/transport-rust` — Phase-1 TS surface.
 *
 * The substance of this package is the native `peerbit_transport` Rust crate
 * (see `src/lib.rs` and the crate `Cargo.toml`): a rust-libp2p 0.56 node swarm
 * that owns the connection layer, bridges the Ed25519 identity, mounts the
 * three frozen `/peerbit/*` multicodecs, and frames with the `peerbit_wire`
 * codec. That crate is NOT compiled into the wasm pipeline and NOT built by
 * this package's `build` script — it is exercised only by `cargo test` + the
 * live interop harness in the `test_native` CI job.
 *
 * This TS module is intentionally thin: it exposes the frozen protocol ids and
 * framing caps as constants so JS-side callers and tests can reference the same
 * wire contract the Rust crate mounts. The full adapter/node wiring into
 * `createLibp2pExtended` is Phase 2+ and depends on the maintainer's decision
 * about how a native tokio swarm binds into the wasm node runtime, so it is
 * deliberately absent here. The default node path stays js-libp2p; importing
 * this module has no runtime effect on it.
 */

/**
 * The three frozen `/peerbit/*` protocol multicodecs the native transport
 * mounts. Byte-identical to `packages/transport/transport-rust/src/protocol.rs`
 * and to the js registrar mounts (`blocks`, `pubsub`).
 */
export const PEERBIT_PROTOCOLS = {
  /** DirectBlock exchange (`blocks/src/libp2p.ts:52`). */
  directBlock: '/peerbit/direct-block/1.0.0',
  /** pubsub TopicControlPlane (`pubsub/src/index.ts:317`). */
  topicControlPlane: '/peerbit/topic-control-plane/2.0.0',
  /** FanoutTree overlay (`pubsub/src/fanout-tree.ts:873,1365`). */
  fanoutTree: '/peerbit/fanout-tree/0.5.0'
} as const

/** The union of frozen `/peerbit/*` protocol id string literals. */
export type PeerbitProtocolId = (typeof PEERBIT_PROTOCOLS)[keyof typeof PEERBIT_PROTOCOLS]

/**
 * All three protocol ids as a readonly array, in the same order the native
 * transport iterates them.
 */
export const PEERBIT_PROTOCOL_IDS: readonly PeerbitProtocolId[] = [
  PEERBIT_PROTOCOLS.directBlock,
  PEERBIT_PROTOCOLS.topicControlPlane,
  PEERBIT_PROTOCOLS.fanoutTree
]

/**
 * Inbound frame length cap — matches `MAX_DATA_LENGTH_IN`
 * (`stream/src/index.ts:245`) and the Rust `framing::MAX_DATA_LENGTH_IN`.
 */
export const MAX_DATA_LENGTH_IN = 15_000_000 + 1000

/**
 * Outbound frame length cap — matches `MAX_DATA_LENGTH_OUT`
 * (`stream/src/index.ts:246`) and the Rust `framing::MAX_DATA_LENGTH_OUT`.
 */
export const MAX_DATA_LENGTH_OUT = 10_000_000 + 1000
