//! `peerbit_transport` — the native rust-libp2p node transport for Peerbit.
//!
//! This crate owns the connection layer a Peerbit **node** peer needs to speak
//! the unchanged `/peerbit/*` wire to the rest of the (js-libp2p) fleet:
//!
//! * a tokio [`libp2p`] Swarm matching Peerbit's node interop contract —
//!   TCP + WebSocket(+DNS) + Noise + Yamux + identify + circuit-relay-v2
//!   **client**, with Yamux as the only muxer and Noise as the only encrypter
//!   ([`swarm`]);
//! * an Ed25519 identity bridge that turns one raw 32-byte key into both the
//!   libp2p peerId Peerbit derives *and* the DirectStream message-signing key
//!   ([`identity`]);
//! * the three frozen `/peerbit/*` protocol multicodecs, each mounted over
//!   `libp2p-stream` ([`protocol`]);
//! * native it-length-prefixed unsigned-varint framing over each mounted
//!   stream, wrapping the Borsh envelope, calling the **already-frozen**
//!   `peerbit_wire` codec directly on socket slices ([`framing`]).
//!
//! # What this crate is NOT (Phase 1 boundary)
//!
//! It does not wire into the JS Peerbit runtime. The mechanism by which a
//! native tokio swarm binds into the wasm-based node runtime (napi vs sidecar
//! vs feature-gated native build) is a maintainer decision that Phase 1 is
//! deliberately independent of. Everything here is a standalone, in-tree,
//! CI-safe, interop-provable Rust crate. The default node path stays
//! js-libp2p; this crate is inert unless explicitly used.
//!
//! # Byte-parity by construction
//!
//! The codec is **not** re-implemented. [`framing`] calls
//! [`peerbit_wire::wire::encode_frame`] / [`peerbit_wire::wire::decode_frame`] /
//! [`peerbit_wire::wire::decode_and_verify_frames`] directly, and the outbound
//! ordering reuses [`peerbit_wire::direct_stream::lanes::LaneScheduler`]. A
//! divergence from the js fleet is therefore impossible without also breaking
//! the parity tests already guarding the merged `peerbit_wire` crate.

pub mod framing;
pub mod identity;
pub mod protocol;
pub mod swarm;

pub use framing::{FrameCodec, FramingError, MAX_DATA_LENGTH_IN, MAX_DATA_LENGTH_OUT};
pub use identity::{IdentityError, NodeIdentity};
pub use protocol::{
    PeerbitProtocol, DIRECT_BLOCK_PROTOCOL, FANOUT_TREE_PROTOCOL, PEERBIT_PROTOCOLS,
    TOPIC_CONTROL_PROTOCOL,
};
pub use swarm::{build_node_swarm, NodeBehaviour, NodeBehaviourEvent, SwarmError};
