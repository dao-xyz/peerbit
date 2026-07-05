//! The frozen Peerbit `/peerbit/*` protocol multicodecs.
//!
//! "DirectStream" is an abstract routing/relay/ACK/lane engine; it has **no
//! single protocol id**. Each consumer registers its own multicodec and runs
//! the identical framing + Borsh envelope underneath. A node must therefore
//! mount the DirectStream wire three times over three distinct multicodec
//! strings (FEASIBILITY.md §3, verified against the repo):
//!
//! 1. `/peerbit/direct-block/1.0.0` — DirectBlock exchange
//!    (`blocks/src/libp2p.ts:52`).
//! 2. `/peerbit/topic-control-plane/2.0.0` — pubsub TopicControlPlane
//!    (`pubsub/src/index.ts:317`).
//! 3. `/peerbit/fanout-tree/0.5.0` — FanoutTree overlay
//!    (`pubsub/src/fanout-tree.ts:873,1365`).
//!
//! These strings are **frozen and byte-identical** across the js/rust fleet;
//! changing one silently breaks mixed-fleet interop. They are pinned here as
//! `const` so a typo is a compile error, and the [`PEERBIT_PROTOCOLS`] array is
//! the single source the swarm iterates when mounting streams.

use libp2p::StreamProtocol;

/// `/peerbit/direct-block/1.0.0` — DirectBlock exchange multicodec.
pub const DIRECT_BLOCK_PROTOCOL: StreamProtocol =
    StreamProtocol::new("/peerbit/direct-block/1.0.0");

/// `/peerbit/topic-control-plane/2.0.0` — pubsub TopicControlPlane multicodec.
pub const TOPIC_CONTROL_PROTOCOL: StreamProtocol =
    StreamProtocol::new("/peerbit/topic-control-plane/2.0.0");

/// `/peerbit/fanout-tree/0.5.0` — FanoutTree overlay multicodec.
pub const FANOUT_TREE_PROTOCOL: StreamProtocol = StreamProtocol::new("/peerbit/fanout-tree/0.5.0");

/// All three frozen `/peerbit/*` multicodecs, in a stable order. The swarm
/// mounts (accepts) and can open one `libp2p-stream` per entry.
pub const PEERBIT_PROTOCOLS: [PeerbitProtocol; 3] = [
    PeerbitProtocol::DirectBlock,
    PeerbitProtocol::TopicControlPlane,
    PeerbitProtocol::FanoutTree,
];

/// The three Peerbit application protocols carried over the native transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PeerbitProtocol {
    /// `/peerbit/direct-block/1.0.0`
    DirectBlock,
    /// `/peerbit/topic-control-plane/2.0.0`
    TopicControlPlane,
    /// `/peerbit/fanout-tree/0.5.0`
    FanoutTree,
}

impl PeerbitProtocol {
    /// The frozen multicodec string for this protocol.
    pub const fn stream_protocol(self) -> StreamProtocol {
        match self {
            PeerbitProtocol::DirectBlock => DIRECT_BLOCK_PROTOCOL,
            PeerbitProtocol::TopicControlPlane => TOPIC_CONTROL_PROTOCOL,
            PeerbitProtocol::FanoutTree => FANOUT_TREE_PROTOCOL,
        }
    }

    /// The multicodec as a `&str`.
    pub const fn as_str(self) -> &'static str {
        match self {
            PeerbitProtocol::DirectBlock => "/peerbit/direct-block/1.0.0",
            PeerbitProtocol::TopicControlPlane => "/peerbit/topic-control-plane/2.0.0",
            PeerbitProtocol::FanoutTree => "/peerbit/fanout-tree/0.5.0",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_ids_are_frozen_byte_identical() {
        // These strings are part of the wire contract; if any of these
        // assertions ever needs changing, mixed-fleet interop is broken.
        assert_eq!(
            DIRECT_BLOCK_PROTOCOL.as_ref(),
            "/peerbit/direct-block/1.0.0"
        );
        assert_eq!(
            TOPIC_CONTROL_PROTOCOL.as_ref(),
            "/peerbit/topic-control-plane/2.0.0"
        );
        assert_eq!(FANOUT_TREE_PROTOCOL.as_ref(), "/peerbit/fanout-tree/0.5.0");
    }

    #[test]
    fn all_three_protocols_are_enumerated() {
        assert_eq!(PEERBIT_PROTOCOLS.len(), 3);
        for protocol in PEERBIT_PROTOCOLS {
            // as_str and the StreamProtocol must agree.
            assert_eq!(protocol.stream_protocol().as_ref(), protocol.as_str());
        }
    }
}
