//! The tokio rust-libp2p Swarm matching Peerbit's NODE interop contract.
//!
//! Transports: TCP + WebSocket(+DNS) + circuit-relay-v2 **client**. Muxer:
//! Yamux only. Encrypter: Noise only. Behaviours: `identify` +
//! `libp2p-stream` (the `/peerbit/*` binding point) + the relay client. This
//! is exactly the Peerbit node config from FEASIBILITY.md §3 and the feature
//! set the spike proved live against js-libp2p 3.3.4.
//!
//! # Why relay-client, not relay-server
//!
//! A Peerbit node needs to be *reachable* via `/p2p-circuit` (dial-through a js
//! relay), which is the **client** side. The rendezvous/server side
//! (`circuitRelayServer`) is Phase-2+/relay-hardening scope (FEASIBILITY.md
//! Phase 4). Phase 1 mounts the client so the swarm can reserve on a relay;
//! promoting `/peerbit/*` streams off a limited connection is the deferred
//! open question, not built here.
//!
//! # multistream-select 1.0, Yamux-only, Noise-only
//!
//! These are forced by construction: `noise::Config::new` is the only
//! encrypter and `yamux::Config::default` the only muxer passed to the builder,
//! and libp2p negotiates mss 1.0. That mirrors `libp2p.ts:90-91`
//! (`connectionEncrypters:[noise()]`, `streamMuxers:[yamux()]`).

use std::time::Duration;

use libp2p::{identify, noise, swarm::NetworkBehaviour, tcp, yamux, Swarm};
use libp2p_stream as stream;

use crate::identity::NodeIdentity;

/// identify protocol id — matches js-libp2p's default (`ipfs` prefix →
/// `/ipfs/id/1.0.0`), the same string the spike negotiated with the js node.
pub const IDENTIFY_PROTOCOL: &str = "/ipfs/id/1.0.0";

/// The idle-connection timeout. Peerbit does not aggressively prune
/// (`reconnectRetries:0`, connection-monitor does not abort on ping failure);
/// a generous idle keeps `/peerbit/*` streams alive between bursts.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);

/// The node's `NetworkBehaviour`: identify + the `libp2p-stream` binding point
/// for the three `/peerbit/*` multicodecs + the circuit-relay-v2 client.
#[derive(NetworkBehaviour)]
pub struct NodeBehaviour {
    /// Peer/protocol discovery. Peerbit mounts `identify()` and topology
    /// registration depends on it (`libp2p.ts:67-69`).
    pub identify: identify::Behaviour,
    /// The DirectStream binding point. `Control::open_stream`/`accept` per
    /// `/peerbit/*` multicodec. Alpha (`0.4.0-alpha`) — hand-rolled behaviour
    /// fallback budgeted (FEASIBILITY.md open-Q3).
    pub stream: stream::Behaviour,
    /// Circuit-relay-v2 client, so the node is reachable via `/p2p-circuit`.
    pub relay_client: libp2p::relay::client::Behaviour,
}

/// Errors building the node swarm.
#[derive(Debug)]
pub enum SwarmError {
    /// A transport-construction step failed.
    Build(String),
}

impl std::fmt::Display for SwarmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SwarmError::Build(message) => write!(f, "failed to build node swarm: {message}"),
        }
    }
}

impl std::error::Error for SwarmError {}

/// Build the Peerbit node swarm from a [`NodeIdentity`].
///
/// The transport chain is the Peerbit node stack: TCP (nodelay) + WebSocket
/// (DNS-wrapped, required in 0.56) + circuit-relay-v2 client, all
/// Noise-encrypted and Yamux-muxed. `with_websocket` is async (it resolves the
/// DNS transport), so this fn is async too.
pub async fn build_node_swarm(identity: &NodeIdentity) -> Result<Swarm<NodeBehaviour>, SwarmError> {
    let keypair = identity.keypair().clone();

    let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        // TCP — the rust<->js end-to-end interop path.
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|error| SwarmError::Build(format!("tcp: {error}")))?
        // WebSocket(+DNS). In 0.56 `websocket` requires `dns`; this phase
        // resolves the DNS transport, hence `.await`. js listens on /ws too.
        .with_websocket(noise::Config::new, yamux::Config::default)
        .await
        .map_err(|error| SwarmError::Build(format!("websocket: {error}")))?
        // Circuit-relay-v2 CLIENT — reachability via /p2p-circuit. Injects the
        // relay client behaviour into the behaviour closure below.
        .with_relay_client(noise::Config::new, yamux::Config::default)
        .map_err(|error| SwarmError::Build(format!("relay-client: {error}")))?
        .with_behaviour(|key, relay_client| NodeBehaviour {
            identify: identify::Behaviour::new(identify::Config::new(
                IDENTIFY_PROTOCOL.to_string(),
                key.public(),
            )),
            stream: stream::Behaviour::new(),
            relay_client,
        })
        .map_err(|error| SwarmError::Build(format!("behaviour: {error}")))?
        .with_swarm_config(|config| config.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
        .build();

    Ok(swarm)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PEERBIT_PROTOCOLS;

    fn test_identity() -> NodeIdentity {
        NodeIdentity::from_ed25519_bytes([7u8; 32]).expect("valid ed25519 seed")
    }

    #[tokio::test]
    async fn swarm_builds_with_node_transport_stack() {
        // Proves the full node transport chain (tcp + websocket(+dns) +
        // relay-client + noise + yamux + identify + libp2p-stream) type-checks
        // and constructs. This is the compile-and-construct proof of the node
        // interop contract.
        let identity = test_identity();
        let swarm = build_node_swarm(&identity)
            .await
            .expect("node swarm builds");
        // The swarm's local peer id is the identity's peerId (one key drives it).
        assert_eq!(*swarm.local_peer_id(), identity.peer_id());
    }

    #[tokio::test]
    async fn control_can_be_obtained_for_each_peerbit_protocol() {
        // The libp2p-stream binding point yields a Control; the three frozen
        // multicodecs can each be prepared for accept. (Actual open/accept is
        // exercised by the live interop test.)
        let identity = test_identity();
        let swarm = build_node_swarm(&identity).await.expect("swarm");
        let mut control = swarm.behaviour().stream.new_control();
        for protocol in PEERBIT_PROTOCOLS {
            // accept() registers the inbound handler for the multicodec; a
            // successful call proves the id is a valid StreamProtocol and the
            // control accepts it. Drop the IncomingStreams immediately.
            let incoming = control.accept(protocol.stream_protocol());
            assert!(
                incoming.is_ok(),
                "control.accept failed for {}",
                protocol.as_str()
            );
        }
    }
}
