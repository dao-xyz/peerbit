//! The NATIVE rust-libp2p circuit-relay-v2 SERVER swarm — Path B of the relay
//! A/B, and the subject of the interop gate.
//!
//! # What a relay node does (and does NOT do)
//!
//! A circuit-relay-v2 relay forwards opaque bytes between two peers that cannot
//! connect directly. It operates purely at the libp2p transport layer: it
//! accepts HOP reservations from destination peers, accepts HOP `CONNECT`
//! requests from source peers, opens a STOP stream to the destination, and then
//! **pipes bytes socket→socket** for the lifetime of the circuit. It NEVER
//! decodes or verifies the `/peerbit/*` Borsh payloads it carries — the
//! per-frame Ed25519 verify that dominates the RECEIVE path (see PROFILING.md)
//! is ABSENT here. Relaying is an I/O + concurrency workload, which is exactly
//! where native tokio async is expected to beat the JS event loop under load.
//!
//! # Why this mirrors the js relay it is A/B'd against
//!
//! Peerbit's node relay is `circuitRelayServer({ reservations: {
//! applyDefaultLimit: false, maxReservations: 1000 } })`
//! (`clients/peerbit/src/transports.ts:19-23`). js-libp2p's `applyDefaultLimit:
//! false` removes the per-circuit data/duration caps entirely. The rust
//! `relay::Config` default is far stricter (`max_circuits: 16`,
//! `max_circuit_bytes: 128 KiB`, `max_circuit_duration: 2 min`, plus reservation
//! + circuit-source RATE LIMITERS). To compare like-for-like — an unthrottled,
//! high-capacity relay on both sides — [`relay_config`] raises the capacity
//! knobs and CLEARS the rate limiters. Anything left at the stricter rust
//! default would throttle the native relay and make the A/B measure the config,
//! not the runtime.

use std::time::Duration;

use libp2p::{identify, noise, relay, swarm::NetworkBehaviour, tcp, yamux, Swarm};

use crate::identity::NodeIdentity;

/// identify protocol id — matches js-libp2p's default (`/ipfs/id/1.0.0`), the
/// same string the node swarm and the js fleet negotiate.
pub const IDENTIFY_PROTOCOL: &str = "/ipfs/id/1.0.0";

/// Idle-connection timeout for the relay. A relay must hold the reservation and
/// circuit connections open across bursts; a generous idle keeps a reserved but
/// momentarily-quiet destination connected (js `circuitRelayServer` holds
/// reservations for `reservationTtl`, default 2 h). We keep it long so idle
/// reservations in a concurrency sweep are not reaped mid-run.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(600);

/// The relay node's behaviour: identify + the circuit-relay-v2 **server**.
///
/// No `libp2p-stream` here — a relay forwards bytes at the transport layer and
/// never mounts the `/peerbit/*` application protocols. Identify is kept so the
/// js clients learn the relay's observed address / protocols exactly as they do
/// against a js `circuitRelayServer` (which also runs identify).
#[derive(NetworkBehaviour)]
pub struct RelayBehaviour {
    /// Peer/protocol discovery — js clients expect identify on the relay.
    pub identify: identify::Behaviour,
    /// The circuit-relay-v2 SERVER. Accepts HOP reservations + CONNECTs and
    /// forwards STOP streams between js source/dest peers.
    pub relay: relay::Behaviour,
}

/// Build the relay-server [`relay::Config`] that mirrors the js Peerbit relay's
/// `applyDefaultLimit: false, maxReservations: 1000`.
///
/// The rust default is deliberately conservative; for an apples-to-apples A/B
/// against the unthrottled js relay we:
///   - raise `max_reservations` to 1000 (js `maxReservations: 1000`),
///   - raise per-peer reservation + circuit caps so many circuits between the
///     SAME source/dest pair are allowed (the sweep reuses a small peer set),
///   - raise `max_circuits` / `max_circuit_bytes` / `max_circuit_duration` so a
///     high-concurrency byte-streaming sweep is never truncated
///     (`applyDefaultLimit: false` on the js side removes these caps outright),
///   - CLEAR both rate-limiter vecs (the js relay applies none once
///     `applyDefaultLimit: false`); a default rust rate limiter would deny
///     bursty reservations/circuits and corrupt the scaling curve.
pub fn relay_config() -> relay::Config {
    relay::Config {
        max_reservations: 1000,
        max_reservations_per_peer: 1000,
        reservation_duration: Duration::from_secs(2 * 60 * 60),
        reservation_rate_limiters: Vec::new(),
        max_circuits: 4096,
        max_circuits_per_peer: 4096,
        max_circuit_duration: Duration::from_secs(10 * 60),
        max_circuit_bytes: 0, // 0 = no byte cap (js applyDefaultLimit:false)
        circuit_src_rate_limiters: Vec::new(),
    }
}

/// Errors building the relay swarm.
#[derive(Debug)]
pub enum RelaySwarmError {
    /// A transport-construction step failed.
    Build(String),
}

impl std::fmt::Display for RelaySwarmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelaySwarmError::Build(message) => {
                write!(f, "failed to build relay swarm: {message}")
            }
        }
    }
}

impl std::error::Error for RelaySwarmError {}

/// Build the native circuit-relay-v2 relay swarm from a [`NodeIdentity`].
///
/// Transport chain is TCP (nodelay) + WebSocket(+DNS), Noise-encrypted and
/// Yamux-muxed — the same encrypter/muxer the js relay and the js clients use,
/// so cross-impl negotiation is identical to the proven direct-dial path. No
/// relay-CLIENT phase: a relay server is reachable directly (the js clients dial
/// its TCP addr to reserve), so we do not add `.with_relay_client`.
pub async fn build_relay_swarm(
    identity: &NodeIdentity,
) -> Result<Swarm<RelayBehaviour>, RelaySwarmError> {
    let keypair = identity.keypair().clone();
    let local_peer_id = identity.peer_id();

    let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|error| RelaySwarmError::Build(format!("tcp: {error}")))?
        .with_websocket(noise::Config::new, yamux::Config::default)
        .await
        .map_err(|error| RelaySwarmError::Build(format!("websocket: {error}")))?
        .with_behaviour(|key| RelayBehaviour {
            identify: identify::Behaviour::new(identify::Config::new(
                IDENTIFY_PROTOCOL.to_string(),
                key.public(),
            )),
            relay: relay::Behaviour::new(local_peer_id, relay_config()),
        })
        .map_err(|error| RelaySwarmError::Build(format!("behaviour: {error}")))?
        .with_swarm_config(|config| config.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
        .build();

    Ok(swarm)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity() -> NodeIdentity {
        NodeIdentity::from_ed25519_bytes([9u8; 32]).expect("valid ed25519 seed")
    }

    #[tokio::test]
    async fn relay_swarm_builds_with_relay_server_stack() {
        // Proves the relay transport chain (tcp + websocket(+dns) + noise +
        // yamux + identify + relay-SERVER behaviour) type-checks and constructs,
        // and that the local peer id is the identity's peerId.
        let identity = test_identity();
        let swarm = build_relay_swarm(&identity).await.expect("relay swarm builds");
        assert_eq!(*swarm.local_peer_id(), identity.peer_id());
    }

    #[test]
    fn relay_config_matches_js_unthrottled_relay() {
        // The config must mirror the js `applyDefaultLimit:false, maxReservations:
        // 1000` relay: high capacity, no byte cap, and NO rate limiters (an
        // apples-to-apples A/B needs both relays unthrottled).
        let config = relay_config();
        assert_eq!(config.max_reservations, 1000);
        assert_eq!(config.max_circuit_bytes, 0, "no byte cap");
        assert!(
            config.reservation_rate_limiters.is_empty(),
            "reservation rate limiters must be cleared"
        );
        assert!(
            config.circuit_src_rate_limiters.is_empty(),
            "circuit-source rate limiters must be cleared"
        );
        assert!(config.max_circuits >= 1000, "high circuit ceiling for the sweep");
    }
}
