//! NATIVE circuit-relay-v2 relay node — Path B of the relay A/B and the subject
//! of the interop gate.
//!
//! Boots a rust-libp2p 0.56 relay-server swarm (TCP+WS+Noise+Yamux+identify+
//! relay-v2 server, limits matching the js `applyDefaultLimit:false,
//! maxReservations:1000` relay), listens on a TCP addr, prints
//! `RELAY_ADDR=/ip4/127.0.0.1/tcp/<port>/p2p/<peerId>` for the js harness to
//! reserve/dial through, and then drives the swarm forever, logging every
//! reservation and circuit event.
//!
//! Evidence for the interop gate is emitted as single-line, grep-able records:
//!   RESERVATION_ACCEPTED src=<peer> renewed=<bool>
//!   CIRCUIT_ACCEPTED src=<peer> dst=<peer>          (a js↔js circuit is up)
//!   CIRCUIT_CLOSED src=<peer> dst=<peer> error=<..> (bytes forwarded, closed)
//! A CIRCUIT_ACCEPTED followed by a clean CIRCUIT_CLOSED between two DISTINCT js
//! peers is the proof that the native relay forwarded js↔js traffic.
//!
//! Usage:
//!   relay_node                 # ephemeral TCP port, prints RELAY_ADDR
//!   relay_node <listen-multiaddr>   # e.g. /ip4/127.0.0.1/tcp/40001
//!
//! Runs until SIGINT/SIGTERM. Deterministic identity (fixed seed) so the peerId
//! is stable across runs.

use std::time::Duration;

use anyhow::{Context, Result};
use libp2p::{multiaddr::Protocol, relay, swarm::SwarmEvent, Multiaddr};
use tracing_subscriber::EnvFilter;

use futures::StreamExt;
use peerbit_transport::identity::NodeIdentity;
use peerbit_transport::relay::{build_relay_swarm, RelayBehaviourEvent};

/// Deterministic 32-byte Ed25519 seed for the relay identity. Fixed so the
/// relay peerId is stable across runs (the js harness pins nothing, it reads
/// RELAY_ADDR, but a stable id keeps logs comparable).
const RELAY_SEED: [u8; 32] = [77u8; 32];

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .try_init();

    // Listen addr: argv[1] if given, else an ephemeral TCP port on loopback.
    let listen: Multiaddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/ip4/127.0.0.1/tcp/0".to_string())
        .parse()
        .context("argv[1] is not a valid listen multiaddr")?;

    let identity = NodeIdentity::from_ed25519_bytes(RELAY_SEED)?;
    let me = identity.peer_id();
    let mut swarm = build_relay_swarm(&identity).await?;
    swarm
        .listen_on(listen.clone())
        .context("relay listen_on failed")?;

    tracing::info!(%me, %listen, "native rust-libp2p circuit-relay-v2 server starting");

    // Track the number of live circuits so the sweep driver can read the peak
    // concurrent-forwarding count the native relay actually sustained.
    let mut live_circuits: i64 = 0;
    let mut total_circuits: u64 = 0;

    loop {
        let event = swarm.select_next_some().await;
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                // Print the dial-through address once, with /p2p/<peerId>
                // appended, exactly like the js harness's RELAY_ADDR line.
                if address.iter().any(|p| matches!(p, Protocol::Ip4(_))) {
                    let full = address.clone().with(Protocol::P2p(me));
                    println!("RELAY_ADDR={full}");
                    println!("RELAY_SELF_PID={}", std::process::id());
                    tracing::info!(%full, "relay listening; clients may reserve/dial through this addr");
                }
            }
            SwarmEvent::Behaviour(RelayBehaviourEvent::Relay(event)) => match event {
                relay::Event::ReservationReqAccepted { src_peer_id, renewed } => {
                    println!("RESERVATION_ACCEPTED src={src_peer_id} renewed={renewed}");
                }
                relay::Event::ReservationReqDenied { src_peer_id, status } => {
                    println!("RESERVATION_DENIED src={src_peer_id} status={status:?}");
                }
                relay::Event::CircuitReqAccepted { src_peer_id, dst_peer_id } => {
                    live_circuits += 1;
                    total_circuits += 1;
                    println!(
                        "CIRCUIT_ACCEPTED src={src_peer_id} dst={dst_peer_id} live={live_circuits} total={total_circuits}"
                    );
                }
                relay::Event::CircuitReqDenied { src_peer_id, dst_peer_id, status } => {
                    println!(
                        "CIRCUIT_DENIED src={src_peer_id} dst={dst_peer_id} status={status:?}"
                    );
                }
                relay::Event::CircuitClosed { src_peer_id, dst_peer_id, error } => {
                    live_circuits -= 1;
                    println!(
                        "CIRCUIT_CLOSED src={src_peer_id} dst={dst_peer_id} live={live_circuits} error={error:?}"
                    );
                }
                relay::Event::ReservationClosed { src_peer_id } => {
                    tracing::debug!(%src_peer_id, "reservation closed");
                }
                relay::Event::ReservationTimedOut { src_peer_id } => {
                    tracing::debug!(%src_peer_id, "reservation timed out");
                }
                other => {
                    tracing::debug!(?other, "other relay event");
                }
            },
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                tracing::debug!(%peer_id, "connection established");
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                tracing::debug!(%peer_id, "connection closed");
            }
            _ => {}
        }

        // The relay never self-terminates; a bounded idle is not applicable
        // (the sweep driver kills it). This keeps the event loop hot.
        let _ = Duration::from_secs(0);
    }
}
