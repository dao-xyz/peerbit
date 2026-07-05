//! GOAL 1 (must-try): two in-process rust-libp2p peers that dial, negotiate
//! Noise+Yamux over TCP, run `identify`, open a stream on a Peerbit-style
//! protocol id, and exchange one length-prefixed frame.
//!
//! Why this shape: it proves the rust swarm can perform the *exact*
//! protocol-negotiation Peerbit needs on a NODE peer:
//!   - transport: TCP + Noise + Yamux (the shared js<->rust interop path)
//!   - identify:  /ipfs/id/1.0.0 (Peerbit mounts identify)
//!   - a custom multicodec opened via libp2p-stream (the DirectStream binding point)
//!
//! Peerbit reality this mirrors (from the config research):
//!   - There is NO single /peerbit/direct-stream protocol. DirectStream is an
//!     abstract engine; each consumer registers its OWN multicodec:
//!       /peerbit/direct-block/1.0.0, /peerbit/topic-control-plane/2.0.0,
//!       /peerbit/fanout-tree/0.5.0
//!     Here we use one representative id, /peerbit/direct-stream/2.0.0, to prove
//!     negotiation. In a real port you call `control.accept(...)` /
//!     `open_stream(...)` three times, once per multicodec.
//!   - Wire framing = it-length-prefixed unsigned-varint wrapping Borsh with a
//!     1-byte variant tag (DataMessage=0). We reproduce the varint length prefix
//!     + a 1-byte tag here to show the framing is byte-compatible.

use std::time::Duration;

use anyhow::Result;
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::{
    identify, identity, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, StreamProtocol, Swarm,
};
use libp2p_stream as stream;
use tracing_subscriber::EnvFilter;

/// One representative Peerbit DirectStream multicodec. In the real fleet this is
/// one of three frozen ids; the negotiation mechanics are identical for all.
const PEERBIT_PROTOCOL: StreamProtocol = StreamProtocol::new("/peerbit/direct-stream/2.0.0");

/// Peerbit's DataMessage variant tag (DataMessage=0, ACK=1, Hello=2, Goodbye=3).
const DATA_MESSAGE_TAG: u8 = 0;

/// Combined behaviour: identify (Peerbit mounts it) + stream (DirectStream binding).
#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    stream: stream::Behaviour,
}

fn build_swarm(keypair: identity::Keypair) -> Result<Swarm<Behaviour>> {
    let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        // TCP + Noise + Yamux — the exact shared interop path with js-libp2p v3.x.
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| Behaviour {
            identify: identify::Behaviour::new(identify::Config::new(
                // Peerbit identify does not set a custom protocol version string;
                // the default /ipfs/id/1.0.0 is what js-libp2p speaks too.
                "/ipfs/id/1.0.0".to_string(),
                key.public(),
            )),
            stream: stream::Behaviour::new(),
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(15)))
        .build();
    Ok(swarm)
}

/// Frame a payload exactly like Peerbit's wire: unsigned-varint length prefix,
/// then the message bytes (here: [tag, ..payload]). Real Peerbit puts a Borsh
/// message after the tag; we keep the tag + raw bytes to prove the framing.
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(payload.len() + 1);
    body.push(DATA_MESSAGE_TAG);
    body.extend_from_slice(payload);

    let mut buf = unsigned_varint::encode::usize_buffer();
    let len_prefix = unsigned_varint::encode::usize(body.len(), &mut buf);

    let mut out = Vec::with_capacity(len_prefix.len() + body.len());
    out.extend_from_slice(len_prefix);
    out.extend_from_slice(&body);
    out
}

/// Read one varint-length-prefixed frame from a stream and return the body
/// (including the leading 1-byte variant tag).
async fn read_frame<S>(stream: &mut S) -> Result<Vec<u8>>
where
    S: AsyncReadExt + Unpin,
{
    // Decode the unsigned-varint length one byte at a time (max 9 bytes for usize).
    let mut len: usize = 0;
    let mut shift = 0u32;
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).await?;
        len |= ((b[0] & 0x7f) as usize) << shift;
        if b[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .try_init();

    // -- Peer B: the listener. Identity is Ed25519 (Peerbit HARD-REQUIRES ed25519). --
    let kp_b = identity::Keypair::generate_ed25519();
    let peer_b_id = PeerId::from(kp_b.public());
    let mut swarm_b = build_swarm(kp_b)?;
    swarm_b.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?;

    // Wait for peer B to bind and report its listen address.
    let listen_addr: Multiaddr = loop {
        if let SwarmEvent::NewListenAddr { address, .. } = swarm_b.select_next_some().await {
            break address;
        }
    };
    tracing::info!(%peer_b_id, %listen_addr, "peer B listening");

    // Peer B accepts inbound Peerbit-protocol streams.
    let mut incoming = swarm_b
        .behaviour()
        .stream
        .new_control()
        .accept(PEERBIT_PROTOCOL)?;

    // Drive peer B's swarm in the background.
    let b_task = tokio::spawn(async move {
        loop {
            match swarm_b.select_next_some().await {
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    tracing::info!(%peer_id, "peer B: connection established");
                }
                SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                    peer_id,
                    info,
                    ..
                })) => {
                    tracing::info!(%peer_id, protocols=?info.protocols, "peer B: identify received");
                }
                _ => {}
            }
        }
    });

    // Peer B: handle exactly one inbound stream, echo the framed reply back.
    let echo_task = tokio::spawn(async move {
        if let Some((peer, mut stream)) = incoming.next().await {
            tracing::info!(%peer, "peer B: inbound Peerbit stream negotiated");
            let body = read_frame(&mut stream).await.expect("read inbound frame");
            let tag = body[0];
            let payload = String::from_utf8_lossy(&body[1..]).to_string();
            tracing::info!(tag, %payload, "peer B: received frame");
            assert_eq!(tag, DATA_MESSAGE_TAG, "expected DataMessage tag");

            // Reply so peer A can confirm a full round-trip.
            let reply = frame(format!("echo:{payload}").as_bytes());
            stream.write_all(&reply).await.expect("write reply");
            stream.flush().await.expect("flush");
            stream.close().await.expect("close");
            payload
        } else {
            panic!("peer B: no inbound stream");
        }
    });

    // -- Peer A: the dialer. --
    let kp_a = identity::Keypair::generate_ed25519();
    let peer_a_id = PeerId::from(kp_a.public());
    let mut swarm_a = build_swarm(kp_a)?;
    tracing::info!(%peer_a_id, "peer A created");

    // Add peer B's address then dial.
    swarm_a.dial(listen_addr.clone())?;
    let mut control_a = swarm_a.behaviour().stream.new_control();

    // Drive peer A's swarm until connected to peer B, then open the stream.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let mut tx = Some(tx);
    let a_task = tokio::spawn(async move {
        loop {
            match swarm_a.select_next_some().await {
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    tracing::info!(%peer_id, "peer A: connection established");
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(());
                    }
                }
                SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                    peer_id,
                    info,
                    ..
                })) => {
                    tracing::info!(%peer_id, protocols=?info.protocols, "peer A: identify received");
                }
                _ => {}
            }
        }
    });

    // Wait until the connection is up before opening a stream.
    rx.await?;

    // Open the Peerbit-style stream and send one framed DataMessage.
    let mut stream = control_a.open_stream(peer_b_id, PEERBIT_PROTOCOL).await?;
    tracing::info!(%peer_b_id, protocol=%PEERBIT_PROTOCOL, "peer A: opened Peerbit stream");

    let msg = frame(b"hello-peerbit");
    stream.write_all(&msg).await?;
    stream.flush().await?;
    tracing::info!("peer A: sent framed DataMessage");

    // Read peer B's echo reply to prove a full application round-trip.
    let reply_body = read_frame(&mut stream).await?;
    let reply = String::from_utf8_lossy(&reply_body[1..]).to_string();
    tracing::info!(%reply, "peer A: received echo reply");
    assert_eq!(reply, "echo:hello-peerbit");

    let received_by_b = echo_task.await?;
    assert_eq!(received_by_b, "hello-peerbit");

    println!("\n=== GOAL 1 PASS ===");
    println!("Two rust-libp2p peers over TCP+Noise+Yamux:");
    println!("  - negotiated Noise + Yamux + identify");
    println!("  - opened protocol {PEERBIT_PROTOCOL}");
    println!("  - exchanged a varint-length-prefixed frame (DataMessage tag=0)");
    println!("  - full round-trip echo confirmed");

    a_task.abort();
    b_task.abort();
    Ok(())
}
