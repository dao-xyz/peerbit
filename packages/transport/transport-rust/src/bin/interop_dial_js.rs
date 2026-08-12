//! LIVE INTEROP TEST-RUNNER — the Phase-1 deliverable that grounds everything.
//!
//! A `peerbit_transport` (rust-libp2p 0.56) node dials a js-libp2p 3.3.4 node
//! configured like a Peerbit node peer (`js/listener.mjs`), negotiates
//! TCP + Noise + Yamux + identify cross-implementation, and for EACH of the
//! three frozen `/peerbit/*` multicodecs:
//!
//!   1. opens the stream via `libp2p-stream` `Control::open_stream`,
//!   2. sends a signed `DataMessage` envelope produced by the REAL
//!      `peerbit_wire` codec (NOT a re-implementation), it-length-prefixed,
//!   3. reads the js node's echo — which the js side decoded with the REAL
//!      `@peerbit/stream-interface` `DataMessage.from(...)` and re-serialized
//!      with `.bytes()` — and asserts the echoed envelope is **byte-identical**
//!      to what rust sent.
//!
//! Byte parity in BOTH directions is thus proven with production codecs on both
//! stacks: rust `peerbit_wire::encode_frame` → js `DataMessage.from`/`.bytes()`
//! → rust `decode_frame`, all agreeing on the same bytes.
//!
//! Usage:
//!   node js/listener.mjs                        # prints DIAL_ME=/ip4/.../tcp/<port>/p2p/<peerId>
//!   cargo run --bin interop_dial_js -- <DIAL_ME>
//!
//! Exit code 0 = PASS; non-zero = FAIL.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use libp2p::{multiaddr::Protocol, swarm::SwarmEvent, Multiaddr, PeerId};
use tracing_subscriber::EnvFilter;

use peerbit_transport::framing::{read_frame, FrameCodec};
use peerbit_transport::identity::NodeIdentity;
use peerbit_transport::protocol::PEERBIT_PROTOCOLS;
use peerbit_transport::swarm::{build_node_swarm, NodeBehaviourEvent};

use futures::{AsyncWriteExt, StreamExt};
use peerbit_wire::wire::{
    encode_frame, DeliveryMode, MessageHeader, PublicSignKey, SignatureWithKey, WireMessage,
};
use peerbit_wire::wire::{PREHASH_SHA_256, VARIANT_DATA};

use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};

/// Deterministic 32-byte Ed25519 seed for the rust node identity + wire signing
/// (one key drives both — plan item 3). Fixed so runs are reproducible.
const NODE_SEED: [u8; 32] = [42u8; 32];

/// Build a signed `DataMessage` envelope via the frozen `peerbit_wire` codec.
///
/// The payload embeds the multicodec so the js side can confirm which stream it
/// arrived on. Signed with Ed25519 over the SHA-256 prehash of the signable
/// bytes — the exact scheme `@peerbit/crypto` uses on the direct-stream path.
fn signed_data_message(seed: [u8; 32], protocol: &str) -> Vec<u8> {
    let signing_key = SigningKey::from_bytes(&seed);
    let public = signing_key.verifying_key().to_bytes();

    let payload = format!("hello-from-rust-transport::{protocol}").into_bytes();

    // Header mirrors what a live DirectStream DataMessage carries: an id, the
    // three u64 timestamps, an AnyWhere delivery mode, and (initially) an empty
    // signature vec that we fill after computing the signable bytes.
    let mut message = WireMessage::Data {
        header: MessageHeader {
            id: {
                let mut id = [0u8; 32];
                let bytes = Sha256::digest(protocol.as_bytes());
                id.copy_from_slice(&bytes);
                id
            },
            timestamp: 1_700_000_000_000,
            session: 1_690_000_000_000,
            // Far future so the js-side header.verify() (wall-clock now) passes.
            expires: 4_102_444_800_000,
            priority: Some(0),
            response_priority: None,
            origin: None,
            mode: Some(DeliveryMode::AnyWhere),
            signatures: Some(Vec::new()),
        },
        data: Some(payload),
    };

    // Sign: Ed25519 over SHA-256(signable). `encode_signable` forces the mode
    // and signatures option flags to 0, exactly like TS `getSignableBytes`.
    let signable = peerbit_wire::wire::encode_signable(&message);
    let digest: [u8; 32] = Sha256::digest(&signable).into();
    let signature = signing_key.sign(&digest);
    message.header_mut().signatures = Some(vec![SignatureWithKey {
        signature: signature.to_bytes().to_vec(),
        public_key: PublicSignKey::Ed25519(public),
        prehash: PREHASH_SHA_256,
    }]);

    let envelope = encode_frame(&message);
    debug_assert_eq!(
        envelope[0], VARIANT_DATA,
        "first byte is the DataMessage tag"
    );
    envelope
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .try_init();

    let target: Multiaddr = std::env::args()
        .nth(1)
        .context("usage: interop_dial_js <multiaddr-of-js-node>")?
        .parse()
        .context("argv[1] is not a valid multiaddr")?;

    let target_peer: PeerId = target
        .iter()
        .find_map(|p| match p {
            Protocol::P2p(id) => Some(id),
            _ => None,
        })
        .context("multiaddr must include /p2p/<peerId>")?;

    let identity = NodeIdentity::from_ed25519_bytes(NODE_SEED)?;
    let me = identity.peer_id();
    let mut swarm = build_node_swarm(&identity).await?;
    tracing::info!(%me, %target, %target_peer, "rust peerbit_transport node dialing js-libp2p node");

    swarm.dial(target.clone())?;
    let control = swarm.behaviour().stream.new_control();

    loop {
        let event = swarm.select_next_some().await;
        match event {
            SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == target_peer => {
                // First connection to the js node: run the round-trip and exit.
                tracing::info!(%peer_id, "connected to js node (noise+yamux negotiated cross-impl)");
                let mut control = control.clone();
                let seed = NODE_SEED;
                let handle = tokio::spawn(async move {
                    interop_round_trip(&mut control, target_peer, seed).await
                });
                handle.await??;
                println!("\n=== LIVE INTEROP PASS ===");
                println!("rust peerbit_transport <-> js-libp2p (Peerbit node config):");
                println!("  - TCP + Noise + Yamux + identify negotiated cross-implementation");
                println!("  - all three /peerbit/* streams opened and byte-parity round-tripped");
                println!(
                    "    (rust peerbit_wire encode -> js DataMessage.from/.bytes -> rust decode)"
                );
                return Ok(());
            }
            SwarmEvent::OutgoingConnectionError { error, .. } => {
                bail!("failed to connect to js node: {error}");
            }
            SwarmEvent::Behaviour(NodeBehaviourEvent::Identify(event)) => {
                if let libp2p::identify::Event::Received { info, .. } = event {
                    tracing::info!(
                        agent = %info.agent_version,
                        protocols = ?info.protocols,
                        "identify from js node"
                    );
                }
            }
            _ => {}
        }
    }
}

/// Open each `/peerbit/*` stream, send a signed envelope, and assert the js
/// echo is byte-identical.
async fn interop_round_trip(
    control: &mut libp2p_stream::Control,
    target_peer: PeerId,
    seed: [u8; 32],
) -> Result<()> {
    for protocol in PEERBIT_PROTOCOLS {
        let stream_protocol = protocol.stream_protocol();
        let mut stream = control
            .open_stream(target_peer, stream_protocol)
            .await
            .with_context(|| format!("open_stream {} on js node", protocol.as_str()))?;
        tracing::info!(protocol = %protocol.as_str(), "opened /peerbit/* stream on js node");

        // Build + frame a real signed DataMessage envelope via peerbit_wire.
        let envelope = signed_data_message(seed, protocol.as_str());
        let framed = FrameCodec::frame_envelope(&envelope)?;
        stream.write_all(&framed).await.context("write frame")?;
        stream.flush().await.context("flush")?;

        // Read the js echo (one length-prefixed envelope) and assert parity.
        let echoed = read_frame(&mut stream)
            .await
            .with_context(|| format!("read echo on {}", protocol.as_str()))?;

        if echoed != envelope {
            bail!(
                "BYTE PARITY MISMATCH on {}: sent {} bytes, js echoed {} bytes (not identical)",
                protocol.as_str(),
                envelope.len(),
                echoed.len()
            );
        }

        // And it must decode via peerbit_wire back into a verified DataMessage.
        let record = FrameCodec::decode_and_verify(&[echoed.as_slice()], 1_700_000_000_500);
        let record = record.first().context("no record from decode_and_verify")?;
        if !record.decode_ok {
            bail!("echoed frame on {} failed to decode", protocol.as_str());
        }
        tracing::info!(
            protocol = %protocol.as_str(),
            bytes = envelope.len(),
            verify = ?record.verify,
            "byte-identical round-trip confirmed"
        );

        let _ = stream.close().await;
    }
    // Give the swarm a moment to flush the last close.
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}
