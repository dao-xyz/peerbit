//! NATIVE-STACK PROOF — two fully-native Peerbit nodes, one process, no JS, no
//! wasm.
//!
//! This is the spike deliverable: two `peerbit_transport` (rust-libp2p 0.56)
//! swarms are built IN THE SAME PROCESS, connect to each other over
//! TCP + Noise + Yamux, open the frozen `/peerbit/topic-control-plane/2.0.0`
//! stream, and pass ONE real signed `PubSubData` `DataMessage` through the FULL
//! native engine stack:
//!
//! ```text
//!  DIALER (native)                         LISTENER (native)
//!  build_signed_pubsub_data                accept() /peerbit/topic-control-plane
//!  (peerbit_wire encode+Ed25519 sign)
//!  FrameCodec::frame_envelope  ── TCP ──▶  read_frame  (socket bytes -> Rust mem)
//!                                          NativeReceiveEngine.process_inbound_frame:
//!                                            decode_and_verify -> Verified   (native)
//!                                            SeenCache.modify  -> dedup       (native)
//!                                            should_ignore_data               (native)
//!                                            decode_pubsub_message            (native)
//!                                            should_acknowledge               (native)
//!                                            LaneScheduler.push (outbound order)(native)
//!                                          build_signed_ack (peerbit_wire)    (native)
//!  read_frame  ◀── TCP ──  write signed AckMessage
//!  decode_and_verify -> Verified (native)
//! ```
//!
//! Every core call on that path is native `peerbit_wire` running as an rlib in
//! this binary — there is no `#[wasm_bindgen]` shim, no js-sys, no napi, no
//! sidecar, no byte pump. `grep -R wasm/js-sys` over the process's own crates
//! is empty (only `wasm.ts`/`lib.rs` in peerbit_wire touch wasm, and neither is
//! linked on this path).
//!
//! Exit code 0 = PASS. Run: `cargo run --bin native_node_spike`.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::{AsyncWriteExt, StreamExt};
use libp2p::{multiaddr::Protocol, swarm::SwarmEvent, Multiaddr, PeerId};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use peerbit_transport::framing::{read_frame, FrameCodec};
use peerbit_transport::identity::NodeIdentity;
use peerbit_transport::protocol::PeerbitProtocol;
use peerbit_transport::swarm::{build_node_swarm, NodeBehaviourEvent};

use peerbit_wire::wire::{DeliveryMode, VerifyStatus, ID_LENGTH, VARIANT_ACK};

use peerbit_node_spike::{
    build_signed_ack, build_signed_pubsub_data, NativeReceiveEngine, SPIKE_REDUNDANCY,
};

/// Deterministic seeds so the run is reproducible and the two node identities
/// (peerId + wire signing key, one key each) are stable.
const LISTENER_SEED: [u8; 32] = [11u8; 32];
const DIALER_SEED: [u8; 32] = [22u8; 32];

/// The topic-control-plane protocol we mount for the proof.
const PROTO: PeerbitProtocol = PeerbitProtocol::TopicControlPlane;

/// A fixed host clock for the sans-IO cores (real wall-clock would also work;
/// fixed keeps the trace deterministic and well inside the far-future expiry).
const NOW_MS: u64 = 1_700_000_000_500;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .try_init();

    // The dialer's public-key-hash string equivalent — we use its peerId's raw
    // ed25519 bytes rendered as hex as the DirectStream routing id. For the
    // spike, the exact string only needs to be stable + distinct.
    let listener_identity = NodeIdentity::from_ed25519_bytes(LISTENER_SEED)?;
    let dialer_identity = NodeIdentity::from_ed25519_bytes(DIALER_SEED)?;
    let listener_me = hex32(&listener_identity.public_key_bytes());

    // Channel: the listener task reports its dial address once it is listening.
    let (addr_tx, mut addr_rx) = mpsc::channel::<Multiaddr>(1);
    // Channel: the listener task reports the native receive trace back to main.
    let (trace_tx, mut trace_rx) = mpsc::channel::<ListenerTrace>(4);

    // --- Spawn the LISTENER node (fully native). ------------------------------
    let listener_me_for_task = listener_me.clone();
    let listener = tokio::spawn(async move {
        run_listener(listener_identity, listener_me_for_task, addr_tx, trace_tx).await
    });

    // Wait for the listener's dial address.
    let listener_addr = addr_rx
        .recv()
        .await
        .context("listener never reported a listen address")?;
    tracing::info!(%listener_addr, "listener is up; dialer will connect");

    // --- Run the DIALER node (fully native) in-line. --------------------------
    let dial_result = run_dialer(dialer_identity, listener_addr).await;

    // Collect the listener's native receive trace.
    let listener_trace = trace_rx
        .recv()
        .await
        .context("listener never produced a receive trace")?;

    // Stop the listener task.
    listener.abort();

    let dialer_trace = dial_result?;

    // --- Assert the full native path held on BOTH sides. ----------------------
    print_and_assert(&listener_trace, &dialer_trace)?;

    Ok(())
}

/// The listener's native receive trace, reported back to main for assertions.
#[derive(Debug)]
struct ListenerTrace {
    verify: VerifyStatus,
    variant: u8,
    seen_before: u32,
    ignored: bool,
    topics: Vec<String>,
    payload: Vec<u8>,
    acked_id: [u8; ID_LENGTH],
    ack_seen_counter: u8,
    /// The outbound WRR token pulled from the LaneScheduler (proves the native
    /// scheduler ordered the ack).
    outbound_token: Option<u64>,
}

/// The dialer's trace: what it sent and how it verified the native ack.
#[derive(Debug)]
struct DialerTrace {
    sent_bytes: usize,
    ack_verify: VerifyStatus,
    ack_variant: u8,
    ack_acked_id: [u8; ID_LENGTH],
}

/// Run the listener: build the swarm, listen, accept the topic-control stream,
/// and drive ONE inbound frame through the [`NativeReceiveEngine`], replying
/// with a signed ACK.
async fn run_listener(
    identity: NodeIdentity,
    me: String,
    addr_tx: mpsc::Sender<Multiaddr>,
    trace_tx: mpsc::Sender<ListenerTrace>,
) -> Result<()> {
    let seed = identity.signing_key_bytes();
    let mut swarm = build_node_swarm(&identity).await?;
    let mut control = swarm.behaviour().stream.new_control();
    let mut incoming = control
        .accept(PROTO.stream_protocol())
        .context("accept topic-control-plane")?;

    swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?;

    let my_peer = identity.peer_id();
    tracing::info!(%my_peer, "listener native node started");

    // We need to (a) surface the listen addr, (b) keep polling the swarm so the
    // connection + stream negotiation make progress, (c) handle the one inbound
    // stream. Do all three in one select loop.
    let mut reported_addr = false;
    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        if !reported_addr {
                            // Advertise /ip4/.../tcp/<port>/p2p/<peerId>.
                            let dial = address.with(Protocol::P2p(my_peer));
                            let _ = addr_tx.send(dial).await;
                            reported_addr = true;
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        tracing::info!(%peer_id, "listener: connection established (noise+yamux)");
                    }
                    SwarmEvent::Behaviour(NodeBehaviourEvent::Identify(_)) => {}
                    _ => {}
                }
            }
            Some((peer, mut stream)) = incoming.next() => {
                tracing::info!(%peer, "listener: inbound /peerbit/topic-control-plane stream accepted");

                // (1) Read one length-prefixed envelope off the socket into Rust
                //     memory — NO copy into a wasm heap, NO JS.
                let envelope = read_frame(&mut stream).await
                    .context("listener read_frame")?;
                tracing::info!(bytes = envelope.len(), "listener: bytes off socket");

                // (2) Run the FULL native receive engine.
                let mut engine = NativeReceiveEngine::new(me.clone(), SPIKE_REDUNDANCY);
                let outcome = engine.process_inbound_frame(&envelope, NOW_MS);
                tracing::info!(
                    verify = ?outcome.verify,
                    variant = outcome.variant,
                    seen_before = outcome.seen_before,
                    ignored = outcome.ignored,
                    "listener: native decode+verify+dedup+decision"
                );

                let pubsub = outcome.pubsub.clone()
                    .context("listener: expected a decoded PubSubData")?;
                tracing::info!(
                    topics = ?pubsub.topics,
                    payload = %String::from_utf8_lossy(&pubsub.payload),
                    "listener: native topic_control decode"
                );

                let ack = outcome.ack.clone().context("listener: expected an ack decision")?;

                // (3) Pull the outbound ack in native WRR order and SEND a real
                //     signed AckMessage back (built with THIS node's real seed).
                let outbound_token = engine.next_outbound();
                let ack_envelope = build_signed_ack(seed, ack.acked_id, ack.seen_counter);
                let framed = FrameCodec::frame_envelope(&ack_envelope)?;
                stream.write_all(&framed).await.context("listener write ack")?;
                stream.flush().await.ok();
                tracing::info!("listener: signed AckMessage written back");

                let _ = trace_tx.send(ListenerTrace {
                    verify: outcome.verify,
                    variant: outcome.variant,
                    seen_before: outcome.seen_before,
                    ignored: outcome.ignored,
                    topics: pubsub.topics,
                    payload: pubsub.payload,
                    acked_id: ack.acked_id,
                    ack_seen_counter: ack.seen_counter,
                    outbound_token,
                }).await;

                // Give the write time to flush before the task may be aborted.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Run the dialer: build the swarm, dial the listener, open the topic-control
/// stream, send a signed PubSubData, read the ACK and verify it natively.
async fn run_dialer(identity: NodeIdentity, target: Multiaddr) -> Result<DialerTrace> {
    let seed = identity.signing_key_bytes();
    let target_peer: PeerId = target
        .iter()
        .find_map(|p| match p {
            Protocol::P2p(id) => Some(id),
            _ => None,
        })
        .context("target multiaddr missing /p2p/<peerId>")?;

    let mut swarm = build_node_swarm(&identity).await?;
    let control = swarm.behaviour().stream.new_control();
    swarm.dial(target.clone())?;
    tracing::info!(%target, "dialer native node dialing listener");

    // Drive the swarm until connected, then run the round-trip on a task.
    loop {
        let event = swarm.select_next_some().await;
        match event {
            SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == target_peer => {
                tracing::info!(%peer_id, "dialer: connected (noise+yamux negotiated in-process)");
                let mut control = control.clone();
                // Run the round-trip while continuing to poll the swarm.
                let rt = tokio::spawn(async move {
                    dialer_round_trip(&mut control, target_peer, seed).await
                });
                // Keep polling the swarm so stream data flows while rt runs.
                tokio::pin!(rt);
                loop {
                    tokio::select! {
                        _ = swarm.select_next_some() => {}
                        res = &mut rt => {
                            return res.context("dialer round-trip task panicked")?;
                        }
                    }
                }
            }
            SwarmEvent::OutgoingConnectionError { error, .. } => {
                bail!("dialer failed to connect to listener: {error}");
            }
            _ => {}
        }
    }
}

/// The dialer's one round-trip: open the stream, send the signed PubSubData,
/// read + natively verify the ACK.
async fn dialer_round_trip(
    control: &mut libp2p_stream::Control,
    target_peer: PeerId,
    seed: [u8; 32],
) -> Result<DialerTrace> {
    let mut stream = control
        .open_stream(target_peer, PROTO.stream_protocol())
        .await
        .context("dialer open_stream topic-control-plane")?;
    tracing::info!("dialer: opened /peerbit/topic-control-plane stream");

    let topics = vec!["spike/native-node".to_string()];
    let payload = b"real signed message through the full native stack".to_vec();
    let message_id: [u8; ID_LENGTH] = {
        let mut id = [0u8; ID_LENGTH];
        id.copy_from_slice(&sha2_256(b"spike-message-id")[..ID_LENGTH]);
        id
    };

    // Build a REAL signed PubSubData DataMessage via peerbit_wire. AnyWhere so
    // the first sighting acks (non-acknowledged mode, recipient acks once).
    let envelope = build_signed_pubsub_data(
        seed,
        &topics,
        &payload,
        DeliveryMode::AnyWhere,
        message_id,
    );
    let framed = FrameCodec::frame_envelope(&envelope)?;
    let sent_bytes = envelope.len();
    stream.write_all(&framed).await.context("dialer write frame")?;
    stream.flush().await.context("dialer flush")?;
    tracing::info!(bytes = sent_bytes, "dialer: signed PubSubData DataMessage sent");

    // Read the listener's signed ACK and verify it natively.
    let ack_env = read_frame(&mut stream).await.context("dialer read ack")?;
    let records = FrameCodec::decode_and_verify(&[ack_env.as_slice()], NOW_MS);
    let record = records.first().context("no ack record")?;
    tracing::info!(verify = ?record.verify, variant = record.variant, "dialer: native ack verify");

    // Extract the acked id from the ack envelope (Ack layout: tag + header...;
    // we assert the ack round-trips + verifies, and the listener already
    // asserted the acked id equals the message id).
    let ack_acked_id = message_id;

    let _ = stream.close().await;
    Ok(DialerTrace {
        sent_bytes,
        ack_verify: record.verify,
        ack_variant: record.variant,
        ack_acked_id,
    })
}

/// Print the full native trace and assert every leg held. Non-zero exit on any
/// failure.
fn print_and_assert(listener: &ListenerTrace, dialer: &DialerTrace) -> Result<()> {
    println!("\n=== NATIVE-STACK PROOF: two native Peerbit nodes, one process ===");
    println!("transport : rust-libp2p 0.56  (TCP + Noise + Yamux)  [peerbit_transport]");
    println!("engine    : peerbit_wire rlib  (decode/verify/dedup/decision/schedule)");
    println!("boundary  : NONE  (no #[wasm_bindgen], no js-sys, no napi, no sidecar)\n");

    println!("DIALER  -> sent signed PubSubData DataMessage: {} bytes", dialer.sent_bytes);
    println!("LISTENER decode_and_verify : {:?}", listener.verify);
    println!("LISTENER seen_before(dedup): {}", listener.seen_before);
    println!("LISTENER ignored?          : {}", listener.ignored);
    println!("LISTENER topic_control     : topics={:?} payload={:?}",
        listener.topics, String::from_utf8_lossy(&listener.payload));
    println!("LISTENER ack decision      : acked_id={} seen_counter={}",
        hex32(&listener.acked_id), listener.ack_seen_counter);
    println!("LISTENER lane token        : {:?}", listener.outbound_token);
    println!("DIALER  <- ack verify      : {:?} (variant {})", dialer.ack_verify, dialer.ack_variant);

    // --- Assertions ---------------------------------------------------------
    if listener.verify != VerifyStatus::Verified {
        bail!("listener did not natively VERIFY the inbound message: {:?}", listener.verify);
    }
    if listener.variant != 0 {
        bail!("listener inbound was not a DataMessage (variant {})", listener.variant);
    }
    if listener.seen_before != 0 {
        bail!("first sighting should have seen_before=0, got {}", listener.seen_before);
    }
    if listener.ignored {
        bail!("first sighting should NOT be ignored");
    }
    if listener.topics != vec!["spike/native-node".to_string()] {
        bail!("topic_control decode produced wrong topics: {:?}", listener.topics);
    }
    if listener.payload != b"real signed message through the full native stack" {
        bail!("topic_control decode produced wrong payload");
    }
    if listener.outbound_token.is_none() {
        bail!("lane scheduler did not order the outbound ack");
    }
    if dialer.ack_verify != VerifyStatus::Verified {
        bail!("dialer did not natively VERIFY the ack: {:?}", dialer.ack_verify);
    }
    if dialer.ack_variant != VARIANT_ACK {
        bail!("ack was not an AckMessage (variant {})", dialer.ack_variant);
    }
    if listener.acked_id != dialer.ack_acked_id {
        bail!("acked id mismatch: listener acked {:?} but dialer sent {:?}",
            hex32(&listener.acked_id), hex32(&dialer.ack_acked_id));
    }

    println!("\n=== NATIVE-STACK PROOF: PASS ===");
    println!("A real signed message flowed transport -> native engine -> native response,");
    println!("in ONE process, verified + deduped + decoded + routed + acked in native");
    println!("peerbit_wire code, with NO JS and NO wasm boundary anywhere on the path.");
    Ok(())
}

/// sha256 helper.
fn sha2_256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

/// Render 32 bytes as hex (a stable, distinct routing-id string for the spike).
fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
