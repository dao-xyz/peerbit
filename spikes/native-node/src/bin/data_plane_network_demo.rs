//! DATA-PLANE SLICE OVER THE REAL TRANSPORT (two native nodes, one process).
//!
//! The STRETCH goal: an entry APPENDED on node A flows through the native
//! network engine to node B and LANDS in B's native log + index — end to end,
//! no JS, no wasm.
//!
//! ```text
//!  NODE A (dialer, native)                    NODE B (listener, native)
//!  build_signed_entry_v0_storage  ── the entry A "appends"
//!  build_sync_payload (RawExchangeHeads PubSubData)
//!  build_signed_pubsub_data       ── wrap as a signed topic-control DataMessage
//!    whose inner PubSubData.data = the sync payload
//!  FrameCodec::frame_envelope  ── TCP+Noise+Yamux ──▶  read_frame
//!                                     NativeReceiveEngine.process_inbound_frame
//!                                       decode_and_verify -> Verified (envelope)
//!                                       decode_pubsub_message -> outcome.pubsub
//!                                     [DATA-PLANE SLICE]
//!                                     recognize_and_commit(outcome.pubsub.payload):
//!                                       sync_payload::parse_pubsub_data
//!                                       + parse_raw_exchange_rpc_request
//!                                       prepare_raw_entry_v0_..._verify (CID+Ed25519)
//!                                       NativeLogBlockStore.put / LogGraphIndex.put
//!                                       NativeQueryIndex.put
//!                                     ASSERT B's index query returns A's entry
//! ```
//!
//! Two independent Ed25519 verifications happen natively on B: the transport
//! DataMessage envelope (author = node A's transport key) and the inner EntryV0
//! author signature (author = the entry's signing key). Both are native
//! `peerbit_wire` / `peerbit_log_rust`.
//!
//! Exit code 0 = PASS. Run: `cargo run --bin data_plane_network_demo`.

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

use peerbit_wire::wire::{DeliveryMode, VerifyStatus, ID_LENGTH};

use peerbit_node_spike::data_plane::{
    build_signed_entry_v0_storage, build_sync_payload, cid_of_storage, FixtureEntry,
    NativeDataPlane,
};
use peerbit_node_spike::{build_signed_pubsub_data, NativeReceiveEngine, SPIKE_REDUNDANCY};

const LISTENER_SEED: [u8; 32] = [11u8; 32];
const DIALER_SEED: [u8; 32] = [22u8; 32];
const ENTRY_AUTHOR_SEED: [u8; 32] = [77u8; 32];
const PROTO: PeerbitProtocol = PeerbitProtocol::TopicControlPlane;
const NOW_MS: u64 = 1_700_000_000_500;

/// The gid + topic the entry is appended under (asserted on B's side).
const GID: &str = "networked-log-gid";
const TOPIC: &str = "spike/data-plane";

/// What node B committed, reported back to main for assertions.
#[derive(Debug)]
struct ListenerCommit {
    envelope_verify: VerifyStatus,
    committed_cids: Vec<String>,
    heads_for_gid: Vec<String>,
    index_hits_for_gid: Vec<String>,
    has_block: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .try_init();

    let listener_identity = NodeIdentity::from_ed25519_bytes(LISTENER_SEED)?;
    let dialer_identity = NodeIdentity::from_ed25519_bytes(DIALER_SEED)?;
    let listener_me = hex32(&listener_identity.public_key_bytes());

    // Node A builds the entry it "appends" and the sync payload that carries it.
    let (entry_cid, sync_payload) = build_entry_and_sync_payload()?;
    let expected_cid = entry_cid.clone();

    let (addr_tx, mut addr_rx) = mpsc::channel::<Multiaddr>(1);
    let (commit_tx, mut commit_rx) = mpsc::channel::<ListenerCommit>(1);

    let listener_me_for_task = listener_me.clone();
    let listener = tokio::spawn(async move {
        run_listener(listener_identity, listener_me_for_task, addr_tx, commit_tx).await
    });

    let listener_addr = addr_rx
        .recv()
        .await
        .context("listener never reported a listen address")?;
    tracing::info!(%listener_addr, "node B is up; node A will connect");

    run_dialer(dialer_identity, listener_addr, sync_payload).await?;

    let commit = commit_rx
        .recv()
        .await
        .context("node B never reported a commit")?;
    listener.abort();

    print_and_assert(&commit, &expected_cid)
}

/// Node A: build a real signed EntryV0, wrap it as a RawExchangeHeads PubSubData
/// sync payload (the inner data of the topic-control DataMessage).
fn build_entry_and_sync_payload() -> Result<(String, Vec<u8>)> {
    let next: Vec<String> = Vec::new();
    let fixture = FixtureEntry {
        seed: ENTRY_AUTHOR_SEED,
        clock_id: b"networked-clock-id",
        wall_time: 1_700_000_000_000,
        logical: 0,
        gid: GID,
        next: &next,
        entry_type: 0,
        meta_data: None,
        payload_data: b"an entry appended on node A, committed natively on node B",
    };
    let storage = build_signed_entry_v0_storage(&fixture);
    let cid = cid_of_storage(&storage)?;
    let sync_payload = build_sync_payload(
        &[TOPIC.to_string()],
        &[(cid.clone(), storage, vec![GID.to_string()])],
    );
    Ok((cid, sync_payload))
}

/// Node B: accept the topic-control stream, run the receive engine to get the
/// verified pubsub payload, then run the data-plane slice and report what landed.
async fn run_listener(
    identity: NodeIdentity,
    me: String,
    addr_tx: mpsc::Sender<Multiaddr>,
    commit_tx: mpsc::Sender<ListenerCommit>,
) -> Result<()> {
    let mut swarm = build_node_swarm(&identity).await?;
    let mut control = swarm.behaviour().stream.new_control();
    let mut incoming = control
        .accept(PROTO.stream_protocol())
        .context("accept topic-control-plane")?;

    swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?;
    let my_peer = identity.peer_id();
    tracing::info!(%my_peer, "node B native node started");

    let mut reported_addr = false;
    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        if !reported_addr {
                            let dial = address.with(Protocol::P2p(my_peer));
                            let _ = addr_tx.send(dial).await;
                            reported_addr = true;
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        tracing::info!(%peer_id, "node B: connection established (noise+yamux)");
                    }
                    SwarmEvent::Behaviour(NodeBehaviourEvent::Identify(_)) => {}
                    _ => {}
                }
            }
            Some((peer, mut stream)) = incoming.next() => {
                tracing::info!(%peer, "node B: inbound /peerbit/topic-control-plane stream accepted");

                // Bytes off the socket into Rust memory (no wasm heap copy).
                let envelope = read_frame(&mut stream).await.context("node B read_frame")?;
                tracing::info!(bytes = envelope.len(), "node B: bytes off socket");

                // Native network engine: decode+verify envelope, dedup, decode
                // the topic-control PubSubData -> outcome.pubsub.
                let mut engine = NativeReceiveEngine::new(me.clone(), SPIKE_REDUNDANCY);
                let outcome = engine.process_inbound_frame(&envelope, NOW_MS);
                let pubsub = outcome.pubsub.clone()
                    .context("node B: expected a decoded PubSubData")?;
                tracing::info!(
                    verify = ?outcome.verify,
                    topics = ?pubsub.topics,
                    payload_bytes = pubsub.payload.len(),
                    "node B: native engine yielded outcome.pubsub"
                );

                // [DATA-PLANE SLICE] recognize + ingest + verify + commit the
                // entry carried in the verified payload, natively.
                let mut plane = NativeDataPlane::new();
                let committed = plane.recognize_and_commit(&pubsub.payload)
                    .context("node B: recognize_and_commit")?;
                let committed_cids: Vec<String> =
                    committed.iter().map(|entry| entry.cid.clone()).collect();
                tracing::info!(?committed_cids, "node B: committed to native log + index");

                let heads_for_gid = plane.heads(Some(GID));
                let index_hits_for_gid = plane.cids_for_gid(GID);
                let has_block = committed_cids
                    .first()
                    .map(|cid| plane.has_block(cid))
                    .unwrap_or(false);

                let _ = commit_tx.send(ListenerCommit {
                    envelope_verify: outcome.verify,
                    committed_cids,
                    heads_for_gid,
                    index_hits_for_gid,
                    has_block,
                }).await;

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Node A: dial node B, open the topic-control stream, send the signed
/// DataMessage whose inner PubSubData carries the sync payload.
async fn run_dialer(
    identity: NodeIdentity,
    target: Multiaddr,
    sync_payload: Vec<u8>,
) -> Result<()> {
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
    tracing::info!(%target, "node A native node dialing node B");

    loop {
        let event = swarm.select_next_some().await;
        match event {
            SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == target_peer => {
                tracing::info!(%peer_id, "node A: connected (noise+yamux negotiated in-process)");
                let mut control = control.clone();
                let payload = sync_payload.clone();
                let rt = tokio::spawn(async move {
                    dialer_send(&mut control, target_peer, seed, payload).await
                });
                tokio::pin!(rt);
                loop {
                    tokio::select! {
                        _ = swarm.select_next_some() => {}
                        res = &mut rt => {
                            return res.context("node A send task panicked")?;
                        }
                    }
                }
            }
            SwarmEvent::OutgoingConnectionError { error, .. } => {
                bail!("node A failed to connect to node B: {error}");
            }
            _ => {}
        }
    }
}

/// Node A's send: wrap the sync payload as a signed topic-control DataMessage
/// (its inner PubSubData.data = the sync payload) and write it framed.
async fn dialer_send(
    control: &mut libp2p_stream::Control,
    target_peer: PeerId,
    seed: [u8; 32],
    sync_payload: Vec<u8>,
) -> Result<()> {
    let mut stream = control
        .open_stream(target_peer, PROTO.stream_protocol())
        .await
        .context("node A open_stream topic-control-plane")?;
    tracing::info!("node A: opened /peerbit/topic-control-plane stream");

    let topics = vec![TOPIC.to_string()];
    let message_id: [u8; ID_LENGTH] = {
        let mut id = [0u8; ID_LENGTH];
        id.copy_from_slice(&sha2_256(b"data-plane-network-message-id")[..ID_LENGTH]);
        id
    };

    // build_signed_pubsub_data wraps `sync_payload` as the DataMessage's inner
    // topic-control PubSubData.data (encode_pubsub_data(topics, false, payload)).
    // On B, the engine strips that outer PubSubData layer and hands the sync
    // payload back as outcome.pubsub.payload — exactly what recognize_and_commit
    // expects (it re-parses the inner RawExchangeHeads PubSubData nesting).
    let envelope = build_signed_pubsub_data(
        seed,
        &topics,
        &sync_payload,
        DeliveryMode::AnyWhere,
        message_id,
    );
    let framed = FrameCodec::frame_envelope(&envelope)?;
    stream
        .write_all(&framed)
        .await
        .context("node A write frame")?;
    stream.flush().await.context("node A flush")?;
    tracing::info!(
        bytes = envelope.len(),
        "node A: signed DataMessage (carrying the entry) sent"
    );

    // Give B time to read+commit before the connection may drop.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = stream.close().await;
    Ok(())
}

fn print_and_assert(commit: &ListenerCommit, expected_cid: &str) -> Result<()> {
    println!("\n=== DATA-PLANE SLICE OVER TRANSPORT: append on A -> commit on B ===");
    println!("transport : rust-libp2p 0.56  (TCP + Noise + Yamux)  [peerbit_transport]");
    println!("engine    : peerbit_wire rlib  (decode/verify/dedup/topic-control)");
    println!("data plane: peerbit_log_rust + peerbit_indexer_core  (append + commit)");
    println!("boundary  : NONE  (no #[wasm_bindgen], no js-sys, no napi, no sidecar)\n");

    println!("NODE B envelope verify   : {:?}", commit.envelope_verify);
    println!("NODE B committed cids     : {:?}", commit.committed_cids);
    println!("NODE B heads(gid={GID})   : {:?}", commit.heads_for_gid);
    println!(
        "NODE B index(gid={GID})   : {:?}",
        commit.index_hits_for_gid
    );
    println!("NODE B block store has cid: {}", commit.has_block);

    if commit.envelope_verify != VerifyStatus::Verified {
        bail!(
            "node B did not natively verify the transport envelope: {:?}",
            commit.envelope_verify
        );
    }
    if commit.committed_cids != vec![expected_cid.to_string()] {
        bail!(
            "node B committed {:?}, expected [{}]",
            commit.committed_cids,
            expected_cid
        );
    }
    if !commit.heads_for_gid.contains(&expected_cid.to_string()) {
        bail!("node B graph heads for {GID} do not contain the entry");
    }
    if commit.index_hits_for_gid != vec![expected_cid.to_string()] {
        bail!(
            "node B index query for {GID} returned {:?}, expected [{}]",
            commit.index_hits_for_gid,
            expected_cid
        );
    }
    if !commit.has_block {
        bail!("node B block store does not hold the entry bytes");
    }

    println!("\n=== DATA-PLANE SLICE OVER TRANSPORT: PASS ===");
    println!("An entry appended on node A flowed over TCP+Noise+Yamux, through node B's");
    println!("native receive engine, into node B's native log (block store + graph) and");
    println!("native document index — a native index query on B returns A's entry. Two");
    println!("native nodes, one process, no JS and no wasm boundary on the path.");
    Ok(())
}

fn sha2_256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
