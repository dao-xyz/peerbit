//! GOAL 2 (stretch — the real interop question): a rust-libp2p peer dials a
//! js-libp2p NODE peer configured like a Peerbit node (TCP + Noise + Yamux,
//! same protocol id) and opens a stream on it.
//!
//! Run the js listener first (see js/listener.mjs), which prints a line:
//!     DIAL_ME=/ip4/127.0.0.1/tcp/<port>/p2p/<peerId>
//! Pass that multiaddr as argv[1] to this binary:
//!     cargo run --bin goal2_dial_js -- /ip4/127.0.0.1/tcp/PORT/p2p/PEERID
//!
//! This exercises the true cross-implementation path:
//!   rust noise/yamux/multistream-select  <->  js noise/yamux/multistream-select
//! which the libp2p test-plans continuously cross-test as known-good (INTEROP notes).

use std::time::Duration;

use anyhow::{Context, Result};
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::{
    identify, identity, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, StreamProtocol, Swarm,
};
use libp2p_stream as stream;
use tracing_subscriber::EnvFilter;

/// Must byte-match the protocol the js listener registers.
const PEERBIT_PROTOCOL: StreamProtocol = StreamProtocol::new("/peerbit/direct-stream/2.0.0");
const DATA_MESSAGE_TAG: u8 = 0;

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    stream: stream::Behaviour,
}

fn build_swarm(keypair: identity::Keypair) -> Result<Swarm<Behaviour>> {
    Ok(libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| Behaviour {
            identify: identify::Behaviour::new(identify::Config::new(
                "/ipfs/id/1.0.0".to_string(),
                key.public(),
            )),
            stream: stream::Behaviour::new(),
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(15)))
        .build())
}

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

async fn read_frame<S>(stream: &mut S) -> Result<Vec<u8>>
where
    S: AsyncReadExt + Unpin,
{
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

    let target: Multiaddr = std::env::args()
        .nth(1)
        .context("usage: goal2_dial_js <multiaddr-of-js-node>")?
        .parse()
        .context("argv[1] is not a valid multiaddr")?;

    // Extract the js peer id from the /p2p/... component.
    let target_peer: PeerId = target
        .iter()
        .find_map(|p| match p {
            libp2p::multiaddr::Protocol::P2p(id) => Some(id),
            _ => None,
        })
        .context("multiaddr must include /p2p/<peerId>")?;

    let kp = identity::Keypair::generate_ed25519();
    let me = PeerId::from(kp.public());
    let mut swarm = build_swarm(kp)?;
    tracing::info!(%me, %target, %target_peer, "rust peer dialing js-libp2p node");

    swarm.dial(target.clone())?;
    let control = swarm.behaviour().stream.new_control();

    // Drive the swarm; when connected to the js node, open the stream + exchange a frame.
    let mut opened = false;
    loop {
        let ev = swarm.select_next_some().await;
        match ev {
            SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == target_peer => {
                tracing::info!(%peer_id, "connected to js node (noise+yamux negotiated)");
                if opened {
                    continue;
                }
                opened = true;
                let _ = opened; // silence unused-assignment; guards re-entry above
                let mut ctrl = control.clone();
                // Spawn the app logic so the swarm keeps polling connection I/O.
                let handle = tokio::spawn(async move {
                    let mut stream = ctrl
                        .open_stream(target_peer, PEERBIT_PROTOCOL)
                        .await
                        .expect("open_stream to js node");
                    tracing::info!(protocol=%PEERBIT_PROTOCOL, "opened Peerbit stream on js node");

                    stream
                        .write_all(&frame(b"hello-from-rust"))
                        .await
                        .expect("write frame");
                    stream.flush().await.expect("flush");

                    let body = read_frame(&mut stream).await.expect("read js reply");
                    let reply = String::from_utf8_lossy(&body[1..]).to_string();
                    tracing::info!(%reply, "received reply from js node");
                    reply
                });

                let reply = handle.await?;
                println!("\n=== GOAL 2 PASS ===");
                println!("rust-libp2p peer <-> js-libp2p (Peerbit node config):");
                println!("  - TCP + Noise + Yamux negotiated cross-implementation");
                println!("  - opened {PEERBIT_PROTOCOL} on the js node");
                println!("  - js reply: {reply}");
                return Ok(());
            }
            SwarmEvent::OutgoingConnectionError { error, .. } => {
                anyhow::bail!("failed to connect to js node: {error}");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                info,
                ..
            })) => {
                tracing::info!(agent=%info.agent_version, protocols=?info.protocols, "identify from js node");
            }
            _ => {}
        }
    }
}
