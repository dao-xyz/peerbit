//! `peerbit_node_spike` — the native-node engine wiring, sans I/O.
//!
//! This is the piece the feasibility spike is really about: a native event loop
//! that binds the Phase-1 rust-libp2p transport DIRECTLY to the JsValue-free
//! engine cores. Everything here is pure (no sockets, no async, no wasm, no JS):
//! it takes bytes that came off a `/peerbit/*` stream into Rust memory and runs
//! them through the exact same cores that ship as wasm today —
//!
//!   * [`peerbit_wire::wire::decode_and_verify_frames`] — Borsh envelope decode
//!     + Ed25519/SHA-256 batch verify, on BORROWED socket slices (no copy);
//!   * [`peerbit_wire::direct_stream::seen_cache::SeenCache`] — dedup counter;
//!   * [`peerbit_wire::topic_control::decode_pubsub_message`] — the
//!     `/peerbit/topic-control-plane/2.0.0` payload codec;
//!   * [`peerbit_wire::direct_stream::decisions`] — the relay/ack decisions;
//!   * [`peerbit_wire::direct_stream::lanes::LaneScheduler`] — outbound ordering.
//!
//! The [`bin/native_node_spike`](../native_node_spike/index.html) drives this
//! over two real `peerbit_transport` swarms; the unit tests below drive it with
//! hand-built frames so "the engine runs and routes natively" is provable with
//! `cargo test` alone, no network.

use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};

use peerbit_wire::direct_stream::decisions;
use peerbit_wire::direct_stream::lanes::LaneScheduler;
use peerbit_wire::direct_stream::seen_cache::{SeenCache, KEY_KIND_MESSAGE_ID};
use peerbit_wire::topic_control::{decode_pubsub_message, encode_pubsub_data, DecodedPubSubMessage};
use peerbit_wire::wire::{
    encode_frame, encode_signable, DeliveryMode, FrameRecord, MessageHeader, PublicSignKey,
    SignatureWithKey, VerifyStatus, WireMessage, ID_LENGTH, PREHASH_SHA_256, VARIANT_ACK,
    VARIANT_DATA,
};

/// Far-future expiry so `MessageHeader.verify()` (wall-clock `now`) always
/// passes in a spike run and matches the interop bin's choice.
pub const FAR_FUTURE_EXPIRES_MS: u64 = 4_102_444_800_000;

/// DirectStream redundancy used by the ack decision. Peerbit's default is 2;
/// the spike uses 1 so a single sighting of a recipient message acks.
pub const SPIKE_REDUNDANCY: u8 = 1;

/// Build a signed **PubSubData `DataMessage`** envelope with the frozen
/// `peerbit_wire` codec.
///
/// The `DataMessage.data` payload is itself a `/peerbit/topic-control-plane`
/// `PubSubData` frame (`encode_pubsub_data`) carrying `topics` + `payload` —
/// i.e. exactly the nested shape a real TopicControlPlane message has on the
/// wire (`PubSubData` inside a DirectStream `DataMessage`). Signed Ed25519 over
/// SHA-256(signable) — the `@peerbit/crypto` direct-stream scheme, so
/// [`peerbit_wire::wire::decode_and_verify_frames`] returns
/// [`VerifyStatus::Verified`].
///
/// `mode`/`hops` let the caller choose the DeliveryMode so the ack/relay
/// decision path can be exercised (AnyWhere vs AcknowledgeAnyWhere).
pub fn build_signed_pubsub_data(
    seed: [u8; 32],
    topics: &[String],
    payload: &[u8],
    mode: DeliveryMode,
    message_id: [u8; ID_LENGTH],
) -> Vec<u8> {
    let signing_key = SigningKey::from_bytes(&seed);
    let public = signing_key.verifying_key().to_bytes();

    // Nested TopicControlPlane PubSubData frame -> becomes the DataMessage body.
    let inner = encode_pubsub_data(topics, /* strict */ false, payload);

    let mut message = WireMessage::Data {
        header: MessageHeader {
            id: message_id,
            timestamp: 1_700_000_000_000,
            session: 1_690_000_000_000,
            expires: FAR_FUTURE_EXPIRES_MS,
            priority: Some(0),
            response_priority: None,
            origin: None,
            mode: Some(mode),
            signatures: Some(Vec::new()),
        },
        data: Some(inner),
    };

    let signable = encode_signable(&message);
    let digest: [u8; 32] = Sha256::digest(&signable).into();
    let signature = signing_key.sign(&digest);
    message.header_mut().signatures = Some(vec![SignatureWithKey {
        signature: signature.to_bytes().to_vec(),
        public_key: PublicSignKey::Ed25519(public),
        prehash: PREHASH_SHA_256,
    }]);

    let envelope = encode_frame(&message);
    debug_assert_eq!(envelope[0], VARIANT_DATA, "first byte is the DataMessage tag");
    envelope
}

/// Build a signed **`AckMessage`** envelope acknowledging `acked_id`, with
/// `seen_counter`. This is the outbound reply a native node emits when the
/// receive path decides an ACK is due — built with the same frozen codec, so a
/// peer (native OR js) verifies it identically.
pub fn build_signed_ack(seed: [u8; 32], acked_id: [u8; ID_LENGTH], seen_counter: u8) -> Vec<u8> {
    let signing_key = SigningKey::from_bytes(&seed);
    let public = signing_key.verifying_key().to_bytes();

    // The ACK id: sha256("ack" || acked_id), just to be a distinct message id.
    let mut hasher = Sha256::new();
    hasher.update(b"ack");
    hasher.update(acked_id);
    let ack_id: [u8; 32] = hasher.finalize().into();

    let mut message = WireMessage::Ack {
        header: MessageHeader {
            id: ack_id,
            timestamp: 1_700_000_000_001,
            session: 1_690_000_000_000,
            expires: FAR_FUTURE_EXPIRES_MS,
            priority: Some(1),
            response_priority: None,
            origin: None,
            // Acks are traced back along the delivery path; AnyWhere is fine for
            // the point-to-point spike (no multi-hop relay in the base proof).
            mode: Some(DeliveryMode::AnyWhere),
            signatures: Some(Vec::new()),
        },
        message_id_to_acknowledge: acked_id,
        seen_counter,
    };

    let signable = encode_signable(&message);
    let digest: [u8; 32] = Sha256::digest(&signable).into();
    let signature = signing_key.sign(&digest);
    message.header_mut().signatures = Some(vec![SignatureWithKey {
        signature: signature.to_bytes().to_vec(),
        public_key: PublicSignKey::Ed25519(public),
        prehash: PREHASH_SHA_256,
    }]);

    let envelope = encode_frame(&message);
    debug_assert_eq!(envelope[0], VARIANT_ACK, "first byte is the AckMessage tag");
    envelope
}

/// What the native receive path decided about one inbound frame. This is the
/// structured trace the spike asserts on: it proves the message was verified,
/// deduped, decoded and routed ENTIRELY in native code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundOutcome {
    /// The `peerbit_wire` frame record (variant, verify status, payload range).
    pub verify: VerifyStatus,
    pub variant: u8,
    /// How many times this message id was seen BEFORE this frame (dedup).
    pub seen_before: u32,
    /// The dedup decision: did we drop it as a duplicate / self-echo?
    pub ignored: bool,
    /// If it was a TopicControlPlane PubSubData, the decoded topics + payload.
    pub pubsub: Option<PubSubDataDecoded>,
    /// If an ACK is due, the id to acknowledge and the seen counter to send.
    pub ack: Option<AckDecision>,
}

/// The decoded inner PubSubData carried by a DataMessage on the topic-control
/// plane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PubSubDataDecoded {
    pub topics: Vec<String>,
    pub payload: Vec<u8>,
}

/// The ack the receive path decided to send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckDecision {
    pub acked_id: [u8; ID_LENGTH],
    pub seen_counter: u8,
}

/// The native receive engine: everything a `/peerbit/topic-control-plane` frame
/// goes through AFTER it lands in Rust memory, with ZERO JS and ZERO wasm.
///
/// It owns the per-peer state the DirectStream engine keeps — here just the
/// dedup [`SeenCache`] and the outbound [`LaneScheduler`]. `me` is our
/// public-key-hash (the routing identity); `redundancy` the ack threshold.
pub struct NativeReceiveEngine {
    me: String,
    redundancy: u8,
    seen: SeenCache,
    scheduler: LaneScheduler,
}

impl NativeReceiveEngine {
    /// `me` is this node's public-key-hash string (the DirectStream routing id).
    pub fn new(me: String, redundancy: u8) -> Self {
        NativeReceiveEngine {
            me,
            redundancy,
            // Same shape as the TS DirectStream seen-cache (FIFO, TTL).
            seen: SeenCache::new(10_000, 30_000),
            // 4-lane WRR scheduler, no byte cap in the spike.
            scheduler: LaneScheduler::new(4, None, None),
        }
    }

    /// Process ONE inbound envelope (a socket-borrowed slice) through the full
    /// native stack. Returns the structured outcome. `now_ms` is the host clock
    /// the sans-IO cores require.
    ///
    /// Path: `decode_and_verify_frames` (native codec + Ed25519 verify)
    ///   -> `SeenCache.modify` (native dedup counter)
    ///   -> `decisions::should_ignore_data` (native relay decision)
    ///   -> `topic_control::decode_pubsub_message` (native plane codec)
    ///   -> `decisions::should_acknowledge` (native ack decision)
    ///   -> `LaneScheduler.push` (native outbound ordering token).
    pub fn process_inbound_frame(&mut self, envelope: &[u8], now_ms: u64) -> InboundOutcome {
        // 1) Native decode + Ed25519/SHA-256 verify on the borrowed slice.
        let records: Vec<FrameRecord> =
            peerbit_wire::wire::decode_and_verify_frames(&[envelope], now_ms);
        let record = records
            .into_iter()
            .next()
            .expect("decode_and_verify_frames returns one record per frame");

        // 2) Native dedup: how many times have we seen this message id before?
        let seen_before = self.seen.modify(envelope, KEY_KIND_MESSAGE_ID, now_ms);

        // 3) Native relay/ignore decision. AnyWhere (variant 4) is NOT an
        //    acknowledged mode; AcknowledgeAnyWhere (5)/Acknowledge (1) are. We
        //    read the mode straight off the decoded header. `signed_by_self` is
        //    false here (the message is signed by the remote peer).
        let (acknowledged_mode, hops) = delivery_mode_shape(envelope);
        let ignored = decisions::should_ignore_data(
            seen_before,
            acknowledged_mode,
            self.redundancy,
            &hops,
            &self.me,
            /* signed_by_self */ false,
        );

        // 4) If it's a DataMessage with a payload, decode the nested
        //    TopicControlPlane PubSubData with the native plane codec.
        let mut pubsub = None;
        if record.variant == VARIANT_DATA && record.has_data && !ignored {
            let data = &envelope[record.data_offset as usize
                ..record.data_offset as usize + record.data_length as usize];
            if let Ok(DecodedPubSubMessage::Data {
                topics,
                data_offset,
                data_length,
                ..
            }) = decode_pubsub_message(data)
            {
                let payload = data[data_offset..data_offset + data_length].to_vec();
                pubsub = Some(PubSubDataDecoded { topics, payload });
            }
        }

        // 5) Native ack decision. We are a recipient of a PubSubData we could
        //    decode; ack if under the redundancy threshold and not ignored.
        let mut ack = None;
        if !ignored && record.variant == VARIANT_DATA && pubsub.is_some() {
            let is_recipient = true;
            if decisions::should_acknowledge(is_recipient, seen_before, self.redundancy) {
                let acked_id = message_id_of(envelope);
                let seen_counter = seen_before.min(u8::MAX as u32) as u8;
                // Order the outbound ack through the native lane scheduler
                // (lane 0 = control/ack priority). The token proves the
                // scheduler accepted it; the bin drains shift() to send.
                let ack_envelope = build_signed_ack(ack_signing_seed_placeholder(), acked_id, seen_counter);
                let _ = self.scheduler.push(0, ack_envelope.len() as u64);
                ack = Some(AckDecision {
                    acked_id,
                    seen_counter,
                });
            }
        }

        InboundOutcome {
            verify: record.verify,
            variant: record.variant,
            seen_before,
            ignored,
            pubsub,
            ack,
        }
    }

    /// Drain the next lane-scheduled outbound token, if any. The bin uses this
    /// to pull queued acks in WRR order.
    pub fn next_outbound(&mut self) -> Option<u64> {
        self.scheduler.shift()
    }

    /// The dedup cache size / scheduler state, for assertions.
    pub fn scheduler_total_bytes(&self) -> u64 {
        self.scheduler.total_bytes()
    }
}

/// The DirectStream message id = the decoded `header.id`. Read it from the
/// codec, not by hand-indexing: the on-wire layout is `[variant tag][header
/// variant byte][32-byte id]...`, so the id starts at raw offset 2, and letting
/// `decode_frame` do it keeps this correct if the header framing ever shifts.
/// This mirrors `getMsgId` (which keys off the header id).
fn message_id_of(envelope: &[u8]) -> [u8; ID_LENGTH] {
    match peerbit_wire::wire::decode_frame(envelope) {
        Ok(decoded) => decoded.message.header().id,
        // A frame that failed to decode never reaches the ack path, but return
        // a zeroed id defensively rather than panicking.
        Err(_) => [0u8; ID_LENGTH],
    }
}

/// Read (acknowledged_mode, hops) from an envelope's decoded delivery mode.
/// Only the shape needed by `should_ignore_data` — whether the mode is an
/// acknowledged variant, and the hops list if any.
fn delivery_mode_shape(envelope: &[u8]) -> (bool, Vec<String>) {
    match peerbit_wire::wire::decode_frame(envelope) {
        Ok(decoded) => match decoded.message.header().mode.clone() {
            Some(DeliveryMode::Acknowledge { hops, .. }) => (true, hops),
            Some(DeliveryMode::AcknowledgeAnyWhere { hops, .. }) => (true, hops),
            _ => (false, Vec::new()),
        },
        Err(_) => (false, Vec::new()),
    }
}

/// The ack is signed by THIS node's key in the bin (which threads the real
/// seed). Inside the pure engine we don't hold the seed, so the queued-ack
/// length used for scheduler accounting is built with a fixed placeholder seed
/// of the same length (ack envelope length is seed-independent). The bin
/// rebuilds+signs the ack with the node's real seed before sending.
fn ack_signing_seed_placeholder() -> [u8; 32] {
    [0u8; 32]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn me_hash() -> String {
        "0000recipient".to_string()
    }

    fn topic() -> Vec<String> {
        vec!["spike/topic".to_string()]
    }

    #[test]
    fn signed_pubsub_data_verifies_natively() {
        // Build a signed PubSubData DataMessage and run it through the native
        // decode+verify — it must come back Verified with the DataMessage tag.
        let seed = [9u8; 32];
        let env = build_signed_pubsub_data(
            seed,
            &topic(),
            b"hello native plane",
            DeliveryMode::AnyWhere,
            [1u8; 32],
        );
        let records = peerbit_wire::wire::decode_and_verify_frames(&[env.as_slice()], 1_700_000_000_500);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].verify, VerifyStatus::Verified);
        assert_eq!(records[0].variant, VARIANT_DATA);
        assert!(records[0].has_data);
    }

    #[test]
    fn tampered_signature_fails_native_verify() {
        // Flip a byte in the signed body: native verify must reject it. Proves
        // the verify is real (not a rubber stamp).
        let seed = [9u8; 32];
        let mut env = build_signed_pubsub_data(
            seed,
            &topic(),
            b"tamper me",
            DeliveryMode::AnyWhere,
            [2u8; 32],
        );
        // Corrupt a payload byte near the end (inside the signed region).
        let last = env.len() - 1;
        env[last] ^= 0xff;
        let records = peerbit_wire::wire::decode_and_verify_frames(&[env.as_slice()], 1_700_000_000_500);
        assert_eq!(records[0].verify, VerifyStatus::Failed);
    }

    #[test]
    fn full_receive_path_decodes_and_acks() {
        // The end-to-end native engine: a signed AnyWhere PubSubData is
        // verified, deduped, its inner topic-control payload decoded, and an
        // ack is decided — all in native code.
        let sender_seed = [3u8; 32];
        let payload = b"payload-through-the-native-stack";
        let env = build_signed_pubsub_data(
            sender_seed,
            &topic(),
            payload,
            DeliveryMode::AnyWhere,
            [7u8; 32],
        );

        let mut engine = NativeReceiveEngine::new(me_hash(), SPIKE_REDUNDANCY);
        let out = engine.process_inbound_frame(&env, 1_700_000_000_500);

        assert_eq!(out.verify, VerifyStatus::Verified, "must verify natively");
        assert_eq!(out.variant, VARIANT_DATA);
        assert_eq!(out.seen_before, 0, "first sighting");
        assert!(!out.ignored, "first sighting is not ignored");

        let pubsub = out.pubsub.expect("PubSubData decoded natively");
        assert_eq!(pubsub.topics, topic());
        assert_eq!(pubsub.payload, payload);

        let ack = out.ack.expect("ack decided");
        assert_eq!(ack.acked_id, message_id_of(&env));
        assert_eq!(ack.seen_counter, 0);

        // The scheduler queued one outbound ack (WRR ordering token present).
        assert!(engine.scheduler_total_bytes() > 0);
        assert!(engine.next_outbound().is_some());
    }

    #[test]
    fn duplicate_frame_is_deduped_no_second_ack() {
        // Feed the SAME frame twice: the native seen-cache counts the repeat and
        // the ignore/ack decisions change on the second sighting.
        let sender_seed = [4u8; 32];
        let env = build_signed_pubsub_data(
            sender_seed,
            &topic(),
            b"dup",
            DeliveryMode::AnyWhere,
            [8u8; 32],
        );
        let mut engine = NativeReceiveEngine::new(me_hash(), SPIKE_REDUNDANCY);

        let first = engine.process_inbound_frame(&env, 1_700_000_000_500);
        assert_eq!(first.seen_before, 0);
        assert!(!first.ignored);
        assert!(first.ack.is_some());

        let second = engine.process_inbound_frame(&env, 1_700_000_000_501);
        assert_eq!(second.seen_before, 1, "seen once before");
        // AnyWhere (non-acknowledged) mode: seen_before > 0 => ignore.
        assert!(second.ignored, "duplicate is ignored by the native decision");
        assert!(second.ack.is_none(), "no second ack for a duplicate");
    }

    #[test]
    fn signed_ack_verifies_natively() {
        // The ack we emit must itself verify with the frozen codec (so a peer —
        // native or js — accepts it).
        let seed = [5u8; 32];
        let ack = build_signed_ack(seed, [7u8; 32], 0);
        let records = peerbit_wire::wire::decode_and_verify_frames(&[ack.as_slice()], 1_700_000_000_500);
        assert_eq!(records[0].verify, VerifyStatus::Verified);
        assert_eq!(records[0].variant, VARIANT_ACK);
    }
}
