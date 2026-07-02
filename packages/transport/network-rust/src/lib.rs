//! wasm-bindgen surface for the `peerbit_wire` envelope codec.
//!
//! All decode/verify logic lives in the `JsValue`-free `wire` module so it
//! can run under host `cargo test`; this file only translates across the
//! wasm boundary.

pub mod block_exchange;
pub mod direct_stream;
pub mod fanout_tree;
pub mod sync_payload;
pub mod topic_control;
pub mod wire;

use js_sys::{Array, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use block_exchange::{DecodedBlockMessage, EagerBlockIndex, ProviderHintCache};
use direct_stream::lanes::{LaneScheduler, PushOutcome};
use direct_stream::routes::{AddOutcome, Routes};
use direct_stream::seen_cache::SeenCache;
use direct_stream::{decisions, routes};
use fanout_tree::{JoinRejectRedirectInput, ProviderEntryInput, TrackerEntryInput};
use topic_control::{DecodedPubSubMessage, TopicRootDirectoryCore};
use wire::{FrameRecord, VerifyStatus};

/// Flat record layout returned by [`decode_and_verify_batch`]: 4 u32 words
/// per input frame. Mirrored by the TS glue in `src/index.ts` and by the
/// `NativeWire` consumer inside `@peerbit/stream`.
///
/// word 0, byte 0: flags — bit 0 = decode ok, bit 1 = payload present
/// word 0, byte 1: top-level message variant (0 data, 1 ack, 2 hello, 3 goodbye)
/// word 0, byte 2: verify status (0 failed, 1 verified, 2 unsupported → TS fallback)
/// word 0, byte 3: signature count (clamped to 255)
/// word 1: header priority, or 0xffff_ffff when absent
/// word 2: payload byte offset into the frame (data variant only)
/// word 3: payload byte length (data variant only)
pub const RECORD_WORDS: usize = 4;
pub const RECORD_FLAG_DECODE_OK: u32 = 0x01;
pub const RECORD_FLAG_HAS_DATA: u32 = 0x02;
/// Set by receive-fusion decoders (the native-backbone re-export) when the
/// frame's sync payload was stashed for a registered topic. Plain
/// `decode_and_verify_batch` never sets it; consumers that only mask the two
/// bits above are unaffected.
pub const RECORD_FLAG_SYNC_STASHED: u32 = 0x04;
pub const RECORD_NO_PRIORITY: u32 = u32::MAX;

pub fn record_to_words(record: &FrameRecord, out: &mut Vec<u32>) {
    let mut flags = 0u32;
    if record.decode_ok {
        flags |= RECORD_FLAG_DECODE_OK;
    }
    if record.has_data {
        flags |= RECORD_FLAG_HAS_DATA;
    }
    let verify = match record.verify {
        VerifyStatus::Failed => 0u32,
        VerifyStatus::Verified => 1u32,
        VerifyStatus::Unsupported => 2u32,
    };
    out.push(
        flags
            | ((record.variant as u32) << 8)
            | (verify << 16)
            | ((record.signature_count as u32) << 24),
    );
    out.push(record.priority.unwrap_or(RECORD_NO_PRIORITY));
    out.push(record.data_offset);
    out.push(record.data_length);
}

/// Decode a batch of direct-stream frames and verify their signatures
/// (sha256-prehashed Ed25519, batched via ed25519-dalek). Returns
/// [`RECORD_WORDS`] u32 words per input frame; see the layout above.
///
/// `now_ms` is the wall clock used for the header expiry check.
#[wasm_bindgen]
pub fn decode_and_verify_batch(frames: Array, now_ms: f64) -> Vec<u32> {
    let buffers: Vec<Option<Vec<u8>>> = frames
        .iter()
        .map(|value| {
            value
                .dyn_into::<Uint8Array>()
                .ok()
                .map(|array| array.to_vec())
        })
        .collect();
    let slices: Vec<&[u8]> = buffers
        .iter()
        .map(|buffer| buffer.as_deref().unwrap_or(&[]))
        .collect();
    let records = wire::decode_and_verify_frames(&slices, now_ms as u64);
    let mut words = Vec::with_capacity(records.len() * RECORD_WORDS);
    for record in &records {
        record_to_words(record, &mut words);
    }
    words
}

/// Decode a frame and re-encode it from the parsed representation. Used by
/// the golden-vector parity tests to prove Rust encoding is byte-identical
/// to the TS wire format.
#[wasm_bindgen]
pub fn reencode_frame(frame: &[u8]) -> Result<Vec<u8>, JsValue> {
    let decoded = wire::decode_frame(frame).map_err(|error| JsValue::from_str(&error))?;
    Ok(wire::encode_frame(&decoded.message))
}

/// Decode a frame into the stable debug-JSON shape used by the parity tests.
#[wasm_bindgen]
pub fn decode_frame_to_json(frame: &[u8]) -> Result<String, JsValue> {
    let decoded = wire::decode_frame(frame).map_err(|error| JsValue::from_str(&error))?;
    Ok(wire::frame_to_debug_json(&decoded.message))
}

/// The signable byte range of a frame: the serialized message with the
/// delivery mode and signatures excluded (both are mutated in transit).
/// Must match `Message.getSignableBytes()` in the TS implementation.
#[wasm_bindgen]
pub fn signable_bytes(frame: &[u8]) -> Result<Vec<u8>, JsValue> {
    wire::signable_bytes(frame).map_err(|error| JsValue::from_str(&error))
}

/// Deterministic Rust-authored golden vectors for the reverse parity
/// direction (Rust encode → TS decode). See `wire::build_test_corpus`.
#[wasm_bindgen]
pub fn test_corpus_frames() -> Array {
    let corpus = wire::build_test_corpus();
    let out = Array::new();
    for frame in corpus {
        out.push(&Uint8Array::from(frame.as_slice()));
    }
    out
}

// --- DirectStream core (peerbit_direct_stream state machine) ---------------

/// `Routes.add` outcome codes; bit 8 flags that the host must (re)arm the
/// coalesced cleanup timer (`routeMaxRetentionPeriod + 100` ms).
pub const ROUTES_ADD_NEW: u32 = 0;
pub const ROUTES_ADD_UPDATED: u32 = 1;
pub const ROUTES_ADD_RESTART: u32 = 2;
pub const ROUTES_ADD_CLEANUP_REQUESTED: u32 = 0x100;

/// The DirectStream multi-hop routing table (`stream/src/routes.ts` port).
/// All timestamps/sessions are millisecond wall-clock numbers supplied by
/// the host, so behavior under test clocks matches the TS implementation.
#[wasm_bindgen]
pub struct DirectStreamRoutes {
    inner: Routes,
}

#[wasm_bindgen]
impl DirectStreamRoutes {
    #[wasm_bindgen(constructor)]
    pub fn new(
        me: String,
        route_max_retention_period_ms: Option<f64>,
        max_from_entries: Option<u32>,
        max_targets_per_from: Option<u32>,
        max_relays_per_target: Option<u32>,
    ) -> DirectStreamRoutes {
        DirectStreamRoutes {
            inner: Routes::new(
                me,
                route_max_retention_period_ms.map(|ms| ms.max(0.0) as u64),
                max_from_entries.map(|value| value as usize),
                max_targets_per_from.map(|value| value as usize),
                max_relays_per_target.map(|value| value as usize),
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add(
        &mut self,
        from: &str,
        neighbour: &str,
        target: &str,
        distance: f64,
        session: f64,
        remote_session: f64,
        now_ms: f64,
    ) -> u32 {
        let result = self.inner.add(
            from,
            neighbour,
            target,
            distance as i64,
            session as i64,
            remote_session as i64,
            now_ms as u64,
        );
        let code = match result.outcome {
            AddOutcome::New => ROUTES_ADD_NEW,
            AddOutcome::Updated => ROUTES_ADD_UPDATED,
            AddOutcome::Restart => ROUTES_ADD_RESTART,
        };
        code | if result.cleanup_requested {
            ROUTES_ADD_CLEANUP_REQUESTED
        } else {
            0
        }
    }

    pub fn cleanup_pending(&mut self, now_ms: f64) {
        self.inner.cleanup_pending(now_ms as u64);
    }

    pub fn has_pending_cleanup(&self) -> bool {
        self.inner.has_pending_cleanup()
    }

    pub fn get_route_max_retention_period(&self) -> f64 {
        self.inner.route_max_retention_period as f64
    }

    pub fn set_route_max_retention_period(&mut self, ms: f64) {
        self.inner.route_max_retention_period = ms.max(0.0) as u64;
    }

    pub fn remove(&mut self, target: &str) -> Vec<String> {
        self.inner.remove(target)
    }

    pub fn remove_neighbour(&mut self, neighbour: &str) {
        self.inner.remove_neighbour(neighbour);
    }

    pub fn find_neighbor_json(&self, from: &str, target: &str) -> Option<String> {
        self.inner.route_info_json(from, target)
    }

    pub fn get_route_hints_json(&self, from: &str, target: &str, now_ms: f64) -> String {
        self.inner.route_hints_json(from, target, now_ms as u64)
    }

    pub fn is_reachable(&self, from: &str, target: &str, max_distance: Option<f64>) -> bool {
        self.inner.is_reachable(
            from,
            target,
            max_distance
                .map(|value| value as i64)
                .unwrap_or(routes::MAX_ROUTE_DISTANCE),
        )
    }

    pub fn has_target(&self, target: &str) -> bool {
        self.inner.has_target(target)
    }

    pub fn update_session(&mut self, remote: &str, session: Option<f64>) -> bool {
        self.inner
            .update_session(remote, session.map(|value| value as i64))
    }

    pub fn get_session(&self, remote: &str) -> Option<f64> {
        self.inner.get_session(remote).map(|value| value as f64)
    }

    pub fn get_dependent(&self, peer: &str) -> Vec<String> {
        self.inner.get_dependent(peer)
    }

    pub fn count(&self, from: &str) -> u32 {
        self.inner.count(from) as u32
    }

    pub fn count_all(&self) -> u32 {
        self.inner.count_all() as u32
    }

    pub fn get_fanout_json(&self, from: &str, tos: Vec<String>, redundancy: u8) -> Option<String> {
        self.inner.fanout_json(from, &tos, redundancy)
    }

    pub fn get_prunable(&self, neighbours: Vec<String>) -> Vec<String> {
        self.inner.get_prunable(&neighbours)
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn dump_json(&self) -> String {
        self.inner.dump_json()
    }
}

/// Seen-cache dedup counter (`modifySeenCache` semantics).
#[wasm_bindgen]
pub struct DirectStreamSeenCache {
    inner: SeenCache,
}

#[wasm_bindgen]
impl DirectStreamSeenCache {
    #[wasm_bindgen(constructor)]
    pub fn new(max: u32, ttl_ms: f64) -> DirectStreamSeenCache {
        DirectStreamSeenCache {
            inner: SeenCache::new(max as usize, ttl_ms.max(1.0) as u64),
        }
    }

    /// `key_kind` 0 = message id (first 33 frame bytes), 1 = sha256 of the
    /// whole frame (the ACK path). Returns the seen-before counter.
    pub fn modify(&mut self, frame: &[u8], key_kind: u8, now_ms: f64) -> u32 {
        self.inner.modify(frame, key_kind, now_ms as u64)
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

/// 4-lane WRR outbound scheduler with byte budget (`pushable-lanes.ts`
/// queue core). The host keeps the byte chunks and maps the returned
/// sequence numbers back to them, so bytes never cross the boundary.
#[wasm_bindgen]
pub struct DirectStreamLanes {
    inner: LaneScheduler,
}

#[wasm_bindgen]
impl DirectStreamLanes {
    #[wasm_bindgen(constructor)]
    pub fn new(lanes: u32, max_buffered_bytes: Option<f64>) -> DirectStreamLanes {
        DirectStreamLanes {
            inner: LaneScheduler::new(
                lanes as usize,
                max_buffered_bytes.map(|bytes| bytes.max(0.0) as u64),
                None,
            ),
        }
    }

    /// Returns the assigned sequence (>= 0), or `-(wouldBeBytes) - 1` when
    /// the push would exceed the byte budget (overflow policy 'throw').
    pub fn push(&mut self, lane: u32, byte_length: f64) -> f64 {
        match self.inner.push(lane as usize, byte_length as u64) {
            PushOutcome::Pushed(sequence) => sequence as f64,
            PushOutcome::Overflow { would_be } => -(would_be as f64) - 1.0,
        }
    }

    /// Next sequence to emit in WRR order, or -1 when empty.
    pub fn shift(&mut self) -> f64 {
        self.inner
            .shift()
            .map(|sequence| sequence as f64)
            .unwrap_or(-1.0)
    }

    pub fn total_bytes(&self) -> f64 {
        self.inner.total_bytes() as f64
    }

    pub fn lane_bytes(&self, lane: u32) -> f64 {
        self.inner.lane_bytes(lane as usize) as f64
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

#[wasm_bindgen]
pub fn ds_should_ignore_data(
    seen_before: u32,
    acknowledged_mode: bool,
    redundancy: u8,
    hops: Vec<String>,
    me: &str,
    signed_by_self: bool,
) -> bool {
    decisions::should_ignore_data(
        seen_before,
        acknowledged_mode,
        redundancy,
        &hops,
        me,
        signed_by_self,
    )
}

#[wasm_bindgen]
pub fn ds_should_acknowledge(is_recipient: bool, seen_before: u32, redundancy: u8) -> bool {
    decisions::should_acknowledge(is_recipient, seen_before, redundancy)
}

/// Returns `[myIndexAsString, nextHop?]`: the first element is our index in
/// the trace ("-1" when absent), the second — present only when there is a
/// previous hop — is the peer to relay the ACK back to.
#[wasm_bindgen]
pub fn ds_ack_next_hop(trace: Vec<String>, me: &str) -> Vec<String> {
    let (my_index, next) = decisions::ack_next_hop(&trace, me);
    let mut out = vec![my_index.to_string()];
    if let Some(next) = next {
        out.push(next.to_string());
    }
    out
}

/// Returns `[from, neighbour]` — the route edge to learn from an ACK.
#[wasm_bindgen]
pub fn ds_seek_ack_route_update(
    current: &str,
    upstream: Option<String>,
    downstream: &str,
) -> Vec<String> {
    let (from, neighbour) =
        decisions::seek_ack_route_update(current, upstream.as_deref(), downstream);
    vec![from.to_string(), neighbour.to_string()]
}

#[wasm_bindgen]
pub fn ds_filter_flood_targets(
    candidates: Vec<String>,
    from: &str,
    signed: Vec<String>,
    hops: Vec<String>,
) -> Vec<u32> {
    decisions::filter_flood_targets(&candidates, from, &signed, &hops)
}

#[wasm_bindgen]
pub fn ds_filter_silent_relay_recipients(
    recipients: Vec<String>,
    me: &str,
    from: &str,
    connected: Vec<String>,
    hops: Vec<String>,
) -> Vec<String> {
    decisions::filter_silent_relay_recipients(&recipients, me, from, &connected, &hops)
}

#[wasm_bindgen]
pub fn ds_select_redundancy_probes(
    peers: Vec<String>,
    used: Vec<String>,
    redundancy: u8,
) -> Vec<String> {
    decisions::select_redundancy_probes(&peers, &used, redundancy)
}

// --- DirectBlock exchange (block_exchange module) ---------------------------

pub const BLOCK_MESSAGE_REQUEST: u8 = block_exchange::BLOCK_MESSAGE_VARIANT_REQUEST;
pub const BLOCK_MESSAGE_RESPONSE: u8 = block_exchange::BLOCK_MESSAGE_VARIANT_RESPONSE;

/// A decoded `/peerbit/direct-block` message. Response payload bytes are
/// reported as a range into the input frame so the host can alias them
/// without copying.
#[wasm_bindgen]
pub struct DirectBlockDecodedMessage {
    variant: u8,
    cid: String,
    bytes_offset: u32,
    bytes_length: u32,
}

#[wasm_bindgen]
impl DirectBlockDecodedMessage {
    #[wasm_bindgen(getter)]
    pub fn variant(&self) -> u8 {
        self.variant
    }

    #[wasm_bindgen(getter)]
    pub fn cid(&self) -> String {
        self.cid.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn bytes_offset(&self) -> u32 {
        self.bytes_offset
    }

    #[wasm_bindgen(getter)]
    pub fn bytes_length(&self) -> u32 {
        self.bytes_length
    }
}

/// Decode a borsh `BlockMessage` payload (`BlockRequest(0)`/`BlockResponse(1)`).
#[wasm_bindgen]
pub fn db_decode_block_message(frame: &[u8]) -> Result<DirectBlockDecodedMessage, JsValue> {
    match block_exchange::decode_block_message(frame).map_err(|error| JsValue::from_str(&error))? {
        DecodedBlockMessage::Request { cid } => Ok(DirectBlockDecodedMessage {
            variant: BLOCK_MESSAGE_REQUEST,
            cid,
            bytes_offset: 0,
            bytes_length: 0,
        }),
        DecodedBlockMessage::Response {
            cid,
            bytes_offset,
            bytes_length,
        } => Ok(DirectBlockDecodedMessage {
            variant: BLOCK_MESSAGE_RESPONSE,
            cid,
            bytes_offset: bytes_offset as u32,
            bytes_length: bytes_length as u32,
        }),
    }
}

#[wasm_bindgen]
pub fn db_encode_block_request(cid: &str) -> Vec<u8> {
    block_exchange::encode_block_request(cid)
}

#[wasm_bindgen]
pub fn db_encode_block_response(cid: &str, bytes: &[u8]) -> Vec<u8> {
    block_exchange::encode_block_response(cid, bytes)
}

#[wasm_bindgen]
pub fn db_normalize_provider_hints(providers: Vec<String>, me: &str, limit: u32) -> Vec<String> {
    block_exchange::normalize_provider_hints(&providers, me, limit.max(1) as usize)
}

#[wasm_bindgen]
pub fn db_pick_request_batch(providers: Vec<String>, me: &str, attempt: u32) -> Vec<String> {
    block_exchange::pick_request_batch(&providers, me, attempt as usize)
}

#[wasm_bindgen]
pub fn db_default_provider_candidates(
    negotiated: Vec<String>,
    connected: Vec<String>,
    me: &str,
) -> Vec<String> {
    block_exchange::default_provider_candidates(&negotiated, &connected, me)
}

/// Provider-hint cache of `RemoteBlocks` (`rememberProvider`/
/// `rememberProviderHints`/lookup). Timestamps are host-supplied wall-clock
/// milliseconds, as in the other DirectStream cores.
#[wasm_bindgen]
pub struct DirectBlockProviderCache {
    inner: ProviderHintCache,
}

#[wasm_bindgen]
impl DirectBlockProviderCache {
    #[wasm_bindgen(constructor)]
    pub fn new(
        me: String,
        max_entries: u32,
        ttl_ms: f64,
        max_providers_per_cid: u32,
    ) -> DirectBlockProviderCache {
        DirectBlockProviderCache {
            inner: ProviderHintCache::new(
                me,
                max_entries as usize,
                ttl_ms.max(1.0) as u64,
                max_providers_per_cid as usize,
            ),
        }
    }

    pub fn get(&mut self, cid: &str, now_ms: f64) -> Option<Vec<String>> {
        self.inner.get(cid, now_ms as u64)
    }

    pub fn remember_provider(&mut self, cid: &str, provider: &str, now_ms: f64) {
        self.inner.remember_provider(cid, provider, now_ms as u64);
    }

    pub fn remember_hints(&mut self, cid: &str, providers: Vec<String>, now_ms: f64) {
        self.inner.remember_hints(cid, &providers, now_ms as u64);
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

/// Eager-block bookkeeping (`_blockCache` in `RemoteBlocks`). The host keeps
/// the block bytes and drops the buffers named by the returned eviction
/// lists, so bytes never cross the boundary.
#[wasm_bindgen]
pub struct DirectBlockEagerIndex {
    inner: EagerBlockIndex,
}

#[wasm_bindgen]
impl DirectBlockEagerIndex {
    #[wasm_bindgen(constructor)]
    pub fn new(max: u32, ttl_ms: f64) -> DirectBlockEagerIndex {
        DirectBlockEagerIndex {
            inner: EagerBlockIndex::new(max as usize, ttl_ms.max(1.0) as u64),
        }
    }

    /// Track a cid; returns the cids evicted by the insert (ttl/max bound).
    pub fn add(&mut self, cid: &str, now_ms: f64) -> Vec<String> {
        self.inner.add(cid, now_ms as u64)
    }

    /// Evict expired entries and return their cids.
    pub fn sweep(&mut self, now_ms: f64) -> Vec<String> {
        self.inner.sweep(now_ms as u64)
    }

    pub fn contains(&self, cid: &str) -> bool {
        self.inner.contains(cid)
    }

    pub fn del(&mut self, cid: &str) {
        self.inner.del(cid);
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

// --- TopicControlPlane (topic_control module) --------------------------------

/// A decoded `/peerbit/topic-control-plane` message. `Data` payload bytes are
/// reported as a range into the input frame so the host can alias them
/// without copying. `topics` doubles as the candidate list for the
/// `TopicRootCandidates` variant; `text` carries the public-key hash
/// (`PeerUnavailable`) or the topic (`TopicRootQuery`/`Response`).
#[wasm_bindgen]
pub struct TopicControlDecodedMessage {
    variant: u8,
    topics: Vec<String>,
    flag: bool,
    data_offset: u32,
    data_length: u32,
    text: String,
    root: Option<String>,
    request_id: u32,
    session: u64,
    timestamp: u64,
}

#[wasm_bindgen]
impl TopicControlDecodedMessage {
    #[wasm_bindgen(getter)]
    pub fn variant(&self) -> u8 {
        self.variant
    }

    #[wasm_bindgen(getter)]
    pub fn topics(&self) -> Vec<String> {
        self.topics.clone()
    }

    /// `strict` (PubSubData) or `requestSubscribers` (Subscribe).
    #[wasm_bindgen(getter)]
    pub fn flag(&self) -> bool {
        self.flag
    }

    #[wasm_bindgen(getter)]
    pub fn data_offset(&self) -> u32 {
        self.data_offset
    }

    #[wasm_bindgen(getter)]
    pub fn data_length(&self) -> u32 {
        self.data_length
    }

    #[wasm_bindgen(getter)]
    pub fn text(&self) -> String {
        self.text.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn root(&self) -> Option<String> {
        self.root.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn request_id(&self) -> u32 {
        self.request_id
    }

    #[wasm_bindgen(getter)]
    pub fn session(&self) -> u64 {
        self.session
    }

    #[wasm_bindgen(getter)]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Decode a borsh `PubSubMessage` payload (variants 0-7).
#[wasm_bindgen]
pub fn tc_decode_pubsub_message(frame: &[u8]) -> Result<TopicControlDecodedMessage, JsValue> {
    let decoded =
        topic_control::decode_pubsub_message(frame).map_err(|error| JsValue::from_str(&error))?;
    let mut message = TopicControlDecodedMessage {
        variant: 0,
        topics: Vec::new(),
        flag: false,
        data_offset: 0,
        data_length: 0,
        text: String::new(),
        root: None,
        request_id: 0,
        session: 0,
        timestamp: 0,
    };
    match decoded {
        DecodedPubSubMessage::Data {
            topics,
            strict,
            data_offset,
            data_length,
        } => {
            message.variant = topic_control::PUBSUB_VARIANT_DATA;
            message.topics = topics;
            message.flag = strict;
            message.data_offset = data_offset as u32;
            message.data_length = data_length as u32;
        }
        DecodedPubSubMessage::Subscribe {
            topics,
            request_subscribers,
        } => {
            message.variant = topic_control::PUBSUB_VARIANT_SUBSCRIBE;
            message.topics = topics;
            message.flag = request_subscribers;
        }
        DecodedPubSubMessage::Unsubscribe { topics } => {
            message.variant = topic_control::PUBSUB_VARIANT_UNSUBSCRIBE;
            message.topics = topics;
        }
        DecodedPubSubMessage::GetSubscribers { topics } => {
            message.variant = topic_control::PUBSUB_VARIANT_GET_SUBSCRIBERS;
            message.topics = topics;
        }
        DecodedPubSubMessage::TopicRootCandidates { candidates } => {
            message.variant = topic_control::PUBSUB_VARIANT_TOPIC_ROOT_CANDIDATES;
            message.topics = candidates;
        }
        DecodedPubSubMessage::PeerUnavailable {
            public_key_hash,
            session,
            timestamp,
            topics,
        } => {
            message.variant = topic_control::PUBSUB_VARIANT_PEER_UNAVAILABLE;
            message.text = public_key_hash;
            message.session = session;
            message.timestamp = timestamp;
            message.topics = topics;
        }
        DecodedPubSubMessage::TopicRootQuery { request_id, topic } => {
            message.variant = topic_control::PUBSUB_VARIANT_TOPIC_ROOT_QUERY;
            message.request_id = request_id;
            message.text = topic;
        }
        DecodedPubSubMessage::TopicRootQueryResponse {
            request_id,
            topic,
            root,
        } => {
            message.variant = topic_control::PUBSUB_VARIANT_TOPIC_ROOT_QUERY_RESPONSE;
            message.request_id = request_id;
            message.text = topic;
            message.root = root;
        }
    }
    Ok(message)
}

#[wasm_bindgen]
pub fn tc_encode_pubsub_data(topics: Vec<String>, strict: bool, data: &[u8]) -> Vec<u8> {
    topic_control::encode_pubsub_data(&topics, strict, data)
}

#[wasm_bindgen]
pub fn tc_encode_subscribe(topics: Vec<String>, request_subscribers: bool) -> Vec<u8> {
    topic_control::encode_subscribe(&topics, request_subscribers)
}

#[wasm_bindgen]
pub fn tc_encode_unsubscribe(topics: Vec<String>) -> Vec<u8> {
    topic_control::encode_unsubscribe(&topics)
}

#[wasm_bindgen]
pub fn tc_encode_get_subscribers(topics: Vec<String>) -> Vec<u8> {
    topic_control::encode_get_subscribers(&topics)
}

#[wasm_bindgen]
pub fn tc_encode_topic_root_candidates(candidates: Vec<String>) -> Vec<u8> {
    topic_control::encode_topic_root_candidates(&candidates)
}

#[wasm_bindgen]
pub fn tc_encode_peer_unavailable(
    public_key_hash: &str,
    session: u64,
    timestamp: u64,
    topics: Vec<String>,
) -> Vec<u8> {
    topic_control::encode_peer_unavailable(public_key_hash, session, timestamp, &topics)
}

#[wasm_bindgen]
pub fn tc_encode_topic_root_query(request_id: u32, topic: &str) -> Vec<u8> {
    topic_control::encode_topic_root_query(request_id, topic)
}

#[wasm_bindgen]
pub fn tc_encode_topic_root_query_response(
    request_id: u32,
    topic: &str,
    root: Option<String>,
) -> Vec<u8> {
    topic_control::encode_topic_root_query_response(request_id, topic, root.as_deref())
}

#[wasm_bindgen]
pub fn tc_topic_hash32(topic: &str) -> u32 {
    topic_control::topic_hash32(topic)
}

#[wasm_bindgen]
pub fn tc_shard_topic(topic: &str, shard_count: u32, prefix: &str) -> String {
    topic_control::shard_topic_for(topic, shard_count, prefix)
}

#[wasm_bindgen]
pub fn tc_normalize_auto_candidates(candidates: Vec<String>, me: &str) -> Vec<String> {
    topic_control::normalize_auto_candidates(&candidates, me)
}

/// `lasts` carries interleaved (session, timestamp) watermark pairs for the
/// relevant topics that have one; see `subscription_is_latest`.
#[wasm_bindgen]
pub fn tc_subscription_is_latest(lasts: Vec<u64>, session: u64, timestamp: u64) -> bool {
    topic_control::subscription_is_latest(&lasts, session, timestamp)
}

#[wasm_bindgen]
pub fn tc_subscribe_should_replace(existing_session: Option<u64>, session: u64) -> bool {
    topic_control::subscribe_should_replace(existing_session, session)
}

/// `TopicRootDirectory` root-resolution state (explicit roots + normalized
/// deterministic candidates). Trackers and the resolver callback stay
/// host-side.
#[wasm_bindgen]
#[derive(Default)]
pub struct TopicControlRootDirectory {
    inner: TopicRootDirectoryCore,
}

#[wasm_bindgen]
impl TopicControlRootDirectory {
    #[wasm_bindgen(constructor)]
    pub fn new() -> TopicControlRootDirectory {
        TopicControlRootDirectory {
            inner: TopicRootDirectoryCore::new(),
        }
    }

    pub fn set_root(&mut self, topic: &str, root: &str) {
        self.inner.set_root(topic, root);
    }

    pub fn delete_root(&mut self, topic: &str) {
        self.inner.delete_root(topic);
    }

    pub fn get_root(&self, topic: &str) -> Option<String> {
        self.inner.get_root(topic)
    }

    pub fn set_default_candidates(&mut self, candidates: Vec<String>) {
        self.inner.set_default_candidates(&candidates);
    }

    pub fn get_default_candidates(&self) -> Vec<String> {
        self.inner.get_default_candidates()
    }

    pub fn resolve_deterministic_candidate(&self, topic: &str) -> Option<String> {
        self.inner.resolve_deterministic_candidate(topic)
    }
}

// --- FanoutTree (fanout_tree module) ------------------------------------------

fn array_to_byte_vecs(values: &Array) -> Vec<Vec<u8>> {
    values
        .iter()
        .map(|value| {
            value
                .dyn_into::<Uint8Array>()
                .map(|array| array.to_vec())
                .unwrap_or_default()
        })
        .collect()
}

fn byte_vecs_to_array(values: &[Vec<u8>]) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&Uint8Array::from(value.as_slice()));
    }
    out
}

/// Group a flat addr list into per-entry lists using `addr_counts`.
fn group_addrs(addr_counts: &[u32], flat: Vec<Vec<u8>>) -> Vec<Vec<Vec<u8>>> {
    let mut grouped: Vec<Vec<Vec<u8>>> = Vec::with_capacity(addr_counts.len());
    let mut iter = flat.into_iter();
    for count in addr_counts {
        grouped.push(iter.by_ref().take(*count as usize).collect());
    }
    grouped
}

/// A decoded `/peerbit/fanout-tree` control frame. One shared shape covers
/// every message kind; the per-kind `ft_decode_*` function documents which
/// fields it populates. Entry lists (tracker reply, provider reply/notify)
/// are flattened into parallel arrays with `entry_addr_counts` delimiting
/// each entry's slice of `entry_addrs`.
#[wasm_bindgen]
#[derive(Default)]
pub struct FanoutTreeDecodedFrame {
    req_id: u32,
    bid_per_byte: u32,
    reservation_token: u32,
    level: u32,
    max_children: u32,
    free_slots: u32,
    children: u32,
    have_from: u32,
    have_to_exclusive: u32,
    has_have_range: bool,
    missing_seqs: u32,
    data_write_drops: u32,
    dropped_forwards: u32,
    ttl_ms: u32,
    want: u32,
    seed: u32,
    flags: u32,
    event: u32,
    reason: u32,
    ack_token: u64,
    has_ack: bool,
    seqs: Vec<u32>,
    route: Vec<String>,
    reply_route: Vec<String>,
    has_reply_route: bool,
    text: String,
    has_text: bool,
    payload_offset: u32,
    min_free_slots: u32,
    reserve_root_capacity: bool,
    addrs: Vec<Vec<u8>>,
    entry_hashes: Vec<String>,
    entry_levels: Vec<u32>,
    entry_free_slots: Vec<u32>,
    entry_bids: Vec<u32>,
    entry_addr_counts: Vec<u32>,
    entry_addrs: Vec<Vec<u8>>,
}

#[wasm_bindgen]
impl FanoutTreeDecodedFrame {
    #[wasm_bindgen(getter)]
    pub fn req_id(&self) -> u32 {
        self.req_id
    }

    #[wasm_bindgen(getter)]
    pub fn bid_per_byte(&self) -> u32 {
        self.bid_per_byte
    }

    #[wasm_bindgen(getter)]
    pub fn reservation_token(&self) -> u32 {
        self.reservation_token
    }

    #[wasm_bindgen(getter)]
    pub fn level(&self) -> u32 {
        self.level
    }

    #[wasm_bindgen(getter)]
    pub fn max_children(&self) -> u32 {
        self.max_children
    }

    #[wasm_bindgen(getter)]
    pub fn free_slots(&self) -> u32 {
        self.free_slots
    }

    #[wasm_bindgen(getter)]
    pub fn children(&self) -> u32 {
        self.children
    }

    #[wasm_bindgen(getter)]
    pub fn have_from(&self) -> u32 {
        self.have_from
    }

    #[wasm_bindgen(getter)]
    pub fn have_to_exclusive(&self) -> u32 {
        self.have_to_exclusive
    }

    #[wasm_bindgen(getter)]
    pub fn has_have_range(&self) -> bool {
        self.has_have_range
    }

    #[wasm_bindgen(getter)]
    pub fn missing_seqs(&self) -> u32 {
        self.missing_seqs
    }

    #[wasm_bindgen(getter)]
    pub fn data_write_drops(&self) -> u32 {
        self.data_write_drops
    }

    #[wasm_bindgen(getter)]
    pub fn dropped_forwards(&self) -> u32 {
        self.dropped_forwards
    }

    #[wasm_bindgen(getter)]
    pub fn ttl_ms(&self) -> u32 {
        self.ttl_ms
    }

    #[wasm_bindgen(getter)]
    pub fn want(&self) -> u32 {
        self.want
    }

    #[wasm_bindgen(getter)]
    pub fn seed(&self) -> u32 {
        self.seed
    }

    #[wasm_bindgen(getter)]
    pub fn flags(&self) -> u32 {
        self.flags
    }

    #[wasm_bindgen(getter)]
    pub fn event(&self) -> u32 {
        self.event
    }

    #[wasm_bindgen(getter)]
    pub fn reason(&self) -> u32 {
        self.reason
    }

    #[wasm_bindgen(getter)]
    pub fn ack_token(&self) -> u64 {
        self.ack_token
    }

    #[wasm_bindgen(getter)]
    pub fn has_ack(&self) -> bool {
        self.has_ack
    }

    #[wasm_bindgen(getter)]
    pub fn seqs(&self) -> Vec<u32> {
        self.seqs.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn route(&self) -> Vec<String> {
        self.route.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn reply_route(&self) -> Vec<String> {
        self.reply_route.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn has_reply_route(&self) -> bool {
        self.has_reply_route
    }

    #[wasm_bindgen(getter)]
    pub fn text(&self) -> String {
        self.text.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn has_text(&self) -> bool {
        self.has_text
    }

    #[wasm_bindgen(getter)]
    pub fn payload_offset(&self) -> u32 {
        self.payload_offset
    }

    #[wasm_bindgen(getter)]
    pub fn min_free_slots(&self) -> u32 {
        self.min_free_slots
    }

    #[wasm_bindgen(getter)]
    pub fn reserve_root_capacity(&self) -> bool {
        self.reserve_root_capacity
    }

    #[wasm_bindgen(getter)]
    pub fn addrs(&self) -> Array {
        byte_vecs_to_array(&self.addrs)
    }

    #[wasm_bindgen(getter)]
    pub fn entry_hashes(&self) -> Vec<String> {
        self.entry_hashes.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn entry_levels(&self) -> Vec<u32> {
        self.entry_levels.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn entry_free_slots(&self) -> Vec<u32> {
        self.entry_free_slots.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn entry_bids(&self) -> Vec<u32> {
        self.entry_bids.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn entry_addr_counts(&self) -> Vec<u32> {
        self.entry_addr_counts.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn entry_addrs(&self) -> Array {
        byte_vecs_to_array(&self.entry_addrs)
    }
}

#[wasm_bindgen]
pub fn ft_encode_join_req(
    channel_key: &[u8],
    req_id: f64,
    bid_per_byte: f64,
    parent_upgrade_reservation_token: f64,
) -> Vec<u8> {
    fanout_tree::encode_join_req(
        channel_key,
        req_id,
        bid_per_byte,
        parent_upgrade_reservation_token,
    )
}

#[wasm_bindgen]
pub fn ft_encode_join_accept(
    channel_key: &[u8],
    req_id: f64,
    level: f64,
    parent_route_from_root: Vec<String>,
    has_have_range: bool,
    have_from: f64,
    have_to_exclusive: f64,
) -> Vec<u8> {
    fanout_tree::encode_join_accept(
        channel_key,
        req_id,
        level,
        &parent_route_from_root,
        has_have_range.then_some((have_from, have_to_exclusive)),
    )
}

#[wasm_bindgen]
pub fn ft_encode_join_reject(
    channel_key: &[u8],
    req_id: f64,
    reason: f64,
    redirect_hashes: Vec<String>,
    redirect_addr_counts: Vec<u32>,
    redirect_addrs: Array,
) -> Vec<u8> {
    let grouped = group_addrs(&redirect_addr_counts, array_to_byte_vecs(&redirect_addrs));
    let redirects: Vec<JoinRejectRedirectInput> = redirect_hashes
        .into_iter()
        .zip(grouped)
        .map(|(hash, addrs)| JoinRejectRedirectInput { hash, addrs })
        .collect();
    fanout_tree::encode_join_reject(channel_key, req_id, reason, &redirects)
}

#[wasm_bindgen]
pub fn ft_encode_kick(channel_key: &[u8]) -> Vec<u8> {
    fanout_tree::encode_kick(channel_key)
}

#[wasm_bindgen]
pub fn ft_encode_end(channel_key: &[u8], last_seq_exclusive: f64) -> Vec<u8> {
    fanout_tree::encode_end(channel_key, last_seq_exclusive)
}

#[wasm_bindgen]
pub fn ft_encode_repair_req(channel_key: &[u8], req_id: f64, missing_seqs: Vec<f64>) -> Vec<u8> {
    fanout_tree::encode_repair_req(channel_key, req_id, &missing_seqs)
}

#[wasm_bindgen]
pub fn ft_encode_fetch_req(channel_key: &[u8], req_id: f64, missing_seqs: Vec<f64>) -> Vec<u8> {
    fanout_tree::encode_fetch_req(channel_key, req_id, &missing_seqs)
}

#[wasm_bindgen]
pub fn ft_encode_ihave(channel_key: &[u8], have_from: f64, have_to_exclusive: f64) -> Vec<u8> {
    fanout_tree::encode_ihave(channel_key, have_from, have_to_exclusive)
}

#[wasm_bindgen]
pub fn ft_encode_data(payload: &[u8]) -> Vec<u8> {
    fanout_tree::encode_data(payload)
}

#[wasm_bindgen]
pub fn ft_encode_publish_proxy(channel_key: &[u8], payload: &[u8]) -> Vec<u8> {
    fanout_tree::encode_publish_proxy(channel_key, payload)
}

#[wasm_bindgen]
pub fn ft_encode_leave(channel_key: &[u8]) -> Vec<u8> {
    fanout_tree::encode_leave(channel_key)
}

#[wasm_bindgen]
pub fn ft_encode_unicast(
    channel_key: &[u8],
    route: Vec<String>,
    payload: &[u8],
    has_ack: bool,
    ack_token: u64,
    reply_route: Vec<String>,
) -> Vec<u8> {
    fanout_tree::encode_unicast(
        channel_key,
        &route,
        payload,
        has_ack.then_some(ack_token),
        &reply_route,
    )
}

#[wasm_bindgen]
pub fn ft_encode_unicast_ack(channel_key: &[u8], ack_token: u64, route: Vec<String>) -> Vec<u8> {
    fanout_tree::encode_unicast_ack(channel_key, ack_token, &route)
}

#[wasm_bindgen]
pub fn ft_encode_route_query(channel_key: &[u8], req_id: f64, target_hash: &str) -> Vec<u8> {
    fanout_tree::encode_route_query(channel_key, req_id, target_hash)
}

#[wasm_bindgen]
pub fn ft_encode_route_reply(channel_key: &[u8], req_id: f64, route: Vec<String>) -> Vec<u8> {
    fanout_tree::encode_route_reply(channel_key, req_id, &route)
}

#[wasm_bindgen]
pub fn ft_encode_tracker_announce(
    channel_key: &[u8],
    ttl_ms: f64,
    level: f64,
    max_children: f64,
    free_slots: f64,
    bid_per_byte: f64,
    addrs: Array,
) -> Vec<u8> {
    fanout_tree::encode_tracker_announce(
        channel_key,
        ttl_ms,
        level,
        max_children,
        free_slots,
        bid_per_byte,
        &array_to_byte_vecs(&addrs),
    )
}

#[wasm_bindgen]
pub fn ft_encode_tracker_query(channel_key: &[u8], req_id: f64, want: f64) -> Vec<u8> {
    fanout_tree::encode_tracker_query(channel_key, req_id, want)
}

#[allow(clippy::too_many_arguments)]
#[wasm_bindgen]
pub fn ft_encode_tracker_reply(
    channel_key: &[u8],
    req_id: f64,
    entry_hashes: Vec<String>,
    entry_levels: Vec<f64>,
    entry_free_slots: Vec<f64>,
    entry_bids: Vec<f64>,
    entry_addr_counts: Vec<u32>,
    entry_addrs: Array,
) -> Vec<u8> {
    let grouped = group_addrs(&entry_addr_counts, array_to_byte_vecs(&entry_addrs));
    let entries: Vec<TrackerEntryInput> = entry_hashes
        .into_iter()
        .zip(entry_levels)
        .zip(entry_free_slots)
        .zip(entry_bids)
        .zip(grouped)
        .map(
            |((((hash, level), free_slots), bid_per_byte), addrs)| TrackerEntryInput {
                hash,
                level,
                free_slots,
                bid_per_byte,
                addrs,
            },
        )
        .collect();
    fanout_tree::encode_tracker_reply(channel_key, req_id, &entries)
}

#[wasm_bindgen]
pub fn ft_encode_tracker_feedback(
    channel_key: &[u8],
    candidate_hash: &str,
    event: f64,
    reason: f64,
) -> Vec<u8> {
    fanout_tree::encode_tracker_feedback(channel_key, candidate_hash, event, reason)
}

#[wasm_bindgen]
pub fn ft_encode_parent_probe_req(
    channel_key: &[u8],
    req_id: f64,
    min_free_slots: f64,
    reserve_root_capacity: bool,
) -> Vec<u8> {
    fanout_tree::encode_parent_probe_req(channel_key, req_id, min_free_slots, reserve_root_capacity)
}

#[allow(clippy::too_many_arguments)]
#[wasm_bindgen]
pub fn ft_encode_parent_probe_reply(
    channel_key: &[u8],
    req_id: f64,
    flags: f64,
    level: f64,
    max_children: f64,
    free_slots: f64,
    children: f64,
    have_to_exclusive: f64,
    missing_seqs: f64,
    data_write_drops: f64,
    dropped_forwards: f64,
    reservation_token: f64,
) -> Vec<u8> {
    fanout_tree::encode_parent_probe_reply(
        channel_key,
        req_id,
        flags,
        level,
        max_children,
        free_slots,
        children,
        have_to_exclusive,
        missing_seqs,
        data_write_drops,
        dropped_forwards,
        reservation_token,
    )
}

#[wasm_bindgen]
pub fn ft_encode_provider_announce(namespace_key: &[u8], ttl_ms: f64, addrs: Array) -> Vec<u8> {
    fanout_tree::encode_provider_announce(namespace_key, ttl_ms, &array_to_byte_vecs(&addrs))
}

#[wasm_bindgen]
pub fn ft_encode_provider_query(
    namespace_key: &[u8],
    req_id: f64,
    want: f64,
    seed: f64,
) -> Vec<u8> {
    fanout_tree::encode_provider_query(namespace_key, req_id, want, seed)
}

fn provider_entries_from_parts(
    hashes: Vec<String>,
    addr_counts: Vec<u32>,
    addrs: Array,
) -> Vec<ProviderEntryInput> {
    let grouped = group_addrs(&addr_counts, array_to_byte_vecs(&addrs));
    hashes
        .into_iter()
        .zip(grouped)
        .map(|(hash, addrs)| ProviderEntryInput { hash, addrs })
        .collect()
}

#[wasm_bindgen]
pub fn ft_encode_provider_reply(
    namespace_key: &[u8],
    req_id: f64,
    entry_hashes: Vec<String>,
    entry_addr_counts: Vec<u32>,
    entry_addrs: Array,
) -> Vec<u8> {
    fanout_tree::encode_provider_reply(
        namespace_key,
        req_id,
        &provider_entries_from_parts(entry_hashes, entry_addr_counts, entry_addrs),
    )
}

#[wasm_bindgen]
pub fn ft_encode_provider_subscribe(namespace_key: &[u8], want: f64, ttl_ms: f64) -> Vec<u8> {
    fanout_tree::encode_provider_subscribe(namespace_key, want, ttl_ms)
}

#[wasm_bindgen]
pub fn ft_encode_provider_unsubscribe(namespace_key: &[u8]) -> Vec<u8> {
    fanout_tree::encode_provider_unsubscribe(namespace_key)
}

#[wasm_bindgen]
pub fn ft_encode_provider_notify(
    namespace_key: &[u8],
    entry_hashes: Vec<String>,
    entry_addr_counts: Vec<u32>,
    entry_addrs: Array,
) -> Vec<u8> {
    fanout_tree::encode_provider_notify(
        namespace_key,
        &provider_entries_from_parts(entry_hashes, entry_addr_counts, entry_addrs),
    )
}

#[wasm_bindgen]
pub fn ft_decode_join_req(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_join_req(data).map(|decoded| FanoutTreeDecodedFrame {
        req_id: decoded.req_id,
        bid_per_byte: decoded.bid_per_byte,
        reservation_token: decoded.parent_upgrade_reservation_token,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_join_response_req_id(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_join_response_req_id(data).map(|req_id| FanoutTreeDecodedFrame {
        req_id,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_join_accept(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_join_accept(data).map(|decoded| FanoutTreeDecodedFrame {
        level: decoded.parent_level as u32,
        route: decoded.parent_route_from_root,
        has_have_range: decoded.have_range.is_some(),
        have_from: decoded.have_range.map(|range| range.0).unwrap_or(0),
        have_to_exclusive: decoded.have_range.map(|range| range.1).unwrap_or(0),
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_join_reject(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_join_reject(data).map(|decoded| {
        let mut frame = FanoutTreeDecodedFrame {
            reason: decoded.reason as u32,
            ..Default::default()
        };
        for redirect in decoded.redirects {
            frame.entry_hashes.push(redirect.hash);
            frame.entry_addr_counts.push(redirect.addrs.len() as u32);
            frame.entry_addrs.extend(redirect.addrs);
        }
        frame
    })
}

#[wasm_bindgen]
pub fn ft_decode_end(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_end(data).map(|last_seq_exclusive| FanoutTreeDecodedFrame {
        have_to_exclusive: last_seq_exclusive,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_repair_seqs(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_repair_seqs(data).map(|seqs| FanoutTreeDecodedFrame {
        seqs,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_ihave(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_ihave(data).map(|(have_from, have_to_exclusive)| FanoutTreeDecodedFrame {
        have_from,
        have_to_exclusive,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_unicast(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_unicast(data).map(|decoded| FanoutTreeDecodedFrame {
        has_ack: decoded.ack_token.is_some(),
        ack_token: decoded.ack_token.unwrap_or(0),
        route: decoded.route,
        has_reply_route: decoded.reply_route.is_some(),
        reply_route: decoded.reply_route.unwrap_or_default(),
        payload_offset: decoded.payload_offset as u32,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_unicast_ack(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_unicast_ack(data).map(|decoded| FanoutTreeDecodedFrame {
        has_ack: true,
        ack_token: decoded.ack_token,
        route: decoded.route,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_route_query(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_route_query(data).map(|decoded| FanoutTreeDecodedFrame {
        req_id: decoded.req_id,
        has_text: decoded.target_hash.is_some(),
        text: decoded.target_hash.unwrap_or_default(),
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_route_reply(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_route_reply(data).map(|decoded| FanoutTreeDecodedFrame {
        req_id: decoded.req_id,
        route: decoded.route,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_tracker_announce(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_tracker_announce(data).map(|decoded| FanoutTreeDecodedFrame {
        ttl_ms: decoded.ttl_ms,
        level: decoded.level as u32,
        free_slots: decoded.free_slots as u32,
        bid_per_byte: decoded.bid_per_byte,
        addrs: decoded.addrs,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_tracker_query(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_tracker_query(data).map(|decoded| FanoutTreeDecodedFrame {
        req_id: decoded.req_id,
        want: decoded.want as u32,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_tracker_reply(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_tracker_reply(data).map(|decoded| {
        let mut frame = FanoutTreeDecodedFrame {
            req_id: decoded.req_id,
            ..Default::default()
        };
        for entry in decoded.entries {
            frame.entry_hashes.push(entry.hash);
            frame.entry_levels.push(entry.level as u32);
            frame.entry_free_slots.push(entry.free_slots as u32);
            frame.entry_bids.push(entry.bid_per_byte);
            frame.entry_addr_counts.push(entry.addrs.len() as u32);
            frame.entry_addrs.extend(entry.addrs);
        }
        frame
    })
}

#[wasm_bindgen]
pub fn ft_decode_tracker_feedback(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_tracker_feedback(data).map(|decoded| FanoutTreeDecodedFrame {
        has_text: true,
        text: decoded.candidate_hash,
        event: decoded.event as u32,
        reason: decoded.reason as u32,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_parent_probe_req(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_parent_probe_req(data).map(|decoded| FanoutTreeDecodedFrame {
        req_id: decoded.req_id,
        min_free_slots: decoded.min_free_slots as u32,
        reserve_root_capacity: decoded.reserve_root_capacity,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_parent_probe_reply(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_parent_probe_reply(data).map(|decoded| FanoutTreeDecodedFrame {
        req_id: decoded.req_id,
        flags: decoded.flags as u32,
        reservation_token: decoded.reservation_token,
        level: decoded.level as u32,
        max_children: decoded.max_children as u32,
        free_slots: decoded.free_slots as u32,
        children: decoded.children as u32,
        have_to_exclusive: decoded.have_to_exclusive,
        missing_seqs: decoded.missing_seqs as u32,
        data_write_drops: decoded.data_write_drops,
        dropped_forwards: decoded.dropped_forwards,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_provider_announce(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_provider_announce(data).map(|decoded| FanoutTreeDecodedFrame {
        ttl_ms: decoded.ttl_ms,
        addrs: decoded.addrs,
        ..Default::default()
    })
}

#[wasm_bindgen]
pub fn ft_decode_provider_query(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_provider_query(data).map(|decoded| FanoutTreeDecodedFrame {
        req_id: decoded.req_id,
        want: decoded.want as u32,
        seed: decoded.seed,
        ..Default::default()
    })
}

fn provider_entries_into_frame(
    frame: &mut FanoutTreeDecodedFrame,
    entries: Vec<fanout_tree::DecodedProviderEntry>,
) {
    for entry in entries {
        frame.entry_hashes.push(entry.hash);
        frame.entry_addr_counts.push(entry.addrs.len() as u32);
        frame.entry_addrs.extend(entry.addrs);
    }
}

#[wasm_bindgen]
pub fn ft_decode_provider_reply(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_provider_reply(data).map(|decoded| {
        let mut frame = FanoutTreeDecodedFrame {
            req_id: decoded.req_id,
            ..Default::default()
        };
        provider_entries_into_frame(&mut frame, decoded.entries);
        frame
    })
}

#[wasm_bindgen]
pub fn ft_decode_provider_notify(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_provider_notify(data).map(|entries| {
        let mut frame = FanoutTreeDecodedFrame::default();
        provider_entries_into_frame(&mut frame, entries);
        frame
    })
}

#[wasm_bindgen]
pub fn ft_decode_provider_subscribe(data: &[u8]) -> Option<FanoutTreeDecodedFrame> {
    fanout_tree::decode_provider_subscribe(data).map(|decoded| FanoutTreeDecodedFrame {
        want: decoded.want as u32,
        ttl_ms: decoded.ttl_ms,
        ..Default::default()
    })
}

/// `normalizeParentUpgradePolicy` over the fixed-order f64 protocol
/// documented in `fanout_tree.rs` (NaN = unset numeric option, -1/0/1
/// tri-state booleans, mode 0 unset / 1 direct / 2 probe / 3 shadow).
#[wasm_bindgen]
pub fn ft_pu_normalize_policy(options: Vec<f64>) -> Vec<f64> {
    fanout_tree::normalize_parent_upgrade_policy(&options)
}

/// `evaluateParentUpgradeGate`; returns the skip-reason code in the low
/// byte (0 = run) plus the retry-after-seq reset flag (0x100).
#[allow(clippy::too_many_arguments)]
#[wasm_bindgen]
pub fn ft_pu_evaluate_gate(
    children_size: f64,
    missing_seqs_size: f64,
    last_repair_sent_at: f64,
    end_seq_exclusive: f64,
    parent_upgrade_retry_after_seq: f64,
    max_seq_seen: f64,
    parent_upgrade_count: f64,
    parent_upgrade_backoff_until: f64,
    parent_upgrade_last_at: f64,
    last_parent_data_at: f64,
    last_parent_upgrade_activity_at: f64,
    leaf_only: bool,
    repair_guard: bool,
    data_guard: bool,
    ended_and_complete: bool,
    max_per_peer: f64,
    cooldown_ms: f64,
    quiet_ms: f64,
    repair_quiet_ms: f64,
    now: f64,
) -> u32 {
    fanout_tree::evaluate_parent_upgrade_gate(
        &fanout_tree::ParentUpgradeGateState {
            children_size,
            missing_seqs_size,
            last_repair_sent_at,
            end_seq_exclusive,
            parent_upgrade_retry_after_seq,
            max_seq_seen,
            parent_upgrade_count,
            parent_upgrade_backoff_until,
            parent_upgrade_last_at,
            last_parent_data_at,
            last_parent_upgrade_activity_at,
        },
        &fanout_tree::ParentUpgradeGateOptions {
            leaf_only,
            repair_guard,
            data_guard,
            ended_and_complete,
            max_per_peer,
            cooldown_ms,
            quiet_ms,
            repair_quiet_ms,
            now,
        },
    )
}
