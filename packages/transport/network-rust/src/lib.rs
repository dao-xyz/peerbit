//! wasm-bindgen surface for the `peerbit_wire` envelope codec.
//!
//! All decode/verify logic lives in the `JsValue`-free `wire` module so it
//! can run under host `cargo test`; this file only translates across the
//! wasm boundary.

pub mod direct_stream;
pub mod sync_payload;
pub mod wire;

use js_sys::{Array, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use direct_stream::lanes::{LaneScheduler, PushOutcome};
use direct_stream::routes::{AddOutcome, Routes};
use direct_stream::seen_cache::SeenCache;
use direct_stream::{decisions, routes};
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
