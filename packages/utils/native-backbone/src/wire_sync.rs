//! Receive fusion for shared-log raw exchange-heads sync.
//!
//! The `peerbit_wire` envelope codec is compiled into this wasm module (rather
//! than the standalone `@peerbit/network-rust` module) so the single boundary
//! write that `decode_and_verify_batch` already performs — copying the inbound
//! frame into wasm linear memory — can be shared with the native-backbone
//! prepared-raw-receive pipeline. Frames whose DataMessage payload is a
//! shared-log RPC `RawExchangeHeadsMessage([0,7])` on a registered topic are
//! kept ("stashed") in that memory; the entry block bytes are later fed into
//! `prepare_raw_receive_*` as in-memory slices instead of a second
//! JS→wasm copy, and JS never materializes the borsh `RawEntryWithRefs`.
//!
//! One [`NativeWireSyncSession`] belongs to one node: its DirectStream feeds
//! `decode_and_verify_batch` and its shared-log programs register their topics
//! and consume stashed payloads by message id.
//!
//! The stash core is `JsValue`-free so host `cargo test` covers the decision
//! and eviction logic.

use js_sys::{Array, Uint32Array, Uint8Array};
use peerbit_wire::sync_payload::{
    parse_pubsub_data, parse_raw_exchange_rpc_request, SyncPayloadHead,
};
use peerbit_wire::wire::{
    decode_and_verify_frames, decode_frame_delivery_meta, DeliveryMode, FrameRecord, VerifyStatus,
    ID_LENGTH, VARIANT_DATA,
};
use peerbit_wire::{record_to_words, RECORD_FLAG_SYNC_STASHED, RECORD_WORDS};
use std::collections::{HashMap, VecDeque};
use wasm_bindgen::prelude::*;

use crate::js_interop::strings_slice_to_array;
use crate::NativePeerbitBackbone;

/// Bounds for never-consumed stash entries (a message can be stashed at the
/// wire level and then dropped before program dispatch, e.g. by the seen
/// cache). FIFO-evicted; eviction only costs the fused fast path — the TS
/// fallback still processes the message.
pub(crate) const WIRE_SYNC_MAX_STASHED_MESSAGES: usize = 512;
pub(crate) const WIRE_SYNC_MAX_STASHED_BYTES: usize = 64 * 1024 * 1024;

pub(crate) struct StashedSyncMessage {
    frame: Vec<u8>,
    /// Head byte offsets are absolute within `frame`.
    heads: Vec<SyncPayloadHead>,
    reserved: [u8; 4],
    payload_length: usize,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WireSyncCounters {
    pub stashed: u32,
    pub evicted: u32,
    pub meta_reads: u32,
    pub block_copy_outs: u32,
    pub released: u32,
}

#[derive(Default)]
pub(crate) struct WireSyncCore {
    self_hash: String,
    topic_refs: HashMap<String, usize>,
    stash: HashMap<[u8; ID_LENGTH], StashedSyncMessage>,
    order: VecDeque<[u8; ID_LENGTH]>,
    stashed_bytes: usize,
    pub(crate) counters: WireSyncCounters,
}

impl WireSyncCore {
    pub(crate) fn new(self_hash: String) -> Self {
        WireSyncCore {
            self_hash,
            ..WireSyncCore::default()
        }
    }

    pub(crate) fn register_topic(&mut self, topic: String) {
        *self.topic_refs.entry(topic).or_insert(0) += 1;
    }

    pub(crate) fn unregister_topic(&mut self, topic: &str) -> bool {
        match self.topic_refs.get_mut(topic) {
            Some(count) if *count > 1 => {
                *count -= 1;
                true
            }
            Some(_) => {
                self.topic_refs.remove(topic);
                true
            }
            None => false,
        }
    }

    pub(crate) fn topic_count(&self) -> usize {
        self.topic_refs.len()
    }

    fn delivered_locally(&self, mode: Option<&DeliveryMode>) -> bool {
        // Mirrors TopicControlPlane.onDataMessage: explicit receivers must
        // include this node; AnyWhere modes are always delivered locally.
        match mode {
            Some(DeliveryMode::Silent { to, .. }) | Some(DeliveryMode::Acknowledge { to, .. }) => {
                to.iter().any(|hash| hash == &self.self_hash)
            }
            Some(DeliveryMode::AnyWhere) | Some(DeliveryMode::AcknowledgeAnyWhere { .. }) => true,
            Some(DeliveryMode::Traced { .. }) | None => false,
        }
    }

    /// Try to stash a decoded-and-verified DataMessage frame. Returns `true`
    /// when the frame carried a raw exchange sync payload for a registered
    /// topic addressed to this node.
    pub(crate) fn try_stash(
        &mut self,
        frame: &mut Option<Vec<u8>>,
        data_offset: usize,
        data_length: usize,
    ) -> bool {
        if self.topic_refs.is_empty() {
            return false;
        }
        let Some(frame_bytes) = frame.as_deref() else {
            return false;
        };
        let Some(payload) = frame_bytes.get(data_offset..data_offset + data_length) else {
            return false;
        };
        let Ok(pubsub) = parse_pubsub_data(payload) else {
            return false;
        };
        if !pubsub
            .topics
            .iter()
            .any(|topic| self.topic_refs.contains_key(topic))
        {
            return false;
        }
        let Some(data) = payload.get(pubsub.data_offset..pubsub.data_offset + pubsub.data_length)
        else {
            return false;
        };
        let Ok(parsed) = parse_raw_exchange_rpc_request(data) else {
            return false;
        };
        let Ok(meta) = decode_frame_delivery_meta(frame_bytes) else {
            return false;
        };
        if meta.variant != VARIANT_DATA || !self.delivered_locally(meta.mode.as_ref()) {
            return false;
        }

        let heads = parsed
            .heads
            .into_iter()
            .map(|head| SyncPayloadHead {
                // Translate payload-relative offsets to frame-absolute ones.
                bytes_offset: data_offset + pubsub.data_offset + head.bytes_offset,
                ..head
            })
            .collect();
        let frame = frame.take().expect("frame checked above");
        let frame_length = frame.len();
        if let Some(previous) = self.stash.insert(
            meta.id,
            StashedSyncMessage {
                frame,
                heads,
                reserved: parsed.reserved,
                payload_length: data_length,
            },
        ) {
            self.stashed_bytes -= previous.frame.len();
            self.order.retain(|id| id != &meta.id);
        }
        self.stashed_bytes += frame_length;
        self.order.push_back(meta.id);
        self.counters.stashed += 1;
        while self.stash.len() > WIRE_SYNC_MAX_STASHED_MESSAGES
            || self.stashed_bytes > WIRE_SYNC_MAX_STASHED_BYTES
        {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.stash.remove(&oldest) {
                self.stashed_bytes -= evicted.frame.len();
                self.counters.evicted += 1;
            }
        }
        true
    }

    pub(crate) fn get(&self, id: &[u8]) -> Option<&StashedSyncMessage> {
        let id: &[u8; ID_LENGTH] = id.try_into().ok()?;
        self.stash.get(id)
    }

    pub(crate) fn release(&mut self, id: &[u8]) -> bool {
        let Ok(id) = <&[u8; ID_LENGTH]>::try_from(id) else {
            return false;
        };
        if let Some(removed) = self.stash.remove(id) {
            self.stashed_bytes -= removed.frame.len();
            self.order.retain(|entry| entry != id);
            self.counters.released += 1;
            true
        } else {
            false
        }
    }

    pub(crate) fn stash_len(&self) -> usize {
        self.stash.len()
    }

    /// Copy the selected head block bytes out of a stashed message. `indexes`
    /// of `None` selects every head. This is the in-wasm handoff used by the
    /// stashed prepare entry points and — as `Uint8Array`s — the JS fallback.
    pub(crate) fn blocks(&self, id: &[u8], indexes: Option<&[u32]>) -> Option<Vec<Vec<u8>>> {
        let stashed = self.get(id)?;
        let select = |head: &SyncPayloadHead| {
            stashed
                .frame
                .get(head.bytes_offset..head.bytes_offset + head.bytes_length)
                .map(<[u8]>::to_vec)
        };
        match indexes {
            Some(indexes) => indexes
                .iter()
                .map(|index| stashed.heads.get(*index as usize).and_then(select))
                .collect(),
            None => stashed.heads.iter().map(select).collect(),
        }
    }
}

#[cfg(test)]
impl StashedSyncMessage {
    pub(crate) fn head_count(&self) -> usize {
        self.heads.len()
    }

    pub(crate) fn head(&self, index: usize) -> Option<&SyncPayloadHead> {
        self.heads.get(index)
    }

    pub(crate) fn reserved(&self) -> [u8; 4] {
        self.reserved
    }
}

/// Per-node receive-fusion state: the fused wire decoder for DirectStream and
/// the stash consumed by shared-log programs. See the module docs.
#[wasm_bindgen]
pub struct NativeWireSyncSession {
    pub(crate) core: WireSyncCore,
}

#[wasm_bindgen]
impl NativeWireSyncSession {
    #[wasm_bindgen(constructor)]
    pub fn new(self_hash: String) -> Self {
        NativeWireSyncSession {
            core: WireSyncCore::new(self_hash),
        }
    }

    pub fn register_topic(&mut self, topic: String) {
        self.core.register_topic(topic);
    }

    pub fn unregister_topic(&mut self, topic: &str) -> bool {
        self.core.unregister_topic(topic)
    }

    pub fn topic_count(&self) -> usize {
        self.core.topic_count()
    }

    /// Drop-in replacement for `peerbit_wire`'s `decode_and_verify_batch`
    /// (same flat u32 record layout) that additionally stashes raw exchange
    /// sync payloads for registered topics, flagging their records with
    /// `RECORD_FLAG_SYNC_STASHED`.
    pub fn decode_and_verify_batch(&mut self, frames: Array, now_ms: f64) -> Vec<u32> {
        let mut buffers: Vec<Option<Vec<u8>>> = frames
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
        let records = decode_and_verify_frames(&slices, now_ms as u64);
        let mut words = Vec::with_capacity(records.len() * RECORD_WORDS);
        for (record, buffer) in records.iter().zip(buffers.iter_mut()) {
            let stashed = record_is_stash_candidate(record)
                && self.core.try_stash(
                    buffer,
                    record.data_offset as usize,
                    record.data_length as usize,
                );
            record_to_words(record, &mut words);
            if stashed {
                let flag_word = words.len() - RECORD_WORDS;
                words[flag_word] |= RECORD_FLAG_SYNC_STASHED;
            }
        }
        words
    }

    /// Stash facts for a message id: `[hashes, gidRefrences, byteLengths,
    /// reserved, payloadLength]`, or `undefined` when not stashed. Does not
    /// consume the entry — call `release` when processing finishes.
    pub fn stashed_meta(&mut self, id: &[u8]) -> JsValue {
        if self.core.get(id).is_none() {
            return JsValue::UNDEFINED;
        }
        self.core.counters.meta_reads += 1;
        let stashed = self.core.get(id).expect("checked above");
        let hashes = Array::new();
        let gid_refrences = Array::new();
        let mut byte_lengths: Vec<u32> = Vec::with_capacity(stashed.heads.len());
        for head in &stashed.heads {
            hashes.push(&JsValue::from_str(&head.hash));
            gid_refrences.push(&strings_slice_to_array(&head.gid_refrences));
            byte_lengths.push(head.bytes_length as u32);
        }
        let out = Array::new();
        out.push(&hashes);
        out.push(&gid_refrences);
        out.push(&Uint32Array::from(byte_lengths.as_slice()));
        out.push(&Uint8Array::from(stashed.reserved.as_slice()));
        out.push(&JsValue::from_f64(stashed.payload_length as f64));
        out.into()
    }

    /// Copy head block bytes out to JS (fallback paths only — the fused path
    /// hands blocks to `prepare_stashed_raw_receive_*` inside wasm memory).
    pub fn stashed_blocks(&mut self, id: &[u8], indexes: Option<Uint32Array>) -> JsValue {
        let indexes = indexes.map(|indexes| indexes.to_vec());
        let Some(blocks) = self.core.blocks(id, indexes.as_deref()) else {
            return JsValue::UNDEFINED;
        };
        self.core.counters.block_copy_outs += blocks.len() as u32;
        let out = Array::new();
        for block in blocks {
            out.push(&Uint8Array::from(block.as_slice()));
        }
        out.into()
    }

    pub fn release(&mut self, id: &[u8]) -> bool {
        self.core.release(id)
    }

    pub fn stash_len(&self) -> usize {
        self.core.stash_len()
    }

    /// `[stashed, evicted, metaReads, blockCopyOuts, released]`.
    pub fn counters(&self) -> Vec<u32> {
        let counters = &self.core.counters;
        vec![
            counters.stashed,
            counters.evicted,
            counters.meta_reads,
            counters.block_copy_outs,
            counters.released,
        ]
    }
}

fn record_is_stash_candidate(record: &FrameRecord) -> bool {
    record.decode_ok
        && record.variant == VARIANT_DATA
        && record.has_data
        && record.verify == VerifyStatus::Verified
}

#[wasm_bindgen]
impl NativePeerbitBackbone {
    /// Stashed-input twin of
    /// `prepare_raw_receive_unverified_expected_compact_columns_batch`: the
    /// blocks come from the wire stash (wasm memory) instead of a JS array.
    pub fn prepare_stashed_raw_receive_expected_compact_columns_batch(
        &mut self,
        session: &NativeWireSyncSession,
        id: &[u8],
        indexes: Uint32Array,
        hashes: Array,
        verify_signatures: bool,
    ) -> Result<JsValue, JsValue> {
        let indexes = indexes.to_vec();
        let Some(blocks) = session.core.blocks(id, Some(&indexes)) else {
            return Ok(JsValue::UNDEFINED);
        };
        let hashes = crate::js_interop::strings_from_array(hashes)?;
        crate::js_interop::ensure_same_len(blocks.len(), hashes.len(), "stashed raw receive")?;
        let prepared = self.prepare_raw_receive_entries(blocks, Some(hashes), verify_signatures)?;
        Ok(self
            .prepare_raw_receive_columns_from_entries(prepared, false, false)?
            .into())
    }

    /// Stashed-input twin of
    /// `prepare_raw_receive_unverified_expected_compact_columns_and_selection_batch`.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_stashed_raw_receive_expected_compact_columns_and_selection_batch(
        &mut self,
        session: &NativeWireSyncSession,
        id: &[u8],
        indexes: Uint32Array,
        hashes: Array,
        min_replicas: u32,
        max_replicas: JsValue,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
        _from_hash: String,
    ) -> Result<JsValue, JsValue> {
        let indexes = indexes.to_vec();
        let Some(blocks) = session.core.blocks(id, Some(&indexes)) else {
            return Ok(JsValue::UNDEFINED);
        };
        let hashes = crate::js_interop::strings_from_array(hashes)?;
        crate::js_interop::ensure_same_len(blocks.len(), hashes.len(), "stashed raw receive")?;
        let prepared = self.prepare_raw_receive_entries(blocks, Some(hashes.clone()), false)?;
        let columns = self.prepare_raw_receive_columns_from_entries(prepared, false, false)?;
        let selection = self.plan_prepared_raw_receive_selection_core(
            hashes,
            min_replicas,
            max_replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        let out = Array::new();
        out.push(&columns);
        out.push(&selection);
        Ok(out.into())
    }

    /// Sync fallback for lazily materialized stash-backed heads whose stash
    /// entry was already released: serve the raw block bytes from the pending
    /// prepared entries or the committed block store.
    pub fn raw_receive_block_bytes(&self, hash: &str) -> JsValue {
        if let Some(pending) = self.pending_raw_receive_entries.get(hash) {
            return Uint8Array::from(pending.storage_bytes()).into();
        }
        match self.blocks.get_ref(hash) {
            Some(bytes) => Uint8Array::from(bytes).into(),
            None => JsValue::UNDEFINED,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use peerbit_wire::sync_payload::encode_raw_exchange_sync_payload;
    use peerbit_wire::wire::{encode_frame, MessageHeader, WireMessage};

    fn sync_frame(
        id_byte: u8,
        topic: &str,
        mode: Option<DeliveryMode>,
        heads: &[(String, Vec<u8>, Vec<String>)],
    ) -> (Vec<u8>, usize, usize) {
        let payload =
            encode_raw_exchange_sync_payload(&[topic.to_string()], true, heads, [0, 0, 0, 0]);
        let message = WireMessage::Data {
            header: MessageHeader {
                id: [id_byte; ID_LENGTH],
                timestamp: 1,
                session: 2,
                expires: u64::MAX,
                priority: Some(0),
                response_priority: None,
                origin: None,
                mode,
                signatures: Some(Vec::new()),
            },
            data: Some(payload.clone()),
        };
        let frame = encode_frame(&message);
        let data_offset = frame.len() - payload.len();
        (frame, data_offset, payload.len())
    }

    fn heads() -> Vec<(String, Vec<u8>, Vec<String>)> {
        vec![
            ("h0".to_string(), vec![1, 2, 3], Vec::new()),
            ("h1".to_string(), vec![4, 5], vec!["g".to_string()]),
        ]
    }

    fn silent_to_self() -> Option<DeliveryMode> {
        Some(DeliveryMode::Silent {
            to: vec!["self-hash".to_string()],
            redundancy: 1,
        })
    }

    #[test]
    fn stashes_registered_topic_frames_addressed_to_self() {
        let mut core = WireSyncCore::new("self-hash".to_string());
        core.register_topic("topic".to_string());
        let (frame, data_offset, data_length) = sync_frame(7, "topic", silent_to_self(), &heads());
        let mut buffer = Some(frame.clone());
        assert!(core.try_stash(&mut buffer, data_offset, data_length));
        assert!(buffer.is_none(), "stash takes frame ownership");
        assert_eq!(core.stash_len(), 1);

        let stashed = core.get(&[7u8; ID_LENGTH]).unwrap();
        assert_eq!(stashed.head_count(), 2);
        assert_eq!(stashed.head(0).unwrap().hash, "h0");
        assert_eq!(stashed.reserved(), [0, 0, 0, 0]);
        let blocks = core.blocks(&[7u8; ID_LENGTH], None).unwrap();
        assert_eq!(blocks, vec![vec![1, 2, 3], vec![4, 5]]);
        let selected = core.blocks(&[7u8; ID_LENGTH], Some(&[1])).unwrap();
        assert_eq!(selected, vec![vec![4, 5]]);

        assert!(core.release(&[7u8; ID_LENGTH]));
        assert_eq!(core.stash_len(), 0);
        assert!(!core.release(&[7u8; ID_LENGTH]));
    }

    #[test]
    fn skips_unregistered_topics_and_foreign_recipients() {
        let mut core = WireSyncCore::new("self-hash".to_string());
        core.register_topic("topic".to_string());

        let (frame, data_offset, data_length) =
            sync_frame(1, "other-topic", silent_to_self(), &heads());
        let mut buffer = Some(frame);
        assert!(!core.try_stash(&mut buffer, data_offset, data_length));
        assert!(buffer.is_some(), "rejected frames keep their buffer");

        let relay_mode = Some(DeliveryMode::Silent {
            to: vec!["someone-else".to_string()],
            redundancy: 1,
        });
        let (frame, data_offset, data_length) = sync_frame(2, "topic", relay_mode, &heads());
        let mut buffer = Some(frame);
        assert!(!core.try_stash(&mut buffer, data_offset, data_length));

        let (frame, data_offset, data_length) =
            sync_frame(3, "topic", Some(DeliveryMode::AnyWhere), &heads());
        let mut buffer = Some(frame);
        assert!(core.try_stash(&mut buffer, data_offset, data_length));

        core.unregister_topic("topic");
        let (frame, data_offset, data_length) = sync_frame(4, "topic", silent_to_self(), &heads());
        let mut buffer = Some(frame);
        assert!(!core.try_stash(&mut buffer, data_offset, data_length));
    }

    #[test]
    fn topic_registration_is_refcounted() {
        let mut core = WireSyncCore::new("self-hash".to_string());
        core.register_topic("topic".to_string());
        core.register_topic("topic".to_string());
        assert!(core.unregister_topic("topic"));
        assert_eq!(core.topic_count(), 1);
        assert!(core.unregister_topic("topic"));
        assert_eq!(core.topic_count(), 0);
        assert!(!core.unregister_topic("topic"));
    }

    #[test]
    fn evicts_oldest_when_over_message_cap() {
        let mut core = WireSyncCore::new("self-hash".to_string());
        core.register_topic("topic".to_string());
        for index in 0..(WIRE_SYNC_MAX_STASHED_MESSAGES + 3) {
            let (frame, data_offset, data_length) =
                sync_frame(index as u8, "topic", silent_to_self(), &heads());
            // ids repeat every 256 messages; use distinct high bytes instead
            let mut frame = frame;
            frame[2] = (index >> 8) as u8; // second byte of the 32-byte id
            let mut buffer = Some(frame);
            assert!(core.try_stash(&mut buffer, data_offset, data_length));
        }
        assert_eq!(core.stash_len(), WIRE_SYNC_MAX_STASHED_MESSAGES);
        assert_eq!(core.counters.evicted, 3);
    }

    #[test]
    fn restashing_same_id_replaces_entry() {
        let mut core = WireSyncCore::new("self-hash".to_string());
        core.register_topic("topic".to_string());
        let (frame, data_offset, data_length) = sync_frame(9, "topic", silent_to_self(), &heads());
        let mut buffer = Some(frame.clone());
        assert!(core.try_stash(&mut buffer, data_offset, data_length));
        let mut buffer = Some(frame);
        assert!(core.try_stash(&mut buffer, data_offset, data_length));
        assert_eq!(core.stash_len(), 1);
        assert_eq!(core.counters.stashed, 2);
    }
}
