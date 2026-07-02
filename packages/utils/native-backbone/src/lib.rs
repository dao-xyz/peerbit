use js_sys::{Array, Uint8Array};
use peerbit_indexer_core::planner::NativeQueryIndex;
use peerbit_indexer_core::schema::NativeSchemaIr;
use peerbit_indexer_core::storage::MemoryByteStorage;
use peerbit_log_rust::{NativeEntryV0PlainBuilder, NativeLogBlockStore, NativeLogIndex};
use peerbit_shared_log_rust::NativeSharedLogState;
use std::collections::{HashMap, HashSet};
use wasm_bindgen::prelude::*;

mod append_tx;
mod coordinates;
mod documents;
mod graph_blocks;
mod js_interop;
mod profile;
mod raw_receive;
mod shared_log_plan;

use crate::documents::{DocumentContextFields, DocumentPreviousSignerFact, ParsedProjectionPlan};
use crate::profile::NativeBackboneAppendProfile;
use crate::raw_receive::PendingRawReceiveEntry;

// Native-backbone is optimized for document-store hot paths where large byte
// fields are payloads, not query keys. Standalone indexer-rust keeps exact large
// byte matching through the compatibility extractor.
const NATIVE_BACKBONE_BYTE_EXACT_INDEX_LIMIT: usize = 128;

#[wasm_bindgen]
pub struct NativePeerbitBackbone {
    resolution: String,
    log: NativeLogIndex,
    blocks: NativeLogBlockStore,
    shared_log: NativeSharedLogState,
    coordinate_index: HashSet<String>,
    coordinate_values: MemoryByteStorage,
    coordinate_journal: Vec<u8>,
    coordinate_journal_record_count: usize,
    coordinate_journal_enabled: bool,
    document_index: NativeQueryIndex,
    document_values: MemoryByteStorage,
    document_journal: Vec<u8>,
    document_journal_record_count: usize,
    document_journal_enabled: bool,
    document_byte_element_index_limit: usize,
    document_key_by_head: HashMap<String, String>,
    document_previous_signer_by_key: HashMap<String, DocumentPreviousSignerFact>,
    document_signer_journal: Vec<u8>,
    document_signer_journal_record_count: usize,
    document_signer_journal_enabled: bool,
    document_schema_ir: Option<NativeSchemaIr>,
    document_context_head_field: Option<u32>,
    document_context_fields: Option<DocumentContextFields>,
    document_projection_plans: Vec<ParsedProjectionPlan>,
    local_public_key: Vec<u8>,
    builder: NativeEntryV0PlainBuilder,
    pending_raw_receive_entries: HashMap<String, PendingRawReceiveEntry>,
    append_profile_enabled: bool,
    append_profile: NativeBackboneAppendProfile,
}

#[wasm_bindgen]
impl NativePeerbitBackbone {
    #[wasm_bindgen(constructor)]
    pub fn new(
        resolution: String,
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
    ) -> Result<Self, JsValue> {
        if resolution != "u32" && resolution != "u64" {
            return Err(JsValue::from_str("resolution must be u32 or u64"));
        }
        let local_public_key = public_key.to_vec();
        Ok(Self {
            resolution: resolution.clone(),
            log: NativeLogIndex::new(),
            blocks: NativeLogBlockStore::new(),
            shared_log: NativeSharedLogState::new(resolution),
            coordinate_index: HashSet::new(),
            coordinate_values: MemoryByteStorage::new(),
            coordinate_journal: Vec::new(),
            coordinate_journal_record_count: 0,
            coordinate_journal_enabled: false,
            document_index: NativeQueryIndex::new(),
            document_values: MemoryByteStorage::new(),
            document_journal: Vec::new(),
            document_journal_record_count: 0,
            document_journal_enabled: false,
            document_byte_element_index_limit: 0,
            document_key_by_head: HashMap::new(),
            document_previous_signer_by_key: HashMap::new(),
            document_signer_journal: Vec::new(),
            document_signer_journal_record_count: 0,
            document_signer_journal_enabled: false,
            document_schema_ir: None,
            document_context_head_field: None,
            document_context_fields: None,
            document_projection_plans: Vec::new(),
            local_public_key,
            builder: NativeEntryV0PlainBuilder::new(clock_id, private_key, public_key)?,
            pending_raw_receive_entries: HashMap::new(),
            append_profile_enabled: false,
            append_profile: NativeBackboneAppendProfile::default(),
        })
    }

    pub fn set_append_profile_enabled(&mut self, enabled: bool) {
        self.append_profile_enabled = enabled;
    }

    pub fn reset_append_profile(&mut self) {
        self.append_profile = NativeBackboneAppendProfile::default();
    }

    pub fn append_profile(&self) -> Array {
        self.append_profile.to_row()
    }

    pub fn log_len(&self) -> usize {
        self.log.len()
    }

    pub fn block_len(&self) -> usize {
        self.blocks.len()
    }

    pub fn has_log_entry(&self, hash: &str) -> bool {
        self.log.has(hash)
    }

    pub fn has_block(&self, hash: &str) -> bool {
        self.blocks.has(hash)
    }

    pub fn clear(&mut self) {
        self.log.clear();
        self.blocks.clear();
        self.shared_log.clear();
        self.clear_coordinate_core();
        self.clear_document_core();
    }
}
