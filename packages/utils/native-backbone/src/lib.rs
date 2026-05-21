use js_sys::{Array, BigUint64Array, Reflect, Uint32Array, Uint8Array};
use peerbit_indexer_core::codec::{decode_query, decode_sort};
use peerbit_indexer_core::persistence::{
    decode_journal, decode_key_value_snapshot, encode_journal_delete_record,
    encode_journal_put_record, encode_key_value_snapshot, JournalRecord, JOURNAL_MAGIC,
};
use peerbit_indexer_core::planner::{FieldPath, FieldValue, NativeQueryIndex, SumResult};
use peerbit_indexer_core::schema::{
    decode_native_schema_ir, extract_encoded_document_fields_from_parts_with_byte_limits,
    NativeSchemaIr,
};
use peerbit_indexer_core::storage::{ByteStorage, MemoryByteStorage};
use peerbit_log_rust::{
    LogIndexEntry, NativeCommittedEntryFacts, NativeEntryV0PlainBuilder, NativeLogAppendProfile,
    NativeLogBlockStore, NativeLogIndex,
};
use peerbit_shared_log_rust::{
    commit_local_append_for_gid_compact_core, NativeLocalAppendCompactFacts, NativeSharedLogState,
};
use std::collections::{HashMap, HashSet};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

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
    document_schema_ir: Option<NativeSchemaIr>,
    document_context_head_field: Option<u32>,
    document_projection_plans: Vec<ParsedProjectionPlan>,
    builder: NativeEntryV0PlainBuilder,
    append_profile_enabled: bool,
    append_profile: NativeBackboneAppendProfile,
}

struct DocumentIndexAppendCommit {
    key: String,
    value_prefix: DocumentIndexValuePrefix,
    existing_created: Option<u64>,
    byte_element_index_limit: usize,
    delete_trimmed_heads: bool,
}

struct ParsedProjectionPlan {
    document_variant_type: Option<String>,
    document_variant_value: Option<String>,
    output_variant_type: Option<String>,
    output_variant_value: Option<String>,
    document_field_names: Vec<String>,
    document_field_types: Vec<String>,
    output_field_types: Vec<String>,
    source_kinds: Vec<String>,
    source_values: Vec<String>,
}

enum DocumentIndexValuePrefix {
    Bytes(Vec<u8>),
    Projection {
        encoded_document: Vec<u8>,
        plan: DocumentIndexProjectionPlan,
        signer: Option<Vec<u8>>,
    },
}

enum DocumentIndexProjectionPlan {
    Inline(ParsedProjectionPlan),
    Cached(usize),
}

#[derive(Clone, Default)]
struct NativeBackboneAppendProfile {
    storage_append_inner_ms: f64,
    input_copy_ms: f64,
    log_total_ms: f64,
    log_next_clone_ms: f64,
    log_entry_core_ms: f64,
    log_encode_meta_ms: f64,
    log_encode_payload_ms: f64,
    log_encode_signable_ms: f64,
    log_sign_ms: f64,
    log_encode_signature_ms: f64,
    log_encode_storage_ms: f64,
    log_cid_ms: f64,
    log_cid_hash_ms: f64,
    log_cid_string_ms: f64,
    log_index_entry_ms: f64,
    log_facts_ms: f64,
    log_block_put_ms: f64,
    log_graph_put_ms: f64,
    log_trim_ms: f64,
    entry_row_ms: f64,
    trim_rows_ms: f64,
    hash_number_ms: f64,
    coordinate_plan_ms: f64,
    coordinate_core_ms: f64,
    coordinate_fields_build_ms: f64,
    coordinate_value_encode_ms: f64,
    coordinate_journal_put_ms: f64,
    coordinate_index_put_ms: f64,
    coordinate_value_put_ms: f64,
    coordinate_delete_ms: f64,
    document_index_commit_ms: f64,
    document_index_context_encode_ms: f64,
    document_index_extract_ms: f64,
    document_index_value_build_ms: f64,
    document_index_put_ms: f64,
    document_value_put_ms: f64,
    result_row_ms: f64,
}

impl NativeBackboneAppendProfile {
    fn add_log_profile(&mut self, profile: &NativeLogAppendProfile) {
        self.log_next_clone_ms += profile.next_clone_ms;
        self.log_entry_core_ms += profile.entry_core_ms;
        self.log_encode_meta_ms += profile.encode_meta_ms;
        self.log_encode_payload_ms += profile.encode_payload_ms;
        self.log_encode_signable_ms += profile.encode_signable_ms;
        self.log_sign_ms += profile.sign_ms;
        self.log_encode_signature_ms += profile.encode_signature_ms;
        self.log_encode_storage_ms += profile.encode_storage_ms;
        self.log_cid_ms += profile.cid_ms;
        self.log_cid_hash_ms += profile.cid_hash_ms;
        self.log_cid_string_ms += profile.cid_string_ms;
        self.log_index_entry_ms += profile.index_entry_ms;
        self.log_facts_ms += profile.facts_ms;
        self.log_block_put_ms += profile.block_put_ms;
        self.log_graph_put_ms += profile.graph_put_ms;
        self.log_trim_ms += profile.trim_ms;
    }

    fn to_row(&self) -> Array {
        let row = Array::new();
        row.push(&JsValue::from_f64(self.storage_append_inner_ms));
        row.push(&JsValue::from_f64(self.input_copy_ms));
        row.push(&JsValue::from_f64(self.log_total_ms));
        row.push(&JsValue::from_f64(self.log_next_clone_ms));
        row.push(&JsValue::from_f64(self.log_entry_core_ms));
        row.push(&JsValue::from_f64(self.log_encode_meta_ms));
        row.push(&JsValue::from_f64(self.log_encode_payload_ms));
        row.push(&JsValue::from_f64(self.log_encode_signable_ms));
        row.push(&JsValue::from_f64(self.log_sign_ms));
        row.push(&JsValue::from_f64(self.log_encode_signature_ms));
        row.push(&JsValue::from_f64(self.log_encode_storage_ms));
        row.push(&JsValue::from_f64(self.log_cid_ms));
        row.push(&JsValue::from_f64(self.log_cid_hash_ms));
        row.push(&JsValue::from_f64(self.log_cid_string_ms));
        row.push(&JsValue::from_f64(self.log_index_entry_ms));
        row.push(&JsValue::from_f64(self.log_facts_ms));
        row.push(&JsValue::from_f64(self.log_block_put_ms));
        row.push(&JsValue::from_f64(self.log_graph_put_ms));
        row.push(&JsValue::from_f64(self.log_trim_ms));
        row.push(&JsValue::from_f64(self.entry_row_ms));
        row.push(&JsValue::from_f64(self.trim_rows_ms));
        row.push(&JsValue::from_f64(self.hash_number_ms));
        row.push(&JsValue::from_f64(self.coordinate_plan_ms));
        row.push(&JsValue::from_f64(self.coordinate_core_ms));
        row.push(&JsValue::from_f64(self.coordinate_fields_build_ms));
        row.push(&JsValue::from_f64(self.coordinate_value_encode_ms));
        row.push(&JsValue::from_f64(self.coordinate_journal_put_ms));
        row.push(&JsValue::from_f64(self.coordinate_index_put_ms));
        row.push(&JsValue::from_f64(self.coordinate_value_put_ms));
        row.push(&JsValue::from_f64(self.coordinate_delete_ms));
        row.push(&JsValue::from_f64(self.document_index_commit_ms));
        row.push(&JsValue::from_f64(self.document_index_context_encode_ms));
        row.push(&JsValue::from_f64(self.document_index_extract_ms));
        row.push(&JsValue::from_f64(self.document_index_value_build_ms));
        row.push(&JsValue::from_f64(self.document_index_put_ms));
        row.push(&JsValue::from_f64(self.document_value_put_ms));
        row.push(&JsValue::from_f64(self.result_row_ms));
        row
    }
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
            document_schema_ir: None,
            document_context_head_field: None,
            document_projection_plans: Vec::new(),
            builder: NativeEntryV0PlainBuilder::new(clock_id, private_key, public_key)?,
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

    pub fn entry_coordinate_hashes(&self) -> Array {
        self.shared_log.entry_coordinate_hashes()
    }

    pub fn get_entry_coordinates(&self, hash: &str) -> JsValue {
        self.shared_log.get_entry_coordinates(hash)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_leaders(
        &self,
        cursors: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.find_leaders(
            cursors,
            replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_leaders_batch(
        &self,
        cursor_batches: Array,
        replica_counts: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.find_leaders_batch(
            cursor_batches,
            replica_counts,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    pub fn get_grid(&self, from: String, count: usize) -> Result<Array, JsValue> {
        self.shared_log.get_grid(from, count)
    }

    pub fn get_gid_coordinates(&self, gid: String, count: usize) -> Array {
        self.shared_log.get_gid_coordinates(gid, count)
    }

    pub fn entry_hashes_for_hash_numbers(&self, hash_numbers: Array) -> Result<Array, JsValue> {
        self.shared_log.entry_hashes_for_hash_numbers(hash_numbers)
    }

    pub fn entry_hashes_for_hash_numbers_u64(
        &self,
        hash_numbers: BigUint64Array,
    ) -> Result<Array, JsValue> {
        self.shared_log
            .entry_hashes_for_hash_numbers_u64(hash_numbers)
    }

    pub fn entry_hashes_for_hash_numbers_flat_u64(&self, hash_numbers: BigUint64Array) -> Array {
        self.shared_log
            .entry_hashes_for_hash_numbers_flat_u64(hash_numbers)
    }

    pub fn entry_hash_numbers_in_range(
        &self,
        start1: String,
        end1: String,
        start2: String,
        end2: String,
    ) -> Result<Array, JsValue> {
        self.shared_log
            .entry_hash_numbers_in_range(start1, end1, start2, end2)
    }

    pub fn entry_hash_numbers_in_range_u64(
        &self,
        start1: String,
        end1: String,
        start2: String,
        end2: String,
    ) -> Result<BigUint64Array, JsValue> {
        self.shared_log
            .entry_hash_numbers_in_range_u64(start1, end1, start2, end2)
    }

    pub fn count_entry_coordinates_in_ranges(
        &self,
        start1: Array,
        end1: Array,
        start2: Array,
        end2: Array,
        include_assigned_to_range_boundary: bool,
    ) -> Result<usize, JsValue> {
        self.shared_log.count_entry_coordinates_in_ranges(
            start1,
            end1,
            start2,
            end2,
            include_assigned_to_range_boundary,
        )
    }

    pub fn entry_coordinate_fields(&self) -> Result<Array, JsValue> {
        let out = Array::new();
        for (_, value) in self.coordinate_values.entries() {
            let coordinate = decode_coordinate_value(&value)?;
            out.push(&coordinate_core_value_to_row(&coordinate));
        }
        Ok(out)
    }

    pub fn coordinate_index_len(&self) -> usize {
        self.coordinate_index.len()
    }

    pub fn coordinate_value_len(&self) -> usize {
        self.coordinate_values.len()
    }

    pub fn coordinate_index_has_hash(&self, hash: &str) -> bool {
        self.coordinate_index.contains(hash)
    }

    pub fn configure_document_schema_ir(
        &mut self,
        schema_ir_bytes: Vec<u8>,
    ) -> Result<Array, JsValue> {
        let schema_ir = decode_native_schema_ir(&schema_ir_bytes).map_err(js_error)?;
        let stats = schema_ir.stats();
        self.document_schema_ir = Some(schema_ir);
        let out = Array::new();
        out.push(&JsValue::from_f64(stats.root_fields as f64));
        out.push(&JsValue::from_f64(stats.node_count as f64));
        out.push(&JsValue::from_f64(stats.generic_nodes as f64));
        Ok(out)
    }

    pub fn set_document_context_head_field(&mut self, field: u32) {
        self.document_context_head_field = Some(field);
    }

    pub fn register_document_projection_plan(&mut self, plan: JsValue) -> Result<u32, JsValue> {
        let id = self.document_projection_plans.len();
        if id > u32::MAX as usize {
            return Err(JsValue::from_str("Too many document projection plans"));
        }
        self.document_projection_plans
            .push(parse_projection_plan(&plan)?);
        Ok(id as u32)
    }

    pub fn project_document_index_simple(
        &self,
        encoded_document: Uint8Array,
        plan: JsValue,
        created: &str,
        modified: &str,
        gid: &str,
        size: u32,
        signer: JsValue,
    ) -> Result<Uint8Array, JsValue> {
        let bytes = project_document_index_simple_bytes(
            &encoded_document.to_vec(),
            &plan,
            created,
            modified,
            gid,
            size,
            signer,
        )?;
        Ok(Uint8Array::from(bytes.as_slice()))
    }

    pub fn document_index_len(&self) -> usize {
        self.document_index.len()
    }

    pub fn document_value_len(&self) -> usize {
        self.document_values.len()
    }

    pub fn document_exact_string_first_key(&self, field: u32, value: String) -> JsValue {
        self.document_index
            .exact_first(&FieldPath::Id(field), &FieldValue::from(value))
            .map(|key| JsValue::from_str(&key))
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn document_index_has_exact_string(&self, field: u32, value: String, key: &str) -> bool {
        self.document_index
            .exact_first(&FieldPath::Id(field), &FieldValue::from(value))
            .is_some_and(|id| id == key)
    }

    pub fn document_value_bytes(&self, key: &str) -> JsValue {
        self.document_values
            .get(key)
            .map(|value| Uint8Array::from(value).into())
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn document_entry(&self, key: &str) -> JsValue {
        self.document_values
            .get(key)
            .map(|value| document_entry_to_row(key, value).into())
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn document_query(
        &self,
        query_bytes: Vec<u8>,
        sort_bytes: Vec<u8>,
    ) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes).map_err(js_error)?;
        let sort = decode_sort(&sort_bytes).map_err(js_error)?;
        let keys = self.document_index.search(&query, &sort, None);
        Ok(self.document_entries_for_keys(&keys))
    }

    pub fn document_query_page(
        &self,
        query_bytes: Vec<u8>,
        sort_bytes: Vec<u8>,
        offset: usize,
        limit: usize,
    ) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes).map_err(js_error)?;
        let sort = decode_sort(&sort_bytes).map_err(js_error)?;
        let keys = self
            .document_index
            .search_page(&query, &sort, offset, Some(limit));
        Ok(self.document_entries_for_keys(&keys))
    }

    pub fn document_count(&self, query_bytes: Vec<u8>) -> Result<usize, JsValue> {
        let query = decode_query(&query_bytes).map_err(js_error)?;
        Ok(self.document_index.count(&query) as usize)
    }

    pub fn document_sum(&self, query_bytes: Vec<u8>, field: u32) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes).map_err(js_error)?;
        let sum = self
            .document_index
            .sum(&query, FieldPath::Id(field))
            .map_err(js_error)?;
        Ok(sum_to_js(sum))
    }

    pub fn put_document_encoded_parts_stored(
        &mut self,
        key: String,
        mut value_prefix_bytes: Vec<u8>,
        value_suffix_bytes: Vec<u8>,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let extract_started = profile_enabled.then(js_sys::Date::now);
        let fields = {
            let schema_ir = self.document_schema_ir.as_ref().ok_or_else(|| {
                js_error("Native backbone document schema IR has not been configured")
            })?;
            extract_encoded_document_fields_from_parts_with_byte_limits(
                schema_ir,
                &value_prefix_bytes,
                &value_suffix_bytes,
                byte_element_index_limit,
                NATIVE_BACKBONE_BYTE_EXACT_INDEX_LIMIT,
            )
            .map_err(js_error)?
        };
        if let Some(started) = extract_started {
            self.append_profile.document_index_extract_ms += js_sys::Date::now() - started;
        }
        let value_build_started = profile_enabled.then(js_sys::Date::now);
        value_prefix_bytes.reserve(value_suffix_bytes.len());
        value_prefix_bytes.extend_from_slice(&value_suffix_bytes);
        if let Some(started) = value_build_started {
            self.append_profile.document_index_value_build_ms += js_sys::Date::now() - started;
        }
        let value_put_started = profile_enabled.then(js_sys::Date::now);
        let previous_value = self
            .document_values
            .put_return_previous(key.clone(), value_prefix_bytes);
        if let Some(started) = value_put_started {
            self.append_profile.document_value_put_ms += js_sys::Date::now() - started;
        }
        let index_put_started = profile_enabled.then(js_sys::Date::now);
        if previous_value.is_none() {
            self.document_index.put_new_unchecked(key, fields);
        } else {
            self.document_index.put(key, fields);
        }
        if let Some(started) = index_put_started {
            self.append_profile.document_index_put_ms += js_sys::Date::now() - started;
        }
        Ok(())
    }

    pub fn delete_document(&mut self, key: &str) -> bool {
        self.document_index.delete_id(key);
        self.document_values.delete(key)
    }

    pub fn clear_document_index(&mut self) {
        self.document_index.clear();
        self.document_values.clear();
    }

    pub fn coordinate_journal_header(&self) -> Vec<u8> {
        JOURNAL_MAGIC.to_vec()
    }

    pub fn coordinate_pending_journal_len(&self) -> usize {
        self.coordinate_journal_record_count
    }

    pub fn coordinate_pending_journal_byte_len(&self) -> usize {
        self.coordinate_journal.len()
    }

    pub fn coordinate_journal_enabled(&self) -> bool {
        self.coordinate_journal_enabled
    }

    pub fn set_coordinate_journal_enabled(&mut self, enabled: bool) {
        self.coordinate_journal_enabled = enabled;
        if !enabled {
            self.coordinate_journal.clear();
            self.coordinate_journal_record_count = 0;
        }
    }

    pub fn coordinate_journal(&self) -> Vec<u8> {
        self.coordinate_journal.clone()
    }

    pub fn clear_coordinate_journal(&mut self) {
        self.coordinate_journal.clear();
        self.coordinate_journal_record_count = 0;
    }

    pub fn drain_coordinate_journal(&mut self) -> Vec<u8> {
        self.coordinate_journal_record_count = 0;
        std::mem::take(&mut self.coordinate_journal)
    }

    pub fn coordinate_snapshot(&self) -> Vec<u8> {
        encode_key_value_snapshot(
            self.coordinate_values
                .entries()
                .into_iter()
                .map(|(key, value)| (key, value)),
        )
    }

    pub fn load_coordinate_snapshot_and_journal(
        &mut self,
        snapshot: Uint8Array,
        journal: Uint8Array,
    ) -> Result<usize, JsValue> {
        let mut entries = if snapshot.length() == 0 {
            Default::default()
        } else {
            decode_key_value_snapshot(&snapshot.to_vec()).map_err(decode_error)?
        };
        let journal_records = if journal.length() == 0 {
            Vec::new()
        } else {
            decode_journal(&journal.to_vec()).map_err(decode_error)?
        };
        let operations = journal_records.len();
        for record in journal_records {
            match record {
                JournalRecord {
                    key,
                    value: Some(value),
                    ..
                } => {
                    entries.insert(key, value);
                }
                JournalRecord { key, .. } => {
                    entries.shift_remove(&key);
                }
            }
        }

        self.shared_log.clear_entry_coordinates();
        self.clear_coordinate_core();
        for (_, value) in entries {
            let coordinate = decode_coordinate_value(&value)?;
            self.put_decoded_coordinate_core(coordinate, false)?;
        }
        self.coordinate_journal.clear();
        self.coordinate_journal_record_count = 0;
        Ok(operations)
    }

    pub fn graph_has_many(&self, hashes: Array) -> Result<Array, JsValue> {
        self.log.has_many(hashes)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn graph_put(
        &mut self,
        hash: String,
        gid: String,
        next: Array,
        entry_type: u8,
        wall_time: u64,
        logical: u32,
        payload_size: u32,
        head: bool,
        data: JsValue,
    ) -> Result<(), JsValue> {
        self.log.put(
            hash,
            gid,
            next,
            entry_type,
            wall_time,
            logical,
            payload_size,
            head,
            data,
        )
    }

    pub fn graph_put_batch(
        &mut self,
        hashes: Array,
        gids: Array,
        nexts: Array,
        entry_types: Uint8Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        payload_sizes: Uint32Array,
        heads: Uint8Array,
        datas: Array,
    ) -> Result<(), JsValue> {
        self.log.put_many(
            hashes,
            gids,
            nexts,
            entry_types,
            wall_times,
            logicals,
            payload_sizes,
            heads,
            datas,
        )
    }

    pub fn graph_put_append_chain(
        &mut self,
        hashes: Array,
        gid: String,
        initial_next: Array,
        entry_type: u8,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        payload_sizes: Uint32Array,
        datas: Array,
    ) -> Result<(), JsValue> {
        self.log.put_append_chain(
            hashes,
            gid,
            initial_next,
            entry_type,
            wall_times,
            logicals,
            payload_sizes,
            datas,
        )
    }

    pub fn commit_log_blocks_and_graph_batch(
        &mut self,
        hashes: Array,
        block_bytes: Array,
        gids: Array,
        nexts: Array,
        entry_types: Uint8Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        payload_sizes: Uint32Array,
        heads: Uint8Array,
        datas: Array,
    ) -> Result<(), JsValue> {
        self.blocks.put_many(hashes.clone(), block_bytes)?;
        self.log.put_many(
            hashes,
            gids,
            nexts,
            entry_types,
            wall_times,
            logicals,
            payload_sizes,
            heads,
            datas,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_log_blocks_graph_and_coordinates_batch(
        &mut self,
        hashes: Array,
        block_bytes: Array,
        gids: Array,
        nexts: Array,
        entry_types: Uint8Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        payload_sizes: Uint32Array,
        heads: Uint8Array,
        datas: Array,
        coordinate_hashes: Array,
        coordinate_gids: Array,
        coordinate_hash_numbers: Array,
        coordinate_batches: Array,
        coordinate_next_hash_batches: Array,
        coordinate_assigned_to_range_boundaries: Uint8Array,
        coordinate_requested_replicas: Array,
    ) -> Result<(), JsValue> {
        self.blocks.put_many(hashes.clone(), block_bytes)?;
        self.log.put_many(
            hashes,
            gids,
            nexts,
            entry_types,
            wall_times,
            logicals,
            payload_sizes,
            heads,
            datas,
        )?;
        if coordinate_hashes.length() > 0 {
            self.commit_entry_coordinates_batch(
                coordinate_hashes,
                coordinate_gids,
                coordinate_hash_numbers,
                coordinate_batches,
                coordinate_next_hash_batches,
                coordinate_assigned_to_range_boundaries,
                coordinate_requested_replicas,
            )?;
        }
        Ok(())
    }

    pub fn graph_delete(&mut self, hash: &str) -> bool {
        self.log.delete(hash)
    }

    pub fn graph_delete_many(&mut self, hashes: Array) -> Result<usize, JsValue> {
        self.log.delete_many(hashes)
    }

    pub fn graph_oldest_entries(&self, limit: usize) -> Array {
        self.log.oldest_entries(limit)
    }

    pub fn graph_heads(&self, gid: Option<String>) -> Array {
        self.log.heads(gid)
    }

    pub fn graph_has_head(&self, gid: Option<String>) -> bool {
        self.log.has_head(gid)
    }

    pub fn graph_has_any_head(&self, gids: Array) -> Result<bool, JsValue> {
        self.log.has_any_head(gids)
    }

    pub fn graph_has_any_head_batch(&self, gid_sets: Array) -> Result<Array, JsValue> {
        self.log.has_any_head_batch(gid_sets)
    }

    pub fn graph_head_entries(&self, gid: Option<String>) -> Array {
        self.log.head_entries(gid)
    }

    pub fn graph_head_data_entries(&self, gid: Option<String>) -> Array {
        self.log.head_data_entries(gid)
    }

    pub fn graph_max_head_data_u32(&self, gid: Option<String>) -> JsValue {
        self.log.max_head_data_u32(gid)
    }

    pub fn graph_max_head_data_u32_batch(&self, gids: Array) -> Result<Array, JsValue> {
        self.log.max_head_data_u32_batch(gids)
    }

    pub fn graph_join_head_entries(&self, gid: Option<String>) -> Array {
        self.log.head_join_entries(gid)
    }

    pub fn graph_child_join_entries(&self, hash: &str) -> Array {
        self.log.child_join_entries(hash)
    }

    pub fn graph_entry_metadata_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        self.log.entry_metadata_batch(hashes)
    }

    pub fn graph_unique_reference_gids(&self, hash: &str) -> JsValue {
        self.log.unique_reference_gids(hash)
    }

    pub fn graph_unique_reference_gid_rows_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        self.log.unique_reference_gid_rows_batch(hashes)
    }

    pub fn graph_unique_reference_gid_rows_flat_batch(
        &self,
        hashes: Array,
    ) -> Result<JsValue, JsValue> {
        self.log.unique_reference_gid_rows_flat_batch(hashes)
    }

    pub fn graph_plan_delete_recursively(
        &self,
        hashes: Array,
        skip_first: bool,
    ) -> Result<Array, JsValue> {
        self.log.plan_delete_recursively(hashes, skip_first)
    }

    pub fn graph_payload_size_sum(&self) -> f64 {
        self.log.payload_size_sum()
    }

    pub fn graph_oldest_hash(&self) -> JsValue {
        self.log.oldest_hash()
    }

    pub fn graph_newest_hash(&self) -> JsValue {
        self.log.newest_hash()
    }

    pub fn graph_count_has_next(&self, next: &str, exclude_hash: Option<String>) -> usize {
        self.log.count_has_next(next, exclude_hash)
    }

    pub fn graph_shadowed_gids(
        &self,
        gid: String,
        next: Array,
        exclude_hash: Option<String>,
    ) -> Result<Array, JsValue> {
        self.log.shadowed_gids(&gid, next, exclude_hash)
    }

    pub fn graph_plan_join(
        &self,
        hash: String,
        next: Array,
        entry_type: u8,
        reset: bool,
        gid: Option<String>,
        wall_time: Option<u64>,
        logical: Option<u32>,
    ) -> Result<Array, JsValue> {
        self.log
            .plan_join(&hash, next, entry_type, reset, gid, wall_time, logical)
    }

    pub fn graph_plan_join_batch(
        &self,
        hashes: Array,
        nexts: Array,
        entry_types: Uint8Array,
        reset: bool,
        gids: Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        cut_check: bool,
    ) -> Result<Array, JsValue> {
        self.log.plan_join_batch(
            hashes,
            nexts,
            entry_types,
            reset,
            gids,
            wall_times,
            logicals,
            cut_check,
        )
    }

    pub fn block_get(&self, key: &str) -> Option<Vec<u8>> {
        self.blocks.get(key)
    }

    pub fn block_get_many(&self, keys: Array) -> Result<Array, JsValue> {
        self.blocks.get_many(keys)
    }

    pub fn block_has_many(&self, keys: Array) -> Result<Array, JsValue> {
        self.blocks.has_many(keys)
    }

    pub fn block_put(&mut self, key: String, value: Vec<u8>) {
        self.blocks.put(key, value);
    }

    pub fn block_put_many(&mut self, keys: Array, values: Array) -> Result<(), JsValue> {
        self.blocks.put_many(keys, values)
    }

    pub fn block_delete(&mut self, key: &str) -> bool {
        self.blocks.delete(key)
    }

    pub fn block_delete_many(&mut self, keys: Array) -> Result<usize, JsValue> {
        self.blocks.delete_many(keys)
    }

    pub fn block_entries(&self) -> Array {
        self.blocks.entries()
    }

    pub fn block_size(&self) -> f64 {
        self.blocks.size()
    }

    pub fn clear(&mut self) {
        self.log.clear();
        self.blocks.clear();
        self.shared_log.clear();
        self.clear_coordinate_core();
        self.clear_document_core();
    }

    pub fn clear_shared_log(&mut self) {
        self.shared_log.clear();
        self.clear_coordinate_core();
    }

    pub fn clear_entry_coordinates(&mut self) {
        self.shared_log.clear_entry_coordinates();
        self.clear_coordinate_core();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_range(
        &mut self,
        id: String,
        hash: String,
        timestamp: String,
        start1: String,
        end1: String,
        start2: String,
        end2: String,
        width: String,
        mode: u8,
    ) -> Result<(), JsValue> {
        self.shared_log
            .put(id, hash, timestamp, start1, end1, start2, end2, width, mode)
    }

    pub fn delete_range(&mut self, id: &str) -> bool {
        self.shared_log.delete(id)
    }

    pub fn put_entry_coordinates(
        &mut self,
        hash: String,
        gid: String,
        hash_number: String,
        coordinates: Array,
        assigned_to_range_boundary: bool,
        requested_replicas: usize,
    ) -> Result<(), JsValue> {
        let hash_for_core = hash.clone();
        let gid_for_core = gid.clone();
        let hash_number_for_core = hash_number.clone();
        let coordinates_for_core = coordinates.clone();
        self.shared_log.put_entry_coordinates(
            hash,
            gid,
            hash_number,
            coordinates,
            assigned_to_range_boundary,
            requested_replicas,
        )?;
        self.put_coordinate_core_from_parts(
            hash_for_core,
            gid_for_core,
            &hash_number_for_core,
            coordinates_for_core,
            assigned_to_range_boundary,
            requested_replicas,
            0,
            Vec::new(),
        )
    }

    pub fn delete_entry_coordinates(&mut self, hash: &str) -> bool {
        let deleted_shared_log = self.shared_log.delete_entry_coordinates(hash);
        let deleted_core = self.delete_coordinate_core(hash);
        deleted_shared_log || deleted_core
    }

    pub fn delete_entry_coordinates_batch(&mut self, hashes: Array) -> Result<(), JsValue> {
        let hashes_for_core = hashes.clone();
        self.shared_log.delete_entry_coordinates_batch(hashes)?;
        self.delete_coordinate_core_batch(hashes_for_core)
    }

    pub fn commit_entry_coordinates(
        &mut self,
        hash: String,
        gid: String,
        hash_number: String,
        coordinates: Array,
        next_hashes: Array,
        assigned_to_range_boundary: bool,
        requested_replicas: usize,
    ) -> Result<(), JsValue> {
        let hash_for_core = hash.clone();
        let gid_for_core = gid.clone();
        let hash_number_for_core = hash_number.clone();
        let coordinates_for_core = coordinates.clone();
        let next_hashes_for_core = next_hashes.clone();
        self.shared_log.commit_entry_coordinates(
            hash,
            gid,
            hash_number,
            coordinates,
            next_hashes,
            assigned_to_range_boundary,
            requested_replicas,
        )?;
        self.put_coordinate_core_from_parts(
            hash_for_core,
            gid_for_core,
            &hash_number_for_core,
            coordinates_for_core,
            assigned_to_range_boundary,
            requested_replicas,
            0,
            Vec::new(),
        )?;
        self.delete_coordinate_core_batch(next_hashes_for_core)
    }

    pub fn commit_entry_coordinates_batch(
        &mut self,
        hashes: Array,
        gids: Array,
        hash_numbers: Array,
        coordinate_batches: Array,
        next_hash_batches: Array,
        assigned_to_range_boundaries: Uint8Array,
        requested_replicas: Array,
    ) -> Result<(), JsValue> {
        let hashes_for_core = hashes.clone();
        let gids_for_core = gids.clone();
        let hash_numbers_for_core = hash_numbers.clone();
        let coordinate_batches_for_core = coordinate_batches.clone();
        let next_hash_batches_for_core = next_hash_batches.clone();
        let assigned_to_range_boundaries_for_core = assigned_to_range_boundaries.clone();
        let requested_replicas_for_core = requested_replicas.clone();
        self.shared_log.commit_entry_coordinates_batch(
            hashes,
            gids,
            hash_numbers,
            coordinate_batches,
            next_hash_batches,
            assigned_to_range_boundaries,
            requested_replicas,
        )?;

        let hashes = strings_from_array(hashes_for_core)?;
        let gids = strings_from_array(gids_for_core)?;
        let hash_numbers = strings_from_array(hash_numbers_for_core)?;
        let coordinate_batches = coordinate_batches_from_array(coordinate_batches_for_core)?;
        let next_hash_batches =
            string_batches_from_array(next_hash_batches_for_core, "coordinate commit next hashes")?;
        let requested_replicas = usize_values_from_array(requested_replicas_for_core)?;
        ensure_same_len(hashes.len(), gids.len(), "coordinate commit gid")?;
        ensure_same_len(
            hashes.len(),
            hash_numbers.len(),
            "coordinate commit hash number",
        )?;
        ensure_same_len(
            hashes.len(),
            coordinate_batches.len(),
            "coordinate commit coordinates",
        )?;
        ensure_same_len(
            hashes.len(),
            next_hash_batches.len(),
            "coordinate commit next hashes",
        )?;
        ensure_same_len(
            hashes.len(),
            assigned_to_range_boundaries_for_core.length() as usize,
            "coordinate commit assigned flags",
        )?;
        ensure_same_len(
            hashes.len(),
            requested_replicas.len(),
            "coordinate commit replicas",
        )?;

        for index in 0..hashes.len() {
            self.put_coordinate_core(
                hashes[index].clone(),
                gids[index].clone(),
                parse_u64_string(&hash_numbers[index], "coordinate hash number")?,
                coordinate_batches[index].clone(),
                assigned_to_range_boundaries_for_core.get_index(index as u32) != 0,
                requested_replicas[index],
                0,
                Vec::new(),
                true,
            );
            self.delete_coordinate_core_strings(next_hash_batches[index].clone());
        }
        Ok(())
    }

    pub fn add_gid_peers(
        &mut self,
        gid: String,
        peers: Array,
        reset: bool,
    ) -> Result<usize, JsValue> {
        self.shared_log.add_gid_peers(gid, peers, reset)
    }

    pub fn remove_gid_peer(&mut self, peer: &str, gid: JsValue) -> Result<(), JsValue> {
        self.shared_log.remove_gid_peer(peer, gid)
    }

    pub fn remove_gid_peers(&mut self, peer: &str, gids: Array) -> Result<(), JsValue> {
        self.shared_log.remove_gid_peers(peer, gids)
    }

    pub fn delete_gid_peers(&mut self, gid: &str) -> bool {
        self.shared_log.delete_gid_peers(gid)
    }

    pub fn clear_gid_peers(&mut self) {
        self.shared_log.clear_gid_peers();
    }

    pub fn mark_entries_known_by_peer(
        &mut self,
        hashes: Array,
        peer: String,
    ) -> Result<(), JsValue> {
        self.shared_log.mark_entries_known_by_peer(hashes, peer)
    }

    pub fn remove_entries_known_by_peer(
        &mut self,
        hashes: Array,
        peer: &str,
    ) -> Result<(), JsValue> {
        self.shared_log.remove_entries_known_by_peer(hashes, peer)
    }

    pub fn remove_peer_from_entry_known_peers(&mut self, peer: &str) {
        self.shared_log.remove_peer_from_entry_known_peers(peer);
    }

    pub fn clear_entry_known_peers(&mut self) {
        self.shared_log.clear_entry_known_peers();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_entry_leaders_for_gid(
        &self,
        gid: String,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_entry_leaders_for_gid(
            gid,
            replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_leaders_for_gids_batch(
        &self,
        gids: Array,
        replica_counts: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_leaders_for_gids_batch(
            gids,
            replica_counts,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_entry_assignment_for_gid(
        &self,
        gid: String,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_entry_assignment_for_gid(
            gid,
            replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_repair_dispatch_for_entries(
        &self,
        entry_hashes: Array,
        entry_gids: Array,
        entry_requested_replicas: Array,
        entry_coordinate_batches: Array,
        pending_modes: Array,
        pending_peers_by_mode: Array,
        optimistic_peers_by_mode: Array,
        full_replica_repair_candidates: Array,
        full_replica_repair_candidate_count: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_repair_dispatch_for_entries(
            entry_hashes,
            entry_gids,
            entry_requested_replicas,
            entry_coordinate_batches,
            pending_modes,
            pending_peers_by_mode,
            optimistic_peers_by_mode,
            full_replica_repair_candidates,
            full_replica_repair_candidate_count,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_repair_dispatch_for_resident_entries(
        &self,
        pending_modes: Array,
        pending_peers_by_mode: Array,
        optimistic_gids_by_mode: Array,
        optimistic_peers_by_gid_by_mode: Array,
        full_replica_repair_candidates: Array,
        full_replica_repair_candidate_count: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_repair_dispatch_for_resident_entries(
            pending_modes,
            pending_peers_by_mode,
            optimistic_gids_by_mode,
            optimistic_peers_by_gid_by_mode,
            full_replica_repair_candidates,
            full_replica_repair_candidate_count,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_local_append_for_gid_compact(
        &mut self,
        entry_hash: String,
        gid: String,
        entry_hash_number: String,
        next_hashes: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hashes_for_core = next_hashes.clone();
        let row = self.shared_log.plan_local_append_for_gid_compact(
            entry_hash,
            gid,
            entry_hash_number,
            next_hashes,
            replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        self.commit_coordinate_core_from_compact_row(
            row.get(3),
            next_hashes_for_core,
            Array::new(),
            0,
            Vec::new(),
        )?;
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_local_append_for_gid_compact(
        &mut self,
        entry_hash: String,
        gid: String,
        entry_hash_number: String,
        next_hashes: Array,
        delete_hashes: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hashes_for_core = next_hashes.clone();
        let delete_hashes_for_core = delete_hashes.clone();
        let row = self.shared_log.commit_local_append_for_gid_compact(
            entry_hash,
            gid,
            entry_hash_number,
            next_hashes,
            delete_hashes,
            replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        self.commit_coordinate_core_from_compact_row(
            row.get(3),
            next_hashes_for_core,
            delete_hashes_for_core,
            0,
            Vec::new(),
        )?;
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_append_for_gid(
        &mut self,
        entry_hash: String,
        gid: String,
        entry_hash_number: String,
        next_hashes: Array,
        replicas: usize,
        full_replica_candidates: Array,
        fallback_recipients: Array,
        delivery_self_hash: String,
        delivery_enabled: bool,
        reliability_ack: bool,
        min_acks: JsValue,
        require_recipients: bool,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hashes_for_core = next_hashes.clone();
        let row = self.shared_log.plan_append_for_gid(
            entry_hash,
            gid,
            entry_hash_number,
            next_hashes,
            replicas,
            full_replica_candidates,
            fallback_recipients,
            delivery_self_hash,
            delivery_enabled,
            reliability_ack,
            min_acks,
            require_recipients,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        self.commit_coordinate_core_from_compact_row(
            row.get(5),
            next_hashes_for_core,
            Array::new(),
            0,
            Vec::new(),
        )?;
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_append_for_gids_batch(
        &mut self,
        entry_hashes: Array,
        gids: Array,
        entry_hash_numbers: Array,
        next_hash_batches: Array,
        replica_counts: Array,
        full_replica_candidates: Array,
        fallback_recipients: Array,
        delivery_self_hash: String,
        delivery_enabled: bool,
        reliability_ack: bool,
        min_acks: JsValue,
        require_recipients: bool,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hash_batches_for_core = next_hash_batches.clone();
        let rows = self.shared_log.plan_append_for_gids_batch(
            entry_hashes,
            gids,
            entry_hash_numbers,
            next_hash_batches,
            replica_counts,
            full_replica_candidates,
            fallback_recipients,
            delivery_self_hash,
            delivery_enabled,
            reliability_ack,
            min_acks,
            require_recipients,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        for index in 0..rows.length() {
            let row = array_from_value(rows.get(index), "append batch plan row")?;
            let next_hashes = array_from_value(
                next_hash_batches_for_core.get(index),
                "append batch next hashes",
            )?;
            self.commit_coordinate_core_from_compact_row(
                row.get(5),
                next_hashes,
                Array::new(),
                0,
                Vec::new(),
            )?;
        }
        Ok(rows)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_receive_coordinates_for_gids_batch(
        &mut self,
        entry_hashes: Array,
        gids: Array,
        entry_hash_numbers: Array,
        next_hash_batches: Array,
        replica_counts: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hash_batches_for_core = next_hash_batches.clone();
        let rows = self.shared_log.plan_receive_coordinates_for_gids_batch(
            entry_hashes,
            gids,
            entry_hash_numbers,
            next_hash_batches,
            replica_counts,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        for index in 0..rows.length() {
            let row = array_from_value(rows.get(index), "receive coordinate batch plan row")?;
            let next_hashes = array_from_value(
                next_hash_batches_for_core.get(index),
                "receive coordinate batch next hashes",
            )?;
            self.commit_coordinate_core_from_compact_row(
                row.get(4),
                next_hashes,
                Array::new(),
                0,
                Vec::new(),
            )?;
        }
        Ok(rows)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_facts(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = if trim_length_to.is_undefined() || trim_length_to.is_null() {
            None
        } else {
            Some(
                trim_length_to
                    .as_f64()
                    .ok_or_else(|| JsValue::from_str("trimLengthTo must be a number"))?
                    as usize,
            )
        };
        let has_no_next = next.length() == 0;
        match (has_no_next, trim_length_to) {
            (true, Some(trim_length_to)) => self
                .log
                .prepare_entry_v0_plain_entry_commit_no_next_facts_trim_and_put_with_builder(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid,
                    entry_type,
                    meta_data,
                    payload_data,
                    trim_length_to,
                ),
            (true, None) => self
                .log
                .prepare_entry_v0_plain_entry_commit_no_next_facts_and_put_with_builder(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid,
                    entry_type,
                    meta_data,
                    payload_data,
                ),
            (false, Some(trim_length_to)) => self
                .log
                .prepare_entry_v0_plain_entry_commit_facts_trim_and_put_with_builder(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid,
                    next,
                    entry_type,
                    meta_data,
                    payload_data,
                    trim_length_to,
                ),
            (false, None) => self
                .log
                .prepare_entry_v0_plain_entry_commit_facts_and_put_with_builder(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid,
                    next,
                    entry_type,
                    meta_data,
                    payload_data,
                ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_facts_document_index(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: JsValue,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let document_index_commit = document_index_append_commit(
            document_key,
            document_value_prefix_bytes,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        let row = self.prepare_plain_entry_commit_facts(
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            trim_length_to,
        )?;
        let entry_row = if row.length() == 2 && Array::is_array(&row.get(0)) {
            array_from_value(row.get(0), "native trim document index entry row")?
        } else {
            row.clone()
        };
        let document_hash = string_field(&entry_row, 0, "document index entry hash")?;
        self.put_document_index_for_append(
            Some(document_index_commit),
            wall_time,
            &document_hash,
            &document_gid,
            payload_size,
        )?;
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_facts_document_index_cached_plan(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: JsValue,
        document_key: String,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let document_index_commit = document_index_cached_projection_append_commit(
            document_key,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan_id,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        let row = self.prepare_plain_entry_commit_facts(
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            trim_length_to,
        )?;
        let entry_row = if row.length() == 2 && Array::is_array(&row.get(0)) {
            array_from_value(row.get(0), "native trim document index entry row")?
        } else {
            row.clone()
        };
        let document_hash = string_field(&entry_row, 0, "document index entry hash")?;
        self.put_document_index_for_append(
            Some(document_index_commit),
            wall_time,
            &document_hash,
            &document_gid,
            payload_size,
        )?;
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_no_next_facts_document_index_compact(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let document_index_commit = document_index_append_commit(
            document_key,
            document_value_prefix_bytes,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        self.prepare_plain_entry_commit_no_next_document_index_compact(
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            payload_data,
            document_gid,
            payload_size,
            document_index_commit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        document_key: String,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let document_index_commit = document_index_cached_projection_append_commit(
            document_key,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan_id,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        self.prepare_plain_entry_commit_no_next_document_index_compact(
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            payload_data,
            document_gid,
            payload_size,
            document_index_commit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_entry_commit_no_next_document_index_compact(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        document_gid: String,
        payload_size: u32,
        document_index_commit: DocumentIndexAppendCommit,
    ) -> Result<Array, JsValue> {
        let entry_facts = self
            .log
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder(
                &self.builder,
                &mut self.blocks,
                wall_time,
                logical,
                gid,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                None,
            )?;
        self.put_document_index_for_append(
            Some(document_index_commit),
            wall_time,
            &entry_facts.hash,
            &document_gid,
            payload_size,
        )?;
        Ok(committed_entry_facts_to_row(&entry_facts, false))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_no_next_facts_document_index_trim_hashes(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let document_index_commit = document_index_append_commit(
            document_key,
            document_value_prefix_bytes,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        let (entry_facts, trim_hashes) = self
            .log
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes(
                &self.builder,
                &mut self.blocks,
                wall_time,
                logical,
                gid,
                Vec::new(),
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                Some(trim_length_to),
                None,
            )?;
        self.put_document_index_for_append(
            Some(document_index_commit),
            wall_time,
            &entry_facts.hash,
            &document_gid,
            payload_size,
        )?;
        let document_trimmed_heads_processed =
            document_delete_trimmed_heads && self.delete_documents_by_context_heads(&trim_hashes);
        let out = Array::new();
        out.push(&committed_entry_facts_to_row(&entry_facts, false));
        out.push(&strings_to_array(trim_hashes));
        out.push(&JsValue::from_bool(document_trimmed_heads_processed));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_no_next_facts_document_index_compact_trim_hashes(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let document_index_commit = document_index_append_commit(
            document_key,
            document_value_prefix_bytes,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        self.prepare_plain_entry_commit_no_next_document_index_compact_trim_hashes(
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            payload_data,
            trim_length_to,
            document_gid,
            payload_size,
            document_index_commit,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_trim_hashes(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
        document_key: String,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let document_index_commit = document_index_cached_projection_append_commit(
            document_key,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan_id,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        self.prepare_plain_entry_commit_no_next_document_index_compact_trim_hashes(
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            payload_data,
            trim_length_to,
            document_gid,
            payload_size,
            document_index_commit,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_entry_commit_no_next_document_index_compact_trim_hashes(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
        document_gid: String,
        payload_size: u32,
        document_index_commit: DocumentIndexAppendCommit,
        compact_row: bool,
    ) -> Result<Array, JsValue> {
        let delete_trimmed_document_heads = document_index_commit.delete_trimmed_heads;
        let (entry_facts, trim_hashes) = self
            .log
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes(
                &self.builder,
                &mut self.blocks,
                wall_time,
                logical,
                gid,
                Vec::new(),
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                Some(trim_length_to),
                None,
            )?;
        self.put_document_index_for_append(
            Some(document_index_commit),
            wall_time,
            &entry_facts.hash,
            &document_gid,
            payload_size,
        )?;
        let document_trimmed_heads_processed =
            delete_trimmed_document_heads && self.delete_documents_by_context_heads(&trim_hashes);
        if compact_row {
            return Ok(compact_committed_entry_facts_trim_hashes_to_row(
                &entry_facts,
                trim_hashes,
                document_trimmed_heads_processed,
            ));
        }
        let out = Array::new();
        out.push(&committed_entry_facts_to_row(&entry_facts, false));
        out.push(&strings_to_array(trim_hashes));
        out.push(&JsValue::from_bool(document_trimmed_heads_processed));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_hashes(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
        document_key: String,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let document_index_commit = document_index_cached_projection_append_commit(
            document_key,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan_id,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        self.prepare_plain_entry_commit_no_next_document_index_compact_trim_hashes(
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            payload_data,
            trim_length_to,
            document_gid,
            payload_size,
            document_index_commit,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_storage_facts_and_put(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        self.log
            .prepare_entry_v0_plain_entry_storage_facts_and_put_with_builder(
                &self.builder,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
            )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_storage_facts_trim_and_put(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.log
            .prepare_entry_v0_plain_entry_storage_facts_trim_and_put_with_builder(
                &self.builder,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
            )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_no_next_storage_append_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            None,
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_no_next_storage_append_transaction_trim(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            Some(trim_length_to),
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_no_next_storage_append_document_index_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            None,
            false,
            Some(document_index_append_commit(
                document_key,
                document_value_prefix_bytes,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_no_next_storage_append_document_index_transaction_trim(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            Some(trim_length_to),
            false,
            Some(document_index_append_commit(
                document_key,
                document_value_prefix_bytes,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_no_next_storage_append_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            None,
            true,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_no_next_storage_append_transaction_trim(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            Some(trim_length_to),
            true,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn benchmark_plain_committed_no_next_storage_append_transaction_loop(
        &mut self,
        iterations: u32,
        wall_time_start: u64,
        payload_data: Uint8Array,
        replicas: usize,
        self_hash: String,
        use_document_index: bool,
        document_byte_element_index_limit: usize,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let profile_enabled = self.append_profile_enabled;
        let input_copy_started = profile_enabled.then(js_sys::Date::now);
        let payload_template = payload_data.to_vec();
        if let Some(started) = input_copy_started {
            self.append_profile.input_copy_ms += js_sys::Date::now() - started;
        }
        let payload_size = payload_template.len() as u32;
        let now = wall_time_start.to_string();
        let started = js_sys::Date::now();
        for i in 0..iterations {
            let wall_time = wall_time_start + i as u64;
            let logical = i;
            let gid = format!("native-backbone-loop-{wall_time_start}-{i}");
            let storage_append_started = profile_enabled.then(js_sys::Date::now);
            let payload_copy_started = profile_enabled.then(js_sys::Date::now);
            let payload_data = payload_template.clone();
            if let Some(started) = payload_copy_started {
                self.append_profile.input_copy_ms += js_sys::Date::now() - started;
            }

            let log_started = profile_enabled.then(js_sys::Date::now);
            let mut log_profile = NativeLogAppendProfile::default();
            let (entry_facts, trim_hashes) = self
                .log
                .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid.clone(),
                    Vec::new(),
                    0,
                    None,
                    payload_data,
                    trim_length_to,
                    profile_enabled.then_some(&mut log_profile),
                )?;
            if let Some(started) = log_started {
                self.append_profile.log_total_ms += js_sys::Date::now() - started;
                self.append_profile.add_log_profile(&log_profile);
            }

            let hash_number_started = profile_enabled.then(js_sys::Date::now);
            let hash_number = hash_number_u64(&self.resolution, &entry_facts.hash_digest_bytes)?;
            if let Some(started) = hash_number_started {
                self.append_profile.hash_number_ms += js_sys::Date::now() - started;
            }

            let coordinate_plan_started = profile_enabled.then(js_sys::Date::now);
            let coordinate_facts = commit_local_append_for_gid_compact_core(
                &mut self.shared_log,
                entry_facts.hash.clone(),
                gid.clone(),
                hash_number,
                Vec::new(),
                trim_hashes.clone(),
                replicas,
                0.0,
                &now,
                &self_hash,
                true,
                true,
                true,
            )?;
            if let Some(started) = coordinate_plan_started {
                self.append_profile.coordinate_plan_ms += js_sys::Date::now() - started;
            }

            let coordinate_core_started = profile_enabled.then(js_sys::Date::now);
            self.commit_coordinate_core_from_compact_facts(
                &coordinate_facts,
                Vec::new(),
                trim_hashes,
                wall_time,
                entry_facts.meta_bytes.clone(),
            );
            if let Some(started) = coordinate_core_started {
                self.append_profile.coordinate_core_ms += js_sys::Date::now() - started;
            }

            let document_index_started = profile_enabled.then(js_sys::Date::now);
            let document_index_commit = use_document_index.then(|| DocumentIndexAppendCommit {
                key: format!("native-backbone-loop-doc-{wall_time_start}-{i}"),
                value_prefix: DocumentIndexValuePrefix::Bytes(Vec::new()),
                existing_created: None,
                byte_element_index_limit: document_byte_element_index_limit,
                delete_trimmed_heads: false,
            });
            self.put_document_index_for_append(
                document_index_commit,
                wall_time,
                &entry_facts.hash,
                &gid,
                payload_size,
            )?;
            if let Some(started) = document_index_started {
                self.append_profile.document_index_commit_ms += js_sys::Date::now() - started;
            }

            if let Some(started) = storage_append_started {
                self.append_profile.storage_append_inner_ms += js_sys::Date::now() - started;
            }
        }
        let row = Array::new();
        row.push(&JsValue::from_f64(js_sys::Date::now() - started));
        row.push(&JsValue::from_f64(self.log.len() as f64));
        row.push(&JsValue::from_f64(self.blocks.len() as f64));
        row.push(&JsValue::from_f64(self.coordinate_index.len() as f64));
        row.push(&JsValue::from_f64(self.document_index.len() as f64));
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_no_next_storage_append_document_index_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            None,
            true,
            Some(document_index_append_commit(
                document_key,
                document_value_prefix_bytes,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_no_next_storage_append_document_index_transaction_trim(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            Some(trim_length_to),
            true,
            Some(document_index_append_commit(
                document_key,
                document_value_prefix_bytes,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_no_next_storage_append_document_index_cached_plan_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            None,
            true,
            Some(document_index_cached_projection_append_commit(
                document_key,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan_id,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_no_next_storage_append_document_index_cached_plan_transaction_trim(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            Some(trim_length_to),
            true,
            Some(document_index_cached_projection_append_commit(
                document_key,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan_id,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_no_next_storage_append_document_index_compact_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_index_commit = document_index_append_commit(
            document_key,
            document_value_prefix_bytes,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            JsValue::UNDEFINED,
            JsValue::UNDEFINED,
            JsValue::UNDEFINED,
        )?;
        self.prepare_plain_committed_no_next_storage_append_document_index_compact_transaction_inner(
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            document_index_commit,
            trim_length_to,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_key: String,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_index_commit = document_index_cached_projection_append_commit(
            document_key,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan_id,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        self.prepare_plain_committed_no_next_storage_append_document_index_compact_transaction_inner(
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            document_index_commit,
            trim_length_to,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_committed_no_next_storage_append_document_index_compact_transaction_inner(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_index_commit: DocumentIndexAppendCommit,
        trim_length_to: Option<usize>,
    ) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let storage_append_started = profile_enabled.then(js_sys::Date::now);
        let payload_size = payload_data.length();
        let delete_trimmed_document_heads = document_index_commit.delete_trimmed_heads;

        let input_copy_started = profile_enabled.then(js_sys::Date::now);
        let meta_data = optional_bytes_from_js(meta_data);
        let payload_data = payload_data.to_vec();
        if let Some(started) = input_copy_started {
            self.append_profile.input_copy_ms += js_sys::Date::now() - started;
        }

        let log_started = profile_enabled.then(js_sys::Date::now);
        let mut log_profile = NativeLogAppendProfile::default();
        let (entry_facts, trim_hashes) = self
            .log
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes(
                &self.builder,
                &mut self.blocks,
                wall_time,
                logical,
                gid.clone(),
                Vec::new(),
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                profile_enabled.then_some(&mut log_profile),
            )?;
        if let Some(started) = log_started {
            self.append_profile.log_total_ms += js_sys::Date::now() - started;
            self.append_profile.add_log_profile(&log_profile);
        }

        let hash_number_started = profile_enabled.then(js_sys::Date::now);
        let hash_number = hash_number_u64(&self.resolution, &entry_facts.hash_digest_bytes)?;
        if let Some(started) = hash_number_started {
            self.append_profile.hash_number_ms += js_sys::Date::now() - started;
        }

        let coordinate_plan_started = profile_enabled.then(js_sys::Date::now);
        let coordinate_facts = commit_local_append_for_gid_compact_core(
            &mut self.shared_log,
            entry_facts.hash.clone(),
            gid.clone(),
            hash_number,
            Vec::new(),
            trim_hashes.clone(),
            replicas,
            role_age_ms,
            &now,
            &self_hash,
            self_replicating,
            true,
            true,
        )?;
        if let Some(started) = coordinate_plan_started {
            self.append_profile.coordinate_plan_ms += js_sys::Date::now() - started;
        }

        let coordinate_core_started = profile_enabled.then(js_sys::Date::now);
        self.commit_coordinate_core_from_compact_facts(
            &coordinate_facts,
            Vec::new(),
            trim_hashes.clone(),
            wall_time,
            entry_facts.meta_bytes.clone(),
        );
        if let Some(started) = coordinate_core_started {
            self.append_profile.coordinate_core_ms += js_sys::Date::now() - started;
        }

        let document_index_started = profile_enabled.then(js_sys::Date::now);
        self.put_document_index_for_append(
            Some(document_index_commit),
            wall_time,
            &entry_facts.hash,
            &gid,
            payload_size,
        )?;
        let document_trimmed_heads_processed =
            delete_trimmed_document_heads && self.delete_documents_by_context_heads(&trim_hashes);
        if let Some(started) = document_index_started {
            self.append_profile.document_index_commit_ms += js_sys::Date::now() - started;
        }

        let result_row_started = profile_enabled.then(js_sys::Date::now);
        let out = Array::new();
        out.push(&JsValue::from_str(&entry_facts.hash));
        out.push(&JsValue::from_f64(entry_facts.byte_length as f64));
        out.push(&Uint8Array::from(entry_facts.meta_bytes.as_slice()));
        out.push(&coordinate_plan_to_row(&self.resolution, &coordinate_facts));
        out.push(&leader_samples_to_optional_rows(&coordinate_facts.leaders));
        out.push(&JsValue::from_bool(coordinate_facts.is_leader));
        out.push(&strings_to_array(trim_hashes));
        out.push(&JsValue::from_bool(document_trimmed_heads_processed));
        if let Some(started) = result_row_started {
            self.append_profile.result_row_ms += js_sys::Date::now() - started;
        }
        if let Some(started) = storage_append_started {
            self.append_profile.storage_append_inner_ms += js_sys::Date::now() - started;
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_storage_append_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            strings_from_array(next_hashes)?,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            None,
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_storage_append_transaction_trim(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            strings_from_array(next_hashes)?,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            Some(trim_length_to),
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_storage_append_document_index_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            strings_from_array(next_hashes)?,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            None,
            false,
            Some(document_index_append_commit(
                document_key,
                document_value_prefix_bytes,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_storage_append_document_index_transaction_trim(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            strings_from_array(next_hashes)?,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            Some(trim_length_to),
            false,
            Some(document_index_append_commit(
                document_key,
                document_value_prefix_bytes,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            strings_from_array(next_hashes)?,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            None,
            true,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            strings_from_array(next_hashes)?,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            None,
            true,
            Some(document_index_append_commit(
                document_key,
                document_value_prefix_bytes,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_transaction_trim(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            strings_from_array(next_hashes)?,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            Some(trim_length_to),
            true,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_transaction_trim(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            strings_from_array(next_hashes)?,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            Some(trim_length_to),
            true,
            Some(document_index_append_commit(
                document_key,
                document_value_prefix_bytes,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan,
                document_projection_encoded_document,
                document_projection_signer,
            )?),
        )
    }
}

impl NativePeerbitBackbone {
    fn clear_coordinate_core(&mut self) {
        self.coordinate_index.clear();
        self.coordinate_values.clear();
        self.coordinate_journal.clear();
        self.coordinate_journal_record_count = 0;
    }

    fn clear_document_core(&mut self) {
        self.document_index.clear();
        self.document_values.clear();
    }

    fn document_entries_for_keys(&self, keys: &[String]) -> Array {
        let out = Array::new();
        for key in keys {
            if let Some(value) = self.document_values.get(key) {
                out.push(&document_entry_to_row(key, value));
            }
        }
        out
    }

    fn put_coordinate_core_from_parts(
        &mut self,
        hash: String,
        gid: String,
        hash_number: &str,
        coordinates: Array,
        assigned_to_range_boundary: bool,
        requested_replicas: usize,
        wall_time: u64,
        meta_bytes: Vec<u8>,
    ) -> Result<(), JsValue> {
        let hash_number = parse_u64_string(hash_number, "coordinate hash number")?;
        let coordinates = coordinate_numbers_from_array(coordinates)?;
        self.put_coordinate_core(
            hash,
            gid,
            hash_number,
            coordinates,
            assigned_to_range_boundary,
            requested_replicas,
            wall_time,
            meta_bytes,
            true,
        );
        Ok(())
    }

    fn commit_coordinate_core_from_compact_row(
        &mut self,
        coordinate_row: JsValue,
        next_hashes: Array,
        delete_hashes: Array,
        wall_time: u64,
        meta_bytes: Vec<u8>,
    ) -> Result<(), JsValue> {
        let row = array_from_value(coordinate_row, "coordinate plan row")?;
        let hash = string_field(&row, 0, "coordinate hash")?;
        let hash_number = stringish_field(&row, 1, "coordinate hash number")?;
        let gid = string_field(&row, 2, "coordinate gid")?;
        let coordinates = array_from_value(row.get(3), "coordinate rows")?;
        let assigned_to_range_boundary = bool_field(&row, 4, "assigned to range boundary")?;
        let requested_replicas = usize_field(&row, 5, "requested replicas")?;
        self.put_coordinate_core_from_parts(
            hash,
            gid,
            &hash_number,
            coordinates,
            assigned_to_range_boundary,
            requested_replicas,
            wall_time,
            meta_bytes,
        )?;
        self.delete_coordinate_core_batch(next_hashes)?;
        self.delete_coordinate_core_batch(delete_hashes)
    }

    fn commit_coordinate_core_from_compact_facts(
        &mut self,
        facts: &NativeLocalAppendCompactFacts,
        next_hashes: Vec<String>,
        delete_hashes: Vec<String>,
        wall_time: u64,
        meta_bytes: Vec<u8>,
    ) {
        let coordinate = &facts.coordinate;
        self.put_coordinate_core(
            coordinate.hash.clone(),
            coordinate.gid.clone(),
            coordinate.hash_number,
            coordinate.coordinates.clone(),
            coordinate.assigned_to_range_boundary,
            coordinate.requested_replicas,
            wall_time,
            meta_bytes,
            true,
        );
        let profile_enabled = self.append_profile_enabled;
        let coordinate_delete_started = profile_enabled.then(js_sys::Date::now);
        self.delete_coordinate_core_strings(next_hashes);
        self.delete_coordinate_core_strings(delete_hashes);
        if let Some(started) = coordinate_delete_started {
            self.append_profile.coordinate_delete_ms += js_sys::Date::now() - started;
        }
    }

    fn put_decoded_coordinate_core(
        &mut self,
        coordinate: CoordinateCoreValue,
        record_journal: bool,
    ) -> Result<(), JsValue> {
        self.shared_log.put_entry_coordinates(
            coordinate.hash.clone(),
            coordinate.gid.clone(),
            coordinate.hash_number.to_string(),
            number_strings_to_array(&coordinate.coordinates),
            coordinate.assigned_to_range_boundary,
            coordinate.requested_replicas,
        )?;
        self.put_coordinate_core(
            coordinate.hash,
            coordinate.gid,
            coordinate.hash_number,
            coordinate.coordinates,
            coordinate.assigned_to_range_boundary,
            coordinate.requested_replicas,
            coordinate.wall_time,
            coordinate.meta_bytes,
            record_journal,
        );
        Ok(())
    }

    fn put_coordinate_core(
        &mut self,
        hash: String,
        gid: String,
        hash_number: u64,
        coordinates: Vec<u64>,
        assigned_to_range_boundary: bool,
        requested_replicas: usize,
        wall_time: u64,
        meta_bytes: Vec<u8>,
        record_journal: bool,
    ) {
        let profile_enabled = self.append_profile_enabled;
        let value_encode_started = profile_enabled.then(js_sys::Date::now);
        let value = encode_coordinate_value(
            &hash,
            &gid,
            hash_number,
            &coordinates,
            assigned_to_range_boundary,
            requested_replicas,
            wall_time,
            &meta_bytes,
        );
        if let Some(started) = value_encode_started {
            self.append_profile.coordinate_value_encode_ms += js_sys::Date::now() - started;
        }
        if record_journal && self.coordinate_journal_enabled {
            let journal_started = profile_enabled.then(js_sys::Date::now);
            self.push_coordinate_journal_put(&hash, &value);
            if let Some(started) = journal_started {
                self.append_profile.coordinate_journal_put_ms += js_sys::Date::now() - started;
            }
        }
        let index_put_started = profile_enabled.then(js_sys::Date::now);
        self.coordinate_index.insert(hash.clone());
        if let Some(started) = index_put_started {
            self.append_profile.coordinate_index_put_ms += js_sys::Date::now() - started;
        }
        let value_put_started = profile_enabled.then(js_sys::Date::now);
        self.coordinate_values.put(hash, value);
        if let Some(started) = value_put_started {
            self.append_profile.coordinate_value_put_ms += js_sys::Date::now() - started;
        }
    }

    fn delete_coordinate_core(&mut self, hash: &str) -> bool {
        self.coordinate_index.remove(hash);
        if self.coordinate_journal_enabled {
            self.push_coordinate_journal_delete(hash);
        }
        self.coordinate_values.delete(hash)
    }

    fn push_coordinate_journal_put(&mut self, key: &str, value: &[u8]) {
        self.coordinate_journal
            .extend_from_slice(&encode_journal_put_record(key, value));
        self.coordinate_journal_record_count += 1;
    }

    fn push_coordinate_journal_delete(&mut self, key: &str) {
        self.coordinate_journal
            .extend_from_slice(&encode_journal_delete_record(key));
        self.coordinate_journal_record_count += 1;
    }

    fn delete_coordinate_core_batch(&mut self, hashes: Array) -> Result<(), JsValue> {
        for hash in strings_from_array(hashes)? {
            self.delete_coordinate_core(&hash);
        }
        Ok(())
    }

    fn delete_coordinate_core_strings(&mut self, hashes: Vec<String>) {
        for hash in hashes {
            self.delete_coordinate_core(&hash);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_storage_append_transaction_inner(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Vec<String>,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        trim_length_to: Option<usize>,
        commit_blocks: bool,
        document_index_commit: Option<DocumentIndexAppendCommit>,
    ) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let storage_append_started = profile_enabled.then(js_sys::Date::now);
        let payload_size = payload_data.length();
        let delete_trimmed_document_heads = document_index_commit
            .as_ref()
            .is_some_and(|commit| commit.delete_trimmed_heads);
        let (entry_row, trim_rows, trim_hashes, hash, digest, meta_bytes) = if commit_blocks {
            let input_copy_started = profile_enabled.then(js_sys::Date::now);
            let meta_data = optional_bytes_from_js(meta_data);
            let payload_data = payload_data.to_vec();
            if let Some(started) = input_copy_started {
                self.append_profile.input_copy_ms += js_sys::Date::now() - started;
            }
            let log_started = profile_enabled.then(js_sys::Date::now);
            let mut log_profile = NativeLogAppendProfile::default();
            let (entry_facts, trimmed_entries, trim_hashes) = if resolve_trimmed_entries {
                let (entry_facts, trimmed_entries) = self
                    .log
                    .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder(
                        &self.builder,
                        &mut self.blocks,
                        wall_time,
                        logical,
                        gid.clone(),
                        next_hashes.clone(),
                        entry_type,
                        meta_data,
                        payload_data,
                        trim_length_to,
                        profile_enabled.then_some(&mut log_profile),
                    )?;
                let trim_hashes = trimmed_entries
                    .iter()
                    .map(|entry| entry.hash.clone())
                    .collect::<Vec<_>>();
                (entry_facts, trimmed_entries, trim_hashes)
            } else {
                let (entry_facts, trim_hashes) = self
                    .log
                    .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes(
                        &self.builder,
                        &mut self.blocks,
                        wall_time,
                        logical,
                        gid.clone(),
                        next_hashes.clone(),
                        entry_type,
                        meta_data,
                        payload_data,
                        trim_length_to,
                        profile_enabled.then_some(&mut log_profile),
                    )?;
                (entry_facts, Vec::new(), trim_hashes)
            };
            if let Some(started) = log_started {
                self.append_profile.log_total_ms += js_sys::Date::now() - started;
                self.append_profile.add_log_profile(&log_profile);
            }
            let entry_row_started = profile_enabled.then(js_sys::Date::now);
            let entry_row =
                committed_entry_facts_to_row(&entry_facts, !entry_facts.next.is_empty());
            if let Some(started) = entry_row_started {
                self.append_profile.entry_row_ms += js_sys::Date::now() - started;
            }
            let trim_rows_started = profile_enabled.then(js_sys::Date::now);
            let trim_rows = if resolve_trimmed_entries {
                native_backbone_trim_entries_to_rows(trimmed_entries)
            } else {
                Array::new()
            };
            if let Some(started) = trim_rows_started {
                self.append_profile.trim_rows_ms += js_sys::Date::now() - started;
            }
            (
                entry_row,
                trim_rows,
                trim_hashes,
                entry_facts.hash,
                entry_facts.hash_digest_bytes,
                entry_facts.meta_bytes,
            )
        } else if let Some(trim_length_to) = trim_length_to {
            let next_hashes_array = strings_to_array(next_hashes.clone());
            let row = self
                .log
                .prepare_entry_v0_plain_entry_storage_facts_trim_and_put_with_builder(
                    &self.builder,
                    wall_time,
                    logical,
                    gid.clone(),
                    next_hashes_array,
                    entry_type,
                    meta_data,
                    payload_data,
                    trim_length_to,
                )?;
            let row = array_from_value(row.into(), "native storage trim append row")?;
            let entry_row = array_from_value(row.get(0), "native storage trim append entry row")?;
            let trim_rows = array_from_value(row.get(1), "native storage trim append trim rows")?;
            let trim_hashes = trim_hashes_vec(&trim_rows)?;
            let hash = string_field(&entry_row, 1, "storage entry hash")?;
            let digest = bytes_field(&entry_row, 5, "storage entry hash digest")?;
            let meta_bytes = bytes_field(&entry_row, 4, "storage entry meta bytes")?;
            (entry_row, trim_rows, trim_hashes, hash, digest, meta_bytes)
        } else {
            let next_hashes_array = strings_to_array(next_hashes.clone());
            let row = self
                .log
                .prepare_entry_v0_plain_entry_storage_facts_and_put_with_builder(
                    &self.builder,
                    wall_time,
                    logical,
                    gid.clone(),
                    next_hashes_array,
                    entry_type,
                    meta_data,
                    payload_data,
                )?;
            let hash = string_field(&row, 1, "storage entry hash")?;
            let digest = bytes_field(&row, 5, "storage entry hash digest")?;
            let meta_bytes = bytes_field(&row, 4, "storage entry meta bytes")?;
            (row, Array::new(), Vec::new(), hash, digest, meta_bytes)
        };

        let hash_number_started = profile_enabled.then(js_sys::Date::now);
        let hash_number = hash_number_u64(&self.resolution, &digest)?;
        if let Some(started) = hash_number_started {
            self.append_profile.hash_number_ms += js_sys::Date::now() - started;
        }
        let next_hashes_for_core = next_hashes.clone();
        let trim_hashes_for_core = trim_hashes.clone();
        let trim_hashes_for_result = trim_hashes.clone();
        let document_hash = hash.clone();
        let document_gid = gid.clone();
        let coordinate_plan_started = profile_enabled.then(js_sys::Date::now);
        let coordinate_facts = commit_local_append_for_gid_compact_core(
            &mut self.shared_log,
            hash,
            gid,
            hash_number,
            next_hashes,
            trim_hashes,
            replicas,
            role_age_ms,
            &now,
            &self_hash,
            self_replicating,
            true,
            true,
        )?;
        if let Some(started) = coordinate_plan_started {
            self.append_profile.coordinate_plan_ms += js_sys::Date::now() - started;
        }
        let coordinate_core_started = profile_enabled.then(js_sys::Date::now);
        self.commit_coordinate_core_from_compact_facts(
            &coordinate_facts,
            next_hashes_for_core,
            trim_hashes_for_core,
            wall_time,
            meta_bytes,
        );
        if let Some(started) = coordinate_core_started {
            self.append_profile.coordinate_core_ms += js_sys::Date::now() - started;
        }
        let document_index_started = profile_enabled.then(js_sys::Date::now);
        self.put_document_index_for_append(
            document_index_commit,
            wall_time,
            &document_hash,
            &document_gid,
            payload_size,
        )?;
        let document_trimmed_heads_processed = delete_trimmed_document_heads
            && self.delete_documents_by_context_heads(&trim_hashes_for_result);
        if let Some(started) = document_index_started {
            self.append_profile.document_index_commit_ms += js_sys::Date::now() - started;
        }

        let result_row_started = profile_enabled.then(js_sys::Date::now);
        let out = Array::new();
        out.push(&entry_row);
        out.push(&leader_samples_to_optional_rows(&coordinate_facts.leaders));
        out.push(&JsValue::from_bool(coordinate_facts.is_leader));
        out.push(&JsValue::from_bool(
            coordinate_facts.assigned_to_range_boundary,
        ));
        out.push(&coordinate_plan_to_row(&self.resolution, &coordinate_facts));
        out.push(&trim_rows);
        out.push(&strings_to_array(trim_hashes_for_result));
        out.push(&JsValue::from_bool(document_trimmed_heads_processed));
        if let Some(started) = result_row_started {
            self.append_profile.result_row_ms += js_sys::Date::now() - started;
        }
        if let Some(started) = storage_append_started {
            self.append_profile.storage_append_inner_ms += js_sys::Date::now() - started;
        }
        Ok(out)
    }

    fn put_document_index_for_append(
        &mut self,
        document_index_commit: Option<DocumentIndexAppendCommit>,
        wall_time: u64,
        hash: &str,
        gid: &str,
        payload_size: u32,
    ) -> Result<(), JsValue> {
        let Some(document_index_commit) = document_index_commit else {
            return Ok(());
        };
        let profile_enabled = self.append_profile_enabled;
        let context_started = profile_enabled.then(js_sys::Date::now);
        let context_suffix = encode_document_context_suffix(
            document_index_commit.existing_created.unwrap_or(wall_time),
            wall_time,
            hash,
            gid,
            payload_size,
        )?;
        if let Some(started) = context_started {
            self.append_profile.document_index_context_encode_ms += js_sys::Date::now() - started;
        }
        let value_prefix_bytes = match document_index_commit.value_prefix {
            DocumentIndexValuePrefix::Bytes(bytes) => bytes,
            DocumentIndexValuePrefix::Projection {
                encoded_document,
                plan,
                signer,
            } => match plan {
                DocumentIndexProjectionPlan::Inline(plan) => {
                    project_document_index_simple_bytes_with_plan(
                        &encoded_document,
                        &plan,
                        document_index_commit.existing_created.unwrap_or(wall_time),
                        wall_time,
                        gid,
                        payload_size,
                        signer.as_deref(),
                    )?
                }
                DocumentIndexProjectionPlan::Cached(index) => {
                    let plan = self.document_projection_plans.get(index).ok_or_else(|| {
                        JsValue::from_str("Missing cached document projection plan")
                    })?;
                    project_document_index_simple_bytes_with_plan(
                        &encoded_document,
                        plan,
                        document_index_commit.existing_created.unwrap_or(wall_time),
                        wall_time,
                        gid,
                        payload_size,
                        signer.as_deref(),
                    )?
                }
            },
        };
        self.put_document_encoded_parts_stored(
            document_index_commit.key,
            value_prefix_bytes,
            context_suffix,
            document_index_commit.byte_element_index_limit,
        )
    }

    fn delete_documents_by_context_heads(&mut self, heads: &[String]) -> bool {
        if heads.is_empty() {
            return false;
        }
        let Some(field) = self.document_context_head_field else {
            return false;
        };
        for head in heads {
            if let Some(key) = self
                .document_index
                .exact_first(&FieldPath::Id(field), &FieldValue::from(head.clone()))
            {
                self.document_index.delete_id(&key);
                self.document_values.delete(&key);
            }
        }
        true
    }
}

fn array_from_value(value: JsValue, label: &str) -> Result<Array, JsValue> {
    value
        .dyn_into::<Array>()
        .map_err(|_| JsValue::from_str(&format!("Expected {label} array")))
}

fn committed_entry_facts_to_row(entry: &NativeCommittedEntryFacts, include_next: bool) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&entry.hash));
    if include_next {
        row.push(&strings_to_array(entry.next.clone()));
    }
    row.push(&Uint8Array::from(entry.meta_bytes.as_slice()));
    row.push(&JsValue::from_f64(entry.byte_length as f64));
    row.push(&Uint8Array::from(entry.hash_digest_bytes.as_slice()));
    row
}

fn compact_committed_entry_facts_trim_hashes_to_row(
    entry: &NativeCommittedEntryFacts,
    trim_hashes: Vec<String>,
    document_trimmed_heads_processed: bool,
) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&entry.hash));
    row.push(&JsValue::from_f64(entry.byte_length as f64));
    row.push(&Uint8Array::from(entry.meta_bytes.as_slice()));
    row.push(&Uint8Array::from(entry.hash_digest_bytes.as_slice()));
    row.push(&strings_to_array(trim_hashes));
    row.push(&JsValue::from_bool(document_trimmed_heads_processed));
    row
}

fn native_backbone_trim_entries_to_rows(values: Vec<LogIndexEntry>) -> Array {
    let out = Array::new();
    for entry in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&entry.hash));
        row.push(&JsValue::from_str(&entry.gid));
        row.push(&strings_to_array(entry.next));
        row.push(&JsValue::from_f64(entry.entry_type as f64));
        row.push(&JsValue::from_str(&entry.wall_time.to_string()));
        row.push(&JsValue::from_f64(entry.logical as f64));
        row.push(&JsValue::from_f64(entry.payload_size as f64));
        match entry.data {
            Some(data) => row.push(&Uint8Array::from(data.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
        out.push(&row);
    }
    out
}

fn string_field(row: &Array, index: u32, label: &str) -> Result<String, JsValue> {
    row.get(index)
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("Expected {label} string")))
}

fn stringish_field(row: &Array, index: u32, label: &str) -> Result<String, JsValue> {
    let value = row.get(index);
    if let Some(value) = value.as_string() {
        return Ok(value);
    }
    if let Some(value) = value.as_f64() {
        return Ok((value as u64).to_string());
    }
    Err(JsValue::from_str(&format!("Expected {label} string")))
}

fn bool_field(row: &Array, index: u32, label: &str) -> Result<bool, JsValue> {
    row.get(index)
        .as_bool()
        .ok_or_else(|| JsValue::from_str(&format!("Expected {label} boolean")))
}

fn usize_field(row: &Array, index: u32, label: &str) -> Result<usize, JsValue> {
    row.get(index)
        .as_f64()
        .map(|value| value as usize)
        .ok_or_else(|| JsValue::from_str(&format!("Expected {label} number")))
}

fn bytes_field(row: &Array, index: u32, label: &str) -> Result<Vec<u8>, JsValue> {
    let value = row.get(index);
    if value.is_undefined() || value.is_null() {
        return Err(JsValue::from_str(&format!("Expected {label} bytes")));
    }
    Ok(Uint8Array::new(&value).to_vec())
}

fn trim_hashes_vec(trim_rows: &Array) -> Result<Vec<String>, JsValue> {
    let mut hashes = Vec::with_capacity(trim_rows.length() as usize);
    for index in 0..trim_rows.length() {
        let row = array_from_value(trim_rows.get(index), "trim row")?;
        hashes.push(string_field(&row, 0, "trim hash")?);
    }
    Ok(hashes)
}

fn leader_samples_to_optional_rows(
    values: &Option<Vec<peerbit_shared_log_rust::LeaderSample>>,
) -> JsValue {
    let Some(values) = values else {
        return JsValue::UNDEFINED;
    };
    let out = Array::new();
    for value in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&value.hash));
        row.push(&JsValue::from_bool(value.intersecting));
        out.push(&row);
    }
    out.into()
}

fn coordinate_plan_to_row(resolution: &str, facts: &NativeLocalAppendCompactFacts) -> Array {
    let coordinate = &facts.coordinate;
    let out = Array::new();
    out.push(&JsValue::from_str(&coordinate.hash));
    out.push(&number_to_row(resolution, coordinate.hash_number));
    out.push(&JsValue::from_str(&coordinate.gid));
    out.push(&numbers_to_rows(resolution, &coordinate.coordinates));
    out.push(&JsValue::from_bool(coordinate.assigned_to_range_boundary));
    out.push(&JsValue::from_f64(coordinate.requested_replicas as f64));
    out
}

fn numbers_to_rows(resolution: &str, values: &[u64]) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&number_to_row(resolution, *value));
    }
    out
}

fn number_to_row(resolution: &str, value: u64) -> JsValue {
    match resolution {
        "u64" => JsValue::from_str(&value.to_string()),
        _ => JsValue::from_f64(value as f64),
    }
}

fn strings_to_array(values: Vec<String>) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&JsValue::from_str(&value));
    }
    out
}

fn strings_from_array(values: Array) -> Result<Vec<String>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        out.push(
            values
                .get(index)
                .as_string()
                .ok_or_else(|| JsValue::from_str("Expected string array"))?,
        );
    }
    Ok(out)
}

fn string_batches_from_array(values: Array, label: &str) -> Result<Vec<Vec<String>>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values.get(index);
        if !Array::is_array(&value) {
            return Err(JsValue::from_str(&format!("Expected {label}")));
        }
        out.push(strings_from_array(Array::from(&value))?);
    }
    Ok(out)
}

fn coordinate_batches_from_array(values: Array) -> Result<Vec<Vec<u64>>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values.get(index);
        if !Array::is_array(&value) {
            return Err(JsValue::from_str("Expected coordinate batch array"));
        }
        out.push(coordinate_numbers_from_array(Array::from(&value))?);
    }
    Ok(out)
}

fn usize_values_from_array(values: Array) -> Result<Vec<usize>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values
            .get(index)
            .as_f64()
            .ok_or_else(|| JsValue::from_str("Expected unsigned integer array"))?;
        if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
            return Err(JsValue::from_str("Expected unsigned integer array"));
        }
        out.push(value as usize);
    }
    Ok(out)
}

fn ensure_same_len(left: usize, right: usize, label: &str) -> Result<(), JsValue> {
    if left == right {
        Ok(())
    } else {
        Err(JsValue::from_str(&format!(
            "Mismatched {label} input lengths"
        )))
    }
}

fn optional_bytes_from_js(value: JsValue) -> Option<Vec<u8>> {
    if value.is_undefined() || value.is_null() {
        return None;
    }
    Some(Uint8Array::new(&value).to_vec())
}

fn optional_usize_from_js(value: JsValue, label: &str) -> Result<Option<usize>, JsValue> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    value
        .as_f64()
        .map(|value| Some(value as usize))
        .ok_or_else(|| JsValue::from_str(&format!("{label} must be a number")))
}

fn coordinate_numbers_from_array(values: Array) -> Result<Vec<u64>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values.get(index);
        if let Some(value) = value.as_string() {
            out.push(parse_u64_string(&value, "coordinate")?);
        } else if let Some(value) = value.as_f64() {
            out.push(value as u64);
        } else {
            return Err(JsValue::from_str("Expected coordinate string array"));
        }
    }
    Ok(out)
}

fn number_strings_to_array(values: &[u64]) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&JsValue::from_str(&value.to_string()));
    }
    out
}

fn parse_u64_string(value: &str, label: &str) -> Result<u64, JsValue> {
    value
        .parse::<u64>()
        .map_err(|_| JsValue::from_str(&format!("Expected {label} u64 string")))
}

fn parse_optional_u64_string(value: &str, label: &str) -> Result<Option<u64>, JsValue> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_u64_string(value, label).map(Some)
    }
}

fn document_index_append_commit(
    key: String,
    value_prefix_bytes: Vec<u8>,
    existing_created: String,
    byte_element_index_limit: usize,
    delete_trimmed_heads: bool,
    projection_plan: JsValue,
    projection_encoded_document: JsValue,
    projection_signer: JsValue,
) -> Result<DocumentIndexAppendCommit, JsValue> {
    let value_prefix = if projection_plan.is_null() || projection_plan.is_undefined() {
        DocumentIndexValuePrefix::Bytes(value_prefix_bytes)
    } else {
        DocumentIndexValuePrefix::Projection {
            encoded_document: Uint8Array::new(&projection_encoded_document).to_vec(),
            plan: DocumentIndexProjectionPlan::Inline(parse_projection_plan(&projection_plan)?),
            signer: if projection_signer.is_null() || projection_signer.is_undefined() {
                None
            } else {
                Some(Uint8Array::new(&projection_signer).to_vec())
            },
        }
    };
    Ok(DocumentIndexAppendCommit {
        key,
        value_prefix,
        existing_created: parse_optional_u64_string(
            &existing_created,
            "document existing created",
        )?,
        byte_element_index_limit,
        delete_trimmed_heads,
    })
}

fn document_index_cached_projection_append_commit(
    key: String,
    existing_created: String,
    byte_element_index_limit: usize,
    delete_trimmed_heads: bool,
    projection_plan_id: u32,
    projection_encoded_document: JsValue,
    projection_signer: JsValue,
) -> Result<DocumentIndexAppendCommit, JsValue> {
    Ok(DocumentIndexAppendCommit {
        key,
        value_prefix: DocumentIndexValuePrefix::Projection {
            encoded_document: Uint8Array::new(&projection_encoded_document).to_vec(),
            plan: DocumentIndexProjectionPlan::Cached(projection_plan_id as usize),
            signer: if projection_signer.is_null() || projection_signer.is_undefined() {
                None
            } else {
                Some(Uint8Array::new(&projection_signer).to_vec())
            },
        },
        existing_created: parse_optional_u64_string(
            &existing_created,
            "document existing created",
        )?,
        byte_element_index_limit,
        delete_trimmed_heads,
    })
}

struct CoordinateCoreValue {
    hash: String,
    gid: String,
    hash_number: u64,
    coordinates: Vec<u64>,
    assigned_to_range_boundary: bool,
    requested_replicas: usize,
    wall_time: u64,
    meta_bytes: Vec<u8>,
}

fn encode_coordinate_value(
    hash: &str,
    gid: &str,
    hash_number: u64,
    coordinates: &[u64],
    assigned_to_range_boundary: bool,
    requested_replicas: usize,
    wall_time: u64,
    meta_bytes: &[u8],
) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(76 + hash.len() + gid.len() + coordinates.len() * 8 + meta_bytes.len());
    write_string(&mut out, hash);
    write_string(&mut out, gid);
    out.extend_from_slice(&hash_number.to_le_bytes());
    out.push(u8::from(assigned_to_range_boundary));
    out.extend_from_slice(&(requested_replicas as u64).to_le_bytes());
    out.extend_from_slice(&(coordinates.len() as u32).to_le_bytes());
    for coordinate in coordinates {
        out.extend_from_slice(&coordinate.to_le_bytes());
    }
    out.extend_from_slice(&wall_time.to_le_bytes());
    write_bytes(&mut out, meta_bytes);
    out
}

fn coordinate_core_value_to_row(value: &CoordinateCoreValue) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&value.hash));
    row.push(&JsValue::from_str(&value.hash_number.to_string()));
    row.push(&JsValue::from_str(&value.gid));
    row.push(&number_strings_to_array(&value.coordinates));
    row.push(&JsValue::from_bool(value.assigned_to_range_boundary));
    row.push(&JsValue::from_f64(value.requested_replicas as f64));
    row.push(&JsValue::from_str(&value.wall_time.to_string()));
    row.push(&Uint8Array::from(value.meta_bytes.as_slice()));
    row
}

fn document_entry_to_row(key: &str, value: &[u8]) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(key));
    row.push(&Uint8Array::from(value));
    row
}

fn sum_to_js(sum: SumResult) -> Array {
    let out = Array::new();
    match sum {
        SumResult::None => {
            out.push(&JsValue::from_str("none"));
            out.push(&JsValue::from_str("0"));
        }
        SumResult::I64(value) => {
            out.push(&JsValue::from_str("i64"));
            out.push(&JsValue::from_str(&value.to_string()));
        }
        SumResult::U64(value) => {
            out.push(&JsValue::from_str("u64"));
            out.push(&JsValue::from_str(&value.to_string()));
        }
    }
    out
}

fn decode_coordinate_value(bytes: &[u8]) -> Result<CoordinateCoreValue, JsValue> {
    let mut offset = 0usize;
    let hash = read_encoded_string(bytes, &mut offset, "coordinate hash")?;
    let gid = read_encoded_string(bytes, &mut offset, "coordinate gid")?;
    let hash_number = read_u64(bytes, &mut offset, "coordinate hash number")?;
    let assigned_to_range_boundary = read_bool(bytes, &mut offset, "assigned to range boundary")?;
    let requested_replicas = read_u64(bytes, &mut offset, "requested replicas")? as usize;
    let coordinate_count = read_u32(bytes, &mut offset, "coordinate count")? as usize;
    let mut coordinates = Vec::with_capacity(coordinate_count);
    for _ in 0..coordinate_count {
        coordinates.push(read_u64(bytes, &mut offset, "coordinate value")?);
    }
    let (wall_time, meta_bytes) = if offset == bytes.len() {
        (0, Vec::new())
    } else {
        let wall_time = read_u64(bytes, &mut offset, "coordinate wall time")?;
        let meta_bytes = read_bytes(bytes, &mut offset, "coordinate meta bytes")?;
        (wall_time, meta_bytes)
    };
    if offset != bytes.len() {
        return Err(JsValue::from_str("Trailing coordinate value bytes"));
    }
    Ok(CoordinateCoreValue {
        hash,
        gid,
        hash_number,
        coordinates,
        assigned_to_range_boundary,
        requested_replicas,
        wall_time,
        meta_bytes,
    })
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

fn encode_document_context_suffix(
    created: u64,
    modified: u64,
    head: &str,
    gid: &str,
    size: u32,
) -> Result<Vec<u8>, JsValue> {
    let capacity = 1usize
        .checked_add(8)
        .and_then(|value| value.checked_add(8))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(head.len()))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(gid.len()))
        .and_then(|value| value.checked_add(4))
        .ok_or_else(|| JsValue::from_str("Document context suffix capacity overflow"))?;
    let mut out = Vec::with_capacity(capacity);
    // Context is @variant(0); keep this byte-for-byte aligned with Borsh.
    out.push(0);
    write_u64(&mut out, created);
    write_u64(&mut out, modified);
    write_string(&mut out, head);
    write_string(&mut out, gid);
    write_u32(&mut out, size);
    Ok(out)
}

fn read_u32(bytes: &[u8], offset: &mut usize, label: &str) -> Result<u32, JsValue> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| JsValue::from_str(&format!("Truncated {label}")))?;
    if end > bytes.len() {
        return Err(JsValue::from_str(&format!("Truncated {label}")));
    }
    let value = u32::from_le_bytes(bytes[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_u64(bytes: &[u8], offset: &mut usize, label: &str) -> Result<u64, JsValue> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| JsValue::from_str(&format!("Truncated {label}")))?;
    if end > bytes.len() {
        return Err(JsValue::from_str(&format!("Truncated {label}")));
    }
    let value = u64::from_le_bytes(bytes[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_bool(bytes: &[u8], offset: &mut usize, label: &str) -> Result<bool, JsValue> {
    if *offset >= bytes.len() {
        return Err(JsValue::from_str(&format!("Truncated {label}")));
    }
    let value = bytes[*offset] != 0;
    *offset += 1;
    Ok(value)
}

fn read_encoded_string(bytes: &[u8], offset: &mut usize, label: &str) -> Result<String, JsValue> {
    let length = read_u32(bytes, offset, label)? as usize;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| JsValue::from_str(&format!("Truncated {label}")))?;
    if end > bytes.len() {
        return Err(JsValue::from_str(&format!("Truncated {label}")));
    }
    let value = std::str::from_utf8(&bytes[*offset..end])
        .map_err(|_| JsValue::from_str(&format!("Invalid utf-8 {label}")))?
        .to_string();
    *offset = end;
    Ok(value)
}

fn read_bytes(bytes: &[u8], offset: &mut usize, label: &str) -> Result<Vec<u8>, JsValue> {
    let length = read_u32(bytes, offset, label)? as usize;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| JsValue::from_str(&format!("Truncated {label}")))?;
    if end > bytes.len() {
        return Err(JsValue::from_str(&format!("Truncated {label}")));
    }
    let value = bytes[*offset..end].to_vec();
    *offset = end;
    Ok(value)
}

fn js_get(value: &JsValue, key: &str) -> JsValue {
    Reflect::get(value, &JsValue::from_str(key)).unwrap_or(JsValue::UNDEFINED)
}

fn js_string(value: JsValue, field: &str) -> Result<String, JsValue> {
    value
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("Missing or invalid {field}")))
}

fn array_strings(value: JsValue, field: &str) -> Result<Vec<String>, JsValue> {
    if !Array::is_array(&value) {
        return Err(JsValue::from_str(&format!("{field} must be an array")));
    }
    let array = Array::from(&value);
    let mut out = Vec::with_capacity(array.length() as usize);
    for index in 0..array.length() {
        out.push(js_string(array.get(index), field)?);
    }
    Ok(out)
}

fn optional_string(value: JsValue) -> Option<String> {
    if value.is_null() || value.is_undefined() {
        None
    } else {
        value.as_string()
    }
}

#[derive(Clone, Debug)]
enum ProjectionValue {
    String(String),
    U64(u64),
    Bool(bool),
    Bytes(Vec<u8>),
    None,
}

fn read_u8_projection(bytes: &[u8], offset: &mut usize, label: &str) -> Result<u8, JsValue> {
    if *offset >= bytes.len() {
        return Err(JsValue::from_str(&format!("Truncated {label}")));
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_bool_projection(bytes: &[u8], offset: &mut usize, label: &str) -> Result<bool, JsValue> {
    match read_u8_projection(bytes, offset, label)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(JsValue::from_str(&format!("Invalid bool {label}"))),
    }
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(if value { 1 } else { 0 });
}

fn skip_projection_value(bytes: &[u8], offset: &mut usize, kind: &str) -> Result<(), JsValue> {
    match kind {
        "string" => {
            read_encoded_string(bytes, offset, "projected string")?;
        }
        "u8" => {
            read_u8_projection(bytes, offset, "projected u8")?;
        }
        "u32" => {
            read_u32(bytes, offset, "projected u32")?;
        }
        "u64" => {
            read_u64(bytes, offset, "projected u64")?;
        }
        "bool" => {
            read_bool_projection(bytes, offset, "projected bool")?;
        }
        "bytes" => {
            read_bytes(bytes, offset, "projected bytes")?;
        }
        "option:string" | "option:u8" | "option:u32" | "option:u64" | "option:bool"
        | "option:bytes" => {
            let has_value = read_u8_projection(bytes, offset, "projected option")?;
            if has_value == 1 {
                skip_projection_value(bytes, offset, &kind["option:".len()..])?;
            } else if has_value != 0 {
                return Err(JsValue::from_str("Invalid projection option marker"));
            }
        }
        "vec:string" => {
            let len = read_u32(bytes, offset, "projected string vec length")?;
            for _ in 0..len {
                read_encoded_string(bytes, offset, "projected string vec item")?;
            }
        }
        "vec:bytes" => {
            let len = read_u32(bytes, offset, "projected bytes vec length")?;
            for _ in 0..len {
                read_bytes(bytes, offset, "projected bytes vec item")?;
            }
        }
        _ => {
            return Err(JsValue::from_str(
                "Unsupported document projection field type",
            ))
        }
    }
    Ok(())
}

fn read_projection_value(
    bytes: &[u8],
    offset: &mut usize,
    kind: &str,
) -> Result<ProjectionValue, JsValue> {
    match kind {
        "string" => Ok(ProjectionValue::String(read_encoded_string(
            bytes,
            offset,
            "projection string",
        )?)),
        "u8" => Ok(ProjectionValue::U64(
            read_u8_projection(bytes, offset, "projection u8")? as u64,
        )),
        "u32" => Ok(ProjectionValue::U64(
            read_u32(bytes, offset, "projection u32")? as u64,
        )),
        "u64" => Ok(ProjectionValue::U64(read_u64(
            bytes,
            offset,
            "projection u64",
        )?)),
        "bool" => Ok(ProjectionValue::Bool(read_bool_projection(
            bytes,
            offset,
            "projection bool",
        )?)),
        "bytes" => Ok(ProjectionValue::Bytes(read_bytes(
            bytes,
            offset,
            "projection bytes",
        )?)),
        "option:string" => {
            let has_value = read_u8_projection(bytes, offset, "projection option string")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::String(read_encoded_string(
                    bytes,
                    offset,
                    "projection option string",
                )?))
            } else {
                Err(JsValue::from_str("Invalid projection option marker"))
            }
        }
        "option:u8" => {
            let has_value = read_u8_projection(bytes, offset, "projection option u8")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::U64(
                    read_u8_projection(bytes, offset, "projection option u8")? as u64,
                ))
            } else {
                Err(JsValue::from_str("Invalid projection option marker"))
            }
        }
        "option:u32" => {
            let has_value = read_u8_projection(bytes, offset, "projection option u32")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::U64(
                    read_u32(bytes, offset, "projection option u32")? as u64,
                ))
            } else {
                Err(JsValue::from_str("Invalid projection option marker"))
            }
        }
        "option:u64" => {
            let has_value = read_u8_projection(bytes, offset, "projection option u64")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::U64(read_u64(
                    bytes,
                    offset,
                    "projection option u64",
                )?))
            } else {
                Err(JsValue::from_str("Invalid projection option marker"))
            }
        }
        "option:bool" => {
            let has_value = read_u8_projection(bytes, offset, "projection option bool")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::Bool(read_bool_projection(
                    bytes,
                    offset,
                    "projection option bool",
                )?))
            } else {
                Err(JsValue::from_str("Invalid projection option marker"))
            }
        }
        "option:bytes" => {
            let has_value = read_u8_projection(bytes, offset, "projection option bytes")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::Bytes(read_bytes(
                    bytes,
                    offset,
                    "projection option bytes",
                )?))
            } else {
                Err(JsValue::from_str("Invalid projection option marker"))
            }
        }
        _ => Err(JsValue::from_str(
            "Unsupported projected document field type",
        )),
    }
}

fn write_projection_value(
    out: &mut Vec<u8>,
    kind: &str,
    value: &ProjectionValue,
) -> Result<(), JsValue> {
    match (kind, value) {
        ("string", ProjectionValue::String(value)) => write_string(out, value),
        ("u8", ProjectionValue::U64(value)) => write_u8(out, *value as u8),
        ("u32", ProjectionValue::U64(value)) => write_u32(out, *value as u32),
        ("u64", ProjectionValue::U64(value)) => write_u64(out, *value),
        ("bool", ProjectionValue::Bool(value)) => write_bool(out, *value),
        ("bytes", ProjectionValue::Bytes(value)) => write_bytes(out, value),
        ("option:string", ProjectionValue::None)
        | ("option:u8", ProjectionValue::None)
        | ("option:u32", ProjectionValue::None)
        | ("option:u64", ProjectionValue::None)
        | ("option:bool", ProjectionValue::None)
        | ("option:bytes", ProjectionValue::None) => write_u8(out, 0),
        ("option:string", ProjectionValue::String(value)) => {
            write_u8(out, 1);
            write_string(out, value);
        }
        ("option:u8", ProjectionValue::U64(value)) => {
            write_u8(out, 1);
            write_u8(out, *value as u8);
        }
        ("option:u32", ProjectionValue::U64(value)) => {
            write_u8(out, 1);
            write_u32(out, *value as u32);
        }
        ("option:u64", ProjectionValue::U64(value)) => {
            write_u8(out, 1);
            write_u64(out, *value);
        }
        ("option:bool", ProjectionValue::Bool(value)) => {
            write_u8(out, 1);
            write_bool(out, *value);
        }
        ("option:bytes", ProjectionValue::Bytes(value)) => {
            write_u8(out, 1);
            write_bytes(out, value);
        }
        _ => {
            return Err(JsValue::from_str(
                "Projection value does not match output type",
            ))
        }
    }
    Ok(())
}

fn read_projected_document_fields(
    encoded_document: &[u8],
    variant_type: Option<&str>,
    variant_value: Option<&str>,
    names: &[String],
    types: &[String],
) -> Result<HashMap<String, ProjectionValue>, JsValue> {
    if names.len() != types.len() {
        return Err(JsValue::from_str(
            "Document projection plan length mismatch",
        ));
    }
    let mut offset = 0usize;
    match variant_type {
        Some("u8") => {
            let expected = variant_value
                .ok_or_else(|| JsValue::from_str("Missing document variant"))?
                .parse::<u8>()
                .map_err(|_| JsValue::from_str("Invalid document variant"))?;
            if read_u8_projection(encoded_document, &mut offset, "document variant")? != expected {
                return Err(JsValue::from_str("Document variant mismatch"));
            }
        }
        Some("string") => {
            let expected =
                variant_value.ok_or_else(|| JsValue::from_str("Missing document variant"))?;
            if read_encoded_string(encoded_document, &mut offset, "document variant")? != expected {
                return Err(JsValue::from_str("Document variant mismatch"));
            }
        }
        Some("") | None => {}
        _ => return Err(JsValue::from_str("Unsupported document variant type")),
    }
    let mut out = HashMap::with_capacity(names.len());
    for (name, kind) in names.iter().zip(types.iter()) {
        let before = offset;
        let value = read_projection_value(encoded_document, &mut offset, kind);
        match value {
            Ok(value) => {
                out.insert(name.clone(), value);
            }
            Err(_) => {
                offset = before;
                skip_projection_value(encoded_document, &mut offset, kind)?;
            }
        }
    }
    Ok(out)
}

fn write_projection_variant(
    out: &mut Vec<u8>,
    variant_type: Option<&str>,
    variant_value: Option<&str>,
) -> Result<(), JsValue> {
    match variant_type {
        Some("u8") => {
            let value = variant_value
                .ok_or_else(|| JsValue::from_str("Missing output variant"))?
                .parse::<u8>()
                .map_err(|_| JsValue::from_str("Invalid output variant"))?;
            write_u8(out, value);
        }
        Some("string") => {
            let value = variant_value.ok_or_else(|| JsValue::from_str("Missing output variant"))?;
            write_string(out, value);
        }
        Some("") | None => {}
        _ => return Err(JsValue::from_str("Unsupported output variant type")),
    }
    Ok(())
}

fn parse_projection_plan(plan: &JsValue) -> Result<ParsedProjectionPlan, JsValue> {
    let document_field_names =
        array_strings(js_get(plan, "documentFieldNames"), "documentFieldNames")?;
    let document_field_types =
        array_strings(js_get(plan, "documentFieldTypes"), "documentFieldTypes")?;
    let output_field_types = array_strings(js_get(plan, "outputFieldTypes"), "outputFieldTypes")?;
    let source_kinds = array_strings(js_get(plan, "sourceKinds"), "sourceKinds")?;
    let source_values = array_strings(js_get(plan, "sourceValues"), "sourceValues")?;
    if output_field_types.len() != source_kinds.len() || source_kinds.len() != source_values.len() {
        return Err(JsValue::from_str("Projection plan length mismatch"));
    }
    Ok(ParsedProjectionPlan {
        document_variant_type: optional_string(js_get(plan, "documentVariantType")),
        document_variant_value: optional_string(js_get(plan, "documentVariantValue")),
        output_variant_type: optional_string(js_get(plan, "outputVariantType")),
        output_variant_value: optional_string(js_get(plan, "outputVariantValue")),
        document_field_names,
        document_field_types,
        output_field_types,
        source_kinds,
        source_values,
    })
}

fn project_document_index_simple_bytes_with_plan(
    encoded_document: &[u8],
    plan: &ParsedProjectionPlan,
    created: u64,
    modified: u64,
    gid: &str,
    size: u32,
    signer: Option<&[u8]>,
) -> Result<Vec<u8>, JsValue> {
    let document_values = read_projected_document_fields(
        encoded_document,
        plan.document_variant_type.as_deref(),
        plan.document_variant_value.as_deref(),
        &plan.document_field_names,
        &plan.document_field_types,
    )?;

    let mut out = Vec::new();
    write_projection_variant(
        &mut out,
        plan.output_variant_type.as_deref(),
        plan.output_variant_value.as_deref(),
    )?;

    for index in 0..plan.output_field_types.len() {
        let value = match plan.source_kinds[index].as_str() {
            "field" => document_values
                .get(&plan.source_values[index])
                .cloned()
                .unwrap_or(ProjectionValue::None),
            "context" => match plan.source_values[index].as_str() {
                "created" => ProjectionValue::U64(created),
                "modified" => ProjectionValue::U64(modified),
                "gid" => ProjectionValue::String(gid.to_string()),
                "size" => ProjectionValue::U64(size as u64),
                _ => return Err(JsValue::from_str("Unsupported context projection source")),
            },
            "entryFirstSignerPublicKey" => signer
                .map(|bytes| ProjectionValue::Bytes(bytes.to_vec()))
                .unwrap_or(ProjectionValue::None),
            _ => return Err(JsValue::from_str("Unsupported projection source kind")),
        };
        write_projection_value(&mut out, &plan.output_field_types[index], &value)?;
    }

    Ok(out)
}

fn project_document_index_simple_bytes(
    encoded_document: &[u8],
    plan: &JsValue,
    created: &str,
    modified: &str,
    gid: &str,
    size: u32,
    signer: JsValue,
) -> Result<Vec<u8>, JsValue> {
    let plan = parse_projection_plan(plan)?;
    let created = parse_u64_string(created, "created")?;
    let modified = parse_u64_string(modified, "modified")?;
    let signer = if signer.is_null() || signer.is_undefined() {
        None
    } else {
        Some(Uint8Array::new(&signer).to_vec())
    };
    project_document_index_simple_bytes_with_plan(
        encoded_document,
        &plan,
        created,
        modified,
        gid,
        size,
        signer.as_deref(),
    )
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn decode_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn hash_number_u64(resolution: &str, digest: &[u8]) -> Result<u64, JsValue> {
    match resolution {
        "u32" => {
            if digest.len() < 4 {
                return Err(JsValue::from_str("hash digest must have at least 4 bytes"));
            }
            Ok(u32::from_le_bytes(digest[0..4].try_into().unwrap()) as u64)
        }
        "u64" => {
            if digest.len() < 8 {
                return Err(JsValue::from_str("hash digest must have at least 8 bytes"));
            }
            Ok(u64::from_le_bytes(digest[0..8].try_into().unwrap()))
        }
        _ => Err(JsValue::from_str("resolution must be u32 or u64")),
    }
}

#[cfg(test)]
mod tests {
    use super::hash_number_u64;

    #[test]
    fn decodes_hash_numbers_like_shared_log_integer_helpers() {
        let bytes = [1, 0, 0, 0, 2, 0, 0, 0];
        assert_eq!(hash_number_u64("u32", &bytes).unwrap(), 1);
        assert_eq!(hash_number_u64("u64", &bytes).unwrap(), 8_589_934_593);
    }
}
