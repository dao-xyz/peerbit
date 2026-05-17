use js_sys::{Array, Uint8Array};
use peerbit_indexer_core::codec::{decode_query, decode_sort};
use peerbit_indexer_core::persistence::{
    decode_journal, decode_key_value_snapshot, encode_journal_payload, encode_journal_record,
    encode_journal_records, encode_key_value_snapshot, JournalRecord, JOURNAL_MAGIC,
};
use peerbit_indexer_core::planner::{
    DocumentFields, FieldPath, FieldValue, NativeQueryIndex, SumResult,
};
use peerbit_indexer_core::schema::{
    decode_native_schema_ir, extract_encoded_document_fields_from_parts, NativeSchemaIr,
};
use peerbit_indexer_core::storage::{ByteStorage, MemoryByteStorage};
use peerbit_log_rust::{
    LogIndexEntry, NativeCommittedEntryFacts, NativeEntryV0PlainBuilder, NativeLogBlockStore,
    NativeLogIndex,
};
use peerbit_shared_log_rust::{
    commit_local_append_for_gid_compact_core, NativeLocalAppendCompactFacts, NativeSharedLogState,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

const COORD_HASH_FIELD: u32 = 1;
const COORD_GID_FIELD: u32 = 2;
const COORD_HASH_NUMBER_FIELD: u32 = 3;
const COORD_COORDINATE_FIELD: u32 = 4;
const COORD_ASSIGNED_TO_RANGE_BOUNDARY_FIELD: u32 = 5;
const COORD_REQUESTED_REPLICAS_FIELD: u32 = 6;

#[wasm_bindgen]
pub struct NativePeerbitBackbone {
    resolution: String,
    log: NativeLogIndex,
    blocks: NativeLogBlockStore,
    shared_log: NativeSharedLogState,
    coordinate_index: NativeQueryIndex,
    coordinate_values: MemoryByteStorage,
    coordinate_journal: Vec<JournalRecord>,
    coordinate_journal_byte_len: usize,
    coordinate_journal_enabled: bool,
    document_index: NativeQueryIndex,
    document_values: MemoryByteStorage,
    document_schema_ir: Option<NativeSchemaIr>,
    builder: NativeEntryV0PlainBuilder,
}

struct DocumentIndexAppendCommit {
    key: String,
    value_prefix_bytes: Vec<u8>,
    existing_created: Option<u64>,
    byte_element_index_limit: usize,
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
            coordinate_index: NativeQueryIndex::new(),
            coordinate_values: MemoryByteStorage::new(),
            coordinate_journal: Vec::new(),
            coordinate_journal_byte_len: 0,
            coordinate_journal_enabled: false,
            document_index: NativeQueryIndex::new(),
            document_values: MemoryByteStorage::new(),
            document_schema_ir: None,
            builder: NativeEntryV0PlainBuilder::new(clock_id, private_key, public_key)?,
        })
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
        self.coordinate_index
            .exact_first(
                &FieldPath::Id(COORD_HASH_FIELD),
                &FieldValue::String(hash.to_string()),
            )
            .is_some_and(|id| id == hash)
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

    pub fn document_index_len(&self) -> usize {
        self.document_index.len()
    }

    pub fn document_value_len(&self) -> usize {
        self.document_values.len()
    }

    pub fn document_exact_string_first_key(&self, field: u32, value: String) -> JsValue {
        self.document_index
            .exact_first(&FieldPath::Id(field), &FieldValue::String(value))
            .map(|key| JsValue::from_str(&key))
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn document_index_has_exact_string(&self, field: u32, value: String, key: &str) -> bool {
        self.document_index
            .exact_first(&FieldPath::Id(field), &FieldValue::String(value))
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
        value_prefix_bytes: Vec<u8>,
        value_suffix_bytes: Vec<u8>,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let schema_ir = self.document_schema_ir.as_ref().ok_or_else(|| {
            js_error("Native backbone document schema IR has not been configured")
        })?;
        let fields = extract_encoded_document_fields_from_parts(
            schema_ir,
            &value_prefix_bytes,
            &value_suffix_bytes,
            byte_element_index_limit,
        )
        .map_err(js_error)?;
        let value = concat_bytes(&value_prefix_bytes, &value_suffix_bytes);
        self.document_index.put(key.clone(), fields);
        self.document_values.put(key, value);
        Ok(())
    }

    pub fn delete_document(&mut self, key: &str) -> bool {
        self.document_index.delete(key.to_string());
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
        self.coordinate_journal.len()
    }

    pub fn coordinate_pending_journal_byte_len(&self) -> usize {
        self.coordinate_journal_byte_len
    }

    pub fn coordinate_journal_enabled(&self) -> bool {
        self.coordinate_journal_enabled
    }

    pub fn set_coordinate_journal_enabled(&mut self, enabled: bool) {
        self.coordinate_journal_enabled = enabled;
        if !enabled {
            self.coordinate_journal.clear();
            self.coordinate_journal_byte_len = 0;
        }
    }

    pub fn coordinate_journal(&self) -> Vec<u8> {
        encode_journal_records(self.coordinate_journal.clone())
    }

    pub fn clear_coordinate_journal(&mut self) {
        self.coordinate_journal.clear();
        self.coordinate_journal_byte_len = 0;
    }

    pub fn drain_coordinate_journal(&mut self) -> Vec<u8> {
        self.coordinate_journal_byte_len = 0;
        encode_journal_records(std::mem::take(&mut self.coordinate_journal))
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
        self.coordinate_journal_byte_len = 0;
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
    pub fn append_plain_no_next_transaction(
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
    ) -> Result<Array, JsValue> {
        self.append_plain_no_next_transaction_inner(
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
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_plain_no_next_transaction_trim(
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
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.append_plain_no_next_transaction_inner(
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
            Some(trim_length_to),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_plain_no_next_document_index_transaction(
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
    ) -> Result<Array, JsValue> {
        self.append_plain_no_next_transaction_inner(
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
            None,
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_plain_no_next_document_index_transaction_trim(
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
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        self.append_plain_no_next_transaction_inner(
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
            Some(trim_length_to),
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
        )
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
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let document_index_commit = DocumentIndexAppendCommit {
            key: document_key,
            value_prefix_bytes: document_value_prefix_bytes,
            existing_created: parse_optional_u64_string(
                &document_existing_created,
                "document existing created",
            )?,
            byte_element_index_limit: document_byte_element_index_limit,
        };
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
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
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
            None,
            false,
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
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
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
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
            Some(trim_length_to),
            false,
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
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
            Some(trim_length_to),
            true,
            None,
        )
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
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
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
            None,
            true,
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
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
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
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
            Some(trim_length_to),
            true,
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
        )
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
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
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
            None,
            false,
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
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
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
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
            Some(trim_length_to),
            false,
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
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
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
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
            None,
            true,
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
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
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_existing_created: String,
        document_byte_element_index_limit: usize,
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
            Some(trim_length_to),
            true,
            Some(DocumentIndexAppendCommit {
                key: document_key,
                value_prefix_bytes: document_value_prefix_bytes,
                existing_created: parse_optional_u64_string(
                    &document_existing_created,
                    "document existing created",
                )?,
                byte_element_index_limit: document_byte_element_index_limit,
            }),
        )
    }
}

impl NativePeerbitBackbone {
    fn clear_coordinate_core(&mut self) {
        self.coordinate_index.clear();
        self.coordinate_values.clear();
        self.coordinate_journal.clear();
        self.coordinate_journal_byte_len = 0;
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
        self.delete_coordinate_core_strings(next_hashes);
        self.delete_coordinate_core_strings(delete_hashes);
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
        let mut fields = DocumentFields::with_scalar_capacity(6 + coordinates.len());
        fields.insert_scalar(FieldPath::Id(COORD_HASH_FIELD), hash.clone());
        fields.insert_scalar(FieldPath::Id(COORD_GID_FIELD), gid.clone());
        fields.insert_scalar(FieldPath::Id(COORD_HASH_NUMBER_FIELD), hash_number);
        for coordinate in &coordinates {
            fields.insert_scalar(FieldPath::Id(COORD_COORDINATE_FIELD), *coordinate);
        }
        fields.insert_scalar(
            FieldPath::Id(COORD_ASSIGNED_TO_RANGE_BOUNDARY_FIELD),
            assigned_to_range_boundary,
        );
        fields.insert_scalar(
            FieldPath::Id(COORD_REQUESTED_REPLICAS_FIELD),
            requested_replicas as u64,
        );

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
        let journal_record = if record_journal {
            Some(JournalRecord::put(hash.clone(), value.clone()))
        } else {
            None
        };
        self.coordinate_index.put(hash.clone(), fields);
        self.coordinate_values.put(hash, value);
        if self.coordinate_journal_enabled {
            if let Some(record) = journal_record {
                self.push_coordinate_journal_record(record);
            }
        }
    }

    fn delete_coordinate_core(&mut self, hash: &str) -> bool {
        self.coordinate_index.delete(hash.to_string());
        if self.coordinate_journal_enabled {
            self.push_coordinate_journal_record(JournalRecord::delete(hash));
        }
        self.coordinate_values.delete(hash)
    }

    fn push_coordinate_journal_record(&mut self, record: JournalRecord) {
        self.coordinate_journal_byte_len +=
            encode_journal_record(&encode_journal_payload(&record)).len();
        self.coordinate_journal.push(record);
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
    fn append_plain_no_next_transaction_inner(
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
        trim_length_to: Option<usize>,
        document_index_commit: Option<DocumentIndexAppendCommit>,
    ) -> Result<Array, JsValue> {
        let payload_size = payload_data.length();
        let (entry_row, trim_rows) = if let Some(trim_length_to) = trim_length_to {
            let row = self
                .log
                .prepare_entry_v0_plain_entry_commit_no_next_facts_trim_and_put_with_builder(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid.clone(),
                    entry_type,
                    meta_data,
                    payload_data,
                    trim_length_to,
                )?;
            let row = array_from_value(row.into(), "native trim append row")?;
            let entry_row = array_from_value(row.get(0), "native trim append entry row")?;
            let trim_rows = array_from_value(row.get(1), "native trim append trim rows")?;
            (entry_row, trim_rows)
        } else {
            let row = self
                .log
                .prepare_entry_v0_plain_entry_commit_no_next_facts_and_put_with_builder(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid.clone(),
                    entry_type,
                    meta_data,
                    payload_data,
                )?;
            (row, Array::new())
        };

        let hash = string_field(&entry_row, 0, "entry hash")?;
        let digest = bytes_field(&entry_row, 3, "entry hash digest")?;
        let hash_number = hash_number_string(&self.resolution, &digest)?;
        let delete_hashes = trim_hashes(&trim_rows)?;
        let delete_hashes_for_core = delete_hashes.clone();
        let next_hashes = Array::new();
        let next_hashes_for_core = next_hashes.clone();
        let meta_bytes = bytes_field(&entry_row, 1, "entry meta bytes")?;
        let document_hash = hash.clone();
        let document_gid = gid.clone();
        let coordinate_row = self.shared_log.commit_local_append_for_gid_compact(
            hash,
            gid,
            hash_number,
            next_hashes,
            delete_hashes,
            replicas,
            role_age_ms,
            now,
            JsValue::UNDEFINED,
            true,
            self_hash,
            self_replicating,
            true,
            true,
        )?;
        self.commit_coordinate_core_from_compact_row(
            coordinate_row.get(3),
            next_hashes_for_core,
            delete_hashes_for_core,
            wall_time,
            meta_bytes,
        )?;
        self.put_document_index_for_append(
            document_index_commit,
            wall_time,
            &document_hash,
            &document_gid,
            payload_size,
        )?;

        let out = Array::new();
        out.push(&entry_row);
        out.push(&coordinate_row.get(0));
        out.push(&coordinate_row.get(1));
        out.push(&coordinate_row.get(2));
        out.push(&coordinate_row.get(3));
        out.push(&trim_rows);
        Ok(out)
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
        trim_length_to: Option<usize>,
        commit_blocks: bool,
        document_index_commit: Option<DocumentIndexAppendCommit>,
    ) -> Result<Array, JsValue> {
        let payload_size = payload_data.length();
        let (entry_row, trim_rows, trim_hashes, hash, digest, meta_bytes) = if commit_blocks {
            let (entry_facts, trimmed_entries) = self
                .log
                .prepare_entry_v0_plain_entry_commit_facts_core_and_put_with_builder(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid.clone(),
                    next_hashes.clone(),
                    entry_type,
                    optional_bytes_from_js(meta_data),
                    payload_data.to_vec(),
                    trim_length_to,
                )?;
            let trim_hashes = trimmed_entries
                .iter()
                .map(|entry| entry.hash.clone())
                .collect::<Vec<_>>();
            let entry_row = committed_entry_facts_to_row(&entry_facts);
            let trim_rows = native_backbone_trim_entries_to_rows(trimmed_entries);
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

        let hash_number = hash_number_u64(&self.resolution, &digest)?;
        let next_hashes_for_core = next_hashes.clone();
        let trim_hashes_for_core = trim_hashes.clone();
        let document_hash = hash.clone();
        let document_gid = gid.clone();
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
        self.commit_coordinate_core_from_compact_facts(
            &coordinate_facts,
            next_hashes_for_core,
            trim_hashes_for_core,
            wall_time,
            meta_bytes,
        );
        self.put_document_index_for_append(
            document_index_commit,
            wall_time,
            &document_hash,
            &document_gid,
            payload_size,
        )?;

        let out = Array::new();
        out.push(&entry_row);
        out.push(&leader_samples_to_optional_rows(&coordinate_facts.leaders));
        out.push(&JsValue::from_bool(coordinate_facts.is_leader));
        out.push(&JsValue::from_bool(
            coordinate_facts.assigned_to_range_boundary,
        ));
        out.push(&coordinate_plan_to_row(&self.resolution, &coordinate_facts));
        out.push(&trim_rows);
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
        let context_suffix = encode_document_context_suffix(
            document_index_commit.existing_created.unwrap_or(wall_time),
            wall_time,
            hash,
            gid,
            payload_size,
        )?;
        self.put_document_encoded_parts_stored(
            document_index_commit.key,
            document_index_commit.value_prefix_bytes,
            context_suffix,
            document_index_commit.byte_element_index_limit,
        )
    }
}

fn array_from_value(value: JsValue, label: &str) -> Result<Array, JsValue> {
    value
        .dyn_into::<Array>()
        .map_err(|_| JsValue::from_str(&format!("Expected {label} array")))
}

fn committed_entry_facts_to_row(entry: &NativeCommittedEntryFacts) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&entry.hash));
    row.push(&strings_to_array(entry.next.clone()));
    row.push(&Uint8Array::from(entry.meta_bytes.as_slice()));
    row.push(&JsValue::from_f64(entry.byte_length as f64));
    row.push(&Uint8Array::from(entry.hash_digest_bytes.as_slice()));
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

fn trim_hashes(trim_rows: &Array) -> Result<Array, JsValue> {
    let hashes = Array::new();
    for index in 0..trim_rows.length() {
        let row = array_from_value(trim_rows.get(index), "trim row")?;
        hashes.push(&JsValue::from_str(&string_field(&row, 0, "trim hash")?));
    }
    Ok(hashes)
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

fn optional_bytes_from_js(value: JsValue) -> Option<Vec<u8>> {
    if value.is_undefined() || value.is_null() {
        return None;
    }
    Some(Uint8Array::new(&value).to_vec())
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

fn concat_bytes(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + suffix.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(suffix);
    out
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn decode_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn hash_number_string(resolution: &str, digest: &[u8]) -> Result<String, JsValue> {
    Ok(hash_number_u64(resolution, digest)?.to_string())
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
    use super::hash_number_string;

    #[test]
    fn decodes_hash_numbers_like_shared_log_integer_helpers() {
        let bytes = [1, 0, 0, 0, 2, 0, 0, 0];
        assert_eq!(hash_number_string("u32", &bytes).unwrap(), "1");
        assert_eq!(hash_number_string("u64", &bytes).unwrap(), "8589934593");
    }
}
