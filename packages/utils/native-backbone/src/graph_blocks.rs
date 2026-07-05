use js_sys::{Array, BigUint64Array, Uint32Array, Uint8Array};
use peerbit_log_rust::entry_v0_signature_public_key_from_storage_bytes;
use wasm_bindgen::prelude::*;

use crate::js_interop::strings_from_array;
use crate::NativePeerbitBackbone;

#[wasm_bindgen]
impl NativePeerbitBackbone {
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

    pub fn graph_clear(&mut self) {
        self.log.clear();
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

    pub fn graph_entry_metadata_hints_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        self.log.entry_metadata_hints_batch(hashes)
    }

    pub fn graph_entry_signature_public_key_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let out = Array::new();
        for hash in hashes {
            match self
                .blocks
                .get(&hash)
                .and_then(|bytes| entry_v0_signature_public_key_from_storage_bytes(&bytes).ok())
            {
                Some(public_key) => out.push(&Uint8Array::from(public_key.as_slice())),
                None => out.push(&JsValue::UNDEFINED),
            };
        }
        Ok(out)
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
}
