use indexmap::IndexMap;
use js_sys::{Array, Uint8Array};
use peerbit_indexer_core::codec::{
    decode_query as decode_core_query, decode_sort as decode_core_sort,
};
use peerbit_indexer_core::planner::{
    DocumentFields, FieldPath, FieldValue, NativeQueryIndex, Query, SortField, SumResult,
};
use peerbit_indexer_core::schema::{
    decode_document_fields as decode_core_document_fields,
    decode_native_schema_ir as decode_core_native_schema_ir,
    extract_encoded_document_fields as extract_core_encoded_document_fields,
    extract_encoded_document_fields_from_parts as extract_core_encoded_document_fields_from_parts,
    NativeSchemaIr as CoreNativeSchemaIr,
};
use wasm_bindgen::prelude::*;

pub use peerbit_indexer_core::{planner, storage};

#[cfg(not(target_arch = "wasm32"))]
pub mod native_fs;

struct StoredEntry {
    id: JsValue,
    value: JsValue,
}

#[wasm_bindgen]
pub struct NativeIndexStore {
    entries: IndexMap<String, StoredEntry>,
}

#[wasm_bindgen]
impl NativeIndexStore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> NativeIndexStore {
        NativeIndexStore {
            entries: IndexMap::new(),
        }
    }

    pub fn put(&mut self, key: String, id: JsValue, value: JsValue) {
        self.entries.insert(key, StoredEntry { id, value });
    }

    pub fn get(&self, key: &str) -> JsValue {
        match self.entries.get(key) {
            Some(entry) => entry_to_js(entry),
            None => JsValue::UNDEFINED,
        }
    }

    pub fn get_many(&self, keys: Array) -> Array {
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        self.entries_for_keys(&keys)
    }

    pub fn delete(&mut self, key: &str) -> bool {
        self.entries.shift_remove(key).is_some()
    }

    pub fn delete_many(&mut self, keys: Array) -> Array {
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        self.delete_keys(&keys)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> Array {
        let entries = Array::new();
        for entry in self.entries.values() {
            entries.push(&entry_to_js(entry));
        }
        entries
    }
}

impl NativeIndexStore {
    fn entries_for_keys(&self, keys: &[String]) -> Array {
        let entries = Array::new();
        for key in keys {
            if let Some(entry) = self.entries.get(key) {
                entries.push(&entry_to_js(entry));
            }
        }
        entries
    }

    fn delete_keys(&mut self, keys: &[String]) -> Array {
        let entries = Array::new();
        for key in keys {
            if let Some(entry) = self.entries.shift_remove(key) {
                entries.push(&entry_to_js(&entry));
            }
        }
        entries
    }

    fn delete_keys_void(&mut self, keys: &[String]) {
        for key in keys {
            self.entries.shift_remove(key);
        }
    }

    fn delete_keys_count(&mut self, keys: &[String]) -> usize {
        let mut deleted = 0;
        for key in keys {
            if self.entries.shift_remove(key).is_some() {
                deleted += 1;
            }
        }
        deleted
    }
}

fn entry_to_js(entry: &StoredEntry) -> JsValue {
    let pair = Array::new();
    pair.push(&entry.id);
    pair.push(&entry.value);
    pair.into()
}

fn encoded_parts_to_js_value(prefix: JsValue, suffix: JsValue) -> JsValue {
    let pair = Array::new();
    pair.push(&prefix);
    pair.push(&suffix);
    pair.into()
}

#[wasm_bindgen]
pub struct NativeQueryPlanner {
    index: NativeQueryIndex,
}

#[wasm_bindgen]
impl NativeQueryPlanner {
    #[wasm_bindgen(constructor)]
    pub fn new() -> NativeQueryPlanner {
        NativeQueryPlanner {
            index: NativeQueryIndex::new(),
        }
    }

    pub fn clear(&mut self) {
        self.index.clear();
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn put_document(&mut self, id: String, fields_bytes: Vec<u8>) -> Result<(), JsValue> {
        let fields = decode_document_fields(&fields_bytes)?;
        self.index.put(id, fields);
        Ok(())
    }

    pub fn delete_document(&mut self, id: String) {
        self.index.delete(id);
    }

    pub fn query(&self, query_bytes: Vec<u8>, sort_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sort = decode_sort(&sort_bytes)?;
        let ids = self.index.search(&query, &sort, None);
        Ok(ids_to_js(ids))
    }

    pub fn query_page(
        &self,
        query_bytes: Vec<u8>,
        sort_bytes: Vec<u8>,
        offset: usize,
        limit: usize,
    ) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sort = decode_sort(&sort_bytes)?;
        let ids = self.index.search_page(&query, &sort, offset, Some(limit));
        Ok(ids_to_js(ids))
    }

    pub fn count(&self, query_bytes: Vec<u8>) -> Result<usize, JsValue> {
        let query = decode_query(&query_bytes)?;
        Ok(self.index.count(&query) as usize)
    }

    pub fn sum(&self, query_bytes: Vec<u8>, field: u32) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sum = self
            .index
            .sum(&query, FieldPath::Id(field))
            .map_err(js_error)?;
        Ok(sum_to_js(sum))
    }

    pub fn delete_matching(&mut self, query_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let ids = self.index.delete_matching(&query);
        Ok(ids_to_js(ids))
    }
}

#[wasm_bindgen]
pub struct NativeRustIndex {
    store: NativeIndexStore,
    planner: NativeQueryPlanner,
    schema_ir: Option<CoreNativeSchemaIr>,
}

#[wasm_bindgen]
impl NativeRustIndex {
    #[wasm_bindgen(constructor)]
    pub fn new() -> NativeRustIndex {
        NativeRustIndex {
            store: NativeIndexStore::new(),
            planner: NativeQueryPlanner::new(),
            schema_ir: None,
        }
    }

    pub fn clear(&mut self) {
        self.store.clear();
        self.planner.clear();
    }

    pub fn configure_schema_ir(&mut self, schema_ir_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let schema_ir = decode_core_native_schema_ir(&schema_ir_bytes).map_err(js_error)?;
        let stats = schema_ir.stats();
        self.schema_ir = Some(schema_ir);
        let out = Array::new();
        out.push(&JsValue::from_f64(stats.root_fields as f64));
        out.push(&JsValue::from_f64(stats.node_count as f64));
        out.push(&JsValue::from_f64(stats.generic_nodes as f64));
        Ok(out)
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn put(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        fields_bytes: Vec<u8>,
    ) -> Result<(), JsValue> {
        let fields = decode_document_fields(&fields_bytes)?;
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        Ok(())
    }

    pub fn put_encoded(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        value_bytes: Vec<u8>,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let fields =
            self.extract_encoded_document_fields(&value_bytes, byte_element_index_limit)?;
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        Ok(())
    }

    pub fn put_encoded_parts(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        value_prefix_bytes: Vec<u8>,
        value_suffix_bytes: Vec<u8>,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let fields = self.extract_encoded_document_fields_from_parts(
            &value_prefix_bytes,
            &value_suffix_bytes,
            byte_element_index_limit,
        )?;
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        Ok(())
    }

    pub fn validate_encoded_parts(
        &self,
        value_prefix_bytes: Vec<u8>,
        value_suffix_bytes: Vec<u8>,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        self.extract_encoded_document_fields_from_parts(
            &value_prefix_bytes,
            &value_suffix_bytes,
            byte_element_index_limit,
        )?;
        Ok(())
    }

    pub fn put_encoded_parts_stored(
        &mut self,
        key: String,
        id: JsValue,
        value_prefix_bytes: JsValue,
        value_suffix_bytes: JsValue,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let prefix = Uint8Array::new(&value_prefix_bytes).to_vec();
        let suffix = Uint8Array::new(&value_suffix_bytes).to_vec();
        let fields = self.extract_encoded_document_fields_from_parts(
            &prefix,
            &suffix,
            byte_element_index_limit,
        )?;
        let stored_value = encoded_parts_to_js_value(value_prefix_bytes, value_suffix_bytes);
        self.store.put(key.clone(), id, stored_value);
        self.planner.index.put(key, fields);
        Ok(())
    }

    pub fn put_encoded_parts_batch(
        &mut self,
        keys: Array,
        ids: Array,
        values: Array,
        value_prefix_bytes: Array,
        value_suffix_bytes: Array,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let len = keys.length();
        if ids.length() != len
            || values.length() != len
            || value_prefix_bytes.length() != len
            || value_suffix_bytes.length() != len
        {
            return Err(js_error("Mismatched encoded parts batch lengths"));
        }

        let mut prepared = Vec::with_capacity(len as usize);
        for index in 0..len {
            let key = keys
                .get(index)
                .as_string()
                .ok_or_else(|| js_error("Invalid encoded parts batch key"))?;
            let prefix = Uint8Array::new(&value_prefix_bytes.get(index)).to_vec();
            let suffix = Uint8Array::new(&value_suffix_bytes.get(index)).to_vec();
            let fields = self.extract_encoded_document_fields_from_parts(
                &prefix,
                &suffix,
                byte_element_index_limit,
            )?;
            prepared.push((key, ids.get(index), values.get(index), fields));
        }

        for (key, id, value, fields) in prepared {
            self.store.put(key.clone(), id, value);
            self.planner.index.put(key, fields);
        }
        Ok(())
    }

    pub fn validate_encoded_parts_batch(
        &self,
        value_prefix_bytes: Array,
        value_suffix_bytes: Array,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let len = value_prefix_bytes.length();
        if value_suffix_bytes.length() != len {
            return Err(js_error("Mismatched encoded parts batch lengths"));
        }
        for index in 0..len {
            let prefix = Uint8Array::new(&value_prefix_bytes.get(index)).to_vec();
            let suffix = Uint8Array::new(&value_suffix_bytes.get(index)).to_vec();
            self.extract_encoded_document_fields_from_parts(
                &prefix,
                &suffix,
                byte_element_index_limit,
            )?;
        }
        Ok(())
    }

    pub fn put_encoded_parts_stored_batch(
        &mut self,
        keys: Array,
        ids: Array,
        value_prefix_bytes: Array,
        value_suffix_bytes: Array,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let len = keys.length();
        if ids.length() != len
            || value_prefix_bytes.length() != len
            || value_suffix_bytes.length() != len
        {
            return Err(js_error("Mismatched encoded parts stored batch lengths"));
        }

        let mut prepared = Vec::with_capacity(len as usize);
        for index in 0..len {
            let key = keys
                .get(index)
                .as_string()
                .ok_or_else(|| js_error("Invalid encoded parts stored batch key"))?;
            let prefix_value = value_prefix_bytes.get(index);
            let suffix_value = value_suffix_bytes.get(index);
            let prefix = Uint8Array::new(&prefix_value).to_vec();
            let suffix = Uint8Array::new(&suffix_value).to_vec();
            let fields = self.extract_encoded_document_fields_from_parts(
                &prefix,
                &suffix,
                byte_element_index_limit,
            )?;
            prepared.push((key, ids.get(index), prefix_value, suffix_value, fields));
        }

        for (key, id, prefix_value, suffix_value, fields) in prepared {
            self.store.put(
                key.clone(),
                id,
                encoded_parts_to_js_value(prefix_value, suffix_value),
            );
            self.planner.index.put(key, fields);
        }
        Ok(())
    }

    pub fn put_and_delete_matching(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        fields_bytes: Vec<u8>,
        query_bytes: Vec<u8>,
    ) -> Result<Array, JsValue> {
        let fields = decode_document_fields(&fields_bytes)?;
        let query = decode_query(&query_bytes)?;
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        let keys = self.planner.index.delete_matching(&query);
        Ok(self.store.delete_keys(&keys))
    }

    pub fn put_and_delete_keys(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        fields_bytes: Vec<u8>,
        keys: Array,
    ) -> Result<Array, JsValue> {
        let fields = decode_document_fields(&fields_bytes)?;
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        for key in &keys {
            self.planner.index.delete(key);
        }
        Ok(self.store.delete_keys(&keys))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_shared_log_coordinate(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        hash_field: u32,
        hash_number_field: u32,
        gid_field: u32,
        coordinates_field: u32,
        coordinates_array_field: u32,
        wall_time_field: u32,
        assigned_to_range_boundary_field: u32,
        meta_field: u32,
        hash: String,
        hash_number: String,
        gid: String,
        coordinates: Array,
        wall_time: String,
        assigned_to_range_boundary: bool,
        meta_bytes: Vec<u8>,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let fields = shared_log_coordinate_fields(SharedLogCoordinateFieldsInput {
            hash_field,
            hash_number_field,
            gid_field,
            coordinates_field,
            coordinates_array_field,
            wall_time_field,
            assigned_to_range_boundary_field,
            meta_field,
            hash,
            hash_number,
            gid,
            coordinates,
            wall_time,
            assigned_to_range_boundary,
            meta_bytes,
            byte_element_index_limit,
        })?;
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_shared_log_coordinate_and_delete_keys(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        hash_field: u32,
        hash_number_field: u32,
        gid_field: u32,
        coordinates_field: u32,
        coordinates_array_field: u32,
        wall_time_field: u32,
        assigned_to_range_boundary_field: u32,
        meta_field: u32,
        hash: String,
        hash_number: String,
        gid: String,
        coordinates: Array,
        wall_time: String,
        assigned_to_range_boundary: bool,
        meta_bytes: Vec<u8>,
        byte_element_index_limit: usize,
        keys: Array,
    ) -> Result<Array, JsValue> {
        let fields = shared_log_coordinate_fields(SharedLogCoordinateFieldsInput {
            hash_field,
            hash_number_field,
            gid_field,
            coordinates_field,
            coordinates_array_field,
            wall_time_field,
            assigned_to_range_boundary_field,
            meta_field,
            hash,
            hash_number,
            gid,
            coordinates,
            wall_time,
            assigned_to_range_boundary,
            meta_bytes,
            byte_element_index_limit,
        })?;
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        for key in &keys {
            self.planner.index.delete(key);
        }
        Ok(self.store.delete_keys(&keys))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_shared_log_coordinate_and_delete_keys_void(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        hash_field: u32,
        hash_number_field: u32,
        gid_field: u32,
        coordinates_field: u32,
        coordinates_array_field: u32,
        wall_time_field: u32,
        assigned_to_range_boundary_field: u32,
        meta_field: u32,
        hash: String,
        hash_number: String,
        gid: String,
        coordinates: Array,
        wall_time: String,
        assigned_to_range_boundary: bool,
        meta_bytes: Vec<u8>,
        byte_element_index_limit: usize,
        keys: Array,
    ) -> Result<(), JsValue> {
        let fields = shared_log_coordinate_fields(SharedLogCoordinateFieldsInput {
            hash_field,
            hash_number_field,
            gid_field,
            coordinates_field,
            coordinates_array_field,
            wall_time_field,
            assigned_to_range_boundary_field,
            meta_field,
            hash,
            hash_number,
            gid,
            coordinates,
            wall_time,
            assigned_to_range_boundary,
            meta_bytes,
            byte_element_index_limit,
        })?;
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        for key in &keys {
            self.planner.index.delete(key);
            self.store.delete(key);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_shared_log_coordinates_and_delete_keys_void(
        &mut self,
        keys: Array,
        ids: Array,
        values: Array,
        hash_field: u32,
        hash_number_field: u32,
        gid_field: u32,
        coordinates_field: u32,
        coordinates_array_field: u32,
        wall_time_field: u32,
        assigned_to_range_boundary_field: u32,
        meta_field: u32,
        hashes: Array,
        hash_numbers: Array,
        gids: Array,
        coordinates: Array,
        wall_times: Array,
        assigned_to_range_boundaries: Uint8Array,
        meta_bytes: Array,
        byte_element_index_limit: usize,
        delete_keys: Array,
    ) -> Result<(), JsValue> {
        let len = keys.length();
        if ids.length() != len
            || values.length() != len
            || hashes.length() != len
            || hash_numbers.length() != len
            || gids.length() != len
            || coordinates.length() != len
            || wall_times.length() != len
            || assigned_to_range_boundaries.length() != len
            || meta_bytes.length() != len
            || delete_keys.length() != len
        {
            return Err(js_error("Mismatched shared-log coordinate batch lengths"));
        }

        let mut prepared = Vec::with_capacity(len as usize);
        for index in 0..len {
            let key = required_array_string(&keys, index, "shared-log coordinate key")?;
            let fields = shared_log_coordinate_fields(SharedLogCoordinateFieldsInput {
                hash_field,
                hash_number_field,
                gid_field,
                coordinates_field,
                coordinates_array_field,
                wall_time_field,
                assigned_to_range_boundary_field,
                meta_field,
                hash: required_array_string(&hashes, index, "shared-log coordinate hash")?,
                hash_number: required_array_string(
                    &hash_numbers,
                    index,
                    "shared-log coordinate hashNumber",
                )?,
                gid: required_array_string(&gids, index, "shared-log coordinate gid")?,
                coordinates: required_nested_array(
                    &coordinates,
                    index,
                    "shared-log coordinate coordinates",
                )?,
                wall_time: required_array_string(
                    &wall_times,
                    index,
                    "shared-log coordinate wallTime",
                )?,
                assigned_to_range_boundary: assigned_to_range_boundaries.get_index(index) != 0,
                meta_bytes: Uint8Array::new(&meta_bytes.get(index)).to_vec(),
                byte_element_index_limit,
            })?;
            let keys_to_delete =
                required_nested_array(&delete_keys, index, "shared-log coordinate deleteKeys")?;
            let keys_to_delete: Vec<_> = keys_to_delete
                .iter()
                .filter_map(|key| key.as_string())
                .collect();
            prepared.push((
                key,
                ids.get(index),
                values.get(index),
                fields,
                keys_to_delete,
            ));
        }

        for (key, id, value, fields, keys_to_delete) in prepared {
            self.store.put(key.clone(), id, value);
            self.planner.index.put(key, fields);
            for key in keys_to_delete {
                self.planner.index.delete(&key);
                self.store.delete(&key);
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_shared_log_coordinate_encoded_and_delete_keys_void(
        &mut self,
        key: String,
        id: JsValue,
        value_bytes: Vec<u8>,
        hash_field: u32,
        hash_number_field: u32,
        gid_field: u32,
        coordinates_field: u32,
        coordinates_array_field: u32,
        wall_time_field: u32,
        assigned_to_range_boundary_field: u32,
        meta_field: u32,
        hash: String,
        hash_number: String,
        gid: String,
        coordinates: Array,
        wall_time: String,
        assigned_to_range_boundary: bool,
        meta_bytes: Vec<u8>,
        byte_element_index_limit: usize,
        keys: Array,
    ) -> Result<(), JsValue> {
        let fields = shared_log_coordinate_fields(SharedLogCoordinateFieldsInput {
            hash_field,
            hash_number_field,
            gid_field,
            coordinates_field,
            coordinates_array_field,
            wall_time_field,
            assigned_to_range_boundary_field,
            meta_field,
            hash,
            hash_number,
            gid,
            coordinates,
            wall_time,
            assigned_to_range_boundary,
            meta_bytes,
            byte_element_index_limit,
        })?;
        let value = Uint8Array::from(value_bytes.as_slice());
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        self.store.put(key.clone(), id, value.into());
        self.planner.index.put(key, fields);
        for key in &keys {
            self.planner.index.delete(key);
            self.store.delete(key);
        }
        Ok(())
    }

    pub fn delete_keys(&mut self, keys: Array) -> Array {
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        for key in &keys {
            self.planner.index.delete(key);
        }
        self.store.delete_keys(&keys)
    }

    pub fn delete_keys_void(&mut self, keys: Array) {
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        for key in &keys {
            self.planner.index.delete(key);
        }
        self.store.delete_keys_void(&keys);
    }

    pub fn delete_keys_count(&mut self, keys: Array) -> usize {
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        for key in &keys {
            self.planner.index.delete(key);
        }
        self.store.delete_keys_count(&keys)
    }

    pub fn get(&self, key: &str) -> JsValue {
        self.store.get(key)
    }

    pub fn entries(&self) -> Array {
        self.store.entries()
    }

    pub fn query(&self, query_bytes: Vec<u8>, sort_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sort = decode_sort(&sort_bytes)?;
        let keys = self.planner.index.search(&query, &sort, None);
        Ok(self.store.entries_for_keys(&keys))
    }

    pub fn query_page(
        &self,
        query_bytes: Vec<u8>,
        sort_bytes: Vec<u8>,
        offset: usize,
        limit: usize,
    ) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sort = decode_sort(&sort_bytes)?;
        let keys = self
            .planner
            .index
            .search_page(&query, &sort, offset, Some(limit));
        Ok(self.store.entries_for_keys(&keys))
    }

    pub fn query_exact_string_first_batch(&self, field: u32, values: Array) -> Array {
        let out = Array::new();
        let field = FieldPath::Id(field);
        for value in values.iter() {
            let entry = value
                .as_string()
                .and_then(|value| {
                    self.planner
                        .index
                        .exact_first(&field, &FieldValue::from(value))
                })
                .map(|key| self.store.get(&key))
                .unwrap_or(JsValue::UNDEFINED);
            out.push(&entry);
        }
        out
    }

    pub fn count(&self, query_bytes: Vec<u8>) -> Result<usize, JsValue> {
        self.planner.count(query_bytes)
    }

    pub fn sum(&self, query_bytes: Vec<u8>, field: u32) -> Result<Array, JsValue> {
        self.planner.sum(query_bytes, field)
    }

    pub fn delete_matching(&mut self, query_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let keys = self.planner.index.delete_matching(&query);
        Ok(self.store.delete_keys(&keys))
    }
}

impl NativeRustIndex {
    fn extract_encoded_document_fields(
        &self,
        value_bytes: &[u8],
        byte_element_index_limit: usize,
    ) -> Result<DocumentFields, JsValue> {
        let schema_ir = self
            .schema_ir
            .as_ref()
            .ok_or_else(|| js_error("Native schema IR has not been configured"))?;
        extract_core_encoded_document_fields(schema_ir, value_bytes, byte_element_index_limit)
            .map_err(js_error)
    }

    fn extract_encoded_document_fields_from_parts(
        &self,
        prefix: &[u8],
        suffix: &[u8],
        byte_element_index_limit: usize,
    ) -> Result<DocumentFields, JsValue> {
        let schema_ir = self
            .schema_ir
            .as_ref()
            .ok_or_else(|| js_error("Native schema IR has not been configured"))?;
        extract_core_encoded_document_fields_from_parts(
            schema_ir,
            prefix,
            suffix,
            byte_element_index_limit,
        )
        .map_err(js_error)
    }
}

fn ids_to_js(ids: Vec<String>) -> Array {
    let out = Array::new();
    for id in ids {
        out.push(&JsValue::from_str(&id));
    }
    out
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

struct NativeExtractState {
    next_scope: u32,
    byte_element_index_limit: usize,
}

impl NativeExtractState {
    fn next_scope(&mut self) -> Result<u32, JsValue> {
        let scope = self.next_scope;
        self.next_scope = self
            .next_scope
            .checked_add(1)
            .ok_or_else(|| js_error("Native schema extraction scope overflow"))?;
        Ok(scope)
    }
}

fn insert_scalar(fields: &mut DocumentFields, scope: u32, field: u32, value: FieldValue) {
    fields.insert_scoped_scalar(scope, FieldPath::Id(field), value);
}

fn insert_bytes_facts(
    fields: &mut DocumentFields,
    state: &mut NativeExtractState,
    scope: u32,
    field: u32,
    bytes: Vec<u8>,
) -> Result<(), JsValue> {
    let index_byte_elements = bytes.len() <= state.byte_element_index_limit;
    if index_byte_elements {
        for byte in bytes.iter().copied() {
            let byte_scope = state.next_scope()?;
            fields.insert_scoped_scalar(
                byte_scope,
                FieldPath::Id(field),
                FieldValue::U64(byte as u64),
            );
        }
    }
    fields.insert_scoped_scalar(scope, FieldPath::Id(field), FieldValue::from(bytes));
    Ok(())
}

struct SharedLogCoordinateFieldsInput {
    hash_field: u32,
    hash_number_field: u32,
    gid_field: u32,
    coordinates_field: u32,
    coordinates_array_field: u32,
    wall_time_field: u32,
    assigned_to_range_boundary_field: u32,
    meta_field: u32,
    hash: String,
    hash_number: String,
    gid: String,
    coordinates: Array,
    wall_time: String,
    assigned_to_range_boundary: bool,
    meta_bytes: Vec<u8>,
    byte_element_index_limit: usize,
}

fn shared_log_coordinate_fields(
    input: SharedLogCoordinateFieldsInput,
) -> Result<DocumentFields, JsValue> {
    let mut fields =
        DocumentFields::with_scalar_capacity(8 + input.coordinates.length() as usize * 2);
    let mut next_scope = 1u32;
    insert_scalar(
        &mut fields,
        0,
        input.hash_field,
        FieldValue::from(input.hash),
    );
    insert_scalar(
        &mut fields,
        0,
        input.hash_number_field,
        FieldValue::U64(parse_u64_string(&input.hash_number, "hashNumber")?),
    );
    insert_scalar(&mut fields, 0, input.gid_field, FieldValue::from(input.gid));
    for coordinate in input.coordinates.iter() {
        let coordinate = coordinate
            .as_string()
            .ok_or_else(|| js_error("Invalid shared-log coordinate"))?;
        let scope = next_scope;
        next_scope = next_scope
            .checked_add(1)
            .ok_or_else(|| js_error("Shared-log coordinate scope overflow"))?;
        insert_scalar(
            &mut fields,
            scope,
            input.coordinates_array_field,
            FieldValue::Bool(true),
        );
        insert_scalar(
            &mut fields,
            scope,
            input.coordinates_field,
            FieldValue::U64(parse_u64_string(&coordinate, "coordinate")?),
        );
    }
    insert_scalar(
        &mut fields,
        0,
        input.wall_time_field,
        FieldValue::U64(parse_u64_string(&input.wall_time, "wallTime")?),
    );
    insert_scalar(
        &mut fields,
        0,
        input.assigned_to_range_boundary_field,
        FieldValue::Bool(input.assigned_to_range_boundary),
    );
    let mut state = NativeExtractState {
        next_scope,
        byte_element_index_limit: input.byte_element_index_limit,
    };
    insert_bytes_facts(
        &mut fields,
        &mut state,
        0,
        input.meta_field,
        input.meta_bytes,
    )?;
    Ok(fields)
}

fn parse_u64_string(value: &str, field: &str) -> Result<u64, JsValue> {
    value
        .parse::<u64>()
        .map_err(|_| js_error(format!("Invalid shared-log {field}")))
}

fn required_array_string(array: &Array, index: u32, field: &str) -> Result<String, JsValue> {
    array
        .get(index)
        .as_string()
        .ok_or_else(|| js_error(format!("Invalid {field}")))
}

fn required_nested_array(array: &Array, index: u32, field: &str) -> Result<Array, JsValue> {
    let value = array.get(index);
    if !Array::is_array(&value) {
        return Err(js_error(format!("Invalid {field}")));
    }
    Ok(Array::from(&value))
}

fn decode_document_fields(fields_bytes: &[u8]) -> Result<DocumentFields, JsValue> {
    decode_core_document_fields(fields_bytes).map_err(js_error)
}

fn decode_query(query_bytes: &[u8]) -> Result<Query, JsValue> {
    decode_core_query(query_bytes).map_err(js_error)
}

fn decode_sort(sort_bytes: &[u8]) -> Result<Vec<SortField>, JsValue> {
    decode_core_sort(sort_bytes).map_err(js_error)
}

fn js_error(error: impl ToString) -> JsValue {
    JsValue::from_str(&error.to_string())
}
