use js_sys::{Array, Uint8Array};
use peerbit_log_rust::{NativeEntryV0PlainBuilder, NativeLogBlockStore, NativeLogIndex};
use peerbit_shared_log_rust::NativeSharedLogState;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[wasm_bindgen]
pub struct NativePeerbitBackbone {
    resolution: String,
    log: NativeLogIndex,
    blocks: NativeLogBlockStore,
    shared_log: NativeSharedLogState,
    builder: NativeEntryV0PlainBuilder,
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

    pub fn clear(&mut self) {
        self.log.clear();
        self.blocks.clear();
        self.shared_log.clear();
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
        )
    }
}

impl NativePeerbitBackbone {
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
    ) -> Result<Array, JsValue> {
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
        let next_hashes = Array::new();
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

        let out = Array::new();
        out.push(&entry_row);
        out.push(&coordinate_row.get(0));
        out.push(&coordinate_row.get(1));
        out.push(&coordinate_row.get(2));
        out.push(&coordinate_row.get(3));
        out.push(&trim_rows);
        Ok(out)
    }
}

fn array_from_value(value: JsValue, label: &str) -> Result<Array, JsValue> {
    value
        .dyn_into::<Array>()
        .map_err(|_| JsValue::from_str(&format!("Expected {label} array")))
}

fn string_field(row: &Array, index: u32, label: &str) -> Result<String, JsValue> {
    row.get(index)
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("Expected {label} string")))
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

fn hash_number_string(resolution: &str, digest: &[u8]) -> Result<String, JsValue> {
    match resolution {
        "u32" => {
            if digest.len() < 4 {
                return Err(JsValue::from_str("hash digest must have at least 4 bytes"));
            }
            Ok(u32::from_le_bytes(digest[0..4].try_into().unwrap()).to_string())
        }
        "u64" => {
            if digest.len() < 8 {
                return Err(JsValue::from_str("hash digest must have at least 8 bytes"));
            }
            Ok(u64::from_le_bytes(digest[0..8].try_into().unwrap()).to_string())
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
