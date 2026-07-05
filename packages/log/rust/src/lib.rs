mod append;
mod cid;
mod codec;
mod crypto;
mod error;
mod graph;
mod time;

pub use append::{
    prepare_raw_entry_v0_blocks, prepare_raw_entry_v0_blocks_with_expected_cids,
    prepare_raw_entry_v0_blocks_with_expected_cids_and_verify,
    prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled, NativeCommittedEntryFacts,
    NativeLogAppendProfile, PreparedRawEntryV0, RawEntryV0PrepareProfile,
};
pub use codec::entry_v0_signature_public_key_from_storage_bytes;
pub use crypto::{
    verify_entry_v0_ed25519_storage_slices, verify_entry_v0_ed25519_storage_slices_all,
    verify_prepared_entry_v0_ed25519_storage_slices,
    verify_prepared_entry_v0_ed25519_storage_slices_all, PreparedEntryV0SignatureInput,
};
pub use error::LogError;
pub use graph::{
    decode_absolute_replica_data_u32, JoinPlan, LogEntryMetadata, LogEntryPruneConfirmMetadata,
    LogEntryPruneMetadata, LogGraphIndex, LogIndexEntry,
};

use crate::append::{
    prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled,
    prepare_entry_v0_plain_entry_commit_digest_key_core_profiled,
    prepare_entry_v0_plain_entry_core_with_signer_parts, trim_oldest_log_entries_core,
    trim_oldest_log_entry_hashes_core, trim_oldest_log_index_entries_core, PreparedPlainEntryCore,
};
use crate::cid::{
    calculate_raw_cid_v1_from_bytes, calculate_raw_cid_v1_parts, raw_cid_v1_string_from_digest,
};
use crate::codec::{
    encode_entry_v0, encode_entry_v0_parts_unsigned_for_signing,
    encode_entry_v0_payload_data_unsigned_for_signing, encode_meta, encode_meta_parts,
    encode_payload, encode_signature_with_key_parts, parse_plain_entry_v0_storage,
    parse_plain_entry_v0_storage_signature, parse_raw_entry_v0_payload,
    signable_entry_to_signed_storage, EntryV0EncodeInput, SignatureInput,
};
use crate::crypto::{
    cached_verifying_key, sign_ed25519_raw, sign_ed25519_with_key, validate_ed25519_keypair,
    validate_signature_lengths,
};

use ed25519_dalek::{verify_batch, Signature, SigningKey, Verifier, VerifyingKey};
use js_sys::{Array, BigUint64Array, Uint32Array, Uint8Array};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

enum PreparedPlainEntryRowMode {
    Full { include_storage_bytes: bool },
    StorageOnly,
    StorageWithFacts,
    CommitFactsOnly,
    CommitFactsNoNext,
}

#[wasm_bindgen]
pub struct NativeLogIndex {
    inner: LogGraphIndex,
}

#[wasm_bindgen]
pub struct NativeLogBlockStore {
    entries: HashMap<String, Vec<u8>>,
    total_size: u64,
}

#[wasm_bindgen]
pub struct NativeEntryV0PlainBuilder {
    clock_id: Vec<u8>,
    public_key: Vec<u8>,
    signing_key: SigningKey,
}

#[wasm_bindgen]
impl NativeLogBlockStore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            total_size: 0,
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.entries.get(key).cloned()
    }

    pub fn get_many(&self, keys: Array) -> Result<Array, JsValue> {
        let values = Array::new();
        for key in strings_from_array(keys)? {
            match self.entries.get(&key) {
                Some(value) => values.push(&Uint8Array::from(value.as_slice())),
                None => values.push(&JsValue::UNDEFINED),
            };
        }
        Ok(values)
    }

    pub fn has(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    pub fn has_many(&self, keys: Array) -> Result<Array, JsValue> {
        let present = Array::new();
        for key in strings_from_array(keys)? {
            present.push(&JsValue::from_bool(self.entries.contains_key(&key)));
        }
        Ok(present)
    }

    pub fn put(&mut self, key: String, value: Vec<u8>) {
        self.put_entry(key, value);
    }

    pub fn put_many(&mut self, keys: Array, values: Array) -> Result<(), JsValue> {
        self.put_entries(block_key_values_from_arrays(&keys, &values)?);
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> bool {
        if let Some(previous) = self.entries.remove(key) {
            self.total_size = self.total_size.saturating_sub(previous.len() as u64);
            true
        } else {
            false
        }
    }

    pub fn delete_many(&mut self, keys: Array) -> Result<usize, JsValue> {
        let mut deleted = 0;
        for key in strings_from_array(keys)? {
            if self.delete(&key) {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.total_size = 0;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn size(&self) -> f64 {
        self.total_size as f64
    }

    pub fn entries(&self) -> Array {
        let entries = Array::new();
        for (key, value) in &self.entries {
            let pair = Array::new();
            pair.push(&JsValue::from_str(key));
            pair.push(&Uint8Array::from(value.as_slice()));
            entries.push(&pair);
        }
        entries
    }
}

/// Serialize a `/peerbit/direct-block` `BlockResponse` payload for a block
/// held in the native store. The stored bytes are copied straight into the
/// borsh payload (codec owned by `peerbit_wire::block_exchange`), so serving
/// a natively stored block never materializes the block bytes as a JS value.
#[wasm_bindgen]
pub fn block_response_payload(store: &NativeLogBlockStore, cid: &str) -> Option<Vec<u8>> {
    store
        .get_ref(cid)
        .map(|bytes| peerbit_wire::block_exchange::encode_block_response(cid, bytes))
}

fn trim_oldest_log_entries(
    index: &mut LogGraphIndex,
    block_store: &mut NativeLogBlockStore,
    trim_length_to: usize,
) -> Array {
    log_trim_entries_to_rows(trim_oldest_log_entries_core(
        index,
        block_store,
        trim_length_to,
    ))
}

fn trim_oldest_log_index_entries(index: &mut LogGraphIndex, trim_length_to: usize) -> Array {
    log_trim_entries_to_rows(trim_oldest_log_index_entries_core(index, trim_length_to))
}

#[wasm_bindgen]
impl NativeEntryV0PlainBuilder {
    #[wasm_bindgen(constructor)]
    pub fn new(
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
    ) -> Result<Self, JsValue> {
        let private_key = private_key.to_vec();
        let public_key = public_key.to_vec();
        let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
        Ok(Self {
            clock_id: clock_id.to_vec(),
            public_key,
            signing_key,
        })
    }
}

#[wasm_bindgen]
impl NativeLogIndex {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: LogGraphIndex::new(),
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn payload_size_sum(&self) -> f64 {
        self.inner.payload_size_sum() as f64
    }

    pub fn has(&self, hash: &str) -> bool {
        self.inner.has(hash)
    }

    pub fn oldest_hash(&self) -> JsValue {
        self.inner
            .oldest_hash()
            .map(|hash| JsValue::from_str(&hash))
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn newest_hash(&self) -> JsValue {
        self.inner
            .newest_hash()
            .map(|hash| JsValue::from_str(&hash))
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn oldest_entries(&self, limit: usize) -> Array {
        log_trim_entries_to_rows(self.inner.oldest_entries(limit))
    }

    pub fn has_many(&self, hashes: Array) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        Ok(strings_to_array(self.inner.has_many(&hashes)))
    }

    pub fn put(
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
        let next = strings_from_array(next)?;
        self.inner.put(LogIndexEntry::new_with_data(
            hash,
            gid,
            next,
            entry_type,
            wall_time,
            logical,
            payload_size,
            head,
            optional_bytes_from_js(data),
        ));
        Ok(())
    }

    pub fn put_many(
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
        let len = hashes.length();
        for values in [&gids, &nexts, &datas] {
            if values.length() != len {
                return Err(JsValue::from_str("Expected equal column lengths"));
            }
        }
        for numeric_len in [
            entry_types.length(),
            wall_times.length(),
            logicals.length(),
            payload_sizes.length(),
            heads.length(),
        ] {
            if numeric_len != len {
                return Err(JsValue::from_str("Expected equal column lengths"));
            }
        }

        let mut entries = Vec::with_capacity(len as usize);
        for i in 0..len {
            entries.push(LogIndexEntry::new_with_data(
                required_string_from_array(&hashes, i)?,
                required_string_from_array(&gids, i)?,
                strings_from_array(required_array_from_array(&nexts, i)?)?,
                entry_types.get_index(i),
                wall_times.get_index(i),
                logicals.get_index(i),
                payload_sizes.get_index(i),
                heads.get_index(i) != 0,
                optional_bytes_from_js(datas.get(i)),
            ));
        }
        self.inner.put_many(entries);
        Ok(())
    }

    pub fn put_append_chain(
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
        let len = hashes.length();
        if datas.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
        for numeric_len in [
            wall_times.length(),
            logicals.length(),
            payload_sizes.length(),
        ] {
            if numeric_len != len {
                return Err(JsValue::from_str("Expected equal column lengths"));
            }
        }

        let initial_nexts = strings_from_array(initial_next)?;
        let mut next = initial_nexts.clone();
        let mut entries = Vec::with_capacity(len as usize);
        for i in 0..len {
            let hash = required_string_from_array(&hashes, i)?;
            entries.push(LogIndexEntry::new_with_data(
                hash.clone(),
                gid.clone(),
                next.clone(),
                entry_type,
                wall_times.get_index(i),
                logicals.get_index(i),
                payload_sizes.get_index(i),
                i + 1 == len,
                optional_bytes_from_js(datas.get(i)),
            ));
            next = vec![hash];
        }
        self.inner.put_append_chain(entries, &initial_nexts);
        Ok(())
    }

    pub fn prepare_entry_v0_plain_chain_and_put(
        &mut self,
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gid: String,
        initial_next: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
    ) -> Result<Array, JsValue> {
        let (rows, entries, initial_nexts, _blocks) = prepare_entry_v0_plain_chain_rows(
            clock_id,
            private_key,
            public_key,
            wall_times,
            logicals,
            gid,
            initial_next,
            entry_type,
            meta_datas,
            payload_datas,
            true,
        )?;
        self.inner.put_append_chain(entries, &initial_nexts);
        Ok(rows)
    }

    pub fn prepare_entry_v0_plain_entry_and_put(
        &mut self,
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) = prepare_entry_v0_plain_entry_row(
            clock_id,
            private_key,
            public_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            true,
        )?;
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) = prepare_entry_v0_plain_entry_row_with_signer(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            true,
        )?;
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_storage_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) =
            prepare_entry_v0_plain_entry_storage_row_with_signer(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
            )?;
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_storage_facts_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::StorageWithFacts,
            )?;
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_storage_trim_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) =
            prepare_entry_v0_plain_entry_storage_row_with_signer(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
            )?;
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&trim_oldest_log_index_entries(
            &mut self.inner,
            trim_length_to,
        ));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_storage_facts_trim_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::StorageWithFacts,
            )?;
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&trim_oldest_log_index_entries(
            &mut self.inner,
            trim_length_to,
        ));
        Ok(out)
    }

    pub fn prepare_entry_v0_plain_chain_commit_blocks_and_put(
        &mut self,
        block_store: &mut NativeLogBlockStore,
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gid: String,
        initial_next: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
    ) -> Result<Array, JsValue> {
        let (rows, entries, initial_nexts, blocks) = prepare_entry_v0_plain_chain_rows(
            clock_id,
            private_key,
            public_key,
            wall_times,
            logicals,
            gid,
            initial_next,
            entry_type,
            meta_datas,
            payload_datas,
            false,
        )?;
        block_store.put_entries(blocks);
        self.inner.put_append_chain(entries, &initial_nexts);
        Ok(rows)
    }

    pub fn prepare_entry_v0_plain_entry_commit_block_and_put(
        &mut self,
        block_store: &mut NativeLogBlockStore,
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) = prepare_entry_v0_plain_entry_row(
            clock_id,
            private_key,
            public_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            false,
        )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_commit_block_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) = prepare_entry_v0_plain_entry_row_with_signer(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            false,
        )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_storage_commit_block_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_storage_row_with_signer(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_storage_commit_block_trim_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_storage_row_with_signer(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&trim_oldest_log_entries(
            &mut self.inner,
            block_store,
            trim_length_to,
        ));
        Ok(out)
    }

    pub fn prepare_entry_v0_plain_entry_commit_facts_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::CommitFactsOnly,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                Vec::new(),
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::CommitFactsNoNext,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_trim_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::CommitFactsOnly,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&trim_oldest_log_entries(
            &mut self.inner,
            block_store,
            trim_length_to,
        ));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_trim_hashes_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::CommitFactsOnly,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&strings_to_array(trim_oldest_log_entry_hashes_core(
            &mut self.inner,
            block_store,
            trim_length_to,
        )));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_trim_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (facts, trimmed_entries) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                trim_length_to,
                None,
            )?;
        let out = Array::new();
        out.push(&committed_entry_facts_to_row(&facts, false));
        out.push(&log_trim_entries_to_rows(trimmed_entries));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_trim_hashes_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (facts, trim_hashes) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_hashes(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                trim_length_to,
                None,
            )?;
        let out = Array::new();
        out.push(&committed_entry_facts_to_row(&facts, false));
        out.push(&strings_to_array(trim_hashes));
        Ok(out)
    }

    pub fn prepare_entry_v0_plain_entries_commit_blocks_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gids: Array,
        nexts: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
    ) -> Result<Array, JsValue> {
        let (rows, entries, blocks) = prepare_entry_v0_plain_entries_rows_with_signer(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_times,
            logicals,
            gids,
            nexts,
            entry_type,
            meta_datas,
            payload_datas,
            false,
        )?;
        block_store.put_entries(blocks);
        self.inner.put_many(entries);
        Ok(rows)
    }

    pub fn prepare_entry_v0_plain_entries_no_next_commit_blocks_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
    ) -> Result<Array, JsValue> {
        let (rows, entries, blocks) = prepare_entry_v0_plain_entries_rows_with_signer_inner(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_times,
            logicals,
            gids,
            None,
            entry_type,
            meta_datas,
            payload_datas,
            false,
        )?;
        block_store.put_entries(blocks);
        self.inner.put_many(entries);
        Ok(rows)
    }

    pub fn delete(&mut self, hash: &str) -> bool {
        self.inner.delete(hash).is_some()
    }

    pub fn delete_many(&mut self, hashes: Array) -> Result<usize, JsValue> {
        let hashes = strings_from_array(hashes)?;
        Ok(self.inner.delete_many(&hashes))
    }

    pub fn heads(&self, gid: Option<String>) -> Array {
        strings_to_array(self.inner.heads(gid.as_deref()))
    }

    pub fn has_head(&self, gid: Option<String>) -> bool {
        self.inner.has_head(gid.as_deref())
    }

    pub fn has_any_head(&self, gids: Array) -> Result<bool, JsValue> {
        let gids = strings_from_array(gids)?;
        Ok(self.inner.has_any_head(&gids))
    }

    pub fn has_any_head_batch(&self, gid_sets: Array) -> Result<Array, JsValue> {
        let gid_sets = string_arrays_from_array(gid_sets)?;
        let out = Array::new();
        for value in self.inner.has_any_head_batch(&gid_sets) {
            out.push(&JsValue::from_bool(value));
        }
        Ok(out)
    }

    pub fn head_entries(&self, gid: Option<String>) -> Array {
        log_entries_to_rows(self.inner.head_entries(gid.as_deref()))
    }

    pub fn head_data_entries(&self, gid: Option<String>) -> Array {
        log_data_entries_to_rows(self.inner.head_data_entries(gid.as_deref()))
    }

    pub fn max_head_data_u32(&self, gid: Option<String>) -> JsValue {
        self.inner
            .max_head_data_u32(gid.as_deref())
            .map(|value| JsValue::from_f64(value as f64))
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn max_head_data_u32_batch(&self, gids: Array) -> Result<Array, JsValue> {
        let gids = strings_from_array(gids)?;
        let out = Array::new();
        for value in self.inner.max_head_data_u32_batch(&gids) {
            out.push(
                &value
                    .map(|value| JsValue::from_f64(value as f64))
                    .unwrap_or(JsValue::UNDEFINED),
            );
        }
        Ok(out)
    }

    pub fn head_join_entries(&self, gid: Option<String>) -> Array {
        log_join_entries_to_rows(self.inner.head_join_entries(gid.as_deref()))
    }

    pub fn child_join_entries(&self, hash: &str) -> Array {
        log_join_entries_to_rows(self.inner.child_join_entries(hash))
    }

    pub fn entry_metadata_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        Ok(log_optional_entry_metadata_to_rows(
            self.inner.entry_metadata_batch(&hashes),
        ))
    }

    pub fn entry_metadata_hints_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        Ok(log_optional_entry_metadata_hints_to_rows(
            self.inner.entry_metadata_batch(&hashes),
        ))
    }

    pub fn unique_reference_gids(&self, hash: &str) -> JsValue {
        self.inner
            .unique_reference_gids(hash)
            .map(|gids| strings_to_array(gids).into())
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn unique_reference_gid_rows_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let out = Array::new();
        for rows in self.inner.unique_reference_gid_rows_batch(&hashes) {
            out.push(
                &rows
                    .map(|rows| reference_gid_rows_to_array(rows).into())
                    .unwrap_or(JsValue::UNDEFINED),
            );
        }
        Ok(out)
    }

    pub fn unique_reference_gid_rows_flat_batch(&self, hashes: Array) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let Some(rows) = self.inner.unique_reference_gid_rows_flat_batch(&hashes) else {
            return Ok(JsValue::UNDEFINED);
        };
        Ok(reference_gid_flat_rows_to_array(rows).into())
    }

    pub fn plan_delete_recursively(&self, from: Array, skip_first: bool) -> Result<Array, JsValue> {
        let from = strings_from_array(from)?;
        Ok(strings_to_array(
            self.inner.plan_delete_recursively(&from, skip_first),
        ))
    }

    pub fn children(&self, hash: &str) -> Array {
        strings_to_array(self.inner.children(hash))
    }

    pub fn count_has_next(&self, next: &str, exclude_hash: Option<String>) -> usize {
        self.inner.count_has_next(next, exclude_hash.as_deref())
    }

    pub fn shadowed_gids(
        &self,
        gid: &str,
        next: Array,
        exclude_hash: Option<String>,
    ) -> Result<Array, JsValue> {
        let next = strings_from_array(next)?;
        Ok(strings_to_array(self.inner.shadowed_gids(
            gid,
            &next,
            exclude_hash.as_deref(),
        )))
    }

    pub fn plan_join(
        &self,
        hash: &str,
        next: Array,
        entry_type: u8,
        reset: bool,
        gid: Option<String>,
        wall_time: Option<u64>,
        logical: Option<u32>,
    ) -> Result<Array, JsValue> {
        let next = strings_from_array(next)?;
        Ok(join_plan_to_row(self.inner.plan_join(
            hash,
            &next,
            entry_type,
            reset,
            gid.as_deref(),
            wall_time,
            logical,
        )))
    }

    pub fn plan_join_batch(
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
        let len = hashes.length();
        if nexts.length() != len || entry_types.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
        if cut_check
            && (gids.length() != len || wall_times.length() != len || logicals.length() != len)
        {
            return Err(JsValue::from_str("Expected equal cut-check column lengths"));
        }

        let mut parsed_hashes = Vec::with_capacity(len as usize);
        let mut parsed_nexts = Vec::with_capacity(len as usize);
        let mut parsed_entry_types = Vec::with_capacity(len as usize);
        let mut parsed_gids = if cut_check {
            Vec::with_capacity(len as usize)
        } else {
            Vec::new()
        };
        let mut parsed_wall_times = if cut_check {
            Vec::with_capacity(len as usize)
        } else {
            Vec::new()
        };
        let mut parsed_logicals = if cut_check {
            Vec::with_capacity(len as usize)
        } else {
            Vec::new()
        };
        for i in 0..len {
            parsed_hashes.push(required_string_from_array(&hashes, i)?);
            parsed_nexts.push(strings_from_array(required_array_from_array(&nexts, i)?)?);
            parsed_entry_types.push(entry_types.get_index(i));
            if cut_check {
                parsed_gids.push(required_string_from_array(&gids, i)?);
                parsed_wall_times.push(wall_times.get_index(i));
                parsed_logicals.push(logicals.get_index(i));
            }
        }
        let cut_checks = if cut_check {
            Some((
                parsed_gids.as_slice(),
                parsed_wall_times.as_slice(),
                parsed_logicals.as_slice(),
            ))
        } else {
            None
        };
        let out = Array::new();
        for plan in self.inner.plan_join_batch(
            &parsed_hashes,
            &parsed_nexts,
            &parsed_entry_types,
            reset,
            cut_checks,
        ) {
            out.push(&join_plan_to_row(plan));
        }
        Ok(out)
    }
}

impl Default for NativeLogIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
pub fn encode_entry_v0_signable(
    clock_id: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
) -> Result<Uint8Array, JsValue> {
    let next = strings_from_array(next)?;
    let bytes = encode_entry_v0(
        EntryV0EncodeInput {
            clock_id: clock_id.to_vec(),
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data: optional_bytes_from_js(meta_data),
            payload_data: payload_data.to_vec(),
        },
        None,
    );
    Ok(Uint8Array::from(bytes.as_slice()))
}

#[wasm_bindgen]
pub fn sign_ed25519(
    private_key: Uint8Array,
    public_key: Uint8Array,
    data: Uint8Array,
) -> Result<Uint8Array, JsValue> {
    let signature = sign_ed25519_raw(&private_key.to_vec(), &public_key.to_vec(), &data.to_vec())?;
    Ok(Uint8Array::from(signature.as_slice()))
}

#[wasm_bindgen]
pub fn verify_ed25519_batch(
    signatures: Array,
    public_keys: Array,
    messages: Array,
) -> Result<Uint8Array, JsValue> {
    let len = signatures.length();
    if public_keys.length() != len || messages.length() != len {
        return Err(JsValue::from_str(
            "Expected equal Ed25519 verification batch lengths",
        ));
    }

    let mut parsed_signatures = Vec::with_capacity(len as usize);
    let mut parsed_public_keys = Vec::with_capacity(len as usize);
    let mut parsed_messages = Vec::with_capacity(len as usize);
    let mut verifying_key_cache = HashMap::new();
    for i in 0..len {
        let signature = required_bytes_from_array(&signatures, i, "signature")?;
        let public_key = required_bytes_from_array(&public_keys, i, "public key")?;
        let message = required_bytes_from_array(&messages, i, "message")?;
        validate_signature_lengths(&signature, &public_key)?;

        let signature_bytes: [u8; 64] = signature
            .as_slice()
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 signature length 64"))?;
        let verifying_key = cached_verifying_key(&mut verifying_key_cache, public_key.as_slice())?;
        let signature = Signature::from_bytes(&signature_bytes);
        parsed_signatures.push(signature);
        parsed_public_keys.push(verifying_key);
        parsed_messages.push(message);
    }

    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(Uint8Array::from(vec![1u8; len as usize].as_slice()));
    }

    let mut out = Vec::with_capacity(len as usize);
    for i in 0..parsed_signatures.len() {
        out.push(
            if parsed_public_keys[i]
                .verify(&parsed_messages[i], &parsed_signatures[i])
                .is_ok()
            {
                1
            } else {
                0
            },
        );
    }

    Ok(Uint8Array::from(out.as_slice()))
}

#[wasm_bindgen]
pub fn verify_entry_v0_ed25519_storage_batch(blocks: Array) -> Result<Uint8Array, JsValue> {
    let mut storage = Vec::with_capacity(blocks.length() as usize);
    for i in 0..blocks.length() {
        storage.push(required_bytes_from_array(&blocks, i, "entry storage")?);
    }
    let storage_refs = storage
        .iter()
        .map(|bytes| bytes.as_slice())
        .collect::<Vec<_>>();
    let out = verify_entry_v0_ed25519_storage_slices(&storage_refs)?;
    Ok(Uint8Array::from(out.as_slice()))
}

#[wasm_bindgen]
pub fn verify_entry_v0_ed25519_batch(
    clock_ids: Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Array,
    entry_types: Uint8Array,
    meta_datas: Array,
    payload_datas: Array,
    signatures: Array,
    public_keys: Array,
) -> Result<Uint8Array, JsValue> {
    let len = clock_ids.length();
    validate_entry_batch_lengths(
        len,
        &gids,
        &nexts,
        &meta_datas,
        &payload_datas,
        &wall_times,
        &logicals,
        &entry_types,
    )?;
    for values in [&signatures, &public_keys] {
        if values.length() != len {
            return Err(JsValue::from_str(
                "Expected equal Ed25519 entry verification batch lengths",
            ));
        }
    }

    let mut parsed_signatures = Vec::with_capacity(len as usize);
    let mut parsed_public_keys = Vec::with_capacity(len as usize);
    let mut parsed_messages = Vec::with_capacity(len as usize);
    for i in 0..len {
        let input = entry_input_from_batch(
            i,
            &clock_ids,
            &wall_times,
            &logicals,
            &gids,
            &nexts,
            &entry_types,
            &meta_datas,
            &payload_datas,
        )?;
        let signature = required_bytes_from_array(&signatures, i, "signature")?;
        let public_key = required_bytes_from_array(&public_keys, i, "public key")?;
        validate_signature_lengths(&signature, &public_key)?;

        let signature_bytes: [u8; 64] = signature
            .as_slice()
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 signature length 64"))?;
        let public_key_bytes: [u8; 32] = public_key
            .as_slice()
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 public key length 32"))?;
        let verifying_key = VerifyingKey::from_bytes(&public_key_bytes)
            .map_err(|_| JsValue::from_str("Invalid Ed25519 public key"))?;
        parsed_signatures.push(Signature::from_bytes(&signature_bytes));
        parsed_public_keys.push(verifying_key);
        parsed_messages.push(encode_entry_v0(input, None));
    }

    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(Uint8Array::from(vec![1u8; len as usize].as_slice()));
    }

    let mut out = Vec::with_capacity(len as usize);
    for i in 0..parsed_signatures.len() {
        out.push(
            if parsed_public_keys[i]
                .verify(&parsed_messages[i], &parsed_signatures[i])
                .is_ok()
            {
                1
            } else {
                0
            },
        );
    }

    Ok(Uint8Array::from(out.as_slice()))
}

#[wasm_bindgen]
pub fn encode_entry_v0_storage(
    clock_id: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    signature: Uint8Array,
    signature_public_key: Uint8Array,
    prehash: u8,
) -> Result<Uint8Array, JsValue> {
    let bytes = encode_entry_v0_storage_vec(
        clock_id,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        signature,
        signature_public_key,
        prehash,
    )?;
    Ok(Uint8Array::from(bytes.as_slice()))
}

#[wasm_bindgen]
pub fn encode_entry_v0_storage_with_cid(
    clock_id: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    signature: Uint8Array,
    signature_public_key: Uint8Array,
    prehash: u8,
) -> Result<Array, JsValue> {
    let bytes = encode_entry_v0_storage_vec(
        clock_id,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        signature,
        signature_public_key,
        prehash,
    )?;
    Ok(storage_with_cid_to_row(bytes))
}

#[wasm_bindgen]
pub fn encode_entry_v0_signable_batch(
    clock_ids: Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Array,
    entry_types: Uint8Array,
    meta_datas: Array,
    payload_datas: Array,
) -> Result<Array, JsValue> {
    let len = clock_ids.length();
    validate_entry_batch_lengths(
        len,
        &gids,
        &nexts,
        &meta_datas,
        &payload_datas,
        &wall_times,
        &logicals,
        &entry_types,
    )?;

    let out = Array::new();
    for i in 0..len {
        let input = entry_input_from_batch(
            i,
            &clock_ids,
            &wall_times,
            &logicals,
            &gids,
            &nexts,
            &entry_types,
            &meta_datas,
            &payload_datas,
        )?;
        let bytes = encode_entry_v0(input, None);
        out.push(&Uint8Array::from(bytes.as_slice()));
    }
    Ok(out)
}

#[wasm_bindgen]
pub fn encode_entry_v0_storage_batch_with_cids(
    clock_ids: Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Array,
    entry_types: Uint8Array,
    meta_datas: Array,
    payload_datas: Array,
    signatures: Array,
    signature_public_keys: Array,
    prehashes: Uint8Array,
) -> Result<Array, JsValue> {
    let len = clock_ids.length();
    validate_entry_batch_lengths(
        len,
        &gids,
        &nexts,
        &meta_datas,
        &payload_datas,
        &wall_times,
        &logicals,
        &entry_types,
    )?;
    for values in [&signatures, &signature_public_keys] {
        if values.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }
    if prehashes.length() != len {
        return Err(JsValue::from_str("Expected equal column lengths"));
    }

    let out = Array::new();
    for i in 0..len {
        let input = entry_input_from_batch(
            i,
            &clock_ids,
            &wall_times,
            &logicals,
            &gids,
            &nexts,
            &entry_types,
            &meta_datas,
            &payload_datas,
        )?;
        let signature = required_bytes_from_array(&signatures, i, "signature")?;
        let public_key = required_bytes_from_array(&signature_public_keys, i, "public key")?;
        validate_signature_lengths(&signature, &public_key)?;
        let bytes = encode_entry_v0(
            input,
            Some(SignatureInput {
                signature,
                public_key,
                prehash: prehashes.get_index(i),
            }),
        );
        out.push(&storage_with_cid_to_row(bytes));
    }
    Ok(out)
}

fn prepare_entry_v0_plain_chain_rows(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gid: String,
    initial_next: Array,
    entry_type: u8,
    meta_datas: Array,
    payload_datas: Array,
    include_storage_bytes: bool,
) -> Result<
    (
        Array,
        Vec<LogIndexEntry>,
        Vec<String>,
        Vec<(String, Vec<u8>)>,
    ),
    JsValue,
> {
    let len = payload_datas.length();
    if meta_datas.length() != len || wall_times.length() != len || logicals.length() != len {
        return Err(JsValue::from_str("Expected equal column lengths"));
    }

    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;

    let initial_nexts = strings_from_array(initial_next)?;
    let mut next = initial_nexts.clone();
    let out = Array::new();
    let mut entries = Vec::with_capacity(len as usize);
    let mut blocks = Vec::with_capacity(len as usize);
    for i in 0..len {
        let payload_data = required_bytes_from_array(&payload_datas, i, "payload")?;
        let input = EntryV0EncodeInput {
            clock_id: clock_id.clone(),
            wall_time: wall_times.get_index(i),
            logical: logicals.get_index(i),
            gid: gid.clone(),
            next: next.clone(),
            entry_type,
            meta_data: optional_bytes_from_js(meta_datas.get(i)),
            payload_data,
        };
        let meta = encode_meta(&input);
        let payload = encode_payload(&input.payload_data);
        let signable = encode_entry_v0_parts_unsigned_for_signing(&meta, &payload);
        let signature = sign_ed25519_with_key(&signing_key, &signable);
        let signature_with_key = encode_signature_with_key_parts(&signature, &public_key, 0);
        let storage = signable_entry_to_signed_storage(signable, &signature_with_key);
        let storage_len = storage.len();
        let (cid, hash_digest) = calculate_raw_cid_v1_parts(&storage);

        let row = Array::new();
        if include_storage_bytes {
            row.push(&Uint8Array::from(storage.as_slice()));
            row.push(&JsValue::from_str(&cid));
            row.push(&Uint8Array::from(signature.as_slice()));
            row.push(&strings_to_array(next.clone()));
            row.push(&Uint8Array::from(meta.as_slice()));
            row.push(&Uint8Array::from(payload.as_slice()));
            row.push(&Uint8Array::from(signature_with_key.as_slice()));
            row.push(&Uint8Array::from(hash_digest.as_slice()));
        } else {
            row.push(&JsValue::from_str(&cid));
            row.push(&Uint8Array::from(signature.as_slice()));
            row.push(&strings_to_array(next.clone()));
            row.push(&Uint8Array::from(meta.as_slice()));
            row.push(&Uint8Array::from(payload.as_slice()));
            row.push(&Uint8Array::from(signature_with_key.as_slice()));
            row.push(&JsValue::from_f64(storage_len as f64));
            row.push(&Uint8Array::from(hash_digest.as_slice()));
        }
        out.push(&row);
        entries.push(LogIndexEntry::new_with_data(
            cid.clone(),
            gid.clone(),
            next.clone(),
            entry_type,
            input.wall_time,
            input.logical,
            input.payload_data.len() as u32,
            i + 1 == len,
            input.meta_data.clone(),
        ));
        blocks.push((cid.clone(), storage));

        next = vec![cid];
    }
    Ok((out, entries, initial_nexts, blocks))
}

fn prepare_entry_v0_plain_entry_row(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    include_storage_bytes: bool,
) -> Result<(Array, LogIndexEntry, Vec<String>, (String, Vec<u8>)), JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    prepare_entry_v0_plain_entry_row_with_signer(
        &clock_id,
        &public_key,
        &signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        include_storage_bytes,
    )
}

fn prepare_entry_v0_plain_entry_row_with_signer(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    include_storage_bytes: bool,
) -> Result<(Array, LogIndexEntry, Vec<String>, (String, Vec<u8>)), JsValue> {
    let next = strings_from_array(next)?;
    let payload_data = payload_data.to_vec();
    let meta_data = optional_bytes_from_js(meta_data);
    prepare_entry_v0_plain_entry_row_with_signer_parts(
        clock_id,
        public_key,
        signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        PreparedPlainEntryRowMode::Full {
            include_storage_bytes,
        },
    )
}

fn prepare_entry_v0_plain_entry_storage_row_with_signer(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
) -> Result<(Array, LogIndexEntry, Vec<String>, (String, Vec<u8>)), JsValue> {
    let next = strings_from_array(next)?;
    let payload_data = payload_data.to_vec();
    let meta_data = optional_bytes_from_js(meta_data);
    prepare_entry_v0_plain_entry_row_with_signer_parts(
        clock_id,
        public_key,
        signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        PreparedPlainEntryRowMode::StorageOnly,
    )
}

fn prepare_entry_v0_plain_entry_row_with_signer_parts(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
    payload_data: Vec<u8>,
    row_mode: PreparedPlainEntryRowMode,
) -> Result<(Array, LogIndexEntry, Vec<String>, (String, Vec<u8>)), JsValue> {
    let core = prepare_entry_v0_plain_entry_core_with_signer_parts(
        clock_id,
        public_key,
        signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
    )?;
    let row = prepared_plain_entry_core_to_row(&core, row_mode);
    let entry = core.entry.clone();
    let initial_nexts = core.next.clone();
    let block = (core.hash, core.storage_bytes);
    Ok((row, entry, initial_nexts, block))
}

fn prepared_plain_entry_core_to_row(
    core: &PreparedPlainEntryCore,
    row_mode: PreparedPlainEntryRowMode,
) -> Array {
    let row = Array::new();
    match row_mode {
        PreparedPlainEntryRowMode::Full {
            include_storage_bytes,
        } => {
            if include_storage_bytes {
                row.push(&Uint8Array::from(core.storage_bytes.as_slice()));
                row.push(&JsValue::from_str(&core.hash));
                row.push(&Uint8Array::from(core.signature_bytes.as_slice()));
                row.push(&strings_to_array(core.next.clone()));
                row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
                row.push(&Uint8Array::from(core.payload_bytes.as_slice()));
                row.push(&Uint8Array::from(core.signature_with_key_bytes.as_slice()));
                row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
            } else {
                row.push(&JsValue::from_str(&core.hash));
                row.push(&Uint8Array::from(core.signature_bytes.as_slice()));
                row.push(&strings_to_array(core.next.clone()));
                row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
                row.push(&Uint8Array::from(core.payload_bytes.as_slice()));
                row.push(&Uint8Array::from(core.signature_with_key_bytes.as_slice()));
                row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
                row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
            }
        }
        PreparedPlainEntryRowMode::StorageOnly => {
            row.push(&Uint8Array::from(core.storage_bytes.as_slice()));
            row.push(&JsValue::from_str(&core.hash));
            row.push(&strings_to_array(core.next.clone()));
            row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
        }
        PreparedPlainEntryRowMode::StorageWithFacts => {
            row.push(&Uint8Array::from(core.storage_bytes.as_slice()));
            row.push(&JsValue::from_str(&core.hash));
            row.push(&strings_to_array(core.next.clone()));
            row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
            row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
            row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
        }
        PreparedPlainEntryRowMode::CommitFactsOnly => {
            row.push(&JsValue::from_str(&core.hash));
            row.push(&strings_to_array(core.next.clone()));
            row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
            row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
            row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
        }
        PreparedPlainEntryRowMode::CommitFactsNoNext => {
            row.push(&JsValue::from_str(&core.hash));
            row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
            row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
            row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
        }
    }
    row
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

fn prepare_entry_v0_plain_entries_rows_with_signer(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Array,
    entry_type: u8,
    meta_datas: Array,
    payload_datas: Array,
    include_storage_bytes: bool,
) -> Result<(Array, Vec<LogIndexEntry>, Vec<(String, Vec<u8>)>), JsValue> {
    prepare_entry_v0_plain_entries_rows_with_signer_inner(
        clock_id,
        public_key,
        signing_key,
        wall_times,
        logicals,
        gids,
        Some(nexts),
        entry_type,
        meta_datas,
        payload_datas,
        include_storage_bytes,
    )
}

fn prepare_entry_v0_plain_entries_rows_with_signer_inner(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Option<Array>,
    entry_type: u8,
    meta_datas: Array,
    payload_datas: Array,
    include_storage_bytes: bool,
) -> Result<(Array, Vec<LogIndexEntry>, Vec<(String, Vec<u8>)>), JsValue> {
    let len = payload_datas.length();
    if gids.length() != len || meta_datas.length() != len {
        return Err(JsValue::from_str("Expected equal column lengths"));
    }
    if let Some(nexts) = &nexts {
        if nexts.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }
    for numeric_len in [wall_times.length(), logicals.length()] {
        if numeric_len != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }

    let out = Array::new();
    let mut entries = Vec::with_capacity(len as usize);
    let mut blocks = Vec::with_capacity(len as usize);
    for i in 0..len {
        let payload_data = required_bytes_from_array(&payload_datas, i, "payload")?;
        let next = match &nexts {
            Some(nexts) => strings_from_array(required_array_from_array(nexts, i)?)?,
            None => Vec::new(),
        };
        let (row, entry, _initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                clock_id,
                public_key,
                signing_key,
                wall_times.get_index(i),
                logicals.get_index(i),
                required_string_from_array(&gids, i)?,
                next,
                entry_type,
                optional_bytes_from_js(meta_datas.get(i)),
                payload_data,
                PreparedPlainEntryRowMode::Full {
                    include_storage_bytes,
                },
            )?;
        out.push(&row);
        entries.push(entry);
        blocks.push(block);
    }

    Ok((out, entries, blocks))
}

#[wasm_bindgen]
pub fn prepare_entry_v0_plain_chain(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gid: String,
    initial_next: Array,
    entry_type: u8,
    meta_datas: Array,
    payload_datas: Array,
) -> Result<Array, JsValue> {
    Ok(prepare_entry_v0_plain_chain_rows(
        clock_id,
        private_key,
        public_key,
        wall_times,
        logicals,
        gid,
        initial_next,
        entry_type,
        meta_datas,
        payload_datas,
        true,
    )?
    .0)
}

#[wasm_bindgen]
pub fn prepare_entry_v0_plain_entry(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let (row, _entry, _initial_nexts, _block) = prepare_entry_v0_plain_entry_row(
        clock_id,
        private_key,
        public_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        true,
    )?;
    Ok(row)
}

#[wasm_bindgen]
pub fn calculate_raw_cid_v1(bytes: Uint8Array) -> String {
    calculate_raw_cid_v1_from_bytes(&bytes.to_vec())
}

#[wasm_bindgen]
pub fn calculate_raw_cid_v1_batch(blocks: Array) -> Result<Array, JsValue> {
    let out = Array::new();
    for i in 0..blocks.length() {
        let value = blocks.get(i);
        if value.is_undefined() || value.is_null() {
            return Err(JsValue::from_str("Expected block bytes"));
        }
        let bytes = Uint8Array::new(&value).to_vec();
        out.push(&JsValue::from_str(&calculate_raw_cid_v1_from_bytes(&bytes)));
    }
    Ok(out)
}

#[wasm_bindgen]
pub fn prepare_raw_entry_v0_batch(blocks: Array) -> Result<Array, JsValue> {
    let mut raw_blocks = Vec::with_capacity(blocks.length() as usize);
    for i in 0..blocks.length() {
        raw_blocks.push(required_bytes_from_array(&blocks, i, "entry storage")?);
    }
    let entries = prepare_raw_entry_v0_blocks(raw_blocks)?;
    let out = Array::new();
    for entry in &entries {
        out.push(&prepared_raw_entry_v0_to_row(entry));
    }
    Ok(out)
}

fn prepared_raw_entry_v0_to_row(entry: &PreparedRawEntryV0) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&entry.cid));
    row.push(&Uint8Array::from(entry.hash_digest_bytes.as_slice()));
    row.push(&JsValue::from_f64(entry.byte_length as f64));
    row.push(&Uint8Array::from(entry.clock_id.as_slice()));
    row.push(&JsValue::from_str(&entry.wall_time.to_string()));
    row.push(&JsValue::from_f64(entry.logical as f64));
    row.push(&JsValue::from_str(&entry.gid));

    let next = Array::new();
    for hash in &entry.next {
        next.push(&JsValue::from_str(hash));
    }
    row.push(&next);
    row.push(&JsValue::from_f64(entry.entry_type as f64));
    row.push(&Uint8Array::from(entry.meta_bytes.as_slice()));
    match &entry.meta_data {
        Some(data) => row.push(&Uint8Array::from(data.as_slice())),
        None => row.push(&JsValue::UNDEFINED),
    };
    row.push(&JsValue::from_f64(entry.payload_byte_length as f64));
    row.push(&JsValue::from_bool(entry.signature_verified));
    match entry.requested_replicas {
        Some(value) => row.push(&JsValue::from_f64(value as f64)),
        None => row.push(&JsValue::UNDEFINED),
    };
    row
}

#[wasm_bindgen]
pub fn benchmark_plain_entry_v0_core(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    iterations: u32,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    let payload_data = payload_data.to_vec();
    let gid = String::from("native-log-core-ceiling");
    let mut profile = NativeLogAppendProfile::default();
    let mut input_copy_ms = 0.0;
    let mut storage_bytes_total = 0usize;
    let mut hash_bytes_total = 0usize;
    let started = js_sys::Date::now();

    for i in 0..iterations {
        let copy_started = js_sys::Date::now();
        let payload_data = payload_data.clone();
        input_copy_ms += js_sys::Date::now() - copy_started;

        let core_started = js_sys::Date::now();
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &clock_id,
            &public_key,
            &signing_key,
            1_700_000_000_000 + i as u64,
            i,
            gid.clone(),
            Vec::new(),
            0,
            None,
            &payload_data,
            Some(&mut profile),
        )?;
        profile.entry_core_ms += js_sys::Date::now() - core_started;
        storage_bytes_total += core.storage_bytes.len();
        hash_bytes_total += core.hash.len();
    }

    let total_ms = js_sys::Date::now() - started;
    let row = Array::new();
    row.push(&JsValue::from_f64(total_ms));
    row.push(&JsValue::from_f64(input_copy_ms));
    row.push(&JsValue::from_f64(profile.entry_core_ms));
    row.push(&JsValue::from_f64(profile.encode_meta_ms));
    row.push(&JsValue::from_f64(profile.encode_payload_ms));
    row.push(&JsValue::from_f64(profile.encode_signable_ms));
    row.push(&JsValue::from_f64(profile.sign_ms));
    row.push(&JsValue::from_f64(profile.encode_signature_ms));
    row.push(&JsValue::from_f64(profile.encode_storage_ms));
    row.push(&JsValue::from_f64(profile.cid_ms));
    row.push(&JsValue::from_f64(profile.cid_hash_ms));
    row.push(&JsValue::from_f64(profile.cid_string_ms));
    row.push(&JsValue::from_f64(profile.index_entry_ms));
    row.push(&JsValue::from_f64(storage_bytes_total as f64));
    row.push(&JsValue::from_f64(hash_bytes_total as f64));
    Ok(row)
}

#[wasm_bindgen]
pub fn benchmark_plain_entry_v0_digest_key_core(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    iterations: u32,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    let payload_data = payload_data.to_vec();
    let gid = String::from("native-log-digest-key-core-ceiling");
    let mut profile = NativeLogAppendProfile::default();
    let mut input_copy_ms = 0.0;
    let mut storage_bytes_total = 0usize;
    let mut hash_bytes_total = 0usize;
    let started = js_sys::Date::now();

    for i in 0..iterations {
        let copy_started = js_sys::Date::now();
        let payload_data = payload_data.clone();
        input_copy_ms += js_sys::Date::now() - copy_started;

        let core_started = js_sys::Date::now();
        let (storage_len, digest_len) =
            prepare_entry_v0_plain_entry_commit_digest_key_core_profiled(
                &clock_id,
                &public_key,
                &signing_key,
                1_700_000_000_000 + i as u64,
                i,
                &gid,
                0,
                payload_data,
                Some(&mut profile),
            );
        profile.entry_core_ms += js_sys::Date::now() - core_started;
        storage_bytes_total += storage_len;
        hash_bytes_total += digest_len;
    }

    let total_ms = js_sys::Date::now() - started;
    let row = Array::new();
    row.push(&JsValue::from_f64(total_ms));
    row.push(&JsValue::from_f64(input_copy_ms));
    row.push(&JsValue::from_f64(profile.entry_core_ms));
    row.push(&JsValue::from_f64(profile.encode_meta_ms));
    row.push(&JsValue::from_f64(profile.encode_payload_ms));
    row.push(&JsValue::from_f64(profile.encode_signable_ms));
    row.push(&JsValue::from_f64(profile.sign_ms));
    row.push(&JsValue::from_f64(profile.encode_signature_ms));
    row.push(&JsValue::from_f64(profile.encode_storage_ms));
    row.push(&JsValue::from_f64(profile.cid_ms));
    row.push(&JsValue::from_f64(profile.cid_hash_ms));
    row.push(&JsValue::from_f64(profile.cid_string_ms));
    row.push(&JsValue::from_f64(profile.index_entry_ms));
    row.push(&JsValue::from_f64(storage_bytes_total as f64));
    row.push(&JsValue::from_f64(hash_bytes_total as f64));
    Ok(row)
}

#[wasm_bindgen]
pub fn benchmark_plain_entry_v0_crypto(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    iterations: u32,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    let verifying_key = signing_key.verifying_key();
    let payload_data = payload_data.to_vec();
    let gid = String::from("native-log-crypto-ceiling");
    let meta = encode_meta_parts(&clock_id, 1_700_000_000_000, 0, &gid, &[], 0, None);
    let signable = encode_entry_v0_payload_data_unsigned_for_signing(&meta, &payload_data);
    let mut checksum = 0u32;
    let mut signature_bytes = [0u8; 64];
    let started = js_sys::Date::now();

    let sign_started = js_sys::Date::now();
    for i in 0..iterations {
        signature_bytes = sign_ed25519_with_key(&signing_key, &signable);
        checksum ^= signature_bytes[(i as usize) & 63] as u32;
    }
    let sign_ms = js_sys::Date::now() - sign_started;

    let signature = Signature::from_bytes(&signature_bytes);
    let verify_started = js_sys::Date::now();
    for i in 0..iterations {
        verifying_key
            .verify(&signable, &signature)
            .map_err(|_| JsValue::from_str("Ed25519 signature verification failed"))?;
        checksum ^= signature_bytes[((i as usize) + 17) & 63] as u32;
    }
    let verify_ms = js_sys::Date::now() - verify_started;

    let signature_with_key = encode_signature_with_key_parts(&signature_bytes, &public_key, 0);
    let storage = signable_entry_to_signed_storage(signable.clone(), &signature_with_key);
    let mut digest_bytes = [0u8; 32];
    let sha_started = js_sys::Date::now();
    for i in 0..iterations {
        let digest = Sha256::digest(&storage);
        digest_bytes = digest.into();
        checksum ^= digest_bytes[(i as usize) & 31] as u32;
    }
    let sha256_ms = js_sys::Date::now() - sha_started;

    let mut cid_len_total = 0usize;
    let cid_string_started = js_sys::Date::now();
    for i in 0..iterations {
        let cid = raw_cid_v1_string_from_digest(&digest_bytes);
        cid_len_total += cid.len();
        checksum ^= cid.as_bytes()[(i as usize) % cid.len()] as u32;
    }
    let cid_string_ms = js_sys::Date::now() - cid_string_started;

    let total_ms = js_sys::Date::now() - started;
    let row = Array::new();
    row.push(&JsValue::from_f64(total_ms));
    row.push(&JsValue::from_f64(signable.len() as f64));
    row.push(&JsValue::from_f64(storage.len() as f64));
    row.push(&JsValue::from_f64(sign_ms));
    row.push(&JsValue::from_f64(verify_ms));
    row.push(&JsValue::from_f64(sha256_ms));
    row.push(&JsValue::from_f64(cid_string_ms));
    row.push(&JsValue::from_f64(checksum as f64));
    row.push(&JsValue::from_f64(cid_len_total as f64));
    Ok(row)
}

#[wasm_bindgen]
pub fn benchmark_entry_v0_storage_verify_modes(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    iterations: u32,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    let payload_data = payload_data.to_vec();
    let len = iterations as usize;
    let mut storages = Vec::with_capacity(len);
    let mut storage_bytes_total = 0usize;
    for i in 0..iterations {
        let payload_data = payload_data.clone();
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &clock_id,
            &public_key,
            &signing_key,
            1_700_000_000_000 + i as u64,
            i,
            format!("native-log-verify-{i}"),
            Vec::new(),
            0,
            None,
            &payload_data,
            None,
        )?;
        storage_bytes_total += core.storage_bytes.len();
        storages.push(core.storage_bytes);
    }

    let parse_started = js_sys::Date::now();
    let mut parsed_signatures = Vec::with_capacity(len);
    let mut parsed_public_keys = Vec::with_capacity(len);
    let mut parsed_messages = Vec::with_capacity(len);
    let mut verifying_key_cache = HashMap::new();
    for storage in &storages {
        let parsed = parse_plain_entry_v0_storage_signature(storage)?;
        validate_signature_lengths(&parsed.signature, &parsed.public_key)?;
        let signature_bytes: [u8; 64] = parsed
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 signature length 64"))?;
        let verifying_key = cached_verifying_key(&mut verifying_key_cache, &parsed.public_key)?;
        parsed_signatures.push(Signature::from_bytes(&signature_bytes));
        parsed_public_keys.push(verifying_key);
        parsed_messages.push(parsed.signable);
    }
    let parse_ms = js_sys::Date::now() - parse_started;
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();

    let mut checksum = 0u32;
    let batch_started = js_sys::Date::now();
    let batch_ok = verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok();
    let batch_ms = js_sys::Date::now() - batch_started;
    checksum ^= u32::from(batch_ok);

    let serial_started = js_sys::Date::now();
    let mut serial_ok = true;
    for i in 0..parsed_signatures.len() {
        let ok = parsed_public_keys[i]
            .verify(&parsed_messages[i], &parsed_signatures[i])
            .is_ok();
        serial_ok = serial_ok && ok;
        checksum ^= (u32::from(ok)) << (i & 7);
    }
    let serial_ms = js_sys::Date::now() - serial_started;

    let storage_refs = storages
        .iter()
        .map(|storage: &Vec<u8>| storage.as_slice())
        .collect::<Vec<_>>();
    let storage_verify_started = js_sys::Date::now();
    let storage_verified = verify_entry_v0_ed25519_storage_slices(&storage_refs)?;
    let storage_verify_ms = js_sys::Date::now() - storage_verify_started;
    let storage_ok = storage_verified.iter().all(|flag| *flag != 0);
    checksum ^= u32::from(storage_ok) << 16;

    let row = Array::new();
    row.push(&JsValue::from_f64(parse_ms));
    row.push(&JsValue::from_f64(batch_ms));
    row.push(&JsValue::from_f64(serial_ms));
    row.push(&JsValue::from_f64(storage_verify_ms));
    row.push(&JsValue::from_f64(iterations as f64));
    row.push(&JsValue::from_bool(batch_ok));
    row.push(&JsValue::from_bool(serial_ok));
    row.push(&JsValue::from_bool(storage_ok));
    row.push(&JsValue::from_f64(checksum as f64));
    row.push(&JsValue::from_f64(storage_bytes_total as f64));
    Ok(row)
}

fn encode_entry_v0_storage_vec(
    clock_id: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    signature: Uint8Array,
    signature_public_key: Uint8Array,
    prehash: u8,
) -> Result<Vec<u8>, JsValue> {
    let signature = signature.to_vec();
    let public_key = signature_public_key.to_vec();
    validate_signature_lengths(&signature, &public_key)?;
    let next = strings_from_array(next)?;
    Ok(encode_entry_v0(
        EntryV0EncodeInput {
            clock_id: clock_id.to_vec(),
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data: optional_bytes_from_js(meta_data),
            payload_data: payload_data.to_vec(),
        },
        Some(SignatureInput {
            signature,
            public_key,
            prehash,
        }),
    ))
}

fn storage_with_cid_to_row(bytes: Vec<u8>) -> Array {
    let row = Array::new();
    row.push(&Uint8Array::from(bytes.as_slice()));
    row.push(&JsValue::from_str(&calculate_raw_cid_v1_from_bytes(&bytes)));
    row
}

#[wasm_bindgen]
pub fn entry_v0_plain_payload_data_from_storage(bytes: Uint8Array) -> Result<Uint8Array, JsValue> {
    let bytes = bytes.to_vec();
    let storage = parse_plain_entry_v0_storage(&bytes)?;
    let payload = parse_raw_entry_v0_payload(storage.payload)?;
    Ok(Uint8Array::from(payload.data))
}

fn strings_from_array(values: Array) -> Result<Vec<String>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for value in values.iter() {
        let Some(value) = value.as_string() else {
            return Err(JsValue::from_str("Expected string array"));
        };
        out.push(value);
    }
    Ok(out)
}

fn string_arrays_from_array(values: Array) -> Result<Vec<Vec<String>>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for value in values.iter() {
        if !Array::is_array(&value) {
            return Err(JsValue::from_str("Expected string array array"));
        }
        out.push(strings_from_array(Array::from(&value))?);
    }
    Ok(out)
}

fn block_key_values_from_arrays(
    keys: &Array,
    values: &Array,
) -> Result<Vec<(String, Vec<u8>)>, JsValue> {
    if keys.length() != values.length() {
        return Err(JsValue::from_str("Expected equal column lengths"));
    }
    let mut entries = Vec::with_capacity(keys.length() as usize);
    for index in 0..keys.length() {
        entries.push((
            required_string_from_array(keys, index)?,
            required_bytes_from_array(values, index, "block")?,
        ));
    }
    Ok(entries)
}

#[allow(clippy::too_many_arguments)]
fn validate_entry_batch_lengths(
    len: u32,
    gids: &Array,
    nexts: &Array,
    meta_datas: &Array,
    payload_datas: &Array,
    wall_times: &BigUint64Array,
    logicals: &Uint32Array,
    entry_types: &Uint8Array,
) -> Result<(), JsValue> {
    for values in [gids, nexts, meta_datas, payload_datas] {
        if values.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }
    for numeric_len in [wall_times.length(), logicals.length(), entry_types.length()] {
        if numeric_len != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn entry_input_from_batch(
    index: u32,
    clock_ids: &Array,
    wall_times: &BigUint64Array,
    logicals: &Uint32Array,
    gids: &Array,
    nexts: &Array,
    entry_types: &Uint8Array,
    meta_datas: &Array,
    payload_datas: &Array,
) -> Result<EntryV0EncodeInput, JsValue> {
    Ok(EntryV0EncodeInput {
        clock_id: required_bytes_from_array(clock_ids, index, "clock id")?,
        wall_time: wall_times.get_index(index),
        logical: logicals.get_index(index),
        gid: required_string_from_array(gids, index)?,
        next: strings_from_array(required_array_from_array(nexts, index)?)?,
        entry_type: entry_types.get_index(index),
        meta_data: optional_bytes_from_js(meta_datas.get(index)),
        payload_data: required_bytes_from_array(payload_datas, index, "payload")?,
    })
}

fn strings_to_array(values: Vec<String>) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&JsValue::from_str(&value));
    }
    out
}

fn reference_gid_rows_to_array(values: Vec<(String, String)>) -> Array {
    let out = Array::new();
    for (hash, gid) in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&hash));
        row.push(&JsValue::from_str(&gid));
        out.push(&row);
    }
    out
}

fn reference_gid_flat_rows_to_array(values: Vec<(u32, String, String)>) -> Array {
    let out = Array::new();
    for (position, hash, gid) in values {
        let row = Array::new();
        row.push(&JsValue::from_f64(position as f64));
        row.push(&JsValue::from_str(&hash));
        row.push(&JsValue::from_str(&gid));
        out.push(&row);
    }
    out
}

fn optional_bytes_from_js(value: JsValue) -> Option<Vec<u8>> {
    if value.is_undefined() || value.is_null() {
        return None;
    }
    Some(Uint8Array::new(&value).to_vec())
}

fn required_string_from_array(values: &Array, index: u32) -> Result<String, JsValue> {
    values
        .get(index)
        .as_string()
        .ok_or_else(|| JsValue::from_str("Expected string field"))
}

fn required_bytes_from_array(values: &Array, index: u32, field: &str) -> Result<Vec<u8>, JsValue> {
    let value = values.get(index);
    if value.is_undefined() || value.is_null() {
        return Err(JsValue::from_str(&format!("Expected {field} bytes")));
    }
    Ok(Uint8Array::new(&value).to_vec())
}

fn required_array_from_array(values: &Array, index: u32) -> Result<Array, JsValue> {
    let value = values.get(index);
    if !Array::is_array(&value) {
        return Err(JsValue::from_str("Expected array field"));
    }
    Ok(Array::from(&value))
}

fn log_entries_to_rows(values: Vec<LogIndexEntry>) -> Array {
    let out = Array::new();
    for entry in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&entry.hash));
        row.push(&JsValue::from_str(&entry.gid));
        row.push(&JsValue::from_str(&entry.wall_time.to_string()));
        row.push(&JsValue::from_f64(entry.logical as f64));
        out.push(&row);
    }
    out
}

fn log_data_entries_to_rows(values: Vec<LogIndexEntry>) -> Array {
    let out = Array::new();
    for entry in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&entry.hash));
        match entry.data {
            Some(data) => row.push(&Uint8Array::from(data.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
        out.push(&row);
    }
    out
}

fn log_join_entries_to_rows(values: Vec<LogIndexEntry>) -> Array {
    let out = Array::new();
    for entry in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&entry.hash));
        row.push(&JsValue::from_str(&entry.gid));
        row.push(&JsValue::from_str(&entry.wall_time.to_string()));
        row.push(&JsValue::from_f64(entry.logical as f64));
        row.push(&JsValue::from_f64(entry.entry_type as f64));
        row.push(&strings_to_array(entry.next));
        out.push(&row);
    }
    out
}

fn log_trim_entries_to_rows(values: Vec<LogIndexEntry>) -> Array {
    let out = Array::new();
    for entry in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&entry.hash));
        row.push(&JsValue::from_str(&entry.gid));
        row.push(&JsValue::from_str(&entry.wall_time.to_string()));
        row.push(&JsValue::from_f64(entry.logical as f64));
        row.push(&JsValue::from_f64(entry.entry_type as f64));
        row.push(&strings_to_array(entry.next));
        row.push(&JsValue::from_f64(entry.payload_size as f64));
        row.push(&JsValue::from_bool(entry.head));
        match entry.data {
            Some(data) => row.push(&Uint8Array::from(data.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
        out.push(&row);
    }
    out
}

fn log_optional_entry_metadata_to_rows(values: Vec<Option<LogEntryMetadata>>) -> Array {
    let out = Array::new();
    for value in values {
        let Some((hash, gid, data, replicas)) = value else {
            out.push(&JsValue::UNDEFINED);
            continue;
        };
        let row = Array::new();
        row.push(&JsValue::from_str(&hash));
        row.push(&JsValue::from_str(&gid));
        match data {
            Some(data) => row.push(&Uint8Array::from(data.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
        match replicas {
            Some(replicas) => row.push(&JsValue::from_f64(replicas as f64)),
            None => row.push(&JsValue::UNDEFINED),
        };
        out.push(&row);
    }
    out
}

fn log_optional_entry_metadata_hints_to_rows(values: Vec<Option<LogEntryMetadata>>) -> Array {
    let out = Array::new();
    for value in values {
        let Some((hash, gid, data, replicas)) = value else {
            out.push(&JsValue::UNDEFINED);
            continue;
        };
        let row = Array::new();
        row.push(&JsValue::from_str(&hash));
        row.push(&JsValue::from_str(&gid));
        match replicas {
            Some(replicas) => row.push(&JsValue::from_f64(replicas as f64)),
            None => row.push(&JsValue::UNDEFINED),
        };
        match data.filter(|_| replicas.is_none()) {
            Some(data) => row.push(&Uint8Array::from(data.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
        out.push(&row);
    }
    out
}

fn join_plan_to_row(plan: JoinPlan) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_bool(plan.skip));
    row.push(&strings_to_array(plan.missing_parents));
    row.push(&JsValue::from_bool(plan.cut_checked));
    row.push(&JsValue::from_bool(plan.covered_by_cut));
    row
}

#[cfg(test)]
mod tests {
    use super::{
        verify_prepared_entry_v0_ed25519_storage_slices_all, JoinPlan, LogGraphIndex,
        LogIndexEntry, NativeLogBlockStore, PreparedEntryV0SignatureInput,
    };
    use crate::append::trim_oldest_log_entry_hashes_core;
    use crate::codec::{
        encode_entry_v0_parts_unsigned_for_signing, encode_entry_v0_parts_with_signature_bytes,
        encode_entry_v0_payload_data_unsigned_for_signing, encode_payload,
        encode_signature_with_key_parts, parse_plain_entry_v0_storage,
        signable_entry_to_signed_storage, unsigned_entry_v0_storage_for_signing,
    };
    use crate::crypto::{prepared_entry_v0_signature_with_key, sign_ed25519_with_key};
    use ed25519_dalek::SigningKey;

    const APPEND: u8 = 0;
    const CUT: u8 = 1;

    #[test]
    fn signed_storage_reused_from_signable_entry_matches_full_encoder() {
        let meta = b"encoded-meta".to_vec();
        let payload = b"encoded-payload".to_vec();
        let signature_with_key = (0..96).map(|value| value as u8).collect::<Vec<_>>();

        let signable = encode_entry_v0_parts_with_signature_bytes(&meta, &payload, None);
        let optimized = signable_entry_to_signed_storage(signable, &signature_with_key);
        let expected =
            encode_entry_v0_parts_with_signature_bytes(&meta, &payload, Some(&signature_with_key));

        assert_eq!(optimized, expected);
    }

    #[test]
    fn unsigned_storage_prefix_reuses_signed_storage_prefix() {
        let meta = b"encoded-meta".to_vec();
        let payload = b"encoded-payload".to_vec();
        let signature_with_key = (0..96).map(|value| value as u8).collect::<Vec<_>>();
        let signable = encode_entry_v0_parts_unsigned_for_signing(&meta, &payload);
        let storage = signable_entry_to_signed_storage(signable.clone(), &signature_with_key);
        let parsed = parse_plain_entry_v0_storage(&storage).unwrap();

        assert_eq!(
            unsigned_entry_v0_storage_for_signing(&storage, parsed.signable_prefix_len).unwrap(),
            encode_entry_v0_parts_unsigned_for_signing(&meta, &payload)
        );
    }

    #[test]
    fn prepared_signature_offsets_verify_signed_storage() {
        let meta = b"encoded-meta".to_vec();
        let payload = b"encoded-payload".to_vec();
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let public_key = signing_key.verifying_key().to_bytes();
        let signable = encode_entry_v0_parts_unsigned_for_signing(&meta, &payload);
        let signature = sign_ed25519_with_key(&signing_key, &signable);
        let signature_with_key = encode_signature_with_key_parts(&signature, &public_key, 0);
        let storage = signable_entry_to_signed_storage(signable, &signature_with_key);
        let parsed = parse_plain_entry_v0_storage(&storage).unwrap();
        let input = PreparedEntryV0SignatureInput {
            storage_bytes: &storage,
            signable_prefix_len: parsed.signable_prefix_len,
            signature_with_key_start: parsed.signature_with_key_start,
            signature_with_key_len: parsed.signature_with_key_len,
        };

        assert_eq!(
            prepared_entry_v0_signature_with_key(&input).unwrap(),
            signature_with_key.as_slice()
        );
        assert!(verify_prepared_entry_v0_ed25519_storage_slices_all(&[input]).unwrap());
    }

    #[test]
    fn direct_payload_signable_encoding_matches_payload_parts_encoder() {
        let meta = b"encoded-meta".to_vec();
        let payload_data = b"document-payload".to_vec();
        let payload = encode_payload(&payload_data);

        assert_eq!(
            encode_entry_v0_payload_data_unsigned_for_signing(&meta, &payload_data),
            encode_entry_v0_parts_unsigned_for_signing(&meta, &payload)
        );
    }

    fn entry(hash: &str, gid: &str, next: &[&str], wall_time: u64) -> LogIndexEntry {
        LogIndexEntry::new(
            hash,
            gid,
            next.iter().map(|next| next.to_string()).collect(),
            APPEND,
            wall_time,
            0,
            1,
            true,
        )
    }

    #[test]
    fn tracks_heads_and_next_adjacency() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));
        assert_eq!(index.heads(None), vec!["a"]);

        index.put(entry("b", "g", &["a"], 2));
        assert_eq!(index.heads(None), vec!["b"]);
        assert_eq!(index.children("a"), vec!["b"]);
        assert_eq!(index.count_has_next("a", None), 1);

        index.put(entry("c", "g", &["a"], 3));
        assert_eq!(index.heads(None), vec!["b", "c"]);
        assert_eq!(index.count_has_next("a", None), 2);

        assert!(index.delete("b").is_some());
        assert_eq!(index.heads(None), vec!["c"]);
        assert_eq!(index.count_has_next("a", None), 1);

        assert!(index.delete("c").is_some());
        assert_eq!(index.heads(None), vec!["a"]);
        assert_eq!(index.count_has_next("a", None), 0);
    }

    #[test]
    fn deletes_many_entries() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));
        index.put(entry("b", "g", &["a"], 2));
        index.put(entry("c", "g", &["b"], 3));

        assert_eq!(index.delete_many(&["b".to_string(), "c".to_string()]), 2);
        assert!(!index.has("b"));
        assert!(!index.has("c"));
        assert_eq!(index.heads(None), vec!["a"]);
        assert_eq!(index.count_has_next("a", None), 0);
    }

    #[test]
    fn puts_append_chain_without_promoting_internal_heads() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put_append_chain(
            vec![
                LogIndexEntry::new("a", "g", vec!["root".to_string()], APPEND, 2, 0, 1, false),
                LogIndexEntry::new("b", "g", vec!["a".to_string()], APPEND, 3, 0, 1, false),
                LogIndexEntry::new("c", "g", vec!["b".to_string()], APPEND, 4, 0, 1, true),
            ],
            &["root".to_string()],
        );

        assert_eq!(index.heads(None), vec!["c"]);
        assert_eq!(index.children("root"), vec!["a"]);
        assert_eq!(index.children("a"), vec!["b"]);
        assert_eq!(index.children("b"), vec!["c"]);
    }

    #[test]
    fn puts_single_append_entry_without_one_item_chain_batch() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put_append_entry(
            LogIndexEntry::new("a", "g", vec!["root".to_string()], APPEND, 2, 0, 1, true),
            &["root".to_string()],
        );

        assert_eq!(index.heads(None), vec!["a"]);
        assert_eq!(index.children("root"), vec!["a"]);

        let mut cut_index = LogGraphIndex::new();
        cut_index.put(entry("root", "g", &[], 1));
        cut_index.put_append_entry(
            LogIndexEntry::new("cut", "g", vec!["root".to_string()], CUT, 2, 0, 1, true),
            &["root".to_string()],
        );

        assert_eq!(cut_index.heads(None), vec!["root", "cut"]);
        assert_eq!(cut_index.children("root"), vec!["cut"]);
    }

    #[test]
    fn prune_metadata_omits_data_when_replicas_decode() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new_with_data(
            "a",
            "g",
            vec![],
            APPEND,
            1,
            0,
            1,
            true,
            Some(vec![0, 2, 0, 0, 0]),
        ));
        index.put(LogIndexEntry::new_with_data(
            "b",
            "g",
            vec![],
            APPEND,
            2,
            0,
            1,
            true,
            Some(vec![9, 1, 2]),
        ));

        let metadata =
            index.entry_prune_metadata_batch(&["a".to_string(), "b".to_string(), "c".to_string()]);

        assert_eq!(metadata[0], Some(("g".to_string(), None, Some(2))));
        assert_eq!(
            metadata[1],
            Some(("g".to_string(), Some(vec![9, 1, 2]), None))
        );
        assert_eq!(metadata[2], None);

        let confirm_metadata =
            index.entry_prune_confirm_metadata_batch(&["a".to_string(), "b".to_string()]);
        assert_eq!(confirm_metadata[0], Some(("g".to_string(), Some(2))));
        assert_eq!(confirm_metadata[1], Some(("g".to_string(), None)));
        assert_eq!(
            index.entry_prune_confirm_metadata_ref("a"),
            Some(("g", Some(2)))
        );
    }

    #[test]
    fn puts_join_batch_without_rechecking_internal_heads() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put_join_batch(vec![
            LogIndexEntry::new("a", "g", vec!["root".to_string()], APPEND, 2, 0, 1, false),
            LogIndexEntry::new("b", "g", vec!["a".to_string()], APPEND, 3, 0, 1, true),
            LogIndexEntry::new("c", "g", vec!["root".to_string()], APPEND, 4, 0, 1, true),
        ]);

        assert_eq!(index.heads(None), vec!["b", "c"]);
        assert_eq!(index.children("root"), vec!["a", "c"]);
        assert_eq!(index.children("a"), vec!["b"]);

        let mut cut_index = LogGraphIndex::new();
        cut_index.put(entry("root", "g", &[], 1));
        cut_index.put_join_batch(vec![LogIndexEntry::new(
            "cut",
            "g",
            vec!["root".to_string()],
            CUT,
            2,
            0,
            1,
            true,
        )]);

        assert_eq!(cut_index.heads(None), vec!["root", "cut"]);
        assert_eq!(cut_index.children("root"), vec!["cut"]);
    }

    #[test]
    fn filters_heads_by_gid_and_clock_order() {
        let mut index = LogGraphIndex::new();
        index.put(entry("b", "one", &[], 2));
        index.put(entry("a", "one", &[], 1));
        index.put(entry("c", "two", &[], 3));

        assert_eq!(index.heads(None), vec!["a", "b", "c"]);
        assert_eq!(index.heads(Some("one")), vec!["a", "b"]);
        assert_eq!(index.heads(Some("two")), vec!["c"]);
        assert!(index.has_head(None));
        assert!(index.has_head(Some("one")));
        assert!(index.has_head(Some("two")));
        assert!(!index.has_head(Some("missing")));
        assert!(index.has_any_head(&["missing".to_string(), "two".to_string()]));
        assert!(!index.has_any_head(&["missing".to_string()]));
        assert_eq!(
            index.has_any_head_batch(&[
                vec!["missing".to_string(), "two".to_string()],
                vec!["missing".to_string()],
                Vec::new(),
            ]),
            vec![true, false, false],
        );
    }

    #[test]
    fn returns_oldest_and_newest_hash_by_clock_order() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new("b", "g", vec![], APPEND, 2, 0, 1, true));
        index.put(LogIndexEntry::new("a", "g", vec![], APPEND, 1, 1, 1, true));
        index.put(LogIndexEntry::new("c", "g", vec![], APPEND, 1, 0, 1, true));

        assert_eq!(index.oldest_hash(), Some("c".to_string()));
        assert_eq!(index.newest_hash(), Some("b".to_string()));

        index.delete("c");
        assert_eq!(index.oldest_hash(), Some("a".to_string()));
    }

    #[test]
    fn returns_oldest_entries_by_clock_order() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new("b", "g", vec![], APPEND, 2, 0, 1, true));
        index.put(LogIndexEntry::new("a", "g", vec![], APPEND, 1, 1, 1, true));
        index.put(LogIndexEntry::new("c", "g", vec![], APPEND, 1, 0, 1, true));

        assert_eq!(
            index
                .oldest_entries(2)
                .into_iter()
                .map(|entry| entry.hash)
                .collect::<Vec<_>>(),
            vec!["c", "a"]
        );
    }

    #[test]
    fn trims_oldest_hashes_without_materializing_entries() {
        let mut index = LogGraphIndex::new();
        let mut blocks = NativeLogBlockStore::new();
        for (hash, wall_time, logical) in [("b", 2, 0), ("a", 1, 1), ("c", 1, 0)] {
            index.put(LogIndexEntry::new(
                hash,
                "g",
                vec![],
                APPEND,
                wall_time,
                logical,
                1,
                true,
            ));
            blocks.put(hash.to_string(), vec![wall_time as u8, logical as u8]);
        }

        let trimmed = trim_oldest_log_entry_hashes_core(&mut index, &mut blocks, 1);

        assert_eq!(trimmed, vec!["c", "a"]);
        assert!(!index.has("c"));
        assert!(!index.has("a"));
        assert!(index.has("b"));
        assert!(!blocks.has("c"));
        assert!(!blocks.has("a"));
        assert!(blocks.has("b"));
    }

    #[test]
    fn trims_single_oldest_hash_without_batch_delete() {
        let mut index = LogGraphIndex::new();
        let mut blocks = NativeLogBlockStore::new();
        for (hash, wall_time) in [("a", 1), ("b", 2), ("c", 3)] {
            index.put(LogIndexEntry::new(
                hash,
                "g",
                vec![],
                APPEND,
                wall_time,
                0,
                1,
                true,
            ));
            blocks.put(hash.to_string(), vec![wall_time as u8]);
        }

        let trimmed = trim_oldest_log_entry_hashes_core(&mut index, &mut blocks, 2);

        assert_eq!(trimmed, vec!["a"]);
        assert!(!index.has("a"));
        assert!(index.has("b"));
        assert!(index.has("c"));
        assert!(!blocks.has("a"));
        assert!(blocks.has("b"));
        assert!(blocks.has("c"));
    }

    #[test]
    fn sums_payload_sizes() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new(
            "a",
            "one",
            Vec::new(),
            APPEND,
            1,
            0,
            7,
            true,
        ));
        index.put(LogIndexEntry::new(
            "b",
            "one",
            Vec::new(),
            APPEND,
            2,
            0,
            9,
            true,
        ));

        assert_eq!(index.payload_size_sum(), 16);

        index.delete("a");
        assert_eq!(index.payload_size_sum(), 9);
    }

    #[test]
    fn batches_membership_checks() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "one", &[], 1));
        index.put(entry("c", "one", &[], 3));

        assert_eq!(
            index.has_many(&["missing".to_string(), "a".to_string(), "c".to_string()]),
            vec!["a".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn returns_head_entries_for_append_planning() {
        let mut index = LogGraphIndex::new();
        index.put(entry("b", "one", &[], 2));
        index.put(entry("a", "one", &[], 1));
        index.put(entry("c", "two", &[], 3));

        let heads = index.head_entries(Some("one"));
        assert_eq!(heads.len(), 2);
        assert_eq!(heads[0].hash, "a");
        assert_eq!(heads[0].gid, "one");
        assert_eq!(heads[0].wall_time, 1);
        assert_eq!(heads[1].hash, "b");
        assert_eq!(heads[1].gid, "one");
        assert_eq!(heads[1].wall_time, 2);
    }

    #[test]
    fn returns_head_join_entries_for_cut_checks() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new(
            "cut",
            "one",
            vec!["a".to_string()],
            CUT,
            2,
            0,
            1,
            true,
        ));

        let heads = index.head_join_entries(Some("one"));
        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0].hash, "cut");
        assert_eq!(heads[0].entry_type, CUT);
        assert_eq!(heads[0].next, vec!["a".to_string()]);
    }

    #[test]
    fn returns_child_join_entries_for_cut_recursion() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));
        index.put(entry("b", "g", &["a"], 2));
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["a".to_string()],
            CUT,
            3,
            0,
            1,
            true,
        ));

        let children = index.child_join_entries("a");
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].hash, "b");
        assert_eq!(children[0].entry_type, APPEND);
        assert_eq!(children[1].hash, "cut");
        assert_eq!(children[1].entry_type, CUT);
    }

    #[test]
    fn plans_recursive_cut_deletes() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put(entry("child", "g", &["root"], 2));
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["child".to_string()],
            CUT,
            3,
            0,
            1,
            true,
        ));

        assert_eq!(
            index.plan_delete_recursively(&["cut".to_string()], true),
            vec!["child".to_string(), "root".to_string()]
        );
    }

    #[test]
    fn recursive_cut_delete_plan_keeps_alternative_branches() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put(entry("child", "g", &["root"], 2));
        index.put(entry("sibling", "g", &["root"], 3));
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["child".to_string()],
            CUT,
            4,
            0,
            1,
            true,
        ));

        assert_eq!(
            index.plan_delete_recursively(&["cut".to_string()], true),
            vec!["child".to_string()]
        );
    }

    #[test]
    fn cut_entries_do_not_demote_their_nexts() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["a".to_string()],
            CUT,
            2,
            0,
            1,
            true,
        ));

        assert_eq!(index.heads(None), vec!["a", "cut"]);
        assert_eq!(index.count_has_next("a", None), 1);

        assert!(index.delete("cut").is_some());
        assert_eq!(index.heads(None), vec!["a"]);
    }

    #[test]
    fn plans_join_missing_parents() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));

        assert_eq!(
            index.plan_join(
                "b",
                &["a".to_string(), "missing".to_string()],
                APPEND,
                false,
                None,
                None,
                None
            ),
            JoinPlan {
                skip: false,
                missing_parents: vec!["missing".to_string()],
                cut_checked: false,
                covered_by_cut: false
            }
        );
        assert_eq!(
            index.plan_join("a", &[], APPEND, false, None, None, None),
            JoinPlan {
                skip: true,
                missing_parents: Vec::new(),
                cut_checked: false,
                covered_by_cut: false
            }
        );
        assert_eq!(
            index.plan_join("a", &[], APPEND, true, None, None, None),
            JoinPlan {
                skip: false,
                missing_parents: Vec::new(),
                cut_checked: false,
                covered_by_cut: false
            }
        );
        assert_eq!(
            index.plan_join(
                "cut",
                &["missing".to_string()],
                CUT,
                false,
                None,
                None,
                None
            ),
            JoinPlan {
                skip: false,
                missing_parents: Vec::new(),
                cut_checked: false,
                covered_by_cut: false
            }
        );
    }

    #[test]
    fn plans_join_cut_coverage() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["old".to_string()],
            CUT,
            2,
            0,
            1,
            true,
        ));

        assert_eq!(
            index.plan_join(
                "old",
                &["missing".to_string()],
                APPEND,
                false,
                Some("g"),
                Some(1),
                Some(0)
            ),
            JoinPlan {
                skip: false,
                missing_parents: Vec::new(),
                cut_checked: true,
                covered_by_cut: true
            }
        );
        assert_eq!(
            index.plan_join(
                "new",
                &["missing".to_string()],
                APPEND,
                false,
                Some("g"),
                Some(3),
                Some(0)
            ),
            JoinPlan {
                skip: false,
                missing_parents: vec!["missing".to_string()],
                cut_checked: true,
                covered_by_cut: false
            }
        );
    }

    #[test]
    fn batch_plans_join_cut_coverage() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["old".to_string()],
            CUT,
            2,
            0,
            1,
            true,
        ));

        assert_eq!(
            index.plan_join_batch(
                &["old".to_string(), "new".to_string()],
                &[vec!["missing".to_string()], vec!["missing".to_string()]],
                &[APPEND, APPEND],
                false,
                Some((&["g".to_string(), "g".to_string()], &[1, 3], &[0, 0],)),
            ),
            vec![
                JoinPlan {
                    skip: false,
                    missing_parents: Vec::new(),
                    cut_checked: true,
                    covered_by_cut: true
                },
                JoinPlan {
                    skip: false,
                    missing_parents: vec!["missing".to_string()],
                    cut_checked: true,
                    covered_by_cut: false
                }
            ]
        );
    }

    #[test]
    fn reports_shadowed_gids_for_cross_gid_nexts() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "old", &[], 1));

        assert_eq!(
            index.shadowed_gids("new", &["a".to_string()], Some("b")),
            vec!["old"]
        );

        index.put(entry("c", "other", &["a"], 2));
        assert!(index
            .shadowed_gids("new", &["a".to_string()], Some("b"))
            .is_empty());
    }
}
