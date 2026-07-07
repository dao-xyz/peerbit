use js_sys::{Array, BigUint64Array, Uint32Array, Uint8Array};
use peerbit_shared_log_rust::{
    commit_local_append_for_gid_compact_core, commit_local_appends_for_gids_compact_core,
    NativeLocalAppendCompactInput,
};
use wasm_bindgen::prelude::*;

use crate::append_tx::{
    coordinate_plan_to_row, ensure_batch_append_lens, ensure_batch_projection_lens,
    latest_compact_entry_row, required_projection_encoded_document, LatestBatchPendingAppend,
    LatestCompactBatchPendingAppend,
};
use crate::documents::{
    document_context_facts_to_row, document_index_append_commit,
    document_index_cached_projection_append_commit,
    document_index_cached_projection_plain_put_payload_append_commit,
    document_index_plain_put_payload_append_commit, DocumentIndexAppendCommit,
    DocumentIndexValuePrefix,
};
use crate::error::BackboneError;
use crate::js_interop::{
    ensure_same_len, has_duplicate_strings, optional_usize_from_js, required_bytes_from_array,
    strings_from_array, strings_to_array,
};
use crate::shared_log_plan::leader_samples_to_optional_rows;
use crate::NativePeerbitBackbone;

/// Forward a JsValue error from an untyped documents-layer helper without
/// altering its message. The document-commit builders and the
/// required-previous-signer validator construct every error via
/// `JsValue::from_str`, so `as_string()` recovers the exact string they would
/// have thrown; the fallback only guards the theoretically-non-string case.
fn js_wrapper_error(error: JsValue) -> BackboneError {
    BackboneError::Message(
        error
            .as_string()
            .unwrap_or_else(|| "Invalid document index append input".to_string()),
    )
}

#[wasm_bindgen]
impl NativePeerbitBackbone {
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_delete_transaction(
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
    ) -> Result<Array, JsValue> {
        let row = self.prepare_plain_storage_append_transaction_inner(
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
        )?;
        self.delete_document_inner(&document_key, true);
        Ok(row)
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
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
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let document_index_commit = document_index_append_commit(
            document_key,
            document_value_prefix_bytes,
            String::new(),
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        Ok(
            self.prepare_plain_committed_storage_append_document_index_latest_transaction_inner(
                wall_time,
                logical,
                fallback_gid,
                entry_type,
                meta_data,
                payload_data,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                resolve_trimmed_entries,
                trim_length_to,
                document_index_commit,
            )?,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
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
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        required_previous_signer_public_key: Uint8Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let mut document_index_commit = document_index_append_commit(
            document_key,
            document_value_prefix_bytes,
            String::new(),
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        document_index_commit.required_previous_signer_public_key =
            Some(required_previous_signer_public_key.to_vec());
        Ok(
            self.prepare_plain_committed_storage_append_document_index_latest_transaction_inner(
                wall_time,
                logical,
                fallback_gid,
                entry_type,
                meta_data,
                payload_data,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                resolve_trimmed_entries,
                trim_length_to,
                document_index_commit,
            )?,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_cached_plan_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
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
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let document_index_commit = document_index_cached_projection_append_commit(
            document_key,
            String::new(),
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan_id,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        Ok(
            self.prepare_plain_committed_storage_append_document_index_latest_transaction_inner(
                wall_time,
                logical,
                fallback_gid,
                entry_type,
                meta_data,
                payload_data,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                resolve_trimmed_entries,
                trim_length_to,
                document_index_commit,
            )?,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_compact_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
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
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_index_commit = document_index_append_commit(
            document_key,
            document_value_prefix_bytes,
            String::new(),
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        Ok(self
            .prepare_plain_committed_storage_append_document_index_latest_compact_transaction_inner(
                wall_time,
                logical,
                fallback_gid,
                entry_type,
                meta_data,
                payload_data,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                trim_length_to,
                document_index_commit,
            )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_compact_plain_put_payload_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_key: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_index_commit = document_index_plain_put_payload_append_commit(
            document_key,
            String::new(),
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
        )?;
        Ok(self
            .prepare_plain_committed_storage_append_document_index_latest_compact_transaction_inner(
                wall_time,
                logical,
                fallback_gid,
                entry_type,
                meta_data,
                payload_data,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                trim_length_to,
                document_index_commit,
            )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_compact_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
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
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
        required_previous_signer_public_key: Uint8Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let mut document_index_commit = document_index_append_commit(
            document_key,
            document_value_prefix_bytes,
            String::new(),
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        document_index_commit.required_previous_signer_public_key =
            Some(required_previous_signer_public_key.to_vec());
        Ok(self
            .prepare_plain_committed_storage_append_document_index_latest_compact_transaction_inner(
                wall_time,
                logical,
                fallback_gid,
                entry_type,
                meta_data,
                payload_data,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                trim_length_to,
                document_index_commit,
            )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_key: String,
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
            String::new(),
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
            document_projection_plan_id,
            document_projection_encoded_document,
            document_projection_signer,
        )?;
        Ok(self
            .prepare_plain_committed_storage_append_document_index_latest_compact_transaction_inner(
                wall_time,
                logical,
                fallback_gid,
                entry_type,
                meta_data,
                payload_data,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                trim_length_to,
                document_index_commit,
            )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_plain_put_payload_transaction(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_key: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_signer: JsValue,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_index_commit =
            document_index_cached_projection_plain_put_payload_append_commit(
                document_key,
                String::new(),
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan_id,
                document_projection_signer,
            )?;
        Ok(self
            .prepare_plain_committed_storage_append_document_index_latest_compact_transaction_inner(
                wall_time,
                logical,
                fallback_gid,
                entry_type,
                meta_data,
                payload_data,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                trim_length_to,
                document_index_commit,
            )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_keys: Array,
        document_value_prefix_bytes: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_same_len(
            len,
            document_value_prefix_bytes.length() as usize,
            "batch document value prefixes",
        )?;
        let document_keys = strings_from_array(document_keys)?;
        Ok(self
            .prepare_plain_committed_storage_append_document_index_latest_batch_transaction_inner(
                wall_times,
                logicals,
                fallback_gids,
                entry_type,
                meta_datas,
                payload_datas,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                resolve_trimmed_entries,
                trim_length_to,
                &mut |index| {
                    document_index_append_commit(
                        document_keys[index as usize].clone(),
                        required_bytes_from_array(
                            &document_value_prefix_bytes,
                            index,
                            "document value prefix",
                        )?
                        .to_vec(),
                        String::new(),
                        document_byte_element_index_limit,
                        document_delete_trimmed_heads,
                        JsValue::UNDEFINED,
                        JsValue::UNDEFINED,
                        JsValue::UNDEFINED,
                    )
                },
            )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_keys: Array,
        document_value_prefix_bytes: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        required_previous_signer_public_key: Uint8Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_same_len(
            len,
            document_value_prefix_bytes.length() as usize,
            "batch document value prefixes",
        )?;
        let required_previous_signer_public_key = required_previous_signer_public_key.to_vec();
        let document_keys = strings_from_array(document_keys)?;
        Ok(self
            .prepare_plain_committed_storage_append_document_index_latest_batch_transaction_inner(
                wall_times,
                logicals,
                fallback_gids,
                entry_type,
                meta_datas,
                payload_datas,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                resolve_trimmed_entries,
                trim_length_to,
                &mut |index| {
                    let mut document_index_commit = document_index_append_commit(
                        document_keys[index as usize].clone(),
                        required_bytes_from_array(
                            &document_value_prefix_bytes,
                            index,
                            "document value prefix",
                        )?
                        .to_vec(),
                        String::new(),
                        document_byte_element_index_limit,
                        document_delete_trimmed_heads,
                        JsValue::UNDEFINED,
                        JsValue::UNDEFINED,
                        JsValue::UNDEFINED,
                    )?;
                    document_index_commit.required_previous_signer_public_key =
                        Some(required_previous_signer_public_key.clone());
                    Ok(document_index_commit)
                },
            )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_keys: Array,
        document_value_prefix_bytes: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_same_len(
            len,
            document_value_prefix_bytes.length() as usize,
            "batch document value prefixes",
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_keys = strings_from_array(document_keys)?;
        let mut document_index_commits = Vec::with_capacity(len);
        for (index, document_key) in document_keys.iter().enumerate() {
            let index_u32 = index as u32;
            document_index_commits.push(document_index_append_commit(
                document_key.clone(),
                required_bytes_from_array(
                    &document_value_prefix_bytes,
                    index_u32,
                    "document value prefix",
                )?
                .to_vec(),
                String::new(),
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                JsValue::UNDEFINED,
                JsValue::UNDEFINED,
                JsValue::UNDEFINED,
            )?);
        }
        Ok(self.prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction_dispatch(
            wall_times,
            logicals,
            fallback_gids,
            entry_type,
            meta_datas,
            payload_datas,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            trim_length_to,
            &document_keys,
            document_index_commits,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_compact_plain_put_payload_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_keys: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_keys = strings_from_array(document_keys)?;
        let mut document_index_commits = Vec::with_capacity(len);
        for document_key in &document_keys {
            document_index_commits.push(document_index_plain_put_payload_append_commit(
                document_key.clone(),
                String::new(),
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
            )?);
        }
        Ok(self.prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction_dispatch(
            wall_times,
            logicals,
            fallback_gids,
            entry_type,
            meta_datas,
            payload_datas,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            trim_length_to,
            &document_keys,
            document_index_commits,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_compact_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_keys: Array,
        document_value_prefix_bytes: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        required_previous_signer_public_key: Uint8Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_same_len(
            len,
            document_value_prefix_bytes.length() as usize,
            "batch document value prefixes",
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let required_previous_signer_public_key = required_previous_signer_public_key.to_vec();
        let document_keys = strings_from_array(document_keys)?;
        let mut document_index_commits = Vec::with_capacity(len);
        for (index, document_key) in document_keys.iter().enumerate() {
            let index_u32 = index as u32;
            let mut document_index_commit = document_index_append_commit(
                document_key.clone(),
                required_bytes_from_array(
                    &document_value_prefix_bytes,
                    index_u32,
                    "document value prefix",
                )?
                .to_vec(),
                String::new(),
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                JsValue::UNDEFINED,
                JsValue::UNDEFINED,
                JsValue::UNDEFINED,
            )?;
            document_index_commit.required_previous_signer_public_key =
                Some(required_previous_signer_public_key.clone());
            document_index_commits.push(document_index_commit);
        }
        Ok(self.prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction_dispatch(
            wall_times,
            logicals,
            fallback_gids,
            entry_type,
            meta_datas,
            payload_datas,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            trim_length_to,
            &document_keys,
            document_index_commits,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_cached_plan_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_keys: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_ids: Uint32Array,
        document_projection_encoded_documents: Array,
        document_projection_signers: Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_batch_projection_lens(
            len,
            &document_projection_plan_ids,
            Some(&document_projection_encoded_documents),
            &document_projection_signers,
        )?;
        let document_keys = strings_from_array(document_keys)?;
        Ok(self
            .prepare_plain_committed_storage_append_document_index_latest_batch_transaction_inner(
                wall_times,
                logicals,
                fallback_gids,
                entry_type,
                meta_datas,
                payload_datas,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                resolve_trimmed_entries,
                trim_length_to,
                &mut |index| {
                    let encoded_document = required_projection_encoded_document(
                        &document_projection_encoded_documents,
                        index,
                    )?;
                    document_index_cached_projection_append_commit(
                        document_keys[index as usize].clone(),
                        String::new(),
                        document_byte_element_index_limit,
                        document_delete_trimmed_heads,
                        document_projection_plan_ids.get_index(index),
                        encoded_document,
                        document_projection_signers.get(index),
                    )
                },
            )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_cached_plan_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        document_keys: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_ids: Uint32Array,
        document_projection_encoded_documents: Array,
        document_projection_signers: Array,
        required_previous_signer_public_key: Uint8Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_batch_projection_lens(
            len,
            &document_projection_plan_ids,
            Some(&document_projection_encoded_documents),
            &document_projection_signers,
        )?;
        let required_previous_signer_public_key = required_previous_signer_public_key.to_vec();
        let document_keys = strings_from_array(document_keys)?;
        Ok(self
            .prepare_plain_committed_storage_append_document_index_latest_batch_transaction_inner(
                wall_times,
                logicals,
                fallback_gids,
                entry_type,
                meta_datas,
                payload_datas,
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                resolve_trimmed_entries,
                trim_length_to,
                &mut |index| {
                    let encoded_document = required_projection_encoded_document(
                        &document_projection_encoded_documents,
                        index,
                    )?;
                    let mut document_index_commit = document_index_cached_projection_append_commit(
                        document_keys[index as usize].clone(),
                        String::new(),
                        document_byte_element_index_limit,
                        document_delete_trimmed_heads,
                        document_projection_plan_ids.get_index(index),
                        encoded_document,
                        document_projection_signers.get(index),
                    )?;
                    document_index_commit.required_previous_signer_public_key =
                        Some(required_previous_signer_public_key.clone());
                    Ok(document_index_commit)
                },
            )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_keys: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_ids: Uint32Array,
        document_projection_encoded_documents: Array,
        document_projection_signers: Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_batch_projection_lens(
            len,
            &document_projection_plan_ids,
            Some(&document_projection_encoded_documents),
            &document_projection_signers,
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_keys = strings_from_array(document_keys)?;
        let mut document_index_commits = Vec::with_capacity(len);
        for (index, document_key) in document_keys.iter().enumerate() {
            let index_u32 = index as u32;
            let encoded_document = required_projection_encoded_document(
                &document_projection_encoded_documents,
                index_u32,
            )?;
            document_index_commits.push(document_index_cached_projection_append_commit(
                document_key.clone(),
                String::new(),
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan_ids.get_index(index_u32),
                encoded_document,
                document_projection_signers.get(index_u32),
            )?);
        }
        Ok(self.prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction_dispatch(
            wall_times,
            logicals,
            fallback_gids,
            entry_type,
            meta_datas,
            payload_datas,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            trim_length_to,
            &document_keys,
            document_index_commits,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_cached_plan_compact_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_keys: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_ids: Uint32Array,
        document_projection_encoded_documents: Array,
        document_projection_signers: Array,
        required_previous_signer_public_key: Uint8Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_batch_projection_lens(
            len,
            &document_projection_plan_ids,
            Some(&document_projection_encoded_documents),
            &document_projection_signers,
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let required_previous_signer_public_key = required_previous_signer_public_key.to_vec();
        let document_keys = strings_from_array(document_keys)?;
        let mut document_index_commits = Vec::with_capacity(len);
        for (index, document_key) in document_keys.iter().enumerate() {
            let index_u32 = index as u32;
            let encoded_document = required_projection_encoded_document(
                &document_projection_encoded_documents,
                index_u32,
            )?;
            let mut document_index_commit = document_index_cached_projection_append_commit(
                document_key.clone(),
                String::new(),
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan_ids.get_index(index_u32),
                encoded_document,
                document_projection_signers.get(index_u32),
            )?;
            document_index_commit.required_previous_signer_public_key =
                Some(required_previous_signer_public_key.clone());
            document_index_commits.push(document_index_commit);
        }
        Ok(self.prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction_dispatch(
            wall_times,
            logicals,
            fallback_gids,
            entry_type,
            meta_datas,
            payload_datas,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            trim_length_to,
            &document_keys,
            document_index_commits,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_plain_put_payload_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_keys: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_ids: Uint32Array,
        document_projection_signers: Array,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &fallback_gids,
            "batch fallback gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_batch_projection_lens(
            len,
            &document_projection_plan_ids,
            None,
            &document_projection_signers,
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_keys = strings_from_array(document_keys)?;
        let mut document_index_commits = Vec::with_capacity(len);
        for (index, document_key) in document_keys.iter().enumerate() {
            let index_u32 = index as u32;
            document_index_commits.push(
                document_index_cached_projection_plain_put_payload_append_commit(
                    document_key.clone(),
                    String::new(),
                    document_byte_element_index_limit,
                    document_delete_trimmed_heads,
                    document_projection_plan_ids.get_index(index_u32),
                    document_projection_signers.get(index_u32),
                )?,
            );
        }
        Ok(self.prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction_dispatch(
            wall_times,
            logicals,
            fallback_gids,
            entry_type,
            meta_datas,
            payload_datas,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            trim_length_to,
            &document_keys,
            document_index_commits,
        )?)
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_committed_storage_append_document_delete_transaction_trim(
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
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let row = self.prepare_plain_storage_append_transaction_inner(
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
        )?;
        self.delete_document_inner(&document_key, true);
        Ok(row)
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
    }
}

impl NativePeerbitBackbone {
    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction_dispatch(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        trim_length_to: Option<usize>,
        document_keys: &[String],
        document_index_commits: Vec<DocumentIndexAppendCommit>,
    ) -> Result<Array, BackboneError> {
        if has_duplicate_strings(document_keys) {
            let out = Array::new();
            for (index, document_index_commit) in document_index_commits.into_iter().enumerate() {
                let index_u32 = index as u32;
                let fallback_gid = fallback_gids
                    .get(index_u32)
                    .as_string()
                    .ok_or(BackboneError::ExpectedString("batch fallback gid"))?;
                let row = self
                    .prepare_plain_committed_storage_append_document_index_latest_compact_transaction_inner(
                        wall_times.get_index(index_u32),
                        logicals.get_index(index_u32),
                        fallback_gid,
                        entry_type,
                        meta_datas.get(index_u32),
                        required_bytes_from_array(&payload_datas, index_u32, "payload")?,
                        replicas,
                        role_age_ms,
                        now.clone(),
                        self_hash.clone(),
                        self_replicating,
                        trim_length_to,
                        document_index_commit,
                    )?;
                out.push(&row);
            }
            return Ok(out);
        }
        self.prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction_inner(
            wall_times,
            logicals,
            fallback_gids,
            entry_type,
            meta_datas,
            payload_datas,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            trim_length_to,
            document_index_commits,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_committed_storage_append_document_index_latest_transaction_inner(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        trim_length_to: JsValue,
        mut document_index_commit: DocumentIndexAppendCommit,
    ) -> Result<Array, BackboneError> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let (_, gid, next_hashes) =
            self.resolve_latest_document_append_context(&mut document_index_commit, fallback_gid)?;
        self.validate_document_index_required_previous_signer(&document_index_commit)
            .map_err(js_wrapper_error)?;
        self.prepare_plain_storage_append_transaction_inner(
            wall_time,
            logical,
            gid,
            next_hashes,
            entry_type,
            meta_data,
            payload_data,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            resolve_trimmed_entries,
            trim_length_to,
            true,
            Some(document_index_commit),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_committed_storage_append_document_index_latest_batch_transaction_inner(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        trim_length_to: JsValue,
        make_document_index_commit: &mut dyn FnMut(
            u32,
        )
            -> Result<DocumentIndexAppendCommit, JsValue>,
    ) -> Result<Array, BackboneError> {
        let batch_len = payload_datas.length() as usize;
        if batch_len == 0 {
            return Ok(Array::new());
        }
        let profile_enabled = self.append_profile_enabled;
        let storage_append_started = profile_enabled.then(crate::time::now_ms);
        self.reserve_document_batch(batch_len);
        let mut pending_appends = Vec::with_capacity(batch_len);
        let mut coordinate_inputs = Vec::with_capacity(batch_len);

        for index in 0..batch_len {
            if let Err(error) = self.prepare_latest_batch_append_item(
                index as u32,
                &wall_times,
                &logicals,
                &fallback_gids,
                entry_type,
                &meta_datas,
                &payload_datas,
                replicas,
                role_age_ms,
                &now,
                &self_hash,
                self_replicating,
                resolve_trimmed_entries,
                &trim_length_to,
                make_document_index_commit,
                &mut pending_appends,
                &mut coordinate_inputs,
            ) {
                // The per-item path fully committed every earlier entry before
                // failing, so flush the deferred coordinate commits to match.
                self.flush_latest_batch_pending_coordinates(
                    pending_appends,
                    coordinate_inputs,
                    replicas,
                    role_age_ms,
                    &now,
                    &self_hash,
                    self_replicating,
                )?;
                return Err(error);
            }
        }

        let coordinate_facts = self.plan_batch_compact_coordinates_profiled(
            coordinate_inputs,
            replicas,
            role_age_ms,
            &now,
            &self_hash,
            self_replicating,
            pending_appends.len(),
            "Native latest batch returned mismatched coordinate facts",
        )?;

        let out = Array::new();
        for (pending, coordinate_facts) in pending_appends.into_iter().zip(coordinate_facts) {
            let coordinate_core_started = profile_enabled.then(crate::time::now_ms);
            self.commit_coordinate_core_from_compact_facts(
                &coordinate_facts,
                &pending.next_hashes,
                &pending.trim_hashes,
                pending.wall_time,
                pending.meta_bytes,
            );
            if let Some(started) = coordinate_core_started {
                self.append_profile.coordinate_core_ms += crate::time::now_ms() - started;
            }

            // Rebuild the frozen JS entry/trim rows from the owned pending state
            // at the emit boundary, timed against the same entry_row/trim_rows
            // profile counters the pre-lift inline build used.
            let entry_row = self.committed_entry_facts_to_row_profiled(&pending.entry_facts);
            let trim_rows = self.native_backbone_trim_entries_to_rows_profiled(
                pending.trimmed_entries,
                pending.resolve_trimmed_entries,
            );

            let result_row_started = profile_enabled.then(crate::time::now_ms);
            let row = Array::new();
            row.push(&entry_row);
            row.push(&leader_samples_to_optional_rows(&coordinate_facts.leaders));
            row.push(&JsValue::from_bool(coordinate_facts.is_leader));
            row.push(&JsValue::from_bool(
                coordinate_facts.assigned_to_range_boundary,
            ));
            row.push(&coordinate_plan_to_row(&self.resolution, &coordinate_facts));
            row.push(&trim_rows);
            row.push(&strings_to_array(pending.trim_hashes));
            row.push(&JsValue::from_bool(
                pending.document_trimmed_heads_processed,
            ));
            row.push(
                &pending
                    .previous_document_context
                    .as_ref()
                    .map(|context| document_context_facts_to_row(context).into())
                    .unwrap_or(JsValue::UNDEFINED),
            );
            out.push(&row);
            if let Some(started) = result_row_started {
                self.append_profile.result_row_ms += crate::time::now_ms() - started;
            }
        }

        if let Some(started) = storage_append_started {
            self.append_profile.storage_append_inner_ms += crate::time::now_ms() - started;
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_latest_batch_append_item(
        &mut self,
        index: u32,
        wall_times: &BigUint64Array,
        logicals: &Uint32Array,
        fallback_gids: &Array,
        entry_type: u8,
        meta_datas: &Array,
        payload_datas: &Array,
        replicas: usize,
        role_age_ms: f64,
        now: &str,
        self_hash: &str,
        self_replicating: bool,
        resolve_trimmed_entries: bool,
        trim_length_to: &JsValue,
        make_document_index_commit: &mut dyn FnMut(
            u32,
        )
            -> Result<DocumentIndexAppendCommit, JsValue>,
        pending_appends: &mut Vec<LatestBatchPendingAppend>,
        coordinate_inputs: &mut Vec<NativeLocalAppendCompactInput>,
    ) -> Result<(), BackboneError> {
        let profile_enabled = self.append_profile_enabled;
        let fallback_gid = fallback_gids
            .get(index)
            .as_string()
            .ok_or(BackboneError::ExpectedString("batch fallback gid"))?;
        let mut document_index_commit =
            make_document_index_commit(index).map_err(js_wrapper_error)?;
        let payload_data = required_bytes_from_array(payload_datas, index, "payload")?;
        let trim_length_to = optional_usize_from_js(trim_length_to.clone(), "trimLengthTo")?;
        let payload_size = payload_data.length();
        let delete_trimmed_document_heads = document_index_commit.delete_trimmed_heads;
        let (previous_document_context, gid, next_hashes) =
            self.resolve_latest_document_append_context(&mut document_index_commit, fallback_gid)?;
        self.validate_document_index_required_previous_signer(&document_index_commit)
            .map_err(js_wrapper_error)?;

        let (meta_data, payload_data) =
            self.copy_append_inputs_profiled(meta_datas.get(index), &payload_data)?;

        let wall_time = wall_times.get_index(index);
        let (entry_facts, trimmed_entries, trim_hashes) = self
            .prepare_committed_log_append_owned_profiled(
                wall_time,
                logicals.get_index(index),
                gid.clone(),
                next_hashes.clone(),
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                resolve_trimmed_entries,
            )?;

        let hash_number = self.hash_number_profiled(&entry_facts.hash_digest_bytes)?;
        if index == 0 {
            // An unparseable `now` must surface exactly where the per-item
            // path raised it: after the first log append, before any other
            // entry or commit is applied.
            commit_local_appends_for_gids_compact_core(
                &mut self.shared_log,
                Vec::new(),
                replicas,
                role_age_ms,
                now,
                self_hash,
                self_replicating,
                true,
                true,
            )?;
        }

        coordinate_inputs.push(NativeLocalAppendCompactInput {
            entry_hash: entry_facts.hash.clone(),
            gid: gid.clone(),
            entry_hash_number: hash_number,
            next_hashes: next_hashes.clone(),
            delete_hashes: trim_hashes.clone(),
        });
        // Keep the entry hash for the document-index put; `entry_facts` itself
        // moves into the pending state so the JS entry row is rebuilt only when
        // the batch is emitted.
        let entry_hash = entry_facts.hash.clone();
        let mut pending_append = LatestBatchPendingAppend {
            wall_time,
            next_hashes,
            meta_bytes: entry_facts.meta_bytes.clone(),
            trim_hashes,
            entry_facts,
            trimmed_entries,
            resolve_trimmed_entries,
            document_trimmed_heads_processed: false,
            previous_document_context,
        };

        let document_index_started = profile_enabled.then(crate::time::now_ms);
        let prepared_document_put = match self.prepare_document_index_append_put(
            document_index_commit,
            wall_time,
            &entry_hash,
            &gid,
            payload_size,
            None,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                // The per-item path commits the coordinate before the document
                // index write can fail, so leave this entry coordinate-flushable.
                pending_appends.push(pending_append);
                return Err(error);
            }
        };
        self.commit_prepared_document_index_append_put(prepared_document_put);
        let document_trimmed_heads_processed = delete_trimmed_document_heads
            && self.delete_documents_by_context_heads_profiled(&pending_append.trim_hashes);
        pending_append.document_trimmed_heads_processed = document_trimmed_heads_processed;
        if let Some(started) = document_index_started {
            self.append_profile.document_index_commit_ms += crate::time::now_ms() - started;
        }
        pending_appends.push(pending_append);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn flush_latest_batch_pending_coordinates(
        &mut self,
        pending_appends: Vec<LatestBatchPendingAppend>,
        coordinate_inputs: Vec<NativeLocalAppendCompactInput>,
        replicas: usize,
        role_age_ms: f64,
        now: &str,
        self_hash: &str,
        self_replicating: bool,
    ) -> Result<(), BackboneError> {
        if coordinate_inputs.is_empty() {
            return Ok(());
        }
        let coordinate_facts = commit_local_appends_for_gids_compact_core(
            &mut self.shared_log,
            coordinate_inputs,
            replicas,
            role_age_ms,
            now,
            self_hash,
            self_replicating,
            true,
            true,
        )?;
        for (pending, coordinate_facts) in pending_appends.into_iter().zip(coordinate_facts) {
            self.commit_coordinate_core_from_compact_facts(
                &coordinate_facts,
                &pending.next_hashes,
                &pending.trim_hashes,
                pending.wall_time,
                pending.meta_bytes,
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction_inner(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        fallback_gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        trim_length_to: Option<usize>,
        document_index_commits: Vec<DocumentIndexAppendCommit>,
    ) -> Result<Array, BackboneError> {
        let profile_enabled = self.append_profile_enabled;
        let storage_append_started = profile_enabled.then(crate::time::now_ms);
        let batch_len = document_index_commits.len();
        self.reserve_document_batch(batch_len);
        let mut pending_appends = Vec::with_capacity(batch_len);
        let mut coordinate_inputs = Vec::with_capacity(batch_len);

        for (index, mut document_index_commit) in document_index_commits.into_iter().enumerate() {
            let index_u32 = index as u32;
            let fallback_gid = fallback_gids
                .get(index_u32)
                .as_string()
                .ok_or(BackboneError::ExpectedString("batch fallback gid"))?;
            let payload_bytes = required_bytes_from_array(&payload_datas, index_u32, "payload")?;
            let payload_size = payload_bytes.length();
            let delete_trimmed_document_heads = document_index_commit.delete_trimmed_heads;
            let (previous_document_context, gid, next_hashes) = self
                .resolve_latest_document_append_context(&mut document_index_commit, fallback_gid)?;
            self.validate_document_index_required_previous_signer(&document_index_commit)
                .map_err(js_wrapper_error)?;

            let (meta_data, payload_data) =
                self.copy_append_inputs_profiled(meta_datas.get(index_u32), &payload_bytes)?;

            let (entry_facts, trim_hashes) = self.prepare_latest_log_append_profiled(
                wall_times.get_index(index_u32),
                logicals.get_index(index_u32),
                gid.clone(),
                next_hashes,
                entry_type,
                meta_data,
                &payload_data,
                trim_length_to,
            )?;

            let hash_number = self.hash_number_profiled(&entry_facts.hash_digest_bytes)?;
            let materialization_bytes = trim_length_to
                .is_some()
                .then(|| self.blocks.get_ref(&entry_facts.hash).map(Uint8Array::from))
                .flatten();

            coordinate_inputs.push(NativeLocalAppendCompactInput {
                entry_hash: entry_facts.hash.clone(),
                gid: gid.clone(),
                entry_hash_number: hash_number,
                next_hashes: entry_facts.next.clone(),
                delete_hashes: trim_hashes.clone(),
            });
            let plain_put_payload_data = matches!(
                &document_index_commit.value_prefix,
                DocumentIndexValuePrefix::PlainPutPayloadIdentity
                    | DocumentIndexValuePrefix::PlainPutPayloadProjection { .. }
            )
            .then_some(payload_data);
            pending_appends.push(LatestCompactBatchPendingAppend {
                wall_time: wall_times.get_index(index_u32),
                payload_size,
                gid,
                entry_facts,
                trim_hashes,
                document_index_commit,
                previous_document_context,
                delete_trimmed_document_heads,
                plain_put_payload_data,
                materialization_bytes,
            });
        }

        let coordinate_facts = self.plan_batch_compact_coordinates_profiled(
            coordinate_inputs,
            replicas,
            role_age_ms,
            &now,
            &self_hash,
            self_replicating,
            pending_appends.len(),
            "Native compact batch returned mismatched coordinate facts",
        )?;

        let out = Array::new();
        for (pending, coordinate_facts) in pending_appends.into_iter().zip(coordinate_facts) {
            let coordinate_core_started = profile_enabled.then(crate::time::now_ms);
            self.commit_coordinate_core_from_compact_facts(
                &coordinate_facts,
                &pending.entry_facts.next,
                &pending.trim_hashes,
                pending.wall_time,
                pending.entry_facts.meta_bytes.clone(),
            );
            if let Some(started) = coordinate_core_started {
                self.append_profile.coordinate_core_ms += crate::time::now_ms() - started;
            }

            let document_trimmed_heads_processed = self.commit_append_document_index_profiled(
                Some(pending.document_index_commit),
                pending.wall_time,
                &pending.entry_facts.hash,
                &pending.gid,
                pending.payload_size,
                pending.plain_put_payload_data.as_deref(),
                pending.delete_trimmed_document_heads,
                &pending.trim_hashes,
            )?;

            let result_row_started = profile_enabled.then(crate::time::now_ms);
            let row = latest_compact_entry_row(
                &self.resolution,
                pending.entry_facts,
                &coordinate_facts,
                pending.trim_hashes,
                document_trimmed_heads_processed,
                pending.previous_document_context.as_ref(),
            );
            row.push(
                &pending
                    .materialization_bytes
                    .map(JsValue::from)
                    .unwrap_or(JsValue::UNDEFINED),
            );
            out.push(&row);
            if let Some(started) = result_row_started {
                self.append_profile.result_row_ms += crate::time::now_ms() - started;
            }
        }

        if let Some(started) = storage_append_started {
            self.append_profile.storage_append_inner_ms += crate::time::now_ms() - started;
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_committed_storage_append_document_index_latest_compact_transaction_inner(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        trim_length_to: Option<usize>,
        mut document_index_commit: DocumentIndexAppendCommit,
    ) -> Result<Array, BackboneError> {
        let profile_enabled = self.append_profile_enabled;
        let storage_append_started = profile_enabled.then(crate::time::now_ms);
        let payload_size = payload_data.length();
        let delete_trimmed_document_heads = document_index_commit.delete_trimmed_heads;
        let (previous_document_context, gid, next_hashes) =
            self.resolve_latest_document_append_context(&mut document_index_commit, fallback_gid)?;
        self.validate_document_index_required_previous_signer(&document_index_commit)
            .map_err(js_wrapper_error)?;

        let (meta_data, payload_data) =
            self.copy_append_inputs_profiled(meta_data, &payload_data)?;

        let (entry_facts, trim_hashes) = self.prepare_latest_log_append_profiled(
            wall_time,
            logical,
            gid.clone(),
            next_hashes,
            entry_type,
            meta_data,
            &payload_data,
            trim_length_to,
        )?;

        let hash_number = self.hash_number_profiled(&entry_facts.hash_digest_bytes)?;

        let coordinate_plan_started = profile_enabled.then(crate::time::now_ms);
        let coordinate_facts = commit_local_append_for_gid_compact_core(
            &mut self.shared_log,
            entry_facts.hash.clone(),
            gid.clone(),
            hash_number,
            &entry_facts.next,
            &trim_hashes,
            replicas,
            role_age_ms,
            &now,
            &self_hash,
            self_replicating,
            true,
            true,
        )?;
        if let Some(started) = coordinate_plan_started {
            self.append_profile.coordinate_plan_ms += crate::time::now_ms() - started;
        }

        let coordinate_core_started = profile_enabled.then(crate::time::now_ms);
        self.commit_coordinate_core_from_compact_facts(
            &coordinate_facts,
            &entry_facts.next,
            &trim_hashes,
            wall_time,
            entry_facts.meta_bytes.clone(),
        );
        if let Some(started) = coordinate_core_started {
            self.append_profile.coordinate_core_ms += crate::time::now_ms() - started;
        }

        let document_trimmed_heads_processed = self.commit_append_document_index_profiled(
            Some(document_index_commit),
            wall_time,
            &entry_facts.hash,
            &gid,
            payload_size,
            Some(&payload_data),
            delete_trimmed_document_heads,
            &trim_hashes,
        )?;

        let result_row_started = profile_enabled.then(crate::time::now_ms);
        let out = latest_compact_entry_row(
            &self.resolution,
            entry_facts,
            &coordinate_facts,
            trim_hashes,
            document_trimmed_heads_processed,
            previous_document_context.as_ref(),
        );
        if let Some(started) = result_row_started {
            self.append_profile.result_row_ms += crate::time::now_ms() - started;
        }
        if let Some(started) = storage_append_started {
            self.append_profile.storage_append_inner_ms += crate::time::now_ms() - started;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::BackboneError;

    // `js_wrapper_error` forwards the untyped documents-layer JsValue messages
    // verbatim through `BackboneError::Message`. These are the exact strings
    // the required-previous-signer validator throws; pin their Display so the
    // forwarded messages stay byte-for-byte with master. (The `JsValue` recovery
    // itself is exercised on wasm by the TS suite; host `cargo test` cannot
    // construct a round-tripping `JsValue`, so it pins the Message rendering.)
    #[test]
    fn forwarded_document_signer_messages_render_verbatim() {
        for message in [
            "Previous document signer public key unavailable",
            "Previous document signer public key did not match native policy",
        ] {
            assert_eq!(
                BackboneError::Message(message.to_string()).to_string(),
                message
            );
        }
    }
}
