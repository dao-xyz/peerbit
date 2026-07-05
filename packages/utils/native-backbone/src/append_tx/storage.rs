use js_sys::{Array, Uint8Array};
use peerbit_shared_log_rust::commit_local_append_for_gid_compact_core;
use wasm_bindgen::prelude::*;

use crate::append_tx::coordinate_plan_to_row;
use crate::documents::{
    document_context_facts_to_row, document_index_append_commit, DocumentIndexAppendCommit,
};
use crate::js_interop::{
    array_from_value, bytes_field, string_field, strings_from_array, strings_to_array,
    trim_hashes_vec,
};
use crate::shared_log_plan::leader_samples_to_optional_rows;
use crate::NativePeerbitBackbone;

#[wasm_bindgen]
impl NativePeerbitBackbone {
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
}

impl NativePeerbitBackbone {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_plain_storage_append_transaction_inner(
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
        let previous_document_context = document_index_commit
            .as_ref()
            .and_then(|commit| commit.previous_context.clone());
        let (entry_row, trim_rows, trim_hashes, hash, digest, meta_bytes) = if commit_blocks {
            let (meta_data, payload_data) =
                self.copy_append_inputs_profiled(meta_data, &payload_data);
            let (entry_facts, trim_hashes, entry_row, trim_rows) = self
                .prepare_committed_log_append_rows_profiled(
                    wall_time,
                    logical,
                    gid.clone(),
                    next_hashes.clone(),
                    entry_type,
                    meta_data,
                    payload_data,
                    trim_length_to,
                    resolve_trimmed_entries,
                )?;
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

        let hash_number = self.hash_number_profiled(&digest)?;
        let document_hash = hash.clone();
        let document_gid = gid.clone();
        let coordinate_plan_started = profile_enabled.then(js_sys::Date::now);
        let coordinate_facts = commit_local_append_for_gid_compact_core(
            &mut self.shared_log,
            hash,
            gid,
            hash_number,
            &next_hashes,
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
            self.append_profile.coordinate_plan_ms += js_sys::Date::now() - started;
        }
        let coordinate_core_started = profile_enabled.then(js_sys::Date::now);
        self.commit_coordinate_core_from_compact_facts(
            &coordinate_facts,
            &next_hashes,
            &trim_hashes,
            wall_time,
            meta_bytes,
        );
        if let Some(started) = coordinate_core_started {
            self.append_profile.coordinate_core_ms += js_sys::Date::now() - started;
        }
        let document_trimmed_heads_processed = self.commit_append_document_index_profiled(
            document_index_commit,
            wall_time,
            &document_hash,
            &document_gid,
            payload_size,
            None,
            delete_trimmed_document_heads,
            &trim_hashes,
        )?;

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
        out.push(&strings_to_array(trim_hashes));
        out.push(&JsValue::from_bool(document_trimmed_heads_processed));
        out.push(
            &previous_document_context
                .as_ref()
                .map(|context| document_context_facts_to_row(context).into())
                .unwrap_or(JsValue::UNDEFINED),
        );
        if let Some(started) = result_row_started {
            self.append_profile.result_row_ms += js_sys::Date::now() - started;
        }
        if let Some(started) = storage_append_started {
            self.append_profile.storage_append_inner_ms += js_sys::Date::now() - started;
        }
        Ok(out)
    }
}
