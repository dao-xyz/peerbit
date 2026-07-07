use js_sys::{Array, BigUint64Array, Uint32Array, Uint8Array};
use peerbit_log_rust::NativeLogAppendProfile;
use peerbit_shared_log_rust::{
    commit_local_append_for_gid_compact_core, NativeLocalAppendCompactInput,
};
use wasm_bindgen::prelude::*;

use crate::append_tx::{
    ensure_batch_append_lens, ensure_batch_projection_lens, no_next_compact_entry_row,
    push_optional_trim_result, required_projection_encoded_document,
    LatestCompactBatchPendingAppend,
};
use crate::documents::{
    document_index_append_commit, document_index_cached_projection_append_commit,
    document_index_cached_projection_plain_put_payload_append_commit,
    document_index_plain_put_payload_append_commit, DocumentIndexAppendCommit,
    DocumentIndexValuePrefix,
};
use crate::js_interop::{
    ensure_same_len, optional_usize_from_js, required_bytes_from_array, strings_from_array,
    strings_to_array,
};
use crate::NativePeerbitBackbone;

#[wasm_bindgen]
impl NativePeerbitBackbone {
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
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
        let input_copy_started = profile_enabled.then(crate::time::now_ms);
        let payload_template = payload_data.to_vec();
        if let Some(started) = input_copy_started {
            self.append_profile.input_copy_ms += crate::time::now_ms() - started;
        }
        let payload_size = payload_template.len() as u32;
        let now = wall_time_start.to_string();
        let started = crate::time::now_ms();
        for i in 0..iterations {
            let wall_time = wall_time_start + i as u64;
            let logical = i;
            let gid = format!("native-backbone-loop-{wall_time_start}-{i}");
            let storage_append_started = profile_enabled.then(crate::time::now_ms);
            let payload_copy_started = profile_enabled.then(crate::time::now_ms);
            let payload_data = payload_template.clone();
            if let Some(started) = payload_copy_started {
                self.append_profile.input_copy_ms += crate::time::now_ms() - started;
            }

            let log_started = profile_enabled.then(crate::time::now_ms);
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
                self.append_profile.log_total_ms += crate::time::now_ms() - started;
                self.append_profile.add_log_profile(&log_profile);
            }

            let hash_number = self.hash_number_profiled(&entry_facts.hash_digest_bytes)?;

            let coordinate_plan_started = profile_enabled.then(crate::time::now_ms);
            let coordinate_facts = commit_local_append_for_gid_compact_core(
                &mut self.shared_log,
                entry_facts.hash.clone(),
                gid.clone(),
                hash_number,
                &[],
                &trim_hashes,
                replicas,
                0.0,
                &now,
                &self_hash,
                true,
                true,
                true,
            )?;
            if let Some(started) = coordinate_plan_started {
                self.append_profile.coordinate_plan_ms += crate::time::now_ms() - started;
            }

            let coordinate_core_started = profile_enabled.then(crate::time::now_ms);
            self.commit_coordinate_core_from_compact_facts(
                &coordinate_facts,
                &[],
                &trim_hashes,
                wall_time,
                entry_facts.meta_bytes.clone(),
            );
            if let Some(started) = coordinate_core_started {
                self.append_profile.coordinate_core_ms += crate::time::now_ms() - started;
            }

            let document_index_started = profile_enabled.then(crate::time::now_ms);
            let document_index_commit = use_document_index.then(|| DocumentIndexAppendCommit {
                key: format!("native-backbone-loop-doc-{wall_time_start}-{i}"),
                value_prefix: DocumentIndexValuePrefix::Bytes(Vec::new()),
                existing_created: None,
                byte_element_index_limit: document_byte_element_index_limit,
                delete_trimmed_heads: false,
                previous_context: None,
                known_existing: false,
                required_previous_signer_public_key: None,
            });
            self.put_document_index_for_append(
                document_index_commit,
                wall_time,
                &entry_facts.hash,
                &gid,
                payload_size,
            )?;
            if let Some(started) = document_index_started {
                self.append_profile.document_index_commit_ms += crate::time::now_ms() - started;
            }

            if let Some(started) = storage_append_started {
                self.append_profile.storage_append_inner_ms += crate::time::now_ms() - started;
            }
        }
        let row = Array::new();
        row.push(&JsValue::from_f64(crate::time::now_ms() - started));
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
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
        Ok(self.prepare_plain_storage_append_transaction_inner(
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
        )?)
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
    pub fn prepare_plain_committed_no_next_storage_append_document_index_compact_plain_put_payload_transaction(
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
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_index_commit = document_index_plain_put_payload_append_commit(
            document_key,
            document_existing_created,
            document_byte_element_index_limit,
            document_delete_trimmed_heads,
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
    pub fn prepare_plain_committed_no_next_storage_append_document_index_compact_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gids: Array,
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
        document_existing_created: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &gids,
            "batch gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_same_len(
            len,
            document_value_prefix_bytes.length() as usize,
            "batch document value prefixes",
        )?;
        ensure_same_len(
            len,
            document_existing_created.length() as usize,
            "batch document existing-created values",
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_keys = strings_from_array(document_keys)?;
        let document_existing_created = strings_from_array(document_existing_created)?;
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
                document_existing_created[index].clone(),
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                JsValue::UNDEFINED,
                JsValue::UNDEFINED,
                JsValue::UNDEFINED,
            )?);
        }
        self.prepare_plain_committed_no_next_storage_append_document_index_compact_batch_transaction_inner(
            wall_times,
            logicals,
            gids,
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
    pub fn prepare_plain_committed_no_next_storage_append_document_index_compact_plain_put_payload_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_keys: Array,
        document_existing_created: Array,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let len = payload_datas.length() as usize;
        ensure_batch_append_lens(
            len,
            &wall_times,
            &logicals,
            &gids,
            "batch gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_same_len(
            len,
            document_existing_created.length() as usize,
            "batch document existing-created values",
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_keys = strings_from_array(document_keys)?;
        let document_existing_created = strings_from_array(document_existing_created)?;
        let mut document_index_commits = Vec::with_capacity(len);
        for (index, document_key) in document_keys.iter().enumerate() {
            document_index_commits.push(document_index_plain_put_payload_append_commit(
                document_key.clone(),
                document_existing_created[index].clone(),
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
            )?);
        }
        self.prepare_plain_committed_no_next_storage_append_document_index_compact_batch_transaction_inner(
            wall_times,
            logicals,
            gids,
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
    pub fn prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_keys: Array,
        document_existing_created: Array,
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
            &gids,
            "batch gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_same_len(
            len,
            document_existing_created.length() as usize,
            "batch document existing-created values",
        )?;
        ensure_batch_projection_lens(
            len,
            &document_projection_plan_ids,
            Some(&document_projection_encoded_documents),
            &document_projection_signers,
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_keys = strings_from_array(document_keys)?;
        let document_existing_created = strings_from_array(document_existing_created)?;
        let mut document_index_commits = Vec::with_capacity(len);
        for (index, document_key) in document_keys.iter().enumerate() {
            let index_u32 = index as u32;
            let encoded_document = required_projection_encoded_document(
                &document_projection_encoded_documents,
                index_u32,
            )?;
            document_index_commits.push(document_index_cached_projection_append_commit(
                document_key.clone(),
                document_existing_created[index].clone(),
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan_ids.get_index(index_u32),
                encoded_document,
                document_projection_signers.get(index_u32),
            )?);
        }
        self.prepare_plain_committed_no_next_storage_append_document_index_compact_batch_transaction_inner(
            wall_times,
            logicals,
            gids,
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
    pub fn prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_plain_put_payload_batch_transaction(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        self_replicating: bool,
        document_keys: Array,
        document_existing_created: Array,
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
            &gids,
            "batch gids",
            &meta_datas,
            &document_keys,
        )?;
        ensure_same_len(
            len,
            document_existing_created.length() as usize,
            "batch document existing-created values",
        )?;
        ensure_batch_projection_lens(
            len,
            &document_projection_plan_ids,
            None,
            &document_projection_signers,
        )?;
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_keys = strings_from_array(document_keys)?;
        let document_existing_created = strings_from_array(document_existing_created)?;
        let mut document_index_commits = Vec::with_capacity(len);
        for (index, document_key) in document_keys.iter().enumerate() {
            let index_u32 = index as u32;
            document_index_commits.push(
                document_index_cached_projection_plain_put_payload_append_commit(
                    document_key.clone(),
                    document_existing_created[index].clone(),
                    document_byte_element_index_limit,
                    document_delete_trimmed_heads,
                    document_projection_plan_ids.get_index(index_u32),
                    document_projection_signers.get(index_u32),
                )?,
            );
        }
        self.prepare_plain_committed_no_next_storage_append_document_index_compact_batch_transaction_inner(
            wall_times,
            logicals,
            gids,
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
    pub fn prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_plain_put_payload_transaction(
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
        document_projection_signer: JsValue,
        trim_length_to: JsValue,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let document_index_commit =
            document_index_cached_projection_plain_put_payload_append_commit(
                document_key,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan_id,
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
}

impl NativePeerbitBackbone {
    #[allow(clippy::too_many_arguments)]
    fn prepare_plain_committed_no_next_storage_append_document_index_compact_batch_transaction_inner(
        &mut self,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gids: Array,
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
    ) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let storage_append_started = profile_enabled.then(crate::time::now_ms);
        let batch_len = document_index_commits.len();
        self.reserve_document_batch(batch_len);
        let mut pending_appends = Vec::with_capacity(batch_len);
        let mut coordinate_inputs = Vec::with_capacity(batch_len);

        for (index, document_index_commit) in document_index_commits.into_iter().enumerate() {
            let index_u32 = index as u32;
            let gid = gids
                .get(index_u32)
                .as_string()
                .ok_or_else(|| JsValue::from_str("Expected batch gid string"))?;
            let payload_bytes = required_bytes_from_array(&payload_datas, index_u32, "payload")?;
            let payload_size = payload_bytes.length();
            let delete_trimmed_document_heads = document_index_commit.delete_trimmed_heads;

            let (meta_data, payload_data) =
                self.copy_append_inputs_profiled(meta_datas.get(index_u32), &payload_bytes)?;

            let (entry_facts, trim_hashes) = self.prepare_no_next_log_append_profiled(
                wall_times.get_index(index_u32),
                logicals.get_index(index_u32),
                gid.clone(),
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
                next_hashes: Vec::new(),
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
                previous_document_context: None,
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
            "Native no-next compact batch returned mismatched coordinate facts",
        )?;

        let out = Array::new();
        for (pending, coordinate_facts) in pending_appends.into_iter().zip(coordinate_facts) {
            let coordinate_core_started = profile_enabled.then(crate::time::now_ms);
            self.commit_coordinate_core_from_compact_facts(
                &coordinate_facts,
                &[],
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
            let row = no_next_compact_entry_row(
                &self.resolution,
                &pending.entry_facts,
                &coordinate_facts,
            );
            if let Some(bytes) = pending.materialization_bytes.as_ref() {
                row.push(&strings_to_array(pending.trim_hashes));
                row.push(&JsValue::from_bool(document_trimmed_heads_processed));
                row.push(bytes);
            } else {
                push_optional_trim_result(
                    &row,
                    pending.trim_hashes,
                    document_trimmed_heads_processed,
                );
            }
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
        let storage_append_started = profile_enabled.then(crate::time::now_ms);
        let payload_size = payload_data.length();
        let delete_trimmed_document_heads = document_index_commit.delete_trimmed_heads;

        let (meta_data, payload_data) =
            self.copy_append_inputs_profiled(meta_data, &payload_data)?;

        let (entry_facts, trim_hashes) = self.prepare_no_next_log_append_profiled(
            wall_time,
            logical,
            gid.clone(),
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
            &[],
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
            &[],
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
        let out = no_next_compact_entry_row(&self.resolution, &entry_facts, &coordinate_facts);
        push_optional_trim_result(&out, trim_hashes, document_trimmed_heads_processed);
        if let Some(started) = result_row_started {
            self.append_profile.result_row_ms += crate::time::now_ms() - started;
        }
        if let Some(started) = storage_append_started {
            self.append_profile.storage_append_inner_ms += crate::time::now_ms() - started;
        }
        Ok(out)
    }
}
