use js_sys::{Array, BigUint64Array, Uint32Array, Uint8Array};
use peerbit_indexer_core::planner::{FieldPath, FieldValue};
use peerbit_log_rust::{LogIndexEntry, NativeCommittedEntryFacts, NativeLogAppendProfile};
use peerbit_shared_log_rust::{
    commit_local_appends_for_gids_compact_core, NativeLocalAppendCompactFacts,
    NativeLocalAppendCompactInput,
};
use wasm_bindgen::prelude::*;

use crate::documents::{
    document_context_facts_to_row, encode_document_context_suffix,
    plain_put_document_bytes_from_payload, project_document_index_simple_bytes_with_plan,
    DocumentContextFacts, DocumentIndexAppendCommit, DocumentIndexProjectionPlan,
    DocumentIndexValuePrefix, PreparedDocumentIndexAppendPut,
};
use crate::error::BackboneError;
use crate::js_interop::{
    array_from_value, ensure_same_len, hash_number_u64, number_to_row, numbers_to_rows,
    optional_bytes_from_js, string_field, strings_to_array,
};
use crate::shared_log_plan::leader_samples_to_optional_rows;
use crate::NativePeerbitBackbone;

mod committed_latest;
mod committed_no_next;
mod facts;
mod storage;

struct LatestCompactBatchPendingAppend {
    wall_time: u64,
    payload_size: u32,
    gid: String,
    entry_facts: NativeCommittedEntryFacts,
    trim_hashes: Vec<String>,
    document_index_commit: DocumentIndexAppendCommit,
    previous_document_context: Option<DocumentContextFacts>,
    delete_trimmed_document_heads: bool,
    plain_put_payload_data: Option<Vec<u8>>,
    materialization_bytes: Option<Uint8Array>,
}

struct LatestBatchPendingAppend {
    wall_time: u64,
    next_hashes: Vec<String>,
    meta_bytes: Vec<u8>,
    trim_hashes: Vec<String>,
    entry_row: Array,
    trim_rows: Array,
    document_trimmed_heads_processed: bool,
    previous_document_context: Option<DocumentContextFacts>,
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

fn no_next_compact_entry_row(
    resolution: &str,
    entry_facts: &NativeCommittedEntryFacts,
    coordinate_facts: &NativeLocalAppendCompactFacts,
) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&entry_facts.hash));
    row.push(&JsValue::from_f64(entry_facts.byte_length as f64));
    row.push(&Uint8Array::from(entry_facts.meta_bytes.as_slice()));
    row.push(&number_to_row(
        resolution,
        coordinate_facts.coordinate.hash_number,
    ));
    row.push(&JsValue::from_str(&coordinate_facts.coordinate.gid));
    row.push(&numbers_to_rows(
        resolution,
        &coordinate_facts.coordinate.coordinates,
    ));
    row.push(&JsValue::from_bool(
        coordinate_facts.coordinate.assigned_to_range_boundary,
    ));
    row.push(&JsValue::from_f64(
        coordinate_facts.coordinate.requested_replicas as f64,
    ));
    row.push(&leader_samples_to_optional_rows(&coordinate_facts.leaders));
    row.push(&JsValue::from_bool(coordinate_facts.is_leader));
    row
}

fn latest_compact_entry_row(
    resolution: &str,
    entry_facts: NativeCommittedEntryFacts,
    coordinate_facts: &NativeLocalAppendCompactFacts,
    trim_hashes: Vec<String>,
    document_trimmed_heads_processed: bool,
    previous_document_context: Option<&DocumentContextFacts>,
) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&entry_facts.hash));
    row.push(&JsValue::from_f64(entry_facts.byte_length as f64));
    row.push(&Uint8Array::from(entry_facts.meta_bytes.as_slice()));
    row.push(&Uint8Array::from(entry_facts.hash_digest_bytes.as_slice()));
    row.push(&strings_to_array(entry_facts.next));
    row.push(&coordinate_plan_to_row(resolution, coordinate_facts));
    row.push(&leader_samples_to_optional_rows(&coordinate_facts.leaders));
    row.push(&JsValue::from_bool(coordinate_facts.is_leader));
    row.push(&strings_to_array(trim_hashes));
    row.push(&JsValue::from_bool(document_trimmed_heads_processed));
    row.push(
        &previous_document_context
            .map(|context| document_context_facts_to_row(context).into())
            .unwrap_or(JsValue::UNDEFINED),
    );
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

fn push_optional_trim_result(row: &Array, trim_hashes: Vec<String>, document_trimmed: bool) {
    if trim_hashes.is_empty() {
        if document_trimmed {
            row.push(&JsValue::UNDEFINED);
            row.push(&JsValue::TRUE);
        }
        return;
    }
    row.push(&strings_to_array(trim_hashes));
    if document_trimmed {
        row.push(&JsValue::TRUE);
    }
}

fn ensure_batch_append_lens(
    len: usize,
    wall_times: &BigUint64Array,
    logicals: &Uint32Array,
    gids: &Array,
    gids_label: &'static str,
    meta_datas: &Array,
    document_keys: &Array,
) -> Result<(), JsValue> {
    ensure_same_len(len, wall_times.length() as usize, "batch wall times")?;
    ensure_same_len(len, logicals.length() as usize, "batch logicals")?;
    ensure_same_len(len, gids.length() as usize, gids_label)?;
    ensure_same_len(len, meta_datas.length() as usize, "batch meta data")?;
    ensure_same_len(len, document_keys.length() as usize, "batch document keys")?;
    Ok(())
}

fn ensure_batch_projection_lens(
    len: usize,
    plan_ids: &Uint32Array,
    encoded_documents: Option<&Array>,
    signers: &Array,
) -> Result<(), JsValue> {
    ensure_same_len(
        len,
        plan_ids.length() as usize,
        "batch document projection plan ids",
    )?;
    if let Some(encoded_documents) = encoded_documents {
        ensure_same_len(
            len,
            encoded_documents.length() as usize,
            "batch document projection encoded documents",
        )?;
    }
    ensure_same_len(
        len,
        signers.length() as usize,
        "batch document projection signers",
    )?;
    Ok(())
}

fn required_projection_encoded_document(
    encoded_documents: &Array,
    index: u32,
) -> Result<JsValue, JsValue> {
    let encoded_document = encoded_documents.get(index);
    if encoded_document.is_undefined() || encoded_document.is_null() {
        return Err(JsValue::from_str(
            "Expected batch document projection encoded document",
        ));
    }
    Ok(encoded_document)
}

impl NativePeerbitBackbone {
    fn copy_append_inputs_profiled(
        &mut self,
        meta_data: JsValue,
        payload_data: &Uint8Array,
    ) -> Result<(Option<Vec<u8>>, Vec<u8>), BackboneError> {
        let input_copy_started = self.append_profile_enabled.then(crate::time::now_ms);
        let meta_data = optional_bytes_from_js(meta_data, "meta data")?;
        let payload_data = payload_data.to_vec();
        if let Some(started) = input_copy_started {
            self.append_profile.input_copy_ms += crate::time::now_ms() - started;
        }
        Ok((meta_data, payload_data))
    }

    fn hash_number_profiled(&mut self, hash_digest_bytes: &[u8]) -> Result<u64, BackboneError> {
        let hash_number_started = self.append_profile_enabled.then(crate::time::now_ms);
        let hash_number = hash_number_u64(&self.resolution, hash_digest_bytes)?;
        if let Some(started) = hash_number_started {
            self.append_profile.hash_number_ms += crate::time::now_ms() - started;
        }
        Ok(hash_number)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_no_next_log_append_profiled(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: Option<usize>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>), JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let log_started = profile_enabled.then(crate::time::now_ms);
        let mut log_profile = NativeLogAppendProfile::default();
        let result = if let Some(trim_length_to) = trim_length_to {
            self.log
                .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_hashes_borrowed(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid,
                    entry_type,
                    meta_data,
                    payload_data,
                    trim_length_to,
                    profile_enabled.then_some(&mut log_profile),
                )?
        } else {
            (
                self.log
                    .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_borrowed(
                        &self.builder,
                        &mut self.blocks,
                        wall_time,
                        logical,
                        gid,
                        entry_type,
                        meta_data,
                        payload_data,
                        profile_enabled.then_some(&mut log_profile),
                    )?,
                Vec::new(),
            )
        };
        if let Some(started) = log_started {
            self.append_profile.log_total_ms += crate::time::now_ms() - started;
            self.append_profile.add_log_profile(&log_profile);
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_latest_log_append_profiled(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: Option<usize>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>), JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let log_started = profile_enabled.then(crate::time::now_ms);
        let mut log_profile = NativeLogAppendProfile::default();
        let result = self
            .log
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes_borrowed(
                &self.builder,
                &mut self.blocks,
                wall_time,
                logical,
                gid,
                next_hashes,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                profile_enabled.then_some(&mut log_profile),
            )?;
        if let Some(started) = log_started {
            self.append_profile.log_total_ms += crate::time::now_ms() - started;
            self.append_profile.add_log_profile(&log_profile);
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_committed_log_append_rows_profiled(
        &mut self,
        wall_time: u64,
        logical: u32,
        gid: String,
        next_hashes: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: Option<usize>,
        resolve_trimmed_entries: bool,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>, Array, Array), BackboneError> {
        let profile_enabled = self.append_profile_enabled;
        let log_started = profile_enabled.then(crate::time::now_ms);
        let mut log_profile = NativeLogAppendProfile::default();
        let (entry_facts, trimmed_entries, trim_hashes) = if resolve_trimmed_entries {
            let (entry_facts, trimmed_entries) = self
                .log
                .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder(
                    &self.builder,
                    &mut self.blocks,
                    wall_time,
                    logical,
                    gid,
                    next_hashes,
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
                    gid,
                    next_hashes,
                    entry_type,
                    meta_data,
                    payload_data,
                    trim_length_to,
                    profile_enabled.then_some(&mut log_profile),
                )?;
            (entry_facts, Vec::new(), trim_hashes)
        };
        if let Some(started) = log_started {
            self.append_profile.log_total_ms += crate::time::now_ms() - started;
            self.append_profile.add_log_profile(&log_profile);
        }
        let entry_row_started = profile_enabled.then(crate::time::now_ms);
        let entry_row = committed_entry_facts_to_row(&entry_facts, !entry_facts.next.is_empty());
        if let Some(started) = entry_row_started {
            self.append_profile.entry_row_ms += crate::time::now_ms() - started;
        }
        let trim_rows_started = profile_enabled.then(crate::time::now_ms);
        let trim_rows = if resolve_trimmed_entries {
            native_backbone_trim_entries_to_rows(trimmed_entries)
        } else {
            Array::new()
        };
        if let Some(started) = trim_rows_started {
            self.append_profile.trim_rows_ms += crate::time::now_ms() - started;
        }
        Ok((entry_facts, trim_hashes, entry_row, trim_rows))
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_batch_compact_coordinates_profiled(
        &mut self,
        coordinate_inputs: Vec<NativeLocalAppendCompactInput>,
        replicas: usize,
        role_age_ms: f64,
        now: &str,
        self_hash: &str,
        self_replicating: bool,
        expected_len: usize,
        mismatch_label: &str,
    ) -> Result<Vec<NativeLocalAppendCompactFacts>, JsValue> {
        let coordinate_plan_started = self.append_profile_enabled.then(crate::time::now_ms);
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
        if let Some(started) = coordinate_plan_started {
            self.append_profile.coordinate_plan_ms += crate::time::now_ms() - started;
        }
        if coordinate_facts.len() != expected_len {
            return Err(JsValue::from_str(mismatch_label));
        }
        Ok(coordinate_facts)
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_append_document_index_profiled(
        &mut self,
        document_index_commit: Option<DocumentIndexAppendCommit>,
        wall_time: u64,
        hash: &str,
        gid: &str,
        payload_size: u32,
        plain_put_payload_data: Option<&[u8]>,
        delete_trimmed_document_heads: bool,
        trim_hashes: &[String],
    ) -> Result<bool, BackboneError> {
        let document_index_started = self.append_profile_enabled.then(crate::time::now_ms);
        self.put_document_index_for_append_with_plain_put_payload(
            document_index_commit,
            wall_time,
            hash,
            gid,
            payload_size,
            plain_put_payload_data,
        )?;
        let document_trimmed_heads_processed = delete_trimmed_document_heads
            && self.delete_documents_by_context_heads_profiled(trim_hashes);
        if let Some(started) = document_index_started {
            self.append_profile.document_index_commit_ms += crate::time::now_ms() - started;
        }
        Ok(document_trimmed_heads_processed)
    }

    fn resolve_latest_document_append_context(
        &self,
        document_index_commit: &mut DocumentIndexAppendCommit,
        fallback_gid: String,
    ) -> Result<(Option<DocumentContextFacts>, String, Vec<String>), BackboneError> {
        let previous_context = self.document_context_facts_by_key(&document_index_commit.key)?;
        let known_existing = previous_context.is_some();
        let gid = previous_context
            .as_ref()
            .map(|context| context.gid.clone())
            .unwrap_or(fallback_gid);
        let next_hashes = previous_context
            .as_ref()
            .map(|context| vec![context.head.clone()])
            .unwrap_or_default();
        if document_index_commit.existing_created.is_none() {
            document_index_commit.existing_created =
                previous_context.as_ref().map(|context| context.created);
        }
        document_index_commit.previous_context = previous_context.clone();
        document_index_commit.known_existing = known_existing;
        Ok((previous_context, gid, next_hashes))
    }

    fn put_document_index_for_facts_row(
        &mut self,
        row: &Array,
        document_index_commit: DocumentIndexAppendCommit,
        wall_time: u64,
        document_gid: &str,
        payload_size: u32,
    ) -> Result<(), BackboneError> {
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
            document_gid,
            payload_size,
        )
    }

    fn put_document_index_for_append(
        &mut self,
        document_index_commit: Option<DocumentIndexAppendCommit>,
        wall_time: u64,
        hash: &str,
        gid: &str,
        payload_size: u32,
    ) -> Result<(), BackboneError> {
        self.put_document_index_for_append_with_plain_put_payload(
            document_index_commit,
            wall_time,
            hash,
            gid,
            payload_size,
            None,
        )
    }

    fn put_document_index_for_append_with_plain_put_payload(
        &mut self,
        document_index_commit: Option<DocumentIndexAppendCommit>,
        wall_time: u64,
        hash: &str,
        gid: &str,
        payload_size: u32,
        plain_put_payload_data: Option<&[u8]>,
    ) -> Result<(), BackboneError> {
        let Some(document_index_commit) = document_index_commit else {
            return Ok(());
        };
        let prepared = self.prepare_document_index_append_put(
            document_index_commit,
            wall_time,
            hash,
            gid,
            payload_size,
            plain_put_payload_data,
        )?;
        self.commit_prepared_document_index_append_put(prepared);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_document_index_append_put(
        &mut self,
        document_index_commit: DocumentIndexAppendCommit,
        wall_time: u64,
        hash: &str,
        gid: &str,
        payload_size: u32,
        plain_put_payload_data: Option<&[u8]>,
    ) -> Result<PreparedDocumentIndexAppendPut, BackboneError> {
        let record_previous_signer = document_index_commit
            .required_previous_signer_public_key
            .is_some();
        let key = document_index_commit.key;
        let previous_head = document_index_commit
            .previous_context
            .as_ref()
            .map(|context| context.head.clone());
        let byte_element_index_limit = document_index_commit.byte_element_index_limit;
        let known_existing = document_index_commit.known_existing;
        let profile_enabled = self.append_profile_enabled;
        let context_started = profile_enabled.then(crate::time::now_ms);
        let context_suffix = encode_document_context_suffix(
            document_index_commit.existing_created.unwrap_or(wall_time),
            wall_time,
            hash,
            gid,
            payload_size,
        )?;
        if let Some(started) = context_started {
            self.append_profile.document_index_context_encode_ms += crate::time::now_ms() - started;
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
                        hash,
                        gid,
                        payload_size,
                        signer.as_deref(),
                    )?
                }
                DocumentIndexProjectionPlan::Cached(index) => {
                    let plan = self
                        .document_projection_plans
                        .get(index)
                        .ok_or(BackboneError::MissingCachedDocumentProjectionPlan)?;
                    project_document_index_simple_bytes_with_plan(
                        &encoded_document,
                        plan,
                        document_index_commit.existing_created.unwrap_or(wall_time),
                        wall_time,
                        hash,
                        gid,
                        payload_size,
                        signer.as_deref(),
                    )?
                }
            },
            DocumentIndexValuePrefix::PlainPutPayloadIdentity => plain_put_payload_data
                .map(plain_put_document_bytes_from_payload)
                .transpose()?
                .ok_or(BackboneError::MissingPlainPutPayloadForDocumentIndex)?
                .to_vec(),
            DocumentIndexValuePrefix::PlainPutPayloadProjection { plan, signer } => {
                let encoded_document = plain_put_payload_data
                    .map(plain_put_document_bytes_from_payload)
                    .transpose()?
                    .ok_or(BackboneError::MissingPlainPutPayloadForDocumentProjection)?;
                match plan {
                    DocumentIndexProjectionPlan::Inline(plan) => {
                        project_document_index_simple_bytes_with_plan(
                            encoded_document,
                            &plan,
                            document_index_commit.existing_created.unwrap_or(wall_time),
                            wall_time,
                            hash,
                            gid,
                            payload_size,
                            signer.as_deref(),
                        )?
                    }
                    DocumentIndexProjectionPlan::Cached(index) => {
                        let plan = self
                            .document_projection_plans
                            .get(index)
                            .ok_or(BackboneError::MissingCachedDocumentProjectionPlan)?;
                        project_document_index_simple_bytes_with_plan(
                            encoded_document,
                            plan,
                            document_index_commit.existing_created.unwrap_or(wall_time),
                            wall_time,
                            hash,
                            gid,
                            payload_size,
                            signer.as_deref(),
                        )?
                    }
                }
            }
        };
        let parts = self.prepare_document_encoded_parts_put(
            key,
            value_prefix_bytes,
            context_suffix,
            byte_element_index_limit,
            known_existing,
            Some(hash),
            previous_head.as_deref(),
            true,
        )?;
        Ok(PreparedDocumentIndexAppendPut {
            parts,
            previous_signer_head: record_previous_signer.then(|| hash.to_string()),
        })
    }

    fn commit_prepared_document_index_append_put(
        &mut self,
        prepared: PreparedDocumentIndexAppendPut,
    ) {
        let PreparedDocumentIndexAppendPut {
            parts,
            previous_signer_head,
        } = prepared;
        let previous_signer = previous_signer_head.map(|head| (parts.key.clone(), head));
        self.commit_prepared_document_encoded_parts_put(parts);
        if let Some((key, head)) = previous_signer {
            self.put_document_previous_signer_fact(key, head, self.local_public_key.clone(), true);
        }
    }

    fn delete_documents_by_context_heads_profiled(&mut self, heads: &[String]) -> bool {
        if heads.is_empty() {
            return false;
        }
        let started = self.append_profile_enabled.then(crate::time::now_ms);
        let deleted = self.delete_documents_by_context_heads(heads);
        if let Some(started) = started {
            self.append_profile.document_index_trim_delete_ms += crate::time::now_ms() - started;
        }
        deleted
    }

    fn delete_documents_by_context_heads(&mut self, heads: &[String]) -> bool {
        if heads.is_empty() {
            return false;
        }
        let Some(field) = self.document_context_head_field else {
            return false;
        };
        for head in heads {
            if let Some(key) = self.document_key_by_head.remove(head) {
                self.delete_document_inner(&key, true);
                continue;
            }
            if let Some(key) = self
                .document_index
                .exact_first(&FieldPath::Id(field), &FieldValue::from(head.clone()))
            {
                self.delete_document_inner(&key, true);
            }
        }
        true
    }
}
