use js_sys::{Array, Uint8Array};
use wasm_bindgen::prelude::*;

use crate::append_tx::{
    committed_entry_facts_to_row, compact_committed_entry_facts_trim_hashes_to_row,
};
use crate::documents::{
    document_context_facts_to_row, document_index_append_commit,
    document_index_cached_projection_append_commit,
    document_index_cached_projection_plain_put_payload_append_commit, DocumentIndexAppendCommit,
};
use crate::js_interop::{optional_bytes_from_js, optional_usize_from_js, strings_to_array};
use crate::NativePeerbitBackbone;

#[wasm_bindgen]
impl NativePeerbitBackbone {
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
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
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
        self.put_document_index_for_facts_row(
            &row,
            document_index_commit,
            wall_time,
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
        self.put_document_index_for_facts_row(
            &row,
            document_index_commit,
            wall_time,
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
    pub fn prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_plain_put_payload(
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
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let payload_data = payload_data.to_vec();
        let document_index_commit =
            document_index_cached_projection_plain_put_payload_append_commit(
                document_key,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan_id,
                document_projection_signer,
            )?;
        let entry_facts = self
            .log
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_borrowed(
                &self.builder,
                &mut self.blocks,
                wall_time,
                logical,
                gid,
                entry_type,
                optional_bytes_from_js(meta_data, "meta data")?,
                &payload_data,
                None,
            )?;
        self.put_document_index_for_append_with_plain_put_payload(
            Some(document_index_commit),
            wall_time,
            &entry_facts.hash,
            &document_gid,
            payload_size,
            Some(&payload_data),
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
    pub fn prepare_plain_entry_commit_latest_facts_document_index_trim_hashes(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: JsValue,
        document_key: String,
        document_value_prefix_bytes: Vec<u8>,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan: JsValue,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
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
        self.prepare_plain_entry_commit_latest_document_index_trim_hashes_inner(
            wall_time,
            logical,
            fallback_gid,
            entry_type,
            meta_data,
            payload_data,
            trim_length_to,
            document_index_commit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_plain_entry_commit_latest_facts_document_index_cached_plan_trim_hashes(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: JsValue,
        document_key: String,
        document_byte_element_index_limit: usize,
        document_delete_trimmed_heads: bool,
        document_projection_plan_id: u32,
        document_projection_encoded_document: JsValue,
        document_projection_signer: JsValue,
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
        self.prepare_plain_entry_commit_latest_document_index_trim_hashes_inner(
            wall_time,
            logical,
            fallback_gid,
            entry_type,
            meta_data,
            payload_data,
            trim_length_to,
            document_index_commit,
        )
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
    pub fn prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_hashes_plain_put_payload(
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
        document_projection_signer: JsValue,
    ) -> Result<Array, JsValue> {
        let document_gid = gid.clone();
        let payload_size = payload_data.length();
        let payload_data = payload_data.to_vec();
        let document_index_commit =
            document_index_cached_projection_plain_put_payload_append_commit(
                document_key,
                document_existing_created,
                document_byte_element_index_limit,
                document_delete_trimmed_heads,
                document_projection_plan_id,
                document_projection_signer,
            )?;
        let delete_trimmed_document_heads = document_index_commit.delete_trimmed_heads;
        let (entry_facts, trim_hashes) = self
            .log
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_hashes_borrowed(
                &self.builder,
                &mut self.blocks,
                wall_time,
                logical,
                gid,
                entry_type,
                optional_bytes_from_js(meta_data, "meta data")?,
                &payload_data,
                trim_length_to,
                None,
            )?;
        self.put_document_index_for_append_with_plain_put_payload(
            Some(document_index_commit),
            wall_time,
            &entry_facts.hash,
            &document_gid,
            payload_size,
            Some(&payload_data),
        )?;
        let document_trimmed_heads_processed = delete_trimmed_document_heads
            && self.delete_documents_by_context_heads_profiled(&trim_hashes);
        Ok(compact_committed_entry_facts_trim_hashes_to_row(
            &entry_facts,
            trim_hashes,
            document_trimmed_heads_processed,
        ))
    }
}

impl NativePeerbitBackbone {
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
                optional_bytes_from_js(meta_data, "meta data")?,
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
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_hashes(
                &self.builder,
                &mut self.blocks,
                wall_time,
                logical,
                gid,
                entry_type,
                optional_bytes_from_js(meta_data, "meta data")?,
                payload_data.to_vec(),
                trim_length_to,
                None,
            )?;
        self.put_document_index_for_append(
            Some(document_index_commit),
            wall_time,
            &entry_facts.hash,
            &document_gid,
            payload_size,
        )?;
        let document_trimmed_heads_processed = delete_trimmed_document_heads
            && self.delete_documents_by_context_heads_profiled(&trim_hashes);
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
    fn prepare_plain_entry_commit_latest_document_index_trim_hashes_inner(
        &mut self,
        wall_time: u64,
        logical: u32,
        fallback_gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: JsValue,
        mut document_index_commit: DocumentIndexAppendCommit,
    ) -> Result<Array, JsValue> {
        let trim_length_to = optional_usize_from_js(trim_length_to, "trimLengthTo")?;
        let (previous_context, gid, next_hashes) =
            self.resolve_latest_document_append_context(&mut document_index_commit, fallback_gid)?;
        let delete_trimmed_document_heads = document_index_commit.delete_trimmed_heads;
        let payload_size = payload_data.length();
        let (entry_facts, trim_hashes) = self
            .log
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes(
                &self.builder,
                &mut self.blocks,
                wall_time,
                logical,
                gid.clone(),
                next_hashes,
                entry_type,
                optional_bytes_from_js(meta_data, "meta data")?,
                payload_data.to_vec(),
                trim_length_to,
                None,
            )?;
        self.put_document_index_for_append(
            Some(document_index_commit),
            wall_time,
            &entry_facts.hash,
            &gid,
            payload_size,
        )?;
        let document_trimmed_heads_processed = delete_trimmed_document_heads
            && self.delete_documents_by_context_heads_profiled(&trim_hashes);
        let out = Array::new();
        out.push(&committed_entry_facts_to_row(
            &entry_facts,
            !entry_facts.next.is_empty(),
        ));
        out.push(&strings_to_array(trim_hashes));
        out.push(&JsValue::from_bool(document_trimmed_heads_processed));
        out.push(
            &previous_context
                .as_ref()
                .map(|context| document_context_facts_to_row(context).into())
                .unwrap_or(JsValue::UNDEFINED),
        );
        Ok(out)
    }
}
