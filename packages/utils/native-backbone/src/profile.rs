use js_sys::Array;
use peerbit_log_rust::{NativeLogAppendProfile, RawEntryV0PrepareProfile};
use wasm_bindgen::prelude::*;

#[derive(Clone, Default)]
pub(crate) struct NativeBackboneAppendProfile {
    pub(crate) storage_append_inner_ms: f64,
    pub(crate) input_copy_ms: f64,
    pub(crate) log_total_ms: f64,
    pub(crate) log_next_clone_ms: f64,
    pub(crate) log_entry_core_ms: f64,
    pub(crate) log_encode_meta_ms: f64,
    pub(crate) log_encode_payload_ms: f64,
    pub(crate) log_encode_signable_ms: f64,
    pub(crate) log_sign_ms: f64,
    pub(crate) log_encode_signature_ms: f64,
    pub(crate) log_encode_storage_ms: f64,
    pub(crate) log_cid_ms: f64,
    pub(crate) log_cid_hash_ms: f64,
    pub(crate) log_cid_string_ms: f64,
    pub(crate) log_index_entry_ms: f64,
    pub(crate) log_facts_ms: f64,
    pub(crate) log_block_put_ms: f64,
    pub(crate) log_graph_put_ms: f64,
    pub(crate) log_trim_ms: f64,
    pub(crate) entry_row_ms: f64,
    pub(crate) trim_rows_ms: f64,
    pub(crate) hash_number_ms: f64,
    pub(crate) coordinate_plan_ms: f64,
    pub(crate) coordinate_core_ms: f64,
    pub(crate) coordinate_fields_build_ms: f64,
    pub(crate) coordinate_value_encode_ms: f64,
    pub(crate) coordinate_journal_put_ms: f64,
    pub(crate) coordinate_index_put_ms: f64,
    pub(crate) coordinate_value_put_ms: f64,
    pub(crate) coordinate_delete_ms: f64,
    pub(crate) document_index_commit_ms: f64,
    pub(crate) document_index_context_encode_ms: f64,
    pub(crate) document_index_extract_ms: f64,
    pub(crate) document_index_value_build_ms: f64,
    pub(crate) document_index_put_ms: f64,
    pub(crate) document_value_put_ms: f64,
    pub(crate) document_index_trim_delete_ms: f64,
    pub(crate) result_row_ms: f64,
    pub(crate) raw_receive_input_copy_ms: f64,
    pub(crate) raw_receive_prepare_ms: f64,
    pub(crate) raw_receive_digest_ms: f64,
    pub(crate) raw_receive_cid_string_ms: f64,
    pub(crate) raw_receive_expected_cid_ms: f64,
    pub(crate) raw_receive_storage_parse_ms: f64,
    pub(crate) raw_receive_meta_parse_ms: f64,
    pub(crate) raw_receive_payload_parse_ms: f64,
    pub(crate) raw_receive_signature_parse_ms: f64,
    pub(crate) raw_receive_signable_ms: f64,
    pub(crate) raw_receive_verify_batch_ms: f64,
    pub(crate) raw_receive_verify_fallback_ms: f64,
    pub(crate) raw_receive_prepare_columns_ms: f64,
    pub(crate) raw_receive_pending_check_ms: f64,
    pub(crate) raw_receive_verify_ms: f64,
    pub(crate) raw_receive_verify_status_ms: f64,
    pub(crate) raw_receive_join_plan_ms: f64,
    pub(crate) raw_receive_remove_ms: f64,
    pub(crate) raw_receive_block_put_ms: f64,
    pub(crate) raw_receive_graph_put_ms: f64,
    pub(crate) raw_receive_coordinate_commit_ms: f64,
}

impl NativeBackboneAppendProfile {
    pub(crate) fn add_log_profile(&mut self, profile: &NativeLogAppendProfile) {
        self.log_next_clone_ms += profile.next_clone_ms;
        self.log_entry_core_ms += profile.entry_core_ms;
        self.log_encode_meta_ms += profile.encode_meta_ms;
        self.log_encode_payload_ms += profile.encode_payload_ms;
        self.log_encode_signable_ms += profile.encode_signable_ms;
        self.log_sign_ms += profile.sign_ms;
        self.log_encode_signature_ms += profile.encode_signature_ms;
        self.log_encode_storage_ms += profile.encode_storage_ms;
        self.log_cid_ms += profile.cid_ms;
        self.log_cid_hash_ms += profile.cid_hash_ms;
        self.log_cid_string_ms += profile.cid_string_ms;
        self.log_index_entry_ms += profile.index_entry_ms;
        self.log_facts_ms += profile.facts_ms;
        self.log_block_put_ms += profile.block_put_ms;
        self.log_graph_put_ms += profile.graph_put_ms;
        self.log_trim_ms += profile.trim_ms;
    }

    pub(crate) fn add_raw_prepare_profile(&mut self, profile: &RawEntryV0PrepareProfile) {
        self.raw_receive_digest_ms += profile.digest_ms;
        self.raw_receive_cid_string_ms += profile.cid_string_ms;
        self.raw_receive_expected_cid_ms += profile.expected_cid_ms;
        self.raw_receive_storage_parse_ms += profile.storage_parse_ms;
        self.raw_receive_meta_parse_ms += profile.meta_parse_ms;
        self.raw_receive_payload_parse_ms += profile.payload_parse_ms;
        self.raw_receive_signature_parse_ms += profile.signature_parse_ms;
        self.raw_receive_signable_ms += profile.signable_ms;
        self.raw_receive_verify_batch_ms += profile.verify_batch_ms;
        self.raw_receive_verify_fallback_ms += profile.verify_fallback_ms;
    }

    pub(crate) fn to_row(&self) -> Array {
        let row = Array::new();
        row.push(&JsValue::from_f64(self.storage_append_inner_ms));
        row.push(&JsValue::from_f64(self.input_copy_ms));
        row.push(&JsValue::from_f64(self.log_total_ms));
        row.push(&JsValue::from_f64(self.log_next_clone_ms));
        row.push(&JsValue::from_f64(self.log_entry_core_ms));
        row.push(&JsValue::from_f64(self.log_encode_meta_ms));
        row.push(&JsValue::from_f64(self.log_encode_payload_ms));
        row.push(&JsValue::from_f64(self.log_encode_signable_ms));
        row.push(&JsValue::from_f64(self.log_sign_ms));
        row.push(&JsValue::from_f64(self.log_encode_signature_ms));
        row.push(&JsValue::from_f64(self.log_encode_storage_ms));
        row.push(&JsValue::from_f64(self.log_cid_ms));
        row.push(&JsValue::from_f64(self.log_cid_hash_ms));
        row.push(&JsValue::from_f64(self.log_cid_string_ms));
        row.push(&JsValue::from_f64(self.log_index_entry_ms));
        row.push(&JsValue::from_f64(self.log_facts_ms));
        row.push(&JsValue::from_f64(self.log_block_put_ms));
        row.push(&JsValue::from_f64(self.log_graph_put_ms));
        row.push(&JsValue::from_f64(self.log_trim_ms));
        row.push(&JsValue::from_f64(self.entry_row_ms));
        row.push(&JsValue::from_f64(self.trim_rows_ms));
        row.push(&JsValue::from_f64(self.hash_number_ms));
        row.push(&JsValue::from_f64(self.coordinate_plan_ms));
        row.push(&JsValue::from_f64(self.coordinate_core_ms));
        row.push(&JsValue::from_f64(self.coordinate_fields_build_ms));
        row.push(&JsValue::from_f64(self.coordinate_value_encode_ms));
        row.push(&JsValue::from_f64(self.coordinate_journal_put_ms));
        row.push(&JsValue::from_f64(self.coordinate_index_put_ms));
        row.push(&JsValue::from_f64(self.coordinate_value_put_ms));
        row.push(&JsValue::from_f64(self.coordinate_delete_ms));
        row.push(&JsValue::from_f64(self.document_index_commit_ms));
        row.push(&JsValue::from_f64(self.document_index_context_encode_ms));
        row.push(&JsValue::from_f64(self.document_index_extract_ms));
        row.push(&JsValue::from_f64(self.document_index_value_build_ms));
        row.push(&JsValue::from_f64(self.document_index_put_ms));
        row.push(&JsValue::from_f64(self.document_value_put_ms));
        row.push(&JsValue::from_f64(self.document_index_trim_delete_ms));
        row.push(&JsValue::from_f64(self.result_row_ms));
        row.push(&JsValue::from_f64(self.raw_receive_input_copy_ms));
        row.push(&JsValue::from_f64(self.raw_receive_prepare_ms));
        row.push(&JsValue::from_f64(self.raw_receive_digest_ms));
        row.push(&JsValue::from_f64(self.raw_receive_cid_string_ms));
        row.push(&JsValue::from_f64(self.raw_receive_expected_cid_ms));
        row.push(&JsValue::from_f64(self.raw_receive_storage_parse_ms));
        row.push(&JsValue::from_f64(self.raw_receive_meta_parse_ms));
        row.push(&JsValue::from_f64(self.raw_receive_payload_parse_ms));
        row.push(&JsValue::from_f64(self.raw_receive_signature_parse_ms));
        row.push(&JsValue::from_f64(self.raw_receive_signable_ms));
        row.push(&JsValue::from_f64(self.raw_receive_verify_batch_ms));
        row.push(&JsValue::from_f64(self.raw_receive_verify_fallback_ms));
        row.push(&JsValue::from_f64(self.raw_receive_prepare_columns_ms));
        row.push(&JsValue::from_f64(self.raw_receive_pending_check_ms));
        row.push(&JsValue::from_f64(self.raw_receive_verify_ms));
        row.push(&JsValue::from_f64(self.raw_receive_verify_status_ms));
        row.push(&JsValue::from_f64(self.raw_receive_join_plan_ms));
        row.push(&JsValue::from_f64(self.raw_receive_remove_ms));
        row.push(&JsValue::from_f64(self.raw_receive_block_put_ms));
        row.push(&JsValue::from_f64(self.raw_receive_graph_put_ms));
        row.push(&JsValue::from_f64(self.raw_receive_coordinate_commit_ms));
        row
    }
}
