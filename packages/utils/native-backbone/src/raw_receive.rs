use js_sys::{Array, BigUint64Array, Uint32Array, Uint8Array};
use peerbit_log_rust::{
    prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled,
    verify_prepared_entry_v0_ed25519_storage_slices,
    verify_prepared_entry_v0_ed25519_storage_slices_all, LogIndexEntry,
    PreparedEntryV0SignatureInput, PreparedRawEntryV0, RawEntryV0PrepareProfile,
};
use peerbit_shared_log_rust::{EntryCoordinateCommit, GidLeaderPlan};
use std::collections::{HashMap, HashSet};
use wasm_bindgen::prelude::*;

use crate::js_interop::{
    bytes_vec_from_array, ensure_same_len, hash_number_u64, numbers_to_rows, strings_from_array,
    strings_slice_to_array, strings_to_array,
};
use crate::shared_log_plan::{
    clamp_replicas_u32, coordinate_commits_from_string_columns,
    coordinate_commits_from_u64_columns, leader_samples_to_rows,
};
use crate::NativePeerbitBackbone;

pub(crate) struct PendingRawReceiveEntry {
    storage_bytes: Vec<u8>,
    entry: LogIndexEntry,
    requested_replicas: Option<u32>,
    signature_verified: bool,
    signable_prefix_len: usize,
    signature_with_key_start: usize,
    signature_with_key_len: usize,
}

impl PendingRawReceiveEntry {
    fn signature_input(&self) -> PreparedEntryV0SignatureInput<'_> {
        PreparedEntryV0SignatureInput {
            storage_bytes: &self.storage_bytes,
            signable_prefix_len: self.signable_prefix_len,
            signature_with_key_start: self.signature_with_key_start,
            signature_with_key_len: self.signature_with_key_len,
        }
    }
}

struct PendingRawReceiveGroupPlan {
    gid: String,
    hashes: Vec<String>,
    indexes: Vec<u32>,
    requested_replicas: Vec<u32>,
    latest_hash: String,
    latest_index: u32,
    latest_wall_time: u64,
    latest_logical: u32,
    max_requested_replicas: u32,
}

struct ResolvedPendingRawReceiveGroupPlan {
    gid: String,
    hashes: Vec<String>,
    indexes: Vec<u32>,
    requested_replicas: Vec<u32>,
    latest_hash: String,
    latest_index: u32,
    max_replicas_from_head: u32,
    max_replicas_from_new_entries: u32,
    max_max_replicas: u32,
}

fn leader_plan_contains_hash(plan: &GidLeaderPlan, hash: &str) -> bool {
    plan.leaders.iter().any(|leader| leader.hash == hash)
}

fn prepared_raw_receive_selection_all_drop(
    hashes: Vec<String>,
    group_count: usize,
    planned_hash_count: usize,
    used_leader_sample_plans: bool,
) -> JsValue {
    let retained_indexes: Vec<u32> = Vec::new();
    let dropped_indexes: Vec<u32> = (0..hashes.len() as u32).collect();
    let out = Array::new();
    out.push(&strings_to_array(Vec::new()));
    out.push(&strings_to_array(hashes));
    out.push(&JsValue::from_f64(group_count as f64));
    out.push(&JsValue::from_f64(planned_hash_count as f64));
    out.push(&JsValue::TRUE);
    out.push(&JsValue::from_bool(used_leader_sample_plans));
    out.push(&JsValue::UNDEFINED);
    out.push(&Uint32Array::from(retained_indexes.as_slice()));
    out.push(&Uint32Array::from(dropped_indexes.as_slice()));
    out.into()
}

fn prepared_raw_receive_selection_from_leader_plans(
    resolution: &str,
    groups: &[ResolvedPendingRawReceiveGroupPlan],
    expected_hash_count: usize,
    planned_hash_count: usize,
    leader_plans: &[GidLeaderPlan],
    self_hash: &str,
    used_leader_sample_plans: bool,
) -> Result<JsValue, JsValue> {
    if leader_plans.len() != groups.len() {
        return Ok(JsValue::UNDEFINED);
    }

    let mut retained_hashes = Vec::new();
    let mut dropped_hashes = Vec::new();
    let mut retained_groups = vec![false; groups.len()];
    let mut retained_original_indexes = vec![false; expected_hash_count];
    for (index, (group, leader_plan)) in groups.iter().zip(leader_plans).enumerate() {
        if leader_plan_contains_hash(leader_plan, self_hash) {
            retained_groups[index] = true;
            retained_hashes.extend(group.hashes.iter().cloned());
            for original_index in &group.indexes {
                let original_index = *original_index as usize;
                if original_index >= retained_original_indexes.len() {
                    return Ok(JsValue::UNDEFINED);
                }
                retained_original_indexes[original_index] = true;
            }
        } else {
            dropped_hashes.extend(group.hashes.iter().cloned());
        }
    }

    let mut retained_indexes = Vec::new();
    let mut dropped_indexes = Vec::new();
    for (original_index, retained) in retained_original_indexes.iter().enumerate() {
        let original_index = u32::try_from(original_index)
            .map_err(|_| JsValue::from_str("Raw receive original index overflow"))?;
        if *retained {
            retained_indexes.push(original_index);
        } else {
            dropped_indexes.push(original_index);
        }
    }

    let used_native_fast_drop_plan = retained_hashes.is_empty();
    let retained_group_leader_plans = Array::new();
    if !used_native_fast_drop_plan {
        let mut selected_index_by_original = vec![None; expected_hash_count];
        let mut selected_index = 0u32;
        for (original_index, retained) in retained_original_indexes.iter().enumerate() {
            if *retained {
                selected_index_by_original[original_index] = Some(selected_index);
                selected_index = selected_index
                    .checked_add(1)
                    .ok_or_else(|| JsValue::from_str("Raw receive selected index overflow"))?;
            }
        }
        for (index, (group, leader_plan)) in groups.iter().zip(leader_plans).enumerate() {
            if !retained_groups[index] {
                continue;
            }
            let mut selected_indexes = Vec::with_capacity(group.indexes.len());
            for original_index in &group.indexes {
                let Some(selected_index) = selected_index_by_original
                    .get(*original_index as usize)
                    .and_then(|value| *value)
                else {
                    return Ok(JsValue::UNDEFINED);
                };
                selected_indexes.push(selected_index);
            }
            let Some(selected_latest_index) = selected_index_by_original
                .get(group.latest_index as usize)
                .and_then(|value| *value)
            else {
                return Ok(JsValue::UNDEFINED);
            };
            let row = Array::new();
            row.push(&JsValue::from_str(&group.gid));
            row.push(&Uint32Array::from(selected_indexes.as_slice()));
            row.push(&Uint32Array::from(group.requested_replicas.as_slice()));
            row.push(&JsValue::from_f64(selected_latest_index as f64));
            row.push(&JsValue::from_f64(group.max_replicas_from_head as f64));
            row.push(&JsValue::from_f64(
                group.max_replicas_from_new_entries as f64,
            ));
            row.push(&JsValue::from_f64(group.max_max_replicas as f64));
            row.push(&numbers_to_rows(resolution, &leader_plan.coordinates));
            row.push(&leader_samples_to_rows(&leader_plan.leaders));
            retained_group_leader_plans.push(&row);
        }
    }
    let out = Array::new();
    out.push(&strings_to_array(retained_hashes));
    out.push(&strings_to_array(dropped_hashes));
    out.push(&JsValue::from_f64(groups.len() as f64));
    out.push(&JsValue::from_f64(planned_hash_count as f64));
    out.push(&JsValue::from_bool(used_native_fast_drop_plan));
    out.push(&JsValue::from_bool(used_leader_sample_plans));
    if retained_group_leader_plans.length() > 0 {
        out.push(&retained_group_leader_plans);
    } else {
        out.push(&JsValue::UNDEFINED);
    }
    out.push(&Uint32Array::from(retained_indexes.as_slice()));
    out.push(&Uint32Array::from(dropped_indexes.as_slice()));
    Ok(out.into())
}

fn prepared_raw_entry_v0_to_row(entry: &PreparedRawEntryV0, hash_number: Option<u64>) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&entry.cid));
    row.push(&Uint8Array::from(entry.hash_digest_bytes.as_slice()));
    row.push(&JsValue::from_f64(entry.byte_length as f64));
    row.push(&Uint8Array::from(entry.clock_id.as_slice()));
    row.push(&JsValue::from_str(&entry.wall_time.to_string()));
    row.push(&JsValue::from_f64(entry.logical as f64));
    row.push(&JsValue::from_str(&entry.gid));
    row.push(&strings_to_array(entry.next.clone()));
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
    match hash_number {
        Some(value) => row.push(&JsValue::from_str(&value.to_string())),
        None => row.push(&JsValue::UNDEFINED),
    };
    row
}

#[wasm_bindgen]
impl NativePeerbitBackbone {
    pub fn prepare_raw_receive_batch(&mut self, blocks: Array) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let input_started = profile_enabled.then(js_sys::Date::now);
        let blocks = bytes_vec_from_array(blocks)?;
        if let Some(started) = input_started {
            self.append_profile.raw_receive_input_copy_ms += js_sys::Date::now() - started;
        }
        let prepared = self.prepare_raw_receive_entries(blocks, None, true)?;
        let out = Array::new();
        for entry in prepared {
            let hash_number = hash_number_u64(&self.resolution, &entry.hash_digest_bytes)?;
            let row = prepared_raw_entry_v0_to_row(&entry, Some(hash_number));
            let log_entry = entry.log_index_entry(true)?;
            self.pending_raw_receive_entries.insert(
                entry.cid.clone(),
                PendingRawReceiveEntry {
                    storage_bytes: entry.storage_bytes,
                    entry: log_entry,
                    requested_replicas: entry.requested_replicas,
                    signature_verified: entry.signature_verified,
                    signable_prefix_len: entry.signable_prefix_len,
                    signature_with_key_start: entry.signature_with_key_start,
                    signature_with_key_len: entry.signature_with_key_len,
                },
            );
            out.push(&row);
        }
        Ok(out)
    }

    pub fn prepare_raw_receive_columns_batch(&mut self, blocks: Array) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let input_started = profile_enabled.then(js_sys::Date::now);
        let blocks = bytes_vec_from_array(blocks)?;
        if let Some(started) = input_started {
            self.append_profile.raw_receive_input_copy_ms += js_sys::Date::now() - started;
        }
        let prepared = self.prepare_raw_receive_entries(blocks, None, true)?;
        self.prepare_raw_receive_columns_from_entries(prepared, true, true)
    }

    pub fn prepare_raw_receive_unverified_columns_batch(
        &mut self,
        blocks: Array,
    ) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let input_started = profile_enabled.then(js_sys::Date::now);
        let blocks = bytes_vec_from_array(blocks)?;
        if let Some(started) = input_started {
            self.append_profile.raw_receive_input_copy_ms += js_sys::Date::now() - started;
        }
        let prepared = self.prepare_raw_receive_entries(blocks, None, false)?;
        self.prepare_raw_receive_columns_from_entries(prepared, true, true)
    }

    pub fn prepare_raw_receive_expected_columns_batch(
        &mut self,
        blocks: Array,
        hashes: Array,
    ) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let input_started = profile_enabled.then(js_sys::Date::now);
        let blocks = bytes_vec_from_array(blocks)?;
        let hashes = strings_from_array(hashes)?;
        if let Some(started) = input_started {
            self.append_profile.raw_receive_input_copy_ms += js_sys::Date::now() - started;
        }
        let prepared = self.prepare_raw_receive_entries(blocks, Some(hashes), true)?;
        self.prepare_raw_receive_columns_from_entries(prepared, true, true)
    }

    pub fn prepare_raw_receive_unverified_expected_columns_batch(
        &mut self,
        blocks: Array,
        hashes: Array,
    ) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let input_started = profile_enabled.then(js_sys::Date::now);
        let blocks = bytes_vec_from_array(blocks)?;
        let hashes = strings_from_array(hashes)?;
        if let Some(started) = input_started {
            self.append_profile.raw_receive_input_copy_ms += js_sys::Date::now() - started;
        }
        let prepared = self.prepare_raw_receive_entries(blocks, Some(hashes), false)?;
        self.prepare_raw_receive_columns_from_entries(prepared, true, true)
    }

    pub fn prepare_raw_receive_expected_compact_columns_batch(
        &mut self,
        blocks: Array,
        hashes: Array,
    ) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let input_started = profile_enabled.then(js_sys::Date::now);
        let blocks = bytes_vec_from_array(blocks)?;
        let hashes = strings_from_array(hashes)?;
        if let Some(started) = input_started {
            self.append_profile.raw_receive_input_copy_ms += js_sys::Date::now() - started;
        }
        let prepared = self.prepare_raw_receive_entries(blocks, Some(hashes), true)?;
        self.prepare_raw_receive_columns_from_entries(prepared, false, false)
    }

    pub fn prepare_raw_receive_unverified_expected_compact_columns_batch(
        &mut self,
        blocks: Array,
        hashes: Array,
    ) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let input_started = profile_enabled.then(js_sys::Date::now);
        let blocks = bytes_vec_from_array(blocks)?;
        let hashes = strings_from_array(hashes)?;
        if let Some(started) = input_started {
            self.append_profile.raw_receive_input_copy_ms += js_sys::Date::now() - started;
        }
        let prepared = self.prepare_raw_receive_entries(blocks, Some(hashes), false)?;
        self.prepare_raw_receive_columns_from_entries(prepared, false, false)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_raw_receive_unverified_expected_compact_columns_and_selection_batch(
        &mut self,
        blocks: Array,
        hashes: Array,
        min_replicas: u32,
        max_replicas: JsValue,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
        _from_hash: String,
    ) -> Result<JsValue, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let input_started = profile_enabled.then(js_sys::Date::now);
        let blocks = bytes_vec_from_array(blocks)?;
        let hashes = strings_from_array(hashes)?;
        if let Some(started) = input_started {
            self.append_profile.raw_receive_input_copy_ms += js_sys::Date::now() - started;
        }
        let prepared = self.prepare_raw_receive_entries(blocks, Some(hashes.clone()), false)?;
        let columns = self.prepare_raw_receive_columns_from_entries(prepared, false, false)?;
        let selection = self.plan_prepared_raw_receive_selection_core(
            hashes,
            min_replicas,
            max_replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        let out = Array::new();
        out.push(&columns);
        out.push(&selection);
        Ok(out.into())
    }

    pub fn plan_prepared_raw_receive_groups(
        &self,
        hashes: Array,
        min_replicas: u32,
        max_replicas: JsValue,
    ) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let Some(groups) =
            self.prepared_raw_receive_group_plans(hashes, min_replicas, max_replicas)?
        else {
            return Ok(JsValue::UNDEFINED);
        };

        let out = Array::new();
        for group in groups {
            let row = Array::new();
            row.push(&JsValue::from_str(&group.gid));
            row.push(&strings_to_array(group.hashes));
            row.push(&Uint32Array::from(group.requested_replicas.as_slice()));
            row.push(&JsValue::from_str(&group.latest_hash));
            row.push(&JsValue::from_f64(group.max_replicas_from_head as f64));
            row.push(&JsValue::from_f64(
                group.max_replicas_from_new_entries as f64,
            ));
            row.push(&JsValue::from_f64(group.max_max_replicas as f64));
            out.push(&row);
        }

        Ok(out.into())
    }

    pub fn plan_prepared_raw_receive_group_indexes(
        &self,
        hashes: Array,
        min_replicas: u32,
        max_replicas: JsValue,
    ) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let Some(groups) =
            self.prepared_raw_receive_group_plans(hashes, min_replicas, max_replicas)?
        else {
            return Ok(JsValue::UNDEFINED);
        };

        let out = Array::new();
        for group in groups {
            let row = Array::new();
            row.push(&JsValue::from_str(&group.gid));
            row.push(&Uint32Array::from(group.indexes.as_slice()));
            row.push(&Uint32Array::from(group.requested_replicas.as_slice()));
            row.push(&JsValue::from_f64(group.latest_index as f64));
            row.push(&JsValue::from_f64(group.max_replicas_from_head as f64));
            row.push(&JsValue::from_f64(
                group.max_replicas_from_new_entries as f64,
            ));
            row.push(&JsValue::from_f64(group.max_max_replicas as f64));
            out.push(&row);
        }

        Ok(out.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_prepared_raw_receive_group_leaders(
        &self,
        hashes: Array,
        min_replicas: u32,
        max_replicas: JsValue,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let Some(groups) =
            self.prepared_raw_receive_group_plans(hashes, min_replicas, max_replicas)?
        else {
            return Ok(JsValue::UNDEFINED);
        };
        let gids = Array::new();
        let replica_counts = Array::new();
        for group in &groups {
            gids.push(&JsValue::from_str(&group.gid));
            replica_counts.push(&JsValue::from_f64(group.max_max_replicas as f64));
        }
        let leader_plans = self.shared_log.plan_leaders_for_gids_batch(
            gids,
            replica_counts,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        if leader_plans.length() as usize != groups.len() {
            return Ok(JsValue::UNDEFINED);
        }

        let out = Array::new();
        for (index, group) in groups.into_iter().enumerate() {
            let leader_plan = Array::from(&leader_plans.get(index as u32));
            if leader_plan.length() < 2 {
                return Ok(JsValue::UNDEFINED);
            }
            let row = Array::new();
            row.push(&JsValue::from_str(&group.gid));
            row.push(&Uint32Array::from(group.indexes.as_slice()));
            row.push(&Uint32Array::from(group.requested_replicas.as_slice()));
            row.push(&JsValue::from_f64(group.latest_index as f64));
            row.push(&JsValue::from_f64(group.max_replicas_from_head as f64));
            row.push(&JsValue::from_f64(
                group.max_replicas_from_new_entries as f64,
            ));
            row.push(&JsValue::from_f64(group.max_max_replicas as f64));
            row.push(&leader_plan.get(0));
            row.push(&leader_plan.get(1));
            out.push(&row);
        }

        Ok(out.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_prepared_raw_receive_group_assignments(
        &self,
        hashes: Array,
        min_replicas: u32,
        max_replicas: JsValue,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
        from_hash: String,
    ) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let Some(groups) =
            self.prepared_raw_receive_group_plans(hashes, min_replicas, max_replicas)?
        else {
            return Ok(JsValue::UNDEFINED);
        };
        let gids: Vec<String> = groups.iter().map(|group| group.gid.clone()).collect();
        let replica_counts: Vec<usize> = groups
            .iter()
            .map(|group| group.max_max_replicas as usize)
            .collect();
        let assignments = self
            .shared_log
            .plan_leader_assignments_for_gids_batch_core(
                &gids,
                &replica_counts,
                role_age_ms,
                now,
                peer_filter,
                expand_peer_filter,
                &self_hash,
                include_self,
                full_replica_fallback,
                include_strict_full_replica,
                &from_hash,
            )?;
        if assignments.len() != groups.len() {
            return Ok(JsValue::UNDEFINED);
        }

        let out = Array::new();
        for (group, assignment) in groups.into_iter().zip(assignments.into_iter()) {
            let row = Array::new();
            row.push(&JsValue::from_str(&group.gid));
            row.push(&Uint32Array::from(group.indexes.as_slice()));
            row.push(&Uint32Array::from(group.requested_replicas.as_slice()));
            row.push(&JsValue::from_f64(group.latest_index as f64));
            row.push(&JsValue::from_f64(group.max_replicas_from_head as f64));
            row.push(&JsValue::from_f64(
                group.max_replicas_from_new_entries as f64,
            ));
            row.push(&JsValue::from_f64(group.max_max_replicas as f64));
            row.push(&numbers_to_rows(&self.resolution, &assignment.coordinates));
            row.push(&JsValue::from_bool(assignment.is_self_leader));
            row.push(&JsValue::from_bool(assignment.from_is_leader));
            row.push(&JsValue::from_bool(assignment.assigned_to_range_boundary));
            out.push(&row);
        }

        Ok(out.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_prepared_raw_receive_selection(
        &self,
        hashes: Array,
        min_replicas: u32,
        max_replicas: JsValue,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
        _from_hash: String,
    ) -> Result<JsValue, JsValue> {
        self.plan_prepared_raw_receive_selection_core(
            strings_from_array(hashes)?,
            min_replicas,
            max_replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_prepared_raw_receive_fast_drop(
        &self,
        hashes: Array,
        min_replicas: u32,
        max_replicas: JsValue,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
        _from_hash: String,
    ) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let expected_hash_count = hashes.len();
        if expected_hash_count == 0 {
            return Ok(JsValue::UNDEFINED);
        }
        let Some(groups) =
            self.prepared_raw_receive_group_plans(hashes, min_replicas, max_replicas)?
        else {
            return Ok(JsValue::UNDEFINED);
        };
        let mut planned_hash_count = 0usize;
        let gids = Array::new();
        let replica_counts = Array::new();
        for group in &groups {
            planned_hash_count += group.hashes.len();
            gids.push(&JsValue::from_str(&group.gid));
            replica_counts.push(&JsValue::from_f64(group.max_max_replicas as f64));
        }
        if planned_hash_count != expected_hash_count {
            return Ok(JsValue::UNDEFINED);
        }

        let leader_rows = self.shared_log.plan_leader_samples_for_gids_batch(
            gids,
            replica_counts,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash.clone(),
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        if leader_rows.length() as usize != groups.len() {
            return Ok(JsValue::UNDEFINED);
        }
        for group_leaders in leader_rows.iter() {
            let rows = Array::from(&group_leaders);
            for row_value in rows.iter() {
                let row = Array::from(&row_value);
                let Some(leader_hash) = row.get(0).as_string() else {
                    return Ok(JsValue::UNDEFINED);
                };
                if leader_hash == self_hash {
                    let out = Array::new();
                    out.push(&JsValue::FALSE);
                    out.push(&JsValue::from_f64(groups.len() as f64));
                    out.push(&JsValue::from_f64(planned_hash_count as f64));
                    return Ok(out.into());
                }
            }
        }

        let out = Array::new();
        out.push(&JsValue::TRUE);
        out.push(&JsValue::from_f64(groups.len() as f64));
        out.push(&JsValue::from_f64(planned_hash_count as f64));
        Ok(out.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn select_prepared_raw_receive_hashes(
        &self,
        hashes: Array,
        min_replicas: u32,
        max_replicas: JsValue,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
        _from_hash: String,
    ) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let expected_hash_count = hashes.len();
        if expected_hash_count == 0 {
            return Ok(JsValue::UNDEFINED);
        }
        let Some(groups) =
            self.prepared_raw_receive_group_plans(hashes, min_replicas, max_replicas)?
        else {
            return Ok(JsValue::UNDEFINED);
        };
        let mut planned_hash_count = 0usize;
        let mut gids = Vec::with_capacity(groups.len());
        let mut replica_counts = Vec::with_capacity(groups.len());
        for group in &groups {
            planned_hash_count += group.hashes.len();
            gids.push(group.gid.clone());
            replica_counts.push(group.max_max_replicas as usize);
        }
        if planned_hash_count != expected_hash_count {
            return Ok(JsValue::UNDEFINED);
        }

        let leader_plans = self.shared_log.plan_leaders_for_gids_batch_core(
            &gids,
            &replica_counts,
            role_age_ms,
            &now,
            peer_filter,
            expand_peer_filter,
            &self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        if leader_plans.len() != groups.len() {
            return Ok(JsValue::UNDEFINED);
        }

        prepared_raw_receive_selection_from_leader_plans(
            &self.resolution,
            &groups,
            expected_hash_count,
            planned_hash_count,
            &leader_plans,
            &self_hash,
            false,
        )
    }

    pub fn verify_prepared_raw_receive_entries(
        &mut self,
        hashes: Array,
    ) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let Some(out) = self.verify_pending_raw_receive_entries(&hashes)? else {
            return Ok(JsValue::UNDEFINED);
        };
        Ok(Uint8Array::from(out.as_slice()).into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_prepared_raw_receive_batch(
        &mut self,
        hashes: Array,
        heads: Uint8Array,
        coordinate_hashes: Array,
        coordinate_gids: Array,
        coordinate_hash_numbers: Array,
        coordinate_batches: Array,
        coordinate_next_hash_batches: Array,
        coordinate_assigned_to_range_boundaries: Uint8Array,
        coordinate_requested_replicas: Array,
    ) -> Result<bool, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let coordinate_commits = coordinate_commits_from_string_columns(
            coordinate_hashes,
            coordinate_gids,
            coordinate_hash_numbers,
            coordinate_batches,
            coordinate_next_hash_batches,
            coordinate_assigned_to_range_boundaries,
            coordinate_requested_replicas,
        )?;
        self.commit_prepared_raw_receive_batch_core(hashes, &heads, coordinate_commits)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_prepared_raw_receive_batch_u64(
        &mut self,
        hashes: Array,
        heads: Uint8Array,
        coordinate_hashes: Array,
        coordinate_gids: Array,
        coordinate_hash_numbers: BigUint64Array,
        coordinate_counts: Uint32Array,
        coordinates: BigUint64Array,
        coordinate_next_hash_batches: Array,
        coordinate_assigned_to_range_boundaries: Uint8Array,
        coordinate_requested_replicas: Uint32Array,
    ) -> Result<bool, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let coordinate_commits = coordinate_commits_from_u64_columns(
            coordinate_hashes,
            coordinate_gids,
            coordinate_hash_numbers,
            coordinate_counts,
            coordinates,
            coordinate_next_hash_batches,
            coordinate_assigned_to_range_boundaries,
            coordinate_requested_replicas,
        )?;
        self.commit_prepared_raw_receive_batch_core(hashes, &heads, coordinate_commits)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_prepared_raw_receive_join_batch(
        &mut self,
        hashes: Array,
        heads: Uint8Array,
        coordinate_hashes: Array,
        coordinate_gids: Array,
        coordinate_hash_numbers: Array,
        coordinate_batches: Array,
        coordinate_next_hash_batches: Array,
        coordinate_assigned_to_range_boundaries: Uint8Array,
        coordinate_requested_replicas: Array,
    ) -> Result<bool, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let coordinate_commits = coordinate_commits_from_string_columns(
            coordinate_hashes,
            coordinate_gids,
            coordinate_hash_numbers,
            coordinate_batches,
            coordinate_next_hash_batches,
            coordinate_assigned_to_range_boundaries,
            coordinate_requested_replicas,
        )?;
        self.commit_prepared_raw_receive_join_batch_core(hashes, &heads, coordinate_commits)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_prepared_raw_receive_join_batch_u64(
        &mut self,
        hashes: Array,
        heads: Uint8Array,
        coordinate_hashes: Array,
        coordinate_gids: Array,
        coordinate_hash_numbers: BigUint64Array,
        coordinate_counts: Uint32Array,
        coordinates: BigUint64Array,
        coordinate_next_hash_batches: Array,
        coordinate_assigned_to_range_boundaries: Uint8Array,
        coordinate_requested_replicas: Uint32Array,
    ) -> Result<bool, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let coordinate_commits = coordinate_commits_from_u64_columns(
            coordinate_hashes,
            coordinate_gids,
            coordinate_hash_numbers,
            coordinate_counts,
            coordinates,
            coordinate_next_hash_batches,
            coordinate_assigned_to_range_boundaries,
            coordinate_requested_replicas,
        )?;
        self.commit_prepared_raw_receive_join_batch_core(hashes, &heads, coordinate_commits)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_verified_prepared_raw_receive_join_batch(
        &mut self,
        hashes: Array,
        heads: Uint8Array,
        verify_hashes: Array,
        coordinate_hashes: Array,
        coordinate_gids: Array,
        coordinate_hash_numbers: Array,
        coordinate_batches: Array,
        coordinate_next_hash_batches: Array,
        coordinate_assigned_to_range_boundaries: Uint8Array,
        coordinate_requested_replicas: Array,
    ) -> Result<bool, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let verify_hashes = strings_from_array(verify_hashes)?;
        let coordinate_commits = coordinate_commits_from_string_columns(
            coordinate_hashes,
            coordinate_gids,
            coordinate_hash_numbers,
            coordinate_batches,
            coordinate_next_hash_batches,
            coordinate_assigned_to_range_boundaries,
            coordinate_requested_replicas,
        )?;
        self.commit_verified_prepared_raw_receive_join_batch_core(
            hashes,
            &heads,
            Some(verify_hashes),
            coordinate_commits,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_verified_prepared_raw_receive_join_batch_u64(
        &mut self,
        hashes: Array,
        heads: Uint8Array,
        verify_hashes: Array,
        coordinate_hashes: Array,
        coordinate_gids: Array,
        coordinate_hash_numbers: BigUint64Array,
        coordinate_counts: Uint32Array,
        coordinates: BigUint64Array,
        coordinate_next_hash_batches: Array,
        coordinate_assigned_to_range_boundaries: Uint8Array,
        coordinate_requested_replicas: Uint32Array,
    ) -> Result<bool, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let verify_hashes = strings_from_array(verify_hashes)?;
        let coordinate_commits = coordinate_commits_from_u64_columns(
            coordinate_hashes,
            coordinate_gids,
            coordinate_hash_numbers,
            coordinate_counts,
            coordinates,
            coordinate_next_hash_batches,
            coordinate_assigned_to_range_boundaries,
            coordinate_requested_replicas,
        )?;
        self.commit_verified_prepared_raw_receive_join_batch_core(
            hashes,
            &heads,
            Some(verify_hashes),
            coordinate_commits,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_verified_all_prepared_raw_receive_join_batch(
        &mut self,
        hashes: Array,
        heads: Uint8Array,
        coordinate_hashes: Array,
        coordinate_gids: Array,
        coordinate_hash_numbers: Array,
        coordinate_batches: Array,
        coordinate_next_hash_batches: Array,
        coordinate_assigned_to_range_boundaries: Uint8Array,
        coordinate_requested_replicas: Array,
    ) -> Result<bool, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let coordinate_commits = coordinate_commits_from_string_columns(
            coordinate_hashes,
            coordinate_gids,
            coordinate_hash_numbers,
            coordinate_batches,
            coordinate_next_hash_batches,
            coordinate_assigned_to_range_boundaries,
            coordinate_requested_replicas,
        )?;
        self.commit_verified_prepared_raw_receive_join_batch_core(
            hashes,
            &heads,
            None,
            coordinate_commits,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_verified_all_prepared_raw_receive_join_batch_u64(
        &mut self,
        hashes: Array,
        heads: Uint8Array,
        coordinate_hashes: Array,
        coordinate_gids: Array,
        coordinate_hash_numbers: BigUint64Array,
        coordinate_counts: Uint32Array,
        coordinates: BigUint64Array,
        coordinate_next_hash_batches: Array,
        coordinate_assigned_to_range_boundaries: Uint8Array,
        coordinate_requested_replicas: Uint32Array,
    ) -> Result<bool, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let coordinate_commits = coordinate_commits_from_u64_columns(
            coordinate_hashes,
            coordinate_gids,
            coordinate_hash_numbers,
            coordinate_counts,
            coordinates,
            coordinate_next_hash_batches,
            coordinate_assigned_to_range_boundaries,
            coordinate_requested_replicas,
        )?;
        self.commit_verified_prepared_raw_receive_join_batch_core(
            hashes,
            &heads,
            None,
            coordinate_commits,
        )
    }

    pub fn clear_prepared_raw_receive_entries(&mut self, hashes: Array) -> Result<usize, JsValue> {
        let mut removed = 0;
        for hash in strings_from_array(hashes)? {
            if self.pending_raw_receive_entries.remove(&hash).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

impl NativePeerbitBackbone {
    fn prepare_raw_receive_entries(
        &mut self,
        blocks: Vec<Vec<u8>>,
        expected_cids: Option<Vec<String>>,
        verify_signatures: bool,
    ) -> Result<Vec<PreparedRawEntryV0>, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let prepare_started = profile_enabled.then(js_sys::Date::now);
        let mut raw_profile = profile_enabled.then(RawEntryV0PrepareProfile::default);
        let prepared = prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled(
            blocks,
            expected_cids,
            verify_signatures,
            raw_profile.as_mut(),
        )?;
        if let Some(started) = prepare_started {
            self.append_profile.raw_receive_prepare_ms += js_sys::Date::now() - started;
        }
        if let Some(raw_profile) = raw_profile {
            self.append_profile.add_raw_prepare_profile(&raw_profile);
        }
        Ok(prepared)
    }

    fn prepare_raw_receive_columns_from_entries(
        &mut self,
        prepared: Vec<PreparedRawEntryV0>,
        include_hash_digest_bytes: bool,
        include_cids: bool,
    ) -> Result<Array, JsValue> {
        let profile_enabled = self.append_profile_enabled;
        let columns_started = profile_enabled.then(js_sys::Date::now);
        let len = prepared.len();
        let cids = Array::new();
        let hash_digest_bytes = Array::new();
        let mut byte_lengths = Vec::with_capacity(len);
        let clock_ids = Array::new();
        let mut wall_times = Vec::with_capacity(len);
        let mut logicals = Vec::with_capacity(len);
        let gids = Array::new();
        let nexts = Array::new();
        let mut entry_types = Vec::with_capacity(len);
        let meta_bytes = Array::new();
        let meta_datas = Array::new();
        let mut payload_byte_lengths = Vec::with_capacity(len);
        let mut signature_verified = Vec::with_capacity(len);
        let mut requested_replicas = Vec::with_capacity(len);
        let mut hash_numbers = Vec::with_capacity(len);

        for entry in prepared {
            let hash_number = hash_number_u64(&self.resolution, &entry.hash_digest_bytes)?;
            if include_cids {
                cids.push(&JsValue::from_str(&entry.cid));
            }
            if include_hash_digest_bytes {
                hash_digest_bytes.push(&Uint8Array::from(entry.hash_digest_bytes.as_slice()));
            }
            byte_lengths.push(entry.byte_length as u32);
            clock_ids.push(&Uint8Array::from(entry.clock_id.as_slice()));
            wall_times.push(entry.wall_time);
            logicals.push(entry.logical);
            gids.push(&JsValue::from_str(&entry.gid));
            nexts.push(&strings_slice_to_array(&entry.next));
            entry_types.push(entry.entry_type);
            meta_bytes.push(&Uint8Array::from(entry.meta_bytes.as_slice()));
            match &entry.meta_data {
                Some(data) => meta_datas.push(&Uint8Array::from(data.as_slice())),
                None => meta_datas.push(&JsValue::UNDEFINED),
            };
            payload_byte_lengths.push(entry.payload_byte_length as u32);
            signature_verified.push(u8::from(entry.signature_verified));
            requested_replicas.push(entry.requested_replicas.unwrap_or(0));
            hash_numbers.push(hash_number);

            let log_entry = entry.log_index_entry(true)?;
            self.pending_raw_receive_entries.insert(
                entry.cid.clone(),
                PendingRawReceiveEntry {
                    storage_bytes: entry.storage_bytes,
                    entry: log_entry,
                    requested_replicas: entry.requested_replicas,
                    signature_verified: entry.signature_verified,
                    signable_prefix_len: entry.signable_prefix_len,
                    signature_with_key_start: entry.signature_with_key_start,
                    signature_with_key_len: entry.signature_with_key_len,
                },
            );
        }

        let out = Array::new();
        out.push(&cids);
        out.push(&hash_digest_bytes);
        out.push(&Uint32Array::from(byte_lengths.as_slice()));
        out.push(&clock_ids);
        out.push(&BigUint64Array::from(wall_times.as_slice()));
        out.push(&Uint32Array::from(logicals.as_slice()));
        out.push(&gids);
        out.push(&nexts);
        out.push(&Uint8Array::from(entry_types.as_slice()));
        out.push(&meta_bytes);
        out.push(&meta_datas);
        out.push(&Uint32Array::from(payload_byte_lengths.as_slice()));
        out.push(&Uint8Array::from(signature_verified.as_slice()));
        out.push(&Uint32Array::from(requested_replicas.as_slice()));
        out.push(&BigUint64Array::from(hash_numbers.as_slice()));
        if let Some(started) = columns_started {
            self.append_profile.raw_receive_prepare_columns_ms += js_sys::Date::now() - started;
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_prepared_raw_receive_selection_core(
        &self,
        hashes: Vec<String>,
        min_replicas: u32,
        max_replicas: JsValue,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<JsValue, JsValue> {
        let expected_hash_count = hashes.len();
        if expected_hash_count == 0 {
            return Ok(JsValue::UNDEFINED);
        }
        let Some(groups) =
            self.prepared_raw_receive_group_plans(hashes.clone(), min_replicas, max_replicas)?
        else {
            return Ok(JsValue::UNDEFINED);
        };
        self.plan_prepared_raw_receive_selection_for_groups(
            hashes,
            groups,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_prepared_raw_receive_selection_for_groups(
        &self,
        hashes: Vec<String>,
        groups: Vec<ResolvedPendingRawReceiveGroupPlan>,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<JsValue, JsValue> {
        let expected_hash_count = hashes.len();
        let mut planned_hash_count = 0usize;
        let mut gids = Vec::with_capacity(groups.len());
        let mut replica_counts = Vec::with_capacity(groups.len());
        for group in &groups {
            planned_hash_count += group.hashes.len();
            gids.push(group.gid.clone());
            replica_counts.push(group.max_max_replicas as usize);
        }
        if planned_hash_count != expected_hash_count {
            return Ok(JsValue::UNDEFINED);
        }

        let leader_plans = self.shared_log.plan_leaders_for_gids_batch_core(
            &gids,
            &replica_counts,
            role_age_ms,
            &now,
            peer_filter,
            expand_peer_filter,
            &self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        if leader_plans.len() != groups.len() {
            return Ok(JsValue::UNDEFINED);
        }
        if !leader_plans
            .iter()
            .any(|plan| leader_plan_contains_hash(plan, &self_hash))
        {
            return Ok(prepared_raw_receive_selection_all_drop(
                hashes,
                groups.len(),
                planned_hash_count,
                true,
            ));
        }

        prepared_raw_receive_selection_from_leader_plans(
            &self.resolution,
            &groups,
            expected_hash_count,
            planned_hash_count,
            &leader_plans,
            &self_hash,
            false,
        )
    }

    fn prepared_raw_receive_group_plans(
        &self,
        hashes: Vec<String>,
        min_replicas: u32,
        max_replicas: JsValue,
    ) -> Result<Option<Vec<ResolvedPendingRawReceiveGroupPlan>>, JsValue> {
        if hashes.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let lower = min_replicas.max(1);
        let higher = if max_replicas.is_undefined() || max_replicas.is_null() {
            u32::MAX
        } else {
            max_replicas
                .as_f64()
                .ok_or_else(|| JsValue::from_str("maxReplicas must be a number"))?
                .max(0.0)
                .min(u32::MAX as f64) as u32
        };

        let mut group_indexes: HashMap<String, usize> = HashMap::new();
        let mut groups: Vec<PendingRawReceiveGroupPlan> = Vec::new();

        for (input_index, hash) in hashes.into_iter().enumerate() {
            let Some(pending) = self.pending_raw_receive_entries.get(&hash) else {
                return Ok(None);
            };
            let Some(requested_replicas) = pending.requested_replicas else {
                return Ok(None);
            };
            let input_index = u32::try_from(input_index)
                .map_err(|_| JsValue::from_str("Raw receive group index overflow"))?;

            let group_index = if let Some(index) = group_indexes.get(&pending.entry.gid) {
                *index
            } else {
                let index = groups.len();
                group_indexes.insert(pending.entry.gid.clone(), index);
                groups.push(PendingRawReceiveGroupPlan {
                    gid: pending.entry.gid.clone(),
                    hashes: Vec::new(),
                    indexes: Vec::new(),
                    requested_replicas: Vec::new(),
                    latest_hash: hash.clone(),
                    latest_index: input_index,
                    latest_wall_time: pending.entry.wall_time,
                    latest_logical: pending.entry.logical,
                    max_requested_replicas: requested_replicas,
                });
                index
            };

            let group = &mut groups[group_index];
            group.hashes.push(hash.clone());
            group.indexes.push(input_index);
            group.requested_replicas.push(requested_replicas);
            group.max_requested_replicas = group.max_requested_replicas.max(requested_replicas);
            if pending.entry.wall_time > group.latest_wall_time
                || (pending.entry.wall_time == group.latest_wall_time
                    && pending.entry.logical > group.latest_logical)
            {
                group.latest_hash = hash;
                group.latest_index = input_index;
                group.latest_wall_time = pending.entry.wall_time;
                group.latest_logical = pending.entry.logical;
            }
        }

        let gids: Vec<String> = groups.iter().map(|group| group.gid.clone()).collect();
        let max_heads = self.log.max_head_data_u32_values(&gids);

        Ok(Some(
            groups
                .into_iter()
                .zip(max_heads)
                .map(|(group, max_head)| {
                    let max_head = max_head
                        .map(|value| clamp_replicas_u32(value, lower, higher))
                        .unwrap_or(lower);
                    let max_new = clamp_replicas_u32(group.max_requested_replicas, lower, higher);
                    ResolvedPendingRawReceiveGroupPlan {
                        gid: group.gid,
                        hashes: group.hashes,
                        indexes: group.indexes,
                        requested_replicas: group.requested_replicas,
                        latest_hash: group.latest_hash,
                        latest_index: group.latest_index,
                        max_replicas_from_head: max_head,
                        max_replicas_from_new_entries: max_new,
                        max_max_replicas: max_head.max(max_new),
                    }
                })
                .collect(),
        ))
    }

    fn verify_pending_raw_receive_entries(
        &mut self,
        hashes: &[String],
    ) -> Result<Option<Vec<u8>>, JsValue> {
        if hashes.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let mut out = vec![1u8; hashes.len()];
        let mut verify_positions = Vec::new();
        let verified = {
            let mut entries = Vec::new();
            for (index, hash) in hashes.iter().enumerate() {
                let Some(pending) = self.pending_raw_receive_entries.get(hash) else {
                    return Ok(None);
                };
                if !pending.signature_verified {
                    verify_positions.push(index);
                    entries.push(pending.signature_input());
                }
            }
            if entries.is_empty() {
                Some(Vec::new())
            } else {
                verify_prepared_entry_v0_ed25519_storage_slices(&entries).ok()
            }
        };

        let Some(verified) = verified else {
            return Ok(None);
        };
        if verified.len() != verify_positions.len() {
            return Err(JsValue::from_str(
                "Expected equal prepared raw receive verify lengths",
            ));
        }
        for (index, flag) in verify_positions.iter().zip(verified.iter()) {
            out[*index] = *flag;
            if *flag != 0 {
                if let Some(pending) = self.pending_raw_receive_entries.get_mut(&hashes[*index]) {
                    pending.signature_verified = true;
                }
            }
        }

        Ok(Some(out))
    }

    fn verify_pending_raw_receive_entries_all(
        &mut self,
        hashes: &[String],
    ) -> Result<Option<bool>, JsValue> {
        if hashes.is_empty() {
            return Ok(Some(true));
        }

        let mut verify_positions = Vec::new();
        let verified = {
            let mut entries = Vec::new();
            for (index, hash) in hashes.iter().enumerate() {
                let Some(pending) = self.pending_raw_receive_entries.get(hash) else {
                    return Ok(None);
                };
                if !pending.signature_verified {
                    verify_positions.push(index);
                    entries.push(pending.signature_input());
                }
            }
            if entries.is_empty() {
                Some(true)
            } else {
                verify_prepared_entry_v0_ed25519_storage_slices_all(&entries).ok()
            }
        };

        let Some(verified) = verified else {
            return Ok(None);
        };
        if verified {
            for index in verify_positions {
                if let Some(pending) = self.pending_raw_receive_entries.get_mut(&hashes[index]) {
                    pending.signature_verified = true;
                }
            }
        }

        Ok(Some(verified))
    }

    fn commit_prepared_raw_receive_batch_core(
        &mut self,
        hashes: Vec<String>,
        heads: &Uint8Array,
        coordinate_commits: Vec<EntryCoordinateCommit>,
    ) -> Result<bool, JsValue> {
        ensure_same_len(hashes.len(), heads.length() as usize, "raw receive heads")?;
        let profile_enabled = self.append_profile_enabled;
        let pending_check_started = profile_enabled.then(js_sys::Date::now);
        let missing_pending = hashes
            .iter()
            .any(|hash| !self.pending_raw_receive_entries.contains_key(hash));
        if let Some(started) = pending_check_started {
            self.append_profile.raw_receive_pending_check_ms += js_sys::Date::now() - started;
        }
        if missing_pending {
            return Ok(false);
        }

        let remove_started = profile_enabled.then(js_sys::Date::now);
        let mut block_entries = Vec::with_capacity(hashes.len());
        let mut graph_entries = Vec::with_capacity(hashes.len());
        for (index, hash) in hashes.into_iter().enumerate() {
            let pending = self
                .pending_raw_receive_entries
                .remove(&hash)
                .ok_or_else(|| JsValue::from_str("Missing prepared raw receive entry"))?;
            block_entries.push((hash, pending.storage_bytes));
            let mut graph_entry = pending.entry;
            graph_entry.head = heads.get_index(index as u32) != 0;
            graph_entries.push(graph_entry);
        }
        if let Some(started) = remove_started {
            self.append_profile.raw_receive_remove_ms += js_sys::Date::now() - started;
        }

        let block_put_started = profile_enabled.then(js_sys::Date::now);
        self.blocks.put_entries_core(block_entries);
        if let Some(started) = block_put_started {
            self.append_profile.raw_receive_block_put_ms += js_sys::Date::now() - started;
        }
        let graph_put_started = profile_enabled.then(js_sys::Date::now);
        self.log.put_entries_core(graph_entries);
        if let Some(started) = graph_put_started {
            self.append_profile.raw_receive_graph_put_ms += js_sys::Date::now() - started;
        }
        if !coordinate_commits.is_empty() {
            let coordinate_started = profile_enabled.then(js_sys::Date::now);
            self.commit_entry_coordinate_commits(coordinate_commits);
            if let Some(started) = coordinate_started {
                self.append_profile.raw_receive_coordinate_commit_ms +=
                    js_sys::Date::now() - started;
            }
        }
        Ok(true)
    }

    fn commit_prepared_raw_receive_join_batch_core(
        &mut self,
        hashes: Vec<String>,
        heads: &Uint8Array,
        coordinate_commits: Vec<EntryCoordinateCommit>,
    ) -> Result<bool, JsValue> {
        ensure_same_len(hashes.len(), heads.length() as usize, "raw receive heads")?;
        let profile_enabled = self.append_profile_enabled;
        let pending_check_started = profile_enabled.then(js_sys::Date::now);
        let missing_pending = hashes
            .iter()
            .any(|hash| !self.pending_raw_receive_entries.contains_key(hash));
        if let Some(started) = pending_check_started {
            self.append_profile.raw_receive_pending_check_ms += js_sys::Date::now() - started;
        }
        if missing_pending {
            return Ok(false);
        }

        {
            let join_plan_started = profile_enabled.then(js_sys::Date::now);
            let mut graph_entries = Vec::with_capacity(hashes.len());
            for hash in &hashes {
                let pending = self
                    .pending_raw_receive_entries
                    .get(hash)
                    .ok_or_else(|| JsValue::from_str("Missing prepared raw receive entry"))?;
                graph_entries.push(&pending.entry);
            }
            let join_plans = self
                .log
                .plan_join_entry_refs_core(&graph_entries, false, true);
            if let Some(started) = join_plan_started {
                self.append_profile.raw_receive_join_plan_ms += js_sys::Date::now() - started;
            }
            let mut batch_hashes: Option<HashSet<&str>> = None;
            for plan in join_plans {
                if plan.skip || plan.covered_by_cut || !plan.cut_checked {
                    return Ok(false);
                }
                if !plan.missing_parents.is_empty() {
                    let batch_hashes = batch_hashes
                        .get_or_insert_with(|| hashes.iter().map(String::as_str).collect());
                    if plan
                        .missing_parents
                        .iter()
                        .any(|hash| !batch_hashes.contains(hash.as_str()))
                    {
                        return Ok(false);
                    }
                }
            }
        }

        let remove_started = profile_enabled.then(js_sys::Date::now);
        let mut block_entries = Vec::with_capacity(hashes.len());
        let mut graph_entries = Vec::with_capacity(hashes.len());
        for (index, hash) in hashes.into_iter().enumerate() {
            let pending = self
                .pending_raw_receive_entries
                .remove(&hash)
                .ok_or_else(|| JsValue::from_str("Missing prepared raw receive entry"))?;
            block_entries.push((hash, pending.storage_bytes));
            let mut graph_entry = pending.entry;
            graph_entry.head = heads.get_index(index as u32) != 0;
            graph_entries.push(graph_entry);
        }
        if let Some(started) = remove_started {
            self.append_profile.raw_receive_remove_ms += js_sys::Date::now() - started;
        }

        let block_put_started = profile_enabled.then(js_sys::Date::now);
        self.blocks.put_entries_core(block_entries);
        if let Some(started) = block_put_started {
            self.append_profile.raw_receive_block_put_ms += js_sys::Date::now() - started;
        }
        let graph_put_started = profile_enabled.then(js_sys::Date::now);
        self.log.put_join_batch_entries_core(graph_entries);
        if let Some(started) = graph_put_started {
            self.append_profile.raw_receive_graph_put_ms += js_sys::Date::now() - started;
        }
        if !coordinate_commits.is_empty() {
            let coordinate_started = profile_enabled.then(js_sys::Date::now);
            self.commit_entry_coordinate_commits(coordinate_commits);
            if let Some(started) = coordinate_started {
                self.append_profile.raw_receive_coordinate_commit_ms +=
                    js_sys::Date::now() - started;
            }
        }
        Ok(true)
    }

    fn commit_verified_prepared_raw_receive_join_batch_core(
        &mut self,
        hashes: Vec<String>,
        heads: &Uint8Array,
        verify_hashes: Option<Vec<String>>,
        coordinate_commits: Vec<EntryCoordinateCommit>,
    ) -> Result<bool, JsValue> {
        ensure_same_len(hashes.len(), heads.length() as usize, "raw receive heads")?;
        let profile_enabled = self.append_profile_enabled;
        let verify_hashes_cover_commit = match verify_hashes.as_ref() {
            None => true,
            Some(verify_hashes) => {
                verify_hashes.len() == hashes.len()
                    && verify_hashes
                        .iter()
                        .zip(hashes.iter())
                        .all(|(verified_hash, hash)| verified_hash == hash)
            }
        };
        if !verify_hashes_cover_commit {
            let pending_check_started = profile_enabled.then(js_sys::Date::now);
            let missing_pending = hashes
                .iter()
                .any(|hash| !self.pending_raw_receive_entries.contains_key(hash));
            if let Some(started) = pending_check_started {
                self.append_profile.raw_receive_pending_check_ms += js_sys::Date::now() - started;
            }
            if missing_pending {
                return Ok(false);
            }
        }
        let verify_started = profile_enabled.then(js_sys::Date::now);
        if verify_hashes_cover_commit {
            let verify_hashes = verify_hashes.as_ref().unwrap_or(&hashes);
            let Some(verified) = self.verify_pending_raw_receive_entries_all(verify_hashes)? else {
                return Ok(false);
            };
            if let Some(started) = verify_started {
                self.append_profile.raw_receive_verify_ms += js_sys::Date::now() - started;
            }
            if !verified {
                return Ok(false);
            }
        } else {
            let verify_hashes = verify_hashes.expect("partial verify hashes");
            let Some(verified) = self.verify_pending_raw_receive_entries(&verify_hashes)? else {
                return Ok(false);
            };
            if let Some(started) = verify_started {
                self.append_profile.raw_receive_verify_ms += js_sys::Date::now() - started;
            }
            if verified.iter().any(|flag| *flag == 0) {
                return Ok(false);
            }
            let verify_status_started = profile_enabled.then(js_sys::Date::now);
            let missing_verified = hashes.iter().any(|hash| {
                self.pending_raw_receive_entries
                    .get(hash)
                    .map(|pending| !pending.signature_verified)
                    .unwrap_or(true)
            });
            if let Some(started) = verify_status_started {
                self.append_profile.raw_receive_verify_status_ms += js_sys::Date::now() - started;
            }
            if missing_verified {
                return Ok(false);
            }
        }

        {
            let join_plan_started = profile_enabled.then(js_sys::Date::now);
            let mut graph_entries = Vec::with_capacity(hashes.len());
            for hash in &hashes {
                let pending = self
                    .pending_raw_receive_entries
                    .get(hash)
                    .ok_or_else(|| JsValue::from_str("Missing prepared raw receive entry"))?;
                graph_entries.push(&pending.entry);
            }
            let join_plans = self
                .log
                .plan_join_entry_refs_core(&graph_entries, false, true);
            if let Some(started) = join_plan_started {
                self.append_profile.raw_receive_join_plan_ms += js_sys::Date::now() - started;
            }
            let mut batch_hashes: Option<HashSet<&str>> = None;
            for plan in join_plans {
                if plan.skip || plan.covered_by_cut || !plan.cut_checked {
                    return Ok(false);
                }
                if !plan.missing_parents.is_empty() {
                    let batch_hashes = batch_hashes
                        .get_or_insert_with(|| hashes.iter().map(String::as_str).collect());
                    if plan
                        .missing_parents
                        .iter()
                        .any(|hash| !batch_hashes.contains(hash.as_str()))
                    {
                        return Ok(false);
                    }
                }
            }
        }

        let remove_started = profile_enabled.then(js_sys::Date::now);
        let mut block_entries = Vec::with_capacity(hashes.len());
        let mut graph_entries = Vec::with_capacity(hashes.len());
        for (index, hash) in hashes.into_iter().enumerate() {
            let pending = self
                .pending_raw_receive_entries
                .remove(&hash)
                .ok_or_else(|| JsValue::from_str("Missing prepared raw receive entry"))?;
            block_entries.push((hash, pending.storage_bytes));
            let mut graph_entry = pending.entry;
            graph_entry.head = heads.get_index(index as u32) != 0;
            graph_entries.push(graph_entry);
        }
        if let Some(started) = remove_started {
            self.append_profile.raw_receive_remove_ms += js_sys::Date::now() - started;
        }

        let block_put_started = profile_enabled.then(js_sys::Date::now);
        self.blocks.put_entries_core(block_entries);
        if let Some(started) = block_put_started {
            self.append_profile.raw_receive_block_put_ms += js_sys::Date::now() - started;
        }
        let graph_put_started = profile_enabled.then(js_sys::Date::now);
        self.log.put_join_batch_entries_core(graph_entries);
        if let Some(started) = graph_put_started {
            self.append_profile.raw_receive_graph_put_ms += js_sys::Date::now() - started;
        }
        if !coordinate_commits.is_empty() {
            let coordinate_started = profile_enabled.then(js_sys::Date::now);
            self.commit_entry_coordinate_commits(coordinate_commits);
            if let Some(started) = coordinate_started {
                self.append_profile.raw_receive_coordinate_commit_ms +=
                    js_sys::Date::now() - started;
            }
        }
        Ok(true)
    }
}
