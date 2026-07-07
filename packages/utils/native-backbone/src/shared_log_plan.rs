use js_sys::{Array, BigUint64Array, Uint32Array, Uint8Array};
use peerbit_indexer_core::storage::ByteStorage;
use peerbit_shared_log_rust::EntryCoordinateCommit;
use std::collections::HashSet;
use wasm_bindgen::prelude::*;

use crate::coordinates::{
    coordinate_core_value_to_row, decode_coordinate_value, CoordinateCoreValue,
};
use crate::error::BackboneError;
use crate::js_interop::{
    array_from_value, ensure_same_len, number_strings_to_array, parse_u64_string,
    string_batches_from_array, strings_from_array, strings_to_array, usize_values_from_array,
};
use crate::NativePeerbitBackbone;

pub(crate) fn leader_samples_to_rows(values: &[peerbit_shared_log_rust::LeaderSample]) -> Array {
    let out = Array::new();
    for value in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&value.hash));
        row.push(&JsValue::from_bool(value.intersecting));
        out.push(&row);
    }
    out
}

pub(crate) fn leader_samples_to_optional_rows(
    values: &Option<Vec<peerbit_shared_log_rust::LeaderSample>>,
) -> JsValue {
    let Some(values) = values else {
        return JsValue::UNDEFINED;
    };
    leader_samples_to_rows(values).into()
}

pub(crate) fn clamp_replicas_u32(value: u32, lower: u32, higher: u32) -> u32 {
    value.min(higher).max(lower)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn coordinate_commits_from_string_columns(
    hashes: Array,
    gids: Array,
    hash_numbers: Array,
    coordinate_batches: Array,
    next_hash_batches: Array,
    assigned_to_range_boundaries: Uint8Array,
    requested_replicas: Array,
) -> Result<Vec<EntryCoordinateCommit>, JsValue> {
    let hashes = strings_from_array(hashes)?;
    let gids = strings_from_array(gids)?;
    let hash_numbers = strings_from_array(hash_numbers)?;
    let coordinate_batches = coordinate_batches_from_array(coordinate_batches)?;
    let next_hash_batches =
        string_batches_from_array(next_hash_batches, "coordinate commit next hashes")?;
    let requested_replicas = usize_values_from_array(requested_replicas)?;
    coordinate_commits_from_parts(
        hashes,
        gids,
        hash_numbers
            .iter()
            .map(|value| parse_u64_string(value, "coordinate hash number"))
            .collect::<Result<Vec<_>, _>>()?,
        coordinate_batches,
        next_hash_batches,
        assigned_to_range_boundaries,
        requested_replicas,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn coordinate_commits_from_u64_columns(
    hashes: Array,
    gids: Array,
    hash_numbers: BigUint64Array,
    coordinate_counts: Uint32Array,
    coordinates: BigUint64Array,
    next_hash_batches: Array,
    assigned_to_range_boundaries: Uint8Array,
    requested_replicas: Uint32Array,
) -> Result<Vec<EntryCoordinateCommit>, JsValue> {
    let hashes = strings_from_array(hashes)?;
    let gids = strings_from_array(gids)?;
    let hash_numbers = hash_numbers.to_vec();
    let coordinate_counts = coordinate_counts.to_vec();
    let coordinates = coordinates.to_vec();
    let next_hash_batches =
        string_batches_from_array(next_hash_batches, "coordinate commit next hashes")?;
    let requested_replicas = requested_replicas
        .to_vec()
        .into_iter()
        .map(|value| value as usize)
        .collect::<Vec<_>>();
    ensure_same_len(
        hashes.len(),
        coordinate_counts.len(),
        "coordinate commit coordinate counts",
    )?;
    let coordinate_total = coordinate_counts
        .iter()
        .try_fold(0usize, |sum, count| sum.checked_add(*count as usize))
        .ok_or_else(|| JsValue::from_str("Coordinate count overflow"))?;
    ensure_same_len(
        coordinate_total,
        coordinates.len(),
        "coordinate commit flattened coordinates",
    )?;
    let mut coordinate_batches = Vec::with_capacity(coordinate_counts.len());
    let mut offset = 0usize;
    for count in coordinate_counts {
        let end = offset + count as usize;
        coordinate_batches.push(coordinates[offset..end].to_vec());
        offset = end;
    }
    coordinate_commits_from_parts(
        hashes,
        gids,
        hash_numbers,
        coordinate_batches,
        next_hash_batches,
        assigned_to_range_boundaries,
        requested_replicas,
    )
}

#[allow(clippy::too_many_arguments)]
fn coordinate_commits_from_parts(
    hashes: Vec<String>,
    gids: Vec<String>,
    hash_numbers: Vec<u64>,
    coordinate_batches: Vec<Vec<u64>>,
    next_hash_batches: Vec<Vec<String>>,
    assigned_to_range_boundaries: Uint8Array,
    requested_replicas: Vec<usize>,
) -> Result<Vec<EntryCoordinateCommit>, JsValue> {
    ensure_same_len(hashes.len(), gids.len(), "coordinate commit gid")?;
    ensure_same_len(
        hashes.len(),
        hash_numbers.len(),
        "coordinate commit hash number",
    )?;
    ensure_same_len(
        hashes.len(),
        coordinate_batches.len(),
        "coordinate commit coordinates",
    )?;
    ensure_same_len(
        hashes.len(),
        next_hash_batches.len(),
        "coordinate commit next hashes",
    )?;
    ensure_same_len(
        hashes.len(),
        assigned_to_range_boundaries.length() as usize,
        "coordinate commit assigned flags",
    )?;
    ensure_same_len(
        hashes.len(),
        requested_replicas.len(),
        "coordinate commit replicas",
    )?;

    let mut commits = Vec::with_capacity(hashes.len());
    for (index, ((((hash, gid), coordinates), next_hashes), requested_replicas)) in hashes
        .into_iter()
        .zip(gids)
        .zip(coordinate_batches)
        .zip(next_hash_batches)
        .zip(requested_replicas)
        .enumerate()
    {
        commits.push(EntryCoordinateCommit {
            hash,
            gid,
            hash_number: hash_numbers[index],
            coordinates,
            next_hashes,
            assigned_to_range_boundary: assigned_to_range_boundaries.get_index(index as u32) != 0,
            requested_replicas,
        });
    }
    Ok(commits)
}

fn coordinate_batches_from_array(values: Array) -> Result<Vec<Vec<u64>>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values.get(index);
        if !Array::is_array(&value) {
            return Err(JsValue::from_str("Expected coordinate batch array"));
        }
        out.push(coordinate_numbers_from_array(Array::from(&value))?);
    }
    Ok(out)
}

pub(crate) fn coordinate_numbers_from_array(values: Array) -> Result<Vec<u64>, BackboneError> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values.get(index);
        if let Some(value) = value.as_string() {
            out.push(parse_u64_string(&value, "coordinate")?);
        } else if let Some(value) = value.as_f64() {
            out.push(value as u64);
        } else {
            return Err(BackboneError::Expected("coordinate string array"));
        }
    }
    Ok(out)
}

#[wasm_bindgen]
impl NativePeerbitBackbone {
    pub fn entry_coordinate_hashes(&self) -> Array {
        self.shared_log.entry_coordinate_hashes()
    }

    pub fn get_entry_coordinates(&self, hash: &str) -> JsValue {
        self.shared_log.get_entry_coordinates(hash)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_leaders(
        &self,
        cursors: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.find_leaders(
            cursors,
            replicas,
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
    pub fn find_leaders_batch(
        &self,
        cursor_batches: Array,
        replica_counts: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.find_leaders_batch(
            cursor_batches,
            replica_counts,
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

    pub fn get_grid(&self, from: String, count: usize) -> Result<Array, JsValue> {
        self.shared_log.get_grid(from, count)
    }

    pub fn get_gid_coordinates(&self, gid: String, count: usize) -> Array {
        self.shared_log.get_gid_coordinates(gid, count)
    }

    pub fn entry_hashes_for_hash_numbers(&self, hash_numbers: Array) -> Result<Array, JsValue> {
        self.shared_log.entry_hashes_for_hash_numbers(hash_numbers)
    }

    pub fn entry_hashes_for_hash_numbers_u64(
        &self,
        hash_numbers: BigUint64Array,
    ) -> Result<Array, JsValue> {
        self.shared_log
            .entry_hashes_for_hash_numbers_u64(hash_numbers)
    }

    pub fn entry_hashes_for_hash_numbers_flat_u64(&self, hash_numbers: BigUint64Array) -> Array {
        self.shared_log
            .entry_hashes_for_hash_numbers_flat_u64(hash_numbers)
    }

    pub fn entry_hash_numbers_in_range(
        &self,
        start1: String,
        end1: String,
        start2: String,
        end2: String,
    ) -> Result<Array, JsValue> {
        self.shared_log
            .entry_hash_numbers_in_range(start1, end1, start2, end2)
    }

    pub fn entry_hash_numbers_in_range_u64(
        &self,
        start1: String,
        end1: String,
        start2: String,
        end2: String,
    ) -> Result<BigUint64Array, JsValue> {
        self.shared_log
            .entry_hash_numbers_in_range_u64(start1, end1, start2, end2)
    }

    pub fn count_entry_coordinates_in_ranges(
        &self,
        start1: Array,
        end1: Array,
        start2: Array,
        end2: Array,
        include_assigned_to_range_boundary: bool,
    ) -> Result<usize, JsValue> {
        self.shared_log.count_entry_coordinates_in_ranges(
            start1,
            end1,
            start2,
            end2,
            include_assigned_to_range_boundary,
        )
    }

    pub fn entry_coordinate_fields(&self) -> Result<Array, JsValue> {
        let out = Array::new();
        for (_, value) in self.coordinate_values.entries() {
            let coordinate = decode_coordinate_value(&value)?;
            out.push(&coordinate_core_value_to_row(&coordinate));
        }
        Ok(out)
    }

    pub fn clear_shared_log(&mut self) {
        self.shared_log.clear();
        self.clear_coordinate_core();
    }

    pub fn clear_entry_coordinates(&mut self) {
        self.shared_log.clear_entry_coordinates();
        self.clear_coordinate_core();
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

    pub fn delete_range(&mut self, id: &str) -> bool {
        self.shared_log.delete(id)
    }

    pub fn put_entry_coordinates(
        &mut self,
        hash: String,
        gid: String,
        hash_number: String,
        coordinates: Array,
        assigned_to_range_boundary: bool,
        requested_replicas: usize,
    ) -> Result<(), JsValue> {
        let hash_for_core = hash.clone();
        let gid_for_core = gid.clone();
        let hash_number_for_core = hash_number.clone();
        let coordinates_for_core = coordinates.clone();
        self.shared_log.put_entry_coordinates(
            hash,
            gid,
            hash_number,
            coordinates,
            assigned_to_range_boundary,
            requested_replicas,
        )?;
        self.put_coordinate_core_from_parts(
            hash_for_core,
            gid_for_core,
            &hash_number_for_core,
            coordinates_for_core,
            assigned_to_range_boundary,
            requested_replicas,
            0,
            Vec::new(),
        )?;
        Ok(())
    }

    pub fn delete_entry_coordinates(&mut self, hash: &str) -> bool {
        let deleted_shared_log = self.shared_log.delete_entry_coordinates(hash);
        let deleted_core = self.delete_coordinate_core(hash);
        deleted_shared_log || deleted_core
    }

    pub fn delete_entry_coordinates_batch(&mut self, hashes: Array) -> Result<(), JsValue> {
        let hashes_for_core = hashes.clone();
        self.shared_log.delete_entry_coordinates_batch(hashes)?;
        self.delete_coordinate_core_batch(hashes_for_core)?;
        Ok(())
    }

    pub fn commit_entry_coordinates(
        &mut self,
        hash: String,
        gid: String,
        hash_number: String,
        coordinates: Array,
        next_hashes: Array,
        assigned_to_range_boundary: bool,
        requested_replicas: usize,
    ) -> Result<(), JsValue> {
        let hash_for_core = hash.clone();
        let gid_for_core = gid.clone();
        let hash_number_for_core = hash_number.clone();
        let coordinates_for_core = coordinates.clone();
        let next_hashes_for_core = next_hashes.clone();
        self.shared_log.commit_entry_coordinates(
            hash,
            gid,
            hash_number,
            coordinates,
            next_hashes,
            assigned_to_range_boundary,
            requested_replicas,
        )?;
        self.put_coordinate_core_from_parts(
            hash_for_core,
            gid_for_core,
            &hash_number_for_core,
            coordinates_for_core,
            assigned_to_range_boundary,
            requested_replicas,
            0,
            Vec::new(),
        )?;
        self.delete_coordinate_core_batch(next_hashes_for_core)?;
        Ok(())
    }

    pub fn commit_entry_coordinates_batch(
        &mut self,
        hashes: Array,
        gids: Array,
        hash_numbers: Array,
        coordinate_batches: Array,
        next_hash_batches: Array,
        assigned_to_range_boundaries: Uint8Array,
        requested_replicas: Array,
    ) -> Result<(), JsValue> {
        let commits = coordinate_commits_from_string_columns(
            hashes,
            gids,
            hash_numbers,
            coordinate_batches,
            next_hash_batches,
            assigned_to_range_boundaries,
            requested_replicas,
        )?;
        self.commit_entry_coordinate_commits(commits);
        Ok(())
    }

    pub fn commit_entry_coordinates_batch_u64(
        &mut self,
        hashes: Array,
        gids: Array,
        hash_numbers: BigUint64Array,
        coordinate_counts: Uint32Array,
        coordinates: BigUint64Array,
        next_hash_batches: Array,
        assigned_to_range_boundaries: Uint8Array,
        requested_replicas: Uint32Array,
    ) -> Result<(), JsValue> {
        let commits = coordinate_commits_from_u64_columns(
            hashes,
            gids,
            hash_numbers,
            coordinate_counts,
            coordinates,
            next_hash_batches,
            assigned_to_range_boundaries,
            requested_replicas,
        )?;
        self.commit_entry_coordinate_commits(commits);
        Ok(())
    }

    pub fn add_gid_peers(
        &mut self,
        gid: String,
        peers: Array,
        reset: bool,
    ) -> Result<usize, JsValue> {
        self.shared_log.add_gid_peers(gid, peers, reset)
    }

    pub fn remove_gid_peer(&mut self, peer: &str, gid: JsValue) -> Result<(), JsValue> {
        self.shared_log.remove_gid_peer(peer, gid)
    }

    pub fn remove_gid_peers(&mut self, peer: &str, gids: Array) -> Result<(), JsValue> {
        self.shared_log.remove_gid_peers(peer, gids)
    }

    pub fn delete_gid_peers(&mut self, gid: &str) -> bool {
        self.shared_log.delete_gid_peers(gid)
    }

    pub fn clear_gid_peers(&mut self) {
        self.shared_log.clear_gid_peers();
    }

    pub fn mark_entries_known_by_peer(
        &mut self,
        hashes: Array,
        peer: String,
    ) -> Result<(), JsValue> {
        self.shared_log.mark_entries_known_by_peer(hashes, peer)
    }

    pub fn remove_entries_known_by_peer(
        &mut self,
        hashes: Array,
        peer: &str,
    ) -> Result<(), JsValue> {
        self.shared_log.remove_entries_known_by_peer(hashes, peer)
    }

    pub fn remove_peer_from_entry_known_peers(&mut self, peer: &str) {
        self.shared_log.remove_peer_from_entry_known_peers(peer);
    }

    pub fn clear_entry_known_peers(&mut self) {
        self.shared_log.clear_entry_known_peers();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_entry_leaders_for_gid(
        &self,
        gid: String,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_entry_leaders_for_gid(
            gid,
            replicas,
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
    pub fn plan_leaders_for_gids_batch(
        &self,
        gids: Array,
        replica_counts: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_leaders_for_gids_batch(
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
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_leader_samples_for_gids_batch(
        &self,
        gids: Array,
        replica_counts: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_leader_samples_for_gids_batch(
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
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_request_prune_leader_hints(
        &self,
        hashes: Array,
        skip_hashes: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let skip_hashes = strings_from_array(skip_hashes)?;
        let skip_hashes = if skip_hashes.is_empty() {
            None
        } else {
            Some(skip_hashes.into_iter().collect::<HashSet<String>>())
        };
        let metadata = self.log.entry_prune_metadata_values(&hashes);
        let entry_rows = Array::new();
        let mut present_block_hashes = Vec::new();
        let mut candidate_hashes = Vec::new();
        let mut candidate_gids = Vec::new();
        let mut candidate_replicas = Vec::new();

        for (hash, metadata) in hashes.iter().zip(metadata) {
            let has_block = self.blocks.has(hash);
            if has_block {
                present_block_hashes.push(hash.clone());
            }

            let Some((gid, data, replicas)) = metadata else {
                continue;
            };
            let requested_replicas = replicas
                .map(|replicas| replicas as usize)
                .or_else(|| self.shared_log.entry_requested_replicas(hash));

            let row = Array::new();
            row.push(&JsValue::from_str(hash));
            row.push(&JsValue::from_str(&gid));
            match requested_replicas {
                Some(replicas) => row.push(&JsValue::from_f64(replicas as f64)),
                None => row.push(&JsValue::UNDEFINED),
            };
            match data.as_ref().filter(|_| requested_replicas.is_none()) {
                Some(data) => row.push(&Uint8Array::from(data.as_slice())),
                None => row.push(&JsValue::UNDEFINED),
            };
            entry_rows.push(&row);

            if has_block
                && skip_hashes
                    .as_ref()
                    .map_or(true, |skip_hashes| !skip_hashes.contains(hash))
            {
                if let Some(replicas) = requested_replicas {
                    candidate_hashes.push(hash.clone());
                    candidate_gids.push(gid);
                    candidate_replicas.push(replicas);
                }
            }
        }

        let local_flags = if candidate_hashes.is_empty() {
            Vec::new()
        } else {
            self.shared_log.local_leader_flags_for_gids_batch(
                &candidate_gids,
                &candidate_replicas,
                role_age_ms,
                &now,
                peer_filter,
                expand_peer_filter,
                &self_hash,
                include_self,
                full_replica_fallback,
                include_strict_full_replica,
            )?
        };
        let local_leader_hashes = candidate_hashes
            .iter()
            .zip(local_flags)
            .filter_map(|(hash, is_local)| is_local.then(|| hash.clone()))
            .collect::<Vec<_>>();

        let out = Array::new();
        out.push(&entry_rows);
        out.push(&strings_to_array(present_block_hashes));
        out.push(&strings_to_array(local_leader_hashes));
        out.push(&strings_to_array(candidate_gids));
        out.push(&strings_to_array(candidate_hashes));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_request_prune_leader_hint_columns(
        &self,
        hashes: Array,
        skip_hashes: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let skip_hashes = strings_from_array(skip_hashes)?;
        let skip_hashes = if skip_hashes.is_empty() {
            None
        } else {
            Some(skip_hashes.into_iter().collect::<HashSet<String>>())
        };
        let metadata = self.log.entry_prune_metadata_values(&hashes);
        let gids = Array::new();
        let data_rows = Array::new();
        let mut replica_counts = Vec::with_capacity(hashes.len());
        let mut present_block_flags = Vec::with_capacity(hashes.len());
        let mut candidate_indexes = Vec::new();
        let mut candidate_gids = Vec::new();
        let mut candidate_replicas = Vec::new();

        for (index, (hash, metadata)) in hashes.iter().zip(metadata).enumerate() {
            let has_block = self.blocks.has(hash);
            present_block_flags.push(u8::from(has_block));

            let Some((gid, data, replicas)) = metadata else {
                gids.push(&JsValue::UNDEFINED);
                data_rows.push(&JsValue::UNDEFINED);
                replica_counts.push(0);
                continue;
            };
            let requested_replicas = replicas
                .map(|replicas| replicas as usize)
                .or_else(|| self.shared_log.entry_requested_replicas(hash));

            gids.push(&JsValue::from_str(&gid));
            replica_counts.push(requested_replicas.unwrap_or(0) as u32);
            match data.as_ref().filter(|_| requested_replicas.is_none()) {
                Some(data) => data_rows.push(&Uint8Array::from(data.as_slice())),
                None => data_rows.push(&JsValue::UNDEFINED),
            };

            if has_block
                && skip_hashes
                    .as_ref()
                    .map_or(true, |skip_hashes| !skip_hashes.contains(hash))
            {
                if let Some(replicas) = requested_replicas {
                    candidate_indexes.push(index);
                    candidate_gids.push(gid);
                    candidate_replicas.push(replicas);
                }
            }
        }

        let local_flags = if candidate_gids.is_empty() {
            Vec::new()
        } else {
            self.shared_log.local_leader_flags_for_gids_batch(
                &candidate_gids,
                &candidate_replicas,
                role_age_ms,
                &now,
                peer_filter,
                expand_peer_filter,
                &self_hash,
                include_self,
                full_replica_fallback,
                include_strict_full_replica,
            )?
        };
        let mut local_leader_flags = vec![0u8; hashes.len()];
        for (index, is_local) in candidate_indexes.iter().zip(local_flags) {
            local_leader_flags[*index] = u8::from(is_local);
        }

        let mut peer_history_removed_flags = vec![0u8; hashes.len()];
        for index in candidate_indexes {
            peer_history_removed_flags[index] = 1;
        }

        let out = Array::new();
        out.push(&gids);
        out.push(&data_rows);
        out.push(&Uint8Array::from(present_block_flags.as_slice()));
        out.push(&Uint8Array::from(local_leader_flags.as_slice()));
        out.push(&Uint32Array::from(replica_counts.as_slice()));
        out.push(&strings_to_array(candidate_gids));
        out.push(&Uint8Array::from(peer_history_removed_flags.as_slice()));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_request_prune_all_confirmed(
        &mut self,
        hashes: Array,
        prune_peer: String,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let peer_history_gids = self.plan_request_prune_all_confirmed_core(
            hashes,
            &prune_peer,
            role_age_ms,
            &now,
            peer_filter,
            expand_peer_filter,
            &self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        let out = Array::new();
        match peer_history_gids {
            Some(peer_history_gids) => {
                out.push(&JsValue::TRUE);
                out.push(&strings_to_array(peer_history_gids));
            }
            None => {
                out.push(&JsValue::FALSE);
                out.push(&Array::new());
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_request_prune_all_confirmed_no_gid_return(
        &mut self,
        hashes: Array,
        prune_peer: String,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<bool, JsValue> {
        let hashes = strings_from_array(hashes)?;
        if self.shared_log.gid_peer_history_empty_core() {
            if let Some(all_confirmed) = self.try_plan_request_prune_full_replica_confirm(
                &hashes,
                role_age_ms,
                &now,
                peer_filter.clone(),
                expand_peer_filter,
                &self_hash,
                include_self,
                full_replica_fallback,
                include_strict_full_replica,
            )? {
                return Ok(all_confirmed);
            }
        }
        Ok(self
            .plan_request_prune_all_confirmed_core(
                hashes,
                &prune_peer,
                role_age_ms,
                &now,
                peer_filter,
                expand_peer_filter,
                &self_hash,
                include_self,
                full_replica_fallback,
                include_strict_full_replica,
            )?
            .is_some())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_entry_assignment_for_gid(
        &self,
        gid: String,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_entry_assignment_for_gid(
            gid,
            replicas,
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
    pub fn plan_repair_dispatch_for_entries(
        &self,
        entry_hashes: Array,
        entry_gids: Array,
        entry_requested_replicas: Array,
        entry_coordinate_batches: Array,
        pending_modes: Array,
        pending_peers_by_mode: Array,
        optimistic_peers_by_mode: Array,
        full_replica_repair_candidates: Array,
        full_replica_repair_candidate_count: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_repair_dispatch_for_entries(
            entry_hashes,
            entry_gids,
            entry_requested_replicas,
            entry_coordinate_batches,
            pending_modes,
            pending_peers_by_mode,
            optimistic_peers_by_mode,
            full_replica_repair_candidates,
            full_replica_repair_candidate_count,
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
    pub fn plan_repair_dispatch_for_resident_entries(
        &self,
        pending_modes: Array,
        pending_peers_by_mode: Array,
        optimistic_gids_by_mode: Array,
        optimistic_peers_by_gid_by_mode: Array,
        full_replica_repair_candidates: Array,
        full_replica_repair_candidate_count: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        self.shared_log.plan_repair_dispatch_for_resident_entries(
            pending_modes,
            pending_peers_by_mode,
            optimistic_gids_by_mode,
            optimistic_peers_by_gid_by_mode,
            full_replica_repair_candidates,
            full_replica_repair_candidate_count,
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
    pub fn plan_local_append_for_gid_compact(
        &mut self,
        entry_hash: String,
        gid: String,
        entry_hash_number: String,
        next_hashes: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hashes_for_core = next_hashes.clone();
        let row = self.shared_log.plan_local_append_for_gid_compact(
            entry_hash,
            gid,
            entry_hash_number,
            next_hashes,
            replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        self.commit_coordinate_core_from_compact_row(
            row.get(3),
            next_hashes_for_core,
            Array::new(),
            0,
            Vec::new(),
        )?;
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_local_append_for_gid_compact(
        &mut self,
        entry_hash: String,
        gid: String,
        entry_hash_number: String,
        next_hashes: Array,
        delete_hashes: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hashes_for_core = next_hashes.clone();
        let delete_hashes_for_core = delete_hashes.clone();
        let row = self.shared_log.commit_local_append_for_gid_compact(
            entry_hash,
            gid,
            entry_hash_number,
            next_hashes,
            delete_hashes,
            replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        self.commit_coordinate_core_from_compact_row(
            row.get(3),
            next_hashes_for_core,
            delete_hashes_for_core,
            0,
            Vec::new(),
        )?;
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_append_for_gid(
        &mut self,
        entry_hash: String,
        gid: String,
        entry_hash_number: String,
        next_hashes: Array,
        replicas: usize,
        full_replica_candidates: Array,
        fallback_recipients: Array,
        delivery_self_hash: String,
        delivery_enabled: bool,
        reliability_ack: bool,
        min_acks: JsValue,
        require_recipients: bool,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hashes_for_core = next_hashes.clone();
        let row = self.shared_log.plan_append_for_gid(
            entry_hash,
            gid,
            entry_hash_number,
            next_hashes,
            replicas,
            full_replica_candidates,
            fallback_recipients,
            delivery_self_hash,
            delivery_enabled,
            reliability_ack,
            min_acks,
            require_recipients,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        self.commit_coordinate_core_from_compact_row(
            row.get(5),
            next_hashes_for_core,
            Array::new(),
            0,
            Vec::new(),
        )?;
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_append_for_gids_batch(
        &mut self,
        entry_hashes: Array,
        gids: Array,
        entry_hash_numbers: Array,
        next_hash_batches: Array,
        replica_counts: Array,
        full_replica_candidates: Array,
        fallback_recipients: Array,
        delivery_self_hash: String,
        delivery_enabled: bool,
        reliability_ack: bool,
        min_acks: JsValue,
        require_recipients: bool,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hash_batches_for_core = next_hash_batches.clone();
        let rows = self.shared_log.plan_append_for_gids_batch(
            entry_hashes,
            gids,
            entry_hash_numbers,
            next_hash_batches,
            replica_counts,
            full_replica_candidates,
            fallback_recipients,
            delivery_self_hash,
            delivery_enabled,
            reliability_ack,
            min_acks,
            require_recipients,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        for index in 0..rows.length() {
            let row = array_from_value(rows.get(index), "append batch plan row")?;
            let next_hashes = array_from_value(
                next_hash_batches_for_core.get(index),
                "append batch next hashes",
            )?;
            self.commit_coordinate_core_from_compact_row(
                row.get(5),
                next_hashes,
                Array::new(),
                0,
                Vec::new(),
            )?;
        }
        Ok(rows)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_receive_coordinates_for_gids_batch(
        &mut self,
        entry_hashes: Array,
        gids: Array,
        entry_hash_numbers: Array,
        next_hash_batches: Array,
        replica_counts: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let next_hash_batches_for_core = next_hash_batches.clone();
        let rows = self.shared_log.plan_receive_coordinates_for_gids_batch(
            entry_hashes,
            gids,
            entry_hash_numbers,
            next_hash_batches,
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
        for index in 0..rows.length() {
            let row = array_from_value(rows.get(index), "receive coordinate batch plan row")?;
            let next_hashes = array_from_value(
                next_hash_batches_for_core.get(index),
                "receive coordinate batch next hashes",
            )?;
            self.commit_coordinate_core_from_compact_row(
                row.get(4),
                next_hashes,
                Array::new(),
                0,
                Vec::new(),
            )?;
        }
        Ok(rows)
    }
}

impl NativePeerbitBackbone {
    pub(crate) fn commit_entry_coordinate_commits(&mut self, commits: Vec<EntryCoordinateCommit>) {
        if !commits.is_empty() {
            self.coordinate_index.reserve(commits.len());
            self.coordinate_values.reserve(commits.len());
        }
        for commit in &commits {
            self.put_coordinate_core(
                commit.hash.clone(),
                &commit.gid,
                commit.hash_number,
                &commit.coordinates,
                commit.assigned_to_range_boundary,
                commit.requested_replicas,
                0,
                Vec::new(),
                true,
            );
            self.delete_coordinate_core_strings(&commit.next_hashes);
        }
        self.shared_log.commit_entry_coordinates_batch_core(commits);
    }

    #[allow(clippy::too_many_arguments)]
    fn try_plan_request_prune_full_replica_confirm(
        &self,
        hashes: &[String],
        role_age_ms: f64,
        now: &str,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: &str,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Option<bool>, JsValue> {
        if hashes.is_empty() {
            return Ok(Some(false));
        }
        let mut common_replicas = None;
        for hash in hashes {
            if !self.blocks.has(hash) {
                return Ok(Some(false));
            }
            let Some((_, replicas)) = self.log.entry_prune_confirm_metadata_ref(hash) else {
                return Ok(Some(false));
            };
            let Some(requested_replicas) = replicas
                .map(|replicas| replicas as usize)
                .or_else(|| self.shared_log.entry_requested_replicas(hash))
            else {
                return Ok(Some(false));
            };
            match common_replicas {
                Some(common_replicas) if common_replicas != requested_replicas => {
                    return Ok(None);
                }
                Some(_) => {}
                None => common_replicas = Some(requested_replicas),
            }
        }

        let Some(common_replicas) = common_replicas else {
            return Ok(Some(false));
        };
        self.shared_log.full_replica_self_leader_for_replicas(
            common_replicas,
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
    fn plan_request_prune_all_confirmed_core(
        &mut self,
        hashes: Vec<String>,
        prune_peer: &str,
        role_age_ms: f64,
        now: &str,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: &str,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Option<Vec<String>>, JsValue> {
        let empty = || Ok(None);
        if hashes.is_empty() {
            return empty();
        }

        let mut candidate_gids = Vec::with_capacity(hashes.len());
        let mut candidate_replicas = Vec::with_capacity(hashes.len());

        for hash in hashes.iter() {
            if !self.blocks.has(hash) {
                return empty();
            }

            let Some((gid, replicas)) = self.log.entry_prune_confirm_metadata_ref(hash) else {
                return empty();
            };
            let Some(requested_replicas) = replicas
                .map(|replicas| replicas as usize)
                .or_else(|| self.shared_log.entry_requested_replicas(hash))
            else {
                return empty();
            };

            candidate_gids.push(gid.to_string());
            candidate_replicas.push(requested_replicas);
        }

        let all_local_leaders = self.shared_log.all_local_leaders_for_gids_batch(
            &candidate_gids,
            &candidate_replicas,
            role_age_ms,
            now,
            peer_filter,
            expand_peer_filter,
            self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )?;
        if !all_local_leaders {
            return empty();
        }

        self.shared_log
            .remove_gid_peers_core(prune_peer, &candidate_gids);
        Ok(Some(candidate_gids))
    }

    pub(crate) fn put_decoded_coordinate_core(
        &mut self,
        coordinate: CoordinateCoreValue,
        record_journal: bool,
    ) -> Result<(), JsValue> {
        self.shared_log.put_entry_coordinates(
            coordinate.hash.clone(),
            coordinate.gid.clone(),
            coordinate.hash_number.to_string(),
            number_strings_to_array(&coordinate.coordinates),
            coordinate.assigned_to_range_boundary,
            coordinate.requested_replicas,
        )?;
        self.put_coordinate_core(
            coordinate.hash,
            &coordinate.gid,
            coordinate.hash_number,
            &coordinate.coordinates,
            coordinate.assigned_to_range_boundary,
            coordinate.requested_replicas,
            coordinate.wall_time,
            coordinate.meta_bytes,
            record_journal,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::BackboneError;

    #[test]
    fn coordinate_array_error_message_matches_historical_string() {
        // `coordinate_numbers_from_array` needs a live JS engine, but its
        // error variant must keep rendering the exact string previously
        // built with `JsValue::from_str`.
        assert_eq!(
            BackboneError::Expected("coordinate string array").to_string(),
            "Expected coordinate string array"
        );
    }
}
