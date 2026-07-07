use js_sys::{Array, Uint8Array};
use peerbit_indexer_core::persistence::{
    decode_journal, decode_key_value_snapshot, encode_key_value_snapshot, JournalRecord,
    JOURNAL_MAGIC,
};
use peerbit_indexer_core::storage::ByteStorage;
use peerbit_indexer_core::wire::{self, WireError};
use peerbit_shared_log_rust::NativeLocalAppendCompactFacts;
use wasm_bindgen::prelude::*;

use crate::error::BackboneError;
use crate::js_interop::{
    append_journal_delete_record, append_journal_put_record, array_from_value, bool_field,
    clear_journal_prefix, number_strings_to_array, parse_u64_string, string_field, stringish_field,
    strings_from_array, usize_field, write_bytes, write_string,
};
use crate::shared_log_plan::coordinate_numbers_from_array;
use crate::NativePeerbitBackbone;

#[derive(Debug)]
pub(crate) struct CoordinateCoreValue {
    pub(crate) hash: String,
    pub(crate) gid: String,
    pub(crate) hash_number: u64,
    pub(crate) coordinates: Vec<u64>,
    pub(crate) assigned_to_range_boundary: bool,
    pub(crate) requested_replicas: usize,
    pub(crate) wall_time: u64,
    pub(crate) meta_bytes: Vec<u8>,
}

fn encode_coordinate_value(
    hash: &str,
    gid: &str,
    hash_number: u64,
    coordinates: &[u64],
    assigned_to_range_boundary: bool,
    requested_replicas: usize,
    wall_time: u64,
    meta_bytes: &[u8],
) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(76 + hash.len() + gid.len() + coordinates.len() * 8 + meta_bytes.len());
    write_string(&mut out, hash);
    write_string(&mut out, gid);
    out.extend_from_slice(&hash_number.to_le_bytes());
    out.push(u8::from(assigned_to_range_boundary));
    out.extend_from_slice(&(requested_replicas as u64).to_le_bytes());
    out.extend_from_slice(&(coordinates.len() as u32).to_le_bytes());
    for coordinate in coordinates {
        out.extend_from_slice(&coordinate.to_le_bytes());
    }
    out.extend_from_slice(&wall_time.to_le_bytes());
    write_bytes(&mut out, meta_bytes);
    out
}

pub(crate) fn coordinate_core_value_to_row(value: &CoordinateCoreValue) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&value.hash));
    row.push(&JsValue::from_str(&value.hash_number.to_string()));
    row.push(&JsValue::from_str(&value.gid));
    row.push(&number_strings_to_array(&value.coordinates));
    row.push(&JsValue::from_bool(value.assigned_to_range_boundary));
    row.push(&JsValue::from_f64(value.requested_replicas as f64));
    row.push(&JsValue::from_str(&value.wall_time.to_string()));
    row.push(&Uint8Array::from(value.meta_bytes.as_slice()));
    row
}

pub(crate) fn decode_coordinate_value(bytes: &[u8]) -> Result<CoordinateCoreValue, BackboneError> {
    Ok(decode_coordinate_value_core(bytes)?)
}

/// Decode a key/value snapshot and apply journal records on top, returning
/// the merged entries in insertion order plus the number of journal
/// operations applied.
pub(crate) fn merged_coordinate_entries(
    snapshot: &[u8],
    journal: &[u8],
) -> Result<(Vec<(String, Vec<u8>)>, usize), BackboneError> {
    let mut entries = if snapshot.is_empty() {
        Default::default()
    } else {
        decode_key_value_snapshot(snapshot)?
    };
    let journal_records = if journal.is_empty() {
        Vec::new()
    } else {
        decode_journal(journal)?
    };
    let operations = journal_records.len();
    for record in journal_records {
        match record {
            JournalRecord {
                key,
                value: Some(value),
                ..
            } => {
                entries.insert(key, value);
            }
            JournalRecord { key, .. } => {
                entries.shift_remove(&key);
            }
        }
    }
    Ok((entries.into_iter().collect(), operations))
}

fn decode_coordinate_value_core(bytes: &[u8]) -> Result<CoordinateCoreValue, WireError> {
    let mut offset = 0usize;
    let hash = wire::read_encoded_string(bytes, &mut offset, "coordinate hash")?;
    let gid = wire::read_encoded_string(bytes, &mut offset, "coordinate gid")?;
    let hash_number = wire::read_u64(bytes, &mut offset, "coordinate hash number")?;
    let assigned_to_range_boundary =
        wire::read_bool(bytes, &mut offset, "assigned to range boundary")?;
    let requested_replicas = wire::read_u64(bytes, &mut offset, "requested replicas")? as usize;
    let coordinate_count = wire::read_u32(bytes, &mut offset, "coordinate count")? as usize;
    if (coordinate_count as u64).saturating_mul(8) > bytes.len().saturating_sub(offset) as u64 {
        return Err(WireError::Truncated("coordinate values"));
    }
    let mut coordinates = Vec::with_capacity(coordinate_count);
    for _ in 0..coordinate_count {
        coordinates.push(wire::read_u64(bytes, &mut offset, "coordinate value")?);
    }
    let (wall_time, meta_bytes) = if offset == bytes.len() {
        (0, Vec::new())
    } else {
        let wall_time = wire::read_u64(bytes, &mut offset, "coordinate wall time")?;
        let meta_bytes = wire::read_bytes(bytes, &mut offset, "coordinate meta bytes")?;
        (wall_time, meta_bytes)
    };
    if offset != bytes.len() {
        return Err(WireError::Trailing("coordinate value"));
    }
    Ok(CoordinateCoreValue {
        hash,
        gid,
        hash_number,
        coordinates,
        assigned_to_range_boundary,
        requested_replicas,
        wall_time,
        meta_bytes,
    })
}

#[wasm_bindgen]
impl NativePeerbitBackbone {
    pub fn coordinate_index_len(&self) -> usize {
        self.coordinate_index.len()
    }

    pub fn coordinate_value_len(&self) -> usize {
        self.coordinate_values.len()
    }

    pub fn coordinate_index_has_hash(&self, hash: &str) -> bool {
        self.coordinate_index.contains(hash)
    }

    pub fn coordinate_journal_header(&self) -> Vec<u8> {
        JOURNAL_MAGIC.to_vec()
    }

    pub fn coordinate_pending_journal_len(&self) -> usize {
        self.coordinate_journal_record_count
    }

    pub fn coordinate_pending_journal_byte_len(&self) -> usize {
        self.coordinate_journal.len()
    }

    pub fn coordinate_journal_enabled(&self) -> bool {
        self.coordinate_journal_enabled
    }

    pub fn set_coordinate_journal_enabled(&mut self, enabled: bool) {
        self.coordinate_journal_enabled = enabled;
        if !enabled {
            self.coordinate_journal.clear();
            self.coordinate_journal_record_count = 0;
        }
    }

    pub fn coordinate_journal(&self) -> Vec<u8> {
        self.coordinate_journal.clone()
    }

    pub fn clear_coordinate_journal(&mut self) {
        self.coordinate_journal.clear();
        self.coordinate_journal_record_count = 0;
    }

    pub fn clear_coordinate_journal_prefix(&mut self, byte_len: usize, record_count: usize) {
        clear_journal_prefix(
            &mut self.coordinate_journal,
            &mut self.coordinate_journal_record_count,
            byte_len,
            record_count,
        );
    }

    pub fn coordinate_snapshot(&self) -> Vec<u8> {
        encode_key_value_snapshot(
            self.coordinate_values
                .entries()
                .into_iter()
                .map(|(key, value)| (key, value)),
        )
    }

    pub fn load_coordinate_snapshot_and_journal(
        &mut self,
        snapshot: Uint8Array,
        journal: Uint8Array,
    ) -> Result<usize, JsValue> {
        let (entries, operations) =
            merged_coordinate_entries(&snapshot.to_vec(), &journal.to_vec())?;
        self.shared_log.clear_entry_coordinates();
        self.clear_coordinate_core();
        for (_, value) in entries {
            let coordinate = decode_coordinate_value(&value)?;
            self.put_decoded_coordinate_core(coordinate, false)?;
        }
        self.coordinate_journal.clear();
        self.coordinate_journal_record_count = 0;
        Ok(operations)
    }
}

impl NativePeerbitBackbone {
    pub(crate) fn clear_coordinate_core(&mut self) {
        self.coordinate_index.clear();
        self.coordinate_values.clear();
        self.coordinate_journal.clear();
        self.coordinate_journal_record_count = 0;
    }

    pub(crate) fn put_coordinate_core_from_parts(
        &mut self,
        hash: String,
        gid: String,
        hash_number: &str,
        coordinates: Array,
        assigned_to_range_boundary: bool,
        requested_replicas: usize,
        wall_time: u64,
        meta_bytes: Vec<u8>,
    ) -> Result<(), BackboneError> {
        let hash_number = parse_u64_string(hash_number, "coordinate hash number")?;
        let coordinates = coordinate_numbers_from_array(coordinates)?;
        self.put_coordinate_core(
            hash,
            &gid,
            hash_number,
            &coordinates,
            assigned_to_range_boundary,
            requested_replicas,
            wall_time,
            meta_bytes,
            true,
        );
        Ok(())
    }

    pub(crate) fn commit_coordinate_core_from_compact_row(
        &mut self,
        coordinate_row: JsValue,
        next_hashes: Array,
        delete_hashes: Array,
        wall_time: u64,
        meta_bytes: Vec<u8>,
    ) -> Result<(), BackboneError> {
        let row = array_from_value(coordinate_row, "coordinate plan row")?;
        let hash = string_field(&row, 0, "coordinate hash")?;
        let hash_number = stringish_field(&row, 1, "coordinate hash number")?;
        let gid = string_field(&row, 2, "coordinate gid")?;
        let coordinates = array_from_value(row.get(3), "coordinate rows")?;
        let assigned_to_range_boundary = bool_field(&row, 4, "assigned to range boundary")?;
        let requested_replicas = usize_field(&row, 5, "requested replicas")?;
        self.put_coordinate_core_from_parts(
            hash,
            gid,
            &hash_number,
            coordinates,
            assigned_to_range_boundary,
            requested_replicas,
            wall_time,
            meta_bytes,
        )?;
        self.delete_coordinate_core_batch(next_hashes)?;
        self.delete_coordinate_core_batch(delete_hashes)
    }

    pub(crate) fn commit_coordinate_core_from_compact_facts(
        &mut self,
        facts: &NativeLocalAppendCompactFacts,
        next_hashes: &[String],
        delete_hashes: &[String],
        wall_time: u64,
        meta_bytes: Vec<u8>,
    ) {
        let coordinate = &facts.coordinate;
        self.put_coordinate_core(
            coordinate.hash.clone(),
            &coordinate.gid,
            coordinate.hash_number,
            &coordinate.coordinates,
            coordinate.assigned_to_range_boundary,
            coordinate.requested_replicas,
            wall_time,
            meta_bytes,
            true,
        );
        let profile_enabled = self.append_profile_enabled;
        let coordinate_delete_started = profile_enabled.then(crate::time::now_ms);
        self.delete_coordinate_core_strings(next_hashes);
        self.delete_coordinate_core_strings(delete_hashes);
        if let Some(started) = coordinate_delete_started {
            self.append_profile.coordinate_delete_ms += crate::time::now_ms() - started;
        }
    }

    pub(crate) fn put_coordinate_core(
        &mut self,
        hash: String,
        gid: &str,
        hash_number: u64,
        coordinates: &[u64],
        assigned_to_range_boundary: bool,
        requested_replicas: usize,
        wall_time: u64,
        meta_bytes: Vec<u8>,
        record_journal: bool,
    ) {
        let profile_enabled = self.append_profile_enabled;
        let value_encode_started = profile_enabled.then(crate::time::now_ms);
        let value = encode_coordinate_value(
            &hash,
            gid,
            hash_number,
            coordinates,
            assigned_to_range_boundary,
            requested_replicas,
            wall_time,
            &meta_bytes,
        );
        if let Some(started) = value_encode_started {
            self.append_profile.coordinate_value_encode_ms += crate::time::now_ms() - started;
        }
        if record_journal && self.coordinate_journal_enabled {
            let journal_started = profile_enabled.then(crate::time::now_ms);
            self.push_coordinate_journal_put(&hash, &value);
            if let Some(started) = journal_started {
                self.append_profile.coordinate_journal_put_ms += crate::time::now_ms() - started;
            }
        }
        let index_put_started = profile_enabled.then(crate::time::now_ms);
        self.coordinate_index.insert(hash.clone());
        if let Some(started) = index_put_started {
            self.append_profile.coordinate_index_put_ms += crate::time::now_ms() - started;
        }
        let value_put_started = profile_enabled.then(crate::time::now_ms);
        self.coordinate_values.put(hash, value);
        if let Some(started) = value_put_started {
            self.append_profile.coordinate_value_put_ms += crate::time::now_ms() - started;
        }
    }

    pub(crate) fn delete_coordinate_core(&mut self, hash: &str) -> bool {
        self.coordinate_index.remove(hash);
        if self.coordinate_journal_enabled {
            self.push_coordinate_journal_delete(hash);
        }
        self.coordinate_values.delete(hash)
    }

    fn push_coordinate_journal_put(&mut self, key: &str, value: &[u8]) {
        append_journal_put_record(&mut self.coordinate_journal, key, value);
        self.coordinate_journal_record_count += 1;
    }

    fn push_coordinate_journal_delete(&mut self, key: &str) {
        append_journal_delete_record(&mut self.coordinate_journal, key);
        self.coordinate_journal_record_count += 1;
    }

    pub(crate) fn delete_coordinate_core_batch(
        &mut self,
        hashes: Array,
    ) -> Result<(), BackboneError> {
        for hash in strings_from_array(hashes)? {
            self.delete_coordinate_core(&hash);
        }
        Ok(())
    }

    pub(crate) fn delete_coordinate_core_strings(&mut self, hashes: &[String]) {
        for hash in hashes {
            self.delete_coordinate_core(hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_coordinate_value, decode_coordinate_value_core, encode_coordinate_value,
        merged_coordinate_entries,
    };
    use crate::error::BackboneError;
    use peerbit_indexer_core::persistence::{
        encode_journal_delete_record, encode_journal_put_record, encode_journal_record,
        encode_key_value_snapshot, DecodeError,
    };
    use peerbit_indexer_core::wire::WireError;

    #[test]
    fn coordinate_value_round_trips() {
        let bytes = encode_coordinate_value("hash", "gid", 7, &[1, 2], true, 3, 11, &[9]);
        let valid = decode_coordinate_value_core(&bytes).unwrap();
        assert_eq!(valid.hash, "hash");
        assert_eq!(valid.gid, "gid");
        assert_eq!(valid.hash_number, 7);
        assert_eq!(valid.coordinates, vec![1, 2]);
        assert!(valid.assigned_to_range_boundary);
        assert_eq!(valid.requested_replicas, 3);
        assert_eq!(valid.wall_time, 11);
        assert_eq!(valid.meta_bytes, vec![9]);
    }

    #[test]
    fn corrupt_coordinate_count_errors_instead_of_aborting() {
        let mut bytes = encode_coordinate_value("hash", "gid", 7, &[1, 2], true, 3, 11, &[9]);
        // The count sits after the hash and gid strings, the u64 hash number,
        // the boundary flag byte, and the u64 requested replicas.
        let count_offset = 4 + "hash".len() + 4 + "gid".len() + 8 + 1 + 8;
        bytes[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());

        assert!(matches!(
            decode_coordinate_value_core(&bytes),
            Err(WireError::Truncated("coordinate values"))
        ));
    }

    #[test]
    fn decode_coordinate_value_reports_typed_wire_errors() {
        let mut bytes = encode_coordinate_value("hash", "gid", 7, &[1, 2], true, 3, 11, &[9]);
        bytes.truncate(4);

        let error = decode_coordinate_value(&bytes).unwrap_err();
        assert_eq!(
            error,
            BackboneError::Wire(WireError::Truncated("coordinate hash"))
        );
        assert_eq!(error.to_string(), "Truncated coordinate hash");
    }

    #[test]
    fn merges_snapshot_entries_with_journal_records() {
        let snapshot = encode_key_value_snapshot(
            [
                ("a".to_string(), vec![1u8]),
                ("b".to_string(), vec![2u8]),
                ("c".to_string(), vec![3u8]),
            ]
            .into_iter(),
        );
        let mut journal = encode_journal_put_record("b", &[42]);
        journal.extend_from_slice(&encode_journal_delete_record("c"));
        journal.extend_from_slice(&encode_journal_put_record("d", &[4]));

        let (entries, operations) = merged_coordinate_entries(&snapshot, &journal).unwrap();
        assert_eq!(operations, 3);
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), vec![1u8]),
                ("b".to_string(), vec![42u8]),
                ("d".to_string(), vec![4u8]),
            ]
        );

        let (entries, operations) = merged_coordinate_entries(&[], &[]).unwrap();
        assert_eq!(operations, 0);
        assert!(entries.is_empty());
    }

    #[test]
    fn merged_coordinate_entries_reports_typed_decode_errors() {
        // The rendered message must equal the `DecodeError` Display output the
        // old `decode_error` funnel flattened into an untyped string.
        let snapshot = encode_key_value_snapshot([("a".to_string(), vec![1u8])].into_iter());
        let truncated = &snapshot[..snapshot.len() - 1];
        let expected = match super::decode_key_value_snapshot(truncated) {
            Err(error) => error,
            Ok(_) => panic!("snapshot decode must fail"),
        };

        let error = merged_coordinate_entries(truncated, &[]).unwrap_err();
        assert_eq!(error, BackboneError::Decode(expected.clone()));
        assert_eq!(error.to_string(), expected.to_string());

        let bad_journal = encode_journal_record(&[9]);
        let error = merged_coordinate_entries(&[], &bad_journal).unwrap_err();
        assert_eq!(
            error,
            BackboneError::Decode(DecodeError::InvalidOperation(9))
        );
        assert_eq!(error.to_string(), "invalid operation 9");
    }
}
