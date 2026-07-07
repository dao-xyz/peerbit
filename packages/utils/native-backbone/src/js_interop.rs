use crate::error::BackboneError;
use js_sys::{Array, Reflect, Uint8Array};
use peerbit_indexer_core::wire;
use std::collections::HashSet;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

/// Drops only the first `byte_len` bytes / `record_count` records so records
/// appended while a flush was awaiting its disk write are kept for the next
/// flush instead of being discarded.
pub(crate) fn clear_journal_prefix(
    journal: &mut Vec<u8>,
    journal_record_count: &mut usize,
    byte_len: usize,
    record_count: usize,
) {
    if byte_len >= journal.len() {
        journal.clear();
        *journal_record_count = 0;
        return;
    }
    journal.drain(..byte_len);
    *journal_record_count = journal_record_count.saturating_sub(record_count);
}

/// Exclusive upper bound for f64 → u64 conversion. `u64::MAX as f64` rounds
/// UP to 2^64 (exactly representable), so a `> u64::MAX as f64` check admits
/// 2^64 itself, which an `as` cast then saturates to `u64::MAX`. Valid u64
/// values in f64 form are exactly the integers in [0, 2^64).
const F64_U64_EXCLUSIVE_BOUND: f64 = 18_446_744_073_709_551_616.0; // 2^64

/// Rejects non-integral, negative, non-finite and out-of-range values instead
/// of silently truncating them with an `as` cast.
pub(crate) fn checked_usize_from_f64(value: f64) -> Option<usize> {
    checked_u64_from_f64(value).and_then(|v| usize::try_from(v).ok())
}

/// Rejects non-integral, negative, non-finite and out-of-range values instead
/// of silently truncating them with an `as` cast.
pub(crate) fn checked_u64_from_f64(value: f64) -> Option<u64> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value >= F64_U64_EXCLUSIVE_BOUND
    {
        return None;
    }
    Some(value as u64)
}

pub(crate) fn array_from_value(
    value: JsValue,
    label: &'static str,
) -> Result<Array, BackboneError> {
    value
        .dyn_into::<Array>()
        .map_err(|_| BackboneError::ExpectedArray(label))
}

pub(crate) fn string_field(
    row: &Array,
    index: u32,
    label: &'static str,
) -> Result<String, BackboneError> {
    row.get(index)
        .as_string()
        .ok_or(BackboneError::ExpectedString(label))
}

pub(crate) fn stringish_field(
    row: &Array,
    index: u32,
    label: &'static str,
) -> Result<String, BackboneError> {
    let value = row.get(index);
    if let Some(value) = value.as_string() {
        return Ok(value);
    }
    if let Some(value) = value.as_f64() {
        if let Some(value) = checked_u64_from_f64(value) {
            return Ok(value.to_string());
        }
    }
    Err(BackboneError::ExpectedString(label))
}

pub(crate) fn bool_field(
    row: &Array,
    index: u32,
    label: &'static str,
) -> Result<bool, BackboneError> {
    row.get(index)
        .as_bool()
        .ok_or(BackboneError::ExpectedBoolean(label))
}

pub(crate) fn usize_field(
    row: &Array,
    index: u32,
    label: &'static str,
) -> Result<usize, BackboneError> {
    row.get(index)
        .as_f64()
        .and_then(checked_usize_from_f64)
        .ok_or(BackboneError::ExpectedNumber(label))
}

pub(crate) fn bytes_field(
    row: &Array,
    index: u32,
    label: &'static str,
) -> Result<Vec<u8>, BackboneError> {
    row.get(index)
        .dyn_ref::<Uint8Array>()
        .map(Uint8Array::to_vec)
        .ok_or(BackboneError::ExpectedBytes(label))
}

pub(crate) fn trim_hashes_vec(trim_rows: &Array) -> Result<Vec<String>, BackboneError> {
    let mut hashes = Vec::with_capacity(trim_rows.length() as usize);
    for index in 0..trim_rows.length() {
        let row = array_from_value(trim_rows.get(index), "trim row")?;
        hashes.push(string_field(&row, 0, "trim hash")?);
    }
    Ok(hashes)
}

pub(crate) fn numbers_to_rows(resolution: &str, values: &[u64]) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&number_to_row(resolution, *value));
    }
    out
}

pub(crate) fn number_to_row(resolution: &str, value: u64) -> JsValue {
    match resolution {
        "u64" => JsValue::from_str(&value.to_string()),
        _ => JsValue::from_f64(value as f64),
    }
}

pub(crate) fn strings_to_array(values: Vec<String>) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&JsValue::from_str(&value));
    }
    out
}

pub(crate) fn has_duplicate_strings(values: &[String]) -> bool {
    let mut seen = HashSet::with_capacity(values.len());
    values.iter().any(|value| !seen.insert(value))
}

pub(crate) fn strings_slice_to_array(values: &[String]) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&JsValue::from_str(value));
    }
    out
}

pub(crate) fn strings_from_array(values: Array) -> Result<Vec<String>, BackboneError> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        out.push(
            values
                .get(index)
                .as_string()
                .ok_or(BackboneError::ExpectedStringArray)?,
        );
    }
    Ok(out)
}

pub(crate) fn bytes_vec_from_array(values: Array) -> Result<Vec<Vec<u8>>, BackboneError> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values.get(index);
        out.push(
            value
                .dyn_ref::<Uint8Array>()
                .map(Uint8Array::to_vec)
                .ok_or(BackboneError::ExpectedBytesArray)?,
        );
    }
    Ok(out)
}

pub(crate) fn required_bytes_from_array(
    values: &Array,
    index: u32,
    field: &'static str,
) -> Result<Uint8Array, BackboneError> {
    values
        .get(index)
        .dyn_into::<Uint8Array>()
        .map_err(|_| BackboneError::ExpectedBytes(field))
}

pub(crate) fn string_batches_from_array(
    values: Array,
    label: &'static str,
) -> Result<Vec<Vec<String>>, BackboneError> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values.get(index);
        if !Array::is_array(&value) {
            return Err(BackboneError::Expected(label));
        }
        out.push(strings_from_array(Array::from(&value))?);
    }
    Ok(out)
}

pub(crate) fn usize_values_from_array(values: Array) -> Result<Vec<usize>, BackboneError> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values
            .get(index)
            .as_f64()
            .and_then(checked_usize_from_f64)
            .ok_or(BackboneError::ExpectedUnsignedIntegerArray)?;
        out.push(value);
    }
    Ok(out)
}

pub(crate) fn ensure_same_len(
    left: usize,
    right: usize,
    label: &'static str,
) -> Result<(), BackboneError> {
    if left == right {
        Ok(())
    } else {
        Err(BackboneError::MismatchedInputLengths(label))
    }
}

pub(crate) fn optional_bytes_from_js(
    value: JsValue,
    label: &'static str,
) -> Result<Option<Vec<u8>>, BackboneError> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    value
        .dyn_ref::<Uint8Array>()
        .map(|value| Some(value.to_vec()))
        .ok_or(BackboneError::ExpectedBytes(label))
}

pub(crate) fn optional_usize_from_js(
    value: JsValue,
    label: &'static str,
) -> Result<Option<usize>, BackboneError> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    value
        .as_f64()
        .and_then(checked_usize_from_f64)
        .map(Some)
        .ok_or(BackboneError::MustBeNumber(label))
}

pub(crate) fn number_strings_to_array(values: &[u64]) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&JsValue::from_str(&value.to_string()));
    }
    out
}

pub(crate) fn parse_u64_string(value: &str, label: &'static str) -> Result<u64, BackboneError> {
    value
        .parse::<u64>()
        .map_err(|_| BackboneError::ExpectedU64String(label))
}

pub(crate) fn parse_optional_u64_string(
    value: &str,
    label: &'static str,
) -> Result<Option<u64>, BackboneError> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_u64_string(value, label).map(Some)
    }
}

const JOURNAL_PUT_OPERATION: u8 = 1;
const JOURNAL_DELETE_OPERATION: u8 = 2;

/// Writes the same wire format as
/// `peerbit_indexer_core::persistence::encode_journal_put_record`, but
/// directly into `out` — the journal push runs once per append transaction,
/// so the intermediate payload/record allocations are avoided on purpose
/// (see the `journal record encoding matches indexer core` parity test).
pub(crate) fn append_journal_put_record(out: &mut Vec<u8>, key: &str, value: &[u8]) {
    let payload_len = 1usize + 4 + key.len() + 4 + value.len();
    let key_len = (key.len() as u32).to_le_bytes();
    let value_len = (value.len() as u32).to_le_bytes();
    let checksum = fnv1a_parts([
        &[JOURNAL_PUT_OPERATION][..],
        key_len.as_slice(),
        key.as_bytes(),
        value_len.as_slice(),
        value,
    ]);
    out.reserve(8 + payload_len);
    out.extend_from_slice(&(payload_len as u32).to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.push(JOURNAL_PUT_OPERATION);
    out.extend_from_slice(&key_len);
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(&value_len);
    out.extend_from_slice(value);
}

/// Allocation-free equivalent of
/// `peerbit_indexer_core::persistence::encode_journal_delete_record`.
pub(crate) fn append_journal_delete_record(out: &mut Vec<u8>, key: &str) {
    let payload_len = 1usize + 4 + key.len();
    let key_len = (key.len() as u32).to_le_bytes();
    let checksum = fnv1a_parts([
        &[JOURNAL_DELETE_OPERATION][..],
        key_len.as_slice(),
        key.as_bytes(),
    ]);
    out.reserve(8 + payload_len);
    out.extend_from_slice(&(payload_len as u32).to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.push(JOURNAL_DELETE_OPERATION);
    out.extend_from_slice(&key_len);
    out.extend_from_slice(key.as_bytes());
}

/// FNV-1a over concatenated parts; must stay identical to
/// `peerbit_indexer_core::persistence::fnv1a` over the joined bytes.
fn fnv1a_parts<const N: usize>(parts: [&[u8]; N]) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for part in parts {
        for byte in part {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(0x01000193);
        }
    }
    hash
}

pub(crate) fn write_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}

pub(crate) fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

pub(crate) fn read_u32(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<u32, BackboneError> {
    Ok(wire::read_u32(bytes, offset, label)?)
}

pub(crate) fn read_u64(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<u64, BackboneError> {
    Ok(wire::read_u64(bytes, offset, label)?)
}

pub(crate) fn read_encoded_string(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<String, BackboneError> {
    Ok(wire::read_encoded_string(bytes, offset, label)?)
}

pub(crate) fn read_bytes(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<Vec<u8>, BackboneError> {
    Ok(wire::read_bytes(bytes, offset, label)?)
}

pub(crate) fn js_get(value: &JsValue, key: &str) -> JsValue {
    Reflect::get(value, &JsValue::from_str(key)).unwrap_or(JsValue::UNDEFINED)
}

fn js_string(value: JsValue, field: &'static str) -> Result<String, BackboneError> {
    value
        .as_string()
        .ok_or(BackboneError::MissingOrInvalid(field))
}

pub(crate) fn array_strings(
    value: JsValue,
    field: &'static str,
) -> Result<Vec<String>, BackboneError> {
    if !Array::is_array(&value) {
        return Err(BackboneError::MustBeArray(field));
    }
    let array = Array::from(&value);
    let mut out = Vec::with_capacity(array.length() as usize);
    for index in 0..array.length() {
        out.push(js_string(array.get(index), field)?);
    }
    Ok(out)
}

pub(crate) fn optional_string(
    value: JsValue,
    field: &'static str,
) -> Result<Option<String>, BackboneError> {
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    value
        .as_string()
        .map(Some)
        .ok_or(BackboneError::MissingOrInvalid(field))
}

pub(crate) fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(crate) fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(if value { 1 } else { 0 });
}

pub(crate) fn hash_number_u64(resolution: &str, digest: &[u8]) -> Result<u64, BackboneError> {
    match resolution {
        "u32" => {
            if digest.len() < 4 {
                return Err(BackboneError::HashDigestTooShortU32);
            }
            Ok(u32::from_le_bytes(digest[0..4].try_into().unwrap()) as u64)
        }
        "u64" => {
            if digest.len() < 8 {
                return Err(BackboneError::HashDigestTooShortU64);
            }
            Ok(u64::from_le_bytes(digest[0..8].try_into().unwrap()))
        }
        _ => Err(BackboneError::ResolutionMustBeU32OrU64),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        append_journal_delete_record, append_journal_put_record, checked_u64_from_f64,
        checked_usize_from_f64, ensure_same_len, hash_number_u64, parse_optional_u64_string,
        parse_u64_string, read_u32,
    };
    use crate::error::BackboneError;
    use peerbit_indexer_core::persistence::{
        encode_journal_delete_record, encode_journal_put_record,
    };
    use peerbit_indexer_core::wire::WireError;

    #[test]
    fn decodes_hash_numbers_like_shared_log_integer_helpers() {
        let bytes = [1, 0, 0, 0, 2, 0, 0, 0];
        assert_eq!(hash_number_u64("u32", &bytes).unwrap(), 1);
        assert_eq!(hash_number_u64("u64", &bytes).unwrap(), 8_589_934_593);
    }

    #[test]
    fn hash_number_u64_reports_typed_errors() {
        let error = hash_number_u64("u32", &[1, 2, 3]).unwrap_err();
        assert_eq!(error, BackboneError::HashDigestTooShortU32);
        assert_eq!(error.to_string(), "hash digest must have at least 4 bytes");

        let error = hash_number_u64("u64", &[1, 2, 3, 4]).unwrap_err();
        assert_eq!(error, BackboneError::HashDigestTooShortU64);
        assert_eq!(error.to_string(), "hash digest must have at least 8 bytes");

        let error = hash_number_u64("u128", &[0; 16]).unwrap_err();
        assert_eq!(error, BackboneError::ResolutionMustBeU32OrU64);
        assert_eq!(error.to_string(), "resolution must be u32 or u64");
    }

    #[test]
    fn wire_reads_report_typed_errors() {
        let mut offset = 0usize;
        let error = read_u32(&[1, 2], &mut offset, "coordinate count").unwrap_err();
        assert_eq!(
            error,
            BackboneError::Wire(WireError::Truncated("coordinate count"))
        );
        assert_eq!(error.to_string(), "Truncated coordinate count");
    }

    #[test]
    fn parse_u64_string_reports_typed_errors() {
        assert_eq!(parse_u64_string("42", "coordinate").unwrap(), 42);
        assert_eq!(parse_optional_u64_string("", "coordinate").unwrap(), None);

        let error = parse_u64_string("not-a-number", "coordinate").unwrap_err();
        assert_eq!(error, BackboneError::ExpectedU64String("coordinate"));
        assert_eq!(error.to_string(), "Expected coordinate u64 string");
    }

    #[test]
    fn ensure_same_len_reports_typed_errors() {
        assert_eq!(ensure_same_len(2, 2, "batch gids"), Ok(()));

        let error = ensure_same_len(1, 2, "batch gids").unwrap_err();
        assert_eq!(error, BackboneError::MismatchedInputLengths("batch gids"));
        assert_eq!(error.to_string(), "Mismatched batch gids input lengths");
    }

    #[test]
    fn checked_integer_conversions_reject_invalid_numbers() {
        assert_eq!(checked_usize_from_f64(0.0), Some(0));
        assert_eq!(checked_usize_from_f64(3.0), Some(3));
        assert_eq!(checked_usize_from_f64(-1.0), None);
        assert_eq!(checked_usize_from_f64(1.5), None);
        assert_eq!(checked_usize_from_f64(f64::NAN), None);
        assert_eq!(checked_usize_from_f64(f64::INFINITY), None);
        assert_eq!(checked_usize_from_f64(1e20), None);

        assert_eq!(checked_u64_from_f64(42.0), Some(42));
        assert_eq!(checked_u64_from_f64(-0.5), None);
        assert_eq!(checked_u64_from_f64(f64::NEG_INFINITY), None);
        assert_eq!(checked_u64_from_f64(1e20), None);
    }

    #[test]
    fn checked_integer_conversions_handle_the_two_pow_64_boundary() {
        // 2^64 is exactly representable and equals `u64::MAX as f64` after
        // rounding; it must be rejected, not saturated to u64::MAX.
        let two_pow_64 = 18_446_744_073_709_551_616.0_f64;
        assert_eq!(checked_u64_from_f64(two_pow_64), None);
        assert_eq!(checked_u64_from_f64(u64::MAX as f64), None);
        assert_eq!(checked_usize_from_f64(two_pow_64), None);
        // The largest f64 strictly below 2^64 is a valid u64.
        let below = 18_446_744_073_709_549_568.0_f64; // 2^64 - 2048
        assert_eq!(
            checked_u64_from_f64(below),
            Some(18_446_744_073_709_549_568)
        );
        // 2^53 region is unaffected.
        let two_pow_53 = 9_007_199_254_740_992.0_f64;
        assert_eq!(checked_u64_from_f64(two_pow_53), Some(1 << 53));
    }

    #[test]
    fn journal_record_encoding_matches_indexer_core() {
        for (key, value) in [
            ("", &[][..]),
            ("k", &[0u8][..]),
            ("some-journal-key", &[1u8, 2, 3, 255, 0, 42][..]),
            ("zAbc123", &[7u8; 1200][..]),
        ] {
            let mut direct = vec![9u8, 9, 9];
            append_journal_put_record(&mut direct, key, value);
            let mut expected = vec![9u8, 9, 9];
            expected.extend_from_slice(&encode_journal_put_record(key, value));
            assert_eq!(direct, expected, "put record for key {key:?}");

            let mut direct = Vec::new();
            append_journal_delete_record(&mut direct, key);
            assert_eq!(
                direct,
                encode_journal_delete_record(key),
                "delete record for key {key:?}"
            );
        }
    }
}
