use js_sys::{Array, Reflect, Uint8Array};
use peerbit_indexer_core::persistence::{encode_journal_delete_record, encode_journal_put_record};
use peerbit_indexer_core::wire::{self, WireError};
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

pub(crate) fn array_from_value(value: JsValue, label: &str) -> Result<Array, JsValue> {
    value
        .dyn_into::<Array>()
        .map_err(|_| JsValue::from_str(&format!("Expected {label} array")))
}

pub(crate) fn string_field(row: &Array, index: u32, label: &str) -> Result<String, JsValue> {
    row.get(index)
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("Expected {label} string")))
}

pub(crate) fn stringish_field(row: &Array, index: u32, label: &str) -> Result<String, JsValue> {
    let value = row.get(index);
    if let Some(value) = value.as_string() {
        return Ok(value);
    }
    if let Some(value) = value.as_f64() {
        return Ok((value as u64).to_string());
    }
    Err(JsValue::from_str(&format!("Expected {label} string")))
}

pub(crate) fn bool_field(row: &Array, index: u32, label: &str) -> Result<bool, JsValue> {
    row.get(index)
        .as_bool()
        .ok_or_else(|| JsValue::from_str(&format!("Expected {label} boolean")))
}

pub(crate) fn usize_field(row: &Array, index: u32, label: &str) -> Result<usize, JsValue> {
    row.get(index)
        .as_f64()
        .map(|value| value as usize)
        .ok_or_else(|| JsValue::from_str(&format!("Expected {label} number")))
}

pub(crate) fn bytes_field(row: &Array, index: u32, label: &str) -> Result<Vec<u8>, JsValue> {
    let value = row.get(index);
    if value.is_undefined() || value.is_null() {
        return Err(JsValue::from_str(&format!("Expected {label} bytes")));
    }
    Ok(Uint8Array::new(&value).to_vec())
}

pub(crate) fn trim_hashes_vec(trim_rows: &Array) -> Result<Vec<String>, JsValue> {
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

pub(crate) fn strings_from_array(values: Array) -> Result<Vec<String>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        out.push(
            values
                .get(index)
                .as_string()
                .ok_or_else(|| JsValue::from_str("Expected string array"))?,
        );
    }
    Ok(out)
}

pub(crate) fn bytes_vec_from_array(values: Array) -> Result<Vec<Vec<u8>>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values.get(index);
        if value.is_undefined() || value.is_null() {
            return Err(JsValue::from_str("Expected bytes array"));
        }
        out.push(Uint8Array::new(&value).to_vec());
    }
    Ok(out)
}

pub(crate) fn required_bytes_from_array(
    values: &Array,
    index: u32,
    field: &str,
) -> Result<Uint8Array, JsValue> {
    let value = values.get(index);
    if value.is_undefined() || value.is_null() {
        return Err(JsValue::from_str(&format!("Expected {field} bytes")));
    }
    Ok(Uint8Array::new(&value))
}

pub(crate) fn string_batches_from_array(
    values: Array,
    label: &str,
) -> Result<Vec<Vec<String>>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values.get(index);
        if !Array::is_array(&value) {
            return Err(JsValue::from_str(&format!("Expected {label}")));
        }
        out.push(strings_from_array(Array::from(&value))?);
    }
    Ok(out)
}

pub(crate) fn usize_values_from_array(values: Array) -> Result<Vec<usize>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for index in 0..values.length() {
        let value = values
            .get(index)
            .as_f64()
            .ok_or_else(|| JsValue::from_str("Expected unsigned integer array"))?;
        if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
            return Err(JsValue::from_str("Expected unsigned integer array"));
        }
        out.push(value as usize);
    }
    Ok(out)
}

pub(crate) fn ensure_same_len(left: usize, right: usize, label: &str) -> Result<(), JsValue> {
    if left == right {
        Ok(())
    } else {
        Err(JsValue::from_str(&format!(
            "Mismatched {label} input lengths"
        )))
    }
}

pub(crate) fn optional_bytes_from_js(value: JsValue) -> Option<Vec<u8>> {
    if value.is_undefined() || value.is_null() {
        return None;
    }
    Some(Uint8Array::new(&value).to_vec())
}

pub(crate) fn optional_usize_from_js(
    value: JsValue,
    label: &str,
) -> Result<Option<usize>, JsValue> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    value
        .as_f64()
        .map(|value| Some(value as usize))
        .ok_or_else(|| JsValue::from_str(&format!("{label} must be a number")))
}

pub(crate) fn number_strings_to_array(values: &[u64]) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&JsValue::from_str(&value.to_string()));
    }
    out
}

pub(crate) fn parse_u64_string(value: &str, label: &str) -> Result<u64, JsValue> {
    value
        .parse::<u64>()
        .map_err(|_| JsValue::from_str(&format!("Expected {label} u64 string")))
}

pub(crate) fn parse_optional_u64_string(value: &str, label: &str) -> Result<Option<u64>, JsValue> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_u64_string(value, label).map(Some)
    }
}

pub(crate) fn append_journal_put_record(out: &mut Vec<u8>, key: &str, value: &[u8]) {
    out.extend_from_slice(&encode_journal_put_record(key, value));
}

pub(crate) fn append_journal_delete_record(out: &mut Vec<u8>, key: &str) {
    out.extend_from_slice(&encode_journal_delete_record(key));
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

pub(crate) fn wire_error_to_js(error: WireError) -> JsValue {
    JsValue::from_str(&error.to_string())
}

pub(crate) fn read_u32(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<u32, JsValue> {
    wire::read_u32(bytes, offset, label).map_err(wire_error_to_js)
}

pub(crate) fn read_u64(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<u64, JsValue> {
    wire::read_u64(bytes, offset, label).map_err(wire_error_to_js)
}

pub(crate) fn read_encoded_string(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<String, JsValue> {
    wire::read_encoded_string(bytes, offset, label).map_err(wire_error_to_js)
}

pub(crate) fn read_bytes(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<Vec<u8>, JsValue> {
    wire::read_bytes(bytes, offset, label).map_err(wire_error_to_js)
}

pub(crate) fn js_get(value: &JsValue, key: &str) -> JsValue {
    Reflect::get(value, &JsValue::from_str(key)).unwrap_or(JsValue::UNDEFINED)
}

fn js_string(value: JsValue, field: &str) -> Result<String, JsValue> {
    value
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("Missing or invalid {field}")))
}

pub(crate) fn array_strings(value: JsValue, field: &str) -> Result<Vec<String>, JsValue> {
    if !Array::is_array(&value) {
        return Err(JsValue::from_str(&format!("{field} must be an array")));
    }
    let array = Array::from(&value);
    let mut out = Vec::with_capacity(array.length() as usize);
    for index in 0..array.length() {
        out.push(js_string(array.get(index), field)?);
    }
    Ok(out)
}

pub(crate) fn optional_string(value: JsValue) -> Option<String> {
    if value.is_null() || value.is_undefined() {
        None
    } else {
        value.as_string()
    }
}

pub(crate) fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(crate) fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(if value { 1 } else { 0 });
}

pub(crate) fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

pub(crate) fn decode_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

pub(crate) fn hash_number_u64(resolution: &str, digest: &[u8]) -> Result<u64, JsValue> {
    match resolution {
        "u32" => {
            if digest.len() < 4 {
                return Err(JsValue::from_str("hash digest must have at least 4 bytes"));
            }
            Ok(u32::from_le_bytes(digest[0..4].try_into().unwrap()) as u64)
        }
        "u64" => {
            if digest.len() < 8 {
                return Err(JsValue::from_str("hash digest must have at least 8 bytes"));
            }
            Ok(u64::from_le_bytes(digest[0..8].try_into().unwrap()))
        }
        _ => Err(JsValue::from_str("resolution must be u32 or u64")),
    }
}

#[cfg(test)]
mod tests {
    use super::hash_number_u64;

    #[test]
    fn decodes_hash_numbers_like_shared_log_integer_helpers() {
        let bytes = [1, 0, 0, 0, 2, 0, 0, 0];
        assert_eq!(hash_number_u64("u32", &bytes).unwrap(), 1);
        assert_eq!(hash_number_u64("u64", &bytes).unwrap(), 8_589_934_593);
    }
}
