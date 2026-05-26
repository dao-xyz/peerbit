use js_sys::{Array, Reflect, Uint32Array, Uint8Array};
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

fn parse_u64(value: &str, field: &str) -> Result<u64, JsValue> {
    value
        .parse::<u64>()
        .map_err(|_| JsValue::from_str(&format!("Invalid {field} value")))
}

fn write_u32_le(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64_le(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(if value { 1 } else { 0 });
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    write_u32_le(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    write_u32_le(out, value.len() as u32);
    out.extend_from_slice(value);
}

fn read_u32_le(bytes: &[u8], offset: &mut usize) -> Result<u32, JsValue> {
    let end = *offset + 4;
    if end > bytes.len() {
        return Err(JsValue::from_str("Unexpected end while reading u32"));
    }
    let value = u32::from_le_bytes(bytes[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_u64_le(bytes: &[u8], offset: &mut usize) -> Result<u64, JsValue> {
    let end = *offset + 8;
    if end > bytes.len() {
        return Err(JsValue::from_str("Unexpected end while reading u64"));
    }
    let value = u64::from_le_bytes(bytes[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8, JsValue> {
    if *offset >= bytes.len() {
        return Err(JsValue::from_str("Unexpected end while reading u8"));
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_bool(bytes: &[u8], offset: &mut usize) -> Result<bool, JsValue> {
    match read_u8(bytes, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(JsValue::from_str("Invalid bool value")),
    }
}

fn read_bytes<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8], JsValue> {
    let len = read_u32_le(bytes, offset)? as usize;
    let end = *offset + len;
    if end > bytes.len() {
        return Err(JsValue::from_str("Unexpected end while reading bytes"));
    }
    let value = &bytes[*offset..end];
    *offset = end;
    Ok(value)
}

fn read_string(bytes: &[u8], offset: &mut usize) -> Result<String, JsValue> {
    let raw = read_bytes(bytes, offset)?;
    std::str::from_utf8(raw)
        .map(|value| value.to_string())
        .map_err(|_| JsValue::from_str("Invalid UTF-8 string"))
}

fn js_get(value: &JsValue, key: &str) -> JsValue {
    Reflect::get(value, &JsValue::from_str(key)).unwrap_or(JsValue::UNDEFINED)
}

fn js_string(value: JsValue, field: &str) -> Result<String, JsValue> {
    value
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("Missing or invalid {field}")))
}

fn array_strings(value: JsValue, field: &str) -> Result<Vec<String>, JsValue> {
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

fn optional_string(value: JsValue) -> Option<String> {
    if value.is_null() || value.is_undefined() {
        None
    } else {
        value.as_string()
    }
}

#[derive(Clone, Debug)]
enum ProjectionValue {
    String(String),
    U8(u8),
    U32(u32),
    U64(u64),
    Bool(bool),
    Bytes(Vec<u8>),
    None,
}

fn skip_value(bytes: &[u8], offset: &mut usize, kind: &str) -> Result<(), JsValue> {
    match kind {
        "string" => {
            read_string(bytes, offset)?;
        }
        "u8" => {
            read_u8(bytes, offset)?;
        }
        "u32" => {
            read_u32_le(bytes, offset)?;
        }
        "u64" => {
            read_u64_le(bytes, offset)?;
        }
        "bool" => {
            read_bool(bytes, offset)?;
        }
        "bytes" => {
            read_bytes(bytes, offset)?;
        }
        "option:string" | "option:u8" | "option:u32" | "option:u64" | "option:bool"
        | "option:bytes" => {
            let has_value = read_u8(bytes, offset)?;
            if has_value == 1 {
                skip_value(bytes, offset, &kind["option:".len()..])?;
            } else if has_value != 0 {
                return Err(JsValue::from_str("Invalid option marker"));
            }
        }
        "vec:string" => {
            let len = read_u32_le(bytes, offset)?;
            for _ in 0..len {
                read_string(bytes, offset)?;
            }
        }
        "vec:bytes" => {
            let len = read_u32_le(bytes, offset)?;
            for _ in 0..len {
                read_bytes(bytes, offset)?;
            }
        }
        _ => return Err(JsValue::from_str("Unsupported document field type")),
    }
    Ok(())
}

fn read_value(bytes: &[u8], offset: &mut usize, kind: &str) -> Result<ProjectionValue, JsValue> {
    match kind {
        "string" => Ok(ProjectionValue::String(read_string(bytes, offset)?)),
        "u8" => Ok(ProjectionValue::U8(read_u8(bytes, offset)?)),
        "u32" => Ok(ProjectionValue::U32(read_u32_le(bytes, offset)?)),
        "u64" => Ok(ProjectionValue::U64(read_u64_le(bytes, offset)?)),
        "bool" => Ok(ProjectionValue::Bool(read_bool(bytes, offset)?)),
        "bytes" => Ok(ProjectionValue::Bytes(read_bytes(bytes, offset)?.to_vec())),
        "option:string" => {
            let has_value = read_u8(bytes, offset)?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::String(read_string(bytes, offset)?))
            } else {
                Err(JsValue::from_str("Invalid option marker"))
            }
        }
        "option:u64" => {
            let has_value = read_u8(bytes, offset)?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::U64(read_u64_le(bytes, offset)?))
            } else {
                Err(JsValue::from_str("Invalid option marker"))
            }
        }
        "option:u32" => {
            let has_value = read_u8(bytes, offset)?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::U32(read_u32_le(bytes, offset)?))
            } else {
                Err(JsValue::from_str("Invalid option marker"))
            }
        }
        "option:u8" => {
            let has_value = read_u8(bytes, offset)?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::U8(read_u8(bytes, offset)?))
            } else {
                Err(JsValue::from_str("Invalid option marker"))
            }
        }
        "option:bool" => {
            let has_value = read_u8(bytes, offset)?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::Bool(read_bool(bytes, offset)?))
            } else {
                Err(JsValue::from_str("Invalid option marker"))
            }
        }
        "option:bytes" => {
            let has_value = read_u8(bytes, offset)?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::Bytes(read_bytes(bytes, offset)?.to_vec()))
            } else {
                Err(JsValue::from_str("Invalid option marker"))
            }
        }
        _ => Err(JsValue::from_str(
            "Unsupported projected document field type",
        )),
    }
}

fn write_projection_value(
    out: &mut Vec<u8>,
    kind: &str,
    value: &ProjectionValue,
) -> Result<(), JsValue> {
    match (kind, value) {
        ("string", ProjectionValue::String(value)) => write_string(out, value),
        ("u8", ProjectionValue::U8(value)) => write_u8(out, *value),
        ("u32", ProjectionValue::U32(value)) => write_u32_le(out, *value),
        ("u64", ProjectionValue::U64(value)) => write_u64_le(out, *value),
        ("u32", ProjectionValue::U64(value)) => write_u32_le(out, *value as u32),
        ("bool", ProjectionValue::Bool(value)) => write_bool(out, *value),
        ("bytes", ProjectionValue::Bytes(value)) => write_bytes(out, value),
        ("option:string", ProjectionValue::None)
        | ("option:u64", ProjectionValue::None)
        | ("option:u32", ProjectionValue::None)
        | ("option:bool", ProjectionValue::None)
        | ("option:bytes", ProjectionValue::None) => write_u8(out, 0),
        ("option:string", ProjectionValue::String(value)) => {
            write_u8(out, 1);
            write_string(out, value);
        }
        ("option:u8", ProjectionValue::U8(value)) => {
            write_u8(out, 1);
            write_u8(out, *value);
        }
        ("option:u32", ProjectionValue::U32(value)) => {
            write_u8(out, 1);
            write_u32_le(out, *value);
        }
        ("option:u64", ProjectionValue::U64(value)) => {
            write_u8(out, 1);
            write_u64_le(out, *value);
        }
        ("option:u32", ProjectionValue::U64(value)) => {
            write_u8(out, 1);
            write_u32_le(out, *value as u32);
        }
        ("option:bool", ProjectionValue::Bool(value)) => {
            write_u8(out, 1);
            write_bool(out, *value);
        }
        ("option:bytes", ProjectionValue::Bytes(value)) => {
            write_u8(out, 1);
            write_bytes(out, value);
        }
        _ => {
            return Err(JsValue::from_str(
                "Projection value does not match output type",
            ))
        }
    }
    Ok(())
}

fn read_document_fields(
    encoded_document: &[u8],
    variant_type: Option<&str>,
    variant_value: Option<&str>,
    names: &[String],
    types: &[String],
) -> Result<HashMap<String, ProjectionValue>, JsValue> {
    if names.len() != types.len() {
        return Err(JsValue::from_str("Document field plan length mismatch"));
    }
    let mut offset = 0usize;
    match variant_type {
        Some("u8") => {
            let expected = variant_value
                .ok_or_else(|| JsValue::from_str("Missing document variant"))?
                .parse::<u8>()
                .map_err(|_| JsValue::from_str("Invalid document variant"))?;
            if read_u8(encoded_document, &mut offset)? != expected {
                return Err(JsValue::from_str("Document variant mismatch"));
            }
        }
        Some("string") => {
            let expected =
                variant_value.ok_or_else(|| JsValue::from_str("Missing document variant"))?;
            if read_string(encoded_document, &mut offset)? != expected {
                return Err(JsValue::from_str("Document variant mismatch"));
            }
        }
        Some("") | None => {}
        _ => return Err(JsValue::from_str("Unsupported document variant type")),
    }
    let mut out = HashMap::with_capacity(names.len());
    for (name, kind) in names.iter().zip(types.iter()) {
        let before = offset;
        let value = read_value(encoded_document, &mut offset, kind);
        match value {
            Ok(value) => {
                out.insert(name.clone(), value);
            }
            Err(_) => {
                offset = before;
                skip_value(encoded_document, &mut offset, kind)?;
            }
        }
    }
    Ok(out)
}

fn write_variant(
    out: &mut Vec<u8>,
    variant_type: Option<&str>,
    variant_value: Option<&str>,
) -> Result<(), JsValue> {
    match variant_type {
        Some("u8") => {
            let value = variant_value
                .ok_or_else(|| JsValue::from_str("Missing output variant"))?
                .parse::<u8>()
                .map_err(|_| JsValue::from_str("Invalid output variant"))?;
            write_u8(out, value);
        }
        Some("string") => {
            let value = variant_value.ok_or_else(|| JsValue::from_str("Missing output variant"))?;
            write_string(out, value);
        }
        Some("") | None => {}
        _ => return Err(JsValue::from_str("Unsupported output variant type")),
    }
    Ok(())
}

fn encode_context_suffix_inner(
    created: u64,
    modified: u64,
    head: &str,
    gid: &str,
    size: u32,
) -> Vec<u8> {
    let head_bytes = head.as_bytes();
    let gid_bytes = gid.as_bytes();
    let mut out = Vec::with_capacity(1 + 8 + 8 + 4 + head_bytes.len() + 4 + gid_bytes.len() + 4);
    out.push(0);
    write_u64_le(&mut out, created);
    write_u64_le(&mut out, modified);
    write_u32_le(&mut out, head_bytes.len() as u32);
    out.extend_from_slice(head_bytes);
    write_u32_le(&mut out, gid_bytes.len() as u32);
    out.extend_from_slice(gid_bytes);
    write_u32_le(&mut out, size);
    out
}

fn existing_created_or_modified(
    existing_created: Option<&str>,
    modified: u64,
    field: &str,
) -> Result<u64, JsValue> {
    let Some(value) = existing_created else {
        return Ok(modified);
    };
    if value == "0" {
        return Ok(modified);
    }
    parse_u64(value, field)
}

fn parse_optional_u64(value: JsValue, fallback: u64, field: &str) -> Result<u64, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(fallback);
    }
    let Some(value) = value.as_string() else {
        return Err(JsValue::from_str(&format!("Invalid {field} value")));
    };
    existing_created_or_modified(Some(&value), fallback, field)
}

fn context_plan_row(created: u64, bytes: Vec<u8>) -> Array {
    let row = Array::new_with_length(2);
    row.set(0, JsValue::from_str(&created.to_string()));
    row.set(1, Uint8Array::from(bytes.as_slice()).into());
    row
}

#[wasm_bindgen]
pub fn encode_context_suffix(
    created: &str,
    modified: &str,
    head: &str,
    gid: &str,
    size: u32,
) -> Result<Uint8Array, JsValue> {
    let created = parse_u64(created, "created")?;
    let modified = parse_u64(modified, "modified")?;
    let bytes = encode_context_suffix_inner(created, modified, head, gid, size);
    Ok(Uint8Array::from(bytes.as_slice()))
}

#[wasm_bindgen]
pub fn encode_context_suffix_batch(
    createds: Array,
    modifieds: Array,
    heads: Array,
    gids: Array,
    sizes: Uint32Array,
) -> Result<Array, JsValue> {
    let len = createds.length();
    if modifieds.length() != len
        || heads.length() != len
        || gids.length() != len
        || sizes.length() != len
    {
        return Err(JsValue::from_str("Mismatched context batch lengths"));
    }

    let out = Array::new_with_length(len);
    for index in 0..len {
        let created = createds
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid created value"))?;
        let modified = modifieds
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid modified value"))?;
        let head = heads
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid head value"))?;
        let gid = gids
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid gid value"))?;
        let bytes = encode_context_suffix_inner(
            parse_u64(&created, "created")?,
            parse_u64(&modified, "modified")?,
            &head,
            &gid,
            sizes.get_index(index),
        );
        out.set(index, Uint8Array::from(bytes.as_slice()).into());
    }
    Ok(out)
}

#[wasm_bindgen]
pub fn plan_document_context(
    existing_created: JsValue,
    modified: &str,
    head: &str,
    gid: &str,
    size: u32,
) -> Result<Array, JsValue> {
    let modified = parse_u64(modified, "modified")?;
    let created = parse_optional_u64(existing_created, modified, "created")?;
    let bytes = encode_context_suffix_inner(created, modified, head, gid, size);
    Ok(context_plan_row(created, bytes))
}

#[wasm_bindgen]
pub fn plan_document_context_batch(
    existing_createds: Array,
    modifieds: Array,
    heads: Array,
    gids: Array,
    sizes: Uint32Array,
) -> Result<Array, JsValue> {
    let len = existing_createds.length();
    if modifieds.length() != len
        || heads.length() != len
        || gids.length() != len
        || sizes.length() != len
    {
        return Err(JsValue::from_str("Mismatched context batch lengths"));
    }

    let out = Array::new_with_length(len);
    for index in 0..len {
        let modified = modifieds
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid modified value"))?;
        let head = heads
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid head value"))?;
        let gid = gids
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid gid value"))?;
        let modified = parse_u64(&modified, "modified")?;
        let created = parse_optional_u64(existing_createds.get(index), modified, "created")?;
        let bytes =
            encode_context_suffix_inner(created, modified, &head, &gid, sizes.get_index(index));
        out.set(index, context_plan_row(created, bytes).into());
    }
    Ok(out)
}

#[wasm_bindgen]
pub fn project_document_index_simple(
    encoded_document: Uint8Array,
    plan: JsValue,
    created: &str,
    modified: &str,
    gid: &str,
    size: u32,
    signer: JsValue,
) -> Result<Uint8Array, JsValue> {
    let document_variant_type = optional_string(js_get(&plan, "documentVariantType"));
    let document_variant_value = optional_string(js_get(&plan, "documentVariantValue"));
    let output_variant_type = optional_string(js_get(&plan, "outputVariantType"));
    let output_variant_value = optional_string(js_get(&plan, "outputVariantValue"));
    let document_field_names =
        array_strings(js_get(&plan, "documentFieldNames"), "documentFieldNames")?;
    let document_field_types =
        array_strings(js_get(&plan, "documentFieldTypes"), "documentFieldTypes")?;
    let output_field_types = array_strings(js_get(&plan, "outputFieldTypes"), "outputFieldTypes")?;
    let source_kinds = array_strings(js_get(&plan, "sourceKinds"), "sourceKinds")?;
    let source_values = array_strings(js_get(&plan, "sourceValues"), "sourceValues")?;
    if output_field_types.len() != source_kinds.len() || source_kinds.len() != source_values.len() {
        return Err(JsValue::from_str("Projection plan length mismatch"));
    }

    let encoded_document = encoded_document.to_vec();
    let document_values = read_document_fields(
        &encoded_document,
        document_variant_type.as_deref(),
        document_variant_value.as_deref(),
        &document_field_names,
        &document_field_types,
    )?;
    let created = parse_u64(created, "created")?;
    let modified = parse_u64(modified, "modified")?;
    let signer = if signer.is_null() || signer.is_undefined() {
        None
    } else {
        Some(Uint8Array::new(&signer).to_vec())
    };

    let mut out = Vec::new();
    write_variant(
        &mut out,
        output_variant_type.as_deref(),
        output_variant_value.as_deref(),
    )?;

    for index in 0..output_field_types.len() {
        let value = match source_kinds[index].as_str() {
            "field" => document_values
                .get(&source_values[index])
                .cloned()
                .unwrap_or(ProjectionValue::None),
            "context" => match source_values[index].as_str() {
                "created" => ProjectionValue::U64(created),
                "modified" => ProjectionValue::U64(modified),
                "gid" => ProjectionValue::String(gid.to_string()),
                "size" => ProjectionValue::U64(size as u64),
                _ => return Err(JsValue::from_str("Unsupported context projection source")),
            },
            "entryFirstSignerPublicKey" => signer
                .as_ref()
                .map(|bytes| ProjectionValue::Bytes(bytes.clone()))
                .unwrap_or(ProjectionValue::None),
            _ => return Err(JsValue::from_str("Unsupported projection source kind")),
        };
        write_projection_value(&mut out, &output_field_types[index], &value)?;
    }

    Ok(Uint8Array::from(out.as_slice()))
}

fn projection_value_as_js(value: ProjectionValue) -> Result<JsValue, JsValue> {
    match value {
        ProjectionValue::None => Ok(JsValue::UNDEFINED),
        ProjectionValue::String(value) => {
            let row = Array::new_with_length(2);
            row.set(0, JsValue::from_str("string"));
            row.set(1, JsValue::from_str(&value));
            Ok(row.into())
        }
        ProjectionValue::U8(value) => {
            let row = Array::new_with_length(2);
            row.set(0, JsValue::from_str("number"));
            row.set(1, JsValue::from_f64(value as f64));
            Ok(row.into())
        }
        ProjectionValue::U32(value) => {
            let row = Array::new_with_length(2);
            row.set(0, JsValue::from_str("number"));
            row.set(1, JsValue::from_f64(value as f64));
            Ok(row.into())
        }
        ProjectionValue::U64(value) => {
            let row = Array::new_with_length(2);
            row.set(0, JsValue::from_str("u64"));
            row.set(1, JsValue::from_str(&value.to_string()));
            Ok(row.into())
        }
        ProjectionValue::Bool(_) => Err(JsValue::from_str(
            "Boolean document fields cannot be used as document ids",
        )),
        ProjectionValue::Bytes(value) => {
            let row = Array::new_with_length(2);
            row.set(0, JsValue::from_str("bytes"));
            row.set(1, Uint8Array::from(value.as_slice()).into());
            Ok(row.into())
        }
    }
}

#[wasm_bindgen]
pub fn extract_document_field_simple(
    encoded_document: Uint8Array,
    plan: JsValue,
) -> Result<JsValue, JsValue> {
    let document_variant_type = optional_string(js_get(&plan, "documentVariantType"));
    let document_variant_value = optional_string(js_get(&plan, "documentVariantValue"));
    let document_field_names =
        array_strings(js_get(&plan, "documentFieldNames"), "documentFieldNames")?;
    let document_field_types =
        array_strings(js_get(&plan, "documentFieldTypes"), "documentFieldTypes")?;
    let field_name = js_string(js_get(&plan, "fieldName"), "fieldName")?;
    let encoded_document = encoded_document.to_vec();
    let document_values = read_document_fields(
        &encoded_document,
        document_variant_type.as_deref(),
        document_variant_value.as_deref(),
        &document_field_names,
        &document_field_types,
    )?;
    projection_value_as_js(
        document_values
            .get(&field_name)
            .cloned()
            .unwrap_or(ProjectionValue::None),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        encode_context_suffix_inner, existing_created_or_modified, read_document_fields,
        write_projection_value, write_string, write_u64_le, write_u8, ProjectionValue,
    };

    #[test]
    fn encodes_context_suffix() {
        let bytes = encode_context_suffix_inner(1, 2, "head", "gid", 3);
        assert_eq!(bytes[0], 0);
        assert_eq!(&bytes[1..9], &1u64.to_le_bytes());
        assert_eq!(&bytes[9..17], &2u64.to_le_bytes());
        assert_eq!(&bytes[17..21], &4u32.to_le_bytes());
        assert_eq!(&bytes[21..25], b"head");
        assert_eq!(&bytes[25..29], &3u32.to_le_bytes());
        assert_eq!(&bytes[29..32], b"gid");
        assert_eq!(&bytes[32..36], &3u32.to_le_bytes());
    }

    #[test]
    fn falls_back_to_modified_when_existing_created_is_missing_or_zero() {
        assert_eq!(
            existing_created_or_modified(None, 11, "created").unwrap(),
            11
        );
        assert_eq!(
            existing_created_or_modified(Some("0"), 11, "created").unwrap(),
            11
        );
        assert_eq!(
            existing_created_or_modified(Some("7"), 11, "created").unwrap(),
            7
        );
    }

    #[test]
    fn reads_simple_document_fields() {
        let mut bytes = Vec::new();
        write_u8(&mut bytes, 0);
        write_string(&mut bytes, "abc");
        write_u8(&mut bytes, 1);
        write_string(&mut bytes, "name");
        write_u8(&mut bytes, 0);
        let fields = read_document_fields(
            &bytes,
            Some("u8"),
            Some("0"),
            &["id".to_string(), "name".to_string(), "count".to_string()],
            &[
                "string".to_string(),
                "option:string".to_string(),
                "option:u64".to_string(),
            ],
        )
        .unwrap();
        assert!(matches!(
            fields.get("id"),
            Some(ProjectionValue::String(value)) if value == "abc"
        ));
        assert!(matches!(
            fields.get("name"),
            Some(ProjectionValue::String(value)) if value == "name"
        ));
        assert!(matches!(fields.get("count"), Some(ProjectionValue::None)));
    }

    #[test]
    fn writes_projected_values() {
        let mut out = Vec::new();
        write_projection_value(
            &mut out,
            "string",
            &ProjectionValue::String("a".to_string()),
        )
        .unwrap();
        write_projection_value(&mut out, "u64", &ProjectionValue::U64(2)).unwrap();
        write_projection_value(
            &mut out,
            "option:bytes",
            &ProjectionValue::Bytes(vec![3, 4]),
        )
        .unwrap();
        let mut expected = Vec::new();
        write_string(&mut expected, "a");
        write_u64_le(&mut expected, 2);
        write_u8(&mut expected, 1);
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&[3, 4]);
        assert_eq!(out, expected);
    }
}
