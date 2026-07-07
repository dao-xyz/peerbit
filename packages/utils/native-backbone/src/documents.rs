use js_sys::{Array, Uint8Array};
use peerbit_indexer_core::codec::{decode_query, decode_sort};
use peerbit_indexer_core::persistence::{
    decode_journal, decode_key_value_snapshot, encode_key_value_snapshot, JournalRecord,
    JOURNAL_MAGIC,
};
use peerbit_indexer_core::planner::{DocumentFields, FieldPath, FieldValue, SumResult};
use peerbit_indexer_core::schema::{
    decode_native_schema_ir, extract_encoded_document_fields_from_parts_with_byte_limits,
};
use peerbit_indexer_core::storage::ByteStorage;
use peerbit_log_rust::entry_v0_signature_public_key_from_storage_bytes;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

use crate::error::BackboneError;
use crate::js_interop::{
    append_journal_delete_record, append_journal_put_record, array_strings, bytes_vec_from_array,
    clear_journal_prefix, ensure_same_len, js_get, optional_string, parse_optional_u64_string,
    parse_u64_string, read_bytes, read_encoded_string, read_u32, read_u64, strings_from_array,
    write_bool, write_bytes, write_string, write_u32, write_u64, write_u8,
};
use crate::{NativePeerbitBackbone, NATIVE_BACKBONE_BYTE_EXACT_INDEX_LIMIT};

pub(crate) struct DocumentIndexAppendCommit {
    pub(crate) key: String,
    pub(crate) value_prefix: DocumentIndexValuePrefix,
    pub(crate) existing_created: Option<u64>,
    pub(crate) byte_element_index_limit: usize,
    pub(crate) delete_trimmed_heads: bool,
    pub(crate) previous_context: Option<DocumentContextFacts>,
    pub(crate) known_existing: bool,
    pub(crate) required_previous_signer_public_key: Option<Vec<u8>>,
}

#[derive(Clone, Copy)]
pub(crate) struct DocumentContextFields {
    created: u32,
    modified: u32,
    head: u32,
    gid: u32,
    size: u32,
}

#[derive(Clone)]
pub(crate) struct DocumentContextFacts {
    pub(crate) created: u64,
    pub(crate) modified: u64,
    pub(crate) head: String,
    pub(crate) gid: String,
    pub(crate) size: u32,
}

#[derive(Clone)]
pub(crate) struct DocumentPreviousSignerFact {
    head: String,
    public_key: Vec<u8>,
}

pub(crate) struct PreparedDocumentEncodedPartsPut {
    pub(crate) key: String,
    pub(crate) value_bytes: Vec<u8>,
    pub(crate) fields: DocumentFields,
    pub(crate) known_existing: bool,
    pub(crate) new_head: Option<String>,
    pub(crate) previous_head: Option<String>,
    pub(crate) record_document_journal: bool,
}

pub(crate) struct PreparedDocumentIndexAppendPut {
    pub(crate) parts: PreparedDocumentEncodedPartsPut,
    pub(crate) previous_signer_head: Option<String>,
}

pub(crate) struct ParsedProjectionPlan {
    document_variant_type: Option<String>,
    document_variant_value: Option<String>,
    output_variant_type: Option<String>,
    output_variant_value: Option<String>,
    document_field_names: Vec<String>,
    document_field_types: Vec<String>,
    output_field_types: Vec<String>,
    source_kinds: Vec<String>,
    source_values: Vec<String>,
}

pub(crate) enum DocumentIndexValuePrefix {
    Bytes(Vec<u8>),
    Projection {
        encoded_document: Vec<u8>,
        plan: DocumentIndexProjectionPlan,
        signer: Option<Vec<u8>>,
    },
    PlainPutPayloadIdentity,
    PlainPutPayloadProjection {
        plan: DocumentIndexProjectionPlan,
        signer: Option<Vec<u8>>,
    },
}

pub(crate) enum DocumentIndexProjectionPlan {
    Inline(ParsedProjectionPlan),
    Cached(usize),
}

pub(crate) fn document_index_append_commit(
    key: String,
    value_prefix_bytes: Vec<u8>,
    existing_created: String,
    byte_element_index_limit: usize,
    delete_trimmed_heads: bool,
    projection_plan: JsValue,
    projection_encoded_document: JsValue,
    projection_signer: JsValue,
) -> Result<DocumentIndexAppendCommit, JsValue> {
    let value_prefix = if projection_plan.is_null() || projection_plan.is_undefined() {
        DocumentIndexValuePrefix::Bytes(value_prefix_bytes)
    } else {
        DocumentIndexValuePrefix::Projection {
            encoded_document: Uint8Array::new(&projection_encoded_document).to_vec(),
            plan: DocumentIndexProjectionPlan::Inline(parse_projection_plan(&projection_plan)?),
            signer: if projection_signer.is_null() || projection_signer.is_undefined() {
                None
            } else {
                Some(Uint8Array::new(&projection_signer).to_vec())
            },
        }
    };
    Ok(DocumentIndexAppendCommit {
        key,
        value_prefix,
        existing_created: parse_optional_u64_string(
            &existing_created,
            "document existing created",
        )?,
        byte_element_index_limit,
        delete_trimmed_heads,
        previous_context: None,
        known_existing: false,
        required_previous_signer_public_key: None,
    })
}

pub(crate) fn document_index_cached_projection_append_commit(
    key: String,
    existing_created: String,
    byte_element_index_limit: usize,
    delete_trimmed_heads: bool,
    projection_plan_id: u32,
    projection_encoded_document: JsValue,
    projection_signer: JsValue,
) -> Result<DocumentIndexAppendCommit, JsValue> {
    Ok(DocumentIndexAppendCommit {
        key,
        value_prefix: DocumentIndexValuePrefix::Projection {
            encoded_document: Uint8Array::new(&projection_encoded_document).to_vec(),
            plan: DocumentIndexProjectionPlan::Cached(projection_plan_id as usize),
            signer: if projection_signer.is_null() || projection_signer.is_undefined() {
                None
            } else {
                Some(Uint8Array::new(&projection_signer).to_vec())
            },
        },
        existing_created: parse_optional_u64_string(
            &existing_created,
            "document existing created",
        )?,
        byte_element_index_limit,
        delete_trimmed_heads,
        previous_context: None,
        known_existing: false,
        required_previous_signer_public_key: None,
    })
}

pub(crate) fn document_index_plain_put_payload_append_commit(
    key: String,
    existing_created: String,
    byte_element_index_limit: usize,
    delete_trimmed_heads: bool,
) -> Result<DocumentIndexAppendCommit, JsValue> {
    Ok(DocumentIndexAppendCommit {
        key,
        value_prefix: DocumentIndexValuePrefix::PlainPutPayloadIdentity,
        existing_created: parse_optional_u64_string(
            &existing_created,
            "document existing created",
        )?,
        byte_element_index_limit,
        delete_trimmed_heads,
        previous_context: None,
        known_existing: false,
        required_previous_signer_public_key: None,
    })
}

pub(crate) fn document_index_cached_projection_plain_put_payload_append_commit(
    key: String,
    existing_created: String,
    byte_element_index_limit: usize,
    delete_trimmed_heads: bool,
    projection_plan_id: u32,
    projection_signer: JsValue,
) -> Result<DocumentIndexAppendCommit, JsValue> {
    Ok(DocumentIndexAppendCommit {
        key,
        value_prefix: DocumentIndexValuePrefix::PlainPutPayloadProjection {
            plan: DocumentIndexProjectionPlan::Cached(projection_plan_id as usize),
            signer: if projection_signer.is_null() || projection_signer.is_undefined() {
                None
            } else {
                Some(Uint8Array::new(&projection_signer).to_vec())
            },
        },
        existing_created: parse_optional_u64_string(
            &existing_created,
            "document existing created",
        )?,
        byte_element_index_limit,
        delete_trimmed_heads,
        previous_context: None,
        known_existing: false,
        required_previous_signer_public_key: None,
    })
}

pub(crate) fn plain_put_document_bytes_from_payload(
    payload_data: &[u8],
) -> Result<&[u8], BackboneError> {
    const PLAIN_PUT_OPERATION_PREFIX_LENGTH: usize = 6;
    if payload_data.len() < PLAIN_PUT_OPERATION_PREFIX_LENGTH {
        return Err(BackboneError::PlainPutPayloadTooShort);
    }
    if payload_data[0] != 0 || payload_data[1] != 3 {
        return Err(BackboneError::Expected("native plain put payload"));
    }
    let declared_len = u32::from_le_bytes([
        payload_data[2],
        payload_data[3],
        payload_data[4],
        payload_data[5],
    ]) as usize;
    let document_bytes = &payload_data[PLAIN_PUT_OPERATION_PREFIX_LENGTH..];
    if document_bytes.len() != declared_len {
        return Err(BackboneError::PlainPutPayloadLengthMismatch);
    }
    Ok(document_bytes)
}

fn document_entry_to_row(key: &str, value: &[u8]) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(key));
    row.push(&Uint8Array::from(value));
    row
}

pub(crate) fn document_context_facts_to_row(context: &DocumentContextFacts) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&context.created.to_string()));
    row.push(&JsValue::from_str(&context.modified.to_string()));
    row.push(&JsValue::from_str(&context.head));
    row.push(&JsValue::from_str(&context.gid));
    row.push(&JsValue::from_f64(context.size as f64));
    row
}

fn document_u64_field(fields: &DocumentFields, field: u32) -> Option<u64> {
    match fields.scalar_values(&FieldPath::Id(field))?.first()? {
        FieldValue::U64(value) => Some(*value),
        FieldValue::I64(value) if *value >= 0 => Some(*value as u64),
        _ => None,
    }
}

fn document_string_field(fields: &DocumentFields, field: u32) -> Option<String> {
    match fields.scalar_values(&FieldPath::Id(field))?.first()? {
        FieldValue::String(value) => Some(value.to_string()),
        _ => None,
    }
}

fn field_value_to_row(value: &FieldValue) -> JsValue {
    let row = Array::new_with_length(2);
    match value {
        FieldValue::Bool(value) => {
            row.set(0, JsValue::from_str("bool"));
            row.set(1, JsValue::from_bool(*value));
        }
        FieldValue::I64(value) => {
            row.set(0, JsValue::from_str("i64"));
            row.set(1, JsValue::from_str(&value.to_string()));
        }
        FieldValue::U64(value) => {
            row.set(0, JsValue::from_str("u64"));
            row.set(1, JsValue::from_str(&value.to_string()));
        }
        FieldValue::String(value) => {
            row.set(0, JsValue::from_str("string"));
            row.set(1, JsValue::from_str(value));
        }
        FieldValue::Bytes(value) => {
            row.set(0, JsValue::from_str("bytes"));
            row.set(1, Uint8Array::from(value.as_ref()).into());
        }
    }
    row.into()
}

fn sum_to_js(sum: SumResult) -> Array {
    let out = Array::new();
    match sum {
        SumResult::None => {
            out.push(&JsValue::from_str("none"));
            out.push(&JsValue::from_str("0"));
        }
        SumResult::I64(value) => {
            out.push(&JsValue::from_str("i64"));
            out.push(&JsValue::from_str(&value.to_string()));
        }
        SumResult::U64(value) => {
            out.push(&JsValue::from_str("u64"));
            out.push(&JsValue::from_str(&value.to_string()));
        }
    }
    out
}

fn encode_document_signer_fact(fact: &DocumentPreviousSignerFact) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + fact.head.len() + fact.public_key.len());
    write_string(&mut out, &fact.head);
    write_bytes(&mut out, &fact.public_key);
    out
}

fn decode_document_signer_fact(bytes: &[u8]) -> Result<DocumentPreviousSignerFact, BackboneError> {
    let mut offset = 0usize;
    let head = read_encoded_string(bytes, &mut offset, "document signer head")?;
    let public_key = read_bytes(bytes, &mut offset, "document signer public key")?;
    if offset != bytes.len() {
        return Err(BackboneError::TrailingDocumentSignerFactBytes);
    }
    Ok(DocumentPreviousSignerFact { head, public_key })
}

pub(crate) fn encode_document_context_suffix(
    created: u64,
    modified: u64,
    head: &str,
    gid: &str,
    size: u32,
) -> Result<Vec<u8>, BackboneError> {
    let capacity = 1usize
        .checked_add(8)
        .and_then(|value| value.checked_add(8))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(head.len()))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(gid.len()))
        .and_then(|value| value.checked_add(4))
        .ok_or(BackboneError::DocumentContextSuffixCapacityOverflow)?;
    let mut out = Vec::with_capacity(capacity);
    // Context is @variant(0); keep this byte-for-byte aligned with Borsh.
    out.push(0);
    write_u64(&mut out, created);
    write_u64(&mut out, modified);
    write_string(&mut out, head);
    write_string(&mut out, gid);
    write_u32(&mut out, size);
    Ok(out)
}

#[derive(Clone, Debug)]
enum ProjectionValue {
    String(String),
    U64(u64),
    Bool(bool),
    Bytes(Vec<u8>),
    None,
}

fn read_u8_projection(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<u8, BackboneError> {
    if *offset >= bytes.len() {
        return Err(BackboneError::Truncated(label));
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_bool_projection(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<bool, BackboneError> {
    match read_u8_projection(bytes, offset, label)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(BackboneError::InvalidBool(label)),
    }
}

fn skip_projection_value(
    bytes: &[u8],
    offset: &mut usize,
    kind: &str,
) -> Result<(), BackboneError> {
    match kind {
        "string" => {
            read_encoded_string(bytes, offset, "projected string")?;
        }
        "u8" => {
            read_u8_projection(bytes, offset, "projected u8")?;
        }
        "u32" => {
            read_u32(bytes, offset, "projected u32")?;
        }
        "u64" => {
            read_u64(bytes, offset, "projected u64")?;
        }
        "bool" => {
            read_bool_projection(bytes, offset, "projected bool")?;
        }
        "bytes" => {
            read_bytes(bytes, offset, "projected bytes")?;
        }
        "option:string" | "option:u8" | "option:u32" | "option:u64" | "option:bool"
        | "option:bytes" => {
            let has_value = read_u8_projection(bytes, offset, "projected option")?;
            if has_value == 1 {
                skip_projection_value(bytes, offset, &kind["option:".len()..])?;
            } else if has_value != 0 {
                return Err(BackboneError::InvalidProjectionOptionMarker);
            }
        }
        "vec:string" => {
            let len = read_u32(bytes, offset, "projected string vec length")?;
            for _ in 0..len {
                read_encoded_string(bytes, offset, "projected string vec item")?;
            }
        }
        "vec:bytes" => {
            let len = read_u32(bytes, offset, "projected bytes vec length")?;
            for _ in 0..len {
                read_bytes(bytes, offset, "projected bytes vec item")?;
            }
        }
        _ => {
            return Err(BackboneError::UnsupportedDocumentProjectionFieldType);
        }
    }
    Ok(())
}

fn read_projection_value(
    bytes: &[u8],
    offset: &mut usize,
    kind: &str,
) -> Result<ProjectionValue, BackboneError> {
    match kind {
        "string" => Ok(ProjectionValue::String(read_encoded_string(
            bytes,
            offset,
            "projection string",
        )?)),
        "u8" => Ok(ProjectionValue::U64(
            read_u8_projection(bytes, offset, "projection u8")? as u64,
        )),
        "u32" => Ok(ProjectionValue::U64(
            read_u32(bytes, offset, "projection u32")? as u64,
        )),
        "u64" => Ok(ProjectionValue::U64(read_u64(
            bytes,
            offset,
            "projection u64",
        )?)),
        "bool" => Ok(ProjectionValue::Bool(read_bool_projection(
            bytes,
            offset,
            "projection bool",
        )?)),
        "bytes" => Ok(ProjectionValue::Bytes(read_bytes(
            bytes,
            offset,
            "projection bytes",
        )?)),
        "option:string" => {
            let has_value = read_u8_projection(bytes, offset, "projection option string")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::String(read_encoded_string(
                    bytes,
                    offset,
                    "projection option string",
                )?))
            } else {
                Err(BackboneError::InvalidProjectionOptionMarker)
            }
        }
        "option:u8" => {
            let has_value = read_u8_projection(bytes, offset, "projection option u8")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::U64(
                    read_u8_projection(bytes, offset, "projection option u8")? as u64,
                ))
            } else {
                Err(BackboneError::InvalidProjectionOptionMarker)
            }
        }
        "option:u32" => {
            let has_value = read_u8_projection(bytes, offset, "projection option u32")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::U64(
                    read_u32(bytes, offset, "projection option u32")? as u64,
                ))
            } else {
                Err(BackboneError::InvalidProjectionOptionMarker)
            }
        }
        "option:u64" => {
            let has_value = read_u8_projection(bytes, offset, "projection option u64")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::U64(read_u64(
                    bytes,
                    offset,
                    "projection option u64",
                )?))
            } else {
                Err(BackboneError::InvalidProjectionOptionMarker)
            }
        }
        "option:bool" => {
            let has_value = read_u8_projection(bytes, offset, "projection option bool")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::Bool(read_bool_projection(
                    bytes,
                    offset,
                    "projection option bool",
                )?))
            } else {
                Err(BackboneError::InvalidProjectionOptionMarker)
            }
        }
        "option:bytes" => {
            let has_value = read_u8_projection(bytes, offset, "projection option bytes")?;
            if has_value == 0 {
                Ok(ProjectionValue::None)
            } else if has_value == 1 {
                Ok(ProjectionValue::Bytes(read_bytes(
                    bytes,
                    offset,
                    "projection option bytes",
                )?))
            } else {
                Err(BackboneError::InvalidProjectionOptionMarker)
            }
        }
        _ => Err(BackboneError::UnsupportedProjectedDocumentFieldType),
    }
}

fn write_projection_value(
    out: &mut Vec<u8>,
    kind: &str,
    value: &ProjectionValue,
) -> Result<(), BackboneError> {
    match (kind, value) {
        ("string", ProjectionValue::String(value)) => write_string(out, value),
        ("u8", ProjectionValue::U64(value)) => write_u8(out, *value as u8),
        ("u32", ProjectionValue::U64(value)) => write_u32(out, *value as u32),
        ("u64", ProjectionValue::U64(value)) => write_u64(out, *value),
        ("bool", ProjectionValue::Bool(value)) => write_bool(out, *value),
        ("bytes", ProjectionValue::Bytes(value)) => write_bytes(out, value),
        ("option:string", ProjectionValue::None)
        | ("option:u8", ProjectionValue::None)
        | ("option:u32", ProjectionValue::None)
        | ("option:u64", ProjectionValue::None)
        | ("option:bool", ProjectionValue::None)
        | ("option:bytes", ProjectionValue::None) => write_u8(out, 0),
        ("option:string", ProjectionValue::String(value)) => {
            write_u8(out, 1);
            write_string(out, value);
        }
        ("option:u8", ProjectionValue::U64(value)) => {
            write_u8(out, 1);
            write_u8(out, *value as u8);
        }
        ("option:u32", ProjectionValue::U64(value)) => {
            write_u8(out, 1);
            write_u32(out, *value as u32);
        }
        ("option:u64", ProjectionValue::U64(value)) => {
            write_u8(out, 1);
            write_u64(out, *value);
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
            return Err(BackboneError::ProjectionValueOutputTypeMismatch);
        }
    }
    Ok(())
}

fn read_projected_document_fields(
    encoded_document: &[u8],
    variant_type: Option<&str>,
    variant_value: Option<&str>,
    names: &[String],
    types: &[String],
) -> Result<HashMap<String, ProjectionValue>, BackboneError> {
    if names.len() != types.len() {
        return Err(BackboneError::DocumentProjectionPlanLengthMismatch);
    }
    let mut offset = 0usize;
    match variant_type {
        Some("u8") => {
            let expected = variant_value
                .ok_or(BackboneError::MissingDocumentVariant)?
                .parse::<u8>()
                .map_err(|_| BackboneError::InvalidDocumentVariant)?;
            if read_u8_projection(encoded_document, &mut offset, "document variant")? != expected {
                return Err(BackboneError::DocumentVariantMismatch);
            }
        }
        Some("string") => {
            let expected = variant_value.ok_or(BackboneError::MissingDocumentVariant)?;
            if read_encoded_string(encoded_document, &mut offset, "document variant")? != expected {
                return Err(BackboneError::DocumentVariantMismatch);
            }
        }
        Some("") | None => {}
        _ => return Err(BackboneError::UnsupportedDocumentVariantType),
    }
    let mut out = HashMap::with_capacity(names.len());
    for (name, kind) in names.iter().zip(types.iter()) {
        let before = offset;
        let value = read_projection_value(encoded_document, &mut offset, kind);
        match value {
            Ok(value) => {
                out.insert(name.clone(), value);
            }
            Err(_) => {
                offset = before;
                skip_projection_value(encoded_document, &mut offset, kind)?;
            }
        }
    }
    Ok(out)
}

fn write_projection_variant(
    out: &mut Vec<u8>,
    variant_type: Option<&str>,
    variant_value: Option<&str>,
) -> Result<(), BackboneError> {
    match variant_type {
        Some("u8") => {
            let value = variant_value
                .ok_or(BackboneError::MissingOutputVariant)?
                .parse::<u8>()
                .map_err(|_| BackboneError::InvalidOutputVariant)?;
            write_u8(out, value);
        }
        Some("string") => {
            let value = variant_value.ok_or(BackboneError::MissingOutputVariant)?;
            write_string(out, value);
        }
        Some("") | None => {}
        _ => return Err(BackboneError::UnsupportedOutputVariantType),
    }
    Ok(())
}

fn parse_projection_plan(plan: &JsValue) -> Result<ParsedProjectionPlan, BackboneError> {
    let document_field_names =
        array_strings(js_get(plan, "documentFieldNames"), "documentFieldNames")?;
    let document_field_types =
        array_strings(js_get(plan, "documentFieldTypes"), "documentFieldTypes")?;
    let output_field_types = array_strings(js_get(plan, "outputFieldTypes"), "outputFieldTypes")?;
    let source_kinds = array_strings(js_get(plan, "sourceKinds"), "sourceKinds")?;
    let source_values = array_strings(js_get(plan, "sourceValues"), "sourceValues")?;
    if output_field_types.len() != source_kinds.len() || source_kinds.len() != source_values.len() {
        return Err(BackboneError::ProjectionPlanLengthMismatch);
    }
    Ok(ParsedProjectionPlan {
        document_variant_type: optional_string(
            js_get(plan, "documentVariantType"),
            "documentVariantType",
        )?,
        document_variant_value: optional_string(
            js_get(plan, "documentVariantValue"),
            "documentVariantValue",
        )?,
        output_variant_type: optional_string(
            js_get(plan, "outputVariantType"),
            "outputVariantType",
        )?,
        output_variant_value: optional_string(
            js_get(plan, "outputVariantValue"),
            "outputVariantValue",
        )?,
        document_field_names,
        document_field_types,
        output_field_types,
        source_kinds,
        source_values,
    })
}

pub(crate) fn project_document_index_simple_bytes_with_plan(
    encoded_document: &[u8],
    plan: &ParsedProjectionPlan,
    created: u64,
    modified: u64,
    head: &str,
    gid: &str,
    size: u32,
    signer: Option<&[u8]>,
) -> Result<Vec<u8>, BackboneError> {
    let document_values = read_projected_document_fields(
        encoded_document,
        plan.document_variant_type.as_deref(),
        plan.document_variant_value.as_deref(),
        &plan.document_field_names,
        &plan.document_field_types,
    )?;

    let mut out = Vec::new();
    write_projection_variant(
        &mut out,
        plan.output_variant_type.as_deref(),
        plan.output_variant_value.as_deref(),
    )?;

    for index in 0..plan.output_field_types.len() {
        let value = match plan.source_kinds[index].as_str() {
            "field" => document_values
                .get(&plan.source_values[index])
                .cloned()
                .unwrap_or(ProjectionValue::None),
            "context" => match plan.source_values[index].as_str() {
                "created" => ProjectionValue::U64(created),
                "modified" => ProjectionValue::U64(modified),
                "head" => ProjectionValue::String(head.to_string()),
                "gid" => ProjectionValue::String(gid.to_string()),
                "size" => ProjectionValue::U64(size as u64),
                _ => return Err(BackboneError::UnsupportedContextProjectionSource),
            },
            "entryFirstSignerPublicKey" => signer
                .map(|bytes| ProjectionValue::Bytes(bytes.to_vec()))
                .unwrap_or(ProjectionValue::None),
            _ => return Err(BackboneError::UnsupportedProjectionSourceKind),
        };
        write_projection_value(&mut out, &plan.output_field_types[index], &value)?;
    }

    Ok(out)
}

fn project_document_index_simple_bytes(
    encoded_document: &[u8],
    plan: &JsValue,
    created: &str,
    modified: &str,
    head: &str,
    gid: &str,
    size: u32,
    signer: JsValue,
) -> Result<Vec<u8>, BackboneError> {
    let plan = parse_projection_plan(plan)?;
    let created = parse_u64_string(created, "created")?;
    let modified = parse_u64_string(modified, "modified")?;
    let signer = if signer.is_null() || signer.is_undefined() {
        None
    } else {
        Some(Uint8Array::new(&signer).to_vec())
    };
    project_document_index_simple_bytes_with_plan(
        encoded_document,
        &plan,
        created,
        modified,
        head,
        gid,
        size,
        signer.as_deref(),
    )
}

#[wasm_bindgen]
impl NativePeerbitBackbone {
    pub fn configure_document_schema_ir(
        &mut self,
        schema_ir_bytes: Vec<u8>,
    ) -> Result<Array, JsValue> {
        let schema_ir = decode_native_schema_ir(&schema_ir_bytes).map_err(BackboneError::from)?;
        let stats = schema_ir.stats();
        self.document_schema_ir = Some(schema_ir);
        self.rebuild_document_index_from_values()?;
        let out = Array::new();
        out.push(&JsValue::from_f64(stats.root_fields as f64));
        out.push(&JsValue::from_f64(stats.node_count as f64));
        out.push(&JsValue::from_f64(stats.generic_nodes as f64));
        Ok(out)
    }

    pub fn set_document_byte_element_index_limit(&mut self, limit: usize) -> Result<(), JsValue> {
        if self.document_byte_element_index_limit == limit {
            return Ok(());
        }
        self.document_byte_element_index_limit = limit;
        Ok(self.rebuild_document_index_from_values()?)
    }

    pub fn set_document_context_head_field(&mut self, field: u32) {
        self.document_context_head_field = Some(field);
    }

    pub fn set_document_context_fields(
        &mut self,
        created: u32,
        modified: u32,
        head: u32,
        gid: u32,
        size: u32,
    ) {
        self.document_context_head_field = Some(head);
        self.document_context_fields = Some(DocumentContextFields {
            created,
            modified,
            head,
            gid,
            size,
        });
        self.rebuild_document_head_keys();
    }

    pub fn register_document_projection_plan(&mut self, plan: JsValue) -> Result<u32, JsValue> {
        let id = self.document_projection_plans.len();
        if id > u32::MAX as usize {
            return Err(JsValue::from_str("Too many document projection plans"));
        }
        self.document_projection_plans
            .push(parse_projection_plan(&plan)?);
        Ok(id as u32)
    }

    pub fn project_document_index_simple(
        &self,
        encoded_document: Uint8Array,
        plan: JsValue,
        created: &str,
        modified: &str,
        head: &str,
        gid: &str,
        size: u32,
        signer: JsValue,
    ) -> Result<Uint8Array, JsValue> {
        let bytes = project_document_index_simple_bytes(
            &encoded_document.to_vec(),
            &plan,
            created,
            modified,
            head,
            gid,
            size,
            signer,
        )?;
        Ok(Uint8Array::from(bytes.as_slice()))
    }

    pub fn document_index_len(&self) -> usize {
        self.document_index.len()
    }

    pub fn document_value_len(&self) -> usize {
        self.document_values.len()
    }

    pub fn document_exact_string_first_key(&self, field: u32, value: String) -> JsValue {
        self.document_index
            .exact_first(&FieldPath::Id(field), &FieldValue::from(value))
            .map(|key| JsValue::from_str(&key))
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn document_value_bytes(&self, key: &str) -> JsValue {
        self.document_values
            .get(key)
            .map(|value| Uint8Array::from(value).into())
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn document_entry(&self, key: &str) -> JsValue {
        self.document_values
            .get(key)
            .map(|value| document_entry_to_row(key, value).into())
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn document_keys_exist(&self, keys: Vec<String>) -> Uint8Array {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(u8::from(self.document_values.get(&key).is_some()));
        }
        Uint8Array::from(out.as_slice())
    }

    pub fn document_field_value(&self, key: &str, field: u32) -> JsValue {
        self.document_index
            .document_fields_by_id(key)
            .and_then(|fields| fields.scalar_values(&FieldPath::Id(field)))
            .and_then(|values| values.first())
            .map(field_value_to_row)
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn document_context(&self, key: &str) -> Result<JsValue, JsValue> {
        Ok(self
            .document_context_facts_by_key(key)?
            .map(|context| document_context_facts_to_row(&context).into())
            .unwrap_or(JsValue::UNDEFINED))
    }

    pub fn document_context_batch(&self, keys: Vec<String>) -> Result<Array, JsValue> {
        let rows = Array::new_with_length(keys.len() as u32);
        for (index, key) in keys.iter().enumerate() {
            let value = self
                .document_context_facts_by_key(key)?
                .map(|context| document_context_facts_to_row(&context).into())
                .unwrap_or(JsValue::UNDEFINED);
            rows.set(index as u32, value);
        }
        Ok(rows)
    }

    pub fn document_previous_signature_public_key(&self, key: &str) -> Result<Array, JsValue> {
        let row = Array::new();
        let Some(context) = self.document_context_facts_by_key(key)? else {
            row.push(&JsValue::from_bool(false));
            return Ok(row);
        };
        row.push(&JsValue::from_bool(true));
        match self.document_previous_signer_public_key(key, &context) {
            Some(public_key) => row.push(&Uint8Array::from(public_key.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
        Ok(row)
    }

    pub fn document_context_previous_signature_public_key_batch(
        &self,
        keys: Vec<String>,
    ) -> Result<Array, JsValue> {
        let rows = Array::new_with_length(keys.len() as u32);
        for (index, key) in keys.iter().enumerate() {
            let row = Array::new();
            match self.document_context_facts_by_key(key)? {
                Some(context) => {
                    row.push(&document_context_facts_to_row(&context));
                    match self.document_previous_signer_public_key(key, &context) {
                        Some(public_key) => row.push(&Uint8Array::from(public_key.as_slice())),
                        None => row.push(&JsValue::UNDEFINED),
                    };
                }
                None => {
                    row.push(&JsValue::UNDEFINED);
                    row.push(&JsValue::UNDEFINED);
                }
            }
            rows.set(index as u32, row.into());
        }
        Ok(rows)
    }

    pub fn document_query(
        &self,
        query_bytes: Vec<u8>,
        sort_bytes: Vec<u8>,
    ) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes).map_err(BackboneError::Message)?;
        let sort = decode_sort(&sort_bytes).map_err(BackboneError::Message)?;
        let keys = self.document_index.search(&query, &sort, None);
        Ok(self.document_entries_for_keys(&keys))
    }

    pub fn document_query_page(
        &self,
        query_bytes: Vec<u8>,
        sort_bytes: Vec<u8>,
        offset: usize,
        limit: usize,
    ) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes).map_err(BackboneError::Message)?;
        let sort = decode_sort(&sort_bytes).map_err(BackboneError::Message)?;
        let keys = self
            .document_index
            .search_page(&query, &sort, offset, Some(limit));
        Ok(self.document_entries_for_keys(&keys))
    }

    pub fn document_count(&self, query_bytes: Vec<u8>) -> Result<usize, JsValue> {
        let query = decode_query(&query_bytes).map_err(BackboneError::Message)?;
        Ok(self.document_index.count(&query) as usize)
    }

    pub fn document_sum(&self, query_bytes: Vec<u8>, field: u32) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes).map_err(BackboneError::Message)?;
        let sum = self
            .document_index
            .sum(&query, FieldPath::Id(field))
            .map_err(BackboneError::Message)?;
        Ok(sum_to_js(sum))
    }

    pub fn put_document_encoded_parts_stored(
        &mut self,
        key: String,
        value_prefix_bytes: Vec<u8>,
        value_suffix_bytes: Vec<u8>,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let stored_key = key.clone();
        self.put_document_encoded_parts_stored_inner(
            key,
            value_prefix_bytes,
            value_suffix_bytes,
            byte_element_index_limit,
            false,
            None,
            None,
            true,
        )?;
        self.refresh_document_previous_signer_fact(&stored_key)?;
        Ok(())
    }

    pub fn put_document_encoded_parts_stored_batch(
        &mut self,
        keys: Array,
        value_prefix_bytes: Array,
        value_suffix_bytes: Array,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let keys = strings_from_array(keys)?;
        let value_prefix_bytes = bytes_vec_from_array(value_prefix_bytes)?;
        let value_suffix_bytes = bytes_vec_from_array(value_suffix_bytes)?;
        ensure_same_len(
            keys.len(),
            value_prefix_bytes.len(),
            "document value prefix",
        )?;
        ensure_same_len(
            keys.len(),
            value_suffix_bytes.len(),
            "document value suffix",
        )?;
        self.document_values.reserve(keys.len());
        self.document_index.reserve_documents(keys.len());

        for ((key, prefix), suffix) in keys
            .into_iter()
            .zip(value_prefix_bytes.into_iter())
            .zip(value_suffix_bytes.into_iter())
        {
            let stored_key = key.clone();
            self.put_document_encoded_parts_stored_inner(
                key,
                prefix,
                suffix,
                byte_element_index_limit,
                false,
                None,
                None,
                true,
            )?;
            self.refresh_document_previous_signer_fact(&stored_key)?;
        }

        Ok(())
    }

    pub fn delete_document(&mut self, key: &str) -> bool {
        self.delete_document_inner(key, true)
    }

    pub fn delete_documents(&mut self, keys: Array) -> Result<u32, JsValue> {
        let keys = strings_from_array(keys)?;
        let mut deleted = 0u32;
        for key in keys {
            if self.delete_document_inner(&key, true) {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    pub fn delete_documents_result(&mut self, keys: Array) -> Result<Uint8Array, JsValue> {
        let keys = strings_from_array(keys)?;
        let mut deleted = Vec::with_capacity(keys.len());
        for key in keys {
            deleted.push(u8::from(self.delete_document_inner(&key, true)));
        }
        Ok(Uint8Array::from(deleted.as_slice()))
    }

    pub fn clear_document_index(&mut self) {
        if self.document_journal_enabled {
            let keys: Vec<String> = self
                .document_values
                .entries()
                .into_iter()
                .map(|(key, _)| key.to_string())
                .collect();
            for key in keys {
                self.push_document_journal_delete(&key);
            }
        }
        self.document_index.clear();
        self.document_values.clear();
        self.document_key_by_head.clear();
    }

    pub fn document_signer_journal_header(&self) -> Vec<u8> {
        JOURNAL_MAGIC.to_vec()
    }

    pub fn document_journal_header(&self) -> Vec<u8> {
        JOURNAL_MAGIC.to_vec()
    }

    pub fn document_pending_journal_len(&self) -> usize {
        self.document_journal_record_count
    }

    pub fn document_pending_journal_byte_len(&self) -> usize {
        self.document_journal.len()
    }

    pub fn document_journal_enabled(&self) -> bool {
        self.document_journal_enabled
    }

    pub fn set_document_journal_enabled(&mut self, enabled: bool) {
        self.document_journal_enabled = enabled;
        if !enabled {
            self.document_journal.clear();
            self.document_journal_record_count = 0;
        }
    }

    pub fn document_journal(&self) -> Vec<u8> {
        self.document_journal.clone()
    }

    pub fn clear_document_journal(&mut self) {
        self.document_journal.clear();
        self.document_journal_record_count = 0;
    }

    pub fn clear_document_journal_prefix(&mut self, byte_len: usize, record_count: usize) {
        clear_journal_prefix(
            &mut self.document_journal,
            &mut self.document_journal_record_count,
            byte_len,
            record_count,
        );
    }

    pub fn document_snapshot(&self) -> Vec<u8> {
        encode_key_value_snapshot(self.document_values.entries())
    }

    pub fn load_document_snapshot_and_journal(
        &mut self,
        snapshot: Uint8Array,
        journal: Uint8Array,
    ) -> Result<usize, JsValue> {
        let mut entries = if snapshot.length() == 0 {
            Default::default()
        } else {
            decode_key_value_snapshot(&snapshot.to_vec()).map_err(BackboneError::from)?
        };
        let journal_records = if journal.length() == 0 {
            Vec::new()
        } else {
            decode_journal(&journal.to_vec()).map_err(BackboneError::from)?
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

        self.document_values.clear();
        for (key, value) in entries {
            self.document_values.put(key, value);
        }
        self.rebuild_document_index_from_values()?;
        self.document_journal.clear();
        self.document_journal_record_count = 0;
        Ok(operations)
    }

    pub fn document_signer_pending_journal_len(&self) -> usize {
        self.document_signer_journal_record_count
    }

    pub fn document_signer_pending_journal_byte_len(&self) -> usize {
        self.document_signer_journal.len()
    }

    pub fn document_signer_journal_enabled(&self) -> bool {
        self.document_signer_journal_enabled
    }

    pub fn set_document_signer_journal_enabled(&mut self, enabled: bool) {
        self.document_signer_journal_enabled = enabled;
        if !enabled {
            self.document_signer_journal.clear();
            self.document_signer_journal_record_count = 0;
        }
    }

    pub fn document_signer_journal(&self) -> Vec<u8> {
        self.document_signer_journal.clone()
    }

    pub fn clear_document_signer_journal(&mut self) {
        self.document_signer_journal.clear();
        self.document_signer_journal_record_count = 0;
    }

    pub fn clear_document_signer_journal_prefix(&mut self, byte_len: usize, record_count: usize) {
        clear_journal_prefix(
            &mut self.document_signer_journal,
            &mut self.document_signer_journal_record_count,
            byte_len,
            record_count,
        );
    }

    pub fn document_signer_snapshot(&self) -> Vec<u8> {
        encode_key_value_snapshot(
            self.document_previous_signer_by_key
                .iter()
                .map(|(key, fact)| (key.clone(), encode_document_signer_fact(fact))),
        )
    }

    pub fn load_document_signer_snapshot_and_journal(
        &mut self,
        snapshot: Uint8Array,
        journal: Uint8Array,
    ) -> Result<usize, JsValue> {
        let mut entries = if snapshot.length() == 0 {
            Default::default()
        } else {
            decode_key_value_snapshot(&snapshot.to_vec()).map_err(BackboneError::from)?
        };
        let journal_records = if journal.length() == 0 {
            Vec::new()
        } else {
            decode_journal(&journal.to_vec()).map_err(BackboneError::from)?
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

        self.document_previous_signer_by_key.clear();
        for (key, value) in entries {
            self.document_previous_signer_by_key
                .insert(key, decode_document_signer_fact(&value)?);
        }
        self.document_signer_journal.clear();
        self.document_signer_journal_record_count = 0;
        Ok(operations)
    }
}

impl NativePeerbitBackbone {
    fn document_previous_signer_public_key(
        &self,
        key: &str,
        context: &DocumentContextFacts,
    ) -> Option<Vec<u8>> {
        self.blocks
            .get_ref(&context.head)
            .and_then(|bytes| entry_v0_signature_public_key_from_storage_bytes(bytes).ok())
            .or_else(|| {
                self.document_previous_signer_by_key
                    .get(key)
                    .filter(|fact| fact.head == context.head)
                    .map(|fact| fact.public_key.clone())
            })
    }

    pub(crate) fn document_context_facts_by_key(
        &self,
        key: &str,
    ) -> Result<Option<DocumentContextFacts>, BackboneError> {
        let Some(document_fields) = self.document_index.document_fields_by_id(key) else {
            return Ok(None);
        };
        self.document_context_facts_from_fields(document_fields)
    }

    fn document_context_facts_from_fields(
        &self,
        document_fields: &DocumentFields,
    ) -> Result<Option<DocumentContextFacts>, BackboneError> {
        let Some(fields) = self.document_context_fields else {
            return Ok(None);
        };
        let created = document_u64_field(document_fields, fields.created)
            .ok_or(BackboneError::MissingDocumentContextField("created"))?;
        let modified = document_u64_field(document_fields, fields.modified)
            .ok_or(BackboneError::MissingDocumentContextField("modified"))?;
        let head = document_string_field(document_fields, fields.head)
            .ok_or(BackboneError::MissingDocumentContextField("head"))?;
        let gid = document_string_field(document_fields, fields.gid)
            .ok_or(BackboneError::MissingDocumentContextField("gid"))?;
        let size = document_u64_field(document_fields, fields.size)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(BackboneError::MissingDocumentContextField("size"))?;
        Ok(Some(DocumentContextFacts {
            created,
            modified,
            head,
            gid,
            size,
        }))
    }

    pub(crate) fn validate_document_index_required_previous_signer(
        &self,
        document_index_commit: &DocumentIndexAppendCommit,
    ) -> Result<(), JsValue> {
        let Some(required_public_key) = document_index_commit
            .required_previous_signer_public_key
            .as_ref()
        else {
            return Ok(());
        };
        let Some(previous_context) = document_index_commit.previous_context.as_ref() else {
            return Ok(());
        };
        let previous_public_key = self
            .document_previous_signer_public_key(&document_index_commit.key, previous_context)
            .ok_or_else(|| JsValue::from_str("Previous document signer public key unavailable"))?;
        if previous_public_key.as_slice() != required_public_key.as_slice() {
            return Err(JsValue::from_str(
                "Previous document signer public key did not match native policy",
            ));
        }
        Ok(())
    }

    fn put_document_encoded_parts_stored_inner(
        &mut self,
        key: String,
        value_prefix_bytes: Vec<u8>,
        value_suffix_bytes: Vec<u8>,
        byte_element_index_limit: usize,
        known_existing: bool,
        new_head: Option<&str>,
        previous_head: Option<&str>,
        record_document_journal: bool,
    ) -> Result<(), BackboneError> {
        let prepared = self.prepare_document_encoded_parts_put(
            key,
            value_prefix_bytes,
            value_suffix_bytes,
            byte_element_index_limit,
            known_existing,
            new_head,
            previous_head,
            record_document_journal,
        )?;
        self.commit_prepared_document_encoded_parts_put(prepared);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_document_encoded_parts_put(
        &mut self,
        key: String,
        mut value_prefix_bytes: Vec<u8>,
        value_suffix_bytes: Vec<u8>,
        byte_element_index_limit: usize,
        known_existing: bool,
        new_head: Option<&str>,
        previous_head: Option<&str>,
        record_document_journal: bool,
    ) -> Result<PreparedDocumentEncodedPartsPut, BackboneError> {
        self.document_byte_element_index_limit = byte_element_index_limit;
        let profile_enabled = self.append_profile_enabled;
        let extract_started = profile_enabled.then(crate::time::now_ms);
        let fields = {
            let schema_ir = self
                .document_schema_ir
                .as_ref()
                .ok_or(BackboneError::DocumentSchemaIrNotConfigured)?;
            extract_encoded_document_fields_from_parts_with_byte_limits(
                &schema_ir,
                &value_prefix_bytes,
                &value_suffix_bytes,
                byte_element_index_limit,
                NATIVE_BACKBONE_BYTE_EXACT_INDEX_LIMIT,
            )?
        };
        if let Some(started) = extract_started {
            self.append_profile.document_index_extract_ms += crate::time::now_ms() - started;
        }
        let value_build_started = profile_enabled.then(crate::time::now_ms);
        value_prefix_bytes.reserve(value_suffix_bytes.len());
        value_prefix_bytes.extend_from_slice(&value_suffix_bytes);
        if let Some(started) = value_build_started {
            self.append_profile.document_index_value_build_ms += crate::time::now_ms() - started;
        }
        Ok(PreparedDocumentEncodedPartsPut {
            key,
            value_bytes: value_prefix_bytes,
            fields,
            known_existing,
            new_head: new_head.map(str::to_string),
            previous_head: previous_head.map(str::to_string),
            record_document_journal,
        })
    }

    pub(crate) fn commit_prepared_document_encoded_parts_put(
        &mut self,
        prepared: PreparedDocumentEncodedPartsPut,
    ) {
        let PreparedDocumentEncodedPartsPut {
            key,
            value_bytes,
            fields,
            known_existing,
            new_head,
            previous_head,
            record_document_journal,
        } = prepared;
        let profile_enabled = self.append_profile_enabled;
        let should_record_document_journal =
            record_document_journal && self.document_journal_enabled;
        if should_record_document_journal {
            self.push_document_journal_put(&key, &value_bytes);
        }
        let value_put_started = profile_enabled.then(crate::time::now_ms);
        let was_existing = if known_existing {
            self.document_values.put(key.clone(), value_bytes);
            true
        } else {
            self.document_values
                .put_return_previous(key.clone(), value_bytes)
                .is_some()
        };
        if let Some(started) = value_put_started {
            self.append_profile.document_value_put_ms += crate::time::now_ms() - started;
        }
        let index_put_started = profile_enabled.then(crate::time::now_ms);
        if known_existing || was_existing {
            self.document_index.put(&key, fields);
        } else {
            self.document_index.put_new_unchecked(&key, fields);
        }
        if let Some(started) = index_put_started {
            self.append_profile.document_index_put_ms += crate::time::now_ms() - started;
        }
        self.update_document_head_key(
            &key,
            new_head.as_deref(),
            previous_head.as_deref(),
            was_existing,
        );
    }

    fn update_document_head_key(
        &mut self,
        key: &str,
        new_head: Option<&str>,
        previous_head: Option<&str>,
        was_existing: bool,
    ) {
        let Some(new_head) = new_head else {
            return;
        };
        if self.document_context_head_field.is_none() {
            return;
        }
        if let Some(previous_head) = previous_head {
            if previous_head != new_head {
                self.document_key_by_head.remove(previous_head);
            }
        } else if was_existing {
            self.document_key_by_head
                .retain(|_, existing_key| existing_key != key);
        }
        self.document_key_by_head
            .insert(new_head.to_string(), key.to_string());
    }

    fn refresh_document_previous_signer_fact(&mut self, key: &str) -> Result<(), BackboneError> {
        let Some(context) = self.document_context_facts_by_key(key)? else {
            self.delete_document_previous_signer_fact(key, true);
            return Ok(());
        };
        if let Some(public_key) = self
            .blocks
            .get_ref(&context.head)
            .and_then(|bytes| entry_v0_signature_public_key_from_storage_bytes(bytes).ok())
        {
            self.put_document_previous_signer_fact(key.to_string(), context.head, public_key, true);
            return Ok(());
        }
        // Rebuilding the document index can observe context state before the
        // corresponding block is resident. Keep durable signer facts in that
        // case; lookups filter by the current head so stale facts are inert,
        // while deleting them here would make strict native same-signer
        // policies lose their persisted proof across reopen.
        Ok(())
    }

    pub(crate) fn delete_document_inner(
        &mut self,
        key: &str,
        record_document_journal: bool,
    ) -> bool {
        if let Ok(Some(context)) = self.document_context_facts_by_key(key) {
            self.document_key_by_head.remove(&context.head);
        } else {
            self.document_key_by_head
                .retain(|_, existing_key| existing_key != key);
        }
        self.document_index.delete_id(key);
        self.delete_document_previous_signer_fact(key, true);
        let deleted = self.document_values.delete(key);
        if deleted && record_document_journal && self.document_journal_enabled {
            self.push_document_journal_delete(key);
        }
        deleted
    }

    pub(crate) fn clear_document_core(&mut self) {
        self.document_index.clear();
        self.document_values.clear();
        self.document_journal.clear();
        self.document_journal_record_count = 0;
        self.document_key_by_head.clear();
        self.document_previous_signer_by_key.clear();
        self.document_signer_journal.clear();
        self.document_signer_journal_record_count = 0;
    }

    fn rebuild_document_index_from_values(&mut self) -> Result<(), BackboneError> {
        let Some(schema_ir) = self.document_schema_ir.clone() else {
            self.document_index.clear();
            self.document_key_by_head.clear();
            return Ok(());
        };
        let values: Vec<(String, Vec<u8>)> = self
            .document_values
            .entries()
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_vec()))
            .collect();
        self.document_index.clear();
        self.document_key_by_head.clear();
        self.document_index.reserve_documents(values.len());
        for (key, value) in values {
            let fields = extract_encoded_document_fields_from_parts_with_byte_limits(
                &schema_ir,
                &value,
                &[],
                self.document_byte_element_index_limit,
                NATIVE_BACKBONE_BYTE_EXACT_INDEX_LIMIT,
            )?;
            if let Some(context) = self.document_context_facts_from_fields(&fields)? {
                self.document_key_by_head.insert(context.head, key.clone());
            }
            self.document_index.put_new_unchecked(key, fields);
        }
        Ok(())
    }

    fn rebuild_document_head_keys(&mut self) {
        let Some(fields) = self.document_context_fields else {
            self.document_key_by_head.clear();
            return;
        };
        let values: Vec<(String, Vec<u8>)> = self
            .document_values
            .entries()
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_vec()))
            .collect();
        self.document_key_by_head.clear();
        for (key, _) in values {
            let Some(document_fields) = self.document_index.document_fields_by_id(&key) else {
                continue;
            };
            let Some(head) = document_string_field(document_fields, fields.head) else {
                continue;
            };
            self.document_key_by_head.insert(head, key);
        }
    }

    pub(crate) fn put_document_previous_signer_fact(
        &mut self,
        key: String,
        head: String,
        public_key: Vec<u8>,
        record_journal: bool,
    ) {
        let should_record = record_journal
            && self.document_signer_journal_enabled
            && !self
                .document_previous_signer_by_key
                .get(&key)
                .is_some_and(|fact| fact.head == head && fact.public_key == public_key);
        let fact = DocumentPreviousSignerFact { head, public_key };
        if should_record {
            self.push_document_signer_journal_put(&key, &encode_document_signer_fact(&fact));
        }
        self.document_previous_signer_by_key.insert(key, fact);
    }

    fn delete_document_previous_signer_fact(&mut self, key: &str, record_journal: bool) {
        let existed = self.document_previous_signer_by_key.remove(key).is_some();
        if existed && record_journal && self.document_signer_journal_enabled {
            self.push_document_signer_journal_delete(key);
        }
    }

    fn push_document_signer_journal_put(&mut self, key: &str, value: &[u8]) {
        append_journal_put_record(&mut self.document_signer_journal, key, value);
        self.document_signer_journal_record_count += 1;
    }

    fn push_document_signer_journal_delete(&mut self, key: &str) {
        append_journal_delete_record(&mut self.document_signer_journal, key);
        self.document_signer_journal_record_count += 1;
    }

    fn push_document_journal_put(&mut self, key: &str, value: &[u8]) {
        append_journal_put_record(&mut self.document_journal, key, value);
        self.document_journal_record_count += 1;
    }

    fn push_document_journal_delete(&mut self, key: &str) {
        append_journal_delete_record(&mut self.document_journal, key);
        self.document_journal_record_count += 1;
    }

    fn document_entries_for_keys(&self, keys: &[String]) -> Array {
        let out = Array::new();
        for key in keys {
            if let Some(value) = self.document_values.get(key) {
                out.push(&document_entry_to_row(key, value));
            }
        }
        out
    }

    pub(crate) fn reserve_document_batch(&mut self, batch_len: usize) {
        self.document_values.reserve(batch_len);
        self.document_index.reserve_documents(batch_len);
        self.document_key_by_head.reserve(batch_len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projection_plan(
        source_kinds: &[&str],
        source_values: &[&str],
        output_field_types: &[&str],
    ) -> ParsedProjectionPlan {
        ParsedProjectionPlan {
            document_variant_type: None,
            document_variant_value: None,
            output_variant_type: None,
            output_variant_value: None,
            document_field_names: Vec::new(),
            document_field_types: Vec::new(),
            output_field_types: output_field_types.iter().map(|s| s.to_string()).collect(),
            source_kinds: source_kinds.iter().map(|s| s.to_string()).collect(),
            source_values: source_values.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn plain_put_payload_reports_typed_errors() {
        let error = plain_put_document_bytes_from_payload(&[0, 3]).unwrap_err();
        assert_eq!(error, BackboneError::PlainPutPayloadTooShort);
        assert_eq!(error.to_string(), "Plain put payload is too short");

        let error = plain_put_document_bytes_from_payload(&[1, 3, 0, 0, 0, 0]).unwrap_err();
        assert_eq!(error, BackboneError::Expected("native plain put payload"));
        assert_eq!(error.to_string(), "Expected native plain put payload");

        let error = plain_put_document_bytes_from_payload(&[0, 3, 2, 0, 0, 0, 9]).unwrap_err();
        assert_eq!(error, BackboneError::PlainPutPayloadLengthMismatch);
        assert_eq!(error.to_string(), "Plain put payload length mismatch");
    }

    #[test]
    fn document_signer_fact_decode_reports_typed_errors() {
        let fact = DocumentPreviousSignerFact {
            head: "head".to_string(),
            public_key: vec![1, 2, 3],
        };
        let encoded = encode_document_signer_fact(&fact);
        let decoded = decode_document_signer_fact(&encoded).unwrap();
        assert_eq!(decoded.head, fact.head);
        assert_eq!(decoded.public_key, fact.public_key);

        let mut trailing = encoded.clone();
        trailing.push(0);
        let error = match decode_document_signer_fact(&trailing) {
            Ok(_) => panic!("expected trailing-bytes error"),
            Err(error) => error,
        };
        assert_eq!(error, BackboneError::TrailingDocumentSignerFactBytes);
        assert_eq!(error.to_string(), "Trailing document signer fact bytes");
    }

    #[test]
    fn projection_plan_length_mismatch_renders_historical_string() {
        assert_eq!(
            BackboneError::ProjectionPlanLengthMismatch.to_string(),
            "Projection plan length mismatch"
        );
    }

    #[test]
    fn query_decode_errors_forward_the_indexer_string_verbatim() {
        // decode_query/decode_sort have no typed core error; the document
        // query/count/sum boundaries forward their String verbatim through
        // BackboneError::Message. Assert the rendered message is byte-for-byte
        // the string the indexer core produced (matching the old js_error
        // funnel that also built Message(error.to_string())).
        let raw = decode_query(&[0xff]).unwrap_err();
        assert_eq!(BackboneError::Message(raw.clone()).to_string(), raw);
    }

    #[test]
    fn projected_document_fields_report_typed_errors() {
        let error =
            read_projected_document_fields(&[], None, None, &["a".to_string()], &[]).unwrap_err();
        assert_eq!(error, BackboneError::DocumentProjectionPlanLengthMismatch);
        assert_eq!(
            error.to_string(),
            "Document projection plan length mismatch"
        );

        let error =
            read_projected_document_fields(&[], Some("u16"), Some("1"), &[], &[]).unwrap_err();
        assert_eq!(error, BackboneError::UnsupportedDocumentVariantType);
        assert_eq!(error.to_string(), "Unsupported document variant type");

        let error = read_projected_document_fields(&[], Some("u8"), None, &[], &[]).unwrap_err();
        assert_eq!(error, BackboneError::MissingDocumentVariant);
        assert_eq!(error.to_string(), "Missing document variant");

        let error =
            read_projected_document_fields(&[], Some("u8"), Some("abc"), &[], &[]).unwrap_err();
        assert_eq!(error, BackboneError::InvalidDocumentVariant);
        assert_eq!(error.to_string(), "Invalid document variant");

        let error =
            read_projected_document_fields(&[], Some("u8"), Some("1"), &[], &[]).unwrap_err();
        assert_eq!(error, BackboneError::Truncated("document variant"));
        assert_eq!(error.to_string(), "Truncated document variant");

        let error =
            read_projected_document_fields(&[0], Some("u8"), Some("1"), &[], &[]).unwrap_err();
        assert_eq!(error, BackboneError::DocumentVariantMismatch);
        assert_eq!(error.to_string(), "Document variant mismatch");
    }

    #[test]
    fn projection_variant_and_value_writers_report_typed_errors() {
        let mut out = Vec::new();
        let error = write_projection_variant(&mut out, Some("u8"), None).unwrap_err();
        assert_eq!(error, BackboneError::MissingOutputVariant);
        assert_eq!(error.to_string(), "Missing output variant");

        let error = write_projection_variant(&mut out, Some("u8"), Some("abc")).unwrap_err();
        assert_eq!(error, BackboneError::InvalidOutputVariant);
        assert_eq!(error.to_string(), "Invalid output variant");

        let error = write_projection_variant(&mut out, Some("u16"), Some("1")).unwrap_err();
        assert_eq!(error, BackboneError::UnsupportedOutputVariantType);
        assert_eq!(error.to_string(), "Unsupported output variant type");

        let error = write_projection_value(&mut out, "string", &ProjectionValue::None).unwrap_err();
        assert_eq!(error, BackboneError::ProjectionValueOutputTypeMismatch);
        assert_eq!(
            error.to_string(),
            "Projection value does not match output type"
        );
    }

    #[test]
    fn projection_value_readers_report_typed_errors() {
        let mut offset = 0usize;
        let error = read_projection_value(&[2], &mut offset, "bool").unwrap_err();
        assert_eq!(error, BackboneError::InvalidBool("projection bool"));
        assert_eq!(error.to_string(), "Invalid bool projection bool");

        let mut offset = 0usize;
        let error = read_projection_value(&[2], &mut offset, "option:u8").unwrap_err();
        assert_eq!(error, BackboneError::InvalidProjectionOptionMarker);
        assert_eq!(error.to_string(), "Invalid projection option marker");

        let mut offset = 0usize;
        let error = read_projection_value(&[], &mut offset, "u128").unwrap_err();
        assert_eq!(error, BackboneError::UnsupportedProjectedDocumentFieldType);
        assert_eq!(
            error.to_string(),
            "Unsupported projected document field type"
        );

        let mut offset = 0usize;
        let error = skip_projection_value(&[], &mut offset, "u128").unwrap_err();
        assert_eq!(error, BackboneError::UnsupportedDocumentProjectionFieldType);
        assert_eq!(
            error.to_string(),
            "Unsupported document projection field type"
        );
    }

    #[test]
    fn projection_sources_report_typed_errors() {
        let plan = projection_plan(&["context"], &["bogus"], &["u64"]);
        let error =
            project_document_index_simple_bytes_with_plan(&[], &plan, 0, 0, "h", "g", 0, None)
                .unwrap_err();
        assert_eq!(error, BackboneError::UnsupportedContextProjectionSource);
        assert_eq!(error.to_string(), "Unsupported context projection source");

        let plan = projection_plan(&["bogus"], &["x"], &["u64"]);
        let error =
            project_document_index_simple_bytes_with_plan(&[], &plan, 0, 0, "h", "g", 0, None)
                .unwrap_err();
        assert_eq!(error, BackboneError::UnsupportedProjectionSourceKind);
        assert_eq!(error.to_string(), "Unsupported projection source kind");
    }

    #[test]
    fn document_context_field_variants_render_exact_messages() {
        for label in ["created", "modified", "head", "gid", "size"] {
            assert_eq!(
                BackboneError::MissingDocumentContextField(label).to_string(),
                format!("Missing document context {label} field")
            );
        }
        assert_eq!(
            BackboneError::MissingDocumentContextField("created").to_string(),
            "Missing document context created field"
        );
    }

    #[test]
    fn document_index_put_chain_variants_render_exact_messages() {
        for (error, message) in [
            (
                BackboneError::DocumentContextSuffixCapacityOverflow,
                "Document context suffix capacity overflow",
            ),
            (
                BackboneError::DocumentSchemaIrNotConfigured,
                "Native backbone document schema IR has not been configured",
            ),
            (
                BackboneError::MissingCachedDocumentProjectionPlan,
                "Missing cached document projection plan",
            ),
            (
                BackboneError::MissingPlainPutPayloadForDocumentIndex,
                "Missing plain put payload for document index",
            ),
            (
                BackboneError::MissingPlainPutPayloadForDocumentProjection,
                "Missing plain put payload for document projection",
            ),
        ] {
            assert_eq!(error.to_string(), message);
        }
    }
}
