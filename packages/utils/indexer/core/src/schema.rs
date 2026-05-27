use crate::planner::{DocumentFields, FieldPath, FieldValue};
use std::fmt;

const BRIDGE_VERSION: u8 = 1;

#[derive(Clone, Debug)]
pub struct NativeSchemaIr {
    root: NativeSchemaNode,
}

#[derive(Clone, Debug)]
struct NativeSchemaField {
    key: String,
    field: u32,
    array_field: u32,
    node: NativeSchemaNode,
}

#[derive(Clone, Debug)]
enum NativeSchemaNode {
    Bool,
    U8,
    U16,
    U32,
    U64,
    U128,
    U256,
    U512,
    I8,
    I16,
    I32,
    I64,
    String,
    Uint8Array,
    Object {
        variant_prefix: Vec<u8>,
        fields: Vec<NativeSchemaField>,
    },
    Option(Box<NativeSchemaNode>),
    Vec(Box<NativeSchemaNode>),
    FixedArray {
        length: u32,
        element: Box<NativeSchemaNode>,
    },
    Generic,
    PublicSignKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeSchemaIrStats {
    pub root_fields: usize,
    pub node_count: usize,
    pub generic_nodes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaError {
    InvalidVersion(u8),
    UnknownSchemaNode(u8),
    InvalidBool(u8),
    InvalidOption(u8),
    VariantPrefixMismatch,
    MissingFieldMetadata,
    GenericNode,
    ScopeOverflow,
    OffsetOverflow,
    UnexpectedEof,
    TrailingBytes,
    InvalidUtf8,
    InvalidFieldValueTag(u8),
    UnsupportedPublicSignKeyVariant(u8),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidVersion(version) => {
                write!(formatter, "unsupported bridge version {version}")
            }
            Self::UnknownSchemaNode(tag) => {
                write!(formatter, "unknown native schema node tag {tag}")
            }
            Self::InvalidBool(value) => write!(formatter, "invalid bool value {value}"),
            Self::InvalidOption(tag) => write!(formatter, "invalid option tag {tag}"),
            Self::VariantPrefixMismatch => write!(
                formatter,
                "Borsh variant prefix did not match native schema"
            ),
            Self::MissingFieldMetadata => write!(
                formatter,
                "native schema scalar node is missing field metadata"
            ),
            Self::GenericNode => write!(
                formatter,
                "native schema IR contains a generic node that cannot be extracted from Borsh"
            ),
            Self::ScopeOverflow => write!(formatter, "native schema extraction scope overflow"),
            Self::OffsetOverflow => write!(formatter, "bridge payload offset overflow"),
            Self::UnexpectedEof => write!(formatter, "unexpected end of bridge payload"),
            Self::TrailingBytes => write!(formatter, "trailing bytes in bridge payload"),
            Self::InvalidUtf8 => write!(formatter, "invalid utf-8 in bridge payload"),
            Self::InvalidFieldValueTag(tag) => {
                write!(formatter, "unknown bridge field value tag {tag}")
            }
            Self::UnsupportedPublicSignKeyVariant(tag) => {
                write!(formatter, "unsupported public sign key variant {tag}")
            }
        }
    }
}

impl std::error::Error for SchemaError {}

impl NativeSchemaIr {
    pub fn stats(&self) -> NativeSchemaIrStats {
        NativeSchemaIrStats {
            root_fields: match &self.root {
                NativeSchemaNode::Object { fields, .. } => fields.len(),
                _ => 0,
            },
            node_count: self.root.node_count(),
            generic_nodes: self.root.generic_count(),
        }
    }

    fn scalar_capacity(&self, byte_element_index_limit: usize) -> usize {
        self.root.scalar_capacity(byte_element_index_limit)
    }
}

impl NativeSchemaNode {
    fn node_count(&self) -> usize {
        match self {
            NativeSchemaNode::Object { fields, .. } => {
                1 + fields
                    .iter()
                    .map(|field| {
                        let _ = (&field.key, field.field, field.array_field);
                        field.node.node_count()
                    })
                    .sum::<usize>()
            }
            NativeSchemaNode::Option(node) | NativeSchemaNode::Vec(node) => 1 + node.node_count(),
            NativeSchemaNode::FixedArray { length, element } => {
                let _ = length;
                1 + element.node_count()
            }
            NativeSchemaNode::Bool
            | NativeSchemaNode::U8
            | NativeSchemaNode::U16
            | NativeSchemaNode::U32
            | NativeSchemaNode::U64
            | NativeSchemaNode::U128
            | NativeSchemaNode::U256
            | NativeSchemaNode::U512
            | NativeSchemaNode::I8
            | NativeSchemaNode::I16
            | NativeSchemaNode::I32
            | NativeSchemaNode::I64
            | NativeSchemaNode::String
            | NativeSchemaNode::Uint8Array
            | NativeSchemaNode::PublicSignKey
            | NativeSchemaNode::Generic => 1,
        }
    }

    fn generic_count(&self) -> usize {
        match self {
            NativeSchemaNode::Object { fields, .. } => fields
                .iter()
                .map(|field| field.node.generic_count())
                .sum::<usize>(),
            NativeSchemaNode::Option(node) | NativeSchemaNode::Vec(node) => node.generic_count(),
            NativeSchemaNode::FixedArray { element, .. } => element.generic_count(),
            NativeSchemaNode::Generic => 1,
            NativeSchemaNode::Bool
            | NativeSchemaNode::U8
            | NativeSchemaNode::U16
            | NativeSchemaNode::U32
            | NativeSchemaNode::U64
            | NativeSchemaNode::U128
            | NativeSchemaNode::U256
            | NativeSchemaNode::U512
            | NativeSchemaNode::I8
            | NativeSchemaNode::I16
            | NativeSchemaNode::I32
            | NativeSchemaNode::I64
            | NativeSchemaNode::String
            | NativeSchemaNode::Uint8Array
            | NativeSchemaNode::PublicSignKey => 0,
        }
    }

    fn scalar_capacity(&self, byte_element_index_limit: usize) -> usize {
        match self {
            NativeSchemaNode::Object { fields, .. } => fields
                .iter()
                .map(|field| field.node.scalar_capacity(byte_element_index_limit))
                .sum(),
            NativeSchemaNode::Option(node) => node.scalar_capacity(byte_element_index_limit),
            NativeSchemaNode::Vec(node) => match node.as_ref() {
                NativeSchemaNode::U8 => 1,
                node => 1 + node.scalar_capacity(byte_element_index_limit),
            },
            NativeSchemaNode::FixedArray { length, element } => match element.as_ref() {
                NativeSchemaNode::U8 => {
                    1 + if (*length as usize) <= byte_element_index_limit {
                        *length as usize
                    } else {
                        0
                    }
                }
                node => (*length as usize) * (1 + node.scalar_capacity(byte_element_index_limit)),
            },
            NativeSchemaNode::Bool
            | NativeSchemaNode::U8
            | NativeSchemaNode::U16
            | NativeSchemaNode::U32
            | NativeSchemaNode::U64
            | NativeSchemaNode::U128
            | NativeSchemaNode::U256
            | NativeSchemaNode::U512
            | NativeSchemaNode::I8
            | NativeSchemaNode::I16
            | NativeSchemaNode::I32
            | NativeSchemaNode::I64
            | NativeSchemaNode::String
            | NativeSchemaNode::Uint8Array
            | NativeSchemaNode::PublicSignKey => 1,
            NativeSchemaNode::Generic => 0,
        }
    }
}

pub fn decode_native_schema_ir(schema_ir_bytes: &[u8]) -> Result<NativeSchemaIr, SchemaError> {
    let mut reader = BridgeReader::new(schema_ir_bytes);
    ensure_bridge_version(reader.read_u8()?)?;
    let root = read_native_schema_node(&mut reader)?;
    reader.finish()?;
    Ok(NativeSchemaIr { root })
}

pub fn extract_encoded_document_fields(
    schema_ir: &NativeSchemaIr,
    value_bytes: &[u8],
    byte_element_index_limit: usize,
) -> Result<DocumentFields, SchemaError> {
    extract_encoded_document_fields_from_reader(
        schema_ir,
        BridgeReader::new(value_bytes),
        byte_element_index_limit,
        usize::MAX,
    )
}

pub fn extract_encoded_document_fields_from_parts(
    schema_ir: &NativeSchemaIr,
    prefix: &[u8],
    suffix: &[u8],
    byte_element_index_limit: usize,
) -> Result<DocumentFields, SchemaError> {
    extract_encoded_document_fields_from_parts_with_byte_limits(
        schema_ir,
        prefix,
        suffix,
        byte_element_index_limit,
        usize::MAX,
    )
}

pub fn extract_encoded_document_fields_from_parts_with_byte_limits(
    schema_ir: &NativeSchemaIr,
    prefix: &[u8],
    suffix: &[u8],
    byte_element_index_limit: usize,
    byte_exact_index_limit: usize,
) -> Result<DocumentFields, SchemaError> {
    extract_encoded_document_fields_from_reader(
        schema_ir,
        BridgeReader::from_parts(prefix, suffix),
        byte_element_index_limit,
        byte_exact_index_limit,
    )
}

pub fn decode_document_fields(fields_bytes: &[u8]) -> Result<DocumentFields, SchemaError> {
    let mut reader = BridgeReader::new(fields_bytes);
    ensure_bridge_version(reader.read_u8()?)?;
    let fact_count = reader.read_u32()? as usize;
    let mut fields = DocumentFields::with_scalar_capacity(fact_count);
    for _ in 0..fact_count {
        let scope = reader.read_u32()?;
        let field = reader.read_u32()?;
        let value = reader.read_field_value()?;
        fields.insert_scoped_scalar(scope, field, value);
    }
    reader.finish()?;
    Ok(fields)
}

struct BridgeReader<'a> {
    first: &'a [u8],
    second: &'a [u8],
    len: usize,
    offset: usize,
    scratch: Vec<u8>,
}

impl<'a> BridgeReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self::from_parts(bytes, &[])
    }

    fn from_parts(first: &'a [u8], second: &'a [u8]) -> Self {
        Self {
            first,
            second,
            len: first.len() + second.len(),
            offset: 0,
            scratch: Vec::new(),
        }
    }

    fn finish(&self) -> Result<(), SchemaError> {
        if self.offset == self.len {
            Ok(())
        } else {
            Err(SchemaError::TrailingBytes)
        }
    }

    fn read_u8(&mut self) -> Result<u8, SchemaError> {
        let bytes = self.read_exact(1)?;
        Ok(bytes[0])
    }

    fn read_u32(&mut self) -> Result<u32, SchemaError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.read_exact(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, SchemaError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_exact(8)?);
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, SchemaError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_exact(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String, SchemaError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| SchemaError::InvalidUtf8)
    }

    fn read_field_value(&mut self) -> Result<FieldValue, SchemaError> {
        Ok(match self.read_u8()? {
            0 => match self.read_u8()? {
                0 => FieldValue::Bool(false),
                1 => FieldValue::Bool(true),
                value => return Err(SchemaError::InvalidBool(value)),
            },
            1 => FieldValue::I64(self.read_i64()?),
            2 => FieldValue::U64(self.read_u64()?),
            3 => FieldValue::from(self.read_string()?),
            4 => {
                let len = self.read_u32()? as usize;
                FieldValue::from(self.read_exact(len)?.to_vec())
            }
            tag => return Err(SchemaError::InvalidFieldValueTag(tag)),
        })
    }

    fn read_exact(&mut self, len: usize) -> Result<&[u8], SchemaError> {
        let Some(end) = self.offset.checked_add(len) else {
            return Err(SchemaError::OffsetOverflow);
        };
        if end > self.len {
            return Err(SchemaError::UnexpectedEof);
        }
        if len == 0 {
            return Ok(&[]);
        }
        if self.offset < self.first.len() {
            if end <= self.first.len() {
                let bytes = &self.first[self.offset..end];
                self.offset = end;
                return Ok(bytes);
            }
        } else {
            let start = self.offset - self.first.len();
            let second_end = end - self.first.len();
            let bytes = &self.second[start..second_end];
            self.offset = end;
            return Ok(bytes);
        }

        self.scratch.clear();
        self.scratch.extend_from_slice(&self.first[self.offset..]);
        self.scratch
            .extend_from_slice(&self.second[..end - self.first.len()]);
        self.offset = end;
        Ok(&self.scratch)
    }
}

fn read_native_schema_node(reader: &mut BridgeReader) -> Result<NativeSchemaNode, SchemaError> {
    Ok(match reader.read_u8()? {
        0 => NativeSchemaNode::Bool,
        1 => NativeSchemaNode::U8,
        2 => NativeSchemaNode::U16,
        3 => NativeSchemaNode::U32,
        4 => NativeSchemaNode::U64,
        5 => NativeSchemaNode::U128,
        6 => NativeSchemaNode::U256,
        7 => NativeSchemaNode::U512,
        8 => NativeSchemaNode::I8,
        9 => NativeSchemaNode::I16,
        10 => NativeSchemaNode::I32,
        11 => NativeSchemaNode::I64,
        12 => NativeSchemaNode::String,
        13 => NativeSchemaNode::Uint8Array,
        14 => {
            let variant_prefix_len = reader.read_u32()? as usize;
            let variant_prefix = reader.read_exact(variant_prefix_len)?.to_vec();
            let field_count = reader.read_u32()? as usize;
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                fields.push(NativeSchemaField {
                    key: reader.read_string()?,
                    field: reader.read_u32()?,
                    array_field: reader.read_u32()?,
                    node: read_native_schema_node(reader)?,
                });
            }
            NativeSchemaNode::Object {
                variant_prefix,
                fields,
            }
        }
        15 => NativeSchemaNode::Option(Box::new(read_native_schema_node(reader)?)),
        16 => NativeSchemaNode::Vec(Box::new(read_native_schema_node(reader)?)),
        17 => NativeSchemaNode::FixedArray {
            length: reader.read_u32()?,
            element: Box::new(read_native_schema_node(reader)?),
        },
        18 => NativeSchemaNode::Generic,
        19 => NativeSchemaNode::PublicSignKey,
        tag => return Err(SchemaError::UnknownSchemaNode(tag)),
    })
}

fn extract_encoded_document_fields_from_reader(
    schema_ir: &NativeSchemaIr,
    mut reader: BridgeReader,
    byte_element_index_limit: usize,
    byte_exact_index_limit: usize,
) -> Result<DocumentFields, SchemaError> {
    let mut fields =
        DocumentFields::with_scalar_capacity(schema_ir.scalar_capacity(byte_element_index_limit));
    let mut state = NativeExtractState {
        next_scope: 1,
        byte_element_index_limit,
        byte_exact_index_limit,
    };
    extract_schema_node(
        &schema_ir.root,
        &mut reader,
        &mut fields,
        0,
        &mut state,
        None,
    )?;
    reader.finish()?;
    Ok(fields)
}

struct NativeExtractState {
    next_scope: u32,
    byte_element_index_limit: usize,
    byte_exact_index_limit: usize,
}

impl NativeExtractState {
    fn next_scope(&mut self) -> Result<u32, SchemaError> {
        let scope = self.next_scope;
        self.next_scope = self
            .next_scope
            .checked_add(1)
            .ok_or(SchemaError::ScopeOverflow)?;
        Ok(scope)
    }
}

fn extract_schema_node(
    node: &NativeSchemaNode,
    reader: &mut BridgeReader,
    fields: &mut DocumentFields,
    scope: u32,
    state: &mut NativeExtractState,
    field: Option<&NativeSchemaField>,
) -> Result<(), SchemaError> {
    match node {
        NativeSchemaNode::Bool => {
            let value = match reader.read_u8()? {
                0 => false,
                1 => true,
                value => return Err(SchemaError::InvalidBool(value)),
            };
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::Bool(value),
            );
        }
        NativeSchemaNode::U8 => {
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::U64(reader.read_u8()? as u64),
            );
        }
        NativeSchemaNode::U16 => {
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::U64(read_le_u64_with_width(reader, 2)?.unwrap_or_default()),
            );
        }
        NativeSchemaNode::U32 => {
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::U64(reader.read_u32()? as u64),
            );
        }
        NativeSchemaNode::U64 => {
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::U64(reader.read_u64()?),
            );
        }
        NativeSchemaNode::U128 => {
            if let Some(value) = read_le_u64_with_width(reader, 16)? {
                insert_scalar(
                    fields,
                    scope,
                    required_field(field)?,
                    FieldValue::U64(value),
                );
            }
        }
        NativeSchemaNode::U256 => {
            if let Some(value) = read_le_u64_with_width(reader, 32)? {
                insert_scalar(
                    fields,
                    scope,
                    required_field(field)?,
                    FieldValue::U64(value),
                );
            }
        }
        NativeSchemaNode::U512 => {
            if let Some(value) = read_le_u64_with_width(reader, 64)? {
                insert_scalar(
                    fields,
                    scope,
                    required_field(field)?,
                    FieldValue::U64(value),
                );
            }
        }
        NativeSchemaNode::I8 => {
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::I64((reader.read_u8()? as i8) as i64),
            );
        }
        NativeSchemaNode::I16 => {
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::I64(read_le_i64_with_width(reader, 2)?),
            );
        }
        NativeSchemaNode::I32 => {
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::I64(read_le_i64_with_width(reader, 4)?),
            );
        }
        NativeSchemaNode::I64 => {
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::I64(reader.read_i64()?),
            );
        }
        NativeSchemaNode::String => {
            let value = reader.read_string()?;
            insert_scalar(
                fields,
                scope,
                required_field(field)?,
                FieldValue::from(value),
            );
        }
        NativeSchemaNode::Uint8Array => {
            let len = reader.read_u32()? as usize;
            let bytes = reader.read_exact(len)?;
            insert_bytes_facts(fields, state, scope, required_field(field)?, bytes)?;
        }
        NativeSchemaNode::Object {
            variant_prefix,
            fields: schema_fields,
        } => {
            if !variant_prefix.is_empty() {
                let actual = reader.read_exact(variant_prefix.len())?;
                if actual != variant_prefix.as_slice() {
                    return Err(SchemaError::VariantPrefixMismatch);
                }
            }
            for schema_field in schema_fields {
                extract_schema_node(
                    &schema_field.node,
                    reader,
                    fields,
                    scope,
                    state,
                    Some(schema_field),
                )?;
            }
        }
        NativeSchemaNode::Option(child) => match reader.read_u8()? {
            0 => {}
            1 => extract_schema_node(child, reader, fields, scope, state, field)?,
            tag => return Err(SchemaError::InvalidOption(tag)),
        },
        NativeSchemaNode::Vec(child) if matches!(child.as_ref(), NativeSchemaNode::U8) => {
            let len = reader.read_u32()? as usize;
            let bytes = reader.read_exact(len)?;
            insert_bytes_facts(fields, state, scope, required_field(field)?, bytes)?;
        }
        NativeSchemaNode::Vec(child) => {
            let len = reader.read_u32()? as usize;
            let field = required_schema_field(field)?;
            for _ in 0..len {
                let item_scope = state.next_scope()?;
                insert_scalar(
                    fields,
                    item_scope,
                    field.array_field,
                    FieldValue::Bool(true),
                );
                extract_schema_node(child, reader, fields, item_scope, state, Some(field))?;
            }
        }
        NativeSchemaNode::FixedArray { length, element }
            if matches!(element.as_ref(), NativeSchemaNode::U8) =>
        {
            let bytes = reader.read_exact(*length as usize)?;
            insert_bytes_facts(fields, state, scope, required_field(field)?, bytes)?;
        }
        NativeSchemaNode::FixedArray { length, element } => {
            let field = required_schema_field(field)?;
            for _ in 0..*length {
                let item_scope = state.next_scope()?;
                insert_scalar(
                    fields,
                    item_scope,
                    field.array_field,
                    FieldValue::Bool(true),
                );
                extract_schema_node(element, reader, fields, item_scope, state, Some(field))?;
            }
        }
        NativeSchemaNode::PublicSignKey => {
            let variant = reader.read_u8()?;
            let len = match variant {
                0 => 32,
                1 => 33,
                tag => return Err(SchemaError::UnsupportedPublicSignKeyVariant(tag)),
            };
            let bytes = reader.read_exact(len)?;
            insert_bytes_facts(fields, state, scope, required_field(field)?, bytes)?;
        }
        NativeSchemaNode::Generic => return Err(SchemaError::GenericNode),
    }
    Ok(())
}

fn required_schema_field(
    field: Option<&NativeSchemaField>,
) -> Result<&NativeSchemaField, SchemaError> {
    field.ok_or(SchemaError::MissingFieldMetadata)
}

fn required_field(field: Option<&NativeSchemaField>) -> Result<u32, SchemaError> {
    Ok(required_schema_field(field)?.field)
}

fn insert_scalar(fields: &mut DocumentFields, scope: u32, field: u32, value: FieldValue) {
    fields.insert_scoped_scalar(scope, FieldPath::Id(field), value);
}

fn insert_bytes_facts(
    fields: &mut DocumentFields,
    state: &mut NativeExtractState,
    scope: u32,
    field: u32,
    bytes: &[u8],
) -> Result<(), SchemaError> {
    if bytes.len() <= state.byte_element_index_limit {
        for byte in bytes.iter().copied() {
            let byte_scope = state.next_scope()?;
            fields.insert_scoped_scalar(
                byte_scope,
                FieldPath::Id(field),
                FieldValue::U64(byte as u64),
            );
        }
    }
    if bytes.len() <= state.byte_exact_index_limit {
        fields.insert_scoped_scalar(
            scope,
            FieldPath::Id(field),
            FieldValue::from(bytes.to_vec()),
        );
    }
    Ok(())
}

fn read_le_u64_with_width(
    reader: &mut BridgeReader,
    width: usize,
) -> Result<Option<u64>, SchemaError> {
    let bytes = reader.read_exact(width)?;
    let low_width = width.min(8);
    let mut low = [0u8; 8];
    low[..low_width].copy_from_slice(&bytes[..low_width]);
    if bytes.iter().skip(8).any(|byte| *byte != 0) {
        return Ok(None);
    }
    Ok(Some(u64::from_le_bytes(low)))
}

fn read_le_i64_with_width(reader: &mut BridgeReader, width: usize) -> Result<i64, SchemaError> {
    let bytes = reader.read_exact(width)?;
    let mut out = if bytes.last().is_some_and(|byte| byte & 0x80 != 0) {
        [0xffu8; 8]
    } else {
        [0u8; 8]
    };
    out[..width].copy_from_slice(bytes);
    Ok(i64::from_le_bytes(out))
}

fn ensure_bridge_version(version: u8) -> Result<(), SchemaError> {
    if version == BRIDGE_VERSION {
        Ok(())
    } else {
        Err(SchemaError::InvalidVersion(version))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_native_schema_ir, extract_encoded_document_fields,
        extract_encoded_document_fields_from_parts,
        extract_encoded_document_fields_from_parts_with_byte_limits,
    };
    use crate::planner::{FieldPath, FieldValue};

    fn write_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn write_string(out: &mut Vec<u8>, value: &str) {
        write_u32(out, value.len() as u32);
        out.extend_from_slice(value.as_bytes());
    }

    fn schema_with_id_score_and_bytes() -> Vec<u8> {
        let mut out = vec![1, 14];
        write_u32(&mut out, 0);
        write_u32(&mut out, 3);
        write_string(&mut out, "id");
        write_u32(&mut out, 1);
        write_u32(&mut out, 101);
        out.push(12);
        write_string(&mut out, "score");
        write_u32(&mut out, 2);
        write_u32(&mut out, 102);
        out.push(3);
        write_string(&mut out, "bytes");
        write_u32(&mut out, 3);
        write_u32(&mut out, 103);
        out.push(13);
        out
    }

    fn encoded_document() -> Vec<u8> {
        let mut out = Vec::new();
        write_string(&mut out, "abc");
        write_u32(&mut out, 7);
        write_u32(&mut out, 2);
        out.extend_from_slice(&[9, 10]);
        out
    }

    fn schema_with_public_sign_key() -> Vec<u8> {
        let mut out = vec![1, 14];
        write_u32(&mut out, 0);
        write_u32(&mut out, 1);
        write_string(&mut out, "owner");
        write_u32(&mut out, 4);
        write_u32(&mut out, 104);
        out.push(19);
        out
    }

    fn encoded_public_sign_key_document() -> Vec<u8> {
        let mut out = Vec::new();
        out.push(0);
        out.extend_from_slice(&[7u8; 32]);
        out
    }

    #[test]
    fn extracts_fields_from_encoded_document() {
        let schema = decode_native_schema_ir(&schema_with_id_score_and_bytes()).unwrap();
        let fields = extract_encoded_document_fields(&schema, &encoded_document(), 8).unwrap();

        assert_eq!(
            fields.scalar_values(&FieldPath::Id(1)),
            Some([FieldValue::from("abc")].as_slice())
        );
        assert_eq!(
            fields.scalar_values(&FieldPath::Id(2)),
            Some([FieldValue::U64(7)].as_slice())
        );
        assert_eq!(
            fields.scalar_values(&FieldPath::Id(3)).unwrap(),
            &[
                FieldValue::U64(9),
                FieldValue::U64(10),
                FieldValue::from(vec![9, 10])
            ]
        );
        assert_eq!(schema.stats().root_fields, 3);
    }

    #[test]
    fn extracts_public_sign_key_fields_from_encoded_document() {
        let schema = decode_native_schema_ir(&schema_with_public_sign_key()).unwrap();
        let fields =
            extract_encoded_document_fields(&schema, &encoded_public_sign_key_document(), 0)
                .unwrap();

        assert_eq!(
            fields.scalar_values(&FieldPath::Id(4)),
            Some([FieldValue::from(vec![7u8; 32])].as_slice())
        );
        assert_eq!(schema.stats().generic_nodes, 0);
    }

    #[test]
    fn extracts_fields_across_encoded_parts() {
        let schema = decode_native_schema_ir(&schema_with_id_score_and_bytes()).unwrap();
        let encoded = encoded_document();
        let fields =
            extract_encoded_document_fields_from_parts(&schema, &encoded[..6], &encoded[6..], 0)
                .unwrap();

        assert_eq!(
            fields.scalar_values(&FieldPath::Id(3)),
            Some([FieldValue::from(vec![9, 10])].as_slice())
        );
    }

    #[test]
    fn can_skip_large_exact_byte_facts() {
        let schema = decode_native_schema_ir(&schema_with_id_score_and_bytes()).unwrap();
        let encoded = encoded_document();
        let fields = extract_encoded_document_fields_from_parts_with_byte_limits(
            &schema,
            &encoded[..6],
            &encoded[6..],
            0,
            1,
        )
        .unwrap();

        assert_eq!(fields.scalar_values(&FieldPath::Id(3)), None);
    }
}
