use borsh::BorshDeserialize;
use indexmap::IndexMap;
use js_sys::{Array, Uint8Array};
use planner::{
    Compare, DocumentFields, FieldPath, FieldValue, NativeQueryIndex, Query, SortDirection,
    SortField, StringMatchMethod, SumResult,
};
use wasm_bindgen::prelude::*;

pub mod planner;
pub mod storage;

#[cfg(not(target_arch = "wasm32"))]
pub mod native_fs;

struct StoredEntry {
    id: JsValue,
    value: JsValue,
}

#[wasm_bindgen]
pub struct NativeIndexStore {
    entries: IndexMap<String, StoredEntry>,
}

#[wasm_bindgen]
impl NativeIndexStore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> NativeIndexStore {
        NativeIndexStore {
            entries: IndexMap::new(),
        }
    }

    pub fn put(&mut self, key: String, id: JsValue, value: JsValue) {
        self.entries.insert(key, StoredEntry { id, value });
    }

    pub fn get(&self, key: &str) -> JsValue {
        match self.entries.get(key) {
            Some(entry) => entry_to_js(entry),
            None => JsValue::UNDEFINED,
        }
    }

    pub fn get_many(&self, keys: Array) -> Array {
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        self.entries_for_keys(&keys)
    }

    pub fn delete(&mut self, key: &str) -> bool {
        self.entries.shift_remove(key).is_some()
    }

    pub fn delete_many(&mut self, keys: Array) -> Array {
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        self.delete_keys(&keys)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> Array {
        let entries = Array::new();
        for entry in self.entries.values() {
            entries.push(&entry_to_js(entry));
        }
        entries
    }
}

impl NativeIndexStore {
    fn entries_for_keys(&self, keys: &[String]) -> Array {
        let entries = Array::new();
        for key in keys {
            if let Some(entry) = self.entries.get(key) {
                entries.push(&entry_to_js(entry));
            }
        }
        entries
    }

    fn delete_keys(&mut self, keys: &[String]) -> Array {
        let entries = Array::new();
        for key in keys {
            if let Some(entry) = self.entries.shift_remove(key) {
                entries.push(&entry_to_js(&entry));
            }
        }
        entries
    }
}

fn entry_to_js(entry: &StoredEntry) -> JsValue {
    let pair = Array::new();
    pair.push(&entry.id);
    pair.push(&entry.value);
    pair.into()
}

#[wasm_bindgen]
pub struct NativeQueryPlanner {
    index: NativeQueryIndex,
}

#[wasm_bindgen]
impl NativeQueryPlanner {
    #[wasm_bindgen(constructor)]
    pub fn new() -> NativeQueryPlanner {
        NativeQueryPlanner {
            index: NativeQueryIndex::new(),
        }
    }

    pub fn clear(&mut self) {
        self.index.clear();
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn put_document(&mut self, id: String, fields_bytes: Vec<u8>) -> Result<(), JsValue> {
        let fields = decode_document_fields(&fields_bytes)?;
        self.index.put(id, fields);
        Ok(())
    }

    pub fn delete_document(&mut self, id: String) {
        self.index.delete(id);
    }

    pub fn query(&self, query_bytes: Vec<u8>, sort_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sort = decode_sort(&sort_bytes)?;
        let ids = self.index.search(&query, &sort, None);
        Ok(ids_to_js(ids))
    }

    pub fn query_page(
        &self,
        query_bytes: Vec<u8>,
        sort_bytes: Vec<u8>,
        offset: usize,
        limit: usize,
    ) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sort = decode_sort(&sort_bytes)?;
        let ids = self.index.search_page(&query, &sort, offset, Some(limit));
        Ok(ids_to_js(ids))
    }

    pub fn count(&self, query_bytes: Vec<u8>) -> Result<usize, JsValue> {
        let query = decode_query(&query_bytes)?;
        Ok(self.index.count(&query) as usize)
    }

    pub fn sum(&self, query_bytes: Vec<u8>, field: u32) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sum = self
            .index
            .sum(&query, FieldPath::Id(field))
            .map_err(js_error)?;
        Ok(sum_to_js(sum))
    }

    pub fn delete_matching(&mut self, query_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let ids = self.index.delete_matching(&query);
        Ok(ids_to_js(ids))
    }
}

#[wasm_bindgen]
pub struct NativeRustIndex {
    store: NativeIndexStore,
    planner: NativeQueryPlanner,
    schema_ir: Option<NativeSchemaIr>,
}

#[wasm_bindgen]
impl NativeRustIndex {
    #[wasm_bindgen(constructor)]
    pub fn new() -> NativeRustIndex {
        NativeRustIndex {
            store: NativeIndexStore::new(),
            planner: NativeQueryPlanner::new(),
            schema_ir: None,
        }
    }

    pub fn clear(&mut self) {
        self.store.clear();
        self.planner.clear();
    }

    pub fn configure_schema_ir(&mut self, schema_ir_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let schema_ir = decode_native_schema_ir(&schema_ir_bytes)?;
        let stats = schema_ir.stats();
        self.schema_ir = Some(schema_ir);
        let out = Array::new();
        out.push(&JsValue::from_f64(stats.root_fields as f64));
        out.push(&JsValue::from_f64(stats.node_count as f64));
        out.push(&JsValue::from_f64(stats.generic_nodes as f64));
        Ok(out)
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn put(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        fields_bytes: Vec<u8>,
    ) -> Result<(), JsValue> {
        let fields = decode_document_fields(&fields_bytes)?;
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        Ok(())
    }

    pub fn put_encoded(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        value_bytes: Vec<u8>,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let fields =
            self.extract_encoded_document_fields(&value_bytes, byte_element_index_limit)?;
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        Ok(())
    }

    pub fn put_encoded_parts(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        value_prefix_bytes: Vec<u8>,
        value_suffix_bytes: Vec<u8>,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let fields = self.extract_encoded_document_fields_from_reader(
            BridgeReader::from_parts(&value_prefix_bytes, &value_suffix_bytes),
            byte_element_index_limit,
        )?;
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        Ok(())
    }

    pub fn put_encoded_parts_batch(
        &mut self,
        keys: Array,
        ids: Array,
        values: Array,
        value_prefix_bytes: Array,
        value_suffix_bytes: Array,
        byte_element_index_limit: usize,
    ) -> Result<(), JsValue> {
        let len = keys.length();
        if ids.length() != len
            || values.length() != len
            || value_prefix_bytes.length() != len
            || value_suffix_bytes.length() != len
        {
            return Err(js_error("Mismatched encoded parts batch lengths"));
        }

        let mut prepared = Vec::with_capacity(len as usize);
        for index in 0..len {
            let key = keys
                .get(index)
                .as_string()
                .ok_or_else(|| js_error("Invalid encoded parts batch key"))?;
            let prefix = Uint8Array::new(&value_prefix_bytes.get(index)).to_vec();
            let suffix = Uint8Array::new(&value_suffix_bytes.get(index)).to_vec();
            let fields = self.extract_encoded_document_fields_from_reader(
                BridgeReader::from_parts(&prefix, &suffix),
                byte_element_index_limit,
            )?;
            prepared.push((key, ids.get(index), values.get(index), fields));
        }

        for (key, id, value, fields) in prepared {
            self.store.put(key.clone(), id, value);
            self.planner.index.put(key, fields);
        }
        Ok(())
    }

    pub fn put_and_delete_matching(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        fields_bytes: Vec<u8>,
        query_bytes: Vec<u8>,
    ) -> Result<Array, JsValue> {
        let fields = decode_document_fields(&fields_bytes)?;
        let query = decode_query(&query_bytes)?;
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        let keys = self.planner.index.delete_matching(&query);
        Ok(self.store.delete_keys(&keys))
    }

    pub fn put_and_delete_keys(
        &mut self,
        key: String,
        id: JsValue,
        value: JsValue,
        fields_bytes: Vec<u8>,
        keys: Array,
    ) -> Result<Array, JsValue> {
        let fields = decode_document_fields(&fields_bytes)?;
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        self.store.put(key.clone(), id, value);
        self.planner.index.put(key, fields);
        for key in &keys {
            self.planner.index.delete(key);
        }
        Ok(self.store.delete_keys(&keys))
    }

    pub fn delete_keys(&mut self, keys: Array) -> Array {
        let keys: Vec<_> = keys.iter().filter_map(|key| key.as_string()).collect();
        for key in &keys {
            self.planner.index.delete(key);
        }
        self.store.delete_keys(&keys)
    }

    pub fn get(&self, key: &str) -> JsValue {
        self.store.get(key)
    }

    pub fn entries(&self) -> Array {
        self.store.entries()
    }

    pub fn query(&self, query_bytes: Vec<u8>, sort_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sort = decode_sort(&sort_bytes)?;
        let keys = self.planner.index.search(&query, &sort, None);
        Ok(self.store.entries_for_keys(&keys))
    }

    pub fn query_page(
        &self,
        query_bytes: Vec<u8>,
        sort_bytes: Vec<u8>,
        offset: usize,
        limit: usize,
    ) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let sort = decode_sort(&sort_bytes)?;
        let keys = self
            .planner
            .index
            .search_page(&query, &sort, offset, Some(limit));
        Ok(self.store.entries_for_keys(&keys))
    }

    pub fn query_exact_string_first_batch(&self, field: u32, values: Array) -> Array {
        let out = Array::new();
        let field = FieldPath::Id(field);
        for value in values.iter() {
            let entry = value
                .as_string()
                .and_then(|value| {
                    self.planner
                        .index
                        .exact_first(&field, &FieldValue::String(value))
                })
                .map(|key| self.store.get(&key))
                .unwrap_or(JsValue::UNDEFINED);
            out.push(&entry);
        }
        out
    }

    pub fn count(&self, query_bytes: Vec<u8>) -> Result<usize, JsValue> {
        self.planner.count(query_bytes)
    }

    pub fn sum(&self, query_bytes: Vec<u8>, field: u32) -> Result<Array, JsValue> {
        self.planner.sum(query_bytes, field)
    }

    pub fn delete_matching(&mut self, query_bytes: Vec<u8>) -> Result<Array, JsValue> {
        let query = decode_query(&query_bytes)?;
        let keys = self.planner.index.delete_matching(&query);
        Ok(self.store.delete_keys(&keys))
    }
}

impl NativeRustIndex {
    fn extract_encoded_document_fields(
        &self,
        value_bytes: &[u8],
        byte_element_index_limit: usize,
    ) -> Result<DocumentFields, JsValue> {
        self.extract_encoded_document_fields_from_reader(
            BridgeReader::new(value_bytes),
            byte_element_index_limit,
        )
    }

    fn extract_encoded_document_fields_from_reader(
        &self,
        reader: BridgeReader,
        byte_element_index_limit: usize,
    ) -> Result<DocumentFields, JsValue> {
        let schema_ir = self
            .schema_ir
            .as_ref()
            .ok_or_else(|| js_error("Native schema IR has not been configured"))?;
        extract_encoded_document_fields_from_reader(schema_ir, reader, byte_element_index_limit)
    }
}

fn ids_to_js(ids: Vec<String>) -> Array {
    let out = Array::new();
    for id in ids {
        out.push(&JsValue::from_str(&id));
    }
    out
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

const BRIDGE_VERSION: u8 = 1;

// Enum declaration order is part of the TS/Rust bridge ABI.
#[derive(Clone, BorshDeserialize)]
enum FieldValueDto {
    Bool(bool),
    I64(i64),
    U64(u64),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(BorshDeserialize)]
struct QueryPayloadDto {
    version: u8,
    query: QueryDto,
}

// Enum declaration order is part of the TS/Rust bridge ABI.
#[derive(Clone, BorshDeserialize)]
enum QueryDto {
    All,
    Exact {
        field: u32,
        value: FieldValueDto,
    },
    Range {
        field: u32,
        compare: CompareDto,
        value: FieldValueDto,
    },
    And {
        queries: Vec<QueryDto>,
    },
    Or {
        queries: Vec<QueryDto>,
    },
    Not {
        query: Box<QueryDto>,
    },
    StringMatch {
        field: u32,
        value: String,
        method: StringMatchMethodDto,
        case_insensitive: bool,
    },
    IsNull {
        field: u32,
    },
}

// Enum declaration order is part of the TS/Rust bridge ABI.
#[derive(Clone, Copy, BorshDeserialize)]
enum CompareDto {
    Equal,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

#[derive(BorshDeserialize)]
struct SortPayloadDto {
    version: u8,
    fields: Vec<SortFieldDto>,
}

#[derive(BorshDeserialize)]
struct SortFieldDto {
    field: u32,
    direction: SortDirectionDto,
}

// Enum declaration order is part of the TS/Rust bridge ABI.
#[derive(BorshDeserialize)]
enum SortDirectionDto {
    Asc,
    Desc,
}

// Enum declaration order is part of the TS/Rust bridge ABI.
#[derive(Clone, Copy, BorshDeserialize)]
enum StringMatchMethodDto {
    Exact,
    Prefix,
    Contains,
}

#[derive(Clone, Debug)]
struct NativeSchemaIr {
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
}

struct NativeSchemaIrStats {
    root_fields: usize,
    node_count: usize,
    generic_nodes: usize,
}

impl NativeSchemaIr {
    fn stats(&self) -> NativeSchemaIrStats {
        NativeSchemaIrStats {
            root_fields: match &self.root {
                NativeSchemaNode::Object { fields, .. } => fields.len(),
                _ => 0,
            },
            node_count: self.root.node_count(),
            generic_nodes: self.root.generic_count(),
        }
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
            | NativeSchemaNode::Uint8Array => 0,
        }
    }
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

    fn finish(&self) -> Result<(), JsValue> {
        if self.offset == self.len {
            Ok(())
        } else {
            Err(js_error("Trailing bytes in bridge payload"))
        }
    }

    fn read_u8(&mut self) -> Result<u8, JsValue> {
        let bytes = self.read_exact(1)?;
        Ok(bytes[0])
    }

    fn read_u32(&mut self) -> Result<u32, JsValue> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.read_exact(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, JsValue> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_exact(8)?);
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, JsValue> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_exact(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String, JsValue> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(js_error)
    }

    fn read_field_value(&mut self) -> Result<FieldValue, JsValue> {
        Ok(match self.read_u8()? {
            0 => match self.read_u8()? {
                0 => FieldValue::Bool(false),
                1 => FieldValue::Bool(true),
                value => return Err(js_error(format!("Invalid bridge bool value {value}"))),
            },
            1 => FieldValue::I64(self.read_i64()?),
            2 => FieldValue::U64(self.read_u64()?),
            3 => FieldValue::String(self.read_string()?),
            4 => {
                let len = self.read_u32()? as usize;
                FieldValue::Bytes(self.read_exact(len)?.to_vec())
            }
            tag => return Err(js_error(format!("Unknown bridge field value tag {tag}"))),
        })
    }

    fn read_exact(&mut self, len: usize) -> Result<&[u8], JsValue> {
        let Some(end) = self.offset.checked_add(len) else {
            return Err(js_error("Bridge payload offset overflow"));
        };
        if end > self.len {
            return Err(js_error("Unexpected end of bridge payload"));
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

fn extract_encoded_document_fields_from_reader(
    schema_ir: &NativeSchemaIr,
    mut reader: BridgeReader,
    byte_element_index_limit: usize,
) -> Result<DocumentFields, JsValue> {
    let mut fields = DocumentFields::new();
    let mut state = NativeExtractState {
        next_scope: 1,
        byte_element_index_limit,
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

fn decode_native_schema_ir(schema_ir_bytes: &[u8]) -> Result<NativeSchemaIr, JsValue> {
    let mut reader = BridgeReader::new(schema_ir_bytes);
    ensure_bridge_version(reader.read_u8()?)?;
    let root = read_native_schema_node(&mut reader)?;
    reader.finish()?;
    Ok(NativeSchemaIr { root })
}

fn read_native_schema_node(reader: &mut BridgeReader) -> Result<NativeSchemaNode, JsValue> {
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
        tag => return Err(js_error(format!("Unknown native schema node tag {tag}"))),
    })
}

struct NativeExtractState {
    next_scope: u32,
    byte_element_index_limit: usize,
}

impl NativeExtractState {
    fn next_scope(&mut self) -> Result<u32, JsValue> {
        let scope = self.next_scope;
        self.next_scope = self
            .next_scope
            .checked_add(1)
            .ok_or_else(|| js_error("Native schema extraction scope overflow"))?;
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
) -> Result<(), JsValue> {
    match node {
        NativeSchemaNode::Bool => {
            let value = match reader.read_u8()? {
                0 => false,
                1 => true,
                value => return Err(js_error(format!("Invalid Borsh bool value {value}"))),
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
                FieldValue::String(value),
            );
        }
        NativeSchemaNode::Uint8Array => {
            let len = reader.read_u32()? as usize;
            let bytes = reader.read_exact(len)?.to_vec();
            insert_bytes_facts(fields, state, scope, required_field(field)?, bytes)?;
        }
        NativeSchemaNode::Object {
            variant_prefix,
            fields: schema_fields,
        } => {
            if !variant_prefix.is_empty() {
                let actual = reader.read_exact(variant_prefix.len())?;
                if actual != variant_prefix.as_slice() {
                    return Err(js_error("Borsh variant prefix did not match native schema"));
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
            tag => return Err(js_error(format!("Invalid Borsh option tag {tag}"))),
        },
        NativeSchemaNode::Vec(child) if matches!(child.as_ref(), NativeSchemaNode::U8) => {
            let len = reader.read_u32()? as usize;
            let bytes = reader.read_exact(len)?.to_vec();
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
            let bytes = reader.read_exact(*length as usize)?.to_vec();
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
        NativeSchemaNode::Generic => {
            return Err(js_error(
                "Native schema IR contains a generic node that cannot be extracted from Borsh",
            ));
        }
    }
    Ok(())
}

fn required_schema_field(field: Option<&NativeSchemaField>) -> Result<&NativeSchemaField, JsValue> {
    field.ok_or_else(|| js_error("Native schema scalar node is missing field metadata"))
}

fn required_field(field: Option<&NativeSchemaField>) -> Result<u32, JsValue> {
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
    bytes: Vec<u8>,
) -> Result<(), JsValue> {
    let index_byte_elements = bytes.len() <= state.byte_element_index_limit;
    if index_byte_elements {
        for byte in bytes.iter().copied() {
            let byte_scope = state.next_scope()?;
            fields.insert_scoped_scalar(
                byte_scope,
                FieldPath::Id(field),
                FieldValue::U64(byte as u64),
            );
        }
    }
    fields.insert_scoped_scalar(scope, FieldPath::Id(field), FieldValue::Bytes(bytes));
    Ok(())
}

fn read_le_u64_with_width(reader: &mut BridgeReader, width: usize) -> Result<Option<u64>, JsValue> {
    let bytes = reader.read_exact(width)?;
    let low_width = width.min(8);
    let mut low = [0u8; 8];
    low[..low_width].copy_from_slice(&bytes[..low_width]);
    if bytes.iter().skip(8).any(|byte| *byte != 0) {
        return Ok(None);
    }
    Ok(Some(u64::from_le_bytes(low)))
}

fn read_le_i64_with_width(reader: &mut BridgeReader, width: usize) -> Result<i64, JsValue> {
    let bytes = reader.read_exact(width)?;
    let mut out = if bytes.last().is_some_and(|byte| byte & 0x80 != 0) {
        [0xffu8; 8]
    } else {
        [0u8; 8]
    };
    out[..width].copy_from_slice(bytes);
    Ok(i64::from_le_bytes(out))
}

fn decode_document_fields(fields_bytes: &[u8]) -> Result<DocumentFields, JsValue> {
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

fn decode_query(query_bytes: &[u8]) -> Result<Query, JsValue> {
    let payload = QueryPayloadDto::try_from_slice(query_bytes).map_err(js_error)?;
    ensure_bridge_version(payload.version)?;
    payload.query.try_into()
}

fn decode_sort(sort_bytes: &[u8]) -> Result<Vec<SortField>, JsValue> {
    let payload = SortPayloadDto::try_from_slice(sort_bytes).map_err(js_error)?;
    ensure_bridge_version(payload.version)?;
    Ok(payload
        .fields
        .into_iter()
        .map(|field| SortField {
            field: FieldPath::Id(field.field),
            direction: field.direction.into(),
        })
        .collect())
}

impl TryFrom<QueryDto> for Query {
    type Error = JsValue;

    fn try_from(value: QueryDto) -> Result<Self, Self::Error> {
        Ok(match value {
            QueryDto::All => Query::All,
            QueryDto::Exact { field, value } => Query::Exact {
                field: FieldPath::Id(field),
                value: value.into(),
            },
            QueryDto::Range {
                field,
                compare,
                value,
            } => Query::Range {
                field: FieldPath::Id(field),
                compare: compare.into(),
                value: value.into(),
            },
            QueryDto::And { queries } => Query::And(decode_queries(queries)?),
            QueryDto::Or { queries } => Query::Or(decode_queries(queries)?),
            QueryDto::Not { query } => Query::Not(Box::new((*query).try_into()?)),
            QueryDto::StringMatch {
                field,
                value,
                method,
                case_insensitive,
            } => Query::StringMatch {
                field: FieldPath::Id(field),
                value,
                method: method.into(),
                case_insensitive,
            },
            QueryDto::IsNull { field } => Query::IsNull {
                field: FieldPath::Id(field),
            },
        })
    }
}

impl From<FieldValueDto> for FieldValue {
    fn from(value: FieldValueDto) -> Self {
        match value {
            FieldValueDto::Bool(value) => FieldValue::Bool(value),
            FieldValueDto::I64(value) => FieldValue::I64(value),
            FieldValueDto::U64(value) => FieldValue::U64(value),
            FieldValueDto::String(value) => FieldValue::String(value),
            FieldValueDto::Bytes(value) => FieldValue::Bytes(value),
        }
    }
}

impl From<CompareDto> for Compare {
    fn from(value: CompareDto) -> Self {
        match value {
            CompareDto::Equal => Compare::Equal,
            CompareDto::Greater => Compare::Greater,
            CompareDto::GreaterOrEqual => Compare::GreaterOrEqual,
            CompareDto::Less => Compare::Less,
            CompareDto::LessOrEqual => Compare::LessOrEqual,
        }
    }
}

impl From<SortDirectionDto> for SortDirection {
    fn from(value: SortDirectionDto) -> Self {
        match value {
            SortDirectionDto::Asc => SortDirection::Asc,
            SortDirectionDto::Desc => SortDirection::Desc,
        }
    }
}

impl From<StringMatchMethodDto> for StringMatchMethod {
    fn from(value: StringMatchMethodDto) -> Self {
        match value {
            StringMatchMethodDto::Exact => StringMatchMethod::Exact,
            StringMatchMethodDto::Prefix => StringMatchMethod::Prefix,
            StringMatchMethodDto::Contains => StringMatchMethod::Contains,
        }
    }
}

fn decode_queries(queries: Vec<QueryDto>) -> Result<Vec<Query>, JsValue> {
    queries.into_iter().map(Query::try_from).collect()
}

fn ensure_bridge_version(version: u8) -> Result<(), JsValue> {
    if version == BRIDGE_VERSION {
        Ok(())
    } else {
        Err(js_error(format!(
            "Unsupported bridge payload version {version}"
        )))
    }
}

fn js_error(error: impl ToString) -> JsValue {
    JsValue::from_str(&error.to_string())
}
