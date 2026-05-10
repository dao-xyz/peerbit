use borsh::BorshDeserialize;
use indexmap::IndexMap;
use js_sys::Array;
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
}

#[wasm_bindgen]
impl NativeRustIndex {
    #[wasm_bindgen(constructor)]
    pub fn new() -> NativeRustIndex {
        NativeRustIndex {
            store: NativeIndexStore::new(),
            planner: NativeQueryPlanner::new(),
        }
    }

    pub fn clear(&mut self) {
        self.store.clear();
        self.planner.clear();
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

struct BridgeReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BridgeReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(&self) -> Result<(), JsValue> {
        if self.offset == self.bytes.len() {
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

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], JsValue> {
        let Some(end) = self.offset.checked_add(len) else {
            return Err(js_error("Bridge payload offset overflow"));
        };
        let Some(bytes) = self.bytes.get(self.offset..end) else {
            return Err(js_error("Unexpected end of bridge payload"));
        };
        self.offset = end;
        Ok(bytes)
    }
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
