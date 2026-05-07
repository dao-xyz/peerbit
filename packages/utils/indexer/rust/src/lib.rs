use borsh::BorshDeserialize;
use indexmap::IndexMap;
use js_sys::Array;
use planner::{
    Compare, DocumentFields, FieldValue, NativeQueryIndex, Query, SortDirection, SortField,
    StringMatchMethod,
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

    pub fn delete(&mut self, key: &str) -> bool {
        self.entries.shift_remove(key).is_some()
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
}

fn ids_to_js(ids: Vec<String>) -> Array {
    let out = Array::new();
    for id in ids {
        out.push(&JsValue::from_str(&id));
    }
    out
}

const BRIDGE_VERSION: u8 = 1;

#[derive(BorshDeserialize)]
struct DocumentFieldsDto {
    version: u8,
    facts: Vec<FieldFactDto>,
}

#[derive(BorshDeserialize)]
struct FieldFactDto {
    scope: u32,
    field: String,
    value: FieldValueDto,
}

// Enum declaration order is part of the TS/Rust bridge ABI.
#[derive(Clone, BorshDeserialize)]
enum FieldValueDto {
    Bool(bool),
    I64(i64),
    U64(u64),
    String(String),
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
        field: String,
        value: FieldValueDto,
    },
    Range {
        field: String,
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
        field: String,
        value: String,
        method: StringMatchMethodDto,
        case_insensitive: bool,
    },
    IsNull {
        field: String,
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
    field: String,
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

fn decode_document_fields(fields_bytes: &[u8]) -> Result<DocumentFields, JsValue> {
    let payload = DocumentFieldsDto::try_from_slice(fields_bytes).map_err(js_error)?;
    ensure_bridge_version(payload.version)?;
    let mut fields = DocumentFields::new();
    for fact in payload.facts {
        fields.insert_scoped_scalar(fact.scope, fact.field, FieldValue::from(fact.value));
    }
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
            field: field.field,
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
                field,
                value: value.into(),
            },
            QueryDto::Range {
                field,
                compare,
                value,
            } => Query::Range {
                field,
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
                field,
                value,
                method: method.into(),
                case_insensitive,
            },
            QueryDto::IsNull { field } => Query::IsNull { field },
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
