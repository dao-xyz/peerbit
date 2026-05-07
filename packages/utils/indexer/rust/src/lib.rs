use indexmap::IndexMap;
use js_sys::Array;
use planner::{
    Compare, DocumentFields, FieldValue, NativeQueryIndex, Query, SortDirection, SortField,
};
use serde::Deserialize;
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

    pub fn put_document(&mut self, id: String, fields_json: String) -> Result<(), JsValue> {
        let fields = decode_document_fields(&fields_json)?;
        self.index.put(id, fields);
        Ok(())
    }

    pub fn delete_document(&mut self, id: String) {
        self.index.delete(id);
    }

    pub fn query(&self, query_json: String, sort_json: String) -> Result<Array, JsValue> {
        let query = decode_query(&query_json)?;
        let sort = decode_sort(&sort_json)?;
        let ids = self.index.search(&query, &sort, None);
        let out = Array::new();
        for id in ids {
            out.push(&JsValue::from_str(&id));
        }
        Ok(out)
    }
}

#[derive(Deserialize)]
struct FieldFactDto {
    field: String,
    value: FieldValueDto,
}

#[derive(Deserialize)]
#[serde(tag = "type", content = "value")]
enum FieldValueDto {
    #[serde(rename = "bool")]
    Bool(bool),
    #[serde(rename = "i64")]
    I64(String),
    #[serde(rename = "u64")]
    U64(String),
    #[serde(rename = "string")]
    String(String),
}

#[derive(Deserialize)]
#[serde(tag = "op")]
enum QueryDto {
    #[serde(rename = "all")]
    All,
    #[serde(rename = "exact")]
    Exact { field: String, value: FieldValueDto },
    #[serde(rename = "range")]
    Range {
        field: String,
        compare: CompareDto,
        value: FieldValueDto,
    },
    #[serde(rename = "and")]
    And { queries: Vec<QueryDto> },
    #[serde(rename = "or")]
    Or { queries: Vec<QueryDto> },
    #[serde(rename = "not")]
    Not { query: Box<QueryDto> },
}

#[derive(Clone, Copy, Deserialize)]
enum CompareDto {
    #[serde(rename = "eq")]
    Equal,
    #[serde(rename = "gt")]
    Greater,
    #[serde(rename = "gte")]
    GreaterOrEqual,
    #[serde(rename = "lt")]
    Less,
    #[serde(rename = "lte")]
    LessOrEqual,
}

#[derive(Deserialize)]
struct SortFieldDto {
    field: String,
    direction: SortDirectionDto,
}

#[derive(Deserialize)]
enum SortDirectionDto {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

fn decode_document_fields(fields_json: &str) -> Result<DocumentFields, JsValue> {
    let facts: Vec<FieldFactDto> = serde_json::from_str(fields_json).map_err(js_error)?;
    let mut fields = DocumentFields::new();
    for fact in facts {
        fields.insert_scalar(fact.field, decode_field_value(fact.value)?);
    }
    Ok(fields)
}

fn decode_query(query_json: &str) -> Result<Query, JsValue> {
    let query: QueryDto = serde_json::from_str(query_json).map_err(js_error)?;
    query.try_into()
}

fn decode_sort(sort_json: &str) -> Result<Vec<SortField>, JsValue> {
    let fields: Vec<SortFieldDto> = serde_json::from_str(sort_json).map_err(js_error)?;
    fields
        .into_iter()
        .map(|field| {
            Ok(SortField {
                field: field.field,
                direction: match field.direction {
                    SortDirectionDto::Asc => SortDirection::Asc,
                    SortDirectionDto::Desc => SortDirection::Desc,
                },
            })
        })
        .collect()
}

impl TryFrom<QueryDto> for Query {
    type Error = JsValue;

    fn try_from(value: QueryDto) -> Result<Self, Self::Error> {
        Ok(match value {
            QueryDto::All => Query::All,
            QueryDto::Exact { field, value } => Query::Exact {
                field,
                value: decode_field_value(value)?,
            },
            QueryDto::Range {
                field,
                compare,
                value,
            } => Query::Range {
                field,
                compare: compare.into(),
                value: decode_field_value(value)?,
            },
            QueryDto::And { queries } => Query::And(decode_queries(queries)?),
            QueryDto::Or { queries } => Query::Or(decode_queries(queries)?),
            QueryDto::Not { query } => Query::Not(Box::new((*query).try_into()?)),
        })
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

fn decode_queries(queries: Vec<QueryDto>) -> Result<Vec<Query>, JsValue> {
    queries.into_iter().map(Query::try_from).collect()
}

fn decode_field_value(value: FieldValueDto) -> Result<FieldValue, JsValue> {
    match value {
        FieldValueDto::Bool(value) => Ok(FieldValue::Bool(value)),
        FieldValueDto::I64(value) => value.parse::<i64>().map(FieldValue::I64).map_err(js_error),
        FieldValueDto::U64(value) => value.parse::<u64>().map(FieldValue::U64).map_err(js_error),
        FieldValueDto::String(value) => Ok(FieldValue::String(value)),
    }
}

fn js_error(error: impl ToString) -> JsValue {
    JsValue::from_str(&error.to_string())
}
