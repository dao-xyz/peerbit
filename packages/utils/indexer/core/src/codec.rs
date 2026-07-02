use crate::planner::{
    Compare, FieldPath, FieldValue, Query, SortDirection, SortField, StringMatchMethod,
};
use borsh::BorshDeserialize;

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

pub fn decode_query(query_bytes: &[u8]) -> Result<Query, String> {
    let payload =
        QueryPayloadDto::try_from_slice(query_bytes).map_err(|error| error.to_string())?;
    ensure_bridge_version(payload.version)?;
    payload.query.try_into()
}

pub fn decode_sort(sort_bytes: &[u8]) -> Result<Vec<SortField>, String> {
    let payload = SortPayloadDto::try_from_slice(sort_bytes).map_err(|error| error.to_string())?;
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
    type Error = String;

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
            FieldValueDto::String(value) => FieldValue::from(value),
            FieldValueDto::Bytes(value) => FieldValue::from(value),
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

fn decode_queries(queries: Vec<QueryDto>) -> Result<Vec<Query>, String> {
    queries.into_iter().map(Query::try_from).collect()
}

fn ensure_bridge_version(version: u8) -> Result<(), String> {
    if version == BRIDGE_VERSION {
        Ok(())
    } else {
        Err(format!("Unsupported bridge payload version {version}"))
    }
}
