use roaring::RoaringBitmap;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};

pub type DocId = u32;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FieldPath {
    Id(u32),
    Name(String),
}

impl From<u32> for FieldPath {
    fn from(value: u32) -> Self {
        Self::Id(value)
    }
}

impl From<String> for FieldPath {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl From<&String> for FieldPath {
    fn from(value: &String) -> Self {
        Self::Name(value.clone())
    }
}

impl From<&str> for FieldPath {
    fn from(value: &str) -> Self {
        Self::Name(value.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FieldValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    String(String),
    Bytes(Vec<u8>),
}

impl FieldValue {
    fn as_i64(&self) -> Option<i64> {
        match self {
            Self::I64(value) => Some(*value),
            _ => None,
        }
    }

    fn as_u64(&self) -> Option<u64> {
        match self {
            Self::U64(value) => Some(*value),
            _ => None,
        }
    }
}

impl From<bool> for FieldValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for FieldValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<u64> for FieldValue {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<String> for FieldValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for FieldValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<Vec<u8>> for FieldValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

#[derive(Clone, Debug, Default)]
pub struct DocumentFields {
    scalars: HashMap<FieldPath, Vec<FieldValue>>,
    scoped_scalars: Vec<ScopedScalar>,
    vectors: HashMap<FieldPath, Vec<f32>>,
}

#[derive(Clone, Debug)]
struct ScopedScalar {
    scope: u32,
    path: FieldPath,
    value: FieldValue,
}

#[derive(Debug)]
enum QueryMatch {
    Matched(RoaringBitmap),
    False,
    Undefined,
}

impl DocumentFields {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_scalar_capacity(capacity: usize) -> Self {
        Self {
            scalars: HashMap::with_capacity(capacity),
            scoped_scalars: Vec::with_capacity(capacity),
            vectors: HashMap::new(),
        }
    }

    pub fn with_scalar(mut self, path: impl Into<FieldPath>, value: impl Into<FieldValue>) -> Self {
        self.insert_scalar(path, value);
        self
    }

    pub fn with_vector(mut self, path: impl Into<FieldPath>, value: Vec<f32>) -> Self {
        self.insert_vector(path, value);
        self
    }

    pub fn insert_scalar(&mut self, path: impl Into<FieldPath>, value: impl Into<FieldValue>) {
        self.insert_scoped_scalar(0, path, value);
    }

    pub fn insert_scoped_scalar(
        &mut self,
        scope: u32,
        path: impl Into<FieldPath>,
        value: impl Into<FieldValue>,
    ) {
        let path = path.into();
        let value = value.into();
        self.scalars
            .entry(path.clone())
            .or_default()
            .push(value.clone());
        self.scoped_scalars
            .push(ScopedScalar { scope, path, value });
    }

    pub fn insert_vector(&mut self, path: impl Into<FieldPath>, value: Vec<f32>) {
        self.vectors.insert(path.into(), value);
    }

    pub fn scalar_values(&self, path: &FieldPath) -> Option<&[FieldValue]> {
        self.scalars.get(path).map(Vec::as_slice)
    }

    pub fn vector(&self, path: &FieldPath) -> Option<&[f32]> {
        self.vectors.get(path).map(Vec::as_slice)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compare {
    Equal,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

#[derive(Clone, Debug)]
pub enum Query {
    All,
    Exact {
        field: FieldPath,
        value: FieldValue,
    },
    Range {
        field: FieldPath,
        compare: Compare,
        value: FieldValue,
    },
    StringMatch {
        field: FieldPath,
        value: String,
        method: StringMatchMethod,
        case_insensitive: bool,
    },
    IsNull {
        field: FieldPath,
    },
    And(Vec<Query>),
    Or(Vec<Query>),
    Not(Box<Query>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StringMatchMethod {
    Exact,
    Prefix,
    Contains,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug)]
pub struct SortField {
    pub field: FieldPath,
    pub direction: SortDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorMetric {
    Cosine,
    Dot,
    L2,
}

#[derive(Clone, Debug)]
pub struct VectorSort {
    pub field: FieldPath,
    pub query: Vec<f32>,
    pub metric: VectorMetric,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScoredDocument {
    pub id: String,
    pub score: f32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SumResult {
    None,
    I64(i128),
    U64(u128),
}

#[derive(Default)]
pub struct IndexBatch {
    deletes: Vec<String>,
    puts: Vec<(String, DocumentFields)>,
}

impl IndexBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(mut self, id: impl Into<String>, fields: DocumentFields) -> Self {
        self.puts.push((id.into(), fields));
        self
    }

    pub fn delete(mut self, id: impl Into<String>) -> Self {
        self.deletes.push(id.into());
        self
    }

    fn is_empty(&self) -> bool {
        self.deletes.is_empty() && self.puts.is_empty()
    }
}

#[derive(Default)]
pub struct NativeQueryIndex {
    generation: u64,
    next_doc_id: DocId,
    all_docs: RoaringBitmap,
    external_to_internal: HashMap<String, DocId>,
    internal_to_external: HashMap<DocId, String>,
    documents: HashMap<DocId, DocumentFields>,
    exact: HashMap<FieldPath, HashMap<FieldValue, RoaringBitmap>>,
    range_i64: HashMap<FieldPath, BTreeMap<i64, RoaringBitmap>>,
    range_u64: HashMap<FieldPath, BTreeMap<u64, RoaringBitmap>>,
    sort_bool: HashMap<FieldPath, BTreeMap<bool, RoaringBitmap>>,
    sort_i64: HashMap<FieldPath, BTreeMap<i64, RoaringBitmap>>,
    sort_u64: HashMap<FieldPath, BTreeMap<u64, RoaringBitmap>>,
    sort_string: HashMap<FieldPath, BTreeMap<String, RoaringBitmap>>,
    sort_bytes: HashMap<FieldPath, BTreeMap<Vec<u8>, RoaringBitmap>>,
    vectors: HashMap<FieldPath, HashMap<DocId, Vec<f32>>>,
}

impl NativeQueryIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn len(&self) -> usize {
        self.documents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn put(&mut self, id: impl Into<String>, fields: DocumentFields) {
        let external_id = id.into();
        let doc_id = match self.external_to_internal.get(&external_id).copied() {
            Some(doc_id) => {
                self.remove_document_fields(doc_id);
                doc_id
            }
            None => self.allocate_doc_id(external_id),
        };
        self.index_document(doc_id, &fields);
        self.documents.insert(doc_id, fields);
        self.all_docs.insert(doc_id);
        self.generation += 1;
    }

    pub fn delete(&mut self, id: impl Into<String>) {
        if self.remove_external(&id.into()) {
            self.generation += 1;
        }
    }

    pub fn apply_batch(&mut self, batch: IndexBatch) -> u64 {
        if batch.is_empty() {
            return self.generation;
        }

        let mut changed = false;

        for external_id in batch.deletes {
            changed |= self.remove_external(&external_id);
        }

        for (external_id, fields) in batch.puts {
            changed = true;
            let doc_id = match self.external_to_internal.get(&external_id).copied() {
                Some(doc_id) => {
                    self.remove_document_fields(doc_id);
                    doc_id
                }
                None => self.allocate_doc_id(external_id),
            };
            self.index_document(doc_id, &fields);
            self.documents.insert(doc_id, fields);
            self.all_docs.insert(doc_id);
        }

        if changed {
            self.generation += 1;
        }
        self.generation
    }

    pub fn candidates(&self, query: &Query) -> RoaringBitmap {
        match query {
            Query::All => self.all_docs.clone(),
            Query::Exact { field, value } => self
                .exact
                .get(field)
                .and_then(|values| values.get(value))
                .cloned()
                .unwrap_or_default(),
            Query::Range {
                field,
                compare,
                value,
            } => self.range_candidates(field, *compare, value),
            Query::StringMatch {
                field,
                value,
                method: StringMatchMethod::Exact,
                case_insensitive: false,
            } => self
                .exact
                .get(field)
                .and_then(|values| values.get(&FieldValue::String(value.clone())))
                .cloned()
                .unwrap_or_default(),
            Query::StringMatch { .. } | Query::IsNull { .. } => self.all_docs.clone(),
            Query::And(queries) => self.and_candidates(queries),
            Query::Or(queries) => self.or_candidates(queries),
            Query::Not(query) => &self.all_docs - &self.candidates(query),
        }
    }

    pub fn count(&self, query: &Query) -> u64 {
        self.matching_doc_ids(query).len()
    }

    pub fn sum(&self, query: &Query, field: impl Into<FieldPath>) -> Result<SumResult, String> {
        let field = field.into();
        let mut result = SumResult::None;
        for doc_id in self.matching_doc_ids(query).iter() {
            let Some(value) = self.first_scalar(doc_id, &field) else {
                continue;
            };
            result.add(value)?;
        }
        Ok(result)
    }

    pub fn delete_matching(&mut self, query: &Query) -> Vec<String> {
        let ids: Vec<_> = self
            .matching_doc_ids(query)
            .iter()
            .filter_map(|doc_id| self.internal_to_external.get(&doc_id).cloned())
            .collect();
        let mut batch = IndexBatch::new();
        for id in &ids {
            batch = batch.delete(id.clone());
        }
        self.apply_batch(batch);
        ids
    }

    pub fn search(&self, query: &Query, sort: &[SortField], limit: Option<usize>) -> Vec<String> {
        self.search_page(query, sort, 0, limit)
    }

    pub fn search_page(
        &self,
        query: &Query,
        sort: &[SortField],
        offset: usize,
        limit: Option<usize>,
    ) -> Vec<String> {
        if sort.is_empty() {
            let matches = self.matching_doc_ids(query);
            let iter = matches.iter().skip(offset);
            if let Some(limit) = limit {
                return iter
                    .take(limit)
                    .filter_map(|doc_id| self.internal_to_external.get(&doc_id).cloned())
                    .collect();
            }
            return iter
                .filter_map(|doc_id| self.internal_to_external.get(&doc_id).cloned())
                .collect();
        }

        if let Some(page) = self.search_index_sorted_page(query, sort, offset, limit) {
            return page;
        }

        let mut doc_ids: Vec<_> = self.matching_doc_ids(query).iter().collect();
        doc_ids.sort_by(|left, right| self.compare_docs(*left, *right, sort));
        let doc_ids = doc_ids.into_iter().skip(offset);
        if let Some(limit) = limit {
            return doc_ids
                .take(limit)
                .filter_map(|doc_id| self.internal_to_external.get(&doc_id).cloned())
                .collect();
        }
        doc_ids
            .filter_map(|doc_id| self.internal_to_external.get(&doc_id).cloned())
            .collect()
    }

    fn matching_doc_ids(&self, query: &Query) -> RoaringBitmap {
        let mut matches = RoaringBitmap::new();
        for doc_id in self.candidates(query).iter() {
            if self.matches_doc(doc_id, query) {
                matches.insert(doc_id);
            }
        }
        matches
    }

    fn matches_doc(&self, doc_id: DocId, query: &Query) -> bool {
        match self.evaluate_doc(doc_id, query) {
            QueryMatch::Matched(scopes) => !scopes.is_empty(),
            QueryMatch::False | QueryMatch::Undefined => false,
        }
    }

    fn evaluate_doc(&self, doc_id: DocId, query: &Query) -> QueryMatch {
        let Some(document) = self.documents.get(&doc_id) else {
            return QueryMatch::Undefined;
        };
        self.evaluate_query(document, query)
    }

    fn evaluate_query(&self, document: &DocumentFields, query: &Query) -> QueryMatch {
        match query {
            Query::All => QueryMatch::Matched(root_scope()),
            Query::Exact { field, value } => {
                self.matching_field_scopes(document, field, |field_value| field_value == value)
            }
            Query::Range {
                field,
                compare,
                value,
            } => self.matching_field_scopes(document, field, |field_value| {
                compare_range_values(field_value, *compare, value)
            }),
            Query::StringMatch {
                field,
                value,
                method,
                case_insensitive,
            } => self.matching_field_scopes(document, field, |field_value| {
                matches_string_value(field_value, value, *method, *case_insensitive)
            }),
            Query::IsNull { field } => {
                if document
                    .scoped_scalars
                    .iter()
                    .any(|fact| fact.path == *field)
                {
                    QueryMatch::False
                } else {
                    QueryMatch::Matched(root_scope())
                }
            }
            Query::And(queries) => {
                let mut scopes = root_scope();
                for query in queries {
                    match self.evaluate_query(document, query) {
                        QueryMatch::Matched(next) => {
                            scopes = and_scope_sets(&scopes, &next);
                            if scopes.is_empty() {
                                return QueryMatch::False;
                            }
                        }
                        QueryMatch::False => return QueryMatch::False,
                        QueryMatch::Undefined => return QueryMatch::Undefined,
                    }
                }
                QueryMatch::Matched(scopes)
            }
            Query::Or(queries) => {
                let mut scopes = RoaringBitmap::new();
                let mut saw_undefined = false;
                for query in queries {
                    match self.evaluate_query(document, query) {
                        QueryMatch::Matched(next) => scopes |= next,
                        QueryMatch::False => {}
                        QueryMatch::Undefined => saw_undefined = true,
                    }
                }
                if scopes.is_empty() {
                    if saw_undefined {
                        QueryMatch::Undefined
                    } else {
                        QueryMatch::False
                    }
                } else {
                    QueryMatch::Matched(scopes)
                }
            }
            Query::Not(query) => match self.evaluate_query(document, query) {
                QueryMatch::Matched(scopes) if !scopes.is_empty() => QueryMatch::False,
                QueryMatch::Matched(_) | QueryMatch::False => QueryMatch::Matched(root_scope()),
                QueryMatch::Undefined => QueryMatch::Undefined,
            },
        }
    }

    fn matching_field_scopes(
        &self,
        document: &DocumentFields,
        field: &FieldPath,
        predicate: impl Fn(&FieldValue) -> bool,
    ) -> QueryMatch {
        let mut scopes = RoaringBitmap::new();
        let mut field_present = false;
        for fact in &document.scoped_scalars {
            if fact.path == *field {
                field_present = true;
                if predicate(&fact.value) {
                    scopes.insert(fact.scope);
                }
            }
        }
        if scopes.is_empty() {
            if field_present {
                QueryMatch::False
            } else {
                QueryMatch::Undefined
            }
        } else {
            QueryMatch::Matched(scopes)
        }
    }

    pub fn vector_search(
        &self,
        filter: &Query,
        vector_sort: &VectorSort,
        limit: usize,
    ) -> Vec<ScoredDocument> {
        let Some(vectors) = self.vectors.get(&vector_sort.field) else {
            return Vec::new();
        };
        let mut scored = Vec::new();
        for doc_id in self.candidates(filter).iter() {
            let Some(vector) = vectors.get(&doc_id) else {
                continue;
            };
            if vector.len() != vector_sort.query.len() {
                continue;
            }
            let Some(id) = self.internal_to_external.get(&doc_id) else {
                continue;
            };
            scored.push(ScoredDocument {
                id: id.clone(),
                score: vector_distance(vector, &vector_sort.query, vector_sort.metric),
            });
        }
        scored.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        scored.truncate(limit);
        scored
    }

    fn allocate_doc_id(&mut self, external_id: String) -> DocId {
        let doc_id = self.next_doc_id;
        self.next_doc_id = self
            .next_doc_id
            .checked_add(1)
            .expect("native index doc id overflow");
        self.external_to_internal
            .insert(external_id.clone(), doc_id);
        self.internal_to_external.insert(doc_id, external_id);
        doc_id
    }

    fn remove_external(&mut self, external_id: &str) -> bool {
        let Some(doc_id) = self.external_to_internal.remove(external_id) else {
            return false;
        };
        self.internal_to_external.remove(&doc_id);
        self.remove_document_fields(doc_id);
        self.all_docs.remove(doc_id);
        true
    }

    fn remove_document_fields(&mut self, doc_id: DocId) {
        let Some(fields) = self.documents.remove(&doc_id) else {
            return;
        };
        for (path, values) in fields.scalars {
            if let Some(value) = values.first() {
                self.remove_sort_value(&path, value, doc_id);
            }
            for value in values {
                remove_from_exact(&mut self.exact, &path, &value, doc_id);
                if let Some(value) = value.as_i64() {
                    remove_from_range_i64(&mut self.range_i64, &path, value, doc_id);
                } else if let Some(value) = value.as_u64() {
                    remove_from_range_u64(&mut self.range_u64, &path, value, doc_id);
                }
            }
        }
        for (path, _) in fields.vectors {
            if let Some(values) = self.vectors.get_mut(&path) {
                values.remove(&doc_id);
            }
        }
    }

    fn index_document(&mut self, doc_id: DocId, fields: &DocumentFields) {
        for (path, values) in &fields.scalars {
            if let Some(value) = values.first() {
                self.insert_sort_value(path, value, doc_id);
            }
            for value in values {
                self.exact
                    .entry(path.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .insert(doc_id);
                if let Some(value) = value.as_i64() {
                    self.range_i64
                        .entry(path.clone())
                        .or_default()
                        .entry(value)
                        .or_default()
                        .insert(doc_id);
                } else if let Some(value) = value.as_u64() {
                    self.range_u64
                        .entry(path.clone())
                        .or_default()
                        .entry(value)
                        .or_default()
                        .insert(doc_id);
                }
            }
        }
        for (path, vector) in &fields.vectors {
            self.vectors
                .entry(path.clone())
                .or_default()
                .insert(doc_id, vector.clone());
        }
    }

    fn and_candidates(&self, queries: &[Query]) -> RoaringBitmap {
        if queries.is_empty() {
            return self.all_docs.clone();
        }
        let mut candidate_sets: Vec<_> =
            queries.iter().map(|query| self.candidates(query)).collect();
        candidate_sets.sort_by_key(RoaringBitmap::len);
        let mut iter = candidate_sets.into_iter();
        let Some(mut result) = iter.next() else {
            return RoaringBitmap::new();
        };
        for candidate in iter {
            result &= candidate;
            if result.is_empty() {
                break;
            }
        }
        result
    }

    fn or_candidates(&self, queries: &[Query]) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        for query in queries {
            result |= self.candidates(query);
        }
        result
    }

    fn range_candidates(
        &self,
        field: &FieldPath,
        compare: Compare,
        value: &FieldValue,
    ) -> RoaringBitmap {
        if let Some(value) = value.as_i64() {
            return range_i64_candidates(self.range_i64.get(field), compare, value);
        }
        if let Some(value) = value.as_u64() {
            return range_u64_candidates(self.range_u64.get(field), compare, value);
        }
        RoaringBitmap::new()
    }

    fn compare_docs(&self, left: DocId, right: DocId, sort: &[SortField]) -> Ordering {
        for field in sort {
            let left_value = self.first_scalar(left, &field.field);
            let right_value = self.first_scalar(right, &field.field);
            let ordering = compare_optional_values(left_value, right_value);
            let ordering = match field.direction {
                SortDirection::Asc => ordering,
                SortDirection::Desc => ordering.reverse(),
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        left.cmp(&right)
    }

    fn first_scalar(&self, doc_id: DocId, path: &FieldPath) -> Option<&FieldValue> {
        self.documents
            .get(&doc_id)
            .and_then(|document| document.scalar_values(path))
            .and_then(|values| values.first())
    }

    fn search_index_sorted_page(
        &self,
        query: &Query,
        sort: &[SortField],
        offset: usize,
        limit: Option<usize>,
    ) -> Option<Vec<String>> {
        if sort.len() != 1 {
            return None;
        }

        let sort = &sort[0];
        let limit = limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Some(Vec::new());
        }

        let mut result = Vec::new();
        let mut skipped = 0;
        let mut seen = RoaringBitmap::new();
        let mut has_ordered_index = false;

        if sort.direction == SortDirection::Desc
            && !self.collect_missing_sorted_docs(
                &sort.field,
                query,
                offset,
                limit,
                &mut skipped,
                &mut seen,
                &mut result,
            )
        {
            return Some(result);
        }

        match sort.direction {
            SortDirection::Asc => {
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_bool.get(&sort.field),
                    false,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_i64.get(&sort.field),
                    false,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_u64.get(&sort.field),
                    false,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_string.get(&sort.field),
                    false,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_bytes.get(&sort.field),
                    false,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
            }
            SortDirection::Desc => {
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_bytes.get(&sort.field),
                    true,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_string.get(&sort.field),
                    true,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_u64.get(&sort.field),
                    true,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_i64.get(&sort.field),
                    true,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
                has_ordered_index |= self.collect_sort_index_docs(
                    self.sort_bool.get(&sort.field),
                    true,
                    query,
                    offset,
                    limit,
                    &mut skipped,
                    &mut seen,
                    &mut result,
                );
            }
        }

        if !has_ordered_index {
            return None;
        }
        if result.len() >= limit {
            return Some(result);
        }

        if sort.direction == SortDirection::Asc {
            self.collect_missing_sorted_docs(
                &sort.field,
                query,
                offset,
                limit,
                &mut skipped,
                &mut seen,
                &mut result,
            );
        }

        Some(result)
    }

    fn collect_sort_index_docs<T: Ord>(
        &self,
        index: Option<&BTreeMap<T, RoaringBitmap>>,
        reverse: bool,
        query: &Query,
        offset: usize,
        limit: usize,
        skipped: &mut usize,
        seen: &mut RoaringBitmap,
        result: &mut Vec<String>,
    ) -> bool {
        let Some(index) = index else {
            return false;
        };
        if reverse {
            self.collect_index_sorted_docs(
                index.values().rev(),
                query,
                offset,
                limit,
                skipped,
                seen,
                result,
            );
        } else {
            self.collect_index_sorted_docs(
                index.values(),
                query,
                offset,
                limit,
                skipped,
                seen,
                result,
            );
        }
        true
    }

    fn collect_index_sorted_docs<'a>(
        &self,
        bitmaps: impl Iterator<Item = &'a RoaringBitmap>,
        query: &Query,
        offset: usize,
        limit: usize,
        skipped: &mut usize,
        seen: &mut RoaringBitmap,
        result: &mut Vec<String>,
    ) -> bool {
        for bitmap in bitmaps {
            for doc_id in bitmap.iter() {
                if !self.collect_sorted_doc(doc_id, query, offset, limit, skipped, seen, result) {
                    return false;
                }
            }
        }
        true
    }

    fn collect_missing_sorted_docs(
        &self,
        field: &FieldPath,
        query: &Query,
        offset: usize,
        limit: usize,
        skipped: &mut usize,
        seen: &mut RoaringBitmap,
        result: &mut Vec<String>,
    ) -> bool {
        for doc_id in self.all_docs.iter() {
            if self.first_scalar(doc_id, field).is_some() {
                continue;
            }
            if !self.collect_sorted_doc(doc_id, query, offset, limit, skipped, seen, result) {
                return false;
            }
        }
        true
    }

    fn collect_sorted_doc(
        &self,
        doc_id: DocId,
        query: &Query,
        offset: usize,
        limit: usize,
        skipped: &mut usize,
        seen: &mut RoaringBitmap,
        result: &mut Vec<String>,
    ) -> bool {
        if result.len() >= limit {
            return false;
        }
        if !seen.insert(doc_id) || !self.matches_doc(doc_id, query) {
            return true;
        }
        if *skipped < offset {
            *skipped += 1;
            return true;
        }
        if let Some(id) = self.internal_to_external.get(&doc_id) {
            result.push(id.clone());
        }
        result.len() < limit
    }

    fn insert_sort_value(&mut self, path: &FieldPath, value: &FieldValue, doc_id: DocId) {
        match value {
            FieldValue::Bool(value) => {
                insert_into_ordered_index(&mut self.sort_bool, path, *value, doc_id)
            }
            FieldValue::I64(value) => {
                insert_into_ordered_index(&mut self.sort_i64, path, *value, doc_id)
            }
            FieldValue::U64(value) => {
                insert_into_ordered_index(&mut self.sort_u64, path, *value, doc_id)
            }
            FieldValue::String(value) => {
                insert_into_ordered_index(&mut self.sort_string, path, value.clone(), doc_id)
            }
            FieldValue::Bytes(value) => {
                insert_into_ordered_index(&mut self.sort_bytes, path, value.clone(), doc_id)
            }
        }
    }

    fn remove_sort_value(&mut self, path: &FieldPath, value: &FieldValue, doc_id: DocId) {
        match value {
            FieldValue::Bool(value) => {
                remove_from_ordered_index(&mut self.sort_bool, path, value, doc_id)
            }
            FieldValue::I64(value) => {
                remove_from_ordered_index(&mut self.sort_i64, path, value, doc_id)
            }
            FieldValue::U64(value) => {
                remove_from_ordered_index(&mut self.sort_u64, path, value, doc_id)
            }
            FieldValue::String(value) => {
                remove_from_ordered_index(&mut self.sort_string, path, value, doc_id)
            }
            FieldValue::Bytes(value) => {
                remove_from_ordered_index(&mut self.sort_bytes, path, value, doc_id)
            }
        }
    }
}

impl SumResult {
    fn add(&mut self, value: &FieldValue) -> Result<(), String> {
        match value {
            FieldValue::I64(value) => self.add_i64(*value as i128),
            FieldValue::U64(value) => self.add_u64(*value as u128),
            FieldValue::Bool(_) | FieldValue::String(_) | FieldValue::Bytes(_) => Ok(()),
        }
    }

    fn add_i64(&mut self, value: i128) -> Result<(), String> {
        match self {
            Self::None => {
                *self = Self::I64(value);
                Ok(())
            }
            Self::I64(sum) => {
                *sum = sum
                    .checked_add(value)
                    .ok_or_else(|| "native i64 sum overflow".to_string())?;
                Ok(())
            }
            Self::U64(sum) => {
                let signed_sum = i128::try_from(*sum)
                    .map_err(|_| "native sum cannot mix large u64 values with i64".to_string())?;
                *self = Self::I64(
                    signed_sum
                        .checked_add(value)
                        .ok_or_else(|| "native mixed sum overflow".to_string())?,
                );
                Ok(())
            }
        }
    }

    fn add_u64(&mut self, value: u128) -> Result<(), String> {
        match self {
            Self::None => {
                *self = Self::U64(value);
                Ok(())
            }
            Self::U64(sum) => {
                *sum = sum
                    .checked_add(value)
                    .ok_or_else(|| "native u64 sum overflow".to_string())?;
                Ok(())
            }
            Self::I64(sum) => {
                let signed_value = i128::try_from(value)
                    .map_err(|_| "native sum cannot mix large u64 values with i64".to_string())?;
                *sum = sum
                    .checked_add(signed_value)
                    .ok_or_else(|| "native mixed sum overflow".to_string())?;
                Ok(())
            }
        }
    }
}

fn root_scope() -> RoaringBitmap {
    let mut scopes = RoaringBitmap::new();
    scopes.insert(0);
    scopes
}

fn and_scope_sets(left: &RoaringBitmap, right: &RoaringBitmap) -> RoaringBitmap {
    let mut result = RoaringBitmap::new();
    if left.contains(0) {
        result |= right;
    }
    if right.contains(0) {
        result |= left;
    }
    let mut left_scoped = left.clone();
    left_scoped.remove(0);
    let mut right_scoped = right.clone();
    right_scoped.remove(0);
    result |= left_scoped & right_scoped;
    result
}

fn compare_range_values(left: &FieldValue, compare: Compare, right: &FieldValue) -> bool {
    match (left, right) {
        (FieldValue::I64(left), FieldValue::I64(right)) => {
            compare_ordering(left.cmp(right), compare)
        }
        (FieldValue::U64(left), FieldValue::U64(right)) => {
            compare_ordering(left.cmp(right), compare)
        }
        _ => false,
    }
}

fn compare_ordering(ordering: Ordering, compare: Compare) -> bool {
    match compare {
        Compare::Equal => ordering == Ordering::Equal,
        Compare::Less => ordering == Ordering::Less,
        Compare::LessOrEqual => ordering != Ordering::Greater,
        Compare::Greater => ordering == Ordering::Greater,
        Compare::GreaterOrEqual => ordering != Ordering::Less,
    }
}

fn matches_string_value(
    field_value: &FieldValue,
    query: &str,
    method: StringMatchMethod,
    case_insensitive: bool,
) -> bool {
    let FieldValue::String(value) = field_value else {
        return false;
    };
    if case_insensitive {
        let value = value.to_lowercase();
        let query = query.to_lowercase();
        return matches_string(&value, &query, method);
    }
    matches_string(value, query, method)
}

fn matches_string(value: &str, query: &str, method: StringMatchMethod) -> bool {
    match method {
        StringMatchMethod::Exact => value == query,
        StringMatchMethod::Prefix => value.starts_with(query),
        StringMatchMethod::Contains => value.contains(query),
    }
}

fn range_i64_candidates(
    index: Option<&BTreeMap<i64, RoaringBitmap>>,
    compare: Compare,
    value: i64,
) -> RoaringBitmap {
    let Some(index) = index else {
        return RoaringBitmap::new();
    };
    match compare {
        Compare::Equal => index.get(&value).cloned().unwrap_or_default(),
        Compare::Less => union_bitmaps(index.range(..value).map(|(_, bitmap)| bitmap)),
        Compare::LessOrEqual => union_bitmaps(index.range(..=value).map(|(_, bitmap)| bitmap)),
        Compare::Greater => union_bitmaps(
            index
                .range((Excluded(value), Unbounded))
                .map(|(_, bitmap)| bitmap),
        ),
        Compare::GreaterOrEqual => union_bitmaps(index.range(value..).map(|(_, bitmap)| bitmap)),
    }
}

fn range_u64_candidates(
    index: Option<&BTreeMap<u64, RoaringBitmap>>,
    compare: Compare,
    value: u64,
) -> RoaringBitmap {
    let Some(index) = index else {
        return RoaringBitmap::new();
    };
    match compare {
        Compare::Equal => index.get(&value).cloned().unwrap_or_default(),
        Compare::Less => union_bitmaps(index.range(..value).map(|(_, bitmap)| bitmap)),
        Compare::LessOrEqual => union_bitmaps(index.range(..=value).map(|(_, bitmap)| bitmap)),
        Compare::Greater => union_bitmaps(
            index
                .range((Excluded(value), Unbounded))
                .map(|(_, bitmap)| bitmap),
        ),
        Compare::GreaterOrEqual => union_bitmaps(index.range(value..).map(|(_, bitmap)| bitmap)),
    }
}

fn union_bitmaps<'a>(bitmaps: impl Iterator<Item = &'a RoaringBitmap>) -> RoaringBitmap {
    let mut result = RoaringBitmap::new();
    for bitmap in bitmaps {
        result |= bitmap;
    }
    result
}

fn remove_from_exact(
    index: &mut HashMap<FieldPath, HashMap<FieldValue, RoaringBitmap>>,
    path: &FieldPath,
    value: &FieldValue,
    doc_id: DocId,
) {
    if let Some(values) = index.get_mut(path) {
        if let Some(bitmap) = values.get_mut(value) {
            bitmap.remove(doc_id);
        }
    }
}

fn remove_from_range_i64(
    index: &mut HashMap<FieldPath, BTreeMap<i64, RoaringBitmap>>,
    path: &FieldPath,
    value: i64,
    doc_id: DocId,
) {
    if let Some(values) = index.get_mut(path) {
        if let Some(bitmap) = values.get_mut(&value) {
            bitmap.remove(doc_id);
        }
    }
}

fn remove_from_range_u64(
    index: &mut HashMap<FieldPath, BTreeMap<u64, RoaringBitmap>>,
    path: &FieldPath,
    value: u64,
    doc_id: DocId,
) {
    if let Some(values) = index.get_mut(path) {
        if let Some(bitmap) = values.get_mut(&value) {
            bitmap.remove(doc_id);
        }
    }
}

fn insert_into_ordered_index<T: Ord>(
    index: &mut HashMap<FieldPath, BTreeMap<T, RoaringBitmap>>,
    path: &FieldPath,
    value: T,
    doc_id: DocId,
) {
    index
        .entry(path.clone())
        .or_default()
        .entry(value)
        .or_default()
        .insert(doc_id);
}

fn remove_from_ordered_index<T: Ord>(
    index: &mut HashMap<FieldPath, BTreeMap<T, RoaringBitmap>>,
    path: &FieldPath,
    value: &T,
    doc_id: DocId,
) {
    if let Some(values) = index.get_mut(path) {
        if let Some(bitmap) = values.get_mut(value) {
            bitmap.remove(doc_id);
        }
    }
}

fn compare_optional_values(left: Option<&FieldValue>, right: Option<&FieldValue>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_field_values(left, right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_field_values(left: &FieldValue, right: &FieldValue) -> Ordering {
    match (left, right) {
        (FieldValue::Bool(left), FieldValue::Bool(right)) => left.cmp(right),
        (FieldValue::I64(left), FieldValue::I64(right)) => left.cmp(right),
        (FieldValue::U64(left), FieldValue::U64(right)) => left.cmp(right),
        (FieldValue::String(left), FieldValue::String(right)) => left.cmp(right),
        (FieldValue::Bytes(left), FieldValue::Bytes(right)) => left.cmp(right),
        _ => field_value_rank(left).cmp(&field_value_rank(right)),
    }
}

fn field_value_rank(value: &FieldValue) -> u8 {
    match value {
        FieldValue::Bool(_) => 0,
        FieldValue::I64(_) => 1,
        FieldValue::U64(_) => 2,
        FieldValue::String(_) => 3,
        FieldValue::Bytes(_) => 4,
    }
}

fn vector_distance(left: &[f32], right: &[f32], metric: VectorMetric) -> f32 {
    match metric {
        VectorMetric::L2 => left
            .iter()
            .zip(right)
            .map(|(left, right)| {
                let delta = left - right;
                delta * delta
            })
            .sum::<f32>()
            .sqrt(),
        VectorMetric::Dot => -left
            .iter()
            .zip(right)
            .map(|(left, right)| left * right)
            .sum::<f32>(),
        VectorMetric::Cosine => {
            let dot = left
                .iter()
                .zip(right)
                .map(|(left, right)| left * right)
                .sum::<f32>();
            let left_norm = left.iter().map(|value| value * value).sum::<f32>().sqrt();
            let right_norm = right.iter().map(|value| value * value).sum::<f32>().sqrt();
            if left_norm == 0.0 || right_norm == 0.0 {
                return f32::INFINITY;
            }
            1.0 - dot / (left_norm * right_norm)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Compare, DocumentFields, FieldValue, IndexBatch, NativeQueryIndex, Query, SortDirection,
        SortField, SumResult, VectorMetric, VectorSort,
    };

    #[test]
    fn plans_multi_field_range_query_with_bitmaps() {
        let mut index = NativeQueryIndex::new();
        for i in 0..1_000_u64 {
            index.put(
                format!("doc-{i}"),
                DocumentFields::new()
                    .with_scalar("mode", i % 3)
                    .with_scalar("timestamp", i)
                    .with_scalar("start1", i)
                    .with_scalar("end1", i + 10)
                    .with_scalar("start2", i + 100)
                    .with_scalar("end2", i + 110),
            );
        }

        let point = 250;
        let query = Query::And(vec![
            Query::Exact {
                field: "mode".into(),
                value: FieldValue::U64(1),
            },
            Query::Range {
                field: "timestamp".into(),
                compare: Compare::Less,
                value: FieldValue::U64(300),
            },
            Query::Or(vec![
                Query::And(vec![
                    Query::Range {
                        field: "start1".into(),
                        compare: Compare::LessOrEqual,
                        value: FieldValue::U64(point),
                    },
                    Query::Range {
                        field: "end1".into(),
                        compare: Compare::Greater,
                        value: FieldValue::U64(point),
                    },
                ]),
                Query::And(vec![
                    Query::Range {
                        field: "start2".into(),
                        compare: Compare::LessOrEqual,
                        value: FieldValue::U64(point),
                    },
                    Query::Range {
                        field: "end2".into(),
                        compare: Compare::Greater,
                        value: FieldValue::U64(point),
                    },
                ]),
            ]),
        ]);

        assert_eq!(
            index.search(&query, &[], None),
            vec!["doc-142", "doc-145", "doc-148", "doc-241", "doc-244", "doc-247", "doc-250"]
        );
    }

    #[test]
    fn sorts_candidates_by_scalar_fields() {
        let mut index = NativeQueryIndex::new();
        for (id, timestamp, group) in [
            ("a", 3_u64, "left"),
            ("b", 1_u64, "right"),
            ("c", 5_u64, "left"),
            ("d", 4_u64, "left"),
        ] {
            index.put(
                id,
                DocumentFields::new()
                    .with_scalar("timestamp", timestamp)
                    .with_scalar("group", group),
            );
        }

        let results = index.search(
            &Query::Exact {
                field: "group".into(),
                value: FieldValue::String("left".to_string()),
            },
            &[SortField {
                field: "timestamp".into(),
                direction: SortDirection::Desc,
            }],
            Some(2),
        );

        assert_eq!(results, vec!["c", "d"]);
    }

    #[test]
    fn sorted_pages_use_first_scalar_values() {
        let mut index = NativeQueryIndex::new();
        index.put(
            "a",
            DocumentFields::new()
                .with_scalar("title", "delta")
                .with_scalar("title", "aardvark"),
        );
        index.put("b", DocumentFields::new().with_scalar("title", "bravo"));
        index.put("c", DocumentFields::new().with_scalar("title", "charlie"));

        let sort = [SortField {
            field: "title".into(),
            direction: SortDirection::Asc,
        }];

        assert_eq!(
            index.search_page(&Query::All, &sort, 0, None),
            vec!["b", "c", "a"]
        );
        assert_eq!(index.search_page(&Query::All, &sort, 1, Some(1)), vec!["c"]);
        assert_eq!(
            index.search_page(&Query::All, &sort, 0, Some(0)),
            Vec::<String>::new()
        );
    }

    #[test]
    fn sorted_pages_match_mixed_scalar_and_missing_order() {
        let mut index = NativeQueryIndex::new();
        index.put(
            "missing",
            DocumentFields::new().with_scalar("group", "left"),
        );
        index.put("bool", DocumentFields::new().with_scalar("sort", false));
        index.put("i64", DocumentFields::new().with_scalar("sort", -1_i64));
        index.put("u64", DocumentFields::new().with_scalar("sort", 2_u64));
        index.put("string", DocumentFields::new().with_scalar("sort", "alpha"));
        index.put(
            "bytes",
            DocumentFields::new().with_scalar("sort", vec![1_u8, 2_u8]),
        );

        assert_eq!(
            index.search(
                &Query::All,
                &[SortField {
                    field: "sort".into(),
                    direction: SortDirection::Asc,
                }],
                None,
            ),
            vec!["bool", "i64", "u64", "string", "bytes", "missing"]
        );
        assert_eq!(
            index.search_page(
                &Query::All,
                &[SortField {
                    field: "sort".into(),
                    direction: SortDirection::Desc,
                }],
                1,
                Some(3),
            ),
            vec!["bytes", "string", "u64"]
        );
    }

    #[test]
    fn sums_and_deletes_matching_docs() {
        let mut index = NativeQueryIndex::new();
        index.put(
            "a",
            DocumentFields::new()
                .with_scalar("group", "left")
                .with_scalar("value", 1_u64),
        );
        index.put(
            "b",
            DocumentFields::new()
                .with_scalar("group", "right")
                .with_scalar("value", 2_u64),
        );
        index.put(
            "c",
            DocumentFields::new()
                .with_scalar("group", "left")
                .with_scalar("value", 3_u64),
        );

        let query = Query::Exact {
            field: "group".into(),
            value: FieldValue::String("left".to_string()),
        };

        assert_eq!(index.sum(&query, "value").unwrap(), SumResult::U64(4));
        assert_eq!(index.delete_matching(&query), vec!["a", "c"]);
        assert_eq!(index.search(&Query::All, &[], None), vec!["b"]);
    }

    #[test]
    fn counts_and_pages_candidates_without_materializing_all_ids() {
        let mut index = NativeQueryIndex::new();
        for i in 0..10_u64 {
            index.put(
                format!("doc-{i}"),
                DocumentFields::new().with_scalar("even", i % 2 == 0),
            );
        }

        let query = Query::Exact {
            field: "even".into(),
            value: FieldValue::Bool(true),
        };

        assert_eq!(index.count(&query), 5);
        assert_eq!(
            index.search_page(&query, &[], 1, Some(2)),
            vec!["doc-2", "doc-4"]
        );
        assert_eq!(
            index.search_page(&query, &[], 5, Some(2)),
            Vec::<String>::new()
        );
    }

    #[test]
    fn batch_replacement_updates_indexes_once() {
        let mut index = NativeQueryIndex::new();
        index.delete("missing");
        assert_eq!(index.generation(), 0);

        index.apply_batch(
            IndexBatch::new()
                .put(
                    "a",
                    DocumentFields::new()
                        .with_scalar("status", "draft")
                        .with_scalar("score", 1_u64),
                )
                .put(
                    "b",
                    DocumentFields::new()
                        .with_scalar("status", "published")
                        .with_scalar("score", 10_u64),
                ),
        );
        let generation = index.generation();

        index.apply_batch(
            IndexBatch::new()
                .put(
                    "a",
                    DocumentFields::new()
                        .with_scalar("status", "published")
                        .with_scalar("score", 2_u64),
                )
                .delete("b"),
        );

        assert_eq!(index.generation(), generation + 1);
        assert_eq!(
            index.search(
                &Query::Exact {
                    field: "status".into(),
                    value: FieldValue::String("draft".to_string()),
                },
                &[],
                None,
            ),
            Vec::<String>::new()
        );
        assert_eq!(
            index.search(
                &Query::Exact {
                    field: "status".into(),
                    value: FieldValue::String("published".to_string()),
                },
                &[],
                None,
            ),
            vec!["a"]
        );
    }

    #[test]
    fn vector_sort_composes_with_scalar_filter() {
        let mut index = NativeQueryIndex::new();
        index.put(
            "a",
            DocumentFields::new()
                .with_scalar("published", true)
                .with_vector("embedding", vec![1.0, 0.0]),
        );
        index.put(
            "b",
            DocumentFields::new()
                .with_scalar("published", false)
                .with_vector("embedding", vec![0.9, 0.1]),
        );
        index.put(
            "c",
            DocumentFields::new()
                .with_scalar("published", true)
                .with_vector("embedding", vec![0.0, 1.0]),
        );

        let results = index.vector_search(
            &Query::Exact {
                field: "published".into(),
                value: FieldValue::Bool(true),
            },
            &VectorSort {
                field: "embedding".into(),
                query: vec![1.0, 0.0],
                metric: VectorMetric::Cosine,
            },
            2,
        );

        assert_eq!(results[0].id, "a");
        assert_eq!(results[1].id, "c");
    }
}
