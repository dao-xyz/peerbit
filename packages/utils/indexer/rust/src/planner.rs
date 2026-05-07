use roaring::RoaringBitmap;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};

pub type DocId = u32;
pub type FieldPath = String;

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
    vectors: HashMap<FieldPath, Vec<f32>>,
}

impl DocumentFields {
    pub fn new() -> Self {
        Self::default()
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
        self.scalars
            .entry(path.into())
            .or_default()
            .push(value.into());
    }

    pub fn insert_vector(&mut self, path: impl Into<FieldPath>, value: Vec<f32>) {
        self.vectors.insert(path.into(), value);
    }

    pub fn scalar_values(&self, path: &str) -> Option<&[FieldValue]> {
        self.scalars.get(path).map(Vec::as_slice)
    }

    pub fn vector(&self, path: &str) -> Option<&[f32]> {
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
    And(Vec<Query>),
    Or(Vec<Query>),
    Not(Box<Query>),
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

    pub fn put(&mut self, id: impl Into<String>, fields: DocumentFields) {
        self.apply_batch(IndexBatch::new().put(id, fields));
    }

    pub fn delete(&mut self, id: impl Into<String>) {
        self.apply_batch(IndexBatch::new().delete(id));
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
            Query::And(queries) => self.and_candidates(queries),
            Query::Or(queries) => self.or_candidates(queries),
            Query::Not(query) => &self.all_docs - &self.candidates(query),
        }
    }

    pub fn search(&self, query: &Query, sort: &[SortField], limit: Option<usize>) -> Vec<String> {
        let mut doc_ids: Vec<_> = self.candidates(query).iter().collect();
        if !sort.is_empty() {
            doc_ids.sort_by(|left, right| self.compare_docs(*left, *right, sort));
        }
        if let Some(limit) = limit {
            doc_ids.truncate(limit);
        }
        doc_ids
            .into_iter()
            .filter_map(|doc_id| self.internal_to_external.get(&doc_id).cloned())
            .collect()
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

    fn range_candidates(&self, field: &str, compare: Compare, value: &FieldValue) -> RoaringBitmap {
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

    fn first_scalar(&self, doc_id: DocId, path: &str) -> Option<&FieldValue> {
        self.documents
            .get(&doc_id)
            .and_then(|document| document.scalar_values(path))
            .and_then(|values| values.first())
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
    path: &str,
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
    path: &str,
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
    path: &str,
    value: u64,
    doc_id: DocId,
) {
    if let Some(values) = index.get_mut(path) {
        if let Some(bitmap) = values.get_mut(&value) {
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
        SortField, VectorMetric, VectorSort,
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
                field: "mode".to_string(),
                value: FieldValue::U64(1),
            },
            Query::Range {
                field: "timestamp".to_string(),
                compare: Compare::Less,
                value: FieldValue::U64(300),
            },
            Query::Or(vec![
                Query::And(vec![
                    Query::Range {
                        field: "start1".to_string(),
                        compare: Compare::LessOrEqual,
                        value: FieldValue::U64(point),
                    },
                    Query::Range {
                        field: "end1".to_string(),
                        compare: Compare::Greater,
                        value: FieldValue::U64(point),
                    },
                ]),
                Query::And(vec![
                    Query::Range {
                        field: "start2".to_string(),
                        compare: Compare::LessOrEqual,
                        value: FieldValue::U64(point),
                    },
                    Query::Range {
                        field: "end2".to_string(),
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
                field: "group".to_string(),
                value: FieldValue::String("left".to_string()),
            },
            &[SortField {
                field: "timestamp".to_string(),
                direction: SortDirection::Desc,
            }],
            Some(2),
        );

        assert_eq!(results, vec!["c", "d"]);
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
                    field: "status".to_string(),
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
                    field: "status".to_string(),
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
                field: "published".to_string(),
                value: FieldValue::Bool(true),
            },
            &VectorSort {
                field: "embedding".to_string(),
                query: vec![1.0, 0.0],
                metric: VectorMetric::Cosine,
            },
            2,
        );

        assert_eq!(results[0].id, "a");
        assert_eq!(results[1].id, "c");
    }
}
