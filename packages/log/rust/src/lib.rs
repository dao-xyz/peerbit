use indexmap::{IndexMap, IndexSet};
use js_sys::Array;
use peerbit_indexer_rust::planner::{
    DocumentFields, FieldValue, NativeQueryIndex, Query, SortDirection, SortField,
};
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

const FIELD_HASH: u32 = 1;
const FIELD_GID: u32 = 2;
const FIELD_HEAD: u32 = 3;
const FIELD_TYPE: u32 = 4;
const FIELD_NEXT: u32 = 5;
const FIELD_WALL_TIME: u32 = 6;
const FIELD_LOGICAL: u32 = 7;
const FIELD_PAYLOAD_SIZE: u32 = 8;
const ENTRY_TYPE_CUT: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogIndexEntry {
    pub hash: String,
    pub gid: String,
    pub next: Vec<String>,
    pub entry_type: u8,
    pub wall_time: u64,
    pub logical: u32,
    pub payload_size: u32,
    pub head: bool,
}

impl LogIndexEntry {
    pub fn new(
        hash: impl Into<String>,
        gid: impl Into<String>,
        next: Vec<String>,
        entry_type: u8,
        wall_time: u64,
        logical: u32,
        payload_size: u32,
        head: bool,
    ) -> Self {
        Self {
            hash: hash.into(),
            gid: gid.into(),
            next,
            entry_type,
            wall_time,
            logical,
            payload_size,
            head,
        }
    }
}

#[derive(Default)]
pub struct LogGraphIndex {
    entries: IndexMap<String, LogIndexEntry>,
    children: HashMap<String, IndexSet<String>>,
    query: NativeQueryIndex,
}

impl LogGraphIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.children.clear();
        self.query.clear();
    }

    pub fn has(&self, hash: &str) -> bool {
        self.entries.contains_key(hash)
    }

    pub fn get(&self, hash: &str) -> Option<&LogIndexEntry> {
        self.entries.get(hash)
    }

    pub fn put(&mut self, entry: LogIndexEntry) {
        if self.entries.contains_key(&entry.hash) {
            self.delete(&entry.hash);
        }

        let hash = entry.hash.clone();
        let demotes_nexts = entry.entry_type != ENTRY_TYPE_CUT;
        let nexts = entry.next.clone();

        self.entries.insert(hash.clone(), entry);
        self.reindex(&hash);

        for next in nexts {
            self.children
                .entry(next.clone())
                .or_default()
                .insert(hash.clone());

            if demotes_nexts {
                self.set_head(&next, false);
            }
        }
    }

    pub fn delete(&mut self, hash: &str) -> Option<LogIndexEntry> {
        let entry = self.entries.shift_remove(hash)?;
        self.query.delete(hash);

        for next in &entry.next {
            if let Some(children) = self.children.get_mut(next) {
                children.shift_remove(hash);
                if children.is_empty() {
                    self.children.remove(next);
                }
            }

            if entry.entry_type != ENTRY_TYPE_CUT && self.count_has_next(next, None) == 0 {
                self.set_head(next, true);
            }
        }

        Some(entry)
    }

    pub fn heads(&self, gid: Option<&str>) -> Vec<String> {
        let query = match gid {
            Some(gid) => Query::And(vec![head_query(), gid_query(gid)]),
            None => head_query(),
        };
        self.query.search(&query, &head_sort(), None)
    }

    pub fn children(&self, hash: &str) -> Vec<String> {
        self.children
            .get(hash)
            .map(|children| children.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn count_has_next(&self, next: &str, exclude_hash: Option<&str>) -> usize {
        let Some(children) = self.children.get(next) else {
            return 0;
        };
        match exclude_hash {
            Some(exclude_hash) => children
                .iter()
                .filter(|hash| hash.as_str() != exclude_hash)
                .count(),
            None => children.len(),
        }
    }

    pub fn shadowed_gids(
        &self,
        gid: &str,
        nexts: &[String],
        exclude_hash: Option<&str>,
    ) -> Vec<String> {
        let mut shadowed = IndexSet::new();
        for next in nexts {
            let Some(next_entry) = self.entries.get(next) else {
                continue;
            };
            if next_entry.gid == gid {
                continue;
            }
            if self.count_has_next(&next_entry.hash, exclude_hash) == 0 {
                shadowed.insert(next_entry.gid.clone());
            }
        }
        shadowed.into_iter().collect()
    }

    fn set_head(&mut self, hash: &str, head: bool) {
        let Some(entry) = self.entries.get_mut(hash) else {
            return;
        };
        if entry.head == head {
            return;
        }
        entry.head = head;
        self.reindex(hash);
    }

    fn reindex(&mut self, hash: &str) {
        let Some(entry) = self.entries.get(hash) else {
            return;
        };
        self.query.put(hash.to_string(), fields_for_entry(entry));
    }
}

fn fields_for_entry(entry: &LogIndexEntry) -> DocumentFields {
    let mut fields = DocumentFields::with_scalar_capacity(8 + entry.next.len());
    fields.insert_scalar(FIELD_HASH, FieldValue::String(entry.hash.clone()));
    fields.insert_scalar(FIELD_GID, FieldValue::String(entry.gid.clone()));
    fields.insert_scalar(FIELD_HEAD, FieldValue::Bool(entry.head));
    fields.insert_scalar(FIELD_TYPE, FieldValue::U64(entry.entry_type as u64));
    fields.insert_scalar(FIELD_WALL_TIME, FieldValue::U64(entry.wall_time));
    fields.insert_scalar(FIELD_LOGICAL, FieldValue::U64(entry.logical as u64));
    fields.insert_scalar(
        FIELD_PAYLOAD_SIZE,
        FieldValue::U64(entry.payload_size as u64),
    );
    for next in &entry.next {
        fields.insert_scalar(FIELD_NEXT, FieldValue::String(next.clone()));
    }
    fields
}

fn head_query() -> Query {
    Query::Exact {
        field: FIELD_HEAD.into(),
        value: FieldValue::Bool(true),
    }
}

fn gid_query(gid: &str) -> Query {
    Query::Exact {
        field: FIELD_GID.into(),
        value: FieldValue::String(gid.to_string()),
    }
}

fn head_sort() -> [SortField; 3] {
    [
        SortField {
            field: FIELD_WALL_TIME.into(),
            direction: SortDirection::Asc,
        },
        SortField {
            field: FIELD_LOGICAL.into(),
            direction: SortDirection::Asc,
        },
        SortField {
            field: FIELD_HASH.into(),
            direction: SortDirection::Asc,
        },
    ]
}

#[wasm_bindgen]
pub struct NativeLogIndex {
    inner: LogGraphIndex,
}

#[wasm_bindgen]
impl NativeLogIndex {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: LogGraphIndex::new(),
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn has(&self, hash: &str) -> bool {
        self.inner.has(hash)
    }

    pub fn put(
        &mut self,
        hash: String,
        gid: String,
        next: Array,
        entry_type: u8,
        wall_time: u64,
        logical: u32,
        payload_size: u32,
        head: bool,
    ) -> Result<(), JsValue> {
        let next = strings_from_array(next)?;
        self.inner.put(LogIndexEntry::new(
            hash,
            gid,
            next,
            entry_type,
            wall_time,
            logical,
            payload_size,
            head,
        ));
        Ok(())
    }

    pub fn delete(&mut self, hash: &str) -> bool {
        self.inner.delete(hash).is_some()
    }

    pub fn heads(&self, gid: Option<String>) -> Array {
        strings_to_array(self.inner.heads(gid.as_deref()))
    }

    pub fn children(&self, hash: &str) -> Array {
        strings_to_array(self.inner.children(hash))
    }

    pub fn count_has_next(&self, next: &str, exclude_hash: Option<String>) -> usize {
        self.inner.count_has_next(next, exclude_hash.as_deref())
    }

    pub fn shadowed_gids(
        &self,
        gid: &str,
        next: Array,
        exclude_hash: Option<String>,
    ) -> Result<Array, JsValue> {
        let next = strings_from_array(next)?;
        Ok(strings_to_array(self.inner.shadowed_gids(
            gid,
            &next,
            exclude_hash.as_deref(),
        )))
    }
}

impl Default for NativeLogIndex {
    fn default() -> Self {
        Self::new()
    }
}

fn strings_from_array(values: Array) -> Result<Vec<String>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for value in values.iter() {
        let Some(value) = value.as_string() else {
            return Err(JsValue::from_str("Expected string array"));
        };
        out.push(value);
    }
    Ok(out)
}

fn strings_to_array(values: Vec<String>) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&JsValue::from_str(&value));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{LogGraphIndex, LogIndexEntry};

    const APPEND: u8 = 0;
    const CUT: u8 = 1;

    fn entry(hash: &str, gid: &str, next: &[&str], wall_time: u64) -> LogIndexEntry {
        LogIndexEntry::new(
            hash,
            gid,
            next.iter().map(|next| next.to_string()).collect(),
            APPEND,
            wall_time,
            0,
            1,
            true,
        )
    }

    #[test]
    fn tracks_heads_and_next_adjacency() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));
        assert_eq!(index.heads(None), vec!["a"]);

        index.put(entry("b", "g", &["a"], 2));
        assert_eq!(index.heads(None), vec!["b"]);
        assert_eq!(index.children("a"), vec!["b"]);
        assert_eq!(index.count_has_next("a", None), 1);

        index.put(entry("c", "g", &["a"], 3));
        assert_eq!(index.heads(None), vec!["b", "c"]);
        assert_eq!(index.count_has_next("a", None), 2);

        assert!(index.delete("b").is_some());
        assert_eq!(index.heads(None), vec!["c"]);
        assert_eq!(index.count_has_next("a", None), 1);

        assert!(index.delete("c").is_some());
        assert_eq!(index.heads(None), vec!["a"]);
        assert_eq!(index.count_has_next("a", None), 0);
    }

    #[test]
    fn filters_heads_by_gid_and_clock_order() {
        let mut index = LogGraphIndex::new();
        index.put(entry("b", "one", &[], 2));
        index.put(entry("a", "one", &[], 1));
        index.put(entry("c", "two", &[], 3));

        assert_eq!(index.heads(None), vec!["a", "b", "c"]);
        assert_eq!(index.heads(Some("one")), vec!["a", "b"]);
        assert_eq!(index.heads(Some("two")), vec!["c"]);
    }

    #[test]
    fn cut_entries_do_not_demote_their_nexts() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["a".to_string()],
            CUT,
            2,
            0,
            1,
            true,
        ));

        assert_eq!(index.heads(None), vec!["a", "cut"]);
        assert_eq!(index.count_has_next("a", None), 1);

        assert!(index.delete("cut").is_some());
        assert_eq!(index.heads(None), vec!["a"]);
    }

    #[test]
    fn reports_shadowed_gids_for_cross_gid_nexts() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "old", &[], 1));

        assert_eq!(
            index.shadowed_gids("new", &["a".to_string()], Some("b")),
            vec!["old"]
        );

        index.put(entry("c", "other", &["a"], 2));
        assert!(index
            .shadowed_gids("new", &["a".to_string()], Some("b"))
            .is_empty());
    }
}
