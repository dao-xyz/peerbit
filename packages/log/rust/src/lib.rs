use indexmap::{IndexMap, IndexSet};
use js_sys::{Array, Uint8Array};
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
    pub data: Option<Vec<u8>>,
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
        Self::new_with_data(
            hash,
            gid,
            next,
            entry_type,
            wall_time,
            logical,
            payload_size,
            head,
            None,
        )
    }

    pub fn new_with_data(
        hash: impl Into<String>,
        gid: impl Into<String>,
        next: Vec<String>,
        entry_type: u8,
        wall_time: u64,
        logical: u32,
        payload_size: u32,
        head: bool,
        data: Option<Vec<u8>>,
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
            data,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinPlan {
    pub skip: bool,
    pub missing_parents: Vec<String>,
    pub cut_checked: bool,
    pub covered_by_cut: bool,
}

#[derive(Default)]
pub struct LogGraphIndex {
    entries: IndexMap<String, LogIndexEntry>,
    children: HashMap<String, IndexSet<String>>,
    query: NativeQueryIndex,
    payload_size_total: u64,
}

impl LogGraphIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn payload_size_sum(&self) -> u64 {
        self.payload_size_total
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.children.clear();
        self.query.clear();
        self.payload_size_total = 0;
    }

    pub fn has(&self, hash: &str) -> bool {
        self.entries.contains_key(hash)
    }

    pub fn has_many(&self, hashes: &[String]) -> Vec<String> {
        hashes
            .iter()
            .filter(|hash| self.has(hash))
            .cloned()
            .collect()
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
        let payload_size = entry.payload_size as u64;

        self.entries.insert(hash.clone(), entry);
        self.payload_size_total += payload_size;
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
        self.payload_size_total = self
            .payload_size_total
            .saturating_sub(entry.payload_size as u64);

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

    pub fn head_entries(&self, gid: Option<&str>) -> Vec<LogIndexEntry> {
        self.heads(gid)
            .into_iter()
            .filter_map(|hash| self.entries.get(&hash).cloned())
            .collect()
    }

    pub fn head_data_entries(&self, gid: Option<&str>) -> Vec<LogIndexEntry> {
        self.head_entries(gid)
    }

    pub fn max_head_data_u32(&self, gid: Option<&str>) -> Option<u32> {
        let mut max = None;
        for entry in self.head_data_entries(gid) {
            let value = decode_absolute_replica_data_u32(entry.data.as_deref())?;
            max = Some(max.map_or(value, |current: u32| current.max(value)));
        }
        max
    }

    pub fn head_join_entries(&self, gid: Option<&str>) -> Vec<LogIndexEntry> {
        self.head_entries(gid)
    }

    pub fn child_join_entries(&self, hash: &str) -> Vec<LogIndexEntry> {
        self.children(hash)
            .into_iter()
            .filter_map(|child_hash| self.entries.get(&child_hash).cloned())
            .collect()
    }

    pub fn plan_delete_recursively(&self, from: &[String], skip_first: bool) -> Vec<String> {
        let mut stack = from.to_vec();
        let mut visited = IndexSet::new();
        let mut delete_hashes = IndexSet::new();
        let mut counter = 0;

        while let Some(hash) = stack.pop() {
            if !visited.insert(hash.clone()) {
                continue;
            }
            let Some(entry) = self.entries.get(&hash) else {
                counter += 1;
                continue;
            };

            let skip = counter == 0 && skip_first;
            if !skip {
                delete_hashes.insert(hash.clone());
            }

            for next in &entry.next {
                let has_alternative_next = self
                    .child_join_entries(next)
                    .into_iter()
                    .any(|child| child.entry_type != ENTRY_TYPE_CUT && child.hash != entry.hash);
                if !has_alternative_next && self.entries.contains_key(next) {
                    stack.push(next.clone());
                }
            }
            counter += 1;
        }

        delete_hashes.into_iter().collect()
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

    pub fn plan_join(
        &self,
        hash: &str,
        nexts: &[String],
        entry_type: u8,
        reset: bool,
        gid: Option<&str>,
        wall_time: Option<u64>,
        logical: Option<u32>,
    ) -> JoinPlan {
        let cut_checked = gid.is_some() && wall_time.is_some() && logical.is_some();
        if !reset && self.has(hash) {
            return JoinPlan {
                skip: true,
                missing_parents: Vec::new(),
                cut_checked,
                covered_by_cut: false,
            };
        }

        let covered_by_cut = match (gid, wall_time, logical) {
            (Some(gid), Some(wall_time), Some(logical)) => {
                self.covered_by_cut(hash, gid, wall_time, logical)
            }
            _ => false,
        };

        let missing_parents = if entry_type == ENTRY_TYPE_CUT {
            Vec::new()
        } else if covered_by_cut {
            Vec::new()
        } else {
            nexts
                .iter()
                .filter(|next| reset || !self.has(next))
                .cloned()
                .collect()
        };

        JoinPlan {
            skip: false,
            missing_parents,
            cut_checked,
            covered_by_cut,
        }
    }

    fn covered_by_cut(&self, hash: &str, gid: &str, wall_time: u64, logical: u32) -> bool {
        self.head_entries(Some(gid)).into_iter().any(|entry| {
            entry.entry_type == ENTRY_TYPE_CUT
                && entry.next.iter().any(|next| next == hash)
                && compare_clock(wall_time, logical, &entry).is_lt()
        })
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

fn compare_clock(wall_time: u64, logical: u32, other: &LogIndexEntry) -> std::cmp::Ordering {
    wall_time
        .cmp(&other.wall_time)
        .then_with(|| logical.cmp(&other.logical))
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

    pub fn payload_size_sum(&self) -> f64 {
        self.inner.payload_size_sum() as f64
    }

    pub fn has(&self, hash: &str) -> bool {
        self.inner.has(hash)
    }

    pub fn has_many(&self, hashes: Array) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        Ok(strings_to_array(self.inner.has_many(&hashes)))
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
        data: JsValue,
    ) -> Result<(), JsValue> {
        let next = strings_from_array(next)?;
        self.inner.put(LogIndexEntry::new_with_data(
            hash,
            gid,
            next,
            entry_type,
            wall_time,
            logical,
            payload_size,
            head,
            optional_bytes_from_js(data),
        ));
        Ok(())
    }

    pub fn delete(&mut self, hash: &str) -> bool {
        self.inner.delete(hash).is_some()
    }

    pub fn heads(&self, gid: Option<String>) -> Array {
        strings_to_array(self.inner.heads(gid.as_deref()))
    }

    pub fn head_entries(&self, gid: Option<String>) -> Array {
        log_entries_to_rows(self.inner.head_entries(gid.as_deref()))
    }

    pub fn head_data_entries(&self, gid: Option<String>) -> Array {
        log_data_entries_to_rows(self.inner.head_data_entries(gid.as_deref()))
    }

    pub fn max_head_data_u32(&self, gid: Option<String>) -> JsValue {
        self.inner
            .max_head_data_u32(gid.as_deref())
            .map(|value| JsValue::from_f64(value as f64))
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn head_join_entries(&self, gid: Option<String>) -> Array {
        log_join_entries_to_rows(self.inner.head_join_entries(gid.as_deref()))
    }

    pub fn child_join_entries(&self, hash: &str) -> Array {
        log_join_entries_to_rows(self.inner.child_join_entries(hash))
    }

    pub fn plan_delete_recursively(&self, from: Array, skip_first: bool) -> Result<Array, JsValue> {
        let from = strings_from_array(from)?;
        Ok(strings_to_array(
            self.inner.plan_delete_recursively(&from, skip_first),
        ))
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

    pub fn plan_join(
        &self,
        hash: &str,
        next: Array,
        entry_type: u8,
        reset: bool,
        gid: Option<String>,
        wall_time: Option<u64>,
        logical: Option<u32>,
    ) -> Result<Array, JsValue> {
        let next = strings_from_array(next)?;
        Ok(join_plan_to_row(self.inner.plan_join(
            hash,
            &next,
            entry_type,
            reset,
            gid.as_deref(),
            wall_time,
            logical,
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

fn optional_bytes_from_js(value: JsValue) -> Option<Vec<u8>> {
    if value.is_undefined() || value.is_null() {
        return None;
    }
    Some(Uint8Array::new(&value).to_vec())
}

fn decode_absolute_replica_data_u32(data: Option<&[u8]>) -> Option<u32> {
    let data = data?;
    if data.len() != 5 || data[0] != 0 {
        return None;
    }
    Some(u32::from_le_bytes([data[1], data[2], data[3], data[4]]))
}

fn log_entries_to_rows(values: Vec<LogIndexEntry>) -> Array {
    let out = Array::new();
    for entry in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&entry.hash));
        row.push(&JsValue::from_str(&entry.gid));
        row.push(&JsValue::from_str(&entry.wall_time.to_string()));
        row.push(&JsValue::from_f64(entry.logical as f64));
        out.push(&row);
    }
    out
}

fn log_data_entries_to_rows(values: Vec<LogIndexEntry>) -> Array {
    let out = Array::new();
    for entry in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&entry.hash));
        match entry.data {
            Some(data) => row.push(&Uint8Array::from(data.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
        out.push(&row);
    }
    out
}

fn log_join_entries_to_rows(values: Vec<LogIndexEntry>) -> Array {
    let out = Array::new();
    for entry in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&entry.hash));
        row.push(&JsValue::from_str(&entry.gid));
        row.push(&JsValue::from_str(&entry.wall_time.to_string()));
        row.push(&JsValue::from_f64(entry.logical as f64));
        row.push(&JsValue::from_f64(entry.entry_type as f64));
        row.push(&strings_to_array(entry.next));
        out.push(&row);
    }
    out
}

fn join_plan_to_row(plan: JoinPlan) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_bool(plan.skip));
    row.push(&strings_to_array(plan.missing_parents));
    row.push(&JsValue::from_bool(plan.cut_checked));
    row.push(&JsValue::from_bool(plan.covered_by_cut));
    row
}

#[cfg(test)]
mod tests {
    use super::{JoinPlan, LogGraphIndex, LogIndexEntry};

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
    fn sums_payload_sizes() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new(
            "a",
            "one",
            Vec::new(),
            APPEND,
            1,
            0,
            7,
            true,
        ));
        index.put(LogIndexEntry::new(
            "b",
            "one",
            Vec::new(),
            APPEND,
            2,
            0,
            9,
            true,
        ));

        assert_eq!(index.payload_size_sum(), 16);

        index.delete("a");
        assert_eq!(index.payload_size_sum(), 9);
    }

    #[test]
    fn batches_membership_checks() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "one", &[], 1));
        index.put(entry("c", "one", &[], 3));

        assert_eq!(
            index.has_many(&["missing".to_string(), "a".to_string(), "c".to_string()]),
            vec!["a".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn returns_head_entries_for_append_planning() {
        let mut index = LogGraphIndex::new();
        index.put(entry("b", "one", &[], 2));
        index.put(entry("a", "one", &[], 1));
        index.put(entry("c", "two", &[], 3));

        let heads = index.head_entries(Some("one"));
        assert_eq!(heads.len(), 2);
        assert_eq!(heads[0].hash, "a");
        assert_eq!(heads[0].gid, "one");
        assert_eq!(heads[0].wall_time, 1);
        assert_eq!(heads[1].hash, "b");
        assert_eq!(heads[1].gid, "one");
        assert_eq!(heads[1].wall_time, 2);
    }

    #[test]
    fn returns_head_join_entries_for_cut_checks() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new(
            "cut",
            "one",
            vec!["a".to_string()],
            CUT,
            2,
            0,
            1,
            true,
        ));

        let heads = index.head_join_entries(Some("one"));
        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0].hash, "cut");
        assert_eq!(heads[0].entry_type, CUT);
        assert_eq!(heads[0].next, vec!["a".to_string()]);
    }

    #[test]
    fn returns_child_join_entries_for_cut_recursion() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));
        index.put(entry("b", "g", &["a"], 2));
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["a".to_string()],
            CUT,
            3,
            0,
            1,
            true,
        ));

        let children = index.child_join_entries("a");
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].hash, "b");
        assert_eq!(children[0].entry_type, APPEND);
        assert_eq!(children[1].hash, "cut");
        assert_eq!(children[1].entry_type, CUT);
    }

    #[test]
    fn plans_recursive_cut_deletes() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put(entry("child", "g", &["root"], 2));
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["child".to_string()],
            CUT,
            3,
            0,
            1,
            true,
        ));

        assert_eq!(
            index.plan_delete_recursively(&["cut".to_string()], true),
            vec!["child".to_string(), "root".to_string()]
        );
    }

    #[test]
    fn recursive_cut_delete_plan_keeps_alternative_branches() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put(entry("child", "g", &["root"], 2));
        index.put(entry("sibling", "g", &["root"], 3));
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["child".to_string()],
            CUT,
            4,
            0,
            1,
            true,
        ));

        assert_eq!(
            index.plan_delete_recursively(&["cut".to_string()], true),
            vec!["child".to_string()]
        );
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
    fn plans_join_missing_parents() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));

        assert_eq!(
            index.plan_join(
                "b",
                &["a".to_string(), "missing".to_string()],
                APPEND,
                false,
                None,
                None,
                None
            ),
            JoinPlan {
                skip: false,
                missing_parents: vec!["missing".to_string()],
                cut_checked: false,
                covered_by_cut: false
            }
        );
        assert_eq!(
            index.plan_join("a", &[], APPEND, false, None, None, None),
            JoinPlan {
                skip: true,
                missing_parents: Vec::new(),
                cut_checked: false,
                covered_by_cut: false
            }
        );
        assert_eq!(
            index.plan_join("a", &[], APPEND, true, None, None, None),
            JoinPlan {
                skip: false,
                missing_parents: Vec::new(),
                cut_checked: false,
                covered_by_cut: false
            }
        );
        assert_eq!(
            index.plan_join(
                "cut",
                &["missing".to_string()],
                CUT,
                false,
                None,
                None,
                None
            ),
            JoinPlan {
                skip: false,
                missing_parents: Vec::new(),
                cut_checked: false,
                covered_by_cut: false
            }
        );
    }

    #[test]
    fn plans_join_cut_coverage() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new(
            "cut",
            "g",
            vec!["old".to_string()],
            CUT,
            2,
            0,
            1,
            true,
        ));

        assert_eq!(
            index.plan_join(
                "old",
                &["missing".to_string()],
                APPEND,
                false,
                Some("g"),
                Some(1),
                Some(0)
            ),
            JoinPlan {
                skip: false,
                missing_parents: Vec::new(),
                cut_checked: true,
                covered_by_cut: true
            }
        );
        assert_eq!(
            index.plan_join(
                "new",
                &["missing".to_string()],
                APPEND,
                false,
                Some("g"),
                Some(3),
                Some(0)
            ),
            JoinPlan {
                skip: false,
                missing_parents: vec!["missing".to_string()],
                cut_checked: true,
                covered_by_cut: false
            }
        );
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
