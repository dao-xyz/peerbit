use ed25519_dalek::{Signer, SigningKey};
use indexmap::{IndexMap, IndexSet};
use js_sys::{Array, BigUint64Array, Uint32Array, Uint8Array};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use wasm_bindgen::prelude::*;

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
    heads: IndexSet<String>,
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
        self.heads.clear();
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

    pub fn oldest_hash(&self) -> Option<String> {
        self.entries
            .values()
            .min_by(|left, right| compare_entry_order(left, right))
            .map(|entry| entry.hash.clone())
    }

    pub fn newest_hash(&self) -> Option<String> {
        self.entries
            .values()
            .max_by(|left, right| compare_entry_order(left, right))
            .map(|entry| entry.hash.clone())
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
        let head = entry.head;

        self.entries.insert(hash.clone(), entry);
        self.payload_size_total += payload_size;
        if head {
            self.heads.insert(hash.clone());
        } else {
            self.heads.shift_remove(&hash);
        }

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

    pub fn put_many(&mut self, entries: Vec<LogIndexEntry>) {
        for entry in entries {
            self.put(entry);
        }
    }

    pub fn put_append_chain(&mut self, entries: Vec<LogIndexEntry>, initial_next: &[String]) {
        let Some(first) = entries.first() else {
            return;
        };
        let demotes_initial_nexts = first.entry_type != ENTRY_TYPE_CUT;

        for entry in entries {
            let hash = entry.hash.clone();
            let nexts = entry.next.clone();
            let payload_size = entry.payload_size as u64;
            let head = entry.head;

            self.entries.insert(hash.clone(), entry);
            self.payload_size_total += payload_size;
            if head {
                self.heads.insert(hash.clone());
            } else {
                self.heads.shift_remove(&hash);
            }

            for next in nexts {
                self.children.entry(next).or_default().insert(hash.clone());
            }
        }

        if demotes_initial_nexts {
            for next in initial_next {
                self.set_head(next, false);
            }
        }
    }

    pub fn delete(&mut self, hash: &str) -> Option<LogIndexEntry> {
        let entry = self.entries.shift_remove(hash)?;
        self.heads.shift_remove(hash);
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
        self.head_entries(gid)
            .into_iter()
            .map(|entry| entry.hash)
            .collect()
    }

    pub fn has_head(&self, gid: Option<&str>) -> bool {
        match gid {
            Some(gid) => self
                .heads
                .iter()
                .filter_map(|hash| self.entries.get(hash))
                .any(|entry| entry.gid == gid),
            None => !self.heads.is_empty(),
        }
    }

    pub fn has_any_head(&self, gids: &[String]) -> bool {
        gids.iter().any(|gid| self.has_head(Some(gid)))
    }

    pub fn has_any_head_batch(&self, gid_sets: &[Vec<String>]) -> Vec<bool> {
        gid_sets
            .iter()
            .map(|gids| self.has_any_head(gids))
            .collect()
    }

    pub fn head_entries(&self, gid: Option<&str>) -> Vec<LogIndexEntry> {
        let mut entries: Vec<_> = self
            .heads
            .iter()
            .filter_map(|hash| self.entries.get(hash))
            .filter(|entry| match gid {
                Some(gid) => entry.gid == gid,
                None => true,
            })
            .cloned()
            .collect();
        entries.sort_by(|left, right| {
            compare_clock(left.wall_time, left.logical, right)
                .then_with(|| left.hash.cmp(&right.hash))
        });
        entries
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

    pub fn unique_reference_gids(&self, hash: &str) -> Option<Vec<String>> {
        let entry = self.entries.get(hash)?;
        if entry.entry_type == ENTRY_TYPE_CUT {
            return Some(Vec::new());
        }

        let mut visited_gids = IndexSet::new();
        visited_gids.insert(entry.gid.clone());
        let mut out = Vec::new();
        let mut queue: VecDeque<String> = entry.next.iter().cloned().collect();

        while let Some(next_hash) = queue.pop_front() {
            let Some(next_entry) = self.entries.get(&next_hash) else {
                return None;
            };
            if !visited_gids.insert(next_entry.gid.clone()) {
                continue;
            }
            out.push(next_entry.gid.clone());
            if next_entry.entry_type == ENTRY_TYPE_CUT {
                continue;
            }
            queue.extend(next_entry.next.iter().cloned());
        }

        Some(out)
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
        if head {
            self.heads.insert(hash.to_string());
        } else {
            self.heads.shift_remove(hash);
        }
    }
}

fn compare_clock(wall_time: u64, logical: u32, other: &LogIndexEntry) -> std::cmp::Ordering {
    wall_time
        .cmp(&other.wall_time)
        .then_with(|| logical.cmp(&other.logical))
}

fn compare_entry_order(left: &LogIndexEntry, right: &LogIndexEntry) -> std::cmp::Ordering {
    compare_clock(left.wall_time, left.logical, right).then_with(|| left.hash.cmp(&right.hash))
}

#[wasm_bindgen]
pub struct NativeLogIndex {
    inner: LogGraphIndex,
}

#[wasm_bindgen]
pub struct NativeLogBlockStore {
    entries: IndexMap<String, Vec<u8>>,
    total_size: u64,
}

#[wasm_bindgen]
pub struct NativeEntryV0PlainBuilder {
    clock_id: Vec<u8>,
    public_key: Vec<u8>,
    signing_key: SigningKey,
}

#[wasm_bindgen]
impl NativeLogBlockStore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            entries: IndexMap::new(),
            total_size: 0,
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.entries.get(key).cloned()
    }

    pub fn get_many(&self, keys: Array) -> Result<Array, JsValue> {
        let values = Array::new();
        for key in strings_from_array(keys)? {
            match self.entries.get(&key) {
                Some(value) => values.push(&Uint8Array::from(value.as_slice())),
                None => values.push(&JsValue::UNDEFINED),
            };
        }
        Ok(values)
    }

    pub fn has(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    pub fn put(&mut self, key: String, value: Vec<u8>) {
        self.put_entry(key, value);
    }

    pub fn put_many(&mut self, keys: Array, values: Array) -> Result<(), JsValue> {
        self.put_entries(block_key_values_from_arrays(&keys, &values)?);
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> bool {
        if let Some((_key, previous)) = self.entries.shift_remove_entry(key) {
            self.total_size = self.total_size.saturating_sub(previous.len() as u64);
            true
        } else {
            false
        }
    }

    pub fn delete_many(&mut self, keys: Array) -> Result<usize, JsValue> {
        let mut deleted = 0;
        for key in strings_from_array(keys)? {
            if self.delete(&key) {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.total_size = 0;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn size(&self) -> f64 {
        self.total_size as f64
    }

    pub fn entries(&self) -> Array {
        let entries = Array::new();
        for (key, value) in &self.entries {
            let pair = Array::new();
            pair.push(&JsValue::from_str(key));
            pair.push(&Uint8Array::from(value.as_slice()));
            entries.push(&pair);
        }
        entries
    }
}

impl NativeLogBlockStore {
    fn put_entry(&mut self, key: String, value: Vec<u8>) {
        let value_len = value.len() as u64;
        if let Some(previous) = self.entries.insert(key, value) {
            self.total_size = self.total_size.saturating_sub(previous.len() as u64);
        }
        self.total_size += value_len;
    }

    fn put_entries(&mut self, entries: Vec<(String, Vec<u8>)>) {
        for (key, value) in entries {
            self.put_entry(key, value);
        }
    }
}

#[wasm_bindgen]
impl NativeEntryV0PlainBuilder {
    #[wasm_bindgen(constructor)]
    pub fn new(
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
    ) -> Result<Self, JsValue> {
        let private_key = private_key.to_vec();
        let public_key = public_key.to_vec();
        let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
        Ok(Self {
            clock_id: clock_id.to_vec(),
            public_key,
            signing_key,
        })
    }
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

    pub fn oldest_hash(&self) -> JsValue {
        self.inner
            .oldest_hash()
            .map(|hash| JsValue::from_str(&hash))
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn newest_hash(&self) -> JsValue {
        self.inner
            .newest_hash()
            .map(|hash| JsValue::from_str(&hash))
            .unwrap_or(JsValue::UNDEFINED)
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

    pub fn put_many(
        &mut self,
        hashes: Array,
        gids: Array,
        nexts: Array,
        entry_types: Uint8Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        payload_sizes: Uint32Array,
        heads: Uint8Array,
        datas: Array,
    ) -> Result<(), JsValue> {
        let len = hashes.length();
        for values in [&gids, &nexts, &datas] {
            if values.length() != len {
                return Err(JsValue::from_str("Expected equal column lengths"));
            }
        }
        for numeric_len in [
            entry_types.length(),
            wall_times.length(),
            logicals.length(),
            payload_sizes.length(),
            heads.length(),
        ] {
            if numeric_len != len {
                return Err(JsValue::from_str("Expected equal column lengths"));
            }
        }

        let mut entries = Vec::with_capacity(len as usize);
        for i in 0..len {
            entries.push(LogIndexEntry::new_with_data(
                required_string_from_array(&hashes, i)?,
                required_string_from_array(&gids, i)?,
                strings_from_array(required_array_from_array(&nexts, i)?)?,
                entry_types.get_index(i),
                wall_times.get_index(i),
                logicals.get_index(i),
                payload_sizes.get_index(i),
                heads.get_index(i) != 0,
                optional_bytes_from_js(datas.get(i)),
            ));
        }
        self.inner.put_many(entries);
        Ok(())
    }

    pub fn put_append_chain(
        &mut self,
        hashes: Array,
        gid: String,
        initial_next: Array,
        entry_type: u8,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        payload_sizes: Uint32Array,
        datas: Array,
    ) -> Result<(), JsValue> {
        let len = hashes.length();
        if datas.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
        for numeric_len in [
            wall_times.length(),
            logicals.length(),
            payload_sizes.length(),
        ] {
            if numeric_len != len {
                return Err(JsValue::from_str("Expected equal column lengths"));
            }
        }

        let initial_nexts = strings_from_array(initial_next)?;
        let mut next = initial_nexts.clone();
        let mut entries = Vec::with_capacity(len as usize);
        for i in 0..len {
            let hash = required_string_from_array(&hashes, i)?;
            entries.push(LogIndexEntry::new_with_data(
                hash.clone(),
                gid.clone(),
                next.clone(),
                entry_type,
                wall_times.get_index(i),
                logicals.get_index(i),
                payload_sizes.get_index(i),
                i + 1 == len,
                optional_bytes_from_js(datas.get(i)),
            ));
            next = vec![hash];
        }
        self.inner.put_append_chain(entries, &initial_nexts);
        Ok(())
    }

    pub fn prepare_entry_v0_plain_chain_and_put(
        &mut self,
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gid: String,
        initial_next: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
    ) -> Result<Array, JsValue> {
        let (rows, entries, initial_nexts, _blocks) = prepare_entry_v0_plain_chain_rows(
            clock_id,
            private_key,
            public_key,
            wall_times,
            logicals,
            gid,
            initial_next,
            entry_type,
            meta_datas,
            payload_datas,
            true,
        )?;
        self.inner.put_append_chain(entries, &initial_nexts);
        Ok(rows)
    }

    pub fn prepare_entry_v0_plain_entry_and_put(
        &mut self,
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) = prepare_entry_v0_plain_entry_row(
            clock_id,
            private_key,
            public_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            true,
        )?;
        self.inner.put_append_chain(vec![entry], &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) = prepare_entry_v0_plain_entry_row_with_signer(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            true,
        )?;
        self.inner.put_append_chain(vec![entry], &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_chain_commit_blocks_and_put(
        &mut self,
        block_store: &mut NativeLogBlockStore,
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gid: String,
        initial_next: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
    ) -> Result<Array, JsValue> {
        let (rows, entries, initial_nexts, blocks) = prepare_entry_v0_plain_chain_rows(
            clock_id,
            private_key,
            public_key,
            wall_times,
            logicals,
            gid,
            initial_next,
            entry_type,
            meta_datas,
            payload_datas,
            false,
        )?;
        block_store.put_entries(blocks);
        self.inner.put_append_chain(entries, &initial_nexts);
        Ok(rows)
    }

    pub fn prepare_entry_v0_plain_entry_commit_block_and_put(
        &mut self,
        block_store: &mut NativeLogBlockStore,
        clock_id: Uint8Array,
        private_key: Uint8Array,
        public_key: Uint8Array,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) = prepare_entry_v0_plain_entry_row(
            clock_id,
            private_key,
            public_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            false,
        )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_chain(vec![entry], &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_commit_block_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) = prepare_entry_v0_plain_entry_row_with_signer(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            payload_data,
            false,
        )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_chain(vec![entry], &initial_nexts);
        Ok(row)
    }

    pub fn delete(&mut self, hash: &str) -> bool {
        self.inner.delete(hash).is_some()
    }

    pub fn heads(&self, gid: Option<String>) -> Array {
        strings_to_array(self.inner.heads(gid.as_deref()))
    }

    pub fn has_head(&self, gid: Option<String>) -> bool {
        self.inner.has_head(gid.as_deref())
    }

    pub fn has_any_head(&self, gids: Array) -> Result<bool, JsValue> {
        let gids = strings_from_array(gids)?;
        Ok(self.inner.has_any_head(&gids))
    }

    pub fn has_any_head_batch(&self, gid_sets: Array) -> Result<Array, JsValue> {
        let gid_sets = string_arrays_from_array(gid_sets)?;
        let out = Array::new();
        for value in self.inner.has_any_head_batch(&gid_sets) {
            out.push(&JsValue::from_bool(value));
        }
        Ok(out)
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

    pub fn unique_reference_gids(&self, hash: &str) -> JsValue {
        self.inner
            .unique_reference_gids(hash)
            .map(|gids| strings_to_array(gids).into())
            .unwrap_or(JsValue::UNDEFINED)
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

#[wasm_bindgen]
pub fn encode_entry_v0_signable(
    clock_id: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
) -> Result<Uint8Array, JsValue> {
    let next = strings_from_array(next)?;
    let bytes = encode_entry_v0(
        EntryV0EncodeInput {
            clock_id: clock_id.to_vec(),
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data: optional_bytes_from_js(meta_data),
            payload_data: payload_data.to_vec(),
        },
        None,
    );
    Ok(Uint8Array::from(bytes.as_slice()))
}

#[wasm_bindgen]
pub fn sign_ed25519(
    private_key: Uint8Array,
    public_key: Uint8Array,
    data: Uint8Array,
) -> Result<Uint8Array, JsValue> {
    let signature = sign_ed25519_raw(&private_key.to_vec(), &public_key.to_vec(), &data.to_vec())?;
    Ok(Uint8Array::from(signature.as_slice()))
}

#[wasm_bindgen]
pub fn encode_entry_v0_storage(
    clock_id: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    signature: Uint8Array,
    signature_public_key: Uint8Array,
    prehash: u8,
) -> Result<Uint8Array, JsValue> {
    let bytes = encode_entry_v0_storage_vec(
        clock_id,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        signature,
        signature_public_key,
        prehash,
    )?;
    Ok(Uint8Array::from(bytes.as_slice()))
}

#[wasm_bindgen]
pub fn encode_entry_v0_storage_with_cid(
    clock_id: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    signature: Uint8Array,
    signature_public_key: Uint8Array,
    prehash: u8,
) -> Result<Array, JsValue> {
    let bytes = encode_entry_v0_storage_vec(
        clock_id,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        signature,
        signature_public_key,
        prehash,
    )?;
    Ok(storage_with_cid_to_row(bytes))
}

#[wasm_bindgen]
pub fn encode_entry_v0_signable_batch(
    clock_ids: Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Array,
    entry_types: Uint8Array,
    meta_datas: Array,
    payload_datas: Array,
) -> Result<Array, JsValue> {
    let len = clock_ids.length();
    validate_entry_batch_lengths(
        len,
        &gids,
        &nexts,
        &meta_datas,
        &payload_datas,
        &wall_times,
        &logicals,
        &entry_types,
    )?;

    let out = Array::new();
    for i in 0..len {
        let input = entry_input_from_batch(
            i,
            &clock_ids,
            &wall_times,
            &logicals,
            &gids,
            &nexts,
            &entry_types,
            &meta_datas,
            &payload_datas,
        )?;
        let bytes = encode_entry_v0(input, None);
        out.push(&Uint8Array::from(bytes.as_slice()));
    }
    Ok(out)
}

#[wasm_bindgen]
pub fn encode_entry_v0_storage_batch_with_cids(
    clock_ids: Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Array,
    entry_types: Uint8Array,
    meta_datas: Array,
    payload_datas: Array,
    signatures: Array,
    signature_public_keys: Array,
    prehashes: Uint8Array,
) -> Result<Array, JsValue> {
    let len = clock_ids.length();
    validate_entry_batch_lengths(
        len,
        &gids,
        &nexts,
        &meta_datas,
        &payload_datas,
        &wall_times,
        &logicals,
        &entry_types,
    )?;
    for values in [&signatures, &signature_public_keys] {
        if values.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }
    if prehashes.length() != len {
        return Err(JsValue::from_str("Expected equal column lengths"));
    }

    let out = Array::new();
    for i in 0..len {
        let input = entry_input_from_batch(
            i,
            &clock_ids,
            &wall_times,
            &logicals,
            &gids,
            &nexts,
            &entry_types,
            &meta_datas,
            &payload_datas,
        )?;
        let signature = required_bytes_from_array(&signatures, i, "signature")?;
        let public_key = required_bytes_from_array(&signature_public_keys, i, "public key")?;
        validate_signature_lengths(&signature, &public_key)?;
        let bytes = encode_entry_v0(
            input,
            Some(SignatureInput {
                signature,
                public_key,
                prehash: prehashes.get_index(i),
            }),
        );
        out.push(&storage_with_cid_to_row(bytes));
    }
    Ok(out)
}

fn prepare_entry_v0_plain_chain_rows(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gid: String,
    initial_next: Array,
    entry_type: u8,
    meta_datas: Array,
    payload_datas: Array,
    include_storage_bytes: bool,
) -> Result<
    (
        Array,
        Vec<LogIndexEntry>,
        Vec<String>,
        Vec<(String, Vec<u8>)>,
    ),
    JsValue,
> {
    let len = payload_datas.length();
    if meta_datas.length() != len || wall_times.length() != len || logicals.length() != len {
        return Err(JsValue::from_str("Expected equal column lengths"));
    }

    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;

    let initial_nexts = strings_from_array(initial_next)?;
    let mut next = initial_nexts.clone();
    let out = Array::new();
    let mut entries = Vec::with_capacity(len as usize);
    let mut blocks = Vec::with_capacity(len as usize);
    for i in 0..len {
        let payload_data = required_bytes_from_array(&payload_datas, i, "payload")?;
        let input = EntryV0EncodeInput {
            clock_id: clock_id.clone(),
            wall_time: wall_times.get_index(i),
            logical: logicals.get_index(i),
            gid: gid.clone(),
            next: next.clone(),
            entry_type,
            meta_data: optional_bytes_from_js(meta_datas.get(i)),
            payload_data,
        };
        let meta = encode_meta(&input);
        let payload = encode_payload(&input.payload_data);
        let signable = encode_entry_v0_parts(&meta, &payload, None);
        let signature = sign_ed25519_with_key(&signing_key, &signable);
        let signature_input = SignatureInput {
            signature: signature.clone(),
            public_key: public_key.clone(),
            prehash: 0,
        };
        let signature_with_key = encode_signature_with_key(&signature_input);
        let storage = encode_entry_v0_parts(&meta, &payload, Some(signature_input));
        let storage_len = storage.len();
        let (cid, hash_digest) = calculate_raw_cid_v1_parts(&storage);

        let row = Array::new();
        if include_storage_bytes {
            row.push(&Uint8Array::from(storage.as_slice()));
            row.push(&JsValue::from_str(&cid));
            row.push(&Uint8Array::from(signature.as_slice()));
            row.push(&strings_to_array(next.clone()));
            row.push(&Uint8Array::from(meta.as_slice()));
            row.push(&Uint8Array::from(payload.as_slice()));
            row.push(&Uint8Array::from(signature_with_key.as_slice()));
            row.push(&Uint8Array::from(hash_digest.as_slice()));
        } else {
            row.push(&JsValue::from_str(&cid));
            row.push(&Uint8Array::from(signature.as_slice()));
            row.push(&strings_to_array(next.clone()));
            row.push(&Uint8Array::from(meta.as_slice()));
            row.push(&Uint8Array::from(payload.as_slice()));
            row.push(&Uint8Array::from(signature_with_key.as_slice()));
            row.push(&JsValue::from_f64(storage_len as f64));
            row.push(&Uint8Array::from(hash_digest.as_slice()));
        }
        out.push(&row);
        entries.push(LogIndexEntry::new_with_data(
            cid.clone(),
            gid.clone(),
            next.clone(),
            entry_type,
            input.wall_time,
            input.logical,
            input.payload_data.len() as u32,
            i + 1 == len,
            input.meta_data.clone(),
        ));
        blocks.push((cid.clone(), storage));

        next = vec![cid];
    }
    Ok((out, entries, initial_nexts, blocks))
}

fn prepare_entry_v0_plain_entry_row(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    include_storage_bytes: bool,
) -> Result<(Array, LogIndexEntry, Vec<String>, (String, Vec<u8>)), JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    prepare_entry_v0_plain_entry_row_with_signer(
        &clock_id,
        &public_key,
        &signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        include_storage_bytes,
    )
}

fn prepare_entry_v0_plain_entry_row_with_signer(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    include_storage_bytes: bool,
) -> Result<(Array, LogIndexEntry, Vec<String>, (String, Vec<u8>)), JsValue> {
    let next = strings_from_array(next)?;
    let payload_data = payload_data.to_vec();
    let meta_data = optional_bytes_from_js(meta_data);
    let payload_size = payload_data.len() as u32;

    let input = EntryV0EncodeInput {
        clock_id: clock_id.to_vec(),
        wall_time,
        logical,
        gid: gid.clone(),
        next: next.clone(),
        entry_type,
        meta_data,
        payload_data,
    };
    let meta = encode_meta(&input);
    let payload = encode_payload(&input.payload_data);
    let signable = encode_entry_v0_parts(&meta, &payload, None);
    let signature = sign_ed25519_with_key(&signing_key, &signable);
    let signature_input = SignatureInput {
        signature: signature.clone(),
        public_key: public_key.to_vec(),
        prehash: 0,
    };
    let signature_with_key = encode_signature_with_key(&signature_input);
    let storage = encode_entry_v0_parts(&meta, &payload, Some(signature_input));
    let storage_len = storage.len();
    let (cid, hash_digest) = calculate_raw_cid_v1_parts(&storage);

    let row = Array::new();
    if include_storage_bytes {
        row.push(&Uint8Array::from(storage.as_slice()));
        row.push(&JsValue::from_str(&cid));
        row.push(&Uint8Array::from(signature.as_slice()));
        row.push(&strings_to_array(next.clone()));
        row.push(&Uint8Array::from(meta.as_slice()));
        row.push(&Uint8Array::from(payload.as_slice()));
        row.push(&Uint8Array::from(signature_with_key.as_slice()));
        row.push(&Uint8Array::from(hash_digest.as_slice()));
    } else {
        row.push(&JsValue::from_str(&cid));
        row.push(&Uint8Array::from(signature.as_slice()));
        row.push(&strings_to_array(next.clone()));
        row.push(&Uint8Array::from(meta.as_slice()));
        row.push(&Uint8Array::from(payload.as_slice()));
        row.push(&Uint8Array::from(signature_with_key.as_slice()));
        row.push(&JsValue::from_f64(storage_len as f64));
        row.push(&Uint8Array::from(hash_digest.as_slice()));
    }

    let entry = LogIndexEntry::new_with_data(
        cid.clone(),
        gid,
        next.clone(),
        entry_type,
        input.wall_time,
        input.logical,
        payload_size,
        true,
        input.meta_data.clone(),
    );
    Ok((row, entry, next, (cid, storage)))
}

#[wasm_bindgen]
pub fn prepare_entry_v0_plain_chain(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gid: String,
    initial_next: Array,
    entry_type: u8,
    meta_datas: Array,
    payload_datas: Array,
) -> Result<Array, JsValue> {
    Ok(prepare_entry_v0_plain_chain_rows(
        clock_id,
        private_key,
        public_key,
        wall_times,
        logicals,
        gid,
        initial_next,
        entry_type,
        meta_datas,
        payload_datas,
        true,
    )?
    .0)
}

#[wasm_bindgen]
pub fn prepare_entry_v0_plain_entry(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let (row, _entry, _initial_nexts, _block) = prepare_entry_v0_plain_entry_row(
        clock_id,
        private_key,
        public_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        true,
    )?;
    Ok(row)
}

#[wasm_bindgen]
pub fn calculate_raw_cid_v1(bytes: Uint8Array) -> String {
    calculate_raw_cid_v1_from_bytes(&bytes.to_vec())
}

fn calculate_raw_cid_v1_from_bytes(bytes: &[u8]) -> String {
    calculate_raw_cid_v1_parts(bytes).0
}

fn calculate_raw_cid_v1_parts(bytes: &[u8]) -> (String, [u8; 32]) {
    let digest = Sha256::digest(bytes);
    let digest_bytes: [u8; 32] = digest.into();
    let mut cid = Vec::with_capacity(36);
    cid.push(0x01); // CIDv1
    cid.push(0x55); // raw codec
    cid.push(0x12); // sha2-256 multihash code
    cid.push(0x20); // 32 byte digest
    cid.extend_from_slice(&digest_bytes);
    (
        format!("z{}", bs58::encode(cid).into_string()),
        digest_bytes,
    )
}

fn encode_entry_v0_storage_vec(
    clock_id: Uint8Array,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Array,
    entry_type: u8,
    meta_data: JsValue,
    payload_data: Uint8Array,
    signature: Uint8Array,
    signature_public_key: Uint8Array,
    prehash: u8,
) -> Result<Vec<u8>, JsValue> {
    let signature = signature.to_vec();
    let public_key = signature_public_key.to_vec();
    validate_signature_lengths(&signature, &public_key)?;
    let next = strings_from_array(next)?;
    Ok(encode_entry_v0(
        EntryV0EncodeInput {
            clock_id: clock_id.to_vec(),
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data: optional_bytes_from_js(meta_data),
            payload_data: payload_data.to_vec(),
        },
        Some(SignatureInput {
            signature,
            public_key,
            prehash,
        }),
    ))
}

fn validate_signature_lengths(signature: &[u8], public_key: &[u8]) -> Result<(), JsValue> {
    if signature.len() != 64 {
        return Err(JsValue::from_str("Expected Ed25519 signature length 64"));
    }
    if public_key.len() != 32 {
        return Err(JsValue::from_str("Expected Ed25519 public key length 32"));
    }
    Ok(())
}

fn storage_with_cid_to_row(bytes: Vec<u8>) -> Array {
    let row = Array::new();
    row.push(&Uint8Array::from(bytes.as_slice()));
    row.push(&JsValue::from_str(&calculate_raw_cid_v1_from_bytes(&bytes)));
    row
}

struct EntryV0EncodeInput {
    clock_id: Vec<u8>,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
    payload_data: Vec<u8>,
}

struct SignatureInput {
    signature: Vec<u8>,
    public_key: Vec<u8>,
    prehash: u8,
}

fn encode_entry_v0(input: EntryV0EncodeInput, signature: Option<SignatureInput>) -> Vec<u8> {
    let meta = encode_meta(&input);
    let payload = encode_payload(&input.payload_data);
    encode_entry_v0_parts(&meta, &payload, signature)
}

fn encode_entry_v0_parts(
    meta: &[u8],
    payload: &[u8],
    signature: Option<SignatureInput>,
) -> Vec<u8> {
    let mut out = Vec::new();
    write_u8(&mut out, 0); // EntryV0 variant
    write_decrypted_thing(&mut out, meta);
    write_decrypted_thing(&mut out, payload);
    out.extend_from_slice(&[0, 0, 0, 0]); // reserved
    match signature {
        Some(signature) => {
            write_u8(&mut out, 1);
            write_signatures(&mut out, signature);
        }
        None => write_u8(&mut out, 0),
    }
    write_u8(&mut out, 0); // hash option
    out
}

fn encode_meta(input: &EntryV0EncodeInput) -> Vec<u8> {
    let mut out = Vec::new();
    write_u8(&mut out, 0); // Meta variant
    write_clock(&mut out, &input.clock_id, input.wall_time, input.logical);
    write_string(&mut out, &input.gid);
    write_u32(&mut out, input.next.len() as u32);
    for next in &input.next {
        write_string(&mut out, next);
    }
    write_u8(&mut out, input.entry_type);
    match input.meta_data.as_ref() {
        Some(data) => {
            write_u8(&mut out, 1);
            write_bytes(&mut out, data);
        }
        None => write_u8(&mut out, 0),
    }
    out
}

fn write_clock(out: &mut Vec<u8>, clock_id: &[u8], wall_time: u64, logical: u32) {
    write_u8(out, 0); // LamportClock variant
    write_bytes(out, clock_id);
    write_u8(out, 0); // Timestamp variant
    write_u64(out, wall_time);
    write_u32(out, logical);
}

fn encode_payload(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    write_u8(&mut out, 0); // Payload variant
    write_bytes(&mut out, data);
    out
}

fn write_signatures(out: &mut Vec<u8>, signature: SignatureInput) {
    write_u8(out, 0); // Signatures variant
    write_u32(out, 1);
    let signature_with_key = encode_signature_with_key(&signature);
    write_decrypted_thing(out, &signature_with_key);
}

fn encode_signature_with_key(signature: &SignatureInput) -> Vec<u8> {
    let mut out = Vec::new();
    write_u8(&mut out, 0); // SignatureWithKey variant
    write_bytes(&mut out, &signature.signature);
    write_u8(&mut out, 0); // Ed25519PublicKey variant
    out.extend_from_slice(&signature.public_key);
    write_u8(&mut out, signature.prehash);
    out
}

fn sign_ed25519_raw(
    private_key: &[u8],
    public_key: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, JsValue> {
    let signing_key = validate_ed25519_keypair(private_key, public_key)?;
    Ok(sign_ed25519_with_key(&signing_key, data))
}

fn sign_ed25519_with_key(signing_key: &SigningKey, data: &[u8]) -> Vec<u8> {
    signing_key.sign(data).to_bytes().to_vec()
}

fn validate_ed25519_keypair(private_key: &[u8], public_key: &[u8]) -> Result<SigningKey, JsValue> {
    if private_key.len() != 32 {
        return Err(JsValue::from_str("Expected Ed25519 private key length 32"));
    }
    if public_key.len() != 32 {
        return Err(JsValue::from_str("Expected Ed25519 public key length 32"));
    }
    let signing_key = SigningKey::from_bytes(
        private_key
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 private key length 32"))?,
    );
    if signing_key.verifying_key().to_bytes().as_slice() != public_key {
        return Err(JsValue::from_str(
            "Ed25519 public key does not match private key",
        ));
    }
    Ok(signing_key)
}

fn write_decrypted_thing(out: &mut Vec<u8>, data: &[u8]) {
    write_u8(out, 0); // MaybeEncrypted variant
    write_u8(out, 0); // DecryptedThing variant
    write_bytes(out, data);
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    write_bytes(out, value.as_bytes());
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    write_u32(out, value.len() as u32);
    out.extend_from_slice(value);
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
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

fn string_arrays_from_array(values: Array) -> Result<Vec<Vec<String>>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for value in values.iter() {
        if !Array::is_array(&value) {
            return Err(JsValue::from_str("Expected string array array"));
        }
        out.push(strings_from_array(Array::from(&value))?);
    }
    Ok(out)
}

fn block_key_values_from_arrays(
    keys: &Array,
    values: &Array,
) -> Result<Vec<(String, Vec<u8>)>, JsValue> {
    if keys.length() != values.length() {
        return Err(JsValue::from_str("Expected equal column lengths"));
    }
    let mut entries = Vec::with_capacity(keys.length() as usize);
    for index in 0..keys.length() {
        entries.push((
            required_string_from_array(keys, index)?,
            required_bytes_from_array(values, index, "block")?,
        ));
    }
    Ok(entries)
}

#[allow(clippy::too_many_arguments)]
fn validate_entry_batch_lengths(
    len: u32,
    gids: &Array,
    nexts: &Array,
    meta_datas: &Array,
    payload_datas: &Array,
    wall_times: &BigUint64Array,
    logicals: &Uint32Array,
    entry_types: &Uint8Array,
) -> Result<(), JsValue> {
    for values in [gids, nexts, meta_datas, payload_datas] {
        if values.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }
    for numeric_len in [wall_times.length(), logicals.length(), entry_types.length()] {
        if numeric_len != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn entry_input_from_batch(
    index: u32,
    clock_ids: &Array,
    wall_times: &BigUint64Array,
    logicals: &Uint32Array,
    gids: &Array,
    nexts: &Array,
    entry_types: &Uint8Array,
    meta_datas: &Array,
    payload_datas: &Array,
) -> Result<EntryV0EncodeInput, JsValue> {
    Ok(EntryV0EncodeInput {
        clock_id: required_bytes_from_array(clock_ids, index, "clock id")?,
        wall_time: wall_times.get_index(index),
        logical: logicals.get_index(index),
        gid: required_string_from_array(gids, index)?,
        next: strings_from_array(required_array_from_array(nexts, index)?)?,
        entry_type: entry_types.get_index(index),
        meta_data: optional_bytes_from_js(meta_datas.get(index)),
        payload_data: required_bytes_from_array(payload_datas, index, "payload")?,
    })
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

fn required_string_from_array(values: &Array, index: u32) -> Result<String, JsValue> {
    values
        .get(index)
        .as_string()
        .ok_or_else(|| JsValue::from_str("Expected string field"))
}

fn required_bytes_from_array(values: &Array, index: u32, field: &str) -> Result<Vec<u8>, JsValue> {
    let value = values.get(index);
    if value.is_undefined() || value.is_null() {
        return Err(JsValue::from_str(&format!("Expected {field} bytes")));
    }
    Ok(Uint8Array::new(&value).to_vec())
}

fn required_array_from_array(values: &Array, index: u32) -> Result<Array, JsValue> {
    let value = values.get(index);
    if !Array::is_array(&value) {
        return Err(JsValue::from_str("Expected array field"));
    }
    Ok(Array::from(&value))
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
    fn puts_append_chain_without_promoting_internal_heads() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put_append_chain(
            vec![
                LogIndexEntry::new("a", "g", vec!["root".to_string()], APPEND, 2, 0, 1, false),
                LogIndexEntry::new("b", "g", vec!["a".to_string()], APPEND, 3, 0, 1, false),
                LogIndexEntry::new("c", "g", vec!["b".to_string()], APPEND, 4, 0, 1, true),
            ],
            &["root".to_string()],
        );

        assert_eq!(index.heads(None), vec!["c"]);
        assert_eq!(index.children("root"), vec!["a"]);
        assert_eq!(index.children("a"), vec!["b"]);
        assert_eq!(index.children("b"), vec!["c"]);
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
        assert!(index.has_head(None));
        assert!(index.has_head(Some("one")));
        assert!(index.has_head(Some("two")));
        assert!(!index.has_head(Some("missing")));
        assert!(index.has_any_head(&["missing".to_string(), "two".to_string()]));
        assert!(!index.has_any_head(&["missing".to_string()]));
        assert_eq!(
            index.has_any_head_batch(&[
                vec!["missing".to_string(), "two".to_string()],
                vec!["missing".to_string()],
                Vec::new(),
            ]),
            vec![true, false, false],
        );
    }

    #[test]
    fn returns_oldest_and_newest_hash_by_clock_order() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new("b", "g", vec![], APPEND, 2, 0, 1, true));
        index.put(LogIndexEntry::new("a", "g", vec![], APPEND, 1, 1, 1, true));
        index.put(LogIndexEntry::new("c", "g", vec![], APPEND, 1, 0, 1, true));

        assert_eq!(index.oldest_hash(), Some("c".to_string()));
        assert_eq!(index.newest_hash(), Some("b".to_string()));

        index.delete("c");
        assert_eq!(index.oldest_hash(), Some("a".to_string()));
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
