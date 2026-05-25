#[cfg(feature = "crypto-bench-candidates")]
use ed25519_compact::{
    PublicKey as CompactPublicKey, SecretKey as CompactSecretKey, Signature as CompactSignature,
};
use ed25519_dalek::{verify_batch, Signature, Signer, SigningKey, Verifier, VerifyingKey};
use indexmap::{IndexMap, IndexSet};
use js_sys::{Array, BigUint64Array, Uint32Array, Uint8Array};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use wasm_bindgen::prelude::*;

const ENTRY_TYPE_CUT: u8 = 1;
const SIGNED_ENTRY_EXTRA_CAPACITY: usize = 128;

enum PreparedPlainEntryRowMode {
    Full { include_storage_bytes: bool },
    StorageOnly,
    StorageWithFacts,
    CommitFactsOnly,
    CommitFactsNoNext,
}

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

#[derive(Clone, Debug)]
pub struct PreparedRawEntryV0 {
    pub cid: String,
    pub hash_digest_bytes: Vec<u8>,
    pub byte_length: usize,
    pub clock_id: Vec<u8>,
    pub wall_time: u64,
    pub logical: u32,
    pub gid: String,
    pub next: Vec<String>,
    pub entry_type: u8,
    pub meta_bytes: Vec<u8>,
    pub meta_data: Option<Vec<u8>>,
    pub payload_byte_length: usize,
    pub signature_verified: bool,
    pub storage_bytes: Vec<u8>,
    pub requested_replicas: Option<u32>,
}

impl PreparedRawEntryV0 {
    pub fn log_index_entry(&self, head: bool) -> Result<LogIndexEntry, JsValue> {
        Ok(LogIndexEntry::new_with_data(
            self.cid.clone(),
            self.gid.clone(),
            self.next.clone(),
            self.entry_type,
            self.wall_time,
            self.logical,
            self.payload_byte_length
                .try_into()
                .map_err(|_| JsValue::from_str("Payload byte length exceeds u32"))?,
            head,
            self.meta_data.clone(),
        ))
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
    ordered_entries: BTreeSet<(u64, u32, String)>,
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
        self.ordered_entries.clear();
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
        self.ordered_entries
            .iter()
            .next()
            .map(|(_, _, hash)| hash.clone())
    }

    pub fn newest_hash(&self) -> Option<String> {
        self.ordered_entries
            .iter()
            .next_back()
            .map(|(_, _, hash)| hash.clone())
    }

    pub fn oldest_entries(&self, limit: usize) -> Vec<LogIndexEntry> {
        if limit == 0 {
            return Vec::new();
        }
        self.ordered_entries
            .iter()
            .take(limit)
            .filter_map(|(_, _, hash)| self.entries.get(hash).cloned())
            .collect()
    }

    pub fn oldest_hashes(&self, limit: usize) -> Vec<String> {
        if limit == 0 {
            return Vec::new();
        }
        self.ordered_entries
            .iter()
            .take(limit)
            .map(|(_, _, hash)| hash.clone())
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
        let head = entry.head;
        let wall_time = entry.wall_time;
        let logical = entry.logical;

        self.entries.insert(hash.clone(), entry);
        self.ordered_entries
            .insert((wall_time, logical, hash.clone()));
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

    pub fn put_join_batch(&mut self, entries: Vec<LogIndexEntry>) {
        if entries.is_empty() {
            return;
        }

        let external_nexts = {
            let batch_hashes: HashSet<&str> =
                entries.iter().map(|entry| entry.hash.as_str()).collect();
            let mut external_nexts = IndexSet::new();
            for entry in &entries {
                if entry.entry_type == ENTRY_TYPE_CUT {
                    continue;
                }
                for next in &entry.next {
                    if !batch_hashes.contains(next.as_str()) {
                        external_nexts.insert(next.clone());
                    }
                }
            }
            external_nexts
        };

        self.entries.reserve(entries.len());
        self.heads.reserve(entries.len());
        self.children.reserve(entries.len());
        for entry in entries {
            let hash = entry.hash.clone();
            let nexts = entry.next.clone();
            let payload_size = entry.payload_size as u64;
            let head = entry.head;
            let wall_time = entry.wall_time;
            let logical = entry.logical;

            if self.entries.contains_key(&hash) {
                self.delete(&hash);
            }
            self.entries.insert(hash.clone(), entry);
            self.ordered_entries
                .insert((wall_time, logical, hash.clone()));
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

        for next in external_nexts {
            self.set_head(&next, false);
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
            let wall_time = entry.wall_time;
            let logical = entry.logical;

            if self.entries.contains_key(&hash) {
                self.delete(&hash);
            }
            self.entries.insert(hash.clone(), entry);
            self.ordered_entries
                .insert((wall_time, logical, hash.clone()));
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

    pub fn put_append_entry(&mut self, entry: LogIndexEntry, initial_next: &[String]) {
        let demotes_initial_nexts = entry.entry_type != ENTRY_TYPE_CUT;
        let hash = entry.hash.clone();
        let nexts = entry.next.clone();
        let payload_size = entry.payload_size as u64;
        let head = entry.head;
        let wall_time = entry.wall_time;
        let logical = entry.logical;

        if self.entries.contains_key(&hash) {
            self.delete(&hash);
        }
        self.entries.insert(hash.clone(), entry);
        self.ordered_entries
            .insert((wall_time, logical, hash.clone()));
        self.payload_size_total += payload_size;
        if head {
            self.heads.insert(hash.clone());
        } else {
            self.heads.shift_remove(&hash);
        }

        for next in nexts {
            self.children.entry(next).or_default().insert(hash.clone());
        }

        if demotes_initial_nexts {
            for next in initial_next {
                self.set_head(next, false);
            }
        }
    }

    pub fn put_no_next(&mut self, entry: LogIndexEntry) {
        debug_assert!(entry.next.is_empty());
        let hash = entry.hash.clone();
        let payload_size = entry.payload_size as u64;
        let head = entry.head;
        let wall_time = entry.wall_time;
        let logical = entry.logical;

        if self.entries.contains_key(&hash) {
            self.delete(&hash);
        }
        self.entries.insert(hash.clone(), entry);
        self.ordered_entries
            .insert((wall_time, logical, hash.clone()));
        self.payload_size_total += payload_size;
        if head {
            self.heads.insert(hash.clone());
        } else {
            self.heads.shift_remove(&hash);
        }
    }

    pub fn delete(&mut self, hash: &str) -> Option<LogIndexEntry> {
        let entry = self.entries.shift_remove(hash)?;
        self.heads.shift_remove(hash);
        self.ordered_entries
            .remove(&(entry.wall_time, entry.logical, entry.hash.clone()));
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

    pub fn delete_many(&mut self, hashes: &[String]) -> usize {
        let mut deleted = 0;
        for hash in hashes {
            if self.delete(hash).is_some() {
                deleted += 1;
            }
        }
        deleted
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

    pub fn max_head_data_u32_batch(&self, gids: &[String]) -> Vec<Option<u32>> {
        if gids.is_empty() {
            return Vec::new();
        }

        let requested: HashSet<&str> = gids.iter().map(String::as_str).collect();
        let mut max_by_gid: HashMap<&str, u32> = HashMap::with_capacity(requested.len());

        for hash in &self.heads {
            let Some(entry) = self.entries.get(hash) else {
                continue;
            };
            let gid = entry.gid.as_str();
            if !requested.contains(gid) {
                continue;
            }
            let Some(value) = decode_absolute_replica_data_u32(entry.data.as_deref()) else {
                continue;
            };
            max_by_gid
                .entry(gid)
                .and_modify(|current| *current = (*current).max(value))
                .or_insert(value);
        }

        gids.iter()
            .map(|gid| max_by_gid.get(gid.as_str()).copied())
            .collect()
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

    pub fn entry_metadata_batch(&self, hashes: &[String]) -> Vec<Option<LogEntryMetadata>> {
        hashes
            .iter()
            .map(|hash| {
                self.entries.get(hash).map(|entry| {
                    let replicas = decode_absolute_replica_data_u32(entry.data.as_deref());
                    (
                        entry.hash.clone(),
                        entry.gid.clone(),
                        entry.data.clone(),
                        replicas,
                    )
                })
            })
            .collect()
    }

    pub fn entry_prune_metadata_batch(
        &self,
        hashes: &[String],
    ) -> Vec<Option<LogEntryPruneMetadata>> {
        hashes
            .iter()
            .map(|hash| {
                self.entries.get(hash).map(|entry| {
                    let replicas = decode_absolute_replica_data_u32(entry.data.as_deref());
                    let data = if replicas.is_none() {
                        entry.data.clone()
                    } else {
                        None
                    };
                    (entry.gid.clone(), data, replicas)
                })
            })
            .collect()
    }

    pub fn entry_prune_confirm_metadata_batch(
        &self,
        hashes: &[String],
    ) -> Vec<Option<LogEntryPruneConfirmMetadata>> {
        hashes
            .iter()
            .map(|hash| {
                self.entries.get(hash).map(|entry| {
                    (
                        entry.gid.clone(),
                        decode_absolute_replica_data_u32(entry.data.as_deref()),
                    )
                })
            })
            .collect()
    }

    pub fn entry_prune_confirm_metadata_ref(&self, hash: &str) -> Option<(&str, Option<u32>)> {
        self.entries.get(hash).map(|entry| {
            (
                entry.gid.as_str(),
                decode_absolute_replica_data_u32(entry.data.as_deref()),
            )
        })
    }

    pub fn unique_reference_gid_rows(&self, hash: &str) -> Option<Vec<(String, String)>> {
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
            out.push((next_hash, next_entry.gid.clone()));
            if next_entry.entry_type == ENTRY_TYPE_CUT {
                continue;
            }
            queue.extend(next_entry.next.iter().cloned());
        }

        Some(out)
    }

    pub fn unique_reference_gids(&self, hash: &str) -> Option<Vec<String>> {
        self.unique_reference_gid_rows(hash)
            .map(|rows| rows.into_iter().map(|(_, gid)| gid).collect())
    }

    pub fn unique_reference_gid_rows_batch(
        &self,
        hashes: &[String],
    ) -> Vec<Option<Vec<(String, String)>>> {
        hashes
            .iter()
            .map(|hash| self.unique_reference_gid_rows(hash))
            .collect()
    }

    pub fn unique_reference_gid_rows_flat_batch(
        &self,
        hashes: &[String],
    ) -> Option<Vec<(u32, String, String)>> {
        let mut out = Vec::new();
        for (position, hash) in hashes.iter().enumerate() {
            let rows = self.unique_reference_gid_rows(hash)?;
            for (reference_hash, gid) in rows {
                out.push((position as u32, reference_hash, gid));
            }
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

    pub fn plan_join_batch(
        &self,
        hashes: &[String],
        nexts: &[Vec<String>],
        entry_types: &[u8],
        reset: bool,
        cut_checks: Option<(&[String], &[u64], &[u32])>,
    ) -> Vec<JoinPlan> {
        let cut_heads_by_gid = cut_checks.map(|_| {
            let mut by_gid: HashMap<&str, Vec<&LogIndexEntry>> = HashMap::new();
            for hash in &self.heads {
                if let Some(entry) = self.entries.get(hash) {
                    if entry.entry_type == ENTRY_TYPE_CUT {
                        by_gid.entry(entry.gid.as_str()).or_default().push(entry);
                    }
                }
            }
            by_gid
        });

        let mut plans = Vec::with_capacity(hashes.len());
        for i in 0..hashes.len() {
            let hash = &hashes[i];
            let current_nexts = &nexts[i];
            let entry_type = entry_types[i];
            let cut_check = cut_checks
                .map(|(gids, wall_times, logicals)| (gids[i].as_str(), wall_times[i], logicals[i]));
            let cut_checked = cut_check.is_some();
            if !reset && self.has(hash) {
                plans.push(JoinPlan {
                    skip: true,
                    missing_parents: Vec::new(),
                    cut_checked,
                    covered_by_cut: false,
                });
                continue;
            }

            let covered_by_cut = match (cut_check, cut_heads_by_gid.as_ref()) {
                (Some((gid, wall_time, logical)), Some(cut_heads)) => cut_heads
                    .get(gid)
                    .map(|heads| {
                        heads.iter().any(|entry| {
                            entry.next.iter().any(|next| next == hash)
                                && compare_clock(wall_time, logical, entry).is_lt()
                        })
                    })
                    .unwrap_or(false),
                _ => false,
            };

            let missing_parents = if entry_type == ENTRY_TYPE_CUT || covered_by_cut {
                Vec::new()
            } else {
                current_nexts
                    .iter()
                    .filter(|next| reset || !self.has(next))
                    .cloned()
                    .collect()
            };

            plans.push(JoinPlan {
                skip: false,
                missing_parents,
                cut_checked,
                covered_by_cut,
            });
        }
        plans
    }

    pub fn plan_join_entry_refs(
        &self,
        entries: &[&LogIndexEntry],
        reset: bool,
        cut_check: bool,
    ) -> Vec<JoinPlan> {
        let cut_heads_by_gid = cut_check.then(|| {
            let mut by_gid: HashMap<&str, Vec<&LogIndexEntry>> = HashMap::new();
            for hash in &self.heads {
                if let Some(entry) = self.entries.get(hash) {
                    if entry.entry_type == ENTRY_TYPE_CUT {
                        by_gid.entry(entry.gid.as_str()).or_default().push(entry);
                    }
                }
            }
            by_gid
        });

        let mut plans = Vec::with_capacity(entries.len());
        for entry in entries {
            let cut_checked = cut_check;
            if !reset && self.has(&entry.hash) {
                plans.push(JoinPlan {
                    skip: true,
                    missing_parents: Vec::new(),
                    cut_checked,
                    covered_by_cut: false,
                });
                continue;
            }

            let covered_by_cut = if cut_check {
                cut_heads_by_gid
                    .as_ref()
                    .and_then(|cut_heads| cut_heads.get(entry.gid.as_str()))
                    .map(|heads| {
                        heads.iter().any(|cut_entry| {
                            cut_entry.next.iter().any(|next| next == &entry.hash)
                                && compare_clock(entry.wall_time, entry.logical, cut_entry).is_lt()
                        })
                    })
                    .unwrap_or(false)
            } else {
                false
            };

            let missing_parents = if entry.entry_type == ENTRY_TYPE_CUT || covered_by_cut {
                Vec::new()
            } else {
                entry
                    .next
                    .iter()
                    .filter(|next| reset || !self.has(next))
                    .cloned()
                    .collect()
            };

            plans.push(JoinPlan {
                skip: false,
                missing_parents,
                cut_checked,
                covered_by_cut,
            });
        }
        plans
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

#[wasm_bindgen]
pub struct NativeLogIndex {
    inner: LogGraphIndex,
}

#[wasm_bindgen]
pub struct NativeLogBlockStore {
    entries: HashMap<String, Vec<u8>>,
    total_size: u64,
}

#[wasm_bindgen]
pub struct NativeEntryV0PlainBuilder {
    clock_id: Vec<u8>,
    public_key: Vec<u8>,
    signing_key: SigningKey,
}

pub struct NativeCommittedEntryFacts {
    pub hash: String,
    pub next: Vec<String>,
    pub meta_bytes: Vec<u8>,
    pub byte_length: usize,
    pub hash_digest_bytes: Vec<u8>,
}

pub type LogEntryMetadata = (String, String, Option<Vec<u8>>, Option<u32>);
pub type LogEntryPruneMetadata = (String, Option<Vec<u8>>, Option<u32>);
pub type LogEntryPruneConfirmMetadata = (String, Option<u32>);

#[derive(Clone, Default)]
pub struct NativeLogAppendProfile {
    pub next_clone_ms: f64,
    pub entry_core_ms: f64,
    pub encode_meta_ms: f64,
    pub encode_payload_ms: f64,
    pub encode_signable_ms: f64,
    pub sign_ms: f64,
    pub encode_signature_ms: f64,
    pub encode_storage_ms: f64,
    pub cid_ms: f64,
    pub cid_hash_ms: f64,
    pub cid_string_ms: f64,
    pub index_entry_ms: f64,
    pub facts_ms: f64,
    pub block_put_ms: f64,
    pub graph_put_ms: f64,
    pub trim_ms: f64,
}

struct PreparedPlainEntryCore {
    hash: String,
    next: Vec<String>,
    meta_bytes: Vec<u8>,
    payload_bytes: Vec<u8>,
    signature_bytes: [u8; 64],
    signature_with_key_bytes: Vec<u8>,
    storage_bytes: Vec<u8>,
    hash_digest_bytes: Vec<u8>,
    entry: LogIndexEntry,
}

struct PreparedPlainEntryCommitCore {
    hash: String,
    next: Vec<String>,
    meta_bytes: Vec<u8>,
    storage_bytes: Vec<u8>,
    hash_digest_bytes: Vec<u8>,
    entry: LogIndexEntry,
}

#[wasm_bindgen]
impl NativeLogBlockStore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
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

    pub fn has_many(&self, keys: Array) -> Result<Array, JsValue> {
        let present = Array::new();
        for key in strings_from_array(keys)? {
            present.push(&JsValue::from_bool(self.entries.contains_key(&key)));
        }
        Ok(present)
    }

    pub fn put(&mut self, key: String, value: Vec<u8>) {
        self.put_entry(key, value);
    }

    pub fn put_many(&mut self, keys: Array, values: Array) -> Result<(), JsValue> {
        self.put_entries(block_key_values_from_arrays(&keys, &values)?);
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> bool {
        if let Some(previous) = self.entries.remove(key) {
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
        self.entries.reserve(entries.len());
        for (key, value) in entries {
            self.put_entry(key, value);
        }
    }

    pub fn put_entries_core(&mut self, entries: Vec<(String, Vec<u8>)>) {
        self.put_entries(entries);
    }
}

impl NativeLogIndex {
    pub fn max_head_data_u32_values(&self, gids: &[String]) -> Vec<Option<u32>> {
        self.inner.max_head_data_u32_batch(gids)
    }

    pub fn entry_metadata_values(&self, hashes: &[String]) -> Vec<Option<LogEntryMetadata>> {
        self.inner.entry_metadata_batch(hashes)
    }

    pub fn entry_prune_metadata_values(
        &self,
        hashes: &[String],
    ) -> Vec<Option<LogEntryPruneMetadata>> {
        self.inner.entry_prune_metadata_batch(hashes)
    }

    pub fn entry_prune_confirm_metadata_values(
        &self,
        hashes: &[String],
    ) -> Vec<Option<LogEntryPruneConfirmMetadata>> {
        self.inner.entry_prune_confirm_metadata_batch(hashes)
    }

    pub fn entry_prune_confirm_metadata_ref(&self, hash: &str) -> Option<(&str, Option<u32>)> {
        self.inner.entry_prune_confirm_metadata_ref(hash)
    }

    pub fn put_entries_core(&mut self, entries: Vec<LogIndexEntry>) {
        self.inner.put_many(entries);
    }

    pub fn put_join_batch_entries_core(&mut self, entries: Vec<LogIndexEntry>) {
        self.inner.put_join_batch(entries);
    }

    pub fn plan_join_entries_core(
        &self,
        entries: &[LogIndexEntry],
        reset: bool,
        cut_check: bool,
    ) -> Vec<JoinPlan> {
        let hashes = entries
            .iter()
            .map(|entry| entry.hash.clone())
            .collect::<Vec<_>>();
        let nexts = entries
            .iter()
            .map(|entry| entry.next.clone())
            .collect::<Vec<_>>();
        let entry_types = entries
            .iter()
            .map(|entry| entry.entry_type)
            .collect::<Vec<_>>();
        let cut_check_values = cut_check.then(|| {
            (
                entries
                    .iter()
                    .map(|entry| entry.gid.clone())
                    .collect::<Vec<_>>(),
                entries
                    .iter()
                    .map(|entry| entry.wall_time)
                    .collect::<Vec<_>>(),
                entries
                    .iter()
                    .map(|entry| entry.logical)
                    .collect::<Vec<_>>(),
            )
        });
        self.inner.plan_join_batch(
            &hashes,
            &nexts,
            &entry_types,
            reset,
            cut_check_values
                .as_ref()
                .map(|(gids, wall_times, logicals)| {
                    (gids.as_slice(), wall_times.as_slice(), logicals.as_slice())
                }),
        )
    }

    pub fn plan_join_entry_refs_core(
        &self,
        entries: &[&LogIndexEntry],
        reset: bool,
        cut_check: bool,
    ) -> Vec<JoinPlan> {
        self.inner.plan_join_entry_refs(entries, reset, cut_check)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_core_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: Option<usize>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<LogIndexEntry>), JsValue> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Entries,
                None,
            )?;
        Ok((facts, trimmed.into_entries()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: Option<usize>,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<LogIndexEntry>), JsValue> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Entries,
                profile.as_deref_mut(),
            )?;
        Ok((facts, trimmed.into_entries()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: Option<usize>,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>), JsValue> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Hashes,
                profile.as_deref_mut(),
            )?;
        Ok((facts, trimmed.into_hashes()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<NativeCommittedEntryFacts, JsValue> {
        self.prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_borrowed(
            builder,
            block_store,
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            &payload_data,
            profile.as_deref_mut(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_borrowed(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<NativeCommittedEntryFacts, JsValue> {
        let core_started = profile.as_ref().map(|_| js_sys::Date::now());
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            profile.as_deref_mut(),
        )?;
        if let Some(started) = core_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.entry_core_ms += js_sys::Date::now() - started;
            }
        }
        let facts_started = profile.as_ref().map(|_| js_sys::Date::now());
        let entry = core.entry;
        let hash = core.hash;
        let facts = NativeCommittedEntryFacts {
            hash: hash.clone(),
            next: core.next,
            meta_bytes: core.meta_bytes,
            byte_length: core.storage_bytes.len(),
            hash_digest_bytes: core.hash_digest_bytes,
        };
        if let Some(started) = facts_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.facts_ms += js_sys::Date::now() - started;
            }
        }
        let block_put_started = profile.as_ref().map(|_| js_sys::Date::now());
        block_store.put_entry(hash, core.storage_bytes);
        if let Some(started) = block_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.block_put_ms += js_sys::Date::now() - started;
            }
        }
        let graph_put_started = profile.as_ref().map(|_| js_sys::Date::now());
        self.inner.put_no_next(entry);
        if let Some(started) = graph_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.graph_put_ms += js_sys::Date::now() - started;
            }
        }
        Ok(facts)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: usize,
        trim_mode: NativeTrimMode,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, NativeTrimResult), JsValue> {
        self.prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner_borrowed(
            builder,
            block_store,
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            &payload_data,
            trim_length_to,
            trim_mode,
            profile.as_deref_mut(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_hashes_borrowed(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: usize,
        profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>), JsValue> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner_borrowed(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Hashes,
                profile,
            )?;
        Ok((facts, trimmed.into_hashes()))
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner_borrowed(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: usize,
        trim_mode: NativeTrimMode,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, NativeTrimResult), JsValue> {
        let core_started = profile.as_ref().map(|_| js_sys::Date::now());
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            profile.as_deref_mut(),
        )?;
        if let Some(started) = core_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.entry_core_ms += js_sys::Date::now() - started;
            }
        }
        let facts_started = profile.as_ref().map(|_| js_sys::Date::now());
        let entry = core.entry;
        let hash = core.hash;
        let facts = NativeCommittedEntryFacts {
            hash: hash.clone(),
            next: core.next,
            meta_bytes: core.meta_bytes,
            byte_length: core.storage_bytes.len(),
            hash_digest_bytes: core.hash_digest_bytes,
        };
        if let Some(started) = facts_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.facts_ms += js_sys::Date::now() - started;
            }
        }
        let block_put_started = profile.as_ref().map(|_| js_sys::Date::now());
        block_store.put_entry(hash, core.storage_bytes);
        if let Some(started) = block_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.block_put_ms += js_sys::Date::now() - started;
            }
        }
        let graph_put_started = profile.as_ref().map(|_| js_sys::Date::now());
        self.inner.put_no_next(entry);
        if let Some(started) = graph_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.graph_put_ms += js_sys::Date::now() - started;
            }
        }
        let trim_started = profile.as_ref().map(|_| js_sys::Date::now());
        let trimmed =
            match trim_mode {
                NativeTrimMode::Entries => NativeTrimResult::Entries(trim_oldest_log_entries_core(
                    &mut self.inner,
                    block_store,
                    trim_length_to,
                )),
                NativeTrimMode::Hashes => NativeTrimResult::Hashes(
                    trim_oldest_log_entry_hashes_core(&mut self.inner, block_store, trim_length_to),
                ),
            };
        if let Some(started) = trim_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.trim_ms += js_sys::Date::now() - started;
            }
        }
        Ok((facts, trimmed))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: usize,
        profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<LogIndexEntry>), JsValue> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Entries,
                profile,
            )?;
        Ok((facts, trimmed.into_entries()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_hashes(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: usize,
        profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>), JsValue> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Hashes,
                profile,
            )?;
        Ok((facts, trimmed.into_hashes()))
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_inner(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: Option<usize>,
        trim_mode: NativeTrimMode,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, NativeTrimResult), JsValue> {
        let next_clone_started = profile.as_ref().map(|_| js_sys::Date::now());
        let initial_nexts = next.clone();
        if let Some(started) = next_clone_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.next_clone_ms += js_sys::Date::now() - started;
            }
        }
        let core_started = profile.as_ref().map(|_| js_sys::Date::now());
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            &payload_data,
            profile.as_deref_mut(),
        )?;
        if let Some(started) = core_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.entry_core_ms += js_sys::Date::now() - started;
            }
        }
        let facts_started = profile.as_ref().map(|_| js_sys::Date::now());
        let entry = core.entry;
        let hash = core.hash;
        let facts = NativeCommittedEntryFacts {
            hash: hash.clone(),
            next: core.next,
            meta_bytes: core.meta_bytes,
            byte_length: core.storage_bytes.len(),
            hash_digest_bytes: core.hash_digest_bytes,
        };
        if let Some(started) = facts_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.facts_ms += js_sys::Date::now() - started;
            }
        }
        let block_put_started = profile.as_ref().map(|_| js_sys::Date::now());
        block_store.put_entry(hash, core.storage_bytes);
        if let Some(started) = block_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.block_put_ms += js_sys::Date::now() - started;
            }
        }
        let graph_put_started = profile.as_ref().map(|_| js_sys::Date::now());
        self.inner.put_append_entry(entry, &initial_nexts);
        if let Some(started) = graph_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.graph_put_ms += js_sys::Date::now() - started;
            }
        }
        let trim_started = profile.as_ref().map(|_| js_sys::Date::now());
        let trimmed = trim_length_to
            .map(|trim_length_to| match trim_mode {
                NativeTrimMode::Entries => NativeTrimResult::Entries(trim_oldest_log_entries_core(
                    &mut self.inner,
                    block_store,
                    trim_length_to,
                )),
                NativeTrimMode::Hashes => NativeTrimResult::Hashes(
                    trim_oldest_log_entry_hashes_core(&mut self.inner, block_store, trim_length_to),
                ),
            })
            .unwrap_or_else(|| trim_mode.empty_result());
        if let Some(started) = trim_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.trim_ms += js_sys::Date::now() - started;
            }
        }
        Ok((facts, trimmed))
    }
}

#[derive(Clone, Copy)]
enum NativeTrimMode {
    Entries,
    Hashes,
}

impl NativeTrimMode {
    fn empty_result(self) -> NativeTrimResult {
        match self {
            NativeTrimMode::Entries => NativeTrimResult::Entries(Vec::new()),
            NativeTrimMode::Hashes => NativeTrimResult::Hashes(Vec::new()),
        }
    }
}

enum NativeTrimResult {
    Entries(Vec<LogIndexEntry>),
    Hashes(Vec<String>),
}

impl NativeTrimResult {
    fn into_entries(self) -> Vec<LogIndexEntry> {
        match self {
            NativeTrimResult::Entries(entries) => entries,
            NativeTrimResult::Hashes(_) => {
                unreachable!("hash-only trim result cannot be converted to entries")
            }
        }
    }

    fn into_hashes(self) -> Vec<String> {
        match self {
            NativeTrimResult::Entries(entries) => {
                entries.into_iter().map(|entry| entry.hash).collect()
            }
            NativeTrimResult::Hashes(hashes) => hashes,
        }
    }
}

fn trim_oldest_log_entries(
    index: &mut LogGraphIndex,
    block_store: &mut NativeLogBlockStore,
    trim_length_to: usize,
) -> Array {
    log_trim_entries_to_rows(trim_oldest_log_entries_core(
        index,
        block_store,
        trim_length_to,
    ))
}

fn trim_oldest_log_entries_core(
    index: &mut LogGraphIndex,
    block_store: &mut NativeLogBlockStore,
    trim_length_to: usize,
) -> Vec<LogIndexEntry> {
    let overage = index.len().saturating_sub(trim_length_to);
    if overage == 0 {
        return Vec::new();
    }
    if overage == 1 {
        let Some(hash) = index.oldest_hash() else {
            return Vec::new();
        };
        block_store.delete(&hash);
        return index.delete(&hash).into_iter().collect();
    }
    let entries = index.oldest_entries(overage);
    for entry in &entries {
        block_store.delete(&entry.hash);
    }
    let hashes = entries
        .iter()
        .map(|entry| entry.hash.clone())
        .collect::<Vec<_>>();
    index.delete_many(&hashes);
    entries
}

fn trim_oldest_log_entry_hashes_core(
    index: &mut LogGraphIndex,
    block_store: &mut NativeLogBlockStore,
    trim_length_to: usize,
) -> Vec<String> {
    let overage = index.len().saturating_sub(trim_length_to);
    if overage == 0 {
        return Vec::new();
    }
    if overage == 1 {
        let Some(hash) = index.oldest_hash() else {
            return Vec::new();
        };
        if index.delete(&hash).is_none() {
            return Vec::new();
        }
        block_store.delete(&hash);
        return vec![hash];
    }
    let hashes = index.oldest_hashes(overage);
    for hash in &hashes {
        block_store.delete(hash);
    }
    index.delete_many(&hashes);
    hashes
}

fn trim_oldest_log_index_entries(index: &mut LogGraphIndex, trim_length_to: usize) -> Array {
    log_trim_entries_to_rows(trim_oldest_log_index_entries_core(index, trim_length_to))
}

fn trim_oldest_log_index_entries_core(
    index: &mut LogGraphIndex,
    trim_length_to: usize,
) -> Vec<LogIndexEntry> {
    let overage = index.len().saturating_sub(trim_length_to);
    if overage == 0 {
        return Vec::new();
    }
    let entries = index.oldest_entries(overage);
    let hashes = entries
        .iter()
        .map(|entry| entry.hash.clone())
        .collect::<Vec<_>>();
    index.delete_many(&hashes);
    entries
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

    pub fn oldest_entries(&self, limit: usize) -> Array {
        log_trim_entries_to_rows(self.inner.oldest_entries(limit))
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
        self.inner.put_append_entry(entry, &initial_nexts);
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
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_storage_and_put_with_builder(
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
        let (row, entry, initial_nexts, _block) =
            prepare_entry_v0_plain_entry_storage_row_with_signer(
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
            )?;
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_storage_facts_and_put_with_builder(
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
        let (row, entry, initial_nexts, _block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::StorageWithFacts,
            )?;
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_storage_trim_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) =
            prepare_entry_v0_plain_entry_storage_row_with_signer(
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
            )?;
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&trim_oldest_log_index_entries(
            &mut self.inner,
            trim_length_to,
        ));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_storage_facts_trim_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Array,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, _block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::StorageWithFacts,
            )?;
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&trim_oldest_log_index_entries(
            &mut self.inner,
            trim_length_to,
        ));
        Ok(out)
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
        self.inner.put_append_entry(entry, &initial_nexts);
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
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_storage_commit_block_and_put_with_builder(
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
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_storage_row_with_signer(
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
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_storage_commit_block_trim_and_put_with_builder(
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
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_storage_row_with_signer(
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
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&trim_oldest_log_entries(
            &mut self.inner,
            block_store,
            trim_length_to,
        ));
        Ok(out)
    }

    pub fn prepare_entry_v0_plain_entry_commit_facts_and_put_with_builder(
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
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::CommitFactsOnly,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                Vec::new(),
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::CommitFactsNoNext,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_trim_and_put_with_builder(
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
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::CommitFactsOnly,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&trim_oldest_log_entries(
            &mut self.inner,
            block_store,
            trim_length_to,
        ));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_trim_hashes_and_put_with_builder(
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
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (row, entry, initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                &builder.clock_id,
                &builder.public_key,
                &builder.signing_key,
                wall_time,
                logical,
                gid,
                strings_from_array(next)?,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                PreparedPlainEntryRowMode::CommitFactsOnly,
            )?;
        block_store.put_entries(vec![block]);
        self.inner.put_append_entry(entry, &initial_nexts);

        let out = Array::new();
        out.push(&row);
        out.push(&strings_to_array(trim_oldest_log_entry_hashes_core(
            &mut self.inner,
            block_store,
            trim_length_to,
        )));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_trim_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (facts, trimmed_entries) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                trim_length_to,
                None,
            )?;
        let out = Array::new();
        out.push(&committed_entry_facts_to_row(&facts, false));
        out.push(&log_trim_entries_to_rows(trimmed_entries));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_trim_hashes_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: JsValue,
        payload_data: Uint8Array,
        trim_length_to: usize,
    ) -> Result<Array, JsValue> {
        let (facts, trim_hashes) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_hashes(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                optional_bytes_from_js(meta_data),
                payload_data.to_vec(),
                trim_length_to,
                None,
            )?;
        let out = Array::new();
        out.push(&committed_entry_facts_to_row(&facts, false));
        out.push(&strings_to_array(trim_hashes));
        Ok(out)
    }

    pub fn prepare_entry_v0_plain_entries_commit_blocks_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gids: Array,
        nexts: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
    ) -> Result<Array, JsValue> {
        let (rows, entries, blocks) = prepare_entry_v0_plain_entries_rows_with_signer(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_times,
            logicals,
            gids,
            nexts,
            entry_type,
            meta_datas,
            payload_datas,
            false,
        )?;
        block_store.put_entries(blocks);
        self.inner.put_many(entries);
        Ok(rows)
    }

    pub fn prepare_entry_v0_plain_entries_no_next_commit_blocks_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        gids: Array,
        entry_type: u8,
        meta_datas: Array,
        payload_datas: Array,
    ) -> Result<Array, JsValue> {
        let (rows, entries, blocks) = prepare_entry_v0_plain_entries_rows_with_signer_inner(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_times,
            logicals,
            gids,
            None,
            entry_type,
            meta_datas,
            payload_datas,
            false,
        )?;
        block_store.put_entries(blocks);
        self.inner.put_many(entries);
        Ok(rows)
    }

    pub fn delete(&mut self, hash: &str) -> bool {
        self.inner.delete(hash).is_some()
    }

    pub fn delete_many(&mut self, hashes: Array) -> Result<usize, JsValue> {
        let hashes = strings_from_array(hashes)?;
        Ok(self.inner.delete_many(&hashes))
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

    pub fn max_head_data_u32_batch(&self, gids: Array) -> Result<Array, JsValue> {
        let gids = strings_from_array(gids)?;
        let out = Array::new();
        for value in self.inner.max_head_data_u32_batch(&gids) {
            out.push(
                &value
                    .map(|value| JsValue::from_f64(value as f64))
                    .unwrap_or(JsValue::UNDEFINED),
            );
        }
        Ok(out)
    }

    pub fn head_join_entries(&self, gid: Option<String>) -> Array {
        log_join_entries_to_rows(self.inner.head_join_entries(gid.as_deref()))
    }

    pub fn child_join_entries(&self, hash: &str) -> Array {
        log_join_entries_to_rows(self.inner.child_join_entries(hash))
    }

    pub fn entry_metadata_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        Ok(log_optional_entry_metadata_to_rows(
            self.inner.entry_metadata_batch(&hashes),
        ))
    }

    pub fn entry_metadata_hints_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        Ok(log_optional_entry_metadata_hints_to_rows(
            self.inner.entry_metadata_batch(&hashes),
        ))
    }

    pub fn unique_reference_gids(&self, hash: &str) -> JsValue {
        self.inner
            .unique_reference_gids(hash)
            .map(|gids| strings_to_array(gids).into())
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn unique_reference_gid_rows_batch(&self, hashes: Array) -> Result<Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let out = Array::new();
        for rows in self.inner.unique_reference_gid_rows_batch(&hashes) {
            out.push(
                &rows
                    .map(|rows| reference_gid_rows_to_array(rows).into())
                    .unwrap_or(JsValue::UNDEFINED),
            );
        }
        Ok(out)
    }

    pub fn unique_reference_gid_rows_flat_batch(&self, hashes: Array) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let Some(rows) = self.inner.unique_reference_gid_rows_flat_batch(&hashes) else {
            return Ok(JsValue::UNDEFINED);
        };
        Ok(reference_gid_flat_rows_to_array(rows).into())
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

    pub fn plan_join_batch(
        &self,
        hashes: Array,
        nexts: Array,
        entry_types: Uint8Array,
        reset: bool,
        gids: Array,
        wall_times: BigUint64Array,
        logicals: Uint32Array,
        cut_check: bool,
    ) -> Result<Array, JsValue> {
        let len = hashes.length();
        if nexts.length() != len || entry_types.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
        if cut_check
            && (gids.length() != len || wall_times.length() != len || logicals.length() != len)
        {
            return Err(JsValue::from_str("Expected equal cut-check column lengths"));
        }

        let mut parsed_hashes = Vec::with_capacity(len as usize);
        let mut parsed_nexts = Vec::with_capacity(len as usize);
        let mut parsed_entry_types = Vec::with_capacity(len as usize);
        let mut parsed_gids = if cut_check {
            Vec::with_capacity(len as usize)
        } else {
            Vec::new()
        };
        let mut parsed_wall_times = if cut_check {
            Vec::with_capacity(len as usize)
        } else {
            Vec::new()
        };
        let mut parsed_logicals = if cut_check {
            Vec::with_capacity(len as usize)
        } else {
            Vec::new()
        };
        for i in 0..len {
            parsed_hashes.push(required_string_from_array(&hashes, i)?);
            parsed_nexts.push(strings_from_array(required_array_from_array(&nexts, i)?)?);
            parsed_entry_types.push(entry_types.get_index(i));
            if cut_check {
                parsed_gids.push(required_string_from_array(&gids, i)?);
                parsed_wall_times.push(wall_times.get_index(i));
                parsed_logicals.push(logicals.get_index(i));
            }
        }
        let cut_checks = if cut_check {
            Some((
                parsed_gids.as_slice(),
                parsed_wall_times.as_slice(),
                parsed_logicals.as_slice(),
            ))
        } else {
            None
        };
        let out = Array::new();
        for plan in self.inner.plan_join_batch(
            &parsed_hashes,
            &parsed_nexts,
            &parsed_entry_types,
            reset,
            cut_checks,
        ) {
            out.push(&join_plan_to_row(plan));
        }
        Ok(out)
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
pub fn verify_ed25519_batch(
    signatures: Array,
    public_keys: Array,
    messages: Array,
) -> Result<Uint8Array, JsValue> {
    let len = signatures.length();
    if public_keys.length() != len || messages.length() != len {
        return Err(JsValue::from_str(
            "Expected equal Ed25519 verification batch lengths",
        ));
    }

    let mut parsed_signatures = Vec::with_capacity(len as usize);
    let mut parsed_public_keys = Vec::with_capacity(len as usize);
    let mut parsed_messages = Vec::with_capacity(len as usize);
    let mut verifying_key_cache = HashMap::new();
    for i in 0..len {
        let signature = required_bytes_from_array(&signatures, i, "signature")?;
        let public_key = required_bytes_from_array(&public_keys, i, "public key")?;
        let message = required_bytes_from_array(&messages, i, "message")?;
        validate_signature_lengths(&signature, &public_key)?;

        let signature_bytes: [u8; 64] = signature
            .as_slice()
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 signature length 64"))?;
        let verifying_key = cached_verifying_key(&mut verifying_key_cache, public_key.as_slice())?;
        let signature = Signature::from_bytes(&signature_bytes);
        parsed_signatures.push(signature);
        parsed_public_keys.push(verifying_key);
        parsed_messages.push(message);
    }

    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(Uint8Array::from(vec![1u8; len as usize].as_slice()));
    }

    let mut out = Vec::with_capacity(len as usize);
    for i in 0..parsed_signatures.len() {
        out.push(
            if parsed_public_keys[i]
                .verify(&parsed_messages[i], &parsed_signatures[i])
                .is_ok()
            {
                1
            } else {
                0
            },
        );
    }

    Ok(Uint8Array::from(out.as_slice()))
}

fn parse_entry_v0_ed25519_storage_slices(
    blocks: &[&[u8]],
) -> Result<(Vec<Signature>, Vec<VerifyingKey>, Vec<Vec<u8>>), JsValue> {
    let mut parsed_signatures = Vec::with_capacity(blocks.len());
    let mut parsed_public_keys = Vec::with_capacity(blocks.len());
    let mut parsed_messages = Vec::with_capacity(blocks.len());
    let mut verifying_key_cache = HashMap::new();

    for bytes in blocks {
        let parsed = parse_plain_entry_v0_storage_signature(bytes)?;
        validate_signature_lengths(&parsed.signature, &parsed.public_key)?;

        let signature_bytes: [u8; 64] = parsed
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 signature length 64"))?;
        let verifying_key = cached_verifying_key(&mut verifying_key_cache, &parsed.public_key)?;
        parsed_signatures.push(Signature::from_bytes(&signature_bytes));
        parsed_public_keys.push(verifying_key);
        parsed_messages.push(parsed.signable);
    }

    Ok((parsed_signatures, parsed_public_keys, parsed_messages))
}

pub fn verify_entry_v0_ed25519_storage_slices(blocks: &[&[u8]]) -> Result<Vec<u8>, JsValue> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    let (parsed_signatures, parsed_public_keys, parsed_messages) =
        parse_entry_v0_ed25519_storage_slices(blocks)?;
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(vec![1u8; blocks.len()]);
    }

    let mut out = Vec::with_capacity(blocks.len());
    for i in 0..parsed_signatures.len() {
        out.push(
            if parsed_public_keys[i]
                .verify(&parsed_messages[i], &parsed_signatures[i])
                .is_ok()
            {
                1
            } else {
                0
            },
        );
    }

    Ok(out)
}

pub fn verify_entry_v0_ed25519_storage_slices_all(blocks: &[&[u8]]) -> Result<bool, JsValue> {
    if blocks.is_empty() {
        return Ok(true);
    }

    let (parsed_signatures, parsed_public_keys, parsed_messages) =
        parse_entry_v0_ed25519_storage_slices(blocks)?;
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(true);
    }

    for i in 0..parsed_signatures.len() {
        if parsed_public_keys[i]
            .verify(&parsed_messages[i], &parsed_signatures[i])
            .is_err()
        {
            return Ok(false);
        }
    }

    Ok(true)
}

#[wasm_bindgen]
pub fn verify_entry_v0_ed25519_storage_batch(blocks: Array) -> Result<Uint8Array, JsValue> {
    let mut storage = Vec::with_capacity(blocks.length() as usize);
    for i in 0..blocks.length() {
        storage.push(required_bytes_from_array(&blocks, i, "entry storage")?);
    }
    let storage_refs = storage
        .iter()
        .map(|bytes| bytes.as_slice())
        .collect::<Vec<_>>();
    let out = verify_entry_v0_ed25519_storage_slices(&storage_refs)?;
    Ok(Uint8Array::from(out.as_slice()))
}

#[wasm_bindgen]
pub fn verify_entry_v0_ed25519_batch(
    clock_ids: Array,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Array,
    entry_types: Uint8Array,
    meta_datas: Array,
    payload_datas: Array,
    signatures: Array,
    public_keys: Array,
) -> Result<Uint8Array, JsValue> {
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
    for values in [&signatures, &public_keys] {
        if values.length() != len {
            return Err(JsValue::from_str(
                "Expected equal Ed25519 entry verification batch lengths",
            ));
        }
    }

    let mut parsed_signatures = Vec::with_capacity(len as usize);
    let mut parsed_public_keys = Vec::with_capacity(len as usize);
    let mut parsed_messages = Vec::with_capacity(len as usize);
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
        let public_key = required_bytes_from_array(&public_keys, i, "public key")?;
        validate_signature_lengths(&signature, &public_key)?;

        let signature_bytes: [u8; 64] = signature
            .as_slice()
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 signature length 64"))?;
        let public_key_bytes: [u8; 32] = public_key
            .as_slice()
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 public key length 32"))?;
        let verifying_key = VerifyingKey::from_bytes(&public_key_bytes)
            .map_err(|_| JsValue::from_str("Invalid Ed25519 public key"))?;
        parsed_signatures.push(Signature::from_bytes(&signature_bytes));
        parsed_public_keys.push(verifying_key);
        parsed_messages.push(encode_entry_v0(input, None));
    }

    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(Uint8Array::from(vec![1u8; len as usize].as_slice()));
    }

    let mut out = Vec::with_capacity(len as usize);
    for i in 0..parsed_signatures.len() {
        out.push(
            if parsed_public_keys[i]
                .verify(&parsed_messages[i], &parsed_signatures[i])
                .is_ok()
            {
                1
            } else {
                0
            },
        );
    }

    Ok(Uint8Array::from(out.as_slice()))
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
        let signable = encode_entry_v0_parts_unsigned_for_signing(&meta, &payload);
        let signature = sign_ed25519_with_key(&signing_key, &signable);
        let signature_with_key = encode_signature_with_key_parts(&signature, &public_key, 0);
        let storage = signable_entry_to_signed_storage(signable, &signature_with_key);
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
    prepare_entry_v0_plain_entry_row_with_signer_parts(
        clock_id,
        public_key,
        signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        PreparedPlainEntryRowMode::Full {
            include_storage_bytes,
        },
    )
}

fn prepare_entry_v0_plain_entry_storage_row_with_signer(
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
) -> Result<(Array, LogIndexEntry, Vec<String>, (String, Vec<u8>)), JsValue> {
    let next = strings_from_array(next)?;
    let payload_data = payload_data.to_vec();
    let meta_data = optional_bytes_from_js(meta_data);
    prepare_entry_v0_plain_entry_row_with_signer_parts(
        clock_id,
        public_key,
        signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        PreparedPlainEntryRowMode::StorageOnly,
    )
}

fn prepare_entry_v0_plain_entry_row_with_signer_parts(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
    payload_data: Vec<u8>,
    row_mode: PreparedPlainEntryRowMode,
) -> Result<(Array, LogIndexEntry, Vec<String>, (String, Vec<u8>)), JsValue> {
    let core = prepare_entry_v0_plain_entry_core_with_signer_parts(
        clock_id,
        public_key,
        signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
    )?;
    let row = prepared_plain_entry_core_to_row(&core, row_mode);
    let entry = core.entry.clone();
    let initial_nexts = core.next.clone();
    let block = (core.hash, core.storage_bytes);
    Ok((row, entry, initial_nexts, block))
}

fn prepare_entry_v0_plain_entry_core_with_signer_parts(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
    payload_data: Vec<u8>,
) -> Result<PreparedPlainEntryCore, JsValue> {
    prepare_entry_v0_plain_entry_core_with_signer_parts_profiled(
        clock_id,
        public_key,
        signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_entry_v0_plain_entry_core_with_signer_parts_profiled(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
    payload_data: Vec<u8>,
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> Result<PreparedPlainEntryCore, JsValue> {
    let payload_size = payload_data.len() as u32;

    let encode_meta_started = profile.as_ref().map(|_| js_sys::Date::now());
    let meta = encode_meta_parts(
        clock_id,
        wall_time,
        logical,
        &gid,
        &next,
        entry_type,
        meta_data.as_deref(),
    );
    if let Some(started) = encode_meta_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_meta_ms += js_sys::Date::now() - started;
        }
    }
    let encode_payload_started = profile.as_ref().map(|_| js_sys::Date::now());
    let payload = encode_payload(&payload_data);
    if let Some(started) = encode_payload_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_payload_ms += js_sys::Date::now() - started;
        }
    }
    let encode_signable_started = profile.as_ref().map(|_| js_sys::Date::now());
    let signable = encode_entry_v0_parts_unsigned_for_signing(&meta, &payload);
    if let Some(started) = encode_signable_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signable_ms += js_sys::Date::now() - started;
        }
    }
    let sign_started = profile.as_ref().map(|_| js_sys::Date::now());
    let signature = sign_ed25519_with_key(&signing_key, &signable);
    if let Some(started) = sign_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.sign_ms += js_sys::Date::now() - started;
        }
    }
    let encode_signature_started = profile.as_ref().map(|_| js_sys::Date::now());
    let signature_with_key = encode_signature_with_key_parts(&signature, public_key, 0);
    if let Some(started) = encode_signature_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signature_ms += js_sys::Date::now() - started;
        }
    }
    let encode_storage_started = profile.as_ref().map(|_| js_sys::Date::now());
    let storage = signable_entry_to_signed_storage(signable, &signature_with_key);
    if let Some(started) = encode_storage_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_storage_ms += js_sys::Date::now() - started;
        }
    }
    let cid_started = profile.as_ref().map(|_| js_sys::Date::now());
    let (cid, hash_digest) = calculate_raw_cid_v1_parts_profiled(&storage, profile.as_deref_mut());
    if let Some(started) = cid_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_ms += js_sys::Date::now() - started;
        }
    }

    let index_entry_started = profile.as_ref().map(|_| js_sys::Date::now());
    let entry = LogIndexEntry::new_with_data(
        cid.clone(),
        gid,
        next.clone(),
        entry_type,
        wall_time,
        logical,
        payload_size,
        true,
        meta_data,
    );
    if let Some(started) = index_entry_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.index_entry_ms += js_sys::Date::now() - started;
        }
    }
    Ok(PreparedPlainEntryCore {
        hash: cid,
        next,
        meta_bytes: meta,
        payload_bytes: payload,
        signature_bytes: signature,
        signature_with_key_bytes: signature_with_key,
        storage_bytes: storage,
        hash_digest_bytes: hash_digest.to_vec(),
        entry,
    })
}

#[allow(clippy::too_many_arguments)]
fn prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
    payload_data: &[u8],
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> Result<PreparedPlainEntryCommitCore, JsValue> {
    let payload_size = payload_data.len() as u32;

    let encode_meta_started = profile.as_ref().map(|_| js_sys::Date::now());
    let meta = encode_meta_parts(
        clock_id,
        wall_time,
        logical,
        &gid,
        &next,
        entry_type,
        meta_data.as_deref(),
    );
    if let Some(started) = encode_meta_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_meta_ms += js_sys::Date::now() - started;
        }
    }
    let encode_signable_started = profile.as_ref().map(|_| js_sys::Date::now());
    let signable = encode_entry_v0_payload_data_unsigned_for_signing(&meta, &payload_data);
    if let Some(started) = encode_signable_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signable_ms += js_sys::Date::now() - started;
        }
    }
    let sign_started = profile.as_ref().map(|_| js_sys::Date::now());
    let signature = sign_ed25519_with_key(signing_key, &signable);
    if let Some(started) = sign_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.sign_ms += js_sys::Date::now() - started;
        }
    }
    let encode_signature_started = profile.as_ref().map(|_| js_sys::Date::now());
    let signature_with_key = encode_signature_with_key_parts(&signature, public_key, 0);
    if let Some(started) = encode_signature_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signature_ms += js_sys::Date::now() - started;
        }
    }
    let encode_storage_started = profile.as_ref().map(|_| js_sys::Date::now());
    let storage = signable_entry_to_signed_storage(signable, &signature_with_key);
    if let Some(started) = encode_storage_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_storage_ms += js_sys::Date::now() - started;
        }
    }
    let cid_started = profile.as_ref().map(|_| js_sys::Date::now());
    let (cid, hash_digest) = calculate_raw_cid_v1_parts_profiled(&storage, profile.as_deref_mut());
    if let Some(started) = cid_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_ms += js_sys::Date::now() - started;
        }
    }

    let index_entry_started = profile.as_ref().map(|_| js_sys::Date::now());
    let entry = LogIndexEntry::new_with_data(
        cid.clone(),
        gid,
        next.clone(),
        entry_type,
        wall_time,
        logical,
        payload_size,
        true,
        meta_data,
    );
    if let Some(started) = index_entry_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.index_entry_ms += js_sys::Date::now() - started;
        }
    }
    Ok(PreparedPlainEntryCommitCore {
        hash: cid,
        next,
        meta_bytes: meta,
        storage_bytes: storage,
        hash_digest_bytes: hash_digest.to_vec(),
        entry,
    })
}

#[allow(clippy::too_many_arguments)]
fn prepare_entry_v0_plain_entry_commit_digest_key_core_profiled(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: &str,
    entry_type: u8,
    payload_data: Vec<u8>,
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> (usize, usize) {
    let encode_meta_started = profile.as_ref().map(|_| js_sys::Date::now());
    let meta = encode_meta_parts(clock_id, wall_time, logical, gid, &[], entry_type, None);
    if let Some(started) = encode_meta_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_meta_ms += js_sys::Date::now() - started;
        }
    }
    let encode_signable_started = profile.as_ref().map(|_| js_sys::Date::now());
    let signable = encode_entry_v0_payload_data_unsigned_for_signing(&meta, &payload_data);
    if let Some(started) = encode_signable_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signable_ms += js_sys::Date::now() - started;
        }
    }
    let sign_started = profile.as_ref().map(|_| js_sys::Date::now());
    let signature = sign_ed25519_with_key(signing_key, &signable);
    if let Some(started) = sign_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.sign_ms += js_sys::Date::now() - started;
        }
    }
    let encode_signature_started = profile.as_ref().map(|_| js_sys::Date::now());
    let signature_with_key = encode_signature_with_key_parts(&signature, public_key, 0);
    if let Some(started) = encode_signature_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signature_ms += js_sys::Date::now() - started;
        }
    }
    let encode_storage_started = profile.as_ref().map(|_| js_sys::Date::now());
    let storage = signable_entry_to_signed_storage(signable, &signature_with_key);
    if let Some(started) = encode_storage_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_storage_ms += js_sys::Date::now() - started;
        }
    }
    let digest = calculate_raw_digest_profiled(&storage, profile.as_deref_mut());
    (storage.len(), digest.len())
}

fn prepared_plain_entry_core_to_row(
    core: &PreparedPlainEntryCore,
    row_mode: PreparedPlainEntryRowMode,
) -> Array {
    let row = Array::new();
    match row_mode {
        PreparedPlainEntryRowMode::Full {
            include_storage_bytes,
        } => {
            if include_storage_bytes {
                row.push(&Uint8Array::from(core.storage_bytes.as_slice()));
                row.push(&JsValue::from_str(&core.hash));
                row.push(&Uint8Array::from(core.signature_bytes.as_slice()));
                row.push(&strings_to_array(core.next.clone()));
                row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
                row.push(&Uint8Array::from(core.payload_bytes.as_slice()));
                row.push(&Uint8Array::from(core.signature_with_key_bytes.as_slice()));
                row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
            } else {
                row.push(&JsValue::from_str(&core.hash));
                row.push(&Uint8Array::from(core.signature_bytes.as_slice()));
                row.push(&strings_to_array(core.next.clone()));
                row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
                row.push(&Uint8Array::from(core.payload_bytes.as_slice()));
                row.push(&Uint8Array::from(core.signature_with_key_bytes.as_slice()));
                row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
                row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
            }
        }
        PreparedPlainEntryRowMode::StorageOnly => {
            row.push(&Uint8Array::from(core.storage_bytes.as_slice()));
            row.push(&JsValue::from_str(&core.hash));
            row.push(&strings_to_array(core.next.clone()));
            row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
        }
        PreparedPlainEntryRowMode::StorageWithFacts => {
            row.push(&Uint8Array::from(core.storage_bytes.as_slice()));
            row.push(&JsValue::from_str(&core.hash));
            row.push(&strings_to_array(core.next.clone()));
            row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
            row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
            row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
        }
        PreparedPlainEntryRowMode::CommitFactsOnly => {
            row.push(&JsValue::from_str(&core.hash));
            row.push(&strings_to_array(core.next.clone()));
            row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
            row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
            row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
        }
        PreparedPlainEntryRowMode::CommitFactsNoNext => {
            row.push(&JsValue::from_str(&core.hash));
            row.push(&Uint8Array::from(core.meta_bytes.as_slice()));
            row.push(&JsValue::from_f64(core.storage_bytes.len() as f64));
            row.push(&Uint8Array::from(core.hash_digest_bytes.as_slice()));
        }
    }
    row
}

fn committed_entry_facts_to_row(entry: &NativeCommittedEntryFacts, include_next: bool) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&entry.hash));
    if include_next {
        row.push(&strings_to_array(entry.next.clone()));
    }
    row.push(&Uint8Array::from(entry.meta_bytes.as_slice()));
    row.push(&JsValue::from_f64(entry.byte_length as f64));
    row.push(&Uint8Array::from(entry.hash_digest_bytes.as_slice()));
    row
}

fn prepare_entry_v0_plain_entries_rows_with_signer(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Array,
    entry_type: u8,
    meta_datas: Array,
    payload_datas: Array,
    include_storage_bytes: bool,
) -> Result<(Array, Vec<LogIndexEntry>, Vec<(String, Vec<u8>)>), JsValue> {
    prepare_entry_v0_plain_entries_rows_with_signer_inner(
        clock_id,
        public_key,
        signing_key,
        wall_times,
        logicals,
        gids,
        Some(nexts),
        entry_type,
        meta_datas,
        payload_datas,
        include_storage_bytes,
    )
}

fn prepare_entry_v0_plain_entries_rows_with_signer_inner(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_times: BigUint64Array,
    logicals: Uint32Array,
    gids: Array,
    nexts: Option<Array>,
    entry_type: u8,
    meta_datas: Array,
    payload_datas: Array,
    include_storage_bytes: bool,
) -> Result<(Array, Vec<LogIndexEntry>, Vec<(String, Vec<u8>)>), JsValue> {
    let len = payload_datas.length();
    if gids.length() != len || meta_datas.length() != len {
        return Err(JsValue::from_str("Expected equal column lengths"));
    }
    if let Some(nexts) = &nexts {
        if nexts.length() != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }
    for numeric_len in [wall_times.length(), logicals.length()] {
        if numeric_len != len {
            return Err(JsValue::from_str("Expected equal column lengths"));
        }
    }

    let out = Array::new();
    let mut entries = Vec::with_capacity(len as usize);
    let mut blocks = Vec::with_capacity(len as usize);
    for i in 0..len {
        let payload_data = required_bytes_from_array(&payload_datas, i, "payload")?;
        let next = match &nexts {
            Some(nexts) => strings_from_array(required_array_from_array(nexts, i)?)?,
            None => Vec::new(),
        };
        let (row, entry, _initial_nexts, block) =
            prepare_entry_v0_plain_entry_row_with_signer_parts(
                clock_id,
                public_key,
                signing_key,
                wall_times.get_index(i),
                logicals.get_index(i),
                required_string_from_array(&gids, i)?,
                next,
                entry_type,
                optional_bytes_from_js(meta_datas.get(i)),
                payload_data,
                PreparedPlainEntryRowMode::Full {
                    include_storage_bytes,
                },
            )?;
        out.push(&row);
        entries.push(entry);
        blocks.push(block);
    }

    Ok((out, entries, blocks))
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

#[wasm_bindgen]
pub fn calculate_raw_cid_v1_batch(blocks: Array) -> Result<Array, JsValue> {
    let out = Array::new();
    for i in 0..blocks.length() {
        let value = blocks.get(i);
        if value.is_undefined() || value.is_null() {
            return Err(JsValue::from_str("Expected block bytes"));
        }
        let bytes = Uint8Array::new(&value).to_vec();
        out.push(&JsValue::from_str(&calculate_raw_cid_v1_from_bytes(&bytes)));
    }
    Ok(out)
}

#[wasm_bindgen]
pub fn prepare_raw_entry_v0_batch(blocks: Array) -> Result<Array, JsValue> {
    let mut raw_blocks = Vec::with_capacity(blocks.length() as usize);
    for i in 0..blocks.length() {
        raw_blocks.push(required_bytes_from_array(&blocks, i, "entry storage")?);
    }
    let entries = prepare_raw_entry_v0_blocks(raw_blocks)?;
    let out = Array::new();
    for entry in &entries {
        out.push(&prepared_raw_entry_v0_to_row(entry));
    }
    Ok(out)
}

pub fn prepare_raw_entry_v0_blocks(
    blocks: Vec<Vec<u8>>,
) -> Result<Vec<PreparedRawEntryV0>, JsValue> {
    prepare_raw_entry_v0_blocks_with_expected_cids_and_verify(blocks, None, true)
}

pub fn prepare_raw_entry_v0_blocks_with_expected_cids(
    blocks: Vec<Vec<u8>>,
    expected_cids: Option<Vec<String>>,
) -> Result<Vec<PreparedRawEntryV0>, JsValue> {
    prepare_raw_entry_v0_blocks_with_expected_cids_and_verify(blocks, expected_cids, true)
}

pub fn prepare_raw_entry_v0_blocks_with_expected_cids_and_verify(
    blocks: Vec<Vec<u8>>,
    expected_cids: Option<Vec<String>>,
    verify_signatures: bool,
) -> Result<Vec<PreparedRawEntryV0>, JsValue> {
    if let Some(expected_cids) = expected_cids.as_ref() {
        if expected_cids.len() != blocks.len() {
            return Err(JsValue::from_str(
                "Expected equal raw entry block and hash lengths",
            ));
        }
    }
    let mut entries = Vec::with_capacity(blocks.len());
    let mut parsed_signatures = Vec::with_capacity(blocks.len());
    let mut parsed_public_keys = Vec::with_capacity(blocks.len());
    let mut parsed_messages = Vec::with_capacity(blocks.len());
    let mut verifying_key_cache = HashMap::new();
    for (index, bytes) in blocks.into_iter().enumerate() {
        let digest = calculate_raw_digest_profiled(&bytes, None);
        let cid = if let Some(expected_cids) = expected_cids.as_ref() {
            let expected_cid = &expected_cids[index];
            let expected_digest = raw_cid_v1_digest_from_string(expected_cid)?;
            if expected_digest != digest {
                return Err(JsValue::from_str("Raw entry hash did not match bytes"));
            }
            expected_cid.clone()
        } else {
            raw_cid_v1_string_from_digest(&digest)
        };
        let storage = parse_plain_entry_v0_storage(&bytes)?;
        let meta = parse_raw_entry_v0_meta(storage.meta)?;
        let payload = parse_raw_entry_v0_payload(storage.payload)?;
        let requested_replicas = decode_absolute_replica_data_u32(meta.meta_data.as_deref());
        if verify_signatures {
            let parsed_signature = parse_plain_signature_with_key(storage.signature_with_key)?;
            validate_signature_lengths(&parsed_signature.signature, &parsed_signature.public_key)?;
            let signature_bytes: [u8; 64] = parsed_signature
                .signature
                .as_slice()
                .try_into()
                .map_err(|_| JsValue::from_str("Expected Ed25519 signature length 64"))?;
            let verifying_key =
                cached_verifying_key(&mut verifying_key_cache, &parsed_signature.public_key)?;
            parsed_signatures.push(Signature::from_bytes(&signature_bytes));
            parsed_public_keys.push(verifying_key);
            parsed_messages.push(encode_entry_v0_parts_unsigned_for_signing(
                storage.meta,
                storage.payload,
            ));
        }

        entries.push(PreparedRawEntryV0 {
            cid,
            hash_digest_bytes: digest.to_vec(),
            byte_length: bytes.len(),
            clock_id: meta.clock_id,
            wall_time: meta.wall_time,
            logical: meta.logical,
            gid: meta.gid,
            next: meta.next,
            entry_type: meta.entry_type,
            meta_bytes: storage.meta.to_vec(),
            meta_data: meta.meta_data,
            payload_byte_length: payload.data_len,
            signature_verified: false,
            storage_bytes: bytes,
            requested_replicas,
        });
    }
    if !verify_signatures {
        return Ok(entries);
    }
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    let verified = if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        vec![true; entries.len()]
    } else {
        let mut out = Vec::with_capacity(entries.len());
        for i in 0..entries.len() {
            out.push(
                parsed_public_keys[i]
                    .verify(&parsed_messages[i], &parsed_signatures[i])
                    .is_ok(),
            );
        }
        out
    };
    for (entry, verified) in entries.iter_mut().zip(verified) {
        entry.signature_verified = verified;
    }
    Ok(entries)
}

fn prepared_raw_entry_v0_to_row(entry: &PreparedRawEntryV0) -> Array {
    let row = Array::new();
    row.push(&JsValue::from_str(&entry.cid));
    row.push(&Uint8Array::from(entry.hash_digest_bytes.as_slice()));
    row.push(&JsValue::from_f64(entry.byte_length as f64));
    row.push(&Uint8Array::from(entry.clock_id.as_slice()));
    row.push(&JsValue::from_str(&entry.wall_time.to_string()));
    row.push(&JsValue::from_f64(entry.logical as f64));
    row.push(&JsValue::from_str(&entry.gid));

    let next = Array::new();
    for hash in &entry.next {
        next.push(&JsValue::from_str(hash));
    }
    row.push(&next);
    row.push(&JsValue::from_f64(entry.entry_type as f64));
    row.push(&Uint8Array::from(entry.meta_bytes.as_slice()));
    match &entry.meta_data {
        Some(data) => row.push(&Uint8Array::from(data.as_slice())),
        None => row.push(&JsValue::UNDEFINED),
    };
    row.push(&JsValue::from_f64(entry.payload_byte_length as f64));
    row.push(&JsValue::from_bool(entry.signature_verified));
    match entry.requested_replicas {
        Some(value) => row.push(&JsValue::from_f64(value as f64)),
        None => row.push(&JsValue::UNDEFINED),
    };
    row
}

#[wasm_bindgen]
pub fn benchmark_plain_entry_v0_core(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    iterations: u32,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    let payload_data = payload_data.to_vec();
    let gid = String::from("native-log-core-ceiling");
    let mut profile = NativeLogAppendProfile::default();
    let mut input_copy_ms = 0.0;
    let mut storage_bytes_total = 0usize;
    let mut hash_bytes_total = 0usize;
    let started = js_sys::Date::now();

    for i in 0..iterations {
        let copy_started = js_sys::Date::now();
        let payload_data = payload_data.clone();
        input_copy_ms += js_sys::Date::now() - copy_started;

        let core_started = js_sys::Date::now();
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &clock_id,
            &public_key,
            &signing_key,
            1_700_000_000_000 + i as u64,
            i,
            gid.clone(),
            Vec::new(),
            0,
            None,
            &payload_data,
            Some(&mut profile),
        )?;
        profile.entry_core_ms += js_sys::Date::now() - core_started;
        storage_bytes_total += core.storage_bytes.len();
        hash_bytes_total += core.hash.len();
    }

    let total_ms = js_sys::Date::now() - started;
    let row = Array::new();
    row.push(&JsValue::from_f64(total_ms));
    row.push(&JsValue::from_f64(input_copy_ms));
    row.push(&JsValue::from_f64(profile.entry_core_ms));
    row.push(&JsValue::from_f64(profile.encode_meta_ms));
    row.push(&JsValue::from_f64(profile.encode_payload_ms));
    row.push(&JsValue::from_f64(profile.encode_signable_ms));
    row.push(&JsValue::from_f64(profile.sign_ms));
    row.push(&JsValue::from_f64(profile.encode_signature_ms));
    row.push(&JsValue::from_f64(profile.encode_storage_ms));
    row.push(&JsValue::from_f64(profile.cid_ms));
    row.push(&JsValue::from_f64(profile.cid_hash_ms));
    row.push(&JsValue::from_f64(profile.cid_string_ms));
    row.push(&JsValue::from_f64(profile.index_entry_ms));
    row.push(&JsValue::from_f64(storage_bytes_total as f64));
    row.push(&JsValue::from_f64(hash_bytes_total as f64));
    Ok(row)
}

#[wasm_bindgen]
pub fn benchmark_plain_entry_v0_digest_key_core(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    iterations: u32,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    let payload_data = payload_data.to_vec();
    let gid = String::from("native-log-digest-key-core-ceiling");
    let mut profile = NativeLogAppendProfile::default();
    let mut input_copy_ms = 0.0;
    let mut storage_bytes_total = 0usize;
    let mut hash_bytes_total = 0usize;
    let started = js_sys::Date::now();

    for i in 0..iterations {
        let copy_started = js_sys::Date::now();
        let payload_data = payload_data.clone();
        input_copy_ms += js_sys::Date::now() - copy_started;

        let core_started = js_sys::Date::now();
        let (storage_len, digest_len) =
            prepare_entry_v0_plain_entry_commit_digest_key_core_profiled(
                &clock_id,
                &public_key,
                &signing_key,
                1_700_000_000_000 + i as u64,
                i,
                &gid,
                0,
                payload_data,
                Some(&mut profile),
            );
        profile.entry_core_ms += js_sys::Date::now() - core_started;
        storage_bytes_total += storage_len;
        hash_bytes_total += digest_len;
    }

    let total_ms = js_sys::Date::now() - started;
    let row = Array::new();
    row.push(&JsValue::from_f64(total_ms));
    row.push(&JsValue::from_f64(input_copy_ms));
    row.push(&JsValue::from_f64(profile.entry_core_ms));
    row.push(&JsValue::from_f64(profile.encode_meta_ms));
    row.push(&JsValue::from_f64(profile.encode_payload_ms));
    row.push(&JsValue::from_f64(profile.encode_signable_ms));
    row.push(&JsValue::from_f64(profile.sign_ms));
    row.push(&JsValue::from_f64(profile.encode_signature_ms));
    row.push(&JsValue::from_f64(profile.encode_storage_ms));
    row.push(&JsValue::from_f64(profile.cid_ms));
    row.push(&JsValue::from_f64(profile.cid_hash_ms));
    row.push(&JsValue::from_f64(profile.cid_string_ms));
    row.push(&JsValue::from_f64(profile.index_entry_ms));
    row.push(&JsValue::from_f64(storage_bytes_total as f64));
    row.push(&JsValue::from_f64(hash_bytes_total as f64));
    Ok(row)
}

#[wasm_bindgen]
pub fn benchmark_plain_entry_v0_crypto(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    iterations: u32,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    let verifying_key = signing_key.verifying_key();
    let payload_data = payload_data.to_vec();
    let gid = String::from("native-log-crypto-ceiling");
    let meta = encode_meta_parts(&clock_id, 1_700_000_000_000, 0, &gid, &[], 0, None);
    let signable = encode_entry_v0_payload_data_unsigned_for_signing(&meta, &payload_data);
    let mut checksum = 0u32;
    let mut signature_bytes = [0u8; 64];
    let started = js_sys::Date::now();

    let sign_started = js_sys::Date::now();
    for i in 0..iterations {
        signature_bytes = sign_ed25519_with_key(&signing_key, &signable);
        checksum ^= signature_bytes[(i as usize) & 63] as u32;
    }
    let sign_ms = js_sys::Date::now() - sign_started;

    let signature = Signature::from_bytes(&signature_bytes);
    let verify_started = js_sys::Date::now();
    for i in 0..iterations {
        verifying_key
            .verify(&signable, &signature)
            .map_err(|_| JsValue::from_str("Ed25519 signature verification failed"))?;
        checksum ^= signature_bytes[((i as usize) + 17) & 63] as u32;
    }
    let verify_ms = js_sys::Date::now() - verify_started;

    #[cfg(feature = "crypto-bench-candidates")]
    let (compact_sign_ms, compact_verify_ms) = {
        let compact_profile = benchmark_compact_ed25519_candidate(
            &private_key,
            &public_key,
            &signable,
            &signature_bytes,
            iterations,
            &mut checksum,
        )?;
        compact_profile
    };
    #[cfg(not(feature = "crypto-bench-candidates"))]
    let (compact_sign_ms, compact_verify_ms) = (0.0, 0.0);

    let signature_with_key = encode_signature_with_key_parts(&signature_bytes, &public_key, 0);
    let storage = signable_entry_to_signed_storage(signable.clone(), &signature_with_key);
    let mut digest_bytes = [0u8; 32];
    let sha_started = js_sys::Date::now();
    for i in 0..iterations {
        let digest = Sha256::digest(&storage);
        digest_bytes = digest.into();
        checksum ^= digest_bytes[(i as usize) & 31] as u32;
    }
    let sha256_ms = js_sys::Date::now() - sha_started;

    let mut cid_len_total = 0usize;
    let cid_string_started = js_sys::Date::now();
    for i in 0..iterations {
        let cid = raw_cid_v1_string_from_digest(&digest_bytes);
        cid_len_total += cid.len();
        checksum ^= cid.as_bytes()[(i as usize) % cid.len()] as u32;
    }
    let cid_string_ms = js_sys::Date::now() - cid_string_started;

    let total_ms = js_sys::Date::now() - started;
    let row = Array::new();
    row.push(&JsValue::from_f64(total_ms));
    row.push(&JsValue::from_f64(signable.len() as f64));
    row.push(&JsValue::from_f64(storage.len() as f64));
    row.push(&JsValue::from_f64(sign_ms));
    row.push(&JsValue::from_f64(verify_ms));
    row.push(&JsValue::from_f64(sha256_ms));
    row.push(&JsValue::from_f64(cid_string_ms));
    row.push(&JsValue::from_f64(checksum as f64));
    row.push(&JsValue::from_f64(cid_len_total as f64));
    row.push(&JsValue::from_f64(compact_sign_ms));
    row.push(&JsValue::from_f64(compact_verify_ms));
    Ok(row)
}

#[wasm_bindgen]
pub fn benchmark_entry_v0_storage_verify_modes(
    clock_id: Uint8Array,
    private_key: Uint8Array,
    public_key: Uint8Array,
    iterations: u32,
    payload_data: Uint8Array,
) -> Result<Array, JsValue> {
    let clock_id = clock_id.to_vec();
    let private_key = private_key.to_vec();
    let public_key = public_key.to_vec();
    let signing_key = validate_ed25519_keypair(&private_key, &public_key)?;
    let payload_data = payload_data.to_vec();
    let len = iterations as usize;
    let mut storages = Vec::with_capacity(len);
    let mut storage_bytes_total = 0usize;
    for i in 0..iterations {
        let payload_data = payload_data.clone();
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &clock_id,
            &public_key,
            &signing_key,
            1_700_000_000_000 + i as u64,
            i,
            format!("native-log-verify-{i}"),
            Vec::new(),
            0,
            None,
            &payload_data,
            None,
        )?;
        storage_bytes_total += core.storage_bytes.len();
        storages.push(core.storage_bytes);
    }

    let parse_started = js_sys::Date::now();
    let mut parsed_signatures = Vec::with_capacity(len);
    let mut parsed_public_keys = Vec::with_capacity(len);
    let mut parsed_messages = Vec::with_capacity(len);
    let mut verifying_key_cache = HashMap::new();
    for storage in &storages {
        let parsed = parse_plain_entry_v0_storage_signature(storage)?;
        validate_signature_lengths(&parsed.signature, &parsed.public_key)?;
        let signature_bytes: [u8; 64] = parsed
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| JsValue::from_str("Expected Ed25519 signature length 64"))?;
        let verifying_key = cached_verifying_key(&mut verifying_key_cache, &parsed.public_key)?;
        parsed_signatures.push(Signature::from_bytes(&signature_bytes));
        parsed_public_keys.push(verifying_key);
        parsed_messages.push(parsed.signable);
    }
    let parse_ms = js_sys::Date::now() - parse_started;
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();

    let mut checksum = 0u32;
    let batch_started = js_sys::Date::now();
    let batch_ok = verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok();
    let batch_ms = js_sys::Date::now() - batch_started;
    checksum ^= u32::from(batch_ok);

    let serial_started = js_sys::Date::now();
    let mut serial_ok = true;
    for i in 0..parsed_signatures.len() {
        let ok = parsed_public_keys[i]
            .verify(&parsed_messages[i], &parsed_signatures[i])
            .is_ok();
        serial_ok = serial_ok && ok;
        checksum ^= (u32::from(ok)) << (i & 7);
    }
    let serial_ms = js_sys::Date::now() - serial_started;

    let storage_refs = storages
        .iter()
        .map(|storage: &Vec<u8>| storage.as_slice())
        .collect::<Vec<_>>();
    let storage_verify_started = js_sys::Date::now();
    let storage_verified = verify_entry_v0_ed25519_storage_slices(&storage_refs)?;
    let storage_verify_ms = js_sys::Date::now() - storage_verify_started;
    let storage_ok = storage_verified.iter().all(|flag| *flag != 0);
    checksum ^= u32::from(storage_ok) << 16;

    let row = Array::new();
    row.push(&JsValue::from_f64(parse_ms));
    row.push(&JsValue::from_f64(batch_ms));
    row.push(&JsValue::from_f64(serial_ms));
    row.push(&JsValue::from_f64(storage_verify_ms));
    row.push(&JsValue::from_f64(iterations as f64));
    row.push(&JsValue::from_bool(batch_ok));
    row.push(&JsValue::from_bool(serial_ok));
    row.push(&JsValue::from_bool(storage_ok));
    row.push(&JsValue::from_f64(checksum as f64));
    row.push(&JsValue::from_f64(storage_bytes_total as f64));
    Ok(row)
}

#[cfg(feature = "crypto-bench-candidates")]
fn benchmark_compact_ed25519_candidate(
    private_key: &[u8],
    public_key: &[u8],
    signable: &[u8],
    expected_signature_bytes: &[u8; 64],
    iterations: u32,
    checksum: &mut u32,
) -> Result<(f64, f64), JsValue> {
    let (compact_secret_key, compact_public_key) =
        validate_compact_ed25519_keypair(private_key, public_key)?;
    let mut compact_signature_bytes = [0u8; 64];
    let compact_sign_started = js_sys::Date::now();
    for i in 0..iterations {
        let signature = compact_secret_key.sign(signable, None);
        compact_signature_bytes.copy_from_slice(signature.as_ref());
        *checksum ^= compact_signature_bytes[((i as usize) + 3) & 63] as u32;
    }
    let compact_sign_ms = js_sys::Date::now() - compact_sign_started;
    if &compact_signature_bytes != expected_signature_bytes {
        return Err(JsValue::from_str(
            "ed25519-compact signature does not match ed25519-dalek",
        ));
    }

    let compact_signature = CompactSignature::from_slice(&compact_signature_bytes)
        .map_err(|_| JsValue::from_str("Invalid ed25519-compact signature"))?;
    let compact_verify_started = js_sys::Date::now();
    for i in 0..iterations {
        compact_public_key
            .verify(signable, &compact_signature)
            .map_err(|_| JsValue::from_str("ed25519-compact signature verification failed"))?;
        *checksum ^= compact_signature_bytes[((i as usize) + 31) & 63] as u32;
    }
    let compact_verify_ms = js_sys::Date::now() - compact_verify_started;
    Ok((compact_sign_ms, compact_verify_ms))
}

fn calculate_raw_cid_v1_from_bytes(bytes: &[u8]) -> String {
    calculate_raw_cid_v1_parts(bytes).0
}

fn calculate_raw_cid_v1_parts(bytes: &[u8]) -> (String, [u8; 32]) {
    calculate_raw_cid_v1_parts_profiled(bytes, None)
}

fn calculate_raw_cid_v1_parts_profiled(
    bytes: &[u8],
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> (String, [u8; 32]) {
    let hash_started = profile.as_ref().map(|_| js_sys::Date::now());
    let digest = Sha256::digest(bytes);
    let digest_bytes: [u8; 32] = digest.into();
    if let Some(started) = hash_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_hash_ms += js_sys::Date::now() - started;
        }
    }
    let string_started = profile.as_ref().map(|_| js_sys::Date::now());
    let cid = raw_cid_v1_string_from_digest(&digest_bytes);
    if let Some(started) = string_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_string_ms += js_sys::Date::now() - started;
        }
    }
    (cid, digest_bytes)
}

fn calculate_raw_digest_profiled(
    bytes: &[u8],
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> [u8; 32] {
    let hash_started = profile.as_ref().map(|_| js_sys::Date::now());
    let digest = Sha256::digest(bytes);
    let digest_bytes: [u8; 32] = digest.into();
    if let Some(started) = hash_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_hash_ms += js_sys::Date::now() - started;
            profile.cid_ms += js_sys::Date::now() - started;
        }
    }
    digest_bytes
}

fn raw_cid_v1_string_from_digest(digest_bytes: &[u8; 32]) -> String {
    let mut cid = Vec::with_capacity(36);
    cid.push(0x01); // CIDv1
    cid.push(0x55); // raw codec
    cid.push(0x12); // sha2-256 multihash code
    cid.push(0x20); // 32 byte digest
    cid.extend_from_slice(digest_bytes.as_slice());
    format!("z{}", bs58::encode(cid).into_string())
}

fn raw_cid_v1_digest_from_string(cid: &str) -> Result<[u8; 32], JsValue> {
    let encoded = cid
        .strip_prefix('z')
        .ok_or_else(|| JsValue::from_str("Expected base58btc CID"))?;
    let mut decoded = [0u8; 36];
    let decoded_len = bs58::decode(encoded)
        .onto(&mut decoded)
        .map_err(|_| JsValue::from_str("Invalid base58btc CID"))?;
    if decoded_len != 36
        || decoded[0] != 0x01
        || decoded[1] != 0x55
        || decoded[2] != 0x12
        || decoded[3] != 0x20
    {
        return Err(JsValue::from_str("Expected raw CIDv1 sha2-256 CID"));
    }
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&decoded[4..36]);
    Ok(digest)
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

fn cached_verifying_key(
    cache: &mut HashMap<[u8; 32], VerifyingKey>,
    public_key: &[u8],
) -> Result<VerifyingKey, JsValue> {
    let public_key_bytes: [u8; 32] = public_key
        .try_into()
        .map_err(|_| JsValue::from_str("Expected Ed25519 public key length 32"))?;
    if let Some(verifying_key) = cache.get(&public_key_bytes) {
        return Ok(*verifying_key);
    }
    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes)
        .map_err(|_| JsValue::from_str("Invalid Ed25519 public key"))?;
    cache.insert(public_key_bytes, verifying_key);
    Ok(verifying_key)
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

struct ParsedEntryV0StorageSignature {
    signable: Vec<u8>,
    signature: Vec<u8>,
    public_key: Vec<u8>,
}

struct ParsedSignatureWithKey {
    signature: Vec<u8>,
    public_key: Vec<u8>,
}

struct ParsedPlainEntryV0Storage<'a> {
    meta: &'a [u8],
    payload: &'a [u8],
    signature_with_key: &'a [u8],
}

struct ParsedRawEntryV0Meta {
    clock_id: Vec<u8>,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
}

struct ParsedRawEntryV0Payload {
    data_len: usize,
}

struct BorshReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BorshReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_done(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_exact(&mut self, len: usize, label: &str) -> Result<&'a [u8], JsValue> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| JsValue::from_str("EntryV0 storage offset overflow"))?;
        if end > self.bytes.len() {
            return Err(JsValue::from_str(&format!(
                "Unexpected end of EntryV0 storage while reading {label}"
            )));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn read_u8(&mut self, label: &str) -> Result<u8, JsValue> {
        Ok(self.read_exact(1, label)?[0])
    }

    fn read_u32(&mut self, label: &str) -> Result<u32, JsValue> {
        let bytes = self.read_exact(4, label)?;
        Ok(u32::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| JsValue::from_str("Expected u32 bytes"))?,
        ))
    }

    fn read_u64(&mut self, label: &str) -> Result<u64, JsValue> {
        let bytes = self.read_exact(8, label)?;
        Ok(u64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| JsValue::from_str("Expected u64 bytes"))?,
        ))
    }

    fn read_bytes(&mut self, label: &str) -> Result<&'a [u8], JsValue> {
        let len = self.read_u32(label)? as usize;
        self.read_exact(len, label)
    }

    fn read_string(&mut self, label: &str) -> Result<String, JsValue> {
        let bytes = self.read_bytes(label)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| JsValue::from_str(&format!("Expected UTF-8 string for {label}")))
    }
}

fn read_plain_decrypted_thing_bytes<'a>(
    reader: &mut BorshReader<'a>,
    label: &str,
) -> Result<&'a [u8], JsValue> {
    if reader.read_u8(label)? != 0 {
        return Err(JsValue::from_str(
            "Only plaintext EntryV0 storage can be verified natively",
        ));
    }
    if reader.read_u8(label)? != 0 {
        return Err(JsValue::from_str(
            "Only decrypted EntryV0 storage can be verified natively",
        ));
    }
    reader.read_bytes(label)
}

fn parse_plain_entry_v0_storage(bytes: &[u8]) -> Result<ParsedPlainEntryV0Storage<'_>, JsValue> {
    let mut reader = BorshReader::new(bytes);
    if reader.read_u8("entry variant")? != 0 {
        return Err(JsValue::from_str("Expected EntryV0 variant"));
    }
    let meta = read_plain_decrypted_thing_bytes(&mut reader, "entry meta")?;
    let payload = read_plain_decrypted_thing_bytes(&mut reader, "entry payload")?;
    reader.read_exact(4, "entry reserved bytes")?;
    if reader.read_u8("entry signatures option")? != 1 {
        return Err(JsValue::from_str("Expected EntryV0 signatures"));
    }
    if reader.read_u8("signatures variant")? != 0 {
        return Err(JsValue::from_str("Expected Signatures variant"));
    }
    if reader.read_u32("signatures length")? != 1 {
        return Err(JsValue::from_str(
            "Expected exactly one EntryV0 signature for native verification",
        ));
    }
    let signature_with_key =
        read_plain_decrypted_thing_bytes(&mut reader, "entry signature with key")?;
    if reader.read_u8("entry hash option")? != 0 {
        return Err(JsValue::from_str(
            "Expected EntryV0 hash option to be empty",
        ));
    }
    if !reader.is_done() {
        return Err(JsValue::from_str(
            "Unexpected trailing EntryV0 storage bytes",
        ));
    }

    Ok(ParsedPlainEntryV0Storage {
        meta,
        payload,
        signature_with_key,
    })
}

fn parse_plain_entry_v0_storage_signature(
    bytes: &[u8],
) -> Result<ParsedEntryV0StorageSignature, JsValue> {
    let storage = parse_plain_entry_v0_storage(bytes)?;
    let parsed_signature = parse_plain_signature_with_key(storage.signature_with_key)?;

    Ok(ParsedEntryV0StorageSignature {
        signable: encode_entry_v0_parts_unsigned_for_signing(storage.meta, storage.payload),
        signature: parsed_signature.signature,
        public_key: parsed_signature.public_key,
    })
}

fn parse_plain_signature_with_key(bytes: &[u8]) -> Result<ParsedSignatureWithKey, JsValue> {
    let mut signature_reader = BorshReader::new(bytes);
    if signature_reader.read_u8("signature variant")? != 0 {
        return Err(JsValue::from_str("Expected SignatureWithKey variant"));
    }
    let signature = signature_reader.read_bytes("signature bytes")?.to_vec();
    if signature_reader.read_u8("signature public key variant")? != 0 {
        return Err(JsValue::from_str(
            "Only Ed25519 EntryV0 signatures can be verified natively",
        ));
    }
    let public_key = signature_reader
        .read_exact(32, "signature public key")?
        .to_vec();
    if signature_reader.read_u8("signature prehash")? != 0 {
        return Err(JsValue::from_str(
            "Only non-prehashed EntryV0 signatures can be verified natively",
        ));
    }
    if !signature_reader.is_done() {
        return Err(JsValue::from_str(
            "Unexpected trailing SignatureWithKey bytes",
        ));
    }

    Ok(ParsedSignatureWithKey {
        signature,
        public_key,
    })
}

fn read_string_vec(reader: &mut BorshReader<'_>, label: &str) -> Result<Vec<String>, JsValue> {
    let len = reader.read_u32(label)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(reader.read_string(label)?);
    }
    Ok(values)
}

fn read_optional_bytes(
    reader: &mut BorshReader<'_>,
    label: &str,
) -> Result<Option<Vec<u8>>, JsValue> {
    match reader.read_u8(label)? {
        0 => Ok(None),
        1 => Ok(Some(reader.read_bytes(label)?.to_vec())),
        _ => Err(JsValue::from_str(&format!(
            "Expected optional bytes tag for {label}"
        ))),
    }
}

fn parse_raw_entry_v0_meta(bytes: &[u8]) -> Result<ParsedRawEntryV0Meta, JsValue> {
    let mut reader = BorshReader::new(bytes);
    if reader.read_u8("meta variant")? != 0 {
        return Err(JsValue::from_str("Expected EntryV0 meta variant"));
    }
    if reader.read_u8("clock variant")? != 0 {
        return Err(JsValue::from_str("Expected LamportClock variant"));
    }
    let clock_id = reader.read_bytes("clock id")?.to_vec();
    if reader.read_u8("timestamp variant")? != 0 {
        return Err(JsValue::from_str("Expected Timestamp variant"));
    }
    let wall_time = reader.read_u64("timestamp wall time")?;
    let logical = reader.read_u32("timestamp logical")?;
    let gid = reader.read_string("meta gid")?;
    let next = read_string_vec(&mut reader, "meta next")?;
    let entry_type = reader.read_u8("meta type")?;
    let meta_data = read_optional_bytes(&mut reader, "meta data")?;
    if !reader.is_done() {
        return Err(JsValue::from_str("Unexpected trailing EntryV0 meta bytes"));
    }
    Ok(ParsedRawEntryV0Meta {
        clock_id,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
    })
}

fn parse_raw_entry_v0_payload(bytes: &[u8]) -> Result<ParsedRawEntryV0Payload, JsValue> {
    let mut reader = BorshReader::new(bytes);
    if reader.read_u8("payload variant")? != 0 {
        return Err(JsValue::from_str("Expected EntryV0 payload variant"));
    }
    let data_len = reader.read_bytes("payload data")?.len();
    if !reader.is_done() {
        return Err(JsValue::from_str(
            "Unexpected trailing EntryV0 payload bytes",
        ));
    }
    Ok(ParsedRawEntryV0Payload { data_len })
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
    let signature_with_key = signature
        .as_ref()
        .map(|signature| encode_signature_with_key(signature));
    encode_entry_v0_parts_with_signature_bytes(meta, payload, signature_with_key.as_deref())
}

fn encode_entry_v0_parts_unsigned_for_signing(meta: &[u8], payload: &[u8]) -> Vec<u8> {
    encode_entry_v0_parts_with_signature_bytes_and_extra_capacity(
        meta,
        payload,
        None,
        SIGNED_ENTRY_EXTRA_CAPACITY,
    )
}

fn encode_entry_v0_payload_data_unsigned_for_signing(meta: &[u8], payload_data: &[u8]) -> Vec<u8> {
    let payload_len = 1 + 4 + payload_data.len();
    let mut out = Vec::with_capacity(
        1 + decrypted_thing_encoded_len(meta.len())
            + decrypted_thing_encoded_len(payload_len)
            + 4
            + 1
            + 1
            + SIGNED_ENTRY_EXTRA_CAPACITY,
    );
    write_u8(&mut out, 0); // EntryV0 variant
    write_decrypted_thing(&mut out, meta);
    write_u8(&mut out, 0); // MaybeEncrypted variant
    write_u8(&mut out, 0); // DecryptedThing variant
    write_u32(&mut out, payload_len as u32);
    write_u8(&mut out, 0); // Payload variant
    write_bytes(&mut out, payload_data);
    out.extend_from_slice(&[0, 0, 0, 0]); // reserved
    write_u8(&mut out, 0); // signatures option
    write_u8(&mut out, 0); // hash option
    out
}

fn encode_entry_v0_parts_with_signature_bytes(
    meta: &[u8],
    payload: &[u8],
    signature_with_key: Option<&[u8]>,
) -> Vec<u8> {
    encode_entry_v0_parts_with_signature_bytes_and_extra_capacity(
        meta,
        payload,
        signature_with_key,
        0,
    )
}

fn encode_entry_v0_parts_with_signature_bytes_and_extra_capacity(
    meta: &[u8],
    payload: &[u8],
    signature_with_key: Option<&[u8]>,
    extra_capacity: usize,
) -> Vec<u8> {
    let signature_len = signature_with_key
        .map(|signature_with_key| 1 + 4 + decrypted_thing_encoded_len(signature_with_key.len()))
        .unwrap_or(0);
    let mut out = Vec::with_capacity(
        1 + decrypted_thing_encoded_len(meta.len())
            + decrypted_thing_encoded_len(payload.len())
            + 4
            + 1
            + signature_len
            + 1
            + extra_capacity,
    );
    write_u8(&mut out, 0); // EntryV0 variant
    write_decrypted_thing(&mut out, meta);
    write_decrypted_thing(&mut out, payload);
    out.extend_from_slice(&[0, 0, 0, 0]); // reserved
    match signature_with_key {
        Some(signature_with_key) => {
            write_u8(&mut out, 1);
            write_signatures_encoded(&mut out, signature_with_key);
        }
        None => write_u8(&mut out, 0),
    }
    write_u8(&mut out, 0); // hash option
    out
}

fn signable_entry_to_signed_storage(
    mut signable_entry: Vec<u8>,
    signature_with_key: &[u8],
) -> Vec<u8> {
    debug_assert!(signable_entry.len() >= 2);
    signable_entry.truncate(signable_entry.len().saturating_sub(2));
    signable_entry.reserve(1 + 1 + 4 + decrypted_thing_encoded_len(signature_with_key.len()) + 1);
    write_u8(&mut signable_entry, 1);
    write_signatures_encoded(&mut signable_entry, signature_with_key);
    write_u8(&mut signable_entry, 0); // hash option
    signable_entry
}

fn encode_meta(input: &EntryV0EncodeInput) -> Vec<u8> {
    encode_meta_parts(
        &input.clock_id,
        input.wall_time,
        input.logical,
        &input.gid,
        &input.next,
        input.entry_type,
        input.meta_data.as_deref(),
    )
}

fn encode_meta_parts(
    clock_id: &[u8],
    wall_time: u64,
    logical: u32,
    gid: &str,
    next: &[String],
    entry_type: u8,
    meta_data: Option<&[u8]>,
) -> Vec<u8> {
    let next_bytes = next.iter().map(|next| 4 + next.len()).sum::<usize>();
    let meta_data_bytes = meta_data.map(|data| 4 + data.len()).unwrap_or(0);
    let mut out = Vec::with_capacity(
        1 + 1
            + 4
            + clock_id.len()
            + 1
            + 8
            + 4
            + 4
            + gid.len()
            + 4
            + next_bytes
            + 1
            + 1
            + meta_data_bytes,
    );
    write_u8(&mut out, 0); // Meta variant
    write_clock(&mut out, clock_id, wall_time, logical);
    write_string(&mut out, gid);
    write_u32(&mut out, next.len() as u32);
    for next_hash in next {
        write_string(&mut out, next_hash);
    }
    write_u8(&mut out, entry_type);
    match meta_data {
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
    let mut out = Vec::with_capacity(1 + 4 + data.len());
    write_u8(&mut out, 0); // Payload variant
    write_bytes(&mut out, data);
    out
}

fn write_signatures_encoded(out: &mut Vec<u8>, signature_with_key: &[u8]) {
    write_u8(out, 0); // Signatures variant
    write_u32(out, 1);
    write_decrypted_thing(out, signature_with_key);
}

fn encode_signature_with_key(signature: &SignatureInput) -> Vec<u8> {
    encode_signature_with_key_parts(
        &signature.signature,
        &signature.public_key,
        signature.prehash,
    )
}

fn encode_signature_with_key_parts(signature: &[u8], public_key: &[u8], prehash: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + signature.len() + 1 + public_key.len() + 1);
    write_u8(&mut out, 0); // SignatureWithKey variant
    write_bytes(&mut out, signature);
    write_u8(&mut out, 0); // Ed25519PublicKey variant
    out.extend_from_slice(public_key);
    write_u8(&mut out, prehash);
    out
}

fn decrypted_thing_encoded_len(data_len: usize) -> usize {
    2 + 4 + data_len
}

fn sign_ed25519_raw(
    private_key: &[u8],
    public_key: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, JsValue> {
    let signing_key = validate_ed25519_keypair(private_key, public_key)?;
    Ok(sign_ed25519_with_key(&signing_key, data).to_vec())
}

fn sign_ed25519_with_key(signing_key: &SigningKey, data: &[u8]) -> [u8; 64] {
    signing_key.sign(data).to_bytes()
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

#[cfg(feature = "crypto-bench-candidates")]
fn validate_compact_ed25519_keypair(
    private_key: &[u8],
    public_key: &[u8],
) -> Result<(CompactSecretKey, CompactPublicKey), JsValue> {
    if private_key.len() != 32 {
        return Err(JsValue::from_str("Expected Ed25519 private key length 32"));
    }
    if public_key.len() != 32 {
        return Err(JsValue::from_str("Expected Ed25519 public key length 32"));
    }
    let mut secret_key_bytes = [0u8; CompactSecretKey::BYTES];
    secret_key_bytes[..32].copy_from_slice(private_key);
    secret_key_bytes[32..].copy_from_slice(public_key);
    let secret_key = CompactSecretKey::new(secret_key_bytes);
    let public_key = CompactPublicKey::from_slice(public_key)
        .map_err(|_| JsValue::from_str("Invalid ed25519-compact public key"))?;
    secret_key
        .validate_public_key(&public_key)
        .map_err(|_| JsValue::from_str("Ed25519 public key does not match private key"))?;
    Ok((secret_key, public_key))
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

fn reference_gid_rows_to_array(values: Vec<(String, String)>) -> Array {
    let out = Array::new();
    for (hash, gid) in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&hash));
        row.push(&JsValue::from_str(&gid));
        out.push(&row);
    }
    out
}

fn reference_gid_flat_rows_to_array(values: Vec<(u32, String, String)>) -> Array {
    let out = Array::new();
    for (position, hash, gid) in values {
        let row = Array::new();
        row.push(&JsValue::from_f64(position as f64));
        row.push(&JsValue::from_str(&hash));
        row.push(&JsValue::from_str(&gid));
        out.push(&row);
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

pub fn decode_absolute_replica_data_u32(data: Option<&[u8]>) -> Option<u32> {
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

fn log_trim_entries_to_rows(values: Vec<LogIndexEntry>) -> Array {
    let out = Array::new();
    for entry in values {
        let row = Array::new();
        row.push(&JsValue::from_str(&entry.hash));
        row.push(&JsValue::from_str(&entry.gid));
        row.push(&JsValue::from_str(&entry.wall_time.to_string()));
        row.push(&JsValue::from_f64(entry.logical as f64));
        row.push(&JsValue::from_f64(entry.entry_type as f64));
        row.push(&strings_to_array(entry.next));
        row.push(&JsValue::from_f64(entry.payload_size as f64));
        row.push(&JsValue::from_bool(entry.head));
        match entry.data {
            Some(data) => row.push(&Uint8Array::from(data.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
        out.push(&row);
    }
    out
}

fn log_optional_entry_metadata_to_rows(values: Vec<Option<LogEntryMetadata>>) -> Array {
    let out = Array::new();
    for value in values {
        let Some((hash, gid, data, replicas)) = value else {
            out.push(&JsValue::UNDEFINED);
            continue;
        };
        let row = Array::new();
        row.push(&JsValue::from_str(&hash));
        row.push(&JsValue::from_str(&gid));
        match data {
            Some(data) => row.push(&Uint8Array::from(data.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
        match replicas {
            Some(replicas) => row.push(&JsValue::from_f64(replicas as f64)),
            None => row.push(&JsValue::UNDEFINED),
        };
        out.push(&row);
    }
    out
}

fn log_optional_entry_metadata_hints_to_rows(values: Vec<Option<LogEntryMetadata>>) -> Array {
    let out = Array::new();
    for value in values {
        let Some((hash, gid, data, replicas)) = value else {
            out.push(&JsValue::UNDEFINED);
            continue;
        };
        let row = Array::new();
        row.push(&JsValue::from_str(&hash));
        row.push(&JsValue::from_str(&gid));
        match replicas {
            Some(replicas) => row.push(&JsValue::from_f64(replicas as f64)),
            None => row.push(&JsValue::UNDEFINED),
        };
        match data.filter(|_| replicas.is_none()) {
            Some(data) => row.push(&Uint8Array::from(data.as_slice())),
            None => row.push(&JsValue::UNDEFINED),
        };
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
    use super::{
        encode_entry_v0_parts_unsigned_for_signing, encode_entry_v0_parts_with_signature_bytes,
        encode_entry_v0_payload_data_unsigned_for_signing, encode_payload,
        signable_entry_to_signed_storage, trim_oldest_log_entry_hashes_core, JoinPlan,
        LogGraphIndex, LogIndexEntry, NativeLogBlockStore,
    };

    const APPEND: u8 = 0;
    const CUT: u8 = 1;

    #[test]
    fn signed_storage_reused_from_signable_entry_matches_full_encoder() {
        let meta = b"encoded-meta".to_vec();
        let payload = b"encoded-payload".to_vec();
        let signature_with_key = (0..96).map(|value| value as u8).collect::<Vec<_>>();

        let signable = encode_entry_v0_parts_with_signature_bytes(&meta, &payload, None);
        let optimized = signable_entry_to_signed_storage(signable, &signature_with_key);
        let expected =
            encode_entry_v0_parts_with_signature_bytes(&meta, &payload, Some(&signature_with_key));

        assert_eq!(optimized, expected);
    }

    #[test]
    fn direct_payload_signable_encoding_matches_payload_parts_encoder() {
        let meta = b"encoded-meta".to_vec();
        let payload_data = b"document-payload".to_vec();
        let payload = encode_payload(&payload_data);

        assert_eq!(
            encode_entry_v0_payload_data_unsigned_for_signing(&meta, &payload_data),
            encode_entry_v0_parts_unsigned_for_signing(&meta, &payload)
        );
    }

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
    fn deletes_many_entries() {
        let mut index = LogGraphIndex::new();
        index.put(entry("a", "g", &[], 1));
        index.put(entry("b", "g", &["a"], 2));
        index.put(entry("c", "g", &["b"], 3));

        assert_eq!(index.delete_many(&["b".to_string(), "c".to_string()]), 2);
        assert!(!index.has("b"));
        assert!(!index.has("c"));
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
    fn puts_single_append_entry_without_one_item_chain_batch() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put_append_entry(
            LogIndexEntry::new("a", "g", vec!["root".to_string()], APPEND, 2, 0, 1, true),
            &["root".to_string()],
        );

        assert_eq!(index.heads(None), vec!["a"]);
        assert_eq!(index.children("root"), vec!["a"]);

        let mut cut_index = LogGraphIndex::new();
        cut_index.put(entry("root", "g", &[], 1));
        cut_index.put_append_entry(
            LogIndexEntry::new("cut", "g", vec!["root".to_string()], CUT, 2, 0, 1, true),
            &["root".to_string()],
        );

        assert_eq!(cut_index.heads(None), vec!["root", "cut"]);
        assert_eq!(cut_index.children("root"), vec!["cut"]);
    }

    #[test]
    fn prune_metadata_omits_data_when_replicas_decode() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new_with_data(
            "a",
            "g",
            vec![],
            APPEND,
            1,
            0,
            1,
            true,
            Some(vec![0, 2, 0, 0, 0]),
        ));
        index.put(LogIndexEntry::new_with_data(
            "b",
            "g",
            vec![],
            APPEND,
            2,
            0,
            1,
            true,
            Some(vec![9, 1, 2]),
        ));

        let metadata =
            index.entry_prune_metadata_batch(&["a".to_string(), "b".to_string(), "c".to_string()]);

        assert_eq!(metadata[0], Some(("g".to_string(), None, Some(2))));
        assert_eq!(
            metadata[1],
            Some(("g".to_string(), Some(vec![9, 1, 2]), None))
        );
        assert_eq!(metadata[2], None);

        let confirm_metadata =
            index.entry_prune_confirm_metadata_batch(&["a".to_string(), "b".to_string()]);
        assert_eq!(confirm_metadata[0], Some(("g".to_string(), Some(2))));
        assert_eq!(confirm_metadata[1], Some(("g".to_string(), None)));
        assert_eq!(
            index.entry_prune_confirm_metadata_ref("a"),
            Some(("g", Some(2)))
        );
    }

    #[test]
    fn puts_join_batch_without_rechecking_internal_heads() {
        let mut index = LogGraphIndex::new();
        index.put(entry("root", "g", &[], 1));
        index.put_join_batch(vec![
            LogIndexEntry::new("a", "g", vec!["root".to_string()], APPEND, 2, 0, 1, false),
            LogIndexEntry::new("b", "g", vec!["a".to_string()], APPEND, 3, 0, 1, true),
            LogIndexEntry::new("c", "g", vec!["root".to_string()], APPEND, 4, 0, 1, true),
        ]);

        assert_eq!(index.heads(None), vec!["b", "c"]);
        assert_eq!(index.children("root"), vec!["a", "c"]);
        assert_eq!(index.children("a"), vec!["b"]);

        let mut cut_index = LogGraphIndex::new();
        cut_index.put(entry("root", "g", &[], 1));
        cut_index.put_join_batch(vec![LogIndexEntry::new(
            "cut",
            "g",
            vec!["root".to_string()],
            CUT,
            2,
            0,
            1,
            true,
        )]);

        assert_eq!(cut_index.heads(None), vec!["root", "cut"]);
        assert_eq!(cut_index.children("root"), vec!["cut"]);
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
    fn returns_oldest_entries_by_clock_order() {
        let mut index = LogGraphIndex::new();
        index.put(LogIndexEntry::new("b", "g", vec![], APPEND, 2, 0, 1, true));
        index.put(LogIndexEntry::new("a", "g", vec![], APPEND, 1, 1, 1, true));
        index.put(LogIndexEntry::new("c", "g", vec![], APPEND, 1, 0, 1, true));

        assert_eq!(
            index
                .oldest_entries(2)
                .into_iter()
                .map(|entry| entry.hash)
                .collect::<Vec<_>>(),
            vec!["c", "a"]
        );
    }

    #[test]
    fn trims_oldest_hashes_without_materializing_entries() {
        let mut index = LogGraphIndex::new();
        let mut blocks = NativeLogBlockStore::new();
        for (hash, wall_time, logical) in [("b", 2, 0), ("a", 1, 1), ("c", 1, 0)] {
            index.put(LogIndexEntry::new(
                hash,
                "g",
                vec![],
                APPEND,
                wall_time,
                logical,
                1,
                true,
            ));
            blocks.put(hash.to_string(), vec![wall_time as u8, logical as u8]);
        }

        let trimmed = trim_oldest_log_entry_hashes_core(&mut index, &mut blocks, 1);

        assert_eq!(trimmed, vec!["c", "a"]);
        assert!(!index.has("c"));
        assert!(!index.has("a"));
        assert!(index.has("b"));
        assert!(!blocks.has("c"));
        assert!(!blocks.has("a"));
        assert!(blocks.has("b"));
    }

    #[test]
    fn trims_single_oldest_hash_without_batch_delete() {
        let mut index = LogGraphIndex::new();
        let mut blocks = NativeLogBlockStore::new();
        for (hash, wall_time) in [("a", 1), ("b", 2), ("c", 3)] {
            index.put(LogIndexEntry::new(
                hash,
                "g",
                vec![],
                APPEND,
                wall_time,
                0,
                1,
                true,
            ));
            blocks.put(hash.to_string(), vec![wall_time as u8]);
        }

        let trimmed = trim_oldest_log_entry_hashes_core(&mut index, &mut blocks, 2);

        assert_eq!(trimmed, vec!["a"]);
        assert!(!index.has("a"));
        assert!(index.has("b"));
        assert!(index.has("c"));
        assert!(!blocks.has("a"));
        assert!(blocks.has("b"));
        assert!(blocks.has("c"));
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
    fn batch_plans_join_cut_coverage() {
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
            index.plan_join_batch(
                &["old".to_string(), "new".to_string()],
                &[vec!["missing".to_string()], vec!["missing".to_string()]],
                &[APPEND, APPEND],
                false,
                Some((&["g".to_string(), "g".to_string()], &[1, 3], &[0, 0],)),
            ),
            vec![
                JoinPlan {
                    skip: false,
                    missing_parents: Vec::new(),
                    cut_checked: true,
                    covered_by_cut: true
                },
                JoinPlan {
                    skip: false,
                    missing_parents: vec!["missing".to_string()],
                    cut_checked: true,
                    covered_by_cut: false
                }
            ]
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
