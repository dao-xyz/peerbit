use indexmap::{IndexMap, IndexSet};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

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

pub type LogEntryMetadata = (String, String, Option<Vec<u8>>, Option<u32>);
pub type LogEntryPruneMetadata = (String, Option<Vec<u8>>, Option<u32>);
pub type LogEntryPruneConfirmMetadata = (String, Option<u32>);

pub fn decode_absolute_replica_data_u32(data: Option<&[u8]>) -> Option<u32> {
    let data = data?;
    if data.len() != 5 || data[0] != 0 {
        return None;
    }
    Some(u32::from_le_bytes([data[1], data[2], data[3], data[4]]))
}
