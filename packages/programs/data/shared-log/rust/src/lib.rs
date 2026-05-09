use indexmap::{IndexMap, IndexSet};
use js_sys::Array;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use wasm_bindgen::prelude::*;

const MODE_NON_STRICT: u8 = 0;
const MAX_U32: u64 = u32::MAX as u64;
const MAX_U64: u64 = u64::MAX;
const CONTAINMENT_BUCKETS: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Resolution {
    U32,
    U64,
}

impl Resolution {
    fn from_str(value: &str) -> Self {
        match value {
            "u64" => Self::U64,
            _ => Self::U32,
        }
    }

    fn max_value(self) -> u64 {
        match self {
            Self::U32 => MAX_U32,
            Self::U64 => MAX_U64,
        }
    }

    fn domain_size(self) -> u128 {
        self.max_value() as u128 + 1
    }

    fn containment_bucket(self, value: u64) -> usize {
        let bucket = (value as u128 * CONTAINMENT_BUCKETS as u128) / self.domain_size();
        (bucket as usize).min(CONTAINMENT_BUCKETS - 1)
    }

    fn number_from_digest(self, digest: &[u8]) -> u64 {
        match self {
            Self::U32 => u32::from_le_bytes(digest[0..4].try_into().expect("sha256 digest")) as u64,
            Self::U64 => u64::from_le_bytes(digest[0..8].try_into().expect("sha256 digest")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationRange {
    id: String,
    hash: String,
    timestamp: u64,
    start1: u64,
    end1: u64,
    start2: u64,
    end2: u64,
    width: u64,
    mode: u8,
}

impl ReplicationRange {
    pub fn new(
        id: impl Into<String>,
        hash: impl Into<String>,
        timestamp: u64,
        start1: u64,
        end1: u64,
        start2: u64,
        end2: u64,
        width: u64,
        mode: u8,
    ) -> Self {
        Self {
            id: id.into(),
            hash: hash.into(),
            timestamp,
            start1,
            end1,
            start2,
            end2,
            width,
            mode,
        }
    }

    fn contains(&self, point: u64) -> bool {
        (point >= self.start1 && point < self.end1) || (point >= self.start2 && point < self.end2)
    }

    fn is_matured(&self, now: u64, role_age_ms: u64) -> bool {
        now >= self.timestamp && now - self.timestamp >= role_age_ms
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderSample {
    pub hash: String,
    pub intersecting: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SampleOptions {
    pub role_age_ms: u64,
    pub now: u64,
    pub only_intersecting: bool,
    pub unique_replicators: Option<IndexSet<String>>,
    pub peer_filter: Option<HashSet<String>>,
}

pub struct RangePlanner {
    resolution: Resolution,
    ranges: IndexMap<String, ReplicationRange>,
    by_start1: BTreeSet<RangeIndexKey>,
    by_end2: BTreeSet<RangeIndexKey>,
    containment_buckets: Vec<IndexSet<String>>,
    peer_ranges: IndexMap<String, PeerRangeStats>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RangeIndexKey {
    value: u64,
    timestamp: u64,
    hash: String,
    id: String,
}

impl RangeIndexKey {
    fn start(range: &ReplicationRange) -> Self {
        Self {
            value: range.start1,
            timestamp: range.timestamp,
            hash: range.hash.clone(),
            id: range.id.clone(),
        }
    }

    fn end(range: &ReplicationRange) -> Self {
        Self {
            value: range.end2,
            timestamp: range.timestamp,
            hash: range.hash.clone(),
            id: range.id.clone(),
        }
    }

    fn min_at(value: u64) -> Self {
        Self {
            value,
            timestamp: 0,
            hash: String::new(),
            id: String::new(),
        }
    }

    fn max_at(value: u64) -> Self {
        let max_string = char::MAX.to_string();
        Self {
            value,
            timestamp: u64::MAX,
            hash: max_string.clone(),
            id: max_string,
        }
    }
}

impl Ord for RangeIndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value
            .cmp(&other.value)
            .then_with(|| self.timestamp.cmp(&other.timestamp))
            .then_with(|| self.hash.cmp(&other.hash))
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for RangeIndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PeerRangeKey {
    timestamp: u64,
    id: String,
}

impl PeerRangeKey {
    fn from_range(range: &ReplicationRange) -> Self {
        Self {
            timestamp: range.timestamp,
            id: range.id.clone(),
        }
    }

    fn is_matured(&self, now: u64, role_age_ms: u64) -> bool {
        now >= self.timestamp && now - self.timestamp >= role_age_ms
    }
}

impl Ord for PeerRangeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for PeerRangeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Default)]
struct PeerRangeStats {
    all: BTreeSet<PeerRangeKey>,
    non_strict: BTreeSet<PeerRangeKey>,
}

impl PeerRangeStats {
    fn insert(&mut self, range: &ReplicationRange) {
        let key = PeerRangeKey::from_range(range);
        self.all.insert(key.clone());
        if range.mode == MODE_NON_STRICT {
            self.non_strict.insert(key);
        }
    }

    fn remove(&mut self, range: &ReplicationRange) {
        let key = PeerRangeKey::from_range(range);
        self.all.remove(&key);
        if range.mode == MODE_NON_STRICT {
            self.non_strict.remove(&key);
        }
    }

    fn is_empty(&self) -> bool {
        self.all.is_empty()
    }

    fn has_matured_range(&self, now: u64, role_age_ms: u64, include_strict: bool) -> bool {
        let ranges = if include_strict {
            &self.all
        } else {
            &self.non_strict
        };
        ranges
            .first()
            .is_some_and(|range| range.is_matured(now, role_age_ms))
    }
}

impl RangePlanner {
    pub fn new(resolution: &str) -> Self {
        Self {
            resolution: Resolution::from_str(resolution),
            ranges: IndexMap::new(),
            by_start1: BTreeSet::new(),
            by_end2: BTreeSet::new(),
            containment_buckets: vec![IndexSet::new(); CONTAINMENT_BUCKETS],
            peer_ranges: IndexMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn clear(&mut self) {
        self.ranges.clear();
        self.by_start1.clear();
        self.by_end2.clear();
        for bucket in &mut self.containment_buckets {
            bucket.clear();
        }
        self.peer_ranges.clear();
    }

    pub fn put(&mut self, range: ReplicationRange) {
        if let Some(previous) = self.ranges.get(&range.id).cloned() {
            self.unindex_range(&previous);
        }
        self.index_range(&range);
        self.ranges.insert(range.id.clone(), range);
    }

    pub fn delete(&mut self, id: &str) -> bool {
        match self.ranges.swap_remove(id) {
            Some(range) => {
                self.unindex_range(&range);
                true
            }
            None => false,
        }
    }

    pub fn get_samples(&self, cursors: &[u64], options: &SampleOptions) -> Vec<LeaderSample> {
        let mut leaders: IndexMap<String, bool> = IndexMap::new();
        let mut matured = 0usize;
        let mut unique_visited: IndexSet<String> = IndexSet::new();

        for (i, point) in cursors.iter().copied().enumerate() {
            for id in self.containment_buckets[self.resolution.containment_bucket(point)].iter() {
                let Some(range) = self.ranges.get(id) else {
                    continue;
                };
                if !self.include_range(range, options.peer_filter.as_ref())
                    || !range.contains(point)
                {
                    continue;
                }
                unique_visited.insert(range.hash.clone());
                match leaders.get_mut(&range.hash) {
                    Some(intersecting) => {
                        *intersecting = true;
                    }
                    None => {
                        if range.is_matured(options.now, options.role_age_ms) {
                            matured += 1;
                        }
                        leaders.insert(range.hash.clone(), true);
                    }
                }
            }

            if let Some(unique_replicators) = &options.unique_replicators {
                if !unique_replicators.is_empty()
                    && (unique_replicators.len() == leaders.len()
                        || unique_replicators.len() == unique_visited.len())
                {
                    break;
                }
            }

            if options.only_intersecting || matured > i {
                continue;
            }

            let mut seen_closest_ids = HashSet::new();
            while let Some(range) =
                self.closest_non_strict(point, options.peer_filter.as_ref(), &seen_closest_ids)
            {
                seen_closest_ids.insert(range.id.clone());
                unique_visited.insert(range.hash.clone());
                if !range.is_matured(options.now, options.role_age_ms) {
                    continue;
                }
                if !leaders.contains_key(&range.hash) {
                    matured += 1;
                    leaders.insert(range.hash.clone(), false);
                }
                if matured > i {
                    break;
                }
            }
        }

        leaders
            .into_iter()
            .map(|(hash, intersecting)| LeaderSample { hash, intersecting })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_leaders(
        &self,
        cursors: &[u64],
        replicas: usize,
        options: &SampleOptions,
        expand_peer_filter: bool,
        self_hash: &str,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Vec<LeaderSample> {
        let options = prepare_find_leader_options(
            self,
            options,
            replicas,
            expand_peer_filter,
            self_hash,
            include_self,
        );
        find_leaders_with_prepared_options(
            self,
            cursors,
            replicas,
            &options,
            full_replica_fallback,
            include_strict_full_replica,
        )
    }

    pub fn get_grid(&self, from: u64, count: usize) -> Vec<u64> {
        if count == 0 {
            return Vec::new();
        }

        match self.resolution {
            Resolution::U32 => {
                let max = MAX_U32 as f64;
                (0..count)
                    .map(|index| {
                        ((from as f64 + (index as f64 * max) / count as f64).round() as u64)
                            % MAX_U32
                    })
                    .collect()
            }
            Resolution::U64 => {
                let max = MAX_U64 as u128;
                (0..count)
                    .map(|index| {
                        ((from as u128 + (index as u128 * max) / count as u128) % max) as u64
                    })
                    .collect()
            }
        }
    }

    pub fn hash_gid(&self, gid: &str) -> u64 {
        let mut bytes = Vec::with_capacity(4 + gid.len());
        bytes.extend_from_slice(&(gid.len() as u32).to_le_bytes());
        bytes.extend_from_slice(gid.as_bytes());
        let digest = Sha256::digest(bytes);
        self.resolution.number_from_digest(&digest)
    }

    pub fn get_gid_coordinates(&self, gid: &str, count: usize) -> Vec<u64> {
        self.get_grid(self.hash_gid(gid), count)
    }

    pub fn get_full_replica_leaders(
        &self,
        replicas: usize,
        options: &SampleOptions,
        include_strict: bool,
    ) -> Option<Vec<LeaderSample>> {
        let mut leaders: IndexSet<String> = IndexSet::new();

        for (hash, stats) in self.peer_ranges.iter() {
            if let Some(peer_filter) = options.peer_filter.as_ref() {
                if !peer_filter.contains(hash) {
                    continue;
                }
            }
            if !stats.has_matured_range(options.now, options.role_age_ms, include_strict) {
                continue;
            }

            leaders.insert(hash.clone());
            if leaders.len() > replicas {
                return None;
            }
        }

        if leaders.is_empty() {
            return None;
        }

        Some(
            leaders
                .into_iter()
                .map(|hash| LeaderSample {
                    hash,
                    intersecting: true,
                })
                .collect(),
        )
    }

    pub fn include_matured_peers(
        &self,
        peer_filter: Option<IndexSet<String>>,
        replicas: usize,
        options: &SampleOptions,
        self_hash: &str,
        include_self: bool,
    ) -> Option<Vec<String>> {
        let mut peers = peer_filter?;
        if peers.len() > replicas {
            return Some(peers.into_iter().collect());
        }

        for (hash, stats) in self.peer_ranges.iter() {
            if !include_self && hash == self_hash {
                continue;
            }
            if stats.has_matured_range(options.now, options.role_age_ms, true) {
                peers.insert(hash.clone());
            }
        }

        Some(peers.into_iter().collect())
    }

    fn include_range(
        &self,
        range: &ReplicationRange,
        peer_filter: Option<&HashSet<String>>,
    ) -> bool {
        if range.width == 0 {
            return false;
        }
        match peer_filter {
            Some(peer_filter) => peer_filter.contains(&range.hash),
            None => true,
        }
    }

    fn is_fallback_indexed(range: &ReplicationRange) -> bool {
        range.width > 0 && range.mode == MODE_NON_STRICT
    }

    fn is_containment_indexed(range: &ReplicationRange) -> bool {
        range.width > 0
    }

    fn index_range(&mut self, range: &ReplicationRange) {
        if Self::is_fallback_indexed(range) {
            self.by_start1.insert(RangeIndexKey::start(range));
            self.by_end2.insert(RangeIndexKey::end(range));
        }
        if Self::is_containment_indexed(range) {
            self.index_containment_range(range);
        }
        self.peer_ranges
            .entry(range.hash.clone())
            .or_default()
            .insert(range);
    }

    fn unindex_range(&mut self, range: &ReplicationRange) {
        if Self::is_fallback_indexed(range) {
            self.by_start1.remove(&RangeIndexKey::start(range));
            self.by_end2.remove(&RangeIndexKey::end(range));
        }
        if Self::is_containment_indexed(range) {
            self.unindex_containment_range(range);
        }
        if let Some(stats) = self.peer_ranges.get_mut(&range.hash) {
            stats.remove(range);
            if stats.is_empty() {
                self.peer_ranges.swap_remove(&range.hash);
            }
        }
    }

    fn index_containment_range(&mut self, range: &ReplicationRange) {
        self.index_containment_interval(&range.id, range.start1, range.end1);
        self.index_containment_interval(&range.id, range.start2, range.end2);
    }

    fn unindex_containment_range(&mut self, range: &ReplicationRange) {
        self.unindex_containment_interval(&range.id, range.start1, range.end1);
        self.unindex_containment_interval(&range.id, range.start2, range.end2);
    }

    fn index_containment_interval(&mut self, id: &str, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let first = self.resolution.containment_bucket(start);
        let last = self.resolution.containment_bucket(end - 1);
        for bucket in first..=last {
            self.containment_buckets[bucket].insert(id.to_string());
        }
    }

    fn unindex_containment_interval(&mut self, id: &str, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let first = self.resolution.containment_bucket(start);
        let last = self.resolution.containment_bucket(end - 1);
        for bucket in first..=last {
            self.containment_buckets[bucket].shift_remove(id);
        }
    }

    fn candidate_from_key(
        &self,
        key: &RangeIndexKey,
        peer_filter: Option<&HashSet<String>>,
        seen_ids: &HashSet<String>,
    ) -> Option<&ReplicationRange> {
        if seen_ids.contains(&key.id) {
            return None;
        }
        let range = self.ranges.get(&key.id)?;
        if !self.include_range(range, peer_filter) || range.mode != MODE_NON_STRICT {
            return None;
        }
        Some(range)
    }

    fn first_candidate_from_keys<'a, I>(
        &'a self,
        keys: I,
        peer_filter: Option<&HashSet<String>>,
        seen_ids: &HashSet<String>,
    ) -> Option<&'a ReplicationRange>
    where
        I: IntoIterator<Item = &'a RangeIndexKey>,
    {
        keys.into_iter()
            .find_map(|key| self.candidate_from_key(key, peer_filter, seen_ids))
    }

    fn closest_start_candidate(
        &self,
        point: u64,
        above: bool,
        peer_filter: Option<&HashSet<String>>,
        seen_ids: &HashSet<String>,
    ) -> Option<&ReplicationRange> {
        if above {
            self.first_candidate_from_keys(
                self.by_start1.range(RangeIndexKey::min_at(point)..),
                peer_filter,
                seen_ids,
            )
            .or_else(|| {
                self.first_candidate_from_keys(self.by_start1.iter(), peer_filter, seen_ids)
            })
        } else {
            self.first_candidate_from_keys(
                self.by_start1.range(..=RangeIndexKey::max_at(point)).rev(),
                peer_filter,
                seen_ids,
            )
            .or_else(|| {
                self.first_candidate_from_keys(self.by_start1.iter().rev(), peer_filter, seen_ids)
            })
        }
    }

    fn closest_end_candidate(
        &self,
        point: u64,
        above: bool,
        peer_filter: Option<&HashSet<String>>,
        seen_ids: &HashSet<String>,
    ) -> Option<&ReplicationRange> {
        if above {
            self.first_candidate_from_keys(
                self.by_end2.range(RangeIndexKey::min_at(point)..),
                peer_filter,
                seen_ids,
            )
            .or_else(|| self.first_candidate_from_keys(self.by_end2.iter(), peer_filter, seen_ids))
        } else {
            self.first_candidate_from_keys(
                self.by_end2.range(..=RangeIndexKey::max_at(point)).rev(),
                peer_filter,
                seen_ids,
            )
            .or_else(|| {
                self.first_candidate_from_keys(self.by_end2.iter().rev(), peer_filter, seen_ids)
            })
        }
    }

    fn closest_non_strict(
        &self,
        point: u64,
        peer_filter: Option<&HashSet<String>>,
        seen_ids: &HashSet<String>,
    ) -> Option<&ReplicationRange> {
        let max_value = self.resolution.max_value();
        let mut best: Option<&ReplicationRange> = None;

        for range in [
            self.closest_start_candidate(point, true, peer_filter, seen_ids),
            self.closest_start_candidate(point, false, peer_filter, seen_ids),
            self.closest_end_candidate(point, true, peer_filter, seen_ids),
            self.closest_end_candidate(point, false, peer_filter, seen_ids),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(previous) = best {
                if compare_closest(range, previous, point, max_value) == Ordering::Less {
                    best = Some(range);
                }
            } else {
                best = Some(range);
            }
        }

        best
    }
}

fn compare_closest(
    left: &ReplicationRange,
    right: &ReplicationRange,
    point: u64,
    max_value: u64,
) -> Ordering {
    closest_distance(left, point, max_value)
        .cmp(&closest_distance(right, point, max_value))
        .then_with(|| left.timestamp.cmp(&right.timestamp))
        .then_with(|| left.hash.cmp(&right.hash))
        .then_with(|| left.id.cmp(&right.id))
}

fn closest_distance(range: &ReplicationRange, point: u64, max_value: u64) -> u64 {
    circular_distance(range.start1, point, max_value)
        .min(circular_distance(range.end2, point, max_value))
}

fn circular_distance(from: u64, to: u64, max_value: u64) -> u64 {
    if from == to {
        return 0;
    }
    let diff = from.abs_diff(to);
    diff.min(max_value.saturating_sub(diff))
}

fn prepare_find_leader_options(
    planner: &RangePlanner,
    options: &SampleOptions,
    replicas: usize,
    expand_peer_filter: bool,
    self_hash: &str,
    include_self: bool,
) -> SampleOptions {
    let mut options = options.clone();

    if expand_peer_filter {
        options.peer_filter = planner
            .include_matured_peers(
                options
                    .peer_filter
                    .as_ref()
                    .map(|peers| IndexSet::from_iter(peers.iter().cloned())),
                replicas,
                &options,
                self_hash,
                include_self,
            )
            .map(HashSet::from_iter);
    }

    options
}

fn find_leaders_with_prepared_options(
    planner: &RangePlanner,
    cursors: &[u64],
    replicas: usize,
    options: &SampleOptions,
    full_replica_fallback: bool,
    include_strict_full_replica: bool,
) -> Vec<LeaderSample> {
    if full_replica_fallback {
        if let Some(leaders) =
            planner.get_full_replica_leaders(replicas, options, include_strict_full_replica)
        {
            return leaders;
        }
    }

    let mut options = options.clone();
    options.unique_replicators = options
        .peer_filter
        .as_ref()
        .map(|peers| IndexSet::from_iter(peers.iter().cloned()));

    planner.get_samples(cursors, &options)
}

#[allow(clippy::too_many_arguments)]
fn find_leaders_with_batch_caches(
    planner: &RangePlanner,
    cursors: &[u64],
    replicas: usize,
    base_options: &SampleOptions,
    prepared_options_by_replicas: &mut HashMap<usize, SampleOptions>,
    full_replica_leaders_by_replicas: &mut HashMap<usize, Option<Vec<LeaderSample>>>,
    expand_peer_filter: bool,
    self_hash: &str,
    include_self: bool,
    full_replica_fallback: bool,
    include_strict_full_replica: bool,
) -> Vec<LeaderSample> {
    let prepared_options = prepared_options_by_replicas
        .entry(replicas)
        .or_insert_with(|| {
            prepare_find_leader_options(
                planner,
                base_options,
                replicas,
                expand_peer_filter,
                self_hash,
                include_self,
            )
        });

    if full_replica_fallback {
        if let Some(leaders) = full_replica_leaders_by_replicas
            .entry(replicas)
            .or_insert_with(|| {
                planner.get_full_replica_leaders(
                    replicas,
                    prepared_options,
                    include_strict_full_replica,
                )
            })
            .clone()
        {
            return leaders;
        }
    }

    find_leaders_with_prepared_options(planner, cursors, replicas, prepared_options, false, true)
}

struct RepairDispatchBatch {
    entry_hashes: Vec<String>,
    entry_gids: Vec<String>,
    entry_requested_replicas: Vec<usize>,
    current_leader_batches: Vec<Vec<String>>,
    known_gid_peer_batches: Vec<Vec<String>>,
    known_entry_peer_batches: Vec<Vec<String>>,
    pending_modes: Vec<String>,
    pending_peers_by_mode: Vec<Vec<String>>,
    optimistic_peers_by_mode: Vec<Vec<Vec<String>>>,
    full_replica_repair_candidates: HashSet<String>,
    full_replica_repair_candidate_count: usize,
    self_hash: String,
}

fn plan_repair_dispatch_rows(batch: RepairDispatchBatch) -> Result<Array, JsValue> {
    let entry_count = batch.entry_hashes.len();

    ensure_same_len(entry_count, batch.entry_gids.len(), "repair entry gid")?;
    ensure_same_len(
        entry_count,
        batch.entry_requested_replicas.len(),
        "repair entry replica",
    )?;
    ensure_same_len(
        entry_count,
        batch.current_leader_batches.len(),
        "repair current leader",
    )?;
    ensure_same_len(
        entry_count,
        batch.known_gid_peer_batches.len(),
        "repair known gid peer",
    )?;
    ensure_same_len(
        entry_count,
        batch.known_entry_peer_batches.len(),
        "repair known entry peer",
    )?;
    ensure_same_len(
        batch.pending_modes.len(),
        batch.pending_peers_by_mode.len(),
        "repair pending peer mode",
    )?;
    ensure_same_len(
        batch.pending_modes.len(),
        batch.optimistic_peers_by_mode.len(),
        "repair optimistic mode",
    )?;
    for optimistic_entries in &batch.optimistic_peers_by_mode {
        ensure_same_len(
            entry_count,
            optimistic_entries.len(),
            "repair optimistic entry",
        )?;
    }

    let has_churn = batch.pending_modes.iter().any(|mode| mode == "churn");
    let mut planned: IndexMap<(String, String), IndexSet<String>> = IndexMap::new();

    for i in 0..entry_count {
        let hash = &batch.entry_hashes[i];
        let current_leaders = &batch.current_leader_batches[i];
        let known_gid_peers = &batch.known_gid_peer_batches[i];
        let known_entry_peers = &batch.known_entry_peer_batches[i];

        if has_churn {
            for peer in current_leaders {
                if peer == &batch.self_hash {
                    continue;
                }
                add_repair_dispatch(&mut planned, "churn", peer, hash);
            }
        }

        for (mode_index, mode) in batch.pending_modes.iter().enumerate() {
            let optimistic_peers = &batch.optimistic_peers_by_mode[mode_index][i];
            for peer in &batch.pending_peers_by_mode[mode_index] {
                if contains_string(known_entry_peers, peer) {
                    continue;
                }
                let is_current_leader = contains_string(current_leaders, peer);
                let was_optimistically_assigned = contains_string(optimistic_peers, peer);
                let is_covered_by_full_replica_repair = mode == "join-authoritative"
                    && batch.entry_requested_replicas[i]
                        >= batch.full_replica_repair_candidate_count
                    && batch.full_replica_repair_candidates.contains(peer.as_str());
                let should_queue = if mode == "join-authoritative" {
                    is_current_leader || is_covered_by_full_replica_repair
                } else {
                    was_optimistically_assigned
                        || (is_current_leader && !contains_string(known_gid_peers, peer))
                };

                if should_queue {
                    add_repair_dispatch(&mut planned, mode, peer, hash);
                }
            }
        }
    }

    let out = Array::new();
    for ((mode, target), hashes) in planned {
        let row = Array::new();
        row.push(&JsValue::from_str(&mode));
        row.push(&JsValue::from_str(&target));
        row.push(&strings_to_array(hashes.into_iter().collect()));
        out.push(&row);
    }
    Ok(out)
}

#[wasm_bindgen]
pub struct NativeRangePlanner {
    inner: RangePlanner,
}

pub struct SharedLogStateInner {
    range_planner: RangePlanner,
    entry_coordinates: HashMap<String, Vec<u64>>,
    gid_peers: HashMap<String, IndexSet<String>>,
    entry_known_peers: HashMap<String, IndexSet<String>>,
}

impl SharedLogStateInner {
    fn new(resolution: String) -> Self {
        Self {
            range_planner: RangePlanner::new(&resolution),
            entry_coordinates: HashMap::new(),
            gid_peers: HashMap::new(),
            entry_known_peers: HashMap::new(),
        }
    }

    fn clear_all(&mut self) {
        self.range_planner.clear();
        self.entry_coordinates.clear();
        self.gid_peers.clear();
        self.entry_known_peers.clear();
    }

    fn put_range(&mut self, range: ReplicationRange) {
        self.range_planner.put(range);
    }

    fn delete_range(&mut self, id: &str) -> bool {
        self.range_planner.delete(id)
    }
}

#[wasm_bindgen]
pub struct NativeSharedLogState {
    inner: SharedLogStateInner,
}

#[wasm_bindgen]
impl NativeRangePlanner {
    #[wasm_bindgen(constructor)]
    pub fn new(resolution: String) -> Self {
        Self {
            inner: RangePlanner::new(&resolution),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put(
        &mut self,
        id: String,
        hash: String,
        timestamp: String,
        start1: String,
        end1: String,
        start2: String,
        end2: String,
        width: String,
        mode: u8,
    ) -> Result<(), JsValue> {
        self.inner.put(ReplicationRange::new(
            id,
            hash,
            parse_u64(&timestamp)?,
            parse_u64(&start1)?,
            parse_u64(&end1)?,
            parse_u64(&start2)?,
            parse_u64(&end2)?,
            parse_u64(&width)?,
            mode,
        ));
        Ok(())
    }

    pub fn delete(&mut self, id: &str) -> bool {
        self.inner.delete(id)
    }

    pub fn get_samples(
        &self,
        cursors: Array,
        role_age_ms: f64,
        now: String,
        only_intersecting: bool,
        unique_replicators: JsValue,
        peer_filter: JsValue,
    ) -> Result<Array, JsValue> {
        let cursors = strings_from_array(cursors)?
            .into_iter()
            .map(|value| parse_u64(&value))
            .collect::<Result<Vec<_>, _>>()?;
        let options = SampleOptions {
            role_age_ms: role_age_ms_from_f64(role_age_ms),
            now: parse_u64(&now)?,
            only_intersecting,
            unique_replicators: optional_string_set(unique_replicators)?.map(IndexSet::from_iter),
            peer_filter: optional_string_set(peer_filter)?.map(HashSet::from_iter),
        };
        Ok(samples_to_rows(self.inner.get_samples(&cursors, &options)))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_leaders(
        &self,
        cursors: Array,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let cursors = strings_from_array(cursors)?
            .into_iter()
            .map(|value| parse_u64(&value))
            .collect::<Result<Vec<_>, _>>()?;
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;

        Ok(samples_to_rows(self.inner.find_leaders(
            &cursors,
            replicas,
            &options,
            expand_peer_filter,
            &self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_leaders_batch(
        &self,
        cursor_batches: Array,
        replica_counts: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let cursor_batches = cursor_batches_from_array(cursor_batches)?;
        let replica_counts = usize_from_array(replica_counts)?;
        ensure_same_len(cursor_batches.len(), replica_counts.len(), "leader batch")?;
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;
        let mut prepared_options_by_replicas = HashMap::new();
        let mut full_replica_leaders_by_replicas = HashMap::new();
        let out = Array::new();

        for (cursors, replicas) in cursor_batches.into_iter().zip(replica_counts) {
            let leaders = find_leaders_with_batch_caches(
                &self.inner,
                &cursors,
                replicas,
                &options,
                &mut prepared_options_by_replicas,
                &mut full_replica_leaders_by_replicas,
                expand_peer_filter,
                &self_hash,
                include_self,
                full_replica_fallback,
                include_strict_full_replica,
            );
            out.push(&samples_to_rows(leaders));
        }

        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_leaders_for_gid(
        &self,
        gid: String,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let coordinates = self.inner.get_gid_coordinates(&gid, replicas);
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;
        Ok(samples_to_rows(self.inner.find_leaders(
            &coordinates,
            replicas,
            &options,
            expand_peer_filter,
            &self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_leaders_for_gid(
        &self,
        gid: String,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let coordinates = self.inner.get_gid_coordinates(&gid, replicas);
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;
        let leaders = self.inner.find_leaders(
            &coordinates,
            replicas,
            &options,
            expand_peer_filter,
            &self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        );
        let out = Array::new();
        out.push(&numbers_to_rows(coordinates, self.inner.resolution));
        out.push(&samples_to_rows(leaders));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_leaders_for_gids_batch(
        &self,
        gids: Array,
        replica_counts: Array,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let gids = strings_from_array(gids)?;
        let replica_counts = usize_from_array(replica_counts)?;
        ensure_same_len(gids.len(), replica_counts.len(), "gid leader batch")?;
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;
        let mut prepared_options_by_replicas = HashMap::new();
        let mut full_replica_leaders_by_replicas = HashMap::new();
        let out = Array::new();

        for (gid, replicas) in gids.into_iter().zip(replica_counts) {
            let coordinates = self.inner.get_gid_coordinates(&gid, replicas);
            let leaders = find_leaders_with_batch_caches(
                &self.inner,
                &coordinates,
                replicas,
                &options,
                &mut prepared_options_by_replicas,
                &mut full_replica_leaders_by_replicas,
                expand_peer_filter,
                &self_hash,
                include_self,
                full_replica_fallback,
                include_strict_full_replica,
            );
            let row = Array::new();
            row.push(&numbers_to_rows(coordinates, self.inner.resolution));
            row.push(&samples_to_rows(leaders));
            out.push(&row);
        }

        Ok(out)
    }

    pub fn plan_repair_dispatch(
        &self,
        entry_hashes: Array,
        entry_gids: Array,
        entry_requested_replicas: Array,
        current_leader_batches: Array,
        known_gid_peer_batches: Array,
        known_entry_peer_batches: Array,
        pending_modes: Array,
        pending_peers_by_mode: Array,
        optimistic_peers_by_mode: Array,
        full_replica_repair_candidates: Array,
        full_replica_repair_candidate_count: usize,
        self_hash: String,
    ) -> Result<Array, JsValue> {
        plan_repair_dispatch_rows(RepairDispatchBatch {
            entry_hashes: strings_from_array(entry_hashes)?,
            entry_gids: strings_from_array(entry_gids)?,
            entry_requested_replicas: usize_from_array(entry_requested_replicas)?,
            current_leader_batches: string_batches_from_array(
                current_leader_batches,
                "current leader batches",
            )?,
            known_gid_peer_batches: string_batches_from_array(
                known_gid_peer_batches,
                "known gid peer batches",
            )?,
            known_entry_peer_batches: string_batches_from_array(
                known_entry_peer_batches,
                "known entry peer batches",
            )?,
            pending_modes: strings_from_array(pending_modes)?,
            pending_peers_by_mode: string_batches_from_array(
                pending_peers_by_mode,
                "pending peers by mode",
            )?,
            optimistic_peers_by_mode: string_matrix_from_array(
                optimistic_peers_by_mode,
                "optimistic peers by mode",
            )?,
            full_replica_repair_candidates: HashSet::<String>::from_iter(strings_from_array(
                full_replica_repair_candidates,
            )?),
            full_replica_repair_candidate_count,
            self_hash,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_repair_dispatch_for_entries(
        &self,
        entry_hashes: Array,
        entry_gids: Array,
        entry_requested_replicas: Array,
        entry_coordinate_batches: Array,
        known_gid_peer_batches: Array,
        known_entry_peer_batches: Array,
        pending_modes: Array,
        pending_peers_by_mode: Array,
        optimistic_peers_by_mode: Array,
        full_replica_repair_candidates: Array,
        full_replica_repair_candidate_count: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let entry_coordinate_batches = cursor_batches_from_array(entry_coordinate_batches)?;
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;
        let mut prepared_options_by_replicas = HashMap::new();
        let mut full_replica_leaders_by_replicas = HashMap::new();
        let mut current_leader_batches = Vec::with_capacity(entry_coordinate_batches.len());

        for coordinates in &entry_coordinate_batches {
            let replicas = coordinates.len();
            let leaders = find_leaders_with_batch_caches(
                &self.inner,
                coordinates,
                replicas,
                &options,
                &mut prepared_options_by_replicas,
                &mut full_replica_leaders_by_replicas,
                expand_peer_filter,
                &self_hash,
                include_self,
                full_replica_fallback,
                include_strict_full_replica,
            );
            current_leader_batches.push(leaders.into_iter().map(|leader| leader.hash).collect());
        }

        plan_repair_dispatch_rows(RepairDispatchBatch {
            entry_hashes: strings_from_array(entry_hashes)?,
            entry_gids: strings_from_array(entry_gids)?,
            entry_requested_replicas: usize_from_array(entry_requested_replicas)?,
            current_leader_batches,
            known_gid_peer_batches: string_batches_from_array(
                known_gid_peer_batches,
                "known gid peer batches",
            )?,
            known_entry_peer_batches: string_batches_from_array(
                known_entry_peer_batches,
                "known entry peer batches",
            )?,
            pending_modes: strings_from_array(pending_modes)?,
            pending_peers_by_mode: string_batches_from_array(
                pending_peers_by_mode,
                "pending peers by mode",
            )?,
            optimistic_peers_by_mode: string_matrix_from_array(
                optimistic_peers_by_mode,
                "optimistic peers by mode",
            )?,
            full_replica_repair_candidates: HashSet::<String>::from_iter(strings_from_array(
                full_replica_repair_candidates,
            )?),
            full_replica_repair_candidate_count,
            self_hash,
        })
    }

    pub fn get_grid(&self, from: String, count: usize) -> Result<Array, JsValue> {
        Ok(numbers_to_rows(
            self.inner.get_grid(parse_u64(&from)?, count),
            self.inner.resolution,
        ))
    }

    pub fn get_gid_coordinates(&self, gid: String, count: usize) -> Array {
        numbers_to_rows(
            self.inner.get_gid_coordinates(&gid, count),
            self.inner.resolution,
        )
    }

    pub fn get_full_replica_leaders(
        &self,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        include_strict: bool,
        peer_filter: JsValue,
    ) -> Result<JsValue, JsValue> {
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;

        Ok(
            match self
                .inner
                .get_full_replica_leaders(replicas, &options, include_strict)
            {
                Some(leaders) => samples_to_rows(leaders).into(),
                None => JsValue::UNDEFINED,
            },
        )
    }

    pub fn include_matured_peers(
        &self,
        peer_filter: JsValue,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        self_hash: String,
        include_self: bool,
    ) -> Result<JsValue, JsValue> {
        let options = SampleOptions {
            role_age_ms: role_age_ms_from_f64(role_age_ms),
            now: parse_u64(&now)?,
            ..Default::default()
        };
        let Some(peers) = self.inner.include_matured_peers(
            optional_string_set(peer_filter)?.map(IndexSet::from_iter),
            replicas,
            &options,
            &self_hash,
            include_self,
        ) else {
            return Ok(JsValue::UNDEFINED);
        };

        Ok(strings_to_array(peers).into())
    }
}

#[wasm_bindgen]
impl NativeSharedLogState {
    #[wasm_bindgen(constructor)]
    pub fn new(resolution: String) -> Self {
        Self {
            inner: SharedLogStateInner::new(resolution),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.range_planner.len()
    }

    pub fn clear(&mut self) {
        self.inner.clear_all();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put(
        &mut self,
        id: String,
        hash: String,
        timestamp: String,
        start1: String,
        end1: String,
        start2: String,
        end2: String,
        width: String,
        mode: u8,
    ) -> Result<(), JsValue> {
        self.inner.put_range(ReplicationRange::new(
            id,
            hash,
            parse_u64(&timestamp)?,
            parse_u64(&start1)?,
            parse_u64(&end1)?,
            parse_u64(&start2)?,
            parse_u64(&end2)?,
            parse_u64(&width)?,
            mode,
        ));
        Ok(())
    }

    pub fn delete(&mut self, id: &str) -> bool {
        self.inner.delete_range(id)
    }

    pub fn put_entry_coordinates(
        &mut self,
        hash: String,
        coordinates: Array,
    ) -> Result<(), JsValue> {
        self.inner
            .entry_coordinates
            .insert(hash, cursor_values_from_array(coordinates)?);
        Ok(())
    }

    pub fn delete_entry_coordinates(&mut self, hash: &str) -> bool {
        self.inner.entry_coordinates.remove(hash).is_some()
    }

    pub fn get_entry_coordinates(&self, hash: &str) -> JsValue {
        self.inner
            .entry_coordinates
            .get(hash)
            .map(|coordinates| {
                numbers_to_rows(coordinates.clone(), self.inner.range_planner.resolution).into()
            })
            .unwrap_or(JsValue::UNDEFINED)
    }

    pub fn commit_entry_coordinates(
        &mut self,
        hash: String,
        coordinates: Array,
        next_hashes: Array,
    ) -> Result<(), JsValue> {
        let coordinates = cursor_values_from_array(coordinates)?;
        let next_hashes = strings_from_array(next_hashes)?;
        self.inner.entry_coordinates.insert(hash, coordinates);
        for next_hash in next_hashes {
            self.inner.entry_coordinates.remove(&next_hash);
        }
        Ok(())
    }

    pub fn delete_entry_coordinates_batch(&mut self, hashes: Array) -> Result<(), JsValue> {
        for hash in strings_from_array(hashes)? {
            self.inner.entry_coordinates.remove(&hash);
        }
        Ok(())
    }

    pub fn clear_entry_coordinates(&mut self) {
        self.inner.entry_coordinates.clear();
    }

    pub fn add_gid_peers(
        &mut self,
        gid: String,
        peers: Array,
        reset: bool,
    ) -> Result<usize, JsValue> {
        let entry = self.inner.gid_peers.entry(gid).or_default();
        if reset {
            entry.clear();
        }
        for peer in strings_from_array(peers)? {
            entry.insert(peer);
        }
        Ok(entry.len())
    }

    pub fn remove_gid_peer(&mut self, peer: &str, gid: JsValue) -> Result<(), JsValue> {
        if gid.is_undefined() || gid.is_null() {
            let empty_gids: Vec<String> = self
                .inner
                .gid_peers
                .iter_mut()
                .filter_map(|(gid, peers)| {
                    peers.shift_remove(peer);
                    if peers.is_empty() {
                        Some(gid.clone())
                    } else {
                        None
                    }
                })
                .collect();
            for gid in empty_gids {
                self.inner.gid_peers.remove(&gid);
            }
            return Ok(());
        }

        let Some(gid) = gid.as_string() else {
            return Err(JsValue::from_str("Expected optional gid string"));
        };
        if let Some(peers) = self.inner.gid_peers.get_mut(&gid) {
            peers.shift_remove(peer);
            if peers.is_empty() {
                self.inner.gid_peers.remove(&gid);
            }
        }
        Ok(())
    }

    pub fn delete_gid_peers(&mut self, gid: &str) -> bool {
        self.inner.gid_peers.remove(gid).is_some()
    }

    pub fn clear_gid_peers(&mut self) {
        self.inner.gid_peers.clear();
    }

    pub fn mark_entries_known_by_peer(
        &mut self,
        hashes: Array,
        peer: String,
    ) -> Result<(), JsValue> {
        for hash in strings_from_array(hashes)? {
            self.inner
                .entry_known_peers
                .entry(hash)
                .or_default()
                .insert(peer.clone());
        }
        Ok(())
    }

    pub fn remove_entries_known_by_peer(
        &mut self,
        hashes: Array,
        peer: &str,
    ) -> Result<(), JsValue> {
        for hash in strings_from_array(hashes)? {
            if let Some(peers) = self.inner.entry_known_peers.get_mut(&hash) {
                peers.shift_remove(peer);
                if peers.is_empty() {
                    self.inner.entry_known_peers.remove(&hash);
                }
            }
        }
        Ok(())
    }

    pub fn remove_peer_from_entry_known_peers(&mut self, peer: &str) {
        let empty_hashes: Vec<String> = self
            .inner
            .entry_known_peers
            .iter_mut()
            .filter_map(|(hash, peers)| {
                peers.shift_remove(peer);
                if peers.is_empty() {
                    Some(hash.clone())
                } else {
                    None
                }
            })
            .collect();
        for hash in empty_hashes {
            self.inner.entry_known_peers.remove(&hash);
        }
    }

    pub fn clear_entry_known_peers(&mut self) {
        self.inner.entry_known_peers.clear();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_entry_leaders_for_gid(
        &self,
        gid: String,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let coordinates = self.inner.range_planner.get_gid_coordinates(&gid, replicas);
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;
        let leaders = self.inner.range_planner.find_leaders(
            &coordinates,
            replicas,
            &options,
            expand_peer_filter,
            &self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        );
        let out = Array::new();
        out.push(&numbers_to_rows(
            coordinates,
            self.inner.range_planner.resolution,
        ));
        out.push(&samples_to_rows(leaders));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_entry_assignment_for_gid(
        &self,
        gid: String,
        replicas: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let coordinates = self.inner.range_planner.get_gid_coordinates(&gid, replicas);
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;
        let leaders = self.inner.range_planner.find_leaders(
            &coordinates,
            replicas,
            &options,
            expand_peer_filter,
            &self_hash,
            include_self,
            full_replica_fallback,
            include_strict_full_replica,
        );
        let assigned_to_range_boundary = should_assign_to_range_boundary(&leaders, replicas);
        let out = Array::new();
        out.push(&numbers_to_rows(
            coordinates,
            self.inner.range_planner.resolution,
        ));
        out.push(&samples_to_rows(leaders));
        out.push(&JsValue::from_bool(assigned_to_range_boundary));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_repair_dispatch_for_entries(
        &self,
        entry_hashes: Array,
        entry_gids: Array,
        entry_requested_replicas: Array,
        entry_coordinate_batches: Array,
        pending_modes: Array,
        pending_peers_by_mode: Array,
        optimistic_peers_by_mode: Array,
        full_replica_repair_candidates: Array,
        full_replica_repair_candidate_count: usize,
        role_age_ms: f64,
        now: String,
        peer_filter: JsValue,
        expand_peer_filter: bool,
        self_hash: String,
        include_self: bool,
        full_replica_fallback: bool,
        include_strict_full_replica: bool,
    ) -> Result<Array, JsValue> {
        let entry_hashes = strings_from_array(entry_hashes)?;
        let entry_gids = strings_from_array(entry_gids)?;
        let entry_coordinate_batches = cursor_batches_from_array(entry_coordinate_batches)?;
        let options = find_leader_options(role_age_ms, &now, peer_filter)?;
        let mut prepared_options_by_replicas = HashMap::new();
        let mut full_replica_leaders_by_replicas = HashMap::new();
        let mut current_leader_batches = Vec::with_capacity(entry_coordinate_batches.len());

        for coordinates in &entry_coordinate_batches {
            let replicas = coordinates.len();
            let leaders = find_leaders_with_batch_caches(
                &self.inner.range_planner,
                coordinates,
                replicas,
                &options,
                &mut prepared_options_by_replicas,
                &mut full_replica_leaders_by_replicas,
                expand_peer_filter,
                &self_hash,
                include_self,
                full_replica_fallback,
                include_strict_full_replica,
            );
            current_leader_batches.push(leaders.into_iter().map(|leader| leader.hash).collect());
        }

        let known_gid_peer_batches = entry_gids
            .iter()
            .map(|gid| {
                self.inner
                    .gid_peers
                    .get(gid)
                    .map(index_set_to_vec)
                    .unwrap_or_default()
            })
            .collect();
        let known_entry_peer_batches = entry_hashes
            .iter()
            .map(|hash| {
                self.inner
                    .entry_known_peers
                    .get(hash)
                    .map(index_set_to_vec)
                    .unwrap_or_default()
            })
            .collect();

        plan_repair_dispatch_rows(RepairDispatchBatch {
            entry_hashes,
            entry_gids,
            entry_requested_replicas: usize_from_array(entry_requested_replicas)?,
            current_leader_batches,
            known_gid_peer_batches,
            known_entry_peer_batches,
            pending_modes: strings_from_array(pending_modes)?,
            pending_peers_by_mode: string_batches_from_array(
                pending_peers_by_mode,
                "pending peers by mode",
            )?,
            optimistic_peers_by_mode: string_matrix_from_array(
                optimistic_peers_by_mode,
                "optimistic peers by mode",
            )?,
            full_replica_repair_candidates: HashSet::<String>::from_iter(strings_from_array(
                full_replica_repair_candidates,
            )?),
            full_replica_repair_candidate_count,
            self_hash,
        })
    }
}

impl Default for NativeRangePlanner {
    fn default() -> Self {
        Self::new("u32".to_string())
    }
}

impl Default for NativeSharedLogState {
    fn default() -> Self {
        Self::new("u32".to_string())
    }
}

fn role_age_ms_from_f64(value: f64) -> u64 {
    if value <= 0.0 {
        0
    } else {
        value.floor() as u64
    }
}

fn find_leader_options(
    role_age_ms: f64,
    now: &str,
    peer_filter: JsValue,
) -> Result<SampleOptions, JsValue> {
    Ok(SampleOptions {
        role_age_ms: role_age_ms_from_f64(role_age_ms),
        now: parse_u64(now)?,
        peer_filter: optional_string_set(peer_filter)?.map(HashSet::from_iter),
        ..Default::default()
    })
}

fn parse_u64(value: &str) -> Result<u64, JsValue> {
    value
        .parse::<u64>()
        .map_err(|_| JsValue::from_str("Expected unsigned integer string"))
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

fn string_batches_from_array(values: Array, label: &str) -> Result<Vec<Vec<String>>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for value in values.iter() {
        if !Array::is_array(&value) {
            return Err(JsValue::from_str(&format!("Expected {label}")));
        }
        out.push(strings_from_array(Array::from(&value))?);
    }
    Ok(out)
}

fn string_matrix_from_array(values: Array, label: &str) -> Result<Vec<Vec<Vec<String>>>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for value in values.iter() {
        if !Array::is_array(&value) {
            return Err(JsValue::from_str(&format!("Expected {label}")));
        }
        out.push(string_batches_from_array(Array::from(&value), label)?);
    }
    Ok(out)
}

fn cursor_values_from_array(values: Array) -> Result<Vec<u64>, JsValue> {
    strings_from_array(values)?
        .into_iter()
        .map(|value| parse_u64(&value))
        .collect::<Result<Vec<_>, _>>()
}

fn usize_from_array(values: Array) -> Result<Vec<usize>, JsValue> {
    let mut out = Vec::with_capacity(values.length() as usize);
    for value in values.iter() {
        let Some(value) = value.as_f64() else {
            return Err(JsValue::from_str("Expected number array"));
        };
        if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
            return Err(JsValue::from_str("Expected unsigned integer array"));
        }
        out.push(value as usize);
    }
    Ok(out)
}

fn cursor_batches_from_array(values: Array) -> Result<Vec<Vec<u64>>, JsValue> {
    let batches = string_batches_from_array(values, "cursor batch array")?;
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        out.push(
            batch
                .into_iter()
                .map(|value| parse_u64(&value))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    Ok(out)
}

fn ensure_same_len(left: usize, right: usize, label: &str) -> Result<(), JsValue> {
    if left == right {
        Ok(())
    } else {
        Err(JsValue::from_str(&format!(
            "Mismatched {label} input lengths"
        )))
    }
}

fn add_repair_dispatch(
    planned: &mut IndexMap<(String, String), IndexSet<String>>,
    mode: &str,
    target: &str,
    hash: &str,
) {
    planned
        .entry((mode.to_string(), target.to_string()))
        .or_default()
        .insert(hash.to_string());
}

fn contains_string(values: &[String], target: &str) -> bool {
    values.iter().any(|value| value == target)
}

fn should_assign_to_range_boundary(leaders: &[LeaderSample], min_replicas: usize) -> bool {
    leaders.len() < min_replicas || leaders.iter().any(|leader| !leader.intersecting)
}

fn index_set_to_vec(values: &IndexSet<String>) -> Vec<String> {
    values.iter().cloned().collect()
}

fn optional_string_set(value: JsValue) -> Result<Option<Vec<String>>, JsValue> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    if !Array::is_array(&value) {
        return Err(JsValue::from_str("Expected optional string array"));
    }
    Ok(Some(strings_from_array(Array::from(&value))?))
}

fn strings_to_array(values: Vec<String>) -> Array {
    let out = Array::new();
    for value in values {
        out.push(&JsValue::from_str(&value));
    }
    out
}

fn numbers_to_rows(values: Vec<u64>, resolution: Resolution) -> Array {
    let out = Array::new();
    for value in values {
        match resolution {
            Resolution::U32 => out.push(&JsValue::from_f64(value as f64)),
            Resolution::U64 => out.push(&JsValue::from_str(&value.to_string())),
        };
    }
    out
}

fn samples_to_rows(samples: Vec<LeaderSample>) -> Array {
    let out = Array::new();
    for sample in samples {
        let row = Array::new();
        row.push(&JsValue::from_str(&sample.hash));
        row.push(&JsValue::from_bool(sample.intersecting));
        out.push(&row);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{RangePlanner, ReplicationRange, SampleOptions};
    use indexmap::IndexSet;

    #[test]
    fn returns_intersecting_leader() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));

        let samples = planner.get_samples(
            &[15],
            &SampleOptions {
                now: 1_000,
                ..Default::default()
            },
        );

        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].hash, "peer-a");
        assert!(samples[0].intersecting);
    }

    #[test]
    fn falls_back_to_closest_mature_non_strict_range() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new("a", "peer-a", 0, 0, 10, 0, 10, 10, 0));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 90, 100, 90, 100, 10, 0,
        ));

        let samples = planner.get_samples(
            &[50, 75],
            &SampleOptions {
                now: 1_000,
                ..Default::default()
            },
        );

        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].hash, "peer-a");
        assert!(!samples[0].intersecting);
        assert_eq!(samples[1].hash, "peer-b");
        assert!(!samples[1].intersecting);
    }

    #[test]
    fn honors_peer_filter() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 10, 20, 10, 20, 10, 0,
        ));

        let samples = planner.get_samples(
            &[15],
            &SampleOptions {
                now: 1_000,
                peer_filter: Some(["peer-b".to_string()].into_iter().collect()),
                ..Default::default()
            },
        );

        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].hash, "peer-b");
    }

    #[test]
    fn does_not_fallback_to_future_range() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 2_000, 10, 20, 10, 20, 10, 0,
        ));

        let samples = planner.get_samples(
            &[50],
            &SampleOptions {
                now: 1_000,
                ..Default::default()
            },
        );

        assert!(samples.is_empty());
    }

    #[test]
    fn supports_wrapped_ranges() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 90, 100, 0, 10, 20, 0,
        ));

        let samples = planner.get_samples(
            &[5],
            &SampleOptions {
                now: 1_000,
                ..Default::default()
            },
        );

        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].hash, "peer-a");
        assert!(samples[0].intersecting);
    }

    #[test]
    fn delete_removes_range_from_sampling() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 10, 20, 10, 20, 10, 0,
        ));

        assert!(planner.delete("a"));
        assert!(!planner.delete("a"));

        let samples = planner.get_samples(
            &[15],
            &SampleOptions {
                now: 1_000,
                ..Default::default()
            },
        );

        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].hash, "peer-b");
        assert!(samples[0].intersecting);
    }

    #[test]
    fn returns_full_replica_leaders_when_under_replica_count() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 0,
        ));

        let leaders = planner
            .get_full_replica_leaders(
                2,
                &SampleOptions {
                    now: 1_000,
                    ..Default::default()
                },
                true,
            )
            .expect("leaders");

        assert_eq!(leaders.len(), 2);
        assert_eq!(leaders[0].hash, "peer-a");
        assert_eq!(leaders[1].hash, "peer-b");
        assert!(leaders.iter().all(|leader| leader.intersecting));
    }

    #[test]
    fn rejects_full_replica_leaders_when_over_replica_count() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 0,
        ));

        assert!(planner
            .get_full_replica_leaders(
                1,
                &SampleOptions {
                    now: 1_000,
                    ..Default::default()
                },
                true,
            )
            .is_none());
    }

    #[test]
    fn full_replica_leaders_honor_filters_maturity_and_strict_mode() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 1,
        ));
        planner.put(ReplicationRange::new(
            "c", "peer-c", 950, 50, 60, 50, 60, 10, 0,
        ));

        let leaders = planner
            .get_full_replica_leaders(
                3,
                &SampleOptions {
                    now: 1_000,
                    role_age_ms: 100,
                    peer_filter: Some(
                        [
                            "peer-a".to_string(),
                            "peer-b".to_string(),
                            "peer-c".to_string(),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..Default::default()
                },
                false,
            )
            .expect("leaders");

        assert_eq!(leaders.len(), 1);
        assert_eq!(leaders[0].hash, "peer-a");
        assert!(leaders[0].intersecting);
    }

    #[test]
    fn full_replica_leaders_track_delete_and_replace() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 0,
        ));

        assert!(planner
            .get_full_replica_leaders(
                1,
                &SampleOptions {
                    now: 1_000,
                    ..Default::default()
                },
                true,
            )
            .is_none());

        assert!(planner.delete("b"));
        let leaders = planner
            .get_full_replica_leaders(
                1,
                &SampleOptions {
                    now: 1_000,
                    ..Default::default()
                },
                true,
            )
            .expect("leaders after delete");

        assert_eq!(leaders.len(), 1);
        assert_eq!(leaders[0].hash, "peer-a");

        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 1,
        ));

        let leaders = planner
            .get_full_replica_leaders(
                1,
                &SampleOptions {
                    now: 1_000,
                    ..Default::default()
                },
                false,
            )
            .expect("leaders after replace");

        assert_eq!(leaders.len(), 1);
        assert_eq!(leaders[0].hash, "peer-a");
    }

    #[test]
    fn include_matured_peers_expands_underfilled_filters() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 1,
        ));
        planner.put(ReplicationRange::new(
            "c", "peer-c", 950, 50, 60, 50, 60, 10, 0,
        ));

        let peers = planner
            .include_matured_peers(
                Some(IndexSet::from_iter(["peer-a".to_string()])),
                1,
                &SampleOptions {
                    now: 1_000,
                    role_age_ms: 100,
                    ..Default::default()
                },
                "peer-self",
                true,
            )
            .expect("peers");

        assert_eq!(peers, vec!["peer-a".to_string(), "peer-b".to_string()]);
    }

    #[test]
    fn include_matured_peers_skips_self_when_not_replicating() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 0,
        ));

        let peers = planner
            .include_matured_peers(
                Some(IndexSet::from_iter(["peer-a".to_string()])),
                1,
                &SampleOptions {
                    now: 1_000,
                    ..Default::default()
                },
                "peer-b",
                false,
            )
            .expect("peers");

        assert_eq!(peers, vec!["peer-a".to_string()]);
    }

    #[test]
    fn find_leaders_combines_filter_fill_full_replica_and_sampling() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 0,
        ));

        let leaders = planner.find_leaders(
            &[50, 75],
            2,
            &SampleOptions {
                now: 1_000,
                peer_filter: Some(["peer-a".to_string()].into_iter().collect()),
                ..Default::default()
            },
            true,
            "peer-self",
            true,
            true,
            true,
        );

        assert_eq!(leaders.len(), 2);
        assert_eq!(leaders[0].hash, "peer-a");
        assert_eq!(leaders[1].hash, "peer-b");
        assert!(leaders.iter().all(|leader| leader.intersecting));
    }

    #[test]
    fn find_leaders_respects_candidate_mode_without_fill_or_full_replica() {
        let mut planner = RangePlanner::new("u32");
        planner.put(ReplicationRange::new(
            "a", "peer-a", 0, 10, 20, 10, 20, 10, 0,
        ));
        planner.put(ReplicationRange::new(
            "b", "peer-b", 0, 30, 40, 30, 40, 10, 0,
        ));

        let leaders = planner.find_leaders(
            &[50],
            1,
            &SampleOptions {
                now: 1_000,
                peer_filter: Some(["peer-a".to_string()].into_iter().collect()),
                ..Default::default()
            },
            false,
            "peer-self",
            true,
            false,
            true,
        );

        assert_eq!(leaders.len(), 1);
        assert_eq!(leaders[0].hash, "peer-a");
        assert!(!leaders[0].intersecting);
    }
}
