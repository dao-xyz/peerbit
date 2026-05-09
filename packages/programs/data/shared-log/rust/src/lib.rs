use indexmap::{IndexMap, IndexSet};
use js_sys::Array;
use std::collections::HashSet;
use wasm_bindgen::prelude::*;

const MODE_NON_STRICT: u8 = 0;
const MAX_U32: u64 = u32::MAX as u64;
const MAX_U64: u64 = u64::MAX;

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
}

impl RangePlanner {
    pub fn new(resolution: &str) -> Self {
        Self {
            resolution: Resolution::from_str(resolution),
            ranges: IndexMap::new(),
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
    }

    pub fn put(&mut self, range: ReplicationRange) {
        self.ranges.insert(range.id.clone(), range);
    }

    pub fn delete(&mut self, id: &str) -> bool {
        self.ranges.shift_remove(id).is_some()
    }

    pub fn get_samples(&self, cursors: &[u64], options: &SampleOptions) -> Vec<LeaderSample> {
        let mut leaders: IndexMap<String, bool> = IndexMap::new();
        let mut matured = 0usize;
        let mut unique_visited: IndexSet<String> = IndexSet::new();

        for (i, point) in cursors.iter().copied().enumerate() {
            for range in self
                .ranges
                .values()
                .filter(|range| self.include_range(range, options.peer_filter.as_ref()))
                .filter(|range| range.contains(point))
            {
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

            let mut closest = self.closest_non_strict(point, options.peer_filter.as_ref());
            for range in closest.drain(..) {
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

    fn closest_non_strict(
        &self,
        point: u64,
        peer_filter: Option<&HashSet<String>>,
    ) -> Vec<&ReplicationRange> {
        let max_value = self.resolution.max_value();
        let mut ranges: Vec<&ReplicationRange> = self
            .ranges
            .values()
            .filter(|range| self.include_range(range, peer_filter))
            .filter(|range| range.mode == MODE_NON_STRICT)
            .collect();
        ranges.sort_by(|left, right| {
            closest_distance(left, point, max_value)
                .cmp(&closest_distance(right, point, max_value))
                .then_with(|| left.timestamp.cmp(&right.timestamp))
                .then_with(|| left.hash.cmp(&right.hash))
                .then_with(|| left.id.cmp(&right.id))
        });
        ranges
    }
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

#[wasm_bindgen]
pub struct NativeRangePlanner {
    inner: RangePlanner,
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
            role_age_ms: if role_age_ms <= 0.0 {
                0
            } else {
                role_age_ms.floor() as u64
            },
            now: parse_u64(&now)?,
            only_intersecting,
            unique_replicators: optional_string_set(unique_replicators)?.map(IndexSet::from_iter),
            peer_filter: optional_string_set(peer_filter)?.map(HashSet::from_iter),
        };
        Ok(samples_to_rows(self.inner.get_samples(&cursors, &options)))
    }
}

impl Default for NativeRangePlanner {
    fn default() -> Self {
        Self::new("u32".to_string())
    }
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

fn optional_string_set(value: JsValue) -> Result<Option<Vec<String>>, JsValue> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    if !Array::is_array(&value) {
        return Err(JsValue::from_str("Expected optional string array"));
    }
    Ok(Some(strings_from_array(Array::from(&value))?))
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
}
