//! Port of `packages/transport/stream/src/routes.ts`.
//!
//! Semantics (session/expiry rules, LRU touching, pruning bounds, fanout
//! selection) are transcribed statement-for-statement from the TS source.
//! The only structural difference is that wall-clock reads and the coalesced
//! cleanup timer are externalized: every mutating call takes `now_ms` and
//! [`Routes::add`] reports `cleanup_requested` so the host can schedule a
//! single timer that later invokes [`Routes::cleanup_pending`] — the same
//! coalescing the TS `requestCleanup` performs with `setTimeout`.

use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};

/// `Number.MAX_SAFE_INTEGER - 1`
pub const MAX_ROUTE_DISTANCE: i64 = 9_007_199_254_740_990;

pub const DEFAULT_MAX_FROM_ENTRIES: usize = 2048;
pub const DEFAULT_MAX_TARGETS_PER_FROM: usize = 10_000;
pub const DEFAULT_MAX_RELAYS_PER_TARGET: usize = 32;

/// Default `routeMaxRetentionPeriod` of the standalone `Routes` class
/// (10_000 ms in routes.ts; DirectStream overrides it with 50_000 ms).
pub const DEFAULT_ROUTE_MAX_RETENTION_PERIOD_MS: u64 = 10 * 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayInfo {
    pub session: i64,
    pub hash: String,
    pub updated_at: u64,
    pub expire_at: Option<u64>,
    pub distance: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteInfo {
    pub remote_session: i64,
    pub session: i64,
    pub list: Vec<RelayInfo>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddOutcome {
    New,
    Updated,
    Restart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AddResult {
    pub outcome: AddOutcome,
    /// The host must (re)arm the coalesced cleanup timer
    /// (`routeMaxRetentionPeriod + 100` ms, single timer).
    pub cleanup_requested: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RouteHint {
    pub from: String,
    pub target: String,
    pub next_hop: String,
    pub distance: i64,
    pub session: i64,
    pub updated_at: u64,
    pub expires_at: Option<u64>,
}

/// sort by distance, if same distance make the routes without expire time first
fn sort_routes(routes: &mut [RelayInfo]) {
    routes.sort_by(|a, b| {
        if a.distance == b.distance {
            return match (a.expire_at.is_some(), b.expire_at.is_some()) {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => std::cmp::Ordering::Equal,
            };
        }
        a.distance.cmp(&b.distance)
    });
}

pub struct Routes {
    pub me: String,
    /// FROM -> TO -> route info (insertion-ordered maps: eviction pops the
    /// oldest entry and updates LRU-touch by re-inserting at the end,
    /// mirroring the JS `Map` delete+set pattern).
    routes: IndexMap<String, IndexMap<String, RouteInfo>>,
    remote_info: HashMap<String, i64>,
    pub route_max_retention_period: u64,
    pending_cleanup_by_from: HashMap<String, HashSet<String>>,
    max_from_entries: usize,
    max_targets_per_from: usize,
    max_relays_per_target: usize,
}

impl Routes {
    pub fn new(
        me: String,
        route_max_retention_period: Option<u64>,
        max_from_entries: Option<usize>,
        max_targets_per_from: Option<usize>,
        max_relays_per_target: Option<usize>,
    ) -> Self {
        Routes {
            me,
            routes: IndexMap::new(),
            remote_info: HashMap::new(),
            route_max_retention_period: route_max_retention_period
                .unwrap_or(DEFAULT_ROUTE_MAX_RETENTION_PERIOD_MS),
            pending_cleanup_by_from: HashMap::new(),
            max_from_entries: max_from_entries.unwrap_or(DEFAULT_MAX_FROM_ENTRIES).max(1),
            max_targets_per_from: max_targets_per_from
                .unwrap_or(DEFAULT_MAX_TARGETS_PER_FROM)
                .max(1),
            max_relays_per_target: max_relays_per_target
                .unwrap_or(DEFAULT_MAX_RELAYS_PER_TARGET)
                .max(1),
        }
    }

    pub fn clear(&mut self) {
        self.routes.clear();
        self.pending_cleanup_by_from.clear();
    }

    pub fn has_pending_cleanup(&self) -> bool {
        !self.pending_cleanup_by_from.is_empty()
    }

    fn request_cleanup(&mut self, from: &str, to: &str) {
        self.pending_cleanup_by_from
            .entry(from.to_string())
            .or_default()
            .insert(to.to_string());
    }

    /// The body of the TS coalesced cleanup timer.
    pub fn cleanup_pending(&mut self, now_ms: u64) {
        let pending = std::mem::take(&mut self.pending_cleanup_by_from);
        for (from, tos) in pending {
            for to in tos {
                self.cleanup(&from, &to, now_ms);
            }
        }
    }

    fn prune_from_maps(&mut self) {
        if self.routes.len() <= self.max_from_entries {
            return;
        }
        // Keep `me` pinned: move it to the end (most recently used) and keep
        // evicting the oldest entries.
        while self.routes.len() > self.max_from_entries {
            let oldest = match self.routes.keys().next() {
                Some(key) => key.clone(),
                None => return,
            };
            if oldest == self.me {
                if let Some(self_map) = self.routes.shift_remove(&oldest) {
                    self.routes.insert(oldest, self_map);
                }
                continue;
            }
            self.routes.shift_remove(&oldest);
        }
    }

    fn prune_targets(&mut self, from: &str) {
        let Some(from_map) = self.routes.get_mut(from) else {
            return;
        };
        if from_map.len() <= self.max_targets_per_from {
            return;
        }
        while from_map.len() > self.max_targets_per_from {
            let Some(oldest) = from_map.keys().next().cloned() else {
                break;
            };
            from_map.shift_remove(&oldest);
        }
        if from_map.is_empty() {
            self.routes.shift_remove(from);
        }
    }

    fn cleanup(&mut self, from: &str, to: &str, now_ms: u64) {
        let mut delete_target = false;
        let mut delete_from = false;
        if let Some(from_map) = self.routes.get_mut(from) {
            if let Some(map) = from_map.get_mut(to) {
                let mut keep_routes: Vec<RelayInfo> = Vec::new();
                for route in map.list.drain(..) {
                    let expired = matches!(route.expire_at, Some(at) if at < now_ms);
                    if !expired {
                        keep_routes.push(route);
                    }
                }
                if keep_routes.len() > self.max_relays_per_target {
                    keep_routes.truncate(self.max_relays_per_target);
                }
                if !keep_routes.is_empty() {
                    map.list = keep_routes;
                } else {
                    delete_target = true;
                }
            }
            if delete_target {
                from_map.shift_remove(to);
                delete_from = from_map.is_empty();
            }
        }
        if delete_from {
            self.routes.shift_remove(from);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add(
        &mut self,
        from: &str,
        neighbour: &str,
        target: &str,
        mut distance: i64,
        session: i64,
        remote_session: i64,
        now_ms: u64,
    ) -> AddResult {
        if !self.routes.contains_key(from) {
            self.routes.insert(from.to_string(), IndexMap::new());
        } else {
            // LRU-touch the `from` map.
            if let Some(map) = self.routes.shift_remove(from) {
                self.routes.insert(from.to_string(), map);
            }
        }
        let max_relays_per_target = self.max_relays_per_target;
        let retention = self.route_max_retention_period;
        let me = self.me.clone();
        let from_map = self.routes.get_mut(from).expect("from map inserted above");

        let route_did_exist = from_map.contains_key(target);
        let (is_new_session, is_old_session) = match from_map.get(target) {
            None => (true, false),
            Some(prev) => (session > prev.session, session < prev.session),
        };

        if !route_did_exist {
            from_map.insert(
                target.to_string(),
                RouteInfo {
                    session,
                    remote_session,
                    list: Vec::new(),
                },
            );
        } else {
            // LRU-touch the target entry.
            if let Some(prev) = from_map.shift_remove(target) {
                from_map.insert(target.to_string(), prev);
            }
        }
        let prev = from_map.get_mut(target).expect("target inserted above");

        let is_relayed = from != me;
        let target_is_neighbour = neighbour == target;
        if target_is_neighbour && !is_relayed {
            // force distance to neighbour as targets to always favor directly
            // sending to them
            distance = -1;
        }

        let mut is_new_remote_session = false;
        if route_did_exist {
            // if the remote session is later, we consider that the remote has
            // 'restarted'. TS reads the stored value as
            // `prev.remoteSession || -1`, so a falsy stored remote session
            // (exactly 0) is coerced to -1: a re-add with remoteSession 0
            // counts as a restart, and merging keeps the coerced -1.
            let prev_remote_session = if prev.remote_session == 0 {
                -1
            } else {
                prev.remote_session
            };
            is_new_remote_session = remote_session > prev_remote_session;
            prev.remote_session = remote_session.max(prev_remote_session);
        }

        prev.session = session.max(prev.session);

        let mut cleanup_requested = false;

        // Update routes and cleanup all old routes that are older than latest
        // session - some threshold
        if is_new_session {
            // Mark previous routes as old
            let expire_at = now_ms + retention;
            let mut found_node_to_expire = false;
            for route in prev.list.iter_mut() {
                if route.expire_at.is_none() {
                    found_node_to_expire = true;
                    route.expire_at = Some(expire_at);
                }
            }
            if distance != -1 && found_node_to_expire {
                self.request_cleanup(from, target);
                cleanup_requested = true;
            }
        } else if is_old_session {
            self.request_cleanup(from, target);
            cleanup_requested = true;
        }

        // request_cleanup borrows self mutably, so re-borrow the entry.
        let from_map = self.routes.get_mut(from).expect("from map present");
        let prev = from_map.get_mut(target).expect("target present");

        // Modify list for new/update route
        let mut exist = false;
        let mut updated = false;
        for index in 0..prev.list.len() {
            if prev.list[index].hash != neighbour {
                continue;
            }
            // if route is faster or just as fast, update existing route
            if is_new_session {
                let route = &mut prev.list[index];
                if route.distance > distance {
                    route.distance = distance;
                    route.session = session;
                    route.updated_at = now_ms;
                    route.expire_at = None; // remove expiry since we updated
                    sort_routes(&mut prev.list);
                    updated = true;
                } else if route.distance == distance {
                    route.session = session;
                    route.updated_at = now_ms;
                    route.expire_at = None; // remove expiry since we updated
                    updated = true;
                }
                if updated {
                    if prev.list.len() > max_relays_per_target {
                        prev.list.truncate(max_relays_per_target);
                    }
                    self.prune_targets(from);
                    self.prune_from_maps();
                    return AddResult {
                        outcome: if is_new_remote_session {
                            AddOutcome::Restart
                        } else {
                            AddOutcome::Updated
                        },
                        cleanup_requested,
                    };
                }
            }
            exist = true;
            // else break and push the route as a new route (that ought to
            // be longer)
            break;
        }

        // if not exist add new route
        // else if it exist then we only end up here if the distance is longer
        // than prev, this means that we want to keep prev while adding the new
        // route
        if !exist || is_new_session {
            prev.list.push(RelayInfo {
                distance,
                session,
                hash: neighbour.to_string(),
                updated_at: now_ms,
                expire_at: if is_old_session {
                    Some(now_ms + retention)
                } else {
                    None
                },
            });
            sort_routes(&mut prev.list);
            if prev.list.len() > max_relays_per_target {
                prev.list.truncate(max_relays_per_target);
            }
        }

        self.prune_targets(from);
        self.prune_from_maps();

        AddResult {
            outcome: if exist {
                if is_new_remote_session {
                    AddOutcome::Restart
                } else {
                    AddOutcome::Updated
                }
            } else {
                AddOutcome::New
            },
            cleanup_requested,
        }
    }

    /// Returns unreachable nodes (from me) after removal.
    pub fn remove(&mut self, target: &str) -> Vec<String> {
        self.routes.shift_remove(target);
        let mut maybe_unreachable: Vec<String> = Vec::new();
        let mut target_removed = false;
        let from_keys: Vec<String> = self.routes.keys().cloned().collect();
        for from_key in from_keys {
            let me = self.me.clone();
            let Some(from_map) = self.routes.get_mut(&from_key) else {
                continue;
            };
            // delete target
            let deleted_as_target = from_map.shift_remove(target).is_some();
            target_removed = target_removed || (deleted_as_target && from_key == me);

            // delete this as neighbour
            let remote_keys: Vec<String> = from_map.keys().cloned().collect();
            for remote in remote_keys {
                let Some(neighbours) = from_map.get_mut(&remote) else {
                    continue;
                };
                neighbours.list.retain(|x| x.hash != target);
                if neighbours.list.is_empty() {
                    from_map.shift_remove(&remote);
                    if from_key == me && !maybe_unreachable.contains(&remote) {
                        maybe_unreachable.push(remote);
                    }
                }
            }
            if from_map.is_empty() {
                self.routes.shift_remove(&from_key);
            }
        }
        self.remote_info.remove(target);

        if target_removed && !maybe_unreachable.contains(&target.to_string()) {
            maybe_unreachable.push(target.to_string());
        }
        let me = self.me.clone();
        maybe_unreachable
            .into_iter()
            .filter(|x| !self.is_reachable(&me, x, MAX_ROUTE_DISTANCE))
            .collect()
    }

    pub fn remove_neighbour(&mut self, neighbour: &str) {
        self.routes.shift_remove(neighbour);
        let from_keys: Vec<String> = self.routes.keys().cloned().collect();
        for from_key in from_keys {
            let Some(from_map) = self.routes.get_mut(&from_key) else {
                continue;
            };
            let keys: Vec<String> = from_map.keys().cloned().collect();
            for key in keys {
                let Some(routes) = from_map.get_mut(&key) else {
                    continue;
                };
                routes.list.retain(|x| x.hash != neighbour);
                if routes.list.is_empty() {
                    from_map.shift_remove(&key);
                }
            }
        }
    }

    pub fn find_neighbor(&self, from: &str, target: &str) -> Option<&RouteInfo> {
        self.routes.get(from)?.get(target)
    }

    pub fn get_route_hints(&self, from: &str, target: &str, now_ms: u64) -> Vec<RouteHint> {
        let Some(route) = self.find_neighbor(from, target) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for next in &route.list {
            if matches!(next.expire_at, Some(at) if at < now_ms) {
                continue;
            }
            out.push(RouteHint {
                from: from.to_string(),
                target: target.to_string(),
                next_hop: next.hash.clone(),
                distance: next.distance,
                session: next.session,
                updated_at: next.updated_at,
                expires_at: next.expire_at,
            });
        }
        out
    }

    pub fn is_reachable(&self, from: &str, target: &str, max_distance: i64) -> bool {
        let Some(remote_session) = self.remote_info.get(target) else {
            return false;
        };
        let Some(route_info) = self.routes.get(from).and_then(|map| map.get(target)) else {
            return false;
        };
        if route_info.remote_session < *remote_session {
            // route info is older than remote info
            return false;
        }
        route_info
            .list
            .first()
            .map(|relay| relay.distance)
            .unwrap_or(i64::MAX)
            <= max_distance
    }

    pub fn has_target(&self, target: &str) -> bool {
        self.routes.values().any(|map| map.contains_key(target))
    }

    pub fn update_session(&mut self, remote: &str, session: Option<i64>) -> bool {
        let Some(session) = session else {
            self.remote_info.remove(remote);
            return false;
        };
        match self.remote_info.get_mut(remote) {
            Some(existing) => {
                // remote has restarted, mark all routes originating from me to
                // the remote as 'old'
                if *existing == -1 {
                    return false;
                }
                if session == -1 {
                    *existing = -1;
                    return false;
                }
                if session > *existing {
                    *existing = session;
                    return true;
                }
                false
            }
            None => {
                self.remote_info.insert(remote.to_string(), session);
                true
            }
        }
    }

    pub fn get_session(&self, remote: &str) -> Option<i64> {
        self.remote_info.get(remote).copied()
    }

    pub fn get_dependent(&self, peer: &str) -> Vec<String> {
        let mut dependent = Vec::new();
        'outer: for (from_key, from_map) in &self.routes {
            if *from_key == self.me {
                continue; // routes starting from me don't affect others
            }
            // If the route is to the target, tell 'from' that it is no longer
            // reachable
            if from_map.contains_key(peer) {
                dependent.push(from_key.clone());
                continue 'outer;
            }
            // If the relay is dependent of peer, tell 'from' that it is no
            // longer reachable
            for through in from_map.values() {
                for neighbour in &through.list {
                    if neighbour.hash == peer {
                        dependent.push(from_key.clone());
                        continue 'outer;
                    }
                }
            }
        }
        dependent
    }

    pub fn count(&self, from: &str) -> usize {
        let mut set: HashSet<&str> = HashSet::new();
        if let Some(map) = self.routes.get(from) {
            for (k, v) in map {
                set.insert(k.as_str());
                for peer in &v.list {
                    set.insert(peer.hash.as_str());
                }
            }
        }
        set.len()
    }

    pub fn count_all(&self) -> usize {
        self.routes
            .values()
            .map(|map| map.values().map(|v| v.list.len()).sum::<usize>())
            .sum()
    }

    /// Fanout selection: which neighbours should carry the message to reach
    /// `tos` with the requested redundancy. `None` means "no full routing
    /// knowledge — flood to all peers instead".
    pub fn get_fanout(
        &self,
        from: &str,
        tos: &[String],
        redundancy: u8,
    ) -> Option<IndexMap<String, IndexMap<String, i64>>> {
        if tos.is_empty() {
            return None;
        }

        let mut fanout_map: Option<IndexMap<String, IndexMap<String, i64>>> = None;
        let relaying = from != self.me;
        let redundancy = redundancy as i64;

        for to in tos {
            if *to == self.me || from == to {
                continue; // don't send to me or backwards
            }

            let Some(neighbour) = self.find_neighbor(from, to) else {
                // we can't find path, send message to all peers
                return None;
            };

            let mut found_closest = false;
            let mut added: i64 = 0;
            let mut found_path_for_distance: i64 = -2;
            for relay in &neighbour.list {
                let (distance, session, expire_at) =
                    (relay.distance, relay.session, relay.expire_at);

                if expire_at.is_some() {
                    // don't send on old paths if not relaying and if we have
                    // already found a path for the same distance
                    if !relaying && found_path_for_distance == distance {
                        continue;
                    }
                } else {
                    found_path_for_distance = distance;
                }

                if distance >= redundancy {
                    break; // because neighbour list is sorted
                }

                let map = fanout_map.get_or_insert_with(IndexMap::new);
                map.entry(relay.hash.clone())
                    .or_default()
                    .insert(to.clone(), session);

                if distance <= 0 && session <= neighbour.session {
                    found_closest = true;
                    if distance == -1 {
                        break; // dont send to more peers if we have the direct route
                    }
                }

                if expire_at.is_none() {
                    added += 1;
                    if added >= redundancy {
                        break;
                    }
                }
            }

            if !found_closest && from == self.me {
                return None; // we dont have the shortest path to our target (yet). Send to all
            }
        }
        Some(fanout_map.unwrap_or_default())
    }

    /// Returns a list of prunable nodes that are not needed to reach all
    /// remote nodes.
    pub fn get_prunable(&self, neighbours: &[String]) -> Vec<String> {
        let Some(map) = self.routes.get(&self.me) else {
            return Vec::new();
        };
        neighbours
            .iter()
            .filter(|candidate| {
                for (target, neighbours) in map {
                    if target != *candidate
                        && neighbours.list.len() == 1
                        && neighbours.list[0].hash == **candidate
                    {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &IndexMap<String, RouteInfo>)> {
        self.routes.iter()
    }
}

// --- JSON snapshots (host-facing observability; hand-rolled like wire.rs) ---

use crate::wire::push_json_string;

fn push_route_info(out: &mut String, info: &RouteInfo) {
    out.push_str(&format!(
        "{{\"session\":{},\"remoteSession\":{},\"list\":[",
        info.session, info.remote_session
    ));
    for (index, relay) in info.list.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str("{\"hash\":");
        push_json_string(out, &relay.hash);
        out.push_str(&format!(
            ",\"distance\":{},\"session\":{},\"updatedAt\":{},\"expireAt\":{}}}",
            relay.distance,
            relay.session,
            relay.updated_at,
            relay
                .expire_at
                .map(|at| at.to_string())
                .unwrap_or_else(|| "null".to_string())
        ));
    }
    out.push_str("]}");
}

impl Routes {
    pub fn route_info_json(&self, from: &str, target: &str) -> Option<String> {
        let info = self.find_neighbor(from, target)?;
        let mut out = String::new();
        push_route_info(&mut out, info);
        Some(out)
    }

    /// Full snapshot as ordered `[[from, [[to, routeInfo], ...]], ...]` pairs.
    pub fn dump_json(&self) -> String {
        let mut out = String::from("[");
        for (from_index, (from, from_map)) in self.routes.iter().enumerate() {
            if from_index > 0 {
                out.push(',');
            }
            out.push('[');
            push_json_string(&mut out, from);
            out.push_str(",[");
            for (target_index, (target, info)) in from_map.iter().enumerate() {
                if target_index > 0 {
                    out.push(',');
                }
                out.push('[');
                push_json_string(&mut out, target);
                out.push(',');
                push_route_info(&mut out, info);
                out.push(']');
            }
            out.push_str("]]");
        }
        out.push(']');
        out
    }

    pub fn route_hints_json(&self, from: &str, target: &str, now_ms: u64) -> String {
        let hints = self.get_route_hints(from, target, now_ms);
        let mut out = String::from("[");
        for (index, hint) in hints.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str("{\"from\":");
            push_json_string(&mut out, &hint.from);
            out.push_str(",\"target\":");
            push_json_string(&mut out, &hint.target);
            out.push_str(",\"nextHop\":");
            push_json_string(&mut out, &hint.next_hop);
            out.push_str(&format!(
                ",\"distance\":{},\"session\":{},\"updatedAt\":{},\"expiresAt\":{}}}",
                hint.distance,
                hint.session,
                hint.updated_at,
                hint.expires_at
                    .map(|at| at.to_string())
                    .unwrap_or_else(|| "null".to_string())
            ));
        }
        out.push(']');
        out
    }

    /// `None` = flood to all peers; otherwise ordered
    /// `[[neighbour, [[to, sessionTimestamp], ...]], ...]` pairs.
    pub fn fanout_json(&self, from: &str, tos: &[String], redundancy: u8) -> Option<String> {
        let fanout = self.get_fanout(from, tos, redundancy)?;
        let mut out = String::from("[");
        for (index, (neighbour, targets)) in fanout.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push('[');
            push_json_string(&mut out, neighbour);
            out.push_str(",[");
            for (target_index, (target, session)) in targets.iter().enumerate() {
                if target_index > 0 {
                    out.push(',');
                }
                out.push('[');
                push_json_string(&mut out, target);
                out.push_str(&format!(",{session}]"));
            }
            out.push_str("]]");
        }
        out.push(']');
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000_000;

    fn routes(me: &str) -> Routes {
        Routes::new(me.to_string(), Some(10_000), None, None, None)
    }

    #[test]
    fn add_one() {
        let mut r = routes("me");
        r.add("me", "n", "t", 0, 0, -1, NOW);
        assert_eq!(r.count("me"), 2);
    }

    #[test]
    fn add_new_session_expires_old_route() {
        let mut r = routes("me");
        r.add("me", "a", "t", 1, 0, -1, NOW);
        let result = r.add("me", "b", "t", 1, 1, -1, NOW);
        assert_eq!(result.outcome, AddOutcome::New);
        assert!(result.cleanup_requested);
        let info = r.find_neighbor("me", "t").unwrap();
        assert_eq!(info.list.len(), 2);
        // fresh route sorts first among equal distances
        assert_eq!(info.list[0].hash, "b");
        assert_eq!(info.list[0].expire_at, None);
        assert_eq!(info.list[1].hash, "a");
        assert_eq!(info.list[1].expire_at, Some(NOW + 10_000));
        // after the retention period, cleanup drops the old route
        r.cleanup_pending(NOW + 10_001);
        let info = r.find_neighbor("me", "t").unwrap();
        assert_eq!(info.list.len(), 1);
        assert_eq!(info.list[0].hash, "b");
    }

    #[test]
    fn add_old_session_expires_new_route() {
        let mut r = routes("me");
        r.add("me", "a", "t", 1, 1, -1, NOW);
        let result = r.add("me", "b", "t", 1, 0, -1, NOW);
        assert!(result.cleanup_requested);
        let info = r.find_neighbor("me", "t").unwrap();
        assert_eq!(info.list.len(), 2);
        assert_eq!(info.list[0].hash, "a");
        assert_eq!(info.list[1].hash, "b");
        assert_eq!(info.list[1].expire_at, Some(NOW + 10_000));
        r.cleanup_pending(NOW + 10_001);
        let info = r.find_neighbor("me", "t").unwrap();
        assert_eq!(info.list.len(), 1);
        assert_eq!(info.list[0].hash, "a");
    }

    #[test]
    fn same_session_update_resets_expiry() {
        let mut r = routes("me");
        // direct neighbour: distance forced to -1
        r.add("me", "n", "n", 0, 100, -1, NOW);
        let result = r.add("me", "n", "n", 0, 200, -1, NOW);
        assert_eq!(result.outcome, AddOutcome::Updated);
        let info = r.find_neighbor("me", "n").unwrap();
        assert_eq!(info.list.len(), 1);
        assert_eq!(info.list[0].expire_at, None);
    }

    #[test]
    fn re_add_with_remote_session_zero_is_a_restart() {
        // TS compares against `prev.remoteSession || -1`: a stored remote
        // session of exactly 0 is coerced to -1, so a re-add with
        // remoteSession 0 reports "restart" rather than "updated".
        let mut r = routes("me");
        assert_eq!(
            r.add("me", "n", "t", 0, 100, 0, NOW).outcome,
            AddOutcome::New
        );
        assert_eq!(
            r.add("me", "n", "t", 0, 100, 0, NOW).outcome,
            AddOutcome::Restart
        );
        // the merge keeps the stored value at max(0, -1) = 0, so it keeps
        // reporting restart
        assert_eq!(
            r.add("me", "n", "t", 0, 100, 0, NOW).outcome,
            AddOutcome::Restart
        );

        // the coercion also applies to the merge: a lower incoming remote
        // session stores Math.max(-5, 0 || -1) = -1
        let mut r = routes("me");
        r.add("me", "n", "t", 0, 100, 0, NOW);
        assert_eq!(
            r.add("me", "n", "t", 0, 100, -5, NOW).outcome,
            AddOutcome::Updated
        );
        assert_eq!(r.find_neighbor("me", "t").unwrap().remote_session, -1);

        // non-zero stored remote sessions are not coerced
        let mut r = routes("me");
        r.add("me", "n", "t", 0, 100, 5, NOW);
        assert_eq!(
            r.add("me", "n", "t", 0, 100, 5, NOW).outcome,
            AddOutcome::Updated
        );
        assert_eq!(
            r.add("me", "n", "t", 0, 100, 6, NOW).outcome,
            AddOutcome::Restart
        );
    }

    #[test]
    fn direct_connection_distance_is_forced() {
        let mut r = routes("me");
        r.add("me", "n", "n", 5, 0, -1, NOW);
        assert_eq!(r.find_neighbor("me", "n").unwrap().list[0].distance, -1);
        // relayed adds are not forced
        r.add("other", "n", "n", 5, 0, -1, NOW);
        assert_eq!(r.find_neighbor("other", "n").unwrap().list[0].distance, 5);
    }

    #[test]
    fn reachability_needs_fresh_remote_session() {
        let mut r = routes("me");
        r.add("me", "n", "t", 0, 1, 10, NOW);
        assert!(!r.is_reachable("me", "t", MAX_ROUTE_DISTANCE));
        r.update_session("t", Some(5));
        assert!(r.is_reachable("me", "t", MAX_ROUTE_DISTANCE));
        // remote restarted with a later session than our route knows about
        r.update_session("t", Some(20));
        assert!(!r.is_reachable("me", "t", MAX_ROUTE_DISTANCE));
    }

    #[test]
    fn update_session_semantics() {
        let mut r = routes("me");
        assert!(r.update_session("t", Some(-1)));
        // locked at -1 until removed
        assert!(!r.update_session("t", Some(5)));
        assert_eq!(r.get_session("t"), Some(-1));
        assert!(!r.update_session("t", None));
        assert_eq!(r.get_session("t"), None);
        assert!(r.update_session("t", Some(5)));
        assert!(!r.update_session("t", Some(4)));
        assert!(r.update_session("t", Some(6)));
    }

    #[test]
    fn remove_reports_unreachable() {
        let mut r = routes("me");
        r.add("me", "n", "n", 0, 0, -1, NOW);
        r.add("me", "n", "t", 1, 0, -1, NOW);
        r.update_session("n", Some(1));
        r.update_session("t", Some(1));
        let unreachable = r.remove("n");
        assert!(unreachable.contains(&"n".to_string()));
        assert!(unreachable.contains(&"t".to_string()));
        assert_eq!(r.count("me"), 0);
    }

    #[test]
    fn fanout_me_will_not_send_through_expired_when_not_relaying() {
        let mut r = routes("me");
        r.add("me", "a", "t", 0, 0, -1, NOW);
        // new session expires the a-route and adds b
        r.add("me", "b", "t", 0, 1, -1, NOW);
        let fanout = r
            .get_fanout("me", &["t".to_string()], 2)
            .expect("has routes");
        // b is fresh (distance 0); a is expired with the same distance → skipped
        assert_eq!(fanout.len(), 1);
        assert!(fanout.contains_key("b"));
    }

    #[test]
    fn fanout_relay_sends_through_expired() {
        let mut r = routes("me");
        r.add("from", "a", "t", 0, 0, -1, NOW);
        r.add("from", "b", "t", 0, 1, -1, NOW);
        let fanout = r
            .get_fanout("from", &["t".to_string()], 2)
            .expect("has routes");
        assert_eq!(fanout.len(), 2);
    }

    #[test]
    fn fanout_without_closest_path_floods() {
        let mut r = routes("me");
        // only a distance-1 path from me: not the shortest possible → None
        r.add("me", "a", "t", 1, 0, -1, NOW);
        assert_eq!(r.get_fanout("me", &["t".to_string()], 1), None);
    }

    #[test]
    fn fanout_direct_route_short_circuits() {
        let mut r = routes("me");
        r.add("me", "t", "t", 0, 0, -1, NOW); // forced distance -1
        r.add("me", "b", "t", 0, 0, -1, NOW);
        let fanout = r
            .get_fanout("me", &["t".to_string()], 2)
            .expect("has routes");
        assert_eq!(fanout.len(), 1);
        assert!(fanout.contains_key("t"));
    }

    #[test]
    fn prunable_keeps_last_route_to_target() {
        let mut r = routes("me");
        r.add("me", "a", "t", 1, 0, -1, NOW);
        r.add("me", "a", "a", 0, 0, -1, NOW);
        r.add("me", "b", "b", 0, 0, -1, NOW);
        let prunable = r.get_prunable(&["a".to_string(), "b".to_string()]);
        assert_eq!(prunable, vec!["b".to_string()]);
    }

    #[test]
    fn dependent_excludes_me() {
        let mut r = routes("me");
        r.add("me", "n", "t", 0, 0, -1, NOW);
        r.add("other", "n", "t", 0, 0, -1, NOW);
        let dependent = r.get_dependent("t");
        assert_eq!(dependent, vec!["other".to_string()]);
    }

    #[test]
    fn max_relays_per_target_bound() {
        let mut r = Routes::new("me".to_string(), Some(10_000), None, None, Some(2));
        r.add("me", "a", "t", 0, 0, -1, NOW);
        r.add("me", "b", "t", 1, 0, -1, NOW);
        r.add("me", "c", "t", 2, 0, -1, NOW);
        assert_eq!(r.find_neighbor("me", "t").unwrap().list.len(), 2);
    }

    #[test]
    fn lru_bounds_from_entries() {
        let mut r = Routes::new("me".to_string(), Some(10_000), Some(2), None, None);
        r.add("me", "a", "t", 0, 0, -1, NOW);
        r.add("f1", "a", "t", 0, 0, -1, NOW);
        r.add("f2", "a", "t", 0, 0, -1, NOW);
        // me stays pinned; oldest non-me entry evicted
        assert!(r.find_neighbor("me", "t").is_some());
        assert!(r.find_neighbor("f1", "t").is_none());
        assert!(r.find_neighbor("f2", "t").is_some());
    }
}
