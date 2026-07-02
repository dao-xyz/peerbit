//! JsValue-free port of the `/peerbit/direct-block/1.0.0` block-exchange
//! protocol (`packages/transport/blocks`): the borsh `BlockMessage` codec
//! (`BlockRequest`/`BlockResponse`), the provider-hint cache and provider
//! resolution rules of `RemoteBlocks`, and the eager-block bookkeeping. The
//! host keeps sockets, promises and byte buffers; block bytes only cross the
//! boundary inside serialized payloads.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::wire::{Reader, WireResult, Writer};

pub const BLOCK_MESSAGE_VARIANT_REQUEST: u8 = 0;
pub const BLOCK_MESSAGE_VARIANT_RESPONSE: u8 = 1;

/// `defaultResolveProviders` bound in `blocks/src/libp2p.ts`: negotiated
/// peers first, then connected peers, capped at 32 candidates.
pub const DEFAULT_PROVIDER_CANDIDATE_CAP: usize = 32;

/// `maxProviderHintsPerCid` default (`providerCache.maxProvidersPerCid`).
pub const DEFAULT_MAX_PROVIDERS_PER_CID: usize = 8;

/// `pickRequestBatch` probes at most two providers per attempt.
pub const REQUEST_BATCH_SIZE: usize = 2;

/// A decoded `BlockMessage`. Response payload bytes are reported as a range
/// into the input frame so the host can alias them without a copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedBlockMessage {
    Request {
        cid: String,
    },
    Response {
        cid: String,
        bytes_offset: usize,
        bytes_length: usize,
    },
}

pub fn decode_block_message(frame: &[u8]) -> WireResult<DecodedBlockMessage> {
    let mut reader = Reader::new(frame);
    let variant = reader.u8()?;
    let message = match variant {
        BLOCK_MESSAGE_VARIANT_REQUEST => DecodedBlockMessage::Request {
            cid: reader.string()?,
        },
        BLOCK_MESSAGE_VARIANT_RESPONSE => {
            let cid = reader.string()?;
            let bytes_length = reader.u32_le()? as usize;
            let bytes_offset = reader.offset;
            reader.take(bytes_length)?;
            DecodedBlockMessage::Response {
                cid,
                bytes_offset,
                bytes_length,
            }
        }
        other => return Err(format!("unknown block message variant {other}")),
    };
    if reader.remaining() != 0 {
        return Err("trailing bytes after block message".to_string());
    }
    Ok(message)
}

pub fn encode_block_request(cid: &str) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(BLOCK_MESSAGE_VARIANT_REQUEST);
    writer.string(cid);
    writer.bytes
}

pub fn encode_block_response(cid: &str, bytes: &[u8]) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(BLOCK_MESSAGE_VARIANT_RESPONSE);
    writer.string(cid);
    writer.u32_le(bytes.len() as u32);
    writer.raw(bytes);
    writer.bytes
}

/// `normalizeProviderHints`: drop empties/self, dedupe preserving order, cap
/// at `limit`.
pub fn normalize_provider_hints(providers: &[String], me: &str, limit: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for provider in providers {
        if provider.is_empty() || provider == me {
            continue;
        }
        if out.iter().any(|existing| existing == provider) {
            continue;
        }
        out.push(provider.clone());
        if out.len() >= limit {
            break;
        }
    }
    out
}

/// `pickRequestBatch`: rotate through the provider list two at a time, keyed
/// by the attempt counter, so retries spread over all candidates.
pub fn pick_request_batch(providers: &[String], me: &str, attempt: usize) -> Vec<String> {
    if providers.len() <= 1 {
        return providers.to_vec();
    }
    let batch_size = REQUEST_BATCH_SIZE.min(providers.len());
    let start = (attempt * batch_size) % providers.len();
    let mut batch: Vec<String> = Vec::with_capacity(batch_size);
    for i in 0..batch_size {
        batch.push(providers[(start + i) % providers.len()].clone());
    }
    normalize_provider_hints(&batch, me, batch_size)
}

/// Default provider resolution: peers with a negotiated block-exchange
/// stream first, then connected libp2p peers, deduped and capped at
/// [`DEFAULT_PROVIDER_CANDIDATE_CAP`].
pub fn default_provider_candidates(
    negotiated: &[String],
    connected: &[String],
    me: &str,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let push = |hash: &String, out: &mut Vec<String>| {
        if hash.is_empty() || hash == me {
            return;
        }
        if out.iter().any(|existing| existing == hash) {
            return;
        }
        out.push(hash.clone());
    };
    for hash in negotiated {
        push(hash, &mut out);
        if out.len() >= DEFAULT_PROVIDER_CANDIDATE_CAP {
            return out;
        }
    }
    for hash in connected {
        push(hash, &mut out);
        if out.len() >= DEFAULT_PROVIDER_CANDIDATE_CAP {
            break;
        }
    }
    out
}

struct FifoEntry<V> {
    time: u64,
    value: V,
}

/// Port of the `@peerbit/cache` FIFO cache (`packages/utils/cache`) with the
/// lazy-delete set, specialized to string keys and per-entry size 1 (the
/// only shape the block exchange uses).
struct FifoCache<V> {
    max: usize,
    ttl_ms: u64,
    map: HashMap<String, FifoEntry<V>>,
    list: VecDeque<String>,
    deleted: HashSet<String>,
    current_size: usize,
}

impl<V> FifoCache<V> {
    fn new(max: usize, ttl_ms: u64) -> Self {
        FifoCache {
            max: max.max(1),
            ttl_ms: ttl_ms.max(1),
            map: HashMap::new(),
            list: VecDeque::new(),
            deleted: HashSet::new(),
            current_size: 0,
        }
    }

    fn trim(&mut self, now_ms: u64, evicted: Option<&mut Vec<String>>) {
        let mut sink = evicted;
        loop {
            let Some(head) = self.list.front() else {
                break;
            };
            let Some(entry) = self.map.get(head) else {
                // Defensive: the TS cache treats a missing head as a fatal
                // invariant break; dropping the stale key keeps the cache
                // usable without panicking inside wasm.
                self.list.pop_front();
                continue;
            };
            let out_of_date = entry.time < now_ms.saturating_sub(self.ttl_ms);
            if out_of_date || self.current_size > self.max {
                let key = self.list.pop_front().expect("head exists");
                self.map.remove(&key);
                let was_deleted = self.deleted.remove(&key);
                if !was_deleted {
                    self.current_size -= 1;
                }
                if let Some(sink) = sink.as_mut() {
                    sink.push(key);
                }
            } else {
                break;
            }
        }
    }

    fn get(&mut self, key: &str, now_ms: u64) -> Option<&V> {
        self.trim(now_ms, None);
        if self.deleted.contains(key) {
            return None;
        }
        self.map.get(key).map(|entry| &entry.value)
    }

    fn add(&mut self, key: &str, value: V, now_ms: u64) -> Vec<String> {
        self.deleted.remove(key);
        if !self.map.contains_key(key) {
            self.list.push_back(key.to_string());
            self.current_size += 1;
        }
        self.map.insert(
            key.to_string(),
            FifoEntry {
                time: now_ms,
                value,
            },
        );
        let mut evicted = Vec::new();
        self.trim(now_ms, Some(&mut evicted));
        evicted
    }

    fn del(&mut self, key: &str) {
        if self.map.contains_key(key) && !self.deleted.contains(key) {
            self.deleted.insert(key.to_string());
            self.current_size -= 1;
        }
    }

    fn contains(&self, key: &str) -> bool {
        !self.deleted.contains(key) && self.map.contains_key(key)
    }

    fn sweep(&mut self, now_ms: u64) -> Vec<String> {
        let mut evicted = Vec::new();
        self.trim(now_ms, Some(&mut evicted));
        evicted
    }

    fn clear(&mut self) {
        self.map.clear();
        self.list.clear();
        self.deleted.clear();
        self.current_size = 0;
    }
}

/// Provider-hint cache of `RemoteBlocks` (`_providerCache` plus the
/// `rememberProvider`/`rememberProviderHints` rules).
pub struct ProviderHintCache {
    me: String,
    max_providers_per_cid: usize,
    cache: FifoCache<Vec<String>>,
}

impl ProviderHintCache {
    pub fn new(me: String, max_entries: usize, ttl_ms: u64, max_providers_per_cid: usize) -> Self {
        ProviderHintCache {
            me,
            max_providers_per_cid: max_providers_per_cid.max(1),
            cache: FifoCache::new(max_entries, ttl_ms),
        }
    }

    pub fn get(&mut self, cid: &str, now_ms: u64) -> Option<Vec<String>> {
        self.cache.get(cid, now_ms).cloned()
    }

    /// `rememberProvider`: front-insert the responding provider, keep the
    /// rest of the current list in order, bounded per cid.
    pub fn remember_provider(&mut self, cid: &str, provider: &str, now_ms: u64) {
        if provider.is_empty() || provider == self.me {
            return;
        }
        let current = self.cache.get(cid, now_ms).cloned().unwrap_or_default();
        let mut next: Vec<String> = vec![provider.to_string()];
        for existing in current {
            if existing == provider || existing.is_empty() || existing == self.me {
                continue;
            }
            next.push(existing);
            if next.len() >= self.max_providers_per_cid {
                break;
            }
        }
        self.cache.add(cid, next, now_ms);
    }

    /// `rememberProviderHints`: replace the cached list with the normalized
    /// hints (no-op when nothing survives normalization).
    pub fn remember_hints(&mut self, cid: &str, providers: &[String], now_ms: u64) {
        let normalized = normalize_provider_hints(providers, &self.me, self.max_providers_per_cid);
        if normalized.is_empty() {
            return;
        }
        self.cache.add(cid, normalized, now_ms);
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

/// Eager-block bookkeeping (`_blockCache` in `RemoteBlocks`): which
/// unresolved `BlockResponse` cids to keep and for how long. The block bytes
/// themselves stay host-side; eviction notices tell the host which byte
/// buffers to drop.
pub struct EagerBlockIndex {
    cache: FifoCache<()>,
}

impl EagerBlockIndex {
    pub fn new(max: usize, ttl_ms: u64) -> Self {
        EagerBlockIndex {
            cache: FifoCache::new(max, ttl_ms),
        }
    }

    pub fn add(&mut self, cid: &str, now_ms: u64) -> Vec<String> {
        self.cache.add(cid, (), now_ms)
    }

    pub fn sweep(&mut self, now_ms: u64) -> Vec<String> {
        self.cache.sweep(now_ms)
    }

    pub fn contains(&self, cid: &str) -> bool {
        self.cache.contains(cid)
    }

    pub fn del(&mut self, cid: &str) {
        self.cache.del(cid);
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000_000;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn block_request_encodes_borsh_layout() {
        let encoded = encode_block_request("zb2abc");
        let mut expected = vec![BLOCK_MESSAGE_VARIANT_REQUEST];
        expected.extend_from_slice(&6u32.to_le_bytes());
        expected.extend_from_slice(b"zb2abc");
        assert_eq!(encoded, expected);
    }

    #[test]
    fn block_response_encodes_borsh_layout() {
        let encoded = encode_block_response("cid", &[9, 8, 7]);
        let mut expected = vec![BLOCK_MESSAGE_VARIANT_RESPONSE];
        expected.extend_from_slice(&3u32.to_le_bytes());
        expected.extend_from_slice(b"cid");
        expected.extend_from_slice(&3u32.to_le_bytes());
        expected.extend_from_slice(&[9, 8, 7]);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn block_messages_roundtrip() {
        let request = encode_block_request("zb2rhbnwih");
        assert_eq!(
            decode_block_message(&request).unwrap(),
            DecodedBlockMessage::Request {
                cid: "zb2rhbnwih".to_string(),
            },
        );

        let payload = [5u8, 4, 3];
        let response = encode_block_response("zb2rhbnwih", &payload);
        let decoded = decode_block_message(&response).unwrap();
        match decoded {
            DecodedBlockMessage::Response {
                cid,
                bytes_offset,
                bytes_length,
            } => {
                assert_eq!(cid, "zb2rhbnwih");
                assert_eq!(
                    &response[bytes_offset..bytes_offset + bytes_length],
                    payload
                );
            }
            other => panic!("expected response, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_bad_frames() {
        assert!(decode_block_message(&[]).is_err());
        assert!(decode_block_message(&[2, 0, 0, 0, 0]).is_err());
        // truncated response payload
        let mut truncated = encode_block_response("cid", &[1, 2, 3]);
        truncated.pop();
        assert!(decode_block_message(&truncated).is_err());
        // trailing bytes
        let mut trailing = encode_block_request("cid");
        trailing.push(0);
        assert!(decode_block_message(&trailing).is_err());
    }

    #[test]
    fn normalize_dedupes_and_caps() {
        let providers = strings(&["a", "", "me", "b", "a", "c", "d"]);
        assert_eq!(
            normalize_provider_hints(&providers, "me", 3),
            strings(&["a", "b", "c"]),
        );
        assert_eq!(normalize_provider_hints(&[], "me", 8), Vec::<String>::new());
    }

    #[test]
    fn pick_request_batch_rotates_by_attempt() {
        let providers = strings(&["a", "b", "c"]);
        assert_eq!(
            pick_request_batch(&providers, "me", 0),
            strings(&["a", "b"])
        );
        assert_eq!(
            pick_request_batch(&providers, "me", 1),
            strings(&["c", "a"])
        );
        assert_eq!(
            pick_request_batch(&providers, "me", 2),
            strings(&["b", "c"])
        );
        assert_eq!(
            pick_request_batch(&providers, "me", 3),
            strings(&["a", "b"])
        );
        // single provider short-circuits without normalization
        assert_eq!(
            pick_request_batch(&strings(&["x"]), "me", 5),
            strings(&["x"])
        );
    }

    #[test]
    fn default_candidates_prefer_negotiated_and_cap_at_32() {
        let negotiated = strings(&["n1", "me", "n2"]);
        let connected = strings(&["n2", "c1", "c2"]);
        assert_eq!(
            default_provider_candidates(&negotiated, &connected, "me"),
            strings(&["n1", "n2", "c1", "c2"]),
        );

        let many: Vec<String> = (0..40).map(|i| format!("peer-{i}")).collect();
        let capped = default_provider_candidates(&many, &[], "me");
        assert_eq!(capped.len(), DEFAULT_PROVIDER_CANDIDATE_CAP);
        assert_eq!(capped[0], "peer-0");
        assert_eq!(capped[31], "peer-31");
        // negotiated already saturated the cap: connected is not consulted
        let capped = default_provider_candidates(&many, &strings(&["extra"]), "me");
        assert!(!capped.contains(&"extra".to_string()));
    }

    #[test]
    fn provider_cache_remembers_responders_first() {
        let mut cache = ProviderHintCache::new("me".to_string(), 2048, 600_000, 8);
        cache.remember_hints("cid", &strings(&["a", "b"]), NOW);
        assert_eq!(cache.get("cid", NOW), Some(strings(&["a", "b"])));

        cache.remember_provider("cid", "b", NOW);
        assert_eq!(cache.get("cid", NOW), Some(strings(&["b", "a"])));

        // self and empty hashes never enter the cache
        cache.remember_provider("cid", "me", NOW);
        cache.remember_provider("cid", "", NOW);
        assert_eq!(cache.get("cid", NOW), Some(strings(&["b", "a"])));

        // per-cid bound applies after the front-insert
        let mut bounded = ProviderHintCache::new("me".to_string(), 2048, 600_000, 2);
        bounded.remember_hints("cid", &strings(&["a", "b"]), NOW);
        bounded.remember_provider("cid", "c", NOW);
        assert_eq!(bounded.get("cid", NOW), Some(strings(&["c", "a"])));
    }

    #[test]
    fn provider_cache_expires_by_ttl() {
        let mut cache = ProviderHintCache::new("me".to_string(), 2048, 600_000, 8);
        cache.remember_hints("cid", &strings(&["a"]), NOW);
        assert_eq!(cache.get("cid", NOW + 600_000), Some(strings(&["a"])));
        assert_eq!(cache.get("cid", NOW + 600_001), None);
    }

    #[test]
    fn provider_cache_ignores_unnormalizable_hints() {
        let mut cache = ProviderHintCache::new("me".to_string(), 2048, 600_000, 8);
        cache.remember_hints("cid", &strings(&["me", ""]), NOW);
        assert_eq!(cache.get("cid", NOW), None);
    }

    #[test]
    fn eager_index_reports_evictions() {
        let mut index = EagerBlockIndex::new(2, 10_000);
        assert!(index.add("a", NOW).is_empty());
        assert!(index.add("b", NOW).is_empty());
        assert_eq!(index.add("c", NOW), strings(&["a"]));
        assert!(index.contains("b"));
        assert!(index.contains("c"));
        assert!(!index.contains("a"));

        // ttl expiry surfaces through sweep
        assert_eq!(index.sweep(NOW + 10_001), strings(&["b", "c"]));
        assert!(!index.contains("b"));

        // lazy delete frees capacity without breaking fifo order
        let mut index = EagerBlockIndex::new(2, 10_000);
        index.add("a", NOW);
        index.del("a");
        assert!(!index.contains("a"));
        assert!(index.add("b", NOW).is_empty());
        assert!(index.add("c", NOW).is_empty());
    }
}
