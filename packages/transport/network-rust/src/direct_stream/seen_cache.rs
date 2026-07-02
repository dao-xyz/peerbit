//! Seen-cache dedup counter: a port of the `@peerbit/cache` FIFO cache
//! (`packages/utils/cache/src/index.ts`) specialized to the DirectStream
//! seen-cache usage (`modifySeenCache` in `stream/src/index.ts`).
//!
//! Message keys mirror the TS ids byte-for-byte in information content:
//! `getMsgId` = the first 33 bytes of the frame (discriminator + header id),
//! and the ACK path keys by sha256 of the whole frame. The TS side base64s
//! these into strings; raw bytes are an equivalent (injective) key space.

use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};

pub const KEY_KIND_MESSAGE_ID: u8 = 0;
pub const KEY_KIND_SHA256: u8 = 1;

struct CacheEntry {
    time: u64,
    value: u32,
}

pub struct SeenCache {
    max: usize,
    ttl_ms: u64,
    map: HashMap<Vec<u8>, CacheEntry>,
    list: VecDeque<Vec<u8>>,
    current_size: usize,
}

impl SeenCache {
    pub fn new(max: usize, ttl_ms: u64) -> Self {
        SeenCache {
            max: max.max(1),
            ttl_ms: ttl_ms.max(1),
            map: HashMap::new(),
            list: VecDeque::new(),
            current_size: 0,
        }
    }

    fn trim(&mut self, now_ms: u64) {
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
                self.map
                    .remove(&self.list.pop_front().expect("head exists"));
                self.current_size -= 1;
            } else {
                break;
            }
        }
    }

    pub fn get(&mut self, key: &[u8], now_ms: u64) -> Option<u32> {
        self.trim(now_ms);
        self.map.get(key).map(|entry| entry.value)
    }

    pub fn add(&mut self, key: &[u8], value: u32, now_ms: u64) {
        if !self.map.contains_key(key) {
            self.list.push_back(key.to_vec());
            self.current_size += 1;
        }
        self.map.insert(
            key.to_vec(),
            CacheEntry {
                time: now_ms,
                value,
            },
        );
        self.trim(now_ms);
    }

    /// `modifySeenCache`: bump the seen counter for the frame and return how
    /// many times it was seen before.
    pub fn modify(&mut self, frame: &[u8], key_kind: u8, now_ms: u64) -> u32 {
        let key: Vec<u8> = match key_kind {
            KEY_KIND_SHA256 => Sha256::digest(frame).to_vec(),
            // discriminator + 32-byte header id
            _ => frame[..frame.len().min(33)].to_vec(),
        };
        let seen = self.get(&key, now_ms);
        self.add(&key, seen.map(|s| s + 1).unwrap_or(1), now_ms);
        seen.unwrap_or(0)
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.list.clear();
        self.current_size = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000_000;

    #[test]
    fn counts_repeat_frames() {
        let mut cache = SeenCache::new(1000, 10_000);
        let frame = [7u8; 40];
        assert_eq!(cache.modify(&frame, KEY_KIND_MESSAGE_ID, NOW), 0);
        assert_eq!(cache.modify(&frame, KEY_KIND_MESSAGE_ID, NOW), 1);
        assert_eq!(cache.modify(&frame, KEY_KIND_MESSAGE_ID, NOW), 2);
    }

    #[test]
    fn message_id_key_is_first_33_bytes() {
        let mut cache = SeenCache::new(1000, 10_000);
        let mut a = vec![0u8; 64];
        let mut b = vec![0u8; 64];
        a[40] = 1;
        b[40] = 2; // differs only outside the id prefix
        assert_eq!(cache.modify(&a, KEY_KIND_MESSAGE_ID, NOW), 0);
        assert_eq!(cache.modify(&b, KEY_KIND_MESSAGE_ID, NOW), 1);
        // sha256 keying distinguishes them
        let mut cache = SeenCache::new(1000, 10_000);
        assert_eq!(cache.modify(&a, KEY_KIND_SHA256, NOW), 0);
        assert_eq!(cache.modify(&b, KEY_KIND_SHA256, NOW), 0);
    }

    #[test]
    fn ttl_expires_counters() {
        let mut cache = SeenCache::new(1000, 10_000);
        let frame = [7u8; 40];
        assert_eq!(cache.modify(&frame, KEY_KIND_MESSAGE_ID, NOW), 0);
        assert_eq!(cache.modify(&frame, KEY_KIND_MESSAGE_ID, NOW + 10_001), 0);
    }

    #[test]
    fn max_bound_evicts_fifo() {
        let mut cache = SeenCache::new(2, 1_000_000);
        assert_eq!(cache.modify(&[1u8; 40], KEY_KIND_MESSAGE_ID, NOW), 0);
        assert_eq!(cache.modify(&[2u8; 40], KEY_KIND_MESSAGE_ID, NOW), 0);
        assert_eq!(cache.modify(&[3u8; 40], KEY_KIND_MESSAGE_ID, NOW), 0);
        // first key evicted → counter reset
        assert_eq!(cache.modify(&[1u8; 40], KEY_KIND_MESSAGE_ID, NOW), 0);
    }
}
