//! JsValue-free port of the `/peerbit/topic-control-plane/2.0.0` control
//! plane (`packages/transport/pubsub`): the borsh `PubSubMessage` codec
//! (variants 0-7), the `TopicRootDirectory` root-resolution state, the
//! FNV-1a topic hashing that keys shard mapping and deterministic root
//! selection, and the subscribe-state convergence rules (subscription
//! watermarks and session replacement). The host keeps the observable
//! subscription maps, sockets, timers and events; every protocol decision
//! that feeds them runs here.

use std::collections::{HashMap, HashSet};

use crate::wire::{Reader, WireResult, Writer};

pub const PUBSUB_VARIANT_DATA: u8 = 0;
pub const PUBSUB_VARIANT_SUBSCRIBE: u8 = 1;
pub const PUBSUB_VARIANT_UNSUBSCRIBE: u8 = 2;
pub const PUBSUB_VARIANT_GET_SUBSCRIBERS: u8 = 3;
pub const PUBSUB_VARIANT_TOPIC_ROOT_CANDIDATES: u8 = 4;
pub const PUBSUB_VARIANT_PEER_UNAVAILABLE: u8 = 5;
pub const PUBSUB_VARIANT_TOPIC_ROOT_QUERY: u8 = 6;
pub const PUBSUB_VARIANT_TOPIC_ROOT_QUERY_RESPONSE: u8 = 7;

/// `AUTO_TOPIC_ROOT_CANDIDATES_MAX` in `pubsub/src/index.ts`.
pub const AUTO_TOPIC_ROOT_CANDIDATES_MAX: usize = 64;

/// A decoded `PubSubMessage`. `Data` payload bytes are reported as a range
/// into the input frame so the host can alias them without a copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedPubSubMessage {
    Data {
        topics: Vec<String>,
        strict: bool,
        data_offset: usize,
        data_length: usize,
    },
    Subscribe {
        topics: Vec<String>,
        request_subscribers: bool,
    },
    Unsubscribe {
        topics: Vec<String>,
    },
    GetSubscribers {
        topics: Vec<String>,
    },
    TopicRootCandidates {
        candidates: Vec<String>,
    },
    PeerUnavailable {
        public_key_hash: String,
        session: u64,
        timestamp: u64,
        topics: Vec<String>,
    },
    TopicRootQuery {
        request_id: u32,
        topic: String,
    },
    TopicRootQueryResponse {
        request_id: u32,
        topic: String,
        root: Option<String>,
    },
}

fn read_bool(reader: &mut Reader) -> WireResult<bool> {
    // borsh-ts BinaryReader.bool: only 0 and 1 are valid encodings.
    match reader.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(format!("unexpected value for boolean: {other}")),
    }
}

pub fn decode_pubsub_message(frame: &[u8]) -> WireResult<DecodedPubSubMessage> {
    let mut reader = Reader::new(frame);
    let variant = reader.u8()?;
    let message = match variant {
        PUBSUB_VARIANT_DATA => {
            let topics = reader.string_vec()?;
            let strict = read_bool(&mut reader)?;
            let data_length = reader.u32_le()? as usize;
            let data_offset = reader.offset;
            reader.take(data_length)?;
            DecodedPubSubMessage::Data {
                topics,
                strict,
                data_offset,
                data_length,
            }
        }
        PUBSUB_VARIANT_SUBSCRIBE => DecodedPubSubMessage::Subscribe {
            topics: reader.string_vec()?,
            request_subscribers: read_bool(&mut reader)?,
        },
        PUBSUB_VARIANT_UNSUBSCRIBE => DecodedPubSubMessage::Unsubscribe {
            topics: reader.string_vec()?,
        },
        PUBSUB_VARIANT_GET_SUBSCRIBERS => DecodedPubSubMessage::GetSubscribers {
            topics: reader.string_vec()?,
        },
        PUBSUB_VARIANT_TOPIC_ROOT_CANDIDATES => DecodedPubSubMessage::TopicRootCandidates {
            candidates: reader.string_vec()?,
        },
        PUBSUB_VARIANT_PEER_UNAVAILABLE => DecodedPubSubMessage::PeerUnavailable {
            public_key_hash: reader.string()?,
            session: reader.u64_le()?,
            timestamp: reader.u64_le()?,
            topics: reader.string_vec()?,
        },
        PUBSUB_VARIANT_TOPIC_ROOT_QUERY => DecodedPubSubMessage::TopicRootQuery {
            request_id: reader.u32_le()?,
            topic: reader.string()?,
        },
        PUBSUB_VARIANT_TOPIC_ROOT_QUERY_RESPONSE => {
            let request_id = reader.u32_le()?;
            let topic = reader.string()?;
            let root = if read_bool(&mut reader)? {
                Some(reader.string()?)
            } else {
                None
            };
            DecodedPubSubMessage::TopicRootQueryResponse {
                request_id,
                topic,
                root,
            }
        }
        other => return Err(format!("unsupported pubsub message variant {other}")),
    };
    if reader.remaining() != 0 {
        return Err("trailing bytes after pubsub message".to_string());
    }
    Ok(message)
}

pub fn encode_pubsub_data(topics: &[String], strict: bool, data: &[u8]) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(PUBSUB_VARIANT_DATA);
    writer.string_vec(topics);
    writer.u8(strict as u8);
    writer.u32_le(data.len() as u32);
    writer.raw(data);
    writer.bytes
}

pub fn encode_subscribe(topics: &[String], request_subscribers: bool) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(PUBSUB_VARIANT_SUBSCRIBE);
    writer.string_vec(topics);
    writer.u8(request_subscribers as u8);
    writer.bytes
}

pub fn encode_unsubscribe(topics: &[String]) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(PUBSUB_VARIANT_UNSUBSCRIBE);
    writer.string_vec(topics);
    writer.bytes
}

pub fn encode_get_subscribers(topics: &[String]) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(PUBSUB_VARIANT_GET_SUBSCRIBERS);
    writer.string_vec(topics);
    writer.bytes
}

pub fn encode_topic_root_candidates(candidates: &[String]) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(PUBSUB_VARIANT_TOPIC_ROOT_CANDIDATES);
    writer.string_vec(candidates);
    writer.bytes
}

pub fn encode_peer_unavailable(
    public_key_hash: &str,
    session: u64,
    timestamp: u64,
    topics: &[String],
) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(PUBSUB_VARIANT_PEER_UNAVAILABLE);
    writer.string(public_key_hash);
    writer.u64_le(session);
    writer.u64_le(timestamp);
    writer.string_vec(topics);
    writer.bytes
}

pub fn encode_topic_root_query(request_id: u32, topic: &str) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(PUBSUB_VARIANT_TOPIC_ROOT_QUERY);
    writer.u32_le(request_id);
    writer.string(topic);
    writer.bytes
}

pub fn encode_topic_root_query_response(
    request_id: u32,
    topic: &str,
    root: Option<&str>,
) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(PUBSUB_VARIANT_TOPIC_ROOT_QUERY_RESPONSE);
    writer.u32_le(request_id);
    writer.string(topic);
    match root {
        Some(root) => {
            writer.u8(1);
            writer.string(root);
        }
        None => writer.u8(0),
    }
    writer.bytes
}

/// The `topicHash32` FNV-1a variant in `pubsub/src/index.ts` and
/// `topic-root-control-plane.ts`. The TS implementation multiplies plain JS
/// numbers, so for signed 32-bit intermediates the product is rounded to the
/// nearest f64 before `>>> 0`; this port reproduces those exact semantics
/// (including the rounding) because the hash keys shard mapping and
/// deterministic root selection across mixed js/rust peers.
pub fn topic_hash32(topic: &str) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for unit in topic.encode_utf16() {
        let xored = (hash as i32) ^ (unit as i32);
        let product = (xored as i64) * 0x0100_0193;
        let rounded = product as f64;
        hash = (rounded as i64 as u64 & 0xffff_ffff) as u32;
    }
    hash
}

/// `getShardTopicForUserTopic`: user topic -> internal shard topic.
pub fn shard_topic_for(topic: &str, shard_count: u32, prefix: &str) -> String {
    let count = shard_count.max(1);
    format!("{prefix}{}", topic_hash32(topic) % count)
}

/// JS `<` on strings compares UTF-16 code units.
fn js_string_lt(a: &str, b: &str) -> bool {
    a.encode_utf16().lt(b.encode_utf16())
}

fn js_string_sort(values: &mut [String]) {
    values.sort_by(|a, b| {
        if js_string_lt(a, b) {
            std::cmp::Ordering::Less
        } else if js_string_lt(b, a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
}

/// `normalizeAutoTopicRootCandidates`: dedupe (dropping empties), always
/// include self, sort by UTF-16 code units and cap at
/// [`AUTO_TOPIC_ROOT_CANDIDATES_MAX`].
pub fn normalize_auto_candidates(candidates: &[String], me: &str) -> Vec<String> {
    let mut unique: HashSet<&str> = HashSet::new();
    for candidate in candidates {
        if candidate.is_empty() {
            continue;
        }
        unique.insert(candidate.as_str());
    }
    unique.insert(me);
    let mut sorted: Vec<String> = unique.into_iter().map(|value| value.to_string()).collect();
    js_string_sort(&mut sorted);
    sorted.truncate(AUTO_TOPIC_ROOT_CANDIDATES_MAX);
    sorted
}

/// `subscriptionStateIsLatest` comparison rule: `lasts` carries the
/// (session, timestamp) watermark pairs that exist for the relevant topics.
/// The message is stale when any watermark has a newer session, or - unless
/// the message timestamp is the 0 fast-path sentinel - an equal session with
/// a newer timestamp.
pub fn subscription_is_latest(lasts: &[u64], session: u64, timestamp: u64) -> bool {
    for pair in lasts.chunks_exact(2) {
        let (last_session, last_timestamp) = (pair[0], pair[1]);
        if last_session > session {
            return false;
        }
        if timestamp != 0 && last_session == session && last_timestamp > timestamp {
            return false;
        }
    }
    true
}

/// The subscribe-apply replacement rule: a `Subscribe` replaces the tracked
/// subscription data only for new subscribers or strictly newer sessions;
/// otherwise the entry is only refreshed in cache order.
pub fn subscribe_should_replace(existing_session: Option<u64>, session: u64) -> bool {
    match existing_session {
        None => true,
        Some(existing) => existing < session,
    }
}

/// `TopicRootDirectory` state: explicit per-topic roots plus the normalized
/// deterministic candidate set. Trackers and the resolver callback stay
/// host-side; this owns everything the TS class keeps as fields.
#[derive(Default)]
pub struct TopicRootDirectoryCore {
    explicit_roots_by_topic: HashMap<String, String>,
    default_candidates: Vec<String>,
}

impl TopicRootDirectoryCore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_root(&mut self, topic: &str, root: &str) {
        self.explicit_roots_by_topic
            .insert(topic.to_string(), root.to_string());
    }

    pub fn delete_root(&mut self, topic: &str) {
        self.explicit_roots_by_topic.remove(topic);
    }

    pub fn get_root(&self, topic: &str) -> Option<String> {
        self.explicit_roots_by_topic.get(topic).cloned()
    }

    /// `setDefaultCandidates`: dedupe (dropping empties) and sort by UTF-16
    /// code units.
    pub fn set_default_candidates(&mut self, candidates: &[String]) {
        let mut unique: HashSet<&str> = HashSet::new();
        for candidate in candidates {
            if candidate.is_empty() {
                continue;
            }
            unique.insert(candidate.as_str());
        }
        let mut sorted: Vec<String> = unique.into_iter().map(|value| value.to_string()).collect();
        js_string_sort(&mut sorted);
        self.default_candidates = sorted;
    }

    pub fn get_default_candidates(&self) -> Vec<String> {
        self.default_candidates.clone()
    }

    pub fn resolve_deterministic_candidate(&self, topic: &str) -> Option<String> {
        if self.default_candidates.is_empty() {
            return None;
        }
        let index = topic_hash32(topic) as usize % self.default_candidates.len();
        Some(self.default_candidates[index].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn pubsub_data_encodes_borsh_layout() {
        let encoded = encode_pubsub_data(&strings(&["t1", "t2"]), true, &[7, 8]);
        let mut expected = vec![PUBSUB_VARIANT_DATA];
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(b"t1");
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(b"t2");
        expected.push(1);
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&[7, 8]);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn peer_unavailable_encodes_borsh_layout() {
        let encoded = encode_peer_unavailable("abc", 5, 9, &strings(&["t"]));
        let mut expected = vec![PUBSUB_VARIANT_PEER_UNAVAILABLE];
        expected.extend_from_slice(&3u32.to_le_bytes());
        expected.extend_from_slice(b"abc");
        expected.extend_from_slice(&5u64.to_le_bytes());
        expected.extend_from_slice(&9u64.to_le_bytes());
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.extend_from_slice(b"t");
        assert_eq!(encoded, expected);
    }

    #[test]
    fn pubsub_messages_roundtrip() {
        let frames = [
            encode_pubsub_data(&strings(&["a"]), false, &[1, 2, 3]),
            encode_subscribe(&strings(&["a", "b"]), true),
            encode_unsubscribe(&strings(&[])),
            encode_get_subscribers(&strings(&["x"])),
            encode_topic_root_candidates(&strings(&["c1", "c2"])),
            encode_peer_unavailable("hash", u64::MAX, 0, &strings(&["t"])),
            encode_topic_root_query(42, "topic"),
            encode_topic_root_query_response(42, "topic", Some("root")),
            encode_topic_root_query_response(43, "topic", None),
        ];
        let expected = [
            DecodedPubSubMessage::Data {
                topics: strings(&["a"]),
                strict: false,
                data_offset: 15,
                data_length: 3,
            },
            DecodedPubSubMessage::Subscribe {
                topics: strings(&["a", "b"]),
                request_subscribers: true,
            },
            DecodedPubSubMessage::Unsubscribe { topics: vec![] },
            DecodedPubSubMessage::GetSubscribers {
                topics: strings(&["x"]),
            },
            DecodedPubSubMessage::TopicRootCandidates {
                candidates: strings(&["c1", "c2"]),
            },
            DecodedPubSubMessage::PeerUnavailable {
                public_key_hash: "hash".to_string(),
                session: u64::MAX,
                timestamp: 0,
                topics: strings(&["t"]),
            },
            DecodedPubSubMessage::TopicRootQuery {
                request_id: 42,
                topic: "topic".to_string(),
            },
            DecodedPubSubMessage::TopicRootQueryResponse {
                request_id: 42,
                topic: "topic".to_string(),
                root: Some("root".to_string()),
            },
            DecodedPubSubMessage::TopicRootQueryResponse {
                request_id: 43,
                topic: "topic".to_string(),
                root: None,
            },
        ];
        for (frame, expected) in frames.iter().zip(expected) {
            assert_eq!(decode_pubsub_message(frame).unwrap(), expected);
        }
    }

    #[test]
    fn decode_rejects_bad_frames() {
        assert!(decode_pubsub_message(&[]).is_err());
        assert!(decode_pubsub_message(&[8]).is_err());
        // non-boolean strict flag (borsh-ts rejects values other than 0/1)
        let mut bad_bool = encode_subscribe(&strings(&["a"]), false);
        let flag_offset = bad_bool.len() - 1;
        bad_bool[flag_offset] = 2;
        assert!(decode_pubsub_message(&bad_bool).is_err());
        // truncated payload
        let mut truncated = encode_pubsub_data(&strings(&["a"]), false, &[1, 2, 3]);
        truncated.pop();
        assert!(decode_pubsub_message(&truncated).is_err());
        // trailing bytes
        let mut trailing = encode_unsubscribe(&strings(&["a"]));
        trailing.push(0);
        assert!(decode_pubsub_message(&trailing).is_err());
    }

    #[test]
    fn topic_hash32_matches_js_reference() {
        // Reference values computed with the TS implementation in
        // pubsub/src/index.ts under Node (including the f64 rounding the
        // unchecked JS multiplication introduces).
        assert_eq!(topic_hash32(""), 2166136261);
        assert_eq!(topic_hash32("a"), 3826002220);
        assert_eq!(topic_hash32("abc"), 440920332);
        assert_eq!(topic_hash32("/peerbit/pubsub-shard/1/0"), 900959932);
        assert_eq!(topic_hash32("hello world"), 3402909720);
        // non-ASCII goes through UTF-16 code units, not UTF-8 bytes
        assert_eq!(topic_hash32("héllo"), 841497836);
        assert_eq!(topic_hash32("日本語"), 1409693520);
        // surrogate pair (two UTF-16 units)
        assert_eq!(topic_hash32("💜"), 1462729500);
    }

    #[test]
    fn shard_topic_matches_ts_mapping() {
        assert_eq!(
            shard_topic_for("topic", 256, "/peerbit/pubsub-shard/1/"),
            format!("/peerbit/pubsub-shard/1/{}", topic_hash32("topic") % 256),
        );
        // shard_count 0 is clamped like a degenerate 1-shard config
        assert_eq!(shard_topic_for("topic", 0, "p/"), "p/0");
    }

    #[test]
    fn normalize_auto_candidates_sorts_and_caps() {
        let normalized = normalize_auto_candidates(&strings(&["b", "", "a", "b"]), "me");
        assert_eq!(normalized, strings(&["a", "b", "me"]));

        let many: Vec<String> = (0..80).map(|i| format!("peer-{i:03}")).collect();
        let normalized = normalize_auto_candidates(&many, "peer-000");
        assert_eq!(normalized.len(), AUTO_TOPIC_ROOT_CANDIDATES_MAX);
        assert_eq!(normalized[0], "peer-000");
    }

    #[test]
    fn subscription_watermark_rule() {
        // no watermarks -> always latest
        assert!(subscription_is_latest(&[], 1, 1));
        // newer session wins
        assert!(subscription_is_latest(&[1, 30], 2, 20));
        // older session loses
        assert!(!subscription_is_latest(&[2, 20], 1, 30));
        // same session: newer timestamp required...
        assert!(!subscription_is_latest(&[2, 30], 2, 20));
        assert!(subscription_is_latest(&[2, 20], 2, 30));
        // ...unless timestamp is the 0 sentinel (shard fast-path)
        assert!(subscription_is_latest(&[2, 30], 2, 0));
        // any stale watermark rejects
        assert!(!subscription_is_latest(&[1, 1, 3, 1], 2, 5));
    }

    #[test]
    fn subscribe_replace_rule() {
        assert!(subscribe_should_replace(None, 1));
        assert!(subscribe_should_replace(Some(1), 2));
        assert!(!subscribe_should_replace(Some(2), 2));
        assert!(!subscribe_should_replace(Some(3), 2));
    }

    #[test]
    fn directory_resolves_explicit_before_deterministic() {
        let mut directory = TopicRootDirectoryCore::new();
        assert_eq!(directory.resolve_deterministic_candidate("t"), None);

        directory.set_default_candidates(&strings(&["c", "a", "", "b", "a"]));
        assert_eq!(
            directory.get_default_candidates(),
            strings(&["a", "b", "c"])
        );

        let deterministic = directory.resolve_deterministic_candidate("t").unwrap();
        let index = topic_hash32("t") as usize % 3;
        assert_eq!(deterministic, ["a", "b", "c"][index]);

        directory.set_root("t", "explicit");
        assert_eq!(directory.get_root("t"), Some("explicit".to_string()));
        directory.delete_root("t");
        assert_eq!(directory.get_root("t"), None);
    }

    #[test]
    fn js_sort_uses_utf16_code_units() {
        // '\u{ff21}' (fullwidth A, one unit 0xFF21) vs '\u{1d400}'
        // (mathematical bold A, surrogate pair starting 0xD835): JS string
        // comparison orders by the first UTF-16 unit, unlike UTF-8 byte
        // order which would reverse these.
        let mut values = strings(&["\u{ff21}", "\u{1d400}"]);
        js_string_sort(&mut values);
        assert_eq!(values, strings(&["\u{1d400}", "\u{ff21}"]));
    }
}
