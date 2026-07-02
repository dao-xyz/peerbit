//! JsValue-free port of the `/peerbit/fanout-tree/0.5.0` protocol core
//! (`packages/transport/pubsub`): the custom hand-written big-endian frame
//! codec for every message kind (`MSG_JOIN_REQ(1)`..`MSG_PARENT_PROBE_REPLY(41)`
//! - join/accept/reject/kick, data/end, unicast(+ack), route query/reply,
//! publish-proxy, repair/fetch/ihave, tracker announce/query/reply/feedback,
//! provider announce/query/reply/subscribe/unsubscribe/notify, parent
//! probes) plus the parent-upgrade policy normalization and upgrade-gate
//! decisions merged in PR #911 (`fanout-tree-parent-upgrade.ts`). The host
//! keeps the channel state machine, timers and sockets; every wire byte and
//! every gate decision that feeds them runs here.
//!
//! Encoders replicate the TS `fanout-tree-codec.ts` byte-for-byte including
//! its JS numeric coercions (`>>> 0`, `| 0`, `clampU16`, `Math.floor`
//! clamps) and its skip/truncate rules for oversized hashes, hop lists and
//! address lists. Decoders replicate the exact tolerance of the inline
//! parsers in `fanout-tree.ts` (`onDataMessage`): the same minimum-length
//! rejects and the same mid-list `break` behavior on truncated input.

/// `MAX_ROUTE_HOPS` in `fanout-tree-codec.ts`.
pub const MAX_ROUTE_HOPS: usize = 32;
/// `JOIN_REJECT_REDIRECT_MAX` / `JOIN_REJECT_REDIRECT_ADDR_MAX`.
pub const JOIN_REJECT_REDIRECT_MAX: usize = 4;
pub const JOIN_REJECT_REDIRECT_ADDR_MAX: usize = 8;
/// The inline handlers cap decoded address lists at 16 entries.
pub const DECODE_ADDRS_MAX: usize = 16;

pub const MSG_JOIN_REQ: u8 = 1;
pub const MSG_JOIN_ACCEPT: u8 = 2;
pub const MSG_JOIN_REJECT: u8 = 3;
pub const MSG_KICK: u8 = 4;
pub const MSG_DATA: u8 = 10;
pub const MSG_END: u8 = 11;
pub const MSG_UNICAST: u8 = 12;
pub const MSG_ROUTE_QUERY: u8 = 13;
pub const MSG_ROUTE_REPLY: u8 = 14;
pub const MSG_PUBLISH_PROXY: u8 = 15;
pub const MSG_LEAVE: u8 = 16;
pub const MSG_UNICAST_ACK: u8 = 17;
pub const MSG_REPAIR_REQ: u8 = 20;
pub const MSG_FETCH_REQ: u8 = 21;
pub const MSG_IHAVE: u8 = 22;
pub const MSG_TRACKER_ANNOUNCE: u8 = 30;
pub const MSG_TRACKER_QUERY: u8 = 31;
pub const MSG_TRACKER_REPLY: u8 = 32;
pub const MSG_TRACKER_FEEDBACK: u8 = 33;
pub const MSG_PROVIDER_ANNOUNCE: u8 = 34;
pub const MSG_PROVIDER_QUERY: u8 = 35;
pub const MSG_PROVIDER_REPLY: u8 = 36;
pub const MSG_PROVIDER_SUBSCRIBE: u8 = 37;
pub const MSG_PROVIDER_UNSUBSCRIBE: u8 = 38;
pub const MSG_PROVIDER_NOTIFY: u8 = 39;
pub const MSG_PARENT_PROBE_REQ: u8 = 40;
pub const MSG_PARENT_PROBE_REPLY: u8 = 41;

pub const UNICAST_FLAG_ACK: u8 = 1;
const PARENT_PROBE_REQ_FLAG_RESERVE_ROOT: u8 = 1 << 0;

const CHANNEL_KEY_BYTES: usize = 32;
const HEADER_BYTES: usize = 1 + CHANNEL_KEY_BYTES;

// --- JS numeric semantics ---------------------------------------------------

/// ECMAScript ToInt32 (the JS `| 0` coercion).
pub fn js_to_int32(value: f64) -> i32 {
    if !value.is_finite() {
        return 0;
    }
    let modulo = value.trunc().rem_euclid(4_294_967_296.0);
    if modulo >= 2_147_483_648.0 {
        (modulo - 4_294_967_296.0) as i32
    } else {
        modulo as i32
    }
}

/// ECMAScript ToUint32 (the JS `>>> 0` coercion).
pub fn js_to_uint32(value: f64) -> u32 {
    if !value.is_finite() {
        return 0;
    }
    value.trunc().rem_euclid(4_294_967_296.0) as u32
}

/// `clampU16(v)`: `Math.max(0, Math.min(0xffff, v | 0))`.
pub fn clamp_u16(value: f64) -> u16 {
    js_to_int32(value).clamp(0, 0xffff) as u16
}

/// `Math.max(0, Math.floor(v)) >>> 0` - the ttl/bid/counter clamp.
pub fn js_floor_clamp_u32(value: f64) -> u32 {
    js_to_uint32(value.floor().max(0.0))
}

// --- primitive writers/readers ----------------------------------------------

fn write_u16_be(buf: &mut [u8], offset: usize, value: u16) {
    buf[offset] = (value >> 8) as u8;
    buf[offset + 1] = value as u8;
}

fn write_u32_be(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn write_u64_be(buf: &mut [u8], offset: usize, value: u64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn read_u16_be(buf: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([buf[offset], buf[offset + 1]])
}

fn read_u32_be(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

fn read_u64_be(buf: &[u8], offset: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[offset..offset + 8]);
    u64::from_be_bytes(bytes)
}

/// `TextDecoder` (non-fatal UTF-8): invalid sequences become U+FFFD.
fn decode_utf8(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn header(kind: u8, channel_key: &[u8], payload_bytes: usize) -> Vec<u8> {
    debug_assert_eq!(channel_key.len(), CHANNEL_KEY_BYTES);
    let mut buf = vec![0u8; HEADER_BYTES + payload_bytes];
    buf[0] = kind;
    buf[1..HEADER_BYTES].copy_from_slice(channel_key);
    buf
}

/// The route-hop pre-encoding shared by JOIN_ACCEPT/UNICAST/UNICAST_ACK/
/// ROUTE_REPLY: hops with UTF-8 length 1..=255, capped at [`MAX_ROUTE_HOPS`]
/// (the TS loop `break`s at the cap, so later valid hops are dropped).
fn encode_route_hops(route: &[String]) -> (Vec<&[u8]>, usize) {
    let mut hops: Vec<&[u8]> = Vec::new();
    let mut bytes = 0usize;
    for hop in route {
        if hops.len() >= MAX_ROUTE_HOPS {
            break;
        }
        let hb = hop.as_bytes();
        if hb.is_empty() || hb.len() > 255 {
            continue;
        }
        bytes += 1 + hb.len();
        hops.push(hb);
    }
    (hops, bytes)
}

fn write_route_hops(buf: &mut [u8], mut offset: usize, hops: &[&[u8]]) -> usize {
    for hb in hops {
        buf[offset] = hb.len() as u8;
        offset += 1;
        buf[offset..offset + hb.len()].copy_from_slice(hb);
        offset += hb.len();
    }
    offset
}

// --- encoders -----------------------------------------------------------------

pub fn encode_join_req(
    channel_key: &[u8],
    req_id: f64,
    bid_per_byte: f64,
    parent_upgrade_reservation_token: f64,
) -> Vec<u8> {
    let has_reservation = parent_upgrade_reservation_token > 0.0;
    let mut buf = header(
        MSG_JOIN_REQ,
        channel_key,
        8 + if has_reservation { 4 } else { 0 },
    );
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    write_u32_be(&mut buf, 37, js_to_uint32(bid_per_byte));
    if has_reservation {
        write_u32_be(&mut buf, 41, js_to_uint32(parent_upgrade_reservation_token));
    }
    buf
}

pub fn encode_join_accept(
    channel_key: &[u8],
    req_id: f64,
    level: f64,
    parent_route_from_root: &[String],
    have_range: Option<(f64, f64)>,
) -> Vec<u8> {
    let (hops, hop_bytes) = encode_route_hops(parent_route_from_root);
    let payload = 4 + 2 + 1 + hop_bytes + if have_range.is_some() { 8 } else { 0 };
    let mut buf = header(MSG_JOIN_ACCEPT, channel_key, payload);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    write_u16_be(&mut buf, 37, (js_to_int32(level) & 0xffff) as u16);
    buf[39] = hops.len() as u8;
    let offset = write_route_hops(&mut buf, 40, &hops);
    if let Some((have_from, have_to_exclusive)) = have_range {
        write_u32_be(&mut buf, offset, js_to_uint32(have_from));
        write_u32_be(&mut buf, offset + 4, js_to_uint32(have_to_exclusive));
    }
    buf
}

pub struct JoinRejectRedirectInput {
    pub hash: String,
    pub addrs: Vec<Vec<u8>>,
}

pub fn encode_join_reject(
    channel_key: &[u8],
    req_id: f64,
    reason: f64,
    redirects: &[JoinRejectRedirectInput],
) -> Vec<u8> {
    let mut encoded: Vec<(&[u8], Vec<&[u8]>)> = Vec::new();
    for redirect in redirects {
        if redirect.hash.is_empty() {
            continue;
        }
        let hash_bytes = redirect.hash.as_bytes();
        if hash_bytes.len() > 255 {
            continue;
        }
        let addrs: Vec<&[u8]> = redirect
            .addrs
            .iter()
            .map(|addr| addr.as_slice())
            .filter(|addr| !addr.is_empty() && addr.len() <= 0xffff)
            .take(JOIN_REJECT_REDIRECT_ADDR_MAX)
            .collect();
        if addrs.is_empty() {
            continue;
        }
        encoded.push((hash_bytes, addrs));
        if encoded.len() >= JOIN_REJECT_REDIRECT_MAX {
            break;
        }
    }

    if encoded.is_empty() {
        let mut buf = header(MSG_JOIN_REJECT, channel_key, 4 + 1);
        write_u32_be(&mut buf, 33, js_to_uint32(req_id));
        buf[37] = (js_to_int32(reason) & 0xff) as u8;
        return buf;
    }

    let mut payload = 4 + 1 + 1;
    for (hash_bytes, addrs) in &encoded {
        payload += 1 + hash_bytes.len() + 1;
        for addr in addrs {
            payload += 2 + addr.len();
        }
    }
    let mut buf = header(MSG_JOIN_REJECT, channel_key, payload);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    buf[37] = (js_to_int32(reason) & 0xff) as u8;
    buf[38] = encoded.len() as u8;
    let mut offset = 39;
    for (hash_bytes, addrs) in &encoded {
        buf[offset] = hash_bytes.len() as u8;
        offset += 1;
        buf[offset..offset + hash_bytes.len()].copy_from_slice(hash_bytes);
        offset += hash_bytes.len();
        buf[offset] = addrs.len() as u8;
        offset += 1;
        for addr in addrs {
            write_u16_be(&mut buf, offset, addr.len() as u16);
            offset += 2;
            buf[offset..offset + addr.len()].copy_from_slice(addr);
            offset += addr.len();
        }
    }
    buf
}

pub fn encode_kick(channel_key: &[u8]) -> Vec<u8> {
    header(MSG_KICK, channel_key, 0)
}

pub fn encode_end(channel_key: &[u8], last_seq_exclusive: f64) -> Vec<u8> {
    let mut buf = header(MSG_END, channel_key, 4);
    write_u32_be(&mut buf, 33, js_to_uint32(last_seq_exclusive));
    buf
}

fn encode_seq_list(kind: u8, channel_key: &[u8], req_id: f64, missing_seqs: &[f64]) -> Vec<u8> {
    let count = missing_seqs.len().min(255);
    let mut buf = header(kind, channel_key, 4 + 1 + count * 4);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    buf[37] = count as u8;
    for (index, seq) in missing_seqs.iter().take(count).enumerate() {
        write_u32_be(&mut buf, 38 + index * 4, js_to_uint32(*seq));
    }
    buf
}

pub fn encode_repair_req(channel_key: &[u8], req_id: f64, missing_seqs: &[f64]) -> Vec<u8> {
    encode_seq_list(MSG_REPAIR_REQ, channel_key, req_id, missing_seqs)
}

pub fn encode_fetch_req(channel_key: &[u8], req_id: f64, missing_seqs: &[f64]) -> Vec<u8> {
    encode_seq_list(MSG_FETCH_REQ, channel_key, req_id, missing_seqs)
}

pub fn encode_ihave(channel_key: &[u8], have_from: f64, have_to_exclusive: f64) -> Vec<u8> {
    let mut buf = header(MSG_IHAVE, channel_key, 8);
    write_u32_be(&mut buf, 33, js_to_uint32(have_from));
    write_u32_be(&mut buf, 37, js_to_uint32(have_to_exclusive));
    buf
}

pub fn encode_data(payload: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; 1 + payload.len()];
    buf[0] = MSG_DATA;
    buf[1..].copy_from_slice(payload);
    buf
}

pub fn encode_publish_proxy(channel_key: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut buf = header(MSG_PUBLISH_PROXY, channel_key, payload.len());
    buf[33..].copy_from_slice(payload);
    buf
}

pub fn encode_leave(channel_key: &[u8]) -> Vec<u8> {
    header(MSG_LEAVE, channel_key, 0)
}

pub fn encode_unicast(
    channel_key: &[u8],
    route: &[String],
    payload: &[u8],
    ack_token: Option<u64>,
    reply_route: &[String],
) -> Vec<u8> {
    let wants_ack = ack_token.is_some();
    let flags = if wants_ack { UNICAST_FLAG_ACK } else { 0 };
    let (to_hops, to_bytes) = encode_route_hops(route);
    let (reply_hops, reply_bytes) = if wants_ack {
        encode_route_hops(reply_route)
    } else {
        (Vec::new(), 0)
    };
    let payload_bytes = 1
        + if wants_ack { 8 } else { 0 }
        + 1
        + to_bytes
        + if wants_ack { 1 + reply_bytes } else { 0 }
        + payload.len();
    let mut buf = header(MSG_UNICAST, channel_key, payload_bytes);
    buf[33] = flags;
    let mut offset = 34;
    if let Some(token) = ack_token {
        write_u64_be(&mut buf, offset, token);
        offset += 8;
    }
    buf[offset] = to_hops.len() as u8;
    offset += 1;
    offset = write_route_hops(&mut buf, offset, &to_hops);
    if wants_ack {
        buf[offset] = reply_hops.len() as u8;
        offset += 1;
        offset = write_route_hops(&mut buf, offset, &reply_hops);
    }
    buf[offset..offset + payload.len()].copy_from_slice(payload);
    buf
}

pub fn encode_unicast_ack(channel_key: &[u8], ack_token: u64, route: &[String]) -> Vec<u8> {
    let (hops, hop_bytes) = encode_route_hops(route);
    let mut buf = header(MSG_UNICAST_ACK, channel_key, 8 + 1 + hop_bytes);
    write_u64_be(&mut buf, 33, ack_token);
    buf[41] = hops.len() as u8;
    write_route_hops(&mut buf, 42, &hops);
    buf
}

pub fn encode_route_query(channel_key: &[u8], req_id: f64, target_hash: &str) -> Vec<u8> {
    let target_bytes = target_hash.as_bytes();
    // The TS encoder truncates (subarray), it does not skip.
    let target_len = target_bytes.len().min(255);
    let mut buf = header(MSG_ROUTE_QUERY, channel_key, 4 + 1 + target_len);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    buf[37] = target_len as u8;
    buf[38..38 + target_len].copy_from_slice(&target_bytes[..target_len]);
    buf
}

pub fn encode_route_reply(channel_key: &[u8], req_id: f64, route: &[String]) -> Vec<u8> {
    let (hops, hop_bytes) = encode_route_hops(route);
    let mut buf = header(MSG_ROUTE_REPLY, channel_key, 4 + 1 + hop_bytes);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    buf[37] = hops.len() as u8;
    write_route_hops(&mut buf, 38, &hops);
    buf
}

fn write_addr_list(buf: &mut [u8], mut offset: usize, addrs: &[Vec<u8>]) -> usize {
    for addr in addrs {
        // The TS encoders write `addr.length` through writeU16BE unchecked,
        // which wraps modulo 2^16.
        write_u16_be(buf, offset, (addr.len() & 0xffff) as u16);
        offset += 2;
        buf[offset..offset + addr.len()].copy_from_slice(addr);
        offset += addr.len();
    }
    offset
}

pub fn encode_tracker_announce(
    channel_key: &[u8],
    ttl_ms: f64,
    level: f64,
    max_children: f64,
    free_slots: f64,
    bid_per_byte: f64,
    addrs: &[Vec<u8>],
) -> Vec<u8> {
    let addr_count = addrs.len().min(255);
    let addrs = &addrs[..addr_count];
    let addr_bytes: usize = addrs.iter().map(|addr| 2 + addr.len()).sum();
    let mut buf = header(
        MSG_TRACKER_ANNOUNCE,
        channel_key,
        4 + 2 + 2 + 2 + 4 + 1 + addr_bytes,
    );
    write_u32_be(&mut buf, 33, js_floor_clamp_u32(ttl_ms));
    write_u16_be(&mut buf, 37, clamp_u16(level));
    write_u16_be(&mut buf, 39, clamp_u16(max_children));
    write_u16_be(&mut buf, 41, clamp_u16(free_slots));
    write_u32_be(&mut buf, 43, js_floor_clamp_u32(bid_per_byte));
    buf[47] = addr_count as u8;
    write_addr_list(&mut buf, 48, addrs);
    buf
}

pub fn encode_tracker_query(channel_key: &[u8], req_id: f64, want: f64) -> Vec<u8> {
    let mut buf = header(MSG_TRACKER_QUERY, channel_key, 4 + 2);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    write_u16_be(&mut buf, 37, clamp_u16(want));
    buf
}

pub struct TrackerEntryInput {
    pub hash: String,
    pub level: f64,
    pub free_slots: f64,
    pub bid_per_byte: f64,
    pub addrs: Vec<Vec<u8>>,
}

pub fn encode_tracker_reply(
    channel_key: &[u8],
    req_id: f64,
    entries: &[TrackerEntryInput],
) -> Vec<u8> {
    let count = entries.len().min(255);
    let mut payload = 4 + 1;
    let mut encoded: Vec<(&TrackerEntryInput, &[Vec<u8>])> = Vec::new();
    for entry in entries.iter().take(count) {
        let hash_bytes = entry.hash.as_bytes();
        if hash_bytes.len() > 255 {
            continue;
        }
        let addr_count = entry.addrs.len().min(255);
        let addrs = &entry.addrs[..addr_count];
        payload += 1 + hash_bytes.len() + 2 + 2 + 4 + 1;
        for addr in addrs {
            payload += 2 + addr.len();
        }
        encoded.push((entry, addrs));
    }
    let mut buf = header(MSG_TRACKER_REPLY, channel_key, payload);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    buf[37] = encoded.len().min(255) as u8;
    let mut offset = 38;
    for (entry, addrs) in &encoded {
        let hash_bytes = entry.hash.as_bytes();
        buf[offset] = hash_bytes.len() as u8;
        offset += 1;
        buf[offset..offset + hash_bytes.len()].copy_from_slice(hash_bytes);
        offset += hash_bytes.len();
        write_u16_be(&mut buf, offset, clamp_u16(entry.level));
        offset += 2;
        write_u16_be(&mut buf, offset, clamp_u16(entry.free_slots));
        offset += 2;
        write_u32_be(&mut buf, offset, js_floor_clamp_u32(entry.bid_per_byte));
        offset += 4;
        buf[offset] = addrs.len().min(255) as u8;
        offset += 1;
        offset = write_addr_list(&mut buf, offset, addrs);
    }
    buf
}

pub fn encode_tracker_feedback(
    channel_key: &[u8],
    candidate_hash: &str,
    event: f64,
    reason: f64,
) -> Vec<u8> {
    let hash_bytes = candidate_hash.as_bytes();
    // Truncated (subarray), not skipped, like encodeRouteQuery.
    let hash_len = hash_bytes.len().min(255);
    let mut buf = header(MSG_TRACKER_FEEDBACK, channel_key, 1 + hash_len + 1 + 1);
    buf[33] = hash_len as u8;
    buf[34..34 + hash_len].copy_from_slice(&hash_bytes[..hash_len]);
    buf[34 + hash_len] = (js_to_int32(event) & 0xff) as u8;
    buf[34 + hash_len + 1] = (js_to_int32(reason) & 0xff) as u8;
    buf
}

pub fn encode_parent_probe_req(
    channel_key: &[u8],
    req_id: f64,
    min_free_slots: f64,
    reserve_root_capacity: bool,
) -> Vec<u8> {
    let encoded_min_free_slots = min_free_slots.floor().max(0.0);
    let has_extension = encoded_min_free_slots > 0.0 || !reserve_root_capacity;
    let mut buf = header(
        MSG_PARENT_PROBE_REQ,
        channel_key,
        4 + if has_extension { 2 + 1 } else { 0 },
    );
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    if has_extension {
        write_u16_be(&mut buf, 37, clamp_u16(encoded_min_free_slots));
        buf[39] = if reserve_root_capacity {
            PARENT_PROBE_REQ_FLAG_RESERVE_ROOT
        } else {
            0
        };
    }
    buf
}

#[allow(clippy::too_many_arguments)]
pub fn encode_parent_probe_reply(
    channel_key: &[u8],
    req_id: f64,
    flags: f64,
    level: f64,
    max_children: f64,
    free_slots: f64,
    children: f64,
    have_to_exclusive: f64,
    missing_seqs: f64,
    data_write_drops: f64,
    dropped_forwards: f64,
    reservation_token: f64,
) -> Vec<u8> {
    let mut buf = header(MSG_PARENT_PROBE_REPLY, channel_key, 27 + 4);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    buf[37] = (js_to_int32(flags) & 0xff) as u8;
    write_u16_be(&mut buf, 38, clamp_u16(level));
    write_u16_be(&mut buf, 40, clamp_u16(max_children));
    write_u16_be(&mut buf, 42, clamp_u16(free_slots));
    write_u16_be(&mut buf, 44, clamp_u16(children));
    write_u32_be(&mut buf, 46, js_floor_clamp_u32(have_to_exclusive));
    write_u16_be(&mut buf, 50, clamp_u16(missing_seqs));
    write_u32_be(&mut buf, 52, js_floor_clamp_u32(data_write_drops));
    write_u32_be(&mut buf, 56, js_floor_clamp_u32(dropped_forwards));
    write_u32_be(&mut buf, 60, js_floor_clamp_u32(reservation_token));
    buf
}

pub fn encode_provider_announce(namespace_key: &[u8], ttl_ms: f64, addrs: &[Vec<u8>]) -> Vec<u8> {
    let addr_count = addrs.len().min(255);
    let addrs = &addrs[..addr_count];
    let addr_bytes: usize = addrs.iter().map(|addr| 2 + addr.len()).sum();
    let mut buf = header(MSG_PROVIDER_ANNOUNCE, namespace_key, 4 + 1 + addr_bytes);
    write_u32_be(&mut buf, 33, js_floor_clamp_u32(ttl_ms));
    buf[37] = addr_count as u8;
    write_addr_list(&mut buf, 38, addrs);
    buf
}

pub fn encode_provider_query(namespace_key: &[u8], req_id: f64, want: f64, seed: f64) -> Vec<u8> {
    let mut buf = header(MSG_PROVIDER_QUERY, namespace_key, 4 + 2 + 4);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    write_u16_be(&mut buf, 37, clamp_u16(want));
    write_u32_be(&mut buf, 39, js_to_uint32(seed));
    buf
}

pub struct ProviderEntryInput {
    pub hash: String,
    pub addrs: Vec<Vec<u8>>,
}

fn provider_entries_layout<'a>(
    entries: &'a [ProviderEntryInput],
) -> (usize, Vec<(&'a ProviderEntryInput, &'a [Vec<u8>])>) {
    let count = entries.len().min(255);
    let mut bytes = 1usize;
    let mut encoded: Vec<(&ProviderEntryInput, &[Vec<u8>])> = Vec::new();
    for entry in entries.iter().take(count) {
        let hash_bytes = entry.hash.as_bytes();
        if hash_bytes.len() > 255 {
            continue;
        }
        let addr_count = entry.addrs.len().min(255);
        let addrs = &entry.addrs[..addr_count];
        bytes += 1 + hash_bytes.len() + 1;
        for addr in addrs {
            bytes += 2 + addr.len();
        }
        encoded.push((entry, addrs));
    }
    (bytes, encoded)
}

fn write_provider_entries(
    buf: &mut [u8],
    mut offset: usize,
    encoded: &[(&ProviderEntryInput, &[Vec<u8>])],
) {
    buf[offset] = encoded.len().min(255) as u8;
    offset += 1;
    for (entry, addrs) in encoded {
        let hash_bytes = entry.hash.as_bytes();
        buf[offset] = hash_bytes.len() as u8;
        offset += 1;
        buf[offset..offset + hash_bytes.len()].copy_from_slice(hash_bytes);
        offset += hash_bytes.len();
        buf[offset] = addrs.len().min(255) as u8;
        offset += 1;
        offset = write_addr_list(buf, offset, addrs);
    }
}

pub fn encode_provider_reply(
    namespace_key: &[u8],
    req_id: f64,
    entries: &[ProviderEntryInput],
) -> Vec<u8> {
    let (entry_bytes, encoded) = provider_entries_layout(entries);
    let mut buf = header(MSG_PROVIDER_REPLY, namespace_key, 4 + entry_bytes);
    write_u32_be(&mut buf, 33, js_to_uint32(req_id));
    write_provider_entries(&mut buf, 37, &encoded);
    buf
}

pub fn encode_provider_subscribe(namespace_key: &[u8], want: f64, ttl_ms: f64) -> Vec<u8> {
    let mut buf = header(MSG_PROVIDER_SUBSCRIBE, namespace_key, 2 + 4);
    write_u16_be(&mut buf, 33, clamp_u16(want));
    write_u32_be(&mut buf, 35, js_floor_clamp_u32(ttl_ms));
    buf
}

pub fn encode_provider_unsubscribe(namespace_key: &[u8]) -> Vec<u8> {
    header(MSG_PROVIDER_UNSUBSCRIBE, namespace_key, 0)
}

pub fn encode_provider_notify(namespace_key: &[u8], entries: &[ProviderEntryInput]) -> Vec<u8> {
    let (entry_bytes, encoded) = provider_entries_layout(entries);
    let mut buf = header(MSG_PROVIDER_NOTIFY, namespace_key, entry_bytes);
    write_provider_entries(&mut buf, 33, &encoded);
    buf
}

// --- decoders -----------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedJoinReq {
    pub req_id: u32,
    pub bid_per_byte: u32,
    pub parent_upgrade_reservation_token: u32,
}

pub fn decode_join_req(data: &[u8]) -> Option<DecodedJoinReq> {
    if data.len() < HEADER_BYTES + 8 {
        return None;
    }
    Some(DecodedJoinReq {
        req_id: read_u32_be(data, 33),
        bid_per_byte: read_u32_be(data, 37),
        parent_upgrade_reservation_token: if data.len() >= HEADER_BYTES + 12 {
            read_u32_be(data, 41)
        } else {
            0
        },
    })
}

/// The shared JOIN_ACCEPT/JOIN_REJECT head (`reqId` at offset 33) used to
/// resolve the pending join before kind-specific parsing.
pub fn decode_join_response_req_id(data: &[u8]) -> Option<u32> {
    if data.len() < HEADER_BYTES + 4 {
        return None;
    }
    Some(read_u32_be(data, 33))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedJoinAccept {
    pub parent_level: u16,
    pub parent_route_from_root: Vec<String>,
    pub have_range: Option<(u32, u32)>,
}

pub fn decode_join_accept(data: &[u8]) -> Option<DecodedJoinAccept> {
    if data.len() < HEADER_BYTES + 4 + 2 + 1 {
        return None;
    }
    let parent_level = read_u16_be(data, 37);
    let route_count = data[39].min(255) as usize;
    let mut offset = 40usize;
    let mut route: Vec<String> = Vec::new();
    // Unlike `decodeRoute`, the inline JOIN_ACCEPT parser stops consuming at
    // MAX_ROUTE_HOPS, which the have-range appendix offset depends on.
    let max = route_count.min(MAX_ROUTE_HOPS);
    for _ in 0..max {
        if offset + 1 > data.len() {
            break;
        }
        let len = data[offset] as usize;
        offset += 1;
        if len == 0 {
            break;
        }
        if offset + len > data.len() {
            break;
        }
        route.push(decode_utf8(&data[offset..offset + len]));
        offset += len;
    }
    let have_range = if offset + 8 <= data.len() {
        Some((read_u32_be(data, offset), read_u32_be(data, offset + 4)))
    } else {
        None
    };
    Some(DecodedJoinAccept {
        parent_level,
        parent_route_from_root: route,
        have_range,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedRedirect {
    pub hash: String,
    /// Raw multiaddr bytes; validity filtering stays host-side.
    pub addrs: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedJoinReject {
    pub reason: u8,
    pub redirects: Vec<DecodedRedirect>,
}

pub fn decode_join_reject(data: &[u8]) -> Option<DecodedJoinReject> {
    if data.len() < HEADER_BYTES + 4 + 1 {
        return None;
    }
    let reason = data[37];
    let mut redirects: Vec<DecodedRedirect> = Vec::new();
    if data.len() >= HEADER_BYTES + 4 + 1 + 1 {
        let count = data[38].min(255) as usize;
        let mut offset = 39usize;
        let max = count.min(JOIN_REJECT_REDIRECT_MAX);
        for _ in 0..max {
            if offset + 1 > data.len() {
                break;
            }
            let hash_len = data[offset] as usize;
            offset += 1;
            if hash_len == 0 {
                break;
            }
            if offset + hash_len > data.len() {
                break;
            }
            let hash = decode_utf8(&data[offset..offset + hash_len]);
            offset += hash_len;
            if offset + 1 > data.len() {
                break;
            }
            let addr_count = data[offset].min(255) as usize;
            offset += 1;
            // Like the TS parser, a truncated addr list only ends this
            // entry's addr loop; the outer loop keeps consuming from the
            // current offset.
            let mut addrs: Vec<Vec<u8>> = Vec::new();
            let addr_max = addr_count.min(JOIN_REJECT_REDIRECT_ADDR_MAX);
            for _ in 0..addr_max {
                if offset + 2 > data.len() {
                    break;
                }
                let len = read_u16_be(data, offset) as usize;
                offset += 2;
                if offset + len > data.len() {
                    break;
                }
                addrs.push(data[offset..offset + len].to_vec());
                offset += len;
            }
            redirects.push(DecodedRedirect { hash, addrs });
        }
    }
    Some(DecodedJoinReject { reason, redirects })
}

pub fn decode_end(data: &[u8]) -> Option<u32> {
    if data.len() < HEADER_BYTES + 4 {
        return None;
    }
    Some(read_u32_be(data, 33))
}

/// MSG_REPAIR_REQ / MSG_FETCH_REQ sequence list: `count` capped by the bytes
/// actually present.
pub fn decode_repair_seqs(data: &[u8]) -> Option<Vec<u32>> {
    if data.len() < HEADER_BYTES + 4 + 1 {
        return None;
    }
    let count = data[37] as usize;
    let max = count.min((data.len() - 38) / 4);
    let mut seqs = Vec::with_capacity(max);
    for index in 0..max {
        seqs.push(read_u32_be(data, 38 + index * 4));
    }
    Some(seqs)
}

pub fn decode_ihave(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < HEADER_BYTES + 4 + 4 {
        return None;
    }
    Some((read_u32_be(data, 33), read_u32_be(data, 37)))
}

/// `decodeRoute`: keeps consuming (and advancing the offset) past
/// [`MAX_ROUTE_HOPS`], but only the first 32 hops are kept.
fn decode_route(data: &[u8], offset_start: usize, route_count: usize) -> (Vec<String>, usize) {
    let mut offset = offset_start;
    let mut route: Vec<String> = Vec::new();
    for _ in 0..route_count {
        if offset + 1 > data.len() {
            break;
        }
        let len = data[offset] as usize;
        offset += 1;
        if len == 0 {
            break;
        }
        if offset + len > data.len() {
            break;
        }
        if route.len() < MAX_ROUTE_HOPS {
            route.push(decode_utf8(&data[offset..offset + len]));
        }
        offset += len;
    }
    (route, offset)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedUnicast {
    pub ack_token: Option<u64>,
    pub route: Vec<String>,
    pub reply_route: Option<Vec<String>>,
    pub payload_offset: usize,
}

pub fn decode_unicast(data: &[u8]) -> Option<DecodedUnicast> {
    if data.len() < HEADER_BYTES + 1 + 1 {
        return None;
    }
    let flags = data[33];
    let mut offset = 34usize;
    let mut ack_token: Option<u64> = None;
    if flags & UNICAST_FLAG_ACK != 0 {
        if data.len() < offset + 8 + 1 {
            return None;
        }
        ack_token = Some(read_u64_be(data, offset));
        offset += 8;
    }
    let route_count = data[offset].min(255) as usize;
    offset += 1;
    let (route, next_offset) = decode_route(data, offset, route_count);
    offset = next_offset;
    let mut reply_route: Option<Vec<String>> = None;
    if ack_token.is_some() {
        if data.len() < offset + 1 {
            return None;
        }
        let reply_count = data[offset].min(255) as usize;
        offset += 1;
        let (decoded_reply, next_offset) = decode_route(data, offset, reply_count);
        reply_route = Some(decoded_reply);
        offset = next_offset;
    }
    Some(DecodedUnicast {
        ack_token,
        route,
        reply_route,
        payload_offset: offset.min(data.len()),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedUnicastAck {
    pub ack_token: u64,
    pub route: Vec<String>,
}

pub fn decode_unicast_ack(data: &[u8]) -> Option<DecodedUnicastAck> {
    if data.len() < HEADER_BYTES + 8 + 1 {
        return None;
    }
    let ack_token = read_u64_be(data, 33);
    let route_count = data[41].min(255) as usize;
    let (route, _) = decode_route(data, 42, route_count);
    Some(DecodedUnicastAck { ack_token, route })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedRouteQuery {
    pub req_id: u32,
    /// `None` reproduces the inline `hashLen === 0 || overflow` case, which
    /// is answered with an empty ROUTE_REPLY rather than dropped.
    pub target_hash: Option<String>,
}

pub fn decode_route_query(data: &[u8]) -> Option<DecodedRouteQuery> {
    if data.len() < HEADER_BYTES + 4 + 1 {
        return None;
    }
    let req_id = read_u32_be(data, 33);
    let hash_len = data[37] as usize;
    let target_hash = if hash_len == 0 || 38 + hash_len > data.len() {
        None
    } else {
        Some(decode_utf8(&data[38..38 + hash_len]))
    };
    Some(DecodedRouteQuery {
        req_id,
        target_hash,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedRouteReply {
    pub req_id: u32,
    pub route: Vec<String>,
}

pub fn decode_route_reply(data: &[u8]) -> Option<DecodedRouteReply> {
    if data.len() < HEADER_BYTES + 4 + 1 {
        return None;
    }
    let req_id = read_u32_be(data, 33);
    let route_count = data[37].min(255) as usize;
    let (route, _) = decode_route(data, 38, route_count);
    Some(DecodedRouteReply { req_id, route })
}

fn decode_addr_list(data: &[u8], offset_start: usize, addr_count: usize) -> Vec<Vec<u8>> {
    let mut offset = offset_start;
    let mut addrs: Vec<Vec<u8>> = Vec::new();
    let max = addr_count.min(DECODE_ADDRS_MAX);
    for _ in 0..max {
        if offset + 2 > data.len() {
            break;
        }
        let len = read_u16_be(data, offset) as usize;
        offset += 2;
        if offset + len > data.len() {
            break;
        }
        addrs.push(data[offset..offset + len].to_vec());
        offset += len;
    }
    addrs
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedTrackerAnnounce {
    pub ttl_ms: u32,
    pub level: u16,
    pub free_slots: u16,
    pub bid_per_byte: u32,
    pub addrs: Vec<Vec<u8>>,
}

pub fn decode_tracker_announce(data: &[u8]) -> Option<DecodedTrackerAnnounce> {
    if data.len() < HEADER_BYTES + 4 + 2 + 2 + 2 + 4 + 1 {
        return None;
    }
    Some(DecodedTrackerAnnounce {
        ttl_ms: read_u32_be(data, 33),
        level: read_u16_be(data, 37),
        // maxChildren at offset 39 is kept in the wire format but unused.
        free_slots: read_u16_be(data, 41),
        bid_per_byte: read_u32_be(data, 43),
        addrs: decode_addr_list(data, 48, data[47] as usize),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedTrackerQuery {
    pub req_id: u32,
    pub want: u16,
}

pub fn decode_tracker_query(data: &[u8]) -> Option<DecodedTrackerQuery> {
    if data.len() < HEADER_BYTES + 4 + 2 {
        return None;
    }
    Some(DecodedTrackerQuery {
        req_id: read_u32_be(data, 33),
        want: read_u16_be(data, 37),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedTrackerCandidate {
    pub hash: String,
    pub level: u16,
    pub free_slots: u16,
    pub bid_per_byte: u32,
    /// Raw multiaddr bytes; validity filtering stays host-side.
    pub addrs: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedTrackerReply {
    pub req_id: u32,
    pub entries: Vec<DecodedTrackerCandidate>,
}

pub fn decode_tracker_reply(data: &[u8]) -> Option<DecodedTrackerReply> {
    if data.len() < HEADER_BYTES + 4 + 1 {
        return None;
    }
    let req_id = read_u32_be(data, 33);
    let count = data[37].min(255) as usize;
    let mut offset = 38usize;
    let mut entries: Vec<DecodedTrackerCandidate> = Vec::new();
    for _ in 0..count {
        if offset + 1 > data.len() {
            break;
        }
        let hash_len = data[offset] as usize;
        offset += 1;
        if offset + hash_len > data.len() {
            break;
        }
        let hash = decode_utf8(&data[offset..offset + hash_len]);
        offset += hash_len;
        if offset + 2 + 2 + 4 + 1 > data.len() {
            break;
        }
        let level = read_u16_be(data, offset);
        offset += 2;
        let free_slots = read_u16_be(data, offset);
        offset += 2;
        let bid_per_byte = read_u32_be(data, offset);
        offset += 4;
        let addr_count = data[offset] as usize;
        offset += 1;
        // Like the TS parser, a truncated addr list only ends this entry's
        // addr loop; the entry is kept and the outer loop keeps consuming
        // from the current offset.
        let mut addrs: Vec<Vec<u8>> = Vec::new();
        let addr_max = addr_count.min(DECODE_ADDRS_MAX);
        for _ in 0..addr_max {
            if offset + 2 > data.len() {
                break;
            }
            let len = read_u16_be(data, offset) as usize;
            offset += 2;
            if offset + len > data.len() {
                break;
            }
            addrs.push(data[offset..offset + len].to_vec());
            offset += len;
        }
        entries.push(DecodedTrackerCandidate {
            hash,
            level,
            free_slots,
            bid_per_byte,
            addrs,
        });
    }
    Some(DecodedTrackerReply { req_id, entries })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedTrackerFeedback {
    pub candidate_hash: String,
    pub event: u8,
    pub reason: u8,
}

pub fn decode_tracker_feedback(data: &[u8]) -> Option<DecodedTrackerFeedback> {
    if data.len() < HEADER_BYTES + 1 + 1 + 1 {
        return None;
    }
    let hash_len = data[33] as usize;
    let offset = 34usize;
    if offset + hash_len + 2 > data.len() {
        return None;
    }
    Some(DecodedTrackerFeedback {
        candidate_hash: decode_utf8(&data[offset..offset + hash_len]),
        event: data[offset + hash_len],
        reason: data[offset + hash_len + 1],
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedParentProbeReq {
    pub req_id: u32,
    pub min_free_slots: u16,
    pub reserve_root_capacity: bool,
}

pub fn decode_parent_probe_req(data: &[u8]) -> Option<DecodedParentProbeReq> {
    if data.len() < HEADER_BYTES + 4 {
        return None;
    }
    let req_id = read_u32_be(data, 33);
    let mut min_free_slots = 0u16;
    let mut probe_flags = PARENT_PROBE_REQ_FLAG_RESERVE_ROOT;
    if data.len() >= 37 + 2 {
        min_free_slots = read_u16_be(data, 37);
        if data.len() >= 39 + 1 {
            probe_flags = data[39];
        }
    }
    Some(DecodedParentProbeReq {
        req_id,
        min_free_slots,
        reserve_root_capacity: probe_flags & PARENT_PROBE_REQ_FLAG_RESERVE_ROOT != 0,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedParentProbeReply {
    pub req_id: u32,
    pub flags: u8,
    pub reservation_token: u32,
    pub level: u16,
    pub max_children: u16,
    pub free_slots: u16,
    pub children: u16,
    pub have_to_exclusive: u32,
    pub missing_seqs: u16,
    pub data_write_drops: u32,
    pub dropped_forwards: u32,
}

pub fn decode_parent_probe_reply(data: &[u8]) -> Option<DecodedParentProbeReply> {
    if data.len() < HEADER_BYTES + 27 {
        return None;
    }
    Some(DecodedParentProbeReply {
        req_id: read_u32_be(data, 33),
        flags: data[37],
        level: read_u16_be(data, 38),
        max_children: read_u16_be(data, 40),
        free_slots: read_u16_be(data, 42),
        children: read_u16_be(data, 44),
        have_to_exclusive: read_u32_be(data, 46),
        missing_seqs: read_u16_be(data, 50),
        data_write_drops: read_u32_be(data, 52),
        dropped_forwards: read_u32_be(data, 56),
        reservation_token: if data.len() >= HEADER_BYTES + 27 + 4 {
            read_u32_be(data, 60)
        } else {
            0
        },
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedProviderAnnounce {
    pub ttl_ms: u32,
    pub addrs: Vec<Vec<u8>>,
}

pub fn decode_provider_announce(data: &[u8]) -> Option<DecodedProviderAnnounce> {
    if data.len() < HEADER_BYTES + 4 + 1 {
        return None;
    }
    Some(DecodedProviderAnnounce {
        ttl_ms: read_u32_be(data, 33),
        addrs: decode_addr_list(data, 38, data[37] as usize),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedProviderQuery {
    pub req_id: u32,
    pub want: u16,
    pub seed: u32,
}

pub fn decode_provider_query(data: &[u8]) -> Option<DecodedProviderQuery> {
    if data.len() < HEADER_BYTES + 4 + 2 + 4 {
        return None;
    }
    Some(DecodedProviderQuery {
        req_id: read_u32_be(data, 33),
        want: read_u16_be(data, 37),
        seed: read_u32_be(data, 39),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedProviderEntry {
    pub hash: String,
    /// Raw multiaddr bytes; validity filtering stays host-side.
    pub addrs: Vec<Vec<u8>>,
}

/// `decodeProviderEntries`: entries with an empty hash are consumed but
/// skipped; the addr loop tolerates truncation per entry.
fn decode_provider_entries(
    data: &[u8],
    offset_start: usize,
    max_count: usize,
) -> Vec<DecodedProviderEntry> {
    let mut offset = offset_start;
    let mut providers: Vec<DecodedProviderEntry> = Vec::new();
    let limit = max_count.min(255);
    for _ in 0..limit {
        if offset + 1 > data.len() {
            break;
        }
        let hash_len = data[offset] as usize;
        offset += 1;
        if offset + hash_len > data.len() {
            break;
        }
        let hash = decode_utf8(&data[offset..offset + hash_len]);
        offset += hash_len;
        if offset + 1 > data.len() {
            break;
        }
        let addr_count = data[offset] as usize;
        offset += 1;
        let mut addrs: Vec<Vec<u8>> = Vec::new();
        let addr_max = addr_count.min(DECODE_ADDRS_MAX);
        for _ in 0..addr_max {
            if offset + 2 > data.len() {
                break;
            }
            let len = read_u16_be(data, offset) as usize;
            offset += 2;
            if offset + len > data.len() {
                break;
            }
            addrs.push(data[offset..offset + len].to_vec());
            offset += len;
        }
        if hash.is_empty() {
            continue;
        }
        providers.push(DecodedProviderEntry { hash, addrs });
    }
    providers
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedProviderReply {
    pub req_id: u32,
    pub entries: Vec<DecodedProviderEntry>,
}

pub fn decode_provider_reply(data: &[u8]) -> Option<DecodedProviderReply> {
    if data.len() < HEADER_BYTES + 4 + 1 {
        return None;
    }
    Some(DecodedProviderReply {
        req_id: read_u32_be(data, 33),
        entries: decode_provider_entries(data, 38, data[37] as usize),
    })
}

pub fn decode_provider_notify(data: &[u8]) -> Option<Vec<DecodedProviderEntry>> {
    if data.len() < HEADER_BYTES + 1 {
        return None;
    }
    Some(decode_provider_entries(data, 34, data[33] as usize))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedProviderSubscribe {
    pub want: u16,
    pub ttl_ms: u32,
}

pub fn decode_provider_subscribe(data: &[u8]) -> Option<DecodedProviderSubscribe> {
    if data.len() < HEADER_BYTES + 2 + 4 {
        return None;
    }
    Some(DecodedProviderSubscribe {
        want: read_u16_be(data, 33),
        ttl_ms: read_u32_be(data, 35),
    })
}

// --- parent-upgrade policy + gate (fanout-tree-parent-upgrade.ts, PR #911) ---

/// `normalizeParentUpgradePolicy` input, in the fixed order documented by
/// the TS adapter. Numeric options (indices 0-24) carry `Number(value)`
/// verbatim; bit `i` of the presence mask (index 30) marks option `i` as
/// provided. The mask exists because the TS core's `??` falls back only on
/// absent options: an explicitly-NaN option must stay distinguishable from
/// an unset one so it can flow through `Math.max(0, Math.floor(NaN))` as
/// NaN, exactly like TS. Tri-state booleans use -1 unset / 0 false /
/// 1 true; mode uses 0 unset-or-unknown / 1 direct / 2 probe / 3 shadow.
pub const PU_OPTIONS_LEN: usize = 31;
pub const PU_POLICY_LEN: usize = 30;
pub const PU_NUMERIC_OPTIONS_LEN: usize = 25;
pub const PU_OPTIONS_PRESENCE_INDEX: usize = 30;

pub const PU_MODE_DIRECT: f64 = 1.0;
pub const PU_MODE_PROBE: f64 = 2.0;
pub const PU_MODE_SHADOW: f64 = 3.0;

fn opt(values: &[f64], index: usize) -> Option<f64> {
    debug_assert!(index < PU_NUMERIC_OPTIONS_LEN);
    let mask = values[PU_OPTIONS_PRESENCE_INDEX] as u32;
    if mask & (1u32 << index) == 0 {
        None
    } else {
        Some(values[index])
    }
}

/// `Math.max(min, Math.floor(value))` with JS semantics: NaN in either
/// operand poisons the result (an explicitly-NaN option disables the
/// downstream `> 0` guards identically in both cores).
fn floor_max(min: f64, value: f64) -> f64 {
    let floored = value.floor();
    if min.is_nan() || floored.is_nan() {
        f64::NAN
    } else {
        floored.max(min)
    }
}

pub fn normalize_parent_upgrade_policy(options: &[f64]) -> Vec<f64> {
    assert_eq!(options.len(), PU_OPTIONS_LEN);
    let interval_ms = floor_max(0.0, opt(options, 0).unwrap_or(0.0));
    let min_level_gain = floor_max(1.0, opt(options, 1).unwrap_or(1.0));
    let root_min_level_gain = floor_max(min_level_gain, opt(options, 2).unwrap_or(3.0));
    let root_min_subtree_gain = floor_max(
        min_level_gain,
        opt(options, 3).unwrap_or(root_min_level_gain),
    );
    let min_free_slots = floor_max(0.0, opt(options, 5).unwrap_or(8.0));
    let max_child_load_ratio_raw = opt(options, 7).unwrap_or(0.5);
    let max_child_load_ratio = if max_child_load_ratio_raw.is_finite() {
        max_child_load_ratio_raw.max(0.0)
    } else {
        0.5
    };
    let root_max_child_load_ratio_raw = opt(options, 8).unwrap_or(max_child_load_ratio.min(0.4));
    let root_max_child_load_ratio = if root_max_child_load_ratio_raw.is_finite() {
        root_max_child_load_ratio_raw.max(0.0)
    } else {
        max_child_load_ratio.min(0.4)
    };
    let stale_root_probe_probability_raw = opt(options, 15).unwrap_or(0.015625);
    let stale_root_probe_probability = if stale_root_probe_probability_raw.is_finite() {
        stale_root_probe_probability_raw.clamp(0.0, 1.0)
    } else {
        0.015625
    };
    let cooldown_ms = floor_max(0.0, opt(options, 9).unwrap_or(5_000.0));
    let failed_backoff_min_ms = floor_max(0.0, opt(options, 10).unwrap_or(cooldown_ms));
    let probe_reject_cooldown_ms = floor_max(0.0, opt(options, 19).unwrap_or(10_000.0));
    let quiet_ms = floor_max(0.0, opt(options, 12).unwrap_or(5_000.0));
    let mode_raw = options[29];
    let mode = if mode_raw == PU_MODE_PROBE || mode_raw == PU_MODE_SHADOW {
        mode_raw
    } else if mode_raw == PU_MODE_DIRECT {
        PU_MODE_DIRECT
    } else {
        PU_MODE_SHADOW
    };
    let leaf_only = options[25] != 0.0; // `!== false`
    let repair_guard = options[26] != 0.0;
    let data_guard = options[27] != 0.0;
    let verify_stale_root_capacity = match options[28] {
        v if v < 0.0 => mode == PU_MODE_SHADOW, // unset -> `?? (mode === "shadow")`
        v => v != 0.0,
    };

    vec![
        interval_ms,
        if leaf_only { 1.0 } else { 0.0 },
        min_level_gain,
        root_min_level_gain,
        root_min_subtree_gain,
        floor_max(min_level_gain, opt(options, 4).unwrap_or(2.0)),
        min_free_slots,
        floor_max(0.0, opt(options, 6).unwrap_or(min_free_slots)),
        max_child_load_ratio,
        root_max_child_load_ratio,
        stale_root_probe_probability,
        cooldown_ms,
        quiet_ms,
        floor_max(0.0, opt(options, 13).unwrap_or(quiet_ms)),
        floor_max(0.0, opt(options, 14).unwrap_or(2.0)),
        if repair_guard { 1.0 } else { 0.0 },
        if data_guard { 1.0 } else { 0.0 },
        mode,
        if verify_stale_root_capacity { 1.0 } else { 0.0 },
        failed_backoff_min_ms,
        floor_max(failed_backoff_min_ms, opt(options, 11).unwrap_or(60_000.0)),
        floor_max(1.0, opt(options, 16).unwrap_or(500.0)),
        floor_max(1.0, opt(options, 17).unwrap_or(2.0)),
        floor_max(0.0, opt(options, 18).unwrap_or(0.0)),
        probe_reject_cooldown_ms,
        floor_max(
            probe_reject_cooldown_ms,
            opt(options, 20).unwrap_or(60_000.0),
        ),
        floor_max(0.0, opt(options, 21).unwrap_or(2_000.0)),
        floor_max(1.0, opt(options, 22).unwrap_or(2.0)),
        floor_max(
            0.0,
            opt(options, 23).unwrap_or(if mode == PU_MODE_SHADOW { 5_000.0 } else { 0.0 }),
        ),
        floor_max(
            1.0,
            opt(options, 24).unwrap_or(if mode == PU_MODE_SHADOW { 32.0 } else { 1.0 }),
        ),
    ]
}

pub const PU_GATE_RUN: u32 = 0;
pub const PU_GATE_SKIP_LEAF: u32 = 1;
pub const PU_GATE_SKIP_REPAIR: u32 = 2;
pub const PU_GATE_SKIP_DATA: u32 = 3;
pub const PU_GATE_SKIP_COOLDOWN: u32 = 4;
pub const PU_GATE_SKIP_QUIET: u32 = 5;
pub const PU_GATE_SKIP_BUDGET: u32 = 6;
/// Set when the host must apply `state.parentUpgradeRetryAfterSeq = -1`.
pub const PU_GATE_RESET_RETRY_AFTER_SEQ: u32 = 0x100;

pub struct ParentUpgradeGateState {
    pub children_size: f64,
    pub missing_seqs_size: f64,
    pub last_repair_sent_at: f64,
    pub end_seq_exclusive: f64,
    pub parent_upgrade_retry_after_seq: f64,
    pub max_seq_seen: f64,
    pub parent_upgrade_count: f64,
    pub parent_upgrade_backoff_until: f64,
    pub parent_upgrade_last_at: f64,
    pub last_parent_data_at: f64,
    /// NaN when the field is unset (`?? lastParentDataAt`).
    pub last_parent_upgrade_activity_at: f64,
}

pub struct ParentUpgradeGateOptions {
    pub leaf_only: bool,
    pub repair_guard: bool,
    pub data_guard: bool,
    pub ended_and_complete: bool,
    pub max_per_peer: f64,
    pub cooldown_ms: f64,
    pub quiet_ms: f64,
    pub repair_quiet_ms: f64,
    pub now: f64,
}

/// `evaluateParentUpgradeGate`; the retry-after-seq reset the TS version
/// applies in place is reported via [`PU_GATE_RESET_RETRY_AFTER_SEQ`].
pub fn evaluate_parent_upgrade_gate(
    state: &ParentUpgradeGateState,
    options: &ParentUpgradeGateOptions,
) -> u32 {
    let mut reset = 0u32;
    if options.leaf_only && state.children_size > 0.0 {
        return PU_GATE_SKIP_LEAF;
    }
    if options.repair_guard && state.missing_seqs_size > 0.0 {
        return PU_GATE_SKIP_REPAIR;
    }
    if options.repair_guard
        && options.repair_quiet_ms > 0.0
        && state.last_repair_sent_at > 0.0
        && options.now - state.last_repair_sent_at < options.repair_quiet_ms
    {
        return PU_GATE_SKIP_REPAIR;
    }
    if options.data_guard && !(state.end_seq_exclusive > 0.0 && options.ended_and_complete) {
        return PU_GATE_SKIP_DATA;
    }
    if options.data_guard && state.parent_upgrade_retry_after_seq >= 0.0 {
        if state.max_seq_seen <= state.parent_upgrade_retry_after_seq {
            return PU_GATE_SKIP_DATA;
        }
        reset = PU_GATE_RESET_RETRY_AFTER_SEQ;
    }
    if options.max_per_peer > 0.0 && state.parent_upgrade_count >= options.max_per_peer {
        return PU_GATE_SKIP_BUDGET | reset;
    }
    if state.parent_upgrade_backoff_until > options.now {
        return PU_GATE_SKIP_COOLDOWN | reset;
    }
    if options.cooldown_ms > 0.0
        && state.parent_upgrade_last_at > 0.0
        && options.now - state.parent_upgrade_last_at < options.cooldown_ms
    {
        return PU_GATE_SKIP_COOLDOWN | reset;
    }
    let last_parent_upgrade_activity_at = if state.last_parent_upgrade_activity_at.is_nan() {
        state.last_parent_data_at
    } else {
        state.last_parent_upgrade_activity_at
    };
    if options.quiet_ms > 0.0
        && last_parent_upgrade_activity_at > 0.0
        && options.now - last_parent_upgrade_activity_at < options.quiet_ms
    {
        return PU_GATE_SKIP_QUIET | reset;
    }
    PU_GATE_RUN | reset
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Vec<u8> {
        (0u8..32).collect()
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn js_numeric_semantics() {
        assert_eq!(js_to_uint32(-1.0), 0xffff_ffff);
        assert_eq!(js_to_uint32(4_294_967_296.0), 0);
        assert_eq!(js_to_uint32(5_000_000_000.0), 705_032_704);
        assert_eq!(js_to_uint32(f64::NAN), 0);
        assert_eq!(js_to_int32(-1.5), -1);
        assert_eq!(js_to_int32(2_147_483_648.0), -2_147_483_648);
        assert_eq!(clamp_u16(-5.0), 0);
        assert_eq!(clamp_u16(70_000.0), 0xffff);
        assert_eq!(clamp_u16(1.9), 1);
        assert_eq!(js_floor_clamp_u32(-3.0), 0);
        assert_eq!(js_floor_clamp_u32(1.9), 1);
        assert_eq!(js_floor_clamp_u32(5_000_000_000.0), 705_032_704);
    }

    #[test]
    fn join_req_layout_and_roundtrip() {
        let frame = encode_join_req(&key(), 7.0, 9.0, 0.0);
        assert_eq!(frame.len(), 41);
        assert_eq!(frame[0], MSG_JOIN_REQ);
        assert_eq!(&frame[1..33], key().as_slice());
        assert_eq!(
            decode_join_req(&frame).unwrap(),
            DecodedJoinReq {
                req_id: 7,
                bid_per_byte: 9,
                parent_upgrade_reservation_token: 0,
            }
        );

        let with_token = encode_join_req(&key(), 7.0, 9.0, 5.0);
        assert_eq!(with_token.len(), 45);
        assert_eq!(
            decode_join_req(&with_token)
                .unwrap()
                .parent_upgrade_reservation_token,
            5
        );
        assert!(decode_join_req(&frame[..40]).is_none());
    }

    #[test]
    fn join_accept_route_and_have_range() {
        let route = strings(&["root", "mid", "leaf"]);
        let frame = encode_join_accept(&key(), 1.0, 2.0, &route, Some((3.0, 9.0)));
        let decoded = decode_join_accept(&frame).unwrap();
        assert_eq!(decoded.parent_level, 2);
        assert_eq!(decoded.parent_route_from_root, route);
        assert_eq!(decoded.have_range, Some((3, 9)));

        // hop cap: only the first 32 hops are encoded, and the appendix is
        // still readable at the resulting offset
        let long: Vec<String> = (0..40).map(|i| format!("hop{i}")).collect();
        let frame = encode_join_accept(&key(), 1.0, 2.0, &long, Some((0.0, 1.0)));
        let decoded = decode_join_accept(&frame).unwrap();
        assert_eq!(decoded.parent_route_from_root.len(), MAX_ROUTE_HOPS);
        assert_eq!(decoded.have_range, Some((0, 1)));

        // empty/oversized hops are skipped by the encoder
        let frame = encode_join_accept(
            &key(),
            1.0,
            2.0,
            &["".to_string(), "x".repeat(256), "ok".to_string()],
            None,
        );
        let decoded = decode_join_accept(&frame).unwrap();
        assert_eq!(decoded.parent_route_from_root, strings(&["ok"]));
        assert_eq!(decoded.have_range, None);
        assert!(decode_join_accept(&frame[..39]).is_none());
    }

    #[test]
    fn join_reject_redirect_rules() {
        // no redirects -> short form
        let frame = encode_join_reject(&key(), 3.0, 2.0, &[]);
        assert_eq!(frame.len(), 38);
        let decoded = decode_join_reject(&frame).unwrap();
        assert_eq!(decoded.reason, 2);
        assert!(decoded.redirects.is_empty());

        // entries without valid addrs are skipped entirely
        let no_addrs = vec![JoinRejectRedirectInput {
            hash: "h".to_string(),
            addrs: vec![vec![]],
        }];
        assert_eq!(encode_join_reject(&key(), 3.0, 2.0, &no_addrs).len(), 38);

        let redirects: Vec<JoinRejectRedirectInput> = (0..6)
            .map(|i| JoinRejectRedirectInput {
                hash: format!("peer-{i}"),
                addrs: (0..10).map(|j| vec![i as u8, j as u8, 1]).collect(),
            })
            .collect();
        let frame = encode_join_reject(&key(), 3.0, 1.0, &redirects);
        let decoded = decode_join_reject(&frame).unwrap();
        assert_eq!(decoded.reason, 1);
        // redirect cap 4, addr cap 8
        assert_eq!(decoded.redirects.len(), JOIN_REJECT_REDIRECT_MAX);
        assert_eq!(
            decoded.redirects[0].addrs.len(),
            JOIN_REJECT_REDIRECT_ADDR_MAX
        );
        assert_eq!(decoded.redirects[3].hash, "peer-3");
    }

    #[test]
    fn simple_frames_roundtrip() {
        assert_eq!(encode_kick(&key()).len(), 33);
        assert_eq!(encode_leave(&key())[0], MSG_LEAVE);
        assert_eq!(
            encode_provider_unsubscribe(&key())[0],
            MSG_PROVIDER_UNSUBSCRIBE
        );

        let end = encode_end(&key(), 77.0);
        assert_eq!(decode_end(&end).unwrap(), 77);
        assert!(decode_end(&end[..36]).is_none());

        let ihave = encode_ihave(&key(), 3.0, 12.0);
        assert_eq!(decode_ihave(&ihave).unwrap(), (3, 12));

        let data = encode_data(&[9, 8, 7]);
        assert_eq!(data, vec![MSG_DATA, 9, 8, 7]);

        let proxy = encode_publish_proxy(&key(), &[1, 2]);
        assert_eq!(proxy[0], MSG_PUBLISH_PROXY);
        assert_eq!(&proxy[33..], &[1, 2]);
    }

    #[test]
    fn repair_seq_list_capped_by_count_and_length() {
        let seqs: Vec<f64> = (0..300).map(|i| i as f64).collect();
        let frame = encode_repair_req(&key(), 5.0, &seqs);
        // count byte caps at 255
        assert_eq!(frame[37], 255);
        let decoded = decode_repair_seqs(&frame).unwrap();
        assert_eq!(decoded.len(), 255);
        assert_eq!(decoded[254], 254);

        // truncated frames yield only the seqs actually present
        let truncated = &frame[..38 + 10 * 4 + 2];
        assert_eq!(decode_repair_seqs(truncated).unwrap().len(), 10);

        let fetch = encode_fetch_req(&key(), 5.0, &[1.0]);
        assert_eq!(fetch[0], MSG_FETCH_REQ);
    }

    #[test]
    fn unicast_roundtrip_with_and_without_ack() {
        let payload = [1u8, 2, 3, 4];
        let plain = encode_unicast(&key(), &strings(&["a", "b"]), &payload, None, &[]);
        let decoded = decode_unicast(&plain).unwrap();
        assert_eq!(decoded.ack_token, None);
        assert_eq!(decoded.route, strings(&["a", "b"]));
        assert_eq!(decoded.reply_route, None);
        assert_eq!(&plain[decoded.payload_offset..], payload);

        let acked = encode_unicast(
            &key(),
            &strings(&["a", "b"]),
            &payload,
            Some(u64::MAX - 3),
            &strings(&["b", "a"]),
        );
        let decoded = decode_unicast(&acked).unwrap();
        assert_eq!(decoded.ack_token, Some(u64::MAX - 3));
        assert_eq!(decoded.reply_route, Some(strings(&["b", "a"])));
        assert_eq!(&acked[decoded.payload_offset..], payload);

        let ack = encode_unicast_ack(&key(), 42, &strings(&["r", "x"]));
        let decoded = decode_unicast_ack(&ack).unwrap();
        assert_eq!(decoded.ack_token, 42);
        assert_eq!(decoded.route, strings(&["r", "x"]));
        assert!(decode_unicast_ack(&ack[..41]).is_none());
    }

    #[test]
    fn route_query_reply_semantics() {
        let query = encode_route_query(&key(), 9.0, "target");
        let decoded = decode_route_query(&query).unwrap();
        assert_eq!(decoded.req_id, 9);
        assert_eq!(decoded.target_hash.as_deref(), Some("target"));

        // oversized target hashes are truncated by the encoder
        let long = "x".repeat(300);
        let query = encode_route_query(&key(), 9.0, &long);
        assert_eq!(query[37], 255);
        assert_eq!(
            decode_route_query(&query)
                .unwrap()
                .target_hash
                .unwrap()
                .len(),
            255
        );

        // zero-length target decodes to None (answered with an empty reply)
        let empty = encode_route_query(&key(), 9.0, "");
        assert_eq!(decode_route_query(&empty).unwrap().target_hash, None);

        let reply = encode_route_reply(&key(), 9.0, &strings(&["a", "b", "c"]));
        let decoded = decode_route_reply(&reply).unwrap();
        assert_eq!(decoded.route, strings(&["a", "b", "c"]));

        let empty_reply = encode_route_reply(&key(), 9.0, &[]);
        assert_eq!(decode_route_reply(&empty_reply).unwrap().route.len(), 0);
    }

    #[test]
    fn tracker_frames_roundtrip() {
        let addrs: Vec<Vec<u8>> = (0..20).map(|i| vec![i as u8; 3]).collect();
        let frame = encode_tracker_announce(&key(), 60_000.9, 2.0, 8.0, 5.0, 100.7, &addrs);
        let decoded = decode_tracker_announce(&frame).unwrap();
        assert_eq!(decoded.ttl_ms, 60_000);
        assert_eq!(decoded.level, 2);
        assert_eq!(decoded.free_slots, 5);
        assert_eq!(decoded.bid_per_byte, 100);
        // encoder keeps up to 255 addrs; decoder caps at 16
        assert_eq!(decoded.addrs.len(), DECODE_ADDRS_MAX);

        let query = encode_tracker_query(&key(), 4.0, 70_000.0);
        let decoded = decode_tracker_query(&query).unwrap();
        assert_eq!(decoded.want, 0xffff);

        let entries = vec![
            TrackerEntryInput {
                hash: "h1".to_string(),
                level: 1.0,
                free_slots: 2.0,
                bid_per_byte: 3.0,
                addrs: vec![vec![1, 2], vec![3]],
            },
            TrackerEntryInput {
                hash: "x".repeat(256), // skipped: hash too long
                level: 1.0,
                free_slots: 2.0,
                bid_per_byte: 3.0,
                addrs: vec![vec![1]],
            },
            TrackerEntryInput {
                hash: String::new(), // zero-length hash is kept
                level: 9.0,
                free_slots: 0.0,
                bid_per_byte: 1.5,
                addrs: vec![],
            },
        ];
        let reply = encode_tracker_reply(&key(), 6.0, &entries);
        let decoded = decode_tracker_reply(&reply).unwrap();
        assert_eq!(decoded.req_id, 6);
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].hash, "h1");
        assert_eq!(decoded.entries[0].addrs, vec![vec![1, 2], vec![3]]);
        assert_eq!(decoded.entries[1].hash, "");
        assert_eq!(decoded.entries[1].bid_per_byte, 1);

        let feedback = encode_tracker_feedback(&key(), "candidate", 1.0, 2.0);
        let decoded = decode_tracker_feedback(&feedback).unwrap();
        assert_eq!(decoded.candidate_hash, "candidate");
        assert_eq!((decoded.event, decoded.reason), (1, 2));
        assert!(decode_tracker_feedback(&feedback[..feedback.len() - 1]).is_none());
    }

    #[test]
    fn parent_probe_frames_roundtrip() {
        // no extension when minFreeSlots == 0 and reserve default
        let plain = encode_parent_probe_req(&key(), 3.0, 0.0, true);
        assert_eq!(plain.len(), 37);
        let decoded = decode_parent_probe_req(&plain).unwrap();
        assert!(decoded.reserve_root_capacity);
        assert_eq!(decoded.min_free_slots, 0);

        let extended = encode_parent_probe_req(&key(), 3.0, 4.0, false);
        assert_eq!(extended.len(), 40);
        let decoded = decode_parent_probe_req(&extended).unwrap();
        assert_eq!(decoded.min_free_slots, 4);
        assert!(!decoded.reserve_root_capacity);

        let reply = encode_parent_probe_reply(
            &key(),
            3.0,
            0b1011 as f64,
            2.0,
            8.0,
            5.0,
            3.0,
            100.0,
            2.0,
            7.0,
            9.0,
            11.0,
        );
        let decoded = decode_parent_probe_reply(&reply).unwrap();
        assert_eq!(decoded.req_id, 3);
        assert_eq!(decoded.flags, 0b1011);
        assert_eq!(decoded.level, 2);
        assert_eq!(decoded.max_children, 8);
        assert_eq!(decoded.free_slots, 5);
        assert_eq!(decoded.children, 3);
        assert_eq!(decoded.have_to_exclusive, 100);
        assert_eq!(decoded.missing_seqs, 2);
        assert_eq!(decoded.data_write_drops, 7);
        assert_eq!(decoded.dropped_forwards, 9);
        assert_eq!(decoded.reservation_token, 11);
        // reservation appendix is optional on decode
        let decoded = decode_parent_probe_reply(&reply[..60]).unwrap();
        assert_eq!(decoded.reservation_token, 0);
        assert!(decode_parent_probe_reply(&reply[..59]).is_none());
    }

    #[test]
    fn provider_frames_roundtrip() {
        let announce = encode_provider_announce(&key(), 30_000.0, &[vec![1, 2, 3]]);
        let decoded = decode_provider_announce(&announce).unwrap();
        assert_eq!(decoded.ttl_ms, 30_000);
        assert_eq!(decoded.addrs, vec![vec![1, 2, 3]]);

        let query = encode_provider_query(&key(), 2.0, 3.0, 4.0);
        let decoded = decode_provider_query(&query).unwrap();
        assert_eq!((decoded.req_id, decoded.want, decoded.seed), (2, 3, 4));

        let entries = vec![
            ProviderEntryInput {
                hash: "p1".to_string(),
                addrs: vec![vec![1], vec![2, 3]],
            },
            ProviderEntryInput {
                hash: String::new(), // consumed but skipped on decode
                addrs: vec![vec![9]],
            },
        ];
        let reply = encode_provider_reply(&key(), 2.0, &entries);
        let decoded = decode_provider_reply(&reply).unwrap();
        assert_eq!(decoded.req_id, 2);
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].hash, "p1");

        let notify = encode_provider_notify(&key(), &entries);
        let decoded = decode_provider_notify(&notify).unwrap();
        assert_eq!(decoded.len(), 1);

        let subscribe = encode_provider_subscribe(&key(), 3.0, 45_000.0);
        let decoded = decode_provider_subscribe(&subscribe).unwrap();
        assert_eq!((decoded.want, decoded.ttl_ms), (3, 45_000));
    }

    fn pu_unset_options() -> [f64; PU_OPTIONS_LEN] {
        let mut options = [f64::NAN; PU_OPTIONS_LEN];
        options[25] = -1.0;
        options[26] = -1.0;
        options[27] = -1.0;
        options[28] = -1.0;
        options[29] = 0.0;
        options[PU_OPTIONS_PRESENCE_INDEX] = 0.0;
        options
    }

    fn pu_set(options: &mut [f64; PU_OPTIONS_LEN], index: usize, value: f64) {
        options[index] = value;
        options[PU_OPTIONS_PRESENCE_INDEX] =
            ((options[PU_OPTIONS_PRESENCE_INDEX] as u32) | (1u32 << index)) as f64;
    }

    #[test]
    fn parent_upgrade_policy_defaults() {
        let options = pu_unset_options();
        let policy = normalize_parent_upgrade_policy(&options);
        assert_eq!(policy.len(), PU_POLICY_LEN);
        assert_eq!(policy[0], 0.0); // intervalMs
        assert_eq!(policy[1], 1.0); // leafOnly
        assert_eq!(policy[2], 1.0); // minLevelGain
        assert_eq!(policy[3], 3.0); // rootMinLevelGain
        assert_eq!(policy[4], 3.0); // rootMinSubtreeGain (defaults to rootMinLevelGain)
        assert_eq!(policy[5], 2.0); // nonRootMinLevelGain
        assert_eq!(policy[6], 8.0); // minFreeSlots
        assert_eq!(policy[7], 8.0); // rootMinFreeSlots (defaults to minFreeSlots)
        assert_eq!(policy[8], 0.5); // maxChildLoadRatio
        assert_eq!(policy[9], 0.4); // rootMaxChildLoadRatio (min(0.5, 0.4))
        assert_eq!(policy[10], 0.015625);
        assert_eq!(policy[11], 5_000.0); // cooldownMs
        assert_eq!(policy[12], 5_000.0); // quietMs
        assert_eq!(policy[13], 5_000.0); // repairQuietMs (defaults to quietMs)
        assert_eq!(policy[14], 2.0); // maxPerPeer
        assert_eq!(policy[17], PU_MODE_SHADOW);
        assert_eq!(policy[18], 1.0); // verifyStaleRootCapacity (mode == shadow)
        assert_eq!(policy[19], 5_000.0); // failedBackoff.min (defaults to cooldownMs)
        assert_eq!(policy[20], 60_000.0);
        assert_eq!(policy[21], 500.0); // probe.timeoutMs
        assert_eq!(policy[28], 5_000.0); // shadow.dualPathMs (mode == shadow)
        assert_eq!(policy[29], 32.0); // shadow.dualPathMinMessages
    }

    #[test]
    fn parent_upgrade_policy_direct_mode_and_overrides() {
        let mut options = pu_unset_options();
        options[25] = 0.0; // leafOnly: false
        options[27] = 1.0; // dataGuard: true
        options[29] = PU_MODE_DIRECT;
        pu_set(&mut options, 1, 5.9); // minLevelGain -> floor 5
        pu_set(&mut options, 2, 2.0); // rootMinLevelGain -> max(minLevelGain, 2) = 5
        pu_set(&mut options, 7, f64::INFINITY); // maxChildLoadRatio -> default 0.5
        let policy = normalize_parent_upgrade_policy(&options);
        assert_eq!(policy[1], 0.0); // leafOnly false
        assert_eq!(policy[2], 5.0);
        assert_eq!(policy[3], 5.0);
        assert_eq!(policy[8], 0.5);
        assert_eq!(policy[17], PU_MODE_DIRECT);
        assert_eq!(policy[18], 0.0); // verifyStaleRootCapacity (mode != shadow)
        assert_eq!(policy[28], 0.0); // dualPathMs (mode != shadow)
        assert_eq!(policy[29], 1.0); // dualPathMinMessages (mode != shadow)
    }

    #[test]
    fn parent_upgrade_policy_explicit_nan_is_kept() {
        // An explicitly-NaN numeric option is "set": the TS core's `??`
        // keeps it and `Math.max(0, Math.floor(NaN))` stays NaN, silently
        // disabling the downstream `> 0` guards. Only absent options take
        // the documented defaults.
        let mut options = pu_unset_options();
        pu_set(&mut options, 4, f64::NAN); // parentUpgradeNonRootMinLevelGain
        pu_set(&mut options, 18, f64::NAN); // parentProbeMaxLagMessages
        pu_set(&mut options, 21, f64::NAN); // parentShadowObserveMs
        let policy = normalize_parent_upgrade_policy(&options);
        assert!(policy[5].is_nan()); // nonRootMinLevelGain (default 2)
        assert!(policy[23].is_nan()); // probe.maxLagMessages (default 0)
        assert!(policy[26].is_nan()); // shadow.observeMs (default 2000)
        assert_eq!(policy[11], 5_000.0); // unrelated cooldownMs keeps its default

        // NaN poisons dependent defaults like TS `Math.max(NaN, ...)` ...
        let mut options = pu_unset_options();
        pu_set(&mut options, 1, f64::NAN); // minLevelGain
        let policy = normalize_parent_upgrade_policy(&options);
        assert!(policy[2].is_nan()); // minLevelGain
        assert!(policy[3].is_nan()); // rootMinLevelGain = max(NaN, 3)
        assert!(policy[4].is_nan()); // rootMinSubtreeGain
        assert!(policy[5].is_nan()); // nonRootMinLevelGain = max(NaN, 2)

        // ... while the isFinite-guarded ratio options fall back to their
        // defaults, exactly like TS
        let mut options = pu_unset_options();
        pu_set(&mut options, 7, f64::NAN); // maxChildLoadRatio
        pu_set(&mut options, 15, f64::NAN); // staleRootProbeProbability
        let policy = normalize_parent_upgrade_policy(&options);
        assert_eq!(policy[8], 0.5);
        assert_eq!(policy[10], 0.015625);
    }

    fn gate_state() -> ParentUpgradeGateState {
        ParentUpgradeGateState {
            children_size: 0.0,
            missing_seqs_size: 0.0,
            last_repair_sent_at: 0.0,
            end_seq_exclusive: 1.0,
            parent_upgrade_retry_after_seq: -1.0,
            max_seq_seen: 0.0,
            parent_upgrade_count: 0.0,
            parent_upgrade_backoff_until: 0.0,
            parent_upgrade_last_at: 0.0,
            last_parent_data_at: 0.0,
            last_parent_upgrade_activity_at: f64::NAN,
        }
    }

    fn gate_options() -> ParentUpgradeGateOptions {
        ParentUpgradeGateOptions {
            leaf_only: true,
            repair_guard: true,
            data_guard: true,
            ended_and_complete: true,
            max_per_peer: 2.0,
            cooldown_ms: 5_000.0,
            quiet_ms: 5_000.0,
            repair_quiet_ms: 5_000.0,
            now: 100_000.0,
        }
    }

    #[test]
    fn parent_upgrade_gate_rules() {
        assert_eq!(
            evaluate_parent_upgrade_gate(&gate_state(), &gate_options()),
            PU_GATE_RUN
        );

        let mut state = gate_state();
        state.children_size = 1.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_SKIP_LEAF
        );

        let mut state = gate_state();
        state.missing_seqs_size = 2.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_SKIP_REPAIR
        );

        let mut state = gate_state();
        state.last_repair_sent_at = 99_000.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_SKIP_REPAIR
        );

        let mut state = gate_state();
        state.end_seq_exclusive = 0.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_SKIP_DATA
        );

        let mut state = gate_state();
        state.parent_upgrade_retry_after_seq = 5.0;
        state.max_seq_seen = 5.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_SKIP_DATA
        );
        // once data advanced, the retry marker is reset and evaluation continues
        state.max_seq_seen = 6.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_RUN | PU_GATE_RESET_RETRY_AFTER_SEQ
        );

        let mut state = gate_state();
        state.parent_upgrade_count = 2.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_SKIP_BUDGET
        );

        let mut state = gate_state();
        state.parent_upgrade_backoff_until = 200_000.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_SKIP_COOLDOWN
        );

        let mut state = gate_state();
        state.parent_upgrade_last_at = 98_000.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_SKIP_COOLDOWN
        );

        let mut state = gate_state();
        state.last_parent_data_at = 99_000.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_SKIP_QUIET
        );
        // explicit activity timestamp takes precedence over lastParentDataAt
        state.last_parent_upgrade_activity_at = 10_000.0;
        assert_eq!(
            evaluate_parent_upgrade_gate(&state, &gate_options()),
            PU_GATE_RUN
        );
    }

    #[test]
    fn truncated_frames_are_rejected_uniformly() {
        let frames: Vec<Vec<u8>> = vec![
            encode_join_req(&key(), 1.0, 2.0, 3.0),
            encode_join_accept(&key(), 1.0, 2.0, &strings(&["a"]), Some((0.0, 5.0))),
            encode_join_reject(
                &key(),
                1.0,
                2.0,
                &[JoinRejectRedirectInput {
                    hash: "h".to_string(),
                    addrs: vec![vec![1, 2, 3]],
                }],
            ),
            encode_end(&key(), 1.0),
            encode_repair_req(&key(), 1.0, &[1.0, 2.0]),
            encode_ihave(&key(), 1.0, 2.0),
            encode_unicast(&key(), &strings(&["a"]), &[1], Some(7), &strings(&["b"])),
            encode_unicast_ack(&key(), 7, &strings(&["a"])),
            encode_route_query(&key(), 1.0, "t"),
            encode_route_reply(&key(), 1.0, &strings(&["a"])),
            encode_tracker_announce(&key(), 1.0, 1.0, 1.0, 1.0, 1.0, &[vec![1]]),
            encode_tracker_query(&key(), 1.0, 1.0),
            encode_tracker_reply(
                &key(),
                1.0,
                &[TrackerEntryInput {
                    hash: "h".to_string(),
                    level: 1.0,
                    free_slots: 1.0,
                    bid_per_byte: 1.0,
                    addrs: vec![vec![1]],
                }],
            ),
            encode_tracker_feedback(&key(), "h", 1.0, 1.0),
            encode_parent_probe_req(&key(), 1.0, 1.0, false),
            encode_parent_probe_reply(
                &key(),
                1.0,
                0.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
            encode_provider_announce(&key(), 1.0, &[vec![1]]),
            encode_provider_query(&key(), 1.0, 1.0, 1.0),
            encode_provider_reply(
                &key(),
                1.0,
                &[ProviderEntryInput {
                    hash: "p".to_string(),
                    addrs: vec![vec![1]],
                }],
            ),
            encode_provider_subscribe(&key(), 1.0, 1.0),
            encode_provider_notify(
                &key(),
                &[ProviderEntryInput {
                    hash: "p".to_string(),
                    addrs: vec![vec![1]],
                }],
            ),
        ];
        // No decoder may panic on any prefix of a valid frame.
        for frame in &frames {
            for len in 0..frame.len() {
                let prefix = &frame[..len];
                let _ = decode_join_req(prefix);
                let _ = decode_join_response_req_id(prefix);
                let _ = decode_join_accept(prefix);
                let _ = decode_join_reject(prefix);
                let _ = decode_end(prefix);
                let _ = decode_repair_seqs(prefix);
                let _ = decode_ihave(prefix);
                let _ = decode_unicast(prefix);
                let _ = decode_unicast_ack(prefix);
                let _ = decode_route_query(prefix);
                let _ = decode_route_reply(prefix);
                let _ = decode_tracker_announce(prefix);
                let _ = decode_tracker_query(prefix);
                let _ = decode_tracker_reply(prefix);
                let _ = decode_tracker_feedback(prefix);
                let _ = decode_parent_probe_req(prefix);
                let _ = decode_parent_probe_reply(prefix);
                let _ = decode_provider_announce(prefix);
                let _ = decode_provider_query(prefix);
                let _ = decode_provider_reply(prefix);
                let _ = decode_provider_subscribe(prefix);
                let _ = decode_provider_notify(prefix);
            }
        }
    }
}
