//! Recognizer for the shared-log raw exchange-heads sync payload carried in a
//! DataMessage payload. Byte-identical to the TS borsh nesting:
//!
//! - `PubSubData` (`packages/transport/pubsub-interface/src/messages.ts`):
//!   variant `0`, `topics: vec<string>`, `strict: bool`, `data: Uint8Array`.
//! - `RPCMessage`/`RequestV0` (`packages/programs/rpc/src/encoding.ts`):
//!   variants `[0, 0]`, `respondTo: option<X25519PublicKey>` (must be absent —
//!   shared-log sync uses fire-and-forget `rpc.send`), then `MaybeEncrypted`.
//! - `MaybeEncrypted`/`DecryptedThing` (`packages/utils/crypto/src/
//!   encryption.ts`): variants `[0, 0]`, `data: Uint8Array`. An
//!   `EncryptedThing` (variant 1) is never fused — the TS path decrypts it.
//! - `TransportMessage`/`RawExchangeHeadsMessage` (`packages/programs/data/
//!   shared-log/src/message.ts`, `exchange-heads.ts`): variants `[0] + [0, 7]`,
//!   `heads: vec<RawEntryWithRefs>` (variant `1`, `hash: string`,
//!   `bytes: Uint8Array`, `gidRefrences: vec<string>`), `reserved: [u8; 4]`.
//!
//! Anything that deviates from this exact shape is reported as "not a raw
//! exchange sync payload" so callers fall back to the TS decode path.
//!
//! `JsValue`-free so host `cargo test` can exercise it.

use crate::wire::{Reader, WireResult, Writer};

/// One head inside a raw exchange payload. `bytes_offset`/`bytes_length`
/// locate the raw entry block bytes relative to the start of the parsed
/// payload slice, so the block bytes never have to be copied to be addressed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncPayloadHead {
    pub hash: String,
    pub bytes_offset: usize,
    pub bytes_length: usize,
    pub gid_refrences: Vec<String>,
}

/// The `PubSubData` framing of a payload; parsed first so callers can check
/// topic registration before committing to the deeper (more expensive) parse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PubSubDataRef {
    pub topics: Vec<String>,
    pub strict: bool,
    /// Range of `PubSubData.data` relative to the payload slice.
    pub data_offset: usize,
    pub data_length: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawExchangeSyncPayload {
    pub heads: Vec<SyncPayloadHead>,
    pub reserved: [u8; 4],
}

pub fn parse_pubsub_data(payload: &[u8]) -> WireResult<PubSubDataRef> {
    let mut reader = Reader::new(payload);
    let variant = reader.u8()?;
    if variant != 0 {
        return Err(format!("not a PubSubData payload: variant {variant}"));
    }
    let topics = reader.string_vec()?;
    let strict = match reader.u8()? {
        0 => false,
        1 => true,
        other => return Err(format!("invalid PubSubData strict flag: {other}")),
    };
    let data_length = reader.u32_le()? as usize;
    let data_offset = reader.offset;
    reader.take(data_length)?;
    if reader.remaining() != 0 {
        return Err(format!(
            "unexpected {} trailing bytes after PubSubData",
            reader.remaining()
        ));
    }
    Ok(PubSubDataRef {
        topics,
        strict,
        data_offset,
        data_length,
    })
}

/// Parse `PubSubData.data` (an RPC `RequestV0` wrapping a plaintext
/// `RawExchangeHeadsMessage`). Head byte offsets are relative to `data` —
/// callers add the enclosing offsets to address into the original frame.
pub fn parse_raw_exchange_rpc_request(data: &[u8]) -> WireResult<RawExchangeSyncPayload> {
    let mut reader = Reader::new(data);
    let rpc_variant = reader.u8()?;
    if rpc_variant != 0 {
        return Err(format!("not an RPCMessage: variant {rpc_variant}"));
    }
    let request_variant = reader.u8()?;
    if request_variant != 0 {
        return Err(format!("not a RequestV0: variant {request_variant}"));
    }
    let respond_to = reader.u8()?;
    if respond_to != 0 {
        return Err("RequestV0 with respondTo is not a sync send".to_string());
    }
    let maybe_encrypted_variant = reader.u8()?;
    if maybe_encrypted_variant != 0 {
        return Err(format!(
            "unsupported MaybeEncrypted variant: {maybe_encrypted_variant}"
        ));
    }
    let decrypted_variant = reader.u8()?;
    if decrypted_variant != 0 {
        return Err("encrypted sync payloads are not fused".to_string());
    }
    let inner_length = reader.u32_le()? as usize;
    let inner_offset = reader.offset;
    reader.take(inner_length)?;
    if reader.remaining() != 0 {
        return Err(format!(
            "unexpected {} trailing bytes after RequestV0",
            reader.remaining()
        ));
    }

    let inner = &data[inner_offset..inner_offset + inner_length];
    let mut reader = Reader::new(inner);
    let transport_variant = reader.u8()?;
    if transport_variant != 0 {
        return Err(format!(
            "unsupported TransportMessage variant: {transport_variant}"
        ));
    }
    let sub_variant = [reader.u8()?, reader.u8()?];
    if sub_variant != [0, 7] {
        return Err(format!(
            "not a RawExchangeHeadsMessage: variant {sub_variant:?}"
        ));
    }
    let head_count = reader.u32_le()? as usize;
    // Cheap sanity bound: every head needs at least its variant byte plus
    // three u32 length prefixes.
    if head_count > inner.len() / 13 {
        return Err("head count exceeds payload".to_string());
    }
    let mut heads = Vec::with_capacity(head_count);
    for _ in 0..head_count {
        let head_variant = reader.u8()?;
        if head_variant != 1 {
            return Err(format!(
                "unsupported RawEntryWithRefs variant: {head_variant}"
            ));
        }
        let hash = reader.string()?;
        let bytes_length = reader.u32_le()? as usize;
        let bytes_offset = inner_offset + reader.offset;
        reader.take(bytes_length)?;
        let gid_refrences = reader.string_vec()?;
        heads.push(SyncPayloadHead {
            hash,
            bytes_offset,
            bytes_length,
            gid_refrences,
        });
    }
    let mut reserved = [0u8; 4];
    reserved.copy_from_slice(reader.take(4)?);
    if reader.remaining() != 0 {
        return Err(format!(
            "unexpected {} trailing bytes after RawExchangeHeadsMessage",
            reader.remaining()
        ));
    }
    Ok(RawExchangeSyncPayload { heads, reserved })
}

/// One head of an outbound raw exchange sync payload, borrowed from wherever
/// the caller keeps the entry block bytes (e.g. a native block store), so the
/// fused send path never copies block bytes to address them.
#[derive(Clone, Copy, Debug)]
pub struct SyncPayloadHeadRef<'a> {
    pub hash: &'a str,
    pub bytes: &'a [u8],
    pub gid_refrences: &'a [String],
}

/// Encoded size of the inner `TransportMessage`/`RawExchangeHeadsMessage`.
fn transport_message_len(heads: &[SyncPayloadHeadRef<'_>]) -> usize {
    // variants [0] + [0, 7], head count, reserved
    let mut len = 3 + 4 + 4;
    for head in heads {
        // RawEntryWithRefs variant + hash + bytes + gidRefrences
        len += 1 + 4 + head.hash.len() + 4 + head.bytes.len() + 4;
        for gid in head.gid_refrences {
            len += 4 + gid.len();
        }
    }
    len
}

/// Encoded size of the full payload produced by
/// [`encode_raw_exchange_sync_payload_refs`].
pub fn encoded_raw_exchange_sync_payload_len(
    topics: &[String],
    heads: &[SyncPayloadHeadRef<'_>],
) -> usize {
    // PubSubData variant + topics + strict + data length prefix
    let mut len = 1 + 4 + 1 + 4;
    for topic in topics {
        len += 4 + topic.len();
    }
    // RPCMessage/RequestV0/respondTo/MaybeEncrypted/DecryptedThing variants +
    // inner length prefix
    len += 5 + 4;
    len + transport_message_len(heads)
}

/// Encode the full outbound payload nesting (PubSubData → RequestV0 →
/// DecryptedThing → RawExchangeHeadsMessage) in one pass into a single
/// exactly-sized buffer. Byte-identical to the TS serialization (pinned by the
/// golden corpus below); this is the fused send-path encoder (plan Section 3,
/// integration point 2).
pub fn encode_raw_exchange_sync_payload_refs(
    topics: &[String],
    strict: bool,
    heads: &[SyncPayloadHeadRef<'_>],
    reserved: [u8; 4],
) -> Vec<u8> {
    let total_len = encoded_raw_exchange_sync_payload_len(topics, heads);
    let transport_len = transport_message_len(heads);
    let mut payload = Writer::new();
    payload.bytes.reserve_exact(total_len);
    payload.u8(0); // PubSubData variant
    payload.string_vec(topics);
    payload.u8(u8::from(strict));
    payload.u32_le((5 + 4 + transport_len) as u32); // PubSubData.data length
    payload.u8(0); // RPCMessage variant
    payload.u8(0); // RequestV0 variant
    payload.u8(0); // respondTo: None
    payload.u8(0); // MaybeEncrypted variant
    payload.u8(0); // DecryptedThing variant
    payload.u32_le(transport_len as u32); // DecryptedThing.data length
    payload.u8(0); // TransportMessage variant
    payload.u8(0); // RawExchangeHeadsMessage variant [0, 7]
    payload.u8(7);
    payload.u32_le(heads.len() as u32);
    for head in heads {
        payload.u8(1); // RawEntryWithRefs variant
        payload.string(head.hash);
        payload.u32_le(head.bytes.len() as u32);
        payload.raw(head.bytes);
        payload.string_vec(head.gid_refrences);
    }
    payload.raw(&reserved);
    debug_assert_eq!(payload.bytes.len(), total_len);
    payload.bytes
}

/// Encode the full payload nesting from owned heads (test/corpus helper and
/// the golden pin for [`encode_raw_exchange_sync_payload_refs`]).
pub fn encode_raw_exchange_sync_payload(
    topics: &[String],
    strict: bool,
    heads: &[(String, Vec<u8>, Vec<String>)],
    reserved: [u8; 4],
) -> Vec<u8> {
    let head_refs: Vec<SyncPayloadHeadRef<'_>> = heads
        .iter()
        .map(|(hash, bytes, gid_refrences)| SyncPayloadHeadRef {
            hash,
            bytes,
            gid_refrences,
        })
        .collect();
    encode_raw_exchange_sync_payload_refs(topics, strict, &head_refs, reserved)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn corpus_heads() -> Vec<(String, Vec<u8>, Vec<String>)> {
        vec![
            (
                "zb2AA".to_string(),
                vec![0xde, 0xad, 0xbe, 0xef],
                vec!["g1".to_string(), "g2".to_string()],
            ),
            ("zb2BB".to_string(), vec![0x01, 0x02], Vec::new()),
        ]
    }

    /// Serialized by the TS classes (PubSubData → RequestV0 → DecryptedThing →
    /// RawExchangeHeadsMessage) with the same content as `corpus_heads()`,
    /// topics=["topicA"], strict=true, reserved=[1,0,0,0]. Pins the encoder
    /// and parser to the real TS layout.
    const TS_GOLDEN_PAYLOAD: [u8; 94] = [
        0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x41,
        0x01, 0x4a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x07, 0x02, 0x00, 0x00, 0x00, 0x01, 0x05, 0x00, 0x00, 0x00, 0x7a, 0x62, 0x32, 0x41,
        0x41, 0x04, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x00, 0x00, 0x67, 0x31, 0x02, 0x00, 0x00, 0x00, 0x67, 0x32, 0x01, 0x05, 0x00, 0x00, 0x00,
        0x7a, 0x62, 0x32, 0x42, 0x42, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00,
    ];

    #[test]
    fn encoder_matches_ts_golden_payload() {
        let encoded = encode_raw_exchange_sync_payload(
            &["topicA".to_string()],
            true,
            &corpus_heads(),
            [1, 0, 0, 0],
        );
        assert_eq!(encoded.as_slice(), TS_GOLDEN_PAYLOAD.as_slice());
    }

    #[test]
    fn parses_ts_golden_payload() {
        let pubsub = parse_pubsub_data(&TS_GOLDEN_PAYLOAD).unwrap();
        assert_eq!(pubsub.topics, vec!["topicA".to_string()]);
        assert!(pubsub.strict);
        let data = &TS_GOLDEN_PAYLOAD[pubsub.data_offset..pubsub.data_offset + pubsub.data_length];
        let parsed = parse_raw_exchange_rpc_request(data).unwrap();
        assert_eq!(parsed.reserved, [1, 0, 0, 0]);
        assert_eq!(parsed.heads.len(), 2);
        assert_eq!(parsed.heads[0].hash, "zb2AA");
        assert_eq!(
            parsed.heads[0].gid_refrences,
            vec!["g1".to_string(), "g2".to_string()]
        );
        let head0 = &data[parsed.heads[0].bytes_offset
            ..parsed.heads[0].bytes_offset + parsed.heads[0].bytes_length];
        assert_eq!(head0, &[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(parsed.heads[1].hash, "zb2BB");
        assert!(parsed.heads[1].gid_refrences.is_empty());
        let head1 = &data[parsed.heads[1].bytes_offset
            ..parsed.heads[1].bytes_offset + parsed.heads[1].bytes_length];
        assert_eq!(head1, &[0x01, 0x02]);
    }

    #[test]
    fn round_trips_generated_payloads() {
        let heads: Vec<(String, Vec<u8>, Vec<String>)> = (0..17)
            .map(|index| {
                (
                    format!("hash-{index}"),
                    (0..(index * 13 + 1)).map(|byte| byte as u8).collect(),
                    (0..(index % 3)).map(|gid| format!("gid-{gid}")).collect(),
                )
            })
            .collect();
        let payload = encode_raw_exchange_sync_payload(
            &["a".to_string(), "b".to_string()],
            false,
            &heads,
            [0, 0, 0, 0],
        );
        let pubsub = parse_pubsub_data(&payload).unwrap();
        assert_eq!(pubsub.topics, vec!["a".to_string(), "b".to_string()]);
        assert!(!pubsub.strict);
        let data = &payload[pubsub.data_offset..pubsub.data_offset + pubsub.data_length];
        let parsed = parse_raw_exchange_rpc_request(data).unwrap();
        assert_eq!(parsed.heads.len(), heads.len());
        for (parsed_head, (hash, bytes, gid_refrences)) in parsed.heads.iter().zip(&heads) {
            assert_eq!(&parsed_head.hash, hash);
            assert_eq!(&parsed_head.gid_refrences, gid_refrences);
            assert_eq!(
                &data
                    [parsed_head.bytes_offset..parsed_head.bytes_offset + parsed_head.bytes_length],
                bytes.as_slice()
            );
        }
    }

    #[test]
    fn encoded_len_matches_encoder_output() {
        for (topics, strict, heads, reserved) in [
            (
                vec!["topicA".to_string()],
                true,
                corpus_heads(),
                [1, 0, 0, 0],
            ),
            (
                vec!["a".to_string(), "b".to_string()],
                false,
                Vec::new(),
                [0, 0, 0, 0],
            ),
            (
                vec![String::new()],
                true,
                vec![(String::new(), Vec::new(), vec![String::new()])],
                [255, 254, 253, 252],
            ),
        ] {
            let head_refs: Vec<SyncPayloadHeadRef<'_>> = heads
                .iter()
                .map(|(hash, bytes, gid_refrences)| SyncPayloadHeadRef {
                    hash,
                    bytes,
                    gid_refrences,
                })
                .collect();
            let encoded =
                encode_raw_exchange_sync_payload_refs(&topics, strict, &head_refs, reserved);
            assert_eq!(
                encoded.len(),
                encoded_raw_exchange_sync_payload_len(&topics, &head_refs)
            );
            // The single-pass encoder parses back to the same content.
            let pubsub = parse_pubsub_data(&encoded).unwrap();
            assert_eq!(pubsub.topics, topics);
            assert_eq!(pubsub.strict, strict);
            let data = &encoded[pubsub.data_offset..pubsub.data_offset + pubsub.data_length];
            let parsed = parse_raw_exchange_rpc_request(data).unwrap();
            assert_eq!(parsed.reserved, reserved);
            assert_eq!(parsed.heads.len(), heads.len());
            for (parsed_head, (hash, bytes, gid_refrences)) in parsed.heads.iter().zip(&heads) {
                assert_eq!(&parsed_head.hash, hash);
                assert_eq!(&parsed_head.gid_refrences, gid_refrences);
                assert_eq!(
                    &data[parsed_head.bytes_offset
                        ..parsed_head.bytes_offset + parsed_head.bytes_length],
                    bytes.as_slice()
                );
            }
        }
    }

    #[test]
    fn rejects_foreign_payloads() {
        let payload = encode_raw_exchange_sync_payload(
            &["t".to_string()],
            true,
            &corpus_heads(),
            [0, 0, 0, 0],
        );
        let pubsub = parse_pubsub_data(&payload).unwrap();
        let data_start = pubsub.data_offset;

        // Not PubSubData.
        let mut wrong = payload.clone();
        wrong[0] = 1;
        assert!(parse_pubsub_data(&wrong).is_err());

        // respondTo present → not a fire-and-forget sync send.
        let mut wrong = payload.clone();
        wrong[data_start + 2] = 1;
        let data = &wrong[data_start..];
        assert!(parse_raw_exchange_rpc_request(data).is_err());

        // EncryptedThing.
        let mut wrong = payload.clone();
        wrong[data_start + 4] = 1;
        assert!(parse_raw_exchange_rpc_request(&wrong[data_start..]).is_err());

        // A different TransportMessage variant ([0, 0] ExchangeHeadsMessage).
        let mut wrong = payload.clone();
        // transport bytes begin after the 5 variant bytes + u32 length
        wrong[data_start + 5 + 4 + 2] = 0;
        assert!(parse_raw_exchange_rpc_request(&wrong[data_start..]).is_err());

        // Truncations never panic.
        for length in 0..payload.len() {
            let slice = &payload[..length];
            if let Ok(pubsub) = parse_pubsub_data(slice) {
                let data = &slice[pubsub.data_offset..pubsub.data_offset + pubsub.data_length];
                assert!(parse_raw_exchange_rpc_request(data).is_err());
            }
        }
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut payload = encode_raw_exchange_sync_payload(
            &["t".to_string()],
            true,
            &corpus_heads(),
            [0, 0, 0, 0],
        );
        payload.push(0);
        assert!(parse_pubsub_data(&payload).is_err());
    }
}
