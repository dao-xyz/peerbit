//! Wire codec for the Peerbit direct-stream envelope.
//!
//! Byte-identical to `packages/transport/stream-interface/src/messages.ts`:
//! top-level variants `DataMessage(0)` / `ACK(1)` / `Hello(2)` / `Goodbye(3)`,
//! the `MessageHeader` layout (32-byte id, u64 timestamp/session/expires,
//! optional u32 priority/responsePriority, optional origin multiaddrs,
//! optional delivery mode, optional multi-signature list) and all delivery
//! mode variants (`SilentDelivery(0)`, `AcknowledgeDelivery(1)`,
//! `TracedDelivery(3)`, `AnyWhere(4)`, `AcknowledgeAnyWhere(5)`; variant 2 is
//! an intentional gap left by the retired SeekDelivery).
//!
//! The signable-bytes rule mirrors `serializeUnsigned`/`getSignableBytes` in
//! the TS source: the header is re-emitted with the `mode` and `signatures`
//! option flags set to 0 (absent) while everything else — including trailing
//! message fields such as the DataMessage payload — is kept as-is. On a raw
//! frame this is equivalent to replacing the byte range
//! `[mode_flag_offset, signatures_end_offset)` with two zero bytes.
//!
//! This module is intentionally `JsValue`-free so it can be exercised by host
//! `cargo test` (constructing `JsValue`s aborts outside a JS runtime).

use ed25519_dalek::{verify_batch, Signature, Signer, SigningKey, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};

pub const ID_LENGTH: usize = 32;

pub const VARIANT_DATA: u8 = 0;
pub const VARIANT_ACK: u8 = 1;
pub const VARIANT_HELLO: u8 = 2;
pub const VARIANT_GOODBYE: u8 = 3;

// PreHash values from packages/utils/crypto/src/prehash.ts. Value 2 is a gap
// (reserved for BLAKE3), 3 is ETH_KECCAK_256 which this crate does not verify
// natively (reported as unsupported so the TS fallback handles it).
pub const PREHASH_NONE: u8 = 0;
pub const PREHASH_SHA_256: u8 = 1;

pub type WireResult<T> = Result<T, String>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicSignKey {
    /// variant 0, 32-byte key (packages/utils/crypto/src/ed25519.ts)
    Ed25519([u8; 32]),
    /// variant 1, 33-byte compressed key (packages/utils/crypto/src/sepc256k1.ts)
    Secp256k1([u8; 33]),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignatureWithKey {
    pub signature: Vec<u8>,
    pub public_key: PublicSignKey,
    pub prehash: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiAddrInfo {
    pub multiaddrs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryMode {
    /// variant 0
    Silent { to: Vec<String>, redundancy: u8 },
    /// variant 1
    Acknowledge {
        to: Vec<String>,
        redundancy: u8,
        hops: Vec<String>,
    },
    /// variant 3 (2 is a gap: the retired SeekDelivery)
    Traced { trace: Vec<String> },
    /// variant 4
    AnyWhere,
    /// variant 5
    AcknowledgeAnyWhere { redundancy: u8, hops: Vec<String> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    pub id: [u8; ID_LENGTH],
    pub timestamp: u64,
    pub session: u64,
    pub expires: u64,
    pub priority: Option<u32>,
    pub response_priority: Option<u32>,
    pub origin: Option<MultiAddrInfo>,
    pub mode: Option<DeliveryMode>,
    /// `Signatures` is a variant(0) wrapper around a u8-length-prefixed vec of
    /// `SignatureWithKey` (SIGNATURES_SIZE_ENCODING = "u8" in the TS source).
    pub signatures: Option<Vec<SignatureWithKey>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireMessage {
    Data {
        header: MessageHeader,
        data: Option<Vec<u8>>,
    },
    Ack {
        header: MessageHeader,
        message_id_to_acknowledge: [u8; ID_LENGTH],
        seen_counter: u8,
    },
    Hello {
        header: MessageHeader,
        joined: Vec<String>,
    },
    Goodbye {
        header: MessageHeader,
        leaving: Vec<String>,
    },
}

impl WireMessage {
    pub fn variant(&self) -> u8 {
        match self {
            WireMessage::Data { .. } => VARIANT_DATA,
            WireMessage::Ack { .. } => VARIANT_ACK,
            WireMessage::Hello { .. } => VARIANT_HELLO,
            WireMessage::Goodbye { .. } => VARIANT_GOODBYE,
        }
    }

    pub fn header(&self) -> &MessageHeader {
        match self {
            WireMessage::Data { header, .. } => header,
            WireMessage::Ack { header, .. } => header,
            WireMessage::Hello { header, .. } => header,
            WireMessage::Goodbye { header, .. } => header,
        }
    }

    pub fn header_mut(&mut self) -> &mut MessageHeader {
        match self {
            WireMessage::Data { header, .. } => header,
            WireMessage::Ack { header, .. } => header,
            WireMessage::Hello { header, .. } => header,
            WireMessage::Goodbye { header, .. } => header,
        }
    }
}

/// A decoded frame together with the raw-frame offsets the hot path and the
/// signable-bytes rule depend on. The offsets intentionally match the
/// hand-rolled readers in the TS source (`getDataMessageDataFlagOffset` &co).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedFrame {
    pub message: WireMessage,
    /// Offset of the option flag byte of `header.mode`.
    pub mode_flag_offset: usize,
    /// Offset just after the `header.signatures` option field ends. For a
    /// DataMessage this equals the data option flag offset returned by the TS
    /// `getDataMessageDataFlagOffset`.
    pub signatures_end_offset: usize,
    /// Absolute payload byte range for `Data` frames with a present payload.
    pub data_offset: usize,
    pub data_length: usize,
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pub(crate) offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Reader { bytes, offset: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    pub(crate) fn u8(&mut self) -> WireResult<u8> {
        if self.remaining() < 1 {
            return Err("unexpected end of frame reading u8".to_string());
        }
        let value = self.bytes[self.offset];
        self.offset += 1;
        Ok(value)
    }

    pub(crate) fn u32_le(&mut self) -> WireResult<u32> {
        if self.remaining() < 4 {
            return Err("unexpected end of frame reading u32".to_string());
        }
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&self.bytes[self.offset..self.offset + 4]);
        self.offset += 4;
        Ok(u32::from_le_bytes(buf))
    }

    pub(crate) fn u64_le(&mut self) -> WireResult<u64> {
        if self.remaining() < 8 {
            return Err("unexpected end of frame reading u64".to_string());
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.bytes[self.offset..self.offset + 8]);
        self.offset += 8;
        Ok(u64::from_le_bytes(buf))
    }

    pub(crate) fn take(&mut self, length: usize) -> WireResult<&'a [u8]> {
        if self.remaining() < length {
            return Err("unexpected end of frame reading bytes".to_string());
        }
        let slice = &self.bytes[self.offset..self.offset + length];
        self.offset += length;
        Ok(slice)
    }

    pub(crate) fn fixed_32(&mut self) -> WireResult<[u8; 32]> {
        let mut out = [0u8; 32];
        out.copy_from_slice(self.take(32)?);
        Ok(out)
    }

    pub(crate) fn string(&mut self) -> WireResult<String> {
        let length = self.u32_le()? as usize;
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| "invalid utf8 in string".to_string())
    }

    pub(crate) fn string_vec(&mut self) -> WireResult<Vec<String>> {
        let length = self.u32_le()? as usize;
        // Cheap sanity bound: every string needs at least its 4-byte length.
        if length > self.remaining() / 4 {
            return Err("string vec length exceeds frame".to_string());
        }
        let mut out = Vec::with_capacity(length);
        for _ in 0..length {
            out.push(self.string()?);
        }
        Ok(out)
    }
}

fn read_delivery_mode(reader: &mut Reader) -> WireResult<DeliveryMode> {
    let variant = reader.u8()?;
    match variant {
        0 => {
            let to = reader.string_vec()?;
            let redundancy = reader.u8()?;
            Ok(DeliveryMode::Silent { to, redundancy })
        }
        1 => {
            let to = reader.string_vec()?;
            let redundancy = reader.u8()?;
            let hops = reader.string_vec()?;
            Ok(DeliveryMode::Acknowledge {
                to,
                redundancy,
                hops,
            })
        }
        3 => {
            let trace = reader.string_vec()?;
            Ok(DeliveryMode::Traced { trace })
        }
        4 => Ok(DeliveryMode::AnyWhere),
        5 => {
            let redundancy = reader.u8()?;
            let hops = reader.string_vec()?;
            Ok(DeliveryMode::AcknowledgeAnyWhere { redundancy, hops })
        }
        other => Err(format!("unsupported delivery mode variant: {other}")),
    }
}

fn read_public_sign_key(reader: &mut Reader) -> WireResult<PublicSignKey> {
    let variant = reader.u8()?;
    match variant {
        0 => Ok(PublicSignKey::Ed25519(reader.fixed_32()?)),
        1 => {
            let mut key = [0u8; 33];
            key.copy_from_slice(reader.take(33)?);
            Ok(PublicSignKey::Secp256k1(key))
        }
        other => Err(format!("unsupported public sign key variant: {other}")),
    }
}

fn read_signature_with_key(reader: &mut Reader) -> WireResult<SignatureWithKey> {
    let variant = reader.u8()?;
    if variant != 0 {
        return Err(format!("unsupported signature variant: {variant}"));
    }
    let signature_length = reader.u32_le()? as usize;
    let signature = reader.take(signature_length)?.to_vec();
    let public_key = read_public_sign_key(reader)?;
    let prehash = reader.u8()?;
    Ok(SignatureWithKey {
        signature,
        public_key,
        prehash,
    })
}

fn read_signatures(reader: &mut Reader) -> WireResult<Vec<SignatureWithKey>> {
    let variant = reader.u8()?;
    if variant != 0 {
        return Err(format!("unsupported signatures variant: {variant}"));
    }
    // SIGNATURES_SIZE_ENCODING = "u8"
    let length = reader.u8()? as usize;
    let mut out = Vec::with_capacity(length);
    for _ in 0..length {
        out.push(read_signature_with_key(reader)?);
    }
    Ok(out)
}

struct HeaderWithOffsets {
    header: MessageHeader,
    mode_flag_offset: usize,
    signatures_end_offset: usize,
}

fn read_header(reader: &mut Reader) -> WireResult<HeaderWithOffsets> {
    let header_variant = reader.u8()?;
    if header_variant != 0 {
        return Err(format!(
            "unsupported message header variant: {header_variant}"
        ));
    }
    let id = reader.fixed_32()?;
    let timestamp = reader.u64_le()?;
    let session = reader.u64_le()?;
    let expires = reader.u64_le()?;
    let priority = if reader.u8()? == 1 {
        Some(reader.u32_le()?)
    } else {
        None
    };
    let response_priority = if reader.u8()? == 1 {
        Some(reader.u32_le()?)
    } else {
        None
    };
    let origin = if reader.u8()? == 1 {
        let peer_info_variant = reader.u8()?;
        if peer_info_variant != 0 {
            return Err(format!(
                "unsupported peer info variant: {peer_info_variant}"
            ));
        }
        Some(MultiAddrInfo {
            multiaddrs: reader.string_vec()?,
        })
    } else {
        None
    };
    let mode_flag_offset = reader.offset;
    let mode = if reader.u8()? == 1 {
        Some(read_delivery_mode(reader)?)
    } else {
        None
    };
    let signatures = if reader.u8()? == 1 {
        Some(read_signatures(reader)?)
    } else {
        None
    };
    let signatures_end_offset = reader.offset;
    Ok(HeaderWithOffsets {
        header: MessageHeader {
            id,
            timestamp,
            session,
            expires,
            priority,
            response_priority,
            origin,
            mode,
            signatures,
        },
        mode_flag_offset,
        signatures_end_offset,
    })
}

/// Decode a full frame. Consumption is strict (no trailing bytes) — the TS
/// borsh deserializer enforces this for ACK/Hello/Goodbye; `DataMessage.from`
/// technically tolerates trailing garbage after the payload, but such frames
/// simply fall back to the TS path when native decode rejects them.
pub fn decode_frame(bytes: &[u8]) -> WireResult<DecodedFrame> {
    let mut reader = Reader::new(bytes);
    let variant = reader.u8()?;
    let HeaderWithOffsets {
        header,
        mode_flag_offset,
        signatures_end_offset,
    } = read_header(&mut reader)?;
    let mut data_offset = 0usize;
    let mut data_length = 0usize;
    let message = match variant {
        VARIANT_DATA => {
            let data = if reader.u8()? == 1 {
                let length = reader.u32_le()? as usize;
                data_offset = reader.offset;
                data_length = length;
                Some(reader.take(length)?.to_vec())
            } else {
                None
            };
            WireMessage::Data { header, data }
        }
        VARIANT_ACK => {
            let message_id_to_acknowledge = reader.fixed_32()?;
            let seen_counter = reader.u8()?;
            WireMessage::Ack {
                header,
                message_id_to_acknowledge,
                seen_counter,
            }
        }
        VARIANT_HELLO => WireMessage::Hello {
            header,
            joined: reader.string_vec()?,
        },
        VARIANT_GOODBYE => WireMessage::Goodbye {
            header,
            leaving: reader.string_vec()?,
        },
        other => Err(format!("unsupported message variant: {other}"))?,
    };
    if reader.remaining() != 0 {
        return Err(format!(
            "unexpected {} trailing bytes after frame",
            reader.remaining()
        ));
    }
    Ok(DecodedFrame {
        message,
        mode_flag_offset,
        signatures_end_offset,
        data_offset,
        data_length,
    })
}

/// Frame metadata needed by receive-fusion stash decisions: the header id
/// (stash key) and the delivery mode (local-delivery check). Parsing stops at
/// the mode field, so this stays cheap enough to run per candidate frame on
/// top of the batched decode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameDeliveryMeta {
    pub variant: u8,
    pub id: [u8; ID_LENGTH],
    pub mode: Option<DeliveryMode>,
}

pub fn decode_frame_delivery_meta(frame: &[u8]) -> WireResult<FrameDeliveryMeta> {
    let mut reader = Reader::new(frame);
    let variant = reader.u8()?;
    let header_variant = reader.u8()?;
    if header_variant != 0 {
        return Err(format!(
            "unsupported message header variant: {header_variant}"
        ));
    }
    let id = reader.fixed_32()?;
    reader.u64_le()?; // timestamp
    reader.u64_le()?; // session
    reader.u64_le()?; // expires
    if reader.u8()? == 1 {
        reader.u32_le()?; // priority
    }
    if reader.u8()? == 1 {
        reader.u32_le()?; // responsePriority
    }
    if reader.u8()? == 1 {
        let peer_info_variant = reader.u8()?;
        if peer_info_variant != 0 {
            return Err(format!(
                "unsupported peer info variant: {peer_info_variant}"
            ));
        }
        reader.string_vec()?; // origin multiaddrs
    }
    let mode = if reader.u8()? == 1 {
        Some(read_delivery_mode(&mut reader)?)
    } else {
        None
    };
    Ok(FrameDeliveryMeta { variant, id, mode })
}

pub(crate) struct Writer {
    pub(crate) bytes: Vec<u8>,
}

impl Writer {
    pub(crate) fn new() -> Self {
        Writer { bytes: Vec::new() }
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(crate) fn u32_le(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn u64_le(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn raw(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub(crate) fn string(&mut self, value: &str) {
        self.u32_le(value.len() as u32);
        self.raw(value.as_bytes());
    }

    pub(crate) fn string_vec(&mut self, values: &[String]) {
        self.u32_le(values.len() as u32);
        for value in values {
            self.string(value);
        }
    }
}

fn write_delivery_mode(writer: &mut Writer, mode: &DeliveryMode) {
    match mode {
        DeliveryMode::Silent { to, redundancy } => {
            writer.u8(0);
            writer.string_vec(to);
            writer.u8(*redundancy);
        }
        DeliveryMode::Acknowledge {
            to,
            redundancy,
            hops,
        } => {
            writer.u8(1);
            writer.string_vec(to);
            writer.u8(*redundancy);
            writer.string_vec(hops);
        }
        DeliveryMode::Traced { trace } => {
            writer.u8(3);
            writer.string_vec(trace);
        }
        DeliveryMode::AnyWhere => {
            writer.u8(4);
        }
        DeliveryMode::AcknowledgeAnyWhere { redundancy, hops } => {
            writer.u8(5);
            writer.u8(*redundancy);
            writer.string_vec(hops);
        }
    }
}

fn write_signature_with_key(writer: &mut Writer, signature: &SignatureWithKey) {
    writer.u8(0); // SignatureWithKey variant
    writer.u32_le(signature.signature.len() as u32);
    writer.raw(&signature.signature);
    match &signature.public_key {
        PublicSignKey::Ed25519(key) => {
            writer.u8(0);
            writer.raw(key);
        }
        PublicSignKey::Secp256k1(key) => {
            writer.u8(1);
            writer.raw(key);
        }
    }
    writer.u8(signature.prehash);
}

fn write_header(
    writer: &mut Writer,
    header: &MessageHeader,
    include_mode: bool,
    include_signatures: bool,
) {
    writer.u8(0); // MessageHeader variant
    writer.raw(&header.id);
    writer.u64_le(header.timestamp);
    writer.u64_le(header.session);
    writer.u64_le(header.expires);
    match header.priority {
        Some(priority) => {
            writer.u8(1);
            writer.u32_le(priority);
        }
        None => writer.u8(0),
    }
    match header.response_priority {
        Some(priority) => {
            writer.u8(1);
            writer.u32_le(priority);
        }
        None => writer.u8(0),
    }
    match &header.origin {
        Some(origin) => {
            writer.u8(1);
            writer.u8(0); // MultiAddrinfo variant
            writer.string_vec(&origin.multiaddrs);
        }
        None => writer.u8(0),
    }
    match &header.mode {
        Some(mode) if include_mode => {
            writer.u8(1);
            write_delivery_mode(writer, mode);
        }
        _ => writer.u8(0),
    }
    match &header.signatures {
        Some(signatures) if include_signatures => {
            writer.u8(1);
            writer.u8(0); // Signatures variant
            writer.u8(signatures.len() as u8);
            for signature in signatures {
                write_signature_with_key(writer, signature);
            }
        }
        _ => writer.u8(0),
    }
}

fn encode_frame_with(message: &WireMessage, include_mode_and_signatures: bool) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(message.variant());
    write_header(
        &mut writer,
        message.header(),
        include_mode_and_signatures,
        include_mode_and_signatures,
    );
    match message {
        WireMessage::Data { data, .. } => match data {
            Some(data) => {
                writer.u8(1);
                writer.u32_le(data.len() as u32);
                writer.raw(data);
            }
            None => writer.u8(0),
        },
        WireMessage::Ack {
            message_id_to_acknowledge,
            seen_counter,
            ..
        } => {
            writer.raw(message_id_to_acknowledge);
            writer.u8(*seen_counter);
        }
        WireMessage::Hello { joined, .. } => writer.string_vec(joined),
        WireMessage::Goodbye { leaving, .. } => writer.string_vec(leaving),
    }
    writer.bytes
}

pub fn encode_frame(message: &WireMessage) -> Vec<u8> {
    encode_frame_with(message, true)
}

/// The signable-bytes rule: serialize with the `mode` and `signatures` option
/// flags forced to 0 (both fields are mutated in transit and excluded from
/// the signed range). Matches TS `serializeUnsigned` / `getSignableBytes`.
pub fn encode_signable(message: &WireMessage) -> Vec<u8> {
    encode_frame_with(message, false)
}

/// Signable bytes recovered from a raw frame without re-encoding the message:
/// everything before the mode option flag, two zero option flags, then
/// everything after the signatures field.
pub fn signable_bytes_from_frame(frame: &[u8], decoded: &DecodedFrame) -> Vec<u8> {
    let prefix = &frame[..decoded.mode_flag_offset];
    let suffix = &frame[decoded.signatures_end_offset..];
    let mut out = Vec::with_capacity(prefix.len() + 2 + suffix.len());
    out.extend_from_slice(prefix);
    out.push(0);
    out.push(0);
    out.extend_from_slice(suffix);
    out
}

pub fn signable_bytes(frame: &[u8]) -> WireResult<Vec<u8>> {
    let decoded = decode_frame(frame)?;
    Ok(signable_bytes_from_frame(frame, &decoded))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerifyStatus {
    /// Header expired, missing/empty signatures (`expectSignatures` hot-path
    /// semantics) or at least one signature failed verification.
    Failed = 0,
    Verified = 1,
    /// Contains a signature scheme this crate does not verify natively
    /// (secp256k1 key or a non sha256/none prehash); callers must fall back
    /// to the TS verification path.
    Unsupported = 2,
}

/// One record per input frame; the flat u32 encoding used across the wasm
/// boundary is defined in `lib.rs`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameRecord {
    pub decode_ok: bool,
    pub variant: u8,
    pub verify: VerifyStatus,
    pub signature_count: u8,
    pub priority: Option<u32>,
    pub has_data: bool,
    pub data_offset: u32,
    pub data_length: u32,
}

impl FrameRecord {
    fn decode_failed() -> Self {
        FrameRecord {
            decode_ok: false,
            variant: 0,
            verify: VerifyStatus::Failed,
            signature_count: 0,
            priority: None,
            has_data: false,
            data_offset: 0,
            data_length: 0,
        }
    }
}

struct PendingSignature {
    frame_index: usize,
    use_digest: bool,
    signature: Signature,
    key: VerifyingKey,
}

struct FrameSignableContext {
    signable: Vec<u8>,
    digest: Option<[u8; 32]>,
}

/// Decode every frame and verify signatures with the exact scheme
/// `@peerbit/crypto` uses for the direct-stream hot path: plain Ed25519 over
/// the (optionally sha256-prehashed) signable bytes. Ed25519 verifications
/// are batched via `ed25519_dalek::verify_batch`, falling back to individual
/// verification to attribute failures when the batch rejects.
///
/// `now_ms` feeds the header expiry check (`expires >= now`), mirroring
/// `MessageHeader.verify()`.
pub fn decode_and_verify_frames(frames: &[&[u8]], now_ms: u64) -> Vec<FrameRecord> {
    let mut records: Vec<FrameRecord> = Vec::with_capacity(frames.len());
    let mut contexts: Vec<Option<FrameSignableContext>> = Vec::with_capacity(frames.len());
    let mut pending: Vec<PendingSignature> = Vec::new();

    for (frame_index, frame) in frames.iter().enumerate() {
        let decoded = match decode_frame(frame) {
            Ok(decoded) => decoded,
            Err(_) => {
                records.push(FrameRecord::decode_failed());
                contexts.push(None);
                continue;
            }
        };
        let header = decoded.message.header();
        let signatures = header.signatures.as_deref().unwrap_or(&[]);
        let mut record = FrameRecord {
            decode_ok: true,
            variant: decoded.message.variant(),
            verify: VerifyStatus::Failed,
            signature_count: signatures.len().min(u8::MAX as usize) as u8,
            priority: header.priority,
            has_data: matches!(&decoded.message, WireMessage::Data { data: Some(_), .. }),
            data_offset: decoded.data_offset as u32,
            data_length: decoded.data_length as u32,
        };

        // MessageHeader.verify(): expires >= now.
        if header.expires < now_ms || signatures.is_empty() {
            records.push(record);
            contexts.push(None);
            continue;
        }

        let unsupported = signatures.iter().any(|signature| {
            !matches!(signature.public_key, PublicSignKey::Ed25519(_))
                || (signature.prehash != PREHASH_NONE && signature.prehash != PREHASH_SHA_256)
        });
        if unsupported {
            record.verify = VerifyStatus::Unsupported;
            records.push(record);
            contexts.push(None);
            continue;
        }

        let signable = signable_bytes_from_frame(frame, &decoded);
        let mut context = FrameSignableContext {
            signable,
            digest: None,
        };
        let mut malformed = false;
        let mut frame_pending: Vec<PendingSignature> = Vec::with_capacity(signatures.len());
        for signature in signatures {
            let key_bytes = match &signature.public_key {
                PublicSignKey::Ed25519(key) => key,
                PublicSignKey::Secp256k1(_) => unreachable!("filtered above"),
            };
            let signature_bytes: &[u8; 64] = match signature.signature.as_slice().try_into() {
                Ok(bytes) => bytes,
                Err(_) => {
                    malformed = true;
                    break;
                }
            };
            let key = match VerifyingKey::from_bytes(key_bytes) {
                Ok(key) => key,
                Err(_) => {
                    malformed = true;
                    break;
                }
            };
            let use_digest = signature.prehash == PREHASH_SHA_256;
            if use_digest && context.digest.is_none() {
                context.digest = Some(Sha256::digest(&context.signable).into());
            }
            frame_pending.push(PendingSignature {
                frame_index,
                use_digest,
                signature: Signature::from_bytes(signature_bytes),
                key,
            });
        }
        if malformed {
            // A malformed signature/key fails verification in TS too (sodium
            // rejects it); keep VerifyStatus::Failed.
            records.push(record);
            contexts.push(None);
            continue;
        }
        // Provisionally verified; individual failures flip it back below.
        record.verify = VerifyStatus::Verified;
        records.push(record);
        contexts.push(Some(context));
        pending.append(&mut frame_pending);
    }

    if pending.is_empty() {
        return records;
    }

    let messages: Vec<&[u8]> = pending
        .iter()
        .map(|entry| {
            let context = contexts[entry.frame_index]
                .as_ref()
                .expect("pending signature without context");
            if entry.use_digest {
                context.digest.as_ref().expect("missing digest").as_slice()
            } else {
                context.signable.as_slice()
            }
        })
        .collect();
    let signatures: Vec<Signature> = pending.iter().map(|entry| entry.signature).collect();
    let keys: Vec<VerifyingKey> = pending.iter().map(|entry| entry.key).collect();

    if verify_batch(&messages, &signatures, &keys).is_err() {
        // Attribute failures per signature.
        for (entry, message) in pending.iter().zip(messages.iter()) {
            if entry.key.verify(message, &entry.signature).is_err() {
                records[entry.frame_index].verify = VerifyStatus::Failed;
            }
        }
    }

    records
}

// --- Debug JSON (parity-test surface; hand-rolled to avoid serde) ----------

pub(crate) fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    out.push('"');
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out.push('"');
}

fn push_string_array(out: &mut String, values: &[String]) {
    out.push('[');
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_json_string(out, value);
    }
    out.push(']');
}

fn push_mode(out: &mut String, mode: &DeliveryMode) {
    match mode {
        DeliveryMode::Silent { to, redundancy } => {
            out.push_str("{\"type\":\"silent\",\"to\":");
            push_string_array(out, to);
            out.push_str(&format!(",\"redundancy\":{redundancy}}}"));
        }
        DeliveryMode::Acknowledge {
            to,
            redundancy,
            hops,
        } => {
            out.push_str("{\"type\":\"acknowledge\",\"to\":");
            push_string_array(out, to);
            out.push_str(&format!(",\"redundancy\":{redundancy},\"hops\":"));
            push_string_array(out, hops);
            out.push('}');
        }
        DeliveryMode::Traced { trace } => {
            out.push_str("{\"type\":\"traced\",\"trace\":");
            push_string_array(out, trace);
            out.push('}');
        }
        DeliveryMode::AnyWhere => {
            out.push_str("{\"type\":\"anyWhere\"}");
        }
        DeliveryMode::AcknowledgeAnyWhere { redundancy, hops } => {
            out.push_str(&format!(
                "{{\"type\":\"acknowledgeAnyWhere\",\"redundancy\":{redundancy},\"hops\":"
            ));
            push_string_array(out, hops);
            out.push('}');
        }
    }
}

fn push_header(out: &mut String, header: &MessageHeader) {
    out.push_str("{\"id\":");
    push_hex(out, &header.id);
    // u64s as decimal strings to avoid JS number precision issues.
    out.push_str(&format!(
        ",\"timestamp\":\"{}\",\"session\":\"{}\",\"expires\":\"{}\"",
        header.timestamp, header.session, header.expires
    ));
    match header.priority {
        Some(priority) => out.push_str(&format!(",\"priority\":{priority}")),
        None => out.push_str(",\"priority\":null"),
    }
    match header.response_priority {
        Some(priority) => out.push_str(&format!(",\"responsePriority\":{priority}")),
        None => out.push_str(",\"responsePriority\":null"),
    }
    match &header.origin {
        Some(origin) => {
            out.push_str(",\"origin\":");
            push_string_array(out, &origin.multiaddrs);
        }
        None => out.push_str(",\"origin\":null"),
    }
    match &header.mode {
        Some(mode) => {
            out.push_str(",\"mode\":");
            push_mode(out, mode);
        }
        None => out.push_str(",\"mode\":null"),
    }
    match &header.signatures {
        Some(signatures) => {
            out.push_str(",\"signatures\":[");
            for (i, signature) in signatures.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str("{\"signature\":");
                push_hex(out, &signature.signature);
                match &signature.public_key {
                    PublicSignKey::Ed25519(key) => {
                        out.push_str(",\"publicKeyType\":\"ed25519\",\"publicKey\":");
                        push_hex(out, key);
                    }
                    PublicSignKey::Secp256k1(key) => {
                        out.push_str(",\"publicKeyType\":\"secp256k1\",\"publicKey\":");
                        push_hex(out, key);
                    }
                }
                out.push_str(&format!(",\"prehash\":{}}}", signature.prehash));
            }
            out.push(']');
        }
        None => out.push_str(",\"signatures\":null"),
    }
    out.push('}');
}

/// Stable JSON rendering of a decoded frame used by the cross-implementation
/// parity tests (compared against the same rendering of the TS decode).
pub fn frame_to_debug_json(message: &WireMessage) -> String {
    let mut out = String::new();
    match message {
        WireMessage::Data { header, data } => {
            out.push_str("{\"type\":\"data\",\"header\":");
            push_header(&mut out, header);
            match data {
                Some(data) => {
                    out.push_str(",\"data\":");
                    push_hex(&mut out, data);
                }
                None => out.push_str(",\"data\":null"),
            }
            out.push('}');
        }
        WireMessage::Ack {
            header,
            message_id_to_acknowledge,
            seen_counter,
        } => {
            out.push_str("{\"type\":\"ack\",\"header\":");
            push_header(&mut out, header);
            out.push_str(",\"messageIdToAcknowledge\":");
            push_hex(&mut out, message_id_to_acknowledge);
            out.push_str(&format!(",\"seenCounter\":{seen_counter}}}"));
        }
        WireMessage::Hello { header, joined } => {
            out.push_str("{\"type\":\"hello\",\"header\":");
            push_header(&mut out, header);
            out.push_str(",\"joined\":");
            push_string_array(&mut out, joined);
            out.push('}');
        }
        WireMessage::Goodbye { header, leaving } => {
            out.push_str("{\"type\":\"goodbye\",\"header\":");
            push_header(&mut out, header);
            out.push_str(",\"leaving\":");
            push_string_array(&mut out, leaving);
            out.push('}');
        }
    }
    out
}

// --- Deterministic golden-vector corpus ------------------------------------

fn corpus_signing_key(seed_byte: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed_byte; 32])
}

fn corpus_header(
    tag: u8,
    mode: Option<DeliveryMode>,
    priority: Option<u32>,
    response_priority: Option<u32>,
    origin: Option<MultiAddrInfo>,
) -> MessageHeader {
    MessageHeader {
        id: [tag; ID_LENGTH],
        timestamp: 1_700_000_000_000 + tag as u64,
        session: 1_690_000_000_000 + tag as u64,
        // Far future so TS-side `verify()` (which uses wall-clock now) passes.
        expires: 4_102_444_800_000,
        priority,
        response_priority,
        origin,
        mode,
        signatures: Some(Vec::new()),
    }
}

fn sign_corpus_message(message: &mut WireMessage, signers: &[(&SigningKey, u8)]) {
    let signable = encode_signable(message);
    let mut signatures = Vec::with_capacity(signers.len());
    for (key, prehash) in signers {
        let signature = match *prehash {
            PREHASH_SHA_256 => {
                let digest: [u8; 32] = Sha256::digest(&signable).into();
                key.sign(&digest)
            }
            PREHASH_NONE => key.sign(&signable),
            other => panic!("unsupported corpus prehash {other}"),
        };
        signatures.push(SignatureWithKey {
            signature: signature.to_bytes().to_vec(),
            public_key: PublicSignKey::Ed25519(key.verifying_key().to_bytes()),
            prehash: *prehash,
        });
    }
    message.header_mut().signatures = Some(signatures);
}

/// Deterministic corpus of Rust-authored frames used for the reverse
/// (Rust encode → TS decode) golden-vector direction. The TS parity suite
/// mirrors the expected semantics per index; keep both in sync.
pub fn build_test_corpus() -> Vec<Vec<u8>> {
    let key_a = corpus_signing_key(42);
    let key_b = corpus_signing_key(43);
    let hash_a = "peer-a-hashcode".to_string();
    let hash_b = "peer-b-hashcode".to_string();

    let mut frames = Vec::new();

    // 0: DataMessage, SilentDelivery, small payload, one sha256 signature.
    let mut message = WireMessage::Data {
        header: corpus_header(
            0,
            Some(DeliveryMode::Silent {
                to: vec![hash_a.clone()],
                redundancy: 1,
            }),
            Some(0),
            None,
            None,
        ),
        data: Some(vec![1, 2, 3]),
    };
    sign_corpus_message(&mut message, &[(&key_a, PREHASH_SHA_256)]);
    frames.push(encode_frame(&message));

    // 1: DataMessage, AcknowledgeDelivery with hops, empty-but-present
    // payload, responsePriority set.
    let mut message = WireMessage::Data {
        header: corpus_header(
            1,
            Some(DeliveryMode::Acknowledge {
                to: vec![hash_a.clone(), hash_b.clone()],
                redundancy: 2,
                hops: vec![hash_b.clone()],
            }),
            Some(1),
            Some(3),
            None,
        ),
        data: Some(Vec::new()),
    };
    sign_corpus_message(&mut message, &[(&key_a, PREHASH_SHA_256)]);
    frames.push(encode_frame(&message));

    // 2: DataMessage, AnyWhere, no payload, unsigned (empty signature list
    // on the wire, exactly what the TS constructor emits before signing).
    let message = WireMessage::Data {
        header: corpus_header(2, Some(DeliveryMode::AnyWhere), Some(0), None, None),
        data: None,
    };
    frames.push(encode_frame(&message));

    // 3: ACK with TracedDelivery and origin multiaddrs.
    let mut message = WireMessage::Ack {
        header: corpus_header(
            3,
            Some(DeliveryMode::Traced {
                trace: vec![hash_a.clone(), hash_b.clone()],
            }),
            Some(3),
            None,
            Some(MultiAddrInfo {
                multiaddrs: vec![
                    "/ip4/127.0.0.1/tcp/4002".to_string(),
                    "/ip4/127.0.0.1/tcp/4003/ws".to_string(),
                ],
            }),
        ),
        message_id_to_acknowledge: [9u8; ID_LENGTH],
        seen_counter: 1,
    };
    sign_corpus_message(&mut message, &[(&key_b, PREHASH_SHA_256)]);
    frames.push(encode_frame(&message));

    // 4: Hello with two signatures mixing sha256 and none prehash.
    let mut message = WireMessage::Hello {
        header: corpus_header(
            4,
            Some(DeliveryMode::Silent {
                to: Vec::new(),
                redundancy: 1,
            }),
            Some(0),
            None,
            None,
        ),
        joined: vec![hash_a.clone()],
    };
    sign_corpus_message(
        &mut message,
        &[(&key_a, PREHASH_SHA_256), (&key_b, PREHASH_NONE)],
    );
    frames.push(encode_frame(&message));

    // 5: Goodbye, SilentDelivery.
    let mut message = WireMessage::Goodbye {
        header: corpus_header(
            5,
            Some(DeliveryMode::Silent {
                to: vec![hash_b.clone()],
                redundancy: 2,
            }),
            Some(0),
            None,
            None,
        ),
        leaving: vec![hash_a.clone()],
    };
    sign_corpus_message(&mut message, &[(&key_a, PREHASH_SHA_256)]);
    frames.push(encode_frame(&message));

    // 6: DataMessage, AcknowledgeAnyWhere, 4096-byte patterned payload.
    let mut message = WireMessage::Data {
        header: corpus_header(
            6,
            Some(DeliveryMode::AcknowledgeAnyWhere {
                redundancy: 2,
                hops: Vec::new(),
            }),
            Some(1),
            Some(3),
            None,
        ),
        data: Some((0..4096u32).map(|i| (i % 251) as u8).collect()),
    };
    sign_corpus_message(&mut message, &[(&key_b, PREHASH_SHA_256)]);
    frames.push(encode_frame(&message));

    frames
}

#[cfg(test)]
mod tests {
    use super::*;

    const CORPUS_VERIFY_NOW_MS: u64 = 1_700_000_000_500;

    #[test]
    fn corpus_roundtrips_byte_identically() {
        for (index, frame) in build_test_corpus().iter().enumerate() {
            let decoded = decode_frame(frame)
                .unwrap_or_else(|error| panic!("corpus frame {index} failed decode: {error}"));
            let reencoded = encode_frame(&decoded.message);
            assert_eq!(&reencoded, frame, "corpus frame {index} not byte-identical");
        }
    }

    #[test]
    fn corpus_offsets_match_signable_reencode() {
        // The offset-based signable slice must equal a full unsigned
        // re-serialization — this pins the layout assumptions that the TS
        // hand-rolled readers (getDataMessageDataFlagOffset) also make.
        for (index, frame) in build_test_corpus().iter().enumerate() {
            let decoded = decode_frame(frame).unwrap();
            assert_eq!(
                signable_bytes_from_frame(frame, &decoded),
                encode_signable(&decoded.message),
                "corpus frame {index} signable mismatch"
            );
        }
    }

    #[test]
    fn signable_bytes_exclude_mode_and_signatures() {
        let frames = build_test_corpus();
        let decoded = decode_frame(&frames[0]).unwrap();
        let signable = signable_bytes_from_frame(&frames[0], &decoded);

        // Mutating the delivery mode must not change the signable bytes.
        let mut mutated = decoded.message.clone();
        mutated.header_mut().mode = Some(DeliveryMode::Acknowledge {
            to: vec!["someone-else".to_string()],
            redundancy: 7,
            hops: vec!["hop".to_string()],
        });
        mutated.header_mut().signatures = Some(Vec::new());
        let mutated_frame = encode_frame(&mutated);
        let mutated_decoded = decode_frame(&mutated_frame).unwrap();
        assert_eq!(
            signable_bytes_from_frame(&mutated_frame, &mutated_decoded),
            signable
        );
    }

    #[test]
    fn corpus_verifies_and_tampering_fails() {
        let frames = build_test_corpus();
        let refs: Vec<&[u8]> = frames.iter().map(|frame| frame.as_slice()).collect();
        let records = decode_and_verify_frames(&refs, CORPUS_VERIFY_NOW_MS);
        assert_eq!(records.len(), frames.len());
        for (index, record) in records.iter().enumerate() {
            assert!(record.decode_ok, "frame {index} decode failed");
            if index == 2 {
                // unsigned frame: hot-path expectSignatures semantics
                assert_eq!(record.verify, VerifyStatus::Failed);
                assert_eq!(record.signature_count, 0);
            } else {
                assert_eq!(
                    record.verify,
                    VerifyStatus::Verified,
                    "frame {index} did not verify"
                );
            }
        }

        // Tamper with the signed payload of frame 0 → signature must fail,
        // while the other frames in the same batch stay verified.
        let mut tampered = frames[0].clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0xff;
        let refs: Vec<&[u8]> = std::iter::once(tampered.as_slice())
            .chain(frames.iter().skip(1).map(|frame| frame.as_slice()))
            .collect();
        let records = decode_and_verify_frames(&refs, CORPUS_VERIFY_NOW_MS);
        assert_eq!(records[0].verify, VerifyStatus::Failed);
        assert_eq!(records[1].verify, VerifyStatus::Verified);

        // Tamper with the signature itself.
        let mut decoded = decode_frame(&frames[0]).unwrap();
        if let Some(signatures) = &mut decoded.message.header_mut().signatures {
            signatures[0].signature[0] ^= 0xff;
        }
        let tampered_signature = encode_frame(&decoded.message);
        let records =
            decode_and_verify_frames(&[tampered_signature.as_slice()], CORPUS_VERIFY_NOW_MS);
        assert_eq!(records[0].verify, VerifyStatus::Failed);
    }

    #[test]
    fn tampering_unsigned_ranges_keeps_verification() {
        // Mode and signatures are excluded from the signable range, so
        // rewriting them (as relays do in transit) must keep verification.
        let frames = build_test_corpus();
        let decoded = decode_frame(&frames[0]).unwrap();
        let mut relayed = decoded.message.clone();
        relayed.header_mut().mode = Some(DeliveryMode::Silent {
            to: vec!["rewritten-by-relay".to_string()],
            redundancy: 3,
        });
        let relayed_frame = encode_frame(&relayed);
        let records = decode_and_verify_frames(&[relayed_frame.as_slice()], CORPUS_VERIFY_NOW_MS);
        assert_eq!(records[0].verify, VerifyStatus::Verified);
    }

    #[test]
    fn expired_header_fails_verification() {
        let frames = build_test_corpus();
        let records = decode_and_verify_frames(&[frames[0].as_slice()], u64::MAX);
        assert_eq!(records[0].verify, VerifyStatus::Failed);
    }

    #[test]
    fn secp256k1_signature_reports_unsupported() {
        let key = corpus_signing_key(1);
        let mut message = WireMessage::Data {
            header: corpus_header(7, Some(DeliveryMode::AnyWhere), Some(0), None, None),
            data: Some(vec![1]),
        };
        sign_corpus_message(&mut message, &[(&key, PREHASH_SHA_256)]);
        if let Some(signatures) = &mut message.header_mut().signatures {
            signatures.push(SignatureWithKey {
                signature: vec![0u8; 64],
                public_key: PublicSignKey::Secp256k1([2u8; 33]),
                prehash: PREHASH_SHA_256,
            });
        }
        let frame = encode_frame(&message);
        let records = decode_and_verify_frames(&[frame.as_slice()], CORPUS_VERIFY_NOW_MS);
        assert_eq!(records[0].verify, VerifyStatus::Unsupported);
        assert_eq!(records[0].signature_count, 2);
    }

    #[test]
    fn unknown_prehash_reports_unsupported() {
        let key = corpus_signing_key(1);
        let mut message = WireMessage::Data {
            header: corpus_header(8, Some(DeliveryMode::AnyWhere), Some(0), None, None),
            data: None,
        };
        sign_corpus_message(&mut message, &[(&key, PREHASH_SHA_256)]);
        if let Some(signatures) = &mut message.header_mut().signatures {
            signatures[0].prehash = 3; // ETH_KECCAK_256
        }
        let frame = encode_frame(&message);
        let records = decode_and_verify_frames(&[frame.as_slice()], CORPUS_VERIFY_NOW_MS);
        assert_eq!(records[0].verify, VerifyStatus::Unsupported);
    }

    #[test]
    fn delivery_mode_variant_gap_rejected() {
        // Variant 2 (retired SeekDelivery) must not decode.
        let frames = build_test_corpus();
        let decoded = decode_frame(&frames[0]).unwrap();
        let mut frame = frames[0].clone();
        frame[decoded.mode_flag_offset + 1] = 2;
        assert!(decode_frame(&frame).is_err());
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut frame = build_test_corpus()[0].clone();
        frame.push(0);
        assert!(decode_frame(&frame).is_err());
    }

    #[test]
    fn truncated_frames_rejected_without_panic() {
        let frame = &build_test_corpus()[0];
        for length in 0..frame.len() {
            assert!(
                decode_frame(&frame[..length]).is_err(),
                "truncated frame of length {length} unexpectedly decoded"
            );
        }
    }

    #[test]
    fn record_reports_payload_range() {
        let frames = build_test_corpus();
        let decoded = decode_frame(&frames[0]).unwrap();
        let records = decode_and_verify_frames(&[frames[0].as_slice()], CORPUS_VERIFY_NOW_MS);
        assert!(records[0].has_data);
        assert_eq!(records[0].data_offset as usize, decoded.data_offset);
        assert_eq!(records[0].data_length, 3);
        assert_eq!(
            &frames[0][decoded.data_offset..decoded.data_offset + decoded.data_length],
            &[1, 2, 3]
        );
        // Unsigned no-payload frame.
        let records = decode_and_verify_frames(&[frames[2].as_slice()], CORPUS_VERIFY_NOW_MS);
        assert!(!records[0].has_data);
        assert_eq!(records[0].data_length, 0);
    }

    #[test]
    fn priority_survives_decode() {
        let frames = build_test_corpus();
        let records = decode_and_verify_frames(
            &frames.iter().map(|f| f.as_slice()).collect::<Vec<_>>(),
            CORPUS_VERIFY_NOW_MS,
        );
        assert_eq!(records[0].priority, Some(0));
        assert_eq!(records[1].priority, Some(1));
        assert_eq!(records[3].priority, Some(3));
    }

    #[test]
    fn debug_json_is_parseable_shape() {
        // Smoke check of the hand-rolled JSON writer.
        let frames = build_test_corpus();
        for frame in &frames {
            let decoded = decode_frame(frame).unwrap();
            let json = frame_to_debug_json(&decoded.message);
            assert!(json.starts_with('{') && json.ends_with('}'));
            assert!(json.contains("\"header\""));
        }
    }
}
