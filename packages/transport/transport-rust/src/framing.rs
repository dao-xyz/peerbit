//! Native it-length-prefixed unsigned-varint framing over the Borsh envelope.
//!
//! Each `/peerbit/*` stream carries a sequence of frames. On the wire a frame
//! is exactly what js-libp2p's `it-length-prefixed` + `lp.encode`/`lp.decode`
//! produce (`stream/src/index.ts:468,808`):
//!
//! ```text
//! [ unsigned-varint length ] [ Borsh envelope of length `len` ]
//! ```
//!
//! and the Borsh envelope is precisely `peerbit_wire::wire::encode_frame`
//! output — a top-level variant tag (`DataMessage=0` / `ACK=1` / `Hello=2` /
//! `Goodbye=3`) followed by the header + body, Ed25519/SHA-256 signable. This
//! module does **not** re-implement the envelope codec: it calls
//! [`peerbit_wire::wire`] directly on slices that came off the socket into Rust
//! memory. That is the whole point of the native transport — the per-frame
//! `array.to_vec()` ingress copy the wasm engine pays (ARCHITECTURE.md
//! exception 2) never happens here; the codec reads socket bytes in place.
//!
//! Length caps mirror the TS source exactly (`stream/src/index.ts:245-246`):
//! inbound `MAX_DATA_LENGTH_IN = 15_000_000 + 1000`, outbound
//! `MAX_DATA_LENGTH_OUT = 10_000_000 + 1000`. A frame whose declared length
//! exceeds the inbound cap is rejected before a single body byte is read.

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use peerbit_wire::wire::{
    decode_and_verify_frames, decode_frame, encode_frame, DecodedFrame, FrameRecord, WireMessage,
};

/// Inbound frame length cap. Matches `MAX_DATA_LENGTH_IN` (15 MB + metadata).
pub const MAX_DATA_LENGTH_IN: usize = 15_000_000 + 1000;

/// Outbound frame length cap. Matches `MAX_DATA_LENGTH_OUT` (10 MB + metadata).
pub const MAX_DATA_LENGTH_OUT: usize = 10_000_000 + 1000;

/// Framing / codec errors surfaced over a `/peerbit/*` stream.
#[derive(Debug)]
pub enum FramingError {
    /// The declared inbound length exceeds [`MAX_DATA_LENGTH_IN`].
    InboundTooLarge { declared: usize },
    /// The outbound envelope exceeds [`MAX_DATA_LENGTH_OUT`].
    OutboundTooLarge { len: usize },
    /// The varint length prefix was malformed (e.g. > 10 continuation bytes).
    MalformedLengthPrefix,
    /// The Borsh envelope failed to decode via `peerbit_wire`.
    Codec(String),
    /// Underlying stream I/O error.
    Io(std::io::Error),
}

impl std::fmt::Display for FramingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FramingError::InboundTooLarge { declared } => write!(
                f,
                "inbound frame length {declared} exceeds MAX_DATA_LENGTH_IN {MAX_DATA_LENGTH_IN}"
            ),
            FramingError::OutboundTooLarge { len } => write!(
                f,
                "outbound frame length {len} exceeds MAX_DATA_LENGTH_OUT {MAX_DATA_LENGTH_OUT}"
            ),
            FramingError::MalformedLengthPrefix => {
                write!(f, "malformed unsigned-varint length prefix")
            }
            FramingError::Codec(message) => write!(f, "peerbit_wire codec error: {message}"),
            FramingError::Io(error) => write!(f, "stream io error: {error}"),
        }
    }
}

impl std::error::Error for FramingError {}

impl From<std::io::Error> for FramingError {
    fn from(error: std::io::Error) -> Self {
        FramingError::Io(error)
    }
}

/// Encode an unsigned-varint length prefix for `len` into `out`.
fn write_uvarint(out: &mut Vec<u8>, mut len: usize) {
    loop {
        let mut byte = (len & 0x7f) as u8;
        len >>= 7;
        if len != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

/// The it-length-prefixed unsigned-varint frame codec over a Peerbit envelope.
///
/// Stateless: framing is a pure function of the bytes. The one shared piece of
/// state a real transport keeps — the outbound ordering — lives in the
/// [`peerbit_wire::direct_stream::lanes::LaneScheduler`], not here.
pub struct FrameCodec;

impl FrameCodec {
    /// Serialize a [`WireMessage`] into a length-prefixed on-wire frame.
    ///
    /// The envelope bytes come straight from `peerbit_wire::encode_frame` — no
    /// re-implementation, byte-identical to what the js fleet emits. Rejects
    /// envelopes over the outbound cap before framing.
    pub fn encode(message: &WireMessage) -> Result<Vec<u8>, FramingError> {
        let envelope = encode_frame(message);
        if envelope.len() > MAX_DATA_LENGTH_OUT {
            return Err(FramingError::OutboundTooLarge {
                len: envelope.len(),
            });
        }
        let mut out = Vec::with_capacity(envelope.len() + 5);
        write_uvarint(&mut out, envelope.len());
        out.extend_from_slice(&envelope);
        Ok(out)
    }

    /// Frame an already-encoded envelope (bytes produced elsewhere by
    /// `peerbit_wire`) with its length prefix. Used on the fused-send path
    /// where the chunk store already holds the encoded bytes.
    pub fn frame_envelope(envelope: &[u8]) -> Result<Vec<u8>, FramingError> {
        if envelope.len() > MAX_DATA_LENGTH_OUT {
            return Err(FramingError::OutboundTooLarge {
                len: envelope.len(),
            });
        }
        let mut out = Vec::with_capacity(envelope.len() + 5);
        write_uvarint(&mut out, envelope.len());
        out.extend_from_slice(envelope);
        Ok(out)
    }

    /// Decode one length-prefixed frame's envelope into a [`DecodedFrame`] via
    /// `peerbit_wire`. Operates on a caller-owned slice, so this is exactly the
    /// "decode socket bytes in place" path (no `array.to_vec()`).
    pub fn decode_envelope(envelope: &[u8]) -> Result<DecodedFrame, FramingError> {
        decode_frame(envelope).map_err(FramingError::Codec)
    }

    /// Decode + Ed25519/SHA-256 verify a batch of envelope slices via
    /// `peerbit_wire::decode_and_verify_frames`. `frames` are borrowed socket
    /// slices; the native transport passes them straight through with no copy —
    /// the ingress-copy elimination the design targets.
    pub fn decode_and_verify(frames: &[&[u8]], now_ms: u64) -> Vec<FrameRecord> {
        decode_and_verify_frames(frames, now_ms)
    }
}

/// Read exactly one length-prefixed frame's **envelope bytes** off an async
/// stream. Enforces [`MAX_DATA_LENGTH_IN`] on the declared length before
/// allocating the body. Returns the raw envelope; the caller then hands it to
/// [`FrameCodec::decode_envelope`] / [`FrameCodec::decode_and_verify`].
pub async fn read_frame<S>(stream: &mut S) -> Result<Vec<u8>, FramingError>
where
    S: AsyncRead + Unpin,
{
    let mut len: usize = 0;
    let mut shift: u32 = 0;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        len |= ((byte[0] & 0x7f) as usize) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
        // unsigned-varint over a usize length: guard runaway prefixes.
        if shift >= 63 {
            return Err(FramingError::MalformedLengthPrefix);
        }
    }
    if len > MAX_DATA_LENGTH_IN {
        return Err(FramingError::InboundTooLarge { declared: len });
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

/// Write one framed [`WireMessage`] to an async stream and flush it.
pub async fn write_frame<S>(stream: &mut S, message: &WireMessage) -> Result<(), FramingError>
where
    S: AsyncWrite + Unpin,
{
    let framed = FrameCodec::encode(message)?;
    stream.write_all(&framed).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use peerbit_wire::wire::build_test_corpus;

    #[test]
    fn uvarint_matches_reference_encoding() {
        // Cross-check against the same hand-rolled encoder the js listener and
        // the spike use (small, boundary, and multi-byte values).
        for value in [0usize, 1, 127, 128, 255, 300, 16383, 16384, 1_000_000] {
            let mut out = Vec::new();
            write_uvarint(&mut out, value);
            // Reference: unsigned_varint crate.
            let mut buf = unsigned_varint::encode::usize_buffer();
            let reference = unsigned_varint::encode::usize(value, &mut buf);
            assert_eq!(out.as_slice(), reference, "uvarint mismatch for {value}");
        }
    }

    #[test]
    fn encode_then_read_frame_roundtrips_every_corpus_message() {
        // Reuse the frozen peerbit_wire corpus: for each canonical envelope,
        // frame it and read it back through the async reader, asserting the
        // envelope survives byte-identically and re-decodes.
        let corpus = build_test_corpus();
        for (index, envelope) in corpus.iter().enumerate() {
            let framed = FrameCodec::frame_envelope(envelope).expect("frame envelope");

            // Read the length prefix + body back with the async reader.
            let mut cursor = futures::io::Cursor::new(framed.clone());
            let read_back = futures::executor::block_on(read_frame(&mut cursor))
                .unwrap_or_else(|error| panic!("corpus {index} read_frame failed: {error}"));
            assert_eq!(&read_back, envelope, "corpus {index} envelope mismatch");

            // And it must still decode via peerbit_wire.
            FrameCodec::decode_envelope(&read_back)
                .unwrap_or_else(|error| panic!("corpus {index} decode failed: {error}"));
        }
    }

    #[test]
    fn inbound_cap_rejected_before_body_read() {
        // A varint declaring > MAX_DATA_LENGTH_IN must be rejected without the
        // body being present at all.
        let mut framed = Vec::new();
        write_uvarint(&mut framed, MAX_DATA_LENGTH_IN + 1);
        let mut cursor = futures::io::Cursor::new(framed);
        let result = futures::executor::block_on(read_frame(&mut cursor));
        assert!(matches!(result, Err(FramingError::InboundTooLarge { .. })));
    }

    #[test]
    fn outbound_cap_rejected() {
        // frame_envelope must refuse an oversized envelope.
        let too_big = vec![0u8; MAX_DATA_LENGTH_OUT + 1];
        assert!(matches!(
            FrameCodec::frame_envelope(&too_big),
            Err(FramingError::OutboundTooLarge { .. })
        ));
    }

    #[test]
    fn decode_and_verify_matches_wire_directly() {
        // The framing wrapper must be a pass-through to peerbit_wire's verifier.
        let corpus = build_test_corpus();
        let refs: Vec<&[u8]> = corpus.iter().map(|frame| frame.as_slice()).collect();
        let now = 1_700_000_000_500u64;
        let via_framing = FrameCodec::decode_and_verify(&refs, now);
        let direct = decode_and_verify_frames(&refs, now);
        assert_eq!(via_framing, direct);
    }
}
