//! wasm-bindgen surface for the `peerbit_wire` envelope codec.
//!
//! All decode/verify logic lives in the `JsValue`-free `wire` module so it
//! can run under host `cargo test`; this file only translates across the
//! wasm boundary.

pub mod wire;

use js_sys::{Array, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use wire::{FrameRecord, VerifyStatus};

/// Flat record layout returned by [`decode_and_verify_batch`]: 4 u32 words
/// per input frame. Mirrored by the TS glue in `src/index.ts` and by the
/// `NativeWire` consumer inside `@peerbit/stream`.
///
/// word 0, byte 0: flags — bit 0 = decode ok, bit 1 = payload present
/// word 0, byte 1: top-level message variant (0 data, 1 ack, 2 hello, 3 goodbye)
/// word 0, byte 2: verify status (0 failed, 1 verified, 2 unsupported → TS fallback)
/// word 0, byte 3: signature count (clamped to 255)
/// word 1: header priority, or 0xffff_ffff when absent
/// word 2: payload byte offset into the frame (data variant only)
/// word 3: payload byte length (data variant only)
pub const RECORD_WORDS: usize = 4;
pub const RECORD_FLAG_DECODE_OK: u32 = 0x01;
pub const RECORD_FLAG_HAS_DATA: u32 = 0x02;
pub const RECORD_NO_PRIORITY: u32 = u32::MAX;

fn record_to_words(record: &FrameRecord, out: &mut Vec<u32>) {
    let mut flags = 0u32;
    if record.decode_ok {
        flags |= RECORD_FLAG_DECODE_OK;
    }
    if record.has_data {
        flags |= RECORD_FLAG_HAS_DATA;
    }
    let verify = match record.verify {
        VerifyStatus::Failed => 0u32,
        VerifyStatus::Verified => 1u32,
        VerifyStatus::Unsupported => 2u32,
    };
    out.push(
        flags
            | ((record.variant as u32) << 8)
            | (verify << 16)
            | ((record.signature_count as u32) << 24),
    );
    out.push(record.priority.unwrap_or(RECORD_NO_PRIORITY));
    out.push(record.data_offset);
    out.push(record.data_length);
}

/// Decode a batch of direct-stream frames and verify their signatures
/// (sha256-prehashed Ed25519, batched via ed25519-dalek). Returns
/// [`RECORD_WORDS`] u32 words per input frame; see the layout above.
///
/// `now_ms` is the wall clock used for the header expiry check.
#[wasm_bindgen]
pub fn decode_and_verify_batch(frames: Array, now_ms: f64) -> Vec<u32> {
    let buffers: Vec<Option<Vec<u8>>> = frames
        .iter()
        .map(|value| {
            value
                .dyn_into::<Uint8Array>()
                .ok()
                .map(|array| array.to_vec())
        })
        .collect();
    let slices: Vec<&[u8]> = buffers
        .iter()
        .map(|buffer| buffer.as_deref().unwrap_or(&[]))
        .collect();
    let records = wire::decode_and_verify_frames(&slices, now_ms as u64);
    let mut words = Vec::with_capacity(records.len() * RECORD_WORDS);
    for record in &records {
        record_to_words(record, &mut words);
    }
    words
}

/// Decode a frame and re-encode it from the parsed representation. Used by
/// the golden-vector parity tests to prove Rust encoding is byte-identical
/// to the TS wire format.
#[wasm_bindgen]
pub fn reencode_frame(frame: &[u8]) -> Result<Vec<u8>, JsValue> {
    let decoded = wire::decode_frame(frame).map_err(|error| JsValue::from_str(&error))?;
    Ok(wire::encode_frame(&decoded.message))
}

/// Decode a frame into the stable debug-JSON shape used by the parity tests.
#[wasm_bindgen]
pub fn decode_frame_to_json(frame: &[u8]) -> Result<String, JsValue> {
    let decoded = wire::decode_frame(frame).map_err(|error| JsValue::from_str(&error))?;
    Ok(wire::frame_to_debug_json(&decoded.message))
}

/// The signable byte range of a frame: the serialized message with the
/// delivery mode and signatures excluded (both are mutated in transit).
/// Must match `Message.getSignableBytes()` in the TS implementation.
#[wasm_bindgen]
pub fn signable_bytes(frame: &[u8]) -> Result<Vec<u8>, JsValue> {
    wire::signable_bytes(frame).map_err(|error| JsValue::from_str(&error))
}

/// Deterministic Rust-authored golden vectors for the reverse parity
/// direction (Rust encode → TS decode). See `wire::build_test_corpus`.
#[wasm_bindgen]
pub fn test_corpus_frames() -> Array {
    let corpus = wire::build_test_corpus();
    let out = Array::new();
    for frame in corpus {
        out.push(&Uint8Array::from(frame.as_slice()));
    }
    out
}
