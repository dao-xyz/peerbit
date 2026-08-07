//! PATH B of MEASUREMENT 2 — the native (zero-copy) receive path.
//!
//! This is the receive cost a NATIVE rust-libp2p transport pays: frames arrive
//! in Rust-owned memory off the socket and the codec decodes + Ed25519
//! batch-verifies them IN PLACE via `peerbit_wire::decode_and_verify_frames`
//! on borrowed `&[&[u8]]` slices — NO `array.to_vec()` ingress copy, NO JS
//! pump, NO wasm boundary (ARCHITECTURE.md exception 2 removed).
//!
//! To make the A/B a true apples-to-apples comparison, this reads the EXACT
//! corpus the JS harness (`benchmark/wire-receive-profile.ts`, Path A) wrote:
//! the same signed `DataMessage` frames, byte-identical, at each payload size.
//! It times the same decode+verify core the JS side calls into — the only
//! differences from Path A are the ones a native transport actually removes
//! (the copy, the JS pump, the wasm engine). The delta is the total native
//! benefit; Measurement 1 attributes how much of it is the copy alone.
//!
//! We deliberately do NOT drive real sockets here: interposing TCP/noise/yamux
//! would measure the network, not the receive-codec cost we are isolating (and
//! the socket path is identical byte-work for both transports). The corpus is
//! held as owned `Vec<u8>` buffers and decoded through borrowed slices — the
//! same zero-copy shape `FrameCodec::decode_and_verify` uses on a real
//! socket-delivered buffer (the "decode socket bytes in place" contract in
//! `framing.rs`). The `read_frame` async reader is covered by the crate's
//! framing tests; here we isolate the codec CPU cost.
//!
//! Run (never concurrently with any build, test, or the JS harness):
//!   cargo run --release --bin wire_receive_native -- --corpus <dir> [--out <file>]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use peerbit_transport::framing::FrameCodec;
use peerbit_wire::wire::VerifyStatus;

const WARMUP_RUNS: usize = 3;
const MEASURED_RUNS: usize = 8;
// Must match the JS harness batch size so the per-call granularity is equal.
const DEFAULT_BATCH: usize = 64;
// Payload sizes must match `PAYLOAD_SIZES` in wire-receive-profile.ts.
const PAYLOAD_SIZES: [usize; 4] = [32, 1024, 16 * 1024, 64 * 1024];

/// Read a corpus file written by the JS harness:
/// `[u32 LE frame_count][ (u32 LE len)(len bytes) ]*`.
fn read_corpus(path: &str) -> std::io::Result<Vec<Vec<u8>>> {
    let bytes = std::fs::read(path)?;
    let mut off = 0usize;
    let read_u32 =
        |b: &[u8], o: usize| -> u32 { u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]) };
    let count = read_u32(&bytes, off) as usize;
    off += 4;
    let mut frames = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(&bytes, off) as usize;
        off += 4;
        frames.push(bytes[off..off + len].to_vec());
        off += len;
    }
    Ok(frames)
}

struct Stats {
    mean: f64,
    stdev: f64,
    min: f64,
}

fn summarize(runs: &[f64]) -> Stats {
    let mean = runs.iter().sum::<f64>() / runs.len() as f64;
    let var = if runs.len() > 1 {
        runs.iter().map(|r| (r - mean) * (r - mean)).sum::<f64>() / (runs.len() as f64 - 1.0)
    } else {
        0.0
    };
    Stats {
        mean,
        stdev: var.sqrt(),
        min: runs.iter().cloned().fold(f64::INFINITY, f64::min),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn arg_of(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn main() {
    let corpus_dir = arg_of("--corpus").unwrap_or_else(|| {
        eprintln!("usage: wire_receive_native --corpus <dir> [--out <file>]");
        std::process::exit(2);
    });
    let batch: usize = arg_of("--batch")
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BATCH);
    let now = now_ms();

    let mut sink: u64 = 0;
    let mut json_rows: Vec<String> = Vec::new();

    eprintln!("=== Path B (native zero-copy receive) ===");
    for payload in PAYLOAD_SIZES {
        let path = format!("{corpus_dir}/corpus-{payload}.bin");
        let frames = match read_corpus(&path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("skip payload {payload}: cannot read {path}: {e}");
                continue;
            }
        };
        let message_count = frames.len();
        // Borrow every frame as a slice — this is the zero-copy input a native
        // transport hands the codec straight off the socket buffer.
        let slices: Vec<&[u8]> = frames.iter().map(|f| f.as_slice()).collect();

        // Correctness gate: every frame must decode + verify (VerifyStatus::Verified)
        // exactly like Path A asserts. If not, the corpus/scheme drifted and the
        // A/B would be meaningless.
        {
            let records = FrameCodec::decode_and_verify(&slices, now);
            let verified = records
                .iter()
                .filter(|r| r.verify == VerifyStatus::Verified)
                .count();
            assert_eq!(
                verified, message_count,
                "payload {payload}: only {verified}/{message_count} frames verified — corpus/scheme drift"
            );
        }

        let run_once = || -> u64 {
            let mut acc: u64 = 0;
            let mut o = 0usize;
            while o < slices.len() {
                let end = (o + batch).min(slices.len());
                let records = FrameCodec::decode_and_verify(&slices[o..end], now);
                // touch the result like the real consumer (it reads records)
                for r in &records {
                    acc = acc.wrapping_add(r.data_length as u64);
                    if r.verify == VerifyStatus::Verified {
                        acc ^= 1;
                    }
                }
                o = end;
            }
            acc
        };

        for _ in 0..WARMUP_RUNS {
            sink ^= run_once();
        }
        let mut runs = Vec::with_capacity(MEASURED_RUNS);
        for _ in 0..MEASURED_RUNS {
            let start = Instant::now();
            sink ^= run_once();
            runs.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let s = summarize(&runs);
        let frames_per_sec = (message_count as f64 / s.mean) * 1000.0;
        let us_per_frame = (s.mean * 1000.0) / message_count as f64;

        eprintln!(
            "payload {payload:>6}B  {:.2}±{:.2}ms  {:>12} frames/s  {:.3} us/frame",
            s.mean,
            s.stdev,
            format!("{:.0}", frames_per_sec),
            us_per_frame
        );

        let runs_json = runs
            .iter()
            .map(|r| format!("{r:.4}"))
            .collect::<Vec<_>>()
            .join(",");
        json_rows.push(format!(
            "    {{\"payload\":{payload},\"messages\":{message_count},\"batch\":{batch},\
\"pathBMeanMs\":{:.4},\"pathBStdevMs\":{:.4},\"pathBMinMs\":{:.4},\
\"pathBFramesPerSec\":{:.2},\"pathBUsPerFrame\":{:.4},\"runs\":[{runs_json}]}}",
            s.mean, s.stdev, s.min, frames_per_sec, us_per_frame
        ));
    }

    let json = format!(
        "{{\n  \"kind\": \"wire-receive-native\",\n  \"runtime\": \"native rust (release)\",\n  \"batch\": {batch},\n  \"sinkGuard\": {sink},\n  \"results\": [\n{}\n  ]\n}}\n",
        json_rows.join(",\n")
    );

    if let Some(out) = arg_of("--out") {
        std::fs::write(&out, &json).expect("write out file");
        eprintln!("wrote {out}");
    } else {
        print!("{json}");
    }
}
