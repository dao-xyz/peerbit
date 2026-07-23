//! DATA-PLANE SLICE PROOF (single native node, one process, no JS/wasm).
//!
//! Runs the layer directly above the network engine: a native node is handed a
//! received `RawExchangeHeads` sync payload carrying ONE real signed EntryV0,
//! recognizes it, ingests+verifies it (CID match + Ed25519 author signature),
//! commits it to the native block store + graph + document index, and then a
//! native INDEX QUERY returns it. Every call is a native rlib in this binary —
//! `peerbit_wire` (recognizer), `peerbit_log_rust` (ingest/verify/blocks/graph),
//! `peerbit_indexer_core` (document index). No `#[wasm_bindgen]` shim runs, no
//! js-sys typed array is touched, no napi, no sidecar.
//!
//! Exit code 0 = PASS. Run: `cargo run --bin data_plane_demo`.

use anyhow::{bail, Result};

use peerbit_node_spike::data_plane::{
    build_signed_entry_v0_storage, build_sync_payload, cid_of_storage, FixtureEntry,
    NativeDataPlane,
};

/// A deterministic author seed so the run is reproducible.
const AUTHOR_SEED: [u8; 32] = [42u8; 32];

fn main() -> Result<()> {
    // --- Build a real signed EntryV0 the way a peer would produce one. --------
    // (In the stretch, node A builds this and sends it over the transport; here
    // we build it in-process and hand it to the data plane as a received frame.)
    let gid = "log-gid-demo";
    let payload_data = b"a real signed entry, committed to a fully native data plane";
    let next: Vec<String> = Vec::new();
    let fixture = FixtureEntry {
        seed: AUTHOR_SEED,
        clock_id: b"data-plane-demo-clock",
        wall_time: 1_700_000_000_000,
        logical: 0,
        gid,
        next: &next,
        entry_type: 0,
        meta_data: None,
        payload_data,
    };
    let storage = build_signed_entry_v0_storage(&fixture);
    // Ask the native primitive for the block's CID (also self-verifies the sig).
    let cid = cid_of_storage(&storage)?;

    // Wrap it as the exact RawExchangeHeads PubSubData payload a peer sends.
    let sync_payload = build_sync_payload(
        &["spike/data-plane".to_string()],
        &[(cid.clone(), storage.clone(), vec![gid.to_string()])],
    );

    // --- Hand the received payload to the native data plane. ------------------
    let mut plane = NativeDataPlane::new();
    let committed = plane.recognize_and_commit(&sync_payload)?;

    // --- Print the proof and assert every leg held. --------------------------
    println!("\n=== DATA-PLANE SLICE PROOF: native log append + index commit ===");
    println!("recognizer : peerbit_wire::sync_payload   (PubSubData -> RawExchangeHeads)");
    println!("ingest     : peerbit_log_rust             (raw CIDv1 + Ed25519 author verify)");
    println!("blocks     : peerbit_log_rust             (NativeLogBlockStore)");
    println!("graph      : peerbit_log_rust             (LogGraphIndex heads)");
    println!("index      : peerbit_indexer_core         (NativeQueryIndex document rows)");
    println!("boundary   : NONE  (no #[wasm_bindgen], no js-sys, no napi, no sidecar)\n");

    if committed.len() != 1 {
        bail!(
            "expected exactly one committed entry, got {}",
            committed.len()
        );
    }
    let entry = &committed[0];
    println!("RECEIVED   -> sync payload: {} bytes", sync_payload.len());
    println!("INGEST     -> cid={}", entry.cid);
    println!(
        "INGEST     -> gid={} wall_time={} logical={}",
        entry.gid, entry.wall_time, entry.logical
    );
    println!(
        "INGEST     -> payload_size={} sig_verified={}",
        entry.payload_size, entry.signature_verified
    );

    if entry.cid != cid {
        bail!("committed cid {} != expected cid {}", entry.cid, cid);
    }
    if !entry.signature_verified {
        bail!("entry was committed without a verified author signature");
    }

    // Block store durably holds the raw entry bytes.
    if !plane.has_block(&cid) {
        bail!("block store does not hold the committed entry");
    }
    let stored = plane
        .blocks
        .get_ref(&cid)
        .map(<[u8]>::to_vec)
        .unwrap_or_default();
    if stored != storage {
        bail!("stored block bytes differ from the received storage block");
    }
    println!(
        "BLOCKS     -> has(cid)=true, bytes match received block ({} bytes)",
        storage.len()
    );

    // Graph heads contain the new entry.
    let heads = plane.heads(Some(gid));
    if !heads.contains(&cid) {
        bail!("graph heads (gid {gid}) do not contain the committed entry");
    }
    println!("GRAPH      -> heads(gid={gid}) contains cid  (heads={heads:?})");

    // THE QUERY: the native document index returns the entry for its gid.
    let hits = plane.cids_for_gid(gid);
    let count = plane.count_for_gid(gid);
    if hits != vec![cid.clone()] {
        bail!("index query by gid returned {hits:?}, expected [{cid}]");
    }
    println!("INDEX      -> search(Exact GID={gid}) = {hits:?}");
    println!("INDEX      -> count(Exact GID={gid}) = {count}");

    // A gid never committed returns nothing (the query is real, not a stamp).
    if !plane.cids_for_gid("absent-gid").is_empty() {
        bail!("index returned rows for a gid that was never committed");
    }

    println!("\n=== DATA-PLANE SLICE PROOF: PASS ===");
    println!("A received, signed entry was recognized, CID-checked, Ed25519-verified,");
    println!("appended to a native log (block store + graph) and committed to a native");
    println!("document index — then a native index query returned it. One process, no JS,");
    println!("no wasm boundary anywhere on the path.");
    Ok(())
}
