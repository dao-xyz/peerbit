//! DATA-PLANE SLICE — a native node takes a received, verified entry and
//! APPENDS it to a native log + COMMITS it to a native index, in one process,
//! with NO JS, NO wasm, NO napi.
//!
//! This is the layer directly ABOVE the network engine the spike already runs.
//! Where [`crate::NativeReceiveEngine`] ends is a native-decoded, Ed25519-
//! verified `PubSubData` payload (`outcome.pubsub`). This module owns the ~40
//! lines of native glue that carry that payload the rest of the way — the
//! native equivalent of native-backbone's `raw_receive` + `append_tx`, minus
//! the deferred orchestration (leader/replication coordinates, journal/flush,
//! trim, head-demotion, batch dedup, Document projection — see the plan).
//!
//! ```text
//!  verified PubSubData payload  (from NativeReceiveEngine.process_inbound_frame)
//!    │
//!    ▼  peerbit_wire::sync_payload::parse_pubsub_data + parse_raw_exchange_rpc_request
//!  Vec<SyncPayloadHead>  — each addresses a raw EntryV0 block by (offset,len)
//!    │
//!    ▼  peerbit_log_rust::prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled
//!  Vec<PreparedRawEntryV0>  — CID re-checked == head.hash, inner Ed25519 sig verified
//!    │
//!    ▼  for each prepared entry (native commit):
//!  NativeLogBlockStore.put(cid, storage_bytes)          [durable block layer]
//!  LogGraphIndex.put(prepared.log_index_entry(true))    [heads/graph]
//!  NativeQueryIndex.put(cid, DocumentFields{hash,gid,…}) [document index]
//!    │
//!    ▼  queryable natively:
//!  NativeQueryIndex.search(Query::Exact{GID}) -> [cid]  ;  LogGraphIndex.heads() ∋ cid
//! ```
//!
//! Every call on that path is a native rlib in this binary. The two primitive
//! crates (`peerbit_log_rust`, `peerbit_indexer_core`) are reused as-is — this
//! module does NOT re-implement them.

use anyhow::{anyhow, bail, Context, Result};

use peerbit_indexer_core::planner::{
    DocumentFields, FieldPath, FieldValue, NativeQueryIndex, Query, SortField,
};
use peerbit_log_rust::{
    prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled, LogGraphIndex,
    NativeLogBlockStore, PreparedRawEntryV0,
};
use peerbit_wire::sync_payload::{parse_pubsub_data, parse_raw_exchange_rpc_request};

/// Stable field ids for the raw document-index row committed per entry. The
/// slice indexes a small handful of scalar facts keyed by the entry CID — enough
/// to prove "received entry is indexed and queryable" without a full Document
/// projection (schema IR, deferred). Field ids are arbitrary but fixed.
pub mod field {
    /// The entry CID (the row's own hash) — String.
    pub const HASH: u32 = 0;
    /// The entry's graph id — String. The shared-log query key.
    pub const GID: u32 = 1;
    /// Lamport wall time — U64.
    pub const WALL_TIME: u32 = 2;
    /// Lamport logical counter — U64.
    pub const LOGICAL: u32 = 3;
    /// Whether the entry is a head at commit time — Bool.
    pub const HEAD: u32 = 4;
    /// The decoded payload byte length — U64.
    pub const PAYLOAD_SIZE: u32 = 5;
}

/// The native data-plane state a node owns for one log: the durable block store,
/// the head/graph index, and the document index. All three are in-memory for the
/// slice (block store = `HashMap`, graph = indexmap, index = indexmap+roaring);
/// nothing touches disk. This is the native equivalent of the per-log stores
/// native-backbone keeps behind its JS-woven journal.
pub struct NativeDataPlane {
    pub blocks: NativeLogBlockStore,
    pub graph: LogGraphIndex,
    pub index: NativeQueryIndex,
}

impl Default for NativeDataPlane {
    fn default() -> Self {
        Self {
            // NativeLogBlockStore is a #[wasm_bindgen] type whose ::new() is a
            // pure native constructor (HashMap::new + zeroed size) — no js_sys
            // is touched, so it is safe to call natively.
            blocks: NativeLogBlockStore::new(),
            graph: LogGraphIndex::new(),
            index: NativeQueryIndex::new(),
        }
    }
}

/// A single entry that landed in the native data plane, returned so callers can
/// assert on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedEntry {
    pub cid: String,
    pub gid: String,
    pub wall_time: u64,
    pub logical: u32,
    pub payload_size: usize,
    pub signature_verified: bool,
}

impl NativeDataPlane {
    pub fn new() -> Self {
        Self::default()
    }

    /// STEP 2+3+ of the plan: recognize the RawExchangeHeads sync payload, slice
    /// each head's raw entry block by offset, INGEST+VERIFY it with the native
    /// log primitive (CID match + Ed25519 author-signature), and COMMIT each
    /// verified entry into the block store + graph + document index.
    ///
    /// `payload` is the exact bytes of `outcome.pubsub.payload` the network
    /// engine already decoded and whose envelope it already verified. This runs
    /// the *inner* EntryV0 author-signature verification (a second, independent
    /// native Ed25519 check) — so a committed entry is verified end to end.
    ///
    /// Returns one [`CommittedEntry`] per head, in payload order. Errors if the
    /// payload is not a raw-exchange sync payload, a block fails to parse, its
    /// CID does not match the advertised head hash, or its signature fails to
    /// verify — nothing is committed in those cases (the whole batch prepares
    /// before any commit, mirroring native-backbone's prepare-then-commit split).
    pub fn recognize_and_commit(&mut self, payload: &[u8]) -> Result<Vec<CommittedEntry>> {
        // --- STEP 2: recognize the sync payload and address each head block. ---
        // parse_pubsub_data gives topics + the (offset,len) of PubSubData.data;
        // parse_raw_exchange_rpc_request walks RequestV0 → DecryptedThing →
        // RawExchangeHeadsMessage and yields each head's block bytes range,
        // relative to `data`. Both are golden-pinned in peerbit_wire.
        let pubsub =
            parse_pubsub_data(payload).map_err(|e| anyhow!("not a PubSubData payload: {e}"))?;
        let data = &payload[pubsub.data_offset..pubsub.data_offset + pubsub.data_length];
        let exchange = parse_raw_exchange_rpc_request(data)
            .map_err(|e| anyhow!("not a raw exchange-heads sync payload: {e}"))?;

        if exchange.heads.is_empty() {
            return Ok(Vec::new());
        }

        // Slice each head's raw EntryV0 block bytes out of `data` (a borrow into
        // the received frame — the only copy is the Vec the ingest primitive
        // owns) and collect the advertised head hashes as the expected CIDs.
        let mut blocks: Vec<Vec<u8>> = Vec::with_capacity(exchange.heads.len());
        let mut expected_cids: Vec<String> = Vec::with_capacity(exchange.heads.len());
        for head in &exchange.heads {
            let end = head
                .bytes_offset
                .checked_add(head.bytes_length)
                .context("head block byte range overflow")?;
            let block = data
                .get(head.bytes_offset..end)
                .context("head block byte range out of bounds")?;
            blocks.push(block.to_vec());
            expected_cids.push(head.hash.clone());
        }

        // --- STEP 3: ingest + verify with the native log primitive. -----------
        // The primitive computes each block's raw CIDv1, parses its EntryV0
        // storage, and batch-verifies the inner Ed25519 author signature.
        //
        // NOTE on the error surface (a native-hardening gap, not a slice bug):
        // `peerbit_log_rust`'s public functions return `Result<_, JsValue>`, and
        // building a `JsValue` calls a wasm-bindgen intrinsic that ABORTS on a
        // non-wasm target. So a block that fails to PARSE, or a CID passed as
        // `expected` that mismatches, would abort the process rather than return
        // a catchable error. A durable native node needs the primitive to grow a
        // native `Result<_, LogError>` (deferred). For the slice we stay on the
        // Ok path: pass `expected_cids = None` so the primitive COMPUTES the CID
        // (never the mismatch-abort branch), then re-check `cid == head.hash` and
        // `signature_verified` in plain Rust — both catchable, both native. The
        // remaining abort surface is a block whose borsh framing is malformed;
        // blocks here arrive inside an envelope the network engine already
        // Ed25519-verified, i.e. from an authenticated peer.
        let _ = &expected_cids; // head hashes are re-checked below, not passed in
        let prepared: Vec<PreparedRawEntryV0> =
            prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled(
                blocks, /* expected_cids */ None, /* verify_signatures */ true, None,
            )
            .map_err(|_| anyhow!("native raw-entry ingest failed (malformed storage block)"))?;

        // Reject the whole batch BEFORE any commit if any entry's author
        // signature did not verify, or its computed CID does not match the
        // advertised head hash (prepare-then-commit — no half-applied batch).
        for (entry, head) in prepared.iter().zip(&exchange.heads) {
            if entry.cid != head.hash {
                bail!(
                    "computed CID {} does not match advertised head hash {}",
                    entry.cid,
                    head.hash
                );
            }
            if !entry.signature_verified {
                bail!(
                    "entry {} failed native Ed25519 author-signature verification",
                    entry.cid
                );
            }
        }

        // --- STEP 4: commit each verified entry natively. ---------------------
        let mut committed = Vec::with_capacity(prepared.len());
        for entry in &prepared {
            committed.push(self.commit_prepared(entry)?);
        }
        Ok(committed)
    }

    /// Commit ONE verified prepared entry into the three native stores. This is
    /// the native core of raw_receive+append: block persisted, graph/heads
    /// updated, document row indexed. Unconditional (the slice skips leader /
    /// replica selection — deferred).
    fn commit_prepared(&mut self, entry: &PreparedRawEntryV0) -> Result<CommittedEntry> {
        // (a) durable block layer: persist the raw entry storage bytes by CID.
        self.blocks
            .put(entry.cid.clone(), entry.storage_bytes.clone());

        // (b) head/graph index: the exact prepared-entry → LogIndexEntry bridge
        //     the primitive ships, committed as a head.
        let index_entry = entry
            .log_index_entry(/* head */ true)
            .map_err(|_| anyhow!("log_index_entry conversion failed for {}", entry.cid))?;
        self.graph.put(index_entry);

        // (c) document index: a raw row keyed by CID with the scalar facts. A
        //     full Document projection via schema IR is deferred; these scalars
        //     are enough to prove the entry is indexed and queryable by GID.
        let fields = DocumentFields::new()
            .with_scalar(field::HASH, entry.cid.as_str())
            .with_scalar(field::GID, entry.gid.as_str())
            .with_scalar(field::WALL_TIME, entry.wall_time)
            .with_scalar(field::LOGICAL, entry.logical as u64)
            .with_scalar(field::HEAD, true)
            .with_scalar(field::PAYLOAD_SIZE, entry.payload_byte_length as u64);
        self.index.put(entry.cid.clone(), fields);

        Ok(CommittedEntry {
            cid: entry.cid.clone(),
            gid: entry.gid.clone(),
            wall_time: entry.wall_time,
            logical: entry.logical,
            payload_size: entry.payload_byte_length,
            signature_verified: entry.signature_verified,
        })
    }

    /// Query the native document index for every committed CID with the given
    /// gid — the "is it queryable?" proof. Returns entry CIDs.
    pub fn cids_for_gid(&self, gid: &str) -> Vec<String> {
        let query = Query::Exact {
            field: FieldPath::Id(field::GID),
            value: FieldValue::from(gid),
        };
        let sort: [SortField; 0] = [];
        self.index.search(&query, &sort, None)
    }

    /// Count of document rows matching a gid (native index count).
    pub fn count_for_gid(&self, gid: &str) -> u64 {
        let query = Query::Exact {
            field: FieldPath::Id(field::GID),
            value: FieldValue::from(gid),
        };
        self.index.count(&query)
    }

    /// The native graph heads (optionally scoped to a gid).
    pub fn heads(&self, gid: Option<&str>) -> Vec<String> {
        self.graph.heads(gid)
    }

    /// Does the block store durably hold the raw bytes for this CID?
    pub fn has_block(&self, cid: &str) -> bool {
        self.blocks.has(cid)
    }
}

// ---------------------------------------------------------------------------
// FIXTURE: a real signed EntryV0 storage block, built natively.
//
// The log kernel's own signer (`NativeEntryV0PlainBuilder`, the `*_with_builder`
// methods, the private `encode_*`/core helpers) is only reachable through a
// `#[wasm_bindgen]` constructor that takes `js_sys::Uint8Array` — constructing
// one calls into a JS runtime that does not exist natively. The design
// explicitly permits "a fixture block" instead. So we re-encode the EntryV0
// storage layout here with the SAME borsh framing the kernel's private
// `encode_meta_parts` / `encode_payload` / `signable_entry_to_signed_storage`
// use, and sign it with ed25519-dalek (already a spike dep, same crate+version
// the kernel signs with). This produces only *input*; the load-bearing code —
// CID computation, storage parse, and Ed25519 verify — is the real native
// `peerbit_log_rust` primitive, exercised by `recognize_and_commit`.
//
// Layout (mirrors packages/log/rust/src/lib.rs, verified against
// parse_plain_entry_v0_storage):
//   EntryV0 = [0]                                            (entry variant)
//             DecryptedThing(meta)                           ([0][0][u32 len][meta])
//             DecryptedThing(payload)                        ([0][0][u32 len][payload])
//             [0,0,0,0]                                      (reserved)
//             [1] Signatures([0][u32 1] DecryptedThing(SWK)) (signatures option=1)
//             [0]                                            (hash option=empty)
//   signable = EntryV0 up to reserved, then [0][0]          (sig option=0, hash option=0)
//   meta    = [0] LamportClock([0][u32 idlen][id][0][u64 wall][u32 logical])
//             string(gid) u32(next.len) next* u8(type) optbytes(meta_data)
//   payload = [0] bytes(data)
//   SWK     = [0] bytes(sig) [0] pubkey[32] u8(prehash=0)
//   signature = Ed25519(signable)   (raw, prehash=0 — NOT sha256-prehashed)
//   CID     = raw CIDv1 of SHA-256(storage)  (computed by the native primitive)
// ---------------------------------------------------------------------------

use ed25519_dalek::{Signer, SigningKey};

/// Parameters for a fixture EntryV0 (mirrors the kernel's builder inputs).
pub struct FixtureEntry<'a> {
    pub seed: [u8; 32],
    pub clock_id: &'a [u8],
    pub wall_time: u64,
    pub logical: u32,
    pub gid: &'a str,
    pub next: &'a [String],
    pub entry_type: u8,
    pub meta_data: Option<&'a [u8]>,
    pub payload_data: &'a [u8],
}

fn write_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}
fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_bytes(out: &mut Vec<u8>, v: &[u8]) {
    write_u32(out, v.len() as u32);
    out.extend_from_slice(v);
}
fn write_string(out: &mut Vec<u8>, v: &str) {
    write_bytes(out, v.as_bytes());
}
/// MaybeEncrypted::Decrypted(DecryptedThing{data}) = [0][0][u32 len][data].
fn write_decrypted_thing(out: &mut Vec<u8>, data: &[u8]) {
    write_u8(out, 0); // MaybeEncrypted variant
    write_u8(out, 0); // DecryptedThing variant
    write_bytes(out, data);
}

fn encode_meta(f: &FixtureEntry<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    write_u8(&mut out, 0); // Meta variant
    write_u8(&mut out, 0); // LamportClock variant
    write_bytes(&mut out, f.clock_id);
    write_u8(&mut out, 0); // Timestamp variant
    write_u64(&mut out, f.wall_time);
    write_u32(&mut out, f.logical);
    write_string(&mut out, f.gid);
    write_u32(&mut out, f.next.len() as u32);
    for n in f.next {
        write_string(&mut out, n);
    }
    write_u8(&mut out, f.entry_type);
    match f.meta_data {
        Some(data) => {
            write_u8(&mut out, 1);
            write_bytes(&mut out, data);
        }
        None => write_u8(&mut out, 0),
    }
    out
}

fn encode_payload(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    write_u8(&mut out, 0); // Payload variant
    write_bytes(&mut out, data);
    out
}

fn encode_signature_with_key(signature: &[u8], public_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    write_u8(&mut out, 0); // SignatureWithKey variant
    write_bytes(&mut out, signature);
    write_u8(&mut out, 0); // Ed25519PublicKey variant
    out.extend_from_slice(public_key);
    write_u8(&mut out, 0); // prehash = 0
    out
}

/// Build a real signed EntryV0 storage block natively. Returns the storage
/// bytes; the caller CID-addresses it via the native primitive.
pub fn build_signed_entry_v0_storage(f: &FixtureEntry<'_>) -> Vec<u8> {
    let signing_key = SigningKey::from_bytes(&f.seed);
    let public_key = signing_key.verifying_key().to_bytes();

    let meta = encode_meta(f);
    let payload = encode_payload(f.payload_data);

    // signable = EntryV0(meta, payload) with signatures option=0, hash option=0.
    let mut signable = Vec::new();
    write_u8(&mut signable, 0); // EntryV0 variant
    write_decrypted_thing(&mut signable, &meta);
    write_decrypted_thing(&mut signable, &payload);
    signable.extend_from_slice(&[0, 0, 0, 0]); // reserved
    write_u8(&mut signable, 0); // signatures option = 0
    write_u8(&mut signable, 0); // hash option = empty

    // Ed25519 over the raw signable bytes (prehash=0), matching the kernel's
    // sign_ed25519_with_key(signing_key, &signable).
    let signature = signing_key.sign(&signable).to_bytes();
    let swk = encode_signature_with_key(&signature, &public_key);

    // signable_entry_to_signed_storage: drop the trailing [sig=0][hash=0] and
    // append [1] Signatures([0][u32 1] DecryptedThing(SWK)) [0].
    let mut storage = signable;
    storage.truncate(storage.len() - 2);
    write_u8(&mut storage, 1); // signatures option = 1
    write_u8(&mut storage, 0); // Signatures variant
    write_u32(&mut storage, 1); // exactly one signature
    write_decrypted_thing(&mut storage, &swk);
    write_u8(&mut storage, 0); // hash option = empty
    storage
}

/// Compute the raw CIDv1 the native primitive will assign to a storage block —
/// by asking the primitive itself (prepare with no expected CID). This keeps the
/// spike from re-implementing the CID string codec: the CID that goes into the
/// head hash is the one `peerbit_log_rust` computes.
pub fn cid_of_storage(storage: &[u8]) -> Result<String> {
    let prepared = prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled(
        vec![storage.to_vec()],
        None,
        /* verify_signatures */ true,
        None,
    )
    .map_err(|_| anyhow!("native CID/verify of fixture storage failed"))?;
    let entry = prepared
        .into_iter()
        .next()
        .context("expected one prepared entry")?;
    if !entry.signature_verified {
        bail!("fixture storage did not self-verify — fixture encoding is wrong");
    }
    Ok(entry.cid)
}

/// Wrap fixture storage blocks as a RawExchangeHeads `PubSubData` payload — the
/// exact bytes a peer sends on the topic-control plane. Reuses peerbit_wire's
/// golden-pinned encoder, so the payload round-trips through the recognizer.
pub fn build_sync_payload(topics: &[String], heads: &[(String, Vec<u8>, Vec<String>)]) -> Vec<u8> {
    peerbit_wire::sync_payload::encode_raw_exchange_sync_payload(
        topics,
        /* strict */ false,
        heads,
        [0, 0, 0, 0],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(n: u8) -> [u8; 32] {
        [n; 32]
    }

    /// Build a single fixture entry (storage bytes + its native CID) for `gid`.
    fn fixture(gid: &str, payload: &[u8], seed_n: u8) -> (String, Vec<u8>) {
        let next: Vec<String> = Vec::new();
        let f = FixtureEntry {
            seed: seed(seed_n),
            clock_id: b"spike-clock-id",
            wall_time: 1_700_000_000_000,
            logical: 0,
            gid,
            next: &next,
            entry_type: 0,
            meta_data: None,
            payload_data: payload,
        };
        let storage = build_signed_entry_v0_storage(&f);
        let cid = cid_of_storage(&storage).expect("fixture must self-verify + get a CID");
        (cid, storage)
    }

    #[test]
    fn fixture_storage_verifies_and_gets_a_cid_natively() {
        // The fixture encoding is accepted by the REAL native primitive: it
        // parses as EntryV0 storage AND its Ed25519 author signature verifies.
        // If the borsh layout or the signable bytes were wrong, this fails.
        let (cid, storage) = fixture("gid-A", b"hello native data plane", 7);
        assert!(
            cid.starts_with('z'),
            "raw CIDv1 base58btc string, got {cid}"
        );
        assert!(!storage.is_empty());

        // Re-prepare with the expected CID: the primitive re-checks CID + sig.
        let prepared = prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled(
            vec![storage.clone()],
            Some(vec![cid.clone()]),
            true,
            None,
        )
        .expect("prepare with expected CID");
        assert_eq!(prepared.len(), 1);
        assert!(prepared[0].signature_verified);
        assert_eq!(prepared[0].cid, cid);
        assert_eq!(prepared[0].gid, "gid-A");
    }

    /// Flip a signed CONTENT byte while keeping the block borsh-parseable, so
    /// the native primitive stays on its Ok path (never builds an aborting
    /// JsValue) yet its Ed25519 verify returns false. The storage layout is
    /// `[0]`(EntryV0) `[0][0]`(MaybeEncrypted+Decrypted) `[u32 metalen @3..7]`
    /// `[0]`(Meta) `[0]`(Clock) `[u32 clockidlen @9..13]` `[clock_id @13..]`.
    /// Our fixtures use a 14-byte clock id, so offset 14 is a clock-id DATA byte
    /// — inside the signable, and NOT any length/framing byte. Flipping it
    /// corrupts the signed bytes without breaking borsh parsing.
    fn tamper_signed_content(storage: &mut [u8]) {
        let idx = 14;
        assert!(storage.len() > idx, "fixture storage shorter than expected");
        storage[idx] ^= 0xff;
    }

    #[test]
    fn tampered_fixture_fails_native_verify() {
        // Tamper a signed content byte, keeping the block parseable: the native
        // primitive parses it (Ok) but its Ed25519 verify must return
        // signature_verified=false. Proves the verify is real, not a stamp.
        let (cid, mut storage) = fixture("gid-A", b"tamper me now", 7);
        tamper_signed_content(&mut storage);
        let prepared = prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled(
            vec![storage],
            /* compute the CID, do not pass expected */ None,
            true,
            None,
        )
        .expect("tampered-but-parseable block still prepares (Ok), just doesn't verify");
        assert_eq!(prepared.len(), 1);
        assert!(
            !prepared[0].signature_verified,
            "tampered block must NOT verify"
        );
        assert_ne!(
            prepared[0].cid, cid,
            "tampering also changes the computed CID"
        );
    }

    #[test]
    fn received_entry_is_committed_and_queryable() {
        // THE SLICE PROOF: a received sync payload carrying one signed entry is
        // recognized, ingested+verified, committed to the native block store +
        // graph + document index, and a native INDEX QUERY returns it.
        let gid = "log-gid-42";
        let (cid, storage) = fixture(gid, b"payload committed to the native data plane", 3);

        let payload = build_sync_payload(
            &["spike/data-plane".to_string()],
            &[(cid.clone(), storage.clone(), vec![gid.to_string()])],
        );

        let mut plane = NativeDataPlane::new();
        let committed = plane
            .recognize_and_commit(&payload)
            .expect("recognize + ingest + commit");

        // One entry committed, signature verified, CID matches.
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].cid, cid);
        assert_eq!(committed[0].gid, gid);
        assert!(committed[0].signature_verified);

        // (a) block store durably holds the raw entry bytes by CID.
        assert!(
            plane.has_block(&cid),
            "block store must hold the entry bytes"
        );
        assert_eq!(
            plane.blocks.get_ref(&cid).map(<[u8]>::to_vec),
            Some(storage),
            "stored bytes are the exact received storage block"
        );

        // (b) graph heads contain the new entry.
        assert!(
            plane.heads(None).contains(&cid),
            "graph heads must contain the committed entry"
        );
        assert!(
            plane.heads(Some(gid)).contains(&cid),
            "gid-scoped heads must contain the committed entry"
        );

        // (c) THE QUERY: the native document index returns the entry for its gid.
        let hits = plane.cids_for_gid(gid);
        assert_eq!(
            hits,
            vec![cid.clone()],
            "index query by gid returns the CID"
        );
        assert_eq!(plane.count_for_gid(gid), 1);

        // A gid that was never committed returns nothing.
        assert!(plane.cids_for_gid("no-such-gid").is_empty());
        assert_eq!(plane.count_for_gid("no-such-gid"), 0);
    }

    #[test]
    fn multiple_heads_all_land_and_query_by_gid() {
        // A batch of three entries (two share a gid, one distinct) all commit,
        // and index queries partition them correctly by gid.
        let (cid_a1, s_a1) = fixture("gid-A", b"a1", 3);
        let (cid_a2, s_a2) = fixture("gid-A", b"a2-different-payload", 4);
        let (cid_b1, s_b1) = fixture("gid-B", b"b1", 5);

        let payload = build_sync_payload(
            &["t".to_string()],
            &[
                (cid_a1.clone(), s_a1, vec!["gid-A".to_string()]),
                (cid_a2.clone(), s_a2, vec!["gid-A".to_string()]),
                (cid_b1.clone(), s_b1, vec!["gid-B".to_string()]),
            ],
        );

        let mut plane = NativeDataPlane::new();
        let committed = plane.recognize_and_commit(&payload).expect("commit batch");
        assert_eq!(committed.len(), 3);

        assert_eq!(plane.count_for_gid("gid-A"), 2);
        assert_eq!(plane.count_for_gid("gid-B"), 1);

        let mut a_hits = plane.cids_for_gid("gid-A");
        a_hits.sort();
        let mut expected_a = vec![cid_a1.clone(), cid_a2.clone()];
        expected_a.sort();
        assert_eq!(a_hits, expected_a);
        assert_eq!(plane.cids_for_gid("gid-B"), vec![cid_b1.clone()]);

        // All three blocks are durable and all three are heads.
        for cid in [&cid_a1, &cid_a2, &cid_b1] {
            assert!(plane.has_block(cid));
            assert!(plane.heads(None).contains(cid));
        }
    }

    #[test]
    fn foreign_payload_is_rejected_not_committed() {
        // A payload that is not a raw-exchange sync payload commits nothing and
        // errors — the recognizer gate holds.
        let mut plane = NativeDataPlane::new();
        let garbage = vec![0xffu8; 32];
        assert!(plane.recognize_and_commit(&garbage).is_err());
        assert_eq!(plane.index.len(), 0);
        assert_eq!(plane.graph.len(), 0);
        assert_eq!(plane.blocks.len(), 0);
    }

    #[test]
    fn tampered_head_in_payload_aborts_batch_before_commit() {
        // If a head block is corrupted (still borsh-parseable) but still
        // advertises its old CID, the computed CID no longer matches the head
        // hash, so `recognize_and_commit` rejects the WHOLE batch in plain Rust
        // and NOTHING is committed (prepare-then-commit — no half-applied state).
        let (cid_good, s_good) = fixture("gid-A", b"good entry here", 3);
        let (cid_bad, mut s_bad) = fixture("gid-B", b"bad entry here", 4);
        // Corrupt the second block's signed content, keep advertising old CID.
        tamper_signed_content(&mut s_bad);

        let payload = build_sync_payload(
            &["t".to_string()],
            &[
                (cid_good.clone(), s_good, vec!["gid-A".to_string()]),
                (cid_bad.clone(), s_bad, vec!["gid-B".to_string()]),
            ],
        );

        let mut plane = NativeDataPlane::new();
        let result = plane.recognize_and_commit(&payload);
        assert!(
            result.is_err(),
            "batch with a CID mismatch must be rejected"
        );

        // Nothing committed — not even the good entry.
        assert_eq!(plane.index.len(), 0);
        assert_eq!(plane.graph.len(), 0);
        assert_eq!(plane.blocks.len(), 0);
    }
}
