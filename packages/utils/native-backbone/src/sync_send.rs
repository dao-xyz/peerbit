//! Send fusion for shared-log raw exchange-heads sync.
//!
//! The outbound counterpart of `wire_sync.rs`: the full
//! `PubSubData → RequestV0 → DecryptedThing → RawExchangeHeadsMessage` payload
//! is serialized inside this wasm module by `peerbit_wire::sync_payload`,
//! reading the entry block bytes straight from the native block store the
//! append/receive paths already committed them to. The only boundary crossing
//! is the single finished payload buffer handed to JS to become the
//! `DataMessage` payload — entry block bytes never materialize as JS values on
//! the send path.
//!
//! The store-reading core is `JsValue`-free so host `cargo test` covers it.

use js_sys::{Array, Uint32Array, Uint8Array};
use peerbit_log_rust::NativeLogBlockStore;
use peerbit_wire::sync_payload::{encode_raw_exchange_sync_payload_refs, SyncPayloadHeadRef};
use wasm_bindgen::prelude::*;

use crate::js_interop::{ensure_same_len, string_batches_from_array, strings_from_array};
use crate::NativePeerbitBackbone;

/// Marks a missing block in `block_byte_lengths` results (a real entry block
/// cannot reach this size: message payloads are bounded far below it).
pub(crate) const SYNC_SEND_MISSING_BLOCK: u32 = u32::MAX;

/// Encode the outbound raw exchange sync payload for `hashes`, resolving each
/// head's block bytes from `blocks`. Returns `None` when any block is missing
/// (callers fall back to the TS send path).
pub(crate) fn encode_sync_payload_from_store(
    blocks: &NativeLogBlockStore,
    topic: &str,
    strict: bool,
    hashes: &[String],
    gid_refrences: &[Vec<String>],
    reserved: [u8; 4],
) -> Option<Vec<u8>> {
    const EMPTY_REFS: &[String] = &[];
    let mut heads = Vec::with_capacity(hashes.len());
    for (index, hash) in hashes.iter().enumerate() {
        let bytes = blocks.get_ref(hash)?;
        heads.push(SyncPayloadHeadRef {
            hash,
            bytes,
            gid_refrences: gid_refrences
                .get(index)
                .map(Vec::as_slice)
                .unwrap_or(EMPTY_REFS),
        });
    }
    Some(encode_raw_exchange_sync_payload_refs(
        &[topic.to_string()],
        strict,
        &heads,
        reserved,
    ))
}

pub(crate) fn block_byte_lengths_core(blocks: &NativeLogBlockStore, hashes: &[String]) -> Vec<u32> {
    hashes
        .iter()
        .map(|hash| {
            blocks
                .get_ref(hash)
                .map(|bytes| bytes.len() as u32)
                .unwrap_or(SYNC_SEND_MISSING_BLOCK)
        })
        .collect()
}

#[wasm_bindgen]
impl NativePeerbitBackbone {
    /// Byte lengths of natively stored entry blocks for `hashes`;
    /// `u32::MAX` marks a missing block. Used by the fused send path to plan
    /// message chunking without materializing block bytes in JS.
    pub fn sync_send_block_byte_lengths(&self, hashes: Array) -> Result<Uint32Array, JsValue> {
        let hashes = strings_from_array(hashes)?;
        Ok(Uint32Array::from(
            block_byte_lengths_core(&self.blocks, &hashes).as_slice(),
        ))
    }

    /// Serialize one outbound raw exchange sync payload (the full PubSubData
    /// nesting) from the native block store. Returns `undefined` when any
    /// head's block is not natively stored (the caller falls back to the TS
    /// serialization path).
    pub fn encode_raw_exchange_sync_payload(
        &self,
        topic: &str,
        strict: bool,
        hashes: Array,
        gid_refrences: Array,
        reserved: &[u8],
    ) -> Result<JsValue, JsValue> {
        let hashes = strings_from_array(hashes)?;
        let gid_refrences = string_batches_from_array(gid_refrences, "sync send gid references")?;
        ensure_same_len(hashes.len(), gid_refrences.len(), "sync send heads")?;
        let reserved: [u8; 4] = reserved
            .try_into()
            .map_err(|_| JsValue::from_str("expected 4 reserved bytes"))?;
        match encode_sync_payload_from_store(
            &self.blocks,
            topic,
            strict,
            &hashes,
            &gid_refrences,
            reserved,
        ) {
            Some(payload) => Ok(Uint8Array::from(payload.as_slice()).into()),
            None => Ok(JsValue::UNDEFINED),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use peerbit_wire::sync_payload::{
        encode_raw_exchange_sync_payload, parse_pubsub_data, parse_raw_exchange_rpc_request,
    };

    fn store_with(entries: &[(&str, Vec<u8>)]) -> NativeLogBlockStore {
        let mut store = NativeLogBlockStore::new();
        store.put_entries_core(
            entries
                .iter()
                .map(|(hash, bytes)| (hash.to_string(), bytes.clone()))
                .collect(),
        );
        store
    }

    #[test]
    fn encodes_store_blocks_byte_identical_to_owned_encoder() {
        let store = store_with(&[("zb2AA", vec![0xde, 0xad]), ("zb2BB", vec![1, 2, 3])]);
        let hashes = vec!["zb2AA".to_string(), "zb2BB".to_string()];
        let gid_refrences = vec![vec!["g1".to_string()], Vec::new()];
        let payload = encode_sync_payload_from_store(
            &store,
            "topic",
            true,
            &hashes,
            &gid_refrences,
            [1, 0, 0, 0],
        )
        .unwrap();
        let expected = encode_raw_exchange_sync_payload(
            &["topic".to_string()],
            true,
            &[
                (
                    "zb2AA".to_string(),
                    vec![0xde, 0xad],
                    vec!["g1".to_string()],
                ),
                ("zb2BB".to_string(), vec![1, 2, 3], Vec::new()),
            ],
            [1, 0, 0, 0],
        );
        assert_eq!(payload, expected);

        let pubsub = parse_pubsub_data(&payload).unwrap();
        let data = &payload[pubsub.data_offset..pubsub.data_offset + pubsub.data_length];
        let parsed = parse_raw_exchange_rpc_request(data).unwrap();
        assert_eq!(parsed.heads.len(), 2);
    }

    #[test]
    fn missing_blocks_fall_back() {
        let store = store_with(&[("zb2AA", vec![0xde])]);
        let hashes = vec!["zb2AA".to_string(), "zb2MISSING".to_string()];
        let refs = vec![Vec::new(), Vec::new()];
        assert!(
            encode_sync_payload_from_store(&store, "t", true, &hashes, &refs, [0; 4]).is_none()
        );
        assert_eq!(
            block_byte_lengths_core(&store, &hashes),
            vec![1, SYNC_SEND_MISSING_BLOCK]
        );
    }
}
