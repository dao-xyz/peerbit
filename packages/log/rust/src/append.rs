use crate::cid::{
    calculate_raw_cid_v1_parts_profiled, calculate_raw_digest_profiled,
    raw_cid_v1_digest_from_string, raw_cid_v1_string_from_digest,
};
use crate::codec::{
    encode_entry_v0_parts_unsigned_for_signing, encode_entry_v0_payload_data_unsigned_for_signing,
    encode_meta_parts, encode_payload, encode_signature_with_key_parts,
    parse_plain_entry_v0_storage, parse_plain_signature_with_key_ref, parse_raw_entry_v0_meta,
    parse_raw_entry_v0_payload, signable_entry_to_signed_storage,
    unsigned_entry_v0_storage_for_signing,
};
use crate::crypto::{cached_verifying_key, sign_ed25519_with_key, validate_signature_lengths};
use crate::error::LogError;
use crate::graph::{
    decode_absolute_replica_data_u32, JoinPlan, LogEntryMetadata, LogEntryPruneConfirmMetadata,
    LogEntryPruneMetadata, LogGraphIndex, LogIndexEntry,
};
use crate::time::now_ms;
use crate::{NativeEntryV0PlainBuilder, NativeLogBlockStore, NativeLogIndex};
use ed25519_dalek::{verify_batch, Signature, SigningKey, Verifier};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct PreparedRawEntryV0 {
    pub cid: String,
    pub hash_digest_bytes: Vec<u8>,
    pub byte_length: usize,
    pub clock_id: Vec<u8>,
    pub wall_time: u64,
    pub logical: u32,
    pub gid: String,
    pub next: Vec<String>,
    pub entry_type: u8,
    pub meta_bytes: Vec<u8>,
    pub meta_data: Option<Vec<u8>>,
    pub payload_byte_length: usize,
    pub signature_verified: bool,
    pub storage_bytes: Vec<u8>,
    pub requested_replicas: Option<u32>,
    pub signable_prefix_len: usize,
    pub signature_with_key_start: usize,
    pub signature_with_key_len: usize,
}

#[derive(Default, Clone, Copy, Debug)]
pub struct RawEntryV0PrepareProfile {
    pub digest_ms: f64,
    pub cid_string_ms: f64,
    pub expected_cid_ms: f64,
    pub storage_parse_ms: f64,
    pub meta_parse_ms: f64,
    pub payload_parse_ms: f64,
    pub signature_parse_ms: f64,
    pub signable_ms: f64,
    pub verify_batch_ms: f64,
    pub verify_fallback_ms: f64,
}

impl PreparedRawEntryV0 {
    pub fn log_index_entry(&self, head: bool) -> Result<LogIndexEntry, LogError> {
        Ok(LogIndexEntry::new_with_data(
            self.cid.clone(),
            self.gid.clone(),
            self.next.clone(),
            self.entry_type,
            self.wall_time,
            self.logical,
            self.payload_byte_length
                .try_into()
                .map_err(|_| LogError::PayloadByteLengthExceedsU32)?,
            head,
            self.meta_data.clone(),
        ))
    }
}

pub struct NativeCommittedEntryFacts {
    pub hash: String,
    pub next: Vec<String>,
    pub meta_bytes: Vec<u8>,
    pub byte_length: usize,
    pub hash_digest_bytes: Vec<u8>,
}

#[derive(Clone, Default)]
pub struct NativeLogAppendProfile {
    pub next_clone_ms: f64,
    pub entry_core_ms: f64,
    pub encode_meta_ms: f64,
    pub encode_payload_ms: f64,
    pub encode_signable_ms: f64,
    pub sign_ms: f64,
    pub encode_signature_ms: f64,
    pub encode_storage_ms: f64,
    pub cid_ms: f64,
    pub cid_hash_ms: f64,
    pub cid_string_ms: f64,
    pub index_entry_ms: f64,
    pub facts_ms: f64,
    pub block_put_ms: f64,
    pub graph_put_ms: f64,
    pub trim_ms: f64,
}

pub(crate) struct PreparedPlainEntryCore {
    pub(crate) hash: String,
    pub(crate) next: Vec<String>,
    pub(crate) meta_bytes: Vec<u8>,
    pub(crate) payload_bytes: Vec<u8>,
    pub(crate) signature_bytes: [u8; 64],
    pub(crate) signature_with_key_bytes: Vec<u8>,
    pub(crate) storage_bytes: Vec<u8>,
    pub(crate) hash_digest_bytes: Vec<u8>,
    pub(crate) entry: LogIndexEntry,
}

pub(crate) struct PreparedPlainEntryCommitCore {
    pub(crate) hash: String,
    pub(crate) next: Vec<String>,
    pub(crate) meta_bytes: Vec<u8>,
    pub(crate) storage_bytes: Vec<u8>,
    pub(crate) hash_digest_bytes: Vec<u8>,
    pub(crate) entry: LogIndexEntry,
}

impl NativeLogBlockStore {
    pub fn get_ref(&self, key: &str) -> Option<&[u8]> {
        self.entries.get(key).map(Vec::as_slice)
    }

    pub(crate) fn put_entry(&mut self, key: String, value: Vec<u8>) {
        let value_len = value.len() as u64;
        if let Some(previous) = self.entries.insert(key, value) {
            self.total_size = self.total_size.saturating_sub(previous.len() as u64);
        }
        self.total_size += value_len;
    }

    pub(crate) fn put_entries(&mut self, entries: Vec<(String, Vec<u8>)>) {
        self.entries.reserve(entries.len());
        for (key, value) in entries {
            self.put_entry(key, value);
        }
    }

    pub fn put_entries_core(&mut self, entries: Vec<(String, Vec<u8>)>) {
        self.put_entries(entries);
    }
}

impl NativeLogIndex {
    pub fn max_head_data_u32_values(&self, gids: &[String]) -> Vec<Option<u32>> {
        self.inner.max_head_data_u32_batch(gids)
    }

    pub fn entry_metadata_values(&self, hashes: &[String]) -> Vec<Option<LogEntryMetadata>> {
        self.inner.entry_metadata_batch(hashes)
    }

    pub fn entry_prune_metadata_values(
        &self,
        hashes: &[String],
    ) -> Vec<Option<LogEntryPruneMetadata>> {
        self.inner.entry_prune_metadata_batch(hashes)
    }

    pub fn entry_prune_confirm_metadata_values(
        &self,
        hashes: &[String],
    ) -> Vec<Option<LogEntryPruneConfirmMetadata>> {
        self.inner.entry_prune_confirm_metadata_batch(hashes)
    }

    pub fn entry_prune_confirm_metadata_ref(&self, hash: &str) -> Option<(&str, Option<u32>)> {
        self.inner.entry_prune_confirm_metadata_ref(hash)
    }

    pub fn put_entries_core(&mut self, entries: Vec<LogIndexEntry>) {
        self.inner.put_many(entries);
    }

    pub fn put_join_batch_entries_core(&mut self, entries: Vec<LogIndexEntry>) {
        self.inner.put_join_batch(entries);
    }

    pub fn plan_join_entries_core(
        &self,
        entries: &[LogIndexEntry],
        reset: bool,
        cut_check: bool,
    ) -> Vec<JoinPlan> {
        let hashes = entries
            .iter()
            .map(|entry| entry.hash.clone())
            .collect::<Vec<_>>();
        let nexts = entries
            .iter()
            .map(|entry| entry.next.clone())
            .collect::<Vec<_>>();
        let entry_types = entries
            .iter()
            .map(|entry| entry.entry_type)
            .collect::<Vec<_>>();
        let cut_check_values = cut_check.then(|| {
            (
                entries
                    .iter()
                    .map(|entry| entry.gid.clone())
                    .collect::<Vec<_>>(),
                entries
                    .iter()
                    .map(|entry| entry.wall_time)
                    .collect::<Vec<_>>(),
                entries
                    .iter()
                    .map(|entry| entry.logical)
                    .collect::<Vec<_>>(),
            )
        });
        self.inner.plan_join_batch(
            &hashes,
            &nexts,
            &entry_types,
            reset,
            cut_check_values
                .as_ref()
                .map(|(gids, wall_times, logicals)| {
                    (gids.as_slice(), wall_times.as_slice(), logicals.as_slice())
                }),
        )
    }

    pub fn plan_join_entry_refs_core(
        &self,
        entries: &[&LogIndexEntry],
        reset: bool,
        cut_check: bool,
    ) -> Vec<JoinPlan> {
        self.inner.plan_join_entry_refs(entries, reset, cut_check)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_core_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: Option<usize>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<LogIndexEntry>), LogError> {
        self.prepare_entry_v0_plain_entry_commit_facts_core_and_put_with_builder_borrowed(
            builder,
            block_store,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            &payload_data,
            trim_length_to,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_core_and_put_with_builder_borrowed(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: Option<usize>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<LogIndexEntry>), LogError> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Entries,
                None,
            )?;
        Ok((facts, trimmed.into_entries()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: Option<usize>,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<LogIndexEntry>), LogError> {
        self.prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_borrowed(
            builder,
            block_store,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            &payload_data,
            trim_length_to,
            profile.as_deref_mut(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_borrowed(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: Option<usize>,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<LogIndexEntry>), LogError> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Entries,
                profile.as_deref_mut(),
            )?;
        Ok((facts, trimmed.into_entries()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: Option<usize>,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>), LogError> {
        self.prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes_borrowed(
            builder,
            block_store,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            &payload_data,
            trim_length_to,
            profile.as_deref_mut(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_trim_hashes_borrowed(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: Option<usize>,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>), LogError> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                next,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Hashes,
                profile.as_deref_mut(),
            )?;
        Ok((facts, trimmed.into_hashes()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<NativeCommittedEntryFacts, LogError> {
        self.prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_borrowed(
            builder,
            block_store,
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            &payload_data,
            profile.as_deref_mut(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_borrowed(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<NativeCommittedEntryFacts, LogError> {
        let core_started = profile.as_ref().map(|_| now_ms());
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            profile.as_deref_mut(),
        )?;
        if let Some(started) = core_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.entry_core_ms += now_ms() - started;
            }
        }
        let facts_started = profile.as_ref().map(|_| now_ms());
        let entry = core.entry;
        let hash = core.hash;
        let facts = NativeCommittedEntryFacts {
            hash: hash.clone(),
            next: core.next,
            meta_bytes: core.meta_bytes,
            byte_length: core.storage_bytes.len(),
            hash_digest_bytes: core.hash_digest_bytes,
        };
        if let Some(started) = facts_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.facts_ms += now_ms() - started;
            }
        }
        let block_put_started = profile.as_ref().map(|_| now_ms());
        block_store.put_entry(hash, core.storage_bytes);
        if let Some(started) = block_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.block_put_ms += now_ms() - started;
            }
        }
        let graph_put_started = profile.as_ref().map(|_| now_ms());
        self.inner.put_no_next(entry);
        if let Some(started) = graph_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.graph_put_ms += now_ms() - started;
            }
        }
        Ok(facts)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: usize,
        trim_mode: NativeTrimMode,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, NativeTrimResult), LogError> {
        self.prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner_borrowed(
            builder,
            block_store,
            wall_time,
            logical,
            gid,
            entry_type,
            meta_data,
            &payload_data,
            trim_length_to,
            trim_mode,
            profile.as_deref_mut(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_hashes_borrowed(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: usize,
        profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>), LogError> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner_borrowed(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Hashes,
                profile,
            )?;
        Ok((facts, trimmed.into_hashes()))
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner_borrowed(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: usize,
        trim_mode: NativeTrimMode,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, NativeTrimResult), LogError> {
        let core_started = profile.as_ref().map(|_| now_ms());
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            Vec::new(),
            entry_type,
            meta_data,
            payload_data,
            profile.as_deref_mut(),
        )?;
        if let Some(started) = core_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.entry_core_ms += now_ms() - started;
            }
        }
        let facts_started = profile.as_ref().map(|_| now_ms());
        let entry = core.entry;
        let hash = core.hash;
        let facts = NativeCommittedEntryFacts {
            hash: hash.clone(),
            next: core.next,
            meta_bytes: core.meta_bytes,
            byte_length: core.storage_bytes.len(),
            hash_digest_bytes: core.hash_digest_bytes,
        };
        if let Some(started) = facts_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.facts_ms += now_ms() - started;
            }
        }
        let block_put_started = profile.as_ref().map(|_| now_ms());
        block_store.put_entry(hash, core.storage_bytes);
        if let Some(started) = block_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.block_put_ms += now_ms() - started;
            }
        }
        let graph_put_started = profile.as_ref().map(|_| now_ms());
        self.inner.put_no_next(entry);
        if let Some(started) = graph_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.graph_put_ms += now_ms() - started;
            }
        }
        let trim_started = profile.as_ref().map(|_| now_ms());
        let trimmed =
            match trim_mode {
                NativeTrimMode::Entries => NativeTrimResult::Entries(trim_oldest_log_entries_core(
                    &mut self.inner,
                    block_store,
                    trim_length_to,
                )),
                NativeTrimMode::Hashes => NativeTrimResult::Hashes(
                    trim_oldest_log_entry_hashes_core(&mut self.inner, block_store, trim_length_to),
                ),
            };
        if let Some(started) = trim_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.trim_ms += now_ms() - started;
            }
        }
        Ok((facts, trimmed))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: usize,
        profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<LogIndexEntry>), LogError> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Entries,
                profile,
            )?;
        Ok((facts, trimmed.into_entries()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_hashes(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: Vec<u8>,
        trim_length_to: usize,
        profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, Vec<String>), LogError> {
        let (facts, trimmed) = self
            .prepare_entry_v0_plain_entry_commit_no_next_facts_core_profiled_and_put_with_builder_trim_inner(
                builder,
                block_store,
                wall_time,
                logical,
                gid,
                entry_type,
                meta_data,
                payload_data,
                trim_length_to,
                NativeTrimMode::Hashes,
                profile,
            )?;
        Ok((facts, trimmed.into_hashes()))
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_entry_v0_plain_entry_commit_facts_core_profiled_and_put_with_builder_inner(
        &mut self,
        builder: &NativeEntryV0PlainBuilder,
        block_store: &mut NativeLogBlockStore,
        wall_time: u64,
        logical: u32,
        gid: String,
        next: Vec<String>,
        entry_type: u8,
        meta_data: Option<Vec<u8>>,
        payload_data: &[u8],
        trim_length_to: Option<usize>,
        trim_mode: NativeTrimMode,
        mut profile: Option<&mut NativeLogAppendProfile>,
    ) -> Result<(NativeCommittedEntryFacts, NativeTrimResult), LogError> {
        let core_started = profile.as_ref().map(|_| now_ms());
        let core = prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
            &builder.clock_id,
            &builder.public_key,
            &builder.signing_key,
            wall_time,
            logical,
            gid,
            next,
            entry_type,
            meta_data,
            &payload_data,
            profile.as_deref_mut(),
        )?;
        if let Some(started) = core_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.entry_core_ms += now_ms() - started;
            }
        }
        let entry = core.entry;
        let hash = core.hash;
        let next = core.next;
        let meta_bytes = core.meta_bytes;
        let byte_length = core.storage_bytes.len();
        let hash_digest_bytes = core.hash_digest_bytes;
        let block_put_started = profile.as_ref().map(|_| now_ms());
        block_store.put_entry(hash.clone(), core.storage_bytes);
        if let Some(started) = block_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.block_put_ms += now_ms() - started;
            }
        }
        let graph_put_started = profile.as_ref().map(|_| now_ms());
        self.inner.put_append_entry(entry, &next);
        if let Some(started) = graph_put_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.graph_put_ms += now_ms() - started;
            }
        }
        let facts_started = profile.as_ref().map(|_| now_ms());
        let facts = NativeCommittedEntryFacts {
            hash,
            next,
            meta_bytes,
            byte_length,
            hash_digest_bytes,
        };
        if let Some(started) = facts_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.facts_ms += now_ms() - started;
            }
        }
        let trim_started = profile.as_ref().map(|_| now_ms());
        let trimmed = trim_length_to
            .map(|trim_length_to| match trim_mode {
                NativeTrimMode::Entries => NativeTrimResult::Entries(trim_oldest_log_entries_core(
                    &mut self.inner,
                    block_store,
                    trim_length_to,
                )),
                NativeTrimMode::Hashes => NativeTrimResult::Hashes(
                    trim_oldest_log_entry_hashes_core(&mut self.inner, block_store, trim_length_to),
                ),
            })
            .unwrap_or_else(|| trim_mode.empty_result());
        if let Some(started) = trim_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.trim_ms += now_ms() - started;
            }
        }
        Ok((facts, trimmed))
    }
}

#[derive(Clone, Copy)]
enum NativeTrimMode {
    Entries,
    Hashes,
}

impl NativeTrimMode {
    fn empty_result(self) -> NativeTrimResult {
        match self {
            NativeTrimMode::Entries => NativeTrimResult::Entries(Vec::new()),
            NativeTrimMode::Hashes => NativeTrimResult::Hashes(Vec::new()),
        }
    }
}

enum NativeTrimResult {
    Entries(Vec<LogIndexEntry>),
    Hashes(Vec<String>),
}

impl NativeTrimResult {
    fn into_entries(self) -> Vec<LogIndexEntry> {
        match self {
            NativeTrimResult::Entries(entries) => entries,
            NativeTrimResult::Hashes(_) => {
                unreachable!("hash-only trim result cannot be converted to entries")
            }
        }
    }

    fn into_hashes(self) -> Vec<String> {
        match self {
            NativeTrimResult::Entries(entries) => {
                entries.into_iter().map(|entry| entry.hash).collect()
            }
            NativeTrimResult::Hashes(hashes) => hashes,
        }
    }
}

pub(crate) fn trim_oldest_log_entries_core(
    index: &mut LogGraphIndex,
    block_store: &mut NativeLogBlockStore,
    trim_length_to: usize,
) -> Vec<LogIndexEntry> {
    let overage = index.len().saturating_sub(trim_length_to);
    if overage == 0 {
        return Vec::new();
    }
    if overage == 1 {
        let Some(hash) = index.oldest_hash() else {
            return Vec::new();
        };
        block_store.delete(&hash);
        return index.delete(&hash).into_iter().collect();
    }
    let entries = index.oldest_entries(overage);
    for entry in &entries {
        block_store.delete(&entry.hash);
    }
    let hashes = entries
        .iter()
        .map(|entry| entry.hash.clone())
        .collect::<Vec<_>>();
    index.delete_many(&hashes);
    entries
}

pub(crate) fn trim_oldest_log_entry_hashes_core(
    index: &mut LogGraphIndex,
    block_store: &mut NativeLogBlockStore,
    trim_length_to: usize,
) -> Vec<String> {
    let overage = index.len().saturating_sub(trim_length_to);
    if overage == 0 {
        return Vec::new();
    }
    if overage == 1 {
        let Some(hash) = index.oldest_hash() else {
            return Vec::new();
        };
        if index.delete(&hash).is_none() {
            return Vec::new();
        }
        block_store.delete(&hash);
        return vec![hash];
    }
    let hashes = index.oldest_hashes(overage);
    for hash in &hashes {
        block_store.delete(hash);
    }
    index.delete_many(&hashes);
    hashes
}

pub(crate) fn trim_oldest_log_index_entries_core(
    index: &mut LogGraphIndex,
    trim_length_to: usize,
) -> Vec<LogIndexEntry> {
    let overage = index.len().saturating_sub(trim_length_to);
    if overage == 0 {
        return Vec::new();
    }
    let entries = index.oldest_entries(overage);
    let hashes = entries
        .iter()
        .map(|entry| entry.hash.clone())
        .collect::<Vec<_>>();
    index.delete_many(&hashes);
    entries
}

pub(crate) fn prepare_entry_v0_plain_entry_core_with_signer_parts(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
    payload_data: Vec<u8>,
) -> Result<PreparedPlainEntryCore, LogError> {
    prepare_entry_v0_plain_entry_core_with_signer_parts_profiled(
        clock_id,
        public_key,
        signing_key,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
        payload_data,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_entry_v0_plain_entry_core_with_signer_parts_profiled(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
    payload_data: Vec<u8>,
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> Result<PreparedPlainEntryCore, LogError> {
    let payload_size = payload_data.len() as u32;

    let encode_meta_started = profile.as_ref().map(|_| now_ms());
    let meta = encode_meta_parts(
        clock_id,
        wall_time,
        logical,
        &gid,
        &next,
        entry_type,
        meta_data.as_deref(),
    );
    if let Some(started) = encode_meta_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_meta_ms += now_ms() - started;
        }
    }
    let encode_payload_started = profile.as_ref().map(|_| now_ms());
    let payload = encode_payload(&payload_data);
    if let Some(started) = encode_payload_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_payload_ms += now_ms() - started;
        }
    }
    let encode_signable_started = profile.as_ref().map(|_| now_ms());
    let signable = encode_entry_v0_parts_unsigned_for_signing(&meta, &payload);
    if let Some(started) = encode_signable_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signable_ms += now_ms() - started;
        }
    }
    let sign_started = profile.as_ref().map(|_| now_ms());
    let signature = sign_ed25519_with_key(&signing_key, &signable);
    if let Some(started) = sign_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.sign_ms += now_ms() - started;
        }
    }
    let encode_signature_started = profile.as_ref().map(|_| now_ms());
    let signature_with_key = encode_signature_with_key_parts(&signature, public_key, 0);
    if let Some(started) = encode_signature_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signature_ms += now_ms() - started;
        }
    }
    let encode_storage_started = profile.as_ref().map(|_| now_ms());
    let storage = signable_entry_to_signed_storage(signable, &signature_with_key);
    if let Some(started) = encode_storage_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_storage_ms += now_ms() - started;
        }
    }
    let cid_started = profile.as_ref().map(|_| now_ms());
    let (cid, hash_digest) = calculate_raw_cid_v1_parts_profiled(&storage, profile.as_deref_mut());
    if let Some(started) = cid_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_ms += now_ms() - started;
        }
    }

    let index_entry_started = profile.as_ref().map(|_| now_ms());
    let entry = LogIndexEntry::new_with_data(
        cid.clone(),
        gid,
        next.clone(),
        entry_type,
        wall_time,
        logical,
        payload_size,
        true,
        meta_data,
    );
    if let Some(started) = index_entry_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.index_entry_ms += now_ms() - started;
        }
    }
    Ok(PreparedPlainEntryCore {
        hash: cid,
        next,
        meta_bytes: meta,
        payload_bytes: payload,
        signature_bytes: signature,
        signature_with_key_bytes: signature_with_key,
        storage_bytes: storage,
        hash_digest_bytes: hash_digest.to_vec(),
        entry,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_entry_v0_plain_entry_commit_core_with_signer_parts_profiled(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: String,
    next: Vec<String>,
    entry_type: u8,
    meta_data: Option<Vec<u8>>,
    payload_data: &[u8],
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> Result<PreparedPlainEntryCommitCore, LogError> {
    let payload_size = payload_data.len() as u32;

    let encode_meta_started = profile.as_ref().map(|_| now_ms());
    let meta = encode_meta_parts(
        clock_id,
        wall_time,
        logical,
        &gid,
        &next,
        entry_type,
        meta_data.as_deref(),
    );
    if let Some(started) = encode_meta_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_meta_ms += now_ms() - started;
        }
    }
    let encode_signable_started = profile.as_ref().map(|_| now_ms());
    let signable = encode_entry_v0_payload_data_unsigned_for_signing(&meta, &payload_data);
    if let Some(started) = encode_signable_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signable_ms += now_ms() - started;
        }
    }
    let sign_started = profile.as_ref().map(|_| now_ms());
    let signature = sign_ed25519_with_key(signing_key, &signable);
    if let Some(started) = sign_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.sign_ms += now_ms() - started;
        }
    }
    let encode_signature_started = profile.as_ref().map(|_| now_ms());
    let signature_with_key = encode_signature_with_key_parts(&signature, public_key, 0);
    if let Some(started) = encode_signature_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signature_ms += now_ms() - started;
        }
    }
    let encode_storage_started = profile.as_ref().map(|_| now_ms());
    let storage = signable_entry_to_signed_storage(signable, &signature_with_key);
    if let Some(started) = encode_storage_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_storage_ms += now_ms() - started;
        }
    }
    let cid_started = profile.as_ref().map(|_| now_ms());
    let (cid, hash_digest) = calculate_raw_cid_v1_parts_profiled(&storage, profile.as_deref_mut());
    if let Some(started) = cid_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_ms += now_ms() - started;
        }
    }

    let index_entry_started = profile.as_ref().map(|_| now_ms());
    let entry = LogIndexEntry::new_with_data(
        cid.clone(),
        gid,
        next.clone(),
        entry_type,
        wall_time,
        logical,
        payload_size,
        true,
        meta_data,
    );
    if let Some(started) = index_entry_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.index_entry_ms += now_ms() - started;
        }
    }
    Ok(PreparedPlainEntryCommitCore {
        hash: cid,
        next,
        meta_bytes: meta,
        storage_bytes: storage,
        hash_digest_bytes: hash_digest.to_vec(),
        entry,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_entry_v0_plain_entry_commit_digest_key_core_profiled(
    clock_id: &[u8],
    public_key: &[u8],
    signing_key: &SigningKey,
    wall_time: u64,
    logical: u32,
    gid: &str,
    entry_type: u8,
    payload_data: Vec<u8>,
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> (usize, usize) {
    let encode_meta_started = profile.as_ref().map(|_| now_ms());
    let meta = encode_meta_parts(clock_id, wall_time, logical, gid, &[], entry_type, None);
    if let Some(started) = encode_meta_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_meta_ms += now_ms() - started;
        }
    }
    let encode_signable_started = profile.as_ref().map(|_| now_ms());
    let signable = encode_entry_v0_payload_data_unsigned_for_signing(&meta, &payload_data);
    if let Some(started) = encode_signable_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signable_ms += now_ms() - started;
        }
    }
    let sign_started = profile.as_ref().map(|_| now_ms());
    let signature = sign_ed25519_with_key(signing_key, &signable);
    if let Some(started) = sign_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.sign_ms += now_ms() - started;
        }
    }
    let encode_signature_started = profile.as_ref().map(|_| now_ms());
    let signature_with_key = encode_signature_with_key_parts(&signature, public_key, 0);
    if let Some(started) = encode_signature_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_signature_ms += now_ms() - started;
        }
    }
    let encode_storage_started = profile.as_ref().map(|_| now_ms());
    let storage = signable_entry_to_signed_storage(signable, &signature_with_key);
    if let Some(started) = encode_storage_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.encode_storage_ms += now_ms() - started;
        }
    }
    let digest = calculate_raw_digest_profiled(&storage, profile.as_deref_mut());
    (storage.len(), digest.len())
}

pub fn prepare_raw_entry_v0_blocks(
    blocks: Vec<Vec<u8>>,
) -> Result<Vec<PreparedRawEntryV0>, LogError> {
    prepare_raw_entry_v0_blocks_with_expected_cids_and_verify(blocks, None, true)
}

pub fn prepare_raw_entry_v0_blocks_with_expected_cids(
    blocks: Vec<Vec<u8>>,
    expected_cids: Option<Vec<String>>,
) -> Result<Vec<PreparedRawEntryV0>, LogError> {
    prepare_raw_entry_v0_blocks_with_expected_cids_and_verify(blocks, expected_cids, true)
}

pub fn prepare_raw_entry_v0_blocks_with_expected_cids_and_verify(
    blocks: Vec<Vec<u8>>,
    expected_cids: Option<Vec<String>>,
    verify_signatures: bool,
) -> Result<Vec<PreparedRawEntryV0>, LogError> {
    prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled(
        blocks,
        expected_cids,
        verify_signatures,
        None,
    )
}

pub fn prepare_raw_entry_v0_blocks_with_expected_cids_and_verify_profiled(
    blocks: Vec<Vec<u8>>,
    expected_cids: Option<Vec<String>>,
    verify_signatures: bool,
    mut profile: Option<&mut RawEntryV0PrepareProfile>,
) -> Result<Vec<PreparedRawEntryV0>, LogError> {
    if let Some(expected_cids) = expected_cids.as_ref() {
        if expected_cids.len() != blocks.len() {
            return Err(LogError::RawEntryBlockHashLengthMismatch);
        }
    }
    let mut entries = Vec::with_capacity(blocks.len());
    let mut parsed_signatures = Vec::with_capacity(blocks.len());
    let mut parsed_public_keys = Vec::with_capacity(blocks.len());
    let mut parsed_messages = Vec::with_capacity(blocks.len());
    let mut verifying_key_cache = HashMap::new();
    for (index, bytes) in blocks.into_iter().enumerate() {
        let digest_started = profile.as_ref().map(|_| now_ms());
        let digest = calculate_raw_digest_profiled(&bytes, None);
        if let Some(started) = digest_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.digest_ms += now_ms() - started;
            }
        }
        let cid = if let Some(expected_cids) = expected_cids.as_ref() {
            let expected_started = profile.as_ref().map(|_| now_ms());
            let expected_cid = &expected_cids[index];
            let expected_digest = raw_cid_v1_digest_from_string(expected_cid)?;
            if expected_digest != digest {
                return Err(LogError::RawEntryHashMismatch);
            }
            if let Some(started) = expected_started {
                if let Some(profile) = profile.as_deref_mut() {
                    profile.expected_cid_ms += now_ms() - started;
                }
            }
            expected_cid.clone()
        } else {
            let cid_started = profile.as_ref().map(|_| now_ms());
            let cid = raw_cid_v1_string_from_digest(&digest);
            if let Some(started) = cid_started {
                if let Some(profile) = profile.as_deref_mut() {
                    profile.cid_string_ms += now_ms() - started;
                }
            }
            cid
        };
        let storage_started = profile.as_ref().map(|_| now_ms());
        let storage = parse_plain_entry_v0_storage(&bytes)?;
        if let Some(started) = storage_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.storage_parse_ms += now_ms() - started;
            }
        }
        let meta_started = profile.as_ref().map(|_| now_ms());
        let meta = parse_raw_entry_v0_meta(storage.meta)?;
        if let Some(started) = meta_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.meta_parse_ms += now_ms() - started;
            }
        }
        let payload_started = profile.as_ref().map(|_| now_ms());
        let payload = parse_raw_entry_v0_payload(storage.payload)?;
        if let Some(started) = payload_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.payload_parse_ms += now_ms() - started;
            }
        }
        let requested_replicas = decode_absolute_replica_data_u32(meta.meta_data.as_deref());
        let meta_bytes = storage.meta.to_vec();
        let signable_prefix_len = storage.signable_prefix_len;
        let signature_with_key_start = storage.signature_with_key_start;
        let signature_with_key_len = storage.signature_with_key_len;
        if verify_signatures {
            let signature_started = profile.as_ref().map(|_| now_ms());
            let parsed_signature = parse_plain_signature_with_key_ref(storage.signature_with_key)?;
            validate_signature_lengths(parsed_signature.signature, parsed_signature.public_key)?;
            let signature_bytes: [u8; 64] = parsed_signature
                .signature
                .try_into()
                .map_err(|_| LogError::ExpectedEd25519SignatureLength64)?;
            let verifying_key =
                cached_verifying_key(&mut verifying_key_cache, parsed_signature.public_key)?;
            parsed_signatures.push(Signature::from_bytes(&signature_bytes));
            parsed_public_keys.push(verifying_key);
            if let Some(started) = signature_started {
                if let Some(profile) = profile.as_deref_mut() {
                    profile.signature_parse_ms += now_ms() - started;
                }
            }
            let signable_started = profile.as_ref().map(|_| now_ms());
            parsed_messages.push(unsigned_entry_v0_storage_for_signing(
                &bytes,
                storage.signable_prefix_len,
            )?);
            if let Some(started) = signable_started {
                if let Some(profile) = profile.as_deref_mut() {
                    profile.signable_ms += now_ms() - started;
                }
            }
        }

        entries.push(PreparedRawEntryV0 {
            cid,
            hash_digest_bytes: digest.to_vec(),
            byte_length: bytes.len(),
            clock_id: meta.clock_id,
            wall_time: meta.wall_time,
            logical: meta.logical,
            gid: meta.gid,
            next: meta.next,
            entry_type: meta.entry_type,
            meta_bytes,
            meta_data: meta.meta_data,
            payload_byte_length: payload.data.len(),
            signature_verified: false,
            storage_bytes: bytes,
            requested_replicas,
            signable_prefix_len,
            signature_with_key_start,
            signature_with_key_len,
        });
    }
    if !verify_signatures {
        return Ok(entries);
    }
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    let verify_batch_started = profile.as_ref().map(|_| now_ms());
    let batch_verified = verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys);
    if let Some(started) = verify_batch_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.verify_batch_ms += now_ms() - started;
        }
    }
    let verified = if batch_verified.is_ok() {
        vec![true; entries.len()]
    } else {
        let verify_fallback_started = profile.as_ref().map(|_| now_ms());
        let mut out = Vec::with_capacity(entries.len());
        for i in 0..entries.len() {
            out.push(
                parsed_public_keys[i]
                    .verify(&parsed_messages[i], &parsed_signatures[i])
                    .is_ok(),
            );
        }
        if let Some(started) = verify_fallback_started {
            if let Some(profile) = profile.as_deref_mut() {
                profile.verify_fallback_ms += now_ms() - started;
            }
        }
        out
    };
    for (entry, verified) in entries.iter_mut().zip(verified) {
        entry.signature_verified = verified;
    }
    Ok(entries)
}
