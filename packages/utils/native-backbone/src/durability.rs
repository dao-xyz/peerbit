//! Storage-independent framing and validation for the native durability journal.
//!
//! This module deliberately performs no I/O. Storage adapters append the bytes
//! produced here and may truncate only the `incomplete_tail_offset` returned by
//! [`scan_journal`]. Every fully present frame is either accepted or reported as
//! corruption; a checksum mismatch is never treated as a torn tail.

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use js_sys::{Array, Uint8Array};

pub const JOURNAL_FORMAT_VERSION: u16 = 1;
pub const MAX_JOURNAL_BODY_LENGTH: usize = 64 * 1024 * 1024;
pub const MAX_PROGRAM_ID_LENGTH: usize = 4096;
pub const MAX_TRANSACTION_ID_LENGTH: usize = 1024;
pub const MAX_WRITER_OWNER_ID_LENGTH: usize = 1024;
pub const MAX_WRITER_DOMAIN_ID_LENGTH: usize = 1024;

const JOURNAL_MAGIC: &[u8; 8] = b"PBDURJ1\0";
const JOURNAL_TRAILER_MAGIC: &[u8; 8] = b"PBDURE1\0";
const CHECKSUM_LENGTH: usize = 32;
const HEADER_PREFIX_LENGTH: usize = 20;
const HEADER_CHECKSUM_OFFSET: usize = HEADER_PREFIX_LENGTH;
const FRAME_CHECKSUM_OFFSET: usize = HEADER_CHECKSUM_OFFSET + CHECKSUM_LENGTH;
const HEADER_LENGTH: usize = FRAME_CHECKSUM_OFFSET + CHECKSUM_LENGTH;
const TRAILER_LENGTH: usize = JOURNAL_TRAILER_MAGIC.len() + 4;
const FIXED_BODY_LENGTH: usize = 8 + 8 + 8 + 1 + 1 + 2 + 2 + 2 + 2 + 2 + 32 + 4;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityPhase {
    DurablePrepared = 1,
    NativeApplied = 2,
    Published = 3,
    Committed = 4,
    CleanupPending = 5,
    Clean = 6,
}

impl TryFrom<u8> for DurabilityPhase {
    type Error = DurabilityJournalError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::DurablePrepared),
            2 => Ok(Self::NativeApplied),
            3 => Ok(Self::Published),
            4 => Ok(Self::Committed),
            5 => Ok(Self::CleanupPending),
            6 => Ok(Self::Clean),
            value => Err(DurabilityJournalError::InvalidPhase(value)),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityOperationKind {
    Append = 1,
    Batch = 2,
    Document = 3,
    Receive = 4,
    Repair = 5,
}

impl TryFrom<u8> for DurabilityOperationKind {
    type Error = DurabilityJournalError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Append),
            2 => Ok(Self::Batch),
            3 => Ok(Self::Document),
            4 => Ok(Self::Receive),
            5 => Ok(Self::Repair),
            value => Err(DurabilityJournalError::InvalidOperationKind(value)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurabilityJournalRecord {
    pub record_lsn: u64,
    pub tx_sequence: u64,
    pub writer_epoch: u64,
    pub writer_owner_id: String,
    pub writer_domain_id: String,
    pub phase: DurabilityPhase,
    pub operation_kind: DurabilityOperationKind,
    pub program_id: Vec<u8>,
    pub transaction_id: String,
    pub plan_digest: [u8; 32],
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurabilityJournalScan {
    pub records: Vec<DurabilityJournalRecord>,
    pub valid_length: usize,
    pub incomplete_tail_offset: Option<usize>,
    pub incomplete_tail_reason: Option<DurabilityIncompleteTailReason>,
    pub last_record_lsn: u64,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityIncompleteTailReason {
    ShortHeader = 1,
    ShortBody = 2,
    ShortTrailer = 3,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurabilityJournalValidationContext {
    pub checkpoint_lsn: u64,
    pub checkpoint_tx_sequence_highwater: u64,
    pub expected_program_id: Vec<u8>,
    pub expected_writer_domain_id: String,
    pub checkpoint_writer_epoch: u64,
    pub checkpoint_writer_owner_id: Option<String>,
    pub current_writer_epoch: u64,
    pub current_writer_owner_id: String,
    pub retained_transactions: Vec<DurabilityCheckpointTransactionState>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurabilityCheckpointTransactionState {
    pub tx_sequence: u64,
    pub transaction_id: String,
    pub phase: DurabilityPhase,
    pub operation_kind: DurabilityOperationKind,
    pub plan_digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurabilityJournalError {
    InvalidRecordLsn(u64),
    InvalidTransactionSequence(u64),
    InvalidWriterEpoch(u64),
    InvalidDecimalU64(&'static str),
    EmptyProgramId,
    ProgramIdTooLong(usize),
    EmptyTransactionId,
    TransactionIdTooLong(usize),
    EmptyWriterOwnerId,
    WriterOwnerIdTooLong(usize),
    EmptyWriterDomainId,
    WriterDomainIdTooLong(usize),
    InvalidPlanDigestLength(usize),
    InvalidRetainedTransactionRow(usize),
    PayloadTooLong(usize),
    BodyTooLong(usize),
    LengthOverflow,
    InvalidMagic {
        offset: usize,
    },
    UnsupportedVersion {
        offset: usize,
        version: u16,
    },
    InvalidHeaderLength {
        offset: usize,
        length: usize,
    },
    InvalidHeaderChecksum {
        offset: usize,
    },
    InvalidFrameLength {
        offset: usize,
        length: usize,
    },
    InvalidBodyLength {
        offset: usize,
        length: usize,
    },
    InvalidFrameChecksum {
        offset: usize,
    },
    InvalidTrailer {
        offset: usize,
    },
    InvalidTrailerLength {
        offset: usize,
        length: usize,
    },
    InvalidPhase(u8),
    InvalidOperationKind(u8),
    InvalidReservedBits(u16),
    InvalidUtf8TransactionId,
    InvalidUtf8WriterOwnerId,
    InvalidUtf8WriterDomainId,
    TrailingBodyBytes,
    RecordLsnOverflow(u64),
    RecordLsnMismatch {
        offset: usize,
        expected: u64,
        actual: u64,
    },
    ProgramIdMismatch {
        offset: usize,
    },
    WriterDomainIdMismatch {
        offset: usize,
    },
    WriterEpochRegression {
        offset: usize,
        previous: u64,
        actual: u64,
    },
    WriterEpochAhead {
        offset: usize,
        current: u64,
        actual: u64,
    },
    WriterOwnerIdConflict {
        offset: usize,
        epoch: u64,
    },
    NewTransactionSequenceNotIncreasing {
        offset: usize,
        previous: u64,
        actual: u64,
    },
    TransactionSequenceConflict {
        offset: usize,
        sequence: u64,
    },
    TransactionIdConflict {
        offset: usize,
        transaction_id: String,
    },
    PlanDigestConflict {
        offset: usize,
        transaction_id: String,
    },
    OperationKindConflict {
        offset: usize,
        transaction_id: String,
    },
    InvalidPhaseTransition {
        offset: usize,
        transaction_id: String,
        previous: Option<DurabilityPhase>,
        next: DurabilityPhase,
    },
    TruncatedBodyField(&'static str),
}

impl fmt::Display for DurabilityJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        use DurabilityJournalError::*;
        match self {
            InvalidRecordLsn(value) => write!(formatter, "invalid record LSN {value}"),
            InvalidTransactionSequence(value) => {
                write!(formatter, "invalid transaction sequence {value}")
            }
            InvalidWriterEpoch(value) => write!(formatter, "invalid writer epoch {value}"),
            InvalidDecimalU64(label) => write!(formatter, "invalid decimal u64 {label}"),
            EmptyProgramId => formatter.write_str("program id must not be empty"),
            ProgramIdTooLong(length) => write!(formatter, "program id too long: {length}"),
            EmptyTransactionId => formatter.write_str("transaction id must not be empty"),
            TransactionIdTooLong(length) => {
                write!(formatter, "transaction id too long: {length}")
            }
            EmptyWriterOwnerId => formatter.write_str("writer owner id must not be empty"),
            WriterOwnerIdTooLong(length) => {
                write!(formatter, "writer owner id too long: {length}")
            }
            EmptyWriterDomainId => formatter.write_str("writer domain id must not be empty"),
            WriterDomainIdTooLong(length) => {
                write!(formatter, "writer domain id too long: {length}")
            }
            InvalidPlanDigestLength(length) => {
                write!(formatter, "invalid plan digest length: {length}")
            }
            InvalidRetainedTransactionRow(index) => {
                write!(formatter, "invalid retained transaction row at {index}")
            }
            PayloadTooLong(length) => write!(formatter, "journal payload too long: {length}"),
            BodyTooLong(length) => write!(formatter, "journal body too long: {length}"),
            LengthOverflow => formatter.write_str("journal frame length overflow"),
            InvalidMagic { offset } => write!(formatter, "invalid journal magic at {offset}"),
            UnsupportedVersion { offset, version } => {
                write!(formatter, "unsupported journal version {version} at {offset}")
            }
            InvalidHeaderLength { offset, length } => {
                write!(formatter, "invalid journal header length {length} at {offset}")
            }
            InvalidHeaderChecksum { offset } => {
                write!(formatter, "invalid journal header checksum at {offset}")
            }
            InvalidFrameLength { offset, length } => {
                write!(formatter, "invalid journal frame length {length} at {offset}")
            }
            InvalidBodyLength { offset, length } => {
                write!(formatter, "invalid journal body length {length} at {offset}")
            }
            InvalidFrameChecksum { offset } => {
                write!(formatter, "invalid journal frame checksum at {offset}")
            }
            InvalidTrailer { offset } => write!(formatter, "invalid journal trailer at {offset}"),
            InvalidTrailerLength { offset, length } => {
                write!(formatter, "invalid journal trailer length {length} at {offset}")
            }
            InvalidPhase(value) => write!(formatter, "invalid durability phase {value}"),
            InvalidOperationKind(value) => {
                write!(formatter, "invalid durability operation kind {value}")
            }
            InvalidReservedBits(value) => {
                write!(formatter, "invalid journal reserved bits {value}")
            }
            InvalidUtf8TransactionId => formatter.write_str("invalid utf-8 transaction id"),
            InvalidUtf8WriterOwnerId => formatter.write_str("invalid utf-8 writer owner id"),
            InvalidUtf8WriterDomainId => formatter.write_str("invalid utf-8 writer domain id"),
            TrailingBodyBytes => formatter.write_str("trailing journal body bytes"),
            RecordLsnOverflow(value) => write!(formatter, "record LSN overflow after {value}"),
            RecordLsnMismatch { offset, expected, actual } => write!(
                formatter,
                "record LSN mismatch at {offset}: expected {expected}, got {actual}"
            ),
            ProgramIdMismatch { offset } => {
                write!(formatter, "program id mismatch at {offset}")
            }
            WriterDomainIdMismatch { offset } => {
                write!(formatter, "writer domain id mismatch at {offset}")
            }
            WriterEpochRegression {
                offset,
                previous,
                actual,
            } => write!(
                formatter,
                "writer epoch regressed at {offset}: previous {previous}, got {actual}"
            ),
            WriterEpochAhead {
                offset,
                current,
                actual,
            } => write!(
                formatter,
                "writer epoch exceeds current fence at {offset}: current {current}, got {actual}"
            ),
            WriterOwnerIdConflict { offset, epoch } => write!(
                formatter,
                "writer owner id conflicts in epoch {epoch} at {offset}"
            ),
            NewTransactionSequenceNotIncreasing {
                offset,
                previous,
                actual,
            } => write!(
                formatter,
                "new transaction sequence did not increase at {offset}: previous {previous}, got {actual}"
            ),
            TransactionSequenceConflict { offset, sequence } => write!(
                formatter,
                "transaction sequence {sequence} conflicts at {offset}"
            ),
            TransactionIdConflict { offset, transaction_id } => write!(
                formatter,
                "transaction id {transaction_id} conflicts at {offset}"
            ),
            PlanDigestConflict { offset, transaction_id } => write!(
                formatter,
                "transaction id {transaction_id} changed plan digest at {offset}"
            ),
            OperationKindConflict { offset, transaction_id } => write!(
                formatter,
                "transaction id {transaction_id} changed operation kind at {offset}"
            ),
            InvalidPhaseTransition {
                offset,
                transaction_id,
                previous,
                next,
            } => write!(
                formatter,
                "invalid phase transition for {transaction_id} at {offset}: {previous:?} -> {next:?}"
            ),
            TruncatedBodyField(label) => write!(formatter, "truncated journal body {label}"),
        }
    }
}

impl std::error::Error for DurabilityJournalError {}

impl DurabilityJournalError {
    pub fn code(&self) -> &'static str {
        use DurabilityJournalError::*;
        match self {
            InvalidRecordLsn(_) => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_RECORD_LSN",
            InvalidTransactionSequence(_) => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_TX_SEQUENCE",
            InvalidWriterEpoch(_) => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_WRITER_EPOCH",
            InvalidDecimalU64(_) => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_DECIMAL_U64",
            EmptyProgramId => "ERR_NATIVE_DURABILITY_JOURNAL_EMPTY_PROGRAM_ID",
            ProgramIdTooLong(_) => "ERR_NATIVE_DURABILITY_JOURNAL_PROGRAM_ID_TOO_LONG",
            EmptyTransactionId => "ERR_NATIVE_DURABILITY_JOURNAL_EMPTY_TX_ID",
            TransactionIdTooLong(_) => "ERR_NATIVE_DURABILITY_JOURNAL_TX_ID_TOO_LONG",
            EmptyWriterOwnerId => "ERR_NATIVE_DURABILITY_JOURNAL_EMPTY_WRITER_OWNER_ID",
            WriterOwnerIdTooLong(_) => "ERR_NATIVE_DURABILITY_JOURNAL_WRITER_OWNER_ID_TOO_LONG",
            EmptyWriterDomainId => "ERR_NATIVE_DURABILITY_JOURNAL_EMPTY_WRITER_DOMAIN_ID",
            WriterDomainIdTooLong(_) => "ERR_NATIVE_DURABILITY_JOURNAL_WRITER_DOMAIN_ID_TOO_LONG",
            InvalidPlanDigestLength(_) => {
                "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_PLAN_DIGEST_LENGTH"
            }
            InvalidRetainedTransactionRow(_) => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_RETAINED_TX",
            PayloadTooLong(_) => "ERR_NATIVE_DURABILITY_JOURNAL_PAYLOAD_TOO_LONG",
            BodyTooLong(_) => "ERR_NATIVE_DURABILITY_JOURNAL_BODY_TOO_LONG",
            LengthOverflow => "ERR_NATIVE_DURABILITY_JOURNAL_LENGTH_OVERFLOW",
            InvalidMagic { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_MAGIC",
            UnsupportedVersion { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_UNSUPPORTED_VERSION",
            InvalidHeaderLength { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_HEADER_LENGTH",
            InvalidHeaderChecksum { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_HEADER_CHECKSUM",
            InvalidFrameLength { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_FRAME_LENGTH",
            InvalidBodyLength { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_BODY_LENGTH",
            InvalidFrameChecksum { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_FRAME_CHECKSUM",
            InvalidTrailer { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_TRAILER",
            InvalidTrailerLength { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_TRAILER_LENGTH",
            InvalidPhase(_) => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_PHASE",
            InvalidOperationKind(_) => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_OPERATION_KIND",
            InvalidReservedBits(_) => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_RESERVED_BITS",
            InvalidUtf8TransactionId => "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_TX_ID_UTF8",
            InvalidUtf8WriterOwnerId => {
                "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_WRITER_OWNER_ID_UTF8"
            }
            InvalidUtf8WriterDomainId => {
                "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_WRITER_DOMAIN_ID_UTF8"
            }
            TrailingBodyBytes => "ERR_NATIVE_DURABILITY_JOURNAL_TRAILING_BODY_BYTES",
            RecordLsnOverflow(_) => "ERR_NATIVE_DURABILITY_JOURNAL_RECORD_LSN_OVERFLOW",
            RecordLsnMismatch { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_RECORD_LSN_MISMATCH",
            ProgramIdMismatch { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_PROGRAM_ID_MISMATCH",
            WriterDomainIdMismatch { .. } => {
                "ERR_NATIVE_DURABILITY_JOURNAL_WRITER_DOMAIN_ID_MISMATCH"
            }
            WriterEpochRegression { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_WRITER_EPOCH_REGRESSION",
            WriterEpochAhead { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_WRITER_EPOCH_AHEAD",
            WriterOwnerIdConflict { .. } => {
                "ERR_NATIVE_DURABILITY_JOURNAL_WRITER_OWNER_ID_CONFLICT"
            }
            NewTransactionSequenceNotIncreasing { .. } => {
                "ERR_NATIVE_DURABILITY_JOURNAL_TX_SEQUENCE_NOT_INCREASING"
            }
            TransactionSequenceConflict { .. } => {
                "ERR_NATIVE_DURABILITY_JOURNAL_TX_SEQUENCE_CONFLICT"
            }
            TransactionIdConflict { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_TX_ID_CONFLICT",
            PlanDigestConflict { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_PLAN_DIGEST_CONFLICT",
            OperationKindConflict { .. } => "ERR_NATIVE_DURABILITY_JOURNAL_OPERATION_KIND_CONFLICT",
            InvalidPhaseTransition { .. } => {
                "ERR_NATIVE_DURABILITY_JOURNAL_INVALID_PHASE_TRANSITION"
            }
            TruncatedBodyField(_) => "ERR_NATIVE_DURABILITY_JOURNAL_TRUNCATED_BODY_FIELD",
        }
    }

    pub fn byte_offset(&self) -> Option<usize> {
        use DurabilityJournalError::*;
        match self {
            InvalidMagic { offset }
            | UnsupportedVersion { offset, .. }
            | InvalidHeaderLength { offset, .. }
            | InvalidHeaderChecksum { offset }
            | InvalidFrameLength { offset, .. }
            | InvalidBodyLength { offset, .. }
            | InvalidFrameChecksum { offset }
            | InvalidTrailer { offset }
            | InvalidTrailerLength { offset, .. }
            | RecordLsnMismatch { offset, .. }
            | ProgramIdMismatch { offset }
            | WriterDomainIdMismatch { offset }
            | WriterEpochRegression { offset, .. }
            | WriterEpochAhead { offset, .. }
            | WriterOwnerIdConflict { offset, .. }
            | NewTransactionSequenceNotIncreasing { offset, .. }
            | TransactionSequenceConflict { offset, .. }
            | TransactionIdConflict { offset, .. }
            | PlanDigestConflict { offset, .. }
            | OperationKindConflict { offset, .. }
            | InvalidPhaseTransition { offset, .. } => Some(*offset),
            _ => None,
        }
    }
}

/// Stateless Wasm boundary for the canonical journal codec. All u64 values are
/// decimal strings so JavaScript never rounds an LSN, sequence, or fence epoch.
#[wasm_bindgen]
pub struct NativeDurabilityJournalCodec;

#[wasm_bindgen]
impl NativeDurabilityJournalCodec {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self
    }

    #[wasm_bindgen(js_name = encodeFrame)]
    #[allow(clippy::too_many_arguments)]
    pub fn encode_frame_wasm(
        &self,
        record_lsn: String,
        tx_sequence: String,
        writer_epoch: String,
        writer_owner_id: String,
        writer_domain_id: String,
        phase: u8,
        operation_kind: u8,
        program_id: Uint8Array,
        transaction_id: String,
        plan_digest: Uint8Array,
        payload: Uint8Array,
    ) -> Result<Vec<u8>, JsValue> {
        let digest = plan_digest.to_vec();
        if digest.len() != 32 {
            return Err(journal_error_to_js(
                DurabilityJournalError::InvalidPlanDigestLength(digest.len()),
            ));
        }
        let mut fixed_digest = [0u8; 32];
        fixed_digest.copy_from_slice(&digest);
        let record = DurabilityJournalRecord {
            record_lsn: parse_decimal_u64(&record_lsn, "record LSN")
                .map_err(journal_error_to_js)?,
            tx_sequence: parse_decimal_u64(&tx_sequence, "transaction sequence")
                .map_err(journal_error_to_js)?,
            writer_epoch: parse_decimal_u64(&writer_epoch, "writer epoch")
                .map_err(journal_error_to_js)?,
            writer_owner_id,
            writer_domain_id,
            phase: DurabilityPhase::try_from(phase).map_err(journal_error_to_js)?,
            operation_kind: DurabilityOperationKind::try_from(operation_kind)
                .map_err(journal_error_to_js)?,
            program_id: program_id.to_vec(),
            transaction_id,
            plan_digest: fixed_digest,
            payload: payload.to_vec(),
        };
        encode_journal_frame(&record).map_err(journal_error_to_js)
    }

    #[wasm_bindgen(js_name = scan)]
    #[allow(clippy::too_many_arguments)]
    pub fn scan_wasm(
        &self,
        bytes: Uint8Array,
        checkpoint_lsn: String,
        checkpoint_tx_sequence_highwater: String,
        expected_program_id: Uint8Array,
        expected_writer_domain_id: String,
        checkpoint_writer_epoch: String,
        checkpoint_writer_owner_id: Option<String>,
        current_writer_epoch: String,
        current_writer_owner_id: String,
        retained_transaction_rows: Array,
    ) -> Result<Array, JsValue> {
        let retained_transactions = parse_retained_transaction_rows(retained_transaction_rows)
            .map_err(journal_error_to_js)?;
        let context = DurabilityJournalValidationContext {
            checkpoint_lsn: parse_decimal_u64(&checkpoint_lsn, "checkpoint LSN")
                .map_err(journal_error_to_js)?,
            checkpoint_tx_sequence_highwater: parse_decimal_u64(
                &checkpoint_tx_sequence_highwater,
                "checkpoint transaction sequence highwater",
            )
            .map_err(journal_error_to_js)?,
            expected_program_id: expected_program_id.to_vec(),
            expected_writer_domain_id,
            checkpoint_writer_epoch: parse_decimal_u64(
                &checkpoint_writer_epoch,
                "checkpoint writer epoch",
            )
            .map_err(journal_error_to_js)?,
            checkpoint_writer_owner_id,
            current_writer_epoch: parse_decimal_u64(&current_writer_epoch, "current writer epoch")
                .map_err(journal_error_to_js)?,
            current_writer_owner_id,
            retained_transactions,
        };
        let scan = scan_journal(&bytes.to_vec(), &context).map_err(journal_error_to_js)?;
        Ok(scan_to_js(scan))
    }
}

impl Default for NativeDurabilityJournalCodec {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_decimal_u64(value: &str, label: &'static str) -> Result<u64, DurabilityJournalError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(DurabilityJournalError::InvalidDecimalU64(label));
    }
    value
        .parse::<u64>()
        .map_err(|_| DurabilityJournalError::InvalidDecimalU64(label))
}

fn parse_retained_transaction_rows(
    rows: Array,
) -> Result<Vec<DurabilityCheckpointTransactionState>, DurabilityJournalError> {
    let mut retained = Vec::with_capacity(rows.length() as usize);
    for index in 0..rows.length() {
        let row = rows
            .get(index)
            .dyn_into::<Array>()
            .map_err(|_| DurabilityJournalError::InvalidRetainedTransactionRow(index as usize))?;
        if row.length() != 5 {
            return Err(DurabilityJournalError::InvalidRetainedTransactionRow(
                index as usize,
            ));
        }
        let tx_sequence =
            row.get(0)
                .as_string()
                .ok_or(DurabilityJournalError::InvalidRetainedTransactionRow(
                    index as usize,
                ))?;
        let transaction_id =
            row.get(1)
                .as_string()
                .ok_or(DurabilityJournalError::InvalidRetainedTransactionRow(
                    index as usize,
                ))?;
        let phase = js_u8(&row.get(2))
            .and_then(DurabilityPhase::try_from)
            .map_err(|_| DurabilityJournalError::InvalidRetainedTransactionRow(index as usize))?;
        let operation_kind = js_u8(&row.get(3))
            .and_then(DurabilityOperationKind::try_from)
            .map_err(|_| DurabilityJournalError::InvalidRetainedTransactionRow(index as usize))?;
        let digest = row
            .get(4)
            .dyn_into::<Uint8Array>()
            .map_err(|_| DurabilityJournalError::InvalidRetainedTransactionRow(index as usize))?
            .to_vec();
        if digest.len() != 32 {
            return Err(DurabilityJournalError::InvalidPlanDigestLength(
                digest.len(),
            ));
        }
        let mut plan_digest = [0u8; 32];
        plan_digest.copy_from_slice(&digest);
        retained.push(DurabilityCheckpointTransactionState {
            tx_sequence: parse_decimal_u64(&tx_sequence, "retained transaction sequence")?,
            transaction_id,
            phase,
            operation_kind,
            plan_digest,
        });
    }
    Ok(retained)
}

fn js_u8(value: &JsValue) -> Result<u8, DurabilityJournalError> {
    let number = value
        .as_f64()
        .ok_or(DurabilityJournalError::InvalidRetainedTransactionRow(0))?;
    if !number.is_finite() || number.fract() != 0.0 || !(0.0..=u8::MAX as f64).contains(&number) {
        return Err(DurabilityJournalError::InvalidRetainedTransactionRow(0));
    }
    Ok(number as u8)
}

fn scan_to_js(scan: DurabilityJournalScan) -> Array {
    let result = Array::new();
    result.push(&JsValue::from_f64(scan.valid_length as f64));
    result.push(
        &scan
            .incomplete_tail_offset
            .map(|value| JsValue::from_f64(value as f64))
            .unwrap_or(JsValue::UNDEFINED),
    );
    result.push(&JsValue::from_f64(
        scan.incomplete_tail_reason
            .map(|reason| reason as u8)
            .unwrap_or(0) as f64,
    ));
    result.push(&JsValue::from_str(&scan.last_record_lsn.to_string()));
    let records = Array::new();
    for record in scan.records {
        let row = Array::new();
        row.push(&JsValue::from_str(&record.record_lsn.to_string()));
        row.push(&JsValue::from_str(&record.tx_sequence.to_string()));
        row.push(&JsValue::from_str(&record.writer_epoch.to_string()));
        row.push(&JsValue::from_str(&record.writer_owner_id));
        row.push(&JsValue::from_str(&record.writer_domain_id));
        row.push(&JsValue::from_f64(record.phase as u8 as f64));
        row.push(&JsValue::from_f64(record.operation_kind as u8 as f64));
        row.push(&Uint8Array::from(record.program_id.as_slice()));
        row.push(&JsValue::from_str(&record.transaction_id));
        row.push(&Uint8Array::from(record.plan_digest.as_slice()));
        row.push(&Uint8Array::from(record.payload.as_slice()));
        records.push(&row);
    }
    result.push(&records);
    result
}

fn journal_error_to_js(error: DurabilityJournalError) -> JsValue {
    let row = Array::new();
    row.push(&JsValue::from_str(error.code()));
    row.push(&JsValue::from_str(&error.to_string()));
    row.push(
        &error
            .byte_offset()
            .map(|value| JsValue::from_f64(value as f64))
            .unwrap_or(JsValue::UNDEFINED),
    );
    row.into()
}

pub fn encode_journal_frame(
    record: &DurabilityJournalRecord,
) -> Result<Vec<u8>, DurabilityJournalError> {
    validate_record(record)?;

    let transaction_id = record.transaction_id.as_bytes();
    let writer_owner_id = record.writer_owner_id.as_bytes();
    let writer_domain_id = record.writer_domain_id.as_bytes();
    let body_length = FIXED_BODY_LENGTH
        .checked_add(record.program_id.len())
        .and_then(|value| value.checked_add(transaction_id.len()))
        .and_then(|value| value.checked_add(writer_owner_id.len()))
        .and_then(|value| value.checked_add(writer_domain_id.len()))
        .and_then(|value| value.checked_add(record.payload.len()))
        .ok_or(DurabilityJournalError::LengthOverflow)?;
    if body_length > MAX_JOURNAL_BODY_LENGTH {
        return Err(DurabilityJournalError::BodyTooLong(body_length));
    }
    let frame_length = HEADER_LENGTH
        .checked_add(body_length)
        .and_then(|value| value.checked_add(TRAILER_LENGTH))
        .ok_or(DurabilityJournalError::LengthOverflow)?;
    let body_length_u32 =
        u32::try_from(body_length).map_err(|_| DurabilityJournalError::LengthOverflow)?;
    let frame_length_u32 =
        u32::try_from(frame_length).map_err(|_| DurabilityJournalError::LengthOverflow)?;

    let mut frame = Vec::with_capacity(frame_length);
    frame.extend_from_slice(JOURNAL_MAGIC);
    frame.extend_from_slice(&JOURNAL_FORMAT_VERSION.to_le_bytes());
    frame.extend_from_slice(&(HEADER_LENGTH as u16).to_le_bytes());
    frame.extend_from_slice(&frame_length_u32.to_le_bytes());
    frame.extend_from_slice(&body_length_u32.to_le_bytes());
    let header_checksum = Sha256::digest(&frame[..HEADER_PREFIX_LENGTH]);
    frame.extend_from_slice(&header_checksum);
    frame.extend_from_slice(&[0; CHECKSUM_LENGTH]);

    frame.extend_from_slice(&record.record_lsn.to_le_bytes());
    frame.extend_from_slice(&record.tx_sequence.to_le_bytes());
    frame.extend_from_slice(&record.writer_epoch.to_le_bytes());
    frame.push(record.phase as u8);
    frame.push(record.operation_kind as u8);
    frame.extend_from_slice(&0u16.to_le_bytes());
    frame.extend_from_slice(&(record.program_id.len() as u16).to_le_bytes());
    frame.extend_from_slice(&(transaction_id.len() as u16).to_le_bytes());
    frame.extend_from_slice(&(writer_owner_id.len() as u16).to_le_bytes());
    frame.extend_from_slice(&(writer_domain_id.len() as u16).to_le_bytes());
    frame.extend_from_slice(&record.plan_digest);
    frame.extend_from_slice(&(record.payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&record.program_id);
    frame.extend_from_slice(transaction_id);
    frame.extend_from_slice(writer_owner_id);
    frame.extend_from_slice(writer_domain_id);
    frame.extend_from_slice(&record.payload);
    frame.extend_from_slice(JOURNAL_TRAILER_MAGIC);
    frame.extend_from_slice(&frame_length_u32.to_le_bytes());

    let frame_checksum = frame_checksum(&frame);
    frame[FRAME_CHECKSUM_OFFSET..HEADER_LENGTH].copy_from_slice(&frame_checksum);
    debug_assert_eq!(frame.len(), frame_length);
    Ok(frame)
}

pub fn scan_journal(
    bytes: &[u8],
    context: &DurabilityJournalValidationContext,
) -> Result<DurabilityJournalScan, DurabilityJournalError> {
    validate_context(context)?;
    let mut records = Vec::new();
    let mut offset = 0usize;
    let mut last_record_lsn = context.checkpoint_lsn;
    let mut validator = TransactionValidator::new(context)?;

    while offset < bytes.len() {
        if last_record_lsn == u64::MAX {
            return Err(DurabilityJournalError::RecordLsnOverflow(last_record_lsn));
        }
        if bytes.len() - offset < HEADER_LENGTH {
            validate_incomplete_header_prefix(&bytes[offset..], offset)?;
            return Ok(DurabilityJournalScan {
                records,
                valid_length: offset,
                incomplete_tail_offset: Some(offset),
                incomplete_tail_reason: Some(DurabilityIncompleteTailReason::ShortHeader),
                last_record_lsn,
            });
        }

        let header = &bytes[offset..offset + HEADER_LENGTH];
        if &header[..JOURNAL_MAGIC.len()] != JOURNAL_MAGIC {
            return Err(DurabilityJournalError::InvalidMagic { offset });
        }
        let version = read_u16_at(header, 8);
        if version != JOURNAL_FORMAT_VERSION {
            return Err(DurabilityJournalError::UnsupportedVersion { offset, version });
        }
        let header_length = read_u16_at(header, 10) as usize;
        if header_length != HEADER_LENGTH {
            return Err(DurabilityJournalError::InvalidHeaderLength {
                offset,
                length: header_length,
            });
        }
        if Sha256::digest(&header[..HEADER_PREFIX_LENGTH]).as_slice()
            != &header[HEADER_CHECKSUM_OFFSET..FRAME_CHECKSUM_OFFSET]
        {
            return Err(DurabilityJournalError::InvalidHeaderChecksum { offset });
        }

        let frame_length = read_u32_at(header, 12) as usize;
        let body_length = read_u32_at(header, 16) as usize;
        if body_length > MAX_JOURNAL_BODY_LENGTH {
            return Err(DurabilityJournalError::InvalidBodyLength {
                offset,
                length: body_length,
            });
        }
        let expected_frame_length = HEADER_LENGTH
            .checked_add(body_length)
            .and_then(|value| value.checked_add(TRAILER_LENGTH))
            .ok_or(DurabilityJournalError::LengthOverflow)?;
        if frame_length != expected_frame_length {
            return Err(DurabilityJournalError::InvalidFrameLength {
                offset,
                length: frame_length,
            });
        }
        let frame_end = offset
            .checked_add(frame_length)
            .ok_or(DurabilityJournalError::LengthOverflow)?;
        if frame_end > bytes.len() {
            let body_end = offset
                .checked_add(HEADER_LENGTH)
                .and_then(|value| value.checked_add(body_length))
                .ok_or(DurabilityJournalError::LengthOverflow)?;
            return Ok(DurabilityJournalScan {
                records,
                valid_length: offset,
                incomplete_tail_offset: Some(offset),
                incomplete_tail_reason: Some(if bytes.len() < body_end {
                    DurabilityIncompleteTailReason::ShortBody
                } else {
                    DurabilityIncompleteTailReason::ShortTrailer
                }),
                last_record_lsn,
            });
        }

        let frame = &bytes[offset..frame_end];
        let trailer_offset = frame_length - TRAILER_LENGTH;
        if &frame[trailer_offset..trailer_offset + JOURNAL_TRAILER_MAGIC.len()]
            != JOURNAL_TRAILER_MAGIC
        {
            return Err(DurabilityJournalError::InvalidTrailer { offset });
        }
        let trailer_frame_length = read_u32_at(frame, trailer_offset + 8) as usize;
        if trailer_frame_length != frame_length {
            return Err(DurabilityJournalError::InvalidTrailerLength {
                offset,
                length: trailer_frame_length,
            });
        }
        if frame_checksum(frame).as_slice() != &frame[FRAME_CHECKSUM_OFFSET..HEADER_LENGTH] {
            return Err(DurabilityJournalError::InvalidFrameChecksum { offset });
        }

        let body = &frame[HEADER_LENGTH..trailer_offset];
        let record = decode_record_body(body)?;
        let expected_lsn = last_record_lsn
            .checked_add(1)
            .ok_or(DurabilityJournalError::RecordLsnOverflow(last_record_lsn))?;
        if record.record_lsn != expected_lsn {
            return Err(DurabilityJournalError::RecordLsnMismatch {
                offset,
                expected: expected_lsn,
                actual: record.record_lsn,
            });
        }
        validator.validate(&record, offset)?;
        records.push(record);
        offset = frame_end;
        last_record_lsn = expected_lsn;
    }

    Ok(DurabilityJournalScan {
        records,
        valid_length: offset,
        incomplete_tail_offset: None,
        incomplete_tail_reason: None,
        last_record_lsn,
    })
}

fn validate_context(
    context: &DurabilityJournalValidationContext,
) -> Result<(), DurabilityJournalError> {
    if context.expected_program_id.is_empty() {
        return Err(DurabilityJournalError::EmptyProgramId);
    }
    if context.expected_program_id.len() > MAX_PROGRAM_ID_LENGTH {
        return Err(DurabilityJournalError::ProgramIdTooLong(
            context.expected_program_id.len(),
        ));
    }
    if context.expected_writer_domain_id.is_empty() {
        return Err(DurabilityJournalError::EmptyWriterDomainId);
    }
    if context.expected_writer_domain_id.len() > MAX_WRITER_DOMAIN_ID_LENGTH {
        return Err(DurabilityJournalError::WriterDomainIdTooLong(
            context.expected_writer_domain_id.len(),
        ));
    }
    if context.current_writer_epoch == 0
        || context.checkpoint_writer_epoch > context.current_writer_epoch
    {
        return Err(DurabilityJournalError::InvalidWriterEpoch(
            context.current_writer_epoch,
        ));
    }
    if context.checkpoint_writer_epoch > 0 && context.checkpoint_writer_owner_id.is_none() {
        return Err(DurabilityJournalError::EmptyWriterOwnerId);
    }
    if let Some(owner_id) = &context.checkpoint_writer_owner_id {
        if owner_id.is_empty() {
            return Err(DurabilityJournalError::EmptyWriterOwnerId);
        }
        if owner_id.len() > MAX_WRITER_OWNER_ID_LENGTH {
            return Err(DurabilityJournalError::WriterOwnerIdTooLong(owner_id.len()));
        }
    }
    if context.current_writer_owner_id.is_empty() {
        return Err(DurabilityJournalError::EmptyWriterOwnerId);
    }
    if context.current_writer_owner_id.len() > MAX_WRITER_OWNER_ID_LENGTH {
        return Err(DurabilityJournalError::WriterOwnerIdTooLong(
            context.current_writer_owner_id.len(),
        ));
    }
    Ok(())
}

fn validate_incomplete_header_prefix(
    bytes: &[u8],
    offset: usize,
) -> Result<(), DurabilityJournalError> {
    let magic_prefix_length = bytes.len().min(JOURNAL_MAGIC.len());
    if bytes[..magic_prefix_length] != JOURNAL_MAGIC[..magic_prefix_length] {
        return Err(DurabilityJournalError::InvalidMagic { offset });
    }
    if bytes.len() > JOURNAL_MAGIC.len() {
        let version_bytes = JOURNAL_FORMAT_VERSION.to_le_bytes();
        let available = (bytes.len() - JOURNAL_MAGIC.len()).min(version_bytes.len());
        if bytes[JOURNAL_MAGIC.len()..JOURNAL_MAGIC.len() + available] != version_bytes[..available]
        {
            let version = if available == 2 {
                read_u16_at(bytes, JOURNAL_MAGIC.len())
            } else {
                bytes[JOURNAL_MAGIC.len()] as u16
            };
            return Err(DurabilityJournalError::UnsupportedVersion { offset, version });
        }
    }
    if bytes.len() > JOURNAL_MAGIC.len() + 2 {
        let header_length_bytes = (HEADER_LENGTH as u16).to_le_bytes();
        let start = JOURNAL_MAGIC.len() + 2;
        let available = (bytes.len() - start).min(header_length_bytes.len());
        if bytes[start..start + available] != header_length_bytes[..available] {
            let length = if available == 2 {
                read_u16_at(bytes, start) as usize
            } else {
                bytes[start] as usize
            };
            return Err(DurabilityJournalError::InvalidHeaderLength { offset, length });
        }
    }
    if bytes.len() >= HEADER_PREFIX_LENGTH {
        let frame_length = read_u32_at(bytes, 12) as usize;
        let body_length = read_u32_at(bytes, 16) as usize;
        if body_length > MAX_JOURNAL_BODY_LENGTH {
            return Err(DurabilityJournalError::InvalidBodyLength {
                offset,
                length: body_length,
            });
        }
        let expected_frame_length = HEADER_LENGTH
            .checked_add(body_length)
            .and_then(|value| value.checked_add(TRAILER_LENGTH))
            .ok_or(DurabilityJournalError::LengthOverflow)?;
        if frame_length != expected_frame_length {
            return Err(DurabilityJournalError::InvalidFrameLength {
                offset,
                length: frame_length,
            });
        }
    }
    if bytes.len() >= FRAME_CHECKSUM_OFFSET
        && Sha256::digest(&bytes[..HEADER_PREFIX_LENGTH]).as_slice()
            != &bytes[HEADER_CHECKSUM_OFFSET..FRAME_CHECKSUM_OFFSET]
    {
        return Err(DurabilityJournalError::InvalidHeaderChecksum { offset });
    }
    Ok(())
}

fn validate_record(record: &DurabilityJournalRecord) -> Result<(), DurabilityJournalError> {
    if record.record_lsn == 0 {
        return Err(DurabilityJournalError::InvalidRecordLsn(record.record_lsn));
    }
    if record.tx_sequence == 0 {
        return Err(DurabilityJournalError::InvalidTransactionSequence(
            record.tx_sequence,
        ));
    }
    if record.writer_epoch == 0 {
        return Err(DurabilityJournalError::InvalidWriterEpoch(
            record.writer_epoch,
        ));
    }
    if record.program_id.is_empty() {
        return Err(DurabilityJournalError::EmptyProgramId);
    }
    if record.program_id.len() > MAX_PROGRAM_ID_LENGTH {
        return Err(DurabilityJournalError::ProgramIdTooLong(
            record.program_id.len(),
        ));
    }
    if record.transaction_id.is_empty() {
        return Err(DurabilityJournalError::EmptyTransactionId);
    }
    if record.transaction_id.len() > MAX_TRANSACTION_ID_LENGTH {
        return Err(DurabilityJournalError::TransactionIdTooLong(
            record.transaction_id.len(),
        ));
    }
    if record.writer_owner_id.is_empty() {
        return Err(DurabilityJournalError::EmptyWriterOwnerId);
    }
    if record.writer_owner_id.len() > MAX_WRITER_OWNER_ID_LENGTH {
        return Err(DurabilityJournalError::WriterOwnerIdTooLong(
            record.writer_owner_id.len(),
        ));
    }
    if record.writer_domain_id.is_empty() {
        return Err(DurabilityJournalError::EmptyWriterDomainId);
    }
    if record.writer_domain_id.len() > MAX_WRITER_DOMAIN_ID_LENGTH {
        return Err(DurabilityJournalError::WriterDomainIdTooLong(
            record.writer_domain_id.len(),
        ));
    }
    if record.payload.len() > MAX_JOURNAL_BODY_LENGTH {
        return Err(DurabilityJournalError::PayloadTooLong(record.payload.len()));
    }
    Ok(())
}

fn decode_record_body(body: &[u8]) -> Result<DurabilityJournalRecord, DurabilityJournalError> {
    let mut offset = 0usize;
    let record_lsn = read_u64(body, &mut offset, "record LSN")?;
    let tx_sequence = read_u64(body, &mut offset, "transaction sequence")?;
    let writer_epoch = read_u64(body, &mut offset, "writer epoch")?;
    let phase = DurabilityPhase::try_from(read_u8(body, &mut offset, "phase")?)?;
    let operation_kind =
        DurabilityOperationKind::try_from(read_u8(body, &mut offset, "operation kind")?)?;
    let reserved = read_u16(body, &mut offset, "reserved bits")?;
    if reserved != 0 {
        return Err(DurabilityJournalError::InvalidReservedBits(reserved));
    }
    let program_id_length = read_u16(body, &mut offset, "program id length")? as usize;
    let transaction_id_length = read_u16(body, &mut offset, "transaction id length")? as usize;
    let writer_owner_id_length = read_u16(body, &mut offset, "writer owner id length")? as usize;
    let writer_domain_id_length = read_u16(body, &mut offset, "writer domain id length")? as usize;
    if program_id_length == 0 {
        return Err(DurabilityJournalError::EmptyProgramId);
    }
    if program_id_length > MAX_PROGRAM_ID_LENGTH {
        return Err(DurabilityJournalError::ProgramIdTooLong(program_id_length));
    }
    if transaction_id_length == 0 {
        return Err(DurabilityJournalError::EmptyTransactionId);
    }
    if transaction_id_length > MAX_TRANSACTION_ID_LENGTH {
        return Err(DurabilityJournalError::TransactionIdTooLong(
            transaction_id_length,
        ));
    }
    if writer_owner_id_length == 0 {
        return Err(DurabilityJournalError::EmptyWriterOwnerId);
    }
    if writer_owner_id_length > MAX_WRITER_OWNER_ID_LENGTH {
        return Err(DurabilityJournalError::WriterOwnerIdTooLong(
            writer_owner_id_length,
        ));
    }
    if writer_domain_id_length == 0 {
        return Err(DurabilityJournalError::EmptyWriterDomainId);
    }
    if writer_domain_id_length > MAX_WRITER_DOMAIN_ID_LENGTH {
        return Err(DurabilityJournalError::WriterDomainIdTooLong(
            writer_domain_id_length,
        ));
    }
    let digest = take(body, &mut offset, 32, "plan digest")?;
    let mut plan_digest = [0u8; 32];
    plan_digest.copy_from_slice(digest);
    let payload_length = read_u32(body, &mut offset, "payload length")? as usize;
    if payload_length > MAX_JOURNAL_BODY_LENGTH {
        return Err(DurabilityJournalError::PayloadTooLong(payload_length));
    }
    let program_id = take(body, &mut offset, program_id_length, "program id")?.to_vec();
    let transaction_id = String::from_utf8(
        take(body, &mut offset, transaction_id_length, "transaction id")?.to_vec(),
    )
    .map_err(|_| DurabilityJournalError::InvalidUtf8TransactionId)?;
    let writer_owner_id = String::from_utf8(
        take(body, &mut offset, writer_owner_id_length, "writer owner id")?.to_vec(),
    )
    .map_err(|_| DurabilityJournalError::InvalidUtf8WriterOwnerId)?;
    let writer_domain_id = String::from_utf8(
        take(
            body,
            &mut offset,
            writer_domain_id_length,
            "writer domain id",
        )?
        .to_vec(),
    )
    .map_err(|_| DurabilityJournalError::InvalidUtf8WriterDomainId)?;
    let payload = take(body, &mut offset, payload_length, "payload")?.to_vec();
    if offset != body.len() {
        return Err(DurabilityJournalError::TrailingBodyBytes);
    }
    let record = DurabilityJournalRecord {
        record_lsn,
        tx_sequence,
        writer_epoch,
        writer_owner_id,
        writer_domain_id,
        phase,
        operation_kind,
        program_id,
        transaction_id,
        plan_digest,
        payload,
    };
    validate_record(&record)?;
    Ok(record)
}

#[derive(Clone)]
struct TransactionState {
    tx_sequence: u64,
    phase: DurabilityPhase,
    operation_kind: DurabilityOperationKind,
    plan_digest: [u8; 32],
}

struct TransactionValidator {
    program_id: Option<Vec<u8>>,
    writer_domain_id: Option<String>,
    writer_epoch: Option<u64>,
    writer_owner_id: Option<String>,
    current_writer_epoch: u64,
    current_writer_owner_id: String,
    last_new_tx_sequence: Option<u64>,
    by_id: HashMap<String, TransactionState>,
    id_by_sequence: HashMap<u64, String>,
}

impl TransactionValidator {
    fn new(context: &DurabilityJournalValidationContext) -> Result<Self, DurabilityJournalError> {
        let mut validator = Self {
            program_id: Some(context.expected_program_id.clone()),
            writer_domain_id: Some(context.expected_writer_domain_id.clone()),
            writer_epoch: Some(context.checkpoint_writer_epoch),
            writer_owner_id: context.checkpoint_writer_owner_id.clone(),
            current_writer_epoch: context.current_writer_epoch,
            current_writer_owner_id: context.current_writer_owner_id.clone(),
            last_new_tx_sequence: Some(context.checkpoint_tx_sequence_highwater),
            by_id: HashMap::new(),
            id_by_sequence: HashMap::new(),
        };
        for retained in &context.retained_transactions {
            if retained.tx_sequence == 0
                || retained.tx_sequence > context.checkpoint_tx_sequence_highwater
            {
                return Err(DurabilityJournalError::InvalidTransactionSequence(
                    retained.tx_sequence,
                ));
            }
            if retained.transaction_id.is_empty() {
                return Err(DurabilityJournalError::EmptyTransactionId);
            }
            if retained.transaction_id.len() > MAX_TRANSACTION_ID_LENGTH {
                return Err(DurabilityJournalError::TransactionIdTooLong(
                    retained.transaction_id.len(),
                ));
            }
            if validator.by_id.contains_key(&retained.transaction_id) {
                return Err(DurabilityJournalError::TransactionIdConflict {
                    offset: 0,
                    transaction_id: retained.transaction_id.clone(),
                });
            }
            if validator.id_by_sequence.contains_key(&retained.tx_sequence) {
                return Err(DurabilityJournalError::TransactionSequenceConflict {
                    offset: 0,
                    sequence: retained.tx_sequence,
                });
            }
            validator
                .id_by_sequence
                .insert(retained.tx_sequence, retained.transaction_id.clone());
            validator.by_id.insert(
                retained.transaction_id.clone(),
                TransactionState {
                    tx_sequence: retained.tx_sequence,
                    phase: retained.phase,
                    operation_kind: retained.operation_kind,
                    plan_digest: retained.plan_digest,
                },
            );
        }
        Ok(validator)
    }

    fn validate(
        &mut self,
        record: &DurabilityJournalRecord,
        offset: usize,
    ) -> Result<(), DurabilityJournalError> {
        if let Some(program_id) = &self.program_id {
            if program_id != &record.program_id {
                return Err(DurabilityJournalError::ProgramIdMismatch { offset });
            }
        } else {
            self.program_id = Some(record.program_id.clone());
        }

        if let Some(writer_domain_id) = &self.writer_domain_id {
            if writer_domain_id != &record.writer_domain_id {
                return Err(DurabilityJournalError::WriterDomainIdMismatch { offset });
            }
        } else {
            self.writer_domain_id = Some(record.writer_domain_id.clone());
        }

        if record.writer_epoch > self.current_writer_epoch {
            return Err(DurabilityJournalError::WriterEpochAhead {
                offset,
                current: self.current_writer_epoch,
                actual: record.writer_epoch,
            });
        }
        if record.writer_epoch == self.current_writer_epoch
            && record.writer_owner_id != self.current_writer_owner_id
        {
            return Err(DurabilityJournalError::WriterOwnerIdConflict {
                offset,
                epoch: record.writer_epoch,
            });
        }
        if let Some(previous_epoch) = self.writer_epoch {
            if record.writer_epoch < previous_epoch {
                return Err(DurabilityJournalError::WriterEpochRegression {
                    offset,
                    previous: previous_epoch,
                    actual: record.writer_epoch,
                });
            }
            if record.writer_epoch == previous_epoch {
                if let Some(previous_owner_id) = &self.writer_owner_id {
                    if previous_owner_id != &record.writer_owner_id {
                        return Err(DurabilityJournalError::WriterOwnerIdConflict {
                            offset,
                            epoch: record.writer_epoch,
                        });
                    }
                } else {
                    self.writer_owner_id = Some(record.writer_owner_id.clone());
                }
            }
            if record.writer_epoch > previous_epoch {
                self.writer_epoch = Some(record.writer_epoch);
                self.writer_owner_id = Some(record.writer_owner_id.clone());
            }
        } else {
            self.writer_epoch = Some(record.writer_epoch);
            self.writer_owner_id = Some(record.writer_owner_id.clone());
        }

        if let Some(existing_id) = self.id_by_sequence.get(&record.tx_sequence) {
            if existing_id != &record.transaction_id {
                return Err(DurabilityJournalError::TransactionSequenceConflict {
                    offset,
                    sequence: record.tx_sequence,
                });
            }
        }

        if let Some(previous) = self.by_id.get_mut(&record.transaction_id) {
            if previous.tx_sequence != record.tx_sequence {
                return Err(DurabilityJournalError::TransactionIdConflict {
                    offset,
                    transaction_id: record.transaction_id.clone(),
                });
            }
            if previous.plan_digest != record.plan_digest {
                return Err(DurabilityJournalError::PlanDigestConflict {
                    offset,
                    transaction_id: record.transaction_id.clone(),
                });
            }
            if previous.operation_kind != record.operation_kind {
                return Err(DurabilityJournalError::OperationKindConflict {
                    offset,
                    transaction_id: record.transaction_id.clone(),
                });
            }
            if !valid_transition(previous.phase, record.phase) {
                return Err(DurabilityJournalError::InvalidPhaseTransition {
                    offset,
                    transaction_id: record.transaction_id.clone(),
                    previous: Some(previous.phase),
                    next: record.phase,
                });
            }
            previous.phase = record.phase;
            return Ok(());
        }

        if record.phase != DurabilityPhase::DurablePrepared {
            return Err(DurabilityJournalError::InvalidPhaseTransition {
                offset,
                transaction_id: record.transaction_id.clone(),
                previous: None,
                next: record.phase,
            });
        }
        if let Some(previous_sequence) = self.last_new_tx_sequence {
            if record.tx_sequence <= previous_sequence {
                return Err(
                    DurabilityJournalError::NewTransactionSequenceNotIncreasing {
                        offset,
                        previous: previous_sequence,
                        actual: record.tx_sequence,
                    },
                );
            }
        }
        self.last_new_tx_sequence = Some(record.tx_sequence);
        self.id_by_sequence
            .insert(record.tx_sequence, record.transaction_id.clone());
        self.by_id.insert(
            record.transaction_id.clone(),
            TransactionState {
                tx_sequence: record.tx_sequence,
                phase: record.phase,
                operation_kind: record.operation_kind,
                plan_digest: record.plan_digest,
            },
        );
        Ok(())
    }
}

fn valid_transition(previous: DurabilityPhase, next: DurabilityPhase) -> bool {
    matches!(
        (previous, next),
        (
            DurabilityPhase::DurablePrepared,
            DurabilityPhase::NativeApplied
        ) | (DurabilityPhase::NativeApplied, DurabilityPhase::Published)
            | (DurabilityPhase::Published, DurabilityPhase::Committed)
            | (DurabilityPhase::Committed, DurabilityPhase::CleanupPending)
            | (DurabilityPhase::Committed, DurabilityPhase::Clean)
            | (DurabilityPhase::CleanupPending, DurabilityPhase::Clean)
    )
}

fn frame_checksum(frame: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(&frame[..FRAME_CHECKSUM_OFFSET]);
    hasher.update(&frame[HEADER_LENGTH..]);
    hasher.finalize().into()
}

fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("fixed header"))
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed frame"))
}

fn read_u8(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<u8, DurabilityJournalError> {
    Ok(*take(bytes, offset, 1, label)?
        .first()
        .expect("one byte requested"))
}

fn read_u16(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<u16, DurabilityJournalError> {
    Ok(u16::from_le_bytes(
        take(bytes, offset, 2, label)?
            .try_into()
            .expect("two bytes requested"),
    ))
}

fn read_u32(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<u32, DurabilityJournalError> {
    Ok(u32::from_le_bytes(
        take(bytes, offset, 4, label)?
            .try_into()
            .expect("four bytes requested"),
    ))
}

fn read_u64(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<u64, DurabilityJournalError> {
    Ok(u64::from_le_bytes(
        take(bytes, offset, 8, label)?
            .try_into()
            .expect("eight bytes requested"),
    ))
}

fn take<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    length: usize,
    label: &'static str,
) -> Result<&'a [u8], DurabilityJournalError> {
    let end = offset
        .checked_add(length)
        .ok_or(DurabilityJournalError::LengthOverflow)?;
    if end > bytes.len() {
        return Err(DurabilityJournalError::TruncatedBodyField(label));
    }
    let value = &bytes[*offset..end];
    *offset = end;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        record_lsn: u64,
        tx_sequence: u64,
        transaction_id: &str,
        phase: DurabilityPhase,
    ) -> DurabilityJournalRecord {
        DurabilityJournalRecord {
            record_lsn,
            tx_sequence,
            writer_epoch: 1,
            writer_owner_id: "writer-1".to_string(),
            writer_domain_id: "program-directory".to_string(),
            phase,
            operation_kind: DurabilityOperationKind::Append,
            program_id: vec![7, 8, 9],
            transaction_id: transaction_id.to_string(),
            plan_digest: [tx_sequence as u8; 32],
            payload: vec![phase as u8, 42],
        }
    }

    fn encoded(records: &[DurabilityJournalRecord]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for record in records {
            bytes.extend_from_slice(&encode_journal_frame(record).unwrap());
        }
        bytes
    }

    fn context(checkpoint_lsn: u64) -> DurabilityJournalValidationContext {
        DurabilityJournalValidationContext {
            checkpoint_lsn,
            checkpoint_tx_sequence_highwater: 0,
            expected_program_id: vec![7, 8, 9],
            expected_writer_domain_id: "program-directory".to_string(),
            checkpoint_writer_epoch: 0,
            checkpoint_writer_owner_id: None,
            current_writer_epoch: 1,
            current_writer_owner_id: "writer-1".to_string(),
            retained_transactions: Vec::new(),
        }
    }

    #[test]
    fn round_trips_records_and_validates_transitions() {
        let records = vec![
            record(1, 1, "tx-1", DurabilityPhase::DurablePrepared),
            record(2, 1, "tx-1", DurabilityPhase::NativeApplied),
            record(3, 1, "tx-1", DurabilityPhase::Published),
            record(4, 1, "tx-1", DurabilityPhase::Committed),
            record(5, 1, "tx-1", DurabilityPhase::CleanupPending),
            record(6, 2, "tx-2", DurabilityPhase::DurablePrepared),
            record(7, 2, "tx-2", DurabilityPhase::NativeApplied),
            record(8, 2, "tx-2", DurabilityPhase::Published),
            record(9, 2, "tx-2", DurabilityPhase::Committed),
            record(10, 2, "tx-2", DurabilityPhase::Clean),
            record(11, 1, "tx-1", DurabilityPhase::Clean),
        ];
        let bytes = encoded(&records);
        let scan = scan_journal(&bytes, &context(0)).unwrap();
        assert_eq!(scan.records, records);
        assert_eq!(scan.valid_length, bytes.len());
        assert_eq!(scan.incomplete_tail_offset, None);
        assert_eq!(scan.last_record_lsn, 11);
    }

    #[test]
    fn every_short_final_frame_is_an_incomplete_tail() {
        let first =
            encode_journal_frame(&record(1, 1, "tx-1", DurabilityPhase::DurablePrepared)).unwrap();
        let second =
            encode_journal_frame(&record(2, 2, "tx-2", DurabilityPhase::DurablePrepared)).unwrap();
        for cut in 0..second.len() {
            let mut bytes = first.clone();
            bytes.extend_from_slice(&second[..cut]);
            let scan = scan_journal(&bytes, &context(0)).unwrap();
            assert_eq!(scan.records.len(), 1, "cut={cut}");
            if cut == 0 {
                assert_eq!(scan.incomplete_tail_offset, None, "cut={cut}");
            } else {
                assert_eq!(scan.incomplete_tail_offset, Some(first.len()), "cut={cut}");
            }
            assert_eq!(scan.valid_length, first.len(), "cut={cut}");
        }
    }

    #[test]
    fn fully_framed_checksum_mismatch_fails_closed_even_at_tail() {
        let mut bytes =
            encode_journal_frame(&record(1, 1, "tx-1", DurabilityPhase::DurablePrepared)).unwrap();
        bytes[HEADER_LENGTH + 1] ^= 0x80;
        assert_eq!(
            scan_journal(&bytes, &context(0)),
            Err(DurabilityJournalError::InvalidFrameChecksum { offset: 0 })
        );
    }

    #[test]
    fn header_corruption_is_not_misclassified_as_a_torn_body() {
        let mut bytes =
            encode_journal_frame(&record(1, 1, "tx-1", DurabilityPhase::DurablePrepared)).unwrap();
        bytes[16] ^= 0x01;
        assert_eq!(
            scan_journal(&bytes, &context(0)),
            Err(DurabilityJournalError::InvalidHeaderChecksum { offset: 0 })
        );
    }

    #[test]
    fn lsn_gap_and_duplicate_fail_closed() {
        let gap = encoded(&[
            record(1, 1, "tx-1", DurabilityPhase::DurablePrepared),
            record(3, 1, "tx-1", DurabilityPhase::NativeApplied),
        ]);
        assert!(matches!(
            scan_journal(&gap, &context(0)),
            Err(DurabilityJournalError::RecordLsnMismatch {
                expected: 2,
                actual: 3,
                ..
            })
        ));

        let duplicate = encoded(&[
            record(1, 1, "tx-1", DurabilityPhase::DurablePrepared),
            record(1, 1, "tx-1", DurabilityPhase::NativeApplied),
        ]);
        assert!(matches!(
            scan_journal(&duplicate, &context(0)),
            Err(DurabilityJournalError::RecordLsnMismatch {
                expected: 2,
                actual: 1,
                ..
            })
        ));
    }

    #[test]
    fn invalid_phase_transition_fails_closed() {
        let bytes = encoded(&[
            record(1, 1, "tx-1", DurabilityPhase::DurablePrepared),
            record(2, 1, "tx-1", DurabilityPhase::Published),
        ]);
        assert!(matches!(
            scan_journal(&bytes, &context(0)),
            Err(DurabilityJournalError::InvalidPhaseTransition {
                previous: Some(DurabilityPhase::DurablePrepared),
                next: DurabilityPhase::Published,
                ..
            })
        ));
    }

    #[test]
    fn transaction_identity_and_digest_are_immutable() {
        let sequence_conflict = encoded(&[
            record(1, 1, "tx-1", DurabilityPhase::DurablePrepared),
            record(2, 1, "tx-other", DurabilityPhase::DurablePrepared),
        ]);
        assert!(matches!(
            scan_journal(&sequence_conflict, &context(0)),
            Err(DurabilityJournalError::TransactionSequenceConflict { .. })
        ));

        let mut changed = record(2, 1, "tx-1", DurabilityPhase::NativeApplied);
        changed.plan_digest = [99; 32];
        let digest_conflict = encoded(&[
            record(1, 1, "tx-1", DurabilityPhase::DurablePrepared),
            changed,
        ]);
        assert!(matches!(
            scan_journal(&digest_conflict, &context(0)),
            Err(DurabilityJournalError::PlanDigestConflict { .. })
        ));
    }

    #[test]
    fn continues_lsn_after_checkpoint() {
        let records = vec![record(
            u64::MAX - 1,
            1,
            "tx-1",
            DurabilityPhase::DurablePrepared,
        )];
        let scan = scan_journal(&encoded(&records), &context(u64::MAX - 2)).unwrap();
        assert_eq!(scan.last_record_lsn, u64::MAX - 1);
    }

    #[test]
    fn new_transaction_sequences_increase_but_late_clean_is_allowed() {
        let decreasing = encoded(&[
            record(1, 5, "tx-5", DurabilityPhase::DurablePrepared),
            record(2, 3, "tx-3", DurabilityPhase::DurablePrepared),
        ]);
        assert!(matches!(
            scan_journal(&decreasing, &context(0)),
            Err(
                DurabilityJournalError::NewTransactionSequenceNotIncreasing {
                    previous: 5,
                    actual: 3,
                    ..
                }
            )
        ));

        let late_clean = encoded(&[
            record(1, 1, "tx-1", DurabilityPhase::DurablePrepared),
            record(2, 1, "tx-1", DurabilityPhase::NativeApplied),
            record(3, 1, "tx-1", DurabilityPhase::Published),
            record(4, 1, "tx-1", DurabilityPhase::Committed),
            record(5, 1, "tx-1", DurabilityPhase::CleanupPending),
            record(6, 2, "tx-2", DurabilityPhase::DurablePrepared),
            record(7, 1, "tx-1", DurabilityPhase::Clean),
        ]);
        assert!(scan_journal(&late_clean, &context(0)).is_ok());
    }

    #[test]
    fn checkpoint_context_fences_program_sequence_domain_and_writer_epoch() {
        let bytes = encoded(&[record(11, 8, "tx-8", DurabilityPhase::DurablePrepared)]);
        let mut valid = context(10);
        valid.checkpoint_tx_sequence_highwater = 7;
        valid.checkpoint_writer_epoch = 1;
        valid.checkpoint_writer_owner_id = Some("writer-1".to_string());
        assert!(scan_journal(&bytes, &valid).is_ok());

        let mut stale_sequence = valid.clone();
        stale_sequence.checkpoint_tx_sequence_highwater = 8;
        assert!(matches!(
            scan_journal(&bytes, &stale_sequence),
            Err(DurabilityJournalError::NewTransactionSequenceNotIncreasing { .. })
        ));

        let mut wrong_program = valid.clone();
        wrong_program.expected_program_id = vec![1, 2, 3];
        assert!(matches!(
            scan_journal(&bytes, &wrong_program),
            Err(DurabilityJournalError::ProgramIdMismatch { .. })
        ));

        let mut wrong_domain = valid.clone();
        wrong_domain.expected_writer_domain_id = "other-domain".to_string();
        assert!(matches!(
            scan_journal(&bytes, &wrong_domain),
            Err(DurabilityJournalError::WriterDomainIdMismatch { .. })
        ));

        let mut ahead = record(11, 8, "tx-8", DurabilityPhase::DurablePrepared);
        ahead.writer_epoch = 2;
        assert!(matches!(
            scan_journal(&encoded(&[ahead]), &valid),
            Err(DurabilityJournalError::WriterEpochAhead { .. })
        ));
    }

    #[test]
    fn checkpoint_retained_transaction_can_finish_below_sequence_highwater() {
        let mut scan_context = context(10);
        scan_context.checkpoint_tx_sequence_highwater = 5;
        scan_context.checkpoint_writer_epoch = 1;
        scan_context.checkpoint_writer_owner_id = Some("writer-1".to_string());
        scan_context.retained_transactions = vec![DurabilityCheckpointTransactionState {
            tx_sequence: 1,
            transaction_id: "tx-1".to_string(),
            phase: DurabilityPhase::CleanupPending,
            operation_kind: DurabilityOperationKind::Append,
            plan_digest: [1; 32],
        }];
        let clean = record(11, 1, "tx-1", DurabilityPhase::Clean);
        assert!(scan_journal(&encoded(&[clean]), &scan_context).is_ok());

        let stale_new = record(11, 3, "tx-3", DurabilityPhase::DurablePrepared);
        assert!(matches!(
            scan_journal(&encoded(&[stale_new]), &scan_context),
            Err(DurabilityJournalError::NewTransactionSequenceNotIncreasing { .. })
        ));
    }

    #[test]
    fn writer_epochs_may_advance_but_never_regress_or_change_owner_in_epoch() {
        let mut old = record(1, 1, "tx-1", DurabilityPhase::DurablePrepared);
        old.writer_owner_id = "old-writer".to_string();
        let mut current = record(2, 1, "tx-1", DurabilityPhase::NativeApplied);
        current.writer_epoch = 2;
        current.writer_owner_id = "writer-2".to_string();
        let mut scan_context = context(0);
        scan_context.current_writer_epoch = 2;
        scan_context.current_writer_owner_id = "writer-2".to_string();
        assert!(scan_journal(&encoded(&[old.clone(), current.clone()]), &scan_context).is_ok());

        let mut regressed = record(3, 1, "tx-1", DurabilityPhase::Published);
        regressed.writer_owner_id = "old-writer".to_string();
        assert!(matches!(
            scan_journal(&encoded(&[old.clone(), current, regressed]), &scan_context),
            Err(DurabilityJournalError::WriterEpochRegression { .. })
        ));

        let mut changed_owner = record(2, 1, "tx-1", DurabilityPhase::NativeApplied);
        changed_owner.writer_owner_id = "different-writer".to_string();
        assert!(matches!(
            scan_journal(&encoded(&[old, changed_owner]), &scan_context),
            Err(DurabilityJournalError::WriterOwnerIdConflict { .. })
        ));
    }

    #[test]
    fn incomplete_header_requires_the_fixed_prefix() {
        let frame =
            encode_journal_frame(&record(1, 1, "tx-1", DurabilityPhase::DurablePrepared)).unwrap();
        for length in 1..HEADER_LENGTH {
            assert!(
                scan_journal(&frame[..length], &context(0)).is_ok(),
                "length={length}"
            );
        }

        assert!(matches!(
            scan_journal(b"X", &context(0)),
            Err(DurabilityJournalError::InvalidMagic { .. })
        ));
        let mut bad_version = JOURNAL_MAGIC.to_vec();
        bad_version.extend_from_slice(&[2]);
        assert!(matches!(
            scan_journal(&bad_version, &context(0)),
            Err(DurabilityJournalError::UnsupportedVersion { .. })
        ));
        let mut bad_header_length = JOURNAL_MAGIC.to_vec();
        bad_header_length.extend_from_slice(&JOURNAL_FORMAT_VERSION.to_le_bytes());
        bad_header_length.push(0);
        assert!(matches!(
            scan_journal(&bad_header_length, &context(0)),
            Err(DurabilityJournalError::InvalidHeaderLength { .. })
        ));

        let mut inconsistent_lengths = frame[..HEADER_PREFIX_LENGTH].to_vec();
        inconsistent_lengths[12] ^= 1;
        assert!(matches!(
            scan_journal(&inconsistent_lengths, &context(0)),
            Err(DurabilityJournalError::InvalidFrameLength { .. })
        ));

        let mut stale_checksum = frame[..FRAME_CHECKSUM_OFFSET].to_vec();
        let frame_length = read_u32_at(&stale_checksum, 12) + 1;
        let body_length = read_u32_at(&stale_checksum, 16) + 1;
        stale_checksum[12..16].copy_from_slice(&frame_length.to_le_bytes());
        stale_checksum[16..20].copy_from_slice(&body_length.to_le_bytes());
        assert!(matches!(
            scan_journal(&stale_checksum, &context(0)),
            Err(DurabilityJournalError::InvalidHeaderChecksum { .. })
        ));
    }

    #[test]
    fn every_full_frame_byte_flip_and_non_tail_corruption_fails_closed() {
        let frame =
            encode_journal_frame(&record(1, 1, "tx-1", DurabilityPhase::DurablePrepared)).unwrap();
        for index in 0..frame.len() {
            let mut corrupt = frame.clone();
            corrupt[index] ^= 1;
            assert!(
                scan_journal(&corrupt, &context(0)).is_err(),
                "byte flip at {index} was accepted"
            );
        }

        let second =
            encode_journal_frame(&record(2, 1, "tx-1", DurabilityPhase::NativeApplied)).unwrap();
        let mut corrupt_non_tail = frame;
        corrupt_non_tail[HEADER_LENGTH + 4] ^= 1;
        corrupt_non_tail.extend_from_slice(&second);
        assert!(matches!(
            scan_journal(&corrupt_non_tail, &context(0)),
            Err(DurabilityJournalError::InvalidFrameChecksum { offset: 0 })
        ));
    }

    #[test]
    fn max_lsn_is_valid_only_when_it_is_the_final_frame() {
        let final_record = record(u64::MAX, 1, "tx-1", DurabilityPhase::DurablePrepared);
        let scan = scan_journal(&encoded(&[final_record]), &context(u64::MAX - 1)).unwrap();
        assert_eq!(scan.last_record_lsn, u64::MAX);

        let empty = scan_journal(&[], &context(u64::MAX)).unwrap();
        assert_eq!(empty.last_record_lsn, u64::MAX);

        assert_eq!(
            scan_journal(JOURNAL_MAGIC, &context(u64::MAX)),
            Err(DurabilityJournalError::RecordLsnOverflow(u64::MAX))
        );
    }
}
