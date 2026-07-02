use indexmap::IndexMap;
use std::fmt;

pub const VALUE_SNAPSHOT_MAGIC: &[u8; 8] = b"PBRIDXS1";
pub const KEY_VALUE_SNAPSHOT_MAGIC: &[u8; 8] = b"PBRIDXK1";
pub const JOURNAL_MAGIC: &[u8; 8] = b"PBRIDXW1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalOperation {
    Put,
    Delete,
}

impl JournalOperation {
    fn from_byte(value: u8) -> Result<Self, DecodeError> {
        match value {
            1 => Ok(Self::Put),
            2 => Ok(Self::Delete),
            _ => Err(DecodeError::InvalidOperation(value)),
        }
    }

    fn as_byte(self) -> u8 {
        match self {
            Self::Put => 1,
            Self::Delete => 2,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRecord {
    pub operation: JournalOperation,
    pub key: String,
    pub value: Option<Vec<u8>>,
}

impl JournalRecord {
    pub fn put(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            operation: JournalOperation::Put,
            key: key.into(),
            value: Some(value.into()),
        }
    }

    pub fn delete(key: impl Into<String>) -> Self {
        Self {
            operation: JournalOperation::Delete,
            key: key.into(),
            value: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DurableIndexState {
    entries: IndexMap<String, Vec<u8>>,
    operations: usize,
}

impl DurableIndexState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_entries(entries: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
        Self {
            entries: entries.into_iter().collect(),
            operations: 0,
        }
    }

    pub fn entries(&self) -> &IndexMap<String, Vec<u8>> {
        &self.entries
    }

    pub fn operations(&self) -> usize {
        self.operations
    }

    pub fn put(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) {
        self.entries.insert(key.into(), value.into());
        self.operations += 1;
    }

    pub fn delete(&mut self, key: &str) -> bool {
        let deleted = self.entries.shift_remove(key).is_some();
        if deleted {
            self.operations += 1;
        }
        deleted
    }

    pub fn apply_journal_records(&mut self, records: impl IntoIterator<Item = JournalRecord>) {
        for record in records {
            match record.operation {
                JournalOperation::Put => {
                    if let Some(value) = record.value {
                        self.entries.insert(record.key, value);
                        self.operations += 1;
                    }
                }
                JournalOperation::Delete => {
                    self.entries.shift_remove(&record.key);
                    self.operations += 1;
                }
            }
        }
    }

    pub fn encode_snapshot(&self) -> Vec<u8> {
        encode_key_value_snapshot(
            self.entries
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_slice())),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    Truncated(&'static str),
    ChecksumMismatch,
    InvalidOperation(u8),
    InvalidUtf8,
    TrailingSnapshotBytes,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated(label) => write!(formatter, "truncated {label}"),
            Self::ChecksumMismatch => write!(formatter, "checksum mismatch"),
            Self::InvalidOperation(operation) => write!(formatter, "invalid operation {operation}"),
            Self::InvalidUtf8 => write!(formatter, "invalid utf-8"),
            Self::TrailingSnapshotBytes => write!(formatter, "trailing snapshot bytes"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub fn encode_value_snapshot(values: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Vec<u8> {
    let values: Vec<Vec<u8>> = values
        .into_iter()
        .map(|value| value.as_ref().to_vec())
        .collect();
    let mut payload = Vec::new();
    write_u32(&mut payload, values.len() as u32);
    for value in values {
        write_bytes(&mut payload, &value);
    }
    encode_envelope(VALUE_SNAPSHOT_MAGIC, &payload)
}

pub fn decode_value_snapshot(bytes: &[u8]) -> Result<Vec<Vec<u8>>, DecodeError> {
    let payload = decode_envelope(bytes, VALUE_SNAPSHOT_MAGIC)?;
    let mut offset = 0;
    let count = read_u32(payload, &mut offset, "value snapshot count")? as usize;
    // Cap preallocation at what the payload could hold (>= 4 bytes per entry)
    // so a corrupt count fails with Truncated below instead of aborting on
    // allocation.
    let mut values = Vec::with_capacity(count.min(payload.len().saturating_sub(offset) / 4));
    for _ in 0..count {
        values.push(read_bytes(payload, &mut offset, "value snapshot entry")?.to_vec());
    }
    if offset != payload.len() {
        return Err(DecodeError::TrailingSnapshotBytes);
    }
    Ok(values)
}

pub fn encode_key_value_snapshot<K, V>(entries: impl IntoIterator<Item = (K, V)>) -> Vec<u8>
where
    K: AsRef<str>,
    V: AsRef<[u8]>,
{
    let entries: Vec<(String, Vec<u8>)> = entries
        .into_iter()
        .map(|(key, value)| (key.as_ref().to_string(), value.as_ref().to_vec()))
        .collect();
    let mut payload = Vec::new();
    write_u32(&mut payload, entries.len() as u32);
    for (key, value) in entries {
        write_string(&mut payload, &key);
        write_bytes(&mut payload, &value);
    }
    encode_envelope(KEY_VALUE_SNAPSHOT_MAGIC, &payload)
}

pub fn decode_key_value_snapshot(bytes: &[u8]) -> Result<IndexMap<String, Vec<u8>>, DecodeError> {
    let payload = decode_envelope(bytes, KEY_VALUE_SNAPSHOT_MAGIC)?;
    let mut offset = 0;
    let count = read_u32(payload, &mut offset, "key-value snapshot count")? as usize;
    // Cap preallocation at what the payload could hold (>= 8 bytes per entry)
    // so a corrupt count fails with Truncated below instead of aborting on
    // allocation.
    let mut entries =
        IndexMap::with_capacity(count.min(payload.len().saturating_sub(offset) / 8));
    for _ in 0..count {
        let key = read_string(payload, &mut offset, "key-value snapshot key")?;
        let value = read_bytes(payload, &mut offset, "key-value snapshot value")?.to_vec();
        entries.insert(key, value);
    }
    if offset != payload.len() {
        return Err(DecodeError::TrailingSnapshotBytes);
    }
    Ok(entries)
}

pub fn encode_journal_payload(record: &JournalRecord) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(record.operation.as_byte());
    write_string(&mut out, &record.key);
    if record.operation == JournalOperation::Put {
        write_bytes(&mut out, record.value.as_deref().unwrap_or_default());
    }
    out
}

pub fn decode_journal_payload(payload: &[u8]) -> Result<JournalRecord, DecodeError> {
    if payload.is_empty() {
        return Err(DecodeError::Truncated("journal operation"));
    }
    let mut offset = 0;
    let operation = JournalOperation::from_byte(payload[offset])?;
    offset += 1;
    let key = read_string(payload, &mut offset, "journal key")?;
    let value = match operation {
        JournalOperation::Put => Some(read_bytes(payload, &mut offset, "journal value")?.to_vec()),
        JournalOperation::Delete => None,
    };
    Ok(JournalRecord {
        operation,
        key,
        value,
    })
}

pub fn encode_journal_record(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + payload.len());
    write_u32(&mut out, payload.len() as u32);
    write_u32(&mut out, fnv1a(payload));
    out.extend_from_slice(payload);
    out
}

pub fn encode_journal_put_record(key: &str, value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
    payload.push(JournalOperation::Put.as_byte());
    write_string(&mut payload, key);
    write_bytes(&mut payload, value);
    encode_journal_record(&payload)
}

pub fn encode_journal_delete_record(key: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 4 + key.len());
    payload.push(JournalOperation::Delete.as_byte());
    write_string(&mut payload, key);
    encode_journal_record(&payload)
}

pub fn encode_journal(records: impl IntoIterator<Item = JournalRecord>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(JOURNAL_MAGIC);
    out.extend_from_slice(&encode_journal_records(records));
    out
}

pub fn encode_journal_records(records: impl IntoIterator<Item = JournalRecord>) -> Vec<u8> {
    let mut out = Vec::new();
    for record in records {
        out.extend_from_slice(&encode_journal_record(&encode_journal_payload(&record)));
    }
    out
}

pub fn decode_journal(bytes: &[u8]) -> Result<Vec<JournalRecord>, DecodeError> {
    let mut offset = if has_magic(bytes, JOURNAL_MAGIC) {
        JOURNAL_MAGIC.len()
    } else {
        0
    };
    let mut records = Vec::new();
    while offset < bytes.len() {
        if offset + 8 > bytes.len() {
            break;
        }
        let length = read_u32(bytes, &mut offset, "journal record length")? as usize;
        let checksum = read_u32(bytes, &mut offset, "journal record checksum")?;
        let end = offset
            .checked_add(length)
            .ok_or(DecodeError::Truncated("journal record"))?;
        if end > bytes.len() {
            break;
        }
        let payload = &bytes[offset..end];
        offset = end;
        if fnv1a(payload) != checksum {
            break;
        }
        records.push(decode_journal_payload(payload)?);
    }
    Ok(records)
}

fn encode_envelope(magic: &[u8; 8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(magic.len() + 8 + payload.len());
    out.extend_from_slice(magic);
    write_u32(&mut out, payload.len() as u32);
    write_u32(&mut out, fnv1a(payload));
    out.extend_from_slice(payload);
    out
}

fn decode_envelope<'a>(bytes: &'a [u8], magic: &[u8; 8]) -> Result<&'a [u8], DecodeError> {
    if !has_magic(bytes, magic) {
        return Ok(bytes);
    }
    let mut offset = magic.len();
    let length = read_u32(bytes, &mut offset, "snapshot payload length")? as usize;
    let checksum = read_u32(bytes, &mut offset, "snapshot checksum")?;
    let end = offset
        .checked_add(length)
        .ok_or(DecodeError::Truncated("snapshot payload"))?;
    if end > bytes.len() || end != bytes.len() {
        return Err(DecodeError::Truncated("snapshot payload"));
    }
    let payload = &bytes[offset..end];
    if fnv1a(payload) != checksum {
        return Err(DecodeError::ChecksumMismatch);
    }
    Ok(payload)
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u32(bytes: &[u8], offset: &mut usize, label: &'static str) -> Result<u32, DecodeError> {
    let end = offset.checked_add(4).ok_or(DecodeError::Truncated(label))?;
    if end > bytes.len() {
        return Err(DecodeError::Truncated(label));
    }
    let value = u32::from_le_bytes(
        bytes[*offset..end]
            .try_into()
            .expect("slice length checked"),
    );
    *offset = end;
    Ok(value)
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    write_u32(out, value.len() as u32);
    out.extend_from_slice(value);
}

fn read_bytes<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<&'a [u8], DecodeError> {
    let length = read_u32(bytes, offset, label)? as usize;
    let end = offset
        .checked_add(length)
        .ok_or(DecodeError::Truncated(label))?;
    if end > bytes.len() {
        return Err(DecodeError::Truncated(label));
    }
    let value = &bytes[*offset..end];
    *offset = end;
    Ok(value)
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    write_bytes(out, value.as_bytes());
}

fn read_string(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<String, DecodeError> {
    String::from_utf8(read_bytes(bytes, offset, label)?.to_vec())
        .map_err(|_| DecodeError::InvalidUtf8)
}

fn has_magic(bytes: &[u8], magic: &[u8]) -> bool {
    bytes.len() >= magic.len() && &bytes[..magic.len()] == magic
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::{
        decode_journal, decode_key_value_snapshot, decode_value_snapshot, encode_journal,
        encode_key_value_snapshot, encode_value_snapshot, DurableIndexState, JournalRecord,
    };

    #[test]
    fn journal_round_trips_and_stops_at_partial_tail() {
        let mut bytes = encode_journal([
            JournalRecord::put("a", vec![1, 2]),
            JournalRecord::delete("b"),
        ]);
        bytes.extend_from_slice(&[1, 2, 3]);

        assert_eq!(
            decode_journal(&bytes).unwrap(),
            vec![
                JournalRecord::put("a", vec![1, 2]),
                JournalRecord::delete("b")
            ]
        );
    }

    #[test]
    fn corrupt_snapshot_count_errors_instead_of_aborting() {
        // Magic-less payload whose leading u32 decodes to ~4.3e9 entries; the
        // decoders must fail with Truncated, not abort on preallocation.
        let bytes = [0xff, 0xff, 0xff, 0xff];
        assert!(decode_value_snapshot(&bytes).is_err());
        assert!(decode_key_value_snapshot(&bytes).is_err());
    }

    #[test]
    fn value_snapshot_round_trips_bytes() {
        let values = vec![vec![1, 2, 3], vec![4, 5]];
        let bytes = encode_value_snapshot(values.iter());

        assert_eq!(decode_value_snapshot(&bytes).unwrap(), values);
    }

    #[test]
    fn key_value_snapshot_keeps_insertion_order() {
        let a = vec![1];
        let b = vec![2, 3];
        let bytes = encode_key_value_snapshot([("a", &a), ("b", &b)]);
        let decoded = decode_key_value_snapshot(&bytes).unwrap();

        assert_eq!(
            decoded
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_slice()))
                .collect::<Vec<_>>(),
            vec![("a", [1].as_slice()), ("b", [2, 3].as_slice())]
        );
    }

    #[test]
    fn durable_state_applies_journal_records() {
        let mut state = DurableIndexState::from_entries([("a".to_string(), vec![1])]);
        state.apply_journal_records([JournalRecord::put("b", vec![2]), JournalRecord::delete("a")]);

        assert_eq!(state.entries().get("a"), None);
        assert_eq!(state.entries().get("b"), Some(&vec![2]));
        assert_eq!(state.operations(), 2);
    }
}
