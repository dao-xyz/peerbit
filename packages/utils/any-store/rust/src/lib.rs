use indexmap::IndexMap;
use js_sys::{Array, Uint8Array};
use redb::{
    backends::InMemoryBackend, Database, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition,
};
use wasm_bindgen::prelude::*;

const SNAPSHOT_MAGIC: &[u8; 8] = b"PBAKVS1\0";
const JOURNAL_MAGIC: &[u8; 8] = b"PBAKVJ1\0";
const REDB_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("entries");

#[repr(u8)]
#[derive(Clone, Copy)]
enum JournalOperation {
    Put = 1,
    Delete = 2,
    Clear = 3,
}

#[wasm_bindgen]
pub struct NativeAnyStore {
    entries: IndexMap<String, Vec<u8>>,
    total_size: usize,
}

#[wasm_bindgen]
impl NativeAnyStore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> NativeAnyStore {
        NativeAnyStore {
            entries: IndexMap::new(),
            total_size: 0,
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.entries.get(key).cloned()
    }

    pub fn has_many(&self, keys: Array) -> Result<Array, JsValue> {
        let present = Array::new();
        for key in parse_keys(&keys).map_err(js_error)? {
            present.push(&JsValue::from_bool(self.entries.contains_key(&key)));
        }
        Ok(present)
    }

    pub fn put(&mut self, key: String, value: Vec<u8>) {
        let value_len = value.len();
        if let Some(previous) = self.entries.insert(key, value) {
            self.total_size = self.total_size.saturating_sub(previous.len());
        }
        self.total_size += value_len;
    }

    pub fn put_many(&mut self, keys: Array, values: Array) -> Result<(), JsValue> {
        for (key, value) in parse_key_values(&keys, &values).map_err(js_error)? {
            self.put(key, value);
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> bool {
        if let Some((_key, previous)) = self.entries.shift_remove_entry(key) {
            self.total_size = self.total_size.saturating_sub(previous.len());
            true
        } else {
            false
        }
    }

    pub fn delete_many(&mut self, keys: Array) -> Result<usize, JsValue> {
        let mut deleted = 0;
        for key in parse_keys(&keys).map_err(js_error)? {
            if self.delete(&key) {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.total_size = 0;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn size(&self) -> usize {
        self.total_size
    }

    pub fn entries(&self) -> Array {
        let entries = Array::new();
        for (key, value) in &self.entries {
            let pair = Array::new();
            pair.push(&JsValue::from_str(key));
            pair.push(&Uint8Array::from(value.as_slice()));
            entries.push(&pair);
        }
        entries
    }

    pub fn get_many(&self, keys: Array) -> Result<Array, JsValue> {
        let values = Array::new();
        for key in parse_keys(&keys).map_err(js_error)? {
            match self.entries.get(&key) {
                Some(value) => values.push(&Uint8Array::from(value.as_slice())),
                None => values.push(&JsValue::UNDEFINED),
            };
        }
        Ok(values)
    }

    pub fn snapshot(&self) -> Vec<u8> {
        encode_snapshot(&self.entries)
    }

    pub fn load_snapshot(&mut self, bytes: Vec<u8>) -> Result<(), JsValue> {
        let entries = decode_snapshot(&bytes).map_err(js_error)?;
        self.entries = entries;
        self.recalculate_size();
        Ok(())
    }

    pub fn apply_journal(&mut self, bytes: Vec<u8>) -> Result<(), JsValue> {
        apply_journal(self, &bytes).map_err(js_error)
    }

    pub fn encode_put_record(&self, key: String, value: Vec<u8>) -> Vec<u8> {
        encode_record(JournalOperation::Put, key.as_bytes(), &value)
    }

    pub fn encode_put_records(&self, keys: Array, values: Array) -> Result<Vec<u8>, JsValue> {
        let entries = parse_key_values(&keys, &values).map_err(js_error)?;
        let mut output = Vec::new();
        for (key, value) in entries {
            output.extend_from_slice(&encode_record(
                JournalOperation::Put,
                key.as_bytes(),
                &value,
            ));
        }
        Ok(output)
    }

    pub fn encode_delete_record(&self, key: String) -> Vec<u8> {
        encode_record(JournalOperation::Delete, key.as_bytes(), &[])
    }

    pub fn encode_delete_records(&self, keys: Array) -> Result<Vec<u8>, JsValue> {
        let keys = parse_keys(&keys).map_err(js_error)?;
        let mut output = Vec::new();
        for key in keys {
            output.extend_from_slice(&encode_record(
                JournalOperation::Delete,
                key.as_bytes(),
                &[],
            ));
        }
        Ok(output)
    }

    pub fn encode_clear_record(&self) -> Vec<u8> {
        encode_record(JournalOperation::Clear, &[], &[])
    }
}

impl NativeAnyStore {
    fn recalculate_size(&mut self) {
        self.total_size = self.entries.values().map(|value| value.len()).sum();
    }
}

fn js_error(error: String) -> JsValue {
    JsValue::from_str(&error)
}

fn external_error(error: impl ToString) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn parse_keys(keys: &Array) -> Result<Vec<String>, String> {
    let mut parsed = Vec::with_capacity(keys.length() as usize);
    for index in 0..keys.length() {
        let key = keys
            .get(index)
            .as_string()
            .ok_or_else(|| format!("key at index {index} is not a string"))?;
        parsed.push(key);
    }
    Ok(parsed)
}

fn parse_key_values(keys: &Array, values: &Array) -> Result<Vec<(String, Vec<u8>)>, String> {
    if keys.length() != values.length() {
        return Err("keys and values length mismatch".to_string());
    }
    let mut parsed = Vec::with_capacity(keys.length() as usize);
    for index in 0..keys.length() {
        let key = keys
            .get(index)
            .as_string()
            .ok_or_else(|| format!("key at index {index} is not a string"))?;
        let value = values.get(index);
        if value.is_null() || value.is_undefined() {
            return Err(format!("value at index {index} is missing"));
        }
        parsed.push((key, Uint8Array::new(&value).to_vec()));
    }
    Ok(parsed)
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5_u32;
    for byte in bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, String> {
    let data = read_bytes(bytes, offset, 4)?;
    Ok(u32::from_le_bytes([data[0], data[1], data[2], data[3]]))
}

fn read_bytes<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8], String> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| "offset overflow".to_string())?;
    if end > bytes.len() {
        return Err("truncated bytes".to_string());
    }
    let data = &bytes[*offset..end];
    *offset = end;
    Ok(data)
}

fn encode_snapshot(entries: &IndexMap<String, Vec<u8>>) -> Vec<u8> {
    let mut payload = Vec::new();
    push_u32(&mut payload, entries.len() as u32);
    for (key, value) in entries {
        push_u32(&mut payload, key.as_bytes().len() as u32);
        push_u32(&mut payload, value.len() as u32);
        payload.extend_from_slice(key.as_bytes());
        payload.extend_from_slice(value);
    }

    let mut output = Vec::with_capacity(SNAPSHOT_MAGIC.len() + 8 + payload.len());
    output.extend_from_slice(SNAPSHOT_MAGIC);
    push_u32(&mut output, payload.len() as u32);
    push_u32(&mut output, fnv1a(&payload));
    output.extend_from_slice(&payload);
    output
}

fn decode_snapshot(bytes: &[u8]) -> Result<IndexMap<String, Vec<u8>>, String> {
    if bytes.is_empty() {
        return Ok(IndexMap::new());
    }

    let payload = if bytes.starts_with(SNAPSHOT_MAGIC) {
        let mut offset = SNAPSHOT_MAGIC.len();
        let payload_len = read_u32(bytes, &mut offset)? as usize;
        let checksum = read_u32(bytes, &mut offset)?;
        let payload = read_bytes(bytes, &mut offset, payload_len)?;
        if fnv1a(payload) != checksum {
            return Err("snapshot checksum mismatch".to_string());
        }
        payload
    } else {
        bytes
    };

    let mut offset = 0;
    let count = read_u32(payload, &mut offset)? as usize;
    let mut entries = IndexMap::new();
    for _ in 0..count {
        let key_len = read_u32(payload, &mut offset)? as usize;
        let value_len = read_u32(payload, &mut offset)? as usize;
        let key = read_bytes(payload, &mut offset, key_len)?;
        let value = read_bytes(payload, &mut offset, value_len)?;
        let key = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
        entries.insert(key, value.to_vec());
    }
    Ok(entries)
}

fn encode_record(operation: JournalOperation, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 8 + key.len() + value.len());
    payload.push(operation as u8);
    push_u32(&mut payload, key.len() as u32);
    push_u32(&mut payload, value.len() as u32);
    payload.extend_from_slice(key);
    payload.extend_from_slice(value);

    let mut output = Vec::with_capacity(JOURNAL_MAGIC.len() + 8 + payload.len());
    output.extend_from_slice(JOURNAL_MAGIC);
    push_u32(&mut output, payload.len() as u32);
    push_u32(&mut output, fnv1a(&payload));
    output.extend_from_slice(&payload);
    output
}

fn apply_journal(store: &mut NativeAnyStore, bytes: &[u8]) -> Result<(), String> {
    let mut offset = 0;
    while offset < bytes.len() {
        if !bytes[offset..].starts_with(JOURNAL_MAGIC) {
            return Err("invalid journal record magic".to_string());
        }
        offset += JOURNAL_MAGIC.len();
        let payload_len = read_u32(bytes, &mut offset)? as usize;
        let checksum = read_u32(bytes, &mut offset)?;
        let payload = read_bytes(bytes, &mut offset, payload_len)?;
        if fnv1a(payload) != checksum {
            return Err("journal checksum mismatch".to_string());
        }
        apply_record_payload(store, payload)?;
    }
    Ok(())
}

fn apply_record_payload(store: &mut NativeAnyStore, payload: &[u8]) -> Result<(), String> {
    let mut offset = 0;
    let operation = *read_bytes(payload, &mut offset, 1)?
        .first()
        .ok_or_else(|| "missing journal operation".to_string())?;
    let key_len = read_u32(payload, &mut offset)? as usize;
    let value_len = read_u32(payload, &mut offset)? as usize;
    let key = read_bytes(payload, &mut offset, key_len)?;
    let value = read_bytes(payload, &mut offset, value_len)?;
    match operation {
        1 => {
            let key = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
            store.put(key, value.to_vec());
        }
        2 => {
            let key = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
            store.delete(&key);
        }
        3 => store.clear(),
        _ => return Err("unknown journal operation".to_string()),
    }
    Ok(())
}

#[wasm_bindgen]
pub struct NativeRedbAnyStore {
    db: Database,
    total_size: usize,
}

#[wasm_bindgen]
impl NativeRedbAnyStore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<NativeRedbAnyStore, JsValue> {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .map_err(external_error)?;
        ensure_redb_table(&db)?;
        Ok(NativeRedbAnyStore { db, total_size: 0 })
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, JsValue> {
        let txn = self.db.begin_read().map_err(external_error)?;
        let table = txn.open_table(REDB_TABLE).map_err(external_error)?;
        table
            .get(key)
            .map(|value| value.map(|value| value.value().to_vec()))
            .map_err(external_error)
    }

    pub fn get_many(&self, keys: Array) -> Result<Array, JsValue> {
        let keys = parse_keys(&keys).map_err(js_error)?;
        let txn = self.db.begin_read().map_err(external_error)?;
        let table = txn.open_table(REDB_TABLE).map_err(external_error)?;
        let values = Array::new();
        for key in keys {
            match table.get(key.as_str()).map_err(external_error)? {
                Some(value) => values.push(&Uint8Array::from(value.value())),
                None => values.push(&JsValue::UNDEFINED),
            };
        }
        Ok(values)
    }

    pub fn has_many(&self, keys: Array) -> Result<Array, JsValue> {
        let keys = parse_keys(&keys).map_err(js_error)?;
        let txn = self.db.begin_read().map_err(external_error)?;
        let table = txn.open_table(REDB_TABLE).map_err(external_error)?;
        let present = Array::new();
        for key in keys {
            present.push(&JsValue::from_bool(
                table.get(key.as_str()).map_err(external_error)?.is_some(),
            ));
        }
        Ok(present)
    }

    pub fn put(&mut self, key: String, value: Vec<u8>) -> Result<(), JsValue> {
        self.put_entries(vec![(key, value)])
    }

    pub fn put_many(&mut self, keys: Array, values: Array) -> Result<(), JsValue> {
        let entries = parse_key_values(&keys, &values).map_err(js_error)?;
        self.put_entries(entries)
    }
}

impl NativeRedbAnyStore {
    fn put_entries(&mut self, entries: Vec<(String, Vec<u8>)>) -> Result<(), JsValue> {
        let txn = self.db.begin_write().map_err(external_error)?;
        let mut total_size = self.total_size;
        {
            let mut table = txn.open_table(REDB_TABLE).map_err(external_error)?;
            for (key, value) in entries {
                let previous_len = table
                    .insert(key.as_str(), value.as_slice())
                    .map_err(external_error)?
                    .map(|previous| previous.value().len());
                if let Some(previous_len) = previous_len {
                    total_size = total_size.saturating_sub(previous_len);
                }
                total_size += value.len();
            }
        }
        txn.commit().map_err(external_error)?;
        self.total_size = total_size;
        Ok(())
    }
}

#[wasm_bindgen]
impl NativeRedbAnyStore {
    pub fn delete(&mut self, key: &str) -> Result<bool, JsValue> {
        let txn = self.db.begin_write().map_err(external_error)?;
        let mut deleted = false;
        let mut total_size = self.total_size;
        {
            let mut table = txn.open_table(REDB_TABLE).map_err(external_error)?;
            let previous_len = table
                .remove(key)
                .map_err(external_error)?
                .map(|previous| previous.value().len());
            if let Some(previous_len) = previous_len {
                total_size = total_size.saturating_sub(previous_len);
                deleted = true;
            }
        }
        txn.commit().map_err(external_error)?;
        self.total_size = total_size;
        Ok(deleted)
    }

    pub fn delete_many(&mut self, keys: Array) -> Result<usize, JsValue> {
        let keys = parse_keys(&keys).map_err(js_error)?;
        let txn = self.db.begin_write().map_err(external_error)?;
        let mut deleted = 0;
        let mut total_size = self.total_size;
        {
            let mut table = txn.open_table(REDB_TABLE).map_err(external_error)?;
            for key in keys {
                let previous_len = table
                    .remove(key.as_str())
                    .map_err(external_error)?
                    .map(|previous| previous.value().len());
                if let Some(previous_len) = previous_len {
                    total_size = total_size.saturating_sub(previous_len);
                    deleted += 1;
                }
            }
        }
        txn.commit().map_err(external_error)?;
        self.total_size = total_size;
        Ok(deleted)
    }

    pub fn clear(&mut self) -> Result<(), JsValue> {
        let txn = self.db.begin_write().map_err(external_error)?;
        {
            let mut table = txn.open_table(REDB_TABLE).map_err(external_error)?;
            table.retain(|_, _| false).map_err(external_error)?;
        }
        txn.commit().map_err(external_error)?;
        self.total_size = 0;
        Ok(())
    }

    pub fn len(&self) -> Result<usize, JsValue> {
        let txn = self.db.begin_read().map_err(external_error)?;
        let table = txn.open_table(REDB_TABLE).map_err(external_error)?;
        table.len().map(|len| len as usize).map_err(external_error)
    }

    pub fn size(&self) -> usize {
        self.total_size
    }

    pub fn entries(&self) -> Result<Array, JsValue> {
        let txn = self.db.begin_read().map_err(external_error)?;
        let table = txn.open_table(REDB_TABLE).map_err(external_error)?;
        let entries = Array::new();
        for entry in table.iter().map_err(external_error)? {
            let (key, value) = entry.map_err(external_error)?;
            let pair = Array::new();
            pair.push(&JsValue::from_str(key.value()));
            pair.push(&Uint8Array::from(value.value()));
            entries.push(&pair);
        }
        Ok(entries)
    }
}

fn ensure_redb_table(db: &Database) -> Result<(), JsValue> {
    let txn = db.begin_write().map_err(external_error)?;
    {
        txn.open_table(REDB_TABLE).map_err(external_error)?;
    }
    txn.commit().map_err(external_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{decode_snapshot, NativeAnyStore, NativeRedbAnyStore};

    #[test]
    fn snapshot_roundtrips() {
        let mut store = NativeAnyStore::new();
        store.put("a".to_string(), vec![1, 2, 3]);
        store.put("b".to_string(), vec![4]);
        let snapshot = store.snapshot();
        let entries = decode_snapshot(&snapshot).unwrap();
        assert_eq!(entries.get("a").unwrap(), &vec![1, 2, 3]);
        assert_eq!(entries.get("b").unwrap(), &vec![4]);
    }

    #[test]
    fn journal_replays() {
        let encoder = NativeAnyStore::new();
        let mut journal = Vec::new();
        journal.extend_from_slice(&encoder.encode_put_record("a".to_string(), vec![1]));
        journal.extend_from_slice(&encoder.encode_put_record("b".to_string(), vec![2, 3]));
        journal.extend_from_slice(&encoder.encode_delete_record("a".to_string()));

        let mut store = NativeAnyStore::new();
        store.apply_journal(journal).unwrap();
        assert_eq!(store.get("a"), None);
        assert_eq!(store.get("b"), Some(vec![2, 3]));
        assert_eq!(store.size(), 2);
    }

    #[test]
    fn redb_roundtrips() {
        let mut store = NativeRedbAnyStore::new().unwrap();
        store.put("a".to_string(), vec![1, 2, 3]).unwrap();
        store.put("b".to_string(), vec![4]).unwrap();

        assert_eq!(store.get("a").unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(store.len().unwrap(), 2);
        assert_eq!(store.size(), 4);

        assert!(store.delete("a").unwrap());
        assert_eq!(store.get("a").unwrap(), None);
        assert_eq!(store.size(), 1);
    }
}
