use crate::storage::{ByteStorage, MemoryByteStorage};
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

pub struct NativeFsSnapshotStorage {
    path: PathBuf,
    memory: MemoryByteStorage,
}

impl NativeFsSnapshotStorage {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let memory = match fs::read(&path) {
            Ok(bytes) => decode_snapshot(&bytes)?,
            Err(error) if error.kind() == ErrorKind::NotFound => MemoryByteStorage::new(),
            Err(error) => return Err(error),
        };
        Ok(Self { path, memory })
    }

    pub fn flush(&self) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.path, encode_snapshot(&self.memory))
    }

    pub fn remove_file(&self) -> io::Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl ByteStorage for NativeFsSnapshotStorage {
    fn get(&self, key: &str) -> Option<&[u8]> {
        self.memory.get(key)
    }

    fn put(&mut self, key: String, value: Vec<u8>) {
        self.memory.put(key, value);
    }

    fn delete(&mut self, key: &str) -> bool {
        self.memory.delete(key)
    }

    fn clear(&mut self) {
        self.memory.clear();
    }

    fn len(&self) -> usize {
        self.memory.len()
    }

    fn entries(&self) -> Vec<(&str, &[u8])> {
        self.memory.entries()
    }
}

fn encode_snapshot(storage: &impl ByteStorage) -> Vec<u8> {
    let entries = storage.entries();
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (key, value) in entries {
        let key_bytes = key.as_bytes();
        out.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(key_bytes);
        out.extend_from_slice(value);
    }
    out
}

fn decode_snapshot(bytes: &[u8]) -> io::Result<MemoryByteStorage> {
    let mut offset = 0;
    let count = read_u32(bytes, &mut offset)? as usize;
    let mut storage = MemoryByteStorage::new();
    for _ in 0..count {
        let key_len = read_u32(bytes, &mut offset)? as usize;
        let value_len = read_u32(bytes, &mut offset)? as usize;
        let key = read_bytes(bytes, &mut offset, key_len)?;
        let value = read_bytes(bytes, &mut offset, value_len)?;
        storage.put(
            String::from_utf8(key.to_vec()).map_err(|error| {
                io::Error::new(ErrorKind::InvalidData, format!("invalid key: {error}"))
            })?,
            value.to_vec(),
        );
    }
    Ok(storage)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> io::Result<u32> {
    let data = read_bytes(bytes, offset, 4)?;
    Ok(u32::from_le_bytes([data[0], data[1], data[2], data[3]]))
}

fn read_bytes<'a>(bytes: &'a [u8], offset: &mut usize, length: usize) -> io::Result<&'a [u8]> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "snapshot offset overflow"))?;
    if end > bytes.len() {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "truncated snapshot",
        ));
    }
    let data = &bytes[*offset..end];
    *offset = end;
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::NativeFsSnapshotStorage;
    use crate::storage::ByteStorage;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn persists_across_reopen() {
        let path = std::env::temp_dir().join(format!(
            "peerbit-indexer-rust-{}.bin",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let mut storage = NativeFsSnapshotStorage::open(&path).unwrap();
        storage.put("a".to_string(), vec![1, 2, 3]);
        storage.flush().unwrap();

        let reopened = NativeFsSnapshotStorage::open(&path).unwrap();
        assert_eq!(reopened.get("a"), Some([1, 2, 3].as_slice()));
        reopened.remove_file().unwrap();
    }
}
