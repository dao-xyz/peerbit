use indexmap::IndexMap;

pub trait ByteStorage {
    fn get(&self, key: &str) -> Option<&[u8]>;
    fn put(&mut self, key: String, value: Vec<u8>);
    fn delete(&mut self, key: &str) -> bool;
    fn clear(&mut self);
    fn len(&self) -> usize;
    fn entries(&self) -> Vec<(&str, &[u8])>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Default, Clone)]
pub struct MemoryByteStorage {
    entries: IndexMap<String, Vec<u8>>,
}

impl MemoryByteStorage {
    pub fn new() -> Self {
        Self {
            entries: IndexMap::new(),
        }
    }
}

impl ByteStorage for MemoryByteStorage {
    fn get(&self, key: &str) -> Option<&[u8]> {
        self.entries.get(key).map(Vec::as_slice)
    }

    fn put(&mut self, key: String, value: Vec<u8>) {
        self.entries.insert(key, value);
    }

    fn delete(&mut self, key: &str) -> bool {
        self.entries.shift_remove(key).is_some()
    }

    fn clear(&mut self) {
        self.entries.clear();
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn entries(&self) -> Vec<(&str, &[u8])> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_slice()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{ByteStorage, MemoryByteStorage};

    #[test]
    fn memory_storage_keeps_insertion_order() {
        let mut storage = MemoryByteStorage::new();
        storage.put("a".to_string(), vec![1]);
        storage.put("b".to_string(), vec![2]);
        storage.put("a".to_string(), vec![3]);

        assert_eq!(storage.get("a"), Some([3].as_slice()));
        assert_eq!(storage.len(), 2);
        assert_eq!(
            storage
                .entries()
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_vec()))
                .collect::<Vec<_>>(),
            vec![("a".to_string(), vec![3]), ("b".to_string(), vec![2])]
        );
    }
}
