use indexmap::IndexMap;
use js_sys::Array;
use wasm_bindgen::prelude::*;

pub mod storage;

#[cfg(not(target_arch = "wasm32"))]
pub mod native_fs;

struct StoredEntry {
    id: JsValue,
    value: JsValue,
}

#[wasm_bindgen]
pub struct NativeIndexStore {
    entries: IndexMap<String, StoredEntry>,
}

#[wasm_bindgen]
impl NativeIndexStore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> NativeIndexStore {
        NativeIndexStore {
            entries: IndexMap::new(),
        }
    }

    pub fn put(&mut self, key: String, id: JsValue, value: JsValue) {
        self.entries.insert(key, StoredEntry { id, value });
    }

    pub fn get(&self, key: &str) -> JsValue {
        match self.entries.get(key) {
            Some(entry) => entry_to_js(entry),
            None => JsValue::UNDEFINED,
        }
    }

    pub fn delete(&mut self, key: &str) -> bool {
        self.entries.shift_remove(key).is_some()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> Array {
        let entries = Array::new();
        for entry in self.entries.values() {
            entries.push(&entry_to_js(entry));
        }
        entries
    }
}

fn entry_to_js(entry: &StoredEntry) -> JsValue {
    let pair = Array::new();
    pair.push(&entry.id);
    pair.push(&entry.value);
    pair.into()
}
