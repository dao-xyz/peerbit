use crate::encoding::*;
use js_sys::Array;
use wasm_bindgen::prelude::*;
/*
#[wasm_bindgen]
#[derive(Clone, Copy, PartialEq)]
pub struct MyU64(u64);

impl From<u64> for MyU64 {
    fn from(value: u64) -> Self {
        MyU64(value)
    }
}

impl Into<u64> for MyU64 {
    fn into(self) -> u64 {
        self.0
    }
}

impl Symbol for MyU64 {
    fn zero() -> MyU64 {
        MyU64(0)
    }

    fn xor(&self, other: &MyU64) -> MyU64 {
        MyU64(self.0 ^ other.0)
    }

    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(self.0);
        hasher.finish()
    }
} */

pub type IdentityU64 = u64;

#[wasm_bindgen]
pub struct EncoderWrapper {
    encoder: Encoder<IdentityU64>,
}

#[wasm_bindgen]
impl EncoderWrapper {
    #[wasm_bindgen(constructor)]
    pub fn new() -> EncoderWrapper {
        EncoderWrapper {
            encoder: Encoder::<IdentityU64>::new(),
        }
    }

    pub fn add_symbol(&mut self, symbol: u64) {
        let my_symbol: IdentityU64 = symbol;
        self.encoder.add_symbol(&my_symbol);
    }

    pub fn add_symbols(&mut self, symbols: Vec<u64>) {
        for symbol in symbols.iter() {
            self.encoder.add_symbol(symbol);
        }
    }

    pub fn produce_next_coded_symbol(&mut self) -> JsValue {
        let coded_symbol = self.encoder.produce_next_coded_symbol();
        let symbol_u64 = coded_symbol.symbol;
        let hash_u64 = coded_symbol.hash;
        let count_i64 = coded_symbol.count;

        // Create a JavaScript object to hold the coded symbol
        let obj = js_sys::Object::new();

        js_sys::Reflect::set(
            &obj,
            &JsValue::from_str("symbol"),
            &JsValue::from(symbol_u64),
        )
        .unwrap();
        js_sys::Reflect::set(&obj, &JsValue::from_str("hash"), &JsValue::from(hash_u64)).unwrap();
        js_sys::Reflect::set(&obj, &JsValue::from_str("count"), &JsValue::from(count_i64)).unwrap();

        JsValue::from(obj)
    }

    pub fn reset(&mut self) {
        self.encoder.reset();
    }

    pub fn to_decoder(&self) -> DecoderWrapper {
        DecoderWrapper {
            decoder: self.encoder.to_decoder(),
        }
    }

    pub fn clone(&self) -> EncoderWrapper {
        EncoderWrapper {
            encoder: self.encoder.clone(),
        }
    }
}

#[wasm_bindgen]
pub struct DecoderWrapper {
    decoder: Decoder<IdentityU64>,
}

#[wasm_bindgen]
impl DecoderWrapper {
    #[wasm_bindgen(constructor)]
    pub fn new() -> DecoderWrapper {
        DecoderWrapper {
            decoder: Decoder::<IdentityU64>::new(),
        }
    }

    pub fn add_symbol(&mut self, symbol: u64) {
        let my_symbol: IdentityU64 = symbol;
        self.decoder.add_symbol(&my_symbol);
    }

    pub fn add_coded_symbol(&mut self, coded_symbol_js: &JsValue) {
        // Extract symbol, hash, and count from JsValue
        let symbol_js =
            js_sys::Reflect::get(coded_symbol_js, &JsValue::from_str("symbol")).unwrap();
        let hash_js = js_sys::Reflect::get(coded_symbol_js, &JsValue::from_str("hash")).unwrap();
        let count_js = js_sys::Reflect::get(coded_symbol_js, &JsValue::from_str("count")).unwrap();

        let symbol_u64: u64 = symbol_js.try_into().unwrap();
        let hash_u64: u64 = hash_js.try_into().unwrap();
        let count_i64: i64 = count_js.try_into().unwrap();
        let coded_symbol = CodedSymbol {
            symbol: symbol_u64,
            hash: hash_u64,
            count: count_i64,
        };

        self.decoder.add_coded_symbol(&coded_symbol);
    }

    pub fn try_decode(&mut self) -> Result<(), JsValue> {
        match self.decoder.try_decode() {
            Ok(_) => Ok(()),
            // error is a enum with number of variants
            Err(e) => {
                return match e {
                    Error::InvalidDegree => Err(JsValue::from_str("Invalid degree")),
                    Error::InvalidSize => Err(JsValue::from_str("Invalid size")),
                    Error::DecodeFailed => Err(JsValue::from_str("Decode failed")),
                };
            }
        }
    }

    pub fn decoded(&self) -> bool {
        self.decoder.decoded()
    }

    pub fn get_remote_symbols(&self) -> Array {
        let symbols = self.decoder.get_remote_symbols();
        let array = Array::new();
        for sym in symbols {
            array.push(&JsValue::from(sym.symbol));
        }
        array
    }

    pub fn get_local_symbols(&self) -> Array {
        let symbols = self.decoder.get_local_symbols();
        let array = Array::new();
        for sym in symbols {
            array.push(&JsValue::from(sym.symbol));
        }
        array
    }

    pub fn reset(&mut self) {
        self.decoder.reset();
    }
}
