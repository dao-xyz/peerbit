# riblt-rust
Rust port of [RIBLT library](https://github.com/yangl1996/riblt) by yang1996.

Implementation of Rateless Invertible Bloom Lookup Tables (Rateless IBLTs), as
proposed in paper Practical Rateless Set Reconciliation by Lei Yang, Yossi
Gilad, and Mohammad Alizadeh. Preprint available at
[arxiv.org/abs/2402.02668](https://arxiv.org/abs/2402.02668).

##  Library API

To use this library, implement a `Symbol` trait, and create `Encoder` or `Decoder` objects to encode and decode symbols.

### `Symbol` trait
- `fn zero() -> Self` - Create a zero symbol.
- `fn xor(&self, other: &Self) -> Self` - XOR of this symbol and another symbol.
- `fn hash(&self) -> u64` - Calculate a hash of the symbol.

Example implementation for 64-bit integer symbols:
```rs
use riblt::*;
use std::hash::{SipHasher, Hasher};

pub type MyU64 = u64;

impl Symbol for MyU64 {
  fn zero() -> MyU64 {
    return 0;
  }

  fn xor(&self, other: &MyU64) -> MyU64 {
    return self ^ other;
  }

  fn hash(&self) -> u64 {
    let mut hasher = SipHasher::new_with_keys(123, 456);
    hasher.write_u64(*self);
    return hasher.finish();
  }
}
```

### `Encoder` methods
- `Encoder::<T>::new()` - Create a new Encoder for symbols of type `T`.
- `enc.reset()` - Reset the Encoder state.
- `enc.add_symbol(symbol: &T)` - Add a new symbol to the Encoder.
- `enc.produce_next_coded_symbol() -> CodedSymbol<T>` - Produce the next coded symbol that can be decoded by the Decoder. 

#### Example usage
```rs
use riblt::*;

fn foo() {
  let mut enc                  = Encoder::<MyU64>::new();
  let     symbols : [MyU64; 5] = [ 1, 2, 3, 4, 5 ];
  for x in symbols {
    enc.add_symbol(&x);
  }

  let coded = enc.produce_next_coded_symbol();

  // send symbol to the decoder...
}
```

### `Decoder` methods
- `Decoder::<T>::new()` - Create a new Decoder for symbols of type `T`.
- `dec.reset()` - Reset the Decoder state.
- `dec.add_symbol(symbol: &T)` - Add a new symbol to the Decoder.
- `dec.add_coded_symbol(symbol: &CodedSymbol<T>)` - Add a new coded symbol to the Decoder.
- `dec.try_decode()` - Try to decode added symbols. May returns `Err(InvalidDegree)`.
- `dec.decoded()` - Returns `true` if all added coded symbols where decoded.
- `dec.get_remote_symbols() -> Vec<HashedSymbol<T>>` - Returns an array of decoded remote symbols.
- `dec.get_local_symbols() -> Vec<HashedSymbol<T>>` - Returns an array of local symbols.

Remote and local symbols can be accessed directly via Decoder properties:
- `dec.remote.symbols`,
- `dec.local.symbols`.

#### Example usage
```rs
use riblt::*;

fn foo() {
  let symbols : [CodedSymbol<MyU64>; 5] = ...;

  let mut dec = Decoder::<MyU64>::new();
  for i in 0..symbols.len() {
    dec.add_coded_symbol(&symbols[i]);
  }

  if dec.try_decode().is_err() {
    // Decoding error...
  }

  if dec.decoded() {
    // Success...
  }
}
```

For the complete example see test `example` in `src/tests.rs`.
