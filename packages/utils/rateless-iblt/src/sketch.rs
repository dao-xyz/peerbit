//  FIXME
//  Mostly untested code.

use super::encoding::*;
use std::vec::Vec;

pub struct Sketch<T: Symbol + Copy> {
    pub v: Vec<CodedSymbol<T>>,
}

pub struct SketchDecodeResult<T: Symbol + Copy> {
    pub fwd: Vec<HashedSymbol<T>>,
    pub rev: Vec<HashedSymbol<T>>,
    pub is_decoded: bool,
}

impl<T: Symbol + Copy + PartialEq> Sketch<T> {
    pub fn new(size: usize) -> Sketch<T> {
        return Sketch::<T> {
            v: vec![
                CodedSymbol::<T> {
                    symbol: T::zero(),
                    hash: 0,
                    count: 0,
                };
                size
            ],
        };
    }

    pub fn add_hashed_symbol(&mut self, sym: &HashedSymbol<T>) {
        let mut mapp = RandomMapping {
            prng: sym.hash,
            last_idx: 0,
        };

        while (mapp.last_idx as usize) < self.v.len() {
            let idx = mapp.last_idx as usize;
            self.v[idx].symbol = self.v[idx].symbol.xor(&sym.symbol);
            self.v[idx].count += 1;
            self.v[idx].hash ^= sym.hash;
            mapp.next_index();
        }
    }

    pub fn remove_hashed_symbol(&mut self, sym: &HashedSymbol<T>) {
        let mut mapp = RandomMapping {
            prng: sym.hash,
            last_idx: 0,
        };

        while (mapp.last_idx as usize) < self.v.len() {
            let idx = mapp.last_idx as usize;
            self.v[idx].symbol = self.v[idx].symbol.xor(&sym.symbol);
            self.v[idx].count -= 1;
            self.v[idx].hash ^= sym.hash;
            mapp.next_index();
        }
    }

    pub fn add_symbol(&mut self, sym: &T) {
        self.add_hashed_symbol(&HashedSymbol::<T> {
            symbol: *sym,
            hash: sym.hash(),
        });
    }

    pub fn remove_symbol(&mut self, sym: &T) {
        self.remove_hashed_symbol(&HashedSymbol::<T> {
            symbol: *sym,
            hash: sym.hash(),
        });
    }

    pub fn subtract(&mut self, other: &Sketch<T>) -> Result<(), Error> {
        if self.v.len() != other.v.len() {
            return Err(Error::InvalidSize);
        }
        for i in 0..self.v.len() {
            self.v[i].symbol = self.v[i].symbol.xor(&other.v[i].symbol);
            self.v[i].count = self.v[i].count - other.v[i].count;
            self.v[i].hash ^= other.v[i].hash;
        }
        return Ok(());
    }

    pub fn decode(&mut self) -> Result<SketchDecodeResult<T>, Error> {
        let mut dec = Decoder::<T>::new();
        for i in 0..self.v.len() {
            dec.add_coded_symbol(&self.v[i]);
        }
        return match dec.try_decode() {
            Ok(()) => Ok(SketchDecodeResult::<T> {
                fwd: dec.get_remote_symbols(),
                rev: dec.get_local_symbols(),
                is_decoded: dec.decoded(),
            }),
            Err(x) => Err(x),
        };
    }
}
