use crate::encoding::Symbol;

#[allow(deprecated)]
use std::hash::{Hasher, SipHasher};

const TEST_SYMBOL_SIZE: usize = 64;

pub type TestSymbol = [u8; TEST_SYMBOL_SIZE];

pub fn new_test_symbol(x: u64) -> TestSymbol {
    return core::array::from_fn::<u8, TEST_SYMBOL_SIZE, _>(|i| {
        x.checked_shr(8 * i as u32).unwrap_or(0) as u8
    });
}

impl Symbol for TestSymbol {
    fn zero() -> TestSymbol {
        return new_test_symbol(0);
    }

    fn xor(&self, other: &TestSymbol) -> TestSymbol {
        return core::array::from_fn(|i| self[i] ^ other[i]);
    }

    #[allow(deprecated)]
    fn hash(&self) -> u64 {
        let mut hasher = SipHasher::new_with_keys(567, 890);
        hasher.write(self);
        return hasher.finish();
    }
}

pub type TestU64 = u64;

impl Symbol for TestU64 {
    fn zero() -> TestU64 {
        return 0;
    }

    fn xor(&self, other: &TestU64) -> TestU64 {
        return self ^ other;
    }

    #[allow(deprecated)]
    fn hash(&self) -> u64 {
        let mut hasher = SipHasher::new_with_keys(123, 456);
        hasher.write_u64(*self);
        return hasher.finish();
    }
}
