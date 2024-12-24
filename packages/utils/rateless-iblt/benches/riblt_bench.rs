use riblt::encoding::*;
use riblt::sketch::*;
use riblt::testing::*;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sha2::{Digest, Sha256};

const N: usize = 10000;
const M: usize = 15000;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut mapp = RandomMapping {
        prng: 123456789,
        last_idx: 0,
    };

    c.bench_function("mapping", |b| b.iter(|| black_box(mapp.next_index())));

    let mut enc = Encoder::<TestSymbol>::new();

    let data: [TestSymbol; N] = core::array::from_fn(|i| new_test_symbol(i as u64));

    c.bench_function("encoding", |b| {
        b.iter(|| {
            let mut dummy: u64 = 0;
            enc.reset();
            for i in 0..N {
                enc.add_symbol(&data[i]);
            }
            for _ in 0..M {
                dummy ^= enc.produce_next_coded_symbol().hash;
            }
            black_box(dummy)
        })
    });

    let sketch_benches = [1000, 100000, 10000000];
    for size in sketch_benches {
        let mut sketch = Sketch::<TestSymbol>::new(size);
        let mut n = 0;
        c.bench_function(format!("sketch_and_symbol_{}", size).as_str(), |b| {
            b.iter(|| {
                sketch.add_symbol(&new_test_symbol(n));
                n += 1;
                black_box(sketch.v[0].hash)
            })
        });
    }

    let mut k = 0;

    c.bench_function("sha256", |b| {
        b.iter(|| {
            let sym = new_test_symbol(k);
            k += 1;
            let mut hasher = Sha256::new();
            hasher.update(sym);
            black_box(hasher.finalize())
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
