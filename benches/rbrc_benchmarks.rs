use criterion::{Criterion, criterion_group, criterion_main};
use std::io::prelude::*;

use rbrc_calc::onebrc;

fn bench_onebrc_classic(c: &mut Criterion) {
    // get data ready
    let mut file = std::fs::File::open("resources/data/measurements_classic.txt").unwrap();
    let filesize = file.metadata().unwrap().len();
    let mut data = Vec::with_capacity(filesize as usize + 8);
    file.read_to_end(&mut data).unwrap();
    data.extend([0u8; 8]);

    // Setup stuff for criterion
    let mut group = c.benchmark_group("onebrc");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.throughput(criterion::Throughput::Bytes(data.len() as u64));

    group.bench_function("classic", |b| {
        b.iter(|| {
            onebrc(&data[..data.len() - 8], 8);
        })
    });

    group.finish();
}

fn bench_onebrc_10k_keys(c: &mut Criterion) {
    // get data ready
    let mut file = std::fs::File::open("resources/data/measurements_10k_keys.txt").unwrap();
    let filesize = file.metadata().unwrap().len();
    let mut data = Vec::with_capacity(filesize as usize + 8);
    file.read_to_end(&mut data).unwrap();
    data.extend([0u8; 8]);

    // setup stuff for criterion
    let mut group = c.benchmark_group("onebrc");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.throughput(criterion::Throughput::Bytes(data.len() as u64));

    group.bench_function("10k Keys", |b| {
        b.iter(|| {
            onebrc(&data[..data.len() - 8], 8);
        })
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().warm_up_time(std::time::Duration::from_secs(10)).measurement_time(std::time::Duration::from_secs(10));
    targets = bench_onebrc_classic, bench_onebrc_10k_keys
);
criterion_main!(benches);
