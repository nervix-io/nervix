//! Criterion throughput of one typed, nullable window argument run.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::ResultExt as _;
use nervix_approx_into::ApproxInto as _;
use nervix_simd_kernels::{RunValidity, bucket_indices, count_booleans, moments, sum_i64};

fn admission(criterion: &mut Criterion) {
    const ROWS: usize = 4096;
    let integers = (0..ROWS)
        .map(|row| i64::try_from(row % 101).assured("a remainder fits i64") - 50)
        .collect::<Vec<_>>();
    let floats = integers
        .iter()
        .map(|value| (*value).approx_into::<f64>() / 8.0)
        .collect::<Vec<_>>();
    let validity = (0..ROWS.div_ceil(8))
        .map(|byte| if byte % 5 == 0 { 0b1110_1111 } else { 0xff })
        .collect::<Vec<_>>();
    let booleans = vec![0b1010_1100; ROWS.div_ceil(8)];
    let mask = RunValidity::new(Some(&validity), 0);
    let mut group = criterion.benchmark_group("window_admission_per_run");
    group.throughput(Throughput::Elements(
        u64::try_from(ROWS).assured("run length fits u64"),
    ));
    group.bench_function("sum_count_moments_histogram", |benchmark| {
        benchmark.iter(|| {
            black_box(sum_i64(black_box(&integers), mask));
            black_box(count_booleans(black_box(&booleans), 0, mask, ROWS));
            black_box(moments(black_box(&floats), mask, |value| value));
            black_box(bucket_indices(
                black_box(&integers),
                mask,
                |value| value.approx_into::<f64>(),
                -64.0,
                64.0,
                1.0,
                128,
            ));
        })
    });
    group.finish();
}

criterion_group!(benches, admission);
criterion_main!(benches);
