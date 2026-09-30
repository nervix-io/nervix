//! Data-plane work a node admits through its bounded executor, measured as the runtime submits it:
//! preparing a branched entrypoint input into its branch batches, and encoding an emitter's rows
//! through a codec transformation.

use arch_into::ArchInto as _;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use nervix_server::runtime::admitted_work_benchmark::{
    BranchedInputBenchmark, TransformedEncodingBenchmark,
};

/// Rows of one input, and how many branch keys they are spread over.
const BRANCHED_INPUTS: [(usize, usize); 3] = [(1_024, 1), (1_024, 16), (1_024, 256)];

/// Rows of one emitter batch.
const ENCODED_ROWS: [usize; 2] = [64, 1_024];

fn admitted_work_benches(criterion: &mut Criterion) {
    let mut branched = criterion.benchmark_group("admitted_work/branched_input");
    for (rows, branches) in BRANCHED_INPUTS {
        let benchmark = BranchedInputBenchmark::new(rows, branches);
        branched.throughput(Throughput::Elements(rows.arch_into()));
        branched.bench_function(format!("{rows}_rows_{branches}_branches"), |bencher| {
            bencher.iter(|| black_box(benchmark.prepare()));
        });
    }
    branched.finish();

    let mut encoding = criterion.benchmark_group("admitted_work/transformed_encoding");
    for rows in ENCODED_ROWS {
        let benchmark = TransformedEncodingBenchmark::new(rows);
        encoding.throughput(Throughput::Elements(rows.arch_into()));
        encoding.bench_function(format!("{rows}_rows"), |bencher| {
            bencher.iter(|| black_box(benchmark.encode()));
        });
    }
    encoding.finish();
}

criterion_group!(benches, admitted_work_benches);
criterion_main!(benches);
