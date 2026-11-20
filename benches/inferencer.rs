//! Admission of one inferencer batch, from `INPUTS` through the model to its route's dispatch.
//!
//! Layer: harness.
//! - **Owns.** Criterion cases for one-by-one, batched and stateful inference at three batch sizes.
//! - **Depends on.** The production inferencer branch fixture, Criterion and the primitive runtime.
//! - **Must not know.** Model lowering, placement, transport or acknowledgement lifecycles.

use std::num::NonZeroUsize;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_server::runtime::inferencer_benchmark::{InferencerBenchmark, InferencerBenchmarkCase};

fn inferencer_admission(criterion: &mut Criterion) {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the benchmark runtime opens");
    let mut group = criterion.benchmark_group("inferencer_admission");
    for (label, case) in [
        ("one_by_one", InferencerBenchmarkCase::OneByOne),
        ("batched", InferencerBenchmarkCase::Batched),
        ("stateful", InferencerBenchmarkCase::Stateful),
    ] {
        for rows in [1, 64, 1_024] {
            let batch = NonZeroUsize::new(rows).verified("every case admits rows");
            let mut fixture = runtime.block_on(InferencerBenchmark::new(case, batch));
            group.throughput(Throughput::Elements(
                u64::try_from(rows).assured("the row count fits u64"),
            ));
            group.bench_function(BenchmarkId::new(label, rows), |bench| {
                bench.iter(|| black_box(runtime.block_on(fixture.admit())))
            });
            runtime.block_on(fixture.stop());
        }
    }
    group.finish();
}

criterion_group!(benches, inferencer_admission);
criterion_main!(benches);
