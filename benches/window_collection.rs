//! Full retained collection emission, including Arrow gathering, VM projection and IPC qualification.
//!
//! Layer: harness.
//! - **Owns.** Criterion cases for complete collection emissions at two retained-row counts.
//! - **Depends on.** The production collection fixture, Criterion and the primitive runtime.
//! - **Must not know.** Model lowering, placement, transport or acknowledgement lifecycles.

use std::num::{NonZeroU16, NonZeroUsize};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_server::runtime::window_collection_benchmark::WindowCollectionBenchmark;

fn collection_emission(criterion: &mut Criterion) {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the benchmark runtime opens");
    let mut group = criterion.benchmark_group("window_collection_emission");
    for count in [64, 4_096] {
        let rows = NonZeroUsize::new(count).verified("the collection retains rows");
        let fixture = WindowCollectionBenchmark::new(rows);
        group.throughput(Throughput::Elements(
            u64::try_from(count).assured("the row count fits u64"),
        ));
        group.bench_function(BenchmarkId::from_parameter(count), |bench| {
            bench.iter(|| black_box(runtime.block_on(fixture.emit())))
        });
        let array_rows =
            NonZeroU16::new(u16::try_from(count).assured("the fixed cases retain 64 or 4096 rows"))
                .verified("the fixed collection retains rows");
        let fixed = WindowCollectionBenchmark::fixed_array(array_rows);
        group.bench_function(BenchmarkId::new("fixed_array", count), |bench| {
            bench.iter(|| black_box(runtime.block_on(fixed.emit())))
        });
    }
    group.finish();
}

criterion_group!(benches, collection_emission);
criterion_main!(benches);
