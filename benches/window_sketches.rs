//! Criterion cost of admitting a window's retained rows into its pane sketches.
//!
//! Rows arrive in pane order; each pane's first row creates its sketch and every later row of the
//! pane updates it. One row per pane makes every admission a new pane, and a thousand rows per pane
//! makes nearly every admission an existing one.

use std::num::NonZeroUsize;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_server::runtime::window_sketch_benchmark::{SketchKind, WindowSketchBenchmark};

/// The rows each measurement admits.
const ROWS: usize = 4_096;

fn pane_admission(criterion: &mut Criterion) {
    let rows = NonZeroUsize::new(ROWS).verified("the benchmark admits rows");
    let mut group = criterion.benchmark_group("window_sketch_admission");
    group.throughput(Throughput::Elements(
        u64::try_from(ROWS).assured("the row count fits u64"),
    ));
    for kind in [SketchKind::Distinct, SketchKind::Quantile, SketchKind::TopK] {
        for rows_per_pane in [1, 16, 1_024] {
            let per_pane = NonZeroUsize::new(rows_per_pane).verified("a pane holds rows");
            let mut fixture = WindowSketchBenchmark::new(kind, rows, per_pane);
            group.bench_function(
                BenchmarkId::new(format!("{kind:?}"), rows_per_pane),
                |bench| bench.iter(|| black_box(fixture.admit())),
            );
        }
    }
    group.finish();
}

criterion_group!(benches, pane_admission);
criterion_main!(benches);
