use std::num::NonZeroUsize;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{EncodedFrame, ServerFrame};
use nervix_server::subscription_row::benchmark::SubscriptionRowBenchmark;

const ROWS: usize = 100;
const DETAIL_BYTES: usize = 1_024;

fn frame_bytes(frames: &[EncodedFrame<ServerFrame>]) -> usize {
    frames.iter().fold(0, |total, frame| {
        total
            .checked_add(frame.bytes().len())
            .assured("the benchmark output is bounded by in-memory frame vectors")
    })
}

fn subscription_row_encoding(criterion: &mut Criterion) {
    let rows = NonZeroUsize::new(ROWS).assured("the benchmark row count is nonzero");
    let detail_bytes =
        NonZeroUsize::new(DETAIL_BYTES).assured("the benchmark detail width is nonzero");
    let benchmark = SubscriptionRowBenchmark::new(rows, detail_bytes);

    let typed = benchmark.encode_typed_rows();
    eprintln!(
        "subscription_row_evidence rows={ROWS} detail_bytes={DETAIL_BYTES} typed_bytes={}",
        frame_bytes(&typed),
    );

    let mut group = criterion.benchmark_group("subscription_row_encoding");
    group.throughput(Throughput::Elements(
        u64::try_from(ROWS).assured("the fixed row count fits u64"),
    ));
    group.bench_function("typed_flatbuffers", |bencher| {
        bencher.iter(|| black_box(benchmark.encode_typed_rows()));
    });
    group.finish();
}

criterion_group!(benches, subscription_row_encoding);
criterion_main!(benches);
