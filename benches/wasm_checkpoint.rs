use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use nervix_server::runtime::wasm_checkpoint_benchmark::WasmCheckpointBenchmark;

/// Branches that checkpoint at the same time, as a WASM processor's branch tasks do.
const CONCURRENT_BRANCHES: [usize; 3] = [1, 16, 128];
/// The size of the guest state each branch saves.
const STATE_BYTES: usize = 4_096;

/// Where the benchmark stores its state. A synchronization only costs what the storage under it
/// costs, so the measurement runs under the build's target directory rather than a temporary
/// filesystem that may be memory-backed.
fn state_parent() -> PathBuf {
    let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target");
    std::fs::create_dir_all(&parent).expect("the target directory must be creatable");
    parent
}

fn rounds(
    branches: usize,
    iterations: u64,
    round: impl AsyncFn(&WasmCheckpointBenchmark),
) -> Duration {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime must build");
    runtime.block_on(async move {
        let benchmark = WasmCheckpointBenchmark::new(&state_parent(), branches, STATE_BYTES);
        let started = Instant::now();
        for _ in 0..iterations {
            tokio::task::consume_budget().await;
            round(&benchmark).await;
        }
        started.elapsed()
    })
}

fn wasm_checkpoint_benches(criterion: &mut Criterion) {
    let mut durable = criterion.benchmark_group("wasm_checkpoint/durable");
    for branches in CONCURRENT_BRANCHES {
        durable.throughput(Throughput::Elements(
            u64::try_from(branches).expect("branch counts fit u64"),
        ));
        durable.bench_function(format!("{branches}_branches"), |bencher| {
            bencher.iter_custom(|iterations| {
                rounds(branches, iterations, async |benchmark| {
                    benchmark.checkpoint_every_branch().await;
                })
            });
        });
    }
    durable.finish();

    let mut buffered = criterion.benchmark_group("wasm_checkpoint/unsynchronized_write");
    for branches in CONCURRENT_BRANCHES {
        buffered.throughput(Throughput::Elements(
            u64::try_from(branches).expect("branch counts fit u64"),
        ));
        buffered.bench_function(format!("{branches}_branches"), |bencher| {
            bencher.iter_custom(|iterations| {
                rounds(branches, iterations, async |benchmark| {
                    benchmark.write_every_branch_without_synchronization().await;
                })
            });
        });
    }
    buffered.finish();
}

criterion_group!(benches, wasm_checkpoint_benches);
criterion_main!(benches);
