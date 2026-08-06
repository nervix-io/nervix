use std::time::{Duration, Instant};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use nervix_server::runtime::relay_interaction_benchmark::{
    RelayInteractionBenchmark, RelayInteractionBenchmarkEvent,
};

const READY_CHUNK: u64 = 1_024;
const COLLECTED_BATCHES: u64 = 64;
const FAN_IN_SOURCES: usize = 8;

fn consume_ready_batches(source_count: usize, iterations: u64) -> Duration {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime must build");
    runtime.block_on(async move {
        let mut benchmark =
            RelayInteractionBenchmark::pass_through(source_count, READY_CHUNK as usize);
        let mut completed = 0;
        let mut elapsed = Duration::ZERO;
        while completed < iterations {
            tokio::task::consume_budget().await;
            let chunk = READY_CHUNK.min(iterations - completed);
            for offset in 0..chunk {
                tokio::task::consume_budget().await;
                benchmark
                    .enqueue(((completed + offset) as usize) % source_count)
                    .await;
            }
            let started = Instant::now();
            for _ in 0..chunk {
                tokio::task::consume_budget().await;
                black_box(match benchmark.next().await {
                    RelayInteractionBenchmarkEvent::Batch { rows } => rows,
                    event => panic!("expected a ready batch, observed {event:?}"),
                });
            }
            elapsed += started.elapsed();
            completed += chunk;
        }
        elapsed
    })
}

fn force_drain_collections(iterations: u64) -> Duration {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime must build");
    runtime.block_on(async move {
        let capacity = COLLECTED_BATCHES.div_ceil(FAN_IN_SOURCES as u64) as usize;
        let mut benchmark = RelayInteractionBenchmark::collecting(FAN_IN_SOURCES, capacity);
        let mut elapsed = Duration::ZERO;
        for iteration in 0..iterations {
            tokio::task::consume_budget().await;
            for batch in 0..COLLECTED_BATCHES {
                tokio::task::consume_budget().await;
                benchmark
                    .enqueue(((iteration + batch) as usize) % FAN_IN_SOURCES)
                    .await;
            }
            benchmark.force_flush();
            let started = Instant::now();
            let mut drained_rows = 0;
            loop {
                tokio::task::consume_budget().await;
                match benchmark.next().await {
                    RelayInteractionBenchmarkEvent::Batch { rows } => drained_rows += rows,
                    RelayInteractionBenchmarkEvent::ForceFlush => break,
                    event => panic!("unexpected force-drain event: {event:?}"),
                }
            }
            elapsed += started.elapsed();
            black_box(drained_rows);
            assert_eq!(drained_rows, COLLECTED_BATCHES);
        }
        elapsed
    })
}

fn due_wakes(iterations: u64) -> Duration {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime must build");
    runtime.block_on(async move {
        let mut benchmark = RelayInteractionBenchmark::pass_through(1, 1);
        let started = Instant::now();
        for _ in 0..iterations {
            tokio::task::consume_budget().await;
            assert_eq!(
                benchmark.wake_now().await,
                RelayInteractionBenchmarkEvent::Wake
            );
            assert_eq!(benchmark.quiesce_work(), 0);
        }
        started.elapsed()
    })
}

fn graceful_commands(iterations: u64) -> Duration {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime must build");
    runtime.block_on(async move {
        let mut benchmark = RelayInteractionBenchmark::pass_through(1, 1);
        let started = Instant::now();
        for _ in 0..iterations {
            tokio::task::consume_budget().await;
            benchmark.graceful_command().await;
            assert_eq!(
                benchmark.next().await,
                RelayInteractionBenchmarkEvent::Command
            );
            assert_eq!(benchmark.quiesce_work(), 0);
        }
        started.elapsed()
    })
}

fn receiver_shutdowns(iterations: u64) -> Duration {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime must build");
    runtime.block_on(async move {
        let mut benchmark = RelayInteractionBenchmark::pass_through(1, 1);
        let started = Instant::now();
        for _ in 0..iterations {
            tokio::task::consume_budget().await;
            benchmark.shutdown();
            assert_eq!(
                benchmark.next().await,
                RelayInteractionBenchmarkEvent::Stopped
            );
            benchmark.clear_shutdown();
            assert_eq!(benchmark.quiesce_work(), 0);
        }
        started.elapsed()
    })
}

fn relay_interaction_benches(criterion: &mut Criterion) {
    let mut ready = criterion.benchmark_group("relay_interaction/ready_pass_through");
    ready.bench_function("single_source", |bencher| {
        bencher.iter_custom(|iterations| consume_ready_batches(1, iterations));
    });
    ready.bench_function("eight_source_fan_in", |bencher| {
        bencher.iter_custom(|iterations| consume_ready_batches(FAN_IN_SOURCES, iterations));
    });
    ready.finish();

    let mut collection = criterion.benchmark_group("relay_interaction/collection");
    collection.bench_function("eight_source_force_drain_64_batches", |bencher| {
        bencher.iter_custom(force_drain_collections);
    });
    collection.finish();

    let mut lifecycle = criterion.benchmark_group("relay_interaction/lifecycle");
    lifecycle.bench_function("due_wake", |bencher| bencher.iter_custom(due_wakes));
    lifecycle.bench_function("drain_first_command", |bencher| {
        bencher.iter_custom(graceful_commands);
    });
    lifecycle.bench_function("receiver_shutdown", |bencher| {
        bencher.iter_custom(receiver_shutdowns);
    });
    lifecycle.finish();
}

criterion_group!(benches, relay_interaction_benches);
criterion_main!(benches);
