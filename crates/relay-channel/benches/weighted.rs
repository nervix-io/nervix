//! Measures the standalone log against the same workloads as the revision-pinned baseline.
//!
//! Layer: test harness.
//! - **Owns.** Ready-operation, contention, and full-buffer timing workloads.
//! - **Depends on.** The public channel, Criterion, and selected runtime primitives.
//! - **Must not know.** Relay runtime entities or product data types.

use std::{
    future::{Future, poll_fn},
    hint::black_box,
    num::NonZeroU64,
    pin::pin,
    time::Duration,
};

use criterion::{BenchmarkId, Criterion, Throughput};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    runtime::{Builder, Runtime},
    sync::{oneshot, watch},
    task::{JoinSet, consume_budget},
    time::Instant,
};
use nervix_relay_channel::{Channel, Reader, ReaderAccess, Send, Sender};

const CHUNK: u64 = 1_024;
const CONTENTION_CAPACITY: u64 = 64;
const BATCHES_PER_SENDER: u64 = 256;
const WORKERS: usize = 8;
const WAITERS: u64 = 8;

fn channel(capacity: u64) -> Channel<u64, u64> {
    Channel::new(NonZeroU64::new(capacity).assured("the benchmark supplies positive capacities"))
}

fn reader(channel: &mut Channel<u64, u64>, key: u64) -> Reader<u64> {
    let ReaderAccess::Attached(reader) = channel.subscribe(key) else {
        panic!("benchmark reader identities are distinct");
    };
    reader
}

async fn publish(sender: &mut Sender<u64>, value: u64) {
    consume_budget().await;
    assert_eq!(
        sender.send(black_box(value), NonZeroU64::MIN).await,
        Send::Published
    );
}

async fn take(reader: &mut Reader<u64>) -> u64 {
    consume_budget().await;
    let value = reader
        .read()
        .await
        .assured("each measurement requests exactly the number of items sent");
    black_box(value)
}

fn ready(runtime: &Runtime, iterations: u64, time_publish: bool) -> Duration {
    runtime.block_on(async {
        let mut channel = channel(CHUNK);
        let mut reader = reader(&mut channel, 0);
        let mut sender = channel.sender();
        let mut remaining = iterations;
        let mut elapsed = Duration::ZERO;
        while remaining > 0 {
            let size = CHUNK.min(remaining);
            let before_publish = Instant::now();
            for value in 0..size {
                publish(&mut sender, value).await;
            }
            let after_publish = Instant::now();
            for expected in 0..size {
                assert_eq!(take(&mut reader).await, expected);
            }
            if time_publish {
                elapsed += after_publish.duration_since(before_publish);
            } else {
                elapsed += after_publish.elapsed();
            }
            remaining -= size;
        }
        elapsed
    })
}

fn contended(runtime: &Runtime, senders: u64, readers: u64) -> Duration {
    runtime.block_on(async {
        let mut channel = channel(CONTENTION_CAPACITY);
        let (start, wait) = watch::channel(false);
        let mut workers = JoinSet::new();
        let total = senders * BATCHES_PER_SENDER;
        let expected_sum = total * (total - 1) / 2;

        for key in 0..readers {
            let mut receiver = reader(&mut channel, key);
            let mut wait = wait.clone();
            workers.spawn(async move {
                wait.changed()
                    .await
                    .assured("the benchmark opens its start gate");
                let mut sum = 0;
                for _ in 0..total {
                    sum += take(&mut receiver).await;
                }
                assert_eq!(sum, expected_sum);
            });
        }
        for sender in 0..senders {
            let mut producer = channel.sender();
            let mut wait = wait.clone();
            workers.spawn(async move {
                wait.changed()
                    .await
                    .assured("the benchmark opens its start gate");
                let start = sender * BATCHES_PER_SENDER;
                for value in start..start + BATCHES_PER_SENDER {
                    publish(&mut producer, value).await;
                }
            });
        }

        let started = Instant::now();
        start.send_replace(true);
        while let Some(outcome) = workers.join_next().await {
            outcome.assured("every benchmark worker completes");
        }
        started.elapsed()
    })
}

fn full(runtime: &Runtime) -> Duration {
    runtime.block_on(async {
        let mut channel = channel(1);
        let mut reader = reader(&mut channel, 0);
        let mut initial = channel.sender();
        publish(&mut initial, 0).await;
        let mut workers = JoinSet::new();
        let mut waiting = Vec::new();
        for index in 0..WAITERS {
            let mut producer = channel.sender();
            let value = index.checked_add(1).assured("the waiter index is bounded");
            let (ready, parked) = oneshot::channel();
            waiting.push(parked);
            workers.spawn(async move {
                let mut ready = Some(ready);
                let mut send = pin!(producer.send(value, NonZeroU64::MIN));
                let outcome = poll_fn(|cx| {
                    let outcome = send.as_mut().poll(cx);
                    if outcome.is_pending()
                        && let Some(first_pending) = ready.take()
                    {
                        first_pending
                            .send(())
                            .assured("the measurement waits for parking");
                    }
                    outcome
                })
                .await;
                assert_eq!(outcome, Send::Published);
            });
        }
        for parked in waiting {
            parked.await.assured("a full relay parks every waiter");
        }

        let started = Instant::now();
        assert_eq!(take(&mut reader).await, 0);
        let mut received = 0;
        for _ in 0..WAITERS {
            received += take(&mut reader).await;
        }
        while let Some(outcome) = workers.join_next().await {
            outcome.assured("every waiting send finishes");
        }
        let elapsed = started.elapsed();
        assert_eq!(received, 36);
        elapsed
    })
}

fn main() {
    let single = Builder::new_current_thread()
        .build()
        .assured("the single worker benchmark runtime starts");
    let multi = Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .build()
        .assured("the multi-worker benchmark runtime starts");
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2))
        .configure_from_args();

    let mut ready_group = criterion.benchmark_group("ready");
    ready_group.throughput(Throughput::Elements(1));
    ready_group.bench_function("send", |bencher| {
        bencher.iter_custom(|iterations| ready(&single, iterations, true));
    });
    ready_group.bench_function("take", |bencher| {
        bencher.iter_custom(|iterations| ready(&single, iterations, false));
    });
    ready_group.finish();

    let mut contention_group = criterion.benchmark_group("contention");
    for senders in [2, 8, 32] {
        for readers in [1, 4, 16] {
            contention_group.throughput(Throughput::Elements(senders * BATCHES_PER_SENDER));
            contention_group.bench_function(
                BenchmarkId::new(format!("{senders}_senders"), format!("{readers}_readers")),
                |bencher| {
                    bencher.iter_custom(|iterations| {
                        (0..iterations)
                            .map(|_| contended(&multi, senders, readers))
                            .sum()
                    });
                },
            );
        }
    }
    contention_group.finish();

    let mut full_group = criterion.benchmark_group("full");
    full_group.throughput(Throughput::Elements(WAITERS));
    full_group.bench_function("eight_waiters", |bencher| {
        bencher.iter_custom(|iterations| (0..iterations).map(|_| full(&multi)).sum());
    });
    full_group.finish();
    criterion.final_summary();
}
