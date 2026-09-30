use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use nervix_primitives::time::Instant;
use nervix_server::runtime::StateReplicationBenchmark;

/// Branches the replica's branch lifecycle names. Installing each of them checks the lifecycle
/// once.
const LIFECYCLE_BRANCHES: [usize; 3] = [16, 128, 1_024];

fn acknowledged_commits(iterations: u64) -> Duration {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime must build");
    runtime.block_on(async move {
        let mut benchmark = StateReplicationBenchmark::new(0);
        let started = Instant::now();
        for _ in 0..iterations {
            nervix_primitives::task::consume_budget().await;
            benchmark.commit_acknowledged_offset().await;
        }
        started.elapsed()
    })
}

fn state_replication_benches(criterion: &mut Criterion) {
    let mut commits = criterion.benchmark_group("state_replication/acknowledged_commit");
    commits.throughput(Throughput::Elements(1));
    commits.bench_function("one_replica", |bencher| {
        bencher.iter_custom(acknowledged_commits);
    });
    commits.finish();

    let mut installations =
        criterion.benchmark_group("state_replication/branch_checkpoint_installation_check");
    for branches in LIFECYCLE_BRANCHES {
        installations.throughput(Throughput::Elements(
            u64::try_from(branches).expect("branch counts fit u64"),
        ));
        let benchmark = StateReplicationBenchmark::new(branches);
        installations.bench_function(format!("{branches}_branches"), |bencher| {
            bencher.iter(|| benchmark.check_every_branch_checkpoint());
        });
    }
    installations.finish();
}

criterion_group!(benches, state_replication_benches);
criterion_main!(benches);
