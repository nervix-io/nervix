//! Same-host cost probes for the ordinary primitive boundary.
//!
//! Outside the layer order: a benchmark harness. Product code must not name it.
//!
//! - **Owns.** Warm operation timings for atomics, publication, blocking locks and shared ownership.
//! - **Depends on.** The ordinary native primitive API and Criterion.
//! - **Must not know.** Runtime graph policy or any model checker backend.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use nervix_primitives::{
    publication::ArcSwap,
    sync::{
        Arc, StdArc,
        atomic::{AtomicU64, Ordering},
        blocking::Mutex,
    },
};

fn ordinary_cost(criterion: &mut Criterion) {
    let counter = AtomicU64::new(0);
    let mut atomics = criterion.benchmark_group("ordinary_primitives/atomic");
    atomics.throughput(Throughput::Elements(1));
    atomics.bench_function("acquire_load", |bencher| {
        bencher.iter(|| black_box(counter.load(Ordering::Acquire)));
    });
    atomics.bench_function("acq_rel_increment", |bencher| {
        bencher.iter(|| black_box(counter.fetch_add(1, Ordering::AcqRel)));
    });
    atomics.finish();

    let publication = ArcSwap::from_pointee(0_u64);
    let values = [StdArc::new(1_u64), StdArc::new(2_u64)];
    let mut publication_group = criterion.benchmark_group("ordinary_primitives/publication");
    publication_group.throughput(Throughput::Elements(1));
    publication_group.bench_function("borrowed_load", |bencher| {
        bencher.iter(|| black_box(publication.load()));
    });
    publication_group.bench_function("owned_load", |bencher| {
        bencher.iter(|| black_box(publication.load_full()));
    });
    let mut next = 0_usize;
    publication_group.bench_function("store_preallocated", |bencher| {
        bencher.iter(|| {
            next ^= 1;
            publication.store(StdArc::clone(&values[next]));
        });
    });
    publication_group.finish();

    let lock = Mutex::new(false);
    let mut locks = criterion.benchmark_group("ordinary_primitives/blocking_lock");
    locks.throughput(Throughput::Elements(1));
    locks.bench_function("uncontended_update", |bencher| {
        bencher.iter(|| {
            let mut guard = lock.lock();
            *guard = !*guard;
            black_box(*guard)
        });
    });
    locks.finish();

    let shared = Arc::new(0_u64);
    let mut references = criterion.benchmark_group("ordinary_primitives/shared_ownership");
    references.throughput(Throughput::Elements(1));
    references.bench_function("clone_drop", |bencher| {
        bencher.iter(|| black_box(shared.clone()));
    });
    references.finish();
}

criterion_group!(benches, ordinary_cost);
criterion_main!(benches);
