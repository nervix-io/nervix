//! Per-batch cost of the branch owner a relay owner task holds.
//!
//! Outside the layer order: a benchmark harness. Product code must not name it.
//!
//! - **Owns.** Established-branch and branch-churn admission workloads at fixed branch counts.
//! - **Depends on.** The public branch owner and presence of `nervix-branch-instances`.
//! - **Must not know.** Relays, batches, metrics or how an owner task reaches its branches.

use std::{hint::black_box, num::NonZeroUsize, sync::Arc as StdArc};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_branch_instances::{BranchPresence, OwnedBranches};
use nervix_models::Timestamp;
use triomphe::Arc;

/// Branch counts an owner holds while it admits batches.
const HELD: [usize; 3] = [1, 64, 4_096];

/// A branch key shared by reference, as a relay's `BranchKey` is: cloning it counts a reference
/// instead of copying the key.
type Key = Arc<str>;

fn keys(count: usize) -> Vec<Key> {
    (0..count)
        .map(|key| Arc::from(format!("tenant-{key}").as_str()))
        .collect()
}

/// The ring position after `index` in a ring of `len` keys.
fn following(index: usize, len: usize) -> usize {
    let next = index
        .checked_add(1)
        .assured("a ring position is below the ring's length");
    next % len
}

fn at(nanos: u64) -> Timestamp {
    Timestamp::from_unix_nanos(
        i64::try_from(nanos).assured("a benchmark admits fewer than 2^63 batches"),
    )
}

fn admit(
    owner: &mut OwnedBranches<Key, u64>,
    key: &Key,
    now: u64,
    capacity: Option<NonZeroUsize>,
) -> usize {
    let admission = owner
        .admit(Some(key), at(now), capacity, |_, incarnation| {
            Ok::<u64, std::convert::Infallible>(incarnation)
        })
        .assured("the benchmark's branch constructor cannot fail");
    admission.evicted.len()
}

/// An owner holding `held` branches admits a batch for each of them in turn, least recently used
/// first, as interleaved branches arrive. No batch changes the membership, so none publishes.
fn established_branches(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("owned_branches/established");
    group.throughput(Throughput::Elements(1));
    for held in HELD {
        let presence = Arc::new(BranchPresence::<Key>::new());
        let mut owner = OwnedBranches::claim(presence.clone());
        let branch_keys = keys(held);
        let mut now = 0_u64;
        for key in &branch_keys {
            now = now
                .checked_add(1)
                .assured("the setup admits a bounded batch count");
            admit(&mut owner, key, now, None);
        }
        let published = presence.load();
        let mut next = 0_usize;
        group.bench_with_input(BenchmarkId::new("held", held), &held, |bencher, _| {
            bencher.iter(|| {
                now = now
                    .checked_add(1)
                    .assured("a benchmark admits a bounded batch count");
                let evicted = admit(&mut owner, &branch_keys[next], now, None);
                next = following(next, branch_keys.len());
                black_box(evicted)
            });
        });
        assert!(
            StdArc::ptr_eq(&published, &presence.load()),
            "established-branch batches must not publish"
        );
    }
    group.finish();
}

/// An owner at capacity `held` admits a batch for a branch it does not hold, which creates that
/// branch, evicts the least recently used one and publishes the membership once.
fn branch_churn(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("owned_branches/churn");
    group.throughput(Throughput::Elements(1));
    for held in HELD {
        let capacity = NonZeroUsize::new(held).assured("the benchmark sizes are positive");
        let presence = Arc::new(BranchPresence::<Key>::new());
        let mut owner = OwnedBranches::claim(presence.clone());
        // A key returns only after `held` other keys were admitted, so it was evicted by then.
        let ring = keys(held.checked_mul(2).assured("the benchmark sizes are small"));
        let mut now = 0_u64;
        let mut next = 0_usize;
        for _ in 0..ring.len() {
            now = now
                .checked_add(1)
                .assured("the setup admits a bounded batch count");
            admit(&mut owner, &ring[next], now, Some(capacity));
            next = following(next, ring.len());
        }
        group.bench_with_input(BenchmarkId::new("held", held), &held, |bencher, _| {
            bencher.iter(|| {
                now = now
                    .checked_add(1)
                    .assured("a benchmark admits a bounded batch count");
                let evicted = admit(&mut owner, &ring[next], now, Some(capacity));
                next = following(next, ring.len());
                black_box(evicted)
            });
        });
        assert_eq!(presence.load().branch_count(), held);
    }
    group.finish();
}

criterion_group!(benches, established_branches, branch_churn);
criterion_main!(benches);
