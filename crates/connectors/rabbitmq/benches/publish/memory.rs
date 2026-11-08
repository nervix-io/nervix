//! jemalloc observations of one warmed production publication.
//!
//! Outside the layer order: a benchmark harness.
//! - **Owns.** The benchmark allocator and before/after process-wide allocator samples.
//! - **Depends on.** The workspace's jemalloc allocator and its statistics API.
//! - **Must not know.** Publication outcomes, protocol state or model-checker scheduling.

use meticulous::OptionExt as _;
use tikv_jemalloc_ctl::{epoch, raw, stats};

use super::require;

#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub(super) struct Snapshot {
    requests: u64,
    allocated: usize,
    resident: usize,
    retained: usize,
}

impl Snapshot {
    pub(super) fn capture() -> Self {
        require(epoch::advance());
        // SAFETY: jemalloc's stats-enabled mallctl contract declares both nrequests values as
        // uint64_t. Its MALLCTL_ARENAS_ALL constant is 4096. The workspace pins this implementation;
        // these null-terminated keys read the aggregate counters, without modifying the allocator.
        let small = require(unsafe { raw::read::<u64>(b"stats.arenas.4096.small.nrequests\0") });
        // SAFETY: the large allocation request counter has the same uint64_t contract above.
        let large = require(unsafe { raw::read::<u64>(b"stats.arenas.4096.large.nrequests\0") });
        Self {
            requests: small
                .checked_add(large)
                .assured("the bounded benchmark cannot make u64::MAX allocation requests"),
            allocated: require(stats::allocated::read()),
            resident: require(stats::resident::read()),
            retained: require(stats::retained::read()),
        }
    }

    pub(super) fn report(&self, before: &Self, count: usize, payload_bytes: usize) {
        let requests = self
            .requests
            .checked_sub(before.requests)
            .assured("the live benchmark's aggregate allocation request counters only increase");
        eprintln!(
            "rabbitmq_memory_evidence messages={count} payload_bytes={payload_bytes} \
             reported_allocation_requests={requests} allocated_before={} allocated_after={} \
             resident_before={} resident_after={} retained_before={} retained_after={}",
            before.allocated,
            self.allocated,
            before.resident,
            self.resident,
            before.retained,
            self.retained,
        );
    }
}
