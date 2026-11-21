//! The host's read-ahead overflow report as the production-owner checks of an MQTT ingestor see it.
//!
//! Layer: test harness.
//! - **Owns.** One count per check, and the overflow reports that record into it.
//! - **Depends on.** The connector contract's read-ahead overflow report and the boundary's
//!   atomics.
//! - **Must not know.** How the host counts an overflow: its metrics, registry, labels or graph.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_connector::IngestorReadAheadOverflow;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Its count uses sequentially consistent operations, the only ordering Shuttle models exactly,
/// because the driver's Shuttle checks read it too.
pub(crate) struct ReadAheadOverflowFixture {
    dropped: Arc<AtomicU64>,
}

impl ReadAheadOverflowFixture {
    pub(crate) fn new() -> Self {
        Self {
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A report that records into this fixture's count, as the host's report records into the
    /// unit's counter.
    pub(crate) fn report(&self) -> IngestorReadAheadOverflow {
        let dropped = self.dropped.clone();
        IngestorReadAheadOverflow::new(move || {
            dropped.fetch_add(1, Ordering::SeqCst);
        })
    }

    /// How many drops the reports of this fixture recorded.
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::SeqCst)
    }
}
