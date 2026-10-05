//! Bounded delivery from detector callbacks to the findings owner.
//!
//! Layer: primitives.
//! - **Owns.** Queue admission, explicit refusal counts and terminal close.
//! - **Depends on.** The selected concurrent queue and standalone atomic counters.
//! - **Must not know.** Deloxide graphs, source context, clocks or process policy.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "diagnostic backend handoff mechanism; no application lock is acquired"
    )
)]

use std::num::NonZeroU64;

use crate::{
    collections::{ConcurrentQueue, PopError},
    sync::atomic::{AtomicU64, Ordering},
};

/// The queue synchronizes payload delivery; the relaxed counter publishes no other location.
pub struct ReportHandoff<T> {
    reports: ConcurrentQueue<T>,
    lost: AtomicU64,
}

impl<T> ReportHandoff<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            reports: ConcurrentQueue::bounded(capacity),
            lost: AtomicU64::new(0),
        }
    }

    pub fn submit(&self, report: T) -> bool {
        if self.reports.push(report).is_ok() {
            return true;
        }
        let mut count = self.lost.load(Ordering::Relaxed);
        loop {
            let next = match count.checked_add(1) {
                Some(next) => next,
                // This is a lower bound on refused reports, already a diagnostic failure.
                // Retain the maximum rather than turning overflow into an absent loss.
                None => count,
            };
            // Even at the limit, compare with the observed count: a concurrent drain must
            // make this retry so the refusal belongs to the next observation.
            match self
                .lost
                .compare_exchange_weak(count, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => count = observed,
            }
        }
        false
    }

    pub fn pop(&self) -> Result<T, PopError> {
        self.reports.pop()
    }
    /// Drain the lower bound on refused reports; a full counter still reports loss.
    pub fn take_lost(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.lost.swap(0, Ordering::Relaxed))
    }
    pub fn close(&self) {
        self.reports.close();
    }
    pub fn is_closed(&self) -> bool {
        self.reports.is_closed()
    }
}

#[cfg(all(test, not(any(feature = "loom", feature = "shuttle"))))]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_count_at_its_limit_still_reports_loss_and_can_be_drained() {
        let reports = ReportHandoff::new(1);
        assert!(reports.submit(1));
        reports.lost.store(u64::MAX - 1, Ordering::Relaxed);
        assert!(!reports.submit(2));
        assert!(!reports.submit(3));
        assert_eq!(reports.take_lost(), NonZeroU64::new(u64::MAX));
        assert!(reports.take_lost().is_none());
        assert!(!reports.submit(4));
        assert_eq!(reports.take_lost(), Some(NonZeroU64::MIN));
        assert_eq!(reports.pop(), Ok(1));
    }
}
