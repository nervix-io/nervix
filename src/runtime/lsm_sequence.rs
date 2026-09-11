//! The per-store sequence number that orders a replicated runtime state's snapshots.

use std::sync::atomic::{AtomicU64, Ordering};

use meticulous::OptionExt as _;

/// The log-structured-merge sequence of one replicated runtime state store.
///
/// The sequence starts at whatever a restored snapshot recorded and advances by one for every
/// state change the store persists or replicates. Followers compare it to decide whether an
/// arriving snapshot is newer than the one they hold, so the value must be strictly increasing
/// and must never be reused.
#[derive(Debug)]
pub(super) struct LsmSequence(AtomicU64);

impl LsmSequence {
    /// Start the sequence at the value a restored snapshot recorded, or at zero for a fresh store.
    pub(super) fn restored(lsm: u64) -> Self {
        Self(AtomicU64::new(lsm))
    }

    /// Advance the sequence by one and return the value the caller's state change is stamped with.
    pub(super) fn advance(&self) -> u64 {
        // One increment per persisted state change: a store would have to persist 2^64 changes
        // before this could overflow, which no node lives long enough to do.
        self.0
            .fetch_add(1, Ordering::SeqCst)
            .checked_add(1)
            .assured("a state store cannot persist 2^64 changes in the lifetime of a node")
    }

    pub(super) fn current(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    /// Adopt the sequence of a snapshot taken elsewhere, after the caller has established that it
    /// supersedes the local state.
    pub(super) fn adopt(&self, lsm: u64) {
        self.0.store(lsm, Ordering::SeqCst);
    }
}
