//! Immutable generations a branch task publishes for the branch-local state it alone changes.
//!
//! Layer: data plane.
//! - **Owns.** Publishing a generation by pointer replacement, the revision each generation is
//!   stamped with, and whether the live state is ahead of the published generation or the
//!   published generation is ahead of the persisted one.
//! - **Depends on.** `arc-swap` and the runtime state sequence.
//! - **Must not know.** What a generation holds, how it is encoded, or where it is persisted.

use std::sync::{
    Arc as StdArc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use arc_swap::ArcSwap;

use super::lsm_sequence::LsmSequence;

/// One value a branch task published, and the revision it stands at.
#[derive(Debug)]
pub(super) struct Generation<T> {
    pub(super) revision: u64,
    pub(super) value: T,
}

/// The generations one branch task publishes for branch-local state that only it changes.
///
/// The task changes its live state without a lock and marks it dirty, and publishing replaces one
/// pointer. A reader keeps the generation it loaded for as long as it needs, so a snapshot encodes,
/// a replica receives, and a later task restores a value the owning task no longer touches while
/// that task goes on changing its live state.
#[derive(Debug)]
pub(super) struct PublishedGenerations<T> {
    published: ArcSwap<Generation<T>>,
    /// Allocates the revision each published generation is stamped with.
    revisions: LsmSequence,
    last_persisted_lsm: AtomicU64,
    /// Whether the owning task changed its live state after its last publication.
    live_dirty: AtomicBool,
}

impl<T> PublishedGenerations<T> {
    /// Start from `value` as the generation at `revision`, which is taken as already persisted.
    pub(super) fn restored(revision: u64, value: T) -> Self {
        Self {
            published: ArcSwap::from_pointee(Generation { revision, value }),
            revisions: LsmSequence::restored(revision),
            last_persisted_lsm: AtomicU64::new(revision),
            live_dirty: AtomicBool::new(false),
        }
    }

    /// Record that the owning task changed its live state after its last publication.
    pub(super) fn mark_live_dirty(&self) {
        self.live_dirty.store(true, Ordering::SeqCst);
    }

    /// Whether the owning task changed its live state after its last publication.
    pub(super) fn is_live_dirty(&self) -> bool {
        self.live_dirty.load(Ordering::SeqCst)
    }

    /// Publish `value`, everything the owning task's live state holds, as the next generation.
    ///
    /// Only the owning task publishes and marks its live state dirty, so no change can land between
    /// stamping this generation and clearing the mark.
    pub(super) fn publish(&self, value: T) {
        let revision = self.revisions.advance();
        self.published
            .store(StdArc::new(Generation { revision, value }));
        self.live_dirty.store(false, Ordering::SeqCst);
    }

    /// The generation published last.
    pub(super) fn load(&self) -> StdArc<Generation<T>> {
        self.published.load_full()
    }

    /// The generation published last, when its revision is after `after_lsm`.
    pub(super) fn load_after(&self, after_lsm: Option<u64>) -> Option<StdArc<Generation<T>>> {
        let published = self.published.load_full();
        if let Some(after_lsm) = after_lsm
            && published.revision <= after_lsm
        {
            return None;
        }
        Some(published)
    }

    pub(super) fn last_persisted_lsm(&self) -> u64 {
        self.last_persisted_lsm.load(Ordering::SeqCst)
    }

    /// Record that the generation at `lsm` is persisted. Persisting an older generation afterwards
    /// never moves this back.
    pub(super) fn record_persisted(&self, lsm: u64) {
        self.last_persisted_lsm.fetch_max(lsm, Ordering::SeqCst);
    }
}
