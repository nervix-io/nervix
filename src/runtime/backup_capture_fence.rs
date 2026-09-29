//! The per-domain publication boundary used by a quiesced backup cut.
//!
//! Layer: data plane.
//! - **Owns.** Ordering branch-task state publications against the database view a backup opens.
//! - **Depends on.** The shared atomic primitive boundary.
//! - **Must not know.** Archive sections, domain mutation leases, or the interconnect.

use nervix_primitives::{
    sync::atomic::{AtomicU64, Ordering},
    thread,
};
use triomphe::Arc;

// The low bits count publications that acquired the current generation. Closing a generation
// prevents new publishers from entering; a later generation opens only after its snapshot closes.
const CLOSED: u64 = 1 << 32;
const ACTIVE: u64 = CLOSED - 1;

#[derive(Debug, Default)]
pub(super) struct BackupCaptureFence {
    state: AtomicU64,
}

/// A publisher registers before it begins encoding or changing its branch-owned checkpoint.
/// A cut that closes after registration waits for this guard to leave.
pub(super) struct BackupPublication {
    fence: Arc<BackupCaptureFence>,
}

/// The exclusive storage-view interval of one quiesced cut.
pub(super) struct BackupCut {
    fence: Arc<BackupCaptureFence>,
}

impl BackupCaptureFence {
    pub(super) fn publication(fence: &Arc<Self>) -> BackupPublication {
        loop {
            let state = fence.state.load(Ordering::Relaxed);
            if state & CLOSED != 0 {
                thread::yield_now();
                continue;
            }
            assert!(
                state & ACTIVE < ACTIVE,
                "too many concurrent state publications"
            );
            // This acquire reads the cut generation published by close(). If close() wins the
            // race, the CAS fails and this publication must wait for the next generation.
            if fence
                .state
                .compare_exchange_weak(state, state + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return BackupPublication {
                    fence: fence.clone(),
                };
            }
        }
    }

    pub(super) fn close(fence: &Arc<Self>) -> BackupCut {
        let previous = fence.state.fetch_or(CLOSED, Ordering::Release);
        assert_eq!(previous & CLOSED, 0, "a domain already has a backup cut");
        // Reading the final publisher's Release decrement with Acquire includes everything that
        // publisher completed before leaving. Earlier decrements form the same RMW sequence.
        while fence.state.load(Ordering::Acquire) & ACTIVE != 0 {
            thread::yield_now();
        }
        BackupCut {
            fence: fence.clone(),
        }
    }
}

impl Drop for BackupPublication {
    fn drop(&mut self) {
        self.fence.state.fetch_sub(1, Ordering::Release);
    }
}

impl Drop for BackupCut {
    fn drop(&mut self) {
        // This relaxed RMW extends close()'s release sequence: a publisher that acquires the
        // opened generation also sees the cut's preceding writes. It advances the generation and
        // clears CLOSED in one atomic operation; no publisher can enter between those events.
        let previous = self.fence.state.fetch_add(CLOSED, Ordering::Relaxed);
        assert_eq!(previous & (CLOSED | ACTIVE), CLOSED);
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use meticulous::ResultExt as _;
    use nervix_primitives::{
        sync::atomic::{AtomicUsize, Ordering},
        thread,
    };
    use triomphe::Arc;

    use super::BackupCaptureFence;
    use crate::shuttle_test::{check_pct, check_random};

    fn publication_and_cut() {
        let fence = Arc::new(BackupCaptureFence::default());
        let started = Arc::new(AtomicUsize::new(0));
        let saved = Arc::new(AtomicUsize::new(0));
        let publisher = thread::spawn({
            let (fence, started, saved) = (fence.clone(), started.clone(), saved.clone());
            move || {
                let _publication = BackupCaptureFence::publication(&fence);
                started.store(1, Ordering::Relaxed);
                saved.store(1, Ordering::Relaxed);
            }
        });
        let cut = BackupCaptureFence::close(&fence);
        if started.load(Ordering::Relaxed) != 0 {
            assert_eq!(
                saved.load(Ordering::Relaxed),
                1,
                "a publication registered before the cut was omitted"
            );
        }
        drop(cut);
        publisher.join().assured("the branch publisher completes");
    }

    #[test]
    fn shuttle_backup_cut_includes_pre_cut_branch_publication() {
        check_random(publication_and_cut, 1_000);
        check_pct(publication_and_cut, 1_000, 3);
    }
}

#[cfg(all(test, feature = "loom"))]
mod loom_models {
    use meticulous::ResultExt as _;
    use nervix_model_harness::{InvariantId, loom::explore};
    use nervix_primitives::{
        sync::atomic::{AtomicUsize, Ordering},
        thread,
    };
    use triomphe::Arc;

    use super::BackupCaptureFence;

    const INCLUDED: InvariantId = InvariantId::new("server.backup.cut-includes-publication");
    const GENERATION: InvariantId = InvariantId::new("server.backup.cut-generation");

    #[test]
    fn loom_a_cut_includes_every_registered_publication() {
        explore(INCLUDED, || {
            let fence = Arc::new(BackupCaptureFence::default());
            let payload = Arc::new(AtomicUsize::new(0));
            let publication = BackupCaptureFence::publication(&fence);
            let publisher = thread::spawn({
                let payload = payload.clone();
                move || {
                    payload.store(7, Ordering::Relaxed);
                    drop(publication);
                }
            });
            let cut = BackupCaptureFence::close(&fence);
            assert_eq!(
                payload.load(Ordering::Relaxed),
                7,
                "the cut omitted a publication registered before it closed"
            );
            drop(cut);
            publisher
                .join()
                .assured("the publisher completes after the cut opens");
        });
    }

    #[test]
    fn loom_a_publisher_acquires_the_cut_generation() {
        explore(GENERATION, || {
            let fence = Arc::new(BackupCaptureFence::default());
            let marker = Arc::new(AtomicUsize::new(0));
            let publisher = thread::spawn({
                let (fence, marker) = (fence.clone(), marker.clone());
                move || {
                    let publication = BackupCaptureFence::publication(&fence);
                    let generation = fence.state.load(Ordering::Relaxed) >> 33;
                    if generation > 0 {
                        assert_eq!(
                            marker.load(Ordering::Relaxed),
                            1,
                            "a publisher observed the cut generation without its prior writes"
                        );
                    }
                    drop(publication);
                }
            });
            marker.store(1, Ordering::Relaxed);
            let cut = BackupCaptureFence::close(&fence);
            drop(cut);
            publisher
                .join()
                .assured("the publisher completes after the cut opens");
        });
    }
}
