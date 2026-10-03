//! The signal a running job checks between its own bounded units, and the obligation that raises it.
//!
//! The protocol is synchronous and owned here, apart from the pool that admits and charges the job:
//! [`Cancellation::armed`] creates both ends of one job's cancellation, the awaiting caller holds
//! the [`CancelOnDrop`] obligation, and the job holds the [`Cancellation`] it checks. The pool and
//! the Loom models of this module create a job's cancellation through the same constructor, so the
//! models check the protocol the pool runs.

use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use thiserror::Error;

/// The caller stopped waiting for this job before it finished.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the work was cancelled between bounded units")]
pub struct Cancelled;

/// Raised when the caller stops awaiting a submitted job. A job that has already started keeps
/// running — and keeps its memory reservation — until it observes this between two of its own
/// bounded units and returns.
///
/// Cancelling publishes: a job that observes its cancellation also observes every write its
/// awaiting side made before it stopped waiting, because raising the flag releases those writes and
/// observing it acquires them.
#[derive(Debug, Clone)]
pub struct Cancellation {
    cancelled: Arc<AtomicBool>,
}

/// Both ends of one job's cancellation, created together by [`Cancellation::armed`].
pub(crate) struct ArmedCancellation {
    /// Held by the caller awaiting the job. Dropping it without disarming cancels the job.
    pub(crate) obligation: CancelOnDrop,
    /// Checked by the job between its bounded units.
    pub(crate) signal: Cancellation,
}

impl Cancellation {
    /// One job's cancellation: the obligation its awaiting caller holds, and the signal the job
    /// checks. The job is not cancelled until the obligation is dropped without being disarmed.
    pub(crate) fn armed() -> ArmedCancellation {
        let signal = Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        ArmedCancellation {
            obligation: CancelOnDrop {
                cancellation: Some(signal.clone()),
            },
            signal,
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// The form a job's inner loop uses, so cancellation propagates as an ordinary typed error
    /// through the `?` the loop already writes.
    pub fn check(&self) -> Result<(), Cancelled> {
        if self.is_cancelled() {
            return Err(Cancelled);
        }
        Ok(())
    }
}

/// Raises the job's cancellation if the future awaiting it is dropped. Disarmed once the job's
/// value has been observed, so an ordinary completion never reports itself as cancelled.
pub(crate) struct CancelOnDrop {
    cancellation: Option<Cancellation>,
}

impl CancelOnDrop {
    pub(crate) fn disarm(mut self) {
        self.cancellation = None;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
    }
}

/// The memory-ordering claims of the protocol, explored by Loom over the production types.
///
/// In each model the main thread is the job and a second thread is the caller awaiting it, so the
/// only synchronization between them is the protocol's own. `just test-loom` runs them, and
/// `just test-loom-qualification` shows that weakening either side of the publication makes the
/// publication model fail.
#[cfg(all(test, feature = "loom"))]
mod loom_models {
    use meticulous::ResultExt as _;
    use nervix_model_harness::{
        InvariantId,
        loom::{explore, spawn},
    };
    use nervix_primitives::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{ArmedCancellation, Cancellation, Cancelled};

    const PUBLICATION: InvariantId = InvariantId::new("execution.cancellation.publication");
    const CANCEL_ON_DROP: InvariantId = InvariantId::new("execution.cancellation.cancel-on-drop");
    const DISARM: InvariantId = InvariantId::new("execution.cancellation.disarm");

    /// What the awaiting side writes just before it stops waiting. Both sides access it with
    /// `Relaxed`, so only the cancellation's own release and acquire can make it visible.
    const WRITTEN_BEFORE_CANCELLING: usize = 1;

    #[test]
    fn loom_a_job_that_observes_cancellation_observes_every_write_made_before_it() {
        explore(PUBLICATION, || {
            let ArmedCancellation { obligation, signal } = Cancellation::armed();
            let witness = Arc::new(AtomicUsize::new(0));
            let awaiting_witness = Arc::clone(&witness);
            let awaiting = spawn(move || {
                awaiting_witness.store(WRITTEN_BEFORE_CANCELLING, Ordering::Relaxed);
                // The awaiting caller stops waiting, which drops its armed obligation.
                drop(obligation);
            });

            // The witness is read the moment the job observes its cancellation, before the join
            // below or anything else could order the two threads.
            if signal.check().is_err() {
                assert_eq!(
                    witness.load(Ordering::Relaxed),
                    WRITTEN_BEFORE_CANCELLING,
                    "the job observed its cancellation without the write made before it"
                );
            }
            awaiting
                .join()
                .assured("the awaiting side only writes the witness and drops its obligation");
        });
    }

    #[test]
    fn loom_dropping_the_obligation_cancels_every_later_check_of_the_job() {
        explore(CANCEL_ON_DROP, || {
            let ArmedCancellation { obligation, signal } = Cancellation::armed();
            // A job hands its signal to the bounded units it runs.
            let unit_signal = signal.clone();
            let awaiting = spawn(move || drop(obligation));

            let first = signal.check();
            let second = unit_signal.check();
            if first.is_err() {
                assert_eq!(
                    second,
                    Err(Cancelled),
                    "a later check lost a cancellation an earlier check observed"
                );
            }
            awaiting
                .join()
                .assured("the awaiting side only drops its obligation");
            assert_eq!(
                signal.check(),
                Err(Cancelled),
                "a dropped armed obligation left its job uncancelled"
            );
            assert!(unit_signal.is_cancelled());
        });
    }

    #[test]
    fn loom_a_disarmed_obligation_never_cancels_its_job() {
        explore(DISARM, || {
            let ArmedCancellation { obligation, signal } = Cancellation::armed();
            let awaiting = spawn(move || obligation.disarm());

            assert_eq!(
                signal.check(),
                Ok(()),
                "a job racing the disarm observed a cancellation"
            );
            awaiting
                .join()
                .assured("the awaiting side only disarms its obligation");
            assert_eq!(
                signal.check(),
                Ok(()),
                "a disarmed obligation cancelled its job when it was dropped"
            );
        });
    }
}
