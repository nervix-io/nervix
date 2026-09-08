//! The signal a running job checks between its own bounded units.

use std::sync::atomic::{AtomicBool, Ordering};

use thiserror::Error;
use triomphe::Arc;

/// The caller stopped waiting for this job before it finished.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the work was cancelled between bounded units")]
pub struct Cancelled;

/// Raised when the caller stops awaiting a submitted job. A job that has already started keeps
/// running — and keeps its memory reservation — until it observes this between two of its own
/// bounded units and returns.
#[derive(Debug, Clone)]
pub struct Cancellation {
    cancelled: Arc<AtomicBool>,
}

impl Cancellation {
    pub(crate) fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn cancel(&self) {
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
    pub(crate) fn new(cancellation: Cancellation) -> Self {
        Self {
            cancellation: Some(cancellation),
        }
    }

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
