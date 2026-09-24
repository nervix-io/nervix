//! Where one session request stands between its arrival and its admission.
//!
//! Layer: edges.
//!
//! - **Owns.** The single decision between admitting a request and cancelling it, and the wake-up
//!   a cancellation that wins that decision delivers.
//! - **Depends on.** Tokio's notification primitive.
//! - **Must not know.** What a request does once admitted, or how its replies travel.

use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::Notify;

/// The stage a request was cancelled at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum CancelledStage {
    /// The request had not been admitted, and never will be. It has no effects.
    BeforeAdmission,
    /// The request was already admitted. Its effects may still complete and are recovered by its
    /// identity; cancelling stops only the wait for them.
    AfterAdmission,
}

/// A request was cancelled before it was admitted, so it must not begin any effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) struct CancelledBeforeAdmission;

/// The admission decision of one request.
///
/// The request calls [`RequestAdmission::admit`] immediately before its first effect, and a
/// cancellation calls [`RequestAdmission::cancel`]. Whichever arrives first decides the stage for
/// good, so a request is never admitted after a cancellation reported it unadmitted.
///
/// The decision is packed into one atomic byte, which this type alone reads and writes: `PENDING`
/// until either transition, then `ADMITTED` or `CANCELLED` forever.
#[derive(Debug, Default)]
pub(in crate::application) struct RequestAdmission {
    state: AtomicU8,
    cancelled: Notify,
}

const PENDING: u8 = 0;
const ADMITTED: u8 = 1;
const CANCELLED: u8 = 2;

impl RequestAdmission {
    /// Admits the request unless a cancellation already decided otherwise.
    pub(in crate::application) fn admit(&self) -> Result<(), CancelledBeforeAdmission> {
        match self
            .state
            .compare_exchange(PENDING, ADMITTED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(ADMITTED) => Ok(()),
            Err(_) => Err(CancelledBeforeAdmission),
        }
    }

    /// Cancels the request, reporting whether the cancellation landed before or after admission.
    pub(in crate::application) fn cancel(&self) -> CancelledStage {
        match self
            .state
            .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                self.cancelled.notify_waiters();
                CancelledStage::BeforeAdmission
            }
            Err(CANCELLED) => CancelledStage::BeforeAdmission,
            Err(_) => CancelledStage::AfterAdmission,
        }
    }

    /// Whether a cancellation won the decision.
    pub(in crate::application) fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == CANCELLED
    }

    /// Resolves once a cancellation wins the decision. It never resolves for an admitted request.
    pub(in crate::application) async fn cancelled_before_admission(&self) {
        loop {
            tokio::task::consume_budget().await;
            let notified = self.cancelled.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn the_first_of_admission_and_cancellation_decides_the_stage() {
        let admitted = RequestAdmission::default();
        assert_eq!(admitted.admit(), Ok(()));
        assert_eq!(admitted.cancel(), CancelledStage::AfterAdmission);
        assert_eq!(admitted.admit(), Ok(()));
        assert!(!admitted.is_cancelled());

        let cancelled = RequestAdmission::default();
        assert_eq!(cancelled.cancel(), CancelledStage::BeforeAdmission);
        assert_eq!(cancelled.admit(), Err(CancelledBeforeAdmission));
        assert_eq!(cancelled.cancel(), CancelledStage::BeforeAdmission);
        assert!(cancelled.is_cancelled());
    }

    #[tokio::test]
    async fn a_cancellation_before_admission_wakes_its_waiter() {
        let admission = triomphe::Arc::new(RequestAdmission::default());
        let waiter = admission.clone();
        let waiting = tokio::spawn(async move { waiter.cancelled_before_admission().await });
        tokio::task::yield_now().await;
        assert_eq!(admission.cancel(), CancelledStage::BeforeAdmission);
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .assured("a cancellation wakes the waiter within the deadline")
            .assured("the waiter does not panic");
    }
}
