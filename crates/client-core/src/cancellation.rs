//! Edges layer: a caller's cancellation of one call.
//!
//! - **Owns.** Whether a caller can cancel a call, waiting for that cancellation, and running a
//!   wait of the call that the cancellation ends.
//! - **Depends on.** The cancellation token of the primitives boundary and the client's errors.
//! - **Must not know.** How a call asks the server to cancel the request it has in flight.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::pin::Pin;

use error_stack::Report;
use nervix_primitives::sync::CancellationToken;

use crate::error::ClientError;

/// The caller's cancellation of one call, or none for a call no caller can cancel.
#[derive(Clone, Copy)]
pub(crate) struct CallCancellation<'call> {
    token: Option<&'call CancellationToken>,
}

impl<'call> CallCancellation<'call> {
    /// A call no caller can cancel.
    pub(crate) const NONE: CallCancellation<'static> = CallCancellation { token: None };

    /// A call its caller cancels by cancelling `token`.
    pub(crate) fn by(token: &'call CancellationToken) -> Self {
        Self { token: Some(token) }
    }

    /// Whether the caller has cancelled the call.
    pub(crate) fn is_requested(self) -> bool {
        match self.token {
            Some(token) => token.is_cancelled(),
            None => false,
        }
    }

    /// Resolves once the caller cancels the call, and never for a call no caller can cancel.
    pub(crate) async fn requested(self) {
        match self.token {
            Some(token) => token.cancelled().await,
            None => std::future::pending().await,
        }
    }

    /// Runs `work` unless the caller cancels the call first. A cancellation stops `work` where it
    /// waits, as dropping it does, and fails the call with [`ClientError::CallCancelled`].
    ///
    /// `work` stays pinned where its caller holds it, so this wait adds only a reference to the
    /// call's state. A future moved into the wait would be copied into every frame that polls it,
    /// which a host blocking on the call pays for on its own thread's stack.
    pub(crate) async fn unless_requested<F: Future>(
        self,
        work: Pin<&mut F>,
    ) -> error_stack::Result<F::Output, ClientError> {
        let Some(token) = self.token else {
            return Ok(work.await);
        };
        match token.run_until_cancelled(work).await {
            Some(output) => Ok(output),
            None => Err(Report::new(ClientError::CallCancelled)),
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use super::*;

    #[nervix_primitives::test]
    async fn a_call_no_caller_can_cancel_runs_its_work_to_the_end() {
        let work = std::pin::pin!(async { 7 });
        let ran = CallCancellation::NONE.unless_requested(work).await;
        assert_eq!(ran.ok(), Some(7));
        assert!(!CallCancellation::NONE.is_requested());
    }

    #[nervix_primitives::test]
    async fn a_cancelled_call_stops_its_work_where_it_waits() {
        let token = CancellationToken::new();
        let cancellation = CallCancellation::by(&token);
        assert!(!cancellation.is_requested());
        token.cancel();
        assert!(cancellation.is_requested());
        let waiting = std::pin::pin!(std::future::pending::<()>());
        let stopped = cancellation.unless_requested(waiting).await;
        let Err(report) = stopped else {
            panic!("a cancelled call must not wait for its work");
        };
        assert!(matches!(
            report.current_context(),
            ClientError::CallCancelled
        ));
        cancellation.requested().await;
    }
}
