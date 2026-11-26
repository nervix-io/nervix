//! Cancellation and deadlines of blocking calls.
//!
//! Layer: edges.
//! - **Owns.** The token a host triggers from any thread, its optional deadline, and bounding a
//!   blocking call by both.
//! - **Depends on.** Selected cancellation, monotonic instant and timer primitives, the Rust
//!   client's shortest deadline and the longest wait a client takes.
//! - **Must not know.** What the bounded call does, or what cancelling it means for admitted
//!   work; the session decides that.

use std::{future::Future, time::Duration};

use nervix_client_core::capabilities::SHORTEST_DEADLINE;
use nervix_models::LONGEST_CLIENT_WAIT;
use nervix_primitives::{sync::CancellationToken, time::Instant};

use crate::failure::Failure;

/// A cancellation token, with the deadline it expires at when it has one.
#[derive(Debug, Default)]
pub struct Cancel {
    token: CancellationToken,
    deadline: Option<Instant>,
}

impl Cancel {
    /// A call-local token that inherits this token's cancellation and exact deadline.
    /// Interrupting the child never interrupts its parent or another child.
    pub fn child(&self) -> Self {
        Self {
            token: self.token.child_token(),
            deadline: self.deadline,
        }
    }

    /// A token that also expires `deadline` from now. It accepts the deadlines the Rust client
    /// accepts, from its shortest deadline, one millisecond, through the longest wait a client
    /// takes, one day, and refuses every other as an invalid argument.
    pub fn with_deadline(deadline: Duration) -> Result<Self, Failure> {
        if deadline < SHORTEST_DEADLINE || deadline > LONGEST_CLIENT_WAIT {
            return Err(Failure::invalid_argument(
                "deadline",
                "must be between one millisecond and one day",
            ));
        }
        let expires = Instant::now()
            .checked_add(deadline)
            .ok_or_else(|| Failure::invalid_argument("deadline", "overflows the clock"))?;
        Ok(Self {
            token: CancellationToken::new(),
            deadline: Some(expires),
        })
    }

    pub fn trigger(&self) {
        self.token.cancel();
    }

    /// Runs `work` until it finishes, the token is triggered, or the deadline passes. A triggered
    /// token wins over a result that is ready at the same time, so a host that cancelled never
    /// reads a result as if it had not.
    pub(crate) async fn bound<T>(
        &self,
        work: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        let expiry = async {
            match self.deadline {
                Some(deadline) => nervix_primitives::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        nervix_primitives::select! {
            biased;
            () = self.token.cancelled() => Err(Failure::cancelled()),
            () = expiry => Err(Failure::deadline()),
            result = work => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::SHORTEST_POSITIVE_DURATION;

    use super::*;
    use crate::failure::FailureKind;

    #[test]
    fn a_child_keeps_its_deadline_and_cancellation_without_cancelling_its_siblings() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .assured("a test runtime starts");
        let parent = Cancel::with_deadline(Duration::from_secs(1))
            .assured("one second is within the token's declared deadline bound");
        let child = parent.child();
        let sibling = parent.child();
        assert_eq!(child.deadline, parent.deadline);
        child.trigger();
        assert_eq!(
            runtime
                .block_on(child.bound(async { Ok(()) }))
                .expect_err("the interrupted child refuses its call")
                .kind(),
            FailureKind::Cancelled
        );
        assert!(runtime.block_on(sibling.bound(async { Ok(()) })).is_ok());
        assert!(runtime.block_on(parent.bound(async { Ok(()) })).is_ok());
        parent.trigger();
        assert_eq!(
            runtime
                .block_on(sibling.bound(async { Ok(()) }))
                .expect_err("parent cancellation reaches the sibling")
                .kind(),
            FailureKind::Cancelled
        );
    }

    #[test]
    fn a_token_bounds_a_call_by_cancellation_and_by_deadline() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .assured("a test runtime starts");
        let forever = || std::future::pending::<Result<(), Failure>>();

        let cancelled = Cancel::default();
        cancelled.trigger();
        let result = runtime.block_on(cancelled.bound(async { Ok(()) }));
        assert_eq!(
            result
                .expect_err("a triggered token wins over a ready result")
                .kind(),
            FailureKind::Cancelled
        );

        let expiring = Cancel::with_deadline(Duration::from_millis(10)).assured("a short deadline");
        let result = runtime.block_on(expiring.bound(forever()));
        assert_eq!(
            result.expect_err("the deadline ends the wait").kind(),
            FailureKind::Deadline
        );

        let open = Cancel::default();
        assert!(runtime.block_on(open.bound(async { Ok(()) })).is_ok());

        let too_long = Cancel::with_deadline(Duration::from_secs(2 * 24 * 60 * 60));
        assert_eq!(
            too_long
                .expect_err("a deadline beyond a day is refused")
                .kind(),
            FailureKind::InvalidArgument
        );
    }

    #[test]
    fn a_token_accepts_exactly_the_deadlines_the_rust_client_accepts() {
        let just_short = SHORTEST_DEADLINE
            .checked_sub(SHORTEST_POSITIVE_DURATION)
            .assured("the shortest deadline is longer than the shortest positive duration");
        for too_short in [Duration::ZERO, just_short] {
            let refused = Cancel::with_deadline(too_short)
                .expect_err("a deadline shorter than the Rust client's shortest is refused");
            assert_eq!(refused.kind(), FailureKind::InvalidArgument);
        }
        let just_long = LONGEST_CLIENT_WAIT
            .checked_add(SHORTEST_POSITIVE_DURATION)
            .assured("a day and a nanosecond fit a Duration");
        let refused = Cancel::with_deadline(just_long)
            .expect_err("a deadline longer than the Rust client's longest is refused");
        assert_eq!(refused.kind(), FailureKind::InvalidArgument);
        for accepted in [SHORTEST_DEADLINE, LONGEST_CLIENT_WAIT] {
            let token = Cancel::with_deadline(accepted)
                .assured("a deadline the Rust client accepts is accepted");
            assert!(token.deadline.is_some());
        }
    }
}
