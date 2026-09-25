//! Cancellation and deadlines of blocking calls: `nx_cancel`.
//!
//! - **Owns.** The token a host triggers from any thread, its optional deadline, and bounding a
//!   blocking call by both.
//! - **Depends on.** Tokio's cancellation token and timer.
//! - **Must not know.** What the bounded call does, or what cancelling it means for admitted
//!   work; the session decides that.

use std::{future::Future, time::Duration};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{abi, failure::Failure};

/// The longest deadline a token accepts: one day, the longest deadline the client accepts.
const MAX_DEADLINE: Duration = Duration::from_secs(24 * 60 * 60);

/// A cancellation token, with the deadline it expires at when it has one.
#[derive(Debug, Default)]
pub struct Cancel {
    token: CancellationToken,
    deadline: Option<Instant>,
}

impl Cancel {
    /// A token that also expires `deadline` from now.
    pub fn with_deadline(deadline: Duration) -> Result<Self, Failure> {
        if deadline > MAX_DEADLINE {
            return Err(Failure::invalid_argument(
                "deadline_millis",
                "exceeds the one-day limit",
            ));
        }
        let expires = Instant::now()
            .checked_add(deadline)
            .ok_or_else(|| Failure::invalid_argument("deadline_millis", "overflows the clock"))?;
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
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            () = self.token.cancelled() => Err(Failure::cancelled()),
            () = expiry => Err(Failure::deadline()),
            result = work => result,
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn nx_cancel_new() -> *mut Cancel {
    abi::into_handle(Cancel::default())
}

/// # Safety
///
/// A non-null `out` is writable for a handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_cancel_with_deadline(
    deadline_millis: u64,
    out: *mut *mut Cancel,
) -> *mut Failure {
    // SAFETY: the header requires a writable `out`.
    abi::outcome(unsafe { write_with_deadline(deadline_millis, out) })
}

/// # Safety
///
/// A non-null `out` is writable for a handle.
unsafe fn write_with_deadline(deadline_millis: u64, out: *mut *mut Cancel) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    let cancel = Cancel::with_deadline(Duration::from_millis(deadline_millis))?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(cancel)) };
    Ok(())
}

/// # Safety
///
/// `cancel` is a live token this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_cancel_trigger(cancel: *const Cancel) {
    // SAFETY: the header requires a live token.
    unsafe { abi::accessor(cancel) }.trigger();
}

/// # Safety
///
/// A non-null `cancel` is a token this library returned that has not been freed, and no call is
/// still waiting on it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_cancel_free(cancel: *mut Cancel) {
    // SAFETY: the header requires an unreleased token or null.
    unsafe { abi::release(cancel) };
}
