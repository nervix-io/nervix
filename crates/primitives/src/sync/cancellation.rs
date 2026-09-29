//! The cancellation token of a Shuttle build.
//!
//! Shuttle's token models cancellation, but it lacks two parts of the interface Tokio Util's token
//! has: equality by clone identity, where a clone equals its original and a child token is a new
//! token, and the operations that take the token by value. This token wraps Shuttle's and supplies
//! both, so an owner compares and consumes tokens the same way in every mode.

use std::{future::Future, ops::Deref, sync::Arc};

use shuttle_tokio_util::sync::{DropGuard, WaitForCancellationFutureOwned};

/// A cancellation token with the clone identity Tokio Util's token has.
#[derive(Debug)]
pub struct CancellationToken {
    inner: shuttle_tokio_util::sync::CancellationToken,
    identity: Arc<()>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            inner: shuttle_tokio_util::sync::CancellationToken::new(),
            identity: Arc::new(()),
        }
    }

    /// A token cancelled with this one, which is a token of its own and so unequal to it.
    pub fn child_token(&self) -> Self {
        Self {
            inner: self.inner.child_token(),
            identity: Arc::new(()),
        }
    }

    pub fn drop_guard(self) -> DropGuard {
        self.inner.drop_guard()
    }

    pub fn cancelled_owned(self) -> WaitForCancellationFutureOwned {
        self.inner.cancelled_owned()
    }

    /// Run `future` until it completes or the token is cancelled, holding the token meanwhile.
    pub async fn run_until_cancelled_owned<F>(self, future: F) -> Option<F::Output>
    where
        F: Future,
    {
        self.inner.run_until_cancelled(future).await
    }
}

impl Clone for CancellationToken {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            identity: Arc::clone(&self.identity),
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for CancellationToken {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }
}

impl Eq for CancellationToken {}

impl Deref for CancellationToken {
    type Target = shuttle_tokio_util::sync::CancellationToken;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
