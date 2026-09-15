//! Consensus-connectivity fault control for the server test harness.
//!
//! Layer: engines and infrastructure; mutation is exposed only to test harnesses.
//! - **Owns.** A process-local switch that rejects Raft and admission-read traffic.
//! - **Depends on.** Atomic synchronization.
//! - **Must not know.** Runtime ownership, scheduling, or application listener behavior.

use std::io;

#[cfg(not(any(test, feature = "testing")))]
#[derive(Clone, Default)]
pub(crate) struct ConnectivityFault(());

#[cfg(not(any(test, feature = "testing")))]
impl ConnectivityFault {
    pub(crate) fn check(&self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(any(test, feature = "testing"))]
pub use enabled::ConnectivityFault;

#[cfg(any(test, feature = "testing"))]
mod enabled {
    use std::sync::atomic::{AtomicBool, Ordering};

    use triomphe::Arc;

    use super::*;

    #[derive(Clone, Debug, Default)]
    pub struct ConnectivityFault {
        blocked: Arc<AtomicBool>,
    }

    impl ConnectivityFault {
        pub fn block(&self) {
            self.blocked.store(true, Ordering::Release);
        }

        pub fn restore(&self) {
            self.blocked.store(false, Ordering::Release);
        }

        pub(crate) fn check(&self) -> io::Result<()> {
            if self.blocked.load(Ordering::Acquire) {
                Err(io::Error::other("injected consensus connectivity failure"))
            } else {
                Ok(())
            }
        }
    }
}
