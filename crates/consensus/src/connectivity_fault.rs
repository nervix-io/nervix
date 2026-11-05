//! Consensus-connectivity fault control for the server test harness.
//!
//! Layer: engines and infrastructure; mutation is exposed only to test harnesses.
//! - **Owns.** Process-local controls for Raft connectivity and follower heartbeat answers.
//! - **Depends on.** Atomic synchronization and the test clock.
//! - **Must not know.** Runtime ownership, scheduling, or application listener behavior.

use std::io;

#[cfg(not(feature = "testing"))]
#[derive(Clone, Default)]
pub(crate) struct ConnectivityFault(());

#[cfg(not(feature = "testing"))]
impl ConnectivityFault {
    pub(crate) fn check(&self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "testing")]
pub use enabled::ConnectivityFault;

#[cfg(feature = "testing")]
mod enabled {
    use std::time::Duration;

    use nervix_primitives::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    use super::*;

    #[derive(Clone, Debug, Default)]
    pub struct ConnectivityFault {
        blocked: Arc<AtomicBool>,
        heartbeat_response_delay: Arc<HeartbeatResponseDelay>,
    }

    #[derive(Debug, Default)]
    struct HeartbeatResponseDelay {
        milliseconds: AtomicU64,
        received: AtomicU64,
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

        pub(crate) fn set_heartbeat_response_delay(&self, delay: Duration) {
            let milliseconds = u64::try_from(delay.as_millis())
                .expect("the fixture's heartbeat answer delay fits milliseconds");
            self.heartbeat_response_delay
                .milliseconds
                .store(milliseconds, Ordering::Relaxed);
        }

        pub(crate) fn delayed_heartbeat_requests(&self) -> u64 {
            self.heartbeat_response_delay
                .received
                .load(Ordering::Relaxed)
        }

        pub(crate) async fn after_heartbeat_append(&self) {
            let milliseconds = self
                .heartbeat_response_delay
                .milliseconds
                .load(Ordering::Relaxed);
            if milliseconds == 0 {
                return;
            }
            self.heartbeat_response_delay
                .received
                .fetch_add(1, Ordering::Relaxed);
            nervix_primitives::time::sleep(Duration::from_millis(milliseconds)).await;
        }
    }
}
