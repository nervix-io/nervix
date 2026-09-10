//! Storage publication fault controls.
//!
//! Layer: engines and infrastructure; testing hooks are enabled only for test harnesses.
//! - **Owns.** Deterministic failures at the durable commit and publication boundaries.
//! - **Depends on.** Synchronization primitives.
//! - **Must not know.** Graph semantics or transport behavior.

use std::io;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageBoundary {
    BeforeCommit,
    AfterSync,
}

#[cfg(not(any(test, feature = "testing")))]
#[derive(Clone, Default)]
pub struct StorageFault;

#[cfg(not(any(test, feature = "testing")))]
impl StorageFault {
    pub(crate) fn check(&self, _: &str, _: StorageBoundary) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(any(test, feature = "testing"))]
pub use enabled::{StorageFault, StoragePause};

#[cfg(any(test, feature = "testing"))]
mod enabled {
    use parking_lot::{Condvar, Mutex};
    use tokio::sync::Notify;
    use triomphe::Arc;

    use super::*;

    #[derive(Clone, Debug, Default)]
    pub struct StorageFault {
        inner: Arc<Mutex<Option<ArmedFault>>>,
    }

    #[derive(Debug)]
    struct ArmedFault {
        operation: String,
        boundary: StorageBoundary,
        effect: Effect,
    }

    #[derive(Debug)]
    enum Effect {
        Fail,
        Pause(Arc<Gate>),
    }

    #[derive(Debug, Default)]
    struct Gate {
        released: Mutex<bool>,
        condition: Condvar,
        entered: Notify,
    }

    /// Dropping a test's pause releases the worker, including when an assertion panics.
    pub struct StoragePause {
        gate: Arc<Gate>,
    }

    impl StoragePause {
        pub async fn entered(&self) {
            self.gate.entered.notified().await;
        }
        pub fn release(&self) {
            *self.gate.released.lock() = true;
            self.gate.condition.notify_all();
        }
    }
    impl Drop for StoragePause {
        fn drop(&mut self) {
            self.release();
        }
    }

    impl StorageFault {
        pub fn fail_next(&self, operation: String, boundary: StorageBoundary) {
            *self.inner.lock() = Some(ArmedFault {
                operation,
                boundary,
                effect: Effect::Fail,
            });
        }
        pub fn pause_next(&self, operation: String, boundary: StorageBoundary) -> StoragePause {
            let gate = Arc::new(Gate::default());
            *self.inner.lock() = Some(ArmedFault {
                operation,
                boundary,
                effect: Effect::Pause(gate.clone()),
            });
            StoragePause { gate }
        }
        pub(crate) fn check(&self, operation: &str, boundary: StorageBoundary) -> io::Result<()> {
            let fault = {
                let mut armed = self.inner.lock();
                if let Some(fault) = armed.as_ref()
                    && fault.operation == operation
                    && fault.boundary == boundary
                {
                    armed.take()
                } else {
                    None
                }
            };
            match fault {
                Some(ArmedFault {
                    effect: Effect::Fail,
                    ..
                }) => Err(io::Error::other("injected consensus storage failure")),
                Some(ArmedFault {
                    effect: Effect::Pause(gate),
                    ..
                }) => {
                    gate.entered.notify_one();
                    let mut released = gate.released.lock();
                    while !*released {
                        gate.condition.wait(&mut released);
                    }
                    Ok(())
                }
                None => Ok(()),
            }
        }
    }
}
