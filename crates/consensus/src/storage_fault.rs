//! Storage publication fault and timing controls.
//!
//! Layer: engines and infrastructure; testing hooks are enabled only for test harnesses.
//! - **Owns.** Deterministic failures, pauses, and delays at durable storage boundaries.
//! - **Depends on.** Synchronization primitives.
//! - **Must not know.** Graph semantics or transport behavior.

use std::{fmt, io};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageBoundary {
    BeforeCommit,
    AfterSync,
}

#[cfg(not(any(test, feature = "testing")))]
#[derive(Clone, Default)]
pub struct StorageFault(());

#[cfg(not(any(test, feature = "testing")))]
impl StorageFault {
    pub(crate) fn check<Operation: fmt::Display>(
        &self,
        _: impl IntoIterator<Item = Operation>,
        _: StorageBoundary,
    ) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(any(test, feature = "testing"))]
pub use enabled::{StorageFault, StoragePause};

#[cfg(any(test, feature = "testing"))]
mod enabled {
    use std::{thread, time::Duration};

    #[cfg(not(feature = "shuttle"))]
    use parking_lot::Condvar as GateCondition;
    use parking_lot::{Mutex, RwLock};
    use tokio::sync::Notify;
    use triomphe::Arc;

    use super::*;

    #[cfg(feature = "shuttle")]
    #[derive(Debug, Default)]
    struct GateCondition;

    #[derive(Clone, Debug, Default)]
    pub struct StorageFault {
        inner: Arc<StorageFaultState>,
    }

    #[derive(Debug, Default)]
    struct StorageFaultState {
        armed: Mutex<Option<ArmedFault>>,
        after_sync_delay: RwLock<Duration>,
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
        condition: GateCondition,
        entered: Notify,
    }

    #[cfg(not(feature = "shuttle"))]
    fn wait_until_released(gate: &Gate) {
        let mut released = gate.released.lock();
        while !*released {
            gate.condition.wait(&mut released);
        }
    }

    #[cfg(feature = "shuttle")]
    fn wait_until_released(gate: &Gate) {
        while !*gate.released.lock() {
            nervix_execution::sync::yield_now();
        }
    }

    #[cfg(not(feature = "shuttle"))]
    fn notify_release(condition: &GateCondition) {
        condition.notify_all();
    }

    #[cfg(feature = "shuttle")]
    fn notify_release(_condition: &GateCondition) {}

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
            notify_release(&self.gate.condition);
        }
    }
    impl Drop for StoragePause {
        fn drop(&mut self) {
            self.release();
        }
    }

    impl StorageFault {
        pub fn fail_next(&self, operation: String, boundary: StorageBoundary) {
            *self.inner.armed.lock() = Some(ArmedFault {
                operation,
                boundary,
                effect: Effect::Fail,
            });
        }
        /// Whether an armed failure is still waiting for the operation it names.
        pub fn is_armed(&self) -> bool {
            self.inner.armed.lock().is_some()
        }
        pub fn pause_next(&self, operation: String, boundary: StorageBoundary) -> StoragePause {
            let gate = Arc::new(Gate::default());
            *self.inner.armed.lock() = Some(ArmedFault {
                operation,
                boundary,
                effect: Effect::Pause(gate.clone()),
            });
            StoragePause { gate }
        }
        /// Delay every completed durable sync; a zero duration disables the delay.
        pub fn set_after_sync_delay(&self, delay: Duration) {
            *self.inner.after_sync_delay.write() = delay;
        }
        /// Check one durable write at `boundary` against the armed fault and the sync delay.
        ///
        /// A write that stores several operations is still one commit: it is delayed once, and a
        /// fault armed for any operation it stores applies to the whole write.
        pub(crate) fn check<Operation: fmt::Display>(
            &self,
            operations: impl IntoIterator<Item = Operation>,
            boundary: StorageBoundary,
        ) -> io::Result<()> {
            if boundary == StorageBoundary::AfterSync {
                let delay = *self.inner.after_sync_delay.read();
                if !delay.is_zero() {
                    thread::sleep(delay);
                }
            }
            let fault = {
                let mut armed = self.inner.armed.lock();
                let mut stored = false;
                if let Some(fault) = armed.as_ref()
                    && fault.boundary == boundary
                {
                    for operation in operations {
                        if fault.operation == operation.to_string() {
                            stored = true;
                            break;
                        }
                    }
                }
                if stored { armed.take() } else { None }
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
                    wait_until_released(&gate);
                    Ok(())
                }
                None => Ok(()),
            }
        }
    }
}
