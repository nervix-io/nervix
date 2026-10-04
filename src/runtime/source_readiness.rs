//! Readiness publication for one exact source instance lifetime.
//!
//! Layer: data plane.
//! - **Owns.** Starting, ready and retired transitions visible to source readiness observers.
//! - **Depends on.** Shared ownership and atomics from the primitive boundary.
//! - **Must not know.** Runtime registries, polling, connectors or source names.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a source instance publishes only its own scalar readiness state"
    )
)]

use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

#[derive(Debug, Clone)]
pub(in crate::runtime) struct SourceInstanceReadiness {
    inner: Arc<InstanceReadiness>,
}

#[derive(Debug)]
struct InstanceReadiness {
    /// Private scalar encoding: starting=0, ready=1, retired=2. It publishes no other data.
    state: AtomicU8,
}

impl SourceInstanceReadiness {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(InstanceReadiness {
                state: AtomicU8::new(0),
            }),
        }
    }

    pub(in crate::runtime) fn mark_ready(&self) {
        self.publish(1);
    }

    pub(in crate::runtime) fn mark_unready(&self) {
        self.publish(0);
    }

    pub(super) fn is_ready(&self) -> bool {
        self.inner.state.load(Ordering::Relaxed) == 1
    }

    pub(in crate::runtime) fn retire(&self) {
        self.inner.state.store(2, Ordering::Relaxed);
    }

    fn publish(&self, state: u8) {
        let mut current = self.inner.state.load(Ordering::Relaxed);
        while current != 2 {
            match self.inner.state.compare_exchange_weak(
                current,
                state,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retired_instance_cannot_publish_readiness_into_its_replacement() {
        let predecessor = SourceInstanceReadiness::new();
        let retained = predecessor.clone();
        predecessor.mark_ready();
        assert!(retained.is_ready());
        predecessor.retire();
        let replacement = SourceInstanceReadiness::new();
        retained.mark_ready();
        assert!(!retained.is_ready());
        assert!(!replacement.is_ready());
        replacement.mark_ready();
        retained.mark_unready();
        assert!(replacement.is_ready());
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use nervix_model_harness::shuttle::check_interleavings;
    use nervix_primitives::thread;

    use super::*;

    #[test]
    fn shuttle_retirement_racing_source_polling_is_final_and_instance_local() {
        check_interleavings(|| {
            let instance = SourceInstanceReadiness::new();
            let reader = instance.clone();
            let publisher = thread::spawn(move || {
                reader.mark_ready();
                reader.mark_unready();
                reader.mark_ready();
            });
            let ending = instance.clone();
            let retire = thread::spawn(move || ending.retire());
            publisher
                .join()
                .expect("Shuttle fails when the thread panics");
            retire.join().expect("Shuttle fails when the thread panics");
            assert!(!instance.is_ready());
            instance.mark_ready();
            assert!(!instance.is_ready());
            let replacement = SourceInstanceReadiness::new();
            replacement.mark_ready();
            instance.mark_unready();
            assert!(replacement.is_ready());
        });
    }
}
