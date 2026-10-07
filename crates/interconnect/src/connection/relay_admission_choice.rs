//! The irreversible admission choice of one received relay attempt.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Choosing runtime admission or cancellation exactly once.
//! - **Depends on.** The shared atomic primitive boundary.
//! - **Must not know.** Connections, wire encodings, runtime graphs or admission permits.

use nervix_primitives::sync::atomic::{AtomicU8, Ordering};

const PENDING: u8 = 0;
const ADMITTED: u8 = 1;
const CANCELLED: u8 = 2;

pub(super) struct AdmissionChoice {
    // The private byte encoding is pending=0, admitted=1, cancelled=2. Callers see typed states.
    state: AtomicU8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChosenAdmission {
    Pending,
    Admitted,
    Cancelled,
}

impl AdmissionChoice {
    pub(super) fn new() -> Self {
        Self {
            state: AtomicU8::new(PENDING),
        }
    }

    pub(super) fn current(&self) -> ChosenAdmission {
        match self.state.load(Ordering::Acquire) {
            PENDING => ChosenAdmission::Pending,
            ADMITTED => ChosenAdmission::Admitted,
            CANCELLED => ChosenAdmission::Cancelled,
            _ => unreachable!("the private admission representation has exactly three states"),
        }
    }

    pub(super) fn admit(&self) -> bool {
        self.choose(ADMITTED)
    }
    pub(super) fn cancel(&self) -> bool {
        self.choose(CANCELLED)
    }

    fn choose(&self, choice: u8) -> bool {
        self.state
            .compare_exchange(PENDING, choice, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

#[cfg(all(test, feature = "loom"))]
mod loom_models {
    use meticulous::ResultExt as _;
    use nervix_model_harness::{
        InvariantId,
        loom::{explore, spawn},
    };
    use nervix_primitives::sync::Arc;

    use super::{AdmissionChoice, ChosenAdmission};

    #[test]
    fn loom_admission_and_cancellation_choose_one_irreversible_verdict() {
        explore(
            InvariantId::new("interconnect.relay.irreversible-admission"),
            || {
                let choice = Arc::new(AdmissionChoice::new());
                let admitting = choice.clone();
                let admitting = spawn(move || admitting.admit());
                let cancelling = choice.clone();
                let cancelling = spawn(move || cancelling.cancel());
                let admitted = admitting
                    .join()
                    .assured("an admission participant completed");
                let cancelled = cancelling
                    .join()
                    .assured("a cancellation participant completed");
                assert_ne!(
                    admitted, cancelled,
                    "one admission participant wins exactly once"
                );
                let expected = if admitted {
                    ChosenAdmission::Admitted
                } else {
                    ChosenAdmission::Cancelled
                };
                assert_eq!(
                    choice.current(),
                    expected,
                    "admission verdict changed after its winning transition"
                );
                assert!(!choice.admit());
                assert!(!choice.cancel());
                assert_eq!(
                    choice.current(),
                    expected,
                    "admission verdict changed after its winning transition"
                );
            },
        );
    }
}
