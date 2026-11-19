//! Whole-state observation during concurrent reuse of a quota slot.
//!
//! Layer: test harness.
//! - **Owns.** The equality witness for a reused budget slot.
//! - **Depends on.** The production budget and Loom.
//! - **Must not know.** The log or the send path.

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};

use super::{Budget, State, Transition};

#[test]
fn loom_reused_budget_slot_never_exposes_mixed_fields() {
    explore(InvariantId::new("relay-channel.budget.reuse"), || {
        let initial = State::new(NonZeroU64::MAX);
        let middle = State {
            limit: NonZeroU64::new(u64::MAX - 1).assured("the new budget is positive"),
            used: 1,
            ..initial
        };
        let reused = State {
            limit: NonZeroU64::new(u64::MAX - 2).assured("the reused budget is positive"),
            used: 2,
            ..initial
        };
        let budget = Budget::new(initial);
        // An observer can stop after acquiring the key. Two updates make that exact slot
        // available to a writer again, with a new key. The observer has to validate its
        // original key even if it resumes during the rewrite of the slot's fields.
        let stale = budget.observe().raw;
        let source = budget.clone();
        let observing = spawn(move || {
            if let Some(observed) = source.observe_slot(stale) {
                assert_eq!(
                    observed.state, initial,
                    "a recycled slot exposed state under its retired key"
                );
            }
        });
        let mut writer = Budget::writer(&budget);
        writer.update(|_| Transition::Changed(middle, ()));
        writer.update(|_| Transition::Changed(reused, ()));
        observing.join().assured("the delayed observer finished");
    });
}
