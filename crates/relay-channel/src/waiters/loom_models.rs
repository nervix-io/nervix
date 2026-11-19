//! The synchronous handoff of the single reservation granter.
//!
//! Layer: test harness.
//! - **Owns.** The publication of work to the current or next granter.
//! - **Depends on.** The production turn state and Loom.
//! - **Must not know.** The queue's item type or async poll implementation.

use meticulous::ResultExt as _;
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::Turns;

#[test]
fn loom_the_next_granter_acquires_the_previous_granters_work() {
    explore(
        InvariantId::new("relay-channel.waiters.granter-handover"),
        || {
            let turns = Arc::new(Turns::new());
            assert!(turns.request());
            turns.take_work();
            let witness = Arc::new(AtomicUsize::new(0));
            let successor = turns.clone();
            let reading = witness.clone();
            let next = spawn(move || {
                if successor.request() {
                    successor.take_work();
                    assert_eq!(
                        reading.load(Ordering::Relaxed),
                        1,
                        "the new granter missed the previous owner's writes"
                    );
                    assert!(successor.try_end());
                }
            });
            witness.store(1, Ordering::Relaxed);
            if !turns.try_end() {
                turns.take_work();
                assert!(turns.try_end());
            }
            next.join().assured("the next turn completed");
        },
    );
}
