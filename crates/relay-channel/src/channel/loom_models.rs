//! The exclusive capture handover.
//!
//! Layer: test harness.
//! - **Owns.** The witness published by the last active handle.
//! - **Depends on.** The production capture owner and Loom.
//! - **Must not know.** Persistence.

use std::num::NonZeroU64;

use meticulous::ResultExt as _;
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::{Capture, Channel};

#[test]
fn loom_standstill_acquires_the_last_handle() {
    explore(
        InvariantId::new("relay-channel.capture.handle-release"),
        || {
            let mut channel = Channel::<u64, u64>::new(NonZeroU64::MIN);
            let sender = channel.sender();
            let witness = Arc::new(AtomicUsize::new(0));
            let publishing = witness.clone();
            let active = spawn(move || {
                publishing.store(1, Ordering::Relaxed);
                drop(sender);
            });
            if let Capture::Saved(_) = channel.capture() {
                assert_eq!(
                    witness.load(Ordering::Relaxed),
                    1,
                    "standstill missed the last handle's work"
                );
            }
            active.join().assured("the sender surrendered its access");
        },
    );
}
