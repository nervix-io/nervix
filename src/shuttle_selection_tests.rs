//! The server's Shuttle build selects Shuttle's primitives inside the crates it depends on.
//!
//! Layer: test harness.
//! - **Owns.** The proof that the server's `shuttle` feature reaches the atomics a dependency owns.
//! - **Depends on.** The vocabulary's atomic timestamp and the server Shuttle runner.
//! - **Must not know.** What a timestamp marks, or any runtime protocol.

use nervix_models::{AtomicTimestamp, Timestamp};
use shuttle::current::context_switches;

use crate::shuttle_test::check_random;

/// Shuttle counts every point at which it could switch threads, including the ones where it keeps
/// running the same thread, so only an operation it observes advances the count. The timestamp
/// belongs to the vocabulary crate, which owns no Shuttle feature: its atomic is modeled only
/// because the server's feature selected the mode for the whole dependency graph.
fn a_vocabulary_atomic_advances_the_schedule() {
    let last_seen = AtomicTimestamp::new(Timestamp::from_unix_nanos(1));

    let before_store = context_switches();
    last_seen.store(Timestamp::from_unix_nanos(2));
    assert!(
        context_switches() > before_store,
        "storing the vocabulary's atomic timestamp was not a Shuttle scheduling point"
    );

    let before_load = context_switches();
    assert_eq!(last_seen.load(), Timestamp::from_unix_nanos(2));
    assert!(
        context_switches() > before_load,
        "loading the vocabulary's atomic timestamp was not a Shuttle scheduling point"
    );
}

#[test]
fn shuttle_a_vocabulary_atomic_is_a_scheduling_point_in_the_server_build() {
    check_random(a_vocabulary_atomic_advances_the_schedule, 1);
}
