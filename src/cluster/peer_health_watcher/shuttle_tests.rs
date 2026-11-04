//! Policy publication racing a retained health wait, explored through its production owner.
//!
//! Layer: test harness.
//! - **Owns.** A changed interval cannot be lost before, during or after wait registration.
//! - **Depends on.** The production policy installer, health watcher and Shuttle runner.
//! - **Must not know.** Cluster commands, domain decisions, elapsed time or transport behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_model_harness::shuttle::{check_pct, check_random};

use super::*;

const RANDOM_ITERATIONS: usize = 1_000;
const PCT_ITERATIONS: usize = 1_000;
const PCT_DEPTH: usize = 3;

fn a_policy_change_racing_a_retained_wait() {
    shuttle::future::block_on(async {
        let freshness = StdArc::new(ArcSwap::from_pointee(Duration::from_secs(100)));
        let (state, receiver) = watch::channel(PeerHealthStateSnapshot::default());
        let mut watcher = PeerHealthStateWatcher::new(receiver, &freshness);
        let publishing = nervix_primitives::task::spawn({
            let freshness = freshness.clone();
            let state = state.clone();
            async move {
                install_observation_freshness(&freshness, &state, Duration::from_secs(20));
            }
        });
        // There is no peer deadline. A missed publication leaves this real owner waiting and
        // Shuttle reports a deadlock; no elapsed-time timeout or completion join wakes the waiter.
        watcher.wait_for_change_or_next_transition().await;
        assert_eq!(**freshness.load(), Duration::from_secs(20));
        publishing.await.assured(
            "Shuttle fails an execution when its publishing task panics, so no join observes one",
        );
        drop(state);
    });
}

#[test]
fn shuttle_a_policy_change_racing_a_retained_health_wait_is_never_missed() {
    // Fair select polling requests modeled random data, so both searches supply that data.
    check_random(a_policy_change_racing_a_retained_wait, RANDOM_ITERATIONS);
    check_pct(
        a_policy_change_racing_a_retained_wait,
        PCT_ITERATIONS,
        PCT_DEPTH,
    );
}
