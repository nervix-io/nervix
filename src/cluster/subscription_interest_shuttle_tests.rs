//! A subscriber's withdrawn interest racing a relay owner's wait for it, explored through its
//! production owner.
//!
//! Layer: test harness.
//! - **Owns.** A withdrawal published before, during or after the wait registers is never missed.
//! - **Depends on.** The production subscription interest publication, its watch and the Shuttle
//!   runner.
//! - **Must not know.** Gossip parsing, relay delivery, elapsed time or transport behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_model_harness::shuttle::{check_pct, check_random};

use super::*;

const RANDOM_ITERATIONS: usize = 1_000;
const PCT_ITERATIONS: usize = 1_000;
const PCT_DEPTH: usize = 3;

fn a_withdrawal_racing_a_relay_owners_wait() {
    shuttle::future::block_on(async {
        let subscriber =
            ClusterNodeName::parse("node-3").assured("the test node name is a valid node name");
        let identity = ClusterNodeIdentity::new(subscriber.clone(), ClusterNodeIncarnation::new(7));
        let mut listed = SubscriptionInterestIndex::default();
        listed.record("sales", "events", &identity, 1);
        let publication = StdArc::new(SubscriptionInterestPublication::new());
        publication.install(listed, BTreeMap::new());
        let watch = SubscriptionInterestWatch {
            publication: publication.clone(),
            changes: publication.changed.subscribe(),
        };
        let withdrawing = nervix_primitives::task::spawn({
            let publication = publication.clone();
            async move {
                publication.install(SubscriptionInterestIndex::default(), BTreeMap::new());
            }
        });
        // Nothing else ever announces a change. A missed announcement leaves this real owner
        // waiting, and Shuttle reports a deadlock; no elapsed-time timeout wakes the waiter.
        watch.until_withdrawn(&subscriber, "sales", "events").await;
        assert!(
            publication.load().nodes("sales", "events").is_none(),
            "the wait ends only once the published index no longer lists the subscriber"
        );
        withdrawing.await.assured(
            "Shuttle fails an execution when its publishing task panics, so no join observes one",
        );
    });
}

#[test]
fn shuttle_a_withdrawal_racing_a_relay_owners_wait_is_never_missed() {
    // Fair select polling requests modeled random data, so both searches supply that data.
    check_random(a_withdrawal_racing_a_relay_owners_wait, RANDOM_ITERATIONS);
    check_pct(
        a_withdrawal_racing_a_relay_owners_wait,
        PCT_ITERATIONS,
        PCT_DEPTH,
    );
}
