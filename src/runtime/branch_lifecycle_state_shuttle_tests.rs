//! The owner's announcements a replica keeps for one branch-keyed entity, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The ordering invariant of an entity's pending announcements: an announcement that
//!   lands while the replica task takes the pending ones, or between that and its wait, is taken in
//!   the task's next round rather than lost.
//! - **Depends on.** The production branch lifecycle handle and the server Shuttle runner.
//! - **Must not know.** What a checkpoint holds, the interconnect, or stable storage.

use std::sync::Arc as StdArc;

use meticulous::ResultExt as _;
use nervix_interconnect::RuntimeState;
use nervix_models::SchemaFingerprint;

use super::{AnnouncedCheckpoint, ReplicatedBranchLifecycle};
use crate::{runtime::test_fixtures::string_branch_key, shuttle_test::check_interleavings};

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";

fn announced(lsm: u64) -> AnnouncedCheckpoint {
    AnnouncedCheckpoint {
        state: RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        lsm,
    }
}

/// The owner announces a branch checkpoint while the replica task of the entity takes the pending
/// announcements and then waits for the next one, as each of its rounds does. The task takes the
/// announcement whether it lands before the take, between the take and the wait, or during the
/// wait: a lost one would leave the task waiting, which Shuttle reports as a deadlock.
fn an_announced_branch_racing_the_replica_round() {
    shuttle::future::block_on(async {
        let lifecycle = StdArc::new(ReplicatedBranchLifecycle::default());
        let branch = string_branch_key("tenant", "acme");
        let replicating = nervix_primitives::task::spawn({
            let lifecycle = lifecycle.clone();
            let branch = branch.clone();
            async move {
                loop {
                    let taken = lifecycle.take_announcements();
                    if let Some(checkpoint) = taken.branches.get(&branch) {
                        return checkpoint.lsm;
                    }
                    lifecycle.replication().next_announcement().await;
                }
            }
        });
        lifecycle.announce_branch(branch, announced(2));
        let taken = replicating.await.assured(MODEL_TASK_JOINS);
        assert_eq!(
            taken, 2,
            "the replica task took an announcement other than the one the owner made"
        );
    });
}

#[test]
fn shuttle_an_announced_branch_racing_the_replica_round_is_never_missed() {
    check_interleavings(an_announced_branch_racing_the_replica_round);
}

/// Two announcements of one branch land in either order while the replica task takes them. The
/// task never ends up with the older revision after it took the newer one: an older announcement
/// never replaces a newer one still pending.
fn announcements_of_one_branch_in_either_order() {
    shuttle::future::block_on(async {
        let lifecycle = StdArc::new(ReplicatedBranchLifecycle::default());
        let branch = string_branch_key("tenant", "acme");
        let older = nervix_primitives::task::spawn({
            let lifecycle = lifecycle.clone();
            let branch = branch.clone();
            async move { lifecycle.announce_branch(branch, announced(1)) }
        });
        let newer = nervix_primitives::task::spawn({
            let lifecycle = lifecycle.clone();
            let branch = branch.clone();
            async move { lifecycle.announce_branch(branch, announced(2)) }
        });
        let mut newest_taken = 0;
        let mut taken_after_newest = Vec::new();
        for _ in 0..3 {
            let taken = lifecycle.take_announcements();
            if let Some(checkpoint) = taken.branches.get(&branch) {
                if newest_taken == 2 {
                    taken_after_newest.push(checkpoint.lsm);
                }
                newest_taken = newest_taken.max(checkpoint.lsm);
            }
            nervix_primitives::task::yield_now().await;
        }
        older.await.assured(MODEL_TASK_JOINS);
        newer.await.assured(MODEL_TASK_JOINS);
        let taken = lifecycle.take_announcements();
        if let Some(checkpoint) = taken.branches.get(&branch) {
            newest_taken = newest_taken.max(checkpoint.lsm);
        }
        assert_eq!(newest_taken, 2, "the newest announcement was lost");
        for lsm in taken_after_newest {
            assert_eq!(
                lsm, 1,
                "only the older announcement, landing after the newer one was taken, follows it"
            );
        }
    });
}

#[test]
fn shuttle_announcements_of_one_branch_keep_the_newest_pending() {
    check_interleavings(announcements_of_one_branch_in_either_order);
}
