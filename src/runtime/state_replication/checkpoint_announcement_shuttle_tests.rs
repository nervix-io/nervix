//! One placement's checkpoint replication, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The ordering invariants of one placement's announcement and replica reports: an
//!   offered revision always has an announcer, a retired replication ends its announcer, and an
//!   owner's announcement wakes the replica task that keeps that replica's copy current.
//! - **Depends on.** The production checkpoint replication and the Shuttle runner of the model
//!   harness.
//! - **Must not know.** What a checkpoint holds, the interconnect, or stable storage.

use std::collections::BTreeSet;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_checkpoint_replication::{Announcer, AnnouncerStep, CheckpointReplication};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::ClusterNodeName;
use nervix_primitives::sync::{
    StdArc,
    atomic::{AtomicU64, Ordering},
    oneshot,
};

use super::CheckpointAnnouncementTasks;

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";

fn replica() -> ClusterNodeName {
    ClusterNodeName::parse("node-2").assured("a literal node name is valid")
}

/// Drive `announcer` as the runtime's announcer task does, with the placement's one replica
/// fetching and acknowledging every revision the announcer offers it.
async fn announce(replication: StdArc<CheckpointReplication>, mut announcer: Announcer) {
    let replicas = BTreeSet::from([replica()]);
    loop {
        let step = announcer.next(&replicas);
        let AnnouncerStep::Offer { revision, lagging } = step else {
            return;
        };
        assert_eq!(lagging, replicas, "the one replica lags until it reports");
        replication.record(&replica(), revision);
        nervix_primitives::task::yield_now().await;
    }
}

/// The owner offers a second revision while the announcer of the first finds its replica holding
/// the first and ends. Whichever comes first, the second revision is announced, so the replica ends
/// up holding it: an offer that joined an announcer in the middle of ending would leave the
/// revision without one.
fn an_offer_racing_the_end_of_an_announcement() {
    shuttle::future::block_on(async {
        let replication = StdArc::new(CheckpointReplication::new());
        let first = replication
            .offer(1)
            .assured("an idle replication starts an announcer");
        let announcing = nervix_primitives::task::spawn(announce(replication.clone(), first));
        let offering = nervix_primitives::task::spawn({
            let replication = replication.clone();
            async move {
                if let Some(second) = replication.offer(2) {
                    announce(replication, second).await;
                }
            }
        });
        announcing.await.assured(MODEL_TASK_JOINS);
        offering.await.assured(MODEL_TASK_JOINS);
        let held = replication.with_progress(|progress| progress.held(&replica()));
        assert_eq!(
            held,
            Some(2),
            "a revision offered while the previous announcement ended was never announced"
        );
    });
}

#[test]
fn shuttle_an_offer_racing_the_end_of_an_announcement_is_always_announced() {
    check_interleavings(an_offer_racing_the_end_of_an_announcement);
}

/// The replicated state goes away while its announcer offers a revision to a replica that never
/// acknowledges it. The announcer ends rather than offering forever, which Shuttle would stop at
/// its step bound.
fn a_retired_replication_while_its_announcer_offers() {
    shuttle::future::block_on(async {
        let replication = CheckpointReplication::new();
        let mut announcer = replication
            .offer(1)
            .assured("an idle replication starts an announcer");
        let announcing = nervix_primitives::task::spawn(async move {
            let replicas = BTreeSet::from([replica()]);
            loop {
                let step = announcer.next(&replicas);
                if step == AnnouncerStep::Finished {
                    return;
                }
                nervix_primitives::task::yield_now().await;
            }
        });
        drop(replication);
        announcing.await.assured(MODEL_TASK_JOINS);
    });
}

#[test]
fn shuttle_a_retired_replication_ends_its_announcer() {
    check_interleavings(a_retired_replication_while_its_announcer_offers);
}

/// The owner holds a newer checkpoint and announces it while the replica task that keeps the
/// replica's copy current synchronizes and then waits for the next announcement. The replica ends
/// up holding the newer checkpoint whether the announcement lands while it synchronizes or while it
/// waits: a lost announcement would leave it waiting, which Shuttle reports as a deadlock.
fn an_announcement_racing_the_replica_wait() {
    shuttle::future::block_on(async {
        let replication = StdArc::new(CheckpointReplication::new());
        let owner_holds = StdArc::new(AtomicU64::new(1));
        let replicating = nervix_primitives::task::spawn({
            let replication = replication.clone();
            let owner_holds = owner_holds.clone();
            async move {
                loop {
                    let held = owner_holds.load(Ordering::SeqCst);
                    if held == 2 {
                        return held;
                    }
                    replication.next_announcement().await;
                }
            }
        });
        owner_holds.store(2, Ordering::SeqCst);
        replication.announced();
        let held = replicating.await.assured(MODEL_TASK_JOINS);
        assert_eq!(
            held, 2,
            "the replica stopped before it held the announced checkpoint"
        );
    });
}

#[test]
fn shuttle_an_announcement_racing_the_replica_wait_is_never_missed() {
    check_interleavings(an_announcement_racing_the_replica_wait);
}

/// Closing the production task owner cancels dispatch even when it never becomes ready. Explore
/// both close racing the task's first poll and close after the pending dispatch has started.
fn close_racing_checkpoint_dispatch() {
    shuttle::future::block_on(async {
        for await_started in [false, true] {
            let tasks = CheckpointAnnouncementTasks::default();
            let replication = CheckpointReplication::new();
            let announcer = replication
                .offer(1)
                .assured("the checkpoint starts an announcer");
            let (started, entered) = oneshot::channel();
            let announcing = tasks.spawn(async move {
                let _announcer = announcer;
                let _ = started.send(());
                std::future::pending::<()>().await;
            });
            if await_started {
                entered
                    .await
                    .assured("the dispatch starts before its owner closes");
            }
            tasks.close();
            assert!(tasks.is_closed());
            tasks.wait().await;
            announcing.await.assured(MODEL_TASK_JOINS);
            assert!(
                replication.offer(2).is_some(),
                "cancelling a pending dispatch releases the exact announcer it retained"
            );
        }
    });
}

#[test]
fn shuttle_checkpoint_announcement_close_cancels_pending_dispatch() {
    check_interleavings(close_racing_checkpoint_dispatch);
}
