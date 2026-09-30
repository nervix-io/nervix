//! One placement's replication, as this node takes part in it.

use std::{collections::BTreeSet, pin::pin};

use nervix_models::ClusterNodeName;
use nervix_primitives::sync::{
    Arc, Notify,
    blocking::{Mutex, MutexGuard},
};

use crate::ReplicaProgress;

/// The replication of one runtime-state placement's checkpoints, as this node takes part in it.
///
/// As the placement's owner, this node records what each replica reported holding, and offers its
/// newest checkpoint to the replicas that do not hold it yet through one [`Announcer`] at a time.
/// As a replica, it wakes the task that keeps this node's copy current when the owner announces a
/// newer checkpoint.
///
/// The state this replicates holds it for as long as that state lives. Dropping it retires the
/// replication: nothing is offered any more, and a running announcer finishes at its next step.
#[derive(Debug)]
pub struct CheckpointReplication {
    shared: Arc<Shared>,
}

/// What the replicated state shares with the announcer offering its checkpoint.
#[derive(Debug)]
struct Shared {
    /// The announcement and the replicas' progress change together, under a lock that belongs to
    /// this one placement and is never held across an await.
    state: Mutex<ReplicationState>,
    /// Signals every report that raised a replica's progress.
    progressed: Notify,
    /// Signals the owner's announcement of a newer checkpoint to the replica task that keeps this
    /// node's copy current. One that arrives while that task is busy is kept as its permit.
    announced: Notify,
}

#[derive(Debug, Default)]
struct ReplicationState {
    progress: ReplicaProgress,
    announcement: Announcement,
}

/// Whether an announcer offers a revision, and which.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Announcement {
    /// No announcer runs.
    #[default]
    Idle,
    /// One announcer offers `revision` to the replicas that do not hold it yet.
    Offering { revision: u64 },
    /// The replicated state is gone, so nothing is offered any more.
    Retired,
}

impl Shared {
    /// The announcement and progress of this placement, locked for one short change that never
    /// crosses an await.
    fn state(&self) -> MutexGuard<'_, ReplicationState> {
        self.state.lock()
    }
}

impl CheckpointReplication {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(ReplicationState::default()),
                progressed: Notify::new(),
                announced: Notify::new(),
            }),
        }
    }

    /// Offer `revision` to the replicas.
    ///
    /// An announcer already offering keeps offering the newer of its revision and `revision`, and
    /// nothing is returned. Otherwise the returned announcer offers `revision`, and the caller
    /// drives it until it finishes. A retired replication offers nothing.
    #[must_use = "an announcer that nothing drives offers nothing"]
    pub fn offer(&self, revision: u64) -> Option<Announcer> {
        let mut state = self.shared.state();
        match state.announcement {
            Announcement::Idle => {}
            Announcement::Offering { revision: offered } => {
                state.announcement = Announcement::Offering {
                    revision: offered.max(revision),
                };
                return None;
            }
            Announcement::Retired => return None,
        }
        state.announcement = Announcement::Offering { revision };
        drop(state);
        Some(Announcer {
            shared: self.shared.clone(),
            finished: false,
        })
    }

    /// Record that `replica` reported holding `revision` on its stable storage, and wake every task
    /// waiting for a report when that raised what the replica holds.
    pub fn record(&self, replica: &ClusterNodeName, revision: u64) {
        let raised = self.shared.state().progress.record(replica, revision);
        if raised {
            self.shared.progressed.notify_waiters();
        }
    }

    /// Read what the replicas reported holding. The read runs under this placement's lock, so it
    /// stays short.
    pub fn with_progress<R>(&self, read: impl FnOnce(&ReplicaProgress) -> R) -> R {
        let state = self.shared.state();
        read(&state.progress)
    }

    /// Wait until `enough` holds for what the replicas reported.
    ///
    /// Each round registers for the next report before it reads the progress, so a report that
    /// lands between the read and the wait wakes the wait instead of being missed.
    pub async fn wait_until(&self, mut enough: impl FnMut(&ReplicaProgress) -> bool) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut progressed = pin!(self.shared.progressed.notified());
            progressed.as_mut().enable();
            let satisfied = enough(&self.shared.state().progress);
            if satisfied {
                return;
            }
            progressed.await;
        }
    }

    /// Signals every report that raised a replica's progress. A waiter registers for the next
    /// signal before it reads the progress, so a report that lands in between is not missed.
    pub fn progress_signal(&self) -> &Notify {
        &self.shared.progressed
    }

    /// Record that the owner announced a newer checkpoint: wake the task that keeps this node's
    /// copy current, or leave it the permit while it is busy.
    pub fn announced(&self) {
        self.shared.announced.notify_one();
    }

    /// Wait for the owner's next announcement, or return at once for one that arrived while
    /// nothing waited.
    pub async fn next_announcement(&self) {
        self.shared.announced.notified().await;
    }
}

impl Default for CheckpointReplication {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CheckpointReplication {
    fn drop(&mut self) {
        self.shared.state().announcement = Announcement::Retired;
    }
}

/// The one announcer of a placement, offering its newest revision to the replicas that do not hold
/// it yet.
///
/// Dropping an announcer before it finished, as cancelling the task that drives it does, hands its
/// announcement back, so the next offer starts another announcer.
#[derive(Debug)]
pub struct Announcer {
    shared: Arc<Shared>,
    finished: bool,
}

/// What an announcer does next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnnouncerStep {
    /// Offer `revision` to `lagging`, the replicas that do not hold it yet, then step again.
    Offer {
        revision: u64,
        lagging: BTreeSet<ClusterNodeName>,
    },
    /// Every replica holds the revision offered, the placement has no replicas, or the replicated
    /// state is gone: the announcement ended.
    Finished,
}

impl Announcer {
    /// The next step while `replicas` are the placement's replicas.
    ///
    /// The announcement ends in the same step that finds every replica holding the revision it
    /// offers. A newer revision offered concurrently is either offered by this step or finds the
    /// announcement ended and starts another announcer, so no offered revision is left without
    /// one.
    pub fn next(&mut self, replicas: &BTreeSet<ClusterNodeName>) -> AnnouncerStep {
        let mut state = self.shared.state();
        let Announcement::Offering { revision } = state.announcement else {
            // The replicated state is gone. Only this announcer hands the announcement back to
            // idle, and it finishes when it does.
            self.finished = true;
            return AnnouncerStep::Finished;
        };
        let lagging = state.progress.awaiting(replicas, revision);
        if !lagging.is_empty() {
            return AnnouncerStep::Offer { revision, lagging };
        }
        state.announcement = Announcement::Idle;
        self.finished = true;
        AnnouncerStep::Finished
    }
}

impl Drop for Announcer {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = self.shared.state();
        if let Announcement::Offering { .. } = state.announcement {
            state.announcement = Announcement::Idle;
        }
    }
}

#[cfg(test)]
#[path = "replication_tests.rs"]
mod tests;
