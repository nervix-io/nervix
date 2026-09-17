//! What a node keeps of its Raft log, and when it replaces the log with a snapshot.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The snapshot cadence, the covered-log retention bounds, and the retained-byte cap
//!   that holds new mutations back when reclamation cannot keep up.
//! - **Depends on.** OpenRaft's snapshot and purge triggers and the consensus store's log state.
//! - **Must not know.** What a replicated command means, or which node should lead.
//!
//! Only entries a durable snapshot already covers are ever purged, so a failed or incomplete
//! snapshot build leaves every entry the node still needs in place.

use std::time::Duration;

use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info};

use crate::{NervixRaft, storage::FjallStore};

/// How often retention is reconsidered when nothing else wakes it.
const RETENTION_INTERVAL: Duration = Duration::from_secs(5);

/// What a node keeps of its Raft log, and when it snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftRetentionPolicy {
    /// Committed entries since the completed snapshot that start a new one.
    pub snapshot_entry_threshold: u64,
    /// Appended bytes since the completed snapshot that start a new one.
    pub snapshot_byte_threshold: u64,
    /// Snapshot-covered entries the node keeps.
    pub covered_entries_retained: u64,
    /// Snapshot-covered bytes the node keeps. Whichever bound is reached first decides.
    pub covered_bytes_retained: u64,
    /// The retained log size above which new mutations wait for reclamation.
    pub retained_log_cap_bytes: u64,
    /// How long a mutation waits for reclamation before it fails.
    pub retention_admission_timeout: Duration,
}

impl Default for RaftRetentionPolicy {
    fn default() -> Self {
        Self {
            snapshot_entry_threshold: 10_000,
            snapshot_byte_threshold: 64 * 1024 * 1024,
            covered_entries_retained: 1_000,
            covered_bytes_retained: 64 * 1024 * 1024,
            retained_log_cap_bytes: 1024 * 1024 * 1024,
            retention_admission_timeout: Duration::from_secs(30),
        }
    }
}

/// Whether a byte-threshold snapshot build has been asked for, and what the node had completed
/// when it was.
///
/// A node has no completed snapshot until its first build finishes, so the completed snapshot it
/// was asked for is itself optional. Keeping that absence distinct from an index means the request
/// issued before any snapshot completed never counts as the request for the snapshot at index
/// zero, which would leave the byte threshold unable to ask again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotTrigger {
    /// No build has been asked for since the completed snapshot last changed.
    NotRequested,
    /// A build was asked for while `completed` was this node's completed snapshot.
    Requested { completed: Option<u64> },
}

impl SnapshotTrigger {
    /// Whether a build has already been asked for while `completed` was the completed snapshot,
    /// which is what keeps a build in progress from being asked for again on every pass.
    fn already_requested_for(self, completed: Option<u64>) -> bool {
        match self {
            Self::NotRequested => false,
            Self::Requested {
                completed: requested,
            } => requested == completed,
        }
    }
}

/// Keeps one node's snapshot cadence and log retention inside the configured policy.
pub(crate) struct RetentionTask {
    raft: NervixRaft,
    store: FjallStore,
    policy: RaftRetentionPolicy,
    /// The byte-threshold trigger this node has issued, so a build in progress is not asked for
    /// again on every pass.
    trigger: SnapshotTrigger,
}

impl RetentionTask {
    pub(crate) fn new(raft: NervixRaft, store: FjallStore, policy: RaftRetentionPolicy) -> Self {
        Self {
            raft,
            store,
            policy,
            trigger: SnapshotTrigger::NotRequested,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut ticks = interval(RETENTION_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::task::consume_budget().await;
            ticks.tick().await;
            if self.raft.is_initialized().await.is_err() {
                return;
            }
            self.reconsider().await;
        }
    }

    async fn reconsider(&mut self) {
        let metrics = {
            use openraft::type_config::async_runtime::watch::WatchReceiver as _;
            self.raft.metrics().borrow_watched().clone()
        };
        let snapshot_index = metrics.snapshot.as_ref().map(|log_id| log_id.index);
        self.request_snapshot_when_bytes_exceed_threshold(snapshot_index)
            .await;
        self.purge_covered_suffix(snapshot_index).await;
    }

    /// Start a snapshot once enough bytes have accumulated since the completed one.
    ///
    /// The entry-count threshold is OpenRaft's own snapshot policy; this only adds the byte bound,
    /// and never asks twice for the same completed snapshot, so at most one build is active.
    async fn request_snapshot_when_bytes_exceed_threshold(&mut self, snapshot_index: Option<u64>) {
        if self.trigger.already_requested_for(snapshot_index) {
            return;
        }
        if self.store.log_bytes_since_snapshot() < self.policy.snapshot_byte_threshold {
            return;
        }
        if self.raft.trigger().snapshot().await.is_err() {
            return;
        }
        info!(
            bytes = self.store.log_bytes_since_snapshot(),
            "raft snapshot requested by the retained byte threshold"
        );
        self.trigger = SnapshotTrigger::Requested {
            completed: snapshot_index,
        };
    }

    /// Purge the part of the covered log that exceeds the retained byte bound.
    ///
    /// OpenRaft's own policy already keeps the covered entry count inside its bound. This adds the
    /// byte bound, and OpenRaft still refuses to purge anything the snapshot does not cover.
    async fn purge_covered_suffix(&mut self, snapshot_index: Option<u64>) {
        let Some(snapshot_index) = snapshot_index else {
            return;
        };
        let retained = match self
            .store
            .covered_retention_boundary(
                snapshot_index,
                self.policy.covered_entries_retained,
                self.policy.covered_bytes_retained,
            )
            .await
        {
            Ok(retained) => retained,
            Err(error) => {
                debug!(%error, "raft log retention could not measure its covered suffix");
                return;
            }
        };
        let Some(purge_upto) = retained else {
            return;
        };
        if self.raft.trigger().purge_log(purge_upto).await.is_err() {
            return;
        }
        debug!(
            purge_upto,
            "raft log purge requested by the retention bound"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::SnapshotTrigger;

    #[test]
    fn a_node_that_has_asked_for_nothing_has_asked_for_no_completed_snapshot() {
        let trigger = SnapshotTrigger::NotRequested;

        assert!(!trigger.already_requested_for(None));
        assert!(!trigger.already_requested_for(Some(0)));
        assert!(!trigger.already_requested_for(Some(7)));
    }

    #[test]
    fn the_request_made_before_any_snapshot_completed_is_not_the_request_for_index_zero() {
        let trigger = SnapshotTrigger::Requested { completed: None };

        assert!(trigger.already_requested_for(None));
        assert!(!trigger.already_requested_for(Some(0)));
    }

    #[test]
    fn a_request_for_the_snapshot_at_index_zero_covers_only_that_snapshot() {
        let trigger = SnapshotTrigger::Requested { completed: Some(0) };

        assert!(trigger.already_requested_for(Some(0)));
        assert!(!trigger.already_requested_for(None));
        assert!(!trigger.already_requested_for(Some(1)));
    }
}
