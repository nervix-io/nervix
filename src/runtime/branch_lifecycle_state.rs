//! The branch lifecycle of one branch-keyed entity, as one node holds and replicates it.
//!
//! Layer: data plane.
//! - **Owns.** The newest branch lifecycle checkpoint this node holds for one entity, the branches
//!   that checkpoint names, decoded once, and this node's part in replicating the lifecycle.
//! - **Depends on.** Runtime-state snapshots, the branch lifecycle codec, typed branch keys, the
//!   primitive publication boundary and checkpoint replication.
//! - **Must not know.** Branch tasks, schedules, how a checkpoint is persisted or fetched, or NSPL.

use ahash::HashSet;
use meticulous::OptionExt as _;
use nervix_checkpoint_replication::CheckpointReplication;
use nervix_primitives::{
    publication::ArcSwapOption,
    sync::{StdArc, blocking::OnceLock},
};
use nervix_recovery::Discarded as _;

use super::{
    BranchKey, PersistedRuntimeStateEntry,
    branch_lru_state::{BranchLruSnapshotError, decode_branch_lru_snapshot},
};

/// The branch lifecycle of one branch-keyed entity as this node holds it.
///
/// On the entity's owner it holds the lifecycle the owner published last, which replicas
/// synchronize, and the owner's replication of it to them. On a replica it holds the newest
/// lifecycle the replica installed, whose branches decide which branch checkpoints the replica
/// accepts, and the owner's announcements that wake the replica's synchronization.
#[derive(Debug, Default)]
pub(super) struct ReplicatedBranchLifecycle {
    /// Replaced whole, so a reader keeps the checkpoint it loaded without holding anything.
    latest: ArcSwapOption<BranchLifecycleCheckpoint>,
    replication: CheckpointReplication,
}

/// One branch lifecycle checkpoint, with the branches it names.
#[derive(Debug)]
pub(super) struct BranchLifecycleCheckpoint {
    snapshot: PersistedRuntimeStateEntry,
    /// Decoded from the snapshot the first time a reader needs it, and never again.
    branches: OnceLock<NamedBranches>,
}

/// The branches one lifecycle checkpoint names.
#[derive(Debug, Default)]
pub(super) struct NamedBranches {
    /// Whether the entity held unbranched work, which has no concrete branch key.
    unbranched: bool,
    concrete: HashSet<BranchKey>,
}

impl ReplicatedBranchLifecycle {
    /// The newest lifecycle checkpoint this node holds, or nothing before it holds one.
    pub(super) fn latest(&self) -> Option<StdArc<BranchLifecycleCheckpoint>> {
        self.latest.load_full()
    }

    /// Replace the lifecycle this node holds with `snapshot`, as its owner publishes a new one or
    /// an ownership transfer activates one.
    pub(super) fn publish(&self, snapshot: PersistedRuntimeStateEntry) {
        let checkpoint = StdArc::new(BranchLifecycleCheckpoint::new(snapshot));
        self.latest.store(Some(checkpoint));
    }

    /// Hold `checkpoint` unless this node already holds the same revision or a newer one, as a
    /// replica installs what its owner sent or reads what its own storage kept, and return the
    /// checkpoint held afterwards.
    pub(super) fn install(
        &self,
        checkpoint: StdArc<BranchLifecycleCheckpoint>,
    ) -> StdArc<BranchLifecycleCheckpoint> {
        let previous = self.latest.rcu(|current| {
            let Some(current) = current else {
                return Some(StdArc::clone(&checkpoint));
            };
            if current.lsm() >= checkpoint.lsm() {
                return Some(StdArc::clone(current));
            }
            Some(StdArc::clone(&checkpoint))
        });
        match previous {
            Some(previous) if previous.lsm() >= checkpoint.lsm() => previous,
            _ => checkpoint,
        }
    }

    pub(super) fn replication(&self) -> &CheckpointReplication {
        &self.replication
    }
}

impl BranchLifecycleCheckpoint {
    pub(super) fn new(snapshot: PersistedRuntimeStateEntry) -> Self {
        Self {
            snapshot,
            branches: OnceLock::new(),
        }
    }

    pub(super) fn lsm(&self) -> u64 {
        self.snapshot.lsm
    }

    pub(super) fn snapshot(&self) -> &PersistedRuntimeStateEntry {
        &self.snapshot
    }

    /// The branches this checkpoint names, decoded the first time they are read.
    pub(super) fn branches(&self) -> error_stack::Result<&NamedBranches, BranchLruSnapshotError> {
        if let Some(branches) = self.branches.get() {
            return Ok(branches);
        }
        let entries = decode_branch_lru_snapshot(&self.snapshot.payload)?;
        let mut named = NamedBranches::default();
        for entry in entries {
            match entry.key {
                Some(key) => {
                    named.concrete.insert(key);
                }
                None => named.unbranched = true,
            }
        }
        self.branches.set(named).discarded(
            "a reader that decoded the same checkpoint concurrently kept the same branches first",
        );
        let branches = self
            .branches
            .get()
            .verified("this reader or a concurrent one set the branches just above");
        Ok(branches)
    }
}

impl NamedBranches {
    /// Whether the checkpoint names the branch `key` identifies. An absent key names unbranched
    /// work.
    pub(super) fn names(&self, key: Option<&BranchKey>) -> bool {
        match key {
            Some(key) => self.concrete.contains(key),
            None => self.unbranched,
        }
    }

    /// Every branch the checkpoint names, unbranched work as an absent key.
    pub(super) fn keys(&self) -> Vec<Option<BranchKey>> {
        let mut keys = Vec::with_capacity(self.concrete.len());
        if self.unbranched {
            keys.push(None);
        }
        for key in &self.concrete {
            keys.push(Some(key.clone()));
        }
        keys
    }
}
