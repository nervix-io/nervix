//! The branch lifecycle of one branch-keyed entity, as one node holds and replicates it.
//!
//! Layer: data plane.
//! - **Owns.** The newest branch lifecycle checkpoint this node holds for one entity, the branches
//!   that checkpoint names, decoded once, this node's part in replicating the lifecycle, the
//!   catalog of the entity's branch checkpoints this node owns, and the owner's announcements a
//!   replica has not acted on yet.
//! - **Depends on.** Runtime-state snapshots, the branch lifecycle codec, typed branch keys, the
//!   branch checkpoint catalog, the primitive publication and synchronization boundary and
//!   checkpoint replication.
//! - **Must not know.** Branch tasks, schedules, how a checkpoint is persisted or fetched, or NSPL.

use std::sync::Arc as StdArc;

use ahash::{HashMap, HashSet};
use meticulous::OptionExt as _;
use nervix_checkpoint_replication::CheckpointReplication;
use nervix_interconnect::RuntimeState;
use nervix_primitives::{
    publication::ArcSwapOption,
    sync::blocking::{Mutex, MutexGuard, OnceLock},
};
use nervix_recovery::Discarded as _;

use super::{
    BranchKey, PersistedRuntimeStateEntry, RuntimeStatePlacement,
    branch_checkpoint_catalog::BranchCheckpointCatalog,
    branch_lru_state::{BranchLruSnapshotError, decode_branch_lru_snapshot},
};

/// The branch lifecycle of one branch-keyed entity as this node holds it.
///
/// On the entity's owner it holds the lifecycle the owner published last, which replicas
/// synchronize, the owner's replication of it to them, and the catalog of the branch checkpoints
/// the owner's branch states publish. On a replica it holds the newest lifecycle the replica
/// installed, whose branches decide which branch checkpoints the replica accepts, and the owner's
/// announcements that wake the replica task keeping the entity current, until that task takes
/// them.
#[derive(Debug, Default)]
pub(super) struct ReplicatedBranchLifecycle {
    /// Replaced whole, so a reader keeps the checkpoint it loaded without holding anything.
    latest: ArcSwapOption<BranchLifecycleCheckpoint>,
    replication: CheckpointReplication,
    catalog: BranchCheckpointCatalog,
    /// Changed under a short lock that belongs to this one entity and is never held across an
    /// await: an announcement adds to it, and the replica task takes everything at once.
    announcements: Mutex<AnnouncedCheckpoints>,
}

/// The owner's announcements a replica task has not taken yet, the newest one of each checkpoint.
#[derive(Debug, Default)]
pub(super) struct AnnouncedCheckpoints {
    /// The newest branch lifecycle revision the owner announced.
    pub(super) lifecycle: Option<u64>,
    /// The newest checkpoint the owner announced for each branch. Unbranched work is the absent
    /// key.
    pub(super) branches: HashMap<Option<BranchKey>, AnnouncedCheckpoint>,
}

/// One branch checkpoint an owner announced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AnnouncedCheckpoint {
    /// The state the checkpoint belongs to, which names the lifetime of guest state for a WASM
    /// processor branch.
    pub(super) state: RuntimeState,
    pub(super) lsm: u64,
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

impl RuntimeStatePlacement {
    /// The placement of the branch lifecycle that names this placement's branch, when this
    /// places the state one branch of a replicated branch-keyed entity keeps. The lifecycle is laid
    /// out by the same schemas as the branch state, and is kept once for the whole entity.
    pub(super) fn branch_lifecycle(&self) -> Option<Self> {
        let schema = match self.state {
            RuntimeState::Deduplicator { schema }
            | RuntimeState::WindowProcessor { schema }
            | RuntimeState::WasmProcessor { schema, .. } => schema,
            RuntimeState::BranchAggregated
            | RuntimeState::Correlator { .. }
            | RuntimeState::KafkaOffset
            | RuntimeState::MaterializedRelay { .. }
            | RuntimeState::BranchLru { .. } => return None,
        };
        Some(Self {
            domain: self.domain.clone(),
            state: RuntimeState::BranchLru { schema },
            kind: self.kind,
            identifier: self.identifier.clone(),
            branch_key: None,
        })
    }
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

    /// Whether the newest lifecycle checkpoint this node holds names the branch `key` identifies.
    /// A node that holds no lifecycle yet names no branch.
    pub(super) fn names(
        &self,
        key: Option<&BranchKey>,
    ) -> error_stack::Result<bool, BranchLruSnapshotError> {
        let Some(held) = self.latest() else {
            return Ok(false);
        };
        Ok(held.branches()?.names(key))
    }

    pub(super) fn replication(&self) -> &CheckpointReplication {
        &self.replication
    }

    /// The catalog of the branch checkpoints this node's branch states of the entity publish.
    pub(super) fn catalog(&self) -> &BranchCheckpointCatalog {
        &self.catalog
    }

    /// Record the owner's announcement of lifecycle revision `lsm`, and wake the replica task.
    pub(super) fn announce_lifecycle(&self, lsm: u64) {
        {
            let mut announced = self.announcements();
            let newest = match announced.lifecycle {
                Some(earlier) => earlier.max(lsm),
                None => lsm,
            };
            announced.lifecycle = Some(newest);
        }
        self.replication.announced();
    }

    /// Record the owner's announcement of `checkpoint` of `branch`, and wake the replica task. An
    /// older announcement of the branch never replaces a newer one.
    pub(super) fn announce_branch(
        &self,
        branch: Option<BranchKey>,
        checkpoint: AnnouncedCheckpoint,
    ) {
        {
            let mut announced = self.announcements();
            let newer = match announced.branches.get(&branch) {
                Some(earlier) => earlier.lsm < checkpoint.lsm,
                None => true,
            };
            if newer {
                announced.branches.insert(branch, checkpoint);
            }
        }
        self.replication.announced();
    }

    /// Take every announcement the replica task has not taken yet.
    pub(super) fn take_announcements(&self) -> AnnouncedCheckpoints {
        std::mem::take(&mut *self.announcements())
    }

    /// The announcements not taken yet, locked for one short change that never crosses an await.
    fn announcements(&self) -> MutexGuard<'_, AnnouncedCheckpoints> {
        self.announcements.lock()
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

    /// The branches a lifecycle names, for a test that plans against a lifecycle it made up.
    #[cfg(test)]
    pub(super) fn from_keys(keys: impl IntoIterator<Item = Option<BranchKey>>) -> Self {
        let mut named = Self::default();
        for key in keys {
            match key {
                Some(key) => {
                    named.concrete.insert(key);
                }
                None => named.unbranched = true,
            }
        }
        named
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "branch_lifecycle_state_shuttle_tests.rs"]
mod shuttle_tests;
