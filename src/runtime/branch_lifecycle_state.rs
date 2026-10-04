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

use ahash::{HashMap, HashSet};
use imbl::{GenericHashMap, shared_ptr::DefaultSharedPtr};
use meticulous::OptionExt as _;
use nervix_checkpoint_replication::CheckpointReplication;
use nervix_interconnect::RuntimeState;
use nervix_primitives::{
    publication::{ArcSwap, ArcSwapOption},
    sync::{
        Arc, StdArc,
        blocking::{Mutex, MutexGuard, OnceLock},
    },
};
use nervix_recovery::Discarded as _;

use super::{
    BranchKey, PersistedRuntimeStateEntry, RuntimeStatePlacement, SharedStateAssignment,
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
#[derive(Debug)]
pub(super) struct ReplicatedBranchLifecycle {
    assignment: SharedStateAssignment,
    /// Passive copies belong to this entity. The replica task publishes complete immutable
    /// values; pruning cannot acquire another entity's registry shard.
    passive: ArcSwap<PassiveCheckpoints>,
    /// Replaced whole, so a reader keeps the checkpoint it loaded without holding anything.
    latest: ArcSwapOption<BranchLifecycleCheckpoint>,
    replication: CheckpointReplication,
    catalog: BranchCheckpointCatalog,
    /// Changed under a short lock that belongs to this one entity and is never held across an
    /// await: an announcement adds to it, and the replica task takes everything at once.
    announcements: Mutex<AnnouncedCheckpoints>,
}

type PassiveCheckpoints = GenericHashMap<
    RuntimeStatePlacement,
    StdArc<PersistedRuntimeStateEntry>,
    ahash::RandomState,
    DefaultSharedPtr,
>;

impl Default for ReplicatedBranchLifecycle {
    fn default() -> Self {
        Self {
            assignment: Arc::new(ArcSwapOption::empty()),
            passive: ArcSwap::from_pointee(PassiveCheckpoints::default()),
            latest: ArcSwapOption::empty(),
            replication: CheckpointReplication::new(),
            catalog: BranchCheckpointCatalog::default(),
            announcements: Mutex::default(),
        }
    }
}

impl ReplicatedBranchLifecycle {
    pub(super) fn assigned(assignment: SharedStateAssignment) -> Self {
        Self {
            assignment,
            ..Self::default()
        }
    }

    pub(super) fn placement_is_current(&self, placement: &RuntimeStatePlacement) -> bool {
        let assignment = self.assignment.load();
        let Some(assignment) = assignment.as_deref() else {
            return false;
        };
        assignment.names(placement)
    }

    pub(super) fn replicates_from(
        &self,
        placement: &RuntimeStatePlacement,
        local: &nervix_models::ClusterNodeName,
        owner: &nervix_models::ClusterNodeName,
    ) -> bool {
        let assignment = self.assignment.load();
        let Some(assignment) = assignment.as_deref() else {
            return false;
        };
        assignment.names(placement) && assignment.replicates_from(local, owner)
    }

    pub(super) fn passive_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<StdArc<PersistedRuntimeStateEntry>> {
        self.passive.load().get(placement).cloned()
    }

    /// Move a fetched payload into the entity's publication only when its revision advances.
    pub(super) fn hold_passive_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
    ) {
        let snapshot = StdArc::new(snapshot);
        self.passive.rcu(|current| {
            if let Some(held) = current.get(placement)
                && held.lsm >= snapshot.lsm
            {
                return current.clone();
            }
            let mut next = current.as_ref().clone();
            next.insert(placement.clone(), snapshot.clone());
            StdArc::new(next)
        });
    }

    pub(super) fn prune_passive_checkpoints(&self, branches: &NamedBranches) {
        self.passive.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.retain(|placement, _| {
                placement.branch_key.is_none() || branches.names(placement.branch_key.as_ref())
            });
            next
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "assignment replacement discards superseded passive state"
        )
    )]
    pub(super) fn purge_stale_passive_checkpoints(&self) {
        let assignment = self.assignment.load();
        self.passive.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.retain(|placement, _| {
                assignment
                    .as_deref()
                    .is_some_and(|assignment| assignment.names(placement))
            });
            next
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "ownership activation consumes the entity's passive checkpoint once"
        )
    )]
    pub(super) fn take_passive_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<PersistedRuntimeStateEntry> {
        let previous = self.passive.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.remove(placement);
            next
        });
        let held = previous.get(placement)?.clone();
        drop(previous);
        match StdArc::try_unwrap(held) {
            Ok(snapshot) => Some(snapshot),
            Err(shared) => Some(shared.as_ref().clone()),
        }
    }
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
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the entity's lifecycle handle keeps the owner's pending announcements for \
                      the one replica task that takes them",
            key = "one entity's branch lifecycle handle",
            bound = "one synchronous insertion or take; the guard never crosses an await"
        )
    )]
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
