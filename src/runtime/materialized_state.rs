//! Branch-owned materialized relay rows and immutable snapshot generations.
//!
//! Layer: data plane.
//! - **Owns.** Exclusive branch records and immutable row and membership publications,
//!   assignment-fenced installation, exact branch-generation capture and sealed artifact retention.
//! - **Depends on.** Typed placements, assignment fences, Arrow row views, codecs and admission.
//! - **Must not know.** Models, NSPL text, archive layouts or consensus restore policies.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "materialized assignment and snapshot installation establish state generations; \
                  record operations override this default"
    )
)]

use ahash::RandomState;
use error_stack::{Report, ResultExt as _};
use imbl::{GenericHashMap, shared_ptr::DefaultSharedPtr};
use nervix_checkpoint_replication::CheckpointReplication;
use nervix_execution::{Executor, MemoryClass, Reservation};
use nervix_models::ClusterNodeName;
use nervix_primitives::{
    publication::{ArcSwap, ArcSwapOption},
    sync::{
        Arc, StdArc,
        atomic::{AtomicU64, Ordering},
    },
};

use super::{
    BranchKey, RuntimeStateOperationError, RuntimeStatePlacement, StateAssignmentAuthority,
    StateAssignmentToken, StateCapability, StateReplicationRoles,
    lsm_sequence::LsmSequence,
    materialized_snapshot::{
        MaterializedGeneration, MaterializedGenerationRecord, MaterializedSnapshotError,
        RestoredMaterializedSnapshot, SealedMaterializedSnapshot,
    },
};
use crate::runtime_schema::RuntimeRow;

#[cfg(test)]
#[path = "materialized_state_publication_tests.rs"]
mod publication_tests;

#[cfg(test)]
#[path = "materialized_state_cost_tests.rs"]
mod cost_tests;

type MaterializedBranches = GenericHashMap<
    Option<BranchKey>,
    Arc<ArcSwapOption<RuntimeRow>>,
    RandomState,
    DefaultSharedPtr,
>;

/// One task's mutable record. Readers and captured generations hold the published Arrow view;
/// the owner retains its selection so a replacement never discovers state through the index.
#[derive(Debug)]
struct MaterializedBranchRecord {
    record: StdArc<RuntimeRow>,
    /// Also retained by readers and the membership publication after this owner's borrow ends.
    published: Arc<ArcSwapOption<RuntimeRow>>,
}

impl MaterializedBranchRecord {
    fn new(record: RuntimeRow) -> Self {
        let record = StdArc::new(record);
        Self {
            published: Arc::new(ArcSwapOption::from(Some(record.clone()))),
            record,
        }
    }

    fn replace(&mut self, record: RuntimeRow) -> bool {
        if !record.metadata().is_newer_than(self.record.metadata()) {
            return false;
        }
        self.record = StdArc::new(record);
        self.published.store(Some(self.record.clone()));
        true
    }
}

#[derive(Debug)]
pub(super) struct ReplicatedMaterializedRelayState {
    placement: RuntimeStatePlacement,
    schema: StdArc<arrow_schema::Schema>,
    assignment: StateAssignmentAuthority,
    /// Lifecycle changes publish a persistent index under the assignment barrier. Established
    /// branch updates retain their row publication and never reach this index.
    entries: ArcSwap<MaterializedBranches>,
    /// Advances whenever a branch appears in or leaves this state. A capture records it, so a
    /// snapshot sealed before an eviction cannot resurrect the branch that eviction dropped.
    branch_generation: AtomicU64,
    /// The highest ownership fence whose snapshot has been installed here. A snapshot sealed under
    /// an assignment the owner has already left behind arrives stale, and installing it would undo
    /// what the current assignment published.
    installed_fence: AtomicU64,
    current_lsm: LsmSequence,
    last_persisted_lsm: AtomicU64,
    /// The most recent generation sealed here, retained so that a requester already holding this
    /// revision, or a second requester arriving for it, is answered without scanning or encoding
    /// the state again. Exactly one generation is retained, so no build pins unbounded history;
    /// a reader that took a copy keeps its own charge until it releases it.
    sealed: nervix_primitives::sync::blocking::Mutex<Option<SealedMaterializedSnapshot>>,
    /// Admits one snapshot build per placement. Requesters that arrive while a build is running
    /// wait for its result instead of starting a second scan of the same state.
    build: nervix_primitives::sync::Mutex<()>,
    /// What each replica reported holding and the offer of the newest snapshot to them while this
    /// node originates the state, and the owner's announcements while it replicates it.
    replication: CheckpointReplication,
}

/// Read-only access to materialized records and snapshots.
#[derive(Debug, Clone)]
pub struct MaterializedRelayStateRead {
    state: Arc<ReplicatedMaterializedRelayState>,
}

/// Authoritative materialization access for one concrete assignment generation.
#[derive(Debug)]
pub struct MaterializedRelayStateOriginator {
    read: MaterializedRelayStateRead,
    assignment: StateAssignmentToken,
    branches: super::HashMap<Option<BranchKey>, MaterializedBranchRecord>,
}

/// Replica snapshot installation access for one concrete assignment generation.
#[derive(Debug, Clone)]
pub struct MaterializedRelaySnapshotInstaller {
    read: MaterializedRelayStateRead,
    assignment: StateAssignmentToken,
}

/// Local snapshot persistence access shared by owners and replicas.
#[derive(Debug, Clone)]
pub(super) struct MaterializedRelayStatePersistence {
    read: MaterializedRelayStateRead,
}

#[derive(Debug)]
pub(super) struct MaterializedRelayStateAssignment {
    pub(super) originator: Option<MaterializedRelayStateOriginator>,
    pub(super) installer: Option<MaterializedRelaySnapshotInstaller>,
    pub(super) persistence: MaterializedRelayStatePersistence,
}

/// One materialized record as it is reported through the public interface: its branch key, the
/// columns of that record rendered for display, and the watermarks it was materialized with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedRecordReport {
    pub(crate) branch: Option<BranchKey>,
    pub(crate) payload: String,
    pub(crate) ingested_at_low_watermark: nervix_models::Timestamp,
    pub(crate) ingested_at_high_watermark: nervix_models::Timestamp,
}

impl ReplicatedMaterializedRelayState {
    /// Build this state from a snapshot that has already been opened, or empty when there is none.
    ///
    /// A restored state adopts the revision and branch lifecycle its snapshot was captured at, so
    /// it continues that history instead of starting a new one beside it.
    pub(super) fn restored(
        placement: RuntimeStatePlacement,
        schema: StdArc<arrow_schema::Schema>,
        restored: Option<RestoredMaterializedSnapshot>,
    ) -> Self {
        let state = Self::new(placement, schema);
        let Some(restored) = restored else {
            return state;
        };
        let mut entries = MaterializedBranches::default();
        for record in restored.records {
            let branch = MaterializedBranchRecord::new(record.row);
            entries.insert(record.branch, branch.published);
        }
        state.entries.store(StdArc::new(entries));
        state
            .branch_generation
            .store(restored.branch_generation, Ordering::SeqCst);
        state
            .installed_fence
            .store(restored.fence, Ordering::SeqCst);
        state.current_lsm.adopt(restored.revision);
        state
            .last_persisted_lsm
            .store(restored.revision, Ordering::SeqCst);
        state
    }

    pub(super) fn new(
        placement: RuntimeStatePlacement,
        schema: StdArc<arrow_schema::Schema>,
    ) -> Self {
        Self {
            placement,
            schema,
            assignment: StateAssignmentAuthority::default(),
            entries: ArcSwap::from_pointee(MaterializedBranches::default()),
            branch_generation: AtomicU64::new(0),
            installed_fence: AtomicU64::new(0),
            current_lsm: LsmSequence::restored(0),
            last_persisted_lsm: AtomicU64::new(0),
            sealed: nervix_primitives::sync::blocking::Mutex::new(None),
            build: nervix_primitives::sync::Mutex::new(()),
            replication: CheckpointReplication::new(),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    pub(super) fn replication(&self) -> &CheckpointReplication {
        &self.replication
    }

    pub(super) fn bind(
        state: &Arc<Self>,
        roles: StateReplicationRoles,
        local_node: Option<&ClusterNodeName>,
    ) -> MaterializedRelayStateAssignment {
        let binding = state.assignment.rebind(roles, local_node);
        let read = MaterializedRelayStateRead {
            state: state.clone(),
        };
        let mut branches = super::HashMap::default();
        if binding.token_for(StateCapability::Originate).is_some() {
            for (key, published) in state.entries.load().iter() {
                if let Some(record) = published.load_full() {
                    branches.insert(
                        key.clone(),
                        MaterializedBranchRecord {
                            record,
                            published: published.clone(),
                        },
                    );
                }
            }
        }
        MaterializedRelayStateAssignment {
            originator: binding
                .token_for(StateCapability::Originate)
                .map(|assignment| MaterializedRelayStateOriginator {
                    read: read.clone(),
                    assignment,
                    branches,
                }),
            installer: binding
                .token_for(StateCapability::InstallSnapshot)
                .map(|assignment| MaterializedRelaySnapshotInstaller {
                    read: read.clone(),
                    assignment,
                }),
            persistence: MaterializedRelayStatePersistence { read: read.clone() },
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    pub(super) fn read(state: &Arc<Self>) -> MaterializedRelayStateRead {
        MaterializedRelayStateRead {
            state: state.clone(),
        }
    }

    pub(super) fn current_installer(
        state: &Arc<Self>,
    ) -> Option<MaterializedRelaySnapshotInstaller> {
        let assignment = state
            .assignment
            .current_binding()
            .token_for(StateCapability::InstallSnapshot)?;
        Some(MaterializedRelaySnapshotInstaller {
            read: Self::read(state),
            assignment,
        })
    }
}

impl MaterializedRelayStateRead {
    pub(super) fn placement(&self) -> &RuntimeStatePlacement {
        &self.state.placement
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    pub(super) fn replication(&self) -> &CheckpointReplication {
        &self.state.replication
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    pub(super) fn schema(&self) -> &StdArc<arrow_schema::Schema> {
        &self.state.schema
    }

    pub(super) fn current_lsm(&self) -> u64 {
        self.state.current_lsm.current()
    }

    pub(super) fn primary_node(&self) -> Option<ClusterNodeName> {
        self.state.assignment.roles().primary_node.clone()
    }

    /// Take one immutable generation of this state: its records, the revision they stand at, the
    /// assignment that owns them, and the branch lifecycle they belong to.
    ///
    /// The barrier excludes every change to which branches exist, so the records and the branch
    /// lifecycle describe the same moment exactly. Replacing an existing branch's record is
    /// admitted without the barrier and may land while the records are read. The revision is read
    /// first, so such a replacement is either inside that revision or ahead of it, and a later
    /// generation carries it again.
    ///
    /// The barrier is held only for the clone of the row views, which share the carrier columns
    /// rather than copying them. Encoding happens afterwards, against a value no later update can
    /// change, so updates and deletions proceed while a snapshot is being written out.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    pub(super) fn capture(&self) -> MaterializedGeneration {
        self.state
            .assignment
            .serialize_with(|binding| self.capture_generation(binding.fence()))
    }

    /// Admit the row-view and grouping arrays before capturing a backup cut. Carrier columns and
    /// branch identities remain shared; the charge follows the immutable generation until its
    /// archive sections have been staged. One capture cannot monopolize the bulk worker budget.
    pub(super) fn capture_for_backup(
        &self,
        executor: &Executor,
    ) -> Result<(MaterializedGeneration, Reservation), Report<MaterializedSnapshotError>> {
        self.state.assignment.serialize_with(|binding| {
            let bytes = self
                .state
                .entries
                .load()
                .len()
                .checked_mul(
                    std::mem::size_of::<MaterializedGenerationRecord>()
                        + std::mem::size_of::<std::ops::Range<usize>>(),
                )
                .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
            let bytes = u64::try_from(bytes)
                .map_err(|_| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
            if bytes > 8 * 1024 * 1024 {
                return Err(Report::new(MaterializedSnapshotError::MetadataTooLarge));
            }
            let charge = executor
                .try_reserve(MemoryClass::Bulk, bytes.max(1))
                .map_err(|error| error.change_context(MaterializedSnapshotError::Admission))?;
            Ok((self.capture_generation(binding.fence()), charge))
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "snapshot callers hold the assignment barrier while sharing branch row views"
        )
    )]
    fn capture_generation(&self, fence: u64) -> MaterializedGeneration {
        let revision = self.state.current_lsm.current();
        let branch_generation = self.state.branch_generation.load(Ordering::SeqCst);
        let entries = self.state.entries.load();
        let mut records = Vec::with_capacity(entries.len());
        for (branch, published) in entries.iter() {
            if let Some(row) = published.load_full() {
                records.push(MaterializedGenerationRecord {
                    branch: branch.clone(),
                    row: (*row).clone(),
                });
            }
        }
        records.sort_unstable_by(|left, right| {
            super::branch_key_display(&left.branch).cmp(super::branch_key_display(&right.branch))
        });
        MaterializedGeneration::new(
            revision,
            fence,
            branch_generation,
            self.state.schema.clone(),
            records,
        )
    }

    /// Seal a generation of this state that is newer than `after_revision`, or report that the
    /// requester already holds the current one.
    ///
    /// One build runs per placement: a requester arriving while another build is in flight waits
    /// for it and takes its result when it covers the same revision. A requester that is already
    /// current causes no scan of the entries and no encoding at all.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the retained materialized assignment serializes snapshot publication",
            key = "materialized state assignment and sealed revision",
            bound = "one assignment builds or installs one monotonically selected sealed \
                     generation"
        )
    )]
    pub(super) async fn seal_after(
        &self,
        executor: &Executor,
        staging: &super::snapshot_staging::SnapshotStaging,
        after_revision: Option<u64>,
    ) -> Result<Option<SealedMaterializedSnapshot>, Report<MaterializedSnapshotError>> {
        if let Some(usable) = self.usable_sealed(after_revision) {
            return Ok(usable.sealed);
        }
        let _build = self.state.build.lock().await;
        // A build that finished while this requester waited may already answer it.
        if let Some(usable) = self.usable_sealed(after_revision) {
            return Ok(usable.sealed);
        }
        let generation = self.capture();
        if after_revision.is_some_and(|after| generation.revision() <= after) {
            return Ok(None);
        }
        let sealed = generation.seal(executor, staging).await?;
        // Sealing runs outside the barrier, so the assignment that authorized this capture may
        // have been superseded while it ran. Serving that generation would present state this
        // node no longer owns as current.
        let fence = self.state.assignment.current_binding().fence();
        if fence != generation.fence() {
            return Err(Report::new(MaterializedSnapshotError::OwnershipChanged {
                captured: generation.fence(),
                current: fence,
            }));
        }
        *self.state.sealed.lock() = Some(sealed.clone());
        Ok(Some(sealed))
    }

    /// The retained sealed generation, when it is exactly the one asked for.
    ///
    /// A transfer names the revision it was described, so an owner that has moved on refuses
    /// instead of substituting a different generation under that description.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the retained materialized assignment serializes snapshot publication",
            key = "materialized state assignment and sealed revision",
            bound = "one assignment builds or installs one monotonically selected sealed \
                     generation"
        )
    )]
    pub(super) fn sealed_at(&self, revision: u64) -> Option<SealedMaterializedSnapshot> {
        self.state
            .sealed
            .lock()
            .clone()
            .filter(|sealed| sealed.descriptor.revision == revision)
    }

    /// What the retained generation answers for `after_revision`, when it answers at all.
    ///
    /// It answers only while it is still the current revision. A retained generation older than
    /// the live state would report stale contents as current, so it is rebuilt instead.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the retained materialized assignment serializes snapshot publication",
            key = "materialized state assignment and sealed revision",
            bound = "one assignment builds or installs one monotonically selected sealed \
                     generation"
        )
    )]
    fn usable_sealed(&self, after_revision: Option<u64>) -> Option<UsableSealedSnapshot> {
        let sealed = self.state.sealed.lock().clone()?;
        if sealed.descriptor.revision != self.state.current_lsm.current()
            || sealed.descriptor.fence != self.state.assignment.current_binding().fence()
        {
            return None;
        }
        if after_revision.is_some_and(|after| sealed.descriptor.revision <= after) {
            return Some(UsableSealedSnapshot { sealed: None });
        }
        Some(UsableSealedSnapshot {
            sealed: Some(sealed),
        })
    }

    /// Every record this state holds, each with the concrete branch it belongs to.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    pub(super) fn records(&self) -> Vec<MaterializedGenerationRecord> {
        self.capture().records().to_vec()
    }

    /// The record of exactly one branch. An absent key names the unbranched record, never every
    /// branch; a branch without a record simply has none.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    pub(super) fn record(&self, key: &Option<BranchKey>) -> Option<MaterializedGenerationRecord> {
        let entries = self.state.entries.load();
        let published = entries.get(key)?;
        let row = published.load_full()?;
        Some(MaterializedGenerationRecord {
            branch: key.clone(),
            row: (*row).clone(),
        })
    }

    pub(super) fn restored_branch_watermarks(
        &self,
    ) -> Vec<(Option<BranchKey>, nervix_models::Timestamp)> {
        let mut watermarks = Vec::new();
        for (branch, published) in self.state.entries.load().iter() {
            if let Some(row) = published.load_full() {
                watermarks.push((branch.clone(), row.metadata().ingested_at_high_watermark()));
            }
        }
        watermarks
    }
}

/// Whether the retained generation answers a requester, and with what. An answer of `None` inside
/// means the requester is already current: no scan, no encoding, nothing to send.
struct UsableSealedSnapshot {
    sealed: Option<SealedMaterializedSnapshot>,
}

impl MaterializedRelayStateOriginator {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    pub(super) fn read(&self) -> &MaterializedRelayStateRead {
        &self.read
    }

    /// Keep `record` as its branch's record when it is newer than the one held, returning the
    /// revision the change is stamped with.
    ///
    /// Replacing an existing branch's record is admitted without the barrier. A branch's first
    /// record changes which branches exist, so it is kept under the barrier, where no capture can
    /// observe the branch apart from the branch lifecycle it belongs to.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the originating task updates its own branch record and row publication"
        )
    )]
    pub(super) fn update_last_by_timestamp(
        &mut self,
        key: &Option<BranchKey>,
        record: RuntimeRow,
    ) -> Result<Option<u64>, Report<super::StateAuthorityError>> {
        let state = &self.read.state;
        if let Some(branch) = self.branches.get_mut(key) {
            return state
                .assignment
                .authorize(self.assignment, StateCapability::Originate, || {
                    if branch.replace(record) {
                        Some(state.advance_revision())
                    } else {
                        None
                    }
                });
        }
        state
            .assignment
            .authorize_exclusive(self.assignment, StateCapability::Originate, || {
                let branch = MaterializedBranchRecord::new(record);
                let mut entries = (**state.entries.load()).clone();
                entries.insert(key.clone(), branch.published.clone());
                self.branches.insert(key.clone(), branch);
                state.advance_branch_generation();
                state.entries.store(StdArc::new(entries));
                Some(state.advance_revision())
            })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    pub(super) fn remove_key(
        &mut self,
        key: &Option<BranchKey>,
    ) -> Result<Option<u64>, Report<super::StateAuthorityError>> {
        let state = &self.read.state;
        state
            .assignment
            .authorize_exclusive(self.assignment, StateCapability::Originate, || {
                let branch = self.branches.remove(key)?;
                branch.published.store(None);
                let mut entries = (**state.entries.load()).clone();
                entries.remove(key);
                state.entries.store(StdArc::new(entries));
                state.advance_branch_generation();
                Some(state.advance_revision())
            })
    }
}

impl ReplicatedMaterializedRelayState {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    fn advance_revision(&self) -> u64 {
        self.current_lsm.advance()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    fn advance_branch_generation(&self) {
        self.branch_generation
            .checked_advance("one process cannot apply 2^64 branch lifecycle changes to one relay");
    }
}

/// Advancing a counter that cannot wrap without the process having outlived the universe. The
/// bound is stated at every call rather than being assumed by a bare addition.
trait CheckedAdvance {
    fn checked_advance(&self, reason: &'static str);
}

impl CheckedAdvance for AtomicU64 {
    fn checked_advance(&self, reason: &'static str) {
        use meticulous::OptionExt as _;

        let mut current = self.load(Ordering::SeqCst);
        loop {
            let next = current.checked_add(1).assured(reason);
            match self.compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

impl MaterializedRelaySnapshotInstaller {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    pub(super) fn read(&self) -> &MaterializedRelayStateRead {
        &self.read
    }

    /// Replace this state with a restored snapshot, or refuse it.
    ///
    /// Decoding already happened. Captures serialize with this publication; point readers may
    /// finish a borrowed row or observe absence while the preceding members end. A snapshot from
    /// an earlier branch lifecycle is refused, so eviction cannot be undone by a preceding capture.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the retained materialized assignment serializes snapshot publication",
            key = "materialized state assignment and sealed revision",
            bound = "one assignment builds or installs one monotonically selected sealed \
                     generation"
        )
    )]
    pub(super) fn install(
        &self,
        restored: RestoredMaterializedSnapshot,
    ) -> error_stack::Result<(), RuntimeStateOperationError> {
        let installation = self.read.state.assignment.authorize_exclusive(
            self.assignment,
            StateCapability::InstallSnapshot,
            || {
                let installed_revision = self.read.state.current_lsm.current();
                if restored.revision < installed_revision {
                    return Err(Report::new(
                        RuntimeStateOperationError::MaterializedSnapshotRevision {
                            received: restored.revision,
                            current: installed_revision,
                        },
                    ));
                }
                let installed_branch_generation =
                    self.read.state.branch_generation.load(Ordering::SeqCst);
                if restored.branch_generation < installed_branch_generation {
                    return Err(Report::new(
                        RuntimeStateOperationError::MaterializedSnapshotBranchGeneration {
                            received: restored.branch_generation,
                            current: installed_branch_generation,
                        },
                    ));
                }
                let installed_fence = self.read.state.installed_fence.load(Ordering::SeqCst);
                if restored.fence < installed_fence {
                    return Err(Report::new(
                        RuntimeStateOperationError::MaterializedSnapshotFence {
                            received: restored.fence,
                            current: installed_fence,
                        },
                    ));
                }
                let mut entries = MaterializedBranches::default();
                for record in restored.records {
                    let branch = MaterializedBranchRecord::new(record.row);
                    entries.insert(record.branch, branch.published);
                }
                for published in self.read.state.entries.load().values() {
                    published.store(None);
                }
                self.read.state.entries.store(StdArc::new(entries));
                self.read
                    .state
                    .branch_generation
                    .fetch_max(restored.branch_generation, Ordering::SeqCst);
                self.read
                    .state
                    .installed_fence
                    .fetch_max(restored.fence, Ordering::SeqCst);
                self.read.state.current_lsm.adopt(restored.revision);
                *self.read.state.sealed.lock() = None;
                Ok(())
            },
        );
        installation.change_context(RuntimeStateOperationError::Authority)?
    }
}

impl MaterializedRelayStatePersistence {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running branches read or advance the retained materialized assignment"
        )
    )]
    pub(super) fn read(&self) -> &MaterializedRelayStateRead {
        &self.read
    }

    /// Whether this state holds a revision it has not persisted yet.
    pub(super) fn is_dirty(&self) -> bool {
        self.read.state.current_lsm.current()
            > self.read.state.last_persisted_lsm.load(Ordering::SeqCst)
    }

    pub(super) fn last_persisted_lsm(&self) -> u64 {
        self.read.state.last_persisted_lsm.load(Ordering::SeqCst)
    }

    pub(super) fn record_persisted(&self, lsm: u64) {
        self.read
            .state
            .last_persisted_lsm
            .fetch_max(lsm, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{DomainName, ModelKind, ModelName, RelayName};

    use super::{
        super::{Runtime, materialized_snapshot::SealedSource},
        *,
    };
    use crate::runtime_schema::{RuntimeValue, test_runtime_row};

    fn test_placement(relay: &str) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("default")
                .assured("the test domain name satisfies the domain grammar"),
            state: super::super::RuntimeState::MaterializedRelay {
                schema: nervix_models::SchemaFingerprint::from_digest([7; 32]),
            },
            kind: ModelKind::Relay,
            identifier: ModelName::from(
                &RelayName::parse(relay).assured("the test relay name satisfies the relay grammar"),
            ),
            branch_key: None,
        }
    }

    fn timestamped_row(value: i64) -> RuntimeRow {
        let row = test_runtime_row([("value".into(), RuntimeValue::I64(value))]);
        RuntimeRow::new(
            Arc::new(row.one_row_batch()),
            0,
            crate::runtime_schema::RuntimeRecordMetadata::from_remote(
                nervix_models::RemoteRuntimeRecordMetadata {
                    ingested_at_low_watermark: nervix_models::Timestamp::from_unix_nanos(value),
                    ingested_at_high_watermark: nervix_models::Timestamp::from_unix_nanos(value),
                },
            ),
        )
        .assured("timestamped row exists")
    }

    fn captured_revision(
        executor: &Executor,
        state: &Arc<ReplicatedMaterializedRelayState>,
    ) -> RestoredMaterializedSnapshot {
        RestoredMaterializedSnapshot::from_captured_generation(
            executor,
            ReplicatedMaterializedRelayState::read(state).capture(),
        )
    }

    fn replica_race_preserves_the_restored_revision() {
        let executor = Executor::default();
        let row = timestamped_row(1);
        let schema = row.arrow_schema();
        let owner = Arc::new(ReplicatedMaterializedRelayState::new(
            test_placement("state"),
            schema.clone(),
        ));
        let mut originator = ReplicatedMaterializedRelayState::bind(
            &owner,
            StateReplicationRoles::owned_by(None),
            None,
        )
        .originator
        .assured("owner originates");
        let alpha = super::super::string_branch_key("tenant", "alpha");
        let beta = super::super::string_branch_key("tenant", "beta");
        originator
            .update_last_by_timestamp(&alpha, row)
            .assured("alpha starts");
        originator
            .update_last_by_timestamp(&beta, timestamped_row(2))
            .assured("beta starts");
        let earlier = captured_revision(&executor, &owner);
        originator
            .update_last_by_timestamp(&alpha, timestamped_row(3))
            .assured("alpha advances");
        let restored = captured_revision(&executor, &owner);
        let replica = Arc::new(ReplicatedMaterializedRelayState::restored(
            test_placement("state"),
            schema,
            Some(restored),
        ));
        let local = ClusterNodeName::parse("replica").assured("node is valid");
        let installer = ReplicatedMaterializedRelayState::bind(
            &replica,
            StateReplicationRoles::new(
                Some(ClusterNodeName::parse("owner").assured("node is valid")),
                vec![local.clone()],
                0,
            ),
            Some(&local),
        )
        .installer
        .assured("replica installs");
        originator
            .update_last_by_timestamp(&alpha, timestamped_row(4))
            .assured("restored owner advances");
        let latest = captured_revision(&executor, &owner);
        #[cfg(feature = "shuttle")]
        {
            let delayed = nervix_primitives::thread::spawn({
                let installer = installer.clone();
                move || {
                    assert!(installer.install(earlier).is_err());
                }
            });
            installer
                .install(latest)
                .assured("latest owner revision installs");
            delayed
                .join()
                .assured("delayed replica synchronization completes");
        }
        #[cfg(not(feature = "shuttle"))]
        {
            assert!(installer.install(earlier).is_err());
            installer
                .install(latest)
                .assured("latest owner revision installs");
        }
        let read = ReplicatedMaterializedRelayState::read(&replica);
        assert_eq!(read.current_lsm(), 4);
        assert_eq!(
            read.record(&alpha)
                .assured("alpha remains")
                .row
                .value_at(0)
                .assured("value loads"),
            Some(RuntimeValue::I64(4))
        );
        assert_eq!(
            read.record(&beta)
                .assured("beta remains")
                .row
                .value_at(0)
                .assured("value loads"),
            Some(RuntimeValue::I64(2))
        );
    }

    #[test]
    fn replica_synchronization_preserves_a_restored_materialized_revision() {
        replica_race_preserves_the_restored_revision();
    }

    #[test]
    fn snapshot_installation_keeps_the_newest_revision_of_one_branch_generation() {
        let executor = Executor::new(nervix_execution::ExecutionConfig::default())
            .assured("valid executor budgets");
        let first = test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]);
        let schema = first.arrow_schema();
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            test_placement("tenant_state"),
            schema.clone(),
        ));
        let local = ClusterNodeName::try_from("node-2".to_string())
            .assured("the test node satisfies the node grammar");
        let mut assignment = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::new(
                Some(
                    ClusterNodeName::try_from("node-1".to_string())
                        .assured("the test node satisfies the node grammar"),
                ),
                vec![local.clone()],
                0,
            ),
            Some(&local),
        );
        let installer = assignment
            .installer
            .take()
            .assured("the local node is a replica");
        let earlier = RestoredMaterializedSnapshot::from_captured_generation(
            &executor,
            MaterializedGeneration::new(
                1,
                1,
                1,
                schema.clone(),
                vec![MaterializedGenerationRecord {
                    branch: None,
                    row: first,
                }],
            ),
        );
        let generation = |revision: u64, fence: u64, branch_generation: u64| {
            RestoredMaterializedSnapshot::from_captured_generation(
                &executor,
                MaterializedGeneration::new(
                    revision,
                    fence,
                    branch_generation,
                    schema.clone(),
                    vec![MaterializedGenerationRecord {
                        branch: None,
                        row: test_runtime_row([("value".to_string(), RuntimeValue::I64(2))]),
                    }],
                ),
            )
        };
        installer
            .install(generation(2, 3, 3))
            .assured("a newer snapshot is admissible");

        let refusal = installer
            .install(earlier)
            .expect_err("an earlier revision must not replace newer rows");
        assert!(
            matches!(
                refusal.current_context(),
                RuntimeStateOperationError::MaterializedSnapshotRevision {
                    received: 1,
                    current: 2
                }
            ),
            "{refusal:?}"
        );
        let refusal = installer
            .install(generation(4, 3, 2))
            .expect_err("a snapshot from an earlier branch lifecycle is refused");
        assert!(
            matches!(
                refusal.current_context(),
                RuntimeStateOperationError::MaterializedSnapshotBranchGeneration {
                    received: 2,
                    current: 3
                }
            ),
            "{refusal:?}"
        );
        let refusal = installer
            .install(generation(4, 2, 3))
            .expect_err("a snapshot sealed under a superseded fence is refused");
        assert!(
            matches!(
                refusal.current_context(),
                RuntimeStateOperationError::MaterializedSnapshotFence {
                    received: 2,
                    current: 3
                }
            ),
            "{refusal:?}"
        );
        assert_eq!(
            installer
                .read()
                .record(&None)
                .assured("the current row exists")
                .row
                .value_at(0)
                .assured("the current column is valid"),
            Some(RuntimeValue::I64(2))
        );
    }

    #[nervix_primitives::test]
    async fn unbranched_materialized_state_snapshot_restores_entries() {
        let executor = Executor::new(nervix_execution::ExecutionConfig::default())
            .assured("the default execution configuration is internally consistent");
        let placement = test_placement("notifications");
        let record = test_runtime_row([(
            "value".to_string(),
            RuntimeValue::String("ready".to_string()),
        )]);
        let schema = record.arrow_schema();
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement.clone(),
            schema.clone(),
        ));
        let mut assignment = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::owned_by(None),
            None,
        );
        let mut originator = assignment
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");

        originator
            .update_last_by_timestamp(&None, record)
            .assured("the assignment remains authoritative")
            .assured("the first record should update state");
        let sealed = originator
            .read()
            .seal_after(&executor, &Runtime::new().inner.snapshot_staging, None)
            .await
            .assured("unbranched materialized state should seal")
            .assured("a sealed generation should exist");

        let restored = RestoredMaterializedSnapshot::open_relay(
            &executor,
            &schema,
            SealedSource::artifact(sealed.artifact)
                .await
                .assured("the artifact opens"),
        )
        .await
        .assured("the sealed generation should open");
        let restored = Arc::new(ReplicatedMaterializedRelayState::restored(
            placement,
            schema,
            Some(restored),
        ));
        let read = ReplicatedMaterializedRelayState::read(&restored);

        assert_eq!(
            read.record(&None)
                .assured("restored record should exist")
                .row
                .value_at(0)
                .assured("restored field should load"),
            Some(RuntimeValue::String("ready".to_string()))
        );
    }

    #[nervix_primitives::test]
    async fn materialized_state_reads_selected_arrow_columns_by_index() {
        let record = test_runtime_row([
            (
                "status".to_string(),
                RuntimeValue::String("ready".to_string()),
            ),
            ("score".to_string(), RuntimeValue::I64(42)),
        ]);
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            test_placement("profiles"),
            record.arrow_schema(),
        ));
        let mut assignment = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::owned_by(None),
            None,
        );
        let mut originator = assignment
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");
        assert!(
            originator
                .update_last_by_timestamp(&None, record)
                .assured("the assignment remains authoritative")
                .is_some()
        );

        let row = originator
            .read()
            .record(&None)
            .assured("the materialized record should exist")
            .row;
        assert_eq!(
            [
                row.value_at(1).assured("the score column should load"),
                row.value_at(0).assured("the status column should load"),
            ],
            [
                Some(RuntimeValue::I64(42)),
                Some(RuntimeValue::String("ready".to_string())),
            ]
        );
    }

    #[nervix_primitives::test]
    async fn a_sealed_generation_matches_the_revision_it_was_captured_at() {
        let executor = Executor::new(nervix_execution::ExecutionConfig::default())
            .assured("the default execution configuration is internally consistent");
        let placement = test_placement("tenant_state");
        let first = test_runtime_row([(
            "value".to_string(),
            RuntimeValue::String("first".to_string()),
        )]);
        let schema = first.arrow_schema();
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement.clone(),
            schema.clone(),
        ));
        let mut assignment = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::owned_by(None),
            None,
        );
        let mut originator = assignment
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");
        let branch = BranchKey::from_fields([(
            nervix_models::FieldName::try_from("tenant".to_string())
                .assured("the test branch field name satisfies the field grammar"),
            RuntimeValue::String("acme".to_string()),
        )])
        .assured("a one-field branch key is well formed");
        originator
            .update_last_by_timestamp(&Some(branch.clone()), first)
            .assured("the assignment remains authoritative")
            .assured("the first record should update state");

        let captured = originator.read().capture();
        // Everything the live state does after the capture is invisible to it.
        originator
            .remove_key(&Some(branch))
            .assured("the assignment remains authoritative")
            .assured("removing the only branch should advance the revision");

        assert_eq!(captured.records().len(), 1);
        let sealed = captured
            .seal(&executor, &Runtime::new().inner.snapshot_staging)
            .await
            .assured("a captured generation should seal");
        assert_eq!(sealed.descriptor.revision, captured.revision());
        assert!(sealed.descriptor.revision < originator.read().current_lsm());
    }

    #[nervix_primitives::test]
    async fn an_already_current_requester_is_answered_without_a_new_generation() {
        let executor = Executor::new(nervix_execution::ExecutionConfig::default())
            .assured("the default execution configuration is internally consistent");
        let placement = test_placement("tenant_state");
        let record = test_runtime_row([(
            "value".to_string(),
            RuntimeValue::String("ready".to_string()),
        )]);
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement,
            record.arrow_schema(),
        ));
        let mut assignment = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::owned_by(None),
            None,
        );
        let mut originator = assignment
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");
        let revision = originator
            .update_last_by_timestamp(&None, record)
            .assured("the assignment remains authoritative")
            .assured("the first record should update state");

        assert!(
            originator
                .read()
                .seal_after(
                    &executor,
                    &Runtime::new().inner.snapshot_staging,
                    Some(revision)
                )
                .await
                .assured("an already-current requester is answered")
                .is_none()
        );
    }

    #[nervix_primitives::test]
    async fn an_evicted_branch_is_not_resurrected_by_an_earlier_generation() {
        let executor = Executor::new(nervix_execution::ExecutionConfig::default())
            .assured("the default execution configuration is internally consistent");
        let placement = test_placement("tenant_state");
        let record = test_runtime_row([(
            "value".to_string(),
            RuntimeValue::String("ready".to_string()),
        )]);
        let schema = record.arrow_schema();
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement.clone(),
            schema.clone(),
        ));
        let mut owner = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::owned_by(None),
            None,
        );
        let mut originator = owner
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");
        let branch = BranchKey::from_fields([(
            nervix_models::FieldName::try_from("tenant".to_string())
                .assured("the test branch field name satisfies the field grammar"),
            RuntimeValue::String("acme".to_string()),
        )])
        .assured("a one-field branch key is well formed");
        originator
            .update_last_by_timestamp(&Some(branch.clone()), record)
            .assured("the assignment remains authoritative")
            .assured("the first record should update state");
        let earlier = originator
            .read()
            .seal_after(&executor, &Runtime::new().inner.snapshot_staging, None)
            .await
            .assured("the owner should seal")
            .assured("a sealed generation should exist");
        originator
            .remove_key(&Some(branch))
            .assured("the assignment remains authoritative")
            .assured("eviction should advance the revision");

        // A replica that already followed the eviction refuses the generation from before it.
        let replica_state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement.clone(),
            schema.clone(),
        ));
        replica_state.branch_generation.store(2, Ordering::SeqCst);
        let local = ClusterNodeName::try_from("node-2".to_string())
            .assured("the test node name satisfies the node grammar");
        let mut replica = ReplicatedMaterializedRelayState::bind(
            &replica_state,
            StateReplicationRoles::new(
                Some(
                    ClusterNodeName::try_from("node-1".to_string())
                        .assured("the test node name satisfies the node grammar"),
                ),
                vec![local.clone()],
                0,
            ),
            Some(&local),
        );
        let installer = replica
            .installer
            .take()
            .assured("a replica may install snapshots");
        let restored = RestoredMaterializedSnapshot::open_relay(
            &executor,
            &schema,
            SealedSource::artifact(earlier.artifact)
                .await
                .assured("the artifact opens"),
        )
        .await
        .assured("the sealed generation should open");

        assert!(installer.install(restored).is_err());
    }

    #[cfg(feature = "shuttle")]
    mod shuttle_checks {
        use nervix_model_harness::shuttle::check_interleavings;
        use nervix_primitives::{sync::blocking::mpsc, thread};

        use super::*;

        /// An originator replaces a branch's record while another thread holds the assignment
        /// barrier, which that thread releases only after the update has returned.
        fn originator_update_under_a_held_barrier() {
            let record = test_runtime_row([(
                "value".to_string(),
                RuntimeValue::String("ready".to_string()),
            )]);
            let state = Arc::new(ReplicatedMaterializedRelayState::new(
                test_placement("tenant_state"),
                record.arrow_schema(),
            ));
            let mut assignment = ReplicatedMaterializedRelayState::bind(
                &state,
                StateReplicationRoles::owned_by(None),
                None,
            );
            let mut originator = assignment
                .originator
                .take()
                .assured("branch-local state is authoritative in this process");
            // A branch's first record adds the branch itself; the update after it only replaces
            // that branch's record.
            originator
                .update_last_by_timestamp(&None, record.clone())
                .assured("the assignment remains authoritative");
            let (held_tx, held_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel::<()>();
            let barrier = thread::spawn({
                let state = state.clone();
                move || {
                    state.assignment.serialize(|| {
                        held_tx
                            .send(())
                            .assured("the model keeps the receiver until the barrier is held");
                        release_rx
                            .recv()
                            .assured("the model releases the barrier once the update returned");
                    });
                }
            });
            held_rx
                .recv()
                .assured("the barrier thread reports once it holds the barrier");

            // The barrier stays held until the update returns, so an update that waited for it
            // would leave every thread blocked, which Shuttle reports as a deadlock.
            let updated = originator.update_last_by_timestamp(
                &None,
                record.with_ingested_at_watermarks(nervix_models::Timestamp::from_unix_nanos(1)),
            );
            release_tx
                .send(())
                .assured("the barrier thread waits for its release");
            barrier.join().assured(
                "Shuttle fails the whole execution when a model thread panics, so no join \
                 observes one",
            );

            assert!(
                matches!(updated, Ok(Some(2))),
                "the originator update was refused while the assignment barrier was held"
            );
        }

        /// A capture holds the assignment barrier while it reads every record. An originator update
        /// that queued behind that barrier would stall the relay-state task for the whole capture.
        #[test]
        fn shuttle_an_originator_update_proceeds_while_the_assignment_barrier_is_held() {
            check_interleavings(originator_update_under_a_held_barrier);
        }

        #[test]
        fn shuttle_replica_synchronization_preserves_the_restored_materialized_revision() {
            check_interleavings(replica_race_preserves_the_restored_revision);
        }

        #[test]
        fn shuttle_materialized_capture_names_exactly_its_branch_generation() {
            check_interleavings(|| {
                let row = timestamped_row(1);
                let state = Arc::new(ReplicatedMaterializedRelayState::new(
                    test_placement("state"),
                    row.arrow_schema(),
                ));
                let mut originator = ReplicatedMaterializedRelayState::bind(
                    &state,
                    StateReplicationRoles::owned_by(None),
                    None,
                )
                .originator
                .assured("owner originates");
                let alpha = super::super::super::string_branch_key("tenant", "alpha");
                let beta = super::super::super::string_branch_key("tenant", "beta");
                originator
                    .update_last_by_timestamp(&alpha, row)
                    .assured("alpha starts");
                originator
                    .update_last_by_timestamp(&beta, timestamped_row(2))
                    .assured("beta starts");
                let writer = thread::spawn({
                    let alpha = alpha.clone();
                    let beta = beta.clone();
                    move || {
                        originator
                            .update_last_by_timestamp(&alpha, timestamped_row(3))
                            .assured("existing branch updates during capture");
                        originator
                            .remove_key(&beta)
                            .assured("beta leaves its lifecycle");
                    }
                });
                let generation = ReplicatedMaterializedRelayState::read(&state).capture();
                let branches = generation
                    .records()
                    .iter()
                    .map(|record| record.branch.clone())
                    .collect::<Vec<_>>();
                assert_eq!(
                    branches,
                    match generation.branch_generation() {
                        2 => vec![alpha.clone(), beta],
                        3 => vec![alpha],
                        other => panic!("unexpected branch generation {other}"),
                    }
                );
                assert!(generation.revision() >= 2);
                writer.join().assured("writer finishes");
            });
        }
    }
}
