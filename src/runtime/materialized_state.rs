use std::sync::{
    Arc as StdArc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use ahash::RandomState;
use dashmap::DashMap;
use error_stack::Report;
use nervix_execution::Executor;
use nervix_models::ClusterNodeName;
use triomphe::Arc;

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

#[derive(Debug)]
pub(super) struct ReplicatedMaterializedRelayState {
    placement: RuntimeStatePlacement,
    schema: StdArc<arrow_schema::Schema>,
    assignment: StateAssignmentAuthority,
    entries: DashMap<Option<BranchKey>, RuntimeRow, RandomState>,
    /// Advances whenever a branch appears in or leaves this state. A capture records it, so a
    /// snapshot sealed before an eviction cannot resurrect the branch that eviction dropped.
    branch_generation: AtomicU64,
    /// The highest ownership fence whose snapshot has been installed here. A snapshot sealed under
    /// an assignment the owner has already left behind arrives stale, and installing it would undo
    /// what the current assignment published.
    installed_fence: AtomicU64,
    current_lsm: LsmSequence,
    last_persisted_lsm: AtomicU64,
    dirty: AtomicBool,
    /// The most recent generation sealed here, retained so that a requester already holding this
    /// revision, or a second requester arriving for it, is answered without scanning or encoding
    /// the state again. Exactly one generation is retained, so no build pins unbounded history;
    /// a reader that took a copy keeps its own charge until it releases it.
    sealed: parking_lot::Mutex<Option<SealedMaterializedSnapshot>>,
    /// Admits one snapshot build per placement. Requesters that arrive while a build is running
    /// wait for its result instead of starting a second scan of the same state.
    build: tokio::sync::Mutex<()>,
}

/// Read-only access to materialized records and snapshots.
#[derive(Debug, Clone)]
pub struct MaterializedRelayStateRead {
    state: Arc<ReplicatedMaterializedRelayState>,
}

/// Authoritative materialization access for one concrete assignment generation.
#[derive(Debug, Clone)]
pub struct MaterializedRelayStateOriginator {
    read: MaterializedRelayStateRead,
    assignment: StateAssignmentToken,
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
    pub(crate) branch: String,
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
        for record in restored.records {
            state.entries.insert(record.branch, record.row);
        }
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
            entries: DashMap::default(),
            branch_generation: AtomicU64::new(0),
            installed_fence: AtomicU64::new(0),
            current_lsm: LsmSequence::restored(0),
            last_persisted_lsm: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            sealed: parking_lot::Mutex::new(None),
            build: tokio::sync::Mutex::new(()),
        }
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
        MaterializedRelayStateAssignment {
            originator: binding
                .token_for(StateCapability::Originate)
                .map(|assignment| MaterializedRelayStateOriginator {
                    read: read.clone(),
                    assignment,
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

    pub(super) fn schema(&self) -> &StdArc<arrow_schema::Schema> {
        &self.state.schema
    }

    pub(super) fn current_lsm(&self) -> u64 {
        self.state.current_lsm.current()
    }

    pub(super) fn primary_node(&self) -> Option<ClusterNodeName> {
        self.state.assignment.roles().primary_node
    }

    /// Take one immutable generation of this state: its records, the revision they stand at, the
    /// assignment that owns them, and the branch lifecycle they belong to, all read together.
    ///
    /// The barrier is held only for the clone of the row views, which share the carrier columns
    /// rather than copying them. Encoding happens afterwards, against a value no later update can
    /// change, so updates and deletions proceed while a snapshot is being written out.
    pub(super) fn capture(&self) -> MaterializedGeneration {
        self.state.assignment.serialize_with(|binding| {
            let mut records = self
                .state
                .entries
                .iter()
                .map(|entry| MaterializedGenerationRecord {
                    branch: entry.key().clone(),
                    row: entry.value().clone(),
                })
                .collect::<Vec<_>>();
            records.sort_by(|left, right| {
                super::branch_key_display(&left.branch)
                    .cmp(super::branch_key_display(&right.branch))
            });
            MaterializedGeneration::new(
                self.state.current_lsm.current(),
                binding.fence(),
                self.state.branch_generation.load(Ordering::SeqCst),
                self.state.placement.schema_fingerprint,
                self.state.schema.clone(),
                records,
            )
        })
    }

    /// Seal a generation of this state that is newer than `after_revision`, or report that the
    /// requester already holds the current one.
    ///
    /// One build runs per placement: a requester arriving while another build is in flight waits
    /// for it and takes its result when it covers the same revision. A requester that is already
    /// current causes no scan of the entries and no encoding at all.
    pub(super) async fn seal_after(
        &self,
        executor: &Executor,
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
        let sealed = generation.seal(executor).await?;
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
    fn usable_sealed(&self, after_revision: Option<u64>) -> Option<UsableSealedSnapshot> {
        let sealed = self.state.sealed.lock().clone()?;
        if sealed.descriptor.revision != self.state.current_lsm.current() {
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
    pub(super) fn records(&self) -> Vec<MaterializedGenerationRecord> {
        self.capture().records().to_vec()
    }

    /// The record of exactly one branch. An absent key names the unbranched record, never every
    /// branch; a branch without a record simply has none.
    pub(super) fn record(&self, key: &Option<BranchKey>) -> Option<MaterializedGenerationRecord> {
        self.state
            .entries
            .get(key)
            .map(|row| MaterializedGenerationRecord {
                branch: key.clone(),
                row: row.clone(),
            })
    }

    pub(super) fn restored_branch_watermarks(
        &self,
    ) -> Vec<(Option<BranchKey>, nervix_models::Timestamp)> {
        self.state
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.key().clone(),
                    entry.value().metadata().ingested_at_high_watermark(),
                )
            })
            .collect()
    }
}

/// Whether the retained generation answers a requester, and with what. An answer of `None` inside
/// means the requester is already current: no scan, no encoding, nothing to send.
struct UsableSealedSnapshot {
    sealed: Option<SealedMaterializedSnapshot>,
}

impl MaterializedRelayStateOriginator {
    pub(super) fn read(&self) -> &MaterializedRelayStateRead {
        &self.read
    }

    pub(super) fn update_last_by_timestamp(
        &self,
        key: &Option<BranchKey>,
        record: &RuntimeRow,
    ) -> Result<Option<u64>, Report<super::StateAuthorityError>> {
        self.read
            .state
            .assignment
            .authorize(self.assignment, StateCapability::Originate, || {
                let existing =
                    self.read.state.entries.get(key).map(|existing| {
                        record.metadata().is_newer_than(existing.value().metadata())
                    });
                match existing {
                    Some(false) => return None,
                    Some(true) => {}
                    None => {
                        self.read.state.advance_branch_generation();
                    }
                }
                self.read.state.entries.insert(key.clone(), record.clone());
                Some(self.read.state.advance_revision())
            })
    }

    pub(super) fn remove_key(
        &self,
        key: &Option<BranchKey>,
    ) -> Result<Option<u64>, Report<super::StateAuthorityError>> {
        self.read
            .state
            .assignment
            .authorize(self.assignment, StateCapability::Originate, || {
                self.read.state.entries.remove(key)?;
                self.read.state.advance_branch_generation();
                Some(self.read.state.advance_revision())
            })
    }
}

impl ReplicatedMaterializedRelayState {
    fn advance_revision(&self) -> u64 {
        let lsm = self.current_lsm.advance();
        self.dirty.store(true, Ordering::SeqCst);
        lsm
    }

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
    pub(super) fn read(&self) -> &MaterializedRelayStateRead {
        &self.read
    }

    /// Replace this state with a restored snapshot, or refuse it.
    ///
    /// The decoding already happened; this is the publication step and it is atomic. A snapshot
    /// from an older branch lifecycle is refused rather than installed, so an evicted branch is
    /// never resurrected by a generation captured before it left.
    pub(super) fn install(
        &self,
        restored: RestoredMaterializedSnapshot,
    ) -> Result<(), RuntimeStateOperationError> {
        self.read.state.assignment.authorize(
            self.assignment,
            StateCapability::InstallSnapshot,
            || {
                let installed_branch_generation =
                    self.read.state.branch_generation.load(Ordering::SeqCst);
                if restored.branch_generation < installed_branch_generation {
                    return Err(RuntimeStateOperationError::Checkpoint(format!(
                        "refused a materialized relay snapshot from branch generation {} while \
                         branch generation {installed_branch_generation} is installed",
                        restored.branch_generation,
                    )));
                }
                let installed_fence = self.read.state.installed_fence.load(Ordering::SeqCst);
                if restored.fence < installed_fence {
                    return Err(RuntimeStateOperationError::Checkpoint(format!(
                        "refused a materialized relay snapshot sealed under ownership fence {} \
                         while fence {installed_fence} is installed",
                        restored.fence,
                    )));
                }
                self.read.state.entries.clear();
                for record in restored.records {
                    self.read.state.entries.insert(record.branch, record.row);
                }
                self.read
                    .state
                    .branch_generation
                    .fetch_max(restored.branch_generation, Ordering::SeqCst);
                self.read
                    .state
                    .installed_fence
                    .fetch_max(restored.fence, Ordering::SeqCst);
                self.read.state.current_lsm.adopt(restored.revision);
                self.read.state.dirty.store(true, Ordering::SeqCst);
                *self.read.state.sealed.lock() = None;
                Ok(())
            },
        )??;
        Ok(())
    }
}

impl MaterializedRelayStatePersistence {
    pub(super) fn read(&self) -> &MaterializedRelayStateRead {
        &self.read
    }

    pub(super) fn take_dirty(&self) -> bool {
        self.read.state.dirty.swap(false, Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn is_dirty(&self) -> bool {
        self.read.state.dirty.load(Ordering::SeqCst)
    }

    pub(super) fn restore_dirty(&self) {
        self.read.state.dirty.store(true, Ordering::SeqCst);
    }

    pub(super) fn last_persisted_lsm(&self) -> u64 {
        self.read.state.last_persisted_lsm.load(Ordering::SeqCst)
    }

    pub(super) fn record_persisted(&self, lsm: u64) {
        self.read.state.assignment.serialize(|| {
            self.read
                .state
                .last_persisted_lsm
                .fetch_max(lsm, Ordering::SeqCst);
            if self.read.state.current_lsm.current() <= lsm {
                self.read.state.dirty.store(false, Ordering::SeqCst);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{DomainName, ModelKind, ModelName, RelayName};

    use super::{super::materialized_snapshot::SealedSource, *};
    use crate::runtime_schema::{RuntimeValue, test_runtime_row};

    fn test_placement(relay: &str) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("default")
                .assured("the test domain name satisfies the domain grammar"),
            state: super::super::RuntimeStateKind::MaterializedRelay,
            kind: ModelKind::Relay,
            identifier: ModelName::from(
                &RelayName::parse(relay).assured("the test relay name satisfies the relay grammar"),
            ),
            schema_fingerprint: [0; 32],
            branch_key: None,
        }
    }

    #[tokio::test]
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
        let originator = assignment
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");

        originator
            .update_last_by_timestamp(&None, &record)
            .assured("the assignment remains authoritative")
            .assured("the first record should update state");
        let sealed = originator
            .read()
            .seal_after(&executor, None)
            .await
            .assured("unbranched materialized state should seal")
            .assured("a sealed generation should exist");

        let restored = RestoredMaterializedSnapshot::open(
            &executor,
            &schema,
            placement.schema_fingerprint,
            SealedSource::memory(sealed.bytes),
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

    #[tokio::test]
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
        let originator = assignment
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");
        assert!(
            originator
                .update_last_by_timestamp(&None, &record)
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

    #[tokio::test]
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
        let originator = assignment
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
            .update_last_by_timestamp(&Some(branch.clone()), &first)
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
            .seal(&executor)
            .await
            .assured("a captured generation should seal");
        assert_eq!(sealed.descriptor.revision, captured.revision());
        assert!(sealed.descriptor.revision < originator.read().current_lsm());
    }

    #[tokio::test]
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
        let originator = assignment
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");
        let revision = originator
            .update_last_by_timestamp(&None, &record)
            .assured("the assignment remains authoritative")
            .assured("the first record should update state");

        assert!(
            originator
                .read()
                .seal_after(&executor, Some(revision))
                .await
                .assured("an already-current requester is answered")
                .is_none()
        );
    }

    #[tokio::test]
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
        let originator = owner
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
            .update_last_by_timestamp(&Some(branch.clone()), &record)
            .assured("the assignment remains authoritative")
            .assured("the first record should update state");
        let earlier = originator
            .read()
            .seal_after(&executor, None)
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
        let restored = RestoredMaterializedSnapshot::open(
            &executor,
            &schema,
            placement.schema_fingerprint,
            SealedSource::memory(earlier.bytes),
        )
        .await
        .assured("the sealed generation should open");

        assert!(installer.install(restored).is_err());
    }
}
