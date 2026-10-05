//! Branch-local runtime-state replication and ownership handoff.
//!
//! Layer: data plane.
//! - **Owns.** In-memory checkpoints, transfer activation and branch-state restoration.
//! - **Depends on.** Typed runtime snapshots, interconnect transfer and node schedules.
//! - **Must not know.** NSPL parsing, graph validation or external connector configuration.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "state assignment and recovery install a placement generation; recurring frame \
                  and catch-up operations override this default"
    )
)]

use std::num::NonZeroUsize;

use error_stack::ResultExt as _;
use nervix_interconnect::BranchCheckpointCursor;

use super::{
    branch_checkpoint_catalog::BranchCheckpointCatalog,
    branch_lifecycle_state::AnnouncedCheckpoint, *,
};

pub(super) const DEFAULT_STATE_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);
pub(super) const DEFAULT_STATE_REPLICATION_POLL_INTERVAL: Duration = Duration::from_secs(1);
const STATE_CHECKPOINT_ANNOUNCEMENT_RETRY_INTERVAL: Duration = Duration::from_millis(100);
/// How many changes one page of an owner's branch checkpoint catalog lists. A change is a branch key
/// and a revision, so a page stays far below the replication class's message limit.
const BRANCH_CHECKPOINT_LISTING_PAGE: NonZeroUsize = nonzero_ext::nonzero!(256_usize);
mod error;
pub(crate) use self::error::{AwaitedReplicas, StateReplicationError};

#[derive(Debug)]
pub(crate) struct StateSyncAck {
    pub(crate) placement: RuntimeStatePlacement,
    pub(crate) lsm: u64,
}
mod handoff;
mod preparation;
mod published_branch_state;

pub(in crate::runtime) use checkpoint_announcement::CheckpointAnnouncementTasks;
pub(in crate::runtime) use checkpoint_listing::OwnerCheckpointListing;
use handoff::OwnershipHandoffTransitionRef;
pub(in crate::runtime) use handoff::{
    ActivatedRuntimeStateHandoff, OwnershipHandoffActivation,
    OwnershipHandoffActivationAuthorization, PreparedRuntimeStateHandoff,
};
use preparation::{ForcedRecoveryCheckpoint, RuntimeStatePreparationIdentity};
pub(in crate::runtime) use preparation::{
    PreparedForcedRuntimeStateRecovery, PreparedRuntimeStateSnapshot,
};
pub(in crate::runtime) use published_branch_state::PublishedBranchState;
use replica_branch_checkpoints::{BranchStep, Held, ReplicaBranchCheckpoints, StepOutcome};
use replica_catch_up::RemoteStateOwner;
use routing::ReplicatedState;
pub(crate) use routing::StateReplicationRequest;

impl Runtime {
    /// Persist the branch lifecycle an owner formed, hold it as the lifecycle replicas
    /// synchronize, and offer it to them.
    pub(super) fn persist_branch_lru_snapshot(
        &self,
        placement: RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let lsm = snapshot.lsm;
        if let Some(store) = self.inner.state_store.as_ref() {
            store.persist_latest_snapshot(&placement, lsm, &snapshot.payload)?;
        }
        let lifecycle = self.replicated_branch_lifecycle(&placement);
        lifecycle.publish(snapshot);
        self.announce_checkpoint(&placement, lifecycle.replication(), lsm);
        Ok(())
    }

    /// Offer a branch lifecycle to this node's replicas at once, without writing it to storage:
    /// the periodic lifecycle snapshot persists it.
    pub(super) fn publish_branch_lru_snapshot(
        &self,
        placement: RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
    ) {
        let lsm = snapshot.lsm;
        let lifecycle = self.replicated_branch_lifecycle(&placement);
        lifecycle.publish(snapshot);
        self.announce_checkpoint(&placement, lifecycle.replication(), lsm);
    }

    pub(super) fn take_restorable_branch_lru_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Result<Option<PersistedRuntimeStateEntry>, Report<RuntimePersistenceError>> {
        if let Some(snapshot) = nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "one placement installation consumes the snapshot transferred for its concrete state \
             lifetime",
            self.take_transferred_runtime_state_snapshot(placement)
        ) {
            return Ok(Some(snapshot));
        }
        if let Some(held) = self.held_branch_lifecycle(placement) {
            return Ok(Some(held.snapshot().clone()));
        }
        self.stored_runtime_state_snapshot(placement)
    }

    /// The branch lifecycle this node holds for `placement`, created empty the first time an owner
    /// publishes one, catalogues a branch state of the entity, or a replica task starts keeping
    /// the entity current.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "an owner installs the entity's lifecycle handle when it first publishes a \
                      lifecycle or creates a branch state, and a replica task when it starts"
        )
    )]
    pub(super) fn replicated_branch_lifecycle(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Arc<ReplicatedBranchLifecycle> {
        if let Some(lifecycle) = self.inner.replicated_branch_lifecycles.get(placement) {
            return lifecycle.clone();
        }
        let assignment = self.state_assignment(&placement.entity());
        let lifecycle = self
            .inner
            .replicated_branch_lifecycles
            .entry(placement.clone())
            .or_insert_with(|| Arc::new(ReplicatedBranchLifecycle::assigned(assignment)))
            .clone();
        self.publish_state_replication_route(
            placement,
            ReplicatedState::BranchLru(lifecycle.clone()),
        );
        lifecycle
    }

    /// The branch lifecycle this node holds for `placement`, when it holds one. Nothing is
    /// created.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "replica frames, synchronization requests and catalog listing requests \
                      resolve the entity's lifecycle handle"
        )
    )]
    pub(super) fn branch_lifecycle(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<Arc<ReplicatedBranchLifecycle>> {
        let route = self.inner.state_replication_routing.resolve(placement)?;
        let state = route.state()?;
        let ReplicatedState::BranchLru(lifecycle) = state.as_ref() else {
            return None;
        };
        Some(lifecycle.clone())
    }

    /// The newest branch lifecycle checkpoint this node holds in memory for `placement`.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    fn held_branch_lifecycle(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<StdArc<BranchLifecycleCheckpoint>> {
        self.branch_lifecycle(placement)?.latest()
    }

    /// The catalog the branch state `placement` places records the revisions it publishes in: the
    /// catalog of its entity's branch lifecycle.
    fn branch_checkpoint_catalog(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> BranchCheckpointCatalog {
        let lifecycle = placement
            .branch_lifecycle()
            .assured("only the state of one branch of a branch-keyed entity is catalogued");
        self.replicated_branch_lifecycle(&lifecycle)
            .catalog()
            .clone()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    fn state_replica_assignment_is_current(
        &self,
        placement: &RuntimeStatePlacement,
        source: &ClusterNodeName,
    ) -> bool {
        let Some(slot) = self.inner.state_replication_routing.assignment(placement) else {
            return false;
        };
        let assignment = slot.load();
        let Some(assignment) = assignment.as_deref() else {
            return false;
        };
        if !assignment.names(placement) {
            return false;
        }
        let dispatcher = self.inner.remote_dispatcher.load();
        let Some(dispatcher) = dispatcher.as_deref() else {
            return false;
        };
        assignment.replicates_from(dispatcher.local_node_id(), source)
    }

    /// What this replica holds of the branch state `placement` places: its copy in memory, or else
    /// what its storage holds. A replica task reads this once for each branch it looks at and
    /// keeps it current itself from then on.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a replica task reads what this node holds of a branch once, when it first \
                      looks at the branch, and keeps that record itself from then on"
        )
    )]
    fn held_branch_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        lifecycle: &ReplicatedBranchLifecycle,
    ) -> Result<Held, Report<RuntimePersistenceError>> {
        if let Some(snapshot) = lifecycle.passive_checkpoint(placement) {
            return Ok(Held::Revision(snapshot.lsm));
        }
        self.stored_replica_checkpoint(placement)
    }

    /// What this node's storage holds of `placement`.
    fn stored_replica_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Result<Held, Report<RuntimePersistenceError>> {
        let Some(store) = self.inner.state_store.as_ref() else {
            return Ok(Held::Nothing);
        };
        let Some(snapshot) = store.latest_snapshot(placement)? else {
            return Ok(Held::Nothing);
        };
        Ok(Held::Revision(snapshot.lsm))
    }

    /// Hold in `lifecycle` the branch lifecycle this node's storage keeps for `placement`, when
    /// `lifecycle` holds none yet, as a replica task that starts keeping the entity current does.
    fn restore_replica_branch_lifecycle(
        &self,
        placement: &RuntimeStatePlacement,
        lifecycle: &ReplicatedBranchLifecycle,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        if lifecycle.latest().is_some() {
            return Ok(());
        }
        let Some(store) = self.inner.state_store.as_ref() else {
            return Ok(());
        };
        let Some(snapshot) = store.latest_snapshot(placement)? else {
            return Ok(());
        };
        lifecycle.install(StdArc::new(BranchLifecycleCheckpoint::new(snapshot)));
        Ok(())
    }

    /// Check the retained assignment again after storage work, before publishing a replica copy.
    fn require_replica_assignment(
        &self,
        owner: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        lifecycle: &ReplicatedBranchLifecycle,
    ) -> RuntimeStateResult<()> {
        let dispatcher = self.inner.remote_dispatcher.load();
        let current = match dispatcher.as_deref() {
            Some(dispatcher) => {
                lifecycle.replicates_from(placement, dispatcher.local_node_id(), owner)
            }
            None => false,
        };
        if !current {
            return Err(RuntimeStateOperationError::replication(
                "runtime state replica assignment changed during synchronization",
            ));
        }
        Ok(())
    }

    /// Refuse a checkpoint `owner` sent of `placement` unless this node still replicates the
    /// placement from that owner and the checkpoint is one of the placement.
    fn verify_replica_checkpoint(
        &self,
        owner: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        snapshot: &PersistedRuntimeStateEntry,
        lifecycle: &ReplicatedBranchLifecycle,
    ) -> RuntimeStateResult<()> {
        self.require_replica_assignment(owner, placement, lifecycle)?;
        if self
            .inner
            .fault_injection
            .state_replica_installation_fails()
        {
            return Err(RuntimeStateOperationError::replication(
                "runtime state replica installation failed",
            ));
        }
        self.validate_ownership_handoff_snapshot(placement, snapshot)
            .map_err(|error| RuntimeStateOperationError::replication(error.to_string()))
    }

    /// Install `snapshot`, the owner's checkpoint of the branch state `placement` places, as this
    /// replica's copy, when it is newer than `held`, what this replica holds of the branch, and
    /// `lifecycle`, the branch lifecycle this replica keeps for the entity, names the branch.
    /// Returns what this replica holds of the branch afterwards.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "a replica task installs the checkpoints it fetches in every catch-up round"
        )
    )]
    async fn install_replica_branch_checkpoint(
        &self,
        owner: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
        lifecycle: &ReplicatedBranchLifecycle,
        held: Held,
    ) -> RuntimeStateResult<Held> {
        self.verify_replica_checkpoint(owner, placement, &snapshot, lifecycle)?;
        if placement.branch_key.is_some() {
            let named = lifecycle
                .names(placement.branch_key.as_ref())
                .map_err(|error| RuntimeStateOperationError::replication(error.to_string()))?;
            if !named {
                return Err(RuntimeStateOperationError::replication(
                    "runtime state checkpoint belongs to an evicted branch",
                ));
            }
        }
        if let Held::Revision(held_lsm) = held
            && held_lsm >= snapshot.lsm
        {
            // This node already holds the checkpoint, or a newer one, and its earlier
            // acknowledgement may have been lost: the owner keeps announcing a checkpoint until
            // this node acknowledges it.
            self.acknowledge_durable_state_replica(owner, placement, held_lsm)
                .await?;
            return Ok(held);
        }
        let snapshot = match self.inner.state_store.as_ref() {
            Some(store) => {
                let installed = store
                    .persist_replica_snapshot_if_newer(placement, snapshot)
                    .await
                    .map_err(|error| {
                        RuntimeStateOperationError::persistence(error.current_context().clone())
                    })?;
                let Some(installed) = installed else {
                    // The storage already holds this revision or a newer one.
                    let stored = self.stored_replica_checkpoint(placement).map_err(|error| {
                        RuntimeStateOperationError::persistence(error.current_context().clone())
                    })?;
                    if let Held::Revision(stored_lsm) = stored {
                        self.acknowledge_durable_state_replica(owner, placement, stored_lsm)
                            .await?;
                    }
                    return Ok(stored);
                };
                installed
            }
            None => snapshot,
        };
        self.require_replica_assignment(owner, placement, lifecycle)?;
        let lsm = snapshot.lsm;
        lifecycle.hold_passive_checkpoint(placement, snapshot);
        if self.inner.state_store.is_some() {
            self.acknowledge_state_replica_install(owner, placement, lsm);
        }
        Ok(Held::Revision(lsm))
    }

    /// Install `snapshot`, the owner's branch lifecycle checkpoint of `placement`, into
    /// `lifecycle`, the lifecycle this replica keeps for the entity, when it is newer than the one
    /// held there.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "a replica task installs the checkpoints it fetches in every catch-up round"
        )
    )]
    async fn install_replica_branch_lifecycle(
        &self,
        owner: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
        lifecycle: &ReplicatedBranchLifecycle,
    ) -> RuntimeStateResult<()> {
        self.verify_replica_checkpoint(owner, placement, &snapshot, lifecycle)?;
        if let Some(held) = lifecycle.latest()
            && held.lsm() >= snapshot.lsm
        {
            return self
                .acknowledge_durable_state_replica(owner, placement, held.lsm())
                .await;
        }
        let snapshot = match self.inner.state_store.as_ref() {
            Some(store) => {
                let installed = store
                    .persist_replica_snapshot_if_newer(placement, snapshot)
                    .await
                    .map_err(|error| {
                        RuntimeStateOperationError::persistence(error.current_context().clone())
                    })?;
                let Some(installed) = installed else {
                    // The storage already holds this revision or a newer one.
                    let stored = store
                        .latest_snapshot(placement)
                        .map_err(RuntimeStateOperationError::persistence)?;
                    let Some(stored) = stored else {
                        return Ok(());
                    };
                    self.require_replica_assignment(owner, placement, lifecycle)?;
                    let stored_lsm = stored.lsm;
                    self.install_replica_branch_lru_snapshot(placement, lifecycle, stored)?;
                    return self
                        .acknowledge_durable_state_replica(owner, placement, stored_lsm)
                        .await;
                };
                installed
            }
            None => snapshot,
        };
        self.require_replica_assignment(owner, placement, lifecycle)?;
        let lsm = snapshot.lsm;
        self.install_replica_branch_lru_snapshot(placement, lifecycle, snapshot)?;
        if self.inner.state_store.is_some() {
            self.acknowledge_state_replica_install(owner, placement, lsm);
        }
        Ok(())
    }

    /// Acknowledge revision `lsm` of `placement` to `source`, once this node's stable storage is
    /// known to hold what it stored for the placement.
    ///
    /// An acknowledgement promises that the checkpoint survives this node, so a node without stable
    /// storage, which only unit tests construct, acknowledges nothing.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "assigned replica polling records durable progress and installs the selected \
                      revision"
        )
    )]
    async fn acknowledge_durable_state_replica(
        &self,
        source: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        lsm: u64,
    ) -> RuntimeStateResult<()> {
        let Some(store) = self.inner.state_store.as_ref() else {
            return Ok(());
        };
        store.synchronize().await.map_err(|error| {
            RuntimeStateOperationError::persistence(error.current_context().clone())
        })?;
        self.acknowledge_state_replica_install(source, placement, lsm);
        Ok(())
    }

    /// Hold `snapshot` as the branch lifecycle this replica keeps in `lifecycle` for the entity of
    /// `placement`, and drop the branch checkpoints this replica holds for branches the held
    /// lifecycle no longer names.
    ///
    /// The pruning follows the lifecycle held after the installation, which is newer than
    /// `snapshot` when a newer one was installed first: a branch that lifecycle does not name was
    /// evicted.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    fn install_replica_branch_lru_snapshot(
        &self,
        _placement: &RuntimeStatePlacement,
        lifecycle: &ReplicatedBranchLifecycle,
        snapshot: PersistedRuntimeStateEntry,
    ) -> RuntimeStateResult<()> {
        let checkpoint = StdArc::new(BranchLifecycleCheckpoint::new(snapshot));
        checkpoint
            .branches()
            .map_err(|error| RuntimeStateOperationError::replication(error.to_string()))?;
        let held = lifecycle.install(checkpoint);
        let branches = held
            .branches()
            .map_err(|error| RuntimeStateOperationError::replication(error.to_string()))?;
        lifecycle.prune_passive_checkpoints(branches);
        Ok(())
    }

    fn acknowledge_state_replica_install(
        &self,
        source: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        lsm: u64,
    ) {
        let Some(dispatcher) = self.inner.remote_dispatcher.load_full() else {
            return;
        };
        let source = source.clone();
        let placement = placement.to_remote();
        drop(nervix_primitives::task::spawn(async move {
            if let Err(error) = dispatcher
                .dispatch(
                    &source,
                    Envelope::Control(nervix_interconnect::ControlEnvelope::StateReplicationAck(
                        nervix_interconnect::StateReplicationAck { placement, lsm },
                    )),
                )
                .await
            {
                warn!(destination = %source, error = %error, "failed to acknowledge replicated runtime state checkpoint");
            }
        }));
    }

    /// Wait for what should make a replica task synchronize again: the owner announcing a newer
    /// checkpoint through `replication`, or the poll interval passing. Returns `false` once the
    /// task should stop.
    async fn wait_for_state_replica_sync_trigger(
        &self,
        shutdown_rx: &mut watch::Receiver<bool>,
        replication: &CheckpointReplication,
        poll_interval: Duration,
        initial_sync_pending: bool,
    ) -> bool {
        if initial_sync_pending && !self.inner.fault_injection.state_replica_polling_is_paused() {
            return true;
        }
        if self.inner.fault_injection.state_replica_polling_is_paused() {
            nervix_primitives::select! {
                changed = shutdown_rx.changed() => {
                    changed.is_ok() && !*shutdown_rx.borrow()
                }
                _ = replication.next_announcement() => true,
            }
        } else {
            nervix_primitives::select! {
                changed = shutdown_rx.changed() => {
                    changed.is_ok() && !*shutdown_rx.borrow()
                }
                _ = replication.next_announcement() => true,
                _ = sleep(poll_interval) => true,
            }
        }
    }

    pub(crate) async fn capture_ownership_handoff_state(
        &self,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        entity: &NodeRef,
        base_schedule_fingerprint: [u8; 32],
    ) -> OwnershipHandoffResult<Vec<nervix_interconnect::OwnershipHandoffCheckpoint>> {
        if !self.entity_gate_operation_owns_entity(
            coordination,
            domain,
            entity,
            EntityGatePurpose::OwnershipHandoff,
        ) {
            return Err(OwnershipHandoffError::participant(format!(
                "coordination identity '{coordination}' does not hold the ownership gate for {} \
                 '{}'",
                entity.kind.as_str(),
                entity.identifier.as_str()
            )));
        }
        self.capture_frozen_ownership_handoff_state(domain, entity, base_schedule_fingerprint)
            .await
    }

    async fn capture_frozen_ownership_handoff_state(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        base_schedule_fingerprint: [u8; 32],
    ) -> OwnershipHandoffResult<Vec<nervix_interconnect::OwnershipHandoffCheckpoint>> {
        let scheduled = self.ownership_handoff_scheduled_node(domain, entity)?;
        self.verify_local_handoff_schedule(domain, base_schedule_fingerprint)?;
        let source_is_passive = self
            .inner
            .executions
            .get(domain)
            .is_some_and(|execution| execution.passive_only);
        let matches_entity = |placement: &RuntimeStatePlacement| {
            placement.domain == *domain
                && placement.kind == entity.kind
                && placement.identifier == entity.identifier
        };
        let final_branch_lru = if entity.kind.is_processor() {
            let commands = {
                let execution = self.inner.executions.get(domain).ok_or_else(|| {
                    OwnershipHandoffError::checkpoint(format!(
                        "domain '{}' has no active execution while checkpointing {} '{}'",
                        domain.as_str(),
                        entity.kind.as_str(),
                        entity.identifier.as_str()
                    ))
                })?;
                let commands = execution
                    .node_tasks
                    .get(entity)
                    .map(|task| task.commands.clone());
                if commands.is_none() && !execution.passive_only {
                    return Err(OwnershipHandoffError::checkpoint(format!(
                        "{} '{}' has no local task while checkpointing ownership",
                        entity.kind.as_str(),
                        entity.identifier.as_str()
                    )));
                }
                commands
            };
            match commands {
                Some(commands) => Some(ScheduledNodeTask::checkpoint_via(&commands).await?),
                None => None,
            }
        } else {
            self.checkpoint_entrypoint_branch_lifecycle(domain, entity)
                .await?
        };
        let mut checkpoints = Vec::new();
        // Branch states leave their maps before they are encoded, so an encode never holds a map
        // shard that a branch appearing or leaving elsewhere has to write.
        let mut deduplicators = Vec::new();
        for state in self.inner.replicated_deduplicator_states.iter() {
            if matches_entity(state.key()) {
                deduplicators.push((state.key().clone(), state.value().clone()));
            }
        }
        for (placement, state) in deduplicators {
            let snapshot = state.latest_snapshot().map_err(|error| {
                OwnershipHandoffError::persistence(error.current_context().clone())
            })?;
            checkpoints.push((placement, snapshot));
        }
        for state in self.inner.replicated_kafka_offset_states.iter() {
            if matches_entity(state.key()) {
                checkpoints.push((
                    state.key().clone(),
                    ReplicatedKafkaOffsetState::read(state.value())
                        .latest_snapshot()
                        .map_err(OwnershipHandoffError::persistence)?,
                ));
            }
        }
        let materialized = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .filter(|state| matches_entity(state.key()))
            .map(|state| {
                (
                    state.key().clone(),
                    ReplicatedMaterializedRelayState::read(state.value()),
                )
            })
            .collect::<Vec<_>>();
        for (placement, state) in materialized {
            nervix_primitives::task::consume_budget().await;
            let sealed = state
                .seal_after(&self.inner.executor, &self.inner.snapshot_staging, None)
                .await
                .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?
                .ok_or_else(|| {
                    OwnershipHandoffError::checkpoint(
                        "the materialized relay state produced no checkpoint generation",
                    )
                })?;
            checkpoints.push((
                placement,
                sealed
                    .into_persisted_entry(&self.inner.executor)
                    .await
                    .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?,
            ));
        }
        let mut windows = Vec::new();
        for state in self.inner.replicated_window_processor_states.iter() {
            if matches_entity(state.key()) {
                windows.push((state.key().clone(), state.value().clone()));
            }
        }
        for (placement, state) in windows {
            nervix_primitives::task::consume_budget().await;
            let snapshot = state
                .latest_snapshot(&self.inner.executor)
                .await
                .map_err(|error| {
                    OwnershipHandoffError::persistence(error.current_context().clone())
                })?;
            checkpoints.push((placement, snapshot));
        }
        let mut wasm_processors = Vec::new();
        for state in self.inner.replicated_wasm_processor_states.iter() {
            if matches_entity(state.key()) {
                wasm_processors.push((state.key().clone(), state.value().clone()));
            }
        }
        for (placement, state) in wasm_processors {
            checkpoints.push((placement, state.latest_snapshot()));
        }
        for state in self.inner.replicated_branch_aggregated_states.iter() {
            if matches_entity(state.key()) {
                checkpoints.push((
                    state.key().clone(),
                    state
                        .latest_snapshot(&self.inner.metrics)
                        .map_err(OwnershipHandoffError::persistence)?,
                ));
            }
        }
        if Self::node_has_branch_lifecycle(entity.kind) {
            let branch_lru = self
                .state_placement(
                    domain,
                    RuntimeStateKind::BranchLru,
                    entity.kind,
                    entity.identifier.clone(),
                    None,
                )
                .change_context_lazy(|| OwnershipHandoffError::StatePlacement {
                    kind: entity.kind,
                    identifier: entity.identifier.clone(),
                })?;
            match final_branch_lru {
                Some(snapshot) => checkpoints.push((branch_lru.clone(), snapshot)),
                None => {
                    if let Some(held) = self.held_branch_lifecycle(&branch_lru) {
                        checkpoints.push((branch_lru.clone(), held.snapshot().clone()));
                    } else if let Some(store) = self.inner.state_store.as_ref()
                        && let Some(snapshot) = store
                            .latest_snapshot(&branch_lru)
                            .map_err(OwnershipHandoffError::persistence)?
                    {
                        checkpoints.push((branch_lru.clone(), snapshot));
                    }
                }
            }
            if source_is_passive
                && !checkpoints
                    .iter()
                    .any(|(placement, _)| placement == &branch_lru)
            {
                checkpoints.push((
                    branch_lru,
                    PersistedRuntimeStateEntry {
                        lsm: 0,
                        payload: encode_branch_lru_snapshot(&[]).map_err(|error| {
                            OwnershipHandoffError::checkpoint(error.to_string())
                        })?,
                    },
                ));
            }
        }
        let expected =
            self.expected_ownership_handoff_placements(domain, &scheduled, &checkpoints)?;
        checkpoints.retain(|(placement, _)| expected.contains(placement));
        let captured = checkpoints
            .iter()
            .map(|(placement, _)| placement.clone())
            .collect::<HashSet<_>>();
        for placement in expected.difference(&captured) {
            let stored = match self.inner.state_store.as_ref() {
                Some(store) => store
                    .latest_snapshot(placement)
                    .map_err(OwnershipHandoffError::persistence)?,
                None => None,
            };
            let snapshot = if let Some(snapshot) = stored {
                snapshot
            } else if source_is_passive {
                self.empty_ownership_handoff_snapshot(placement)?
            } else {
                return Err(OwnershipHandoffError::checkpoint(format!(
                    "final {:?} checkpoint is missing for {} '{}'",
                    placement.state,
                    entity.kind.as_str(),
                    entity.identifier.as_str()
                )));
            };
            checkpoints.push((placement.clone(), snapshot));
        }
        checkpoints.sort_by(|(left, _), (right, _)| {
            u8::from(left.state.kind())
                .cmp(&u8::from(right.state.kind()))
                .then_with(|| {
                    left.branch_key
                        .as_ref()
                        .map(BranchKey::as_str)
                        .cmp(&right.branch_key.as_ref().map(BranchKey::as_str))
                })
        });
        for (placement, snapshot) in &checkpoints {
            if placement.state.kind() == RuntimeStateKind::WasmProcessor {
                // A WASM branch's handoff checkpoint is its committed checkpoint, which already
                // reached stable storage and every replica before it was committed. Writing it again
                // could replace a newer checkpoint this node holds with an older revision.
                continue;
            }
            if placement.state.kind() == RuntimeStateKind::BranchLru {
                self.persist_branch_lru_snapshot(placement.clone(), snapshot.clone())
                    .map_err(|error| {
                        OwnershipHandoffError::persistence(error.current_context().clone())
                    })?;
            } else if let Some(store) = self.inner.state_store.as_ref() {
                store
                    .persist_latest_snapshot(placement, snapshot.lsm, &snapshot.payload)
                    .map_err(OwnershipHandoffError::persistence)?;
                self.announce_stored_checkpoint(placement, snapshot.lsm);
            }
        }
        Ok(checkpoints
            .into_iter()
            .map(
                |(placement, snapshot)| nervix_interconnect::OwnershipHandoffCheckpoint {
                    placement: placement.to_remote(),
                    snapshot: nervix_interconnect::StateSnapshotEnvelope {
                        lsm: snapshot.lsm,
                        payload: snapshot.payload,
                    },
                },
            )
            .collect())
    }

    fn empty_ownership_handoff_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
        let payload = match placement.state.kind() {
            RuntimeStateKind::BranchAggregated => {
                encode_branch_aggregated_snapshot(&BranchAggregatedRuntimeStateSnapshot {
                    metrics: RuntimeMetricsSnapshot::default(),
                })
                .map_err(OwnershipHandoffError::persistence)?
            }
            RuntimeStateKind::KafkaOffset => {
                let state = Arc::new(
                    ReplicatedKafkaOffsetState::new(placement.clone(), None)
                        .map_err(OwnershipHandoffError::persistence)?,
                );
                ReplicatedKafkaOffsetState::read(&state)
                    .latest_snapshot()
                    .map_err(OwnershipHandoffError::persistence)?
                    .payload
            }
            RuntimeStateKind::MaterializedRelay => empty_sealed_container()
                .map_err(|error| OwnershipHandoffError::state(error.to_string()))?,
            RuntimeStateKind::BranchLru => encode_branch_lru_snapshot(&[])
                .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?,
            RuntimeStateKind::Correlator
            | RuntimeStateKind::Deduplicator
            | RuntimeStateKind::WasmProcessor
            | RuntimeStateKind::WindowProcessor => {
                return Err(OwnershipHandoffError::state(format!(
                    "cannot synthesize empty branch-local {:?} state without a branch lifecycle \
                     checkpoint",
                    placement.state
                )));
            }
        };
        Ok(PersistedRuntimeStateEntry { lsm: 0, payload })
    }

    pub(crate) async fn prepare_forced_ownership_recovery(
        &self,
        request: nervix_interconnect::PrepareForcedOwnershipRecoveryRequest,
        deadline: Instant,
    ) -> OwnershipHandoffResult<nervix_interconnect::ForcedOwnershipRecoveryPreparation> {
        let operation_id = request.operation_id.clone();
        let source = &request.source;
        let destination = &request.destination;
        let destination_incarnation = request.destination_incarnation;
        let domain = &request.domain;
        let entity = &request.entity;
        let base_schedule_fingerprint = request.base_schedule_fingerprint;
        let target_schedule_fingerprint = request.target_schedule_fingerprint;
        self.verify_local_handoff_schedule(domain, base_schedule_fingerprint)?;
        let scheduled = self.ownership_handoff_scheduled_node(domain, entity)?;
        if scheduled.primary_node() != Some(source) {
            return Err(OwnershipHandoffError::participant(format!(
                "{} '{}' is no longer assigned to lost owner '{}'",
                entity.kind.as_str(),
                entity.identifier.as_str(),
                source
            )));
        }
        let dispatcher = self.inner.remote_dispatcher.load_full().ok_or_else(|| {
            OwnershipHandoffError::participant("local node identity is unavailable")
        })?;
        let local_node = dispatcher.local_node_id();
        if local_node != destination {
            return Err(OwnershipHandoffError::participant(format!(
                "forced ownership recovery targets node '{destination}' but reached '{local_node}'"
            )));
        }
        let recovery_sources = scheduled
            .assigned_nodes
            .iter()
            .filter(|node| *node != source && *node != destination)
            .cloned()
            .collect::<Vec<_>>();
        let mut checkpoints = Vec::new();
        let mut resets = BTreeMap::new();

        let branch_entries = if Self::node_has_branch_lifecycle(scheduled.kind()) {
            let placement = self
                .state_placement(
                    domain,
                    RuntimeStateKind::BranchLru,
                    scheduled.kind(),
                    scheduled.identifier.clone(),
                    None,
                )
                .change_context_lazy(|| OwnershipHandoffError::StatePlacement {
                    kind: scheduled.kind(),
                    identifier: scheduled.identifier.clone(),
                })?;
            let recovered = self
                .forced_recovery_checkpoint(&placement, &recovery_sources, deadline)
                .await;
            match recovered.snapshot {
                Some(snapshot) => {
                    let entries = decode_branch_lru_snapshot(&snapshot.payload)
                        .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
                    checkpoints.push((placement, snapshot));
                    Some(entries)
                }
                None => {
                    resets.insert(
                        OwnershipStateComponent::BranchLifecycle,
                        recovered.reset_cause,
                    );
                    if let Some(component) = Self::branch_state_component(scheduled.kind()) {
                        resets.insert(component, OwnershipStateResetCause::ExpiredBranchMetadata);
                    }
                    checkpoints.push((
                        placement.clone(),
                        self.empty_ownership_handoff_snapshot(&placement)?,
                    ));
                    None
                }
            }
        } else {
            None
        };

        for (component, state) in Self::global_recovery_state_components(&scheduled) {
            nervix_primitives::task::consume_budget().await;
            let placement = self
                .state_placement(
                    domain,
                    state,
                    scheduled.kind(),
                    scheduled.identifier.clone(),
                    None,
                )
                .change_context_lazy(|| OwnershipHandoffError::StatePlacement {
                    kind: scheduled.kind(),
                    identifier: scheduled.identifier.clone(),
                })?;
            let recovered = self
                .forced_recovery_checkpoint(&placement, &recovery_sources, deadline)
                .await;
            match recovered.snapshot {
                Some(snapshot) => checkpoints.push((placement, snapshot)),
                None => {
                    resets.insert(component, recovered.reset_cause);
                    checkpoints.push((
                        placement.clone(),
                        self.empty_ownership_handoff_snapshot(&placement)?,
                    ));
                }
            }
        }

        if let Some(entries) = branch_entries
            && let Some((component, state)) = Self::branch_recovery_state_component(&scheduled)
        {
            for entry in entries {
                nervix_primitives::task::consume_budget().await;
                let placement = self
                    .state_placement(
                        domain,
                        state,
                        scheduled.kind(),
                        scheduled.identifier.clone(),
                        entry.key,
                    )
                    .change_context_lazy(|| OwnershipHandoffError::StatePlacement {
                        kind: scheduled.kind(),
                        identifier: scheduled.identifier.clone(),
                    })?;
                let recovered = self
                    .forced_recovery_checkpoint(&placement, &recovery_sources, deadline)
                    .await;
                match recovered.snapshot {
                    Some(snapshot) => checkpoints.push((placement, snapshot)),
                    None => {
                        resets.entry(component).or_insert(recovered.reset_cause);
                        checkpoints.push((
                            placement.clone(),
                            self.empty_forced_branch_state_snapshot(&placement).await?,
                        ));
                    }
                }
            }
        }

        self.prepare_ownership_handoff_wasm_guests(domain, &scheduled, &checkpoints)
            .await?;
        checkpoints.sort_by(|(left, _), (right, _)| {
            u8::from(left.state.kind())
                .cmp(&u8::from(right.state.kind()))
                .then_with(|| {
                    left.branch_key
                        .as_ref()
                        .map(BranchKey::as_str)
                        .cmp(&right.branch_key.as_ref().map(BranchKey::as_str))
                })
        });
        let entity_ref = entity.in_domain(domain);
        let transition = ForcedRuntimeStateRecoveryTransition {
            operation_id: &operation_id,
            source,
            destination,
            destination_incarnation,
            entity: &entity_ref,
            target_schedule_fingerprint,
        };
        if let Some(store) = self.inner.state_store.as_ref() {
            store
                .persist_forced_recovery_preparation(&transition, &checkpoints)
                .map_err(|error| {
                    OwnershipHandoffError::persistence(error.current_context().clone())
                })?;
        }
        let recovery = transition.identity();
        self.inner.prepared_forced_runtime_state_recoveries.insert(
            entity_ref,
            PreparedForcedRuntimeStateRecovery {
                recovery,
                destination_incarnation,
                target_schedule_fingerprint,
                checkpoints,
            },
        );
        let resets = resets
            .into_iter()
            .map(|(component, cause)| OwnershipStateReset { component, cause })
            .collect::<Vec<_>>();
        Ok(nervix_interconnect::ForcedOwnershipRecoveryPreparation {
            state_recovery: if resets.is_empty() {
                OwnershipStateRecoveryOutcome::Unverified
            } else {
                OwnershipStateRecoveryOutcome::Reset
            },
            resets,
        })
    }

    fn global_recovery_state_components(
        scheduled: &ExecutionNode,
    ) -> Vec<(OwnershipStateComponent, RuntimeStateKind)> {
        let mut components = Vec::new();
        for component in scheduled.ownership_state_components() {
            let state = match component {
                OwnershipStateComponent::BranchAggregated => RuntimeStateKind::BranchAggregated,
                OwnershipStateComponent::KafkaOffsets => RuntimeStateKind::KafkaOffset,
                OwnershipStateComponent::MaterializedRelay => RuntimeStateKind::MaterializedRelay,
                OwnershipStateComponent::BranchLifecycle
                | OwnershipStateComponent::Deduplicator
                | OwnershipStateComponent::WasmProcessor
                | OwnershipStateComponent::WindowProcessor => continue,
            };
            components.push((component, state));
        }
        components
    }

    fn branch_state_component(kind: ModelKind) -> Option<OwnershipStateComponent> {
        Self::branch_recovery_state_component_kind(kind).map(|(component, _)| component)
    }

    fn branch_recovery_state_component(
        scheduled: &ExecutionNode,
    ) -> Option<(OwnershipStateComponent, RuntimeStateKind)> {
        Self::branch_recovery_state_component_kind(scheduled.kind())
    }

    fn branch_recovery_state_component_kind(
        kind: ModelKind,
    ) -> Option<(OwnershipStateComponent, RuntimeStateKind)> {
        match kind {
            ModelKind::Deduplicator => Some((
                OwnershipStateComponent::Deduplicator,
                RuntimeStateKind::Deduplicator,
            )),
            ModelKind::WasmProcessor => Some((
                OwnershipStateComponent::WasmProcessor,
                RuntimeStateKind::WasmProcessor,
            )),
            ModelKind::WindowProcessor => Some((
                OwnershipStateComponent::WindowProcessor,
                RuntimeStateKind::WindowProcessor,
            )),
            _ => None,
        }
    }

    async fn empty_forced_branch_state_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
        match placement.state.kind() {
            RuntimeStateKind::Deduplicator => {
                ReplicatedDeduplicatorState::new(placement.clone(), None)
                    .map_err(OwnershipHandoffError::persistence)?
                    .latest_snapshot()
                    .map_err(|error| {
                        OwnershipHandoffError::persistence(error.current_context().clone())
                    })
            }
            RuntimeStateKind::WindowProcessor => {
                ReplicatedWindowProcessorState::new(placement.clone(), None)
                    .map_err(OwnershipHandoffError::persistence)?
                    .latest_snapshot(&self.inner.executor)
                    .await
                    .map_err(|error| {
                        OwnershipHandoffError::persistence(error.current_context().clone())
                    })
            }
            RuntimeStateKind::WasmProcessor => Ok(PersistedRuntimeStateEntry {
                lsm: 0,
                payload: Vec::new(),
            }),
            _ => Err(OwnershipHandoffError::state(format!(
                "cannot synthesize branch-local {:?} state for forced recovery",
                placement.state
            ))),
        }
    }

    async fn forced_recovery_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        recovery_sources: &[ClusterNodeName],
        deadline: Instant,
    ) -> ForcedRecoveryCheckpoint {
        let mut valid_snapshots = Vec::new();
        let mut reset_cause = OwnershipStateResetCause::MissingCheckpoint;
        match self.capture_state_checkpoint(placement, None).await {
            Ok(Some(snapshot)) => {
                if self
                    .validate_ownership_handoff_snapshot(placement, &snapshot)
                    .is_ok()
                {
                    valid_snapshots.push(snapshot);
                } else {
                    reset_cause = OwnershipStateResetCause::InvalidCheckpoint;
                }
            }
            Ok(None) => {}
            Err(_) => reset_cause = OwnershipStateResetCause::InvalidCheckpoint,
        }

        let mut requests = FuturesUnordered::new();
        for source in recovery_sources {
            nervix_primitives::task::consume_budget().await;
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            requests.push(self.request_state_sync_with_timeout(source, placement, None, remaining));
        }
        while let Some(response) = requests.next().await {
            nervix_primitives::task::consume_budget().await;
            let snapshot = match response {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) | Err(_) => continue,
            };
            if self
                .validate_ownership_handoff_snapshot(placement, &snapshot)
                .is_err()
            {
                if valid_snapshots.is_empty() {
                    reset_cause = OwnershipStateResetCause::InvalidCheckpoint;
                }
                continue;
            }
            valid_snapshots.push(snapshot);
        }
        let Some(latest_lsm) = valid_snapshots.iter().map(|snapshot| snapshot.lsm).max() else {
            return ForcedRecoveryCheckpoint {
                snapshot: None,
                reset_cause,
            };
        };
        let mut latest = valid_snapshots
            .iter()
            .filter(|snapshot| snapshot.lsm == latest_lsm);
        let selected = latest
            .next()
            .verified("the maximum LSM was selected from at least one valid checkpoint");
        if latest.any(|snapshot| snapshot != selected) {
            return ForcedRecoveryCheckpoint {
                snapshot: None,
                reset_cause: OwnershipStateResetCause::ConflictingCheckpoint,
            };
        }
        debug!(
            domain = placement.domain.as_str(),
            kind = placement.kind.as_str(),
            name = placement.identifier.as_str(),
            state = ?placement.state,
            lsm = selected.lsm,
            "selected surviving checkpoint for forced ownership recovery"
        );
        ForcedRecoveryCheckpoint {
            snapshot: Some(selected.clone()),
            reset_cause,
        }
    }

    #[cfg(test)]
    pub(crate) fn ownership_handoff_entity_is_frozen(&self, entity: &DomainNodeRef) -> bool {
        self.inner
            .frozen_ownership_handoff_entities
            .get(entity)
            .is_some_and(|state| state.is_frozen())
    }

    pub(crate) fn ownership_handoff_entity_is_frozen_by(
        &self,
        entity: &DomainNodeRef,
        coordination: &CoordinationIdentity,
    ) -> bool {
        self.inner
            .frozen_ownership_handoff_entities
            .get(entity)
            .is_some_and(|owners| owners.contains(coordination))
    }

    pub(super) async fn checkpoint_entrypoint_branch_lifecycle(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
    ) -> OwnershipHandoffResult<Option<PersistedRuntimeStateEntry>> {
        if !matches!(entity.kind, ModelKind::Ingestor | ModelKind::Reingestor) {
            return Ok(None);
        }
        let runtimes = {
            let execution = self.inner.executions.get(domain).ok_or_else(|| {
                OwnershipHandoffError::checkpoint(format!(
                    "domain '{}' has no execution while checkpointing {} '{}'",
                    domain.as_str(),
                    entity.kind.as_str(),
                    entity.identifier.as_str()
                ))
            })?;
            if execution.passive_only {
                return Ok(None);
            }
            if entity.kind == ModelKind::Ingestor {
                drop(execution);
                let key = DomainNodeRef::node_in(
                    domain.clone(),
                    ModelKind::Ingestor,
                    entity.identifier.clone(),
                );
                if let Some(ingestor) = self.inner.ingestors.get(&key) {
                    ingestor
                        .branched
                        .iter()
                        .map(|entrypoint| entrypoint.branch_runtime.clone())
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            } else if let Some(entrypoints) = execution.branched_entrypoints.get(&entity.identifier)
            {
                entrypoints
                    .iter()
                    .map(|entrypoint| entrypoint.branch_runtime.clone())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        };
        if runtimes.is_empty() {
            return Err(OwnershipHandoffError::checkpoint(format!(
                "{} '{}' has no branch lifecycle runtime while checkpointing ownership",
                entity.kind.as_str(),
                entity.identifier.as_str()
            )));
        }
        let mut snapshots = Vec::with_capacity(runtimes.len());
        for runtime in runtimes {
            nervix_primitives::task::consume_budget().await;
            snapshots.push(runtime.checkpoint().await?);
        }
        if snapshots.len() == 1 {
            return Ok(snapshots.pop());
        }
        let mut entries =
            HashMap::<Option<BranchKey>, BranchInstanceSnapshotEntry<Option<BranchKey>>>::default();
        let mut lsm = 0_u64;
        for snapshot in snapshots {
            lsm = lsm.max(snapshot.lsm);
            for snapshot_entry in decode_branch_lru_snapshot(&snapshot.payload)
                .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?
            {
                match entries.entry(snapshot_entry.key.clone()) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if entry.get().last_ingestion < snapshot_entry.last_ingestion {
                            entry.insert(snapshot_entry);
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(snapshot_entry);
                    }
                }
            }
        }
        let mut entries = entries.into_values().collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left.key
                .as_ref()
                .map(BranchKey::as_str)
                .cmp(&right.key.as_ref().map(BranchKey::as_str))
        });
        Ok(Some(PersistedRuntimeStateEntry {
            lsm: lsm.checked_add(1).ok_or_else(|| {
                OwnershipHandoffError::checkpoint("branch lifecycle checkpoint revision overflowed")
            })?,
            payload: encode_branch_lru_snapshot(&entries)
                .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?,
        }))
    }

    fn ownership_handoff_scheduled_node(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
    ) -> OwnershipHandoffResult<ExecutionNode> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Err(OwnershipHandoffError::schedule(format!(
                "{} '{}' is absent from the local schedule for domain '{}'",
                entity.kind.as_str(),
                entity.identifier.as_str(),
                domain.as_str()
            )));
        };
        execution
            .revision
            .nodes
            .get(entity)
            .cloned()
            .ok_or_else(|| {
                OwnershipHandoffError::schedule(format!(
                    "{} '{}' is absent from the local schedule for domain '{}'",
                    entity.kind.as_str(),
                    entity.identifier.as_str(),
                    domain.as_str()
                ))
            })
    }

    fn verify_local_handoff_schedule(
        &self,
        domain: &DomainName,
        expected: [u8; 32],
    ) -> OwnershipHandoffResult<()> {
        let execution = self.inner.executions.get(domain).ok_or_else(|| {
            OwnershipHandoffError::schedule(format!(
                "domain '{}' has no local schedule for ownership handoff",
                domain.as_str()
            ))
        })?;
        let actual = execution.revision.ownership_handoff_fingerprint;
        if actual != expected {
            return Err(OwnershipHandoffError::schedule(format!(
                "domain '{}' schedule changed during ownership handoff",
                domain.as_str()
            )));
        }
        Ok(())
    }

    pub(super) fn node_has_branch_lifecycle(kind: ModelKind) -> bool {
        kind.is_processor() || matches!(kind, ModelKind::Ingestor | ModelKind::Reingestor)
    }

    fn expected_ownership_handoff_placements(
        &self,
        domain: &DomainName,
        node: &ExecutionNode,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> OwnershipHandoffResult<HashSet<RuntimeStatePlacement>> {
        let unplaceable = || OwnershipHandoffError::StatePlacement {
            kind: node.kind(),
            identifier: node.identifier.clone(),
        };
        let mut expected = HashSet::default();
        for (_, state) in Self::global_recovery_state_components(node) {
            let placement = self
                .state_placement(domain, state, node.kind(), node.identifier.clone(), None)
                .change_context_lazy(unplaceable)?;
            expected.insert(placement);
        }
        if !Self::node_has_branch_lifecycle(node.kind()) {
            return Ok(expected);
        }
        let branch_lru = self
            .state_placement(
                domain,
                RuntimeStateKind::BranchLru,
                node.kind(),
                node.identifier.clone(),
                None,
            )
            .change_context_lazy(unplaceable)?;
        let snapshot = checkpoints
            .iter()
            .find_map(|(placement, snapshot)| (placement == &branch_lru).then_some(snapshot))
            .ok_or_else(|| {
                OwnershipHandoffError::checkpoint(format!(
                    "final branch lifecycle checkpoint is missing for {} '{}'",
                    node.kind().as_str(),
                    node.identifier.as_str()
                ))
            })?;
        expected.insert(branch_lru);
        let state_kind = match node.kind() {
            ModelKind::Deduplicator => Some(RuntimeStateKind::Deduplicator),
            ModelKind::WasmProcessor => Some(RuntimeStateKind::WasmProcessor),
            ModelKind::WindowProcessor => Some(RuntimeStateKind::WindowProcessor),
            _ => None,
        };
        if let Some(state_kind) = state_kind {
            for entry in decode_branch_lru_snapshot(&snapshot.payload)
                .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?
            {
                let placement = self
                    .state_placement(
                        domain,
                        state_kind,
                        node.kind(),
                        node.identifier.clone(),
                        entry.key,
                    )
                    .change_context_lazy(unplaceable)?;
                expected.insert(placement);
            }
        }
        Ok(expected)
    }

    pub(crate) async fn prepare_ownership_handoff_state(
        &self,
        request: nervix_interconnect::PrepareOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        let nervix_interconnect::PrepareOwnershipHandoffStateRequest {
            coordination,
            operation_id,
            source,
            destination,
            source_incarnation,
            destination_incarnation,
            domain,
            entity,
            base_schedule_fingerprint,
            target_schedule_fingerprint,
            checkpoints,
        } = request;
        let domain = &domain;
        let entity = &entity;
        if !self.entity_gate_operation_owns_entity(
            &coordination,
            domain,
            entity,
            EntityGatePurpose::OwnershipHandoff,
        ) {
            return Err(OwnershipHandoffError::participant(format!(
                "coordination identity '{coordination}' does not hold the ownership gate for {} \
                 '{}'",
                entity.kind.as_str(),
                entity.identifier.as_str()
            )));
        }
        self.verify_local_handoff_schedule(domain, base_schedule_fingerprint)?;
        let mut decoded = Vec::with_capacity(checkpoints.len());
        let mut placements = HashSet::default();
        for checkpoint in checkpoints {
            let placement = RuntimeStatePlacement::from_remote(checkpoint.placement)
                .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            if placement.domain != *domain
                || placement.kind != entity.kind
                || placement.identifier != entity.identifier
            {
                return Err(OwnershipHandoffError::state(format!(
                    "checkpoint scope does not match {} '{}' in domain '{}'",
                    entity.kind.as_str(),
                    entity.identifier.as_str(),
                    domain.as_str()
                )));
            }
            if !placements.insert(placement.clone()) {
                return Err(OwnershipHandoffError::state(format!(
                    "handoff contains duplicate {:?} state for {} '{}'",
                    placement.state,
                    entity.kind.as_str(),
                    entity.identifier.as_str()
                )));
            }
            let snapshot = PersistedRuntimeStateEntry {
                lsm: checkpoint.snapshot.lsm,
                payload: checkpoint.snapshot.payload,
            };
            if !self.runtime_state_placement_is_current(&placement) {
                return Err(OwnershipHandoffError::state(format!(
                    "checkpoint for {:?} state has a stale model or schema fingerprint",
                    placement.state
                )));
            }
            self.validate_ownership_handoff_snapshot(&placement, &snapshot)?;
            decoded.push((placement, snapshot));
        }
        let scheduled = self.ownership_handoff_scheduled_node(domain, entity)?;
        let expected = self.expected_ownership_handoff_placements(domain, &scheduled, &decoded)?;
        if placements != expected {
            let missing = expected
                .difference(&placements)
                .map(|placement| format!("{:?}", placement.state))
                .collect::<Vec<_>>();
            let unexpected = placements
                .difference(&expected)
                .map(|placement| format!("{:?}", placement.state))
                .collect::<Vec<_>>();
            return Err(OwnershipHandoffError::state(format!(
                "ownership handoff checkpoint inventory is incomplete or invalid (missing: {}; \
                 unexpected: {})",
                if missing.is_empty() {
                    "-".to_string()
                } else {
                    missing.join(",")
                },
                if unexpected.is_empty() {
                    "-".to_string()
                } else {
                    unexpected.join(",")
                }
            )));
        }
        self.prepare_ownership_handoff_wasm_guests(domain, &scheduled, &decoded)
            .await?;
        decoded.sort_by(|(left, _), (right, _)| {
            u8::from(left.state.kind())
                .cmp(&u8::from(right.state.kind()))
                .then_with(|| {
                    left.branch_key
                        .as_ref()
                        .map(BranchKey::as_str)
                        .cmp(&right.branch_key.as_ref().map(BranchKey::as_str))
                })
        });
        let entity_ref = entity.in_domain(domain);
        let (activation, _) = watch::channel(OwnershipHandoffActivation::Prepared);
        let replacement = PreparedRuntimeStateHandoff {
            coordination,
            operation_id,
            source,
            destination,
            source_incarnation,
            destination_incarnation,
            base_schedule_fingerprint,
            target_schedule_fingerprint,
            activation_authorization:
                OwnershipHandoffActivationAuthorization::AuthorizedByPreparation,
            activation,
            checkpoints: decoded,
        };
        match self
            .inner
            .prepared_runtime_state_handoffs
            .entry(entity_ref.clone())
        {
            nervix_primitives::collections::dash_map::Entry::Occupied(mut entry) => {
                let existing = entry.get();
                let same_preparation = existing.coordination == replacement.coordination
                    && existing.operation_id == replacement.operation_id
                    && existing.source == replacement.source
                    && existing.destination == replacement.destination
                    && existing.source_incarnation == replacement.source_incarnation
                    && existing.destination_incarnation == replacement.destination_incarnation
                    && existing.base_schedule_fingerprint == replacement.base_schedule_fingerprint
                    && existing.target_schedule_fingerprint
                        == replacement.target_schedule_fingerprint
                    && existing.checkpoints == replacement.checkpoints;
                if same_preparation {
                    return Ok(());
                }

                let existing_gate_is_held = self.entity_gate_operation_owns_entity(
                    &existing.coordination,
                    domain,
                    entity,
                    EntityGatePurpose::OwnershipHandoff,
                );
                if existing_gate_is_held {
                    return Err(OwnershipHandoffError::participant(format!(
                        "{} '{}' already has a different ownership handoff preparation",
                        entity.kind.as_str(),
                        entity.identifier.as_str()
                    )));
                }

                if let Some(store) = self.inner.state_store.as_ref() {
                    let replaced_transition = RuntimeStateHandoffTransition {
                        coordination: &existing.coordination,
                        operation_id: &existing.operation_id,
                        source: &existing.source,
                        destination: &existing.destination,
                        source_incarnation: existing.source_incarnation,
                        destination_incarnation: existing.destination_incarnation,
                        entity: &entity_ref,
                        base_schedule_fingerprint: existing.base_schedule_fingerprint,
                        target_schedule_fingerprint: existing.target_schedule_fingerprint,
                    };
                    let replacement_transition = RuntimeStateHandoffTransition {
                        coordination: &replacement.coordination,
                        operation_id: &replacement.operation_id,
                        source: &replacement.source,
                        destination: &replacement.destination,
                        source_incarnation: replacement.source_incarnation,
                        destination_incarnation: replacement.destination_incarnation,
                        entity: &entity_ref,
                        base_schedule_fingerprint: replacement.base_schedule_fingerprint,
                        target_schedule_fingerprint: replacement.target_schedule_fingerprint,
                    };
                    store
                        .replace_handoff_preparation(
                            &replaced_transition,
                            &replacement_transition,
                            &replacement.checkpoints,
                        )
                        .map_err(|error| {
                            OwnershipHandoffError::persistence(error.current_context().clone())
                        })?;
                }
                entry.insert(replacement);
            }
            nervix_primitives::collections::dash_map::Entry::Vacant(entry) => {
                if let Some(store) = self.inner.state_store.as_ref() {
                    let transition = RuntimeStateHandoffTransition {
                        coordination: &replacement.coordination,
                        operation_id: &replacement.operation_id,
                        source: &replacement.source,
                        destination: &replacement.destination,
                        source_incarnation: replacement.source_incarnation,
                        destination_incarnation: replacement.destination_incarnation,
                        entity: &entity_ref,
                        base_schedule_fingerprint: replacement.base_schedule_fingerprint,
                        target_schedule_fingerprint: replacement.target_schedule_fingerprint,
                    };
                    store
                        .persist_handoff_preparation(&transition, &replacement.checkpoints)
                        .map_err(|error| {
                            OwnershipHandoffError::persistence(error.current_context().clone())
                        })?;
                }
                entry.insert(replacement);
            }
        }
        Ok(())
    }

    fn validate_ownership_handoff_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: &PersistedRuntimeStateEntry,
    ) -> OwnershipHandoffResult<()> {
        match placement.state.kind() {
            RuntimeStateKind::BranchAggregated => {
                decode_branch_aggregated_snapshot(&snapshot.payload)
                    .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            }
            RuntimeStateKind::Correlator => {
                return Err(OwnershipHandoffError::state(
                    "correlator buffers are not transferable runtime state",
                ));
            }
            RuntimeStateKind::Deduplicator => {
                ReplicatedDeduplicatorState::new(placement.clone(), Some(snapshot.clone()))
                    .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            }
            RuntimeStateKind::KafkaOffset => {
                ReplicatedKafkaOffsetState::new(placement.clone(), Some(snapshot.clone()))
                    .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            }
            RuntimeStateKind::MaterializedRelay => {
                inspect_sealed_container(
                    &snapshot.payload,
                    self.inner.executor.limits().snapshot_header_bytes.as_u64(),
                )
                .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            }
            RuntimeStateKind::WasmProcessor => {}
            RuntimeStateKind::WindowProcessor => {
                ReplicatedWindowProcessorState::new(placement.clone(), Some(snapshot.clone()))
                    .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            }
            RuntimeStateKind::BranchLru => {
                decode_branch_lru_snapshot(&snapshot.payload)
                    .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            }
        }
        Ok(())
    }

    pub(super) fn activate_prepared_ownership_handoff_state(
        &self,
        domain: &DomainName,
        node: &ExecutionNode,
        local_node_id: &ClusterNodeName,
        schedule_fingerprint: [u8; 32],
        instantiate_now: bool,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        if !node.is_primary_on(local_node_id) {
            return Ok(());
        }
        let entity = DomainNodeRef::node_in(domain.clone(), node.kind(), node.identifier.clone());
        let Some(prepared) = self
            .inner
            .prepared_runtime_state_handoffs
            .get(&entity)
            .map(|prepared| prepared.clone())
        else {
            return Ok(());
        };
        if prepared.destination != *local_node_id || prepared.source == *local_node_id {
            return Ok(());
        }
        if prepared.target_schedule_fingerprint != schedule_fingerprint {
            return Ok(());
        }
        let committed_transition =
            prepared.belongs_to_committed_transition(node, schedule_fingerprint);
        if !prepared.activation_authorization.is_authorized() && !committed_transition {
            return Ok(());
        }
        if let Some(store) = self.inner.state_store.as_ref() {
            let transition = RuntimeStateHandoffTransition {
                coordination: &prepared.coordination,
                operation_id: &prepared.operation_id,
                source: &prepared.source,
                destination: &prepared.destination,
                source_incarnation: prepared.source_incarnation,
                destination_incarnation: prepared.destination_incarnation,
                entity: &entity,
                base_schedule_fingerprint: prepared.base_schedule_fingerprint,
                target_schedule_fingerprint: prepared.target_schedule_fingerprint,
            };
            store.activate_handoff_preparation(&transition, &prepared.checkpoints)?;
        }
        self.remove_runtime_state_for_entity(domain, node.kind(), &node.identifier);
        for (placement, snapshot) in &prepared.checkpoints {
            if instantiate_now {
                self.inner.prepared_runtime_state_snapshots.insert(
                    placement.clone(),
                    PreparedRuntimeStateSnapshot {
                        preparation: RuntimeStatePreparationIdentity::OwnershipHandoff {
                            coordination: prepared.coordination.clone(),
                            operation_id: prepared.operation_id.clone(),
                        },
                        snapshot: snapshot.clone(),
                    },
                );
            } else if placement.state.kind() == RuntimeStateKind::BranchLru {
                self.replicated_branch_lifecycle(placement)
                    .publish(snapshot.clone());
            } else if let Some(entity) = placement.branch_lifecycle() {
                self.replicated_branch_lifecycle(&entity)
                    .hold_passive_checkpoint(placement, snapshot.clone());
            } else {
                self.inner
                    .passive_runtime_state_snapshots
                    .insert(placement.clone(), snapshot.clone());
            }
        }
        let remove = self
            .inner
            .prepared_runtime_state_handoffs
            .get(&entity)
            .is_some_and(|current| {
                current.coordination == prepared.coordination
                    && current.operation_id == prepared.operation_id
            });
        if remove {
            self.inner.prepared_runtime_state_handoffs.remove(&entity);
        }
        let activation = prepared.activation.clone();
        self.inner.activated_runtime_state_handoffs.insert(
            entity,
            ActivatedRuntimeStateHandoff {
                coordination: prepared.coordination,
                operation_id: prepared.operation_id,
                source: prepared.source,
                destination: prepared.destination,
                source_incarnation: prepared.source_incarnation,
                destination_incarnation: prepared.destination_incarnation,
                base_schedule_fingerprint: prepared.base_schedule_fingerprint,
                target_schedule_fingerprint: prepared.target_schedule_fingerprint,
            },
        );
        activation.send_replace(OwnershipHandoffActivation::Activated);
        Ok(())
    }

    pub(super) fn activate_prepared_forced_ownership_recovery_state(
        &self,
        domain: &DomainName,
        node: &ExecutionNode,
        local_node_id: &ClusterNodeName,
        schedule_fingerprint: [u8; 32],
        instantiate_now: bool,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        if !node.is_primary_on(local_node_id) {
            return Ok(());
        }
        let Some(transition) = node.ownership_transition.as_ref() else {
            return Ok(());
        };
        if transition.destination != *local_node_id {
            return Ok(());
        }
        let Some(authorization) =
            ForcedRuntimeStateRecoveryAuthorization::for_scheduled_node(node, transition)?
        else {
            return Ok(());
        };
        let entity = DomainNodeRef::node_in(domain.clone(), node.kind(), node.identifier.clone());
        let dispatcher = self.inner.remote_dispatcher.load();
        let Some(dispatcher) = dispatcher.as_deref() else {
            return Err(Report::new(RuntimePersistenceError::MissingNodeIncarnation));
        };
        let local_incarnation = dispatcher.local_node_incarnation();
        let recovery = ForcedRuntimeStateRecoveryTransition {
            operation_id: &transition.id,
            source: &transition.source,
            destination: &transition.destination,
            destination_incarnation: local_incarnation,
            entity: &entity,
            target_schedule_fingerprint: schedule_fingerprint,
        };
        let prepared = self
            .inner
            .prepared_forced_runtime_state_recoveries
            .get(&entity)
            .map(|prepared| prepared.clone());
        let generations = node.wasm_state_generations();
        let checkpoints = if let Some(store) = self.inner.state_store.as_ref() {
            let Some(checkpoints) =
                store.activate_forced_recovery(&recovery, authorization, generations)?
            else {
                self.inner
                    .prepared_forced_runtime_state_recoveries
                    .remove_if(&entity, |_, current| current.recovery.matches(&recovery));
                return Ok(());
            };
            checkpoints
        } else {
            match prepared.as_ref() {
                Some(prepared)
                    if prepared.recovery.matches(&recovery)
                        && prepared.destination_incarnation == local_incarnation
                        && prepared.target_schedule_fingerprint == schedule_fingerprint =>
                {
                    prepared
                        .checkpoints
                        .iter()
                        .map(|(placement, snapshot)| {
                            (
                                placement.clone().published_in(generations),
                                snapshot.clone(),
                            )
                        })
                        .collect()
                }
                Some(_) if authorization.recreates_without_preparation() => Vec::new(),
                Some(_) => {
                    return Err(Report::new(
                        RuntimePersistenceError::ForcedRecoveryPreparationMismatch,
                    ));
                }
                None if authorization.recreates_without_preparation() => Vec::new(),
                None => {
                    return Err(Report::new(
                        RuntimePersistenceError::MissingForcedRecoveryPreparation,
                    ));
                }
            }
        };
        self.remove_runtime_state_for_entity(domain, node.kind(), &node.identifier);
        for (placement, snapshot) in &checkpoints {
            if instantiate_now {
                self.inner.prepared_runtime_state_snapshots.insert(
                    placement.clone(),
                    PreparedRuntimeStateSnapshot {
                        preparation: RuntimeStatePreparationIdentity::ForcedRecovery,
                        snapshot: snapshot.clone(),
                    },
                );
            } else if placement.state.kind() == RuntimeStateKind::BranchLru {
                self.replicated_branch_lifecycle(placement)
                    .publish(snapshot.clone());
            } else if let Some(entity) = placement.branch_lifecycle() {
                self.replicated_branch_lifecycle(&entity)
                    .hold_passive_checkpoint(placement, snapshot.clone());
            } else {
                self.inner
                    .passive_runtime_state_snapshots
                    .insert(placement.clone(), snapshot.clone());
            }
        }
        self.inner
            .prepared_forced_runtime_state_recoveries
            .remove_if(&entity, |_, current| current.recovery.matches(&recovery));
        Ok(())
    }

    pub(in crate::runtime) fn verify_ownership_handoff_activation(
        &self,
        request: &nervix_interconnect::ActivateOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        let transition = OwnershipHandoffTransitionRef::from(request);
        let key = transition.entity.in_domain(transition.domain);
        let activated_in_memory = self
            .inner
            .activated_runtime_state_handoffs
            .get(&key)
            .is_some_and(|activated| activated.matches(transition));
        if activated_in_memory {
            return Ok(());
        }
        let activated_on_disk = match self.inner.state_store.as_ref() {
            Some(store) => store
                .handoff_activation(
                    transition.coordination,
                    transition.operation_id,
                    transition.domain,
                    transition.entity.kind,
                    &transition.entity.identifier,
                )
                .map_err(|error| {
                    OwnershipHandoffError::persistence(error.current_context().clone())
                })?
                .is_some_and(|activated| {
                    activated.coordination == *transition.coordination
                        && activated.operation_id == transition.operation_id
                        && activated.source == *transition.source
                        && activated.destination == *transition.destination
                        && activated.source_incarnation == transition.source_incarnation
                        && activated.destination_incarnation == transition.destination_incarnation
                        && activated.domain == *transition.domain
                        && activated.kind == transition.entity.kind
                        && activated.identifier == transition.entity.identifier
                        && activated.base_schedule_fingerprint
                            == transition.base_schedule_fingerprint
                        && activated.target_schedule_fingerprint
                            == transition.target_schedule_fingerprint
                }),
            None => false,
        };
        if !activated_on_disk {
            return Err(OwnershipHandoffError::participant(format!(
                "{} '{}' has not activated the requested ownership handoff transition",
                transition.entity.kind.as_str(),
                transition.entity.identifier.as_str()
            )));
        }
        Ok(())
    }

    pub(crate) fn verify_ownership_handoff_preparation(
        &self,
        request: &nervix_interconnect::ConfirmOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        self.verify_ownership_handoff_preparation_transition(request.into())
    }

    fn verify_ownership_handoff_preparation_transition(
        &self,
        transition: OwnershipHandoffTransitionRef<'_>,
    ) -> OwnershipHandoffResult<()> {
        if !self.entity_gate_operation_owns_entity(
            transition.coordination,
            transition.domain,
            transition.entity,
            EntityGatePurpose::OwnershipHandoff,
        ) {
            return Err(OwnershipHandoffError::participant(format!(
                "coordination identity '{}' does not hold the ownership gate for {} '{}'",
                transition.coordination,
                transition.entity.kind.as_str(),
                transition.entity.identifier.as_str()
            )));
        }
        let key = transition.entity.in_domain(transition.domain);
        let Some(prepared) = self.inner.prepared_runtime_state_handoffs.get(&key) else {
            return Err(OwnershipHandoffError::participant(format!(
                "prepared ownership handoff state for {} '{}' is unavailable",
                transition.entity.kind.as_str(),
                transition.entity.identifier.as_str()
            )));
        };
        if !prepared.matches(transition) {
            return Err(OwnershipHandoffError::participant(format!(
                "prepared state for {} '{}' belongs to a different ownership handoff transition",
                transition.entity.kind.as_str(),
                transition.entity.identifier.as_str()
            )));
        }
        Ok(())
    }

    /// Authorizes recovered state and awaits its retained activation state.
    ///
    /// Taking the schedule lock before subscribing makes the activation check and subscription
    /// atomic with respect to ordinary schedule application. Every recovered activation attempt
    /// rebuilds because an earlier attempt may have failed before it published `Activated`;
    /// otherwise ordinary reconciliation owns schedule application and publishes `Activated`.
    pub(crate) async fn activate_persisted_ownership_handoff(
        &self,
        local_node_id: &ClusterNodeName,
        request: &nervix_interconnect::ActivateOwnershipHandoffStateRequest,
        revision: Arc<ExecutionRevision>,
    ) -> OwnershipHandoffResult<()> {
        let mut activation = {
            let _apply = self.inner.schedule_application.lock().await;
            if self.verify_ownership_handoff_activation(request).is_ok() {
                return Ok(());
            }
            let transition = OwnershipHandoffTransitionRef::from(request);
            self.verify_ownership_handoff_preparation_transition(transition)?;
            let key = transition.entity.in_domain(transition.domain);
            let mut prepared = self
                .inner
                .prepared_runtime_state_handoffs
                .get_mut(&key)
                .verified("the preparation was found and checked directly above");
            let activation = prepared.activation.subscribe();
            let requires_reapply = prepared.activation_authorization.authorize();
            drop(prepared);
            if requires_reapply {
                self.rebuild_domain_from_revision(
                    local_node_id,
                    &request.domain,
                    Some(revision),
                    true,
                )
                .await
                .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            }
            activation
        };
        activation
            .wait_for(|state| *state == OwnershipHandoffActivation::Activated)
            .await
            .map_err(|_| {
                OwnershipHandoffError::participant(format!(
                    "prepared state for {} '{}' was discarded before activation",
                    request.entity.kind.as_str(),
                    request.entity.identifier.as_str()
                ))
            })?;
        self.verify_ownership_handoff_activation(request)
    }

    fn remove_runtime_state_for_entity(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
    ) {
        self.inner
            .state_replication_routing
            .retire_entity(&DomainNodeRef::node_in(
                domain.clone(),
                kind,
                identifier.clone(),
            ));
        let matches_entity = |placement: &RuntimeStatePlacement| {
            placement.domain == *domain
                && placement.kind == kind
                && placement.identifier == *identifier
        };
        self.inner
            .replicated_deduplicator_states
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .replicated_kafka_offset_states
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .replicated_materialized_stream_states
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .replicated_window_processor_states
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .replicated_wasm_processor_states
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .replicated_branch_aggregated_states
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .replicated_branch_lifecycles
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .passive_runtime_state_snapshots
            .retain(|placement, _| !matches_entity(placement));
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "state installation consumes its one prepared checkpoint"
        )
    )]
    pub(super) fn take_prepared_runtime_state_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<PersistedRuntimeStateEntry> {
        self.inner
            .prepared_runtime_state_snapshots
            .remove(placement)
            .map(|(_, prepared)| prepared.snapshot)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "state installation consumes its one transferred checkpoint"
        )
    )]
    fn take_transferred_runtime_state_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<PersistedRuntimeStateEntry> {
        if let Some(snapshot) = self.take_prepared_runtime_state_snapshot(placement) {
            return Some(snapshot);
        }
        if let Some(entity) = placement.branch_lifecycle()
            && let Some(lifecycle) = self.branch_lifecycle(&entity)
            && let Some(snapshot) = lifecycle.take_passive_checkpoint(placement)
        {
            return Some(snapshot);
        }
        if let Some((_, snapshot)) = self.inner.passive_runtime_state_snapshots.remove(placement) {
            return Some(snapshot);
        }
        None
    }

    fn stored_runtime_state_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Result<Option<PersistedRuntimeStateEntry>, Report<RuntimePersistenceError>> {
        let Some(store) = self.inner.state_store.as_ref() else {
            return Ok(None);
        };
        Ok(store.latest_snapshot(placement)?)
    }

    pub(crate) fn discard_prepared_ownership_handoff_state(
        &self,
        coordination: &CoordinationIdentity,
        operation_id: &str,
        domain: &DomainName,
        entity: &NodeRef,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let key = DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone());
        self.inner
            .prepared_runtime_state_handoffs
            .remove_if(&key, |_, prepared| {
                &prepared.coordination == coordination && prepared.operation_id == operation_id
            });
        self.inner
            .activated_runtime_state_handoffs
            .remove_if(&key, |_, activated| {
                &activated.coordination == coordination && activated.operation_id == operation_id
            });
        self.inner
            .prepared_runtime_state_snapshots
            .retain(|_, prepared| {
                !prepared
                    .preparation
                    .is_ownership_handoff(coordination, operation_id)
            });
        if let Some(store) = self.inner.state_store.as_ref() {
            store.discard_handoff_preparation(
                coordination,
                operation_id,
                domain,
                entity.kind,
                &entity.identifier,
            )?;
        }
        Ok(())
    }

    pub(crate) fn has_state_store(&self) -> bool {
        self.inner.state_store.is_some()
    }

    pub(crate) fn state_snapshot_interval(&self) -> Duration {
        self.inner.state_snapshot_interval
    }

    /// The checkpoint of `placement` this node holds when it is newer than `after_lsm`, as a
    /// replica synchronizing the placement or a forced recovery asks for it.
    ///
    /// The checkpoint comes from the state handle published at installation. Only a
    /// placement this node holds no state for is answered from its storage.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    pub(crate) async fn capture_state_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        after_lsm: Option<u64>,
    ) -> error_stack::Result<Option<PersistedRuntimeStateEntry>, StateReplicationError> {
        let state = match self.inner.state_replication_routing.resolve(placement) {
            Some(route) => route.state(),
            None => None,
        };
        self.answer_state_sync_request(
            StateReplicationRequest {
                placement: placement.clone(),
                state,
            },
            after_lsm,
        )
        .await
    }

    /// Complete the request through the exact handle selected at admission.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "state synchronization frames capture through their admitted state handle"
        )
    )]
    pub(crate) async fn answer_state_sync_request(
        &self,
        request: StateReplicationRequest,
        after_lsm: Option<u64>,
    ) -> error_stack::Result<Option<PersistedRuntimeStateEntry>, StateReplicationError> {
        let placement = &request.placement;
        let capture = || StateReplicationError::Capture {
            placement: placement.clone(),
        };
        let state = request.state;
        if let Some(state) = state {
            match state.as_ref() {
                ReplicatedState::Deduplicator(state) => {
                    return state.snapshot_after(after_lsm).change_context_lazy(capture);
                }
                ReplicatedState::KafkaOffset(state) => {
                    let snapshot = ReplicatedKafkaOffsetState::read(state)
                        .latest_snapshot()
                        .map_err(Report::new)
                        .change_context_lazy(capture)?;
                    return Ok(snapshot.after(after_lsm));
                }
                ReplicatedState::WindowProcessor(state) => {
                    return state
                        .snapshot_after(after_lsm, &self.inner.executor)
                        .await
                        .change_context_lazy(capture);
                }
                ReplicatedState::WasmProcessor(state) => {
                    return Ok(state.snapshot_after(after_lsm));
                }
                ReplicatedState::BranchAggregated(state) => {
                    let snapshot = nervix_primitives::expect_lint!(
                        nervix::lifecycle_call,
                        "the explicit state-snapshot response captures one retained metrics \
                         placement revision outside record execution",
                        state.latest_snapshot(&self.inner.metrics)
                    )
                    .map_err(Report::new)
                    .change_context_lazy(capture)?;
                    return Ok(snapshot.after(after_lsm));
                }
                ReplicatedState::BranchLru(lifecycle) => {
                    if let Some(held) = lifecycle.latest() {
                        if !held.snapshot().is_after(after_lsm) {
                            return Ok(None);
                        }
                        return Ok(Some(held.snapshot().clone()));
                    }
                }
                // Materialized state travels as a sealed stream.
                ReplicatedState::MaterializedRelay(_) => {}
            }
        }
        let Some(store) = self.inner.state_store.as_ref() else {
            return Ok(None);
        };
        let stored = store
            .latest_snapshot(placement)
            .map_err(Report::new)
            .change_context_lazy(capture)?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        Ok(stored.after(after_lsm))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "catalog frames read the lifecycle retained at admission"
        )
    )]
    pub(crate) fn answer_branch_checkpoint_listing(
        &self,
        request: StateReplicationRequest,
        after: Option<BranchCheckpointCursor>,
    ) -> nervix_interconnect::BranchCheckpointListing {
        let Some(state) = request.state else {
            return OwnerCheckpointListing::Absent.to_remote();
        };
        let ReplicatedState::BranchLru(lifecycle) = state.as_ref() else {
            return OwnerCheckpointListing::Absent.to_remote();
        };
        OwnerCheckpointListing::Listed(
            lifecycle
                .catalog()
                .changes_after(after, BRANCH_CHECKPOINT_LISTING_PAGE),
        )
        .to_remote()
    }

    /// The first changes after `after` in the catalog of branch checkpoints this node owns for the
    /// entity whose branch lifecycle `placement` places.
    #[cfg(any(test, feature = "benchmarks"))]
    fn branch_checkpoint_listing(
        &self,
        placement: &RuntimeStatePlacement,
        after: Option<BranchCheckpointCursor>,
    ) -> OwnerCheckpointListing {
        let Some(lifecycle) = self.branch_lifecycle(placement) else {
            return OwnerCheckpointListing::Absent;
        };
        OwnerCheckpointListing::Listed(
            lifecycle
                .catalog()
                .changes_after(after, BRANCH_CHECKPOINT_LISTING_PAGE),
        )
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    pub(crate) fn resolve_state_replication_request(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<StateReplicationRequest> {
        let dispatcher = self.inner.remote_dispatcher.load();
        let dispatcher = dispatcher.as_deref()?;
        self.inner
            .state_replication_routing
            .assigned_request(placement, dispatcher.local_node_id())
    }

    /// Backup capture checks placement ownership through the same published assignment admission.
    pub(crate) fn runtime_state_placement_is_assigned_locally(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> bool {
        self.resolve_state_replication_request(placement).is_some()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "assigned replica polling records durable progress and installs the selected \
                      revision"
        )
    )]
    pub(super) async fn request_state_sync_with_timeout(
        &self,
        target_node_id: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        after_lsm: Option<u64>,
        response_timeout: Duration,
    ) -> error_stack::Result<Option<PersistedRuntimeStateEntry>, StateReplicationError> {
        let Some(dispatcher) = self.inner.remote_dispatcher.load_full() else {
            return Err(Report::new(StateReplicationError::DispatcherUnavailable {
                target: target_node_id.clone(),
                placement: placement.clone(),
            }));
        };
        let response = dispatcher
            .request_with_timeout(
                target_node_id,
                nervix_interconnect::StateSyncRequest {
                    placement: placement.to_remote(),
                    after_lsm,
                },
                response_timeout,
            )
            .await
            .map_err(|reason| {
                Report::new(StateReplicationError::Request {
                    target: target_node_id.clone(),
                    placement: placement.clone(),
                })
                .attach_printable(reason)
            })?;
        let snapshot = response.result.map_err(|failure| {
            Report::new(StateReplicationError::RemoteFailure {
                target: target_node_id.clone(),
                placement: placement.clone(),
                failure,
            })
        })?;
        Ok(snapshot.map(|snapshot| PersistedRuntimeStateEntry {
            lsm: snapshot.lsm,
            payload: snapshot.payload,
        }))
    }

    pub(in crate::runtime) async fn persist_kafka_offset_snapshot(
        &self,
        state: &KafkaOffsetStatePersistence,
        lsm: u64,
        payload: &[u8],
    ) -> error_stack::Result<(), StateReplicationError> {
        let offsets = state.read();
        if let Some(store) = &self.inner.state_store {
            store
                .persist_latest_snapshot(offsets.placement(), lsm, payload)
                .map_err(Report::new)
                .change_context(StateReplicationError::Persist {
                    placement: offsets.placement().clone(),
                    lsm,
                })?;
            state.record_persisted(lsm);
            self.announce_checkpoint(offsets.placement(), offsets.replication(), lsm);
        }
        offsets.wait_for_replica_quorum(lsm).await
    }

    /// Record a committed Kafka offset and wait until the offset state's replicas hold it.
    ///
    /// The commit only moves the partition's offset in memory. The offset state's snapshot task
    /// persists it on the snapshot interval and when the domain's execution stops, so a crash
    /// resumes from the last persisted or replicated offsets.
    pub(in crate::runtime) async fn commit_domain_kafka_offset(
        &self,
        state: &KafkaOffsetStateOriginator,
        position: KafkaOffsetPosition,
    ) -> error_stack::Result<(), StateReplicationError> {
        let publication = self.backup_publication(&state.placement().domain);
        let lsm = state
            .apply_committed_offset(&position)
            .change_context_lazy(|| StateReplicationError::CommitKafkaOffset {
                placement: state.placement().clone(),
                topic: position.topic.clone(),
                partition: position.partition,
                next_offset: position.offset,
            })?;
        drop(publication);
        let offsets = state.read();
        if offsets.required_replica_acks() == 0 {
            return Ok(());
        }
        self.announce_checkpoint(offsets.placement(), offsets.replication(), lsm);
        offsets.wait_for_replica_quorum(lsm).await
    }

    pub(in crate::runtime) async fn reset_domain_kafka_offsets(
        &self,
        state: &KafkaOffsetStateOriginator,
        offsets: Vec<KafkaOffsetPosition>,
    ) -> error_stack::Result<(), StateReplicationError> {
        let _publication = self.backup_publication(&state.placement().domain);
        let (lsm, payload) = state
            .replace_offsets(offsets)
            .map_err(Report::new)
            .change_context_lazy(|| StateReplicationError::ReplaceKafkaOffsets {
                placement: state.placement().clone(),
            })?;
        self.persist_kafka_offset_snapshot(&state.persistence(), lsm, &payload)
            .await
    }

    /// Apply one branch's records to materialized relay state, keeping that branch's latest record
    /// by timestamp, and wake the readers waiting on materialized state once for all of them.
    ///
    /// The records applied before an assignment refusal still wake those readers.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the decoded checkpoint supplies iteration over the records \
                                   admitted for one retained placement")
    )]
    pub(in crate::runtime) async fn apply_materialized_stream_records(
        &self,
        state: &mut MaterializedRelayStateOriginator,
        key: &Option<BranchKey>,
        records: impl IntoIterator<Item = RuntimeRow>,
    ) -> Result<(), error_stack::Report<StateAuthorityError>> {
        let mut changed = false;
        let mut outcome = Ok(());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            match state.update_last_by_timestamp(key, record) {
                Ok(Some(_)) => changed = true,
                Ok(None) => {}
                Err(error) => {
                    outcome = Err(error);
                    break;
                }
            }
        }
        if changed {
            self.inner.materialized_state_changed.notify_waiters();
        }
        outcome
    }

    pub(in crate::runtime) fn delete_materialized_stream_key(
        &self,
        state: &mut MaterializedRelayStateOriginator,
        key: &Option<BranchKey>,
    ) -> Result<(), error_stack::Report<StateAuthorityError>> {
        if state.remove_key(key)?.is_some() {
            self.inner.materialized_state_changed.notify_waiters();
        }
        Ok(())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "this operation installs, snapshots or retires retained execution state at \
                      an explicit lifetime boundary"
        )
    )]
    pub(in crate::runtime) fn replicated_deduplicator_state(
        &self,
        placement: RuntimeStatePlacement,
    ) -> Result<Arc<ReplicatedDeduplicatorState>, RuntimePersistenceError> {
        let transferred = self.take_transferred_runtime_state_snapshot(&placement);
        if transferred.is_none()
            && let Some(existing) = self.inner.replicated_deduplicator_states.get(&placement)
        {
            return Ok(existing.clone());
        }
        let initial = match transferred {
            Some(snapshot) => Some(snapshot),
            None => self
                .stored_runtime_state_snapshot(&placement)
                .map_err(|error| error.current_context().clone())?,
        };
        let catalog = self.branch_checkpoint_catalog(&placement);
        let state = Arc::new(
            ReplicatedDeduplicatorState::new(placement.clone(), initial)?.cataloged(&catalog),
        );
        self.inner
            .replicated_deduplicator_states
            .insert(placement.clone(), state.clone());
        self.publish_state_replication_route(
            &placement,
            ReplicatedState::Deduplicator(state.clone()),
        );
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_kafka_offset_state(
        &self,
        placement: RuntimeStatePlacement,
        primary_node: Option<ClusterNodeName>,
        replica_nodes: Vec<ClusterNodeName>,
        required_replica_acks: usize,
        local_node: Option<&ClusterNodeName>,
    ) -> Result<KafkaOffsetStateAssignment, RuntimePersistenceError> {
        let roles = StateReplicationRoles::new(primary_node, replica_nodes, required_replica_acks);
        let transferred = self.take_transferred_runtime_state_snapshot(&placement);
        let state = if transferred.is_none()
            && let Some(existing) = self.inner.replicated_kafka_offset_states.get(&placement)
        {
            existing.clone()
        } else {
            let initial = match transferred {
                Some(snapshot) => Some(snapshot),
                None => self
                    .stored_runtime_state_snapshot(&placement)
                    .map_err(|error| error.current_context().clone())?,
            };
            let state = Arc::new(ReplicatedKafkaOffsetState::new(placement.clone(), initial)?);
            self.inner
                .replicated_kafka_offset_states
                .insert(placement.clone(), state.clone());
            self.publish_state_replication_route(
                &placement,
                ReplicatedState::KafkaOffset(state.clone()),
            );
            state
        };
        Ok(ReplicatedKafkaOffsetState::bind(&state, roles, local_node))
    }

    /// Decode whatever this placement's materialized state should start from, before the state
    /// itself is built.
    ///
    /// Opening a sealed snapshot is bulk work that has to be admitted and charged, so it happens
    /// here, on a path that can wait, rather than inside the synchronous construction that
    /// publishes the state into the runtime.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "this operation installs, snapshots or retires retained execution state at \
                      an explicit lifetime boundary"
        )
    )]
    pub(in crate::runtime) async fn prepare_materialized_stream_restore(
        &self,
        placement: &RuntimeStatePlacement,
        schema: &StdArc<arrow_schema::Schema>,
    ) -> Result<(), RuntimePersistenceError> {
        if self
            .inner
            .restored_materialized_stream_states
            .contains_key(placement)
        {
            return Ok(());
        }
        let source = match self.take_transferred_runtime_state_snapshot(placement) {
            Some(snapshot) => {
                let sealed = self
                    .inner
                    .executor
                    .charge_owned(nervix_execution::MemoryClass::Bulk, snapshot.payload)
                    .await
                    .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
                SealedSource::memory(sealed)
            }
            None => {
                if self
                    .inner
                    .replicated_materialized_stream_states
                    .contains_key(placement)
                {
                    return Ok(());
                }
                let Some(store) = self.inner.state_store.as_ref() else {
                    return Ok(());
                };
                let store = store.clone();
                let placement = placement.clone();
                let charge = self
                    .inner
                    .executor
                    .reserve(
                        nervix_execution::MemoryClass::Bulk,
                        crate::runtime::RESTORE_STATE_WORKING_BYTES,
                    )
                    .await
                    .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
                let reader = self
                    .inner
                    .executor
                    .run_storage(
                        nervix_execution::StorageClass::Filesystem,
                        charge,
                        move |_charge, cancellation| {
                            cancellation
                                .check()
                                .change_context(RuntimePersistenceError::Cancelled)?;
                            store.checkpoint_reader(&placement)
                        },
                    )
                    .await
                    .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?
                    .map_err(|error| error.current_context().clone())?;
                let Some(reader) = reader else {
                    return Ok(());
                };
                SealedSource::stored(self.inner.executor.clone(), reader)
            }
        };
        let restored =
            RestoredMaterializedSnapshot::open_relay(&self.inner.executor, schema, source)
                .await
                .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
        self.inner
            .restored_materialized_stream_states
            .insert(placement.clone(), restored);
        Ok(())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "this operation installs, snapshots or retires retained execution state at \
                      an explicit lifetime boundary"
        )
    )]
    pub(in crate::runtime) fn replicated_materialized_stream_state(
        &self,
        placement: RuntimeStatePlacement,
        schema: StdArc<arrow_schema::Schema>,
        primary_node: Option<ClusterNodeName>,
        replica_nodes: Vec<ClusterNodeName>,
        local_node: Option<&ClusterNodeName>,
    ) -> Result<MaterializedRelayStateAssignment, RuntimePersistenceError> {
        let roles = StateReplicationRoles::new(primary_node, replica_nodes, 0);
        let restored = self
            .inner
            .restored_materialized_stream_states
            .remove(&placement)
            .map(|(_, restored)| restored);
        let state = if restored.is_none()
            && let Some(existing) = self
                .inner
                .replicated_materialized_stream_states
                .get(&placement)
        {
            existing.clone()
        } else {
            let state = Arc::new(ReplicatedMaterializedRelayState::restored(
                placement.clone(),
                schema,
                restored,
            ));
            self.inner
                .replicated_materialized_stream_states
                .insert(placement.clone(), state.clone());
            self.publish_state_replication_route(
                &placement,
                ReplicatedState::MaterializedRelay(state.clone()),
            );
            state
        };
        Ok(ReplicatedMaterializedRelayState::bind(
            &state, roles, local_node,
        ))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "this operation installs, snapshots or retires retained execution state at \
                      an explicit lifetime boundary"
        )
    )]
    pub(in crate::runtime) fn replicated_window_processor_state(
        &self,
        placement: RuntimeStatePlacement,
    ) -> Result<Arc<ReplicatedWindowProcessorState>, RuntimePersistenceError> {
        let transferred = self.take_transferred_runtime_state_snapshot(&placement);
        if transferred.is_none()
            && let Some(existing) = self
                .inner
                .replicated_window_processor_states
                .get(&placement)
        {
            return Ok(existing.clone());
        }
        let initial = match transferred {
            Some(snapshot) => Some(snapshot),
            None => self
                .stored_runtime_state_snapshot(&placement)
                .map_err(|error| error.current_context().clone())?,
        };
        let catalog = self.branch_checkpoint_catalog(&placement);
        let state = Arc::new(
            ReplicatedWindowProcessorState::new(placement.clone(), initial)?.cataloged(&catalog),
        );
        self.inner
            .replicated_window_processor_states
            .insert(placement.clone(), state.clone());
        self.publish_state_replication_route(
            &placement,
            ReplicatedState::WindowProcessor(state.clone()),
        );
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_branch_aggregated_state(
        &self,
        placement: RuntimeStatePlacement,
        primary_node: Option<ClusterNodeName>,
        physical_node_id: ClusterNodeName,
    ) -> Result<Arc<ReplicatedBranchAggregatedState>, RuntimePersistenceError> {
        let transferred = self.take_transferred_runtime_state_snapshot(&placement);
        if transferred.is_none()
            && let Some(existing) = self
                .inner
                .replicated_branch_aggregated_states
                .get(&placement)
        {
            existing.rebind_roles(StateReplicationRoles::owned_by(primary_node.clone()));
            if let Some(snapshot) = self
                .inner
                .state_store
                .as_ref()
                .map(|store| store.latest_snapshot(&placement))
                .transpose()?
                .flatten()
            {
                existing.restore_persisted_snapshot(&self.inner.metrics, snapshot)?;
            }
            return Ok(existing.clone());
        }
        let initial = match transferred {
            Some(snapshot) => Some(snapshot),
            None => self
                .stored_runtime_state_snapshot(&placement)
                .map_err(|error| error.current_context().clone())?,
        };
        let state = Arc::new(ReplicatedBranchAggregatedState::new(
            placement.clone(),
            primary_node,
            physical_node_id,
            &self.inner.metrics,
            initial,
        )?);
        self.inner
            .replicated_branch_aggregated_states
            .insert(placement.clone(), state.clone());
        self.publish_state_replication_route(
            &placement,
            ReplicatedState::BranchAggregated(state.clone()),
        );
        Ok(state)
    }
}

#[cfg(feature = "benchmarks")]
pub mod benchmark;
mod checkpoint_announcement;
mod checkpoint_listing;
mod lifecycle;
mod replica_branch_checkpoints;
mod replica_catch_up;
pub(in crate::runtime) mod routing;
mod tasks;
mod wasm_processor_state;

#[cfg(test)]
mod tests;
