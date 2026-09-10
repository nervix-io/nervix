use super::*;

pub(super) const DEFAULT_STATE_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);

pub(super) const DEFAULT_STATE_REPLICATION_POLL_INTERVAL: Duration = Duration::from_secs(1);

const STATE_CHECKPOINT_ANNOUNCEMENT_RETRY_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(crate) struct StateSyncAck {
    pub(crate) placement: RuntimeStatePlacement,
    pub(crate) lsm: u64,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedRuntimeStateHandoff {
    pub(super) operation_id: String,
    pub(super) source: ClusterNodeName,
    pub(super) destination: ClusterNodeName,
    pub(super) source_incarnation: ClusterNodeIncarnation,
    pub(super) destination_incarnation: ClusterNodeIncarnation,
    pub(super) base_schedule_fingerprint: [u8; 32],
    pub(super) target_schedule_fingerprint: [u8; 32],
    pub(super) activation_authorized: bool,
    pub(super) checkpoints: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
}

#[derive(Debug, Clone, Copy)]
struct OwnershipHandoffTransitionRef<'a> {
    operation_id: &'a str,
    source: &'a ClusterNodeName,
    destination: &'a ClusterNodeName,
    source_incarnation: ClusterNodeIncarnation,
    destination_incarnation: ClusterNodeIncarnation,
    domain: &'a DomainName,
    entity: &'a NodeRef,
    base_schedule_fingerprint: [u8; 32],
    target_schedule_fingerprint: [u8; 32],
}

impl<'a> From<&'a nervix_interconnect::ActivateOwnershipHandoffStateRequest>
    for OwnershipHandoffTransitionRef<'a>
{
    fn from(request: &'a nervix_interconnect::ActivateOwnershipHandoffStateRequest) -> Self {
        Self {
            operation_id: &request.operation_id,
            source: &request.source,
            destination: &request.destination,
            source_incarnation: request.source_incarnation,
            destination_incarnation: request.destination_incarnation,
            domain: &request.domain,
            entity: &request.entity,
            base_schedule_fingerprint: request.base_schedule_fingerprint,
            target_schedule_fingerprint: request.target_schedule_fingerprint,
        }
    }
}

impl<'a> From<&'a nervix_interconnect::ConfirmOwnershipHandoffStateRequest>
    for OwnershipHandoffTransitionRef<'a>
{
    fn from(request: &'a nervix_interconnect::ConfirmOwnershipHandoffStateRequest) -> Self {
        Self {
            operation_id: &request.operation_id,
            source: &request.source,
            destination: &request.destination,
            source_incarnation: request.source_incarnation,
            destination_incarnation: request.destination_incarnation,
            domain: &request.domain,
            entity: &request.entity,
            base_schedule_fingerprint: request.base_schedule_fingerprint,
            target_schedule_fingerprint: request.target_schedule_fingerprint,
        }
    }
}

impl PreparedRuntimeStateHandoff {
    fn matches(&self, transition: OwnershipHandoffTransitionRef<'_>) -> bool {
        self.operation_id == transition.operation_id
            && self.source == *transition.source
            && self.destination == *transition.destination
            && self.source_incarnation == transition.source_incarnation
            && self.destination_incarnation == transition.destination_incarnation
            && self.base_schedule_fingerprint == transition.base_schedule_fingerprint
            && self.target_schedule_fingerprint == transition.target_schedule_fingerprint
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActivatedRuntimeStateHandoff {
    operation_id: String,
    source: ClusterNodeName,
    destination: ClusterNodeName,
    source_incarnation: ClusterNodeIncarnation,
    destination_incarnation: ClusterNodeIncarnation,
    base_schedule_fingerprint: [u8; 32],
    target_schedule_fingerprint: [u8; 32],
}

impl ActivatedRuntimeStateHandoff {
    fn matches(&self, transition: OwnershipHandoffTransitionRef<'_>) -> bool {
        self.operation_id == transition.operation_id
            && self.source == *transition.source
            && self.destination == *transition.destination
            && self.source_incarnation == transition.source_incarnation
            && self.destination_incarnation == transition.destination_incarnation
            && self.base_schedule_fingerprint == transition.base_schedule_fingerprint
            && self.target_schedule_fingerprint == transition.target_schedule_fingerprint
    }
}

#[derive(Debug)]
pub(super) struct PreparedRuntimeStateSnapshot {
    operation_id: String,
    snapshot: PersistedRuntimeStateEntry,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedForcedRuntimeStateRecovery {
    operation_id: String,
    destination_incarnation: ClusterNodeIncarnation,
    target_schedule_fingerprint: [u8; 32],
    checkpoints: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
}

struct ForcedRecoveryCheckpoint {
    snapshot: Option<PersistedRuntimeStateEntry>,
    reset_cause: OwnershipStateResetCause,
}

#[derive(Debug, Clone)]
pub(super) struct PendingStateReplicaSync {
    source: ClusterNodeName,
    target_lsm: u64,
}

#[derive(Debug, Clone)]
pub(super) struct PendingStateCheckpointAnnouncement {
    target_lsm: u64,
    replica_progress: BTreeMap<ClusterNodeName, u64>,
}

pub(super) fn persist_dirty_runtime_state_snapshot(
    store: &RuntimeStateStore,
    placement: &RuntimeStatePlacement,
    last_persisted_lsm: &AtomicU64,
    dirty: &AtomicBool,
    latest_snapshot: impl FnOnce() -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError>,
) -> Result<Option<u64>, Report<RuntimePersistenceError>> {
    if !dirty.swap(false, Ordering::SeqCst) {
        return Ok(None);
    }
    let result = (|| {
        let snapshot = latest_snapshot()?;
        if snapshot.lsm <= last_persisted_lsm.load(Ordering::SeqCst) {
            return Ok(None);
        }
        store.persist_latest_snapshot(placement, snapshot.lsm, &snapshot.payload)?;
        last_persisted_lsm.fetch_max(snapshot.lsm, Ordering::SeqCst);
        Ok(Some(snapshot.lsm))
    })();
    if result.is_err() {
        dirty.store(true, Ordering::SeqCst);
    }
    result
}

pub(super) async fn persist_window_processor_state_snapshot(
    store: &RuntimeStateStore,
    state: &ReplicatedWindowProcessorState,
    snapshot_requests: &mpsc::Sender<WindowProcessorSnapshotRequest>,
) -> RuntimeStateResult<Option<u64>> {
    if state.live_dirty.load(Ordering::SeqCst) {
        let (response_tx, response_rx) = oneshot::channel();
        snapshot_requests.send(response_tx).await.map_err(|_| {
            RuntimeStateOperationError::checkpoint(format!(
                "window processor '{}' snapshot owner is unavailable",
                state.placement.identifier.as_str()
            ))
        })?;
        let response = response_rx.await.map_err(|_| {
            RuntimeStateOperationError::checkpoint(format!(
                "window processor '{}' snapshot owner dropped its response",
                state.placement.identifier.as_str()
            ))
        })?;
        response.map_err(RuntimeStateOperationError::checkpoint)?;
    }
    persist_dirty_runtime_state_snapshot(
        store,
        &state.placement,
        &state.last_persisted_lsm,
        &state.dirty,
        || state.latest_snapshot(),
    )
    .map_err(|error| RuntimeStateOperationError::persistence(error.current_context().clone()))
}

impl Runtime {
    pub(crate) fn ownership_handoff_schedule_fingerprint(
        schedule: &DomainSchedule,
    ) -> OwnershipHandoffResult<[u8; 32]> {
        #[derive(serde::Serialize)]
        struct ScheduledNodeFingerprint<'a> {
            identifier: &'a ModelName,
            config: &'a Model,
            effective_branching: &'a Option<Vec<FieldName>>,
            effective_branching_schema: &'a Option<SchemaName>,
            schema_fingerprint: [u8; 32],
            kafka_partition_schedule: &'a Option<KafkaPartitionSchedule>,
            primary_node: &'a Option<ClusterNodeName>,
            assigned_nodes: &'a [ClusterNodeName],
        }

        #[derive(serde::Serialize)]
        struct DomainScheduleFingerprint<'a> {
            domain: &'a DomainName,
            nodes: Vec<ScheduledNodeFingerprint<'a>>,
            placement_groups: &'a [nervix_models::PlacementGroupSchedule],
        }

        let nodes = schedule
            .nodes
            .values()
            .map(|node| ScheduledNodeFingerprint {
                identifier: &node.identifier,
                config: node.config.as_ref(),
                effective_branching: &node.effective_branching,
                effective_branching_schema: &node.effective_branching_schema,
                schema_fingerprint: node.schema_fingerprint,
                kafka_partition_schedule: &node.kafka_partition_schedule,
                primary_node: &node.primary_node,
                assigned_nodes: &node.assigned_nodes,
            })
            .collect::<Vec<_>>();
        let fingerprint = DomainScheduleFingerprint {
            domain: &schedule.domain,
            nodes,
            placement_groups: &schedule.placement_groups,
        };
        let encoded = serde_json::to_vec(&fingerprint).map_err(|error| {
            OwnershipHandoffError::schedule(format!(
                "failed to encode ownership handoff schedule: {error}"
            ))
        })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/ownership-handoff/domain-schedule");
        hasher.update(&encoded);
        Ok(*hasher.finalize().as_bytes())
    }

    pub(super) fn persist_branch_lru_snapshot(
        &self,
        placement: RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        self.inner
            .replicated_branch_lru_snapshots
            .insert(placement.clone(), snapshot.clone());
        if let Some(store) = self.inner.state_store.as_ref() {
            store.persist_latest_snapshot(&placement, snapshot.lsm, &snapshot.payload)?;
        }
        self.notify_runtime_state_replicas(&placement, snapshot.lsm);
        Ok(())
    }

    pub(super) fn take_restorable_branch_lru_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Result<Option<PersistedRuntimeStateEntry>, Report<RuntimePersistenceError>> {
        if let Some(snapshot) = self.take_transferred_runtime_state_snapshot(placement) {
            return Ok(Some(snapshot));
        }
        if let Some(snapshot) = self.inner.replicated_branch_lru_snapshots.get(placement) {
            return Ok(Some(snapshot.clone()));
        }
        self.stored_runtime_state_snapshot(placement)
    }

    fn state_checkpoint_notification(&self, placement: &RuntimeStatePlacement) -> Arc<Notify> {
        self.inner
            .state_checkpoint_notifications
            .entry(placement.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    pub(crate) fn handle_state_checkpoint_available(
        &self,
        source: &ClusterNodeName,
        checkpoint: nervix_interconnect::StateCheckpointAvailable,
    ) {
        let placement = match RuntimeStatePlacement::from_remote(checkpoint.placement) {
            Ok(placement) => placement,
            Err(error) => {
                warn!(
                    error,
                    "ignored invalid runtime state checkpoint notification"
                );
                return;
            }
        };
        if !self.runtime_state_placement_is_current(&placement) {
            return;
        }
        let local_node_id = self.inner.remote_dispatch.local_node_id.read().clone();
        let Some(local_node_id) = local_node_id else {
            return;
        };
        let valid_assignment = if let Some(execution) = self.inner.executions.get(&placement.domain)
        {
            if let Some(node) = execution
                .schedule
                .nodes
                .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
            {
                node.execution_node() == Some(source)
                    && node.is_assigned_to(&local_node_id)
                    && !node.is_primary_on(&local_node_id)
            } else {
                false
            }
        } else {
            false
        };
        if !valid_assignment {
            return;
        }
        trace!(
            domain = placement.domain.as_str(),
            kind = placement.kind.as_str(),
            name = placement.identifier.as_str(),
            lsm = checkpoint.lsm,
            "runtime state checkpoint is available"
        );
        self.state_checkpoint_notification(&placement).notify_one();
        if let RuntimeStateKind::Deduplicator
        | RuntimeStateKind::WasmProcessor
        | RuntimeStateKind::WindowProcessor
        | RuntimeStateKind::BranchLru = placement.state
        {
            self.schedule_passive_state_replica_sync(placement, source.clone(), checkpoint.lsm);
        }
    }

    fn schedule_passive_state_replica_sync(
        &self,
        placement: RuntimeStatePlacement,
        source: ClusterNodeName,
        target_lsm: u64,
    ) {
        let should_spawn = match self
            .inner
            .pending_state_replica_syncs
            .entry(placement.clone())
        {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let pending = entry.get_mut();
                if pending.source != source {
                    pending.source = source;
                    pending.target_lsm = target_lsm;
                } else {
                    pending.target_lsm = pending.target_lsm.max(target_lsm);
                }
                false
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(PendingStateReplicaSync { source, target_lsm });
                true
            }
        };
        if should_spawn {
            let runtime = self.clone();
            self.inner.state_replication_tasks.spawn(async move {
                runtime.reconcile_passive_state_replica(placement).await;
            });
        }
    }

    async fn reconcile_passive_state_replica(&self, placement: RuntimeStatePlacement) {
        loop {
            tokio::task::consume_budget().await;
            let Some(pending) = self
                .inner
                .pending_state_replica_syncs
                .get(&placement)
                .map(|pending| pending.clone())
            else {
                return;
            };
            if !self.state_replica_assignment_is_current(&placement, &pending.source) {
                self.inner.pending_state_replica_syncs.remove(&placement);
                return;
            }
            let current_lsm = match self.passive_state_replica_lsm(&placement) {
                Ok(current_lsm) => current_lsm,
                Err(error) => {
                    warn!(
                        domain = placement.domain.as_str(),
                        kind = placement.kind.as_str(),
                        name = placement.identifier.as_str(),
                        error = %error,
                        "failed to read replicated runtime state progress"
                    );
                    None
                }
            };
            if current_lsm.is_some_and(|lsm| lsm >= pending.target_lsm) {
                let removed = self
                    .inner
                    .pending_state_replica_syncs
                    .remove_if(&placement, |_, current| {
                        current.source == pending.source && current.target_lsm <= pending.target_lsm
                    })
                    .is_some();
                if removed {
                    return;
                }
                continue;
            }
            let result = self
                .request_state_sync_with_timeout(
                    &pending.source,
                    &placement,
                    current_lsm,
                    self.inner.state_replication_poll_interval,
                )
                .await;
            match result {
                Ok(Some(snapshot)) => {
                    if let Err(error) = self.install_passive_state_replica_snapshot(
                        &pending.source,
                        &placement,
                        snapshot,
                    ) {
                        warn!(
                            domain = placement.domain.as_str(),
                            kind = placement.kind.as_str(),
                            name = placement.identifier.as_str(),
                            error = %error,
                            "failed to install replicated runtime state checkpoint"
                        );
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(
                        domain = placement.domain.as_str(),
                        kind = placement.kind.as_str(),
                        name = placement.identifier.as_str(),
                        error = %error,
                        "failed to fetch announced runtime state checkpoint"
                    );
                }
            }
            sleep(STATE_CHECKPOINT_ANNOUNCEMENT_RETRY_INTERVAL).await;
        }
    }

    fn state_replica_assignment_is_current(
        &self,
        placement: &RuntimeStatePlacement,
        source: &ClusterNodeName,
    ) -> bool {
        if !self.runtime_state_placement_is_current(placement) {
            return false;
        }
        let local_node_id = self.inner.remote_dispatch.local_node_id.read().clone();
        let Some(local_node_id) = local_node_id else {
            return false;
        };
        let Some(execution) = self.inner.executions.get(&placement.domain) else {
            return false;
        };
        let Some(node) = execution
            .schedule
            .nodes
            .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
        else {
            return false;
        };
        node.execution_node() == Some(source)
            && node.is_assigned_to(&local_node_id)
            && !node.is_primary_on(&local_node_id)
    }

    fn passive_state_replica_lsm(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Result<Option<u64>, Report<RuntimePersistenceError>> {
        if placement.state == RuntimeStateKind::BranchLru
            && let Some(snapshot) = self.inner.replicated_branch_lru_snapshots.get(placement)
        {
            return Ok(Some(snapshot.lsm));
        }
        if let Some(snapshot) = self.inner.passive_runtime_state_snapshots.get(placement) {
            return Ok(Some(snapshot.lsm));
        }
        let Some(store) = self.inner.state_store.as_ref() else {
            return Ok(None);
        };
        let snapshot = store.latest_snapshot(placement)?;
        if placement.state == RuntimeStateKind::BranchLru
            && let Some(snapshot) = snapshot.as_ref()
        {
            self.inner
                .replicated_branch_lru_snapshots
                .insert(placement.clone(), snapshot.clone());
        }
        Ok(snapshot.map(|snapshot| snapshot.lsm))
    }

    fn install_passive_state_replica_snapshot(
        &self,
        source: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
    ) -> RuntimeStateResult<()> {
        if !self.state_replica_assignment_is_current(placement, source) {
            return Err(RuntimeStateOperationError::replication(
                "runtime state replica assignment changed during synchronization",
            ));
        }
        if snapshot.schema_fingerprint != placement.schema_fingerprint {
            return Err(RuntimeStateOperationError::replication(
                "runtime state checkpoint schema fingerprint does not match its placement",
            ));
        }
        self.validate_ownership_handoff_snapshot(placement, &snapshot)
            .map_err(|error| RuntimeStateOperationError::replication(error.to_string()))?;
        if placement.branch_key.is_some() && !self.replica_branch_is_current(placement)? {
            return Err(RuntimeStateOperationError::replication(
                "runtime state checkpoint belongs to an evicted branch",
            ));
        }
        if self
            .passive_state_replica_lsm(placement)
            .map_err(|error| {
                RuntimeStateOperationError::persistence(error.current_context().clone())
            })?
            .is_some_and(|current| current >= snapshot.lsm)
        {
            return Ok(());
        }
        if let Some(store) = self.inner.state_store.as_ref()
            && !store
                .persist_replica_snapshot_if_newer(placement, &snapshot)
                .map_err(|error| {
                    RuntimeStateOperationError::persistence(error.current_context().clone())
                })?
        {
            return Ok(());
        }
        if placement.state == RuntimeStateKind::BranchLru {
            self.install_replica_branch_lru_snapshot(placement, &snapshot)?;
        } else {
            match self
                .inner
                .passive_runtime_state_snapshots
                .entry(placement.clone())
            {
                dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                    if entry.get().lsm < snapshot.lsm {
                        entry.insert(snapshot.clone());
                    }
                }
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    entry.insert(snapshot.clone());
                }
            }
        }
        self.acknowledge_state_replica_install(source, placement, snapshot.lsm);
        Ok(())
    }

    fn replica_branch_is_current(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> RuntimeStateResult<bool> {
        let branch_lru = self.state_placement(
            &placement.domain,
            RuntimeStateKind::BranchLru,
            placement.kind,
            placement.identifier.clone(),
            None,
        );
        let snapshot = match self.inner.replicated_branch_lru_snapshots.get(&branch_lru) {
            Some(snapshot) => Some(snapshot.clone()),
            None => match self.inner.state_store.as_ref() {
                Some(store) => store
                    .latest_snapshot(&branch_lru)
                    .map_err(RuntimeStateOperationError::persistence)?,
                None => None,
            },
        };
        let Some(snapshot) = snapshot else {
            return Ok(false);
        };
        let entries = decode_branch_lru_snapshot(&snapshot.payload)
            .map_err(RuntimeStateOperationError::replication)?;
        Ok(entries
            .iter()
            .any(|(branch, _)| branch.as_ref() == placement.branch_key.as_ref()))
    }

    fn install_replica_branch_lru_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: &PersistedRuntimeStateEntry,
    ) -> RuntimeStateResult<()> {
        let branches = decode_branch_lru_snapshot(&snapshot.payload)
            .map_err(RuntimeStateOperationError::replication)?
            .into_iter()
            .map(|(branch, _)| branch)
            .collect::<HashSet<_>>();
        self.inner
            .passive_runtime_state_snapshots
            .retain(|candidate, _| {
                candidate.domain != placement.domain
                    || candidate.kind != placement.kind
                    || candidate.identifier != placement.identifier
                    || candidate
                        .branch_key
                        .as_ref()
                        .is_none_or(|branch| branches.contains(&Some(branch.clone())))
            });
        self.inner
            .replicated_branch_lru_snapshots
            .insert(placement.clone(), snapshot.clone());
        Ok(())
    }

    fn acknowledge_state_replica_install(
        &self,
        source: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        lsm: u64,
    ) {
        let dispatcher = self.inner.remote_dispatcher.read().clone();
        let Some(dispatcher) = dispatcher else {
            return;
        };
        let source = source.clone();
        let placement = placement.to_remote();
        drop(tokio::spawn(async move {
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

    fn notify_runtime_state_replicas(&self, placement: &RuntimeStatePlacement, lsm: u64) {
        let should_spawn = match self
            .inner
            .pending_state_checkpoint_announcements
            .entry(placement.clone())
        {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let pending = entry.get_mut();
                pending.target_lsm = pending.target_lsm.max(lsm);
                false
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(PendingStateCheckpointAnnouncement {
                    target_lsm: lsm,
                    replica_progress: BTreeMap::new(),
                });
                true
            }
        };
        if should_spawn {
            let runtime = self.clone();
            let placement = placement.clone();
            self.inner.state_replication_tasks.spawn(async move {
                runtime
                    .dispatch_pending_state_checkpoint_announcements(placement)
                    .await;
            });
        }
    }

    async fn dispatch_pending_state_checkpoint_announcements(
        &self,
        placement: RuntimeStatePlacement,
    ) {
        loop {
            tokio::task::consume_budget().await;
            let pending = match self
                .inner
                .pending_state_checkpoint_announcements
                .get(&placement)
            {
                Some(pending) => pending.value().clone(),
                None => return,
            };
            let Some(local_node_id) = self.inner.remote_dispatch.local_node_id.read().clone()
            else {
                self.inner
                    .pending_state_checkpoint_announcements
                    .remove(&placement);
                return;
            };
            let replica_nodes =
                if let Some(execution) = self.inner.executions.get(&placement.domain) {
                    if let Some(node) = execution
                        .schedule
                        .nodes
                        .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
                    {
                        if node.is_primary_on(&local_node_id) {
                            node.replica_nodes()
                                .into_iter()
                                .cloned()
                                .collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        }
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };
            if replica_nodes.is_empty() {
                self.inner
                    .pending_state_checkpoint_announcements
                    .remove(&placement);
                return;
            }
            let lagging_replicas = replica_nodes
                .iter()
                .filter(|replica| {
                    pending
                        .replica_progress
                        .get(*replica)
                        .is_none_or(|lsm| *lsm < pending.target_lsm)
                })
                .cloned()
                .collect::<Vec<_>>();
            if lagging_replicas.is_empty() {
                let removed = self
                    .inner
                    .pending_state_checkpoint_announcements
                    .remove_if(&placement, |_, current| {
                        current.target_lsm <= pending.target_lsm
                            && replica_nodes.iter().all(|replica| {
                                current
                                    .replica_progress
                                    .get(replica)
                                    .is_some_and(|lsm| *lsm >= current.target_lsm)
                            })
                    })
                    .is_some();
                if removed {
                    return;
                }
                continue;
            }
            let Some(dispatcher) = self.inner.remote_dispatcher.read().clone() else {
                self.inner
                    .pending_state_checkpoint_announcements
                    .remove(&placement);
                return;
            };
            let checkpoint = nervix_interconnect::StateCheckpointAvailable {
                placement: placement.to_remote(),
                lsm: pending.target_lsm,
            };
            for replica in lagging_replicas {
                tokio::task::consume_budget().await;
                if let Err(error) = dispatcher
                    .dispatch(
                        &replica,
                        Envelope::Control(
                            nervix_interconnect::ControlEnvelope::StateCheckpointAvailable(
                                checkpoint.clone(),
                            ),
                        ),
                    )
                    .await
                {
                    warn!(
                        destination = %replica,
                        error = %error,
                        "failed to announce runtime state checkpoint"
                    );
                }
            }
            sleep(STATE_CHECKPOINT_ANNOUNCEMENT_RETRY_INTERVAL).await;
        }
    }

    async fn wait_for_state_replica_sync_trigger(
        &self,
        shutdown_rx: &mut watch::Receiver<bool>,
        notification: &Notify,
        poll_interval: Duration,
        initial_sync_pending: bool,
    ) -> bool {
        if initial_sync_pending && !self.inner.fault_injection.state_replica_polling_is_paused() {
            return true;
        }
        if self.inner.fault_injection.state_replica_polling_is_paused() {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    changed.is_ok() && !*shutdown_rx.borrow()
                }
                _ = notification.notified() => true,
            }
        } else {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    changed.is_ok() && !*shutdown_rx.borrow()
                }
                _ = notification.notified() => true,
                _ = sleep(poll_interval) => true,
            }
        }
    }

    pub(crate) async fn capture_ownership_handoff_state(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        base_schedule_fingerprint: [u8; 32],
    ) -> OwnershipHandoffResult<Vec<nervix_interconnect::OwnershipHandoffCheckpoint>> {
        let key = DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone());
        self.inner
            .frozen_ownership_handoff_entities
            .insert(key.clone(), ());
        let result = self
            .capture_frozen_ownership_handoff_state(domain, entity, base_schedule_fingerprint)
            .await;
        if result.is_err() {
            self.inner.frozen_ownership_handoff_entities.remove(&key);
            self.inner.ownership_handoff_freeze_changed.notify_waiters();
        }
        result
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
        for state in self.inner.replicated_deduplicator_states.iter() {
            if matches_entity(state.key()) {
                checkpoints.push((
                    state.key().clone(),
                    state
                        .latest_snapshot()
                        .map_err(OwnershipHandoffError::persistence)?,
                ));
            }
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
        for state in self.inner.replicated_materialized_stream_states.iter() {
            if matches_entity(state.key()) {
                checkpoints.push((
                    state.key().clone(),
                    ReplicatedMaterializedRelayState::read(state.value())
                        .latest_snapshot()
                        .map_err(OwnershipHandoffError::persistence)?,
                ));
            }
        }
        for state in self.inner.replicated_window_processor_states.iter() {
            if matches_entity(state.key()) {
                checkpoints.push((
                    state.key().clone(),
                    state
                        .latest_snapshot()
                        .map_err(OwnershipHandoffError::persistence)?,
                ));
            }
        }
        for state in self.inner.replicated_wasm_processor_states.iter() {
            if matches_entity(state.key()) {
                checkpoints.push((
                    state.key().clone(),
                    state
                        .latest_snapshot()
                        .map_err(OwnershipHandoffError::persistence)?,
                ));
            }
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
            let branch_lru = self.state_placement(
                domain,
                RuntimeStateKind::BranchLru,
                entity.kind,
                entity.identifier.clone(),
                None,
            );
            match final_branch_lru {
                Some(snapshot) => checkpoints.push((branch_lru.clone(), snapshot)),
                None => {
                    if let Some(snapshot) = self
                        .inner
                        .replicated_branch_lru_snapshots
                        .get(&branch_lru)
                        .map(|snapshot| snapshot.clone())
                    {
                        checkpoints.push((branch_lru.clone(), snapshot));
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
                let schema_fingerprint = branch_lru.schema_fingerprint;
                checkpoints.push((
                    branch_lru,
                    PersistedRuntimeStateEntry {
                        lsm: 0,
                        schema_fingerprint,
                        payload: encode_branch_lru_snapshot(&[])
                            .map_err(OwnershipHandoffError::checkpoint)?,
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
            u8::from(left.state)
                .cmp(&u8::from(right.state))
                .then_with(|| {
                    left.branch_key
                        .as_ref()
                        .map(BranchKey::as_str)
                        .cmp(&right.branch_key.as_ref().map(BranchKey::as_str))
                })
        });
        for (placement, snapshot) in &checkpoints {
            if placement.state == RuntimeStateKind::BranchLru {
                self.persist_branch_lru_snapshot(placement.clone(), snapshot.clone())
                    .map_err(|error| {
                        OwnershipHandoffError::persistence(error.current_context().clone())
                    })?;
            } else if let Some(store) = self.inner.state_store.as_ref() {
                store
                    .persist_latest_snapshot(placement, snapshot.lsm, &snapshot.payload)
                    .map_err(OwnershipHandoffError::persistence)?;
                self.notify_runtime_state_replicas(placement, snapshot.lsm);
            }
        }
        Ok(checkpoints
            .into_iter()
            .map(
                |(placement, snapshot)| nervix_interconnect::OwnershipHandoffCheckpoint {
                    placement: placement.to_remote(),
                    snapshot: nervix_interconnect::StateSnapshotEnvelope {
                        lsm: snapshot.lsm,
                        schema_fingerprint: snapshot.schema_fingerprint,
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
        let payload = match placement.state {
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
            RuntimeStateKind::MaterializedRelay => encode_materialized_stream_snapshot_entries(&[])
                .map_err(OwnershipHandoffError::persistence)?,
            RuntimeStateKind::BranchLru => {
                encode_branch_lru_snapshot(&[]).map_err(OwnershipHandoffError::checkpoint)?
            }
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
        Ok(PersistedRuntimeStateEntry {
            lsm: 0,
            schema_fingerprint: placement.schema_fingerprint,
            payload,
        })
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
        if scheduled.execution_node() != Some(source) {
            return Err(OwnershipHandoffError::participant(format!(
                "{} '{}' is no longer assigned to lost owner '{}'",
                entity.kind.as_str(),
                entity.identifier.as_str(),
                source
            )));
        }
        let local_node = self
            .inner
            .remote_dispatch
            .local_node_id
            .read()
            .clone()
            .ok_or_else(|| {
                OwnershipHandoffError::participant("local node identity is unavailable")
            })?;
        if local_node != *destination {
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
            let placement = self.state_placement(
                domain,
                RuntimeStateKind::BranchLru,
                scheduled.kind(),
                scheduled.identifier.clone(),
                None,
            );
            let recovered = self
                .forced_recovery_checkpoint(&placement, &recovery_sources, deadline)
                .await;
            match recovered.snapshot {
                Some(snapshot) => {
                    let entries = decode_branch_lru_snapshot(&snapshot.payload)
                        .map_err(OwnershipHandoffError::state)?;
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
            tokio::task::consume_budget().await;
            let placement = self.state_placement(
                domain,
                state,
                scheduled.kind(),
                scheduled.identifier.clone(),
                None,
            );
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
            for (branch_key, _) in entries {
                tokio::task::consume_budget().await;
                let placement = self.state_placement(
                    domain,
                    state,
                    scheduled.kind(),
                    scheduled.identifier.clone(),
                    branch_key,
                );
                let recovered = self
                    .forced_recovery_checkpoint(&placement, &recovery_sources, deadline)
                    .await;
                match recovered.snapshot {
                    Some(snapshot) => checkpoints.push((placement, snapshot)),
                    None => {
                        resets.entry(component).or_insert(recovered.reset_cause);
                        checkpoints.push((
                            placement.clone(),
                            self.empty_forced_branch_state_snapshot(&placement)?,
                        ));
                    }
                }
            }
        }

        self.prepare_ownership_handoff_wasm_guests(domain, &scheduled, &checkpoints)
            .await?;
        checkpoints.sort_by(|(left, _), (right, _)| {
            u8::from(left.state)
                .cmp(&u8::from(right.state))
                .then_with(|| {
                    left.branch_key
                        .as_ref()
                        .map(BranchKey::as_str)
                        .cmp(&right.branch_key.as_ref().map(BranchKey::as_str))
                })
        });
        if let Some(store) = self.inner.state_store.as_ref() {
            let entity_ref = entity.in_domain(domain);
            let transition = ForcedRuntimeStateRecoveryTransition {
                operation_id: &operation_id,
                source,
                destination,
                destination_incarnation,
                entity: &entity_ref,
                target_schedule_fingerprint,
            };
            store
                .persist_forced_recovery_preparation(&transition, &checkpoints)
                .map_err(|error| {
                    OwnershipHandoffError::persistence(error.current_context().clone())
                })?;
        }
        self.inner.prepared_forced_runtime_state_recoveries.insert(
            DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone()),
            PreparedForcedRuntimeStateRecovery {
                operation_id,
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
        scheduled: &ScheduledNode,
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
        scheduled: &ScheduledNode,
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

    fn empty_forced_branch_state_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
        match placement.state {
            RuntimeStateKind::Deduplicator => {
                ReplicatedDeduplicatorState::new(placement.clone(), None)
                    .map_err(OwnershipHandoffError::persistence)?
                    .latest_snapshot()
                    .map_err(OwnershipHandoffError::persistence)
            }
            RuntimeStateKind::WindowProcessor => {
                ReplicatedWindowProcessorState::new(placement.clone(), None)
                    .map_err(OwnershipHandoffError::persistence)?
                    .latest_snapshot()
                    .map_err(OwnershipHandoffError::persistence)
            }
            RuntimeStateKind::WasmProcessor => Ok(PersistedRuntimeStateEntry {
                lsm: 0,
                schema_fingerprint: placement.schema_fingerprint,
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
        match self.handle_state_sync_request(placement, None).await {
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
            tokio::task::consume_budget().await;
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            requests.push(self.request_state_sync_with_timeout(source, placement, None, remaining));
        }
        while let Some(response) = requests.next().await {
            tokio::task::consume_budget().await;
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

    pub(crate) fn ownership_handoff_entity_is_frozen(&self, entity: &DomainNodeRef) -> bool {
        self.inner
            .frozen_ownership_handoff_entities
            .contains_key(entity)
    }

    async fn checkpoint_entrypoint_branch_lifecycle(
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
                        .branch_runtimes()
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
            tokio::task::consume_budget().await;
            snapshots.push(runtime.checkpoint().await?);
        }
        if snapshots.len() == 1 {
            return Ok(snapshots.pop());
        }
        let mut entries = HashMap::default();
        let mut lsm = 0_u64;
        for snapshot in snapshots {
            lsm = lsm.max(snapshot.lsm);
            for (branch, last_ingestion) in decode_branch_lru_snapshot(&snapshot.payload)
                .map_err(OwnershipHandoffError::checkpoint)?
            {
                match entries.entry(branch) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if entry.get() < &last_ingestion {
                            entry.insert(last_ingestion);
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(last_ingestion);
                    }
                }
            }
        }
        let mut entries = entries.into_iter().collect::<Vec<_>>();
        entries.sort_by(|(left, _), (right, _)| {
            left.as_ref()
                .map(BranchKey::as_str)
                .cmp(&right.as_ref().map(BranchKey::as_str))
        });
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::BranchLru,
            entity.kind,
            entity.identifier.clone(),
            None,
        );
        Ok(Some(PersistedRuntimeStateEntry {
            lsm: lsm.checked_add(1).ok_or_else(|| {
                OwnershipHandoffError::checkpoint("branch lifecycle checkpoint revision overflowed")
            })?,
            schema_fingerprint: placement.schema_fingerprint,
            payload: encode_branch_lru_snapshot(&entries)
                .map_err(OwnershipHandoffError::checkpoint)?,
        }))
    }

    fn ownership_handoff_scheduled_node(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
    ) -> OwnershipHandoffResult<ScheduledNode> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Err(OwnershipHandoffError::schedule(format!(
                "{} '{}' is absent from the local schedule for domain '{}'",
                entity.kind.as_str(),
                entity.identifier.as_str(),
                domain.as_str()
            )));
        };
        execution
            .schedule
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
        let actual = Self::ownership_handoff_schedule_fingerprint(&execution.schedule)?;
        if actual != expected {
            return Err(OwnershipHandoffError::schedule(format!(
                "domain '{}' schedule changed during ownership handoff",
                domain.as_str()
            )));
        }
        Ok(())
    }

    fn node_has_branch_lifecycle(kind: ModelKind) -> bool {
        kind.is_processor() || matches!(kind, ModelKind::Ingestor | ModelKind::Reingestor)
    }

    fn expected_ownership_handoff_placements(
        &self,
        domain: &DomainName,
        node: &ScheduledNode,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> OwnershipHandoffResult<HashSet<RuntimeStatePlacement>> {
        let mut expected = HashSet::default();
        for (_, state) in Self::global_recovery_state_components(node) {
            expected.insert(self.state_placement(
                domain,
                state,
                node.kind(),
                node.identifier.clone(),
                None,
            ));
        }
        if !Self::node_has_branch_lifecycle(node.kind()) {
            return Ok(expected);
        }
        let branch_lru = self.state_placement(
            domain,
            RuntimeStateKind::BranchLru,
            node.kind(),
            node.identifier.clone(),
            None,
        );
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
            for (branch_key, _) in decode_branch_lru_snapshot(&snapshot.payload)
                .map_err(OwnershipHandoffError::checkpoint)?
            {
                expected.insert(self.state_placement(
                    domain,
                    state_kind,
                    node.kind(),
                    node.identifier.clone(),
                    branch_key,
                ));
            }
        }
        Ok(expected)
    }

    pub(crate) async fn prepare_ownership_handoff_state(
        &self,
        request: nervix_interconnect::PrepareOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        let nervix_interconnect::PrepareOwnershipHandoffStateRequest {
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
        self.verify_local_handoff_schedule(domain, base_schedule_fingerprint)?;
        let mut decoded = Vec::with_capacity(checkpoints.len());
        let mut placements = HashSet::default();
        for checkpoint in checkpoints {
            let placement = RuntimeStatePlacement::from_remote(checkpoint.placement)
                .map_err(OwnershipHandoffError::state)?;
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
                schema_fingerprint: checkpoint.snapshot.schema_fingerprint,
                payload: checkpoint.snapshot.payload,
            };
            if snapshot.schema_fingerprint != placement.schema_fingerprint {
                return Err(OwnershipHandoffError::state(format!(
                    "checkpoint fingerprint does not match {:?} state placement",
                    placement.state
                )));
            }
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
            u8::from(left.state)
                .cmp(&u8::from(right.state))
                .then_with(|| {
                    left.branch_key
                        .as_ref()
                        .map(BranchKey::as_str)
                        .cmp(&right.branch_key.as_ref().map(BranchKey::as_str))
                })
        });
        if let Some(existing) =
            self.inner
                .prepared_runtime_state_handoffs
                .get(&DomainNodeRef::node_in(
                    domain.clone(),
                    entity.kind,
                    entity.identifier.clone(),
                ))
            && existing.operation_id != operation_id
        {
            return Err(OwnershipHandoffError::participant(format!(
                "{} '{}' already has a different ownership handoff preparation",
                entity.kind.as_str(),
                entity.identifier.as_str()
            )));
        }
        if let Some(store) = self.inner.state_store.as_ref() {
            let entity_ref = entity.in_domain(domain);
            let transition = RuntimeStateHandoffTransition {
                operation_id: &operation_id,
                source: &source,
                destination: &destination,
                source_incarnation,
                destination_incarnation,
                entity: &entity_ref,
                base_schedule_fingerprint,
                target_schedule_fingerprint,
            };
            store
                .persist_handoff_preparation(&transition, &decoded)
                .map_err(|error| {
                    OwnershipHandoffError::persistence(error.current_context().clone())
                })?;
        }
        self.inner.prepared_runtime_state_handoffs.insert(
            DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone()),
            PreparedRuntimeStateHandoff {
                operation_id,
                source,
                destination,
                source_incarnation,
                destination_incarnation,
                base_schedule_fingerprint,
                target_schedule_fingerprint,
                activation_authorized: true,
                checkpoints: decoded,
            },
        );
        Ok(())
    }

    async fn prepare_ownership_handoff_wasm_guests(
        &self,
        domain: &DomainName,
        scheduled: &ScheduledNode,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> OwnershipHandoffResult<()> {
        let Some(processor) = scheduled.wasm_processor() else {
            return Ok(());
        };
        let input_relay = processor.from.first().ok_or_else(|| {
            OwnershipHandoffError::state(format!(
                "wasm processor '{}' has no input relay while preparing ownership handoff",
                processor.name.as_str()
            ))
        })?;
        let (input_schema, output_schemas) = {
            let execution = self.inner.executions.get(domain).ok_or_else(|| {
                OwnershipHandoffError::state(format!(
                    "domain '{}' has no execution while preparing wasm ownership handoff",
                    domain.as_str()
                ))
            })?;
            let input_schema = execution
                .relay_schemas
                .get(input_relay)
                .cloned()
                .ok_or_else(|| {
                    OwnershipHandoffError::state(format!(
                        "wasm processor '{}' input relay '{}' has no runtime schema",
                        processor.name.as_str(),
                        input_relay.as_str()
                    ))
                })?;
            let output_schemas = processor
                .output_routes
                .outputs()
                .map(|output| {
                    let schema = execution.relay_schemas.get(&output.relay).cloned();
                    let Some(schema) = schema else {
                        return Err(OwnershipHandoffError::state(format!(
                            "wasm processor '{}' output relay '{}' has no runtime schema",
                            processor.name.as_str(),
                            output.relay.as_str()
                        )));
                    };
                    Ok((output.relay.clone(), schema))
                })
                .collect::<OwnershipHandoffResult<Vec<_>>>()?;
            (input_schema, output_schemas)
        };
        let compiled = self
            .compile_wasm_processor_module(
                domain,
                &processor.name,
                &processor.resource,
                processor.resource_version,
                &processor.file,
            )
            .await
            .map_err(OwnershipHandoffError::wasm_restore)?;
        let domain_clock = self.bind_domain_clock(domain).map_err(|error| {
            OwnershipHandoffError::wasm_restore(format!(
                "failed to bind WASM processor '{}' to the domain clock: {error}",
                processor.name.as_str()
            ))
        })?;
        for (placement, snapshot) in checkpoints {
            tokio::task::consume_budget().await;
            if placement.state != RuntimeStateKind::WasmProcessor {
                continue;
            }
            let init = WasmBranchInit {
                domain_name: domain.as_str().to_string(),
                domain_type: "runtime".to_string(),
                branch_key: placement
                    .branch_key
                    .as_ref()
                    .map(|key| key.as_str().as_bytes().to_vec()),
                input_schema: input_schema.wasm_processor_schema(input_relay.as_str().to_string()),
                output_schemas: output_schemas
                    .iter()
                    .map(|(relay, schema)| schema.wasm_processor_schema(relay.as_str().to_string()))
                    .collect(),
            };
            let clock = RuntimeWasmDomainClock::new(domain_clock.clone()).map_err(|error| {
                OwnershipHandoffError::wasm_restore(format!(
                    "failed to snapshot WASM processor '{}' domain clock: {error}",
                    processor.name.as_str()
                ))
            })?;
            let restored_state =
                (!snapshot.payload.is_empty()).then_some(snapshot.payload.as_slice());
            compiled
                .compiled
                .instantiate_branch(processor.limits, init, Box::new(clock), restored_state)
                .await
                .map_err(|error| {
                    OwnershipHandoffError::wasm_restore(format!(
                        "wasm processor '{}' rejected transferred branch '{}': {error}",
                        processor.name.as_str(),
                        branch_key_display(&placement.branch_key)
                    ))
                })?;
        }
        Ok(())
    }

    fn validate_ownership_handoff_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: &PersistedRuntimeStateEntry,
    ) -> OwnershipHandoffResult<()> {
        match placement.state {
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
                decode_materialized_stream_snapshot(&snapshot.payload)
                    .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            }
            RuntimeStateKind::WasmProcessor => {}
            RuntimeStateKind::WindowProcessor => {
                ReplicatedWindowProcessorState::new(placement.clone(), Some(snapshot.clone()))
                    .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
            }
            RuntimeStateKind::BranchLru => {
                decode_branch_lru_snapshot(&snapshot.payload)
                    .map_err(OwnershipHandoffError::state)?;
            }
        }
        Ok(())
    }

    pub(super) fn activate_prepared_ownership_handoff_state(
        &self,
        domain: &DomainName,
        node: &ScheduledNode,
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
        if !prepared.activation_authorized {
            return Ok(());
        }
        if let Some(store) = self.inner.state_store.as_ref() {
            let transition = RuntimeStateHandoffTransition {
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
                        operation_id: prepared.operation_id.clone(),
                        snapshot: snapshot.clone(),
                    },
                );
            } else if placement.state == RuntimeStateKind::BranchLru {
                self.inner
                    .replicated_branch_lru_snapshots
                    .insert(placement.clone(), snapshot.clone());
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
            .is_some_and(|current| current.operation_id == prepared.operation_id);
        if remove {
            self.inner.prepared_runtime_state_handoffs.remove(&entity);
        }
        self.inner.activated_runtime_state_handoffs.insert(
            entity,
            ActivatedRuntimeStateHandoff {
                operation_id: prepared.operation_id,
                source: prepared.source,
                destination: prepared.destination,
                source_incarnation: prepared.source_incarnation,
                destination_incarnation: prepared.destination_incarnation,
                base_schedule_fingerprint: prepared.base_schedule_fingerprint,
                target_schedule_fingerprint: prepared.target_schedule_fingerprint,
            },
        );
        Ok(())
    }

    pub(super) fn activate_prepared_forced_ownership_recovery_state(
        &self,
        domain: &DomainName,
        node: &ScheduledNode,
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
        if transition.destination != *local_node_id
            || transition.state_recovery == OwnershipStateRecoveryOutcome::Complete
        {
            return Ok(());
        }
        let entity = DomainNodeRef::node_in(domain.clone(), node.kind(), node.identifier.clone());
        let local_incarnation = *self.inner.remote_dispatch.local_node_incarnation.read();
        let Some(local_incarnation) = local_incarnation else {
            return Err(Report::new(RuntimePersistenceError::MissingNodeIncarnation));
        };
        let prepared = self
            .inner
            .prepared_forced_runtime_state_recoveries
            .get(&entity)
            .map(|prepared| prepared.clone());
        let checkpoints = if let Some(store) = self.inner.state_store.as_ref() {
            let recovery = ForcedRuntimeStateRecoveryTransition {
                operation_id: &transition.id,
                source: &transition.source,
                destination: &transition.destination,
                destination_incarnation: local_incarnation,
                entity: &entity,
                target_schedule_fingerprint: schedule_fingerprint,
            };
            let Some(checkpoints) = store.activate_forced_recovery(&recovery)? else {
                return Ok(());
            };
            checkpoints
        } else {
            match prepared.as_ref() {
                Some(prepared)
                    if prepared.operation_id == transition.id
                        && prepared.destination_incarnation == local_incarnation
                        && prepared.target_schedule_fingerprint == schedule_fingerprint =>
                {
                    prepared.checkpoints.clone()
                }
                Some(_) | None => Vec::new(),
            }
        };
        self.remove_runtime_state_for_entity(domain, node.kind(), &node.identifier);
        for (placement, snapshot) in &checkpoints {
            if instantiate_now {
                self.inner.prepared_runtime_state_snapshots.insert(
                    placement.clone(),
                    PreparedRuntimeStateSnapshot {
                        operation_id: transition.id.clone(),
                        snapshot: snapshot.clone(),
                    },
                );
            } else if placement.state == RuntimeStateKind::BranchLru {
                self.inner
                    .replicated_branch_lru_snapshots
                    .insert(placement.clone(), snapshot.clone());
            } else {
                self.inner
                    .passive_runtime_state_snapshots
                    .insert(placement.clone(), snapshot.clone());
            }
        }
        self.inner
            .prepared_forced_runtime_state_recoveries
            .remove_if(&entity, |_, current| current.operation_id == transition.id);
        Ok(())
    }

    pub(crate) fn verify_ownership_handoff_activation(
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
                    transition.operation_id,
                    transition.domain,
                    transition.entity.kind,
                    &transition.entity.identifier,
                )
                .map_err(|error| {
                    OwnershipHandoffError::persistence(error.current_context().clone())
                })?
                .is_some_and(|activated| {
                    activated.operation_id == transition.operation_id
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

    pub(crate) fn authorize_persisted_ownership_handoff_activation(
        &self,
        request: &nervix_interconnect::ActivateOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<bool> {
        if self.verify_ownership_handoff_activation(request).is_ok() {
            return Ok(false);
        }
        let transition = OwnershipHandoffTransitionRef::from(request);
        self.verify_ownership_handoff_preparation_transition(transition)?;
        let key = transition.entity.in_domain(transition.domain);
        let mut prepared = self
            .inner
            .prepared_runtime_state_handoffs
            .get_mut(&key)
            .verified("the preparation was found and checked directly above");
        prepared.activation_authorized = true;
        Ok(true)
    }

    pub(crate) async fn rebuild_ownership_handoff_target(
        &self,
        local_node_id: &ClusterNodeName,
        domain: &DomainName,
        schedule: DomainSchedule,
    ) -> Result<(), RuntimeError> {
        let _apply = self.inner.schedule_apply_lock.lock().await;
        self.rebuild_domain_from_schedule(local_node_id, domain, Some(schedule), true)
            .await
    }

    fn remove_runtime_state_for_entity(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
    ) {
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
            .replicated_branch_lru_snapshots
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .passive_runtime_state_snapshots
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .pending_state_replica_syncs
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .pending_state_checkpoint_announcements
            .retain(|placement, _| !matches_entity(placement));
        self.inner
            .state_checkpoint_notifications
            .retain(|placement, _| !matches_entity(placement));
    }

    pub(super) fn take_prepared_runtime_state_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<PersistedRuntimeStateEntry> {
        self.inner
            .prepared_runtime_state_snapshots
            .remove(placement)
            .map(|(_, prepared)| prepared.snapshot)
    }

    fn take_transferred_runtime_state_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<PersistedRuntimeStateEntry> {
        if let Some(snapshot) = self.take_prepared_runtime_state_snapshot(placement) {
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
        operation_id: &str,
        domain: &DomainName,
        entity: &NodeRef,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let key = DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone());
        let remove = self
            .inner
            .prepared_runtime_state_handoffs
            .get(&key)
            .is_some_and(|prepared| prepared.operation_id == operation_id);
        if remove {
            self.inner.prepared_runtime_state_handoffs.remove(&key);
        }
        let remove = self
            .inner
            .activated_runtime_state_handoffs
            .get(&key)
            .is_some_and(|activated| activated.operation_id == operation_id);
        if remove {
            self.inner.activated_runtime_state_handoffs.remove(&key);
        }
        self.inner
            .prepared_runtime_state_snapshots
            .retain(|_, prepared| prepared.operation_id != operation_id);
        if let Some(store) = self.inner.state_store.as_ref() {
            store.discard_handoff_preparation(
                operation_id,
                domain,
                entity.kind,
                &entity.identifier,
            )?;
        }
        Ok(())
    }

    pub fn has_state_store(&self) -> bool {
        self.inner.state_store.is_some()
    }

    pub fn state_snapshot_interval(&self) -> Duration {
        self.inner.state_snapshot_interval
    }

    pub(crate) async fn handle_state_sync_request(
        &self,
        placement: &RuntimeStatePlacement,
        after_lsm: Option<u64>,
    ) -> Result<Option<PersistedRuntimeStateEntry>, String> {
        if let RuntimeStateKind::MaterializedRelay = placement.state {
            let mut entries = Vec::new();
            let mut latest_lsm = 0;
            let mut found = false;
            for state in self.inner.replicated_materialized_stream_states.iter() {
                let concrete = state.key();
                let read = ReplicatedMaterializedRelayState::read(state.value());
                if concrete.domain != placement.domain
                    || concrete.state != placement.state
                    || concrete.kind != placement.kind
                    || concrete.identifier != placement.identifier
                    || concrete.schema_fingerprint != placement.schema_fingerprint
                {
                    continue;
                }
                if let Some(requested) = placement.branch_key.as_ref()
                    && concrete
                        .branch_key
                        .as_ref()
                        .is_some_and(|concrete| concrete != requested)
                {
                    continue;
                }
                found = true;
                latest_lsm = latest_lsm.max(read.current_lsm());
                if let Some(requested) = placement.branch_key.as_ref() {
                    let key = Some(requested.clone());
                    if let Some(entry) =
                        self.visible_materialized_stream_remote_entry(concrete, &read, &key)?
                    {
                        entries.push(entry);
                    }
                } else {
                    entries
                        .extend(self.visible_materialized_stream_remote_entries(concrete, &read)?);
                }
            }
            if found {
                let snapshot = PersistedRuntimeStateEntry {
                    lsm: latest_lsm,
                    schema_fingerprint: placement.schema_fingerprint,
                    payload: encode_materialized_stream_snapshot_entries(&entries)
                        .map_err(|error| error.to_string())?,
                };
                return Ok(snapshot.is_after(after_lsm).then_some(snapshot));
            }
        }
        if let Some(state) = self.inner.replicated_deduplicator_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.is_after(after_lsm) {
                return Ok(Some(snapshot));
            }
            return Ok(None);
        }
        if let Some(state) = self.inner.replicated_kafka_offset_states.get(placement) {
            let snapshot = ReplicatedKafkaOffsetState::read(state.value())
                .latest_snapshot()
                .map_err(|error| error.to_string())?;
            if snapshot.is_after(after_lsm) {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self
            .inner
            .replicated_materialized_stream_states
            .get(placement)
        {
            let read = ReplicatedMaterializedRelayState::read(state.value());
            let entries = self.visible_materialized_stream_remote_entries(placement, &read)?;
            let snapshot = PersistedRuntimeStateEntry {
                lsm: read.current_lsm(),
                schema_fingerprint: placement.schema_fingerprint,
                payload: encode_materialized_stream_snapshot_entries(&entries)
                    .map_err(|error| error.to_string())?,
            };
            if snapshot.is_after(after_lsm) {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self.inner.replicated_window_processor_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.is_after(after_lsm) {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self.inner.replicated_wasm_processor_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.is_after(after_lsm) {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self
            .inner
            .replicated_branch_aggregated_states
            .get(placement)
        {
            let snapshot = state
                .latest_snapshot(&self.inner.metrics)
                .map_err(|error| error.to_string())?;
            if snapshot.is_after(after_lsm) {
                return Ok(Some(snapshot));
            }
        }
        if let Some(snapshot) = self.inner.replicated_branch_lru_snapshots.get(placement)
            && snapshot.is_after(after_lsm)
        {
            return Ok(Some(snapshot.clone()));
        }
        if let Some(store) = self.inner.state_store.as_ref()
            && let Some(snapshot) = store
                .latest_snapshot(placement)
                .map_err(|error| error.to_string())?
            && snapshot.is_after(after_lsm)
        {
            return Ok(Some(snapshot));
        }
        Ok(None)
    }

    pub(crate) fn runtime_state_placement_is_assigned_locally(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> bool {
        if !self.runtime_state_placement_is_current(placement) {
            return false;
        }
        let local_node_id = self.inner.remote_dispatch.local_node_id.read().clone();
        let Some(local_node_id) = local_node_id else {
            return false;
        };
        let Some(execution) = self.inner.executions.get(&placement.domain) else {
            return false;
        };
        let Some(node) = execution
            .schedule
            .nodes
            .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
        else {
            return false;
        };
        node.assigned_nodes.contains(&local_node_id)
    }

    pub(crate) fn handle_state_replication_ack(
        &self,
        node_id: &ClusterNodeName,
        ack: StateSyncAck,
    ) {
        if let Some(mut pending) = self
            .inner
            .pending_state_checkpoint_announcements
            .get_mut(&ack.placement)
        {
            let progress = pending.replica_progress.entry(node_id.clone()).or_default();
            *progress = (*progress).max(ack.lsm);
        }
        if let Some(state) = self
            .inner
            .replicated_kafka_offset_states
            .get(&ack.placement)
        {
            state.mark_replica_progress(node_id, ack.lsm);
        }
        if let Some(state) = self
            .inner
            .replicated_wasm_processor_states
            .get(&ack.placement)
        {
            state.mark_replica_progress(node_id, ack.lsm);
        }
        if let Some(state) = self
            .inner
            .replicated_branch_aggregated_states
            .get(&ack.placement)
        {
            state.mark_replica_progress(node_id, ack.lsm);
        }
    }

    pub(in crate::runtime) async fn request_state_sync(
        &self,
        target_node_id: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        after_lsm: u64,
    ) -> Result<Option<PersistedRuntimeStateEntry>, String> {
        self.request_state_sync_with_timeout(
            target_node_id,
            placement,
            Some(after_lsm),
            Duration::from_secs(5),
        )
        .await
    }

    pub(super) async fn request_state_sync_with_timeout(
        &self,
        target_node_id: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        after_lsm: Option<u64>,
        response_timeout: Duration,
    ) -> Result<Option<PersistedRuntimeStateEntry>, String> {
        let Some(dispatcher) = self.inner.remote_dispatcher.read().clone() else {
            return Err("remote dispatcher unavailable".to_string());
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
            .await?;
        response.result.map(|snapshot| {
            snapshot.map(|snapshot| PersistedRuntimeStateEntry {
                lsm: snapshot.lsm,
                schema_fingerprint: snapshot.schema_fingerprint,
                payload: snapshot.payload,
            })
        })
    }

    pub(in crate::runtime) async fn wait_for_kafka_offset_replica_quorum(
        &self,
        state: &KafkaOffsetStateRead,
        lsm: u64,
    ) -> Result<(), String> {
        if state.required_replica_acks() == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            if state.replica_quorum_satisfied(lsm) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for replica quorum for '{}' at lsm {}",
                    state.placement().identifier.as_str(),
                    lsm
                ));
            }
            tokio::select! {
                _ = state.wait_for_replication_progress() => {}
                _ = sleep_until(deadline) => {}
            }
        }
    }

    pub(in crate::runtime) async fn wait_for_wasm_processor_replica_quorum(
        &self,
        state: &ReplicatedWasmProcessorState,
        lsm: u64,
    ) -> Result<(), String> {
        if state.required_replica_acks == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            if state.replica_quorum_satisfied(lsm) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for replica quorum for wasm processor '{}' branch '{}' at \
                     lsm {}",
                    state.placement.identifier.as_str(),
                    state.placement.concrete_branch_key(),
                    lsm
                ));
            }
            tokio::select! {
                _ = state.replication_notify.notified() => {}
                _ = sleep_until(deadline) => {}
            }
        }
    }

    pub(in crate::runtime) async fn persist_kafka_offset_snapshot(
        &self,
        state: &KafkaOffsetStatePersistence,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Some(store) = &self.inner.state_store {
            store
                .persist_latest_snapshot(state.read().placement(), lsm, payload)
                .map_err(|error| error.to_string())?;
            state.record_persisted(lsm);
            self.notify_runtime_state_replicas(state.read().placement(), lsm);
        }
        self.wait_for_kafka_offset_replica_quorum(state.read(), lsm)
            .await
    }

    pub(in crate::runtime) async fn commit_domain_kafka_offset(
        &self,
        state: &KafkaOffsetStateOriginator,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), String> {
        let (lsm, payload) = state
            .apply_committed_offset(topic, partition, next_offset)
            .map_err(|error| error.to_string())?;
        self.persist_kafka_offset_snapshot(&state.persistence(), lsm, &payload)
            .await
    }

    pub(in crate::runtime) async fn reset_domain_kafka_offsets(
        &self,
        state: &KafkaOffsetStateOriginator,
        offsets: HashMap<KafkaTopicPartition, i64>,
    ) -> Result<(), String> {
        let (lsm, payload) = state
            .replace_offsets(offsets)
            .map_err(|error| error.to_string())?;
        self.persist_kafka_offset_snapshot(&state.persistence(), lsm, &payload)
            .await
    }

    pub(in crate::runtime) async fn persist_wasm_processor_snapshot(
        &self,
        state: &ReplicatedWasmProcessorState,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Some(store) = &self.inner.state_store {
            store
                .persist_latest_snapshot(&state.placement, lsm, payload)
                .map_err(|error| error.to_string())?;
            state.last_persisted_lsm.store(lsm, Ordering::SeqCst);
            state.dirty.store(false, Ordering::SeqCst);
            self.notify_runtime_state_replicas(&state.placement, lsm);
        }
        self.wait_for_wasm_processor_replica_quorum(state, lsm)
            .await
    }

    pub(in crate::runtime) fn update_materialized_stream_last_by_timestamp(
        &self,
        state: &MaterializedRelayStateOriginator,
        key: &Option<BranchKey>,
        record: &RuntimeRow,
    ) -> Result<(), error_stack::Report<StateAuthorityError>> {
        if state.update_last_by_timestamp(key, record)?.is_some() {
            self.inner.materialized_state_changed.notify_waiters();
        }
        Ok(())
    }

    pub(in crate::runtime) fn delete_materialized_stream_key(
        &self,
        state: &MaterializedRelayStateOriginator,
        key: &Option<BranchKey>,
    ) -> Result<(), error_stack::Report<StateAuthorityError>> {
        if state.remove_key(key)?.is_some() {
            self.inner.materialized_state_changed.notify_waiters();
        }
        Ok(())
    }

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
        let state = Arc::new(ReplicatedDeduplicatorState::new(
            placement.clone(),
            initial,
        )?);
        self.inner
            .replicated_deduplicator_states
            .insert(placement, state.clone());
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
                .insert(placement, state.clone());
            state
        };
        Ok(ReplicatedKafkaOffsetState::bind(&state, roles, local_node))
    }

    pub(in crate::runtime) fn replicated_materialized_stream_state(
        &self,
        placement: RuntimeStatePlacement,
        schema: StdArc<arrow_schema::Schema>,
        primary_node: Option<ClusterNodeName>,
        replica_nodes: Vec<ClusterNodeName>,
        local_node: Option<&ClusterNodeName>,
    ) -> Result<MaterializedRelayStateAssignment, RuntimePersistenceError> {
        let roles = StateReplicationRoles::new(primary_node, replica_nodes, 0);
        let transferred = self.take_transferred_runtime_state_snapshot(&placement);
        let state = if transferred.is_none()
            && let Some(existing) = self
                .inner
                .replicated_materialized_stream_states
                .get(&placement)
        {
            existing.clone()
        } else {
            let initial = match transferred {
                Some(snapshot) => Some(snapshot),
                None => self
                    .stored_runtime_state_snapshot(&placement)
                    .map_err(|error| error.current_context().clone())?,
            };
            let state = Arc::new(ReplicatedMaterializedRelayState::new(
                placement.clone(),
                schema,
                initial,
            )?);
            self.inner
                .replicated_materialized_stream_states
                .insert(placement, state.clone());
            state
        };
        Ok(ReplicatedMaterializedRelayState::bind(
            &state, roles, local_node,
        ))
    }

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
        let state = Arc::new(ReplicatedWindowProcessorState::new(
            placement.clone(),
            initial,
        )?);
        self.inner
            .replicated_window_processor_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_wasm_processor_state(
        &self,
        placement: RuntimeStatePlacement,
        replica_nodes: Vec<ClusterNodeName>,
        required_replica_acks: usize,
    ) -> Result<Arc<ReplicatedWasmProcessorState>, RuntimePersistenceError> {
        let transferred = self.take_transferred_runtime_state_snapshot(&placement);
        if transferred.is_none()
            && let Some(existing) = self.inner.replicated_wasm_processor_states.get(&placement)
        {
            return Ok(existing.clone());
        }
        let initial = match transferred {
            Some(snapshot) => Some(snapshot),
            None => self
                .stored_runtime_state_snapshot(&placement)
                .map_err(|error| error.current_context().clone())?,
        };
        let state = Arc::new(ReplicatedWasmProcessorState::new(
            placement.clone(),
            replica_nodes,
            required_replica_acks,
            initial,
        )?);
        self.inner
            .replicated_wasm_processor_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_branch_aggregated_state(
        &self,
        placement: RuntimeStatePlacement,
        primary_node: Option<ClusterNodeName>,
        physical_node_id: ClusterNodeName,
        replica_nodes: Vec<ClusterNodeName>,
        required_replica_acks: usize,
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
            replica_nodes,
            required_replica_acks,
            &self.inner.metrics,
            initial,
        )?);
        self.inner
            .replicated_branch_aggregated_states
            .insert(placement, state.clone());
        Ok(state)
    }
}

mod lifecycle;
mod tasks;

#[cfg(test)]
mod tests;
