//! Layer: data plane.
//! Owns: persistence and replica polling tasks for runtime state.
//! May depend on: runtime state carriers, the state store, interconnect, and vocabulary models.
//! Must not know: control-plane transactions, NSPL parsing, or edge protocols.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "replica task and placement installation establish ownership-bound execution \
                  lifetimes"
    )
)]

use super::*;

impl Runtime {
    pub(in crate::runtime) fn spawn_kafka_offset_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: KafkaOffsetStatePersistence,
    ) -> Option<JoinHandle<()>> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(nervix_primitives::task::spawn(async move {
            let flush_latest_snapshot =
                |state: &KafkaOffsetStatePersistence,
                 store: &RuntimeStateStore|
                 -> Result<Option<u64>, RuntimePersistenceError> {
                    if !state.is_dirty() {
                        return Ok(None);
                    }
                    let snapshot = state.read().latest_snapshot()?;
                    if snapshot.lsm <= state.last_persisted_lsm() {
                        return Ok(None);
                    }
                    store.persist_latest_snapshot(
                        state.read().placement(),
                        snapshot.lsm,
                        &snapshot.payload,
                    )?;
                    state.record_persisted(snapshot.lsm);
                    Ok(Some(snapshot.lsm))
                };
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            match flush_latest_snapshot(&state, &store) {
                                Ok(Some(lsm)) => {
                                    let offsets = state.read();
                                    runtime.announce_checkpoint(
                                        offsets.placement(), offsets.replication(), lsm,
                                    );
                                }
                                Ok(None) => {}
                                Err(error) => warn!(error = %error, "failed to flush kafka offset snapshot during shutdown"),
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        match flush_latest_snapshot(&state, &store) {
                            Ok(Some(lsm)) => {
                                let offsets = state.read();
                                runtime.announce_checkpoint(
                                    offsets.placement(), offsets.replication(), lsm,
                                );
                            }
                            Ok(None) => {}
                            Err(error) => warn!(error = %error, "failed to persist kafka offset snapshot"),
                        }
                    }
                }
            }
        }))
    }

    /// Spawn the task that keeps one branch state current for its replicas and on disk.
    ///
    /// At least once per replication poll interval it asks the branch task to publish state that
    /// changed, so replicas follow the branch without anything reading its live state. On every
    /// snapshot interval, and once more when the branch stops, it also persists what was published
    /// last, including when the branch task is gone and can no longer publish.
    pub(in crate::runtime) fn spawn_published_branch_state_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: PublishedBranchState,
        snapshot_requests: mpsc::Sender<ProcessorSnapshotRequest>,
    ) -> Option<JoinHandle<()>> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let publication_interval =
            snapshot_interval.min(self.inner.state_replication_poll_interval);
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(nervix_primitives::task::spawn(async move {
            let mut next_persist = Instant::now() + snapshot_interval;
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            if let Err(error) = state.request_publication(&snapshot_requests).await {
                                warn!(
                                    kind = state.placement().kind.as_str(),
                                    error = %error,
                                    "failed to publish branch state during shutdown"
                                );
                            }
                            match state.persist_published(&store, &runtime.inner.executor).await {
                                Ok(Some(lsm)) => runtime.announce_checkpoint(
                                    state.placement(), state.replication(), lsm,
                                ),
                                Ok(None) => {}
                                Err(error) => warn!(
                                    kind = state.placement().kind.as_str(),
                                    error = %error,
                                    "failed to flush branch state snapshot during shutdown"
                                ),
                            }
                            break;
                        }
                    }
                    _ = sleep(publication_interval) => {
                        if let Err(error) = state.request_publication(&snapshot_requests).await {
                            warn!(
                                kind = state.placement().kind.as_str(),
                                error = %error,
                                "failed to publish branch state"
                            );
                        }
                        if Instant::now() < next_persist {
                            continue;
                        }
                        next_persist = Instant::now() + snapshot_interval;
                        match state.persist_published(&store, &runtime.inner.executor).await {
                            Ok(Some(lsm)) => runtime.announce_checkpoint(
                                state.placement(), state.replication(), lsm,
                            ),
                            Ok(None) => {}
                            Err(error) => warn!(
                                kind = state.placement().kind.as_str(),
                                error = %error,
                                "failed to persist branch state snapshot"
                            ),
                        }
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_materialized_stream_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: MaterializedRelayStatePersistence,
    ) -> Option<JoinHandle<()>> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let executor = self.inner.executor.clone();
        Some(nervix_primitives::task::spawn(async move {
            let flush_latest_snapshot =
                async |state: &MaterializedRelayStatePersistence,
                       store: &Arc<RuntimeStateStore>| {
                    if !state.is_dirty() {
                        return Ok(None);
                    }
                    let last_persisted = state.last_persisted_lsm();
                    let sealed = state
                        .read()
                        .seal_after(
                            &executor,
                            &runtime.inner.snapshot_staging,
                            Some(last_persisted),
                        )
                        .await;
                    let sealed = match sealed {
                        Ok(Some(sealed)) => sealed,
                        Ok(None) => return Ok(None),
                        Err(error) => {
                            return Err(RuntimePersistenceError::EncodeState(error.to_string()));
                        }
                    };
                    let revision = sealed.descriptor.revision;
                    let placement = state.read().placement().clone();
                    let writer = store.checkpoint_stream_writer();
                    // Publishing a generation is filesystem work with a durability barrier, so it
                    // runs on the storage workers rather than on the async worker this task holds.
                    let reservation = executor
                        .reserve(
                            nervix_execution::MemoryClass::Bulk,
                            super::super::RESTORE_STATE_WORKING_BYTES,
                        )
                        .await
                        .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
                    executor
                        .run_storage(
                            nervix_execution::StorageClass::Filesystem,
                            reservation,
                            move |_charge, cancellation| {
                                let file = std::fs::File::open(sealed.artifact.path()).map_err(
                                    |error| RuntimePersistenceError::EncodeState(error.to_string()),
                                )?;
                                writer
                                    .publish_checkpoint_stream(
                                        &placement,
                                        super::super::state_store::generation::CheckpointMetadata {
                                            lsm: revision,
                                            length: sealed.descriptor.length,
                                            digest: sealed.descriptor.digest,
                                        },
                                        file,
                                        || {
                                            cancellation.check().change_context(
                                                RuntimePersistenceError::RestoreRead,
                                            )
                                        },
                                    )
                                    .map_err(|error| error.current_context().clone())
                            },
                        )
                        .await
                        .map_err(|error| {
                            RuntimePersistenceError::EncodeState(error.to_string())
                        })??;
                    state.record_persisted(revision);
                    Ok::<Option<u64>, RuntimePersistenceError>(Some(revision))
                };
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            match flush_latest_snapshot(&state, &store).await {
                                Ok(Some(lsm)) => {
                                    let materialized = state.read();
                                    runtime.announce_checkpoint(
                                        materialized.placement(), materialized.replication(), lsm,
                                    );
                                }
                                Ok(None) => {}
                                Err(error) => warn!(error = %error, "failed to flush materialized relay snapshot during shutdown"),
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        match flush_latest_snapshot(&state, &store).await {
                            Ok(Some(lsm)) => {
                                let materialized = state.read();
                                runtime.announce_checkpoint(
                                    materialized.placement(), materialized.replication(), lsm,
                                );
                            }
                            Ok(None) => {}
                            Err(error) => warn!(error = %error, "failed to persist materialized relay snapshot"),
                        }
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_branch_aggregated_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedBranchAggregatedState>,
    ) -> Option<JoinHandle<()>> {
        let store = self.inner.state_store.as_ref()?.clone();
        let metrics = self.inner.metrics.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(nervix_primitives::task::spawn(async move {
            let flush_latest_snapshot =
                |state: &ReplicatedBranchAggregatedState,
                 metrics: &RuntimeMetrics,
                 store: &RuntimeStateStore| {
                    let Some(snapshot) = state.snapshot_to_persist(metrics)? else {
                        return Ok(None);
                    };
                    store
                        .persist_latest_snapshot(&state.placement, snapshot.lsm, &snapshot.payload)
                        .map_err(Report::new)?;
                    state.persisted(snapshot.lsm);
                    Ok::<Option<u64>, Report<RuntimePersistenceError>>(Some(snapshot.lsm))
                };
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            match flush_latest_snapshot(&state, &metrics, &store) {
                                Ok(Some(lsm)) => runtime.announce_checkpoint(
                                    &state.placement, state.replication(), lsm,
                                ),
                                Ok(None) => {}
                                Err(error) => warn!(error = %error, "failed to flush branch-aggregated state snapshot during shutdown"),
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        match flush_latest_snapshot(&state, &metrics, &store) {
                            Ok(Some(lsm)) => runtime.announce_checkpoint(
                                &state.placement, state.replication(), lsm,
                            ),
                            Ok(None) => {}
                            Err(error) => warn!(error = %error, "failed to persist branch-aggregated state snapshot"),
                        }
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_kafka_offset_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: KafkaOffsetSnapshotInstaller,
    ) -> Option<JoinHandle<()>> {
        let offsets = state.read();
        let primary_node = offsets.primary_node()?;
        let placement = offsets.placement().clone();
        let poll_interval = self.inner.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(nervix_primitives::task::spawn(async move {
            let offsets = state.read();
            let mut initial_sync_pending = true;
            loop {
                nervix_primitives::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        offsets.replication(),
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                let after_lsm = offsets.current_lsm();
                let synchronized = nervix_primitives::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    synchronized = runtime.sync_kafka_offsets_from(
                        &primary_node, &state, after_lsm,
                    ) => synchronized,
                };
                match synchronized {
                    Ok(lsm) => {
                        let dispatcher = runtime.inner.remote_dispatcher.load_full();
                        if let Some(dispatcher) = dispatcher
                            && let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: placement.to_remote(),
                                                lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                        {
                            warn!(node_id = %dispatcher.local_node_id(), error = %error, "failed to acknowledge replicated kafka offset snapshot");
                        }
                    }
                    Err(error) => {
                        warn!(error = ?error, "failed to sync replicated kafka offsets");
                    }
                }
            }
        }))
    }

    /// Spawn the replica task that keeps one branch-keyed entity current on this node: the
    /// entity's branch lifecycle and, for a deduplicator, window or WASM processor, the state of
    /// each of its branches.
    ///
    /// The task retains the entity's lifecycle handle and owns everything it learns of the
    /// entity's branch checkpoints, so each round asks the owner only what changed since the
    /// previous one. It runs a round when it starts, when the owner announces a checkpoint, and
    /// once every replication poll interval, which catches up a checkpoint whose announcement was
    /// lost.
    pub(in crate::runtime) fn spawn_branch_state_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        domain: &DomainName,
        node: &ExecutionNode,
    ) -> error_stack::Result<Option<JoinHandle<()>>, StateIdentityError> {
        let branch_states = match node.kind() {
            ModelKind::Deduplicator => Some(RuntimeStateKind::Deduplicator),
            ModelKind::WasmProcessor => Some(RuntimeStateKind::WasmProcessor),
            ModelKind::WindowProcessor => Some(RuntimeStateKind::WindowProcessor),
            ModelKind::Ingestor
            | ModelKind::Reingestor
            | ModelKind::Inferencer
            | ModelKind::Junction
            | ModelKind::Correlator
            | ModelKind::Reorderer => None,
            _ => return Ok(None),
        };
        let Some(primary_node) = node.execution_node().cloned() else {
            return Ok(None);
        };
        let branch_lru = self.state_placement(
            domain,
            RuntimeStateKind::BranchLru,
            node.kind(),
            node.identifier.clone(),
            None,
        )?;
        let poll_interval = self.inner.state_replication_poll_interval;
        let lifecycle = self.replicated_branch_lifecycle(&branch_lru);
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Ok(Some(nervix_primitives::task::spawn(async move {
            if let Err(error) = runtime.restore_replica_branch_lifecycle(&branch_lru, &lifecycle) {
                warn!(error = %error, "failed to read the stored replicated branch lifecycle");
            }
            let owner = RemoteStateOwner::new(runtime.clone(), primary_node);
            let mut checkpoints = ReplicaBranchCheckpoints::default();
            let mut initial_sync_pending = true;
            loop {
                nervix_primitives::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        lifecycle.replication(),
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                runtime
                    .catch_up_replica_branches(
                        &owner,
                        &branch_lru,
                        &lifecycle,
                        &mut checkpoints,
                        branch_states,
                    )
                    .await;
            }
        })))
    }

    pub(in crate::runtime) fn spawn_materialized_stream_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: MaterializedRelaySnapshotInstaller,
    ) -> Option<JoinHandle<()>> {
        let primary_node = state.read().primary_node()?;
        let poll_interval = self.inner.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(nervix_primitives::task::spawn(async move {
            let materialized = state.read();
            let mut initial_sync_pending = true;
            loop {
                nervix_primitives::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        materialized.replication(),
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                let after_lsm = materialized.current_lsm();
                match runtime
                    .install_materialized_snapshot_from(
                        &primary_node,
                        materialized,
                        &state,
                        Some(after_lsm),
                    )
                    .await
                {
                    Ok(Some(revision)) => {
                        runtime.inner.materialized_state_changed.notify_waiters();
                        let dispatcher = runtime.inner.remote_dispatcher.load_full();
                        if let Some(dispatcher) = dispatcher
                            && let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: materialized.placement().to_remote(),
                                                lsm: revision,
                                            },
                                        ),
                                    ),
                                )
                                .await
                        {
                            warn!(node_id = %dispatcher.local_node_id(), error = %error, "failed to acknowledge replicated materialized relay snapshot");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %error, "failed to sync replicated materialized relay state");
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_branch_aggregated_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedBranchAggregatedState>,
    ) -> Option<JoinHandle<()>> {
        let primary_node = state.primary_node()?;
        let poll_interval = self.inner.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(nervix_primitives::task::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                nervix_primitives::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        state.replication(),
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                let after_lsm = state.current_lsm.current();
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &state.placement,
                        Some(after_lsm),
                        nervix_interconnect::StateSyncRequest::TIMEOUT,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) = state.apply_snapshot(
                            &runtime.inner.metrics,
                            snapshot.lsm,
                            &snapshot.payload,
                        ) {
                            warn!(error = %error, "failed to apply replicated branch-aggregated state snapshot");
                            continue;
                        }
                        let dispatcher = runtime.inner.remote_dispatcher.load_full();
                        if let Some(dispatcher) = dispatcher
                            && let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: state.placement.to_remote(),
                                                lsm: snapshot.lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                        {
                            warn!(node_id = %dispatcher.local_node_id(), error = %error, "failed to acknowledge replicated branch-aggregated state snapshot");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %error, "failed to sync replicated branch-aggregated state");
                    }
                }
            }
        }))
    }

    /// Installs what every runtime state of `schedule`'s domain is keyed by: each node's schema
    /// fingerprint and, for a WASM processor, the guest-state generation of every branch.
    ///
    /// A node the schedule still carries keeps its identity throughout: each one is written before
    /// any stale node is dropped, so a concurrent [`Self::state_placement`] always resolves to the
    /// state the node already owns. Emptying the domain first would expose a window where a
    /// scheduled node has no identity, and a placement resolved in that window cannot address the
    /// state the node owns. Relocation rebuilds these while the relocating node's own state task is
    /// still reading them, which is exactly when that window is observed.
    pub(in crate::runtime) fn state_assignment(
        &self,
        node: &DomainNodeRef,
    ) -> SharedStateAssignment {
        if let Some(slot) = self.inner.state_identities.get(node) {
            return slot.clone();
        }
        // Schedule application is the single registration owner; tasks bind only after publication.
        let slot = Arc::new(ArcSwapOption::empty());
        self.inner
            .state_identities
            .insert(node.clone(), slot.clone());
        self.inner
            .state_replication_routing
            .register_assignment(node, &slot);
        slot
    }

    pub(in crate::runtime) fn publish_state_assignment(
        &self,
        node: DomainNodeRef,
        assignment: ScheduledStateAssignment,
    ) {
        self.state_assignment(&node)
            .store(Some(StdArc::new(assignment)));
    }

    pub(in crate::runtime) fn install_state_identities(&self, revision: &ExecutionRevision) {
        self.install_state_identities_from_nodes(&revision.domain, revision.nodes.values());
    }

    fn install_state_identities_from_nodes<'a>(
        &self,
        domain: &DomainName,
        nodes: impl Iterator<Item = &'a ExecutionNode>,
    ) {
        let start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        let mut scheduled = HashSet::default();
        for node in nodes {
            let node_ref =
                DomainNodeRef::node_in(domain.clone(), node.kind(), node.identifier.clone());
            let executors = node
                .assigned_nodes
                .iter()
                .filter(|owner| node.executes_on(owner))
                .cloned()
                .collect();
            let replicas = node.replica_nodes().into_iter().cloned().collect();
            self.publish_state_assignment(
                node_ref.clone(),
                ScheduledStateAssignment {
                    identity: ScheduledStateIdentity {
                        schema_fingerprint: Self::state_schema_fingerprint(node, start_version),
                        wasm_state_generations: node.wasm_state_generations().cloned(),
                    },
                    checkpoint_owners: Some(CheckpointOwners {
                        primary: node.execution_node().cloned(),
                        executors,
                        replicas,
                    }),
                },
            );
            scheduled.insert(node_ref);
        }
        self.retain_state_identities(domain, &scheduled);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_schedule_state_identities(&self, schedule: &DomainSchedule) {
        let nodes = schedule
            .nodes
            .values()
            .map(|node| ExecutionNode::from_scheduled(node, schedule))
            .collect::<Vec<_>>();
        self.install_state_identities_from_nodes(&schedule.domain, nodes.iter());
    }

    /// Before cluster assignment, preserve an existing guest-state generation while installing
    /// the schema identities of the unplaced revision.
    pub(in crate::runtime) fn install_state_identities_from_unplaced_revision(
        &self,
        domain: &DomainName,
        revision: &ExecutionRevision,
    ) {
        self.install_state_identities_from_unplaced_nodes(domain, revision.nodes.values());
    }

    fn install_state_identities_from_unplaced_nodes<'a>(
        &self,
        domain: &DomainName,
        nodes: impl Iterator<Item = &'a ExecutionNode>,
    ) {
        let start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        let mut active = HashSet::default();
        for node in nodes {
            let node_ref =
                DomainNodeRef::node_in(domain.clone(), node.kind(), node.identifier.clone());
            let schema_fingerprint = Self::state_schema_fingerprint(node, start_version);
            let slot = self.state_assignment(&node_ref);
            let mut assignment = match slot.load_full() {
                Some(current) => (*current).clone(),
                None => ScheduledStateAssignment {
                    identity: ScheduledStateIdentity {
                        schema_fingerprint,
                        wasm_state_generations: None,
                    },
                    checkpoint_owners: None,
                },
            };
            assignment.identity.schema_fingerprint = schema_fingerprint;
            slot.store(Some(StdArc::new(assignment)));
            active.insert(node_ref);
        }
        self.retain_state_identities(domain, &active);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_state_identities_from_graph(
        &self,
        domain: &DomainName,
        nodes: &[ScheduledNode],
    ) {
        let schedule = DomainSchedule::new(domain.clone(), nodes.to_vec(), Vec::new());
        let nodes = schedule
            .nodes
            .values()
            .map(|node| ExecutionNode::from_scheduled(node, &schedule))
            .collect::<Vec<_>>();
        self.install_state_identities_from_unplaced_nodes(domain, nodes.iter());
    }

    /// The fingerprint every schema-bound runtime state of `node` is keyed by in a domain started
    /// `start_version` times.
    ///
    /// Materialized relay state also belongs to the domain start that began it, so a START resets
    /// it: its fingerprint covers the start version beside the node's schemas.
    fn state_schema_fingerprint(node: &ExecutionNode, start_version: u64) -> SchemaFingerprint {
        if !node.materialized_relay {
            return node.schema_fingerprint;
        }
        node.schema_fingerprint.materialized_at(start_version)
    }

    pub(in crate::runtime) fn clear_state_identities(&self, domain: &DomainName) {
        self.retain_state_identities(domain, &HashSet::default());
    }

    /// Drops the identities of `domain` that `keep` no longer names, leaving the rest in place.
    fn retain_state_identities(&self, domain: &DomainName, keep: &HashSet<DomainNodeRef>) {
        let stale = self
            .inner
            .state_identities
            .iter()
            .filter_map(|entry| {
                (&entry.key().domain == domain && !keep.contains(entry.key()))
                    .then(|| entry.key().clone())
            })
            .collect::<Vec<_>>();
        for key in stale {
            if let Some(slot) = self.inner.state_identities.get(&key) {
                slot.store(None);
            }
            self.inner.state_identities.remove(&key);
            self.inner.state_replication_routing.withdraw_entity(&key);
        }
    }

    /// The placement of runtime state of `state` for `branch_key`, in the identity the committed
    /// schedule publishes for its node.
    ///
    /// Branch-aggregated metrics and Kafka offsets depend on no schema and are placed without one.
    /// Every other kind is placed under the schema fingerprint the schedule publishes for the node,
    /// and WASM guest state also in the generation it names for the branch, so neither can be
    /// placed for a node that no schedule has published them for.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the typed model-name conversion")
    )]
    pub(in crate::runtime) fn state_placement(
        &self,
        domain: &DomainName,
        state: RuntimeStateKind,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        branch_key: Option<BranchKey>,
    ) -> error_stack::Result<RuntimeStatePlacement, StateIdentityError> {
        let identifier = identifier.into();
        let state = match state {
            RuntimeStateKind::BranchAggregated => RuntimeState::BranchAggregated,
            RuntimeStateKind::KafkaOffset => RuntimeState::KafkaOffset,
            RuntimeStateKind::Correlator
            | RuntimeStateKind::Deduplicator
            | RuntimeStateKind::MaterializedRelay
            | RuntimeStateKind::WasmProcessor
            | RuntimeStateKind::WindowProcessor
            | RuntimeStateKind::BranchLru => {
                let node = DomainNodeRef::node_in(domain.clone(), kind, identifier.clone());
                let assignment = self
                    .inner
                    .state_replication_routing
                    .assignment_for_entity(&node)
                    .and_then(|slot| slot.load_full());
                let Some(assignment) = assignment else {
                    return Err(Report::new(
                        StateIdentityError::SchemaFingerprintUnpublished {
                            domain: domain.clone(),
                            kind,
                            identifier,
                        },
                    ));
                };
                let branch = branch_key.as_ref().map(BranchKey::fingerprint);
                let Some(state) = assignment.identity.state_of(state, branch.as_ref()) else {
                    return Err(Report::new(StateIdentityError::GenerationUnpublished {
                        domain: domain.clone(),
                        kind,
                        identifier,
                    }));
                };
                state
            }
        };
        Ok(RuntimeStatePlacement {
            domain: domain.clone(),
            state,
            kind,
            identifier,
            branch_key,
        })
    }

    /// Whether `placement` still names the state the committed schedule keys this node's state by.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    pub(in crate::runtime) fn runtime_state_placement_is_current(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> bool {
        let Some(slot) = self.inner.state_replication_routing.assignment(placement) else {
            return false;
        };
        let Some(assignment) = slot.load_full() else {
            return false;
        };
        assignment.names(placement)
    }

    pub(in crate::runtime) fn purge_stale_runtime_state(
        &self,
        domain: &DomainName,
    ) -> Result<(), error_stack::Report<RuntimePersistenceError>> {
        self.inner.state_replication_routing.purge_stale(domain);
        let stale_deduplicators = self
            .inner
            .replicated_deduplicator_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_deduplicators {
            self.inner.replicated_deduplicator_states.remove(&placement);
        }
        let stale_materialized = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_materialized {
            self.inner
                .replicated_materialized_stream_states
                .remove(&placement);
        }
        let stale_windows = self
            .inner
            .replicated_window_processor_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_windows {
            self.inner
                .replicated_window_processor_states
                .remove(&placement);
        }
        let stale_wasm = self
            .inner
            .replicated_wasm_processor_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_wasm {
            self.inner
                .replicated_wasm_processor_states
                .remove(&placement);
        }
        let stale_offsets = self
            .inner
            .replicated_kafka_offset_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_offsets {
            self.inner.replicated_kafka_offset_states.remove(&placement);
        }
        let stale_aggregates = self
            .inner
            .replicated_branch_aggregated_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_aggregates {
            self.inner
                .replicated_branch_aggregated_states
                .remove(&placement);
        }
        let stale_presences = self
            .inner
            .relay_branch_presences
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_presences {
            self.inner.relay_branch_presences.remove(&placement);
        }

        if let Some(store) = self.inner.state_store.as_ref() {
            let current = self
                .inner
                .state_identities
                .iter()
                .filter_map(|entry| {
                    let key = entry.key();
                    if &key.domain != domain {
                        return None;
                    }
                    let assignment = entry.load_full()?;
                    Some((key.node.clone(), assignment.identity.clone()))
                })
                .collect::<HashMap<_, _>>();
            store.purge_stale_state_identities(domain, &current)?;
        }
        Ok(())
    }

    pub(in crate::runtime) fn clear_runtime_state_for_domain(&self, domain: &DomainName) {
        self.inner.state_replication_routing.retire_domain(domain);
        let placements = self
            .inner
            .replicated_deduplicator_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner.replicated_deduplicator_states.remove(&placement);
        }
        let placements = self
            .inner
            .replicated_kafka_offset_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner.replicated_kafka_offset_states.remove(&placement);
        }
        let placements = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner
                .replicated_materialized_stream_states
                .remove(&placement);
        }
        let placements = self
            .inner
            .replicated_window_processor_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner
                .replicated_window_processor_states
                .remove(&placement);
        }
        let placements = self
            .inner
            .replicated_wasm_processor_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner
                .replicated_wasm_processor_states
                .remove(&placement);
        }
        let placements = self
            .inner
            .replicated_branch_aggregated_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner
                .replicated_branch_aggregated_states
                .remove(&placement);
        }
        self.inner
            .replicated_branch_lifecycles
            .retain(|placement, _| &placement.domain != domain);
        self.inner
            .passive_runtime_state_snapshots
            .retain(|placement, _| &placement.domain != domain);
    }
}
