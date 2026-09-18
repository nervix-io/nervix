//! Layer: data plane.
//! Owns: persistence and replica polling tasks for runtime state.
//! May depend on: runtime state carriers, the state store, interconnect, and vocabulary models.
//! Must not know: control-plane transactions, NSPL parsing, or edge protocols.

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
        Some(tokio::spawn(async move {
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
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            match flush_latest_snapshot(&state, &store) {
                                Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                    state.read().placement(), lsm,
                                ),
                                Ok(None) => {}
                                Err(error) => warn!(error = %error, "failed to flush kafka offset snapshot during shutdown"),
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        match flush_latest_snapshot(&state, &store) {
                            Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                state.read().placement(), lsm,
                            ),
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
        Some(tokio::spawn(async move {
            let mut next_persist = Instant::now() + snapshot_interval;
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            if let Err(error) = state.request_publication(&snapshot_requests).await {
                                warn!(
                                    kind = state.placement().kind.as_str(),
                                    error = %error,
                                    "failed to publish branch state during shutdown"
                                );
                            }
                            match state.persist_published(&store) {
                                Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                    state.placement(), lsm,
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
                        match state.persist_published(&store) {
                            Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                state.placement(), lsm,
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
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                async |state: &MaterializedRelayStatePersistence,
                       store: &Arc<RuntimeStateStore>| {
                    if !state.is_dirty() {
                        return Ok(None);
                    }
                    let last_persisted = state.last_persisted_lsm();
                    let sealed = state
                        .read()
                        .seal_after(&executor, Some(last_persisted))
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
                    let store = store.clone();
                    // Publishing a generation is filesystem work with a durability barrier, so it
                    // runs on the storage workers rather than on the async worker this task holds.
                    let reservation = executor
                        .reserve(nervix_execution::MemoryClass::Bulk, 1)
                        .await
                        .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
                    executor
                        .run_storage(
                            nervix_execution::StorageClass::Filesystem,
                            reservation,
                            move |_charge, _cancellation| {
                                store.publish_sealed_snapshot(
                                    &placement,
                                    revision,
                                    sealed.bytes.as_ref(),
                                )
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
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            match flush_latest_snapshot(&state, &store).await {
                                Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                    state.read().placement(), lsm,
                                ),
                                Ok(None) => {}
                                Err(error) => warn!(error = %error, "failed to flush materialized relay snapshot during shutdown"),
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        match flush_latest_snapshot(&state, &store).await {
                            Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                state.read().placement(), lsm,
                            ),
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
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                |state: &ReplicatedBranchAggregatedState,
                 metrics: &RuntimeMetrics,
                 store: &RuntimeStateStore| {
                    if !state.dirty.load(Ordering::SeqCst) {
                        return Ok(None);
                    }
                    let snapshot = state.latest_snapshot(metrics)?;
                    if snapshot.lsm <= state.last_persisted_lsm.load(Ordering::SeqCst) {
                        return Ok(None);
                    }
                    store.persist_latest_snapshot(
                        &state.placement,
                        snapshot.lsm,
                        &snapshot.payload,
                    )?;
                    state
                        .last_persisted_lsm
                        .store(snapshot.lsm, Ordering::SeqCst);
                    state.dirty.store(false, Ordering::SeqCst);
                    Ok::<Option<u64>, RuntimePersistenceError>(Some(snapshot.lsm))
                };
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            match flush_latest_snapshot(&state, &metrics, &store) {
                                Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                    &state.placement, lsm,
                                ),
                                Ok(None) => {}
                                Err(error) => warn!(error = %error, "failed to flush branch-aggregated state snapshot during shutdown"),
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        match flush_latest_snapshot(&state, &metrics, &store) {
                            Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                &state.placement, lsm,
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
        let notification = self.state_checkpoint_notification(&placement);
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        &notification,
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                let after_lsm = state.read().current_lsm();
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &placement,
                        Some(after_lsm),
                        poll_interval,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) = state.install_snapshot(snapshot.lsm, &snapshot.payload)
                        {
                            warn!(error = %error, "failed to apply replicated kafka offset snapshot");
                            break;
                        }
                        let dispatcher = runtime.inner.remote_dispatcher.load_full();
                        if let Some(dispatcher) = dispatcher
                            && let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: placement.to_remote(),
                                                lsm: snapshot.lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                        {
                            warn!(node_id = %dispatcher.local_node_id(), error = %error, "failed to acknowledge replicated kafka offset snapshot");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %error, "failed to sync replicated kafka offsets");
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_branch_state_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        domain: &DomainName,
        node: &ScheduledNode,
    ) -> error_stack::Result<Option<JoinHandle<()>>, StateIdentityError> {
        let state_kind = match node.kind() {
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
        let notification = self.state_checkpoint_notification(&branch_lru);
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Ok(Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        &notification,
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                let after_lsm = match runtime.passive_state_replica_lsm(&branch_lru) {
                    Ok(lsm) => lsm,
                    Err(error) => {
                        warn!(error = %error, "failed to read replicated branch lifecycle progress");
                        None
                    }
                };
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &branch_lru,
                        after_lsm,
                        poll_interval,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) = runtime
                            .install_passive_state_replica_snapshot(
                                &primary_node,
                                &branch_lru,
                                snapshot,
                            )
                            .await
                        {
                            warn!(error = %error, "failed to install replicated branch lifecycle checkpoint");
                            continue;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %error, "failed to sync replicated branch lifecycle state");
                        continue;
                    }
                }
                let Some(state_kind) = state_kind else {
                    continue;
                };
                let branch_snapshot = match runtime
                    .inner
                    .replicated_branch_lru_snapshots
                    .get(&branch_lru)
                    .map(|snapshot| snapshot.clone())
                {
                    Some(snapshot) => snapshot,
                    None => continue,
                };
                let branches = match decode_branch_lru_snapshot(&branch_snapshot.payload) {
                    Ok(branches) => branches,
                    Err(error) => {
                        warn!(
                            error = %error,
                            "failed to decode replicated branch lifecycle checkpoint"
                        );
                        continue;
                    }
                };
                for (branch_key, _) in branches {
                    tokio::task::consume_budget().await;
                    let placement = match runtime.state_placement(
                        &branch_lru.domain,
                        state_kind,
                        branch_lru.kind,
                        branch_lru.identifier.clone(),
                        branch_key,
                    ) {
                        Ok(placement) => placement,
                        Err(error) => {
                            warn!(error = %error, "failed to place replicated branch state");
                            continue;
                        }
                    };
                    let after_lsm = match runtime.passive_state_replica_lsm(&placement) {
                        Ok(lsm) => lsm,
                        Err(error) => {
                            warn!(error = %error, "failed to read replicated branch state progress");
                            continue;
                        }
                    };
                    match runtime
                        .request_state_sync_with_timeout(
                            &primary_node,
                            &placement,
                            after_lsm,
                            poll_interval,
                        )
                        .await
                    {
                        Ok(Some(snapshot)) => {
                            if let Err(error) = runtime
                                .install_passive_state_replica_snapshot(
                                    &primary_node,
                                    &placement,
                                    snapshot,
                                )
                                .await
                            {
                                warn!(error = %error, "failed to install replicated branch state checkpoint");
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            warn!(error = %error, "failed to sync replicated branch state");
                        }
                    }
                }
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
        let notification = self.state_checkpoint_notification(state.read().placement());
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        &notification,
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                let after_lsm = state.read().current_lsm();
                match runtime
                    .install_materialized_snapshot_from(
                        &primary_node,
                        state.read(),
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
                                                placement: state.read().placement().to_remote(),
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
        let notification = self.state_checkpoint_notification(&state.placement);
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        &notification,
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
                        poll_interval,
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
    pub(in crate::runtime) fn install_state_identities(&self, schedule: &DomainSchedule) {
        let start_version = match self.inner.domains.get(&schedule.domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        let mut scheduled = HashSet::default();
        for node in schedule.nodes.values() {
            let node_ref = DomainNodeRef::node_in(
                schedule.domain.clone(),
                node.kind(),
                node.identifier.clone(),
            );
            self.inner.state_identities.insert(
                node_ref.clone(),
                ScheduledStateIdentity {
                    schema_fingerprint: Self::state_schema_fingerprint(node, start_version),
                    wasm_state_generations: node.wasm_state_generations().cloned(),
                },
            );
            scheduled.insert(node_ref);
        }
        self.retain_state_identities(&schedule.domain, &scheduled);
    }

    /// The graph-driven form of [`Self::install_state_identities`], written the same way and for
    /// the same reason: a node the graph still carries never loses its identity. `nodes` are the
    /// graph's unplaced schedule entries, and each schema-bound state is keyed exactly as a schedule
    /// of the same graph keys it. A graph carries no schedule, so it publishes no WASM guest-state
    /// generation; a node that already has one keeps it.
    pub(in crate::runtime) fn install_state_identities_from_graph(
        &self,
        domain: &DomainName,
        nodes: &[ScheduledNode],
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
            if let Some(mut identity) = self.inner.state_identities.get_mut(&node_ref) {
                identity.schema_fingerprint = schema_fingerprint;
            } else {
                self.inner.state_identities.insert(
                    node_ref.clone(),
                    ScheduledStateIdentity {
                        schema_fingerprint,
                        wasm_state_generations: None,
                    },
                );
            }
            active.insert(node_ref);
        }
        self.retain_state_identities(domain, &active);
    }

    /// The fingerprint every schema-bound runtime state of `node` is keyed by in a domain started
    /// `start_version` times.
    ///
    /// Materialized relay state also belongs to the domain start that began it, so a START resets
    /// it: its fingerprint covers the start version beside the node's schemas.
    fn state_schema_fingerprint(node: &ScheduledNode, start_version: u64) -> SchemaFingerprint {
        let Model::Relay(relay) = node.config.as_ref() else {
            return node.schema_fingerprint;
        };
        if relay.materialized_state.is_none() {
            return node.schema_fingerprint;
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/materialized-state/start-version");
        hasher.update(node.schema_fingerprint.as_digest());
        hasher.update(&start_version.to_be_bytes());
        SchemaFingerprint::from_digest(*hasher.finalize().as_bytes())
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
            self.inner.state_identities.remove(&key);
        }
    }

    /// The placement of runtime state of `state` for `branch_key`, in the identity the committed
    /// schedule publishes for its node.
    ///
    /// Branch-aggregated metrics and Kafka offsets depend on no schema and are placed without one.
    /// Every other kind is placed under the schema fingerprint the schedule publishes for the node,
    /// and WASM guest state also in the generation it names for the branch, so neither can be
    /// placed for a node that no schedule has published them for.
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
                let Some(identity) = self.inner.state_identities.get(&node) else {
                    return Err(Report::new(
                        StateIdentityError::SchemaFingerprintUnpublished {
                            domain: domain.clone(),
                            kind,
                            identifier,
                        },
                    ));
                };
                let branch = branch_key.as_ref().map(BranchKey::fingerprint);
                let Some(state) = identity.state_of(state, branch.as_ref()) else {
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
    pub(in crate::runtime) fn runtime_state_placement_is_current(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> bool {
        let node = DomainNodeRef::node_in(
            placement.domain.clone(),
            placement.kind,
            placement.identifier.clone(),
        );
        let branch = placement.branch_key.as_ref().map(BranchKey::fingerprint);
        let Some(identity) = self.inner.state_identities.get(&node) else {
            return false;
        };
        identity.names(placement.state, branch.as_ref())
    }

    pub(in crate::runtime) fn purge_stale_runtime_state(
        &self,
        domain: &DomainName,
    ) -> Result<(), error_stack::Report<RuntimePersistenceError>> {
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
        let stale_expiring = self
            .inner
            .expiring_stream_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_expiring {
            self.inner.expiring_stream_states.remove(&placement);
        }

        if let Some(store) = self.inner.state_store.as_ref() {
            let current = self
                .inner
                .state_identities
                .iter()
                .filter_map(|entry| {
                    let key = entry.key();
                    (&key.domain == domain).then(|| (key.node.clone(), entry.value().clone()))
                })
                .collect::<HashMap<_, _>>();
            store.purge_stale_state_identities(domain, &current)?;
        }
        Ok(())
    }

    pub(in crate::runtime) fn clear_runtime_state_for_domain(&self, domain: &DomainName) {
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
            .replicated_branch_lru_snapshots
            .retain(|placement, _| &placement.domain != domain);
        self.inner
            .passive_runtime_state_snapshots
            .retain(|placement, _| &placement.domain != domain);
        self.inner
            .pending_state_replica_syncs
            .retain(|placement, _| &placement.domain != domain);
        self.inner
            .pending_state_checkpoint_announcements
            .retain(|placement, _| &placement.domain != domain);
        self.inner
            .state_checkpoint_notifications
            .retain(|placement, _| &placement.domain != domain);
    }
}
