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
                |state: &KafkaOffsetStatePersistence, store: &RuntimeStateStore| {
                    if !state.take_dirty() {
                        return Ok(None);
                    }
                    let result = (|| {
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
                        Ok::<Option<u64>, RuntimePersistenceError>(Some(snapshot.lsm))
                    })();
                    if result.is_err() {
                        state.restore_dirty();
                    }
                    result
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

    pub(in crate::runtime) fn spawn_deduplicator_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedDeduplicatorState>,
    ) -> Option<JoinHandle<()>> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                |state: &ReplicatedDeduplicatorState, store: &RuntimeStateStore| {
                    persist_dirty_runtime_state_snapshot(
                        store,
                        &state.placement,
                        &state.last_persisted_lsm,
                        &state.dirty,
                        || state.latest_snapshot(),
                    )
                };
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            match flush_latest_snapshot(&state, &store) {
                                Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                    &state.placement, lsm,
                                ),
                                Ok(None) => {}
                                Err(error) => warn!(error = %error, "failed to flush deduplicator snapshot during shutdown"),
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        match flush_latest_snapshot(&state, &store) {
                            Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                &state.placement, lsm,
                            ),
                            Ok(None) => {}
                            Err(error) => warn!(error = %error, "failed to persist deduplicator snapshot"),
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
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                |state: &MaterializedRelayStatePersistence, store: &RuntimeStateStore| {
                    if !state.take_dirty() {
                        return Ok(None);
                    }
                    let result = (|| {
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
                        Ok::<Option<u64>, RuntimePersistenceError>(Some(snapshot.lsm))
                    })();
                    if result.is_err() {
                        state.restore_dirty();
                    }
                    result
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
                                Err(error) => warn!(error = %error, "failed to flush materialized relay snapshot during shutdown"),
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
                            Err(error) => warn!(error = %error, "failed to persist materialized relay snapshot"),
                        }
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_window_processor_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedWindowProcessorState>,
        snapshot_requests: mpsc::Sender<WindowProcessorSnapshotRequest>,
    ) -> Option<JoinHandle<()>> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            match persist_window_processor_state_snapshot(
                                &store,
                                &state,
                                &snapshot_requests,
                            ).await {
                                Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                    &state.placement, lsm,
                                ),
                                Ok(None) => {}
                                Err(error) => warn!(error = %error, "failed to flush window processor snapshot during shutdown"),
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        match persist_window_processor_state_snapshot(
                            &store,
                            &state,
                            &snapshot_requests,
                        ).await {
                            Ok(Some(lsm)) => runtime.notify_runtime_state_replicas(
                                &state.placement, lsm,
                            ),
                            Ok(None) => {}
                            Err(error) => warn!(error = %error, "failed to persist window processor snapshot"),
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
                    .request_state_sync_with_timeout(
                        &primary_node,
                        state.read().placement(),
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
                        let dispatcher = runtime.inner.remote_dispatcher.read().clone();
                        if let Some(dispatcher) = dispatcher {
                            let local_node_id =
                                runtime.inner.remote_dispatch.local_node_id.read().clone();
                            let Some(local_node_id) = local_node_id else {
                                continue;
                            };
                            if let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: state.read().placement().to_remote(),
                                                lsm: snapshot.lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                            {
                                warn!(node_id = %local_node_id, error = %error, "failed to acknowledge replicated kafka offset snapshot");
                            }
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
    ) -> Option<JoinHandle<()>> {
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
            _ => return None,
        };
        let primary_node = node.execution_node()?.clone();
        let branch_lru = self.state_placement(
            domain,
            RuntimeStateKind::BranchLru,
            node.kind(),
            node.identifier.clone(),
            None,
        );
        let poll_interval = self.inner.state_replication_poll_interval;
        let notification = self.state_checkpoint_notification(&branch_lru);
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
                        if let Err(error) = runtime.install_passive_state_replica_snapshot(
                            &primary_node,
                            &branch_lru,
                            snapshot,
                        ) {
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
                            error,
                            "failed to decode replicated branch lifecycle checkpoint"
                        );
                        continue;
                    }
                };
                for (branch_key, _) in branches {
                    tokio::task::consume_budget().await;
                    let placement = runtime.state_placement(
                        &branch_lru.domain,
                        state_kind,
                        branch_lru.kind,
                        branch_lru.identifier.clone(),
                        branch_key,
                    );
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
                            if let Err(error) = runtime.install_passive_state_replica_snapshot(
                                &primary_node,
                                &placement,
                                snapshot,
                            ) {
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
        }))
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
                    .request_state_sync_with_timeout(
                        &primary_node,
                        state.read().placement(),
                        Some(after_lsm),
                        poll_interval,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) = state.install_snapshot(snapshot.lsm, &snapshot.payload)
                        {
                            warn!(error = %error, "failed to apply replicated materialized relay snapshot");
                            break;
                        }
                        runtime.inner.materialized_state_changed.notify_waiters();
                        let dispatcher = runtime.inner.remote_dispatcher.read().clone();
                        if let Some(dispatcher) = dispatcher {
                            let local_node_id =
                                runtime.inner.remote_dispatch.local_node_id.read().clone();
                            let Some(local_node_id) = local_node_id else {
                                continue;
                            };
                            if let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: state.read().placement().to_remote(),
                                                lsm: snapshot.lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                            {
                                warn!(node_id = %local_node_id, error = %error, "failed to acknowledge replicated materialized relay snapshot");
                            }
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
                        let dispatcher = runtime.inner.remote_dispatcher.read().clone();
                        if let Some(dispatcher) = dispatcher {
                            let local_node_id =
                                runtime.inner.remote_dispatch.local_node_id.read().clone();
                            let Some(local_node_id) = local_node_id else {
                                continue;
                            };
                            if let Err(error) = dispatcher
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
                                warn!(node_id = %local_node_id, error = %error, "failed to acknowledge replicated branch-aggregated state snapshot");
                            }
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

    /// Installs the fingerprint every runtime state of `schedule`'s domain is keyed by.
    ///
    /// A node the schedule still carries keeps its fingerprint throughout: each one is written
    /// before any stale node is dropped, so a concurrent [`Self::state_placement`] always resolves
    /// to the state the node already owns. Emptying the domain first would expose a window where a
    /// scheduled node has no fingerprint, and a placement resolved in that window addresses a
    /// different — empty — runtime state. Relocation rebuilds these while the relocating node's
    /// own state task is still reading them, which is exactly when that window is observed.
    pub(in crate::runtime) fn install_state_schema_fingerprints(&self, schedule: &DomainSchedule) {
        let start_version = match self.inner.domains.get(&schedule.domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        let mut scheduled = HashSet::default();
        for node in schedule.nodes.values() {
            let schema_fingerprint = if matches!(
                node.config.as_ref(),
                Model::Relay(relay) if relay.materialized_state.is_some()
            ) {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"nervix/materialized-state/start-version");
                hasher.update(&node.schema_fingerprint);
                hasher.update(&start_version.to_be_bytes());
                *hasher.finalize().as_bytes()
            } else {
                node.schema_fingerprint
            };
            let node_ref = DomainNodeRef::node_in(
                schedule.domain.clone(),
                node.kind(),
                node.identifier.clone(),
            );
            self.inner
                .state_schema_fingerprints
                .insert(node_ref.clone(), schema_fingerprint);
            scheduled.insert(node_ref);
        }
        self.retain_state_schema_fingerprints(&schedule.domain, &scheduled);
    }

    /// The graph-driven form of [`Self::install_state_schema_fingerprints`], written the same way
    /// and for the same reason: a node the graph still carries never loses its fingerprint.
    pub(in crate::runtime) fn install_state_schema_fingerprints_from_graph(
        &self,
        domain: &DomainName,
        graph: &ActiveGraph,
    ) {
        let mut active = HashSet::default();
        for node in graph.nodes() {
            let node_ref =
                DomainNodeRef::node_in(domain.clone(), node.kind, node.identifier.clone());
            self.inner.state_schema_fingerprints.insert(
                node_ref.clone(),
                graph
                    .schema_fingerprint(node.kind, &node.identifier)
                    .unwrap_or([0; 32]),
            );
            active.insert(node_ref);
        }
        self.retain_state_schema_fingerprints(domain, &active);
    }

    pub(in crate::runtime) fn clear_state_schema_fingerprints(&self, domain: &DomainName) {
        self.retain_state_schema_fingerprints(domain, &HashSet::default());
    }

    /// Drops the fingerprints of `domain` that `keep` no longer names, leaving the rest in place.
    fn retain_state_schema_fingerprints(&self, domain: &DomainName, keep: &HashSet<DomainNodeRef>) {
        let stale = self
            .inner
            .state_schema_fingerprints
            .iter()
            .filter_map(|entry| {
                (&entry.key().domain == domain && !keep.contains(entry.key()))
                    .then(|| entry.key().clone())
            })
            .collect::<Vec<_>>();
        for key in stale {
            self.inner.state_schema_fingerprints.remove(&key);
        }
    }

    pub(in crate::runtime) fn state_placement(
        &self,
        domain: &DomainName,
        state: RuntimeStateKind,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        branch_key: Option<BranchKey>,
    ) -> RuntimeStatePlacement {
        let identifier = identifier.into();
        let schema_fingerprint =
            if let RuntimeStateKind::BranchAggregated | RuntimeStateKind::KafkaOffset = state {
                [0; 32]
            } else {
                let stored = self
                    .inner
                    .state_schema_fingerprints
                    .get(&DomainNodeRef::node_in(
                        domain.clone(),
                        kind,
                        identifier.clone(),
                    ));
                match stored {
                    Some(fingerprint) => *fingerprint,
                    None => [0; 32],
                }
            };
        RuntimeStatePlacement {
            domain: domain.clone(),
            state,
            kind,
            identifier: identifier.clone(),
            schema_fingerprint,
            branch_key,
        }
    }

    pub(in crate::runtime) fn runtime_state_placement_is_current(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> bool {
        let Some(current) = self
            .inner
            .state_schema_fingerprints
            .get(&DomainNodeRef::node_in(
                placement.domain.clone(),
                placement.kind,
                placement.identifier.clone(),
            ))
            .map(|fingerprint| *fingerprint)
        else {
            return false;
        };
        let expected = if let RuntimeStateKind::BranchAggregated | RuntimeStateKind::KafkaOffset =
            placement.state
        {
            [0; 32]
        } else {
            current
        };
        placement.schema_fingerprint == expected
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
                .state_schema_fingerprints
                .iter()
                .filter_map(|entry| {
                    let key = entry.key();
                    (&key.domain == domain).then(|| (key.node.clone(), *entry.value()))
                })
                .collect::<HashMap<_, _>>();
            store.purge_stale_schema_fingerprints(domain, &current)?;
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
