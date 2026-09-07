use super::*;

pub(super) const DEFAULT_STATE_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);

pub(super) const DEFAULT_STATE_REPLICATION_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub(super) type PendingStateSyncSender =
    oneshot::Sender<Result<Option<PersistedRuntimeStateEntry>, String>>;

#[derive(Debug)]
pub(crate) struct StateSyncAck {
    pub(crate) placement: RuntimeStatePlacement,
    pub(crate) lsm: u64,
}

pub(super) fn persist_dirty_runtime_state_snapshot(
    store: &RuntimeStateStore,
    placement: &RuntimeStatePlacement,
    last_persisted_lsm: &AtomicU64,
    dirty: &AtomicBool,
    latest_snapshot: impl FnOnce() -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError>,
) -> Result<(), RuntimePersistenceError> {
    if !dirty.swap(false, Ordering::SeqCst) {
        return Ok(());
    }
    let result = (|| {
        let snapshot = latest_snapshot()?;
        if snapshot.lsm <= last_persisted_lsm.load(Ordering::SeqCst) {
            return Ok(());
        }
        store.persist_latest_snapshot(placement, snapshot.lsm, &snapshot.payload)?;
        last_persisted_lsm.fetch_max(snapshot.lsm, Ordering::SeqCst);
        Ok(())
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
) -> Result<(), String> {
    if state.live_dirty.load(Ordering::SeqCst) {
        let (response_tx, response_rx) = oneshot::channel();
        snapshot_requests.send(response_tx).await.map_err(|_| {
            format!(
                "window processor '{}' snapshot owner is unavailable",
                state.placement.identifier.as_str()
            )
        })?;
        response_rx.await.map_err(|_| {
            format!(
                "window processor '{}' snapshot owner dropped its response",
                state.placement.identifier.as_str()
            )
        })??;
    }
    persist_dirty_runtime_state_snapshot(
        store,
        &state.placement,
        &state.last_persisted_lsm,
        &state.dirty,
        || state.latest_snapshot(),
    )
    .map_err(|error| error.to_string())
}

impl Runtime {
    pub(super) fn relay_state_epoch(&self, domain: &DomainName) -> Arc<AtomicU64> {
        self.inner
            .relay_state_epochs
            .entry(domain.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    pub(super) fn bump_relay_state_epoch(&self, domain: &DomainName) {
        self.relay_state_epoch(domain)
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn purge_materialized_relay_state(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), RuntimeError> {
        let placements = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .filter(|entry| {
                entry.key().domain == *domain
                    && entry.key().kind == ModelKind::Relay
                    && entry.key().identifier == ModelName::from(&*relay)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner
                .replicated_materialized_stream_states
                .remove(&placement);
        }
        if let Some(store) = &self.inner.state_store {
            store
                .purge_entity(
                    domain,
                    RuntimeStateKind::MaterializedRelay,
                    ModelKind::Relay,
                    relay,
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "failed to purge materialized state for relay '{}': {error}",
                        relay.as_str()
                    ),
                })?;
        }
        Ok(())
    }

    pub(super) fn purge_deduplicator_state(
        &self,
        domain: &DomainName,
        deduplicator: &DeduplicatorName,
    ) -> Result<(), RuntimeError> {
        let placements = self
            .inner
            .replicated_deduplicator_states
            .iter()
            .filter(|entry| {
                entry.key().domain == *domain
                    && entry.key().kind == ModelKind::Deduplicator
                    && entry.key().identifier == ModelName::from(&*deduplicator)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner.replicated_deduplicator_states.remove(&placement);
        }
        if let Some(store) = &self.inner.state_store {
            store
                .purge_entity(
                    domain,
                    RuntimeStateKind::Deduplicator,
                    ModelKind::Deduplicator,
                    deduplicator,
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "failed to purge state for deduplicator '{}': {error}",
                        deduplicator.as_str()
                    ),
                })?;
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
        after_lsm: u64,
    ) -> Result<Option<PersistedRuntimeStateEntry>, String> {
        if let RuntimeStateKind::MaterializedRelay = placement.state {
            let mut entries = Vec::new();
            let mut latest_lsm = 0;
            let mut found = false;
            for state in self.inner.replicated_materialized_stream_states.iter() {
                let concrete = state.key();
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
                latest_lsm = latest_lsm.max(state.current_lsm.current());
                if let Some(requested) = placement.branch_key.as_ref() {
                    let key = Some(requested.clone());
                    if let Some(entry) = self.visible_materialized_stream_remote_entry(
                        concrete,
                        state.value(),
                        &key,
                    )? {
                        entries.push(entry);
                    }
                } else {
                    entries.extend(
                        self.visible_materialized_stream_remote_entries(concrete, state.value())?,
                    );
                }
            }
            if found {
                if latest_lsm <= after_lsm {
                    return Ok(None);
                }
                return Ok(Some(PersistedRuntimeStateEntry {
                    lsm: latest_lsm,
                    schema_fingerprint: placement.schema_fingerprint,
                    payload: encode_materialized_stream_snapshot_entries(&entries)
                        .map_err(|error| error.to_string())?,
                }));
            }
        }
        if let Some(state) = self.inner.replicated_deduplicator_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
            return Ok(None);
        }
        if let Some(state) = self.inner.replicated_kafka_offset_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self
            .inner
            .replicated_materialized_stream_states
            .get(placement)
        {
            let entries = self.visible_materialized_stream_remote_entries(placement, &state)?;
            let snapshot = PersistedRuntimeStateEntry {
                lsm: state.current_lsm.current(),
                schema_fingerprint: placement.schema_fingerprint,
                payload: encode_materialized_stream_snapshot_entries(&entries)
                    .map_err(|error| error.to_string())?,
            };
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self.inner.replicated_window_processor_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self.inner.replicated_wasm_processor_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.lsm > after_lsm {
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
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
        }
        Ok(None)
    }

    pub fn handle_state_sync_response(
        &self,
        correlation_id: u64,
        result: Result<Option<PersistedRuntimeStateEntry>, String>,
    ) {
        let Some((_, tx)) = self.inner.pending_state_syncs.remove(&correlation_id) else {
            return;
        };
        let _ = tx.send(result);
    }

    pub(crate) fn handle_state_replication_ack(
        &self,
        node_id: &ClusterNodeName,
        ack: StateSyncAck,
    ) {
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
            after_lsm,
            Duration::from_secs(5),
        )
        .await
    }

    pub(super) async fn request_state_sync_with_timeout(
        &self,
        target_node_id: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        after_lsm: u64,
        response_timeout: Duration,
    ) -> Result<Option<PersistedRuntimeStateEntry>, String> {
        let Some(dispatcher) = self.inner.remote_dispatcher.read().clone() else {
            return Err("remote dispatcher unavailable".to_string());
        };
        let correlation_id = self
            .inner
            .next_state_sync_correlation_id
            .fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.inner.pending_state_syncs.insert(correlation_id, tx);
        let result = dispatcher
            .dispatch(
                target_node_id,
                Envelope::Control(nervix_interconnect::ControlEnvelope::StateSyncRequest(
                    nervix_interconnect::StateSyncRequest {
                        correlation_id,
                        placement: placement.to_remote(),
                        after_lsm,
                    },
                )),
            )
            .await;
        if let Err(error) = result {
            self.inner.pending_state_syncs.remove(&correlation_id);
            return Err(error);
        }
        match tokio::time::timeout(response_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.inner.pending_state_syncs.remove(&correlation_id);
                Err("state sync response channel closed".to_string())
            }
            Err(_) => {
                self.inner.pending_state_syncs.remove(&correlation_id);
                Err("timed out waiting for state sync response".to_string())
            }
        }
    }

    pub(in crate::runtime) async fn wait_for_kafka_offset_replica_quorum(
        &self,
        state: &ReplicatedKafkaOffsetState,
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
                    state.placement.identifier.as_str(),
                    lsm
                ));
            }
            tokio::select! {
                _ = state.replication_notify.notified() => {}
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
        state: &ReplicatedKafkaOffsetState,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Some(store) = &self.inner.state_store {
            store
                .persist_latest_snapshot(&state.placement, lsm, payload)
                .map_err(|error| error.to_string())?;
            state.last_persisted_lsm.store(lsm, Ordering::SeqCst);
            state.dirty.store(false, Ordering::SeqCst);
        }
        self.wait_for_kafka_offset_replica_quorum(state, lsm).await
    }

    pub(in crate::runtime) async fn commit_domain_kafka_offset(
        &self,
        state: &ReplicatedKafkaOffsetState,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), String> {
        let (lsm, payload) = state
            .apply_committed_offset(topic, partition, next_offset)
            .map_err(|error| error.to_string())?;
        self.persist_kafka_offset_snapshot(state, lsm, &payload)
            .await
    }

    pub(in crate::runtime) async fn reset_domain_kafka_offsets(
        &self,
        state: &ReplicatedKafkaOffsetState,
        offsets: HashMap<KafkaTopicPartition, i64>,
    ) -> Result<(), String> {
        let (lsm, payload) = state
            .replace_offsets(offsets)
            .map_err(|error| error.to_string())?;
        self.persist_kafka_offset_snapshot(state, lsm, &payload)
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
        }
        self.wait_for_wasm_processor_replica_quorum(state, lsm)
            .await
    }

    pub(in crate::runtime) fn update_materialized_stream_last_by_timestamp(
        &self,
        state: &ReplicatedMaterializedRelayState,
        key: &Option<BranchKey>,
        record: &RuntimeRow,
    ) {
        if state.update_last_by_timestamp(key, record).is_some() {
            self.inner.materialized_state_changed.notify_waiters();
        }
    }

    pub(in crate::runtime) fn delete_materialized_stream_key(
        &self,
        state: &ReplicatedMaterializedRelayState,
        key: &Option<BranchKey>,
    ) {
        if state.remove_key(key).is_some() {
            self.inner.materialized_state_changed.notify_waiters();
        }
    }

    pub(in crate::runtime) fn replicated_deduplicator_state(
        &self,
        placement: RuntimeStatePlacement,
    ) -> Result<Arc<ReplicatedDeduplicatorState>, RuntimePersistenceError> {
        if let Some(existing) = self.inner.replicated_deduplicator_states.get(&placement) {
            return Ok(existing.clone());
        }
        let initial = self
            .inner
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
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
    ) -> Result<Arc<ReplicatedKafkaOffsetState>, RuntimePersistenceError> {
        if let Some(existing) = self.inner.replicated_kafka_offset_states.get(&placement) {
            existing.rebind_roles(StateReplicationRoles::new(
                primary_node,
                replica_nodes,
                required_replica_acks,
            ));
            return Ok(existing.clone());
        }
        let initial = self
            .inner
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
        let state = Arc::new(ReplicatedKafkaOffsetState::new(
            placement.clone(),
            primary_node,
            replica_nodes,
            required_replica_acks,
            initial,
        )?);
        self.inner
            .replicated_kafka_offset_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_materialized_stream_state(
        &self,
        placement: RuntimeStatePlacement,
        schema: StdArc<arrow_schema::Schema>,
        primary_node: Option<ClusterNodeName>,
    ) -> Result<Arc<ReplicatedMaterializedRelayState>, RuntimePersistenceError> {
        if let Some(existing) = self
            .inner
            .replicated_materialized_stream_states
            .get(&placement)
        {
            existing.rebind_roles(StateReplicationRoles::owned_by(primary_node));
            return Ok(existing.clone());
        }
        let initial = self
            .inner
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement.clone(),
            schema,
            primary_node,
            initial,
        )?);
        self.inner
            .replicated_materialized_stream_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_window_processor_state(
        &self,
        placement: RuntimeStatePlacement,
    ) -> Result<Arc<ReplicatedWindowProcessorState>, RuntimePersistenceError> {
        if let Some(existing) = self
            .inner
            .replicated_window_processor_states
            .get(&placement)
        {
            return Ok(existing.clone());
        }
        let initial = self
            .inner
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
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
        if let Some(existing) = self.inner.replicated_wasm_processor_states.get(&placement) {
            return Ok(existing.clone());
        }
        let initial = self
            .inner
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
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
        if let Some(existing) = self
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
        let initial = self
            .inner
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
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

    pub(in crate::runtime) fn spawn_kafka_offset_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedKafkaOffsetState>,
    ) -> Option<JoinHandle<()>> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                |state: &ReplicatedKafkaOffsetState, store: &RuntimeStateStore| {
                    if !state.dirty.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    let snapshot = state.latest_snapshot()?;
                    if snapshot.lsm <= state.last_persisted_lsm.load(Ordering::SeqCst) {
                        return Ok(());
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
                    Ok::<(), RuntimePersistenceError>(())
                };
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            if let Err(error) = flush_latest_snapshot(&state, &store) {
                                warn!(error = %error, "failed to flush kafka offset snapshot during shutdown");
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        if let Err(error) = flush_latest_snapshot(&state, &store) {
                            warn!(error = %error, "failed to persist kafka offset snapshot");
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
                            if let Err(error) = flush_latest_snapshot(&state, &store) {
                                warn!(error = %error, "failed to flush deduplicator snapshot during shutdown");
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        if let Err(error) = flush_latest_snapshot(&state, &store) {
                            warn!(error = %error, "failed to persist deduplicator snapshot");
                        }
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_materialized_stream_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedMaterializedRelayState>,
    ) -> Option<JoinHandle<()>> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                |state: &ReplicatedMaterializedRelayState, store: &RuntimeStateStore| {
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
                            if let Err(error) = flush_latest_snapshot(&state, &store) {
                                warn!(error = %error, "failed to flush materialized relay snapshot during shutdown");
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        if let Err(error) = flush_latest_snapshot(&state, &store) {
                            warn!(error = %error, "failed to persist materialized relay snapshot");
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
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            if let Err(error) = persist_window_processor_state_snapshot(
                                &store,
                                &state,
                                &snapshot_requests,
                            ).await {
                                warn!(error = %error, "failed to flush window processor snapshot during shutdown");
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        if let Err(error) = persist_window_processor_state_snapshot(
                            &store,
                            &state,
                            &snapshot_requests,
                        ).await {
                            warn!(error = %error, "failed to persist window processor snapshot");
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
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                |state: &ReplicatedBranchAggregatedState,
                 metrics: &RuntimeMetrics,
                 store: &RuntimeStateStore| {
                    if !state.dirty.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    let snapshot = state.latest_snapshot(metrics)?;
                    if snapshot.lsm <= state.last_persisted_lsm.load(Ordering::SeqCst) {
                        return Ok(());
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
                    Ok::<(), RuntimePersistenceError>(())
                };
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            if let Err(error) = flush_latest_snapshot(&state, &metrics, &store) {
                                warn!(error = %error, "failed to flush branch-aggregated state snapshot during shutdown");
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        if let Err(error) = flush_latest_snapshot(&state, &metrics, &store) {
                            warn!(error = %error, "failed to persist branch-aggregated state snapshot");
                        }
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_kafka_offset_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedKafkaOffsetState>,
    ) -> Option<JoinHandle<()>> {
        let primary_node = state.primary_node()?;
        let poll_interval = self.inner.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if initial_sync_pending {
                    initial_sync_pending = false;
                } else {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(poll_interval) => {}
                    }
                }
                let after_lsm = state.current_lsm.current();
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &state.placement,
                        after_lsm,
                        poll_interval,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) = state.apply_snapshot(snapshot.lsm, &snapshot.payload) {
                            warn!(error = %error, "failed to apply replicated kafka offset snapshot");
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

    pub(in crate::runtime) fn spawn_materialized_stream_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedMaterializedRelayState>,
    ) -> Option<JoinHandle<()>> {
        let primary_node = state.primary_node()?;
        let poll_interval = self.inner.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if initial_sync_pending {
                    initial_sync_pending = false;
                } else {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(poll_interval) => {}
                    }
                }
                let after_lsm = state.current_lsm.current();
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &state.placement,
                        after_lsm,
                        poll_interval,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) = state.apply_snapshot(snapshot.lsm, &snapshot.payload) {
                            warn!(error = %error, "failed to apply replicated materialized relay snapshot");
                            continue;
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
                                                placement: state.placement.to_remote(),
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
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if initial_sync_pending {
                    initial_sync_pending = false;
                } else {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(poll_interval) => {}
                    }
                }
                let after_lsm = state.current_lsm.current();
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &state.placement,
                        after_lsm,
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

    pub(in crate::runtime) fn install_state_schema_fingerprints(&self, schedule: &DomainSchedule) {
        self.clear_state_schema_fingerprints(&schedule.domain);
        let start_version = self
            .inner
            .domains
            .get(&schedule.domain)
            .map_or(0, |state| state.start_version);
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
            self.inner.state_schema_fingerprints.insert(
                DomainNodeRef::node_in(schedule.domain.clone(), node.kind, node.identifier.clone()),
                schema_fingerprint,
            );
        }
    }

    pub(super) fn install_state_schema_fingerprints_from_graph(
        &self,
        domain: &DomainName,
        graph: &ActiveGraph,
    ) {
        self.clear_state_schema_fingerprints(domain);
        for node in graph.nodes() {
            self.inner.state_schema_fingerprints.insert(
                DomainNodeRef::node_in(domain.clone(), node.kind, node.identifier.clone()),
                graph
                    .schema_fingerprint(node.kind, &node.identifier)
                    .unwrap_or([0; 32]),
            );
        }
    }

    pub(super) fn clear_state_schema_fingerprints(&self, domain: &DomainName) {
        let keys = self
            .inner
            .state_schema_fingerprints
            .iter()
            .filter_map(|entry| (&entry.key().domain == domain).then(|| entry.key().clone()))
            .collect::<Vec<_>>();
        for key in keys {
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
                self.inner
                    .state_schema_fingerprints
                    .get(&DomainNodeRef::node_in(
                        domain.clone(),
                        kind,
                        identifier.clone(),
                    ))
                    .map(|fingerprint| *fingerprint)
                    .unwrap_or([0; 32])
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

    pub(super) fn runtime_state_placement_is_current(
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

    pub(super) fn clear_runtime_state_for_domain(&self, domain: &DomainName) {
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
    }

    pub(super) fn purge_stopped_domain_runtime_state(
        &self,
        domain: &DomainName,
    ) -> Result<(), RuntimeError> {
        let Some(store) = self.inner.state_store.as_ref() else {
            return Ok(());
        };
        store
            .purge_domain(domain)
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: error.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use ahash::HashMap;
    use fjall::Database;
    use nervix_models::{
        ClusterNodeName, CreateSchema, DomainSchedule, ModelKind, ModelName, NodeRef, ParseAsType,
        ScheduledNode, SchemaName, Timestamp,
    };
    use nonzero_ext::nonzero;
    use tempfile::tempdir;
    use tokio::{
        sync::{mpsc, watch},
        time::{Duration, timeout},
    };
    use triomphe::Arc;

    use super::*;
    use crate::{
        metrics::RuntimeMetrics,
        runtime_schema::{RuntimeValue, test_runtime_row},
    };
    #[test]
    fn runtime_state_store_persists_latest_snapshot_with_monotonic_lsm() {
        let dir = tempdir().expect("temp dir should open");
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let store = RuntimeStateStore::from_database(db).expect("state store should open");
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::Deduplicator,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [0; 32],
            branch_key: string_branch_key("tenant", "acme"),
        };

        let first_lsm = 1;
        store
            .persist_latest_snapshot(&placement, first_lsm, b"first")
            .expect("first snapshot should persist");
        let second_lsm = 2;
        store
            .persist_latest_snapshot(&placement, second_lsm, b"second")
            .expect("second snapshot should persist");

        assert_eq!(first_lsm, 1);
        assert_eq!(second_lsm, 2);
        assert_eq!(
            store
                .latest_snapshot(&placement)
                .expect("latest snapshot should load")
                .expect("latest snapshot should exist")
                .payload,
            b"second".to_vec()
        );
    }

    #[tokio::test]
    async fn deduplicator_snapshot_task_persists_dirty_state_on_interval() {
        let dir = tempdir().expect("temp dir should open");
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let runtime =
            Runtime::with_persistence(Some(db), Duration::from_millis(10), Default::default())
                .expect("runtime should open persisted state");
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::Deduplicator,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [0; 32],
            branch_key: string_branch_key("tenant", "acme"),
        };
        let state = runtime
            .replicated_deduplicator_state(placement.clone())
            .expect("deduplicator state should initialize");
        let (shutdown_tx, _) = watch::channel(false);
        let task = runtime
            .spawn_deduplicator_snapshot_task(&shutdown_tx, state.clone())
            .expect("persisted runtime should spawn a snapshot task");

        assert!(state.reserve_new_key(
            DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]),
            Timestamp::from_unix_nanos(1),
            Duration::from_secs(600),
        ));
        let expected_lsm = state.current_lsm.current();

        wait_for_persisted_runtime_state_lsm(&runtime, &placement, expected_lsm).await;
        assert_eq!(
            state.last_persisted_lsm.load(Ordering::SeqCst),
            expected_lsm
        );
        assert!(!state.dirty.load(Ordering::SeqCst));

        shutdown_tx.send_replace(true);
        task.await.expect("snapshot task should stop cleanly");
    }

    #[tokio::test]
    async fn materialized_relay_snapshot_task_owns_persistence() {
        let dir = tempdir().expect("temp dir should open");
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let runtime =
            Runtime::with_persistence(Some(db), Duration::from_secs(3_600), Default::default())
                .expect("runtime should open persisted state");
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::MaterializedRelay,
            kind: ModelKind::Relay,
            identifier: named("latest_orders"),
            schema_fingerprint: [0; 32],
            branch_key: None,
        };
        let schema = test_schema(&[("status", ParseAsType::String)]);
        let state = runtime
            .replicated_materialized_stream_state(placement.clone(), schema.arrow_schema(), None)
            .expect("materialized relay state should initialize");
        let (shutdown_tx, _) = watch::channel(false);
        let task = runtime
            .spawn_materialized_stream_snapshot_task(&shutdown_tx, state.clone())
            .expect("persisted runtime should spawn a snapshot task");
        let record = test_runtime_row([(
            "status".to_string(),
            RuntimeValue::String("ready".to_string()),
        )]);

        runtime.update_materialized_stream_last_by_timestamp(&state, &None, &record);

        assert_eq!(state.current_lsm.current(), 1);
        assert!(state.dirty.load(Ordering::SeqCst));
        assert_eq!(state.last_persisted_lsm.load(Ordering::SeqCst), 0);
        assert!(
            runtime
                .inner
                .state_store
                .as_ref()
                .expect("test runtime should have a state store")
                .latest_snapshot(&placement)
                .expect("snapshot lookup should succeed")
                .is_none(),
            "the relay-state hot path must not persist a snapshot"
        );

        shutdown_tx.send_replace(true);
        task.await.expect("snapshot task should stop cleanly");
        assert_eq!(
            runtime
                .inner
                .state_store
                .as_ref()
                .expect("test runtime should have a state store")
                .latest_snapshot(&placement)
                .expect("snapshot lookup should succeed")
                .expect("shutdown should flush the dirty snapshot")
                .lsm,
            1
        );
    }

    #[tokio::test]
    async fn window_processor_snapshot_task_persists_dirty_state_on_interval() {
        let dir = tempdir().expect("temp dir should open");
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let runtime =
            Runtime::with_persistence(Some(db), Duration::from_millis(10), Default::default())
                .expect("runtime should open persisted state");
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::WindowProcessor,
            kind: ModelKind::WindowProcessor,
            identifier: named("latency_window"),
            schema_fingerprint: [0; 32],
            branch_key: string_branch_key("tenant", "acme"),
        };
        let state = runtime
            .replicated_window_processor_state(placement.clone())
            .expect("window processor state should initialize");
        let (shutdown_tx, _) = watch::channel(false);
        let (snapshot_request_tx, mut snapshot_requests) = mpsc::channel(1);
        let task = runtime
            .spawn_window_processor_snapshot_task(&shutdown_tx, state.clone(), snapshot_request_tx)
            .expect("persisted runtime should spawn a snapshot task");
        let live_state =
            WindowProcessorState::new(&window_aggregate("SET count = COUNT(input.latency)"));
        let snapshot_state = state.clone();
        let snapshot_owner = tokio::spawn(async move {
            let response = timeout(Duration::from_secs(1), snapshot_requests.recv())
                .await
                .expect("snapshot task should request live state")
                .expect("snapshot request channel should remain open");
            let result = snapshot_state
                .replace_state(&live_state)
                .map(|_| ())
                .map_err(|error| error.to_string());
            let _ = response.send(result);
        });

        state.mark_live_dirty();
        let expected_lsm = 1;

        wait_for_persisted_runtime_state_lsm(&runtime, &placement, expected_lsm).await;
        snapshot_owner
            .await
            .expect("snapshot owner should stop cleanly");
        assert_eq!(
            state.last_persisted_lsm.load(Ordering::SeqCst),
            expected_lsm
        );
        assert!(!state.dirty.load(Ordering::SeqCst));

        shutdown_tx.send_replace(true);
        task.await.expect("snapshot task should stop cleanly");
    }

    #[test]
    fn runtime_state_store_purges_only_stale_schema_fingerprints() {
        let dir = tempdir().expect("temp dir should open");
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let store = RuntimeStateStore::from_database(db).expect("state store should open");
        let base = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::Deduplicator,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [1; 32],
            branch_key: None,
        };
        let current = RuntimeStatePlacement {
            schema_fingerprint: [2; 32],
            ..base.clone()
        };
        store
            .persist_latest_snapshot(&base, 1, b"old")
            .expect("old snapshot should persist");
        store
            .persist_latest_snapshot(&current, 2, b"current")
            .expect("current snapshot should persist");

        store
            .purge_stale_schema_fingerprints(
                &base.domain,
                &HashMap::from_iter([(
                    NodeRef {
                        kind: base.kind,
                        identifier: base.identifier.clone(),
                    },
                    current.schema_fingerprint,
                )]),
            )
            .expect("stale snapshots should purge");

        assert!(
            store
                .latest_snapshot(&base)
                .expect("old snapshot lookup should succeed")
                .is_none()
        );
        assert_eq!(
            store
                .latest_snapshot(&current)
                .expect("current snapshot lookup should succeed")
                .expect("current snapshot should remain")
                .payload,
            b"current".to_vec()
        );
    }

    #[test]
    fn runtime_state_store_purges_only_the_requested_domain() {
        let dir = tempdir().expect("temp dir should open");
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let store = RuntimeStateStore::from_database(db).expect("state store should open");
        let stopped = RuntimeStatePlacement {
            domain: domain("stopped"),
            state: RuntimeStateKind::Deduplicator,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [1; 32],
            branch_key: None,
        };
        let running = RuntimeStatePlacement {
            domain: domain("running"),
            ..stopped.clone()
        };
        store
            .persist_latest_snapshot(&stopped, 1, b"stopped")
            .expect("stopped-domain snapshot should persist");
        store
            .persist_latest_snapshot(&running, 2, b"running")
            .expect("running-domain snapshot should persist");

        store
            .purge_domain(&stopped.domain)
            .expect("stopped-domain snapshots should purge");

        assert!(
            store
                .latest_snapshot(&stopped)
                .expect("stopped-domain snapshot lookup should succeed")
                .is_none()
        );
        assert_eq!(
            store
                .latest_snapshot(&running)
                .expect("running-domain snapshot lookup should succeed")
                .expect("running-domain snapshot should remain")
                .payload,
            b"running".to_vec()
        );
    }

    #[test]
    fn runtime_state_store_purges_only_the_requested_entity() {
        let dir = tempdir().expect("temp dir should open");
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let store = RuntimeStateStore::from_database(db).expect("state store should open");
        let removed = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::MaterializedRelay,
            kind: ModelKind::Relay,
            identifier: named("events"),
            schema_fingerprint: [1; 32],
            branch_key: None,
        };
        let retained = RuntimeStatePlacement {
            identifier: named("audit"),
            ..removed.clone()
        };
        store
            .persist_latest_snapshot(&removed, 1, b"removed")
            .expect("removed snapshot should persist");
        store
            .persist_latest_snapshot(&retained, 2, b"retained")
            .expect("retained snapshot should persist");

        store
            .purge_entity(
                &removed.domain,
                removed.state,
                removed.kind,
                &removed.identifier,
            )
            .expect("entity snapshots should purge");

        assert!(
            store
                .latest_snapshot(&removed)
                .expect("removed snapshot lookup should succeed")
                .is_none()
        );
        assert_eq!(
            store
                .latest_snapshot(&retained)
                .expect("retained snapshot lookup should succeed")
                .expect("unrelated entity snapshot should remain")
                .payload,
            b"retained".to_vec()
        );
    }

    #[test]
    fn kafka_offset_state_roundtrips_partition_schedule_through_fjall() {
        let dir = tempdir().expect("temp dir should open");
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let store = RuntimeStateStore::from_database(db).expect("state store should open");
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::KafkaOffset,
            kind: ModelKind::Ingestor,
            identifier: named("kafka_notifications"),
            schema_fingerprint: [0; 32],
            branch_key: None,
        };
        let state = ReplicatedKafkaOffsetState::new(placement.clone(), None, Vec::new(), 0, None)
            .expect("kafka state should initialize");
        let (offset_lsm, offset_payload) = state
            .replace_offsets(HashMap::from_iter([
                (
                    KafkaTopicPartition {
                        topic: "notifications".to_string(),
                        partition: 0,
                    },
                    12,
                ),
                (
                    KafkaTopicPartition {
                        topic: "notifications".to_string(),
                        partition: 1,
                    },
                    18,
                ),
            ]))
            .expect("offsets should update");
        store
            .persist_latest_snapshot(&placement, offset_lsm, &offset_payload)
            .expect("offset snapshot should persist");
        let (schedule_lsm, schedule_payload) = state
            .update_partition_schedule("notifications", nonzero!(2u64), vec![0, 1])
            .expect("schedule should update")
            .expect("schedule snapshot should be produced");
        store
            .persist_latest_snapshot(&placement, schedule_lsm, &schedule_payload)
            .expect("schedule snapshot should persist");

        let restored = ReplicatedKafkaOffsetState::new(
            placement.clone(),
            None,
            Vec::new(),
            0,
            store
                .latest_snapshot(&placement)
                .expect("snapshot should load"),
        )
        .expect("restored kafka state should initialize");
        assert_eq!(restored.next_offset("notifications", 0), Some(12));
        assert_eq!(restored.next_offset("notifications", 1), Some(18));
        assert_eq!(
            restored.describe_topic("notifications"),
            Some(KafkaDomainOffsetDescribe {
                topic: "notifications".to_string(),
                instances: 2,
                observed_partitions: vec![0, 1],
                rebalance_epoch: 0,
                instance_assignments: vec![vec![0], vec![1]],
            })
        );
    }

    #[test]
    fn branch_aggregated_state_snapshot_roundtrips_metrics() {
        let metrics = RuntimeMetrics::default();
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::BranchAggregated,
            kind: ModelKind::Ingestor,
            identifier: named("redis_notifications"),
            schema_fingerprint: [0; 32],
            branch_key: None,
        };
        let relay = named("notifications");
        let state = ReplicatedBranchAggregatedState::new(
            placement.clone(),
            Some(ClusterNodeName::parse("node-1").expect("valid name")),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Vec::new(),
            0,
            &metrics,
            None,
        )
        .expect("branch-aggregated state should initialize");
        metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
            domain: &placement.domain,
            kind: placement.kind,
            node: &placement.identifier,
            relay: &relay,
            physical_node_id: Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            messages: 2,
            bytes: 64,
            domain_timestamp: None,
        });
        let lsm = state.mark_metrics_updated();
        let snapshot = state
            .latest_snapshot(&metrics)
            .expect("metrics snapshot should encode");
        assert_eq!(snapshot.lsm, lsm);

        let restored_metrics = RuntimeMetrics::default();
        let _restored = ReplicatedBranchAggregatedState::new(
            placement.clone(),
            Some(ClusterNodeName::parse("node-1").expect("valid name")),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Vec::new(),
            0,
            &restored_metrics,
            Some(snapshot),
        )
        .expect("branch-aggregated state should restore");

        let rendered = restored_metrics.describe_global_target(
            &placement.domain,
            "INGESTOR",
            &placement.identifier,
        );
        assert!(
            rendered.iter().any(
                |line| line.contains("messages_total sent relay=notifications")
                    && line.contains("total=2")
            ),
            "expected restored metrics total in {rendered:?}"
        );
    }

    #[tokio::test]
    async fn state_sync_request_returns_latest_snapshot_only_when_lsm_advances() {
        let runtime = Runtime::default();
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::Deduplicator,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [0; 32],
            branch_key: string_branch_key("tenant", "acme"),
        };
        let state = runtime
            .replicated_deduplicator_state(placement.clone())
            .expect("deduplicator state should initialize");
        assert!(state.reserve_new_key(
            DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]),
            Timestamp::from_unix_nanos(1),
            Duration::from_secs(600),
        ));
        let lsm = state.current_lsm.current();

        let first = runtime
            .handle_state_sync_request(&placement, 0)
            .await
            .expect("state sync request should succeed")
            .expect("snapshot should be returned");
        assert_eq!(first.lsm, lsm);

        let none = runtime
            .handle_state_sync_request(&placement, lsm)
            .await
            .expect("state sync request should succeed");
        assert!(none.is_none());
    }

    #[test]
    fn deduplicator_key_reservation_reports_new_and_duplicate_keys() {
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::Deduplicator,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [0; 32],
            branch_key: string_branch_key("tenant", "acme"),
        };
        let state = ReplicatedDeduplicatorState::new(placement, None)
            .expect("deduplicator state should initialize");
        let seen_at = Timestamp::from_unix_nanos(1);
        let max_time = Duration::from_secs(600);

        let key = DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]);
        assert!(state.reserve_new_key(key.clone(), seen_at, max_time));
        assert!(!state.reserve_new_key(key, seen_at, max_time));
        assert_eq!(state.current_lsm.current(), 1);
    }

    #[test]
    fn runtime_state_placement_storage_key_includes_branch_key() {
        let tenant_beta = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::Deduplicator,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [1; 32],
            branch_key: string_branch_key("tenant", "beta"),
        };
        let tenant = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::Deduplicator,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [1; 32],
            branch_key: string_branch_key("tenant", "acme"),
        };

        assert_ne!(tenant_beta.as_storage_key(), tenant.as_storage_key());
        let branch_aggregated = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::BranchAggregated,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [0; 32],
            branch_key: None,
        };
        assert_ne!(
            tenant_beta.as_storage_key(),
            branch_aggregated.as_storage_key()
        );
        let deduplicator_global = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeStateKind::Deduplicator,
            kind: ModelKind::Deduplicator,
            identifier: named("dedup_orders"),
            schema_fingerprint: [1; 32],
            branch_key: None,
        };
        assert_ne!(
            deduplicator_global.as_storage_key(),
            branch_aggregated.as_storage_key()
        );
    }

    #[test]
    fn schema_fingerprints_reuse_unaffected_state_and_isolate_changed_state() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let identifier = named::<ModelName>("dedup_orders");
        let schedule = |fingerprint| {
            DomainSchedule::new(
                domain.clone(),
                vec![ScheduledNode {
                    identifier: identifier.clone(),
                    kind: ModelKind::Deduplicator,
                    config: Box::new(nervix_models::Model::Schema(CreateSchema {
                        name: SchemaName::from(&identifier.clone()),
                        fields: Vec::new(),
                    })),
                    effective_branching: None,
                    effective_branching_schema: None,
                    schema_fingerprint: fingerprint,
                    kafka_partition_schedule: None,
                    primary_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
                    assigned_nodes: vec![ClusterNodeName::parse("node-1").expect("valid name")],
                }],
                Vec::new(),
            )
        };

        runtime.install_state_schema_fingerprints(&schedule([1; 32]));
        let original_placement = runtime.state_placement(
            &domain,
            RuntimeStateKind::Deduplicator,
            ModelKind::Deduplicator,
            &identifier,
            None,
        );
        let original = runtime
            .replicated_deduplicator_state(original_placement.clone())
            .expect("state should initialize");

        runtime.install_state_schema_fingerprints(&schedule([1; 32]));
        let unchanged = runtime
            .replicated_deduplicator_state(runtime.state_placement(
                &domain,
                RuntimeStateKind::Deduplicator,
                ModelKind::Deduplicator,
                &identifier,
                None,
            ))
            .expect("unchanged state should initialize");
        assert!(Arc::ptr_eq(&original, &unchanged));

        runtime.install_state_schema_fingerprints(&schedule([2; 32]));
        let changed = runtime
            .replicated_deduplicator_state(runtime.state_placement(
                &domain,
                RuntimeStateKind::Deduplicator,
                ModelKind::Deduplicator,
                &identifier,
                None,
            ))
            .expect("changed state should initialize");
        assert!(!Arc::ptr_eq(&original, &changed));
        runtime
            .purge_stale_runtime_state(&domain)
            .expect("stale state should purge");
        assert!(
            !runtime
                .inner
                .replicated_deduplicator_states
                .contains_key(&original_placement)
        );
    }
}
