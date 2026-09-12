use super::*;

impl Runtime {
    pub(crate) fn new() -> Self {
        Self::with_persistence(None, DEFAULT_STATE_SNAPSHOT_INTERVAL)
            .verified("the None persistence path has no fallible step")
    }

    pub(in crate::runtime) fn with_persistence(
        db: Option<Database>,
        state_snapshot_interval: Duration,
    ) -> Result<Self, RuntimePersistenceError> {
        Self::with_persistence_and_temp_dir(
            db,
            state_snapshot_interval,
            ConfiguredFaultInjection::default(),
            PathBuf::from(DEFAULT_TEMP_DIR),
        )
    }

    pub(crate) fn with_persistence_and_temp_dir(
        db: Option<Database>,
        state_snapshot_interval: Duration,
        fault_injection: ConfiguredFaultInjection,
        temp_dir: PathBuf,
    ) -> Result<Self, RuntimePersistenceError> {
        let events = RuntimeEvents::new();
        let executor = Executor::default();
        let (domain_status_changed, _) = watch::channel(0);
        let state_store = db
            .map(RuntimeStateStore::from_database)
            .transpose()?
            .map(Arc::new);
        let prepared_runtime_state_handoffs = DashMap::default();
        if let Some(store) = state_store.as_ref() {
            let persisted_handoffs = store
                .handoff_preparations()
                .map_err(|error| error.current_context().clone())?;
            for persisted in persisted_handoffs {
                let mut checkpoints = Vec::with_capacity(persisted.checkpoints.len());
                for (placement, snapshot) in persisted.checkpoints {
                    let placement = RuntimeStatePlacement::from_remote(placement)
                        .map_err(RuntimePersistenceError::DecodeState)?;
                    checkpoints.push((placement, snapshot));
                }
                prepared_runtime_state_handoffs.insert(
                    DomainNodeRef::node_in(persisted.domain, persisted.kind, persisted.identifier),
                    PreparedRuntimeStateHandoff {
                        operation_id: persisted.operation_id,
                        source: persisted.source,
                        destination: persisted.destination,
                        source_incarnation: persisted.source_incarnation,
                        destination_incarnation: persisted.destination_incarnation,
                        base_schedule_fingerprint: persisted.base_schedule_fingerprint,
                        target_schedule_fingerprint: persisted.target_schedule_fingerprint,
                        activation_authorization: super::state_replication::
                            OwnershipHandoffActivationAuthorization::RecoveredAwaitingRequest,
                        activation: watch::channel(
                            super::state_replication::OwnershipHandoffActivation::Prepared,
                        )
                        .0,
                        checkpoints,
                    },
                );
            }
        }
        let branch_instance_expiration_scan_interval = fault_injection
            .branch_instance_expiration_scan_interval()
            .unwrap_or(BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL);
        let domain_drain_timeout = fault_injection
            .domain_drain_timeout()
            .unwrap_or(DEFAULT_DOMAIN_DRAIN_TIMEOUT);
        let entity_gate_deadline = fault_injection
            .entity_gate_deadline()
            .unwrap_or(DEFAULT_DOMAIN_DRAIN_TIMEOUT);
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                ingestors: Arc::new(DashMap::default()),
                ingestor_quiescence: Arc::new(DashMap::default()),
                ingestors_paused_for_memory_pressure: AtomicBool::new(false),
                ingestor_transient_errors: DashMap::default(),
                ingestor_reconnect_backoffs: DashMap::default(),
                ingestor_readiness: DashMap::default(),
                emitter_transient_errors: DashMap::default(),
                shared_clients: DashMap::default(),
                pool_waits: DashMap::default(),
                emitter_retry_statuses: DashMap::default(),
                emitter_confirmation_waits: DashMap::default(),
                executions: DashMap::default(),
                message_error_routes: DashMap::default(),
                compiled_domain_udfs: DashMap::default(),
                schedule_apply_lock: Mutex::new(()),
                applied_cluster_revision: AtomicU64::new(u64::MAX),
                domain_instantiation_errors: DashMap::default(),
                domains: DashMap::default(),
                domain_status_changed,
                in_flight_by_domain: DashMap::default(),
                in_flight_by_ingestor: DashMap::default(),
                generator_activity_by_domain: DashMap::default(),
                emitter_buffers: DashMap::default(),
                force_flush_by_domain: DashMap::default(),
                node_quiesce_counters: DashMap::default(),
                entity_gate_holds: Arc::new(DashMap::default()),
                frozen_ownership_handoff_entities: Arc::new(DashMap::default()),
                ownership_handoff_freeze_changed: Arc::new(Notify::new()),
                active_domain_alters: Arc::new(DashMap::default()),
                state_schema_fingerprints: DashMap::default(),
                domain_graphs: DashMap::default(),
                endpoint_bindings: DashMap::default(),
                routed_endpoints: DashMap::default(),
                relay_boundary_fanouts: DashMap::default(),
                events,
                fault_injection,
                resource_store: RwLock::new(None),
                resource_versions: RwLock::new(ResourceVersionStatus::default()),
                remote_dispatcher: RwLock::new(None),
                remote_dispatch: Arc::new(RemoteDispatchRegistry {
                    local_node_id: RwLock::new(None),
                    local_node_incarnation: RwLock::new(None),
                    next_ack_id: AtomicU64::new(1),
                    pending_acks: DashMap::default(),
                    pending_relay_admissions: DashMap::default(),
                }),
                remote_ack_watcher_shutdown: CancellationToken::new(),
                remote_ack_watcher_tasks: TaskTracker::new(),
                state_checkpoint_notifications: DashMap::default(),
                pending_state_replica_syncs: DashMap::default(),
                pending_state_checkpoint_announcements: DashMap::default(),
                state_replication_tasks: TaskTracker::new(),
                passive_runtime_state_snapshots: DashMap::default(),
                replicated_branch_lru_snapshots: DashMap::default(),
                prepared_runtime_state_handoffs,
                activated_runtime_state_handoffs: DashMap::default(),
                prepared_forced_runtime_state_recoveries: DashMap::default(),
                prepared_runtime_state_snapshots: DashMap::default(),
                expiring_stream_states: DashMap::default(),
                latest_resource_versions: DashMap::default(),
                replicated_deduplicator_states: DashMap::default(),
                replicated_kafka_offset_states: DashMap::default(),
                replicated_materialized_stream_states: DashMap::default(),
                restored_materialized_stream_states: DashMap::default(),
                relay_state_epochs: DashMap::default(),
                materialized_state_changed: Notify::new(),
                replicated_window_processor_states: DashMap::default(),
                replicated_wasm_processor_states: DashMap::default(),
                replicated_branch_aggregated_states: DashMap::default(),
                wasm_runtime: WasmRuntime::new(WasmRuntimeConfig::default())
                    .assured("wasmtime accepts its own default configuration"),
                branch_instance_expiration_scan_interval,
                state_store,
                snapshot_staging: SnapshotStaging::new(
                    temp_dir.join("snapshot-staging"),
                    executor.clone(),
                    SnapshotStagingLimits::default(),
                ),
                state_snapshot_interval,
                state_replication_poll_interval: DEFAULT_STATE_REPLICATION_POLL_INTERVAL,
                domain_drain_timeout,
                entity_gate_deadline,
                temp_dir,
                executor,
                metrics: RuntimeMetrics::default(),
            }),
        })
    }

    pub(crate) fn metrics(&self) -> RuntimeMetrics {
        self.inner.metrics.clone()
    }

    /// The node's bounded execution and transient-memory admission.
    pub(crate) fn executor(&self) -> &Executor {
        &self.inner.executor
    }

    /// The directory connectors stage local files in before they publish them.
    pub(in crate::runtime) fn temp_dir(&self) -> &Path {
        self.inner.temp_dir.as_path()
    }

    /// The node's runtime event bus. Connectors report transient failures here.
    pub(in crate::runtime) fn events(&self) -> &RuntimeEvents {
        &self.inner.events
    }

    pub(crate) fn domain_drain_timeout(&self) -> Duration {
        self.inner.domain_drain_timeout
    }

    /// How long a branch task is given to stop: the configured drain timeout plus the grace it
    /// needs to finish the flush already in progress.
    pub(super) fn branch_task_stop_timeout(&self) -> Duration {
        super::branch_task_stop_timeout(self.inner.domain_drain_timeout)
    }

    pub(crate) fn entity_gate_deadline(&self) -> Duration {
        self.inner.entity_gate_deadline
    }

    #[cfg(feature = "testing")]
    pub(crate) fn take_forced_entity_drain_timeout(&self, domain: &DomainName) -> bool {
        self.inner
            .fault_injection
            .take_forced_entity_drain_timeout(domain)
    }

    #[cfg(feature = "testing")]
    pub(crate) fn take_armed_schedule_publication_fault(&self, domain: &DomainName) -> bool {
        self.inner
            .fault_injection
            .take_armed_schedule_publication_fault(domain)
    }

    #[cfg(feature = "testing")]
    pub(crate) fn take_armed_transaction_binding_drop(&self, node_id: &ClusterNodeName) -> bool {
        self.inner
            .fault_injection
            .take_armed_transaction_binding_drop(node_id)
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_transaction_commit_after_progress_if_armed(
        &self,
        node_id: &ClusterNodeName,
        completed_statements: usize,
    ) {
        self.inner
            .fault_injection
            .pause_transaction_commit_after_progress_if_armed(node_id, completed_statements)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_command_admission_if_armed(&self, node_id: &ClusterNodeName) {
        self.inner
            .fault_injection
            .pause_command_admission_if_armed(node_id)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_ownership_handoff_after_preparation_if_armed(
        &self,
        domain: &DomainName,
    ) {
        self.inner
            .fault_injection
            .pause_ownership_handoff_after_preparation_if_armed(domain)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) fn scheduler_mode(&self) -> crate::registry::SchedulerMode {
        self.inner.fault_injection.scheduler_mode()
    }

    #[cfg(feature = "testing")]
    pub(crate) fn subscribe_leadership_transfers(
        &self,
    ) -> broadcast::Receiver<crate::fault_injection::LeadershipTransferRequest> {
        self.inner.fault_injection.subscribe_leadership_transfers()
    }

    pub(crate) fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.inner.events.subscribe()
    }

    pub(crate) fn report_error(&self, message: impl Into<String>) {
        self.inner.events.report_error(message);
    }

    pub(in crate::runtime) async fn stop_domain_execution(
        &self,
        domain: &DomainName,
        execution: DomainExecution,
    ) {
        self.withdraw_routed_endpoints(domain, &execution);
        execution.shutdown.send_replace(true);
        for (relay, task) in execution.relay_owner_tasks {
            tokio::task::consume_budget().await;
            if let Err(reason) = task.stop(self.branch_task_stop_timeout()).await {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    reason,
                    "relay owner task did not stop cleanly"
                );
            }
        }
        for (relay, task) in execution.relay_state_tasks {
            tokio::task::consume_budget().await;
            if let Err(reason) = task.stop(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE).await {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    reason,
                    "relay state task did not stop cleanly"
                );
            }
        }
        for (entity, node_task) in execution.node_tasks {
            Self::await_shutdown_task(
                node_task.task,
                domain,
                Some(&IngestorName::from(&entity.identifier)),
                "scheduled node",
            )
            .await;
        }
        for (entity, emitter_task) in execution.emitter_tasks {
            Self::await_shutdown_task_with_grace(
                emitter_task.task,
                domain,
                Some(&IngestorName::from(&entity.identifier)),
                "scheduled emitter",
                self.branch_task_stop_timeout(),
            )
            .await;
        }
        for (entity, task) in execution.generator_tasks {
            Self::await_shutdown_task(
                task,
                domain,
                Some(&IngestorName::from(&entity.identifier)),
                "generator",
            )
            .await;
        }
        for (entity, tasks) in execution.reingestor_tasks {
            for task in tasks {
                Self::await_shutdown_task(
                    task,
                    domain,
                    Some(&IngestorName::from(&entity.identifier)),
                    "reingestor",
                )
                .await;
            }
        }
        for (entity, tasks) in execution.placement_tasks {
            for task in tasks {
                Self::await_shutdown_task(
                    task,
                    domain,
                    Some(&IngestorName::from(&entity.identifier)),
                    "scheduled node placement",
                )
                .await;
            }
        }
        for task in execution.tasks {
            Self::await_shutdown_task(task, domain, None, "domain execution").await;
        }
        let quiesce_keys = self
            .inner
            .node_quiesce_counters
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in quiesce_keys {
            self.inner.node_quiesce_counters.remove(&key);
        }
        for (identifier, runtimes) in execution.branched_entrypoints {
            for runtime in runtimes {
                runtime.shutdown().await;
                info!(
                    domain = domain.as_str(),
                    entrypoint = identifier.as_str(),
                    "stopped branched entrypoint runtime"
                );
            }
        }
        self.stop_message_error_routes_for_domain(domain).await;
        if !self.inner.domains.contains_key(domain) {
            self.clear_runtime_state_for_domain(domain);
        }
    }

    pub(super) async fn abort_domain_execution_start(&self, domain: &DomainName) {
        self.stop_domain_ingestors(domain).await;
        if let Some((_, execution)) = self.inner.executions.remove(domain) {
            self.stop_domain_execution(domain, execution).await;
        }
        self.clear_domain_graph_handle(domain).await;
    }

    pub(in crate::runtime) async fn stop_domain_ingestors(&self, domain: &DomainName) {
        let ingestors = self
            .inner
            .ingestors
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|key| &key.domain == domain)
            .collect::<Vec<_>>();

        for key in ingestors {
            if let Err(error) = self
                .stop_ingestor(domain, &IngestorName::from(key.identifier()))
                .await
            {
                warn!(
                    domain = domain.as_str(),
                    ingestor = key.identifier().as_str(),
                    error = %error,
                    "failed to stop domain ingestor during schedule rebuild"
                );
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        let domains = self
            .inner
            .executions
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for domain in &domains {
            self.stop_domain_ingestors(domain).await;
        }
        for domain in &domains {
            if let Some((_, execution)) = self.inner.executions.remove(domain) {
                self.stop_domain_execution(domain, execution).await;
            }
            self.clear_domain_ingestor_quiescence(domain);
        }
        self.inner.endpoint_bindings.clear();
        self.inner.compiled_domain_udfs.clear();
        self.inner.ingestor_readiness.clear();
        self.inner.remote_ack_watcher_shutdown.cancel();
        self.inner.remote_ack_watcher_tasks.close();
        self.inner.remote_ack_watcher_tasks.wait().await;
        self.inner.pending_state_replica_syncs.clear();
        self.inner.pending_state_checkpoint_announcements.clear();
        self.inner.state_replication_tasks.close();
        self.inner.state_replication_tasks.wait().await;
        self.inner.expiring_stream_states.clear();
        self.inner.replicated_deduplicator_states.clear();
        self.inner.replicated_kafka_offset_states.clear();
        self.inner.replicated_materialized_stream_states.clear();
        self.inner.restored_materialized_stream_states.clear();
        self.inner.replicated_window_processor_states.clear();
        self.inner.replicated_branch_aggregated_states.clear();
    }

    pub(in crate::runtime) async fn await_ack_completion(
        shutdown_rx: &mut watch::Receiver<bool>,
        mut completion: AckCompletion,
        timeout_duration: Duration,
    ) -> Option<AckOutcome> {
        loop {
            tokio::select! {
                // A signalled stop and a dropped sender both end this wait, so the outcome
                // carries nothing the caller could act on differently.
                _ = shutdown_rx.changed() => {
                    return None;
                }
                progress = tokio::time::timeout(timeout_duration, completion.wait_for_progress()) => {
                    match progress {
                        Ok(AckProgress::Alive) => {}
                        Ok(AckProgress::Complete(outcome)) => return Some(outcome),
                        Err(_) => {
                            return Some(AckOutcome::NoAck(format!(
                                "ack timeout elapsed after {}",
                                humantime::format_duration(timeout_duration)
                            )));
                        }
                    }
                }
            }
        }
    }

    pub(in crate::runtime) async fn await_shutdown_task(
        task: JoinHandle<()>,
        domain: &DomainName,
        ingestor: Option<&IngestorName>,
        task_kind: &str,
    ) {
        const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

        Self::await_shutdown_task_with_grace(
            task,
            domain,
            ingestor,
            task_kind,
            SHUTDOWN_GRACE_PERIOD,
        )
        .await;
    }

    pub(super) async fn await_shutdown_task_with_grace(
        mut task: JoinHandle<()>,
        domain: &DomainName,
        ingestor: Option<&IngestorName>,
        task_kind: &str,
        grace_period: Duration,
    ) {
        match tokio::time::timeout(grace_period, &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if error.is_cancelled() {
                    warn!(
                        domain = domain.as_str(),
                        ingestor = ingestor.map(|name| name.as_str()),
                        task_kind,
                        "shutdown task was cancelled"
                    );
                } else {
                    error!(
                        domain = domain.as_str(),
                        ingestor = ingestor.map(|name| name.as_str()),
                        task_kind,
                        error = %error,
                        "shutdown task join failed"
                    );
                }
            }
            Err(_) => {
                warn!(
                    domain = domain.as_str(),
                    ingestor = ingestor.map(|name| name.as_str()),
                    task_kind,
                    grace_period = %humantime::format_duration(grace_period),
                    "shutdown task exceeded grace period; aborting"
                );
                task.abort();
                if let Err(error) = task.await
                    && !error.is_cancelled()
                {
                    error!(
                        domain = domain.as_str(),
                        ingestor = ingestor.map(|name| name.as_str()),
                        task_kind,
                        error = %error,
                        "aborted shutdown task join failed"
                    );
                }
            }
        }
    }
}
