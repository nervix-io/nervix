use super::*;

impl Runtime {
    pub fn new() -> Self {
        Self::with_test_hooks(RuntimeTestHooks::default())
    }

    pub fn with_test_hooks(hooks: RuntimeTestHooks) -> Self {
        Self::with_persistence(None, DEFAULT_STATE_SNAPSHOT_INTERVAL, hooks)
            .verified("the None persistence path has no fallible step")
    }

    pub fn with_persistence(
        db: Option<Database>,
        state_snapshot_interval: Duration,
        hooks: RuntimeTestHooks,
    ) -> Result<Self, RuntimePersistenceError> {
        Self::with_persistence_and_temp_dir(
            db,
            state_snapshot_interval,
            hooks,
            PathBuf::from(DEFAULT_TEMP_DIR),
        )
    }

    pub fn with_persistence_and_temp_dir(
        db: Option<Database>,
        state_snapshot_interval: Duration,
        hooks: RuntimeTestHooks,
        temp_dir: PathBuf,
    ) -> Result<Self, RuntimePersistenceError> {
        let events = RuntimeEvents::new();
        let (domain_status_changed, _) = watch::channel(0);
        let state_store = db
            .map(RuntimeStateStore::from_database)
            .transpose()?
            .map(Arc::new);
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                ingestors: Arc::new(DashMap::default()),
                ingestor_quiescence: Arc::new(DashMap::default()),
                ingestors_paused_for_memory_pressure: AtomicBool::new(false),
                ingestor_transient_errors: DashMap::default(),
                ingestor_reconnect_backoffs: DashMap::default(),
                ingestor_readiness: DashMap::default(),
                emitter_transient_errors: DashMap::default(),
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
                active_domain_alters: Arc::new(DashMap::default()),
                state_schema_fingerprints: DashMap::default(),
                domain_graphs: DashMap::default(),
                endpoint_bindings: DashMap::default(),
                routed_endpoints: DashMap::default(),
                relay_boundary_fanouts: DashMap::default(),
                events,
                emitter_faults: hooks.emitter_faults,
                ingestor_faults: hooks.ingestor_faults,
                otel_client_faults: hooks.otel_client_faults,
                #[cfg(feature = "testing")]
                schedule_publication_faults: hooks.schedule_publication_faults,
                #[cfg(feature = "testing")]
                transaction_binding_drops: hooks.transaction_binding_drops,
                #[cfg(feature = "testing")]
                command_pauses: hooks.command_pauses,
                #[cfg(feature = "testing")]
                entity_gate_pauses: hooks.entity_gate_pauses,
                #[cfg(feature = "testing")]
                syslog_ingestor_bind_address_overrides: hooks
                    .syslog_ingestor_bind_address_overrides,
                resource_store: RwLock::new(None),
                resource_versions: RwLock::new(ResourceVersionStatus::default()),
                remote_dispatcher: RwLock::new(None),
                remote_dispatch: Arc::new(RemoteDispatchRegistry {
                    local_node_id: RwLock::new(None),
                    next_ack_id: AtomicU64::new(1),
                    pending_acks: DashMap::default(),
                    pending_relay_admissions: DashMap::default(),
                }),
                next_state_sync_correlation_id: AtomicU64::new(1),
                pending_state_syncs: DashMap::default(),
                expiring_stream_states: DashMap::default(),
                latest_resource_versions: DashMap::default(),
                replicated_deduplicator_states: DashMap::default(),
                replicated_kafka_offset_states: DashMap::default(),
                replicated_materialized_stream_states: DashMap::default(),
                relay_state_epochs: DashMap::default(),
                materialized_state_changed: Notify::new(),
                replicated_window_processor_states: DashMap::default(),
                replicated_wasm_processor_states: DashMap::default(),
                replicated_branch_aggregated_states: DashMap::default(),
                wasm_runtime: WasmRuntime::new(WasmRuntimeConfig::default())
                    .assured("wasmtime accepts its own default configuration"),
                branch_instance_expiration_scan_interval: hooks
                    .branch_instance_expiration_scan_interval
                    .unwrap_or(BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL),
                state_store,
                state_snapshot_interval,
                state_replication_poll_interval: DEFAULT_STATE_REPLICATION_POLL_INTERVAL,
                domain_drain_timeout: hooks
                    .domain_drain_timeout
                    .unwrap_or(DEFAULT_DOMAIN_DRAIN_TIMEOUT),
                entity_gate_deadline: hooks
                    .entity_gate_deadline
                    .unwrap_or(DEFAULT_DOMAIN_DRAIN_TIMEOUT),
                temp_dir,
                executor: Executor::default(),
                metrics: RuntimeMetrics::default(),
            }),
        })
    }

    pub fn metrics(&self) -> RuntimeMetrics {
        self.inner.metrics.clone()
    }

    /// The node's bounded execution and transient-memory admission.
    pub fn executor(&self) -> &Executor {
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

    pub fn domain_drain_timeout(&self) -> Duration {
        self.inner.domain_drain_timeout
    }

    /// How long a branch task is given to stop: the configured drain timeout plus the grace it
    /// needs to finish the flush already in progress.
    pub(super) fn branch_task_stop_timeout(&self) -> Duration {
        // Saturation is the meaning: a drain timeout configured near `Duration::MAX` already asks
        // to wait for as long as the process runs, and no grace can extend that further.
        self.inner
            .domain_drain_timeout
            .saturating_add(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE)
    }

    pub fn entity_gate_deadline(&self) -> Duration {
        self.inner.entity_gate_deadline
    }

    #[cfg(feature = "testing")]
    pub fn take_armed_schedule_publication_fault(&self, domain: &DomainName) -> bool {
        self.inner
            .schedule_publication_faults
            .take_armed_fault(domain)
    }

    #[cfg(feature = "testing")]
    pub fn take_armed_transaction_binding_drop(&self, node_id: &ClusterNodeName) -> bool {
        self.inner.transaction_binding_drops.take(node_id)
    }

    #[cfg(feature = "testing")]
    pub async fn pause_transaction_commit_after_progress_if_armed(
        &self,
        node_id: &ClusterNodeName,
        completed_statements: usize,
    ) {
        self.inner
            .command_pauses
            .pause_if_armed(test_hooks::CommandPausePoint::TransactionCommit {
                node_id: node_id.clone(),
                completed_statements,
            })
            .await;
    }

    #[cfg(feature = "testing")]
    pub async fn pause_command_admission_if_armed(&self, node_id: &ClusterNodeName) {
        self.inner
            .command_pauses
            .pause_if_armed(test_hooks::CommandPausePoint::Admission(node_id.clone()))
            .await;
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.inner.events.subscribe()
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
        if !self
            .inner
            .domains
            .get(domain)
            .is_some_and(|state| matches!(state.status, nervix_models::DomainStatus::Paused))
        {
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

    pub async fn shutdown(&self) {
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
        self.inner.expiring_stream_states.clear();
        self.inner.replicated_deduplicator_states.clear();
        self.inner.replicated_kafka_offset_states.clear();
        self.inner.replicated_materialized_stream_states.clear();
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
