#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "runtime construction, binding and teardown install or withdraw node-owned \
                  services"
    )
)]

use error_stack::ResultExt as _;

use super::*;

impl Runtime {
    pub(crate) fn new() -> Self {
        Self::with_persistence(None, DEFAULT_STATE_SNAPSHOT_INTERVAL)
            .verified("the None persistence path has no fallible step")
    }

    /// A runtime without persistence whose admitted work goes through `executor`.
    #[cfg(test)]
    pub(crate) fn with_executor(executor: Executor) -> Self {
        Self::with_persistence_and_temp_dir(
            executor,
            None,
            None,
            DEFAULT_STATE_SNAPSHOT_INTERVAL,
            ConfiguredFaultInjection::default(),
            PathBuf::from(DEFAULT_TEMP_DIR),
            DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
        .verified("the None persistence path has no fallible step")
    }

    /// A runtime without persistence that consults `fault_injection`, so a test can arm the
    /// failures and waits its runtime seams take.
    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn with_fault_injection(fault_injection: ConfiguredFaultInjection) -> Self {
        Self::with_persistence_and_temp_dir(
            Executor::default(),
            None,
            None,
            DEFAULT_STATE_SNAPSHOT_INTERVAL,
            fault_injection,
            PathBuf::from(DEFAULT_TEMP_DIR),
            DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
        .verified("the None persistence path has no fallible step")
    }

    pub(in crate::runtime) fn with_persistence(
        db: Option<Database>,
        state_snapshot_interval: Duration,
    ) -> error_stack::Result<Self, RuntimePersistenceError> {
        Self::with_persistence_and_temp_dir(
            Executor::default(),
            None,
            db,
            state_snapshot_interval,
            ConfiguredFaultInjection::default(),
            PathBuf::from(DEFAULT_TEMP_DIR),
            DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
    }

    pub(crate) fn with_persistence_and_temp_dir(
        executor: Executor,
        dns: Option<DnsResolver>,
        db: Option<Database>,
        state_snapshot_interval: Duration,
        fault_injection: ConfiguredFaultInjection,
        temp_dir: PathBuf,
        restore_staging_max_bytes: u64,
    ) -> error_stack::Result<Self, RuntimePersistenceError> {
        let events = RuntimeEvents::new();
        let (domain_status_changed, _) = watch::channel(0);
        let state_store = db
            .map(|db| {
                RuntimeStateStore::from_database(db, executor.clone(), restore_staging_max_bytes)
            })
            .transpose()?
            .map(Arc::new);
        let prepared_runtime_state_handoffs = DashMap::default();
        if let Some(store) = state_store.as_ref() {
            let persisted_handoffs = store.handoff_preparations()?;
            for persisted in persisted_handoffs {
                let mut checkpoints = Vec::with_capacity(persisted.checkpoints.len());
                for (placement, snapshot) in persisted.checkpoints {
                    let placement = RuntimeStatePlacement::from_remote(placement)
                        .change_context(RuntimePersistenceError::DecodeState)?;
                    checkpoints.push((placement, snapshot));
                }
                prepared_runtime_state_handoffs.insert(
                    DomainNodeRef::node_in(persisted.domain, persisted.kind, persisted.identifier),
                    PreparedRuntimeStateHandoff {
                        coordination: persisted.coordination,
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
                dns,
                ingestors: Arc::new(DashMap::default()),
                ingestor_quiescence: Arc::new(DashMap::default()),
                memory_pressure: MemoryPressurePause::default(),
                ingestor_statuses: DashMap::default(),
                ingestor_readiness: DashMap::default(),
                emitter_statuses: DashMap::default(),
                shared_clients: DashMap::default(),
                pool_waits: DashMap::default(),
                emitter_confirmation_waits: DashMap::default(),
                executions: DashMap::default(),
                domain_routings: domain_execution::DomainRoutings::default(),
                message_error_routes: DashMap::default(),
                compiled_domain_udfs: DashMap::default(),
                compiled_wasm_modules: DashMap::default(),
                schedule_application: Mutex::new(ScheduleApplication::default()),
                #[cfg(test)]
                test_applied_schedule: ArcSwapOption::empty(),
                applied_recovery_expansions: ArcSwapOption::empty(),
                domain_instantiation_errors: DashMap::default(),
                domains: DashMap::default(),
                domain_status_changed,
                local_intake: watch::channel(LocalIntake::Open).0,
                in_flight_by_domain: DashMap::default(),
                in_flight_by_ingestor: DashMap::default(),
                generator_activity_by_domain: DashMap::default(),
                emitter_buffers: DashMap::default(),
                force_flush_by_domain: DashMap::default(),
                node_quiesce_counters: DashMap::default(),
                entity_gate_holds: Arc::new(DashMap::default()),
                frozen_ownership_handoff_entities: Arc::new(DashMap::default()),
                active_domain_alters: Arc::new(DashMap::default()),
                state_identities: DashMap::default(),
                state_replication_routing: Default::default(),
                client_ingestors: DashMap::default(),
                client_producer_budget: client_ingestor::ClientProducerBudget::default(),
                client_emitters: DashMap::default(),
                client_emitter_budget: client_emitter::ClientEmitterBudget::default(),
                endpoint_intake_routes: EndpointIntakeRoutes::default(),
                relay_boundary_fanouts: DashMap::default(),
                events,
                fault_injection,
                resource_store: ArcSwapOption::empty(),
                remote_dispatcher: ArcSwapOption::empty(),
                remote_dispatch: Arc::new(RemoteDispatchRegistry::new(executor.clone())),
                remote_ack_watcher_shutdown: CancellationToken::new(),
                remote_ack_watcher_tasks: TaskTracker::new(),
                state_replication_tasks: Default::default(),
                passive_runtime_state_snapshots: DashMap::default(),
                backup_capture_fences: ArcSwap::from_pointee(HashMap::default()),
                replicated_branch_lifecycles: DashMap::default(),
                prepared_runtime_state_handoffs,
                activated_runtime_state_handoffs: DashMap::default(),
                prepared_forced_runtime_state_recoveries: DashMap::default(),
                prepared_runtime_state_snapshots: DashMap::default(),
                relay_branch_presences: DashMap::default(),
                replicated_deduplicator_states: DashMap::default(),
                replicated_kafka_offset_states: DashMap::default(),
                replicated_materialized_stream_states: DashMap::default(),
                restored_materialized_stream_states: DashMap::default(),
                relay_state_epochs: DashMap::default(),
                materialized_state_changed: Notify::new(),
                replicated_window_processor_states: DashMap::default(),
                replicated_wasm_processor_states: DashMap::default(),
                wasm_state_recovery_requests: ArcSwapOption::empty(),
                raised_wasm_state_recoveries: RaisedWasmStateRecoveries::default(),
                replicated_branch_aggregated_states: DashMap::default(),
                wasm_runtime: WasmRuntime::new(WasmRuntimeConfig::default())
                    .assured("wasmtime accepts its own default configuration"),
                guest_wasm_state_resets: PendingGuestWasmStateResets::default(),
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
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running tasks borrow retained node services or await their selected \
                      acknowledgement root"
        )
    )]
    pub(crate) fn executor(&self) -> &Executor {
        &self.inner.executor
    }

    /// Reserve staging space for an artifact of exactly `length` bytes this node assembles, under
    /// the same quota incoming snapshot transfers share.
    pub(crate) async fn stage_artifact(
        &self,
        length: u64,
    ) -> Result<StagedSnapshotWriter, Report<SnapshotStagingError>> {
        self.inner.snapshot_staging.stage(length).await
    }

    /// Stages an artifact of `length` bytes, refusing rather than waiting when the node's staging
    /// quota cannot hold it now.
    pub(crate) async fn try_stage_artifact(
        &self,
        length: u64,
    ) -> Result<StagedSnapshotWriter, Report<SnapshotStagingError>> {
        self.inner.snapshot_staging.try_stage(length).await
    }

    pub(crate) fn dns(&self) -> Option<&DnsResolver> {
        self.inner.dns.as_ref()
    }

    /// The directory connectors stage local files in before they publish them.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running tasks borrow retained node services or await their selected \
                      acknowledgement root"
        )
    )]
    pub(in crate::runtime) fn temp_dir(&self) -> &Path {
        self.inner.temp_dir.as_path()
    }

    /// The node's runtime event bus. Connectors report transient failures here.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running tasks borrow retained node services or await their selected \
                      acknowledgement root"
        )
    )]
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
    pub(crate) fn take_forced_domain_drain_timeout(&self, domain: &DomainName) -> bool {
        self.inner
            .fault_injection
            .take_forced_domain_drain_timeout(domain)
    }

    #[cfg(feature = "testing")]
    pub(crate) fn take_failed_entity_gate_engagement(&self, domain: &DomainName) -> bool {
        self.inner
            .fault_injection
            .take_failed_entity_gate_engagement(domain)
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
    pub(crate) fn take_shutdown_cordon_release_delay(
        &self,
        stopping_node: &ClusterNodeName,
    ) -> Option<Duration> {
        self.inner
            .fault_injection
            .take_shutdown_cordon_release_delay(stopping_node)
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_transaction_commit_after_progress_if_armed(
        &self,
        node_id: &ClusterNodeName,
        domain: &DomainName,
        completed_statements: usize,
    ) {
        self.inner
            .fault_injection
            .pause_transaction_commit_after_progress_if_armed(node_id, domain, completed_statements)
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
    pub(crate) async fn pause_restore_step_if_armed(
        &self,
        node_id: &ClusterNodeName,
        step: &nervix_models::RestoreStep,
    ) {
        self.inner
            .fault_injection
            .pause_restore_step_if_armed(node_id, step)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_backup_download_if_armed(&self, node_id: &ClusterNodeName) {
        self.inner
            .fault_injection
            .pause_backup_download_if_armed(node_id)
            .await;
    }

    /// After how many entries a test interrupts the next branch lifecycle or Kafka offset section
    /// of `entity` that a backup of `domain` streams, once. A product build never interrupts one.
    pub(crate) fn native_metadata_capture_interruption(
        &self,
        domain: &DomainName,
        entity: &ModelName,
    ) -> Option<u64> {
        self.inner
            .fault_injection
            .native_metadata_capture_interruption(domain, entity)
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_backup_cut_if_armed(&self, domain: &DomainName) {
        self.inner
            .fault_injection
            .pause_backup_cut_if_armed(domain)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_restore_branch_state_conversion_if_armed(&self, domain: &DomainName) {
        self.inner
            .fault_injection
            .pause_restore_branch_state_conversion_if_armed(domain)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_restore_state_publication_if_armed(
        &self,
        domain: &DomainName,
        coordinator: &ClusterNodeName,
    ) {
        self.inner
            .fault_injection
            .pause_restore_state_publication_if_armed(domain, coordinator)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) fn mark_restore_state_publication_refused(
        &self,
        domain: &DomainName,
        coordinator: &ClusterNodeName,
    ) {
        self.inner
            .fault_injection
            .mark_restore_state_publication_refused(domain, coordinator);
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_runtime_preparation_if_armed(&self, node_id: &ClusterNodeName) {
        self.inner
            .fault_injection
            .pause_runtime_preparation_if_armed(node_id)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_command_reference_lookup_if_armed(&self, node_id: &ClusterNodeName) {
        self.inner
            .fault_injection
            .pause_command_reference_lookup_if_armed(node_id)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) fn take_armed_resource_installation_failure(
        &self,
        node_id: &ClusterNodeName,
    ) -> bool {
        self.inner
            .fault_injection
            .take_armed_resource_installation_failure(node_id)
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_command_response_delivery_if_armed(&self, node_id: &ClusterNodeName) {
        self.inner
            .fault_injection
            .pause_command_response_delivery_if_armed(node_id)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_command_after_durable_admission_if_armed(
        &self,
        node_id: &ClusterNodeName,
    ) {
        self.inner
            .fault_injection
            .pause_command_after_durable_admission_if_armed(node_id)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_relocation_publication_if_armed(&self, domain: &DomainName) {
        self.inner
            .fault_injection
            .pause_relocation_publication_if_armed(domain)
            .await;
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_resource_installation_if_armed(&self, node_id: &ClusterNodeName) {
        self.inner
            .fault_injection
            .pause_resource_installation_if_armed(node_id)
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
    pub(crate) async fn pause_ownership_handoff_prepare_response_if_armed(
        &self,
        domain: &DomainName,
    ) {
        self.inner
            .fault_injection
            .pause_ownership_handoff_prepare_response_if_armed(domain)
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
        self.inner.endpoint_intake_routes.withdraw_domain(domain);
        execution.shutdown.send_replace(true);
        for (relay, task) in execution.relay_owner_tasks {
            nervix_primitives::task::consume_budget().await;
            if let Err(reason) = task.stop(self.branch_task_stop_timeout()).await {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    reason = %reason,
                    "relay owner task did not stop cleanly"
                );
            }
        }
        for (relay, task) in execution.relay_state_tasks {
            nervix_primitives::task::consume_budget().await;
            if let Err(reason) = task.stop(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE).await {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    reason = %reason,
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
        for services in execution.routing.relay_services.values() {
            services.retire_channels();
        }
        if !self.inner.domains.contains_key(domain) {
            self.clear_runtime_state_for_domain(domain);
        }
    }

    pub(super) async fn abort_domain_execution_start(&self, domain: &DomainName) {
        self.stop_domain_ingestors(domain).await;
        if let Some((_, mut execution)) = self.inner.executions.remove(domain) {
            execution.routing.deactivate();
            self.stop_domain_execution(domain, execution).await;
        }
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
                    error = %format_args!("{error:#}"),
                    "failed to stop domain ingestor during schedule rebuild"
                );
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.end_client_ingestor_endpoints(ClientProducerEndReason::ShuttingDown)
            .await;
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
            if let Some((_, mut execution)) = self.inner.executions.remove(domain) {
                execution.routing.deactivate();
                self.stop_domain_execution(domain, execution).await;
            }
            self.clear_domain_ingestor_quiescence(domain);
        }
        self.inner.domain_routings.clear();
        self.inner.endpoint_intake_routes.clear();
        self.inner.compiled_domain_udfs.clear();
        self.inner.compiled_wasm_modules.clear();
        for readiness in self.inner.ingestor_readiness.iter() {
            readiness.retire();
        }
        self.inner.ingestor_readiness.clear();
        self.inner.remote_ack_watcher_shutdown.cancel();
        self.inner.remote_ack_watcher_tasks.close();
        self.inner.remote_ack_watcher_tasks.wait().await;
        self.inner.remote_dispatch.shutdown();
        self.inner.state_replication_tasks.close();
        self.inner.state_replication_tasks.wait().await;
        self.inner.relay_branch_presences.clear();
        self.inner.state_replication_routing.clear();
        self.inner.replicated_deduplicator_states.clear();
        self.inner.replicated_kafka_offset_states.clear();
        self.inner.replicated_materialized_stream_states.clear();
        self.inner.restored_materialized_stream_states.clear();
        self.inner.replicated_window_processor_states.clear();
        self.inner.replicated_branch_aggregated_states.clear();
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "running tasks borrow retained node services or await their selected \
                      acknowledgement root"
        )
    )]
    pub(in crate::runtime) async fn await_ack_completion(
        shutdown_rx: &mut watch::Receiver<bool>,
        mut completion: AckCompletion,
        timeout_duration: Duration,
    ) -> Option<AckOutcome> {
        loop {
            nervix_primitives::select! {
                // A signalled stop and a dropped sender both end this wait, so the outcome
                // carries nothing the caller could act on differently.
                _ = shutdown_rx.changed() => {
                    return None;
                }
                progress = nervix_primitives::time::timeout(timeout_duration, completion.wait_for_progress()) => {
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
        match nervix_primitives::time::timeout(grace_period, &mut task).await {
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
