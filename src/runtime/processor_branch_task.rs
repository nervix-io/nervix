//! Branch-local processor task lifecycle and persisted handoff state.
//!
//! Layer: data plane.
//!
//! - **Owns.** Processor branch task creation, FIFO input execution, ticking, checkpointing,
//!   handoff, and eviction.
//! - **Depends on.** Planned processor templates, concrete branch runtimes, runtime persistence,
//!   and relay boundaries.
//! - **Must not know.** NSPL text, control-plane transactions, consensus, or connector protocols.

use super::*;

pub(super) const PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

pub(super) const PROCESSOR_BRANCH_TASK_IDLE_SLEEP: Duration = Duration::from_secs(86_400);

#[derive(Debug)]
pub(super) enum ProcessorBranchStopMode {
    Evict,
    Detach,
    Handoff(oneshot::Sender<ProcessorBranchHandoff>),
}

pub(super) enum ProcessorBranchCommand {
    Checkpoint {
        response: oneshot::Sender<OwnershipHandoffResult<()>>,
    },
    Stop(ProcessorBranchStopMode),
}

pub(super) struct ProcessorBranchTask {
    pub(super) input: mpsc::Sender<ProcessorBranchInput>,
    pub(super) commands: mpsc::Sender<ProcessorBranchCommand>,
    pub(super) task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

pub(super) struct ProcessorBranchInput {
    pub(super) relay: RelayName,
    pub(super) batch: RelayRecordBatch,
    pub(super) work: NodeQuiesceWorkGuard,
}

pub(super) type WindowProcessorSnapshotRequest = oneshot::Sender<Result<(), String>>;

pub(super) struct ProcessorSnapshotTask {
    pub(super) shutdown_tx: watch::Sender<bool>,
    pub(super) task: Option<JoinHandle<()>>,
    pub(super) requests: Option<mpsc::Receiver<WindowProcessorSnapshotRequest>>,
}

/// What spawning a processor's snapshot task produced. Only the window processor answers snapshot
/// requests, so a processor without them still reports the task it spawned.
pub(super) struct SpawnedSnapshotTask {
    pub(super) task: Option<JoinHandle<()>>,
    pub(super) requests: Option<mpsc::Receiver<WindowProcessorSnapshotRequest>>,
}

#[derive(Debug)]
pub(super) struct ProcessorBranchHandoff {
    pub(super) key: Option<BranchKey>,
    pub(super) restored_at: Timestamp,
    pub(super) pending_materialized: VecDeque<PendingMaterializedBatch>,
}

pub(super) enum ProcessorNodeCommand {
    Checkpoint {
        response: oneshot::Sender<OwnershipHandoffResult<PersistedRuntimeStateEntry>>,
    },
    Handoff {
        response: oneshot::Sender<Vec<ProcessorBranchHandoff>>,
    },
}

impl RelayInteractionCommand for ProcessorNodeCommand {
    fn drain_inputs_before_handling(&self) -> bool {
        true
    }

    fn cancels_external_waits_while_draining(&self) -> bool {
        true
    }
}

/// The domain-scoped execution context a processor task runs inside: the runtime it calls back
/// into, the domain that owns it, and the active graph it evaluates against. Carrying the three as
/// one value keeps a task's spawn and run halves provably agreed on which domain's graph they serve.
#[derive(Clone)]
pub(in crate::runtime) struct ProcessorRuntimeContext {
    pub(super) runtime_handle: Runtime,
    pub(super) domain: DomainName,
    pub(super) graph: SharedActiveGraph,
}

impl ProcessorRuntimeContext {
    pub(in crate::runtime) fn new(
        runtime_handle: Runtime,
        domain: DomainName,
        graph: SharedActiveGraph,
    ) -> Self {
        Self {
            runtime_handle,
            domain,
            graph,
        }
    }
}

pub(in crate::runtime) fn spawn_processor_node_runtime(
    context: ProcessorRuntimeContext,
    shutdown_tx: &watch::Sender<bool>,
    template: BranchInstanceTemplate,
    inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
    expiration_scan_interval: Duration,
) -> ScheduledNodeTask {
    spawn_processor_node_runtime_with_handoffs(
        context,
        shutdown_tx,
        template,
        inputs,
        Vec::new(),
        expiration_scan_interval,
    )
}

pub(in crate::runtime) fn spawn_processor_node_runtime_with_handoffs(
    context: ProcessorRuntimeContext,
    shutdown_tx: &watch::Sender<bool>,
    template: BranchInstanceTemplate,
    inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
    handoffs: Vec<ProcessorBranchHandoff>,
    expiration_scan_interval: Duration,
) -> ScheduledNodeTask {
    let shutdown_rx = shutdown_tx.subscribe();
    let (commands, command_rx) = mpsc::channel(1);
    let task = tokio::spawn(run_processor_node_runtime(
        context,
        template,
        inputs,
        shutdown_rx,
        command_rx,
        handoffs,
        expiration_scan_interval,
    ));
    ScheduledNodeTask { commands, task }
}

pub(super) async fn run_processor_node_runtime(
    context: ProcessorRuntimeContext,
    template: BranchInstanceTemplate,
    inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
    shutdown_rx: watch::Receiver<bool>,
    command_rx: mpsc::Receiver<ProcessorNodeCommand>,
    restored_handoffs: Vec<ProcessorBranchHandoff>,
    expiration_scan_interval: Duration,
) {
    let ProcessorRuntimeContext {
        runtime_handle,
        domain,
        graph,
    } = context;
    let processor = template.source.clone();
    let domain_clock = match runtime_handle.bind_domain_clock(&domain) {
        Ok(clock) => clock,
        Err(error) => {
            runtime_handle.events().report_error(format!(
                "processor '{}' in domain '{}' could not bind its clock: {error}",
                processor.as_str(),
                domain.as_str(),
            ));
            return;
        }
    };
    let ownership_entity = DomainNodeRef::node_in(
        domain.clone(),
        template.source_kind,
        ModelName::from(&processor),
    );
    runtime_handle.register_branch_lifecycle_metrics(&domain, template.branch.as_ref());
    let mut instances = BranchInstanceRegistry::<Option<BranchKey>, ProcessorBranchTask>::new();
    let mut last_persisted_lru_lsm = 0;
    if restored_handoffs.is_empty() {
        last_persisted_lru_lsm = match restore_processor_branch_lru_snapshot(
            &runtime_handle,
            &domain,
            &graph,
            &template,
            &mut instances,
        ) {
            Ok(lsm) => lsm,
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    processor = processor.as_str(),
                    error = %error,
                    "failed to restore processor branch lru snapshot"
                );
                0
            }
        };
    } else {
        for handoff in restored_handoffs {
            let key = handoff.key.clone();
            match spawn_processor_branch_task(
                ProcessorRuntimeContext::new(runtime_handle.clone(), domain.clone(), graph.clone()),
                &template,
                key.clone(),
                handoff.pending_materialized,
            ) {
                Ok(entry) => {
                    runtime_handle.observe_branch_instance_created(
                        &domain,
                        template.branch.as_ref(),
                        &key,
                    );
                    instances.insert_restored(key, handoff.restored_at, entry);
                }
                Err(error) => {
                    warn!(
                        domain = domain.as_str(),
                        processor = processor.as_str(),
                        error = %error,
                        "failed to restore handed-off processor branch"
                    );
                }
            }
        }
    }
    if let Some(max_instances) = template.branch_max_instances {
        evict_processor_branch_instances_to_capacity(
            &runtime_handle,
            &domain,
            &processor,
            template.branch.as_ref(),
            max_instances,
            &mut instances,
        )
        .await;
    }
    let quiesce_counters = runtime_handle
        .node_quiesce_counters(&domain, NodeRef::new(template.source_kind, &processor));
    let interaction_inputs = inputs
        .into_iter()
        // Processor collection is branch-local and paced by the domain clock. The outer relay
        // interaction therefore delivers each dequeued batch unchanged.
        .map(|(relay, receiver)| RelayInteractionInput::new(relay, receiver, None))
        .collect();
    let mut interaction = RelayInteraction::with_commands(
        interaction_inputs,
        shutdown_rx,
        // Branch tasks own processor state and output buffers, so they remain the force-flush
        // participants. The supervisor only drains and dispatches relay input.
        None,
        Some(quiesce_counters),
        command_rx,
    )
    .verified("the registry validated these processor inputs before the node was started");
    let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
    let mut next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();

    let mut handoff_response = None;
    loop {
        tokio::task::consume_budget().await;
        let ownership_frozen = runtime_handle.ownership_handoff_entity_is_frozen(&ownership_entity);
        let now = match domain_clock.snapshot() {
            Ok(snapshot) => snapshot.now(),
            Err(error) => {
                runtime_handle.events().report_error(format!(
                    "processor '{}' in domain '{}' lost its clock: {error}",
                    processor.as_str(),
                    domain.as_str(),
                ));
                break;
            }
        };
        let mut did_scheduled_work = false;
        if !ownership_frozen && Instant::now() >= next_expiration_scan {
            if let Some(branch_ttl) = template.branch_ttl {
                expire_processor_branch_instances(
                    &runtime_handle,
                    &domain,
                    &processor,
                    template.branch.as_ref(),
                    now,
                    branch_ttl,
                    &mut instances,
                )
                .await;
            }
            next_expiration_scan = Instant::now() + expiration_scan_interval;
            did_scheduled_work = true;
        }
        if Instant::now() >= next_lru_snapshot {
            if let Err(error) = persist_branch_instance_lru_snapshot(
                &runtime_handle,
                &domain,
                &template,
                &instances,
                &mut last_persisted_lru_lsm,
            ) {
                warn!(
                    domain = domain.as_str(),
                    processor = processor.as_str(),
                    error = %error,
                    "failed to persist processor branch lru snapshot"
                );
            }
            next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();
            did_scheduled_work = true;
        }
        if did_scheduled_work {
            continue;
        }

        let wake_at = if ownership_frozen {
            Some(Instant::now() + OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL)
        } else {
            Some(next_expiration_scan.min(next_lru_snapshot))
        };
        let work = match interaction.next(wake_at).await {
            Ok(work) => work,
            Err(error) => {
                runtime_handle.handle_internal_processor_error_for_acks(
                    &domain,
                    template.source_kind,
                    &processor,
                    &template.error_policies,
                    error.acks(),
                    format!(
                        "processor '{}' relay interaction failed: {error}",
                        processor.as_str()
                    ),
                );
                continue;
            }
        };
        let (event, work) = work.into_parts();
        match event {
            RelayInteractionEvent::Batch { relay, batch } => {
                let work = work.verified(
                    "a batch event always carries the quiesce work the interaction recorded for it",
                );
                dispatch_processor_node_input(
                    ProcessorNodeDispatchContext {
                        runtime_handle: &runtime_handle,
                        domain: &domain,
                        graph: &graph,
                        template: &template,
                        now,
                    },
                    &mut instances,
                    relay,
                    batch,
                    work,
                )
                .await;
            }
            RelayInteractionEvent::Wake => {}
            RelayInteractionEvent::Command(command) => match command {
                ProcessorNodeCommand::Checkpoint { response } => {
                    let result = checkpoint_all_processor_branch_instances(
                        &runtime_handle,
                        &domain,
                        processor.clone(),
                        &template,
                        &instances,
                    )
                    .await;
                    response
                        .send(result)
                        .means_peer_left("processor lifecycle checkpoint requester");
                }
                ProcessorNodeCommand::Handoff { response } => {
                    handoff_response = Some(response);
                    break;
                }
            },
            RelayInteractionEvent::ForceFlush(completion) => {
                // The supervisor never registers as a participant; keep the exhaustive arm from
                // stranding an obligation if that ownership changes in the future.
                completion.complete();
            }
            RelayInteractionEvent::Stopped(reason) => {
                debug!(
                    domain = domain.as_str(),
                    processor = processor.as_str(),
                    ?reason,
                    "processor relay interaction stopped"
                );
                break;
            }
        }
    }

    if let Err(error) = persist_branch_instance_lru_snapshot(
        &runtime_handle,
        &domain,
        &template,
        &instances,
        &mut last_persisted_lru_lsm,
    ) {
        warn!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            error = %error,
            "failed to persist final processor branch lru snapshot"
        );
    }
    if let Some(response) = handoff_response {
        let handoffs = handoff_all_processor_branch_instances(
            &runtime_handle,
            &domain,
            &processor,
            template.branch.as_ref(),
            &mut instances,
        )
        .await;
        response
            .send(handoffs)
            .means_peer_left("processor handoff requester");
    } else {
        shutdown_all_processor_branch_instances(
            &runtime_handle,
            &domain,
            &processor,
            template.branch.as_ref(),
            &mut instances,
        )
        .await;
    }
}

pub(super) struct ProcessorNodeDispatchContext<'a> {
    pub(super) runtime_handle: &'a Runtime,
    pub(super) domain: &'a DomainName,
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) template: &'a BranchInstanceTemplate,
    pub(super) now: Timestamp,
}

pub(super) async fn dispatch_processor_node_input(
    context: ProcessorNodeDispatchContext<'_>,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
    relay: RelayName,
    batch: RelayRecordBatch,
    dequeued_work: NodeQuiesceWorkGuard,
) {
    let ProcessorNodeDispatchContext {
        runtime_handle,
        domain,
        graph,
        template,
        now,
    } = context;
    let key = batch.key.clone();
    let instance = match instances.get_or_try_create_with(key.clone(), now, |key| {
        spawn_processor_branch_task(
            ProcessorRuntimeContext::new(runtime_handle.clone(), domain.clone(), graph.clone()),
            template,
            key.clone(),
            VecDeque::new(),
        )
    }) {
        Ok(instance) => instance,
        Err(error) => {
            runtime_handle.handle_internal_processor_error_for_acks(
                domain,
                template.source_kind,
                &template.source,
                &template.error_policies,
                batch.acks.iter(),
                format!(
                    "failed to instantiate processor branch '{}': {}",
                    branch_key_display(&key),
                    error
                ),
            );
            return;
        }
    };
    if instance.created {
        runtime_handle.observe_branch_instance_created(domain, template.branch.as_ref(), &key);
        debug!(
            domain = domain.as_str(),
            processor = template.source.as_str(),
            key = branch_key_display(&key),
            "created processor branch task"
        );
        if let Some(max_instances) = template.branch_max_instances {
            evict_processor_branch_instances_to_capacity(
                runtime_handle,
                domain,
                &template.source,
                template.branch.as_ref(),
                max_instances,
                instances,
            )
            .await;
        }
    }
    let input = ProcessorBranchInput {
        relay,
        batch,
        work: dequeued_work,
    };
    if let Err(mpsc::error::SendError(input)) = instance.state.input.send(input).await {
        runtime_handle.handle_internal_processor_error_for_acks(
            domain,
            template.source_kind,
            &template.source,
            &template.error_policies,
            input.batch.acks.iter(),
            format!(
                "processor branch task '{}' is unavailable",
                branch_key_display(&key)
            ),
        );
        if let Some(entry) = instances.remove(&key) {
            runtime_handle.observe_branch_instance_removed(
                domain,
                template.branch.as_ref(),
                &key,
                None,
            );
            stop_processor_branch_task(
                domain,
                &template.source,
                &key,
                entry,
                ProcessorBranchStopMode::Detach,
            )
            .await;
        }
        drop(input.work);
    }
}

pub(super) fn spawn_processor_branch_task(
    context: ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    key: Option<BranchKey>,
    pending_materialized: VecDeque<PendingMaterializedBatch>,
) -> Result<ProcessorBranchTask, String> {
    let mut branch = template
        .instantiate(&context.runtime_handle, &context.domain, key)?
        .into_inner();
    if let Some(processor) = branch
        .processors
        .get_mut(&ModelName::from(&template.source))
    {
        processor.pending_materialized = pending_materialized;
    }
    let (input_tx, input_rx) = mpsc::channel(1);
    let (command_tx, command_rx) = mpsc::channel(1);
    let processor = template.source.clone();
    let (snapshot_shutdown_tx, _) = watch::channel(false);
    let spawned = match branch.processors.get(&ModelName::from(&processor)) {
        Some(processor) => {
            processor.spawn_snapshot_task(&context.runtime_handle, &snapshot_shutdown_tx)
        }
        None => SpawnedSnapshotTask {
            task: None,
            requests: None,
        },
    };
    let snapshot_task = ProcessorSnapshotTask {
        shutdown_tx: snapshot_shutdown_tx,
        task: spawned.task,
        requests: spawned.requests,
    };
    let quiesce_counters = context.runtime_handle.node_quiesce_counters(
        &context.domain,
        NodeRef::new(template.source_kind, &processor),
    );
    let task = tokio::spawn(run_processor_branch_task(
        context,
        ModelName::from(&processor),
        branch,
        input_rx,
        command_rx,
        quiesce_counters,
        snapshot_task,
    ));
    Ok(ProcessorBranchTask {
        input: input_tx,
        commands: command_tx,
        task: parking_lot::Mutex::new(Some(task)),
    })
}

pub(super) async fn stop_processor_snapshot_task(
    branch: &mut BranchRuntime,
    processor: &ModelName,
    snapshot: &mut ProcessorSnapshotTask,
) {
    if let Some(requests) = snapshot.requests.as_mut() {
        requests.close();
        while let Some(response) = requests.recv().await {
            tokio::task::consume_budget().await;
            let result = branch.snapshot_processor_live_state(processor);
            response
                .send(result)
                .means_peer_left("processor snapshot requester");
        }
    }
    // A failed snapshot routes the entries it could not store through the processor's error policy
    // and clears them, so the second attempt is what persists that cleared state: without it a
    // replacement node would restore entries this branch has already failed. The first failure is
    // reported by the error policy; the second one leaves the earlier snapshot as the last state
    // anyone can restore, and this is the only place that fact exists.
    if snapshot.task.is_some()
        && branch.snapshot_processor_live_state(processor).is_err()
        && let Err(error) = branch.snapshot_processor_live_state(processor)
    {
        warn!(
            processor = processor.as_str(),
            error = %error,
            "processor state snapshot failed again after clearing the entries it could not store"
        );
    }
    snapshot.shutdown_tx.send_replace(true);
    if let Some(task) = snapshot.task.take()
        && let Err(error) = task.await
    {
        warn!(
            processor = processor.as_str(),
            error = %error,
            "processor state snapshot task join failed"
        );
    }
}

pub(super) async fn run_processor_branch_task(
    context: ProcessorRuntimeContext,
    processor: ModelName,
    mut branch: BranchRuntime,
    mut input: mpsc::Receiver<ProcessorBranchInput>,
    mut command_rx: mpsc::Receiver<ProcessorBranchCommand>,
    quiesce_counters: Arc<NodeQuiesceCounters>,
    mut snapshot: ProcessorSnapshotTask,
) {
    let ProcessorRuntimeContext {
        runtime_handle,
        domain,
        graph,
    } = context;
    let mut force_flush = runtime_handle.force_flush_participant(&domain, quiesce_counters.clone());
    let mut quiesce_gauges = BranchQuiesceGauges::new(quiesce_counters.clone());
    let ownership_entity =
        DomainNodeRef::node_in(domain.clone(), branch.source_kind, processor.clone());
    let domain_clock = match runtime_handle.bind_domain_clock(&domain) {
        Ok(clock) => clock,
        Err(error) => {
            runtime_handle.events().report_error(format!(
                "processor branch '{}' in domain '{}' could not bind its clock: {error}",
                processor.as_str(),
                domain.as_str(),
            ));
            return;
        }
    };
    quiesce_gauges.observe(&branch, &processor);
    let stop_mode;
    let mut handoff_execution_now = None;
    loop {
        tokio::task::consume_budget().await;
        let ownership_frozen = runtime_handle.ownership_handoff_entity_is_frozen(&ownership_entity);
        let now = match domain_clock.snapshot() {
            Ok(snapshot) => snapshot.now(),
            Err(error) => {
                runtime_handle.events().report_error(format!(
                    "processor branch '{}' in domain '{}' lost its clock: {error}",
                    processor.as_str(),
                    domain.as_str(),
                ));
                stop_mode = Some(ProcessorBranchStopMode::Detach);
                break;
            }
        };
        if !ownership_frozen
            && branch
                .next_deadline()
                .is_some_and(|deadline| deadline <= now)
        {
            branch.tick(&graph, now).await;
            quiesce_gauges.observe(&branch, &processor);
            continue;
        }
        let sleep_duration = if ownership_frozen {
            OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL
        } else {
            match branch.next_deadline() {
                Some(deadline) => {
                    match wall_duration_until_domain_deadline(
                        &runtime_handle,
                        &domain,
                        now,
                        deadline,
                    ) {
                        Ok(duration) => duration,
                        Err(error) => {
                            runtime_handle.events().report_error(format!(
                                "processor '{}' in domain '{}' lost its clock: {error}",
                                processor.as_str(),
                                domain.as_str(),
                            ));
                            stop_mode = Some(ProcessorBranchStopMode::Detach);
                            break;
                        }
                    }
                }
                None => PROCESSOR_BRANCH_TASK_IDLE_SLEEP,
            }
        };
        let has_pending_materialized = branch.processor_has_pending_materialized(&processor);
        tokio::select! {
            biased;
            command = command_rx.recv() => {
                match command {
                    Some(ProcessorBranchCommand::Checkpoint { response }) => {
                        let result = match domain_clock.snapshot() {
                            Ok(snapshot) => {
                                branch
                                    .checkpoint_processor_live_state(&processor, snapshot.now())
                                    .await
                            }
                            Err(error) => Err(OwnershipHandoffError::checkpoint(format!(
                                "processor branch '{}' in domain '{}' lost its clock: {error}",
                                processor.as_str(),
                                domain.as_str(),
                            ))),
                        };
                        response
                            .send(result)
                            .means_peer_left("processor branch checkpoint requester");
                    }
                    Some(ProcessorBranchCommand::Stop(mode)) => {
                        if let ProcessorBranchStopMode::Handoff(_) = &mode {
                            let execution_now = match domain_clock.snapshot() {
                                Ok(snapshot) => snapshot.now(),
                                Err(error) => {
                                    runtime_handle.events().report_error(format!(
                                        "processor branch '{}' in domain '{}' lost its clock: \
                                         {error}",
                                        processor.as_str(),
                                        domain.as_str(),
                                    ));
                                    stop_mode = Some(ProcessorBranchStopMode::Detach);
                                    break;
                                }
                            };
                            handoff_execution_now = Some(execution_now);
                        }
                        stop_mode = Some(mode);
                        break;
                    }
                    None => {
                        stop_mode = Some(ProcessorBranchStopMode::Detach);
                        break;
                    }
                }
            }
            snapshot_request = async {
                snapshot.requests
                    .as_mut()
                    .verified("this select branch only runs while the snapshot receiver is present")
                    .recv()
                    .await
            }, if snapshot.requests.is_some() => {
                match snapshot_request {
                    Some(response) => {
                        let result = branch.snapshot_processor_live_state(&processor);
                        response
                            .send(result)
                            .means_peer_left("processor snapshot requester");
                    }
                    None => snapshot.requests = None,
                }
            }
            received = input.recv(), if !ownership_frozen => {
                match received {
                    Some(ProcessorBranchInput { relay, batch, work }) => {
                        branch
                            .execute_processor_input(&graph, &processor, &relay, batch)
                            .await;
                        quiesce_gauges.observe(&branch, &processor);
                        drop(work);
                    }
                    None => {
                        stop_mode = Some(ProcessorBranchStopMode::Detach);
                        break;
                    }
                }
            }
            _ = async {
                tokio::select! {
                    _ = runtime_handle.inner.materialized_state_changed.notified() => {}
                    _ = sleep(runtime_handle.inner.state_replication_poll_interval) => {}
                }
            }, if has_pending_materialized && !ownership_frozen => {
                branch
                    .retry_processor_pending_materialized(&graph, &processor)
                    .await;
                quiesce_gauges.observe(&branch, &processor);
            }
            completion = force_flush.changed(), if !ownership_frozen => {
                let Ok(completion) = completion else {
                    stop_mode = Some(ProcessorBranchStopMode::Detach);
                    break;
                };
                let execution_now = match domain_clock.snapshot() {
                    Ok(snapshot) => snapshot.now(),
                    Err(error) => {
                        runtime_handle.events().report_error(format!(
                            "processor branch '{}' in domain '{}' lost its clock: {error}",
                            processor.as_str(),
                            domain.as_str(),
                        ));
                        completion.complete();
                        stop_mode = Some(ProcessorBranchStopMode::Detach);
                        break;
                    }
                };
                branch.force_flush(&graph, execution_now).await;
                quiesce_gauges.observe(&branch, &processor);
                completion.complete();
            }
            _ = runtime_handle.inner.ownership_handoff_freeze_changed.notified(), if ownership_frozen => {}
            _ = sleep(sleep_duration) => {}
        }
    }
    while let Ok(ProcessorBranchInput { relay, batch, work }) = input.try_recv() {
        branch
            .execute_processor_input(&graph, &processor, &relay, batch)
            .await;
        quiesce_gauges.observe(&branch, &processor);
        drop(work);
    }
    let handoff_timestamp = match &stop_mode {
        Some(ProcessorBranchStopMode::Handoff(_)) => {
            branch
                .flush_processor_collected_inputs(&graph, &processor)
                .await;
            let now = handoff_execution_now.verified(
                "the handoff command arm captures the validated domain time for this stop mode",
            );
            branch.force_flush(&graph, now).await;
            Some(now)
        }
        Some(ProcessorBranchStopMode::Detach) | None => {
            branch
                .flush_processor_collected_inputs(&graph, &processor)
                .await;
            None
        }
        Some(ProcessorBranchStopMode::Evict) => None,
    };
    stop_processor_snapshot_task(&mut branch, &processor, &mut snapshot).await;
    match stop_mode {
        Some(ProcessorBranchStopMode::Evict) => branch.evict().await,
        Some(ProcessorBranchStopMode::Handoff(response)) => {
            let restored_at = handoff_timestamp
                .verified("the handoff arm above captured the timestamp for this same stop mode");
            let pending_materialized = match branch.processors.get_mut(&processor) {
                Some(processor) => std::mem::take(&mut processor.pending_materialized),
                None => VecDeque::new(),
            };
            let handoff = ProcessorBranchHandoff {
                key: branch.key.clone(),
                restored_at,
                pending_materialized,
            };
            response
                .send(handoff)
                .means_peer_left("processor branch handoff requester");
        }
        Some(ProcessorBranchStopMode::Detach) | None => {}
    }
}

pub(super) async fn stop_processor_branch_task(
    domain: &DomainName,
    processor: impl Into<ModelName>,
    key: &Option<BranchKey>,
    entry: Arc<ProcessorBranchTask>,
    mode: ProcessorBranchStopMode,
) {
    let processor = processor.into();
    entry
        .commands
        .send(ProcessorBranchCommand::Stop(mode))
        .await
        .means_shutdown("processor branch task");
    let Some(mut task) = entry.task.lock().take() else {
        return;
    };
    match tokio::time::timeout(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, &mut task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(
                domain = domain.as_str(),
                processor = processor.as_str(),
                key = branch_key_display(key),
                error = %error,
                "processor branch task join failed"
            );
        }
        Err(_) => {
            warn!(
                domain = domain.as_str(),
                processor = processor.as_str(),
                key = branch_key_display(key),
                grace_period = %humantime::format_duration(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE),
                "processor branch task exceeded shutdown grace period; aborting"
            );
            task.abort();
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                warn!(
                    domain = domain.as_str(),
                    processor = processor.as_str(),
                    key = branch_key_display(key),
                    error = %error,
                    "aborted processor branch task join failed"
                );
            }
        }
    }
}

pub(super) async fn checkpoint_all_processor_branch_instances(
    runtime: &Runtime,
    domain: &DomainName,
    processor: impl Into<ModelName>,
    template: &BranchInstanceTemplate,
    instances: &BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
    let processor = processor.into();
    let states = instances.states();
    for entry in states {
        tokio::task::consume_budget().await;
        let (response, receiver) = oneshot::channel();
        entry
            .commands
            .send(ProcessorBranchCommand::Checkpoint { response })
            .await
            .map_err(|_| {
                OwnershipHandoffError::checkpoint(format!(
                    "processor '{}' branch task is unavailable for checkpoint",
                    processor.as_str()
                ))
            })?;
        receiver.await.map_err(|_| {
            OwnershipHandoffError::checkpoint(format!(
                "processor '{}' branch task dropped its checkpoint response",
                processor.as_str()
            ))
        })??;
    }
    let placement = branch_lru_placement(runtime, domain, template);
    let payload = encode_branch_lru_snapshot(&instances.snapshot_entries())
        .map_err(OwnershipHandoffError::checkpoint)?;
    Ok(PersistedRuntimeStateEntry {
        lsm: instances.version(),
        schema_fingerprint: placement.schema_fingerprint,
        payload,
    })
}

pub(super) async fn handoff_all_processor_branch_instances(
    runtime: &Runtime,
    domain: &DomainName,
    processor: impl Into<ModelName>,
    branch: Option<&BranchName>,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) -> Vec<ProcessorBranchHandoff> {
    let processor = processor.into();
    let mut handoffs = Vec::new();
    for (key, entry) in instances.drain() {
        runtime.observe_branch_instance_removed(domain, branch, &key, None);
        let (response, receiver) = oneshot::channel();
        stop_processor_branch_task(
            domain,
            processor.clone(),
            &key,
            entry,
            ProcessorBranchStopMode::Handoff(response),
        )
        .await;
        if let Ok(handoff) = receiver.await {
            handoffs.push(handoff);
        }
    }
    handoffs
}

pub(super) async fn expire_processor_branch_instances(
    runtime: &Runtime,
    domain: &DomainName,
    processor: impl Into<ModelName>,
    branch: Option<&BranchName>,
    now: Timestamp,
    expiration_after: Duration,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) {
    let processor = processor.into();
    for (key, entry) in instances.expire(now, expiration_after) {
        runtime.observe_branch_instance_removed(
            domain,
            branch,
            &key,
            Some(BranchEvictionReason::Ttl),
        );
        runtime.invalidate_branch_relay_generation(domain, &key);
        stop_processor_branch_task(
            domain,
            processor.clone(),
            &key,
            entry,
            ProcessorBranchStopMode::Evict,
        )
        .await;
        debug!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            key = branch_key_display(&key),
            "expired processor branch task"
        );
    }
}

pub(super) async fn evict_processor_branch_instances_to_capacity(
    runtime: &Runtime,
    domain: &DomainName,
    processor: impl Into<ModelName>,
    branch: Option<&BranchName>,
    max_instances: NonZeroUsize,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) {
    let processor = processor.into();
    for (key, entry) in instances.evict_lru_to_capacity(max_instances) {
        runtime.observe_branch_instance_removed(
            domain,
            branch,
            &key,
            Some(BranchEvictionReason::Lru),
        );
        runtime.invalidate_branch_relay_generation(domain, &key);
        stop_processor_branch_task(
            domain,
            processor.clone(),
            &key,
            entry,
            ProcessorBranchStopMode::Evict,
        )
        .await;
        debug!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            key = branch_key_display(&key),
            max_instances,
            "evicted processor branch task by lru"
        );
    }
}

pub(super) async fn shutdown_all_processor_branch_instances(
    runtime: &Runtime,
    domain: &DomainName,
    processor: impl Into<ModelName>,
    branch: Option<&BranchName>,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) {
    let processor = processor.into();
    for (key, entry) in instances.drain() {
        runtime.observe_branch_instance_removed(domain, branch, &key, None);
        stop_processor_branch_task(
            domain,
            processor.clone(),
            &key,
            entry,
            ProcessorBranchStopMode::Detach,
        )
        .await;
        debug!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            key = branch_key_display(&key),
            "stopped processor branch task"
        );
    }
}

pub(super) fn restore_processor_branch_lru_snapshot(
    runtime: &Runtime,
    domain: &DomainName,
    graph: &SharedActiveGraph,
    template: &BranchInstanceTemplate,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) -> Result<u64, String> {
    let placement = branch_lru_placement(runtime, domain, template);
    let snapshot = runtime
        .take_restorable_branch_lru_snapshot(&placement)
        .map_err(|error| error.to_string())?;
    let Some(snapshot) = snapshot else {
        return Ok(0);
    };
    for (key, last_ingestion) in decode_branch_lru_snapshot(&snapshot.payload)? {
        let entry = spawn_processor_branch_task(
            ProcessorRuntimeContext::new(runtime.clone(), domain.clone(), graph.clone()),
            template,
            key.clone(),
            VecDeque::new(),
        )?;
        runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &key);
        instances.insert_restored(key, last_ingestion, entry);
    }
    instances.set_version(snapshot.lsm);
    Ok(snapshot.lsm)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc as StdArc,
        atomic::{AtomicBool, Ordering},
    };

    use ahash::{HashMap, HashSet};
    use arc_swap::ArcSwapOption;
    use nervix_models::{
        CreateSchema, ErrorPolicies, MessageErrorPolicy, ModelKind, ModelName, NodeRef,
        ParseAsType, RelayName, SchemaField,
    };
    use tokio::{
        sync::{mpsc, watch},
        time::{Duration, timeout},
    };
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime_ack::AckSet,
        runtime_schema::{RuntimeValue, compile_schema, test_runtime_row},
    };
    #[tokio::test]
    async fn processor_branch_tasks_are_created_and_reused_per_branch_key() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let graph: SharedActiveGraph = StdArc::new(ArcSwapOption::from(None));
        let schema = Arc::new(compile_schema(&CreateSchema {
            name: named("notification"),
            fields: vec![SchemaField {
                name: named("user_id"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }));
        let template = BranchInstanceTemplate {
            source_kind: ModelKind::Deduplicator,
            source: named("dedup_users"),
            root_relay: named("orders"),
            branch: None,
            branch_ttl: None,
            branch_max_instances: None,
            error_policies: ErrorPolicies::handled_by_log(),
            relays: [(
                named("projected_orders"),
                RelayProcessorRelayTemplate {
                    registry: RelayRegistry::new(),
                    services: test_relay_boundary_services(),
                },
            )]
            .into_iter()
            .collect(),
            processors: [(
                named("dedup_users"),
                RelayProcessorTemplate {
                    kind: ModelKind::Deduplicator,
                    processor: named("dedup_users"),
                    input_relays: vec![named("orders")],
                    input_collect_policies: HashMap::default(),
                    error_policies: ErrorPolicies::handled_by_log(),
                    from_where: HashMap::default(),
                    filter_where: None,
                    materialized_state: Vec::new(),
                    operation: RelayProcessorOperationTemplate::Deduplicator {
                        output_routes: RelayProcessorOutputsTemplate {
                            routes: vec![RelayProcessorOutputTemplate {
                                output_relay: named("projected_orders"),
                                construction: nervix_models::RouteConstruction {
                                    inherit: Some(nervix_models::Inheritance::All),
                                    ..nervix_models::RouteConstruction::default()
                                },
                                flush_policy: Some(RuntimeFlushPolicy::Immediate),
                                message_error_policy: MessageErrorPolicy::Log,
                            }],
                        },
                        deduplicate_on: vec![expression("input.user_id")],
                        max_time: Duration::from_secs(600),
                    },
                },
            )]
            .into_iter()
            .collect(),
        };
        let mut instances = BranchInstanceRegistry::<Option<BranchKey>, ProcessorBranchTask>::new();
        let now = current_timestamp();
        let dequeued_work = || {
            NodeQuiesceWorkGuard::begin(runtime.node_quiesce_counters(
                &domain,
                NodeRef::new(ModelKind::Deduplicator, named::<ModelName>("dedup_users")),
            ))
        };
        let branch_batch = |user_id: i64, tenant: &str| {
            RelayRecordBatch::from_messages(
                schema.clone(),
                vec![RelayMessage {
                    key: string_branch_key("tenant", tenant),
                    record: test_runtime_row([("user_id".to_string(), RuntimeValue::I64(user_id))]),
                    acks: AckSet::empty(),
                }],
            )
            .expect("branch batch should build")
        };

        dispatch_processor_node_input(
            ProcessorNodeDispatchContext {
                runtime_handle: &runtime,
                domain: &domain,
                graph: &graph,
                template: &template,
                now,
            },
            &mut instances,
            named("orders"),
            branch_batch(42, "acme"),
            dequeued_work(),
        )
        .await;
        let mut states = instances.states();
        assert_eq!(states.len(), 1);
        let first = states.pop().expect("first branch task must exist");

        dispatch_processor_node_input(
            ProcessorNodeDispatchContext {
                runtime_handle: &runtime,
                domain: &domain,
                graph: &graph,
                template: &template,
                now,
            },
            &mut instances,
            named("orders"),
            branch_batch(43, "acme"),
            dequeued_work(),
        )
        .await;
        let states = instances.states();
        assert_eq!(states.len(), 1);
        assert!(
            Arc::ptr_eq(&first, &states[0]),
            "same branch key must reuse the existing processor branch task"
        );

        dispatch_processor_node_input(
            ProcessorNodeDispatchContext {
                runtime_handle: &runtime,
                domain: &domain,
                graph: &graph,
                template: &template,
                now,
            },
            &mut instances,
            named("orders"),
            branch_batch(7, "beta"),
            dequeued_work(),
        )
        .await;
        assert_eq!(instances.states().len(), 2);

        shutdown_all_processor_branch_instances(
            &runtime,
            &domain,
            &named::<ModelName>("dedup_users"),
            None,
            &mut instances,
        )
        .await;
        assert!(instances.states().is_empty());
    }

    #[tokio::test]
    async fn processor_dispatch_hands_dequeued_work_into_branch_mailbox() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let processor = named::<ModelName>("route_orders");
        let input_relay = named::<RelayName>("orders");
        let template = junction_branch_template(processor.as_str(), input_relay.as_str());
        let counters =
            runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Junction, &processor));
        let (input_tx, mut input_rx) = mpsc::channel(1);
        let (commands, _command_rx) = mpsc::channel(1);
        let task = tokio::spawn(std::future::pending::<()>());
        let mut instances = BranchInstanceRegistry::<Option<BranchKey>, ProcessorBranchTask>::new();
        instances.insert_restored(
            None,
            current_timestamp(),
            ProcessorBranchTask {
                input: input_tx,
                commands,
                task: parking_lot::Mutex::new(Some(task)),
            },
        );

        dispatch_processor_node_input(
            ProcessorNodeDispatchContext {
                runtime_handle: &runtime,
                domain: &domain,
                graph: &StdArc::new(ArcSwapOption::from(None)),
                template: &template,
                now: current_timestamp(),
            },
            &mut instances,
            input_relay.clone(),
            quiesce_test_batch(),
            NodeQuiesceWorkGuard::begin(counters.clone()),
        )
        .await;

        assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 1);
        let queued = input_rx
            .recv()
            .await
            .expect("processor input should remain in the branch mailbox");
        assert_eq!(queued.relay, input_relay);
        assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 1);
        drop(queued);
        assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 0);

        let entry = instances
            .remove(&None)
            .expect("test branch task should remain registered");
        let task = entry
            .task
            .lock()
            .take()
            .expect("test branch task should still be running");
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn processor_handoff_drains_ready_batches_from_every_input() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let processor = named::<ModelName>("route_orders");
        let orders = named::<RelayName>("orders");
        let returns = named::<RelayName>("returns");
        let mut template = junction_branch_template(processor.as_str(), orders.as_str());
        template
            .processors
            .get_mut(&processor)
            .expect("junction processor should exist")
            .input_relays
            .push(returns.clone());
        let schema = test_schema(&[("value", ParseAsType::I64)]);
        let orders_broadcast = RelayBroadcast::with_capacity(nonzero_capacity(2));
        let returns_broadcast = RelayBroadcast::with_capacity(nonzero_capacity(2));
        let orders_input = RelayRuntimeFanIn::new(orders_broadcast.new_receiver());
        let returns_input = RelayRuntimeFanIn::new(returns_broadcast.new_receiver());
        let acme = string_branch_key("tenant", "acme");
        let beta = string_branch_key("tenant", "beta");
        let batch = |key, value| {
            RelayRecordBatch::single(
                schema.clone(),
                key,
                test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]),
                AckSet::empty(),
            )
            .expect("processor input batch should build")
        };
        orders_broadcast
            .broadcast(batch(acme.clone(), 1))
            .await
            .expect("orders batch should queue");
        returns_broadcast
            .broadcast(batch(beta.clone(), 2))
            .await
            .expect("returns batch should queue");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (commands, command_rx) = mpsc::channel(1);
        let (response, handoffs) = tokio::sync::oneshot::channel();
        commands
            .send(ProcessorNodeCommand::Handoff { response })
            .await
            .expect("handoff command should queue before the processor starts");
        let task = tokio::spawn(run_processor_node_runtime(
            ProcessorRuntimeContext::new(
                runtime.clone(),
                domain.clone(),
                StdArc::new(ArcSwapOption::from(None)),
            ),
            template,
            vec![(orders, orders_input), (returns, returns_input)],
            shutdown_rx,
            command_rx,
            Vec::new(),
            Duration::from_secs(60),
        ));

        let handoffs = timeout(Duration::from_secs(2), handoffs)
            .await
            .expect("processor handoff should finish")
            .expect("processor should return handoff state");
        timeout(Duration::from_secs(2), task)
            .await
            .expect("processor supervisor should stop")
            .expect("processor supervisor should join");
        assert_eq!(handoffs.len(), 2);
        assert!(handoffs.iter().any(|handoff| handoff.key == acme));
        assert!(handoffs.iter().any(|handoff| handoff.key == beta));
        assert_eq!(
            runtime
                .node_quiesce_counters(&domain, NodeRef::new(ModelKind::Junction, &processor))
                .outstanding_work(),
            0
        );
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn scheduled_processor_handoff_bounds_command_backpressure_and_aborts_the_task() {
        struct Dropped(Arc<AtomicBool>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let (commands, _command_rx) = mpsc::channel(1);
        let (first_response, _first_receiver) = tokio::sync::oneshot::channel();
        commands
            .send(ProcessorNodeCommand::Handoff {
                response: first_response,
            })
            .await
            .expect("first command should fill the processor mailbox");
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            let _dropped = Dropped(task_dropped);
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledNodeTask { commands, task };

        let error = scheduled
            .handoff_within(Duration::from_millis(10))
            .await
            .expect_err("a full command mailbox must bound handoff");

        assert_eq!(
            error,
            "scheduled node task timed out accepting handoff".to_string()
        );
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn scheduled_processor_handoff_aborts_a_task_that_drops_its_response() {
        struct Dropped(Arc<AtomicBool>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let (commands, mut command_rx) = mpsc::channel(1);
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            let _dropped = Dropped(task_dropped);
            let Some(ProcessorNodeCommand::Handoff { response }) = command_rx.recv().await else {
                panic!("scheduled processor must receive its handoff command")
            };
            drop(response);
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledNodeTask { commands, task };

        let error = scheduled
            .handoff()
            .await
            .expect_err("a dropped handoff response must fail");

        assert_eq!(
            error,
            "scheduled node task dropped its handoff response".to_string()
        );
        assert!(dropped.load(Ordering::Acquire));
    }
}
