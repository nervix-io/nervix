//! Branch-local processor task lifecycle and persisted handoff state.
//!
//! Layer: data plane.
//!
//! - **Owns.** Processor branch task creation, FIFO input execution, ticking, checkpointing,
//!   handoff, and eviction.
//! - **Depends on.** Planned processor templates, concrete branch runtimes, runtime persistence,
//!   and relay boundaries.
//! - **Must not know.** NSPL text, control-plane transactions, consensus, or connector protocols.

use error_stack::ResultExt as _;

use super::*;

/// Every way starting or restoring a processor's branch tasks fails.
#[derive(Debug, thiserror::Error)]
pub(super) enum ProcessorBranchTaskError {
    #[error("failed to instantiate processor branch '{}'", branch_key_display(.branch))]
    Instantiate { branch: Option<BranchKey> },
    #[error("failed to initialize a new window branch '{}'", branch_key_display(.branch))]
    InitializeWindow { branch: Option<BranchKey> },
    #[error("failed to read the persisted processor branch LRU snapshot")]
    ReadLruSnapshot,
    #[error("failed to decode the persisted processor branch LRU snapshot")]
    DecodeLruSnapshot,
}

pub(super) const PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

pub(super) const PROCESSOR_BRANCH_TASK_IDLE_SLEEP: Duration = Duration::from_secs(86_400);

#[derive(Debug)]
pub(super) enum ProcessorBranchStopMode {
    Evict,
    Detach,
    Handoff(oneshot::Sender<ProcessorBranchHandoff>),
}

/// Whether a branch resumes a known lifetime or appears after the lifecycle had no such key.
#[derive(Debug, Clone, Copy)]
pub(super) enum ProcessorBranchLifetime {
    Restored,
    Appeared,
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

struct ProcessorBranchRunIdentity {
    processor: ModelName,
    incarnation: u64,
}

/// A snapshot task asking the branch task to publish the live state it owns.
pub(super) type ProcessorSnapshotRequest =
    oneshot::Sender<error_stack::Result<(), ProcessorLiveStateError>>;

pub(super) struct ProcessorSnapshotTask {
    pub(super) shutdown_tx: watch::Sender<bool>,
    pub(super) task: Option<JoinHandle<()>>,
    pub(super) requests: Option<mpsc::Receiver<ProcessorSnapshotRequest>>,
}

/// What spawning a processor's snapshot task produced. Only deduplicators and window processors
/// answer snapshot requests, so a processor without them still reports the task it spawned.
pub(super) struct SpawnedSnapshotTask {
    pub(super) task: Option<JoinHandle<()>>,
    pub(super) requests: Option<mpsc::Receiver<ProcessorSnapshotRequest>>,
}

#[derive(Debug)]
pub(super) struct ProcessorBranchHandoff {
    pub(super) key: Option<BranchKey>,
    pub(super) restored_at: Timestamp,
    pub(super) incarnation: u64,
    pub(super) pending_materialized: VecDeque<PendingMaterializedBatch>,
}

pub(super) enum ProcessorNodeCommand {
    Checkpoint {
        response: oneshot::Sender<OwnershipHandoffResult<PersistedRuntimeStateEntry>>,
    },
    Handoff {
        response: oneshot::Sender<Vec<ProcessorBranchHandoff>>,
    },
    PrepareWasmStateReset {
        preparation: WasmStateResetPreparation,
        response: oneshot::Sender<error_stack::Result<(), WasmStateResetRuntimeError>>,
    },
    AbortWasmStateReset {
        request: CommandExecutionReference,
        response: oneshot::Sender<error_stack::Result<(), WasmStateResetRuntimeError>>,
    },
    ApplyWasmStateReset {
        reset: nervix_models::WasmStateReset,
        response: oneshot::Sender<error_stack::Result<(), WasmStateResetRuntimeError>>,
    },
}

impl RelayInteractionCommand for ProcessorNodeCommand {
    fn drain_inputs_before_handling(&self) -> bool {
        matches!(
            self,
            Self::Checkpoint { .. } | Self::Handoff { .. } | Self::PrepareWasmStateReset { .. }
        )
    }

    fn cancels_external_waits_while_draining(&self) -> bool {
        matches!(
            self,
            Self::Checkpoint { .. } | Self::Handoff { .. } | Self::PrepareWasmStateReset { .. }
        )
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
        )
        .await
        {
            Ok(lsm) => lsm,
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    processor = processor.as_str(),
                    error = %format_args!("{error:#}"),
                    "failed to restore processor branch lru snapshot"
                );
                0
            }
        };
    } else {
        let placement = match branch_lru_placement(&runtime_handle, &domain, &template) {
            Ok(placement) => placement,
            Err(error) => {
                warn!(error = %format_args!("{error:#}"), "failed to place handed-off branch lifecycle");
                return;
            }
        };
        let transferred_lru = match runtime_handle.take_restorable_branch_lru_snapshot(&placement) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                warn!(
                    processor = processor.as_str(),
                    "handed-off branch lifecycle is unavailable"
                );
                return;
            }
            Err(error) => {
                warn!(error = %format_args!("{error:#}"), "failed to read handed-off branch lifecycle");
                return;
            }
        };
        if restored_handoffs
            .iter()
            .any(|handoff| handoff.incarnation > transferred_lru.lsm)
        {
            warn!(
                processor = processor.as_str(),
                "handed-off branch lifetime exceeds its lifecycle revision"
            );
            return;
        }
        instances.set_version(transferred_lru.lsm);
        for handoff in restored_handoffs {
            tokio::task::consume_budget().await;
            let key = handoff.key.clone();
            match spawn_processor_branch_task(
                ProcessorRuntimeContext::new(runtime_handle.clone(), domain.clone(), graph.clone()),
                &template,
                key.clone(),
                handoff.pending_materialized,
                ProcessorBranchLifetime::Restored,
                handoff.incarnation,
            )
            .await
            {
                Ok(entry) => {
                    runtime_handle.observe_branch_instance_created(
                        &domain,
                        template.branch.as_ref(),
                        &key,
                    );
                    instances.insert_restored(key, handoff.restored_at, handoff.incarnation, entry);
                }
                Err(error) => {
                    warn!(
                        domain = domain.as_str(),
                        processor = processor.as_str(),
                        error = %format_args!("{error:#}"),
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
            &template,
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
        .map(|(relay, receiver)| RelayInteractionInput::immediate(relay, receiver))
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
    let mut reset_fence = template
        .wasm_state_reset
        .clone()
        .filter(|reset| reset.phase() == nervix_models::WasmStateResetPhase::Publishing);
    let mut prepared_reset = None::<PreparedWasmStateReset>;

    let mut handoff_response = None;
    loop {
        tokio::task::consume_budget().await;
        let ownership_frozen = runtime_handle.ownership_handoff_entity_is_frozen(&ownership_entity);
        let snapshot = match domain_clock.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                runtime_handle.events().report_error(format!(
                    "processor '{}' in domain '{}' lost its clock: {error}",
                    processor.as_str(),
                    domain.as_str(),
                ));
                break;
            }
        };
        let now = snapshot.now();
        let mut did_scheduled_work = false;
        if !ownership_frozen && Instant::now() >= next_expiration_scan {
            if let Some(branch_ttl) = template.branch_ttl {
                expire_processor_branch_instances(
                    &runtime_handle,
                    &domain,
                    &template,
                    now,
                    branch_ttl,
                    &mut instances,
                )
                .await;
            }
            next_expiration_scan = Instant::now() + expiration_scan_interval;
            did_scheduled_work = true;
        }
        if Instant::now() >= next_lru_snapshot && prepared_reset.is_none() {
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
                    error = %format_args!("{error:#}"),
                    "failed to persist processor branch lru snapshot"
                );
            }
            next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();
            did_scheduled_work = true;
        }
        if did_scheduled_work {
            continue;
        }

        let maintenance_timeout = if ownership_frozen {
            OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL
        } else {
            let next_maintenance = next_expiration_scan.min(next_lru_snapshot);
            next_maintenance
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO)
        };
        let wake = match RuntimeWake::after(maintenance_timeout) {
            Ok(wake) => wake,
            Err(error) => {
                runtime_handle.events().report_error(format!(
                    "processor '{}' in domain '{}' could not schedule its next maintenance scan: \
                     {error}",
                    processor.as_str(),
                    domain.as_str(),
                ));
                break;
            }
        };
        let work = match interaction.next(wake).await {
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
                let branch = batch.key.as_ref().map(BranchKey::fingerprint);
                if reset_fence
                    .as_ref()
                    .is_some_and(|reset| reset.scope().contains(branch.as_ref()))
                {
                    for ack in batch.acks.iter() {
                        ack.no_ack("WASM guest-state reset is not ready for this branch");
                    }
                    drop(work);
                    continue;
                }
                dispatch_processor_node_input(
                    ProcessorNodeDispatchContext {
                        runtime_handle: &runtime_handle,
                        domain: &domain,
                        graph: &graph,
                        template: &template,
                        domain_clock: &domain_clock,
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
                    let result =
                        checkpoint_all_processor_branch_instances(processor.clone(), &instances)
                            .await;
                    response
                        .send(result)
                        .means_peer_left("processor lifecycle checkpoint requester");
                }
                ProcessorNodeCommand::Handoff { response } => {
                    handoff_response = Some(response);
                    break;
                }
                ProcessorNodeCommand::PrepareWasmStateReset {
                    preparation,
                    response,
                } => {
                    let reset_context = ProcessorWasmStateResetContext::new(
                        &runtime_handle,
                        &domain,
                        &graph,
                        &template,
                    );
                    let request = preparation.request.clone();
                    let scope = preparation.scope;
                    let reason = preparation.reason;
                    let result = reset_context
                        .prepare(&mut instances, preparation, &mut prepared_reset)
                        .await;
                    if result.is_ok() {
                        reset_fence = Some(nervix_models::WasmStateReset::publishing(
                            request, scope, reason,
                        ));
                    }
                    response
                        .send(result)
                        .means_peer_left("WASM state reset preparation requester");
                }
                ProcessorNodeCommand::AbortWasmStateReset { request, response } => {
                    let result = abort_processor_wasm_state_reset(
                        &runtime_handle,
                        &domain,
                        &graph,
                        &template,
                        &mut instances,
                        &request,
                        &mut prepared_reset,
                    )
                    .await;
                    if result.is_ok() {
                        reset_fence = template.wasm_state_reset.clone().filter(|reset| {
                            reset.phase() == nervix_models::WasmStateResetPhase::Publishing
                        });
                    }
                    response
                        .send(result)
                        .means_peer_left("WASM state reset abort requester");
                }
                ProcessorNodeCommand::ApplyWasmStateReset { reset, response } => {
                    let result = match reset.phase() {
                        nervix_models::WasmStateResetPhase::Publishing => {
                            reset_fence = Some(reset.clone());
                            ProcessorWasmStateResetContext::new(
                                &runtime_handle,
                                &domain,
                                &graph,
                                &template,
                            )
                            .commit(
                                &mut instances,
                                &mut last_persisted_lru_lsm,
                                &reset,
                                &mut prepared_reset,
                            )
                            .await
                        }
                        nervix_models::WasmStateResetPhase::Ready => {
                            complete_processor_wasm_state_reset(
                                &ModelName::from(&processor),
                                &reset,
                                &mut reset_fence,
                                &mut prepared_reset,
                            )
                        }
                    };
                    response
                        .send(result)
                        .means_peer_left("WASM state reset schedule application requester");
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
            error = %format_args!("{error:#}"),
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

struct ProcessorWasmStateResetContext<'a> {
    runtime: &'a Runtime,
    domain: &'a DomainName,
    graph: &'a SharedActiveGraph,
    template: &'a BranchInstanceTemplate,
}

impl<'a> ProcessorWasmStateResetContext<'a> {
    fn new(
        runtime: &'a Runtime,
        domain: &'a DomainName,
        graph: &'a SharedActiveGraph,
        template: &'a BranchInstanceTemplate,
    ) -> Self {
        Self {
            runtime,
            domain,
            graph,
            template,
        }
    }
}

fn processor_reset_target_keys(
    processor: &ModelName,
    template: &BranchInstanceTemplate,
    instances: &BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
    scope: WasmStateResetScope,
    branch_key: Option<BranchKey>,
) -> error_stack::Result<Vec<Option<BranchKey>>, WasmStateResetRuntimeError> {
    if template.source_kind != ModelKind::WasmProcessor {
        return Err(Report::new(WasmStateResetRuntimeError::NotWasmProcessor {
            processor: processor.clone(),
        }));
    }
    match (template.branch.as_ref(), scope, branch_key) {
        (None, WasmStateResetScope::Unbranched, None) => Ok(vec![None]),
        (Some(_), WasmStateResetScope::Branch(selected), Some(branch))
            if branch.fingerprint() == selected
                && instances.contains_key(&Some(branch.clone())) =>
        {
            Ok(vec![Some(branch)])
        }
        (Some(_), WasmStateResetScope::Branch(selected), Some(branch))
            if branch.fingerprint() == selected =>
        {
            Err(Report::new(WasmStateResetRuntimeError::BranchUnavailable {
                processor: processor.clone(),
            }))
        }
        (Some(_), WasmStateResetScope::Branch(selected), None) => Ok(instances
            .snapshot_entries()
            .into_iter()
            .map(|entry| entry.key)
            .filter(|key| {
                key.as_ref()
                    .is_some_and(|branch| branch.fingerprint() == selected)
            })
            .collect()),
        (Some(_), WasmStateResetScope::AllBranches, None) => Ok(instances
            .snapshot_entries()
            .into_iter()
            .map(|entry| entry.key)
            .collect()),
        _ => Err(Report::new(WasmStateResetRuntimeError::InvalidScope {
            processor: processor.clone(),
        })),
    }
}

async fn restore_processor_wasm_state_reset_branches(
    runtime: &Runtime,
    domain: &DomainName,
    graph: &SharedActiveGraph,
    template: &BranchInstanceTemplate,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
    branches: &mut Vec<PreparedWasmStateResetBranch>,
) -> error_stack::Result<(), WasmStateResetRuntimeError> {
    let processor = ModelName::from(&template.source);
    for mut branch in branches.drain(..) {
        tokio::task::consume_budget().await;
        let Some(handoff) = branch.previous.take() else {
            continue;
        };
        let restored_at = handoff.restored_at;
        let key = branch.key.clone();
        let task = spawn_processor_branch_task(
            ProcessorRuntimeContext::new(runtime.clone(), domain.clone(), graph.clone()),
            template,
            key.clone(),
            handoff.pending_materialized,
            ProcessorBranchLifetime::Restored,
            handoff.incarnation,
        )
        .await
        .change_context_lazy(|| WasmStateResetRuntimeError::RestoreAfterAbort {
            processor: processor.clone(),
        })?;
        runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &key);
        instances.insert_changed(key, restored_at, task);
    }
    Ok(())
}

impl ProcessorWasmStateResetContext<'_> {
    async fn prepare(
        &self,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
        preparation: WasmStateResetPreparation,
        prepared: &mut Option<PreparedWasmStateReset>,
    ) -> error_stack::Result<(), WasmStateResetRuntimeError> {
        let runtime = self.runtime;
        let domain = self.domain;
        let graph = self.graph;
        let template = self.template;
        let WasmStateResetPreparation {
            request,
            scope,
            branch_key,
            published,
            reason: _,
        } = preparation;
        let processor = ModelName::from(&template.source);
        if let Some(current) = prepared.as_mut() {
            if !current.matches(&request, scope) {
                return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
                    processor,
                }));
            }
            current.published |= published;
            return Ok(());
        }

        let targets =
            processor_reset_target_keys(&processor, template, instances, scope, branch_key)?;
        let mut branches = Vec::with_capacity(targets.len());
        for key in targets {
            tokio::task::consume_budget().await;
            let previous = if let Some(entry) = instances.remove(&key) {
                runtime.observe_branch_instance_removed(
                    domain,
                    template.branch.as_ref(),
                    &key,
                    None,
                );
                let (response, receiver) = oneshot::channel();
                stop_processor_branch_task(
                    domain,
                    processor.clone(),
                    &key,
                    entry,
                    ProcessorBranchStopMode::Handoff(response),
                )
                .await;
                match receiver.await {
                    Ok(handoff) => Some(handoff),
                    Err(error) => {
                        let restore = restore_processor_wasm_state_reset_branches(
                            runtime,
                            domain,
                            graph,
                            template,
                            instances,
                            &mut branches,
                        )
                        .await;
                        restore?;
                        return Err(Report::new(WasmStateResetRuntimeError::StopBranch {
                            processor,
                        })
                        .attach_printable(error));
                    }
                }
            } else {
                None
            };
            let initial_state = template
                .prepare_fresh_wasm_state(runtime, domain, key.clone())
                .await;
            match initial_state {
                Ok(initial_state) => branches.push(PreparedWasmStateResetBranch {
                    key,
                    initial_state: Some(initial_state),
                    previous,
                    activated: false,
                }),
                Err(error) => {
                    branches.push(PreparedWasmStateResetBranch {
                        key,
                        initial_state: None,
                        previous,
                        activated: false,
                    });
                    restore_processor_wasm_state_reset_branches(
                        runtime,
                        domain,
                        graph,
                        template,
                        instances,
                        &mut branches,
                    )
                    .await?;
                    return Err(error);
                }
            }
        }
        *prepared = Some(PreparedWasmStateReset {
            request,
            scope,
            published,
            branches,
        });
        Ok(())
    }
}

async fn abort_processor_wasm_state_reset(
    runtime: &Runtime,
    domain: &DomainName,
    graph: &SharedActiveGraph,
    template: &BranchInstanceTemplate,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
    request: &CommandExecutionReference,
    prepared: &mut Option<PreparedWasmStateReset>,
) -> error_stack::Result<(), WasmStateResetRuntimeError> {
    let processor = ModelName::from(&template.source);
    let Some(current) = prepared.as_ref() else {
        return Ok(());
    };
    if &current.request != request || current.published {
        return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
            processor,
        }));
    }
    let mut current = prepared
        .take()
        .verified("the reset preparation was observed immediately before this take");
    restore_processor_wasm_state_reset_branches(
        runtime,
        domain,
        graph,
        template,
        instances,
        &mut current.branches,
    )
    .await
}

impl ProcessorWasmStateResetContext<'_> {
    async fn commit(
        &self,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
        last_persisted_lru_lsm: &mut u64,
        reset: &nervix_models::WasmStateReset,
        prepared: &mut Option<PreparedWasmStateReset>,
    ) -> error_stack::Result<(), WasmStateResetRuntimeError> {
        let runtime = self.runtime;
        let domain = self.domain;
        let graph = self.graph;
        let template = self.template;
        let processor = ModelName::from(&template.source);
        if prepared.is_none() {
            self.prepare(
                instances,
                WasmStateResetPreparation {
                    request: reset.request().clone(),
                    scope: *reset.scope(),
                    branch_key: None,
                    published: true,
                    reason: reset.reason(),
                },
                prepared,
            )
            .await?;
        }
        let current = prepared
            .as_mut()
            .verified("the preparation above either returned an error or installed reset state");
        if !current.matches(reset.request(), *reset.scope()) {
            return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
                processor,
            }));
        }
        current.published = true;

        let execution_now = runtime
            .bind_domain_clock(domain)
            .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
                processor: processor.clone(),
            })?
            .snapshot()
            .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
                processor: processor.clone(),
            })?
            .now();
        for branch in &mut current.branches {
            tokio::task::consume_budget().await;
            if branch.activated {
                continue;
            }
            let key = branch.key.clone();
            let task = spawn_processor_branch_task(
                ProcessorRuntimeContext::new(runtime.clone(), domain.clone(), graph.clone()),
                template,
                key.clone(),
                VecDeque::new(),
                ProcessorBranchLifetime::Appeared,
                instances.next_incarnation(),
            )
            .await
            .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
                processor: processor.clone(),
            })?;
            runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &key);
            instances.insert_changed(key, execution_now, task);
            branch.activated = true;
        }
        publish_branch_instance_lru_snapshot(runtime, domain, template, instances)
            .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
                processor: processor.clone(),
            })?;
        persist_branch_instance_lru_snapshot(
            runtime,
            domain,
            template,
            instances,
            last_persisted_lru_lsm,
        )
        .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
            processor: processor.clone(),
        })?;
        let branch_lru =
            branch_lru_placement(runtime, domain, template).change_context_lazy(|| {
                WasmStateResetRuntimeError::InitialCheckpoint {
                    processor: processor.clone(),
                }
            })?;
        let branch_lru_deadline = Instant::now()
            .checked_add(WASM_CHECKPOINT_DEADLINE)
            .assured("the configured WASM checkpoint deadline stays within Instant");
        runtime
            .confirm_branch_lru_checkpoint(&branch_lru, instances.version(), branch_lru_deadline)
            .await
            .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
                processor: processor.clone(),
            })?;

        for branch in &mut current.branches {
            tokio::task::consume_budget().await;
            let Some(initial_state) = branch.initial_state.as_ref() else {
                continue;
            };
            runtime
                .commit_wasm_state_reset_checkpoint(
                    domain,
                    &processor,
                    branch.key.clone(),
                    initial_state.clone(),
                )
                .await?;
            branch.initial_state = None;
        }
        for branch in &mut current.branches {
            branch.previous.take();
        }
        Ok(())
    }
}

fn complete_processor_wasm_state_reset(
    processor: &ModelName,
    reset: &nervix_models::WasmStateReset,
    fence: &mut Option<nervix_models::WasmStateReset>,
    prepared: &mut Option<PreparedWasmStateReset>,
) -> error_stack::Result<(), WasmStateResetRuntimeError> {
    if let Some(current) = fence.as_ref()
        && (current.request() != reset.request() || current.scope() != reset.scope())
    {
        return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
            processor: processor.clone(),
        }));
    }
    if let Some(current) = prepared.as_ref()
        && !current.matches(reset.request(), *reset.scope())
    {
        return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
            processor: processor.clone(),
        }));
    }
    *prepared = None;
    *fence = None;
    Ok(())
}

pub(super) struct ProcessorNodeDispatchContext<'a> {
    pub(super) runtime_handle: &'a Runtime,
    pub(super) domain: &'a DomainName,
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) template: &'a BranchInstanceTemplate,
    pub(super) domain_clock: &'a DomainClock,
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
        domain_clock,
    } = context;
    let key = batch.key.clone();
    // Branch activity is the domain time at which this input was accepted. The supervisor reads it
    // here, after the wait that delivered the batch, so an idle supervisor never records the
    // activity of a live branch at the instant it started waiting.
    let accepted_at = match domain_clock.snapshot() {
        Ok(snapshot) => snapshot.now(),
        Err(error) => {
            runtime_handle.handle_internal_processor_error_for_acks(
                domain,
                template.source_kind,
                &template.source,
                &template.error_policies,
                batch.acks.iter(),
                format!(
                    "processor '{}' could not read the domain time of accepted input for branch \
                     '{}': {error}",
                    template.source.as_str(),
                    branch_key_display(&key),
                ),
            );
            return;
        }
    };
    let instance = if let Some(state) = instances.touch(&key, accepted_at) {
        GetOrCreateBranchInstance {
            state,
            created: false,
        }
    } else {
        let incarnation = instances.next_incarnation();
        let state = match spawn_processor_branch_task(
            ProcessorRuntimeContext::new(runtime_handle.clone(), domain.clone(), graph.clone()),
            template,
            key.clone(),
            VecDeque::new(),
            ProcessorBranchLifetime::Appeared,
            incarnation,
        )
        .await
        {
            Ok(state) => state,
            Err(error) => {
                runtime_handle.handle_internal_processor_error_for_acks(
                    domain,
                    template.source_kind,
                    &template.source,
                    &template.error_policies,
                    batch.acks.iter(),
                    format!("{error:#}"),
                );
                return;
            }
        };
        let state = instances.insert_changed(key.clone(), accepted_at, state);
        GetOrCreateBranchInstance {
            state,
            created: true,
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
                template,
                max_instances,
                instances,
            )
            .await;
        }
        // A replica installs a branch's checkpoint only once it knows the branch, and a WASM
        // branch acknowledges nothing before its replicas hold its checkpoint. Offering the new
        // branch to them now, rather than on the next lifecycle snapshot, keeps the branch's first
        // checkpoint from waiting on that snapshot.
        if template.source_kind == ModelKind::WasmProcessor
            && let Err(error) =
                publish_branch_instance_lru_snapshot(runtime_handle, domain, template, instances)
        {
            warn!(
                domain = domain.as_str(),
                processor = template.source.as_str(),
                error = %format_args!("{error:#}"),
                "failed to offer a new processor branch to its replicas"
            );
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

pub(super) async fn spawn_processor_branch_task(
    context: ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    key: Option<BranchKey>,
    pending_materialized: VecDeque<PendingMaterializedBatch>,
    lifetime: ProcessorBranchLifetime,
    incarnation: u64,
) -> error_stack::Result<ProcessorBranchTask, ProcessorBranchTaskError> {
    let branch_key = key.clone();
    let mut branch = template
        .instantiate(&context.runtime_handle, &context.domain, key, incarnation)
        .await
        .change_context(ProcessorBranchTaskError::Instantiate {
            branch: branch_key.clone(),
        })?
        .into_inner();
    if template.source_kind == ModelKind::WindowProcessor
        && let ProcessorBranchLifetime::Appeared = lifetime
    {
        let processor = ModelName::from(&template.source);
        if let Some(node) = branch.processors.get_mut(&processor) {
            node.reset_window_state();
        }
        branch
            .snapshot_processor_live_state(&processor)
            .change_context_lazy(|| ProcessorBranchTaskError::InitializeWindow {
                branch: branch_key,
            })?;
    }
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
        ProcessorBranchRunIdentity {
            processor: ModelName::from(&processor),
            incarnation,
        },
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
    // The branch publishes its live state as it stops, whether or not this node persists it: the
    // next task for this branch restores from what was published, and the snapshot task's final
    // flush persists it. A failed snapshot routes the entries it could not store through the
    // processor's error policy and clears them, so the second attempt is what publishes that
    // cleared state: without it a replacement would restore entries this branch has already
    // failed. The first failure is reported by the error policy; the second one leaves the earlier
    // publication as the last state anyone can restore, and this is the only place that fact exists.
    if branch.snapshot_processor_live_state(processor).is_err()
        && let Err(error) = branch.snapshot_processor_live_state(processor)
    {
        warn!(
            processor = processor.as_str(),
            error = %format_args!("{error:#}"),
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

async fn run_processor_branch_task(
    context: ProcessorRuntimeContext,
    identity: ProcessorBranchRunIdentity,
    mut branch: BranchRuntime,
    mut input: mpsc::Receiver<ProcessorBranchInput>,
    mut command_rx: mpsc::Receiver<ProcessorBranchCommand>,
    quiesce_counters: Arc<NodeQuiesceCounters>,
    mut snapshot: ProcessorSnapshotTask,
) {
    let ProcessorBranchRunIdentity {
        processor,
        incarnation,
    } = identity;
    let ProcessorRuntimeContext {
        runtime_handle,
        domain,
        graph,
    } = context;
    let mut force_flush = runtime_handle.force_flush_participant(&domain, quiesce_counters.clone());
    let mut quiesce_gauges = BranchQuiesceGauges::new(quiesce_counters.clone());
    let ownership_freeze = OwnershipHandoffFreezeWatch::new(
        &runtime_handle,
        DomainNodeRef::node_in(domain.clone(), branch.source_kind, processor.clone()),
    );
    let domain_clock = branch.domain_clock.clone();
    quiesce_gauges.observe(&branch, &processor);
    let stop_mode;
    let mut handoff_execution_snapshot = None;
    loop {
        tokio::task::consume_budget().await;
        let freeze = ownership_freeze.observe();
        let ownership_frozen = freeze.is_frozen();
        let execution_snapshot = match domain_clock.snapshot() {
            Ok(snapshot) => snapshot,
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
        let now = execution_snapshot.now();
        let buffer_deadline_due = match branch.buffer_deadline_due(&execution_snapshot) {
            Ok(due) => due,
            Err(error) => {
                runtime_handle.events().report_error(format!(
                    "processor branch '{}' in domain '{}' could not inspect a buffer deadline: \
                     {error}",
                    processor.as_str(),
                    domain.as_str(),
                ));
                stop_mode = Some(ProcessorBranchStopMode::Detach);
                break;
            }
        };
        if !ownership_frozen
            && (buffer_deadline_due
                || branch
                    .next_deadline()
                    .is_some_and(|deadline| deadline <= now))
        {
            branch.tick(&graph, &execution_snapshot).await;
            quiesce_gauges.observe(&branch, &processor);
            continue;
        }
        // Freeze rechecks are physical, so they stay a plain monotonic sleep. The branch's own
        // deadline is logical and is awaited on the domain clock below.
        let idle_sleep = if ownership_frozen {
            OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL
        } else {
            PROCESSOR_BRANCH_TASK_IDLE_SLEEP
        };
        let awaited_branch_deadline = if ownership_frozen {
            None
        } else {
            branch
                .next_deadline()
                .map(|deadline| domain_clock.deadline_at(deadline))
        };
        let buffer_deadlines = if ownership_frozen {
            Vec::new()
        } else {
            branch.buffer_deadlines()
        };
        let has_buffer_deadlines = !buffer_deadlines.is_empty();
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
                            let execution_snapshot = match domain_clock.snapshot() {
                                Ok(snapshot) => snapshot,
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
                            handoff_execution_snapshot = Some(execution_snapshot);
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
                let flush_snapshot = match domain_clock.snapshot() {
                    Ok(snapshot) => snapshot,
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
                // A generation releases everything the branch can release now, including parked
                // messages whose materialized dependency arrived after they were parked.
                if branch.processor_has_pending_materialized(&processor) {
                    branch
                        .retry_processor_pending_materialized(&graph, &processor)
                        .await;
                }
                branch.force_flush(&graph, &flush_snapshot).await;
                quiesce_gauges.observe(&branch, &processor);
                completion.complete();
            }
            _ = freeze.changed(), if ownership_frozen => {}
            result = wait_for_branch_buffer_deadlines(&domain_clock, buffer_deadlines),
                if has_buffer_deadlines =>
            {
                if let Err(error) = result {
                    runtime_handle.events().report_error(format!(
                        "processor branch '{}' in domain '{}' could not wait for a buffer \
                         deadline: {error}",
                        processor.as_str(),
                        domain.as_str(),
                    ));
                    stop_mode = Some(ProcessorBranchStopMode::Detach);
                    break;
                }
            }
            result = wait_for_branch_deadline(&domain_clock, awaited_branch_deadline.clone()),
                if awaited_branch_deadline.is_some() =>
            {
                if let Err(error) = result {
                    runtime_handle.events().report_error(format!(
                        "processor branch '{}' in domain '{}' could not wait for a branch \
                         deadline: {error}",
                        processor.as_str(),
                        domain.as_str(),
                    ));
                    stop_mode = Some(ProcessorBranchStopMode::Detach);
                    break;
                }
            }
            _ = sleep(idle_sleep) => {}
        }
    }
    while let Ok(ProcessorBranchInput { relay, batch, work }) = input.try_recv() {
        branch
            .execute_processor_input(&graph, &processor, &relay, batch)
            .await;
        quiesce_gauges.observe(&branch, &processor);
        drop(work);
    }
    // A detached and a handed-off branch both finish with nothing accepted left inside them, so both
    // publish through the one finalizing flush below: collected input, guest buffers and route
    // output. Only eviction drops branch-local work, which is its contract.
    let finalization_snapshot = match &stop_mode {
        Some(ProcessorBranchStopMode::Handoff(_)) => Some(handoff_execution_snapshot.verified(
            "the handoff command arm captures the validated domain time for this stop mode",
        )),
        Some(ProcessorBranchStopMode::Detach) | None => match domain_clock.snapshot() {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                runtime_handle.events().report_error(format!(
                    "processor branch '{}' in domain '{}' stopped without its clock and cannot \
                     publish its accepted output: {error}",
                    processor.as_str(),
                    domain.as_str(),
                ));
                // Without a clock the processor produces no output. Running its collected input
                // still reports each batch through the processor's error policy.
                branch
                    .flush_processor_collected_inputs(&graph, &processor)
                    .await;
                None
            }
        },
        Some(ProcessorBranchStopMode::Evict) => None,
    };
    if let Some(snapshot) = &finalization_snapshot {
        branch.force_flush(&graph, snapshot).await;
    }
    if let Some(ProcessorBranchStopMode::Evict) = &stop_mode {
        branch.evict().await;
    }
    stop_processor_snapshot_task(&mut branch, &processor, &mut snapshot).await;
    match stop_mode {
        Some(ProcessorBranchStopMode::Evict) => {}
        Some(ProcessorBranchStopMode::Handoff(response)) => {
            let restored_at = finalization_snapshot
                .verified("a handoff stop always finalizes with the snapshot its command captured")
                .now();
            let pending_materialized = match branch.processors.get_mut(&processor) {
                Some(processor) => std::mem::take(&mut processor.pending_materialized),
                None => VecDeque::new(),
            };
            let handoff = ProcessorBranchHandoff {
                key: branch.key.clone(),
                restored_at,
                incarnation,
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
    processor: impl Into<ModelName>,
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
    let payload = encode_branch_lru_snapshot(&instances.snapshot_entries())
        .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?;
    Ok(PersistedRuntimeStateEntry {
        lsm: instances.version(),
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
    template: &BranchInstanceTemplate,
    now: Timestamp,
    expiration_after: Duration,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) {
    let processor = ModelName::from(&template.source);
    for (key, entry) in instances.expire(now, expiration_after) {
        runtime.observe_branch_instance_removed(
            domain,
            template.branch.as_ref(),
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
        if template.source_kind == ModelKind::WindowProcessor
            && let Err(error) = runtime.release_evicted_window_state(domain, &processor, &key)
        {
            warn!(error = %format_args!("{error:#}"), "failed to release evicted window state");
        }
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
    template: &BranchInstanceTemplate,
    max_instances: NonZeroUsize,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) {
    let processor = ModelName::from(&template.source);
    for (key, entry) in instances.evict_lru_to_capacity(max_instances) {
        runtime.observe_branch_instance_removed(
            domain,
            template.branch.as_ref(),
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
        if template.source_kind == ModelKind::WindowProcessor
            && let Err(error) = runtime.release_evicted_window_state(domain, &processor, &key)
        {
            warn!(error = %format_args!("{error:#}"), "failed to release evicted window state");
        }
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

pub(super) async fn restore_processor_branch_lru_snapshot(
    runtime: &Runtime,
    domain: &DomainName,
    graph: &SharedActiveGraph,
    template: &BranchInstanceTemplate,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) -> error_stack::Result<u64, ProcessorBranchTaskError> {
    let placement = branch_lru_placement(runtime, domain, template)
        .change_context(ProcessorBranchTaskError::ReadLruSnapshot)?;
    let snapshot = runtime
        .take_restorable_branch_lru_snapshot(&placement)
        .change_context(ProcessorBranchTaskError::ReadLruSnapshot)?;
    let Some(snapshot) = snapshot else {
        return Ok(0);
    };
    let restored = decode_branch_lru_snapshot(&snapshot.payload)
        .change_context(ProcessorBranchTaskError::DecodeLruSnapshot)?;
    for restored_entry in restored {
        tokio::task::consume_budget().await;
        let key = restored_entry.key;
        let last_ingestion = restored_entry.last_ingestion;
        let incarnation = restored_entry.incarnation;
        let entry = spawn_processor_branch_task(
            ProcessorRuntimeContext::new(runtime.clone(), domain.clone(), graph.clone()),
            template,
            key.clone(),
            VecDeque::new(),
            ProcessorBranchLifetime::Restored,
            incarnation,
        )
        .await?;
        runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &key);
        instances.insert_restored(key, last_ingestion, incarnation, entry);
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

    use ahash::HashMap;
    use nervix_execution::sync::ArcSwapOption;
    use nervix_models::{
        CommandExecutionReference, CreateSchema, ErrorPolicies, MessageErrorPolicy, ModelKind,
        ModelName, NodeRef, ParseAsType, RelayName, SchemaField,
    };
    use tokio::{
        sync::{mpsc, watch},
        time::{Duration, timeout},
    };
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime::scheduled_node::ScheduledNodeHandoffError,
        runtime_ack::AckSet,
        runtime_schema::{RuntimeValue, compile_schema, test_runtime_row},
    };

    fn reset_target_template(source_kind: ModelKind, branched: bool) -> BranchInstanceTemplate {
        BranchInstanceTemplate {
            source_kind,
            source: named("counting_guest"),
            root_relay: named("counted_input_events"),
            branch: branched.then(|| named("by_tenant")),
            branch_ttl: None,
            branch_max_instances: None,
            error_policies: ErrorPolicies::handled_by_log(),
            relays: HashMap::default(),
            processors: HashMap::default(),
            wasm_state_reset: None,
        }
    }

    #[test]
    fn reset_target_keys_enforce_processor_kind_branching_and_fingerprint() {
        let processor = named::<ModelName>("counting_guest");
        let instances = BranchInstanceRegistry::new();
        let alpha =
            string_branch_key("tenant", "alpha").expect("the fixture branch key must be present");
        let alpha_scope = WasmStateResetScope::Branch(alpha.fingerprint());

        let not_wasm = reset_target_template(ModelKind::Deduplicator, false);
        let error = processor_reset_target_keys(
            &processor,
            &not_wasm,
            &instances,
            WasmStateResetScope::Unbranched,
            None,
        )
        .expect_err("a non-WASM processor cannot accept a WASM state reset");
        assert!(matches!(
            error.current_context(),
            WasmStateResetRuntimeError::NotWasmProcessor { .. }
        ));

        let unbranched = reset_target_template(ModelKind::WasmProcessor, false);
        let targets = processor_reset_target_keys(
            &processor,
            &unbranched,
            &instances,
            WasmStateResetScope::Unbranched,
            None,
        )
        .expect("an unbranched reset must select the singleton instance");
        assert_eq!(targets, vec![None]);

        let branched = reset_target_template(ModelKind::WasmProcessor, true);
        let targets =
            processor_reset_target_keys(&processor, &branched, &instances, alpha_scope, None)
                .expect(
                    "a published branch scope may recover without its original branch key fields",
                );
        assert!(targets.is_empty());

        let beta =
            string_branch_key("tenant", "beta").expect("the fixture branch key must be present");
        let error =
            processor_reset_target_keys(&processor, &branched, &instances, alpha_scope, Some(beta))
                .expect_err("branch key fields must match the selected branch fingerprint");
        assert!(matches!(
            error.current_context(),
            WasmStateResetRuntimeError::InvalidScope { .. }
        ));
    }

    #[test]
    fn reset_completion_rejects_a_different_fence_or_preparation() {
        let processor = named::<ModelName>("counting_guest");
        let request = CommandExecutionReference::parse("reset-alpha")
            .expect("the fixture reset reference must be valid");
        let other = CommandExecutionReference::parse("reset-beta")
            .expect("the fixture reset reference must be valid");
        let mut ready = nervix_models::WasmStateReset::publishing(
            request.clone(),
            WasmStateResetScope::Unbranched,
            nervix_models::WasmStateResetReason::Operator,
        );
        ready.mark_ready();

        let mut fence = Some(nervix_models::WasmStateReset::publishing(
            other.clone(),
            WasmStateResetScope::Unbranched,
            nervix_models::WasmStateResetReason::Operator,
        ));
        let mut prepared = None;
        let error =
            complete_processor_wasm_state_reset(&processor, &ready, &mut fence, &mut prepared)
                .expect_err("a ready revision cannot clear another request's fence");
        assert!(matches!(
            error.current_context(),
            WasmStateResetRuntimeError::RequestConflict { .. }
        ));

        fence = Some(nervix_models::WasmStateReset::publishing(
            request.clone(),
            WasmStateResetScope::Unbranched,
            nervix_models::WasmStateResetReason::Operator,
        ));
        prepared = Some(PreparedWasmStateReset {
            request: other,
            scope: WasmStateResetScope::Unbranched,
            published: true,
            branches: Vec::new(),
        });
        let error =
            complete_processor_wasm_state_reset(&processor, &ready, &mut fence, &mut prepared)
                .expect_err("a ready revision cannot clear another request's preparation");
        assert!(matches!(
            error.current_context(),
            WasmStateResetRuntimeError::RequestConflict { .. }
        ));
    }

    #[tokio::test]
    async fn processor_branch_tasks_are_created_and_reused_per_branch_key() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        publish_state_identity(
            &runtime,
            &domain,
            ModelKind::Deduplicator,
            named::<ModelName>("dedup_users"),
        );
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
            wasm_state_reset: None,
        };
        let mut instances = BranchInstanceRegistry::<Option<BranchKey>, ProcessorBranchTask>::new();
        let domain_clock = runtime
            .bind_domain_clock(&domain)
            .expect("the fixture installs a running unpaced clock");
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
                domain_clock: &domain_clock,
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
                domain_clock: &domain_clock,
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
                domain_clock: &domain_clock,
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
            Timestamp::now(),
            1,
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
                domain_clock: &runtime
                    .bind_domain_clock(&domain)
                    .expect("the fixture installs a running unpaced clock"),
            },
            &mut instances,
            input_relay.clone(),
            quiesce_test_batch(),
            NodeQuiesceWorkGuard::begin(counters.clone()),
        )
        .await;

        assert_eq!(counters.admitted_work(), 1);
        let queued = input_rx
            .recv()
            .await
            .expect("processor input should remain in the branch mailbox");
        assert_eq!(queued.relay, input_relay);
        assert_eq!(counters.admitted_work(), 1);
        drop(queued);
        assert_eq!(counters.admitted_work(), 0);

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
    async fn accepted_processor_input_records_branch_activity_at_its_own_domain_time() {
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
        // A supervisor that started waiting long before this batch arrived would hold a sample
        // this old. Accepting the input must replace it with the domain time of the acceptance.
        let stale = Timestamp::from_unix_nanos(1_000_000_000);
        instances.insert_restored(
            None,
            stale,
            1,
            ProcessorBranchTask {
                input: input_tx,
                commands,
                task: parking_lot::Mutex::new(Some(task)),
            },
        );
        let domain_clock = runtime
            .bind_domain_clock(&domain)
            .expect("the fixture installs a running unpaced clock");
        let before = domain_clock
            .snapshot()
            .expect("the fixture installs a running unpaced clock")
            .now();

        dispatch_processor_node_input(
            ProcessorNodeDispatchContext {
                runtime_handle: &runtime,
                domain: &domain,
                graph: &StdArc::new(ArcSwapOption::from(None)),
                template: &template,
                domain_clock: &domain_clock,
            },
            &mut instances,
            input_relay.clone(),
            quiesce_test_batch(),
            NodeQuiesceWorkGuard::begin(counters.clone()),
        )
        .await;

        let mut recorded = None;
        for entry in instances.snapshot_entries() {
            if entry.key.is_none() {
                recorded = Some(entry.last_ingestion);
            }
        }
        let recorded = recorded.expect("the unbranched instance stays registered");
        assert!(
            recorded >= before,
            "accepted input must record activity at or after the acceptance, got {recorded:?}"
        );

        let queued = input_rx
            .recv()
            .await
            .expect("processor input should remain in the branch mailbox");
        drop(queued);
        let entry = instances
            .remove(&None)
            .expect("test branch task should remain registered");
        let task = entry
            .task
            .lock()
            .take()
            .expect("test branch task should still be running");
        task.abort();
        task.await
            .discarded("an aborted fixture task reports only its own cancellation");
    }

    #[tokio::test]
    async fn detached_branch_publishes_buffered_route_output_before_it_stops() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let processor = named::<ModelName>("route_orders");
        let output = named::<RelayName>("routed_orders");
        let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let mut downstream =
            RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
        let services = Arc::new(RelayBoundaryServices::new(fanout, 1, 0, Vec::new(), None));
        let registry = RelayRegistry::new();
        let owner = runtime.spawn_relay_owner_task(
            &domain,
            &output,
            registry.clone(),
            services.clone(),
            RelayRetention::default(),
        );
        let mut template = junction_branch_template(processor.as_str(), "orders");
        template.relays.insert(
            output.clone(),
            RelayProcessorRelayTemplate { registry, services },
        );
        let junction = template
            .processors
            .get_mut(&processor)
            .expect("the fixture declares its junction");
        let RelayProcessorOperationTemplate::Junction { output_routes } = &mut junction.operation
        else {
            panic!("the fixture must declare a junction");
        };
        output_routes.routes.push(RelayProcessorOutputTemplate {
            output_relay: output.clone(),
            construction: nervix_models::RouteConstruction::default(),
            flush_policy: Some(RuntimeFlushPolicy::Each {
                interval: Duration::from_secs(3600),
                max_batch_size: 1024 * 1024,
            }),
            message_error_policy: MessageErrorPolicy::Log,
        });
        let domain_clock = runtime
            .bind_domain_clock(&domain)
            .expect("the fixture installs a running unpaced clock");
        let mut branch = template
            .instantiate(&runtime, &domain, None, 1)
            .await
            .expect("the junction fixture instantiates")
            .into_inner();
        let node = branch
            .processors
            .get_mut(&processor)
            .expect("the fixture instantiates its junction");
        let RelayProcessorOperationNode::Junction { output_routes } = &mut node.operation else {
            panic!("the fixture must instantiate a junction");
        };
        let route = output_routes
            .routes
            .first_mut()
            .expect("the junction instantiates the route its template declares");
        let flush_due = route
            .enqueue(
                quiesce_test_batch(),
                &domain_clock,
                &domain_clock
                    .snapshot()
                    .expect("the fixture installs a running unpaced clock"),
            )
            .expect("an hour-long cadence arms its route deadline");
        assert!(
            !flush_due,
            "an hour-long cadence must hold the accepted output"
        );

        let (_input_sender, input) = mpsc::channel(1);
        let (commands, command_rx) = mpsc::channel(1);
        commands
            .send(ProcessorBranchCommand::Stop(
                ProcessorBranchStopMode::Detach,
            ))
            .await
            .expect("the stop command queues before the branch task runs");
        let (snapshot_shutdown, _) = watch::channel(false);
        timeout(
            Duration::from_secs(2),
            run_processor_branch_task(
                ProcessorRuntimeContext::new(
                    runtime.clone(),
                    domain.clone(),
                    StdArc::new(ArcSwapOption::from(None)),
                ),
                ProcessorBranchRunIdentity {
                    processor: processor.clone(),
                    incarnation: 1,
                },
                branch,
                input,
                command_rx,
                runtime
                    .node_quiesce_counters(&domain, NodeRef::new(ModelKind::Junction, &processor)),
                ProcessorSnapshotTask {
                    shutdown_tx: snapshot_shutdown,
                    task: None,
                    requests: None,
                },
            ),
        )
        .await
        .expect("a detached branch task stops");

        let published = timeout(Duration::from_millis(500), downstream.recv())
            .await
            .expect("detaching a branch must publish the output its route still buffers")
            .expect("the live relay owner keeps the output relay open");
        assert_eq!(published.message_count(), 1);
        owner
            .stop(Duration::from_secs(1))
            .await
            .expect("the fixture relay owner stops");
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

        assert!(matches!(
            error.current_context(),
            ScheduledNodeHandoffError::CommandTimeout
        ));
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

        assert!(matches!(
            error.current_context(),
            ScheduledNodeHandoffError::ResponseDropped
        ));
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn scheduled_processor_handoff_reports_an_unavailable_command_receiver() {
        let (commands, command_rx) = mpsc::channel(1);
        drop(command_rx);
        let task = tokio::spawn(std::future::pending::<()>());
        let scheduled = ScheduledNodeTask { commands, task };

        let error = scheduled
            .handoff_within(Duration::from_millis(10))
            .await
            .expect_err("a closed processor mailbox must fail handoff");

        assert!(matches!(
            error.current_context(),
            ScheduledNodeHandoffError::CommandUnavailable
        ));
    }

    #[tokio::test]
    async fn scheduled_processor_handoff_bounds_the_response_wait() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            let Some(ProcessorNodeCommand::Handoff { response }) = command_rx.recv().await else {
                panic!("scheduled processor must receive its handoff command")
            };
            let _response = response;
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledNodeTask { commands, task };

        let error = scheduled
            .handoff_within(Duration::from_millis(10))
            .await
            .expect_err("a stalled handoff response must time out");

        assert!(matches!(
            error.current_context(),
            ScheduledNodeHandoffError::ResponseTimeout
        ));
    }

    #[tokio::test]
    async fn scheduled_processor_handoff_reports_a_failed_task_join() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            let Some(ProcessorNodeCommand::Handoff { response }) = command_rx.recv().await else {
                panic!("scheduled processor must receive its handoff command")
            };
            response
                .send(Vec::new())
                .expect("handoff receiver must remain while the task responds");
            panic!("test processor task failure");
        });
        let scheduled = ScheduledNodeTask { commands, task };

        let error = scheduled
            .handoff_within(Duration::from_secs(1))
            .await
            .expect_err("a failed processor task must fail handoff");

        assert!(matches!(
            error.current_context(),
            ScheduledNodeHandoffError::TaskJoin
        ));
    }

    #[tokio::test]
    async fn scheduled_processor_handoff_bounds_task_shutdown() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            let Some(ProcessorNodeCommand::Handoff { response }) = command_rx.recv().await else {
                panic!("scheduled processor must receive its handoff command")
            };
            response
                .send(Vec::new())
                .expect("handoff receiver must remain while the task responds");
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledNodeTask { commands, task };

        let error = scheduled
            .handoff_within(Duration::from_millis(10))
            .await
            .expect_err("a processor task that does not stop must time out");

        assert!(matches!(
            error.current_context(),
            ScheduledNodeHandoffError::TaskStopTimeout
        ));
    }
}
