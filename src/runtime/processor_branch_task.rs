//! Branch-local processor task lifecycle and persisted handoff state.
//!
//! Layer: data plane.
//!
//! - **Owns.** Processor branch task creation, FIFO input execution, ticking, checkpointing,
//!   handoff, and eviction.
//! - **Depends on.** Planned processor templates, concrete branch runtimes, runtime persistence,
//!   and relay boundaries.
//! - **Must not know.** Plexus text, control-plane transactions, consensus, or connector protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "processor branch creation binds the retained plan and its branch-local \
                  execution task"
    )
)]

use error_stack::ResultExt as _;

use super::{
    materialized_read::MaterializedStateWait,
    processor_partition_moves::{
        ClaimProgress, HolderKeyRequest, HolderStep, ProcessorBranchSlot, ProcessorBranches,
        ProcessorPartitionMoves, answer_partition_key,
    },
    processor_wasm_state_reset::{
        ProcessorWasmStateResetContext, abort_processor_wasm_state_reset,
        complete_processor_wasm_state_reset,
    },
    relay_interaction::RELAY_INTERACTION_COMMAND_CAPACITY,
    *,
};

/// Every way starting or restoring a processor's branch tasks fails.
///
/// A variant that concerns one branch names it by its [`BranchScope`], never by its key's values.
#[derive(Debug, thiserror::Error)]
pub(super) enum ProcessorBranchTaskError {
    #[error("failed to instantiate the processor ({branch})")]
    Instantiate { branch: BranchScope },
    #[error("failed to initialize a new window ({branch})")]
    InitializeWindow { branch: BranchScope },
    #[error("failed to read the persisted processor branch LRU snapshot")]
    ReadLruSnapshot,
    #[error("failed to decode the persisted processor branch LRU snapshot")]
    DecodeLruSnapshot,
    #[error("could not read the domain time of accepted input ({branch})")]
    AcceptedInputClock { branch: BranchScope },
    #[error("the processor task is unavailable ({branch})")]
    Unavailable { branch: BranchScope },
    #[error("the processor has not restored its branches")]
    RestorePending,
    #[error("the branch lifecycle the handed-over processor branches belong to is unavailable")]
    HandedOffLifecycleUnavailable,
    #[error(
        "the handed-over processor ({branch}) began at lifecycle revision {incarnation}, after \
         the lifecycle revision {lsm} it belongs to"
    )]
    HandedOffLifetimeAfterLifecycle {
        branch: BranchScope,
        incarnation: u64,
        lsm: u64,
    },
}

pub(super) const PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

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

/// What a processor's branches publish when they are asked to checkpoint, decided by who asks.
#[derive(Debug, Clone, Copy)]
pub(super) enum ProcessorCheckpointPurpose {
    /// A backup capture reads the state store next. A branch publishes the state its periodic
    /// snapshot may not hold yet. A WASM branch publishes nothing: the checkpoint that completed
    /// its last callback already holds the guest state on its owner and its replicas, and saving
    /// the guest again would wait for those replicas inside the cut.
    BackupCut,
    /// An ownership handoff moves the processor next, so every branch checkpoints its live state,
    /// a WASM guest included.
    OwnershipHandoff,
}

pub(super) enum ProcessorBranchCommand {
    Checkpoint {
        purpose: ProcessorCheckpointPurpose,
        response: oneshot::Sender<OwnershipHandoffResult<()>>,
    },
    /// An operator inspection of the rows the branch holds, answered between two batches.
    Inspect {
        read: BranchStateRead,
        response: oneshot::Sender<error_stack::Result<BranchStateCopy, BranchStateReadError>>,
    },
    Stop(ProcessorBranchStopMode),
}

pub(super) struct ProcessorBranchTask {
    pub(super) input: mpsc::Sender<ProcessorBranchInput>,
    pub(super) commands: mpsc::Sender<ProcessorBranchCommand>,
    /// The branch's task. It is ended when this handle is dropped, so a branch never outlives the
    /// processor task that owns it: a processor task ended before it could stop its branches, as a
    /// shutdown grace ends one, ends every branch with it rather than leaving them running without
    /// an owner.
    pub(super) task: nervix_primitives::sync::blocking::Mutex<Option<AbortOnDropHandle<()>>>,
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

/// What spawning a processor's snapshot task produced. Stateful inferencers, deduplicators and
/// window processors answer snapshot requests; other processors still report their spawned task.
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
        purpose: ProcessorCheckpointPurpose,
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
    /// A claimant asks this unit, which may hold a key outside its partition, to release or
    /// forget it.
    PartitionKey(HolderKeyRequest),
    /// A claim task this unit started reports its progress.
    PartitionClaim(ClaimProgress),
    /// The partition transition of the unit's branch retired: every key lies in its partition, so
    /// the unit stops claiming keys and runs the input it parked.
    PartitionTransitionRetired,
    /// Names the branch tasks an operator inspection reads, without waiting for any of them.
    SelectStateBranches {
        selection: nervix_models::StateInspectionBranchSelection,
        response: oneshot::Sender<error_stack::Result<SelectedStateBranches, BranchStateReadError>>,
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

/// The command channels of one processor unit's supervisor.
pub(super) struct ProcessorNodeChannels {
    pub(super) commands: mpsc::Receiver<ProcessorNodeCommand>,
    /// A sender into `commands` the supervisor hands its claim tasks, present while its branch's
    /// keys move between units.
    pub(super) claim_reports: Option<mpsc::Sender<ProcessorNodeCommand>>,
}

/// The domain-scoped execution context a processor task runs inside.
#[derive(Clone)]
pub(in crate::runtime) struct ProcessorRuntimeContext {
    pub(super) runtime_handle: Runtime,
    pub(super) domain: DomainName,
}

impl ProcessorRuntimeContext {
    pub(in crate::runtime) fn new(runtime_handle: Runtime, domain: DomainName) -> Self {
        Self {
            runtime_handle,
            domain,
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
    let (commands, command_rx) = mpsc::channel(RELAY_INTERACTION_COMMAND_CAPACITY);
    // While its branch's keys move between units, the supervisor's claim tasks report through its
    // own command channel. It stops on shutdown or handoff, never on its commands closing.
    let claim_reports = if template.partition_transition.is_some() {
        Some(commands.clone())
    } else {
        None
    };
    let channels = ProcessorNodeChannels {
        commands: command_rx,
        claim_reports,
    };
    let task = if template.owner.node.kind == ModelKind::Inferencer {
        let metrics = context.runtime_handle.inferencer_state_metrics(
            &context.domain,
            &template.owner.node.identifier,
            template.owner.unit,
        );
        nervix_primitives::task::spawn(async move {
            let _inferencer_state_metrics = metrics;
            run_processor_node_runtime(
                context,
                template,
                inputs,
                shutdown_rx,
                channels,
                handoffs,
                expiration_scan_interval,
            )
            .await;
        })
    } else {
        nervix_primitives::task::spawn(run_processor_node_runtime(
            context,
            template,
            inputs,
            shutdown_rx,
            channels,
            handoffs,
            expiration_scan_interval,
        ))
    };
    ScheduledNodeTask { commands, task }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the installed processor supervisor dispatches every batch and maintenance poll"
    )
)]
pub(super) async fn run_processor_node_runtime(
    context: ProcessorRuntimeContext,
    mut template: BranchInstanceTemplate,
    inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
    shutdown_rx: watch::Receiver<bool>,
    channels: ProcessorNodeChannels,
    restored_handoffs: Vec<ProcessorBranchHandoff>,
    expiration_scan_interval: Duration,
) {
    let ProcessorNodeChannels {
        commands: command_rx,
        mut claim_reports,
    } = channels;
    let ProcessorRuntimeContext {
        runtime_handle,
        domain,
    } = context;
    let processor = template.owner.node.identifier.clone();
    let domain_clock = match nervix_primitives::expect_lint!(
        nervix::lifecycle_call,
        "this task or concrete branch transition attaches its retained domain clock before \
         executing in the selected lifetime",
        runtime_handle.bind_domain_clock(&domain)
    ) {
        Ok(clock) => clock,
        Err(error) => {
            runtime_handle.events().report_error(format!(
                "processor '{}' in domain '{}' could not bind its clock: {error:#}",
                processor.as_str(),
                domain.as_str(),
            ));
            return;
        }
    };
    let ownership_entity = DomainNodeRef::node_in(
        domain.clone(),
        template.owner.node.kind,
        ModelName::from(&processor),
    );
    let ownership_freeze = nervix_primitives::expect_lint!(
        nervix::lifecycle_call,
        "this task binds its exact ownership freeze publication before entering its recurring loop",
        OwnershipHandoffFreezeWatch::for_unit(
            &runtime_handle,
            DomainNodeUnitRef::new(ownership_entity.clone(), template.owner.unit),
        )
    );
    runtime_handle.register_branch_lifecycle_metrics(&domain, template.branch.as_ref());
    let mut instances = ProcessorBranches::new();
    let mut last_persisted_lru_lsm = 0;
    // The task restores its branches before it takes any input. Until one attempt installs every
    // branch, its input stays queued in its relays, so no record starts a new lifetime for a branch
    // the restore would resume.
    let restore_context = ProcessorRuntimeContext::new(runtime_handle.clone(), domain.clone());
    let mut pending_restore = Some(PendingProcessorBranchRestore::new(restored_handoffs));
    let quiesce_counters = nervix_primitives::expect_lint!(
        nervix::lifecycle_call,
        "task startup installs and retains the exact node quiesce counters before processing input",
        runtime_handle.unit_quiesce_counters(&domain, template.owner.clone())
    );
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
    let mut moves = None::<ProcessorPartitionMoves>;

    let mut handoff_response = None;
    loop {
        nervix_primitives::task::consume_budget().await;
        let ownership_frozen = ownership_freeze.observe().is_frozen();
        let snapshot = match domain_clock.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                runtime_handle.events().report_error(format!(
                    "processor '{}' in domain '{}' lost its clock: {error:#}",
                    processor.as_str(),
                    domain.as_str(),
                ));
                break;
            }
        };
        let now = snapshot.now();
        let mut did_scheduled_work = false;
        if let Some(restore) = pending_restore.as_mut()
            && Instant::now() >= restore.retry_at()
        {
            let attempt = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the processor task installs the branch lifetimes it restores before it accepts \
                 input, and a failed attempt retries only after its backoff",
                restore.attempt(
                    &restore_context,
                    &template,
                    &mut instances,
                    &mut last_persisted_lru_lsm,
                )
            )
            .await;
            match attempt {
                Ok(()) => {
                    pending_restore = None;
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
                    if let (Some(transition), Some(reports)) =
                        (template.partition_transition.clone(), claim_reports.clone())
                    {
                        // Each claim parks one batch of its key's input; a unit parks no more than
                        // its input relay's owner buffers.
                        let capacity = match template.relays.get(&template.root_relay) {
                            Some(relay) => relay.services.owner_capacity(),
                            None => 1,
                        };
                        moves = Some(nervix_primitives::expect_lint!(
                            nervix::lifecycle_call,
                            "a unit in a partition transition begins its moves once, after its \
                             restore installed its branches",
                            ProcessorPartitionMoves::begin(
                                &runtime_handle,
                                &domain,
                                &template,
                                transition,
                                &instances,
                                reports,
                                capacity,
                            )
                        ));
                    }
                }
                Err(error) => {
                    warn!(
                        domain = domain.as_str(),
                        processor = processor.as_str(),
                        error = %format_args!("{error:#}"),
                        "failed to restore processor branches; their input waits for the next attempt"
                    );
                }
            }
            did_scheduled_work = true;
        }
        if pending_restore.is_none() && !ownership_frozen && Instant::now() >= next_expiration_scan
        {
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
                if let Some(moves) = moves.as_mut() {
                    moves.retain_held(&instances);
                }
            }
            next_expiration_scan = Instant::now() + expiration_scan_interval;
            did_scheduled_work = true;
        }
        if let Some(moves) = moves.as_mut()
            && moves
                .next_retry()
                .is_some_and(|retry_at| Instant::now() >= retry_at)
        {
            moves.retry_due(&runtime_handle, &domain, &template);
            did_scheduled_work = true;
        }
        if pending_restore.is_none()
            && Instant::now() >= next_lru_snapshot
            && prepared_reset.is_none()
        {
            if let Err(error) = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "explicit checkpoint cadence or terminal teardown captures one branch lifecycle \
                 generation; the record path does not invoke it",
                persist_branch_instance_lru_snapshot(
                    &runtime_handle,
                    &domain,
                    &template,
                    &instances,
                    &mut last_persisted_lru_lsm,
                )
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
            let mut next_maintenance = match pending_restore.as_ref() {
                Some(restore) => restore.retry_at(),
                None => next_expiration_scan.min(next_lru_snapshot),
            };
            if let Some(retry_at) = moves.as_ref().and_then(ProcessorPartitionMoves::next_retry) {
                next_maintenance = next_maintenance.min(retry_at);
            }
            next_maintenance
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO)
        };
        let wake = match RuntimeWake::after(maintenance_timeout) {
            Ok(wake) => wake,
            Err(error) => {
                runtime_handle.events().report_error(format!(
                    "processor '{}' in domain '{}' could not schedule its next maintenance scan: \
                     {error:#}",
                    processor.as_str(),
                    domain.as_str(),
                ));
                break;
            }
        };
        // A pending restore keeps new input in the relays; only a drain still delivers it. A unit
        // whose parked input fills its bound takes no more until a claim releases some.
        let parked_full = match moves.as_ref() {
            Some(moves) => moves.is_full(),
            None => false,
        };
        let work = match interaction
            .next_with_input(wake, pending_restore.is_none() && !parked_full)
            .await
        {
            Ok(work) => work,
            Err(failure) => {
                runtime_handle.handle_internal_processor_error_for_acks(
                    &domain,
                    template.owner.node.kind,
                    &processor,
                    &template.error_policies,
                    [&failure.acks],
                    &failure.error,
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
                if pending_restore.is_some() {
                    // A drain delivers input the restore has not caught up with. Processing it
                    // would start a new lifetime for a branch the restore resumes.
                    runtime_handle.handle_internal_processor_error_for_acks(
                        &domain,
                        template.owner.node.kind,
                        &processor,
                        &template.error_policies,
                        batch.acks.iter(),
                        &Report::new(ProcessorBranchTaskError::RestorePending),
                    );
                    drop(work);
                    continue;
                }
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
                let input = ProcessorBranchInput { relay, batch, work };
                let input = match moves.as_mut() {
                    Some(moves) => {
                        moves.admit(&runtime_handle, &domain, &template, &instances, input)
                    }
                    None => Some(input),
                };
                if let Some(input) = input {
                    dispatch_processor_node_input(
                        ProcessorNodeDispatchContext {
                            runtime_handle: &runtime_handle,
                            domain: &domain,
                            template: &template,
                            domain_clock: &domain_clock,
                        },
                        &mut instances,
                        input.relay,
                        input.batch,
                        input.work,
                    )
                    .await;
                }
            }
            RelayInteractionEvent::Wake => {}
            RelayInteractionEvent::Command(command) => match command {
                // The branches the processor has not restored exist only in its lifecycle, so it
                // cannot checkpoint them or reset their guest state yet.
                ProcessorNodeCommand::Checkpoint { response, .. } if pending_restore.is_some() => {
                    response
                        .send(Err(Report::new(
                            OwnershipHandoffError::BranchesUnrestored {
                                kind: template.owner.node.kind,
                                identifier: ModelName::from(&processor),
                            },
                        )))
                        .means_peer_left("processor lifecycle checkpoint requester");
                }
                ProcessorNodeCommand::PrepareWasmStateReset { response, .. }
                    if pending_restore.is_some() =>
                {
                    response
                        .send(Err(Report::new(
                            WasmStateResetRuntimeError::BranchesUnrestored {
                                processor: ModelName::from(&processor),
                            },
                        )))
                        .means_peer_left("WASM state reset preparation requester");
                }
                ProcessorNodeCommand::ApplyWasmStateReset { reset, response }
                    if pending_restore.is_some()
                        && reset.phase() == nervix_models::WasmStateResetPhase::Publishing =>
                {
                    response
                        .send(Err(Report::new(
                            WasmStateResetRuntimeError::BranchesUnrestored {
                                processor: ModelName::from(&processor),
                            },
                        )))
                        .means_peer_left("WASM state reset schedule application requester");
                }
                ProcessorNodeCommand::SelectStateBranches {
                    selection,
                    response,
                } => SelectedStateBranches::answer(
                    response,
                    &instances,
                    selection,
                    pending_restore.is_some(),
                ),
                ProcessorNodeCommand::Checkpoint { purpose, response } => {
                    let result = nervix_primitives::expect_lint!(
                        nervix::lifecycle_call,
                        "the explicit checkpoint command captures the retained branch generations",
                        checkpoint_all_processor_branch_instances(
                            processor.clone(),
                            &instances,
                            purpose
                        )
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
                ProcessorNodeCommand::PartitionKey(request) => {
                    let outcome = nervix_primitives::expect_lint!(
                        nervix::lifecycle_call,
                        "a claimant's release or forget moves one key's branch out of this unit \
                         once per claim",
                        answer_partition_key(
                            &restore_context,
                            &template,
                            &mut instances,
                            pending_restore.is_some(),
                            &domain_clock,
                            HolderStep {
                                epoch: request.epoch,
                                key: request.key,
                                step: request.step,
                            },
                        )
                    )
                    .await;
                    if let Some(moves) = moves.as_mut() {
                        moves.retain_held(&instances);
                    }
                    request
                        .response
                        .send(outcome)
                        .means_peer_left("partition claimant");
                }
                ProcessorNodeCommand::PartitionTransitionRetired => {
                    // The leader retires a transition once no unit holds or claims a key outside
                    // its partition, so a claim still in flight finds no state: the input it
                    // parked begins its key here, in arrival order. A claimant still in the
                    // transition elsewhere is told this unit is not ready until it retires too.
                    template.partition_transition = None;
                    // Without claims the supervisor keeps no sender of its own commands, so they
                    // close with its handle as any supervisor's do.
                    claim_reports = None;
                    let Some(mut retired) = moves.take() else {
                        continue;
                    };
                    for input in retired.drain_parked() {
                        nervix_primitives::task::consume_budget().await;
                        dispatch_processor_node_input(
                            ProcessorNodeDispatchContext {
                                runtime_handle: &runtime_handle,
                                domain: &domain,
                                template: &template,
                                domain_clock: &domain_clock,
                            },
                            &mut instances,
                            input.relay,
                            input.batch,
                            input.work,
                        )
                        .await;
                    }
                }
                ProcessorNodeCommand::PartitionClaim(progress) => {
                    let Some(moves_in_progress) = moves.as_mut() else {
                        continue;
                    };
                    let released = nervix_primitives::expect_lint!(
                        nervix::lifecycle_call,
                        "a claim task's report settles one catalog read, claim or forget, and an \
                         installed claim starts its key's branch once",
                        moves_in_progress.progress(
                            &restore_context,
                            &template,
                            &mut instances,
                            progress
                        )
                    )
                    .await;
                    for input in released {
                        nervix_primitives::task::consume_budget().await;
                        dispatch_processor_node_input(
                            ProcessorNodeDispatchContext {
                                runtime_handle: &runtime_handle,
                                domain: &domain,
                                template: &template,
                                domain_clock: &domain_clock,
                            },
                            &mut instances,
                            input.relay,
                            input.batch,
                            input.work,
                        )
                        .await;
                    }
                }
                ProcessorNodeCommand::PrepareWasmStateReset {
                    preparation,
                    response,
                } => {
                    let reset_context =
                        ProcessorWasmStateResetContext::new(&runtime_handle, &domain, &template);
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
                            ProcessorWasmStateResetContext::new(&runtime_handle, &domain, &template)
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

    if let Some(moves) = moves.as_mut() {
        // Input still waiting for its key's claim cannot be handed over with the key's state,
        // which stays in its holder: it is refused, and its source sends it again.
        for input in moves.drain_parked() {
            for ack in input.batch.acks.iter() {
                ack.no_ack("the processor unit stopped before its key's claim settled");
            }
            drop(input.work);
        }
    }
    if let Err(error) = nervix_primitives::expect_lint!(
        nervix::lifecycle_call,
        "explicit checkpoint cadence or terminal teardown captures one branch lifecycle \
         generation; the record path does not invoke it",
        persist_branch_instance_lru_snapshot(
            &runtime_handle,
            &domain,
            &template,
            &instances,
            &mut last_persisted_lru_lsm,
        )
    ) {
        warn!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            error = %format_args!("{error:#}"),
            "failed to persist final processor branch lru snapshot"
        );
    }
    if let Some(response) = handoff_response {
        let handoffs = match pending_restore {
            // The successor restores the branches this task never installed.
            Some(restore) => restore.into_handoffs(),
            None => {
                nervix_primitives::expect_lint!(
                    nervix::lifecycle_call,
                    "the ownership transfer transition captures and detaches its retained branch \
                     generations",
                    handoff_all_processor_branch_instances(
                        &runtime_handle,
                        &domain,
                        &processor,
                        template.branch.as_ref(),
                        &mut instances,
                    )
                )
                .await
            }
        };
        response
            .send(handoffs)
            .means_peer_left("processor handoff requester");
    } else {
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "terminal processor teardown ends its exact retained branch generations",
            shutdown_all_processor_branch_instances(
                &runtime_handle,
                &domain,
                &processor,
                template.branch.as_ref(),
                &mut instances,
            )
        )
        .await;
    }
}

pub(super) struct ProcessorNodeDispatchContext<'a> {
    pub(super) runtime_handle: &'a Runtime,
    pub(super) domain: &'a DomainName,
    pub(super) template: &'a BranchInstanceTemplate,
    pub(super) domain_clock: &'a DomainClock,
}

pub(super) async fn dispatch_processor_node_input(
    context: ProcessorNodeDispatchContext<'_>,
    instances: &mut ProcessorBranches,
    relay: RelayName,
    batch: RelayRecordBatch,
    dequeued_work: NodeQuiesceWorkGuard,
) {
    let ProcessorNodeDispatchContext {
        runtime_handle,
        domain,
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
                template.owner.node.kind,
                &template.owner.node.identifier,
                &template.error_policies,
                batch.acks.iter(),
                &error.change_context(ProcessorBranchTaskError::AcceptedInputClock {
                    branch: BranchScope::from(&key),
                }),
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
        let state = match nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the missing or replacement concrete branch installs one retained task under its \
             branch key and incarnation",
            spawn_processor_branch_task(
                ProcessorRuntimeContext::new(runtime_handle.clone(), domain.clone()),
                template,
                key.clone(),
                VecDeque::new(),
                ProcessorBranchLifetime::Appeared,
                incarnation,
            )
        )
        .await
        {
            Ok(state) => state,
            Err(error) => {
                runtime_handle.handle_internal_processor_error_for_acks(
                    domain,
                    template.owner.node.kind,
                    &template.owner.node.identifier,
                    &template.error_policies,
                    batch.acks.iter(),
                    &error,
                );
                return;
            }
        };
        let state =
            instances.insert_changed(key.clone(), accepted_at, ProcessorBranchSlot::Live(state));
        GetOrCreateBranchInstance {
            state,
            created: true,
        }
    };
    if instance.created {
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the concrete branch creation transition registers its one metric identity",
            runtime_handle.observe_branch_instance_created(domain, template.branch.as_ref(), &key)
        );
        debug!(
            domain = domain.as_str(),
            processor = template.owner.node.identifier.as_str(),
            scope = %BranchScope::from(&key),
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
        if template.owner.node.kind == ModelKind::WasmProcessor
            && let Err(error) = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "a new WASM branch offers the entity's lifecycle to its replicas once, when the \
                 branch is created",
                publish_branch_instance_lru_snapshot(runtime_handle, domain, template, instances)
            )
        {
            warn!(
                domain = domain.as_str(),
                processor = template.owner.node.identifier.as_str(),
                error = %format_args!("{error:#}"),
                "failed to offer a new processor branch to its replicas"
            );
        }
    }
    let Some(task) = instance.state.live() else {
        // The key was released to the unit its branch's partition count places it in, which
        // receives its input from here on; input that still reaches this unit is refused, and
        // its source sends it again.
        for ack in batch.acks.iter() {
            ack.no_ack("the key moved to the partition its branch's count places it in");
        }
        drop(dequeued_work);
        return;
    };
    let input = ProcessorBranchInput {
        relay,
        batch,
        work: dequeued_work,
    };
    if let Err(mpsc::error::SendError(input)) = task.input.send(input).await {
        runtime_handle.handle_internal_processor_error_for_acks(
            domain,
            template.owner.node.kind,
            &template.owner.node.identifier,
            &template.error_policies,
            input.batch.acks.iter(),
            &Report::new(ProcessorBranchTaskError::Unavailable {
                branch: BranchScope::from(&key),
            }),
        );
        if let Some(entry) = instances.remove(&key) {
            nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the concrete branch ending or detach transition withdraws its exact metric \
                 identity",
                runtime_handle.observe_branch_instance_removed(
                    domain,
                    template.branch.as_ref(),
                    &key,
                    None,
                )
            );
            if let Some(task) = entry.live() {
                nervix_primitives::expect_lint!(
                    nervix::lifecycle_call,
                    "the eviction, reset or ownership transfer transition ends its exact retained \
                     branch task lifetime",
                    stop_processor_branch_task(
                        domain,
                        &template.owner.node.identifier,
                        &key,
                        task,
                        ProcessorBranchStopMode::Detach,
                    )
                )
                .await;
            }
        }
        drop(input.work);
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "one concrete processor branch installs its clock, routing and task handles"
    )
)]
pub(super) async fn spawn_processor_branch_task(
    context: ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    key: Option<BranchKey>,
    pending_materialized: VecDeque<PendingMaterializedBatch>,
    lifetime: ProcessorBranchLifetime,
    incarnation: u64,
) -> error_stack::Result<ProcessorBranchTask, ProcessorBranchTaskError> {
    let branch =
        PreparedProcessorBranch::prepare(context, template, key, lifetime, incarnation).await?;
    Ok(branch.start(pending_materialized))
}

/// A concrete processor branch built from its plan and not started yet.
///
/// Building a branch opens its retained state and can fail; starting it cannot. A restore builds
/// every branch it installs before it starts any, so a failed attempt leaves nothing running.
pub(super) struct PreparedProcessorBranch {
    context: ProcessorRuntimeContext,
    branch: BranchRuntime,
    incarnation: u64,
}

impl PreparedProcessorBranch {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one concrete processor branch opens its retained state and binds its clock"
        )
    )]
    pub(super) async fn prepare(
        context: ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
        key: Option<BranchKey>,
        lifetime: ProcessorBranchLifetime,
        incarnation: u64,
    ) -> error_stack::Result<Self, ProcessorBranchTaskError> {
        let published_template = if matches!(lifetime, ProcessorBranchLifetime::Appeared) {
            context
                .runtime_handle
                .domain_routing_cache(&context.domain)
                .and_then(|mut routing| {
                    routing
                        .load()
                        .processor_plans
                        .get(&template.owner)
                        .map(|plan| plan.template.clone())
                })
        } else {
            None
        };
        let template = published_template.unwrap_or_else(|| StdArc::new(template.clone()));
        let branch_key = key.clone();
        let mut branch = template
            .instantiate(&context.runtime_handle, &context.domain, key, incarnation)
            .await
            .change_context_lazy(|| ProcessorBranchTaskError::Instantiate {
                branch: BranchScope::from(&branch_key),
            })?
            .into_inner();
        if template.owner.node.kind == ModelKind::WindowProcessor
            && let ProcessorBranchLifetime::Appeared = lifetime
        {
            let processor = ModelName::from(&template.owner.node.identifier);
            if let Some(node) = branch.processors.get_mut(&processor) {
                node.reset_window_state();
            }
            branch
                .snapshot_processor_live_state(&processor)
                .change_context_lazy(|| ProcessorBranchTaskError::InitializeWindow {
                    branch: BranchScope::from(&branch_key),
                })?;
        }
        Ok(Self {
            context,
            branch,
            incarnation,
        })
    }

    /// Start the branch's task with the input still pending for it.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one concrete processor branch installs its task handles"
        )
    )]
    pub(super) fn start(
        self,
        pending_materialized: VecDeque<PendingMaterializedBatch>,
    ) -> ProcessorBranchTask {
        let Self {
            context,
            mut branch,
            incarnation,
        } = self;
        let processor = branch.owner.node.identifier.clone();
        if let Some(node) = branch.processors.get_mut(&processor) {
            node.pending_materialized = pending_materialized;
        }
        let (input_tx, input_rx) = mpsc::channel(1);
        let (command_tx, command_rx) = mpsc::channel(1);
        let (snapshot_shutdown_tx, _) = watch::channel(false);
        let spawned = match branch.processors.get(&processor) {
            Some(node) => node.spawn_snapshot_task(&context.runtime_handle, &snapshot_shutdown_tx),
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
        let quiesce_counters = context
            .runtime_handle
            .unit_quiesce_counters(&context.domain, branch.owner.clone());
        let task = nervix_primitives::task::spawn(run_processor_branch_task(
            context,
            ProcessorBranchRunIdentity {
                processor,
                incarnation,
            },
            branch,
            input_rx,
            command_rx,
            quiesce_counters,
            snapshot_task,
        ));
        ProcessorBranchTask {
            input: input_tx,
            commands: command_tx,
            task: nervix_primitives::sync::blocking::Mutex::new(Some(AbortOnDropHandle::new(task))),
        }
    }
}

pub(super) async fn stop_processor_snapshot_task(
    branch: &mut BranchRuntime,
    processor: &ModelName,
    snapshot: &mut ProcessorSnapshotTask,
) {
    if let Some(requests) = snapshot.requests.as_mut() {
        requests.close();
        while let Some(response) = requests.recv().await {
            nervix_primitives::task::consume_budget().await;
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

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained processor branch handles every batch and terminal flush"
    )
)]
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
    } = context;
    let mut force_flush = nervix_primitives::expect_lint!(
        nervix::lifecycle_call,
        "branch task startup registers and retains its exact force-flush participant before \
         processing input",
        runtime_handle.force_flush_participant(&domain, quiesce_counters.clone())
    );
    let mut quiesce_gauges = BranchQuiesceGauges::new(quiesce_counters.clone());
    let ownership_freeze = nervix_primitives::expect_lint!(
        nervix::lifecycle_call,
        "this task binds its exact ownership freeze publication before entering its recurring loop",
        OwnershipHandoffFreezeWatch::for_unit(
            &runtime_handle,
            DomainNodeUnitRef::new(branch.owner.node.in_domain(&domain), branch.owner.unit)
        )
    );
    let domain_clock = branch.domain_clock.clone();
    let mut materialized_wait =
        MaterializedStateWait::new(&runtime_handle.inner.materialized_state_changed);
    quiesce_gauges.observe(&branch, &processor);
    let stop_mode;
    let mut handoff_execution_snapshot = None;
    let mut events = HandledEvents::default();
    loop {
        nervix_primitives::task::consume_budget().await;
        let freeze = ownership_freeze.observe();
        let ownership_frozen = freeze.is_frozen();
        let execution_snapshot = match domain_clock.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                runtime_handle.events().report_error(format!(
                    "processor branch '{}' in domain '{}' lost its clock: {error:#}",
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
                     {error:#}",
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
            events.record();
            branch.tick(&execution_snapshot).await;
            quiesce_gauges.observe(&branch, &processor);
            continue;
        }
        // Freeze rechecks are physical, so they stay a plain monotonic sleep. The branch's own
        // deadline is logical and is awaited on the domain clock below. An unfrozen branch with
        // no deadline waits only for its input and commands.
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
        nervix_primitives::select! {
            biased;
            command = command_rx.recv() => {
                match command {
                    Some(ProcessorBranchCommand::Checkpoint { purpose, response }) => {
                        let result = match domain_clock.snapshot() {
                            Ok(snapshot) => {
                                branch
                                    .checkpoint_processor_live_state(
                                        &processor,
                                        snapshot.now(),
                                        purpose,
                                    )
                                    .await
                            }
                            Err(error) => Err(OwnershipHandoffError::checkpoint(format!(
                                "processor branch '{}' in domain '{}' lost its clock: {error:#}",
                                processor.as_str(),
                                domain.as_str(),
                            ))),
                        };
                        response
                            .send(result)
                            .means_peer_left("processor branch checkpoint requester");
                    }
                    Some(ProcessorBranchCommand::Inspect { read, response }) => {
                        let held = branch.copy_processor_held_rows(&processor, read, now);
                        answer_branch_state(response, incarnation, events, held);
                    }
                    Some(ProcessorBranchCommand::Stop(mode)) => {
                        if let ProcessorBranchStopMode::Handoff(_) = &mode {
                            let execution_snapshot = match domain_clock.snapshot() {
                                Ok(snapshot) => snapshot,
                                Err(error) => {
                                    runtime_handle.events().report_error(format!(
                                        "processor branch '{}' in domain '{}' lost its clock: \
                                         {error:#}",
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
                        events.record();
                        branch
                            .execute_processor_input(&processor, &relay, batch)
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
                nervix_primitives::select! {
                    () = materialized_wait.changed() => {}
                    _ = sleep(runtime_handle.inner.state_replication_poll_interval) => {}
                }
            }, if has_pending_materialized && !ownership_frozen => {
                events.record();
                materialized_wait.register_again();
                branch
                    .retry_processor_pending_materialized(&processor)
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
                            "processor branch '{}' in domain '{}' lost its clock: {error:#}",
                            processor.as_str(),
                            domain.as_str(),
                        ));
                        completion.complete();
                        stop_mode = Some(ProcessorBranchStopMode::Detach);
                        break;
                    }
                };
                events.record();
                // A generation releases everything the branch can release now, including parked
                // messages whose materialized dependency arrived after they were parked.
                if branch.processor_has_pending_materialized(&processor) {
                    materialized_wait.register_again();
                    branch
                        .retry_processor_pending_materialized(&processor)
                        .await;
                }
                branch.force_flush(&flush_snapshot).await;
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
                         deadline: {error:#}",
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
                         deadline: {error:#}",
                        processor.as_str(),
                        domain.as_str(),
                    ));
                    stop_mode = Some(ProcessorBranchStopMode::Detach);
                    break;
                }
            }
            _ = sleep(OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL), if ownership_frozen => {}
        }
    }
    while let Ok(ProcessorBranchInput { relay, batch, work }) = input.try_recv() {
        branch
            .execute_processor_input(&processor, &relay, batch)
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
                     publish its accepted output: {error:#}",
                    processor.as_str(),
                    domain.as_str(),
                ));
                // Without a clock the processor produces no output. Running its collected input
                // still reports each batch through the processor's error policy.
                branch.flush_processor_collected_inputs(&processor).await;
                None
            }
        },
        Some(ProcessorBranchStopMode::Evict) => None,
    };
    if let Some(snapshot) = &finalization_snapshot {
        branch.force_flush(snapshot).await;
    }
    if let Some(ProcessorBranchStopMode::Evict) = &stop_mode {
        branch.evict().await;
    }
    stop_processor_snapshot_task(&mut branch, &processor, &mut snapshot).await;
    match stop_mode {
        Some(ProcessorBranchStopMode::Evict) => {
            if let Some(node) = branch.processors.get(&processor) {
                nervix_primitives::expect_lint!(
                    nervix::lifecycle_call,
                    "the evicted branch and its final writer have ended before exact route \
                     retirement",
                    node.retire_evicted_inferencer_state(&branch.runtime)
                );
            }
        }
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

#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "branch teardown ends and joins the retained task lifetime"
    )
)]
pub(super) async fn stop_processor_branch_task(
    domain: &DomainName,
    processor: impl Into<ModelName>,
    key: &Option<BranchKey>,
    entry: &ProcessorBranchTask,
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
    match nervix_primitives::time::timeout(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, &mut task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(
                domain = domain.as_str(),
                processor = processor.as_str(),
                scope = %BranchScope::from(key),
                error = %error,
                "processor branch task join failed"
            );
        }
        Err(_) => {
            warn!(
                domain = domain.as_str(),
                processor = processor.as_str(),
                scope = %BranchScope::from(key),
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
                    scope = %BranchScope::from(key),
                    error = %error,
                    "aborted processor branch task join failed"
                );
            }
        }
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "explicit checkpoint cadence captures the concrete branch generation"
    )
)]
pub(super) async fn checkpoint_all_processor_branch_instances(
    processor: impl Into<ModelName>,
    instances: &ProcessorBranches,
    purpose: ProcessorCheckpointPurpose,
) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
    let processor = processor.into();
    let states = instances.states();
    for entry in states {
        nervix_primitives::task::consume_budget().await;
        // A released key's branch already stopped with its final state persisted.
        let Some(task) = entry.live() else {
            continue;
        };
        let (response, receiver) = oneshot::channel();
        task.commands
            .send(ProcessorBranchCommand::Checkpoint { purpose, response })
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

#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "ownership transfer checkpoints and detaches concrete branch lifetimes"
    )
)]
pub(super) async fn handoff_all_processor_branch_instances(
    runtime: &Runtime,
    domain: &DomainName,
    processor: impl Into<ModelName>,
    branch: Option<&BranchName>,
    instances: &mut ProcessorBranches,
) -> Vec<ProcessorBranchHandoff> {
    let processor = processor.into();
    let mut handoffs = Vec::new();
    for (key, entry) in instances.drain() {
        runtime.observe_branch_instance_removed(domain, branch, &key, None);
        let task = match entry.as_ref() {
            ProcessorBranchSlot::Live(task) => task,
            ProcessorBranchSlot::Released(released) => {
                // A released key's state stays with this unit until its claimant asks to forget
                // it, so the successor restores it as a branch and releases it again.
                handoffs.push(ProcessorBranchHandoff {
                    key,
                    restored_at: released.last_activity,
                    incarnation: released.incarnation,
                    pending_materialized: VecDeque::new(),
                });
                continue;
            }
        };
        let (response, receiver) = oneshot::channel();
        stop_processor_branch_task(
            domain,
            processor.clone(),
            &key,
            task,
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
    instances: &mut ProcessorBranches,
) {
    let processor = ModelName::from(&template.owner.node.identifier);
    for (key, entry) in instances.expire(now, expiration_after) {
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the concrete branch ending or detach transition withdraws its exact metric identity",
            runtime.observe_branch_instance_removed(
                domain,
                template.branch.as_ref(),
                &key,
                Some(BranchEvictionReason::Ttl),
            )
        );
        template.retire_branch_channels(&key);
        if let Some(task) = entry.live() {
            nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the eviction, reset or ownership transfer transition ends its exact retained \
                 branch task lifetime",
                stop_processor_branch_task(
                    domain,
                    processor.clone(),
                    &key,
                    task,
                    ProcessorBranchStopMode::Evict,
                )
            )
            .await;
        }
        if template.owner.node.kind == ModelKind::WindowProcessor
            && let Err(error) = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the concrete branch eviction withdraws its exact retained window placement",
                runtime.release_evicted_window_state(domain, &processor, template.owner.unit, &key)
            )
        {
            warn!(error = %format_args!("{error:#}"), "failed to release evicted window state");
        }
        debug!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            scope = %BranchScope::from(&key),
            "expired processor branch task"
        );
    }
}

pub(super) async fn evict_processor_branch_instances_to_capacity(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
    max_instances: NonZeroUsize,
    instances: &mut ProcessorBranches,
) {
    let processor = ModelName::from(&template.owner.node.identifier);
    for (key, entry) in instances.evict_lru_to_capacity(max_instances) {
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the concrete branch ending or detach transition withdraws its exact metric identity",
            runtime.observe_branch_instance_removed(
                domain,
                template.branch.as_ref(),
                &key,
                Some(BranchEvictionReason::Lru),
            )
        );
        template.retire_branch_channels(&key);
        if let Some(task) = entry.live() {
            nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the eviction, reset or ownership transfer transition ends its exact retained \
                 branch task lifetime",
                stop_processor_branch_task(
                    domain,
                    processor.clone(),
                    &key,
                    task,
                    ProcessorBranchStopMode::Evict,
                )
            )
            .await;
        }
        if template.owner.node.kind == ModelKind::WindowProcessor
            && let Err(error) = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the concrete branch eviction withdraws its exact retained window placement",
                runtime.release_evicted_window_state(domain, &processor, template.owner.unit, &key)
            )
        {
            warn!(error = %format_args!("{error:#}"), "failed to release evicted window state");
        }
        debug!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            scope = %BranchScope::from(&key),
            max_instances,
            "evicted processor branch task by lru"
        );
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "terminal teardown ends every retained branch lifetime"
    )
)]
pub(super) async fn shutdown_all_processor_branch_instances(
    runtime: &Runtime,
    domain: &DomainName,
    processor: impl Into<ModelName>,
    branch: Option<&BranchName>,
    instances: &mut ProcessorBranches,
) {
    let processor = processor.into();
    for (key, entry) in instances.drain() {
        runtime.observe_branch_instance_removed(domain, branch, &key, None);
        let Some(task) = entry.live() else {
            continue;
        };
        stop_processor_branch_task(
            domain,
            processor.clone(),
            &key,
            task,
            ProcessorBranchStopMode::Detach,
        )
        .await;
        debug!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            scope = %BranchScope::from(&key),
            "stopped processor branch task"
        );
    }
}

#[cfg(test)]
#[path = "processor_branch_dispatch_tests.rs"]
mod dispatch_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "processor_branch_task/tests.rs"]
mod tests;
