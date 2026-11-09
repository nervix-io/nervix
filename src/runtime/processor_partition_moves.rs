//! A processor unit's part in moving its branch's keys between units while the branch's partition
//! count changes.
//!
//! Layer: data plane.
//!
//! - **Owns.** The slot a key takes in the processor's branch registry, live or released to the unit
//!   that claims it; a holder's release and forget of one key; a claimant unit's catalog reads,
//!   parked input and claims; the tasks that read catalogs, claim and forget keys; and the unit's
//!   published holdings.
//! - **Depends on.** The processor's branch tasks and registry, state placements, the runtime's
//!   checkpoint transfer and store, the interconnect claim exchange and the unit's partition
//!   transition.
//! - **Must not know.** Plexus text, transaction planning, consensus, or when a transition retires.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a key moves between units once per claim, and a unit's holdings change only \
                  when a key it holds outside its partition is released, forgotten, expires or is \
                  claimed"
    )
)]

use std::collections::VecDeque;

use error_stack::{Report, ResultExt as _};
use nervix_interconnect::{
    PartitionClaimOutcome, PartitionClaimStep, PartitionClaimSubject, ReleasedBranch,
    ReleasedCheckpoint, ReleasedKey, RemoteOperationFailure, RemoteOperationSubject,
    StateCheckpointRead,
};
use nervix_models::{
    ClusterNodeName, DomainName, DomainNodeRef, DomainNodeUnitRef, ModelKind, ModelName, NodeRef,
    PartitionEpochNumber, Timestamp, Unit,
};
use nervix_recovery::NoReceiver as _;
use tracing::{debug, warn};

use super::{
    BranchInstanceRegistry, BranchInstanceTemplate, BranchKey, BranchScope, DomainClock,
    ProcessorBranchLifetime, ProcessorBranchTask, ProcessorNodeCommand, ProcessorRuntimeContext,
    Runtime, RuntimeStateKind, RuntimeStatePlacement,
    partition_claims::{
        CatalogResolution, ClaimAdmission, ClaimRetries, ClaimStart, PartitionClaimError,
        PartitionClaimant, ReadCatalog, RetryStep, UnitPartitionTransition,
    },
    partition_holdings::HoldingsTracker,
    processor_branch_task::{
        ProcessorBranchInput, ProcessorBranchStopMode, spawn_processor_branch_task,
        stop_processor_branch_task,
    },
    state_replication::StateReplicationError,
};

/// The branches of one processor unit, each live or released to the unit claiming it.
pub(super) type ProcessorBranches = BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchSlot>;

/// What one key's entry in a processor unit's branch registry holds.
pub(super) enum ProcessorBranchSlot {
    /// The key's branch runs here.
    Live(ProcessorBranchTask),
    /// The key's branch stopped and its state was released to the unit the current partition count
    /// places it in. The state stays here, and the key in this unit's lifecycle, until that unit
    /// installed it and asks this one to forget it.
    Released(ReleasedProcessorBranch),
}

impl ProcessorBranchSlot {
    /// The key's running branch, unless it was released.
    pub(super) fn live(&self) -> Option<&ProcessorBranchTask> {
        match self {
            Self::Live(task) => Some(task),
            Self::Released(_) => None,
        }
    }
}

/// The state a holding unit released for one key, as the claimant received it.
#[derive(Debug, Clone)]
pub(super) struct ReleasedProcessorBranch {
    pub(super) last_activity: Timestamp,
    pub(super) incarnation: u64,
    pub(super) checkpoints: Vec<ReleasedCheckpoint>,
}

impl ReleasedProcessorBranch {
    fn outcome(&self) -> PartitionClaimOutcome {
        PartitionClaimOutcome::Released(ReleasedKey::Branch(ReleasedBranch {
            incarnation: self.incarnation,
            last_activity: self.last_activity,
            checkpoints: self.checkpoints.clone(),
        }))
    }
}

/// A holder's step for one key, as a claimant asked for it.
pub(super) struct HolderKeyRequest {
    /// The epoch the claimant claims in; a holder in another epoch is not ready for it.
    pub(super) epoch: PartitionEpochNumber,
    pub(super) key: Option<BranchKey>,
    pub(super) step: PartitionClaimStep,
    pub(super) response: nervix_primitives::sync::oneshot::Sender<
        Result<PartitionClaimOutcome, RemoteOperationFailure>,
    >,
}

/// What a claim task reports back to its claimant unit's supervisor.
pub(super) enum ClaimProgress {
    /// The catalog every peer unit answered, or why a peer did not answer.
    CatalogRead(error_stack::Result<ReadCatalog, PartitionClaimError>),
    /// The claim of `key` installed its state here, found it held nowhere, or failed.
    Claimed {
        key: BranchKey,
        result: error_stack::Result<ClaimedKey, PartitionClaimError>,
    },
    /// The holder of `key` forgot it, or why it did not.
    Forgotten {
        key: BranchKey,
        holder: Unit,
        result: error_stack::Result<(), PartitionClaimError>,
    },
}

/// What a settled claim found.
pub(super) enum ClaimedKey {
    /// No unit held state for the key: it begins here.
    Absent,
    /// The key's state was installed under this unit's placements: its branch resumes here with
    /// the lifetime its holder gave it.
    Installed {
        holder: Unit,
        incarnation: u64,
        last_activity: Timestamp,
    },
}

/// A processor unit's moves while its branch is in a partition transition.
pub(super) struct ProcessorPartitionMoves {
    transition: UnitPartitionTransition,
    entity: NodeRef,
    claimant: PartitionClaimant<ProcessorBranchInput>,
    holdings: HoldingsTracker,
    /// The supervisor's own command channel, which claim tasks report their progress through.
    commands: nervix_primitives::sync::mpsc::Sender<ProcessorNodeCommand>,
    /// Claims and forgets that failed and wait for their next attempt.
    retries: ClaimRetries,
}

impl ProcessorPartitionMoves {
    /// Begins the moves of one processor unit whose restored branches are `instances`, publishing
    /// the keys it holds outside their partition. `capacity` bounds the input it parks.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a unit in a transition registers its holdings once, after it restored its \
                      branches"
        )
    )]
    pub(super) fn begin(
        runtime: &Runtime,
        domain: &DomainName,
        template: &BranchInstanceTemplate,
        transition: UnitPartitionTransition,
        instances: &ProcessorBranches,
        commands: nervix_primitives::sync::mpsc::Sender<ProcessorNodeCommand>,
        capacity: usize,
    ) -> Self {
        let entity = template.owner.node.clone();
        let unit = DomainNodeUnitRef::new(
            DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone()),
            transition.unit,
        );
        let entries = instances.snapshot_entries();
        let holdings = HoldingsTracker::begin(
            runtime,
            unit,
            &transition,
            entries.iter().map(|entry| &entry.key),
        );
        Self {
            transition,
            entity,
            claimant: PartitionClaimant::new(capacity),
            holdings,
            commands,
            retries: ClaimRetries::new(),
        }
    }

    /// Whether the unit holds as much parked input as its bound allows, so it takes no more.
    pub(super) fn is_full(&self) -> bool {
        self.claimant.is_full()
    }

    /// When the next failed step is tried again.
    pub(super) fn next_retry(&self) -> Option<nervix_primitives::time::Instant> {
        self.retries.next_due()
    }

    fn publish(&self) {
        self.holdings.publish(self.claimant.claiming());
    }

    /// Takes `input` for a key this unit does not hold, unless the key may lie in another unit:
    /// then the input waits for the key's claim, which this starts when it is the first to wait.
    /// Returns the input to dispatch now, if any.
    pub(super) fn admit(
        &mut self,
        runtime: &Runtime,
        domain: &DomainName,
        template: &BranchInstanceTemplate,
        instances: &ProcessorBranches,
        input: ProcessorBranchInput,
    ) -> Option<ProcessorBranchInput> {
        let key = input.batch.key.clone();
        if instances.contains_key(&key) {
            return Some(input);
        }
        let Some(concrete) = key.clone() else {
            return Some(input);
        };
        if !self.claimant.is_parked(&concrete) && !self.transition.may_lie_elsewhere(&key) {
            return Some(input);
        }
        let parked = concrete.clone();
        match self.claimant.admit(concrete, input) {
            ClaimAdmission::Begin(input) => Some(input),
            ClaimAdmission::Parked {
                start: ClaimStart::Nothing,
            } => None,
            ClaimAdmission::Parked {
                start: ClaimStart::CatalogRead,
            } => {
                self.spawn_catalog_read(runtime, domain);
                None
            }
            ClaimAdmission::Parked {
                start: ClaimStart::Claim { holder },
            } => {
                self.spawn_claim(runtime, domain, template, parked, holder);
                self.publish();
                None
            }
        }
    }

    fn spawn_catalog_read(&self, runtime: &Runtime, domain: &DomainName) {
        let runtime = runtime.clone();
        let domain = domain.clone();
        let entity = self.entity.clone();
        let transition = self.transition.clone();
        let commands = self.commands.clone();
        nervix_primitives::task::spawn(async move {
            let result = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "a unit reads its partition catalog once per transition, again only after a \
                 failed read",
                runtime.read_partition_catalog(&domain, &entity, &transition)
            )
            .await;
            commands
                .send(ProcessorNodeCommand::PartitionClaim(
                    ClaimProgress::CatalogRead(result),
                ))
                .await
                .means_shutdown("processor unit awaiting its partition catalog");
        });
    }

    fn spawn_claim(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        template: &BranchInstanceTemplate,
        key: BranchKey,
        holder: Unit,
    ) {
        let runtime = runtime.clone();
        let domain = domain.clone();
        let entity = self.entity.clone();
        let subject = PartitionClaimSubject {
            domain: domain.clone(),
            entity: entity.clone(),
            epoch: self.transition.epoch,
            holder,
            claimant: self.transition.unit,
        };
        let state = processor_state_kind(template.owner.node.kind);
        let commands = self.commands.clone();
        nervix_primitives::task::spawn(async move {
            let result = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "a claim moves one key's state between units once per key and epoch",
                runtime.claim_processor_key(&subject, state, &key)
            )
            .await;
            commands
                .send(ProcessorNodeCommand::PartitionClaim(
                    ClaimProgress::Claimed { key, result },
                ))
                .await
                .means_shutdown("processor unit awaiting a partition claim");
        });
    }

    fn spawn_forget(&self, runtime: &Runtime, domain: &DomainName, key: BranchKey, holder: Unit) {
        let runtime = runtime.clone();
        let subject = PartitionClaimSubject {
            domain: domain.clone(),
            entity: self.entity.clone(),
            epoch: self.transition.epoch,
            holder,
            claimant: self.transition.unit,
        };
        let commands = self.commands.clone();
        nervix_primitives::task::spawn(async move {
            let result = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "a holder forgets one claimed key once its claimant installed the key",
                runtime.request_partition_step(&subject, &key, PartitionClaimStep::Forget)
            )
            .await
            .map(|_| ());
            commands
                .send(ProcessorNodeCommand::PartitionClaim(
                    ClaimProgress::Forgotten {
                        key,
                        holder,
                        result,
                    },
                ))
                .await
                .means_shutdown("processor unit awaiting a partition forget");
        });
    }

    /// Tries every failed step whose time came again.
    pub(super) fn retry_due(
        &mut self,
        runtime: &Runtime,
        domain: &DomainName,
        template: &BranchInstanceTemplate,
    ) {
        let now = nervix_primitives::time::Instant::now();
        for step in self.retries.take_due(now) {
            match step {
                RetryStep::CatalogRead => {
                    if self.claimant.restart_catalog_read() {
                        self.spawn_catalog_read(runtime, domain);
                    }
                }
                RetryStep::Claim { key, holder } => {
                    self.spawn_claim(runtime, domain, template, key, holder);
                }
                RetryStep::Forget { key, holder } => {
                    self.spawn_forget(runtime, domain, key, holder);
                }
            }
        }
    }

    fn retry_later(&mut self, step: RetryStep) {
        self.retries.later(step);
    }

    /// Applies one claim task's progress. Returns the parked inputs the progress released, in the
    /// order they must be dispatched, after installing any branch whose state arrived.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "each claim task reports once per catalog read, claim or forget, and an \
                      installed claim starts its key's branch once"
        )
    )]
    pub(super) async fn progress(
        &mut self,
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
        instances: &mut ProcessorBranches,
        progress: ClaimProgress,
    ) -> Vec<ProcessorBranchInput> {
        let runtime = &context.runtime_handle;
        let domain = &context.domain;
        let mut released = Vec::new();
        match progress {
            ClaimProgress::CatalogRead(Ok(catalog)) => {
                for resolution in self.claimant.catalog_read(catalog.holders()) {
                    match resolution {
                        CatalogResolution::Begin { inputs, .. } => released.extend(inputs),
                        CatalogResolution::Claim { key, holder } => {
                            self.spawn_claim(runtime, domain, template, key, holder);
                        }
                    }
                }
                self.publish();
            }
            ClaimProgress::CatalogRead(Err(error)) => {
                warn!(
                    domain = domain.as_str(),
                    processor = template.owner.node.identifier.as_str(),
                    error = %format_args!("{error:#}"),
                    "partition catalog read failed; parked input waits for the next attempt"
                );
                self.claimant.catalog_failed();
                self.retry_later(RetryStep::CatalogRead);
            }
            ClaimProgress::Claimed {
                key,
                result: Ok(ClaimedKey::Absent),
            } => {
                released.extend(self.claimant.claim_settled(&key));
                self.publish();
            }
            ClaimProgress::Claimed {
                key,
                result:
                    Ok(ClaimedKey::Installed {
                        holder,
                        incarnation,
                        last_activity,
                    }),
            } => {
                match install_claimed_branch(
                    context,
                    template,
                    instances,
                    &key,
                    incarnation,
                    last_activity,
                )
                .await
                {
                    Ok(()) => {
                        released.extend(self.claimant.claim_settled(&key));
                        self.holdings.claimed_one();
                        self.publish();
                        self.spawn_forget(runtime, domain, key, holder);
                    }
                    Err(error) => {
                        warn!(
                            domain = domain.as_str(),
                            processor = template.owner.node.identifier.as_str(),
                            error = %format_args!("{error:#}"),
                            "claimed branch could not be restored; the claim is tried again"
                        );
                        self.retry_later(RetryStep::Claim { key, holder });
                    }
                }
            }
            ClaimProgress::Claimed {
                key,
                result: Err(error),
            } => {
                warn!(
                    domain = domain.as_str(),
                    processor = template.owner.node.identifier.as_str(),
                    error = %format_args!("{error:#}"),
                    "partition claim failed; the key's input waits for the next attempt"
                );
                // A claim that ended while this one ran leaves nothing waiting for it.
                if let Some(holder) = self.claimant.claim_holder(&key) {
                    self.retry_later(RetryStep::Claim { key, holder });
                }
            }
            ClaimProgress::Forgotten { result: Ok(()), .. } => {}
            ClaimProgress::Forgotten {
                key,
                holder,
                result: Err(error),
            } => {
                debug!(
                    domain = domain.as_str(),
                    processor = template.owner.node.identifier.as_str(),
                    error = %format_args!("{error:#}"),
                    "partition forget failed; it is tried again"
                );
                self.retry_later(RetryStep::Forget { key, holder });
            }
        }
        released
    }

    /// Drops from the published holdings every key this unit no longer holds because it forgot,
    /// expired or evicted it.
    pub(super) fn retain_held(&mut self, instances: &ProcessorBranches) {
        if self
            .holdings
            .retain(|key| instances.contains_key(&Some(key.clone())))
        {
            self.publish();
        }
    }

    /// Every input the unit parked, for a supervisor that ends its claims before they settle: one
    /// that stops refuses them, and one whose transition retired runs them.
    pub(super) fn drain_parked(&mut self) -> Vec<ProcessorBranchInput> {
        let mut inputs = Vec::new();
        for (_, parked) in self.claimant.drain_parked() {
            inputs.extend(parked);
        }
        inputs
    }
}

/// The kind of state a processor of `kind` keeps per key.
fn processor_state_kind(kind: ModelKind) -> RuntimeStateKind {
    match kind {
        ModelKind::WindowProcessor => RuntimeStateKind::WindowProcessor,
        ModelKind::WasmProcessor => RuntimeStateKind::WasmProcessor,
        _ => RuntimeStateKind::Deduplicator,
    }
}

/// Restores the branch of `key`, whose released state was installed under this unit, as the
/// lifetime its holder gave it, and records it in the unit's lifecycle before anything else of it
/// happens.
async fn install_claimed_branch(
    context: &ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    instances: &mut ProcessorBranches,
    key: &BranchKey,
    incarnation: u64,
    last_activity: Timestamp,
) -> error_stack::Result<(), PartitionClaimError> {
    let branch = Some(key.clone());
    let task = spawn_processor_branch_task(
        context.clone(),
        template,
        branch.clone(),
        VecDeque::new(),
        ProcessorBranchLifetime::Restored,
        incarnation,
    )
    .await
    .change_context_lazy(|| PartitionClaimError::Restore {
        branch: BranchScope::from(&branch),
    })?;
    instances.insert_restored(
        branch.clone(),
        last_activity,
        incarnation,
        ProcessorBranchSlot::Live(task),
    );
    if instances.version() < incarnation {
        instances.set_version(incarnation);
    }
    // The claimed branch is a lifecycle change of this unit: refreshing it advances the version, so
    // the lifecycle this unit persists next names it.
    instances.touch(&branch, last_activity);
    context.runtime_handle.observe_branch_instance_created(
        &context.domain,
        template.branch.as_ref(),
        &branch,
    );
    Ok(())
}

/// A claimant's step for one key, as the holder's supervisor received it.
pub(super) struct HolderStep {
    pub(super) epoch: PartitionEpochNumber,
    pub(super) key: Option<BranchKey>,
    pub(super) step: PartitionClaimStep,
}

/// Answers a claimant's step at this unit, once the unit restored its branches and executes in the
/// claimant's epoch; until then the claimant is told to try again.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a holder releases and forgets each key it holds outside its partition once per \
                  claim"
    )
)]
pub(super) async fn answer_partition_key(
    context: &ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    instances: &mut ProcessorBranches,
    restoring: bool,
    clock: &DomainClock,
    step: HolderStep,
) -> Result<PartitionClaimOutcome, RemoteOperationFailure> {
    let subject = || {
        RemoteOperationSubject::entity(
            &context.domain,
            template.owner.node.kind,
            ModelName::from(&template.owner.node.identifier),
        )
    };
    if restoring {
        return Err(RemoteOperationFailure::not_ready(subject()));
    }
    let same_epoch = match &template.partition_transition {
        Some(transition) => transition.epoch == step.epoch,
        None => false,
    };
    if !same_epoch {
        return Err(RemoteOperationFailure::not_ready(subject()));
    }
    let now = match clock.snapshot() {
        Ok(snapshot) => snapshot.now(),
        Err(error) => {
            return Err(RemoteOperationFailure::failed(
                subject(),
                format!("{error:#}"),
            ));
        }
    };
    answer_holder_step(context, template, instances, step.key, step.step, now).await
}

/// Answers a claimant's step for `key` at this holding unit.
///
/// A release stops the key's live branch, which drains its pending work and persists its final
/// state, keeps the key in the lifecycle as a released slot whose expiry counts from now, and
/// answers with the descriptor of each checkpoint of its state. A forget drops the slot and the
/// key's state. Both are idempotent.
async fn answer_holder_step(
    context: &ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    instances: &mut ProcessorBranches,
    key: Option<BranchKey>,
    step: PartitionClaimStep,
    now: Timestamp,
) -> Result<PartitionClaimOutcome, RemoteOperationFailure> {
    match step {
        PartitionClaimStep::Release => release_key(context, template, instances, key, now).await,
        PartitionClaimStep::Forget => {
            forget_key(context, template, instances, key).await;
            Ok(PartitionClaimOutcome::Forgotten)
        }
    }
}

async fn release_key(
    context: &ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    instances: &mut ProcessorBranches,
    key: Option<BranchKey>,
    now: Timestamp,
) -> Result<PartitionClaimOutcome, RemoteOperationFailure> {
    let Some(slot) = instances.get(&key) else {
        return Ok(PartitionClaimOutcome::NotHeld);
    };
    let task = match slot.as_ref() {
        ProcessorBranchSlot::Released(released) => return Ok(released.outcome()),
        ProcessorBranchSlot::Live(task) => task,
    };
    let processor = ModelName::from(&template.owner.node.identifier);
    let (response, receiver) = nervix_primitives::sync::oneshot::channel();
    stop_processor_branch_task(
        &context.domain,
        processor.clone(),
        &key,
        task,
        ProcessorBranchStopMode::Handoff(response),
    )
    .await;
    let (restored_at, incarnation) = match receiver.await {
        Ok(handoff) => {
            // Input the branch held for `REQUIRED WAIT` cannot follow its key to another node,
            // since its acknowledgements belong to this one: it is refused, and its source sends
            // it again to the key's unit.
            for mut pending in handoff.pending_materialized {
                if let Some(batch) = pending.batch.take() {
                    for ack in batch.acks.iter() {
                        ack.no_ack(
                            "the key moved to the partition its branch's count places it in",
                        );
                    }
                }
            }
            (handoff.restored_at, handoff.incarnation)
        }
        Err(_) => {
            return Err(RemoteOperationFailure::not_ready(
                RemoteOperationSubject::entity(
                    &context.domain,
                    template.owner.node.kind,
                    processor,
                ),
            ));
        }
    };
    let checkpoints = describe_released_checkpoints(context, template, &key).await?;
    let released = ReleasedProcessorBranch {
        last_activity: restored_at,
        incarnation,
        checkpoints,
    };
    let outcome = released.outcome();
    instances.replace_state(&key, ProcessorBranchSlot::Released(released));
    // A released slot expires by the branch's TTL counted from its release.
    instances.touch(&key, now);
    Ok(outcome)
}

async fn describe_released_checkpoints(
    context: &ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    key: &Option<BranchKey>,
) -> Result<Vec<ReleasedCheckpoint>, RemoteOperationFailure> {
    let runtime = &context.runtime_handle;
    let processor = ModelName::from(&template.owner.node.identifier);
    let subject = || {
        RemoteOperationSubject::entity(&context.domain, template.owner.node.kind, processor.clone())
    };
    let placement = runtime
        .state_placement_for_unit(
            &context.domain,
            processor_state_kind(template.owner.node.kind),
            template.owner.node.kind,
            processor.clone(),
            template.owner.unit,
            key.clone(),
        )
        .map_err(|error| RemoteOperationFailure::failed(subject(), format!("{error:#}")))?;
    let Some(admitted) = runtime.resolve_state_replication_request(&placement) else {
        // A branch that never published state has nothing to move.
        return Ok(Vec::new());
    };
    let snapshot = runtime
        .answer_state_sync_request(admitted, None)
        .await
        .map_err(|error| RemoteOperationFailure::failed(subject(), format!("{error:#}")))?;
    let Some(snapshot) = snapshot else {
        return Ok(Vec::new());
    };
    let descriptor = runtime
        .describe_state_checkpoint(&placement, snapshot)
        .await
        .map_err(|error| RemoteOperationFailure::failed(subject(), format!("{error:#}")))?;
    Ok(vec![ReleasedCheckpoint {
        placement: placement.to_remote(),
        snapshot: descriptor,
    }])
}

async fn forget_key(
    context: &ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    instances: &mut ProcessorBranches,
    key: Option<BranchKey>,
) {
    let Some(slot) = instances.remove(&key) else {
        return;
    };
    let processor = ModelName::from(&template.owner.node.identifier);
    context.runtime_handle.observe_branch_instance_removed(
        &context.domain,
        template.branch.as_ref(),
        &key,
        None,
    );
    template.retire_branch_channels(&key);
    if let ProcessorBranchSlot::Live(task) = slot.as_ref() {
        stop_processor_branch_task(
            &context.domain,
            processor.clone(),
            &key,
            task,
            ProcessorBranchStopMode::Evict,
        )
        .await;
    }
    context
        .runtime_handle
        .retire_moved_branch_state(&context.domain, template, &key);
}

impl Runtime {
    /// Claims `key` for the claimant `subject` names: asks its holder to release it, fetches each
    /// released checkpoint over the bulk pool and installs it under the claimant's placement as
    /// that placement's newest revision.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a claim moves one key's state between units once per key and epoch"
        )
    )]
    async fn claim_processor_key(
        &self,
        subject: &PartitionClaimSubject,
        state: RuntimeStateKind,
        key: &BranchKey,
    ) -> error_stack::Result<ClaimedKey, PartitionClaimError> {
        let outcome = self
            .request_partition_step(subject, key, PartitionClaimStep::Release)
            .await?;
        let released = match outcome {
            PartitionClaimOutcome::NotHeld => return Ok(ClaimedKey::Absent),
            PartitionClaimOutcome::Released(ReleasedKey::Branch(released)) => released,
            PartitionClaimOutcome::Released(ReleasedKey::Row(_))
            | PartitionClaimOutcome::Forgotten => {
                return Err(Report::new(PartitionClaimError::UnexpectedAnswer {
                    entity: subject.entity.clone(),
                    unit: subject.holder,
                }));
            }
        };
        let holder_owner =
            self.partition_unit_owner(&subject.domain, &subject.entity, subject.holder)?;
        let branch = Some(key.clone());
        for checkpoint in released.checkpoints {
            nervix_primitives::task::consume_budget().await;
            let holder_placement = RuntimeStatePlacement::from_remote(checkpoint.placement)
                .change_context(PartitionClaimError::ReleasedPlacement)?;
            let snapshot = self
                .fetch_released_checkpoint(&holder_owner, &holder_placement, &checkpoint.snapshot)
                .await
                .change_context_lazy(|| PartitionClaimError::Fetch {
                    branch: BranchScope::from(&branch),
                })?;
            let target = self
                .state_placement_for_unit(
                    &subject.domain,
                    state,
                    subject.entity.kind,
                    subject.entity.identifier.clone(),
                    subject.claimant,
                    branch.clone(),
                )
                .change_context_lazy(|| PartitionClaimError::Install {
                    branch: BranchScope::from(&branch),
                })?;
            self.install_claimed_checkpoint(&target, snapshot)
                .await
                .change_context_lazy(|| PartitionClaimError::Install {
                    branch: BranchScope::from(&branch),
                })?;
        }
        Ok(ClaimedKey::Installed {
            holder: subject.holder,
            incarnation: released.incarnation,
            last_activity: released.last_activity,
        })
    }

    /// Installs `snapshot`, the released state of a claimed key, as the newest revision of
    /// `placement`, this unit's placement of the key. A copy this node kept of the placement from
    /// an earlier lifetime of the key is dropped first, so the branch that resumes opens the
    /// installed state.
    async fn install_claimed_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: super::PersistedRuntimeStateEntry,
    ) -> error_stack::Result<(), super::RuntimePersistenceError> {
        self.drop_retained_branch_state(placement);
        let Some(store) = self.inner.state_store.clone() else {
            // A node without a state store keeps no checkpoint; the branch opens the state the
            // transfer installed for it.
            self.inner
                .passive_runtime_state_snapshots
                .insert(placement.clone(), snapshot);
            return Ok(());
        };
        store.install_moved_snapshot(placement, snapshot).await
    }

    /// Withdraws the in-memory state this node retains for `placement` and its replication route,
    /// leaving the stored checkpoint, as evicting a branch does.
    fn drop_retained_branch_state(&self, placement: &RuntimeStatePlacement) {
        self.inner.state_replication_routing.retire(placement);
        self.inner.replicated_deduplicator_states.remove(placement);
        self.inner
            .replicated_window_processor_states
            .remove(placement);
        self.inner
            .replicated_wasm_processor_states
            .remove(placement);
    }

    /// Withdraws the state this unit kept for `key` of `template`'s processor once the key moved
    /// to the unit its partition count places it in.
    fn retire_moved_branch_state(
        &self,
        domain: &DomainName,
        template: &BranchInstanceTemplate,
        key: &Option<BranchKey>,
    ) {
        let state = processor_state_kind(template.owner.node.kind);
        let placement = self.state_placement_for_unit(
            domain,
            state,
            template.owner.node.kind,
            ModelName::from(&template.owner.node.identifier),
            template.owner.unit,
            key.clone(),
        );
        match placement {
            Ok(placement) => self.drop_retained_branch_state(&placement),
            Err(error) => {
                debug!(
                    domain = domain.as_str(),
                    processor = template.owner.node.identifier.as_str(),
                    error = %format_args!("{error:#}"),
                    "a forgotten branch's placement is no longer published"
                );
            }
        }
    }

    /// Reads one released checkpoint from the node owning its holder, locally when that is this
    /// node.
    async fn fetch_released_checkpoint(
        &self,
        owner: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        descriptor: &nervix_interconnect::StateSnapshotEnvelope,
    ) -> error_stack::Result<super::PersistedRuntimeStateEntry, StateReplicationError> {
        let local = match self.inner.remote_dispatcher.load().as_deref() {
            Some(dispatcher) => dispatcher.local_node_id() == owner,
            None => true,
        };
        if local
            && let Some(admitted) = self.resolve_state_replication_request(placement)
            && let Some(snapshot) = self.answer_state_sync_request(admitted, None).await?
            && snapshot.lsm == descriptor.lsm
        {
            return Ok(snapshot);
        }
        self.fetch_state_checkpoint(owner, placement, descriptor, StateCheckpointRead::Published)
            .await
    }
}
