//! Putting a domain's previous execution back on this node when an entity swap fails.
//!
//! Layer: data plane.
//! - **Owns.** The journal of what an entity swap stopped, replaced or started on this node, kept
//!   as the swap goes, and the restore that reinstates the previous runtime from it, last change
//!   first.
//! - **Depends on.** The installed domain execution, the revision it ran before the swap, the
//!   routing it last published and the task supervisors of each entity kind.
//! - **Must not know.** Placement decisions, connector drivers or why the swap failed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a failed entity swap restores the runtime it replaced once per failed swap"
    )
)]

use std::marker::PhantomData;

use error_stack::ResultExt as _;

use super::{emitter_supervision::EmitterInput, partition_claims::UnitPartitionTransition, *};
use crate::emitter_execution_plan::EmitterInputUnits;

// The tests fail swaps through the swap fault injection, which only a test build carries.
#[cfg(all(test, feature = "testing"))]
#[path = "entity_swap_restore_tests.rs"]
mod tests;

/// Why one part of the execution a failed swap had replaced could not be put back.
#[derive(Debug, thiserror::Error)]
pub(super) enum EntitySwapRestoreError {
    #[error(
        "failed to restore {} '{}' after its swap failed",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    Entity { node: NodeRef },
    #[error("the revision before the swap has no plan for it")]
    MissingPreviousPlan,
    #[error("relay '{relay}' has no published boundary to register the restored input with")]
    MissingRelayBoundary { relay: RelayName },
    #[error("the domain execution is no longer installed")]
    ExecutionUnavailable,
    #[error("its replacement did not stop")]
    StopReplacement,
    #[error("its replacement did not hand its branches back")]
    ReplacementHandoff,
    #[error("its previous runtime did not start again")]
    StartPrevious,
    #[error("its previous placement could not be built again")]
    BuildPlacement,
    #[error("its previous placement holds no materialized state for a unit it ran")]
    MissingMaterializedState,
}

/// What an entity swap has taken apart on this node so far, in the order it did so.
///
/// A swap records each step before the operation that can fail after it, so the journal always
/// names everything a failed swap has to put back.
pub(super) struct EntitySwapJournal {
    /// The revision the domain ran before the swap. Without an installed execution a swap
    /// replaces nothing, so there is nothing to put back.
    previous: Option<Arc<ExecutionRevision>>,
    steps: Vec<EntitySwapStep>,
}

/// The position of a step of kind `S` in an [`EntitySwapJournal`], held by a swap that records
/// more of that step as it goes.
pub(super) struct EntitySwapStepIndex<S> {
    index: usize,
    kind: PhantomData<fn() -> S>,
}

impl<S> Clone for EntitySwapStepIndex<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for EntitySwapStepIndex<S> {}

/// A kind of step an entity swap records and completes as it goes.
pub(super) trait JournalStep: Sized {
    fn into_step(self) -> EntitySwapStep;

    fn of(step: &mut EntitySwapStep) -> Option<&mut Self>;
}

macro_rules! journal_step {
    ($step:ty, $variant:ident) => {
        impl JournalStep for $step {
            fn into_step(self) -> EntitySwapStep {
                EntitySwapStep::$variant(self)
            }

            fn of(step: &mut EntitySwapStep) -> Option<&mut Self> {
                match step {
                    EntitySwapStep::$variant(step) => Some(step),
                    _ => None,
                }
            }
        }
    };
}

journal_step!(IngestorSwap, Ingestor);
journal_step!(RelayRepartitionSwap, Repartition);
journal_step!(ReboundNodeSwap, Rebound);
journal_step!(PlacementSwap, RelayTransition);
journal_step!(EmitterUnitSwap, Emitter);
journal_step!(ReingestorSwap, Reingestor);
journal_step!(GeneratorUnitSwap, Generator);
journal_step!(ProcessorUnitSwap, Processor);

/// One step of an entity swap, with what putting it back needs.
pub(super) enum EntitySwapStep {
    /// The swap installed the desired revision's state identities.
    StateIdentities,
    /// The swap stopped or started units of one ingestor.
    Ingestor(IngestorSwap),
    /// The swap gave one relay the services of its new partition count.
    Repartition(RelayRepartitionSwap),
    /// The swap rebound the placement-derived runtime of one reassigned node.
    Rebound(ReboundNodeSwap),
    /// The swap replaced the materialized state runtime of one relay.
    RelayTransition(PlacementSwap),
    /// The swap replaced the task of one emitter unit.
    Emitter(EmitterUnitSwap),
    /// The swap replaced the tasks and entrypoints of one reingestor.
    Reingestor(ReingestorSwap),
    /// The swap replaced the task of one generator unit.
    Generator(GeneratorUnitSwap),
    /// The swap replaced the task of one processor unit.
    Processor(ProcessorUnitSwap),
    /// The swap changed one relay's capacity in place.
    RelayCapacity { relay: RelayName },
    /// The swap gave the running tasks of one emitter its new flush policy.
    EmitterFlush { emitter: EmitterName },
}

/// The units of one ingestor a swap stopped. The swap starts none: the revision's activation
/// starts the units it plans here, once every node has prepared it.
pub(super) struct IngestorSwap {
    ingestor: IngestorName,
    stopped: Vec<Unit>,
}

/// One processor unit a swap replaced: the task it ran before and the replacement, as far as the
/// swap got.
pub(super) struct ProcessorUnitSwap {
    owner: NodeUnitRef,
    previous: Option<PreviousProcessorUnit>,
    replacement: Option<ConsumerRegistrations>,
}

/// A processor unit's previous task, which handed its branches over.
struct PreviousProcessorUnit {
    /// The plan the task ran, as the domain last published it.
    plan: Option<StdArc<PublishedProcessorPlan>>,
    /// The branches the task handed over, until a replacement takes them.
    handoffs: Vec<ProcessorBranchHandoff>,
    /// The input relays whose consumer registration of the unit the swap withdrew.
    withdrawn: BTreeSet<RelayName>,
}

/// The consumer registrations a replacement task added, in the acknowledgement mode it reads in.
struct ConsumerRegistrations {
    mode: AckMode,
    registered: Vec<Arc<RelayBoundaryServices>>,
}

/// One emitter unit a swap replaced.
pub(super) struct EmitterUnitSwap {
    owner: NodeUnitRef,
    /// The previous task stopped, and these are the inputs whose registration the swap withdrew.
    previous: Option<BTreeSet<EmitterInputRegistration>>,
    replacement: Option<EmitterReplacement>,
}

/// One relay input of an emitter unit: the whole relay or one of its units.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct EmitterInputRegistration {
    relay: RelayName,
    relay_unit: Option<Unit>,
}

/// What an emitter unit's replacement registered, and whether its task started.
struct EmitterReplacement {
    mode: AckMode,
    registered: Vec<RegisteredEmitterInput>,
    started: bool,
}

/// One consumer registration an emitter's replacement added.
struct RegisteredEmitterInput {
    services: Arc<RelayBoundaryServices>,
    relay_unit: Option<Unit>,
}

/// One reingestor a swap replaced.
pub(super) struct ReingestorSwap {
    entity: NodeRef,
    /// The previous tasks stopped, and these are the input relays whose registration the swap
    /// withdrew.
    previous: Option<BTreeSet<RelayName>>,
    replacement: Option<ConsumerRegistrations>,
}

/// One generator unit a swap replaced.
pub(super) struct GeneratorUnitSwap {
    owner: NodeUnitRef,
    previous_stopped: bool,
    replacement_started: bool,
}

/// The placement-derived runtime of one node a swap rebuilt: its placement tasks and, for a
/// materialized relay, the state tasks of its units.
pub(super) struct PlacementSwap {
    entity: NodeRef,
    /// The units whose placement the swap rebuilt; `None` is every unit of the node.
    units: Option<BTreeSet<Unit>>,
    aborted: bool,
    stopped_state_tasks: Vec<StoppedStateTask>,
    installed: Vec<Unit>,
    started_state_tasks: Vec<StartedStateTask>,
    reissued_kafka_units: Vec<Unit>,
}

/// A state task of a materialized relay unit a swap stopped.
struct StoppedStateTask {
    unit: Unit,
    /// Whether the swap withdrew the unit's consumer registration with it.
    withdrawn: bool,
}

/// A state task of a materialized relay unit a swap started.
struct StartedStateTask {
    unit: Unit,
    /// The services the task registered its consumer with, when it registered one.
    registered: Option<Arc<RelayBoundaryServices>>,
}

/// One reassigned node a swap rebound.
pub(super) struct ReboundNodeSwap {
    stopped_ingestor_units: Vec<Unit>,
    stopped_owner_units: Vec<Unit>,
    placement: PlacementSwap,
    started_owner_units: Vec<Unit>,
}

/// One relay a swap repartitioned.
pub(super) struct RelayRepartitionSwap {
    relay: RelayName,
    stopped_owner_units: Vec<Unit>,
    redefined: bool,
    started_owner_units: Vec<Unit>,
}

impl EntitySwapJournal {
    pub(super) fn new(previous: Option<Arc<ExecutionRevision>>) -> Self {
        Self {
            previous,
            steps: Vec::new(),
        }
    }

    /// Records `step` before the operations it describes, and returns where to complete it.
    pub(super) fn record<S: JournalStep>(&mut self, step: S) -> EntitySwapStepIndex<S> {
        let index = self.steps.len();
        self.steps.push(step.into_step());
        EntitySwapStepIndex {
            index,
            kind: PhantomData,
        }
    }

    /// Records a step that is complete as it stands.
    pub(super) fn record_step(&mut self, step: EntitySwapStep) {
        self.steps.push(step);
    }

    /// The step recorded at `index`.
    pub(super) fn step<S: JournalStep>(&mut self, index: EntitySwapStepIndex<S>) -> &mut S {
        let step = self.steps.get_mut(index.index).and_then(S::of);
        step.verified("a swap completes only the steps it recorded, at the index recording gave")
    }
}

impl IngestorSwap {
    pub(super) fn new(ingestor: IngestorName) -> Self {
        Self {
            ingestor,
            stopped: Vec::new(),
        }
    }

    pub(super) fn record_stopped(&mut self, unit: Unit) {
        self.stopped.push(unit);
    }
}

impl ProcessorUnitSwap {
    pub(super) fn new(owner: NodeUnitRef) -> Self {
        Self {
            owner,
            previous: None,
            replacement: None,
        }
    }

    /// The unit's previous task is stopping; it ran `plan`.
    pub(super) fn record_previous(&mut self, plan: Option<StdArc<PublishedProcessorPlan>>) {
        self.previous = Some(PreviousProcessorUnit {
            plan,
            handoffs: Vec::new(),
            withdrawn: BTreeSet::new(),
        });
    }

    /// The previous task handed `handoffs` over.
    pub(super) fn record_handoffs(&mut self, handoffs: Vec<ProcessorBranchHandoff>) {
        if let Some(previous) = self.previous.as_mut() {
            previous.handoffs = handoffs;
        }
    }

    /// The branches the previous task handed over, for its replacement to take.
    pub(super) fn take_handoffs(&mut self) -> Vec<ProcessorBranchHandoff> {
        match self.previous.as_mut() {
            Some(previous) => std::mem::take(&mut previous.handoffs),
            None => Vec::new(),
        }
    }

    pub(super) fn record_withdrawn(&mut self, relay: RelayName) {
        if let Some(previous) = self.previous.as_mut() {
            previous.withdrawn.insert(relay);
        }
    }

    /// The replacement runs, reading in `mode` through the registrations it added.
    pub(super) fn record_replacement(
        &mut self,
        mode: AckMode,
        registered: Vec<Arc<RelayBoundaryServices>>,
    ) {
        self.replacement = Some(ConsumerRegistrations { mode, registered });
    }
}

impl EmitterInputRegistration {
    pub(super) fn new(relay: RelayName, relay_unit: Option<Unit>) -> Self {
        Self { relay, relay_unit }
    }
}

impl EmitterUnitSwap {
    pub(super) fn new(owner: NodeUnitRef) -> Self {
        Self {
            owner,
            previous: None,
            replacement: None,
        }
    }

    /// The unit's previous task stopped.
    pub(super) fn record_previous_stopped(&mut self) {
        self.previous = Some(BTreeSet::new());
    }

    pub(super) fn record_withdrawn(&mut self, registration: EmitterInputRegistration) {
        if let Some(withdrawn) = self.previous.as_mut() {
            withdrawn.insert(registration);
        }
    }

    /// The replacement registered a consumer with `services`, before its task starts.
    pub(super) fn record_registered(
        &mut self,
        mode: AckMode,
        services: Arc<RelayBoundaryServices>,
        relay_unit: Option<Unit>,
    ) {
        let replacement = self.replacement.get_or_insert_with(|| EmitterReplacement {
            mode,
            registered: Vec::new(),
            started: false,
        });
        replacement.registered.push(RegisteredEmitterInput {
            services,
            relay_unit,
        });
    }

    pub(super) fn record_started(&mut self, mode: AckMode) {
        let replacement = self.replacement.get_or_insert_with(|| EmitterReplacement {
            mode,
            registered: Vec::new(),
            started: false,
        });
        replacement.started = true;
    }
}

impl ReingestorSwap {
    pub(super) fn new(entity: NodeRef) -> Self {
        Self {
            entity,
            previous: None,
            replacement: None,
        }
    }

    pub(super) fn record_previous_stopped(&mut self) {
        self.previous = Some(BTreeSet::new());
    }

    pub(super) fn record_withdrawn(&mut self, relay: RelayName) {
        if let Some(withdrawn) = self.previous.as_mut() {
            withdrawn.insert(relay);
        }
    }

    pub(super) fn record_replacement(
        &mut self,
        mode: AckMode,
        registered: Vec<Arc<RelayBoundaryServices>>,
    ) {
        self.replacement = Some(ConsumerRegistrations { mode, registered });
    }
}

impl GeneratorUnitSwap {
    pub(super) fn new(owner: NodeUnitRef) -> Self {
        Self {
            owner,
            previous_stopped: false,
            replacement_started: false,
        }
    }

    pub(super) fn record_previous_stopped(&mut self) {
        self.previous_stopped = true;
    }

    pub(super) fn record_replacement_started(&mut self) {
        self.replacement_started = true;
    }
}

impl PlacementSwap {
    pub(super) fn new(entity: NodeRef, units: Option<BTreeSet<Unit>>) -> Self {
        Self {
            entity,
            units,
            aborted: false,
            stopped_state_tasks: Vec::new(),
            installed: Vec::new(),
            started_state_tasks: Vec::new(),
            reissued_kafka_units: Vec::new(),
        }
    }

    pub(super) fn record_aborted(&mut self) {
        self.aborted = true;
    }

    pub(super) fn record_state_task_stopped(&mut self, unit: Unit, withdrawn: bool) {
        self.stopped_state_tasks
            .push(StoppedStateTask { unit, withdrawn });
    }

    pub(super) fn record_installed(&mut self, unit: Unit) {
        self.installed.push(unit);
    }

    pub(super) fn record_state_task_started(
        &mut self,
        unit: Unit,
        registered: Option<Arc<RelayBoundaryServices>>,
    ) {
        self.started_state_tasks
            .push(StartedStateTask { unit, registered });
    }

    pub(super) fn record_reissued_kafka(&mut self, unit: Unit) {
        self.reissued_kafka_units.push(unit);
    }

    /// Whether the swap took apart anything of the previous placement.
    fn replaced_previous(&self) -> bool {
        self.aborted
            || !self.stopped_state_tasks.is_empty()
            || !self.reissued_kafka_units.is_empty()
    }
}

impl ReboundNodeSwap {
    pub(super) fn new(entity: NodeRef, changed_units: BTreeSet<Unit>) -> Self {
        Self {
            stopped_ingestor_units: Vec::new(),
            stopped_owner_units: Vec::new(),
            placement: PlacementSwap::new(entity, Some(changed_units)),
            started_owner_units: Vec::new(),
        }
    }

    pub(super) fn record_ingestor_unit_stopped(&mut self, unit: Unit) {
        self.stopped_ingestor_units.push(unit);
    }

    pub(super) fn record_owner_stopped(&mut self, unit: Unit) {
        self.stopped_owner_units.push(unit);
    }

    pub(super) fn record_owner_started(&mut self, unit: Unit) {
        self.started_owner_units.push(unit);
    }

    pub(super) fn placement(&mut self) -> &mut PlacementSwap {
        &mut self.placement
    }
}

impl RelayRepartitionSwap {
    pub(super) fn new(relay: RelayName) -> Self {
        Self {
            relay,
            stopped_owner_units: Vec::new(),
            redefined: false,
            started_owner_units: Vec::new(),
        }
    }

    pub(super) fn record_owner_stopped(&mut self, unit: Unit) {
        self.stopped_owner_units.push(unit);
    }

    pub(super) fn record_redefined(&mut self) {
        self.redefined = true;
    }

    pub(super) fn record_owner_started(&mut self, unit: Unit) {
        self.started_owner_units.push(unit);
    }
}

/// What every restore step of one failed swap reads: the revision and routing the domain ran
/// before it, and where the restored tasks run.
struct SwapRestore<'a> {
    domain: &'a DomainName,
    previous: &'a ExecutionRevision,
    /// The routing the domain last published, which a failed swap never replaced.
    published: StdArc<DomainRoutingSnapshot>,
    shutdown: watch::Sender<bool>,
    local_node_id: Option<ClusterNodeName>,
}

/// Whether a part of a failed swap's previous execution could not be put back.
#[derive(Default)]
struct RestoreFailures {
    any: bool,
}

impl Runtime {
    /// Puts back what a failed swap of `domain` recorded in `journal`, last step first, so the
    /// domain keeps running the execution it ran before the swap. Returns whether every part came
    /// back; each part that did not is reported where it failed.
    pub(super) async fn restore_entity_swap(
        &self,
        domain: &DomainName,
        journal: EntitySwapJournal,
    ) -> bool {
        let EntitySwapJournal { previous, steps } = journal;
        let Some(previous) = previous else {
            return true;
        };
        let (published, shutdown) = {
            let Some(execution) = self.inner.executions.get(domain) else {
                self.report_restore_failure(
                    domain,
                    Report::new(EntitySwapRestoreError::ExecutionUnavailable),
                );
                return false;
            };
            (
                execution.routing.published_snapshot(),
                execution.shutdown.clone(),
            )
        };
        let dispatcher = self.inner.remote_dispatcher.load_full();
        let restore = SwapRestore {
            domain,
            previous: &previous,
            published,
            shutdown,
            local_node_id: dispatcher
                .as_deref()
                .map(RemoteDispatcher::local_node_id)
                .cloned(),
        };
        let mut failures = RestoreFailures::default();
        let mut materialized_restored = false;
        for step in steps.into_iter().rev() {
            nervix_primitives::task::consume_budget().await;
            if let EntitySwapStep::Rebound(_) | EntitySwapStep::RelayTransition(_) = &step {
                materialized_restored = true;
            }
            Box::pin(self.restore_swap_step(&restore, step, &mut failures)).await;
        }
        // Everything the swap staged for publication goes with it: the routing the domain
        // published before the swap is the one it keeps.
        if let Some(mut execution) = self.inner.executions.get_mut(domain) {
            execution.routing.discard_staged();
        }
        if materialized_restored {
            self.bump_relay_state_epoch(domain);
            self.inner.materialized_state_changed.notify_waiters();
        }
        !failures.any
    }

    async fn restore_swap_step(
        &self,
        restore: &SwapRestore<'_>,
        step: EntitySwapStep,
        failures: &mut RestoreFailures,
    ) {
        match step {
            EntitySwapStep::StateIdentities => self.install_state_identities(restore.previous),
            EntitySwapStep::Ingestor(step) => {
                self.restore_ingestor_units(restore, &step.ingestor, &step.stopped, failures)
                    .await;
            }
            EntitySwapStep::Repartition(step) => {
                Box::pin(self.restore_repartition(restore, step, failures)).await;
            }
            EntitySwapStep::Rebound(step) => {
                Box::pin(self.restore_rebound_node(restore, step, failures)).await;
            }
            EntitySwapStep::RelayTransition(step) => {
                Box::pin(self.restore_placement(restore, step, failures)).await;
            }
            EntitySwapStep::Emitter(step) => {
                Box::pin(self.restore_emitter_unit(restore, step, failures)).await;
            }
            EntitySwapStep::Reingestor(step) => {
                Box::pin(self.restore_reingestor(restore, step, failures)).await;
            }
            EntitySwapStep::Generator(step) => {
                self.restore_generator_unit(restore, step, failures).await;
            }
            EntitySwapStep::Processor(step) => {
                Box::pin(self.restore_processor_unit(restore, step, failures)).await;
            }
            EntitySwapStep::RelayCapacity { relay } => {
                if let Some(plan) = restore.previous.activation.relays.get(&relay) {
                    self.set_relay_capacity(restore.domain, &relay, plan.capacity);
                }
            }
            EntitySwapStep::EmitterFlush { emitter } => {
                Box::pin(self.restore_emitter_flush(restore, emitter, failures)).await;
            }
        }
    }

    fn report_restore_failure(&self, domain: &DomainName, error: Report<EntitySwapRestoreError>) {
        warn!(
            domain = domain.as_str(),
            error = %format_args!("{error:#}"),
            "a failed entity swap could not put back part of the previous execution"
        );
        self.events()
            .report_error(format!("domain '{domain}': {error:#}"));
    }

    fn restore_failed(
        &self,
        restore: &SwapRestore<'_>,
        node: &NodeRef,
        error: Report<EntitySwapRestoreError>,
        failures: &mut RestoreFailures,
    ) {
        failures.any = true;
        self.report_restore_failure(
            restore.domain,
            error.change_context(EntitySwapRestoreError::Entity { node: node.clone() }),
        );
    }

    /// Starts again the units of `ingestor` the swap stopped, with the plans of the revision before
    /// the swap. The swap started no replacement unit, so none runs to stop.
    async fn restore_ingestor_units(
        &self,
        restore: &SwapRestore<'_>,
        ingestor: &IngestorName,
        stopped: &[Unit],
        failures: &mut RestoreFailures,
    ) {
        let node = NodeRef::new(ModelKind::Ingestor, ModelName::from(ingestor));
        for unit in stopped {
            nervix_primitives::task::consume_budget().await;
            let Some(plan) = restore.previous.entrypoints.ingestor(ingestor, *unit) else {
                self.restore_failed(
                    restore,
                    &node,
                    Report::new(EntitySwapRestoreError::MissingPreviousPlan),
                    failures,
                );
                continue;
            };
            if let Err(error) = self.start_ingestor(plan).await {
                self.restore_failed(
                    restore,
                    &node,
                    error.change_context(EntitySwapRestoreError::StartPrevious),
                    failures,
                );
            }
        }
    }

    /// Hands a processor unit's replacement over, if it runs, and starts the unit's previous task
    /// again with the branches it handed over and the input registrations it had.
    async fn restore_processor_unit(
        &self,
        restore: &SwapRestore<'_>,
        step: ProcessorUnitSwap,
        failures: &mut RestoreFailures,
    ) {
        let ProcessorUnitSwap {
            owner,
            previous,
            replacement,
        } = step;
        let mut replacement_handoffs = None;
        if let Some(replacement) = replacement {
            let task = self
                .inner
                .executions
                .get_mut(restore.domain)
                .and_then(|mut execution| execution.node_tasks.remove(&owner));
            if let Some(task) = task {
                match task.handoff().await {
                    Ok(handoffs) => replacement_handoffs = Some(handoffs),
                    Err(error) => self.restore_failed(
                        restore,
                        &owner.node,
                        error.change_context(EntitySwapRestoreError::ReplacementHandoff),
                        failures,
                    ),
                }
            }
            for services in replacement.registered {
                services.remove_local_unit_runtime_consumer(replacement.mode, owner.unit);
            }
        }
        let Some(previous) = previous else {
            return;
        };
        let handoffs = match replacement_handoffs {
            Some(handoffs) => handoffs,
            None => previous.handoffs,
        };
        let Some(plan) = previous.plan else {
            self.restore_failed(
                restore,
                &owner.node,
                Report::new(EntitySwapRestoreError::MissingPreviousPlan),
                failures,
            );
            return;
        };
        let spec = &plan.source.spec;
        let held = restore
            .previous
            .nodes
            .get(&owner.node)
            .is_some_and(|node| node.is_holding_unit(owner.unit));
        let mut boundaries = Vec::with_capacity(spec.input_relays.len());
        for relay in &spec.input_relays {
            let Some(services) = restore.published.relay_services.get(relay) else {
                self.restore_failed(
                    restore,
                    &owner.node,
                    Report::new(EntitySwapRestoreError::MissingRelayBoundary {
                        relay: relay.clone(),
                    }),
                    failures,
                );
                return;
            };
            boundaries.push((relay, services));
        }
        let mut inputs = Vec::with_capacity(boundaries.len());
        for (relay, services) in boundaries {
            let input = if previous.withdrawn.contains(relay) {
                services.add_local_unit_runtime_consumer(spec.mode, owner.unit)
            } else if held {
                services.dormant_unit_fan_in(spec.mode, owner.unit)
            } else {
                services.registered_unit_runtime_fan_in(spec.mode, owner.unit)
            };
            inputs.push((relay.clone(), input));
        }
        let task = spawn_processor_node_runtime_with_handoffs(
            ProcessorRuntimeContext::new(self.clone(), restore.domain.clone()),
            &restore.shutdown,
            plan.template.as_ref().clone(),
            inputs,
            handoffs,
            self.inner.branch_instance_expiration_scan_interval,
        );
        self.install_restored_node_task(restore, owner, task, failures)
            .await;
    }

    async fn install_restored_node_task(
        &self,
        restore: &SwapRestore<'_>,
        owner: NodeUnitRef,
        mut task: ScheduledNodeTask,
        failures: &mut RestoreFailures,
    ) {
        if let Some(mut execution) = self.inner.executions.get_mut(restore.domain) {
            execution.node_tasks.insert(owner, task);
            return;
        }
        task.abort_and_join().await;
        self.restore_failed(
            restore,
            &owner.node,
            Report::new(EntitySwapRestoreError::ExecutionUnavailable),
            failures,
        );
    }

    /// Stops an emitter unit's replacement, if it runs, and starts the unit's previous task again
    /// with the input registrations it had.
    async fn restore_emitter_unit(
        &self,
        restore: &SwapRestore<'_>,
        step: EmitterUnitSwap,
        failures: &mut RestoreFailures,
    ) {
        let EmitterUnitSwap {
            owner,
            previous,
            replacement,
        } = step;
        if let Some(replacement) = replacement {
            if replacement.started
                && !self
                    .stop_emitter_replacement(restore, &owner, failures)
                    .await
            {
                return;
            }
            for registered in replacement.registered {
                match registered.relay_unit {
                    Some(relay_unit) => registered
                        .services
                        .remove_local_unit_runtime_consumer(replacement.mode, relay_unit),
                    None => registered
                        .services
                        .remove_local_runtime_consumer(replacement.mode),
                }
            }
        }
        let Some(withdrawn) = previous else {
            return;
        };
        if let Err(error) = self
            .start_previous_emitter_unit(restore, &owner, &withdrawn)
            .await
        {
            self.restore_failed(restore, &owner.node, error, failures);
        }
    }

    /// Stops the replacement task of an emitter unit. A replacement that does not stop keeps
    /// running, and the unit's previous task cannot come back beside it.
    async fn stop_emitter_replacement(
        &self,
        restore: &SwapRestore<'_>,
        owner: &NodeUnitRef,
        failures: &mut RestoreFailures,
    ) -> bool {
        let task = self
            .inner
            .executions
            .get_mut(restore.domain)
            .and_then(|mut execution| execution.emitter_tasks.remove(owner));
        let Some(task) = task else {
            return true;
        };
        let Err(failure) = task.stop(self.domain_drain_timeout()).await else {
            return true;
        };
        let (error, task) = failure.into_parts();
        if let Some(mut execution) = self.inner.executions.get_mut(restore.domain) {
            execution.emitter_tasks.insert(owner.clone(), task);
        }
        self.restore_failed(
            restore,
            &owner.node,
            error.change_context(EntitySwapRestoreError::StopReplacement),
            failures,
        );
        false
    }

    async fn start_previous_emitter_unit(
        &self,
        restore: &SwapRestore<'_>,
        owner: &NodeUnitRef,
        withdrawn: &BTreeSet<EmitterInputRegistration>,
    ) -> error_stack::Result<(), EntitySwapRestoreError> {
        let name = EmitterName::from(&owner.node.identifier);
        let Some(plan) = restore.previous.emitters.emitter(&name).cloned() else {
            return Err(Report::new(EntitySwapRestoreError::MissingPreviousPlan));
        };
        let deps = self
            .emitter_task_deps(
                ExecutionBuildDeps::from_routing(
                    restore.domain,
                    &restore.published,
                    self.inner.vm_settings,
                ),
                &plan,
            )
            .change_context(EntitySwapRestoreError::StartPrevious)?;
        let mut boundaries = Vec::new();
        for input in &plan.inputs {
            let Some(services) = restore.published.relay_services.get(&input.relay) else {
                return Err(Report::new(EntitySwapRestoreError::MissingRelayBoundary {
                    relay: input.relay.clone(),
                }));
            };
            match plan.input_units(owner.unit, input) {
                EmitterInputUnits::WholeRelay => boundaries.push((input, services, None)),
                EmitterInputUnits::Units(relay_units) => {
                    for relay_unit in relay_units {
                        boundaries.push((input, services, Some(relay_unit)));
                    }
                }
            }
        }
        let mut inputs = Vec::with_capacity(boundaries.len());
        let mut readded = Vec::new();
        for (input, services, relay_unit) in boundaries {
            let registration = EmitterInputRegistration::new(input.relay.clone(), relay_unit);
            let fan_in = if withdrawn.contains(&registration) {
                readded.push((services.clone(), relay_unit));
                match relay_unit {
                    Some(relay_unit) => {
                        services.add_local_unit_runtime_consumer(plan.mode, relay_unit)
                    }
                    None => services.add_local_runtime_consumer(plan.mode),
                }
            } else {
                match relay_unit {
                    Some(relay_unit) => {
                        services.registered_unit_runtime_fan_in(plan.mode, relay_unit)
                    }
                    None => services.registered_unit_runtime_fan_in(plan.mode, Unit::SINGLE),
                }
            };
            inputs.push(EmitterInput {
                relay: input.relay.clone(),
                relay_unit,
                fan_in,
            });
        }
        let spawned = self.spawn_emitter_task(
            EmitterTaskBuildDeps {
                domain: restore.domain,
                shutdown_tx: &restore.shutdown,
                codecs: &restore.published.codecs,
                deps,
            },
            plan.as_ref().clone(),
            owner.unit,
            inputs,
        );
        let task = match spawned {
            Ok(task) => task,
            Err(error) => {
                // A consumer registered without a receiver would hold the relay's attached
                // delivery, so the registrations go with the task that did not start.
                for (services, relay_unit) in readded {
                    match relay_unit {
                        Some(relay_unit) => {
                            services.remove_local_unit_runtime_consumer(plan.mode, relay_unit);
                        }
                        None => services.remove_local_runtime_consumer(plan.mode),
                    }
                }
                return Err(error.change_context(EntitySwapRestoreError::StartPrevious));
            }
        };
        let Some(mut execution) = self.inner.executions.get_mut(restore.domain) else {
            return Err(Report::new(EntitySwapRestoreError::ExecutionUnavailable));
        };
        execution.emitter_tasks.insert(owner.clone(), task);
        Ok(())
    }

    /// Gives the running tasks of `emitter` the flush policy of the revision before the swap.
    async fn restore_emitter_flush(
        &self,
        restore: &SwapRestore<'_>,
        emitter: EmitterName,
        failures: &mut RestoreFailures,
    ) {
        let node = NodeRef::new(ModelKind::Emitter, ModelName::from(&emitter));
        let Some(plan) = restore.previous.emitters.emitter(&emitter) else {
            self.restore_failed(
                restore,
                &node,
                Report::new(EntitySwapRestoreError::MissingPreviousPlan),
                failures,
            );
            return;
        };
        let policy = plan.flush_policy.clone();
        let mut instance_commands = Vec::new();
        if let Some(execution) = self.inner.executions.get(restore.domain) {
            for (owner, task) in &execution.emitter_tasks {
                if owner.node == node {
                    instance_commands.push(task.commands.clone());
                }
            }
        }
        for commands in instance_commands {
            nervix_primitives::task::consume_budget().await;
            if let Err(error) =
                ScheduledEmitterTask::reconfigure_via(&commands, policy.clone()).await
            {
                self.restore_failed(
                    restore,
                    &node,
                    error.change_context(EntitySwapRestoreError::StartPrevious),
                    failures,
                );
            }
        }
    }

    /// Stops a reingestor's replacement, if it runs, and starts its previous tasks and entrypoints
    /// again with the input registrations they had.
    async fn restore_reingestor(
        &self,
        restore: &SwapRestore<'_>,
        step: ReingestorSwap,
        failures: &mut RestoreFailures,
    ) {
        let ReingestorSwap {
            entity,
            previous,
            replacement,
        } = step;
        if let Some(replacement) = replacement {
            let (tasks, entrypoints) = match self.inner.executions.get_mut(restore.domain) {
                Some(mut execution) => (
                    execution
                        .reingestor_tasks
                        .remove(&entity)
                        .unwrap_or_default(),
                    execution
                        .branched_entrypoints
                        .remove(&entity.identifier)
                        .unwrap_or_default(),
                ),
                None => (Vec::new(), Vec::new()),
            };
            for task in tasks {
                nervix_primitives::task::consume_budget().await;
                task.abort();
                task.join_after_shutdown("scheduled node").await;
            }
            for runtime in entrypoints {
                nervix_primitives::task::consume_budget().await;
                runtime.shutdown(self.planned_task_stop()).await;
            }
            for services in replacement.registered {
                services.remove_local_runtime_consumer(replacement.mode);
            }
        }
        let Some(withdrawn) = previous else {
            return;
        };
        if let Err(error) = self
            .start_previous_reingestor(restore, &entity, &withdrawn)
            .await
        {
            self.restore_failed(restore, &entity, error, failures);
        }
    }

    async fn start_previous_reingestor(
        &self,
        restore: &SwapRestore<'_>,
        entity: &NodeRef,
        withdrawn: &BTreeSet<RelayName>,
    ) -> error_stack::Result<(), EntitySwapRestoreError> {
        let reingestor = ReingestorName::from(&entity.identifier);
        let Some(plan) = restore
            .previous
            .entrypoints
            .reingestor(&reingestor)
            .cloned()
        else {
            return Err(Report::new(EntitySwapRestoreError::MissingPreviousPlan));
        };
        let mut inputs = Vec::with_capacity(plan.inputs.len());
        for input in &plan.inputs {
            let Some(services) = restore.published.relay_services.get(&input.relay) else {
                return Err(Report::new(EntitySwapRestoreError::MissingRelayBoundary {
                    relay: input.relay.clone(),
                }));
            };
            let consumer = if withdrawn.contains(&input.relay) {
                ReingestorInputConsumer::Deferred(services.clone())
            } else {
                ReingestorInputConsumer::Registered(
                    services.registered_unit_runtime_fan_in(plan.mode, Unit::SINGLE),
                )
            };
            inputs.push(PlannedReingestorInput {
                plan: plan.clone(),
                input: input.clone(),
                consumer,
            });
        }
        let runtimes = self
            .start_reingestor_runtimes(
                ExecutionBuildDeps::from_routing(
                    restore.domain,
                    &restore.published,
                    self.inner.vm_settings,
                ),
                &restore.shutdown,
                RelayRuntimeHandles {
                    services: &restore.published.relay_services,
                },
                inputs,
            )
            .change_context(EntitySwapRestoreError::StartPrevious)?;
        let Some(mut execution) = self.inner.executions.get_mut(restore.domain) else {
            return Err(Report::new(EntitySwapRestoreError::ExecutionUnavailable));
        };
        execution
            .branched_entrypoints
            .extend(runtimes.branched_entrypoints);
        execution.reingestor_tasks.extend(runtimes.tasks);
        Ok(())
    }

    /// Ends a generator unit's replacement, if it runs, and starts the unit's previous task again.
    async fn restore_generator_unit(
        &self,
        restore: &SwapRestore<'_>,
        step: GeneratorUnitSwap,
        failures: &mut RestoreFailures,
    ) {
        let GeneratorUnitSwap {
            owner,
            previous_stopped,
            replacement_started,
        } = step;
        if replacement_started {
            let task = self
                .inner
                .executions
                .get_mut(restore.domain)
                .and_then(|mut execution| execution.generator_tasks.remove(&owner));
            if let Some(task) = task {
                task.abort();
                task.join_after_shutdown("generator").await;
            }
        }
        if !previous_stopped {
            return;
        }
        if let Err(error) = self.start_previous_generator_unit(restore, &owner) {
            self.restore_failed(restore, &owner.node, error, failures);
        }
    }

    fn start_previous_generator_unit(
        &self,
        restore: &SwapRestore<'_>,
        owner: &NodeUnitRef,
    ) -> error_stack::Result<(), EntitySwapRestoreError> {
        let name = GeneratorName::from(&owner.node.identifier);
        let Some(generator) = restore.previous.resources.generators.get(&name) else {
            return Err(Report::new(EntitySwapRestoreError::MissingPreviousPlan));
        };
        let spec = GeneratorTaskSpec::bind(
            restore.domain,
            generator,
            owner.unit,
            &restore.published.relay_services,
            &restore.published.udfs,
            self.inner.vm_settings,
        )
        .change_context(EntitySwapRestoreError::StartPrevious)?;
        let task = self
            .spawn_generator_task(restore.domain, &restore.shutdown, spec)
            .change_context(EntitySwapRestoreError::StartPrevious)?;
        let Some(mut execution) = self.inner.executions.get_mut(restore.domain) else {
            task.abort();
            return Err(Report::new(EntitySwapRestoreError::ExecutionUnavailable));
        };
        execution.generator_tasks.insert(owner.clone(), task);
        Ok(())
    }

    /// Puts back one rebound node: its owner tasks, its placement and the ingestor units the
    /// rebinding stopped, in the reverse of the order the swap changed them.
    async fn restore_rebound_node(
        &self,
        restore: &SwapRestore<'_>,
        step: ReboundNodeSwap,
        failures: &mut RestoreFailures,
    ) {
        let ReboundNodeSwap {
            stopped_ingestor_units,
            stopped_owner_units,
            placement,
            started_owner_units,
        } = step;
        let entity = placement.entity.clone();
        if entity.kind == ModelKind::Relay {
            let relay = RelayName::from(&entity.identifier);
            self.stop_restored_relay_owners(restore, &relay, &started_owner_units, failures)
                .await;
            Box::pin(self.restore_placement(restore, placement, failures)).await;
            self.start_previous_relay_owners(restore, &relay, &stopped_owner_units, failures);
            return;
        }
        Box::pin(self.restore_placement(restore, placement, failures)).await;
        if entity.kind == ModelKind::Ingestor {
            self.restore_ingestor_units(
                restore,
                &IngestorName::from(&entity.identifier),
                &stopped_ingestor_units,
                failures,
            )
            .await;
        }
    }

    /// Puts one relay back on the services of its previous partition count.
    async fn restore_repartition(
        &self,
        restore: &SwapRestore<'_>,
        step: RelayRepartitionSwap,
        failures: &mut RestoreFailures,
    ) {
        let RelayRepartitionSwap {
            relay,
            stopped_owner_units,
            redefined,
            started_owner_units,
        } = step;
        self.stop_restored_relay_owners(restore, &relay, &started_owner_units, failures)
            .await;
        if redefined && let Some(plan) = restore.previous.activation.relays.get(&relay) {
            // The fan-out keeps its subscribers; it takes back the definition of the previous
            // count, which the published services still read by.
            Box::pin(self.relay_boundary_fanout_with_capacity(
                restore.domain,
                &relay,
                plan.capacity,
                RelaySubscriptionDefinition::new(plan.schema.clone(), plan.branching.clone()),
            ))
            .await;
        }
        self.start_previous_relay_owners(restore, &relay, &stopped_owner_units, failures);
    }

    async fn stop_restored_relay_owners(
        &self,
        restore: &SwapRestore<'_>,
        relay: &RelayName,
        units: &[Unit],
        failures: &mut RestoreFailures,
    ) {
        let node = NodeRef::new(ModelKind::Relay, ModelName::from(relay));
        for unit in units {
            nervix_primitives::task::consume_budget().await;
            let task = self
                .inner
                .executions
                .get_mut(restore.domain)
                .and_then(|mut execution| {
                    execution
                        .relay_owner_tasks
                        .remove(&RelayUnitRef::new(relay.clone(), *unit))
                });
            let Some(task) = task else {
                continue;
            };
            if let Err(error) = task
                .stop(TaskStopWait::UntilStopped {
                    report_after: self.branch_task_stop_timeout(),
                })
                .await
            {
                self.restore_failed(
                    restore,
                    &node,
                    error.change_context(EntitySwapRestoreError::StopReplacement),
                    failures,
                );
            }
        }
    }

    fn start_previous_relay_owners(
        &self,
        restore: &SwapRestore<'_>,
        relay: &RelayName,
        units: &[Unit],
        failures: &mut RestoreFailures,
    ) {
        if units.is_empty() {
            return;
        }
        let node = NodeRef::new(ModelKind::Relay, ModelName::from(relay));
        let (Some(services), Some(plan)) = (
            restore.published.relay_services.get(relay),
            restore.previous.activation.relays.get(relay),
        ) else {
            self.restore_failed(
                restore,
                &node,
                Report::new(EntitySwapRestoreError::MissingRelayBoundary {
                    relay: relay.clone(),
                }),
                failures,
            );
            return;
        };
        for unit in units {
            if !services.declares_unit(*unit) {
                continue;
            }
            let task = match self.spawn_relay_owner_task(
                restore.domain,
                relay,
                *unit,
                services.clone(),
                plan.retention,
            ) {
                Ok(task) => task,
                Err(error) => {
                    self.restore_failed(
                        restore,
                        &node,
                        error.change_context(EntitySwapRestoreError::StartPrevious),
                        failures,
                    );
                    continue;
                }
            };
            if let Some(mut execution) = self.inner.executions.get_mut(restore.domain) {
                execution
                    .relay_owner_tasks
                    .insert(RelayUnitRef::new(relay.clone(), *unit), task);
            }
        }
    }

    /// Puts back the placement-derived runtime of one node: the swap's placement tasks and state
    /// tasks end, and the previous placement is built again, its state tasks taking the units the
    /// swap stopped.
    async fn restore_placement(
        &self,
        restore: &SwapRestore<'_>,
        step: PlacementSwap,
        failures: &mut RestoreFailures,
    ) {
        let relay = RelayName::from(&step.entity.identifier);
        for started in &step.started_state_tasks {
            nervix_primitives::task::consume_budget().await;
            let task = self
                .inner
                .executions
                .get_mut(restore.domain)
                .and_then(|mut execution| {
                    execution
                        .relay_state_tasks
                        .remove(&RelayUnitRef::new(relay.clone(), started.unit))
                });
            if let Some(task) = task
                && let Err(error) = task.stop(self.planned_task_stop()).await
            {
                self.restore_failed(
                    restore,
                    &step.entity,
                    error.change_context(EntitySwapRestoreError::StopReplacement),
                    failures,
                );
            }
            if let Some(services) = started.registered.as_ref() {
                services.remove_local_unit_runtime_consumer(AckMode::Detached, started.unit);
            }
        }
        for unit in &step.installed {
            nervix_primitives::task::consume_budget().await;
            let tasks = self
                .inner
                .executions
                .get_mut(restore.domain)
                .and_then(|mut execution| {
                    execution
                        .placement_tasks
                        .remove(&NodeUnitRef::new(step.entity.clone(), *unit))
                });
            if let Some(tasks) = tasks {
                tasks.abort().await;
            }
        }
        if !step.replaced_previous() {
            return;
        }
        if let Err(error) = self.rebuild_previous_placement(restore, &step) {
            self.restore_failed(restore, &step.entity, error, failures);
        }
    }

    fn rebuild_previous_placement(
        &self,
        restore: &SwapRestore<'_>,
        step: &PlacementSwap,
    ) -> error_stack::Result<(), EntitySwapRestoreError> {
        let Some(local_node_id) = restore.local_node_id.as_ref() else {
            return Ok(());
        };
        let Some(previous_node) = restore.previous.nodes.get(&step.entity) else {
            return Err(Report::new(EntitySwapRestoreError::MissingPreviousPlan));
        };
        let mut placement = self
            .build_scheduled_node_placement(
                restore.domain,
                &restore.shutdown,
                previous_node,
                local_node_id,
                PlacedNodeState::of(previous_node, restore.previous),
                step.units.as_ref(),
            )
            .change_context(EntitySwapRestoreError::BuildPlacement)?;
        for unit in &step.reissued_kafka_units {
            if let Some(originator) = placement.kafka_offset_states.remove(unit) {
                self.reissue_kafka_offset_originator(
                    restore.domain,
                    &step.entity,
                    *unit,
                    originator,
                );
            }
        }
        let relay = RelayName::from(&step.entity.identifier);
        let mut state_tasks = Vec::with_capacity(step.stopped_state_tasks.len());
        for stopped in &step.stopped_state_tasks {
            let Some(state) = placement.materialized_states.remove(&stopped.unit) else {
                return Err(Report::new(
                    EntitySwapRestoreError::MissingMaterializedState,
                ));
            };
            let (Some(services), Some(plan)) = (
                restore.published.relay_services.get(&relay),
                restore.previous.activation.relays.get(&relay),
            ) else {
                return Err(Report::new(EntitySwapRestoreError::MissingRelayBoundary {
                    relay: relay.clone(),
                }));
            };
            let receiver = if stopped.withdrawn {
                services.add_local_unit_runtime_consumer(AckMode::Detached, stopped.unit)
            } else if previous_node.is_holding_unit(stopped.unit) {
                services.dormant_unit_fan_in(AckMode::Detached, stopped.unit)
            } else {
                services.registered_unit_runtime_fan_in(AckMode::Detached, stopped.unit)
            };
            let task = self.spawn_relay_state_task(
                restore.domain,
                RelayStateTaskSpec {
                    relay: relay.clone(),
                    unit: stopped.unit,
                    partitioning: services.partitioning,
                    state,
                    retention: plan.retention,
                    receiver,
                    transition: UnitPartitionTransition::of(previous_node, stopped.unit),
                    parked_capacity: services.owner_capacity(),
                },
            );
            state_tasks.push((RelayUnitRef::new(relay.clone(), stopped.unit), task));
        }
        let Some(mut execution) = self.inner.executions.get_mut(restore.domain) else {
            return Err(Report::new(EntitySwapRestoreError::ExecutionUnavailable));
        };
        execution.placement_tasks.extend(
            placement
                .tasks
                .into_iter()
                .map(|(unit, tasks)| (NodeUnitRef::new(step.entity.clone(), unit), tasks)),
        );
        execution.relay_state_tasks.extend(state_tasks);
        Ok(())
    }
}
