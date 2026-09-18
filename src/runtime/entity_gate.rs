use parking_lot::Mutex;

use super::*;

/// Default deadline for draining one runtime branch during a domain or node transition.
pub const DEFAULT_DOMAIN_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// Includes the grace a branch task receives after its configured drain deadline.
pub const fn branch_task_stop_timeout(domain_drain_timeout: Duration) -> Duration {
    // Saturation is the meaning: a drain timeout configured near `Duration::MAX` already asks to
    // wait for as long as the process runs, and no grace can extend that further.
    domain_drain_timeout.saturating_add(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE)
}

pub(in crate::runtime) const OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL: Duration =
    Duration::from_millis(25);

/// The complete logical scope one coordination operation fenced. A retry must name this exact
/// scope before it can reuse the completed hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EntityGateScope {
    domain: DomainName,
    relays: SortedSet<RelayName>,
    affected_entities: SortedSet<NodeRef>,
    purpose: EntityGatePurpose,
}

impl EntityGateScope {
    fn new(
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
    ) -> Self {
        Self {
            domain: domain.clone(),
            relays: SortedSet::from_unsorted(relays.to_vec()),
            affected_entities: SortedSet::from_unsorted(affected_entities.to_vec()),
            purpose,
        }
    }
}

#[derive(Debug, Clone, Error)]
pub(crate) enum EntityGateOperationError {
    #[error(
        "coordination identity '{coordination}' is already bound to a different entity gate scope"
    )]
    ScopeConflict { coordination: CoordinationIdentity },
    #[error("entity gate operation was already released")]
    Released,
    #[error("the receiver-owned entity gate release task terminated before cleanup completed")]
    ReleaseTaskTerminated,
    #[error("relay dispatch gate fence for domain '{domain}' did not complete before its deadline")]
    RelayFenceDeadline { domain: DomainName },
    #[error("entity gate for domain '{domain}' expired while it was being engaged")]
    EngagementExpired { domain: DomainName },
    #[error("entity gate coordination identity '{coordination}' is not held")]
    NotHeld { coordination: CoordinationIdentity },
    #[error("entity gate coordination identity '{coordination}' does not own the requested scope")]
    ScopeMismatch { coordination: CoordinationIdentity },
    #[error("entity gate coordination identity '{coordination}' has not completed engagement")]
    EngagementIncomplete { coordination: CoordinationIdentity },
    #[error(
        "entity gate coordination identity '{coordination}' belongs to domain '{actual}', not \
         '{requested}'"
    )]
    DomainMismatch {
        coordination: CoordinationIdentity,
        actual: DomainName,
        requested: DomainName,
    },
}

impl EntityGateOperationError {
    /// How a node that could not serve this gate operation reports it to the node that asked.
    ///
    /// A coordination identity this node never held, or holds for another scope or domain, is a
    /// rejection: the asking node addressed a hold that is not here, and retrying against this
    /// node cannot help. Everything else ran against a hold this node does own and lost, so the
    /// caller can only report it.
    pub(crate) fn as_remote_failure(
        &self,
        subject: nervix_interconnect::RemoteOperationSubject,
    ) -> nervix_interconnect::RemoteOperationFailure {
        match self {
            Self::NotHeld { .. }
            | Self::ScopeConflict { .. }
            | Self::ScopeMismatch { .. }
            | Self::DomainMismatch { .. } => {
                nervix_interconnect::RemoteOperationFailure::Rejected { subject }
            }
            Self::EngagementIncomplete { .. } => {
                nervix_interconnect::RemoteOperationFailure::NotReady { subject }
            }
            Self::Released
            | Self::ReleaseTaskTerminated
            | Self::RelayFenceDeadline { .. }
            | Self::EngagementExpired { .. } => {
                nervix_interconnect::RemoteOperationFailure::Failed {
                    subject,
                    reason: self.to_string(),
                }
            }
        }
    }
}

pub(super) struct EntityGateOperation {
    scope: EntityGateScope,
    state: Mutex<EntityGateOperationState>,
    state_changed: Notify,
}

enum EntityGateOperationState {
    Engaging,
    Held(EntityAlterHold),
    Failed(EntityGateOperationError),
    Released,
}

impl EntityGateOperation {
    fn new(scope: EntityGateScope) -> Self {
        Self {
            scope,
            state: Mutex::new(EntityGateOperationState::Engaging),
            state_changed: Notify::new(),
        }
    }

    fn scope(&self) -> &EntityGateScope {
        &self.scope
    }

    fn scope_matches(&self, scope: &EntityGateScope) -> bool {
        &self.scope == scope
    }

    fn complete(&self, hold: EntityAlterHold) {
        *self.state.lock() = EntityGateOperationState::Held(hold);
        self.state_changed.notify_waiters();
    }

    fn fail(&self, error: EntityGateOperationError) {
        *self.state.lock() = EntityGateOperationState::Failed(error);
        self.state_changed.notify_waiters();
    }

    async fn wait_until_held(&self) -> Result<(), Report<EntityGateOperationError>> {
        loop {
            let changed = self.state_changed.notified();
            let result = match &*self.state.lock() {
                EntityGateOperationState::Engaging => None,
                EntityGateOperationState::Held(_) => Some(Ok(())),
                EntityGateOperationState::Failed(error) => Some(Err(Report::new(error.clone()))),
                EntityGateOperationState::Released => {
                    Some(Err(Report::new(EntityGateOperationError::Released)))
                }
            };
            if let Some(result) = result {
                return result;
            }
            changed.await;
        }
    }

    async fn take_hold(&self) -> Option<EntityAlterHold> {
        loop {
            let changed = self.state_changed.notified();
            let ready = {
                let mut state = self.state.lock();
                let current = std::mem::replace(&mut *state, EntityGateOperationState::Released);
                match current {
                    EntityGateOperationState::Engaging => {
                        *state = EntityGateOperationState::Engaging;
                        None
                    }
                    EntityGateOperationState::Held(hold) => Some(Some(hold)),
                    EntityGateOperationState::Failed(error) => {
                        *state = EntityGateOperationState::Failed(error);
                        Some(None)
                    }
                    EntityGateOperationState::Released => Some(None),
                }
            };
            if let Some(hold) = ready {
                self.state_changed.notify_waiters();
                return hold;
            }
            changed.await;
        }
    }

    fn is_held(&self) -> bool {
        matches!(&*self.state.lock(), EntityGateOperationState::Held(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmitterPublishingDrainState {
    AwaitingConfirmation,
    RetryingInfrastructure,
    RetryingIcebergCommit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EmitterPublishingDrainStatus {
    pub(crate) emitter: EmitterName,
    pub(crate) state: EmitterPublishingDrainState,
    pub(crate) pending_messages: usize,
    pub(crate) retry_backoff: Option<Duration>,
    pub(crate) retry_wait: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DomainDrainStatus {
    pub(crate) active_ingestors: usize,
    pub(crate) active_generators: usize,
    pub(crate) outstanding_acks: usize,
    pub(crate) buffered_emitter_messages: usize,
    pub(crate) emitter_publishing: Vec<EmitterPublishingDrainStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EntityDrainStatus {
    pub(crate) buffered_relay_batches: usize,
    pub(crate) node_work_items: usize,
    pub(crate) outstanding_acks: usize,
    pub(crate) emitter_publishing: Vec<EmitterPublishingDrainStatus>,
}

impl EntityDrainStatus {
    #[cfg(test)]
    pub(in crate::runtime) fn is_drained(&self) -> bool {
        self.buffered_relay_batches == 0 && self.node_work_items == 0 && self.outstanding_acks == 0
    }
}

pub(in crate::runtime) struct EntityGateHold {
    pub(super) gates: Vec<RelayDispatchGateLease>,
    pub(super) branch_gates: Vec<BranchRelayDispatchGateLease>,
}

#[derive(Clone, Copy)]
pub(crate) struct EntityGateLease<'a> {
    pub(crate) deadline: Instant,
    pub(crate) reason: &'a str,
}

pub(super) struct EntityAlterHold {
    pub(super) coordination: CoordinationIdentity,
    pub(super) gates: EntityGateHold,
    pub(super) affected_entities: Vec<NodeRef>,
    pub(super) purpose: EntityGatePurpose,
    /// The quiesce this hold engaged, holding the control it engaged rather than a name to look
    /// up again. An ingestor that is dropped and rebuilt gets a fresh control with zero counts,
    /// so releasing by name could decrement a control that was never engaged.
    pub(super) quiesced_ingestors: Vec<QuiescedIngestorHold>,
}

pub(super) struct QuiescedIngestorHold {
    pub(super) ingestor: IngestorName,
    pub(super) cause: IngestorQuiesceCause,
    pub(super) control: Arc<IngestorQuiesceControl>,
}

#[derive(Debug, Default)]
pub(super) struct NodeQuiesceCounters {
    pub(super) mailbox_and_in_flight: AtomicUsize,
    pub(super) collected_inputs: AtomicUsize,
    pub(super) pending_materialized: AtomicUsize,
    pub(super) output_buffers: AtomicUsize,
    pub(super) force_flushes: AtomicUsize,
}

impl NodeQuiesceCounters {
    pub(super) fn outstanding_work(&self) -> usize {
        [
            self.mailbox_and_in_flight.load(Ordering::Acquire),
            self.collected_inputs.load(Ordering::Acquire),
            self.pending_materialized.load(Ordering::Acquire),
            self.output_buffers.load(Ordering::Acquire),
            self.force_flushes.load(Ordering::Acquire),
        ]
        .into_iter()
        .try_fold(0_usize, usize::checked_add)
        .assured("every count totals work items this node already holds in memory")
    }

    /// The admitted work this node holds, apart from messages parked on `REQUIRED WAIT` and
    /// outstanding force-flush obligations, which a local drain weighs on their own.
    pub(super) fn admitted_work(&self) -> usize {
        [
            self.mailbox_and_in_flight.load(Ordering::Acquire),
            self.collected_inputs.load(Ordering::Acquire),
            self.output_buffers.load(Ordering::Acquire),
        ]
        .into_iter()
        .try_fold(0_usize, usize::checked_add)
        .assured("every count totals work items this node already holds in memory")
    }

    pub(super) fn outstanding_work_for(&self, purpose: EntityGatePurpose) -> usize {
        let outstanding = self.outstanding_work();
        if purpose == EntityGatePurpose::OwnershipHandoff {
            // The counters are read one at a time, so a materialized wait resolved between the
            // two loads can leave the subtrahend above the total. An ownership handoff that
            // observes that raced pair has no non-materialized work left to wait for.
            outstanding.saturating_sub(self.pending_materialized.load(Ordering::Acquire))
        } else {
            outstanding
        }
    }
}

pub(super) struct NodeQuiesceWorkGuard {
    pub(super) counters: Arc<NodeQuiesceCounters>,
    pub(super) required_materialized_wait: bool,
}

impl NodeQuiesceWorkGuard {
    pub(super) fn begin(counters: Arc<NodeQuiesceCounters>) -> Self {
        counters
            .mailbox_and_in_flight
            .fetch_add(1, Ordering::AcqRel);
        Self {
            counters,
            required_materialized_wait: false,
        }
    }

    pub(super) fn park_for_required_materialized_state(&mut self) {
        if self.required_materialized_wait {
            return;
        }
        self.counters
            .pending_materialized
            .fetch_add(1, Ordering::AcqRel);
        self.counters
            .mailbox_and_in_flight
            .fetch_sub(1, Ordering::AcqRel);
        self.required_materialized_wait = true;
    }

    pub(super) fn resume_from_required_materialized_state(&mut self) {
        if !self.required_materialized_wait {
            return;
        }
        self.counters
            .mailbox_and_in_flight
            .fetch_add(1, Ordering::AcqRel);
        self.counters
            .pending_materialized
            .fetch_sub(1, Ordering::AcqRel);
        self.required_materialized_wait = false;
    }
}

impl Drop for NodeQuiesceWorkGuard {
    fn drop(&mut self) {
        let counter = if self.required_materialized_wait {
            &self.counters.pending_materialized
        } else {
            &self.counters.mailbox_and_in_flight
        };
        counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Publishes the depth of one task's output buffers into its node's quiesce accounting.
///
/// A batch that a node has accepted but not yet released to its destination is work that node
/// holds in memory, so an entity gate or an ownership handoff has to be able to see it. The gauge
/// owns the count it contributed, so dropping the task that owns the buffers withdraws exactly
/// that contribution and nothing else.
pub(super) struct OutputBufferQuiesceGauge {
    counters: Arc<NodeQuiesceCounters>,
    output_buffers: usize,
}

impl OutputBufferQuiesceGauge {
    pub(super) fn new(counters: Arc<NodeQuiesceCounters>) -> Self {
        Self {
            counters,
            output_buffers: 0,
        }
    }

    pub(super) fn counters(&self) -> Arc<NodeQuiesceCounters> {
        self.counters.clone()
    }

    pub(super) fn add_batch(&mut self) {
        self.output_buffers = self
            .output_buffers
            .checked_add(1)
            .assured("the count cannot exceed the batches this task holds in memory");
        self.counters.output_buffers.fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn remove_batches(&mut self, count: usize) {
        self.output_buffers = self
            .output_buffers
            .checked_sub(count)
            .verified("only batches counted when they entered this buffer can be removed");
        self.counters
            .output_buffers
            .fetch_sub(count, Ordering::AcqRel);
    }
}

impl Drop for OutputBufferQuiesceGauge {
    fn drop(&mut self) {
        self.counters
            .output_buffers
            .fetch_sub(self.output_buffers, Ordering::AcqRel);
    }
}

pub(super) struct BranchQuiesceGauges {
    pub(super) counters: Arc<NodeQuiesceCounters>,
    pub(super) collected_inputs: usize,
    pub(super) pending_materialized: usize,
    pub(super) output_buffers: usize,
}

/// The three depths a processor contributes to its node's quiesce accounting, read together so
/// one observation reports a single consistent view of the processor.
#[derive(Default)]
pub(super) struct BranchQuiesceDepths {
    pub(super) collected_inputs: usize,
    pub(super) pending_materialized: usize,
    pub(super) output_buffers: usize,
}

impl BranchQuiesceGauges {
    pub(super) fn new(counters: Arc<NodeQuiesceCounters>) -> Self {
        Self {
            counters,
            collected_inputs: 0,
            pending_materialized: 0,
            output_buffers: 0,
        }
    }

    pub(super) fn observe(&mut self, branch: &BranchRuntime, processor: &ModelName) {
        let depths = match branch.processors.get(processor) {
            Some(processor) => BranchQuiesceDepths {
                collected_inputs: processor
                    .input_collectors
                    .values()
                    .map(|collector| collector.pending_len())
                    .sum(),
                pending_materialized: processor.pending_materialized.len(),
                output_buffers: processor
                    .operation
                    .output_routes()
                    .routes
                    .iter()
                    .map(|output| output.pending.len())
                    .sum(),
            },
            None => BranchQuiesceDepths::default(),
        };
        Self::replace_gauge(
            &self.counters.collected_inputs,
            &mut self.collected_inputs,
            depths.collected_inputs,
        );
        Self::replace_gauge(
            &self.counters.pending_materialized,
            &mut self.pending_materialized,
            depths.pending_materialized,
        );
        Self::replace_gauge(
            &self.counters.output_buffers,
            &mut self.output_buffers,
            depths.output_buffers,
        );
    }

    pub(super) fn replace_gauge(counter: &AtomicUsize, current: &mut usize, next: usize) {
        if next > *current {
            counter.fetch_add(next - *current, Ordering::AcqRel);
        } else if next < *current {
            counter.fetch_sub(*current - next, Ordering::AcqRel);
        }
        *current = next;
    }
}

impl Drop for BranchQuiesceGauges {
    fn drop(&mut self) {
        self.counters
            .collected_inputs
            .fetch_sub(self.collected_inputs, Ordering::AcqRel);
        self.counters
            .pending_materialized
            .fetch_sub(self.pending_materialized, Ordering::AcqRel);
        self.counters
            .output_buffers
            .fetch_sub(self.output_buffers, Ordering::AcqRel);
    }
}

impl EntityGateHold {
    pub(super) async fn wait_quiescent(&mut self) -> bool {
        for gate in &mut self.gates {
            tokio::task::consume_budget().await;
            if !gate.wait_quiescent().await {
                return false;
            }
        }
        true
    }

    pub(in crate::runtime) fn release(mut self) {
        self.release_all();
    }

    pub(super) fn release_all(&mut self) {
        self.gates.clear();
        self.branch_gates.clear();
    }
}

impl Drop for EntityGateHold {
    fn drop(&mut self) {
        self.release_all();
    }
}

pub(super) struct DomainActivityGuard {
    pub(super) counter: Arc<AtomicUsize>,
    pub(super) active: bool,
}

impl DomainActivityGuard {
    pub(super) fn new(counter: Arc<AtomicUsize>) -> Self {
        Self {
            counter,
            active: false,
        }
    }

    pub(super) fn set_active(&mut self, active: bool) {
        if self.active == active {
            return;
        }
        if active {
            self.counter.fetch_add(1, Ordering::AcqRel);
        } else {
            self.counter.fetch_sub(1, Ordering::AcqRel);
        }
        self.active = active;
    }
}

impl Drop for DomainActivityGuard {
    fn drop(&mut self) {
        self.set_active(false);
    }
}

#[derive(Debug, Clone)]
pub(super) struct ActiveDomainAlter;

pub(crate) struct DomainAlterGuard {
    pub(super) domain: DomainName,
    pub(super) active_domain_alters: Arc<DashMap<DomainName, ActiveDomainAlter, RandomState>>,
}

impl Drop for DomainAlterGuard {
    fn drop(&mut self) {
        self.active_domain_alters.remove(&self.domain);
    }
}

impl Runtime {
    pub(crate) fn entity_pause_relays(
        &self,
        domain: &DomainName,
        affected_entities: &[NodeRef],
    ) -> Vec<RelayName> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Vec::new();
        };
        crate::registry::entity_pause_relays_for_schedule(&execution.schedule, affected_entities)
    }

    pub(in crate::runtime) fn engage_entity_gates(
        &self,
        domain: &DomainName,
        relays: &[RelayName],
        deadline: Instant,
        reason: &str,
    ) -> EntityGateHold {
        let mut gates = Vec::with_capacity(relays.len());
        for relay in relays {
            let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone());
            let Some(fanout) = self
                .inner
                .relay_boundary_fanouts
                .get(&key)
                .map(|fanout| fanout.clone())
            else {
                continue;
            };
            let gate = fanout.dispatch_gate();
            gates.push(RelayDispatchGateLease::engage(gate, deadline, reason));
        }
        EntityGateHold {
            gates,
            branch_gates: Vec::new(),
        }
    }

    async fn engage_wasm_state_reset_gates(
        &self,
        domain: &DomainName,
        relays: &[RelayName],
        scope: WasmStateResetScope,
        deadline: Instant,
        reason: &str,
    ) -> Option<EntityGateHold> {
        let mut branch_gates = Vec::with_capacity(relays.len());
        for relay in relays {
            tokio::task::consume_budget().await;
            let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone());
            let Some(fanout) = self
                .inner
                .relay_boundary_fanouts
                .get(&key)
                .map(|fanout| fanout.clone())
            else {
                continue;
            };
            let gate = fanout
                .engage_branch_dispatch_gate(scope, deadline, reason)
                .await?;
            branch_gates.push(gate);
        }
        Some(EntityGateHold {
            gates: Vec::new(),
            branch_gates,
        })
    }

    pub(crate) async fn engage_entity_gate_operation(
        &self,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
        lease: EntityGateLease<'_>,
    ) -> Result<(), Report<EntityGateOperationError>> {
        let EntityGateLease { deadline, reason } = lease;
        let scope = EntityGateScope::new(domain, relays, affected_entities, purpose);
        let operation = match self.inner.entity_gate_holds.entry(coordination.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let operation = entry.get().clone();
                if !operation.scope_matches(&scope) {
                    return Err(Report::new(EntityGateOperationError::ScopeConflict {
                        coordination: coordination.clone(),
                    }));
                }
                operation
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let operation = Arc::new(EntityGateOperation::new(scope.clone()));
                entry.insert(operation.clone());
                let runtime = self.clone();
                let coordination = coordination.clone();
                let reason = reason.to_string();
                let engagement = operation.clone();
                drop(tokio::spawn(async move {
                    runtime
                        .complete_entity_gate_engagement(
                            coordination,
                            scope,
                            deadline,
                            reason,
                            engagement,
                        )
                        .await;
                }));
                operation
            }
        };
        operation.wait_until_held().await
    }

    async fn complete_entity_gate_engagement(
        self,
        coordination: CoordinationIdentity,
        scope: EntityGateScope,
        deadline: Instant,
        reason: String,
        operation: Arc<EntityGateOperation>,
    ) {
        let domain = &scope.domain;
        let purpose = scope.purpose;
        let mut gates = match purpose {
            EntityGatePurpose::WasmStateReset(reset_scope) => {
                let Some(gates) = self
                    .engage_wasm_state_reset_gates(
                        domain,
                        &scope.relays,
                        reset_scope,
                        deadline,
                        &reason,
                    )
                    .await
                else {
                    let failure = EntityGateOperationError::RelayFenceDeadline {
                        domain: domain.clone(),
                    };
                    operation.fail(failure);
                    self.inner
                        .entity_gate_holds
                        .remove_if(&coordination, |_, current| Arc::ptr_eq(current, &operation));
                    return;
                };
                gates
            }
            EntityGatePurpose::ModelAlteration | EntityGatePurpose::OwnershipHandoff => {
                self.engage_entity_gates(domain, &scope.relays, deadline, &reason)
            }
        };
        if !gates.wait_quiescent().await {
            gates.release();
            let failure = EntityGateOperationError::RelayFenceDeadline {
                domain: domain.clone(),
            };
            operation.fail(failure);
            self.inner
                .entity_gate_holds
                .remove_if(&coordination, |_, current| Arc::ptr_eq(current, &operation));
            return;
        }
        let ingestors = scope
            .affected_entities
            .iter()
            .filter(|entity| entity.kind == ModelKind::Ingestor)
            .map(|entity| entity.identifier.clone())
            .collect::<Vec<_>>();
        let quiesce_cause = match purpose {
            EntityGatePurpose::ModelAlteration => IngestorQuiesceCause::EntityHold,
            EntityGatePurpose::OwnershipHandoff => IngestorQuiesceCause::OwnershipHandoff,
            EntityGatePurpose::WasmStateReset(_) => IngestorQuiesceCause::EntityHold,
        };
        let mut quiesced_ingestors = Vec::new();
        for ingestor in &ingestors {
            tokio::task::consume_budget().await;
            let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
            if !self.inner.ingestors.contains_key(&key) {
                continue;
            }
            if let Some(control) =
                self.engage_ingestor_quiesce(domain, &IngestorName::from(ingestor), quiesce_cause)
            {
                quiesced_ingestors.push(QuiescedIngestorHold {
                    ingestor: IngestorName::from(ingestor),
                    cause: quiesce_cause,
                    control,
                });
            }
        }
        if purpose == EntityGatePurpose::OwnershipHandoff {
            for entity in &scope.affected_entities {
                let key =
                    DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone());
                self.inner
                    .frozen_ownership_handoff_entities
                    .entry(key)
                    .or_default()
                    .insert(coordination.clone());
            }
            self.inner.ownership_handoff_freeze_changed.notify_waiters();
        }
        let hold = EntityAlterHold {
            coordination: coordination.clone(),
            gates,
            affected_entities: scope.affected_entities.to_vec(),
            purpose,
            quiesced_ingestors,
        };
        if !matches!(purpose, EntityGatePurpose::WasmStateReset(_)) {
            self.force_flush_domain(domain);
        }
        if Instant::now() >= deadline {
            Self::release_entity_alter_hold(
                &self.inner.ingestors,
                &self.inner.ingestor_quiescence,
                &self.inner.frozen_ownership_handoff_entities,
                &self.inner.ownership_handoff_freeze_changed,
                domain,
                hold,
            )
            .await;
            let failure = EntityGateOperationError::EngagementExpired {
                domain: domain.clone(),
            };
            operation.fail(failure);
            self.inner
                .entity_gate_holds
                .remove_if(&coordination, |_, current| Arc::ptr_eq(current, &operation));
            return;
        }
        operation.complete(hold);
        let entity_gate_holds = self.inner.entity_gate_holds.clone();
        let ingestors = self.inner.ingestors.clone();
        let ingestor_quiescence = self.inner.ingestor_quiescence.clone();
        let frozen_ownership_handoff_entities =
            self.inner.frozen_ownership_handoff_entities.clone();
        let ownership_handoff_freeze_changed = self.inner.ownership_handoff_freeze_changed.clone();
        let expiring_operation = operation.clone();
        drop(tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            debug!(
                domain = expiring_operation.scope().domain.as_str(),
                %coordination,
                "entity gate lease reached its deadline"
            );
            Self::release_entity_gate_operation_from_state(
                &entity_gate_holds,
                &ingestors,
                &ingestor_quiescence,
                &frozen_ownership_handoff_entities,
                &ownership_handoff_freeze_changed,
                &coordination,
                &expiring_operation,
            )
            .await;
        }));
    }

    pub(crate) fn entity_gate_operation_drain_status(
        &self,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
    ) -> Result<EntityDrainStatus, Report<EntityGateOperationError>> {
        let requested_scope = EntityGateScope::new(domain, relays, affected_entities, purpose);
        let Some(operation) = self.inner.entity_gate_holds.get(coordination) else {
            return Err(Report::new(EntityGateOperationError::NotHeld {
                coordination: coordination.clone(),
            }));
        };
        if !operation.scope_matches(&requested_scope) {
            return Err(Report::new(EntityGateOperationError::ScopeMismatch {
                coordination: coordination.clone(),
            }));
        }
        if !operation.is_held() {
            return Err(Report::new(
                EntityGateOperationError::EngagementIncomplete {
                    coordination: coordination.clone(),
                },
            ));
        }
        let scope = operation.scope();
        Ok(self.entity_drain_status(
            &scope.domain,
            &scope.relays,
            &scope.affected_entities,
            scope.purpose,
        ))
    }

    pub(crate) fn entity_gate_operation_owns_entity(
        &self,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        entity: &NodeRef,
        purpose: EntityGatePurpose,
    ) -> bool {
        let Some(operation) = self.inner.entity_gate_holds.get(coordination) else {
            return false;
        };
        let scope = operation.scope();
        operation.is_held()
            && &scope.domain == domain
            && scope.purpose == purpose
            && scope.affected_entities.binary_search(entity).is_ok()
    }

    pub(crate) async fn release_entity_gate_operation(
        &self,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
    ) -> Result<(), Report<EntityGateOperationError>> {
        let Some(operation) = self
            .inner
            .entity_gate_holds
            .get(coordination)
            .map(|entry| entry.value().clone())
        else {
            return Ok(());
        };
        if &operation.scope().domain != domain {
            return Err(Report::new(EntityGateOperationError::DomainMismatch {
                coordination: coordination.clone(),
                actual: operation.scope().domain.clone(),
                requested: domain.clone(),
            }));
        }
        let entity_gate_holds = self.inner.entity_gate_holds.clone();
        let ingestors = self.inner.ingestors.clone();
        let ingestor_quiescence = self.inner.ingestor_quiescence.clone();
        let frozen_ownership_handoff_entities =
            self.inner.frozen_ownership_handoff_entities.clone();
        let ownership_handoff_freeze_changed = self.inner.ownership_handoff_freeze_changed.clone();
        let coordination = coordination.clone();
        let release = tokio::spawn(async move {
            Self::release_entity_gate_operation_from_state(
                &entity_gate_holds,
                &ingestors,
                &ingestor_quiescence,
                &frozen_ownership_handoff_entities,
                &ownership_handoff_freeze_changed,
                &coordination,
                &operation,
            )
            .await;
        });
        release
            .await
            .map_err(|_| Report::new(EntityGateOperationError::ReleaseTaskTerminated))?;
        Ok(())
    }

    async fn release_entity_gate_operation_from_state(
        entity_gate_holds: &DashMap<CoordinationIdentity, Arc<EntityGateOperation>, RandomState>,
        ingestors: &DashMap<DomainNodeRef, IngestorRuntime, RandomState>,
        ingestor_quiescence: &DashMap<DomainNodeRef, Arc<IngestorQuiesceControl>, RandomState>,
        frozen_ownership_handoff_entities: &DashMap<
            DomainNodeRef,
            BTreeSet<CoordinationIdentity>,
            RandomState,
        >,
        ownership_handoff_freeze_changed: &Notify,
        coordination: &CoordinationIdentity,
        expected_operation: &Arc<EntityGateOperation>,
    ) {
        // The pointer comparison binds this removal to the exact lease instance captured by its
        // deadline task. A delayed deadline cannot take a replacement out of the map.
        let removed = entity_gate_holds.remove_if(coordination, |_, current| {
            Arc::ptr_eq(current, expected_operation)
        });
        let Some((_, operation)) = removed else {
            return;
        };
        let Some(hold) = operation.take_hold().await else {
            return;
        };
        Self::release_entity_alter_hold(
            ingestors,
            ingestor_quiescence,
            frozen_ownership_handoff_entities,
            ownership_handoff_freeze_changed,
            &operation.scope().domain,
            hold,
        )
        .await;
    }

    async fn release_entity_alter_hold(
        ingestors: &DashMap<DomainNodeRef, IngestorRuntime, RandomState>,
        ingestor_quiescence: &DashMap<DomainNodeRef, Arc<IngestorQuiesceControl>, RandomState>,
        frozen_ownership_handoff_entities: &DashMap<
            DomainNodeRef,
            BTreeSet<CoordinationIdentity>,
            RandomState,
        >,
        ownership_handoff_freeze_changed: &Notify,
        domain: &DomainName,
        hold: EntityAlterHold,
    ) {
        if hold.purpose == EntityGatePurpose::OwnershipHandoff {
            for entity in &hold.affected_entities {
                let key =
                    DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone());
                if let dashmap::mapref::entry::Entry::Occupied(mut entry) =
                    frozen_ownership_handoff_entities.entry(key)
                {
                    entry.get_mut().remove(&hold.coordination);
                    if entry.get().is_empty() {
                        entry.remove();
                    }
                }
            }
            ownership_handoff_freeze_changed.notify_waiters();
        }
        for quiesced in &hold.quiesced_ingestors {
            tokio::task::consume_budget().await;
            quiesced.control.release(quiesced.cause);
            info!(
                domain = domain.as_str(),
                ingestor = quiesced.ingestor.as_str(),
                cause = quiesced.cause.as_str(),
                "ingestor left quiesce"
            );
            let key = DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Ingestor,
                quiesced.ingestor.clone(),
            );
            if !ingestors.contains_key(&key)
                && let Some((_, control)) = ingestor_quiescence.remove(&key)
            {
                control.terminate();
            }
        }
        hold.gates.release();
    }

    #[cfg(test)]
    pub(crate) fn entity_gate_operation_is_held(
        &self,
        coordination: &CoordinationIdentity,
    ) -> bool {
        self.inner
            .entity_gate_holds
            .get(coordination)
            .is_some_and(|operation| operation.is_held())
    }

    pub(crate) fn entity_drain_status(
        &self,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
    ) -> EntityDrainStatus {
        let buffered_relay_batches = relays
            .iter()
            .filter_map(|relay| {
                self.inner
                    .relay_boundary_fanouts
                    .get(&DomainNodeRef::node_in(
                        domain.clone(),
                        ModelKind::Relay,
                        relay.clone(),
                    ))
                    .map(|fanout| fanout.outstanding_work_len())
            })
            .sum();
        let node_work_items = affected_entities
            .iter()
            .map(|entity| {
                let quiesce_work = match self
                    .inner
                    .node_quiesce_counters
                    .get(&entity.in_domain(domain))
                {
                    Some(counters) => counters.outstanding_work_for(purpose),
                    None => 0,
                };
                let emitter_work = if entity.kind == ModelKind::Emitter {
                    match self.inner.emitter_buffers.get(&entity.in_domain(domain)) {
                        Some(buffered) => buffered.load(Ordering::Acquire),
                        None => 0,
                    }
                } else {
                    0
                };
                quiesce_work
                    .checked_add(emitter_work)
                    .assured("both counts total work items this node already holds in memory")
            })
            .sum();
        let mut outstanding_acks = 0;
        for entity in affected_entities {
            if entity.kind != ModelKind::Ingestor {
                continue;
            }
            let Some(tracker) = self
                .inner
                .in_flight_by_ingestor
                .get(&entity.in_domain(domain))
            else {
                continue;
            };
            outstanding_acks += if purpose == EntityGatePurpose::OwnershipHandoff {
                tracker.outstanding_for_ownership_handoff()
            } else {
                tracker.outstanding()
            };
        }
        let mut emitter_publishing = affected_entities
            .iter()
            .filter(|entity| entity.kind == ModelKind::Emitter)
            .filter_map(|entity| self.emitter_publishing_drain_status(&entity.in_domain(domain)))
            .collect::<Vec<_>>();
        emitter_publishing.sort_by(|left, right| left.emitter.cmp(&right.emitter));
        EntityDrainStatus {
            buffered_relay_batches,
            node_work_items,
            outstanding_acks,
            emitter_publishing,
        }
    }

    pub(super) fn emitter_publishing_drain_status(
        &self,
        key: &DomainNodeRef,
    ) -> Option<EmitterPublishingDrainStatus> {
        let pending_messages = match self.inner.emitter_buffers.get(key) {
            Some(buffered) => buffered.load(Ordering::Acquire),
            None => 0,
        };
        let awaiting_confirmation = self
            .inner
            .emitter_confirmation_waits
            .get(key)
            .is_some_and(|waits| waits.load(Ordering::Acquire) > 0);
        if awaiting_confirmation {
            return Some(EmitterPublishingDrainStatus {
                emitter: EmitterName::from(key.identifier()),
                state: EmitterPublishingDrainState::AwaitingConfirmation,
                pending_messages,
                retry_backoff: None,
                retry_wait: None,
            });
        }
        let retry = self.inner.emitter_retry_statuses.get(key)?;
        let state = match retry.kind {
            EmitterRetryKind::Infrastructure => EmitterPublishingDrainState::RetryingInfrastructure,
            EmitterRetryKind::IcebergCommit => EmitterPublishingDrainState::RetryingIcebergCommit,
        };
        Some(EmitterPublishingDrainStatus {
            emitter: EmitterName::from(key.identifier()),
            state,
            pending_messages,
            retry_backoff: Some(retry.reconnect.backoff),
            retry_wait: Some(
                retry
                    .reconnect
                    .retry_at
                    .saturating_duration_since(Instant::now()),
            ),
        })
    }

    pub(super) fn node_quiesce_counters(
        &self,
        domain: &DomainName,
        node: NodeRef,
    ) -> Arc<NodeQuiesceCounters> {
        self.inner
            .node_quiesce_counters
            .entry(DomainNodeRef::new(domain.clone(), node))
            .or_insert_with(|| Arc::new(NodeQuiesceCounters::default()))
            .clone()
    }

    pub(crate) fn try_begin_domain_alter(&self, domain: &DomainName) -> Option<DomainAlterGuard> {
        match self.inner.active_domain_alters.entry(domain.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => None,
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(ActiveDomainAlter);
                Some(DomainAlterGuard {
                    domain: domain.clone(),
                    active_domain_alters: self.inner.active_domain_alters.clone(),
                })
            }
        }
    }

    pub(crate) fn domain_alter_is_active(&self, domain: &DomainName) -> bool {
        self.inner.active_domain_alters.contains_key(domain)
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_entity_gate_if_armed(&self, domain: &DomainName) {
        self.inner
            .fault_injection
            .pause_entity_gate_if_armed(domain)
            .await;
    }

    pub(crate) fn domain_drain_status(&self, domain: &DomainName) -> DomainDrainStatus {
        let active_ingestors = self
            .inner
            .ingestors
            .iter()
            .filter(|entry| {
                &entry.key().domain == domain
                    && self
                        .inner
                        .ingestor_quiescence
                        .get(entry.key())
                        .is_none_or(|control| !control.is_quiesced())
            })
            .count();
        let active_generators = match self.inner.generator_activity_by_domain.get(domain) {
            Some(counter) => counter.load(Ordering::Acquire),
            None => 0,
        };
        let buffered_emitter_messages = self
            .inner
            .emitter_buffers
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.value().load(Ordering::Acquire))
            .sum();
        let mut publishing_keys = self
            .inner
            .emitter_confirmation_waits
            .iter()
            .filter(|entry| {
                &entry.key().domain == domain && entry.value().load(Ordering::Acquire) > 0
            })
            .map(|entry| entry.key().clone())
            .collect::<HashSet<_>>();
        publishing_keys.extend(
            self.inner
                .emitter_retry_statuses
                .iter()
                .filter(|entry| &entry.key().domain == domain)
                .map(|entry| entry.key().clone()),
        );
        let mut emitter_publishing = Vec::new();
        for key in publishing_keys {
            let pending_messages = match self.inner.emitter_buffers.get(&key) {
                Some(buffered) => buffered.load(Ordering::Acquire),
                None => 0,
            };
            let awaiting_confirmation = self
                .inner
                .emitter_confirmation_waits
                .get(&key)
                .is_some_and(|waits| waits.load(Ordering::Acquire) > 0);
            if awaiting_confirmation {
                emitter_publishing.push(EmitterPublishingDrainStatus {
                    emitter: EmitterName::from(key.identifier()),
                    state: EmitterPublishingDrainState::AwaitingConfirmation,
                    pending_messages,
                    retry_backoff: None,
                    retry_wait: None,
                });
                continue;
            }
            let Some(retry) = self.inner.emitter_retry_statuses.get(&key) else {
                continue;
            };
            let state = match retry.kind {
                EmitterRetryKind::Infrastructure => {
                    EmitterPublishingDrainState::RetryingInfrastructure
                }
                EmitterRetryKind::IcebergCommit => {
                    EmitterPublishingDrainState::RetryingIcebergCommit
                }
            };
            emitter_publishing.push(EmitterPublishingDrainStatus {
                emitter: EmitterName::from(key.identifier()),
                state,
                pending_messages,
                retry_backoff: Some(retry.reconnect.backoff),
                retry_wait: Some(
                    retry
                        .reconnect
                        .retry_at
                        .saturating_duration_since(Instant::now()),
                ),
            });
        }
        emitter_publishing.sort_by(|left, right| left.emitter.cmp(&right.emitter));
        DomainDrainStatus {
            active_ingestors,
            active_generators,
            outstanding_acks: self.domain_outstanding_work(domain),
            buffered_emitter_messages,
            emitter_publishing,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use nervix_interconnect::EntityGatePurpose;
    use nervix_models::{
        AckMode, BranchSelection, ClusterNodeName, CoordinationIdentity, CreateEmitter,
        CreateJunction, CreateRelay, DomainSchedule, EmitSink, EmitterName, EmitterPublishingMode,
        ErrorPolicies, IngestQuiesceMode, IngestorName, ModelKind, ModelName, NodeRef,
        ProcessorInputs, ProcessorOutputs, RelayBranching, RelayName, RetryPolicy,
    };
    use nonzero_ext::nonzero;
    use tokio::{
        sync::watch,
        time::{Duration, Instant},
    };
    use triomphe::Arc;

    use super::*;

    fn coordination(coordinator: &str, process_epoch: u64, sequence: u64) -> CoordinationIdentity {
        CoordinationIdentity::new(named(coordinator), process_epoch, sequence)
    }

    #[test]
    fn domain_drain_status_reports_structured_emitter_publishing_state() {
        let runtime = Runtime::new();
        let domain = domain("default");
        let confirming = named::<EmitterName>("confirming");
        let retrying = named::<EmitterName>("retrying");
        let iceberg = named::<EmitterName>("iceberg");

        for (emitter, pending_messages) in [
            (&confirming, 3_usize),
            (&retrying, 2_usize),
            (&iceberg, 5_usize),
        ] {
            runtime.inner.emitter_buffers.insert(
                DomainNodeRef::node_in(domain.clone(), ModelKind::Emitter, emitter.clone()),
                Arc::new(AtomicUsize::new(pending_messages)),
            );
        }

        let confirmation = runtime.begin_emitter_confirmation_wait(&domain, &confirming);
        runtime.record_emitter_transient_error_with_backoff(
            &domain,
            &retrying,
            "sensitive infrastructure detail that drain status must not expose",
            Duration::from_secs(2),
        );
        runtime.record_iceberg_commit_failure_with_backoff(
            &domain,
            &iceberg,
            "sensitive catalog detail that drain status must not expose",
            Duration::from_secs(3),
        );

        let status = runtime.domain_drain_status(&domain);

        assert_eq!(status.emitter_publishing.len(), 3);
        assert_eq!(
            status.emitter_publishing[0],
            EmitterPublishingDrainStatus {
                emitter: confirming,
                state: EmitterPublishingDrainState::AwaitingConfirmation,
                pending_messages: 3,
                retry_backoff: None,
                retry_wait: None,
            }
        );
        assert_eq!(status.emitter_publishing[1].emitter, iceberg);
        assert_eq!(
            status.emitter_publishing[1].state,
            EmitterPublishingDrainState::RetryingIcebergCommit
        );
        assert_eq!(
            status.emitter_publishing[1].retry_backoff,
            Some(Duration::from_secs(3))
        );
        assert!(
            status.emitter_publishing[1]
                .retry_wait
                .is_some_and(|wait| wait <= Duration::from_secs(3))
        );
        assert_eq!(status.emitter_publishing[2].emitter, retrying);
        assert_eq!(
            status.emitter_publishing[2].state,
            EmitterPublishingDrainState::RetryingInfrastructure
        );
        assert_eq!(
            status.emitter_publishing[2].retry_backoff,
            Some(Duration::from_secs(2))
        );
        let affected_emitters = [
            status.emitter_publishing[0].emitter.clone(),
            status.emitter_publishing[1].emitter.clone(),
            status.emitter_publishing[2].emitter.clone(),
        ]
        .into_iter()
        .map(|identifier| NodeRef {
            kind: ModelKind::Emitter,
            identifier: ModelName::from(&identifier),
        })
        .collect::<Vec<_>>();
        let entity_status = runtime
            .entity_drain_status(
                &domain,
                &[],
                &affected_emitters,
                EntityGatePurpose::ModelAlteration,
            )
            .emitter_publishing;
        assert_eq!(entity_status.len(), status.emitter_publishing.len());
        for (entity, domain) in entity_status.iter().zip(&status.emitter_publishing) {
            assert_eq!(entity.emitter, domain.emitter);
            assert_eq!(entity.state, domain.state);
            assert_eq!(entity.pending_messages, domain.pending_messages);
            assert_eq!(entity.retry_backoff, domain.retry_backoff);
            assert_eq!(entity.retry_wait.is_some(), domain.retry_wait.is_some());
        }

        drop(confirmation);
        assert!(
            runtime
                .domain_drain_status(&domain)
                .emitter_publishing
                .iter()
                .all(|status| status.emitter != named("confirming")),
            "a completed confirmation must disappear from drain status"
        );
    }

    #[tokio::test]
    async fn entity_gate_hold_quiesces_an_ingestor_without_stopping_it() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let relay = named::<RelayName>("events");
        let ingestor = named::<IngestorName>("events_source");
        let coordination = coordination("coordinator-a", 7, 41);

        let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let gate = fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
            fanout,
        );

        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let stopped = StdArc::new(AtomicBool::new(false));
        let task_stopped = stopped.clone();
        let task = tokio::spawn(async move {
            let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
            task_stopped.store(true, Ordering::SeqCst);
        });
        runtime.inner.ingestors.insert(
            key.clone(),
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: Vec::new(),
                tasks: vec![task],
            },
        );
        runtime.inner.ingestor_quiescence.insert(
            key.clone(),
            test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Suspend),
        );

        let affected = NodeRef {
            kind: ModelKind::Ingestor,
            identifier: ModelName::from(&ingestor.clone()),
        };
        runtime
            .engage_entity_gate_operation(
                &coordination,
                &domain,
                std::slice::from_ref(&relay),
                std::slice::from_ref(&affected),
                EntityGatePurpose::ModelAlteration,
                EntityGateLease {
                    deadline: Instant::now() + Duration::from_secs(5),
                    reason: "quiesce regression",
                },
            )
            .await
            .expect("entity hold should engage");

        assert!(runtime.inner.ingestors.get(&key).is_some());
        assert!(!stopped.load(Ordering::SeqCst));
        assert!(gate.is_closed());
        assert!(runtime.entity_gate_operation_is_held(&coordination));
        assert_eq!(
            runtime
                .inner
                .ingestor_quiescence
                .get(&key)
                .and_then(|control| control.cause()),
            Some(IngestorQuiesceCause::EntityHold)
        );

        runtime
            .release_entity_gate_operation(&coordination, &domain)
            .await
            .expect("entity hold should release");
        assert!(!runtime.entity_gate_operation_is_held(&coordination));
        assert!(!gate.is_closed());
        assert!(runtime.inner.ingestors.get(&key).is_some());
        assert!(!stopped.load(Ordering::SeqCst));
        assert_eq!(
            runtime
                .inner
                .ingestor_quiescence
                .get(&key)
                .and_then(|control| control.cause()),
            None
        );

        runtime
            .stop_ingestor(&domain, &ingestor)
            .await
            .expect("test ingestor should stop");
    }

    #[tokio::test]
    async fn entity_gate_operation_releases_when_its_lease_deadline_expires() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let relay = named::<RelayName>("events");
        let coordination = coordination("coordinator-a", 7, 42);
        let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let gate = fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
            fanout,
        );

        runtime
            .engage_entity_gate_operation(
                &coordination,
                &domain,
                std::slice::from_ref(&relay),
                &[],
                EntityGatePurpose::OwnershipHandoff,
                EntityGateLease {
                    deadline: Instant::now() + Duration::from_millis(25),
                    reason: "deadline regression",
                },
            )
            .await
            .expect("entity hold should engage");
        assert!(gate.is_closed());

        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime.entity_gate_operation_is_held(&coordination) {
                tokio::task::consume_budget().await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("entity hold should release at its deadline");
        assert!(!gate.is_closed());
    }

    #[tokio::test]
    async fn equal_operation_ids_from_different_coordinators_fence_each_requested_relay() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let first_relay = named::<RelayName>("first_events");
        let second_relay = named::<RelayName>("second_events");
        let first_coordination = coordination("leader-a", 10, 1);
        let second_coordination = coordination("leader-b", 20, 1);
        let first_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let first_gate = first_fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, first_relay.clone()),
            first_fanout,
        );
        let second_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let second_gate = second_fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, second_relay.clone()),
            second_fanout,
        );

        runtime
            .engage_entity_gate_operation(
                &first_coordination,
                &domain,
                std::slice::from_ref(&first_relay),
                &[],
                EntityGatePurpose::ModelAlteration,
                EntityGateLease {
                    deadline: Instant::now() + Duration::from_secs(5),
                    reason: "first coordinator",
                },
            )
            .await
            .expect("the first coordinator hold should engage");
        runtime
            .engage_entity_gate_operation(
                &second_coordination,
                &domain,
                std::slice::from_ref(&second_relay),
                &[],
                EntityGatePurpose::ModelAlteration,
                EntityGateLease {
                    deadline: Instant::now() + Duration::from_secs(5),
                    reason: "replacement coordinator",
                },
            )
            .await
            .expect("a successful second coordinator hold should engage its complete scope");

        assert!(first_gate.is_closed());
        assert!(second_gate.is_closed());
    }

    #[tokio::test]
    async fn concurrent_coordinators_with_equal_sequences_hold_independent_scopes() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let first_relay = named::<RelayName>("first_events");
        let second_relay = named::<RelayName>("second_events");
        let first_coordination = coordination("leader-a", 10, 1);
        let second_coordination = coordination("leader-b", 20, 1);
        let first_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let first_gate = first_fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, first_relay.clone()),
            first_fanout,
        );
        let second_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let second_gate = second_fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, second_relay.clone()),
            second_fanout,
        );

        let first = runtime.engage_entity_gate_operation(
            &first_coordination,
            &domain,
            std::slice::from_ref(&first_relay),
            &[],
            EntityGatePurpose::ModelAlteration,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(5),
                reason: "first concurrent coordinator",
            },
        );
        let second = runtime.engage_entity_gate_operation(
            &second_coordination,
            &domain,
            std::slice::from_ref(&second_relay),
            &[],
            EntityGatePurpose::ModelAlteration,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(5),
                reason: "second concurrent coordinator",
            },
        );
        let (first, second) = tokio::join!(first, second);

        first.expect("the first concurrent hold should engage");
        second.expect("the second concurrent hold should engage");
        assert!(first_gate.is_closed());
        assert!(second_gate.is_closed());
    }

    #[tokio::test]
    async fn receiver_finishes_engagement_after_the_coordinator_request_is_cancelled() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let relay = named::<RelayName>("events");
        let coordination = coordination("leader-a", 10, 1);
        let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let gate = fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
            fanout,
        );
        let dispatch = gate.acquire_dispatch().await;
        let request = tokio::spawn({
            let runtime = runtime.clone();
            let domain = domain.clone();
            let relay = relay.clone();
            let coordination = coordination.clone();
            async move {
                runtime
                    .engage_entity_gate_operation(
                        &coordination,
                        &domain,
                        &[relay],
                        &[],
                        EntityGatePurpose::ModelAlteration,
                        EntityGateLease {
                            deadline: Instant::now() + Duration::from_secs(5),
                            reason: "coordinator request cancellation regression",
                        },
                    )
                    .await
            }
        });
        while !gate.is_closed() {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }

        request.abort();
        assert!(
            request
                .await
                .expect_err("the simulated coordinator request should be cancelled")
                .is_cancelled()
        );
        drop(dispatch);

        tokio::time::timeout(
            Duration::from_secs(1),
            runtime.engage_entity_gate_operation(
                &coordination,
                &domain,
                std::slice::from_ref(&relay),
                &[],
                EntityGatePurpose::ModelAlteration,
                EntityGateLease {
                    deadline: Instant::now() + Duration::from_secs(5),
                    reason: "same operation retry after coordinator loss",
                },
            ),
        )
        .await
        .expect("the receiver-owned engagement should finish after request cancellation")
        .expect("the same operation retry should observe the completed hold");
        assert!(runtime.entity_gate_operation_is_held(&coordination));
        assert!(gate.is_closed());
    }

    #[tokio::test]
    async fn retries_require_the_same_scope_and_stale_operations_cannot_observe_or_release_it() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let held_relay = named::<RelayName>("held_events");
        let conflicting_relay = named::<RelayName>("conflicting_events");
        let current = coordination("leader-a", 11, 1);
        let prior_incarnation = coordination("leader-a", 10, 1);
        let held_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let held_gate = held_fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, held_relay.clone()),
            held_fanout,
        );
        let conflicting_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let conflicting_gate = conflicting_fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, conflicting_relay.clone()),
            conflicting_fanout,
        );
        let lease = || EntityGateLease {
            deadline: Instant::now() + Duration::from_secs(5),
            reason: "scope identity regression",
        };

        runtime
            .engage_entity_gate_operation(
                &current,
                &domain,
                std::slice::from_ref(&held_relay),
                &[],
                EntityGatePurpose::ModelAlteration,
                lease(),
            )
            .await
            .expect("the current operation should engage");
        runtime
            .engage_entity_gate_operation(
                &current,
                &domain,
                &[held_relay.clone(), held_relay.clone()],
                &[],
                EntityGatePurpose::ModelAlteration,
                lease(),
            )
            .await
            .expect("a retry with the same canonical scope should succeed");

        let conflict = runtime
            .engage_entity_gate_operation(
                &current,
                &domain,
                std::slice::from_ref(&conflicting_relay),
                &[],
                EntityGatePurpose::ModelAlteration,
                lease(),
            )
            .await;
        assert!(conflict.is_err());
        assert!(!conflicting_gate.is_closed());
        runtime
            .entity_gate_operation_drain_status(
                &current,
                &domain,
                std::slice::from_ref(&held_relay),
                &[],
                EntityGatePurpose::ModelAlteration,
            )
            .expect("the exact operation and scope should report status");
        assert!(
            runtime
                .entity_gate_operation_drain_status(
                    &prior_incarnation,
                    &domain,
                    std::slice::from_ref(&held_relay),
                    &[],
                    EntityGatePurpose::ModelAlteration,
                )
                .is_err()
        );

        runtime
            .release_entity_gate_operation(&prior_incarnation, &domain)
            .await
            .expect("a stale release should be idempotent");
        assert!(runtime.entity_gate_operation_is_held(&current));
        assert!(held_gate.is_closed());
        runtime
            .release_entity_gate_operation(&current, &domain)
            .await
            .expect("the exact operation should release");
        assert!(!held_gate.is_closed());
    }

    #[tokio::test]
    async fn releasing_one_coordinator_preserves_an_overlapping_ownership_freeze() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let affected = NodeRef {
            kind: ModelKind::Deduplicator,
            identifier: named("deduplicate_events"),
        };
        let entity = affected.clone().in_domain(&domain);
        let first = coordination("leader-a", 10, 1);
        let second = coordination("leader-b", 20, 1);
        let lease = || EntityGateLease {
            deadline: Instant::now() + Duration::from_secs(5),
            reason: "overlapping ownership freeze regression",
        };

        runtime
            .engage_entity_gate_operation(
                &first,
                &domain,
                &[],
                std::slice::from_ref(&affected),
                EntityGatePurpose::OwnershipHandoff,
                lease(),
            )
            .await
            .expect("the first ownership hold should engage");
        runtime
            .engage_entity_gate_operation(
                &second,
                &domain,
                &[],
                std::slice::from_ref(&affected),
                EntityGatePurpose::OwnershipHandoff,
                lease(),
            )
            .await
            .expect("the replacement coordinator ownership hold should engage");

        runtime
            .release_entity_gate_operation(&first, &domain)
            .await
            .expect("the first ownership hold should release");
        assert!(!runtime.ownership_handoff_entity_is_frozen_by(&entity, &first));
        assert!(runtime.ownership_handoff_entity_is_frozen_by(&entity, &second));
        assert!(runtime.ownership_handoff_entity_is_frozen(&entity));

        runtime
            .release_entity_gate_operation(&second, &domain)
            .await
            .expect("the replacement ownership hold should release");
        assert!(!runtime.ownership_handoff_entity_is_frozen(&entity));
    }

    #[tokio::test(start_paused = true)]
    async fn preceding_lease_expiry_does_not_release_a_reengaged_hold() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let relay = named::<RelayName>("events");
        let coordination = coordination("leader-a", 10, 1);
        let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let gate = fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
            fanout,
        );

        runtime
            .engage_entity_gate_operation(
                &coordination,
                &domain,
                std::slice::from_ref(&relay),
                &[],
                EntityGatePurpose::ModelAlteration,
                EntityGateLease {
                    deadline: Instant::now() + Duration::from_millis(10),
                    reason: "preceding lease",
                },
            )
            .await
            .expect("the preceding hold should engage");
        runtime
            .release_entity_gate_operation(&coordination, &domain)
            .await
            .expect("the preceding hold should release");
        runtime
            .engage_entity_gate_operation(
                &coordination,
                &domain,
                std::slice::from_ref(&relay),
                &[],
                EntityGatePurpose::ModelAlteration,
                EntityGateLease {
                    deadline: Instant::now() + Duration::from_secs(1),
                    reason: "replacement lease",
                },
            )
            .await
            .expect("the replacement hold should engage");

        tokio::time::advance(Duration::from_millis(11)).await;
        tokio::task::yield_now().await;

        assert!(runtime.entity_gate_operation_is_held(&coordination));
        assert!(gate.is_closed());
    }

    #[test]
    fn emitter_entity_pause_gates_every_input_relay() {
        let emitter = CreateEmitter {
            name: named("combined_sink"),
            from: ProcessorInputs::new(vec![named("source_b"), named("source_a")], Vec::new()),
            encode_using_codec: Some(named("event_codec")),
            sink: Box::new(EmitSink::ZeroMq {
                client: named("sink"),
            }),
            flush_policy: FlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode: EmitterPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            mode: AckMode::Attached,
            construction: nervix_models::RouteConstruction::default(),
            materialized_state: Vec::new(),
        };
        let input_relay = |name: &str| {
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                name: named(name),
                schema: named("event"),
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            }))
        };
        let mut schedule = DomainSchedule::new(
            domain("testing"),
            vec![
                input_relay("source_a"),
                input_relay("source_b"),
                scheduled_model(nervix_models::Model::Emitter(emitter.clone())),
            ],
            Vec::new(),
        );
        let emitter_node = schedule
            .nodes
            .values_mut()
            .find(|node| node.kind() == ModelKind::Emitter)
            .expect("test schedule must contain its emitter");
        emitter_node.primary_node = Some(ClusterNodeName::parse("node-2").expect("valid name"));
        emitter_node.assigned_nodes = vec![ClusterNodeName::parse("node-2").expect("valid name")];
        let entity = NodeRef {
            kind: ModelKind::Emitter,
            identifier: ModelName::from(&emitter.name),
        };

        assert_eq!(
            crate::registry::entity_pause_relays_for_schedule(&schedule, &[entity]),
            vec![named("source_a"), named("source_b")]
        );
        let remote_consumers = Runtime::remote_runtime_consumers_for_schedule(
            &schedule,
            &ClusterNodeName::parse("node-1").expect("valid name"),
        );
        assert_eq!(remote_consumers.len(), 2);
        for relay in [named("source_a"), named("source_b")] {
            let consumers = remote_consumers
                .get(&relay)
                .expect("every emitter input needs a remote consumer");
            assert_eq!(consumers.len(), 1);
            assert_eq!(consumers[0].relay, relay);
            assert_eq!(
                consumers[0].node_id,
                ClusterNodeName::parse("node-2").expect("valid name")
            );
        }
    }

    #[test]
    fn ownership_handoff_keeps_internal_moved_group_relays_open() {
        let junction = |name: &str, input: &str, output: &str| {
            scheduled_model(nervix_models::Model::Junction(CreateJunction {
                name: named(name),
                from: ProcessorInputs::single(named(input)),
                output_routes: (ProcessorOutputs::single(named(output)))
                    .with_flush_policy(FlushPolicy::Immediate),
                branched_by: BranchSelection::unbranched(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            }))
        };
        let schedule = DomainSchedule::new(
            domain("testing"),
            vec![
                junction("corridor_source", "inbound", "corridor_stage"),
                junction("corridor_sink", "corridor_stage", "outbound"),
            ],
            Vec::new(),
        );
        let affected = ["corridor_source", "corridor_sink"].map(|name| NodeRef {
            kind: ModelKind::Junction,
            identifier: named(name),
        });

        assert_eq!(
            crate::registry::entity_pause_relays_for_schedule(&schedule, &affected),
            vec![named("corridor_stage"), named("inbound")]
        );
        assert_eq!(
            crate::registry::ownership_handoff_relays_for_schedule(&schedule, &affected),
            vec![named("inbound")]
        );
    }

    #[test]
    fn quiesce_counters_belong_to_one_node_not_to_a_shared_identifier() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let shared = named::<ModelName>("orders");
        let relay = runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Relay, &shared));
        let emitter =
            runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Emitter, &shared));

        let _work = NodeQuiesceWorkGuard::begin(relay.clone());

        assert_eq!(relay.outstanding_work(), 1);
        assert_eq!(emitter.outstanding_work(), 0);
    }
}
