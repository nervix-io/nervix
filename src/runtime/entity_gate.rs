use super::*;

/// One in-flight entity-gate hold, identified by the domain it pauses and the operation that took
/// it, so a retried operation reuses the hold it already owns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct EntityGateHoldKey {
    pub(super) domain: DomainName,
    pub(super) operation_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterPublishingDrainState {
    AwaitingConfirmation,
    RetryingInfrastructure,
    RetryingIcebergCommit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmitterPublishingDrainStatus {
    pub emitter: EmitterName,
    pub state: EmitterPublishingDrainState,
    pub pending_messages: usize,
    pub retry_backoff: Option<Duration>,
    pub retry_wait: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainDrainStatus {
    pub active_ingestors: usize,
    pub active_generators: usize,
    pub outstanding_acks: usize,
    pub buffered_emitter_messages: usize,
    pub emitter_publishing: Vec<EmitterPublishingDrainStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDrainStatus {
    pub buffered_relay_batches: usize,
    pub node_work_items: usize,
    pub outstanding_acks: usize,
    pub emitter_publishing: Vec<EmitterPublishingDrainStatus>,
}

impl EntityDrainStatus {
    pub fn is_drained(&self) -> bool {
        self.buffered_relay_batches == 0 && self.node_work_items == 0 && self.outstanding_acks == 0
    }

    pub fn outstanding_work(&self) -> usize {
        [
            self.buffered_relay_batches,
            self.node_work_items,
            self.outstanding_acks,
        ]
        .into_iter()
        .try_fold(0_usize, usize::checked_add)
        .assured("every count totals work items this node already holds in memory")
    }
}

pub struct EntityGateHold {
    pub(super) gates: Vec<RelayDispatchGateLease>,
}

#[derive(Clone, Copy)]
pub(crate) struct EntityGateLease<'a> {
    pub(crate) deadline: Instant,
    pub(crate) reason: &'a str,
}

pub(super) struct EntityAlterHold {
    pub(super) gates: EntityGateHold,
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
        let depths = branch
            .processors
            .get(processor)
            .map(|processor| BranchQuiesceDepths {
                collected_inputs: processor
                    .input_collectors
                    .values()
                    .map(|collector| collector.pending.len())
                    .sum(),
                pending_materialized: processor.pending_materialized.len(),
                output_buffers: processor
                    .operation
                    .output_routes()
                    .routes
                    .iter()
                    .map(|output| output.pending.len())
                    .sum(),
            })
            .unwrap_or_default();
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

    pub fn release(mut self) {
        self.release_all();
    }

    pub(super) fn release_all(&mut self) {
        self.gates.clear();
    }
}

impl Drop for EntityGateHold {
    fn drop(&mut self) {
        self.release_all();
    }
}

impl DomainDrainStatus {
    pub fn is_drained(&self) -> bool {
        self.active_ingestors == 0
            && self.active_generators == 0
            && self.outstanding_acks == 0
            && self.buffered_emitter_messages == 0
    }

    pub fn outstanding_work(&self) -> usize {
        [
            self.active_ingestors,
            self.active_generators,
            self.outstanding_acks,
            self.buffered_emitter_messages,
        ]
        .into_iter()
        .try_fold(0_usize, usize::checked_add)
        .assured("every count totals work items this node already holds in memory")
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
    pub fn entity_pause_relays(
        &self,
        domain: &DomainName,
        affected_entities: &[NodeRef],
    ) -> Vec<RelayName> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Vec::new();
        };
        Self::entity_pause_relays_for_schedule(&execution.schedule, affected_entities)
    }

    pub(crate) fn entity_pause_relays_for_schedule(
        schedule: &DomainSchedule,
        affected_entities: &[NodeRef],
    ) -> Vec<RelayName> {
        let processor_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        let mut relays = Vec::new();
        for entity in affected_entities {
            if entity.kind == ModelKind::Relay {
                relays.push(RelayName::from(&entity.identifier));
                continue;
            }
            if let Some(processor) = processor_specs.processor(entity.kind, &entity.identifier) {
                relays.extend(processor.spec.input_relays.clone());
                continue;
            }
            let Some(node) = schedule
                .nodes
                .get(&NodeRef::new(entity.kind, entity.identifier.clone()))
            else {
                continue;
            };
            match node.config.as_ref() {
                Model::Emitter(emitter) => relays.extend(emitter.from.from.clone()),
                Model::Reingestor(reingestor) => relays.extend(reingestor.from.from.clone()),
                Model::Generator(generator) => {
                    relays.push(generator.materialized_relay.clone());
                }
                _ => {}
            }
        }
        relays.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        relays.dedup();
        relays
    }

    pub(crate) fn ownership_handoff_relays_for_schedule(
        schedule: &DomainSchedule,
        affected_entities: &[NodeRef],
    ) -> Vec<RelayName> {
        let mut relays = Self::entity_pause_relays_for_schedule(schedule, affected_entities);
        let affected = affected_entities.iter().cloned().collect::<HashSet<_>>();
        let processor_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        relays.retain(|relay| {
            let producers = schedule.nodes.values().filter(|node| {
                if let Some(processor) = processor_specs.processor(node.kind, &node.identifier) {
                    return processor.spec.output_relays().contains(relay);
                }
                match node.config.as_ref() {
                    Model::Ingestor(ingestor) => {
                        ingestor.output_routes.relays().any(|out| out == relay)
                    }
                    Model::Reingestor(reingestor) => {
                        reingestor.output_routes.relays().any(|out| out == relay)
                    }
                    Model::Generator(generator) => {
                        generator.output_routes.relays().any(|out| out == relay)
                    }
                    _ => false,
                }
            });
            let mut producer_count = 0usize;
            let all_producers_move = producers.fold(true, |all_move, node| {
                producer_count = producer_count
                    .checked_add(1)
                    .assured("the producers counted here are graph nodes held in memory");
                all_move && affected.contains(&node.identity())
            });
            producer_count == 0 || !all_producers_move
        });
        relays
    }

    pub fn engage_entity_gates(
        &self,
        domain: &DomainName,
        relays: &[RelayName],
        deadline: Instant,
        reason: &str,
    ) -> EntityGateHold {
        let mut gates = Vec::with_capacity(relays.len());
        for relay in relays {
            let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone());
            let Some(fanout) = self.inner.relay_boundary_fanouts.get(&key) else {
                continue;
            };
            let gate = fanout.dispatch_gate();
            gates.push(RelayDispatchGateLease::engage(gate, deadline, reason));
        }
        EntityGateHold { gates }
    }

    pub(crate) async fn engage_entity_gate_operation(
        &self,
        operation_id: u64,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
        lease: EntityGateLease<'_>,
    ) -> Result<(), String> {
        let EntityGateLease { deadline, reason } = lease;
        let hold_key = EntityGateHoldKey {
            domain: domain.clone(),
            operation_id,
        };
        if self.inner.entity_gate_holds.contains_key(&hold_key) {
            return Ok(());
        }
        let mut gates = self.engage_entity_gates(domain, relays, deadline, reason);
        if !gates.wait_quiescent().await {
            gates.release();
            return Err(format!(
                "relay dispatch gate fence for domain '{}' did not complete before its deadline",
                domain.as_str()
            ));
        }
        if self.inner.entity_gate_holds.contains_key(&hold_key) {
            gates.release();
            return Ok(());
        }
        self.inner.entity_gate_holds.insert(
            hold_key.clone(),
            EntityAlterHold {
                gates,
                quiesced_ingestors: Vec::new(),
            },
        );
        let ingestors = affected_entities
            .iter()
            .filter(|entity| entity.kind == ModelKind::Ingestor)
            .map(|entity| entity.identifier.clone())
            .collect::<Vec<_>>();
        let quiesce_cause = match purpose {
            EntityGatePurpose::ModelAlteration => IngestorQuiesceCause::EntityHold,
            EntityGatePurpose::OwnershipHandoff => IngestorQuiesceCause::OwnershipHandoff,
        };
        for ingestor in &ingestors {
            tokio::task::consume_budget().await;
            let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
            if !self.inner.ingestors.contains_key(&key) {
                continue;
            }
            if let Some(control) =
                self.engage_ingestor_quiesce(domain, &IngestorName::from(ingestor), quiesce_cause)
            {
                match self.inner.entity_gate_holds.get_mut(&hold_key) {
                    Some(mut hold) => hold.quiesced_ingestors.push(QuiescedIngestorHold {
                        ingestor: IngestorName::from(ingestor),
                        cause: quiesce_cause,
                        control,
                    }),
                    // The gate this quiesce belongs to is already gone, so nothing will release
                    // it later; undo it here instead of leaving the ingestor quiesced forever.
                    None => control.release(quiesce_cause),
                }
            }
        }
        self.force_flush_domain(domain);
        if Instant::now() >= deadline {
            self.release_entity_gate_operation(operation_id, domain)
                .await?;
            return Err(format!(
                "entity gate for domain '{}' expired while it was being engaged",
                domain.as_str()
            ));
        }
        let entity_gate_holds = self.inner.entity_gate_holds.clone();
        let ingestors = self.inner.ingestors.clone();
        let ingestor_quiescence = self.inner.ingestor_quiescence.clone();
        let domain = domain.clone();
        drop(tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            if entity_gate_holds.contains_key(&EntityGateHoldKey {
                domain: domain.clone(),
                operation_id,
            }) {
                debug!(
                    domain = domain.as_str(),
                    operation_id, "entity gate lease reached its deadline"
                );
                if let Err(error) = Self::release_entity_gate_operation_from_state(
                    &entity_gate_holds,
                    &ingestors,
                    &ingestor_quiescence,
                    operation_id,
                    &domain,
                )
                .await
                {
                    warn!(
                        domain = domain.as_str(),
                        operation_id, error, "failed to release expired entity gate lease"
                    );
                }
            }
        }));
        Ok(())
    }

    pub async fn release_entity_gate_operation(
        &self,
        operation_id: u64,
        domain: &DomainName,
    ) -> Result<(), String> {
        Self::release_entity_gate_operation_from_state(
            &self.inner.entity_gate_holds,
            &self.inner.ingestors,
            &self.inner.ingestor_quiescence,
            operation_id,
            domain,
        )
        .await
    }

    pub(super) async fn release_entity_gate_operation_from_state(
        entity_gate_holds: &DashMap<EntityGateHoldKey, EntityAlterHold, RandomState>,
        ingestors: &DashMap<DomainNodeRef, IngestorRuntime, RandomState>,
        ingestor_quiescence: &DashMap<DomainNodeRef, Arc<IngestorQuiesceControl>, RandomState>,
        operation_id: u64,
        domain: &DomainName,
    ) -> Result<(), String> {
        // Taking the hold out of the map is what makes this release exclusive: the entity gate
        // is released both by an explicit request and by its deadline task, and only the caller
        // that removes the hold may release the reasons it engaged.
        let hold_key = EntityGateHoldKey {
            domain: domain.clone(),
            operation_id,
        };
        let Some((_, hold)) = entity_gate_holds.remove(&hold_key) else {
            return Ok(());
        };
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
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn entity_gate_operation_is_held(
        &self,
        operation_id: u64,
        domain: &DomainName,
    ) -> bool {
        self.inner
            .entity_gate_holds
            .contains_key(&EntityGateHoldKey {
                domain: domain.clone(),
                operation_id,
            })
    }

    pub fn entity_drain_status(
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
                let quiesce_work = self
                    .inner
                    .node_quiesce_counters
                    .get(&entity.in_domain(domain))
                    .map(|counters| counters.outstanding_work_for(purpose))
                    .unwrap_or(0);
                let emitter_work = if entity.kind == ModelKind::Emitter {
                    self.inner
                        .emitter_buffers
                        .get(&entity.in_domain(domain))
                        .map(|buffered| buffered.load(Ordering::Acquire))
                        .unwrap_or(0)
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
        let pending_messages = self
            .inner
            .emitter_buffers
            .get(key)
            .map(|buffered| buffered.load(Ordering::Acquire))
            .unwrap_or(0);
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

    pub fn domain_alter_is_active(&self, domain: &DomainName) -> bool {
        self.inner.active_domain_alters.contains_key(domain)
    }

    #[cfg(feature = "testing")]
    pub async fn pause_entity_gate_if_armed(&self, domain: &DomainName) {
        self.inner.entity_gate_pauses.pause_if_armed(domain).await;
    }

    pub fn domain_drain_status(&self, domain: &DomainName) -> DomainDrainStatus {
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
        let active_generators = self
            .inner
            .generator_activity_by_domain
            .get(domain)
            .map_or(0, |counter| counter.load(Ordering::Acquire));
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
            let pending_messages = self
                .inner
                .emitter_buffers
                .get(&key)
                .map(|buffered| buffered.load(Ordering::Acquire))
                .unwrap_or(0);
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
        AckMode, BranchSelection, ClusterNodeName, CreateEmitter, CreateJunction, CreateRelay,
        DomainSchedule, EmitSink, EmitterName, EmitterPublishingMode, ErrorPolicies,
        IngestQuiesceMode, IngestorName, ModelKind, ModelName, NodeRef, ProcessorInputs,
        ProcessorOutputs, RelayBranching, RelayName, RetryPolicy,
    };
    use nonzero_ext::nonzero;
    use tokio::{
        sync::watch,
        time::{Duration, Instant},
    };
    use triomphe::Arc;

    use super::*;

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
        let operation_id = 41;

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
                operation_id,
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
        assert!(runtime.entity_gate_operation_is_held(operation_id, &domain));
        assert_eq!(
            runtime
                .inner
                .ingestor_quiescence
                .get(&key)
                .and_then(|control| control.cause()),
            Some(IngestorQuiesceCause::EntityHold)
        );

        runtime
            .release_entity_gate_operation(operation_id, &domain)
            .await
            .expect("entity hold should release");
        assert!(!runtime.entity_gate_operation_is_held(operation_id, &domain));
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
        let operation_id = 42;
        let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
        let gate = fanout.dispatch_gate();
        runtime.inner.relay_boundary_fanouts.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
            fanout,
        );

        runtime
            .engage_entity_gate_operation(
                operation_id,
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
            while runtime.entity_gate_operation_is_held(operation_id, &domain) {
                tokio::task::consume_budget().await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("entity hold should release at its deadline");
        assert!(!gate.is_closed());
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
            scheduled_model(
                ModelKind::Relay,
                named(name),
                nervix_models::Model::Relay(CreateRelay {
                    name: named(name),
                    schema: named("event"),
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }),
            )
        };
        let mut schedule = DomainSchedule::new(
            domain("testing"),
            vec![
                input_relay("source_a"),
                input_relay("source_b"),
                scheduled_model(
                    ModelKind::Emitter,
                    ModelName::from(&emitter.name.clone()),
                    nervix_models::Model::Emitter(emitter.clone()),
                ),
            ],
            Vec::new(),
        );
        let emitter_node = schedule
            .nodes
            .values_mut()
            .find(|node| node.kind == ModelKind::Emitter)
            .expect("test schedule must contain its emitter");
        emitter_node.primary_node = Some(ClusterNodeName::parse("node-2").expect("valid name"));
        emitter_node.assigned_nodes = vec![ClusterNodeName::parse("node-2").expect("valid name")];
        let entity = NodeRef {
            kind: ModelKind::Emitter,
            identifier: ModelName::from(&emitter.name),
        };

        assert_eq!(
            Runtime::entity_pause_relays_for_schedule(&schedule, &[entity]),
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
            scheduled_model(
                ModelKind::Junction,
                named(name),
                nervix_models::Model::Junction(CreateJunction {
                    name: named(name),
                    from: ProcessorInputs::single(named(input)),
                    output_routes: (ProcessorOutputs::single(named(output)))
                        .with_flush_policy(FlushPolicy::Immediate),
                    branched_by: BranchSelection::unbranched(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            )
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
            Runtime::entity_pause_relays_for_schedule(&schedule, &affected),
            vec![named("corridor_stage"), named("inbound")]
        );
        assert_eq!(
            Runtime::ownership_handoff_relays_for_schedule(&schedule, &affected),
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
