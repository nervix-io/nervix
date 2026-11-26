//! Destroying, at once, the entities a committed revision drops from a running domain on this
//! node, and the report of what each destruction discarded.
//!
//! Layer: data plane.
//!
//! - **Owns.** Finding the entities this node's installed execution runs that the desired revision
//!   no longer declares; disconnecting each from the relays it reads before destroying it; the
//!   release, as done, of every message share a destroyed entity held, counted by kind; and the
//!   drop ledger this node keeps until the change's coordinator collects it.
//! - **Depends on.** The installed domain execution, its relay boundary services, each entity's
//!   task owner and the acknowledgement tree.
//! - **Must not know.** Consensus, the statement that dropped an entity, or how an answer presents
//!   what was discarded.
//!
//! A drop waits for nothing a dropped entity holds. The entity's share of each message it held is
//! released as done, so a source whose message the relay's other consumers delivered hears it
//! acknowledged, and no error record is written. The entities that stay keep running.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a node destroys each dropped entity once, as it applies the revision that \
                  dropped it, and records the drop in its ledger then"
    )
)]

use std::collections::{BTreeMap, BTreeSet};

use nervix_interconnect::entity_drop::DropReportAnswer;
use nervix_models::{DiscardedWork, DiscardedWorkKind, NodeRef};

use super::{relay_repartition::InstalledRegistrations, scheduled_node::PlacementTasks, *};
use crate::emitter_execution_plan::EmitterInputUnits;

/// What destroying a dropped ingestor's units on this node released, and whether what it kept at
/// its broker for this node was given up.
struct IngestorDestruction {
    released: DiscardedWorkCounts,
    retirement: error_stack::Result<(), RuntimeError>,
}

/// What one dropped entity's runtime on this node released as done when it was destroyed,
/// counted by kind. A kind it held nothing of is absent.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct DiscardedWorkCounts {
    counts: BTreeMap<DiscardedWorkKind, u64>,
}

impl DiscardedWorkCounts {
    /// Counts `items` more of `kind`. Nothing is recorded for none.
    pub(super) fn add(&mut self, kind: DiscardedWorkKind, items: u64) {
        if items == 0 {
            return;
        }
        let total = self.counts.entry(kind).or_default();
        *total = total
            .checked_add(items)
            .assured("the messages one entity holds on one node are fewer than u64::MAX");
    }

    /// Releases every row of `batch` as done and counts the rows as `kind`.
    pub(super) fn release_batch(&mut self, kind: DiscardedWorkKind, batch: RelayRecordBatch) {
        let rows: u64 = batch.acks.len().arch_into();
        for acks in &batch.acks {
            acks.ack_success();
        }
        self.add(kind, rows);
    }

    /// Adds everything `other` counted.
    pub(super) fn merge(&mut self, other: Self) {
        for (kind, items) in other.counts {
            self.add(kind, items);
        }
    }

    /// The counts as the report entries of `entity`, in kind order.
    pub(super) fn into_discarded(self, entity: &NodeRef) -> Vec<DiscardedWork> {
        let mut discarded = Vec::with_capacity(self.counts.len());
        for (kind, items) in self.counts {
            discarded.push(DiscardedWork {
                entity_kind: entity.kind,
                entity: entity.identifier.clone(),
                kind,
                items,
            });
        }
        discarded
    }
}

/// What this node discarded for one domain in the runtime revision whose application dropped
/// entities of it.
#[derive(Debug, Clone)]
struct DropLedgerEntry {
    revision: u64,
    discarded: Vec<DiscardedWork>,
    /// Whether a coordinator collected this entry. A collected entry stays for the coordinator's
    /// retries until the domain's next drop replaces it.
    collected: bool,
}

/// The drops this node applied to one domain, until their coordinators collect them.
#[derive(Debug, Clone, Default)]
pub(crate) struct DomainDropLedger {
    entries: Vec<DropLedgerEntry>,
}

/// The runtime revisions this run of the node applied: the first tells a report for an earlier
/// revision, whose discards an earlier run made, from one this run made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AppliedRuntimeRevisions {
    first: Option<u64>,
    latest: Option<u64>,
}

impl AppliedRuntimeRevisions {
    /// The revisions once this run applied `revision`.
    fn applied(self, revision: u64) -> Self {
        let first = match self.first {
            Some(first) => first,
            None => revision,
        };
        Self {
            first: Some(first),
            latest: Some(revision),
        }
    }
}

impl Runtime {
    /// Records that this run of the node applied every domain change of `revision`.
    pub(in crate::runtime) fn record_applied_runtime_revision(&self, revision: u64) {
        self.inner
            .applied_runtime_revisions
            .send_modify(|applied| *applied = applied.applied(revision));
    }

    /// Records what applying `revision` discarded for `domain`. Collected entries of the domain's
    /// earlier drops give way to it; an entry no coordinator collected yet stays.
    pub(in crate::runtime) fn record_drop(
        &self,
        domain: &DomainName,
        revision: u64,
        discarded: Vec<DiscardedWork>,
    ) {
        let mut ledger = self.inner.drop_ledgers.entry(domain.clone()).or_default();
        ledger.entries.retain(|entry| !entry.collected);
        ledger.entries.push(DropLedgerEntry {
            revision,
            discarded,
            collected: false,
        });
    }

    /// What this node discarded for `domain` in every revision from `revision` on: pending until
    /// this node applied `revision`, and unknown when an earlier run of the node applied it.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "the drop's coordinator collects what this node discarded"
        )
    )]
    pub(crate) fn drop_report(&self, domain: &DomainName, revision: u64) -> DropReportAnswer {
        let applied = *self.inner.applied_runtime_revisions.borrow();
        let (Some(first), Some(latest)) = (applied.first, applied.latest) else {
            return DropReportAnswer::Pending;
        };
        if latest < revision {
            return DropReportAnswer::Pending;
        }
        if revision < first {
            return DropReportAnswer::Unknown;
        }
        let mut discarded = Vec::new();
        if let Some(mut ledger) = self.inner.drop_ledgers.get_mut(domain) {
            for entry in &mut ledger.entries {
                if entry.revision < revision {
                    continue;
                }
                entry.collected = true;
                discarded.extend(entry.discarded.iter().cloned());
            }
        }
        DropReportAnswer::Reported(discarded)
    }

    /// Forgets the drops of `domain` once its execution ends on this node.
    pub(in crate::runtime) fn forget_drop_ledger(&self, domain: &DomainName) {
        self.inner.drop_ledgers.remove(domain);
    }
}

impl Runtime {
    /// Destroys at once every entity this node's running execution of `domain` runs that
    /// `desired` no longer declares, and records what each discarded under `revision`.
    ///
    /// Each entity leaves the relays it reads before it is destroyed, so no publication counts it
    /// any more, and its share of every message it held, or that reaches it until it has left, is
    /// released as done. A dropped relay goes last: the registry drops every producer and consumer
    /// of a relay with it. The entities that stay keep running; nothing here pauses, drains or
    /// flushes them.
    pub(in crate::runtime) async fn destroy_dropped_entities(
        &self,
        domain: &DomainName,
        desired: &ExecutionRevision,
        revision: Option<u64>,
    ) -> error_stack::Result<(), RuntimeError> {
        let installed = {
            let Some(execution) = self.inner.executions.get(domain) else {
                return Ok(());
            };
            if execution.passive_only {
                return Ok(());
            }
            execution.revision.clone()
        };
        let mut dropped = Vec::new();
        for entity in installed.nodes.keys() {
            if !desired.nodes.contains_key(entity) {
                dropped.push(entity.clone());
            }
        }
        if dropped.is_empty() {
            return Ok(());
        }
        dropped.sort_by_key(|entity| entity.kind == ModelKind::Relay);
        info!(
            domain = domain.as_str(),
            entities = dropped.len(),
            "destroying the entities a revision dropped"
        );
        let unchanged = BTreeSet::new();
        let registrations = InstalledRegistrations::new(Some(installed.as_ref()), &unchanged);
        let mut discarded = Vec::new();
        for entity in &dropped {
            nervix_primitives::task::consume_budget().await;
            let released = match entity.kind {
                ModelKind::Emitter => {
                    self.destroy_dropped_emitter(domain, &installed, entity, &registrations)
                        .await
                }
                ModelKind::Ingestor => {
                    let destroyed = self.destroy_dropped_ingestor(domain, desired, entity).await;
                    if let Err(error) = destroyed.retirement {
                        // A retirement that failed is retried with the revision, which destroys
                        // nothing twice: what this attempt discarded is recorded now.
                        discarded.extend(destroyed.released.into_discarded(entity));
                        if let Some(revision) = revision {
                            self.record_drop(domain, revision, discarded);
                        }
                        return Err(error);
                    }
                    destroyed.released
                }
                ModelKind::Reingestor => {
                    self.destroy_dropped_reingestor(domain, &installed, entity, &registrations)
                        .await
                }
                ModelKind::Generator => self.destroy_dropped_generator(domain, entity).await,
                ModelKind::Relay => self.destroy_dropped_relay(domain, entity).await,
                ModelKind::Lookup => self.destroy_dropped_lookup(domain, entity),
                _ => {
                    self.destroy_dropped_processor(domain, &installed, entity, &registrations)
                        .await
                }
            };
            discarded.extend(released.into_discarded(entity));
            self.stop_message_error_routes_of_node(domain, entity).await;
            self.forget_dropped_entity(domain, entity);
        }
        self.withdraw_dropped_destinations(domain, &installed, &dropped);
        if let Some(revision) = revision {
            self.record_drop(domain, revision, discarded);
        }
        Ok(())
    }

    /// Takes every placement task of `entity` out of the execution.
    fn take_placement_tasks(
        execution: &mut DomainExecution,
        entity: &NodeRef,
    ) -> Vec<PlacementTasks> {
        let mut owners = Vec::new();
        for owner in execution.placement_tasks.keys() {
            if owner.node == *entity {
                owners.push(owner.clone());
            }
        }
        let mut tasks = Vec::with_capacity(owners.len());
        for owner in owners {
            if let Some(placement) = execution.placement_tasks.remove(&owner) {
                tasks.push(placement);
            }
        }
        tasks
    }

    async fn destroy_dropped_processor(
        &self,
        domain: &DomainName,
        installed: &ExecutionRevision,
        entity: &NodeRef,
        registrations: &InstalledRegistrations<'_>,
    ) -> DiscardedWorkCounts {
        let mut released = DiscardedWorkCounts::default();
        let spec = installed
            .processors
            .processor(entity.kind, &entity.identifier)
            .cloned();
        let (tasks, placements) = {
            let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                return released;
            };
            let mut owners = Vec::new();
            for owner in execution.node_tasks.keys() {
                if owner.node == *entity {
                    owners.push(owner.clone());
                }
            }
            let mut tasks = Vec::with_capacity(owners.len());
            for owner in owners {
                // The unit leaves its relays' counts before its task leaves their fan-outs.
                if let Some(spec) = &spec {
                    for relay in &spec.spec.input_relays {
                        let Some(services) = execution.relay_services.get(relay) else {
                            continue;
                        };
                        if registrations.counts(relay, entity, Some(owner.unit)) {
                            services
                                .remove_dropped_unit_runtime_consumer(spec.spec.mode, owner.unit);
                        }
                    }
                }
                if let Some(task) = execution.node_tasks.remove(&owner) {
                    tasks.push(task);
                }
            }
            (tasks, Self::take_placement_tasks(&mut execution, entity))
        };
        for task in tasks {
            match task.discard().await {
                Ok(unit_released) => released.merge(unit_released),
                Err(error) => warn!(
                    domain = domain.as_str(),
                    processor = entity.identifier.as_str(),
                    error = %format_args!("{error:#}"),
                    "a dropped processor unit ended without reporting what it released"
                ),
            }
        }
        for placement in placements {
            placement.abort().await;
        }
        released
    }

    async fn destroy_dropped_emitter(
        &self,
        domain: &DomainName,
        installed: &ExecutionRevision,
        entity: &NodeRef,
        registrations: &InstalledRegistrations<'_>,
    ) -> DiscardedWorkCounts {
        let mut released = DiscardedWorkCounts::default();
        let plan = installed
            .emitters
            .emitter(&EmitterName::from(&entity.identifier))
            .cloned();
        let tasks = {
            let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                return released;
            };
            let mut owners = Vec::new();
            for owner in execution.emitter_tasks.keys() {
                if owner.node == *entity {
                    owners.push(owner.clone());
                }
            }
            let mut tasks = Vec::with_capacity(owners.len());
            for owner in owners {
                // The unit leaves its relays' counts before its task leaves their fan-outs.
                if let Some(plan) = &plan {
                    for input in &plan.inputs {
                        let Some(services) = execution.relay_services.get(&input.relay) else {
                            continue;
                        };
                        if !registrations.counts(&input.relay, entity, None) {
                            continue;
                        }
                        match plan.input_units(owner.unit, input) {
                            EmitterInputUnits::WholeRelay => services
                                .remove_dropped_unit_runtime_consumer(plan.mode, Unit::SINGLE),
                            EmitterInputUnits::Units(relay_units) => {
                                for relay_unit in relay_units {
                                    services.remove_dropped_unit_runtime_consumer(
                                        plan.mode, relay_unit,
                                    );
                                }
                            }
                        }
                    }
                }
                if let Some(task) = execution.emitter_tasks.remove(&owner) {
                    tasks.push(task);
                }
            }
            tasks
        };
        for task in tasks {
            match task.discard(self.task_stop_grace()).await {
                Some(unit_released) => released.merge(unit_released),
                None => warn!(
                    domain = domain.as_str(),
                    emitter = entity.identifier.as_str(),
                    "a dropped emitter unit ended without reporting what it released"
                ),
            }
        }
        released
    }

    async fn destroy_dropped_reingestor(
        &self,
        domain: &DomainName,
        installed: &ExecutionRevision,
        entity: &NodeRef,
        registrations: &InstalledRegistrations<'_>,
    ) -> DiscardedWorkCounts {
        let plan = installed
            .entrypoints
            .reingestor(&ReingestorName::from(&entity.identifier))
            .cloned();
        let (tasks, routes) = {
            let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                return DiscardedWorkCounts::default();
            };
            // The reingestor's tasks leave their relays' counts before they leave the fan-outs.
            let tasks = execution.reingestor_tasks.remove(entity);
            if tasks.is_some()
                && let Some(plan) = &plan
            {
                for input in &plan.inputs {
                    let Some(services) = execution.relay_services.get(&input.relay) else {
                        continue;
                    };
                    if registrations.counts(&input.relay, entity, None) {
                        services.remove_dropped_unit_runtime_consumer(plan.mode, Unit::SINGLE);
                    }
                }
            }
            (
                tasks,
                execution.branched_entrypoints.remove(&entity.identifier),
            )
        };
        let tasks = tasks.unwrap_or_default();
        let released = self
            .discard_reingestor_tasks(domain, entity, tasks.len())
            .await;
        for task in tasks {
            nervix_primitives::task::consume_budget().await;
            let mut task = task.into_task();
            if nervix_primitives::time::timeout(self.task_stop_grace(), &mut task)
                .await
                .is_err()
            {
                task.abort();
                task.join_after_shutdown("dropped reingestor").await;
            }
        }
        // The reingestor's routes publish what they already built, as an entry point's routes do
        // when it ends, with no limit of their own.
        for route in routes.into_iter().flatten() {
            route.stop(RouteStop::Finish).await;
        }
        released
    }

    async fn destroy_dropped_generator(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
    ) -> DiscardedWorkCounts {
        let tasks = {
            let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                return DiscardedWorkCounts::default();
            };
            let mut owners = Vec::new();
            for owner in execution.generator_tasks.keys() {
                if owner.node == *entity {
                    owners.push(owner.clone());
                }
            }
            let mut tasks = Vec::with_capacity(owners.len());
            for owner in owners {
                if let Some(task) = execution.generator_tasks.remove(&owner) {
                    tasks.push(task);
                }
            }
            tasks
        };
        // A generator's records come from no source, so ending it leaves no share to release.
        for task in tasks {
            nervix_primitives::task::consume_budget().await;
            let task = task.into_task();
            task.abort();
            task.join_after_shutdown("dropped generator").await;
        }
        DiscardedWorkCounts::default()
    }

    async fn destroy_dropped_ingestor(
        &self,
        domain: &DomainName,
        desired: &ExecutionRevision,
        entity: &NodeRef,
    ) -> IngestorDestruction {
        let mut released = DiscardedWorkCounts::default();
        let ingestor = IngestorName::from(&entity.identifier);
        let dispatcher = self.inner.remote_dispatcher.load_full();
        let local_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        // What the ingestor keeps at its broker for this node, such as a subscription it owns, is
        // read from the installed plan before its units stop, and given up once they have.
        let retirement = self
            .retiring_ingestor_inputs(domain, Some(desired), local_node_id)
            .remove(&ingestor);
        let mut units = Vec::new();
        for running in self.inner.ingestors.iter() {
            let key = running.key();
            if &key.entity.domain == domain && key.entity.node == *entity {
                units.push(key.unit);
            }
        }
        for unit in units {
            nervix_primitives::task::consume_budget().await;
            let key =
                DomainNodeUnitRef::new(DomainNodeRef::new(domain.clone(), entity.clone()), unit);
            // What the unit retained without admitting it ends with it.
            if let Some((_, control)) = self.inner.ingestor_quiescence.remove(&key) {
                released.add(
                    DiscardedWorkKind::RetainedPayloads,
                    control.counters().buffered_records.arch_into(),
                );
            }
            // What the unit already admitted travels on: its routes finish with no limit of their
            // own.
            if let Err(error) = self
                .stop_ingestor_unit(domain, &ingestor, unit, RouteStop::Finish)
                .await
            {
                warn!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    unit = ?unit,
                    error = %format_args!("{error:#}"),
                    "a dropped ingestor unit did not stop"
                );
            }
        }
        let settled = Box::pin(self.settle_mqtt_session_retirements_for_rebuild(
            domain,
            Some(desired),
            local_node_id,
        ))
        .await;
        let retirement = match (settled, retirement) {
            (Err(error), _) => Err(error),
            (Ok(()), Some(retirement)) => Box::pin(self.retire_ingestor_input(retirement)).await,
            (Ok(()), None) => Ok(()),
        };
        IngestorDestruction {
            released,
            retirement,
        }
    }

    async fn destroy_dropped_relay(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
    ) -> DiscardedWorkCounts {
        let mut released = DiscardedWorkCounts::default();
        let relay = RelayName::from(&entity.identifier);
        let fanout = DomainNodeRef::new(domain.clone(), entity.clone());
        if let Some((_, fanout)) = self.inner.relay_boundary_fanouts.remove(&fanout) {
            released.add(
                DiscardedWorkKind::RelayMessages,
                fanout.outstanding_work_len().arch_into(),
            );
            // The relay no longer exists, so its session subscriptions end here, as they end
            // when a rebuild finds a relay no longer declared.
            fanout.subscriptions().withdraw();
        }
        let (owner_tasks, state_tasks, placements, services) = {
            let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                return released;
            };
            let owner_tasks = execution.take_relay_owner_tasks(&relay);
            let state_tasks = execution.take_relay_state_tasks(&relay);
            let placements = Self::take_placement_tasks(&mut execution, entity);
            // The services stay registered, marked dropped, until the execution is rebuilt: a batch
            // another node sent before it applied the drop finds them and counts as done.
            let services = execution.routing.relay_services.get(&relay).cloned();
            if let Some(services) = &services {
                services.mark_dropped();
            }
            execution.routing.materialized_stream_specs.remove(&relay);
            execution.routing.materialized_stream_reads.remove(&relay);
            execution
                .routing
                .materialized_stream_owner_nodes
                .remove(&relay);
            execution.routing.relay_transitions.remove(&relay);
            (owner_tasks, state_tasks, placements, services)
        };
        for task in owner_tasks {
            nervix_primitives::task::consume_budget().await;
            if let Err(error) = task
                .stop(TaskStopWait::Teardown {
                    grace: self.branch_task_stop_timeout(),
                })
                .await
            {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    error = %format_args!("{error:#}"),
                    "a dropped relay's owner task did not stop cleanly"
                );
            }
        }
        for task in state_tasks {
            nervix_primitives::task::consume_budget().await;
            if let Err(error) = task.stop(self.teardown_task_stop()).await {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    error = %format_args!("{error:#}"),
                    "a dropped relay's state task did not stop cleanly"
                );
            }
        }
        for placement in placements {
            placement.abort().await;
        }
        if let Some(services) = services {
            services.retire_channels();
        }
        released
    }

    fn destroy_dropped_lookup(&self, domain: &DomainName, entity: &NodeRef) -> DiscardedWorkCounts {
        if let Some(mut execution) = self.inner.executions.get_mut(domain) {
            execution
                .routing
                .lookups
                .remove(&LookupName::from(&entity.identifier));
        }
        DiscardedWorkCounts::default()
    }

    /// Forgets the per-unit bookkeeping this node kept for `entity`, which nothing reads once it
    /// is destroyed.
    fn forget_dropped_entity(&self, domain: &DomainName, entity: &NodeRef) {
        self.inner
            .node_quiesce_counters
            .retain(|key, _| !(&key.entity.domain == domain && key.entity.node == *entity));
        self.inner
            .emitter_buffers
            .retain(|key, _| !(&key.entity.domain == domain && key.entity.node == *entity));
    }

    /// Withdraws, from every relay this node owns, the remote consumers `dropped` names, so an
    /// owner here stops routing to consumers a drop destroyed on another node. What it still
    /// routes to is computed from the `installed` revision, so nothing else of the revision that
    /// drops them is installed early: a unit it moves, or a consumer it adds or alters, waits for
    /// the schedule application that installs it behind its dispatch fence. Owner routes stay as
    /// they are for the same reason.
    fn withdraw_dropped_destinations(
        &self,
        domain: &DomainName,
        installed: &ExecutionRevision,
        dropped: &[NodeRef],
    ) {
        let dispatcher = self.inner.remote_dispatcher.load_full();
        let Some(local_node_id) = dispatcher.as_deref().map(RemoteDispatcher::local_node_id) else {
            return;
        };
        let left_out = dropped.iter().cloned().collect::<HashSet<_>>();
        let remaining =
            Self::remote_runtime_consumers_leaving_out(installed, local_node_id, &left_out);
        let Some(execution) = self.inner.executions.get(domain) else {
            return;
        };
        for (relay, services) in &execution.relay_services {
            let relay_remaining = match remaining.get(relay) {
                Some(consumers) => consumers.clone(),
                None => Vec::new(),
            };
            services.withdraw_dropped_remote_runtime_consumers(relay_remaining);
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::{ModelKind, ModelName};

    use super::*;

    fn named(name: &str) -> ModelName {
        ModelName::parse(name).assured("the test entity name is valid")
    }

    #[test]
    fn counts_merge_by_kind_and_skip_kinds_that_held_nothing() {
        let mut counts = DiscardedWorkCounts::default();
        counts.add(DiscardedWorkKind::HeldRows, 3);
        counts.add(DiscardedWorkKind::WaitingMessages, 0);
        let mut more = DiscardedWorkCounts::default();
        more.add(DiscardedWorkKind::HeldRows, 1);
        more.add(DiscardedWorkKind::UnprocessedMessages, 2);
        counts.merge(more);

        let entity = NodeRef::new(ModelKind::Reorderer, named("order_rows"));
        assert_eq!(
            counts.into_discarded(&entity),
            vec![
                DiscardedWork {
                    entity_kind: ModelKind::Reorderer,
                    entity: named("order_rows"),
                    kind: DiscardedWorkKind::HeldRows,
                    items: 4,
                },
                DiscardedWork {
                    entity_kind: ModelKind::Reorderer,
                    entity: named("order_rows"),
                    kind: DiscardedWorkKind::UnprocessedMessages,
                    items: 2,
                },
            ]
        );
    }

    #[nervix_primitives::test]
    async fn a_drop_report_waits_for_its_revision_and_knows_only_this_run() {
        let runtime = Runtime::default();
        let domain = DomainName::parse("dropping").assured("the test domain name is valid");
        let held = DiscardedWork {
            entity_kind: ModelKind::Emitter,
            entity: named("published"),
            kind: DiscardedWorkKind::UndeliveredRecords,
            items: 2,
        };
        assert_eq!(runtime.drop_report(&domain, 3), DropReportAnswer::Pending);

        runtime.record_applied_runtime_revision(3);
        assert_eq!(
            runtime.drop_report(&domain, 3),
            DropReportAnswer::Reported(Vec::new()),
            "a node that ran none of the dropped entities discarded nothing"
        );
        assert_eq!(
            runtime.drop_report(&domain, 2),
            DropReportAnswer::Unknown,
            "an earlier run of the node applied that revision"
        );
        assert_eq!(runtime.drop_report(&domain, 4), DropReportAnswer::Pending);

        runtime.record_drop(&domain, 4, vec![held.clone()]);
        runtime.record_applied_runtime_revision(4);
        assert_eq!(
            runtime.drop_report(&domain, 4),
            DropReportAnswer::Reported(vec![held.clone()])
        );
        assert_eq!(
            runtime.drop_report(&domain, 4),
            DropReportAnswer::Reported(vec![held]),
            "a coordinator's retry reads the same report"
        );

        // The domain's next drop replaces the collected report.
        runtime.record_drop(&domain, 5, Vec::new());
        runtime.record_applied_runtime_revision(5);
        assert_eq!(
            runtime.drop_report(&domain, 4),
            DropReportAnswer::Reported(Vec::new())
        );
        runtime.forget_drop_ledger(&domain);
        assert_eq!(
            runtime.drop_report(&domain, 5),
            DropReportAnswer::Reported(Vec::new())
        );
    }

    #[test]
    fn applied_revisions_keep_the_first_revision_of_the_run() {
        let applied = AppliedRuntimeRevisions::default().applied(7).applied(9);
        assert_eq!(
            applied,
            AppliedRuntimeRevisions {
                first: Some(7),
                latest: Some(9),
            }
        );
    }
}
