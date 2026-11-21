//! Application of validated schedules to one node's runtime.
//!
//! Layer: data plane.
//! - **Owns.** Materializing, replacing and removing local tasks named by an execution schedule.
//! - **Depends on.** Typed schedules, runtime lifecycle handles and installed domain capabilities.
//! - **Must not know.** Plexus parsing, registry validation or placement-policy computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "schedule publication installs one typed execution revision and its ownership \
                  bindings"
    )
)]

use error_stack::ResultExt as _;

use super::{
    entity_swap::{DeferredSwapWork, EntitySwapContext},
    entity_swap_restore::{EntitySwapJournal, EntitySwapStep, IngestorSwap, ReboundNodeSwap},
    ingestor_start::RetiringIngestorInput,
    partition_claims::UnitPartitionTransition,
    relay_repartition::InstalledRegistrations,
    *,
};

/// What one node has applied of the cluster schedule its leader publishes.
///
/// The applied revision lives behind the same lock that serializes application, so the revision a
/// node acts on and the revision it records cannot be read apart. Before the first successful
/// application there is no applied revision, which absence states directly: every value a
/// published revision can take, including the largest one, is an ordinary revision that still
/// suppresses the stale revisions following it.
#[derive(Debug, Default)]
pub(in crate::runtime) struct ScheduleApplication {
    applied_revision: Option<u64>,
}

/// How a domain's schedule delta reached its running execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ScheduleDeltaApplication {
    /// The running execution took the delta in place.
    Incremental,
    /// The delta replaces the domain's execution as a whole, which only a domain that is not
    /// running its previous revision in place takes.
    RebuildRequired,
}

/// What one entity swap replaces on this node, beside the revision it installs.
struct SwapTargets<'a> {
    entities: &'a [NodeRef],
    explicitly_replaced: &'a BTreeSet<NodeRef>,
    reassignments: &'a [NodeRef],
    dynamic_updates: &'a [DynamicExecutionUpdate],
    state_purges: &'a BTreeMap<NodeRef, Vec<nervix_models::StatePurge>>,
    /// The revision the domain's execution ran before the swap.
    installed: Option<&'a ExecutionRevision>,
    local_node_id: Option<&'a ClusterNodeName>,
}

/// What publishing a swapped revision needs once every entity of it has been replaced.
struct SwapPublication {
    processor_plans: HashMap<NodeUnitRef, StdArc<PublishedProcessorPlan>>,
    message_error_plans: Arc<BoundMessageErrorRoutes>,
    materialized_routing_changed: bool,
}

impl ScheduleApplication {
    /// Whether `revision` carries cluster state this node has not applied. The first revision a
    /// node sees always does, and one at or below the applied revision carries nothing newer.
    fn advances_beyond_applied(&self, revision: u64) -> bool {
        match self.applied_revision {
            None => true,
            Some(applied) => revision > applied,
        }
    }

    /// Record `revision` as this node's applied revision, once its schedule has been applied.
    fn record_applied(&mut self, revision: u64) {
        self.applied_revision = Some(revision);
    }
}

impl Runtime {
    #[cfg(test)]
    pub(in crate::runtime) async fn apply_cluster_schedule(
        &self,
        local_node_id: &ClusterNodeName,
        schedule: &ClusterSchedule,
    ) -> error_stack::Result<(), RuntimeError> {
        let _application = self.inner.schedule_application.lock().await;
        let applied = self.inner.test_applied_schedule.load_full();
        let revision_plan = PlannedClusterRevision::between(applied.as_deref(), schedule)
            .change_context(RuntimeError::PlanScheduleRevision)?;
        Box::pin(self.apply_cluster_schedule_locked(local_node_id, revision_plan, true)).await?;
        self.inner
            .test_applied_schedule
            .store(Some(StdArc::new(schedule.clone())));
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn apply_cluster_state(
        &self,
        local_node_id: &ClusterNodeName,
        revision: u64,
        domains: &BTreeMap<DomainName, DomainState>,
        domain_clock_authorities: &BTreeMap<DomainName, DomainClockAuthority>,
        schedule: &ClusterSchedule,
    ) -> error_stack::Result<(), RuntimeError> {
        let applied = self.inner.test_applied_schedule.load_full();
        let revision_plan = PlannedClusterRevision::between(applied.as_deref(), schedule)
            .change_context(RuntimeError::PlanScheduleRevision)?;
        self.apply_planned_cluster_state(
            local_node_id,
            revision,
            domains,
            domain_clock_authorities,
            revision_plan,
        )
        .await?;
        self.inner
            .test_applied_schedule
            .store(Some(StdArc::new(schedule.clone())));
        Ok(())
    }

    pub(crate) async fn apply_planned_cluster_state(
        &self,
        local_node_id: &ClusterNodeName,
        revision: u64,
        domains: &BTreeMap<DomainName, DomainState>,
        domain_clock_authorities: &BTreeMap<DomainName, DomainClockAuthority>,
        revision_plan: PlannedClusterRevision,
    ) -> error_stack::Result<(), RuntimeError> {
        let mut application = self.inner.schedule_application.lock().await;
        if !application.advances_beyond_applied(revision) {
            return Ok(());
        }

        Box::pin(self.capture_stop_teardowns(domains)).await;
        self.sync_committed_domains(revision, domains, domain_clock_authorities);
        // A failed application records nothing, so the same revision is applied again rather than
        // being suppressed as one this node already holds.
        Box::pin(self.apply_cluster_schedule_locked(local_node_id, revision_plan, false)).await?;
        application.record_applied(revision);
        Ok(())
    }

    pub(super) async fn apply_cluster_schedule_locked(
        &self,
        local_node_id: &ClusterNodeName,
        revision_plan: PlannedClusterRevision,
        start_ingestors: bool,
    ) -> error_stack::Result<(), RuntimeError> {
        // Delta application and full rebuild each own substantial state. Poll them indirectly so
        // applying a cluster revision does not embed both state machines in this coordinator.
        let scheduled_domains = revision_plan
            .domains
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let existing_domains = {
            self.inner
                .executions
                .iter()
                .map(|entry| entry.key().clone())
                .collect::<std::collections::BTreeSet<_>>()
        };
        let existing_passive_only = {
            self.inner
                .executions
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().passive_only))
                .collect::<HashMap<_, _>>()
        };
        let existing_start_versions = {
            self.inner
                .executions
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().start_version))
                .collect::<HashMap<_, _>>()
        };
        for domain in existing_domains.difference(&scheduled_domains) {
            match Box::pin(self.rebuild_domain_from_revision(
                local_node_id,
                domain,
                None,
                start_ingestors,
            ))
            .await
            {
                Ok(()) => {
                    self.inner.domain_instantiation_errors.remove(domain);
                }
                Err(error) => {
                    self.inner
                        .domain_instantiation_errors
                        .insert(domain.clone(), format!("{error:#}"));
                    return Err(error);
                }
            }
        }

        for (domain_id, change) in &revision_plan.domains {
            let domain = &change.revision;
            // An earlier attempt at this revision may have applied it to this domain before a
            // later domain failed. The domain then runs its desired schedule already and takes no
            // change, although it no longer matches the predecessor the delta was planned against.
            let (already_applied, predecessor_matches) = match self.inner.executions.get(domain_id)
            {
                Some(execution) => (
                    execution.revision.source_digest == domain.source_digest,
                    Some(execution.revision.source_digest) == change.predecessor_digest,
                ),
                None => (false, false),
            };
            let predecessor_matches = predecessor_matches || already_applied;
            let delta = if already_applied {
                &ExecutionDelta::Unchanged
            } else {
                &change.delta
            };
            let Some(domain_state) = self.inner.domains.get(domain_id) else {
                continue;
            };
            let domain_status = domain_state.status.clone();
            let desired_start_version = domain_state.start_version;
            drop(domain_state);

            if let nervix_models::DomainStatus::Paused = domain_status {
                self.engage_domain_ingestor_quiesce(&domain.domain);
            }
            // A stopping domain's intake ends on every node at once, whatever each ingestor's
            // `ON QUIESCE` declares; its graph runs on while the work in motion finishes.
            if let nervix_models::DomainStatus::Stopping = domain_status {
                self.engage_domain_stop_intake(&domain.domain);
            }
            let desired_passive_only =
                matches!(domain_status, nervix_models::DomainStatus::Stopped);
            if !matches!(delta, ExecutionDelta::Unchanged)
                || !existing_domains.contains(&domain.domain)
                || !predecessor_matches
                || existing_passive_only.get(&domain.domain) != Some(&desired_passive_only)
                || existing_start_versions.get(&domain.domain) != Some(&desired_start_version)
            {
                let delta_application = if !desired_passive_only
                    && predecessor_matches
                    && existing_passive_only.get(&domain.domain) == Some(&desired_passive_only)
                    && existing_start_versions.get(&domain.domain) == Some(&desired_start_version)
                {
                    Box::pin(self.apply_schedule_delta(domain, delta)).await?
                } else {
                    ScheduleDeltaApplication::RebuildRequired
                };
                if let ScheduleDeltaApplication::RebuildRequired = delta_application {
                    match Box::pin(self.rebuild_domain_from_revision(
                        local_node_id,
                        &domain.domain,
                        Some(domain.clone()),
                        start_ingestors,
                    ))
                    .await
                    {
                        Ok(()) => {
                            self.inner
                                .domain_instantiation_errors
                                .remove(&domain.domain);
                        }
                        Err(error) => {
                            self.inner
                                .domain_instantiation_errors
                                .insert(domain.domain.clone(), format!("{error:#}"));
                            return Err(error);
                        }
                    }
                }
            }

            // A stopping domain withdraws its endpoints once its intake holds, so a request that
            // arrives later finds no route, and withdraws them again after a rebuild published
            // them. A request admitted before the stop finishes through the intake it borrowed. A
            // domain that runs again, its stop called off, publishes them again.
            match domain_status {
                nervix_models::DomainStatus::Stopping => {
                    self.inner
                        .endpoint_intake_routes
                        .withdraw_stopping_domain(&domain.domain);
                }
                nervix_models::DomainStatus::Running | nervix_models::DomainStatus::Paused => {
                    self.inner
                        .endpoint_intake_routes
                        .restore_withdrawn_domain(&domain.domain);
                }
                nervix_models::DomainStatus::Stopped => {}
            }

            if let nervix_models::DomainStatus::Running = domain_status {
                let build = || RuntimeError::build_domain_execution(&domain.domain);
                self.purge_stale_runtime_state(&domain.domain)
                    .change_context(ExecutionBuildError::PurgeStaleState)
                    .change_context_lazy(build)?;
                if start_ingestors {
                    Box::pin(self.start_missing_domain_ingestors(&domain.domain))
                        .await
                        .change_context_lazy(build)?;
                }
                self.release_domain_ingestor_quiesce(&domain.domain);
                self.release_domain_stop_intake(&domain.domain);
            }
        }
        self.retain_assigned_wasm_modules(local_node_id, &revision_plan);
        // Every domain of the revision now publishes its routes, so the endpoint listener answers
        // a route none of them publishes as missing rather than as not yet installed.
        self.inner.endpoint_intake_routes.mark_installed();

        Ok(())
    }

    /// Applies a changed schedule without tearing the domain down when the delta allows it, and
    /// answers [`ScheduleDeltaApplication::RebuildRequired`] when the delta replaces the domain's
    /// execution as a whole. An entity swap that fails is the delta's failure: the swap leaves the
    /// entities it was replacing running, and nothing wider stands in for it.
    pub(super) async fn apply_schedule_delta(
        &self,
        desired: &Arc<ExecutionRevision>,
        delta: &ExecutionDelta,
    ) -> error_stack::Result<ScheduleDeltaApplication, RuntimeError> {
        match delta {
            ExecutionDelta::Unchanged => Ok(ScheduleDeltaApplication::Incremental),
            ExecutionDelta::Dynamic(updates) => {
                Box::pin(self.apply_dynamic_schedule_update(
                    &desired.domain,
                    desired.clone(),
                    updates,
                ))
                .await?;
                Ok(ScheduleDeltaApplication::Incremental)
            }
            ExecutionDelta::EntitySwap(change) => {
                Box::pin(self.swap_scheduled_nodes(&desired.domain, desired.clone(), change))
                    .await?;
                Ok(ScheduleDeltaApplication::Incremental)
            }
            ExecutionDelta::Rebuild => Ok(ScheduleDeltaApplication::RebuildRequired),
        }
    }

    /// The reassigned nodes whose runtime must change on this cluster node: those that started or
    /// stopped executing here. A node that keeps executing here across a reassignment keeps its
    /// task, its buffers, and its branch-local state. Relays are excluded because their ownership
    /// and optional state task are rebound with the placement runtime, and lookups because they
    /// load on every cluster node regardless of assignment.
    pub(super) fn locally_relocated_nodes(
        &self,
        domain: &DomainName,
        desired: &ExecutionRevision,
        reassignments: &[NodeRef],
    ) -> Vec<NodeRef> {
        let dispatcher = self.inner.remote_dispatcher.load();
        let Some(dispatcher) = dispatcher.as_deref() else {
            return Vec::new();
        };
        let local_node_id = dispatcher.local_node_id();
        let Some(execution) = self.inner.executions.get(domain) else {
            return Vec::new();
        };
        reassignments
            .iter()
            .filter(|entity| {
                entity.kind != ModelKind::Relay
                    && entity.kind != ModelKind::Ingestor
                    && entity.kind != ModelKind::Lookup
                    && Self::scheduled_node(&execution.revision, entity).is_some_and(|existing| {
                        Self::scheduled_node(desired, entity).is_some_and(|desired_node| {
                            existing.units_executing_on(local_node_id)
                                != desired_node.units_executing_on(local_node_id)
                        })
                    })
            })
            .cloned()
            .collect()
    }

    pub(super) fn scheduled_node<'a>(
        revision: &'a ExecutionRevision,
        entity: &NodeRef,
    ) -> Option<&'a ExecutionNode> {
        revision.nodes.get(entity)
    }

    pub(super) fn processor_units_to_swap(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        desired: &ExecutionRevision,
        replace_all: bool,
        local: Option<&ClusterNodeName>,
    ) -> Vec<Unit> {
        let execution = self.inner.executions.get(domain);
        let previous = execution
            .as_ref()
            .and_then(|execution| execution.revision.nodes.get(entity));
        let next = desired.nodes.get(entity);
        let mut units = BTreeSet::new();
        if let Some(previous) = previous {
            units.extend(previous.units.keys().copied());
        }
        if let Some(next) = next {
            units.extend(next.units.keys().copied());
        }
        units
            .into_iter()
            .filter(|unit| {
                replace_all
                    || previous.is_some_and(|node| {
                        Self::scheduled_unit_executes_locally(node, *unit, local)
                    }) != next.is_some_and(|node| {
                        Self::scheduled_unit_executes_locally(node, *unit, local)
                    })
            })
            .collect()
    }

    /// Rebuilds the placement-derived runtime of every reassigned node: the replicated states this
    /// cluster node owns or replicates for it, including a materialized relay's state task. Nodes
    /// the schedule did not reassign are never touched.
    pub(super) async fn rebind_reassigned_nodes(
        &self,
        domain: &DomainName,
        revision: &ExecutionRevision,
        reassignments: &[NodeRef],
        local_node_id: Option<&ClusterNodeName>,
        repartitioned: &BTreeSet<RelayName>,
        journal: &mut EntitySwapJournal,
    ) -> error_stack::Result<bool, RuntimeError> {
        let Some(local_node_id) = local_node_id else {
            return Ok(false);
        };
        if reassignments.is_empty() {
            return Ok(false);
        }
        let build = || RuntimeError::build_domain_execution(domain);
        let activation_plan = &revision.activation;
        let shutdown = match self.inner.executions.get(domain) {
            Some(execution) => execution.shutdown.clone(),
            None => {
                return Err(Report::new(ExecutionBuildError::ExecutionUnavailable {
                    step: ExecutionStep::ScheduleReassignment,
                })
                .change_context(build()));
            }
        };
        let mut relay_states_moved = false;
        let schedule_fingerprint = revision.ownership_handoff_fingerprint;
        for entity in reassignments {
            nervix_primitives::task::consume_budget().await;
            let Some(desired_node) = Self::scheduled_node(revision, entity) else {
                return Err(Report::new(ExecutionBuildError::MissingDesiredNode {
                    node: entity.clone(),
                })
                .change_context(build()));
            };
            // A relay is rebound unit by unit: the units this node owned, the owner each unit had,
            // and the units it owns now decide which owner and state tasks stop and start.
            let mut ingestor_layout_replaced = false;
            let mut changed_units = desired_node.units.keys().copied().collect::<BTreeSet<_>>();
            let (previous_local_units, previous_unit_owners) = if let Some(execution) =
                self.inner.executions.get(domain)
                && let Some(existing) = Self::scheduled_node(&execution.revision, entity)
            {
                ingestor_layout_replaced = entity.kind == ModelKind::Ingestor
                    && existing.units.keys().ne(desired_node.units.keys());
                if ingestor_layout_replaced {
                    // Retire the previous placement tasks even for units removed by a shrink.
                    // Then bind prepared state for every replacement, including retained owners.
                    changed_units.extend(existing.units.keys().copied());
                } else {
                    changed_units.retain(|unit| {
                        existing.units.get(unit).is_none_or(|previous| {
                            let desired = &desired_node.units[unit];
                            previous.primary_node != desired.primary_node
                                || previous.assigned_nodes != desired.assigned_nodes
                        })
                    });
                }
                let local = existing
                    .units_executing_on(local_node_id)
                    .intersection(&changed_units)
                    .copied()
                    .collect::<BTreeSet<_>>();
                (local, existing.unit_execution_nodes())
            } else {
                (BTreeSet::new(), BTreeMap::new())
            };
            let desired_local_units = desired_node
                .units_executing_on(local_node_id)
                .intersection(&changed_units)
                .copied()
                .collect::<BTreeSet<_>>();
            let step = journal.record(ReboundNodeSwap::new(entity.clone(), changed_units.clone()));
            if entity.kind == ModelKind::Ingestor {
                for unit in previous_local_units.difference(&desired_local_units) {
                    let key = DomainNodeUnitRef::new(entity.in_domain(domain), *unit);
                    if !ingestor_layout_replaced || self.inner.ingestors.contains_key(&key) {
                        self.stop_ingestor_unit(
                            domain,
                            &IngestorName::from(&entity.identifier),
                            *unit,
                        )
                        .await
                        .change_context_lazy(build)?;
                        journal.step(step).record_ingestor_unit_stopped(*unit);
                    }
                    self.withdraw_ingestor_unit_metrics(
                        domain,
                        &IngestorName::from(&entity.identifier),
                        *unit,
                    );
                }
            }

            self.activate_prepared_forced_ownership_recovery_state(
                domain,
                desired_node,
                local_node_id,
                schedule_fingerprint,
                true,
            )
            .change_context_lazy(|| ExecutionBuildError::ActivateForcedRecoveryState {
                node: desired_node.identity(),
            })
            .change_context_lazy(build)?;
            self.activate_prepared_ownership_handoff_state(
                domain,
                desired_node,
                local_node_id,
                schedule_fingerprint,
                true,
            )
            .change_context_lazy(|| ExecutionBuildError::ActivateHandoffState {
                node: desired_node.identity(),
            })
            .change_context_lazy(build)?;
            let relay_services = if entity.kind == ModelKind::Relay
                && let Some(execution) = self.inner.executions.get(domain)
                && let Some(services) = execution
                    .relay_services
                    .get(&RelayName::from(&entity.identifier))
            {
                Some(services.clone())
            } else {
                None
            };
            // A relay the swap repartitions has its owner and state tasks replaced with its
            // services, by the repartition and the swap's relay transition.
            let repartitioned_relay = entity.kind == ModelKind::Relay
                && repartitioned.contains(&RelayName::from(&entity.identifier));

            if entity.kind == ModelKind::Relay && !repartitioned_relay {
                let relay = RelayName::from(&entity.identifier);
                // Every leaving unit's owner ends, even after one fails to drain: an owner that
                // does not drain within its grace is aborted, and a restore starts each again.
                let mut drain_failure = None;
                for unit in previous_local_units.difference(&desired_local_units) {
                    let previous =
                        self.inner
                            .executions
                            .get_mut(domain)
                            .and_then(|mut execution| {
                                execution
                                    .relay_owner_tasks
                                    .remove(&RelayUnitRef::new(relay.clone(), *unit))
                            });
                    if let Some(task) = previous {
                        let stopped = task.stop(self.branch_task_stop_timeout()).await;
                        journal.step(step).record_owner_stopped(*unit);
                        if let Err(error) = stopped
                            && drain_failure.is_none()
                        {
                            drain_failure = Some(error);
                        }
                    }
                }
                if let Some(error) = drain_failure {
                    return Err(error
                        .change_context(ExecutionBuildError::DrainRelay {
                            relay: relay.clone(),
                        })
                        .change_context(build()));
                }
            }
            let previous_tasks = if let Some(mut execution) = self.inner.executions.get_mut(domain)
            {
                changed_units
                    .iter()
                    .filter_map(|unit| {
                        execution
                            .placement_tasks
                            .remove(&NodeUnitRef::new(entity.clone(), *unit))
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if !previous_tasks.is_empty() {
                journal.step(step).placement().record_aborted();
            }
            for previous in previous_tasks {
                nervix_primitives::task::consume_budget().await;
                previous.abort().await;
            }

            let state = PlacedNodeState::of(desired_node, revision);
            let materialized_relay = if let Some(PlacedNodeState::MaterializedRelay(_)) = &state {
                Some(RelayName::from(&entity.identifier))
            } else {
                None
            };
            if let Some(relay) = materialized_relay.as_ref()
                && let Some(PlacedNodeState::MaterializedRelay(schema)) = &state
            {
                for unit in desired_node.units.keys() {
                    let state_placement = self
                        .state_placement_for_unit(
                            domain,
                            RuntimeStateKind::MaterializedRelay,
                            ModelKind::Relay,
                            relay,
                            *unit,
                            None,
                        )
                        .change_context_lazy(|| ExecutionBuildError::PlaceState {
                            node: entity.clone(),
                        })
                        .change_context_lazy(build)?;
                    self.prepare_materialized_stream_restore(&state_placement, schema)
                        .await
                        .change_context_lazy(|| ExecutionBuildError::PrepareMaterializedRestore {
                            relay: relay.clone(),
                        })
                        .change_context_lazy(build)?;
                }
            }
            for unit in desired_local_units.difference(&previous_local_units) {
                let (Some(relay), Some(previous_owner)) =
                    (materialized_relay.as_ref(), previous_unit_owners.get(unit))
                else {
                    continue;
                };
                let state_placement = self
                    .state_placement_for_unit(
                        domain,
                        RuntimeStateKind::MaterializedRelay,
                        ModelKind::Relay,
                        relay,
                        *unit,
                        None,
                    )
                    .change_context_lazy(|| ExecutionBuildError::PlaceState {
                        node: entity.clone(),
                    })
                    .change_context_lazy(build)?;
                let local_replica = self
                    .inner
                    .replicated_materialized_stream_states
                    .get(&state_placement)
                    .map(|state| state.clone());
                if let Some(local_replica) = local_replica {
                    let read = ReplicatedMaterializedRelayState::read(&local_replica);
                    let after_lsm = read.current_lsm();
                    let Some(installer) =
                        ReplicatedMaterializedRelayState::current_installer(&local_replica)
                    else {
                        return Err(Report::new(ExecutionBuildError::PromotedReplicaInstaller {
                            relay: relay.clone(),
                        })
                        .change_context(build()));
                    };
                    let _installed_revision = self
                        .install_materialized_snapshot_from(
                            previous_owner,
                            &read,
                            &installer,
                            Some(after_lsm),
                        )
                        .await
                        .change_context_lazy(|| ExecutionBuildError::RefreshPromotedReplica {
                            relay: relay.clone(),
                        })
                        .change_context_lazy(build)?;
                }
            }
            // A unit this node keeps owning keeps its state, but the placement below rebinds that
            // state to a new assignment generation, which retires the originator the unit's running
            // state task applies rows through. Each such task therefore stops first, applying the
            // rows already queued to it under the generation they were dispatched in, and the unit
            // starts a new task once the placement has issued the successor. The relay's local
            // dispatch stays gated meanwhile, so nothing is queued between the two.
            if let Some(relay) = materialized_relay.as_ref()
                && !repartitioned_relay
            {
                let mut stop_failure = None;
                for unit in previous_local_units.intersection(&desired_local_units) {
                    let previous =
                        self.inner
                            .executions
                            .get_mut(domain)
                            .and_then(|mut execution| {
                                execution
                                    .relay_state_tasks
                                    .remove(&RelayUnitRef::new(relay.clone(), *unit))
                            });
                    let Some(task) = previous else {
                        continue;
                    };
                    let stopped = task.stop(self.task_stop_grace()).await;
                    let withdrawn = if let Some(services) = relay_services.as_ref()
                        && !desired_node.is_holding_unit(*unit)
                    {
                        services.remove_local_unit_runtime_consumer(AckMode::Detached, *unit);
                        true
                    } else {
                        false
                    };
                    journal
                        .step(step)
                        .placement()
                        .record_state_task_stopped(*unit, withdrawn);
                    if let Err(error) = stopped
                        && stop_failure.is_none()
                    {
                        stop_failure = Some(error);
                    }
                }
                if let Some(error) = stop_failure {
                    return Err(error
                        .change_context(ExecutionBuildError::StopRelayStateTask {
                            relay: relay.clone(),
                        })
                        .change_context(build()));
                }
            }
            let placement = self
                .build_scheduled_node_placement(
                    domain,
                    &shutdown,
                    desired_node,
                    local_node_id,
                    state,
                    Some(&changed_units),
                )
                .change_context_lazy(build)?;
            // The placement's tasks join the execution at once, so whatever fails below leaves
            // them where a restore finds them.
            if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                for (unit, tasks) in placement.tasks {
                    execution
                        .placement_tasks
                        .insert(NodeUnitRef::new(entity.clone(), unit), tasks);
                    journal.step(step).placement().record_installed(unit);
                }
            }
            // A node that keeps executing here keeps its task, while its placement has just
            // rebound the replicated state it owns to a new assignment generation. A Kafka ingestor
            // kept this way commits through the originator that generation issued.
            for (unit, originator) in placement.kafka_offset_states {
                if previous_local_units.contains(&unit) && desired_local_units.contains(&unit) {
                    self.reissue_kafka_offset_originator(domain, entity, unit, originator);
                    journal.step(step).placement().record_reissued_kafka(unit);
                }
            }

            if let Some(relay) = materialized_relay
                && !repartitioned_relay
            {
                relay_states_moved = true;
                let services = self
                    .inner
                    .executions
                    .get(domain)
                    .and_then(|execution| execution.relay_services.get(&relay).cloned());
                let mut materialized_states = placement.materialized_states;
                // The state task of a unit this node stops owning ends here; a unit it keeps
                // stopped its task before the placement. Each unit this node owns now starts a task
                // with the originator its new assignment generation issued.
                let mut stop_failure = None;
                for unit in previous_local_units.difference(&desired_local_units) {
                    let previous =
                        self.inner
                            .executions
                            .get_mut(domain)
                            .and_then(|mut execution| {
                                execution
                                    .relay_state_tasks
                                    .remove(&RelayUnitRef::new(relay.clone(), *unit))
                            });
                    if let Some(task) = previous {
                        let stopped = task.stop(self.task_stop_grace()).await;
                        let withdrawn = if let Some(services) = services.as_ref()
                            && !desired_node.is_holding_unit(*unit)
                        {
                            services.remove_local_unit_runtime_consumer(AckMode::Detached, *unit);
                            true
                        } else {
                            false
                        };
                        journal
                            .step(step)
                            .placement()
                            .record_state_task_stopped(*unit, withdrawn);
                        if let Err(error) = stopped
                            && stop_failure.is_none()
                        {
                            stop_failure = Some(error);
                        }
                    }
                }
                if let Some(error) = stop_failure {
                    return Err(error
                        .change_context(ExecutionBuildError::StopRelayStateTask {
                            relay: relay.clone(),
                        })
                        .change_context(build()));
                }
                for unit in &desired_local_units {
                    let Some(services) = services.clone() else {
                        return Err(Report::new(ExecutionBuildError::MissingRelayBoundary {
                            relay,
                        })
                        .change_context(build()));
                    };
                    let Some(state) = materialized_states.remove(unit) else {
                        return Err(Report::new(ExecutionBuildError::MissingMaterializedState {
                            relay,
                        })
                        .change_context(build()));
                    };
                    let (receiver, registered) = if desired_node.is_holding_unit(*unit) {
                        (services.dormant_unit_fan_in(AckMode::Detached, *unit), None)
                    } else {
                        (
                            services.add_local_unit_runtime_consumer(AckMode::Detached, *unit),
                            Some(services.clone()),
                        )
                    };
                    let task = self.spawn_relay_state_task(
                        domain,
                        RelayStateTaskSpec {
                            relay: relay.clone(),
                            unit: *unit,
                            partitioning: services.partitioning,
                            state,
                            retention: activation_plan
                                .relays
                                .get(&relay)
                                .verified("the domain plan covers every scheduled relay")
                                .retention,
                            receiver,
                            transition: UnitPartitionTransition::of(desired_node, *unit),
                            parked_capacity: services.owner_capacity(),
                        },
                    );
                    if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                        execution
                            .relay_state_tasks
                            .insert(RelayUnitRef::new(relay.clone(), *unit), task);
                    }
                    journal
                        .step(step)
                        .placement()
                        .record_state_task_started(*unit, registered);
                }
                if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                    execution.materialized_stream_owner_nodes.insert(
                        relay,
                        RelayOwnerRoutes::new(desired_node.unit_execution_nodes()),
                    );
                }
            }

            if entity.kind == ModelKind::Relay && !repartitioned_relay {
                let relay = RelayName::from(&entity.identifier);
                for unit in desired_local_units.difference(&previous_local_units) {
                    let Some(services) = relay_services.clone() else {
                        return Err(Report::new(ExecutionBuildError::MissingRelayBoundary {
                            relay,
                        })
                        .change_context(build()));
                    };
                    // A unit holding only keys the previous count placed in it has no owner.
                    if !services.declares_unit(*unit) {
                        continue;
                    }
                    let retention = activation_plan
                        .relays
                        .get(&relay)
                        .verified("the domain plan covers every scheduled relay")
                        .retention;
                    let task = self
                        .spawn_relay_owner_task(domain, &relay, *unit, services, retention)
                        .change_context_lazy(build)?;
                    if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                        execution
                            .relay_owner_tasks
                            .insert(RelayUnitRef::new(relay.clone(), *unit), task);
                    }
                    journal.step(step).record_owner_started(*unit);
                }
            }
        }
        Ok(relay_states_moved)
    }

    pub(super) async fn swap_scheduled_nodes(
        &self,
        domain: &DomainName,
        revision: Arc<ExecutionRevision>,
        change: &EntitySwapExecution,
    ) -> error_stack::Result<(), RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let EntitySwapExecution {
            entities,
            reassignments,
            dynamic_updates,
            state_purges,
            gate_relays,
            forced_recovery_relays,
        } = change;
        let explicitly_replaced = entities.iter().cloned().collect::<BTreeSet<_>>();
        let reassigned_units = reassignments.clone();
        let reassignments =
            SortedSet::from_unsorted(reassignments.iter().map(|unit| unit.node.clone()).collect())
                .into_vec();
        let resource_plans = &revision.resources;
        let dispatcher = self.inner.remote_dispatcher.load_full();
        let local_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        // A reassignment only replaces this cluster node's runtime when the node stopped or
        // started executing here. A node that keeps executing here, such as a server-side ingestor
        // that merely lost one of its other placements, is left running.
        let relocated = self.locally_relocated_nodes(domain, &revision, &reassignments);
        let entities = SortedSet::from_unsorted(
            entities
                .iter()
                .chain(relocated.iter())
                .cloned()
                .collect::<Vec<_>>(),
        )
        .into_vec();
        let entities = entities.as_slice();
        let mut retiring_ingestors =
            self.retiring_ingestor_inputs(domain, Some(&revision), local_node_id);
        for entity in entities {
            if entity.kind != ModelKind::WasmProcessor {
                continue;
            }
            let Some(wasm) = resource_plans.wasm.get(&entity.identifier) else {
                return Err(Report::new(ExecutionBuildError::MissingDesiredNode {
                    node: entity.clone(),
                })
                .change_context(build()));
            };
            let assigned_here = match local_node_id {
                Some(local) => wasm.assignment.is_assigned_to(local),
                None => wasm.assignment.executes_on(None),
            };
            if assigned_here {
                self.prepare_wasm_module(&wasm.module)
                    .await
                    .change_context_lazy(|| ExecutionBuildError::PrepareWasmModule {
                        processor: entity.identifier.clone(),
                    })
                    .change_context_lazy(build)?;
            }
        }
        // Fence the relays feeding every entity this activation replaces, and the relays feeding
        // every reassigned node, so producers pause instead of dispatching into an owner that is
        // about to stop.
        let gated = SortedSet::from_unsorted(
            entities
                .iter()
                .chain(reassignments.iter())
                .cloned()
                .collect::<Vec<_>>(),
        )
        .into_vec();
        let targets = if explicitly_replaced.is_empty() {
            EntityGateTargets::Units(reassigned_units)
        } else {
            EntityGateTargets::Entities(gated)
        };
        let mut relays = self.entity_pause_relays(domain, &targets);
        relays.extend(gate_relays.iter().cloned());
        relays.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        relays.dedup();
        // The swap changes the routes an owner's fan-out reads, so it holds that hand-on back too.
        // The fan-out waits for the swap rather than being refused: nothing the swap waits for
        // waits for that fan-out.
        let mut local_gate_hold = self.engage_entity_unit_gates(
            domain,
            &relays,
            &targets,
            EntityGateLease {
                deadline: Instant::now() + self.entity_gate_deadline(),
                reason: "local scheduled node swap",
            },
            RelayDispatchGateHold::ProducersAndHandOn,
        );
        if let Some(local_node_id) = local_node_id {
            self.fence_forced_recovery_routes(
                domain,
                &revision,
                forced_recovery_relays,
                local_node_id,
            );
        }
        if !local_gate_hold.wait_quiescent().await {
            return Err(Report::new(ExecutionBuildError::GateFenceTimeout).change_context(build()));
        }
        self.force_flush_domain(domain);

        let installed = self
            .inner
            .executions
            .get(domain)
            .map(|execution| execution.revision.clone());
        let mut journal = EntitySwapJournal::new(installed.clone());
        let replaced = Box::pin(self.replace_swapped_entities(
            domain,
            &revision,
            SwapTargets {
                entities,
                explicitly_replaced: &explicitly_replaced,
                reassignments: &reassignments,
                dynamic_updates,
                state_purges,
                installed: installed.as_deref(),
                local_node_id,
            },
            &mut retiring_ingestors,
            &mut journal,
        ))
        .await;
        let SwapPublication {
            processor_plans,
            message_error_plans,
            materialized_routing_changed,
        } = match replaced {
            Ok(publication) => publication,
            Err(error) => {
                // The previous execution is back before the gate opens, so nothing reaches an
                // entity between its replacement and its restore.
                let restored = Box::pin(self.restore_entity_swap(domain, journal)).await;
                local_gate_hold.release();
                if restored {
                    return Err(error);
                }
                return Err(error.change_context(RuntimeError::IncompleteSwapRestore {
                    domain: domain.clone(),
                }));
            }
        };
        let mut routing_published = false;
        if let Some(mut execution) = self.inner.executions.get_mut(domain) {
            if let Some(local_node_id) = local_node_id {
                let remote_consumers =
                    Self::remote_runtime_consumers_for_revision(&revision, local_node_id);
                for (relay, services) in &execution.relay_services {
                    let relay_node = NodeRef::new(ModelKind::Relay, ModelName::from(relay));
                    let owners = match revision.nodes.get(&relay_node) {
                        Some(node) => RelayOwnerRoutes::new(node.unit_execution_nodes()),
                        None => RelayOwnerRoutes::default(),
                    };
                    services.replace_owner_routes(owners);
                    services.replace_remote_runtime_consumers(
                        remote_consumers.get(relay).cloned().unwrap_or_default(),
                    );
                }
            }
            execution
                .mqtt_ingestor_clients
                .retain(|name, prepared| prepared.agrees_with(name, &revision));
            execution.routing.relay_transitions =
                super::relay_repartition::relay_transitions(&revision);
            self.observe_partition_epochs(domain, &revision);
            execution.revision = revision;
            execution.routing.message_error_plans = message_error_plans;
            execution.routing.processor_plans = processor_plans;
            execution.routing.activate(self, domain);
            execution.routing.publish();
            routing_published = true;
        }
        if routing_published && materialized_routing_changed {
            // Readers must refresh only after the owner map is published. Waking them while the
            // replacement is staged lets a waiter consume the signal against the previous owner.
            self.bump_relay_state_epoch(domain);
            self.inner.materialized_state_changed.notify_waiters();
        }
        local_gate_hold.release();
        Ok(())
    }

    /// Replaces this node's runtime of everything `targets` swap, recording each step in
    /// `journal`, and binds what publishing `revision` needs. It publishes nothing, so a swap that
    /// fails here puts the previous execution back from the journal and leaves the routing the
    /// domain last published in place.
    async fn replace_swapped_entities(
        &self,
        domain: &DomainName,
        revision: &Arc<ExecutionRevision>,
        targets: SwapTargets<'_>,
        retiring_ingestors: &mut BTreeMap<IngestorName, RetiringIngestorInput>,
        journal: &mut EntitySwapJournal,
    ) -> error_stack::Result<SwapPublication, RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let SwapTargets {
            entities,
            explicitly_replaced,
            reassignments,
            dynamic_updates,
            state_purges,
            installed,
            local_node_id,
        } = targets;
        let replaced_ingestor_layouts =
            Box::pin(self.stop_replaced_ingestor_layouts_for_swap(domain, revision, journal))
                .await?;
        // A smaller layout can keep every remaining unit on its previous assignment. Its
        // prepared state still needs to replace the placement before the readers start.
        let mut placement_changes = SortedSet::from_unsorted(reassignments.to_vec());
        placement_changes.extend(
            replaced_ingestor_layouts
                .into_iter()
                .map(|ingestor| NodeRef::new(ModelKind::Ingestor, &ingestor)),
        );

        // Materialized relay state uses a start-version-qualified schema fingerprint. Install the
        // desired fingerprints before constructing state so the post-swap stale-state purge does
        // not discard the newly attached state instance.
        journal.record_step(EntitySwapStep::StateIdentities);
        self.install_state_identities(revision);
        let repartitioned =
            Box::pin(self.repartition_relays(domain, revision, local_node_id, journal)).await?;
        let registrations = InstalledRegistrations::new(installed, &repartitioned);
        let mut materialized_routing_changed = Box::pin(self.rebind_reassigned_nodes(
            domain,
            revision,
            &placement_changes,
            local_node_id,
            &repartitioned,
            journal,
        ))
        .await?;

        let context = EntitySwapContext {
            domain,
            revision,
            registrations: &registrations,
            local_node_id,
        };
        let mut deferred = DeferredSwapWork::default();
        for entity in entities {
            nervix_primitives::task::consume_budget().await;
            let replace_all = explicitly_replaced.contains(entity);
            match entity.kind {
                ModelKind::Relay => {
                    Box::pin(self.swap_relay_entity(context, entity, journal, &mut deferred))
                        .await?;
                    materialized_routing_changed = true;
                }
                ModelKind::Ingestor => {
                    Box::pin(self.swap_ingestor_entity(
                        context,
                        entity,
                        retiring_ingestors,
                        journal,
                    ))
                    .await?;
                }
                ModelKind::Emitter => {
                    Box::pin(self.swap_emitter_entity(context, entity, replace_all, journal))
                        .await?;
                }
                ModelKind::Reingestor => {
                    Box::pin(self.swap_reingestor_entity(
                        domain,
                        entity,
                        revision,
                        &registrations,
                        local_node_id,
                        journal,
                    ))
                    .await?;
                }
                ModelKind::Generator => {
                    Box::pin(self.swap_generator_entity(
                        domain,
                        entity,
                        revision,
                        replace_all,
                        local_node_id,
                        journal,
                    ))
                    .await?;
                }
                _ => {
                    Box::pin(self.swap_processor_entity(
                        context,
                        entity,
                        state_purges,
                        replace_all,
                        journal,
                        &mut deferred,
                    ))
                    .await?;
                }
            }
        }

        // Each update a restore can take back is recorded before any of them applies.
        for update in dynamic_updates {
            match update {
                DynamicExecutionUpdate::RelayCapacity { relay, .. } => {
                    journal.record_step(EntitySwapStep::RelayCapacity {
                        relay: relay.clone(),
                    });
                }
                DynamicExecutionUpdate::EmitterFlush { emitter, .. } => {
                    journal.record_step(EntitySwapStep::EmitterFlush {
                        emitter: emitter.clone(),
                    });
                }
                DynamicExecutionUpdate::Processor
                | DynamicExecutionUpdate::WasmStateReset { .. }
                | DynamicExecutionUpdate::VhostTlsVersion
                | DynamicExecutionUpdate::PartitionTransitionRetired { .. } => {}
            }
        }
        self.apply_dynamic_model_updates(domain, revision, dynamic_updates)
            .await?;
        let processor_plans = self
            .bind_installed_processor_plans(domain, revision)
            .await?;
        let message_error_plans = {
            let Some(execution) = self.inner.executions.get(domain) else {
                return Err(Report::new(ExecutionBuildError::ExecutionUnavailable {
                    step: ExecutionStep::MessageErrorBinding,
                })
                .change_context(build()));
            };
            Arc::new(
                BoundMessageErrorRoutes::bind(
                    revision.message_errors.clone(),
                    MessageErrorRouteBindingContext {
                        immediate_minimum: self.data_plane_settings().immediate_flush_minimum(),
                        vm_settings: self.inner.vm_settings,
                        relay_services: &execution.relay_services,
                        materialized_stream_specs: &execution.materialized_stream_specs,
                        lookups: &execution.lookups,
                        udfs: &execution.udfs,
                    },
                )
                .change_context(ExecutionBuildError::BindMessageErrorRoutes)
                .change_context_lazy(build)?,
            )
        };
        // Last: the state only the previous execution reads is deleted once nothing else can
        // fail, and the units whose state it was start their replacements after it.
        self.complete_deferred_swap_work(domain, &registrations, deferred, journal)?;
        Ok(SwapPublication {
            processor_plans,
            message_error_plans,
            materialized_routing_changed,
        })
    }

    /// Stops the local units of every ingestor whose unit layout `desired` changes, recording
    /// each in `journal`, before any of its prepared state is installed. Returns those ingestors,
    /// whose placements need to be installed again.
    async fn stop_replaced_ingestor_layouts_for_swap(
        &self,
        domain: &DomainName,
        desired: &ExecutionRevision,
        journal: &mut EntitySwapJournal,
    ) -> error_stack::Result<Vec<IngestorName>, RuntimeError> {
        let dispatcher = self.inner.remote_dispatcher.load_full();
        let layouts = self.replaced_ingestor_layouts(domain, Some(desired));
        for ingestor in &layouts {
            nervix_primitives::task::consume_budget().await;
            let step = journal.record(IngestorSwap::new(ingestor.clone()));
            for unit in self.running_ingestor_units(domain, ingestor) {
                nervix_primitives::task::consume_budget().await;
                self.stop_ingestor_unit(domain, ingestor, unit).await?;
                journal.step(step).record_stopped(unit);
            }
            self.inner.metrics.remove_ingestor_metrics(
                domain,
                ingestor,
                dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
            );
        }
        Ok(layouts)
    }

    /// Close only changing routing generations selected by the forced-recovery plan. Closing
    /// precedes the gate wait: otherwise a remote admission held by the former owner retains the
    /// very permit that installation needs. The normal publication below opens new generations.
    fn fence_forced_recovery_routes(
        &self,
        domain: &DomainName,
        desired: &ExecutionRevision,
        relays: &[RelayName],
        local_node_id: &ClusterNodeName,
    ) {
        if relays.is_empty() {
            return;
        }
        let consumers = Self::remote_runtime_consumers_for_revision(desired, local_node_id);
        let Some(execution) = self.inner.executions.get(domain) else {
            return;
        };
        for relay in relays {
            let Some(services) = execution.relay_services.get(relay) else {
                continue;
            };
            let node = NodeRef::new(ModelKind::Relay, ModelName::from(relay));
            let owners = match desired.nodes.get(&node) {
                Some(node) => RelayOwnerRoutes::new(node.unit_execution_nodes()),
                None => RelayOwnerRoutes::default(),
            };
            let destinations = match consumers.get(relay) {
                Some(destinations) => destinations.as_slice(),
                None => &[],
            };
            services.fence_routing_change(&owners, destinations);
        }
    }

    pub(super) async fn apply_dynamic_schedule_update(
        &self,
        domain: &DomainName,
        revision: Arc<ExecutionRevision>,
        updates: &[DynamicExecutionUpdate],
    ) -> error_stack::Result<(), RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let processor_plans = self
            .bind_installed_processor_plans(domain, &revision)
            .await?;
        // A reset's new generation must be visible before its supervisor writes the generation's
        // initial checkpoint. Publishing the identity first is safe because the selected inputs
        // remain fenced until the same schedule reaches Ready.
        self.install_state_identities(&revision);
        self.apply_dynamic_model_updates(domain, &revision, updates)
            .await?;
        if let Some(mut execution) = self.inner.executions.get_mut(domain) {
            let message_error_plans = Arc::new(
                BoundMessageErrorRoutes::bind(
                    revision.message_errors.clone(),
                    MessageErrorRouteBindingContext {
                        immediate_minimum: self.data_plane_settings().immediate_flush_minimum(),
                        vm_settings: self.inner.vm_settings,
                        relay_services: &execution.relay_services,
                        materialized_stream_specs: &execution.materialized_stream_specs,
                        lookups: &execution.lookups,
                        udfs: &execution.udfs,
                    },
                )
                .change_context(ExecutionBuildError::BindMessageErrorRoutes)
                .change_context_lazy(build)?,
            );
            execution
                .mqtt_ingestor_clients
                .retain(|name, prepared| prepared.agrees_with(name, &revision));
            execution.routing.relay_transitions =
                super::relay_repartition::relay_transitions(&revision);
            self.observe_partition_epochs(domain, &revision);
            execution.revision = revision;
            execution.routing.message_error_plans = message_error_plans;
            execution.routing.processor_plans = processor_plans;
            execution.routing.activate(self, domain);
            execution.routing.publish();
        } else {
            return Err(Report::new(ExecutionBuildError::ExecutionUnavailable {
                step: ExecutionStep::ProcessorPlanPublication,
            })
            .change_context(build()));
        }
        // A reset fences only its selected inputs, and a retired transition changes no route, so
        // neither flushes what the domain's routes buffer.
        if updates.iter().any(|update| {
            !matches!(
                update,
                DynamicExecutionUpdate::WasmStateReset { .. }
                    | DynamicExecutionUpdate::PartitionTransitionRetired { .. }
            )
        }) {
            self.force_flush_domain(domain);
        }
        Ok(())
    }

    /// Applies `updates` in place, while the execution still has the revision before `desired`.
    pub(super) async fn apply_dynamic_model_updates(
        &self,
        domain: &DomainName,
        desired: &ExecutionRevision,
        updates: &[DynamicExecutionUpdate],
    ) -> error_stack::Result<(), RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        for update in updates {
            nervix_primitives::task::consume_budget().await;
            match update {
                DynamicExecutionUpdate::RelayCapacity { relay, capacity } => {
                    self.set_relay_capacity(domain, relay, *capacity);
                }
                DynamicExecutionUpdate::Processor => {}
                DynamicExecutionUpdate::WasmStateReset { processor, reset } => {
                    let commands = self
                        .wasm_state_reset_commands(domain, processor, *reset.scope())
                        .change_context_lazy(build)?;
                    // Every reset failure names its processor, so the reset's report sits directly
                    // beneath the domain context.
                    for commands in commands {
                        nervix_primitives::task::consume_budget().await;
                        let commands = commands.change_context_lazy(build)?;
                        let (response, receiver) = oneshot::channel();
                        let sent = commands
                            .send(ProcessorNodeCommand::ApplyWasmStateReset {
                                reset: reset.clone(),
                                response,
                            })
                            .await;
                        if sent.is_err() {
                            return Err(Report::new(
                                WasmStateResetRuntimeError::CommandUnavailable {
                                    processor: processor.clone(),
                                },
                            )
                            .change_context(build()));
                        }
                        let Ok(reset_result) = receiver.await else {
                            return Err(Report::new(WasmStateResetRuntimeError::ResponseDropped {
                                processor: processor.clone(),
                            })
                            .change_context(build()));
                        };
                        reset_result.change_context_lazy(build)?;
                    }
                }
                // Endpoint routing reads only a VHOST's hostnames. The certificate belongs to the
                // HTTPS listener, which installs it from the same admitted state on every node.
                DynamicExecutionUpdate::VhostTlsVersion => {}
                DynamicExecutionUpdate::PartitionTransitionRetired { node } => {
                    Box::pin(self.retire_partition_transition(domain, node, desired)).await;
                }
                DynamicExecutionUpdate::EmitterFlush { emitter, policy } => {
                    // Every local instance of the emitter runs its own cadence.
                    let entity = NodeRef {
                        kind: ModelKind::Emitter,
                        identifier: ModelName::from(emitter),
                    };
                    let mut instance_commands = Vec::new();
                    if let Some(execution) = self.inner.executions.get(domain) {
                        for (owner, task) in &execution.emitter_tasks {
                            if owner.node == entity {
                                instance_commands.push(task.commands.clone());
                            }
                        }
                    }
                    for commands in instance_commands {
                        nervix_primitives::task::consume_budget().await;
                        ScheduledEmitterTask::reconfigure_via(
                            &commands,
                            policy.clone(),
                            self.task_stop_grace(),
                        )
                        .await
                        .change_context_lazy(|| ExecutionBuildError::ReconfigureEmitterFlush {
                            emitter: emitter.clone(),
                        })
                        .change_context_lazy(build)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Places the materialized state of every unit of a materialized relay that this node owns or
    /// replicates: the unit's owner keeps the state's originator for its state task, and each of
    /// its replicas polls the owner for the state it keeps.
    fn build_materialized_relay_placement(
        &self,
        domain: &DomainName,
        shutdown_tx: &watch::Sender<bool>,
        node: &ExecutionNode,
        local_node_id: &ClusterNodeName,
        schema: &StdArc<arrow_schema::Schema>,
        selected_units: Option<&BTreeSet<Unit>>,
    ) -> error_stack::Result<ScheduledNodePlacement, ExecutionBuildError> {
        let mut placement = ScheduledNodePlacement::default();
        let relay = RelayName::from(&node.identifier);
        for (unit, scheduled) in &node.units {
            if selected_units.is_some_and(|selected| !selected.contains(unit)) {
                continue;
            }
            let executes_locally = node.unit_executes_on(*unit, local_node_id);
            if !executes_locally && !scheduled.assigned_nodes.contains(local_node_id) {
                continue;
            }
            let replica_nodes = scheduled
                .replica_nodes()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let state_placement = self
                .state_placement_for_unit(
                    domain,
                    RuntimeStateKind::MaterializedRelay,
                    ModelKind::Relay,
                    &relay,
                    *unit,
                    None,
                )
                .change_context_lazy(|| ExecutionBuildError::PlaceState {
                    node: node.identity(),
                })?;
            let mut assignment = self.replicated_materialized_stream_state(
                state_placement,
                schema.clone(),
                node.unit_execution_node(*unit).cloned(),
                replica_nodes,
                Some(local_node_id),
            );
            if let Some(writer) =
                self.spawn_materialized_stream_snapshot_task(assignment.persistence)
            {
                placement
                    .tasks
                    .entry(*unit)
                    .or_default()
                    .checkpoints
                    .push(writer);
            }
            if executes_locally {
                let Some(originator) = assignment.originator.take() else {
                    return Err(Report::new(
                        ExecutionBuildError::MaterializedStateAuthority { relay },
                    ));
                };
                placement.materialized_states.insert(*unit, originator);
            } else {
                let Some(installer) = assignment.installer.take() else {
                    return Err(Report::new(
                        ExecutionBuildError::MaterializedStateInstaller { relay },
                    ));
                };
                if let Some(task) =
                    self.spawn_materialized_stream_replica_poll_task(shutdown_tx, installer)
                {
                    placement
                        .tasks
                        .entry(*unit)
                        .or_default()
                        .replication
                        .push(task);
                }
            }
        }
        Ok(placement)
    }

    /// Builds every part of a scheduled node's runtime that follows only from its assignment: the
    /// replicated states this cluster node owns or replicates for it, and the background tasks
    /// that snapshot and poll them. An assignment change rebuilds exactly this for the moved node
    /// and leaves every other node alone.
    pub(super) fn build_scheduled_node_placement(
        &self,
        domain: &DomainName,
        shutdown_tx: &watch::Sender<bool>,
        node: &ExecutionNode,
        local_node_id: &ClusterNodeName,
        state: Option<PlacedNodeState>,
        selected_units: Option<&BTreeSet<Unit>>,
    ) -> error_stack::Result<ScheduledNodePlacement, ExecutionBuildError> {
        let mut placement = if let Some(PlacedNodeState::MaterializedRelay(schema)) = state.as_ref()
        {
            self.build_materialized_relay_placement(
                domain,
                shutdown_tx,
                node,
                local_node_id,
                schema,
                selected_units,
            )?
        } else {
            ScheduledNodePlacement::default()
        };

        for (unit, scheduled) in &node.units {
            if selected_units.is_some_and(|selected| !selected.contains(unit)) {
                continue;
            }
            if !scheduled.assigned_nodes.contains(local_node_id) {
                continue;
            }
            let executes_locally = node.unit_executes_on(*unit, local_node_id);
            if matches!(state, Some(PlacedNodeState::KafkaDomainOffsets)) {
                let replica_nodes = scheduled
                    .replica_nodes()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let required_acks = replica_nodes.len();
                let mut assignment = self
                    .replicated_kafka_offset_state(
                        RuntimeStatePlacement {
                            domain: domain.clone(),
                            state: RuntimeState::KafkaOffset,
                            kind: node.kind(),
                            identifier: node.identifier.clone(),
                            unit: *unit,
                            branch_key: None,
                        },
                        scheduled.primary_node.clone(),
                        replica_nodes,
                        required_acks,
                        Some(local_node_id),
                    )
                    .change_context_lazy(|| ExecutionBuildError::AssignState {
                        node: node.identity(),
                        state: RuntimeStateKind::KafkaOffset,
                    })?;
                if let Some(writer) = self.spawn_kafka_offset_snapshot_task(assignment.persistence)
                {
                    placement
                        .tasks
                        .entry(*unit)
                        .or_default()
                        .checkpoints
                        .push(writer);
                }
                if executes_locally {
                    let originator = assignment.originator.take().ok_or_else(|| {
                        Report::new(ExecutionBuildError::KafkaOffsetAuthority {
                            ingestor: IngestorName::from(&node.identifier),
                        })
                    })?;
                    placement.kafka_offset_states.insert(*unit, originator);
                } else {
                    let installer = assignment.installer.take().ok_or_else(|| {
                        Report::new(ExecutionBuildError::KafkaOffsetInstaller {
                            ingestor: IngestorName::from(&node.identifier),
                        })
                    })?;
                    if let Some(task) =
                        self.spawn_kafka_offset_replica_poll_task(shutdown_tx, installer)
                    {
                        placement
                            .tasks
                            .entry(*unit)
                            .or_default()
                            .replication
                            .push(task);
                    }
                }
            }
            let aggregate_primary = node
                .unit_execution_node(*unit)
                .cloned()
                .or_else(|| executes_locally.then(|| local_node_id.clone()));
            if node.kind() != ModelKind::Relay
                && let Some(primary) = aggregate_primary
            {
                let aggregate = self
                    .replicated_branch_aggregated_state(
                        RuntimeStatePlacement {
                            domain: domain.clone(),
                            state: RuntimeState::BranchAggregated,
                            kind: node.kind(),
                            identifier: node.identifier.clone(),
                            unit: *unit,
                            branch_key: None,
                        },
                        Some(primary.clone()),
                    )
                    .change_context_lazy(|| ExecutionBuildError::AssignState {
                        node: node.identity(),
                        state: RuntimeStateKind::BranchAggregated,
                    })?;
                if executes_locally {
                    if let Some(writer) = self.spawn_branch_aggregated_snapshot_task(aggregate) {
                        placement
                            .tasks
                            .entry(*unit)
                            .or_default()
                            .checkpoints
                            .push(writer);
                    }
                } else if let Some(task) =
                    self.spawn_branch_aggregated_replica_poll_task(shutdown_tx, aggregate)
                {
                    placement
                        .tasks
                        .entry(*unit)
                        .or_default()
                        .replication
                        .push(task);
                }
            }
            if !executes_locally {
                if let Some(task) = self
                    .spawn_branch_state_replica_poll_task(shutdown_tx, domain, node, *unit)
                    .change_context_lazy(|| ExecutionBuildError::StartStateReplica {
                        node: node.identity(),
                    })?
                {
                    placement
                        .tasks
                        .entry(*unit)
                        .or_default()
                        .replication
                        .push(task);
                }
            } else if node.kind() != ModelKind::Relay {
                self.inner.metrics.register_global_node_unit(
                    domain,
                    node.kind(),
                    &node.identifier,
                    *unit,
                    Some(local_node_id),
                );
            } else if *unit == Unit::SINGLE {
                self.inner.metrics.register_global_node(
                    domain,
                    node.kind(),
                    &node.identifier,
                    Some(local_node_id),
                );
            }
        }

        Ok(placement)
    }

    pub(super) fn scheduled_unit_executes_locally(
        node: &ExecutionNode,
        unit: Unit,
        local: Option<&ClusterNodeName>,
    ) -> bool {
        match local {
            Some(local) => node.unit_executes_on(unit, local),
            None => node
                .units
                .get(&unit)
                .is_some_and(|unit| unit.primary_node.is_none() && unit.assigned_nodes.is_empty()),
        }
    }

    pub(super) fn scheduled_node_executes_locally(
        node: &ExecutionNode,
        local_node_id: Option<&ClusterNodeName>,
    ) -> bool {
        if let Some(local_node_id) = local_node_id {
            return node.executes_on(local_node_id);
        }
        node.units
            .values()
            .all(|unit| unit.primary_node.is_none() && unit.assigned_nodes.is_empty())
    }
}

#[cfg(test)]
#[path = "schedule_apply_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "schedule_apply_failure_tests.rs"]
mod failure_tests;
