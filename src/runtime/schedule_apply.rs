//! Application of validated schedules to one node's runtime.
//!
//! Layer: data plane.
//! - **Owns.** Materializing, replacing and removing local tasks named by an execution schedule.
//! - **Depends on.** Typed schedules, runtime lifecycle handles and installed domain capabilities.
//! - **Must not know.** NSPL parsing, registry validation or placement-policy computation.

use super::*;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeRecoveryExpansion {
    pub(crate) domain: DomainName,
    pub(crate) reason: String,
    pub(crate) scope: Vec<NodeRef>,
}

#[derive(Debug)]
pub(in crate::runtime) struct AppliedRuntimeRecoveryExpansions {
    revision: u64,
    expansions: Vec<RuntimeRecoveryExpansion>,
}

pub(in crate::runtime) struct ScheduleDeltaApplication {
    applied_incrementally: bool,
    recovery_expansion: Option<RuntimeRecoveryExpansion>,
}

impl ScheduleDeltaApplication {
    fn incremental() -> Self {
        Self {
            applied_incrementally: true,
            recovery_expansion: None,
        }
    }

    fn rebuild_required() -> Self {
        Self {
            applied_incrementally: false,
            recovery_expansion: None,
        }
    }
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
    ) -> Result<(), RuntimeError> {
        let _application = self.inner.schedule_application.lock().await;
        let applied = self.inner.test_applied_schedule.load_full();
        let revision_plan =
            PlannedClusterRevision::between(applied.as_deref(), schedule).map_err(|error| {
                RuntimeError::BuildDomainExecution {
                    domain: "cluster".to_string(),
                    reason: format!("{error:#}"),
                }
            })?;
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
        let revision_plan =
            PlannedClusterRevision::between(applied.as_deref(), schedule).map_err(|error| {
                Report::new(RuntimeError::BuildDomainExecution {
                    domain: "cluster".to_string(),
                    reason: format!("{error:#}"),
                })
            })?;
        self.apply_planned_cluster_state(
            local_node_id,
            revision,
            domains,
            domain_clock_authorities,
            revision_plan,
        )
        .await
        .map_err(Report::new)?;
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
    ) -> Result<(), RuntimeError> {
        let mut application = self.inner.schedule_application.lock().await;
        if !application.advances_beyond_applied(revision) {
            return Ok(());
        }

        self.sync_committed_domains(domains, domain_clock_authorities);
        // A failed application records nothing, so the same revision is applied again rather than
        // being suppressed as one this node already holds.
        let recovery_expansions =
            Box::pin(self.apply_cluster_schedule_locked(local_node_id, revision_plan, false))
                .await?;
        application.record_applied(revision);
        self.inner
            .applied_recovery_expansions
            .store(Some(StdArc::new(AppliedRuntimeRecoveryExpansions {
                revision,
                expansions: recovery_expansions,
            })));
        Ok(())
    }

    pub(crate) fn recovery_expansions(&self, revision: u64) -> Vec<RuntimeRecoveryExpansion> {
        let Some(applied) = self.inner.applied_recovery_expansions.load_full() else {
            return Vec::new();
        };
        if applied.revision != revision {
            return Vec::new();
        }
        applied.expansions.clone()
    }

    pub(super) async fn apply_cluster_schedule_locked(
        &self,
        local_node_id: &ClusterNodeName,
        revision_plan: PlannedClusterRevision,
        start_ingestors: bool,
    ) -> Result<Vec<RuntimeRecoveryExpansion>, RuntimeError> {
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
        let mut recovery_expansions = Vec::new();

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
                        .insert(domain.clone(), error.to_string());
                    return Err(error);
                }
            }
        }

        for (domain_id, change) in &revision_plan.domains {
            let domain = &change.revision;
            let predecessor_matches =
                self.inner
                    .executions
                    .get(domain_id)
                    .is_some_and(|execution| {
                        Some(execution.revision.source_digest) == change.predecessor_digest
                    });
            let Some(domain_state) = self.inner.domains.get(domain_id) else {
                continue;
            };
            let domain_status = domain_state.status.clone();
            let desired_start_version = domain_state.start_version;
            drop(domain_state);

            if let nervix_models::DomainStatus::Paused = domain_status {
                self.engage_domain_ingestor_quiesce(&domain.domain);
            }
            let desired_passive_only =
                matches!(domain_status, nervix_models::DomainStatus::Stopped);
            if !matches!(change.delta, ExecutionDelta::Unchanged)
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
                    Box::pin(self.apply_schedule_delta(
                        local_node_id,
                        domain,
                        &change.delta,
                        start_ingestors,
                    ))
                    .await?
                } else {
                    ScheduleDeltaApplication::rebuild_required()
                };
                if let Some(expansion) = delta_application.recovery_expansion {
                    recovery_expansions.push(expansion);
                }
                if !delta_application.applied_incrementally {
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
                                .insert(domain.domain.clone(), error.to_string());
                            return Err(error);
                        }
                    }
                }
            }

            if let nervix_models::DomainStatus::Running = domain_status {
                self.purge_stale_runtime_state(&domain.domain)
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.domain.as_str().to_string(),
                        reason: error.to_string(),
                    })?;
                if start_ingestors {
                    Box::pin(self.start_missing_domain_ingestors(&domain.domain)).await?;
                }
                self.release_domain_ingestor_quiesce(&domain.domain);
            }
        }
        self.retain_assigned_wasm_modules(local_node_id, &revision_plan);

        Ok(recovery_expansions)
    }

    /// Applies a changed schedule without tearing the domain down when the delta allows it.
    /// Returns `false` when the delta demands a full rebuild from the schedule instead.
    pub(super) async fn apply_schedule_delta(
        &self,
        local_node_id: &ClusterNodeName,
        desired: &Arc<ExecutionRevision>,
        delta: &ExecutionDelta,
        start_ingestors: bool,
    ) -> Result<ScheduleDeltaApplication, RuntimeError> {
        match delta {
            ExecutionDelta::Unchanged => Ok(ScheduleDeltaApplication::incremental()),
            ExecutionDelta::Dynamic(updates) => {
                Box::pin(self.apply_dynamic_schedule_update(
                    &desired.domain,
                    desired.clone(),
                    updates,
                ))
                .await?;
                Ok(ScheduleDeltaApplication::incremental())
            }
            ExecutionDelta::EntitySwap(change) => {
                #[cfg(feature = "testing")]
                let swap_result = if self
                    .inner
                    .fault_injection
                    .take_failed_entity_schedule_swap(local_node_id, &desired.domain)
                {
                    Err(RuntimeError::BuildDomainExecution {
                        domain: desired.domain.as_str().to_string(),
                        reason: "injected entity-level schedule apply failure".to_string(),
                    })
                } else {
                    Box::pin(self.swap_scheduled_nodes(&desired.domain, desired.clone(), change))
                        .await
                };
                #[cfg(not(feature = "testing"))]
                let swap_result =
                    Box::pin(self.swap_scheduled_nodes(&desired.domain, desired.clone(), change))
                        .await;
                if let Err(error) = swap_result {
                    let reason = error.to_string();
                    warn!(
                        domain = desired.domain.as_str(),
                        error = %error,
                        "entity-level schedule apply failed; rebuilding domain"
                    );
                    Box::pin(self.rebuild_domain_from_revision(
                        local_node_id,
                        &desired.domain,
                        Some(desired.clone()),
                        start_ingestors,
                    ))
                    .await?;
                    return Ok(ScheduleDeltaApplication {
                        applied_incrementally: true,
                        recovery_expansion: Some(RuntimeRecoveryExpansion {
                            domain: desired.domain.clone(),
                            reason,
                            scope: desired.nodes.keys().cloned().collect(),
                        }),
                    });
                }
                Ok(ScheduleDeltaApplication::incremental())
            }
            ExecutionDelta::Rebuild => Ok(ScheduleDeltaApplication::rebuild_required()),
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
                    && entity.kind != ModelKind::Lookup
                    && Self::scheduled_node(&execution.revision, entity).is_some_and(|existing| {
                        Self::scheduled_node(desired, entity).is_some_and(|desired_node| {
                            existing.executes_on(local_node_id)
                                != desired_node.executes_on(local_node_id)
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

    /// Rebuilds the placement-derived runtime of every reassigned node: the replicated states this
    /// cluster node owns or replicates for it, including a materialized relay's state task. Nodes
    /// the schedule did not reassign are never touched.
    pub(super) async fn rebind_reassigned_nodes(
        &self,
        domain: &DomainName,
        revision: &ExecutionRevision,
        reassignments: &[NodeRef],
        local_node_id: Option<&ClusterNodeName>,
    ) -> Result<bool, RuntimeError> {
        let Some(local_node_id) = local_node_id else {
            return Ok(false);
        };
        if reassignments.is_empty() {
            return Ok(false);
        }
        let activation_plan = &revision.activation;
        let shutdown = match self.inner.executions.get(domain) {
            Some(execution) => execution.shutdown.clone(),
            None => {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: "domain execution is unavailable for schedule reassignment".to_string(),
                });
            }
        };
        let mut relay_states_moved = false;
        let schedule_fingerprint = revision.ownership_handoff_fingerprint;
        for entity in reassignments {
            nervix_primitives::task::consume_budget().await;
            let desired_node = Self::scheduled_node(revision, entity).ok_or_else(|| {
                RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing reassigned {} '{}'",
                        entity.kind.as_str(),
                        entity.identifier.as_str()
                    ),
                }
            })?;
            let was_local = self.inner.executions.get(domain).is_some_and(|execution| {
                Self::scheduled_node(&execution.revision, entity)
                    .is_some_and(|existing| existing.executes_on(local_node_id))
            });
            let previous_owner = if let Some(execution) = self.inner.executions.get(domain)
                && let Some(existing) = Self::scheduled_node(&execution.revision, entity)
            {
                existing.execution_node().cloned()
            } else {
                None
            };
            let executes_locally = desired_node.executes_on(local_node_id);
            self.activate_prepared_forced_ownership_recovery_state(
                domain,
                desired_node,
                local_node_id,
                schedule_fingerprint,
                true,
            )
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "failed to activate forced recovery state for {} '{}': {error}",
                    desired_node.kind().as_str(),
                    desired_node.identifier.as_str()
                ),
            })?;
            self.activate_prepared_ownership_handoff_state(
                domain,
                desired_node,
                local_node_id,
                schedule_fingerprint,
                true,
            )
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "failed to activate prepared state for {} '{}': {error}",
                    desired_node.kind().as_str(),
                    desired_node.identifier.as_str()
                ),
            })?;
            let relay_runtime = if entity.kind == ModelKind::Relay
                && let Some(execution) = self.inner.executions.get(domain)
                && let Some(registry) = execution
                    .relay_registries
                    .get(&RelayName::from(&entity.identifier))
                && let Some(service) = execution
                    .relay_services
                    .get(&RelayName::from(&entity.identifier))
            {
                Some((registry.clone(), service.clone()))
            } else {
                None
            };

            if entity.kind == ModelKind::Relay && was_local && !executes_locally {
                let previous = if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                    execution
                        .relay_owner_tasks
                        .remove(&RelayName::from(&entity.identifier))
                } else {
                    None
                };
                if let Some(task) = previous {
                    task.stop(self.branch_task_stop_timeout())
                        .await
                        .map_err(|reason| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "failed to drain relay '{}' before reassignment: {reason}",
                                entity.identifier.as_str()
                            ),
                        })?;
                }
            }
            let previous_tasks = if let Some(mut execution) = self.inner.executions.get_mut(domain)
            {
                execution.placement_tasks.remove(entity)
            } else {
                None
            };
            for task in previous_tasks.unwrap_or_default() {
                nervix_primitives::task::consume_budget().await;
                task.abort();
                task.join_after_shutdown("placement").await;
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
                let state_placement = self
                    .state_placement(
                        domain,
                        RuntimeStateKind::MaterializedRelay,
                        ModelKind::Relay,
                        relay,
                        None,
                    )
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: error.to_string(),
                    })?;
                self.prepare_materialized_stream_restore(&state_placement, schema)
                    .await
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: error.to_string(),
                    })?;
            }
            if executes_locally
                && !was_local
                && let Some(relay) = materialized_relay.as_ref()
                && let Some(previous_owner) = previous_owner.as_ref()
            {
                let state_placement = self
                    .state_placement(
                        domain,
                        RuntimeStateKind::MaterializedRelay,
                        ModelKind::Relay,
                        relay,
                        None,
                    )
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: error.to_string(),
                    })?;
                let local_replica = self
                    .inner
                    .replicated_materialized_stream_states
                    .get(&state_placement)
                    .map(|state| state.clone());
                if let Some(local_replica) = local_replica {
                    let read = ReplicatedMaterializedRelayState::read(&local_replica);
                    let after_lsm = read.current_lsm();
                    let installer =
                        ReplicatedMaterializedRelayState::current_installer(&local_replica)
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "materialized relay '{}' is no longer a replica while \
                                     refreshing its ownership handoff snapshot",
                                    relay.as_str()
                                ),
                            })?;
                    let _installed_revision = self
                        .install_materialized_snapshot_from(
                            previous_owner,
                            &read,
                            &installer,
                            Some(after_lsm),
                        )
                        .await
                        .map_err(|reason| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "failed to refresh promoted materialized relay replica '{}': \
                                 {reason}",
                                relay.as_str()
                            ),
                        })?;
                }
            }
            let placement = self.build_scheduled_node_placement(
                domain,
                &shutdown,
                desired_node,
                local_node_id,
                state,
            )?;
            let tasks = placement.tasks;

            if let Some(relay) = materialized_relay {
                relay_states_moved = true;
                let services = self
                    .inner
                    .executions
                    .get(domain)
                    .and_then(|execution| execution.relay_services.get(&relay).cloned());
                if was_local && !executes_locally {
                    let previous = self
                        .inner
                        .executions
                        .get_mut(domain)
                        .and_then(|mut execution| execution.relay_state_tasks.remove(&relay));
                    if let Some(task) = previous {
                        task.stop(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE)
                            .await
                            .map_err(|error| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: error.to_string(),
                            })?;
                    }
                    if let Some(services) = services.as_ref() {
                        services.remove_local_runtime_consumer(AckMode::Detached);
                    }
                }
                if executes_locally && !was_local {
                    let services = services.ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing relay services for relocated materialized relay '{}'",
                            relay.as_str()
                        ),
                    })?;
                    let state = placement.materialized_state.ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing materialized relay state '{}'",
                                relay.as_str()
                            ),
                        }
                    })?;
                    let task = self.spawn_relay_state_task(
                        domain,
                        RelayStateTaskSpec {
                            relay: relay.clone(),
                            state,
                            retention: activation_plan
                                .relays
                                .get(&relay)
                                .verified("the domain plan covers every scheduled relay")
                                .retention,
                            receiver: services.add_local_runtime_consumer(AckMode::Detached),
                        },
                    );
                    if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                        execution.relay_state_tasks.insert(relay.clone(), task);
                    }
                }
                if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                    execution
                        .materialized_stream_owner_nodes
                        .insert(relay, desired_node.execution_node().cloned());
                }
            }

            if entity.kind == ModelKind::Relay && executes_locally && !was_local {
                let (registry, services) =
                    relay_runtime.ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing runtime boundary for relocated relay '{}'",
                            entity.identifier.as_str()
                        ),
                    })?;
                let task = self.spawn_relay_owner_task(
                    domain,
                    &RelayName::from(&entity.identifier),
                    registry,
                    services,
                    activation_plan
                        .relays
                        .get(&RelayName::from(&entity.identifier))
                        .verified("the domain plan covers every scheduled relay")
                        .retention,
                );
                if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                    execution
                        .relay_owner_tasks
                        .insert(RelayName::from(&entity.identifier), task);
                }
            }

            if !tasks.is_empty()
                && let Some(mut execution) = self.inner.executions.get_mut(domain)
            {
                execution.placement_tasks.insert(entity.clone(), tasks);
            }
        }
        Ok(relay_states_moved)
    }

    pub(super) async fn swap_scheduled_nodes(
        &self,
        domain: &DomainName,
        revision: Arc<ExecutionRevision>,
        change: &EntitySwapExecution,
    ) -> Result<(), RuntimeError> {
        let EntitySwapExecution {
            entities,
            reassignments,
            dynamic_updates,
            state_purges,
            gate_relays,
        } = change;
        let activation_plan = &revision.activation;
        let resource_plans = &revision.resources;
        let entrypoints = &revision.entrypoints;
        let emitter_plans = &revision.emitters;
        let dispatcher = self.inner.remote_dispatcher.load_full();
        let local_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        // A reassignment only replaces this cluster node's runtime when the node stopped or
        // started executing here. A node that keeps executing here, such as a server-side ingestor
        // that merely lost one of its other placements, is left running.
        let relocated = self.locally_relocated_nodes(domain, &revision, reassignments);
        let entities = SortedSet::from_unsorted(
            entities
                .iter()
                .chain(relocated.iter())
                .cloned()
                .collect::<Vec<_>>(),
        )
        .into_vec();
        let entities = entities.as_slice();
        for entity in entities {
            if entity.kind != ModelKind::WasmProcessor {
                continue;
            }
            let Some(wasm) = resource_plans.wasm.get(&entity.identifier) else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("missing desired WASM processor '{}'", entity.identifier),
                });
            };
            let assigned_here = match local_node_id {
                Some(local) => wasm.assignment.is_assigned_to(local),
                None => wasm.assignment.executes_on(None),
            };
            if assigned_here {
                self.prepare_wasm_module(&wasm.module)
                    .await
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("failed to prepare WASM processor: {error:#}"),
                    })?;
            }
        }
        let desired_specs = &revision.processors;
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
        let mut relays = self.entity_pause_relays(domain, &gated);
        relays.extend(gate_relays.iter().cloned());
        relays.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        relays.dedup();
        let mut local_gate_hold = self.engage_entity_gates(
            domain,
            &relays,
            Instant::now() + self.inner.entity_gate_deadline,
            "local scheduled node swap",
        );
        if !local_gate_hold.wait_quiescent().await {
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: "relay dispatch gate fence did not complete before the local node swap \
                         deadline"
                    .to_string(),
            });
        }
        self.force_flush_domain(domain);

        // Materialized relay state uses a start-version-qualified schema fingerprint. Install the
        // desired fingerprints before constructing state so the post-swap stale-state purge does
        // not discard the newly attached state instance.
        self.install_state_identities(&revision);
        let mut materialized_routing_changed = self
            .rebind_reassigned_nodes(domain, &revision, reassignments, local_node_id)
            .await?;

        for entity in entities {
            nervix_primitives::task::consume_budget().await;
            if entity.kind == ModelKind::Relay {
                let desired_node = revision.nodes.get(entity).ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing desired relay '{}'", entity.identifier.as_str()),
                    }
                })?;
                let desired_materialized = desired_node.materialized_relay;
                let (
                    was_materialized,
                    shutdown,
                    schema,
                    services,
                    previous_state_task,
                    previous_placement_tasks,
                ) = {
                    let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution is unavailable for relay transition"
                                .to_string(),
                        }
                    })?;
                    let was_materialized = execution
                        .materialized_stream_specs
                        .contains_key(&RelayName::from(&entity.identifier));
                    let schema = execution
                        .relay_schemas
                        .get(&RelayName::from(&entity.identifier))
                        .cloned()
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing schema for relay '{}'",
                                entity.identifier.as_str()
                            ),
                        })?;
                    let services = execution
                        .relay_services
                        .get(&RelayName::from(&entity.identifier))
                        .cloned()
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing runtime boundary for relay '{}'",
                                entity.identifier.as_str()
                            ),
                        })?;
                    if desired_materialized {
                        execution.materialized_stream_specs.insert(
                            RelayName::from(&entity.identifier),
                            RuntimeMaterializedRelaySpec::new(
                                schema.arrow_schema(),
                                schema.vm_sensitivity(),
                                desired_node.resolved_branching.clone().assured(
                                    "the schedule resolves every relay branch declaration",
                                ),
                            ),
                        );
                        execution.materialized_stream_owner_nodes.insert(
                            RelayName::from(&entity.identifier),
                            desired_node.execution_node().cloned(),
                        );
                    } else {
                        execution
                            .materialized_stream_specs
                            .remove(&RelayName::from(&entity.identifier));
                        execution
                            .materialized_stream_owner_nodes
                            .remove(&RelayName::from(&entity.identifier));
                    }
                    (
                        was_materialized,
                        execution.shutdown.clone(),
                        schema,
                        services,
                        execution
                            .relay_state_tasks
                            .remove(&RelayName::from(&entity.identifier)),
                        execution.placement_tasks.remove(entity).unwrap_or_default(),
                    )
                };

                if let Some(task) = previous_state_task {
                    task.stop(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE)
                        .await
                        .map_err(|reason| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "failed to stop relay '{}' state task: {reason}",
                                entity.identifier.as_str()
                            ),
                        })?;
                    services.remove_local_runtime_consumer(AckMode::Detached);
                }
                for task in previous_placement_tasks {
                    nervix_primitives::task::consume_budget().await;
                    task.abort();
                    task.join_after_shutdown("placement").await;
                }

                if desired_materialized {
                    let placement = self.build_scheduled_node_placement(
                        domain,
                        &shutdown,
                        desired_node,
                        local_node_id.ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "local node id is unavailable for relay transition".to_string(),
                        })?,
                        Some(PlacedNodeState::MaterializedRelay(schema.arrow_schema())),
                    )?;
                    let state_task = if desired_node.executes_on(local_node_id.verified(
                        "the resolution above returned an error unless the local node id is \
                         present",
                    )) {
                        Some(
                            self.spawn_relay_state_task(
                                domain,
                                RelayStateTaskSpec {
                                    relay: RelayName::from(&entity.identifier.clone()),
                                    state: placement.materialized_state.clone().ok_or_else(
                                        || RuntimeError::BuildDomainExecution {
                                            domain: domain.as_str().to_string(),
                                            reason: format!(
                                                "missing materialized state for relay '{}'",
                                                entity.identifier.as_str()
                                            ),
                                        },
                                    )?,
                                    retention: activation_plan
                                        .relays
                                        .get(&RelayName::from(&entity.identifier))
                                        .verified("the domain plan covers every scheduled relay")
                                        .retention,
                                    receiver: services
                                        .add_local_runtime_consumer(AckMode::Detached),
                                },
                            ),
                        )
                    } else {
                        None
                    };
                    if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                        if !placement.tasks.is_empty() {
                            execution
                                .placement_tasks
                                .insert(entity.clone(), placement.tasks);
                        }
                        if let Some(state_task) = state_task {
                            execution
                                .relay_state_tasks
                                .insert(RelayName::from(&entity.identifier), state_task);
                        }
                    }
                }
                materialized_routing_changed = true;
                if was_materialized && !desired_materialized {
                    self.purge_materialized_relay_state(
                        domain,
                        &RelayName::from(&entity.identifier),
                    )?;
                }
                continue;
            }
            if entity.kind == ModelKind::Ingestor {
                let ingestor = IngestorName::from(&entity.identifier);
                let Some(desired_plan) = entrypoints.ingestor(&ingestor).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing desired ingestor '{}'", ingestor.as_str()),
                    });
                };
                let desired_node = revision
                    .nodes
                    .get(entity)
                    .assured("the entrypoint plans were decided from this same schedule");

                let key = entity.in_domain(domain);
                if self.inner.ingestors.contains_key(&key) {
                    self.stop_ingestor(domain, &ingestor).await?;
                }
                if Self::scheduled_node_executes_locally(desired_node, local_node_id) {
                    self.start_ingestor(&desired_plan).await?;
                }
                continue;
            }
            if entity.kind == ModelKind::Emitter {
                let emitter_name = EmitterName::from(&entity.identifier);
                let desired_emitter =
                    emitter_plans
                        .emitter(&emitter_name)
                        .cloned()
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing desired emitter '{}'",
                                entity.identifier.as_str()
                            ),
                        })?;
                let desired_node = revision
                    .nodes
                    .get(entity)
                    .assured("the emitter plan was decided from this same schedule");
                let (old_emitter, old_task) = {
                    let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution is unavailable for emitter swap".to_string(),
                        }
                    })?;
                    let old_emitter = execution
                        .revision
                        .emitters
                        .emitter(&emitter_name)
                        .cloned()
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing existing emitter '{}'",
                                entity.identifier.as_str()
                            ),
                        })?;
                    let old_task = execution.emitter_tasks.remove(entity);
                    (old_emitter, old_task)
                };
                let had_old_task = old_task.is_some();
                if let Some(old_task) = old_task
                    && let Err(error) = old_task.stop(self.domain_drain_timeout()).await
                {
                    let reason = error.reason().to_string();
                    if let Some(old_task) = error.into_task()
                        && let Some(mut execution) = self.inner.executions.get_mut(domain)
                    {
                        execution.emitter_tasks.insert(entity.clone(), old_task);
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason,
                    });
                }

                let executes_locally =
                    local_node_id.is_some_and(|node_id| desired_node.executes_on(node_id));
                /// What spawning the swapped emitter's task needs from the domain execution,
                /// taken while it is borrowed so the spawn itself runs without holding that
                /// borrow.
                struct EmitterSpawnInputs {
                    shutdown: watch::Sender<bool>,
                    codecs: HashMap<CodecName, Arc<CompiledCodec>>,
                    deps: EmitterTaskDeps,
                    inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
                }

                let spawn = {
                    let execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution disappeared during emitter swap".to_string(),
                        }
                    })?;
                    if had_old_task {
                        for input in &old_emitter.inputs {
                            if let Some(services) = execution.relay_services.get(&input.relay) {
                                services.remove_local_runtime_consumer(old_emitter.mode);
                            }
                        }
                    }
                    if !executes_locally {
                        None
                    } else {
                        let inputs = desired_emitter
                            .inputs
                            .iter()
                            .map(|input| {
                                let Some(services) = execution.relay_services.get(&input.relay)
                                else {
                                    return Err(RuntimeError::BuildDomainExecution {
                                        domain: domain.as_str().to_string(),
                                        reason: format!(
                                            "missing relay services for swapped emitter input '{}'",
                                            input.relay.as_str()
                                        ),
                                    });
                                };
                                Ok((
                                    input.relay.clone(),
                                    services.add_local_runtime_consumer(desired_emitter.mode),
                                ))
                            })
                            .collect::<Result<Vec<_>, RuntimeError>>()?;
                        let deps = self.emitter_task_deps(
                            ExecutionBuildDeps::from_routing(domain, &execution),
                            &desired_emitter,
                        )?;
                        Some(EmitterSpawnInputs {
                            shutdown: execution.shutdown.clone(),
                            codecs: execution.codecs.clone(),
                            deps,
                            inputs,
                        })
                    }
                };
                if let Some(spawn) = spawn {
                    let task = self.spawn_emitter_task(
                        EmitterTaskBuildDeps {
                            domain,
                            shutdown_tx: &spawn.shutdown,
                            codecs: &spawn.codecs,
                            deps: spawn.deps,
                        },
                        desired_emitter.as_ref().clone(),
                        spawn.inputs,
                    )?;
                    self.inner
                        .executions
                        .get_mut(domain)
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution disappeared after emitter spawn".to_string(),
                        })?
                        .emitter_tasks
                        .insert(entity.clone(), task);
                }
                #[cfg(feature = "testing")]
                if had_old_task && !executes_locally {
                    self.inner
                        .fault_injection
                        .pause_emitter_swap_after_detach_if_armed(domain, &emitter_name)
                        .await;
                }
                continue;
            }
            if entity.kind == ModelKind::Reingestor {
                let reingestor = ReingestorName::from(&entity.identifier);
                let Some(desired_plan) = entrypoints.reingestor(&reingestor).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing desired reingestor '{}'", reingestor.as_str()),
                    });
                };
                let desired_node = revision
                    .nodes
                    .get(entity)
                    .assured("the entrypoint plans were decided from this same schedule");
                /// What the outgoing reingestor left behind, taken out while the execution is
                /// borrowed so the tasks below are awaited without holding that borrow.
                struct RetiredReingestor {
                    tasks: Vec<JoinHandle<()>>,
                    entrypoints: Vec<Arc<IngestorRouteRuntime>>,
                }

                let RetiredReingestor {
                    tasks: old_tasks,
                    entrypoints: old_entrypoints,
                } = {
                    let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution is unavailable for reingestor swap"
                                .to_string(),
                        }
                    })?;
                    let Some(old_plan) = execution
                        .revision
                        .entrypoints
                        .reingestor(&reingestor)
                        .cloned()
                    else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing existing reingestor '{}'",
                                reingestor.as_str()
                            ),
                        });
                    };
                    let old_tasks = execution
                        .reingestor_tasks
                        .remove(entity)
                        .unwrap_or_default();
                    if !old_tasks.is_empty() {
                        for input in &old_plan.inputs {
                            if let Some(services) = execution.relay_services.get(&input.relay) {
                                services.remove_local_runtime_consumer(old_plan.mode);
                            }
                        }
                    }
                    let old_entrypoints = execution
                        .branched_entrypoints
                        .remove(&entity.identifier)
                        .unwrap_or_default();
                    RetiredReingestor {
                        tasks: old_tasks,
                        entrypoints: old_entrypoints,
                    }
                };
                for task in old_tasks {
                    nervix_primitives::task::consume_budget().await;
                    task.abort();
                    task.join_after_shutdown("scheduled node").await;
                }
                for runtime in old_entrypoints {
                    nervix_primitives::task::consume_budget().await;
                    runtime.shutdown().await;
                }

                if Self::scheduled_node_executes_locally(desired_node, local_node_id) {
                    let (routing, shutdown) = {
                        let execution = self.inner.executions.get(domain).ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: "domain execution disappeared during reingestor swap"
                                    .to_string(),
                            }
                        })?;
                        (execution.routing.staged(), execution.shutdown.clone())
                    };
                    let mut inputs = Vec::with_capacity(desired_plan.inputs.len());
                    for input in &desired_plan.inputs {
                        let Some(services) = routing.relay_services.get(&input.relay) else {
                            return Err(RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing reingestor input relay services '{}'",
                                    input.relay.as_str()
                                ),
                            });
                        };
                        inputs.push(PlannedReingestorInput {
                            plan: desired_plan.clone(),
                            input: input.clone(),
                            consumer: ReingestorInputConsumer::Deferred(services.clone()),
                        });
                    }
                    let runtimes = self
                        .start_reingestor_runtimes(
                            ExecutionBuildDeps::from_routing(domain, &routing),
                            &shutdown,
                            RelayRuntimeHandles {
                                registries: &routing.relay_registries,
                                services: &routing.relay_services,
                            },
                            inputs,
                        )
                        .map_err(|report| RuntimeError::entrypoint_binding(domain, report))?;
                    let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution disappeared after reingestor spawn"
                                .to_string(),
                        }
                    })?;
                    execution
                        .branched_entrypoints
                        .extend(runtimes.branched_entrypoints);
                    execution.reingestor_tasks.extend(runtimes.tasks);
                }
                continue;
            }
            if entity.kind == ModelKind::Generator {
                let name = GeneratorName::from(&entity.identifier);
                let Some(generator) = resource_plans.generators.get(&name) else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing desired generator '{}'", entity.identifier),
                    });
                };
                let old_task = self
                    .inner
                    .executions
                    .get_mut(domain)
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: "domain execution is unavailable for generator swap".to_string(),
                    })?
                    .generator_tasks
                    .remove(entity);
                if let Some(task) = old_task {
                    task.abort();
                    task.join_after_shutdown("generator").await;
                }

                if generator.assignment.executes_on(local_node_id) {
                    let (shutdown, spec) = {
                        let execution = self.inner.executions.get(domain).ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: "domain execution disappeared during generator swap"
                                    .to_string(),
                            }
                        })?;
                        let spec = GeneratorTaskSpec::bind(
                            domain,
                            generator,
                            &execution.relay_registries,
                            &execution.relay_services,
                            &execution.udfs,
                        )
                        .map_err(|report| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!("generator binding failed: {report:#}"),
                            }
                        })?;
                        (execution.shutdown.clone(), spec)
                    };
                    let task = self.spawn_generator_task(domain, &shutdown, spec)?;
                    self.inner
                        .executions
                        .get_mut(domain)
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution disappeared after generator spawn"
                                .to_string(),
                        })?
                        .generator_tasks
                        .insert(entity.clone(), task);
                }
                continue;
            }
            let desired_node = revision
                .nodes
                .get(&NodeRef::new(entity.kind, entity.identifier.clone()))
                .ok_or_else(|| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing desired {} '{}'",
                        entity.kind.as_str(),
                        entity.identifier.as_str()
                    ),
                })?;
            let desired_spec = desired_specs
                .processor(entity.kind, &entity.identifier)
                .cloned()
                .ok_or_else(|| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "entity swap for {} '{}' has no scheduled processor spec",
                        entity.kind.as_str(),
                        entity.identifier.as_str()
                    ),
                })?;
            let old_spec = {
                let execution = self.inner.executions.get(domain).ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: "domain execution is unavailable for entity swap".to_string(),
                    }
                })?;
                execution
                    .revision
                    .processors
                    .processor(entity.kind, &entity.identifier)
                    .cloned()
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing existing processor spec for '{}'",
                            entity.identifier.as_str()
                        ),
                    })?
            };
            // The change aspects own which node-local state a swap invalidates, so the runtime
            // applies that contract rather than re-deriving it per processor kind.
            for purge in state_purges.get(entity).into_iter().flatten() {
                match purge {
                    nervix_models::StatePurge::DeduplicatorKeyspace => {
                        self.purge_deduplicator_state(
                            domain,
                            &DeduplicatorName::from(&entity.identifier),
                        )?;
                    }
                    // Reorderer, window, correlator, inferencer and WASM state is carried through
                    // the branch handoff rather than persisted per keyspace, so their replacements
                    // start from the flushed snapshot instead of a purge.
                    nervix_models::StatePurge::ReordererBuffer
                    | nervix_models::StatePurge::WindowAccumulator
                    | nervix_models::StatePurge::CorrelationBuffer
                    | nervix_models::StatePurge::InferencerWarmState
                    | nervix_models::StatePurge::WasmGuestState => {}
                }
            }

            let executes_locally =
                Self::scheduled_node_executes_locally(desired_node, local_node_id);
            let published_plan = if executes_locally {
                Some(
                    self.bind_installed_processor_plan(domain, &desired_spec)
                        .await
                        .map_err(|error| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!("{error:#}"),
                        })?,
                )
            } else {
                None
            };
            let template = published_plan
                .as_ref()
                .map(|plan| plan.template.as_ref().clone());
            let old_task = {
                let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: "domain execution is unavailable for entity swap".to_string(),
                    }
                })?;
                execution.node_tasks.remove(entity)
            };
            let had_old_task = old_task.is_some();
            let handoffs = if let Some(old_task) = old_task {
                old_task
                    .handoff()
                    .await
                    .map_err(|reason| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: reason.to_string(),
                    })?
            } else {
                Vec::new()
            };

            let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: "domain execution disappeared during entity swap".to_string(),
                }
            })?;
            if let Some(published_plan) = published_plan {
                execution
                    .routing
                    .processor_plans
                    .insert(entity.clone(), published_plan);
            } else {
                execution.routing.processor_plans.remove(entity);
            }
            for relay in &old_spec.spec.input_relays {
                if let Some(services) = execution.relay_services.get(relay)
                    && had_old_task
                {
                    services.remove_local_runtime_consumer(old_spec.spec.mode);
                }
            }

            if executes_locally {
                let template = template.assured(
                    "a locally executing processor binds its template before its prior task stops",
                );
                let mut inputs = Vec::with_capacity(desired_spec.spec.input_relays.len());
                for relay in &desired_spec.spec.input_relays {
                    let services = execution.relay_services.get(relay).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing relay services for swapped input '{}'",
                                relay.as_str()
                            ),
                        }
                    })?;
                    inputs.push((
                        relay.clone(),
                        services.add_local_runtime_consumer(desired_spec.spec.mode),
                    ));
                }
                let task = spawn_processor_node_runtime_with_handoffs(
                    ProcessorRuntimeContext::new(self.clone(), domain.clone()),
                    &execution.shutdown,
                    template,
                    inputs,
                    handoffs,
                    self.inner.branch_instance_expiration_scan_interval,
                );
                execution.node_tasks.insert(entity.clone(), task);
            }
        }

        self.apply_dynamic_model_updates(domain, dynamic_updates)
            .await?;
        let processor_plans = self
            .bind_installed_processor_plans(domain, &revision)
            .await
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("{error:#}"),
            })?;
        let message_error_plans = {
            let execution = self.inner.executions.get(domain).ok_or_else(|| {
                RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: "domain execution is unavailable for message-error binding".to_string(),
                }
            })?;
            Arc::new(
                BoundMessageErrorRoutes::bind(
                    revision.message_errors.clone(),
                    MessageErrorRouteBindingContext {
                        relay_registries: &execution.relay_registries,
                        relay_services: &execution.relay_services,
                        materialized_stream_specs: &execution.materialized_stream_specs,
                        lookups: &execution.lookups,
                        udfs: &execution.udfs,
                    },
                )
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("failed to bind message-error routes: {reason:#}"),
                })?,
            )
        };
        let mut routing_published = false;
        if let Some(mut execution) = self.inner.executions.get_mut(domain) {
            if let Some(local_node_id) = local_node_id {
                let remote_consumers =
                    Self::remote_runtime_consumers_for_revision(&revision, local_node_id);
                for (relay, services) in &execution.relay_services {
                    let owner_node = if let Some(node) = revision
                        .nodes
                        .get(&NodeRef::new(ModelKind::Relay, ModelName::from(relay)))
                        && let Some(owner) = node.execution_node()
                    {
                        Some(owner.clone())
                    } else {
                        None
                    };
                    services.replace_owner_node(owner_node);
                    services.replace_remote_runtime_consumers(
                        remote_consumers.get(relay).cloned().unwrap_or_default(),
                    );
                }
            }
            execution.revision = revision;
            execution.message_error_plans = message_error_plans;
            execution.routing.processor_plans = processor_plans;
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

    pub(super) async fn apply_dynamic_schedule_update(
        &self,
        domain: &DomainName,
        revision: Arc<ExecutionRevision>,
        updates: &[DynamicExecutionUpdate],
    ) -> Result<(), RuntimeError> {
        let processor_plans = self
            .bind_installed_processor_plans(domain, &revision)
            .await
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("{error:#}"),
            })?;
        // A reset's new generation must be visible before its supervisor writes the generation's
        // initial checkpoint. Publishing the identity first is safe because the selected inputs
        // remain fenced until the same schedule reaches Ready.
        self.install_state_identities(&revision);
        self.apply_dynamic_model_updates(domain, updates).await?;
        if let Some(mut execution) = self.inner.executions.get_mut(domain) {
            let message_error_plans = Arc::new(
                BoundMessageErrorRoutes::bind(
                    revision.message_errors.clone(),
                    MessageErrorRouteBindingContext {
                        relay_registries: &execution.relay_registries,
                        relay_services: &execution.relay_services,
                        materialized_stream_specs: &execution.materialized_stream_specs,
                        lookups: &execution.lookups,
                        udfs: &execution.udfs,
                    },
                )
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("failed to bind message-error routes: {reason:#}"),
                })?,
            );
            execution.revision = revision;
            execution.message_error_plans = message_error_plans;
            execution.routing.processor_plans = processor_plans;
            execution.routing.publish();
        } else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: "domain execution disappeared while publishing processor plans".to_string(),
            });
        }
        if updates
            .iter()
            .any(|update| !matches!(update, DynamicExecutionUpdate::WasmStateReset { .. }))
        {
            self.force_flush_domain(domain);
        }
        Ok(())
    }

    pub(super) async fn apply_dynamic_model_updates(
        &self,
        domain: &DomainName,
        updates: &[DynamicExecutionUpdate],
    ) -> Result<(), RuntimeError> {
        for update in updates {
            nervix_primitives::task::consume_budget().await;
            match update {
                DynamicExecutionUpdate::RelayCapacity { relay, capacity } => {
                    self.set_relay_capacity(domain, relay, *capacity);
                }
                DynamicExecutionUpdate::Processor => {}
                DynamicExecutionUpdate::WasmStateReset { processor, reset } => {
                    let commands = if let Some(execution) = self.inner.executions.get(domain)
                        && let Some(task) = execution
                            .node_tasks
                            .get(&NodeRef::new(ModelKind::WasmProcessor, processor.clone()))
                    {
                        Some(task.commands.clone())
                    } else {
                        None
                    };
                    if let Some(commands) = commands {
                        let (response, receiver) = oneshot::channel();
                        commands
                            .send(ProcessorNodeCommand::ApplyWasmStateReset {
                                reset: reset.clone(),
                                response,
                            })
                            .await
                            .map_err(|_| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "WASM processor '{}' reset command channel closed",
                                    processor.as_str()
                                ),
                            })?;
                        receiver
                            .await
                            .map_err(|_| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "WASM processor '{}' dropped its reset response",
                                    processor.as_str()
                                ),
                            })?
                            .map_err(|error| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!("{error:#}"),
                            })?;
                    }
                }
                // Endpoint routing reads only a VHOST's hostnames. The certificate belongs to the
                // HTTPS listener, which installs it from the same admitted state on every node.
                DynamicExecutionUpdate::VhostTlsVersion => {}
                DynamicExecutionUpdate::EmitterFlush { emitter, policy } => {
                    let commands = if let Some(execution) = self.inner.executions.get(domain)
                        && let Some(task) = execution.emitter_tasks.get(&NodeRef {
                            kind: ModelKind::Emitter,
                            identifier: ModelName::from(emitter),
                        }) {
                        Some(task.commands.clone())
                    } else {
                        None
                    };
                    if let Some(commands) = commands {
                        ScheduledEmitterTask::reconfigure_via(&commands, policy.clone())
                            .await
                            .map_err(|error| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: error.to_string(),
                            })?;
                    }
                }
            }
        }
        Ok(())
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
    ) -> Result<ScheduledNodePlacement, RuntimeError> {
        let mut placement = ScheduledNodePlacement::default();
        let executes_locally = node.executes_on(local_node_id);
        let assigned_locally = node.is_assigned_to(local_node_id);
        let execution_node = node.execution_node().cloned();
        if let Some(PlacedNodeState::MaterializedRelay(schema)) = state.as_ref()
            && (executes_locally || assigned_locally)
        {
            let relay = RelayName::from(&node.identifier);
            let schema = schema.clone();
            let replica_nodes = node
                .replica_nodes()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let state_placement = self
                .state_placement(
                    domain,
                    RuntimeStateKind::MaterializedRelay,
                    ModelKind::Relay,
                    &relay,
                    None,
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            let mut assignment = self
                .replicated_materialized_stream_state(
                    state_placement,
                    schema,
                    execution_node.clone(),
                    replica_nodes,
                    Some(local_node_id),
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            if let Some(task) =
                self.spawn_materialized_stream_snapshot_task(shutdown_tx, assignment.persistence)
            {
                placement.tasks.push(task);
            }
            if executes_locally {
                placement.materialized_state =
                    Some(assignment.originator.take().ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "materialized relay '{}' lacks authoritative state access",
                                relay.as_str()
                            ),
                        }
                    })?);
            } else {
                let installer = assignment.installer.take().ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "materialized relay '{}' lacks replica installation access",
                            relay.as_str()
                        ),
                    }
                })?;
                if let Some(task) =
                    self.spawn_materialized_stream_replica_poll_task(shutdown_tx, installer)
                {
                    placement.tasks.push(task);
                }
            }
        }

        if let Some(PlacedNodeState::KafkaDomainOffsets) = state.as_ref()
            && assigned_locally
        {
            let ingestor = IngestorName::from(&node.identifier);
            let replica_nodes = node
                .replica_nodes()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let required_replica_acks = replica_nodes.len();
            let mut assignment = self
                .replicated_kafka_offset_state(
                    RuntimeStatePlacement {
                        domain: domain.clone(),
                        state: RuntimeState::KafkaOffset,
                        kind: node.kind(),
                        identifier: node.identifier.clone(),
                        branch_key: None,
                    },
                    node.primary_node.clone(),
                    replica_nodes,
                    required_replica_acks,
                    Some(local_node_id),
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            if let Some(task) =
                self.spawn_kafka_offset_snapshot_task(shutdown_tx, assignment.persistence)
            {
                placement.tasks.push(task);
            }
            if node.is_primary_on(local_node_id) {
                placement.kafka_offset_state =
                    Some(assignment.originator.take().ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "Kafka ingestor '{}' lacks authoritative offset access",
                                ingestor.as_str()
                            ),
                        }
                    })?);
            } else {
                let installer = assignment.installer.take().ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "Kafka ingestor '{}' lacks replica installation access",
                            ingestor.as_str()
                        ),
                    }
                })?;
                if let Some(task) =
                    self.spawn_kafka_offset_replica_poll_task(shutdown_tx, installer)
                {
                    placement.tasks.push(task);
                }
            }
        }

        let aggregate_primary_node = execution_node
            .clone()
            .or_else(|| executes_locally.then(|| local_node_id.clone()));
        let aggregate_replica_nodes = if execution_node.is_some() && node.kind() != ModelKind::Relay
        {
            node.replica_nodes()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if node.kind() != ModelKind::Relay
            && (executes_locally || (assigned_locally && aggregate_primary_node.is_some()))
        {
            let required_replica_acks = aggregate_replica_nodes.len();
            let state = self
                .replicated_branch_aggregated_state(
                    RuntimeStatePlacement {
                        domain: domain.clone(),
                        state: RuntimeState::BranchAggregated,
                        kind: node.kind(),
                        identifier: node.identifier.clone(),
                        branch_key: None,
                    },
                    aggregate_primary_node.clone(),
                    aggregate_primary_node.unwrap_or_else(|| local_node_id.clone()),
                    aggregate_replica_nodes,
                    required_replica_acks,
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            let task = if executes_locally {
                self.spawn_branch_aggregated_snapshot_task(shutdown_tx, state)
            } else {
                self.spawn_branch_aggregated_replica_poll_task(shutdown_tx, state)
            };
            if let Some(task) = task {
                placement.tasks.push(task);
            }
        }
        if assigned_locally && !node.is_primary_on(local_node_id) {
            let task = self
                .spawn_branch_state_replica_poll_task(shutdown_tx, domain, node)
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            if let Some(task) = task {
                placement.tasks.push(task);
            }
        }
        if node.kind() != ModelKind::Relay || executes_locally {
            self.inner.metrics.register_global_node(
                domain,
                node.kind(),
                &node.identifier,
                execution_node.as_ref().or(Some(local_node_id)),
            );
        }

        Ok(placement)
    }

    /// Installs the graph a domain's registry state holds when the node starts, before the cluster
    /// schedules it.
    pub(crate) async fn apply_changes(
        &self,
        domain: &DomainName,
        revision: Option<Arc<ExecutionRevision>>,
    ) -> Result<(), RuntimeError> {
        self.rebuild_domain_execution(domain, revision).await
    }

    pub(super) fn scheduled_node_executes_locally(
        node: &ExecutionNode,
        local_node_id: Option<&ClusterNodeName>,
    ) -> bool {
        if let Some(local_node_id) = local_node_id {
            return node.executes_on(local_node_id);
        }
        node.primary_node.is_none() && node.assigned_nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{
        AckMode, BranchSelection, ClusterNodeName, ClusterSchedule, CreateDeduplicator,
        CreateJunction, CreateRelay, CreateSchema, DeduplicatorName, DomainConfig, DomainPace,
        DomainSchedule, DomainState, DomainStatus, ModelKind, ModelName, NodeRef, ParseAsType,
        ProcessorInputs, ProcessorOutputs, RelayBranching, RelayName, ScheduledNode, SchemaField,
        SchemaName,
    };
    use nonzero_ext::nonzero;

    use super::*;

    #[nervix_primitives::test]
    async fn scheduled_relay_placement_does_not_create_metric_replication_state() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let relay = named::<RelayName>("events");
        let schema = named::<SchemaName>("event");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Running,
                start_version: 1,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            },
        )]));
        let schedule = ClusterSchedule::from_iter([DomainSchedule::new(
            domain.clone(),
            vec![
                ScheduledNode::new(
                    nervix_models::Model::Schema(CreateSchema {
                        name: schema.clone(),
                        fields: vec![SchemaField {
                            name: named("value"),
                            ty: ParseAsType::I64,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                scheduled_model(nervix_models::Model::Relay(CreateRelay {
                    name: relay.clone(),
                    schema,
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
            ],
            Vec::new(),
        )]);

        runtime
            .apply_cluster_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &schedule,
            )
            .await
            .expect("relay schedule should build");

        assert!(
            !runtime
                .inner
                .replicated_branch_aggregated_states
                .iter()
                .any(|state| state.key().kind == ModelKind::Relay),
            "relay metrics must remain volatile owner-only state"
        );
    }

    #[nervix_primitives::test]
    async fn paused_schedule_keeps_full_execution_without_rebuilding_unchanged_graph() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let schema = named::<SchemaName>("notification");
        let relay = named::<RelayName>("notifications");
        let running = DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: DomainStatus::Running,
            start_version: 1,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        };
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), running.clone())]));
        let schedule = ClusterSchedule::from_iter([DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_model(nervix_models::Model::Schema(CreateSchema {
                    name: schema.clone(),
                    fields: vec![SchemaField {
                        name: named("user_id"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                })),
                scheduled_model(nervix_models::Model::Relay(CreateRelay {
                    name: relay.clone(),
                    schema,
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
            ],
            Vec::new(),
        )]);
        runtime
            .apply_cluster_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &schedule,
            )
            .await
            .expect("running schedule should build");
        let revision_before_pause = runtime
            .inner
            .executions
            .get(&domain)
            .assured("the running domain has an installed revision")
            .revision
            .clone();

        let mut paused = running;
        paused.status = DomainStatus::Paused;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), paused)]));
        runtime
            .apply_cluster_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &schedule,
            )
            .await
            .expect("paused schedule should remain active");

        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("paused execution should remain");
        assert!(!execution.passive_only);
        assert!(Arc::ptr_eq(&revision_before_pause, &execution.revision));
        assert!(execution.relay_registries.contains_key(&relay));
    }

    #[nervix_primitives::test]
    async fn stale_cluster_state_cannot_replace_a_newer_runtime_schedule() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let schema = named::<SchemaName>("notification");
        let relay = named::<RelayName>("notifications");
        let domains = BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Running,
                start_version: 1,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            },
        )]);
        let schema_node = scheduled_model(nervix_models::Model::Schema(CreateSchema {
            name: schema.clone(),
            fields: vec![SchemaField {
                name: named("user_id"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }));
        let stale_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
            domain.clone(),
            vec![schema_node.clone()],
            Vec::new(),
        )]);
        let current_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
            domain.clone(),
            vec![
                schema_node,
                scheduled_model(nervix_models::Model::Relay(CreateRelay {
                    name: relay.clone(),
                    schema,
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
            ],
            Vec::new(),
        )]);

        runtime
            .apply_cluster_state(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                2,
                &domains,
                &BTreeMap::new(),
                &current_schedule,
            )
            .await
            .expect("current cluster state should build");
        runtime
            .apply_cluster_state(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                1,
                &domains,
                &BTreeMap::new(),
                &stale_schedule,
            )
            .await
            .expect("stale cluster state should be ignored");

        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("current execution should remain");
        assert_eq!(
            execution.revision.ownership_handoff_fingerprint,
            ExecutionRevision::ownership_fingerprint(
                current_schedule
                    .domains
                    .get(&domain)
                    .assured("the current schedule has the domain")
            )
            .assured("the current schedule has an ownership fingerprint")
        );
        assert!(execution.relay_registries.contains_key(&relay));
    }

    #[test]
    fn a_node_that_has_applied_nothing_accepts_every_revision() {
        let application = ScheduleApplication::default();

        assert!(application.advances_beyond_applied(0));
        assert!(application.advances_beyond_applied(1));
        assert!(application.advances_beyond_applied(u64::MAX));
    }

    #[test]
    fn an_applied_revision_suppresses_equal_and_lower_revisions() {
        let mut application = ScheduleApplication::default();

        application.record_applied(0);
        assert!(!application.advances_beyond_applied(0));
        assert!(application.advances_beyond_applied(1));

        application.record_applied(7);
        assert!(!application.advances_beyond_applied(0));
        assert!(!application.advances_beyond_applied(6));
        assert!(!application.advances_beyond_applied(7));
        assert!(application.advances_beyond_applied(8));
    }

    #[test]
    fn the_largest_applied_revision_suppresses_every_other_revision() {
        let mut application = ScheduleApplication::default();

        application.record_applied(u64::MAX);

        assert!(!application.advances_beyond_applied(u64::MAX));
        assert!(!application.advances_beyond_applied(u64::MAX - 1));
        assert!(!application.advances_beyond_applied(0));
    }

    #[nervix_primitives::test]
    async fn the_largest_revision_still_suppresses_a_later_stale_cluster_state() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let schema = named::<SchemaName>("notification");
        let relay = named::<RelayName>("notifications");
        let domains = BTreeMap::from([(domain.clone(), unpaced_domain_state(domain.as_str()))]);
        let schema_node = scheduled_model(nervix_models::Model::Schema(CreateSchema {
            name: schema.clone(),
            fields: vec![SchemaField {
                name: named("user_id"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }));
        let stale_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
            domain.clone(),
            vec![schema_node.clone()],
            Vec::new(),
        )]);
        let current_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
            domain.clone(),
            vec![
                schema_node,
                scheduled_model(nervix_models::Model::Relay(CreateRelay {
                    name: relay.clone(),
                    schema,
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
            ],
            Vec::new(),
        )]);

        runtime
            .apply_cluster_state(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                u64::MAX,
                &domains,
                &BTreeMap::new(),
                &current_schedule,
            )
            .await
            .expect("current cluster state should build");
        runtime
            .apply_cluster_state(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                1,
                &domains,
                &BTreeMap::new(),
                &stale_schedule,
            )
            .await
            .expect("stale cluster state should be ignored");

        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("current execution should remain");
        assert_eq!(
            execution.revision.ownership_handoff_fingerprint,
            ExecutionRevision::ownership_fingerprint(
                current_schedule
                    .domains
                    .get(&domain)
                    .assured("the current schedule has the domain")
            )
            .assured("the current schedule has an ownership fingerprint")
        );
        assert!(execution.relay_registries.contains_key(&relay));
    }

    #[nervix_primitives::test]
    async fn a_failed_application_leaves_its_revision_ready_to_apply_again() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let schema = named::<SchemaName>("notification");
        let relay = named::<RelayName>("notifications");
        let domains = BTreeMap::from([(domain.clone(), unpaced_domain_state(domain.as_str()))]);
        let relay_node = scheduled_model(nervix_models::Model::Relay(CreateRelay {
            name: relay.clone(),
            schema: schema.clone(),
            buffer: nonzero!(2usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        }));
        let unbuildable_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
            domain.clone(),
            vec![relay_node.clone()],
            Vec::new(),
        )]);
        let buildable_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_model(nervix_models::Model::Schema(CreateSchema {
                    name: schema,
                    fields: vec![SchemaField {
                        name: named("user_id"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                })),
                relay_node,
            ],
            Vec::new(),
        )]);

        let failure = runtime
            .apply_cluster_state(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                4,
                &domains,
                &BTreeMap::new(),
                &unbuildable_schedule,
            )
            .await
            .expect_err("a relay without its schema should fail to build");
        assert!(matches!(
            failure.current_context(),
            RuntimeError::BuildDomainExecution { .. }
        ));
        runtime
            .apply_cluster_state(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                4,
                &domains,
                &BTreeMap::new(),
                &buildable_schedule,
            )
            .await
            .expect("the revision that failed should be applied again");

        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("the reapplied execution should exist");
        assert!(execution.relay_registries.contains_key(&relay));
    }

    #[nervix_primitives::test]
    async fn branch_preserving_processors_build_standalone_schedule_nodes() {
        let runtime = Runtime::default();
        let domain = domain("default");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let order_schema = named::<SchemaName>("order_event");
        let order_relay = |name: &str| {
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                name: named(name),
                schema: order_schema.clone(),
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            }))
        };
        let processor_routes = |name: &str| {
            let mut routes = ProcessorOutputs::single(named(name));
            routes.routes[0].construction.inherit = Some(nervix_models::Inheritance::All);
            routes.with_flush_policy(FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string(),
            })
        };
        let schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_model(nervix_models::Model::Schema(CreateSchema {
                    name: order_schema.clone(),
                    fields: vec![SchemaField {
                        name: named("order_id"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                })),
                order_relay("orders"),
                order_relay("projected_orders"),
                order_relay("left_orders"),
                order_relay("right_orders"),
                order_relay("joined_orders"),
                scheduled_model(nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_orders"),
                    from: ProcessorInputs::single(named("orders")),
                    output_routes: processor_routes("projected_orders"),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                })),
                scheduled_model(nervix_models::Model::Junction(CreateJunction {
                    name: named("join_orders"),
                    from: ProcessorInputs::new(
                        vec![named("left_orders"), named("right_orders")],
                        Vec::new(),
                    ),
                    output_routes: processor_routes("joined_orders"),
                    branched_by: BranchSelection::unbranched(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                })),
            ],
            Vec::new(),
        );

        runtime
            .rebuild_domain_from_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &domain,
                Some(schedule),
                true,
            )
            .await
            .expect("standalone branch-preserving processors must build");
        runtime
            .rebuild_domain_from_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &domain,
                None,
                true,
            )
            .await
            .expect("domain teardown must stop processor runtimes");
    }

    #[nervix_primitives::test]
    async fn scheduled_processor_entity_swap_is_not_junction_specific() {
        let runtime = Runtime::default();
        attach_loopback_cluster(
            &runtime,
            &ClusterNodeName::parse("node-1").expect("valid name"),
        )
        .await;
        let domain = domain("default");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let event_schema = named::<SchemaName>("event");
        let processor = named::<DeduplicatorName>("deduplicate_events");
        let schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_model(nervix_models::Model::Schema(CreateSchema {
                    name: event_schema.clone(),
                    fields: vec![SchemaField {
                        name: named("event_id"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                })),
                scheduled_model(nervix_models::Model::Relay(CreateRelay {
                    name: named("events"),
                    schema: event_schema.clone(),
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
                scheduled_model(nervix_models::Model::Relay(CreateRelay {
                    name: named("unique_events"),
                    schema: event_schema,
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
                scheduled_model(nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: processor.clone(),
                    from: ProcessorInputs::single(named("events")),
                    output_routes: with_inherit_all(ProcessorOutputs::single(named(
                        "unique_events",
                    )))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    }),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![expression("input.event_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                })),
            ],
            Vec::new(),
        );

        runtime
            .rebuild_domain_from_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &domain,
                Some(schedule.clone()),
                true,
            )
            .await
            .expect("scheduled deduplicator must build");
        let entity = NodeRef {
            kind: ModelKind::Deduplicator,
            identifier: ModelName::from(&processor),
        };
        assert_eq!(
            runtime.entity_pause_relays(&domain, std::slice::from_ref(&entity)),
            vec![named("events")],
            "every scheduled processor swap must gate its input relays"
        );

        let mut desired = schedule;
        let nervix_models::Model::Deduplicator(config) = desired
            .nodes
            .values_mut()
            .find(|node| node.kind() == ModelKind::Deduplicator)
            .expect("schedule must contain the processor")
            .config
            .as_mut()
        else {
            panic!("scheduled processor must contain a deduplicator model");
        };
        config.mode = AckMode::Detached;

        let desired_revision = ExecutionRevision::from_schedule(&desired)
            .assured("the changed processor has a complete execution revision");
        runtime
            .swap_scheduled_nodes(
                &domain,
                desired_revision.clone(),
                &EntitySwapExecution {
                    entities: vec![entity],
                    reassignments: Vec::new(),
                    dynamic_updates: Vec::new(),
                    state_purges: BTreeMap::new(),
                    gate_relays: Vec::new(),
                },
            )
            .await
            .expect("non-junction scheduled processors must use the shared swap path");
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("domain execution must remain installed");
        assert_eq!(
            execution.revision.ownership_handoff_fingerprint,
            desired_revision.ownership_handoff_fingerprint
        );
        assert!(execution.node_tasks.contains_key(&NodeRef {
            kind: ModelKind::Deduplicator,
            identifier: ModelName::from(&processor),
        }));
        drop(execution);

        #[cfg(feature = "testing")]
        {
            let node = ClusterNodeName::parse("node-1")
                .assured("the test node is an identifier-shaped literal");
            let mut recovered = desired.clone();
            let nervix_models::Model::Deduplicator(config) = recovered
                .nodes
                .values_mut()
                .find(|node| node.kind() == ModelKind::Deduplicator)
                .assured("the test schedule contains the processor")
                .config
                .as_mut()
            else {
                panic!("scheduled processor must contain a deduplicator model");
            };
            config.mode = AckMode::Attached;
            runtime
                .inner
                .fault_injection
                .fail_next_entity_schedule_swap_on(node.clone(), domain.clone());

            let recovered_revision = ExecutionRevision::from_schedule(&recovered)
                .assured("the recovered processor has a complete execution revision");
            let delta = ExecutionDelta::between(Some(&desired), Some(&recovered));
            let application = runtime
                .apply_schedule_delta(&node, &recovered_revision, &delta, true)
                .await
                .assured("the domain rebuild recovers the injected entity swap failure");
            assert!(application.applied_incrementally);
            let expansion = application
                .recovery_expansion
                .assured("entity swap fallback reports its wider recovery scope");
            assert_eq!(expansion.domain, domain);
            assert!(
                expansion
                    .reason
                    .contains("injected entity-level schedule apply failure")
            );
            assert_eq!(
                expansion.scope,
                recovered.nodes.keys().cloned().collect::<Vec<_>>()
            );
            assert_eq!(
                runtime
                    .inner
                    .executions
                    .get(&domain)
                    .assured("recovery reinstalls the test domain")
                    .revision
                    .ownership_handoff_fingerprint,
                recovered_revision.ownership_handoff_fingerprint
            );
        }
    }

    #[nervix_primitives::test]
    async fn scheduled_entity_swap_reinstalls_state_schema_fingerprints() {
        let runtime = Runtime::default();
        attach_loopback_cluster(
            &runtime,
            &ClusterNodeName::parse("node-1").expect("valid name"),
        )
        .await;
        let domain = domain("default");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let event_schema = named::<SchemaName>("event");
        let processor = named::<DeduplicatorName>("deduplicate_events");
        let schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_model(nervix_models::Model::Schema(CreateSchema {
                    name: event_schema.clone(),
                    fields: vec![SchemaField {
                        name: named("event_id"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                })),
                scheduled_model(nervix_models::Model::Relay(CreateRelay {
                    name: named("events"),
                    schema: event_schema.clone(),
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
                scheduled_model(nervix_models::Model::Relay(CreateRelay {
                    name: named("unique_events"),
                    schema: event_schema,
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
                scheduled_model(nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: processor.clone(),
                    from: ProcessorInputs::single(named("events")),
                    output_routes: with_inherit_all(ProcessorOutputs::single(named(
                        "unique_events",
                    )))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    }),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![expression("input.event_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                })),
            ],
            Vec::new(),
        );

        runtime
            .rebuild_domain_from_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &domain,
                Some(schedule.clone()),
                true,
            )
            .await
            .expect("scheduled deduplicator must build");

        let mut desired = schedule;
        let processor_node = desired
            .nodes
            .values_mut()
            .find(|node| node.kind() == ModelKind::Deduplicator)
            .expect("schedule must contain the processor");
        processor_node.schema_fingerprint = SchemaFingerprint::from_digest([7; 32]);
        let nervix_models::Model::Deduplicator(config) = processor_node.config.as_mut() else {
            panic!("scheduled processor must contain a deduplicator model");
        };
        config.mode = AckMode::Detached;
        let entity = NodeRef {
            kind: ModelKind::Deduplicator,
            identifier: ModelName::from(&processor),
        };

        let desired_revision = ExecutionRevision::from_schedule(&desired)
            .assured("the changed processor has a complete execution revision");
        runtime
            .swap_scheduled_nodes(
                &domain,
                desired_revision,
                &EntitySwapExecution {
                    entities: vec![entity],
                    reassignments: Vec::new(),
                    dynamic_updates: Vec::new(),
                    state_purges: BTreeMap::new(),
                    gate_relays: Vec::new(),
                },
            )
            .await
            .expect("entity swap must apply");

        let installed = runtime
            .inner
            .state_identities
            .get(&DomainNodeRef::node_in(
                domain,
                ModelKind::Deduplicator,
                ModelName::from(&processor),
            ))
            .map(|entry| entry.value().schema_fingerprint);
        assert_eq!(
            installed,
            Some(SchemaFingerprint::from_digest([7; 32])),
            "an entity swap must reinstall the schedule's state schema fingerprints so persisted \
             runtime state is not stranded under the pre-swap fingerprint"
        );
    }
}
