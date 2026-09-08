use super::*;

impl Runtime {
    pub(super) fn branched_specs_by_identifier(
        specs: &[BranchedIngestorSpec],
    ) -> HashMap<ModelName, Vec<BranchedIngestorSpec>> {
        let mut specs_by_identifier = HashMap::default();
        for spec in specs {
            specs_by_identifier
                .entry(spec.identifier.clone())
                .or_insert_with(Vec::new)
                .push(spec.clone());
        }
        specs_by_identifier
    }

    pub async fn apply_cluster_schedule(
        &self,
        local_node_id: &ClusterNodeName,
        schedule: &ClusterSchedule,
    ) -> Result<(), RuntimeError> {
        let _lock = self.inner.schedule_apply_lock.lock().await;
        self.apply_cluster_schedule_locked(local_node_id, schedule, true)
            .await
    }

    pub async fn apply_cluster_state(
        &self,
        local_node_id: &ClusterNodeName,
        revision: u64,
        domains: &BTreeMap<DomainName, DomainState>,
        schedule: &ClusterSchedule,
    ) -> Result<(), RuntimeError> {
        let _lock = self.inner.schedule_apply_lock.lock().await;
        let applied_revision = self.inner.applied_cluster_revision.load(Ordering::Acquire);
        if applied_revision != u64::MAX && revision <= applied_revision {
            return Ok(());
        }

        self.sync_domains(domains);
        self.apply_cluster_schedule_locked(local_node_id, schedule, false)
            .await?;
        self.inner
            .applied_cluster_revision
            .store(revision, Ordering::Release);
        Ok(())
    }

    pub(super) async fn apply_cluster_schedule_locked(
        &self,
        local_node_id: &ClusterNodeName,
        schedule: &ClusterSchedule,
        start_ingestors: bool,
    ) -> Result<(), RuntimeError> {
        let scheduled_domains = schedule
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
        let existing_schedules = {
            self.inner
                .executions
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().schedule.clone()))
                .collect::<HashMap<_, _>>()
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
            match self
                .rebuild_domain_from_schedule(local_node_id, domain, None, start_ingestors)
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

        for (domain_id, domain) in &schedule.domains {
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
            if existing_schedules.get(&domain.domain) != Some(domain)
                || existing_passive_only.get(&domain.domain) != Some(&desired_passive_only)
                || existing_start_versions.get(&domain.domain) != Some(&desired_start_version)
            {
                let applied_incrementally = if !desired_passive_only
                    && existing_passive_only.get(&domain.domain) == Some(&desired_passive_only)
                    && existing_start_versions.get(&domain.domain) == Some(&desired_start_version)
                    && let Some(existing_schedule) = existing_schedules.get(&domain.domain)
                {
                    self.apply_schedule_delta(
                        local_node_id,
                        existing_schedule,
                        domain,
                        start_ingestors,
                    )
                    .await?
                } else {
                    false
                };
                if !applied_incrementally {
                    match self
                        .rebuild_domain_from_schedule(
                            local_node_id,
                            &domain.domain,
                            Some(domain.clone()),
                            start_ingestors,
                        )
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
                    self.start_missing_domain_ingestors(&domain.domain).await?;
                }
                self.release_domain_ingestor_quiesce(&domain.domain);
            }
        }

        Ok(())
    }

    /// Applies a changed schedule without tearing the domain down when the delta allows it.
    /// Returns `false` when the delta demands a full rebuild from the schedule instead.
    pub(super) async fn apply_schedule_delta(
        &self,
        local_node_id: &ClusterNodeName,
        existing_schedule: &DomainSchedule,
        desired: &DomainSchedule,
        start_ingestors: bool,
    ) -> Result<bool, RuntimeError> {
        match ScheduleDelta::classify(existing_schedule, desired) {
            ScheduleDelta::Unchanged => Ok(true),
            ScheduleDelta::Dynamic(updates) => {
                self.apply_dynamic_schedule_update(&desired.domain, desired.clone(), &updates)
                    .await?;
                Ok(true)
            }
            ScheduleDelta::EntitySwap {
                entities,
                reassignments,
                dynamic_updates,
            } => {
                if let Err(error) = self
                    .swap_scheduled_nodes(
                        &desired.domain,
                        desired.clone(),
                        &entities,
                        &reassignments,
                        &dynamic_updates,
                    )
                    .await
                {
                    warn!(
                        domain = desired.domain.as_str(),
                        error = %error,
                        "entity-level schedule apply failed; rebuilding domain"
                    );
                    self.rebuild_domain_from_schedule(
                        local_node_id,
                        &desired.domain,
                        Some(desired.clone()),
                        start_ingestors,
                    )
                    .await?;
                }
                Ok(true)
            }
            ScheduleDelta::Rebuild => Ok(false),
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
        desired: &DomainSchedule,
        reassignments: &[NodeRef],
    ) -> Vec<NodeRef> {
        let Some(local_node_id) = self.inner.remote_dispatch.local_node_id.read().clone() else {
            return Vec::new();
        };
        let Some(execution) = self.inner.executions.get(domain) else {
            return Vec::new();
        };
        reassignments
            .iter()
            .filter(|entity| {
                entity.kind != ModelKind::Relay
                    && entity.kind != ModelKind::Lookup
                    && Self::scheduled_node(&execution.schedule, entity).is_some_and(|existing| {
                        Self::scheduled_node(desired, entity).is_some_and(|desired_node| {
                            existing.executes_on(&local_node_id)
                                != desired_node.executes_on(&local_node_id)
                        })
                    })
            })
            .cloned()
            .collect()
    }

    pub(super) fn scheduled_node<'a>(
        schedule: &'a DomainSchedule,
        entity: &NodeRef,
    ) -> Option<&'a ScheduledNode> {
        schedule
            .nodes
            .values()
            .find(|node| node.kind() == entity.kind && node.identifier == entity.identifier)
    }

    /// Rebuilds the placement-derived runtime of every reassigned node: the replicated states this
    /// cluster node owns or replicates for it, including a materialized relay's state task. Nodes
    /// the schedule did not reassign are never touched.
    pub(super) async fn rebind_reassigned_nodes(
        &self,
        domain: &DomainName,
        schedule: &DomainSchedule,
        reassignments: &[NodeRef],
        local_node_id: Option<&ClusterNodeName>,
    ) -> Result<(), RuntimeError> {
        let Some(local_node_id) = local_node_id else {
            return Ok(());
        };
        if reassignments.is_empty() {
            return Ok(());
        }
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
        for entity in reassignments {
            tokio::task::consume_budget().await;
            let desired_node = Self::scheduled_node(schedule, entity).ok_or_else(|| {
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
                Self::scheduled_node(&execution.schedule, entity)
                    .is_some_and(|existing| existing.executes_on(local_node_id))
            });
            let executes_locally = desired_node.executes_on(local_node_id);
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
                tokio::task::consume_budget().await;
                task.abort();
                task.join_after_shutdown("placement").await;
            }

            let materialized_relay = match desired_node.config.as_ref() {
                Model::Relay(relay) if relay.materialized_state.is_some() => {
                    Some(relay.name.clone())
                }
                _ => None,
            };
            let materialized_schema = if let Some(relay) = materialized_relay.as_ref()
                && let Some(execution) = self.inner.executions.get(domain)
                && let Some(schema) = execution.relay_schemas.get(relay)
            {
                Some(schema.arrow_schema())
            } else {
                None
            };
            let placement = self.build_scheduled_node_placement(
                domain,
                &shutdown,
                desired_node,
                local_node_id,
                materialized_schema,
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
                            .map_err(|reason| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason,
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
                            retention: RelayRetention::from_schedule(domain, schedule, &relay)?,
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
                    RelayRetention::from_schedule(
                        domain,
                        schedule,
                        &RelayName::from(&entity.identifier),
                    )?,
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
        if relay_states_moved {
            self.bump_relay_state_epoch(domain);
        }
        Ok(())
    }

    pub(super) async fn swap_scheduled_nodes(
        &self,
        domain: &DomainName,
        schedule: DomainSchedule,
        entities: &[NodeRef],
        reassignments: &[NodeRef],
        dynamic_updates: &[nervix_models::DynamicModelUpdate],
    ) -> Result<(), RuntimeError> {
        let local_node_id = self.inner.remote_dispatch.local_node_id.read().clone();
        // A reassignment only replaces this cluster node's runtime when the node stopped or
        // started executing here. A node that keeps executing here, such as a server-side ingestor
        // that merely lost one of its other placements, is left running.
        let relocated = self.locally_relocated_nodes(domain, &schedule, reassignments);
        let entities = SortedSet::from_unsorted(
            entities
                .iter()
                .chain(relocated.iter())
                .cloned()
                .collect::<Vec<_>>(),
        )
        .into_vec();
        let entities = entities.as_slice();
        let desired_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
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
        relays.extend(Self::entity_pause_relays_for_schedule(&schedule, &gated));
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

        let desired_graph = StdArc::new(ActiveGraph::from_scheduled_models(&schedule).map_err(
            |error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("failed to build entity-swap schedule graph: {error}"),
            },
        )?);
        let graph_handle = self.domain_graph_handle(domain).await;
        // Publish the whole desired graph before any entity swaps so no task observes a
        // half-applied topology while its siblings are still being replaced.
        graph_handle.store(Some(desired_graph));
        let desired_model_index = schedule
            .nodes
            .values()
            .map(|node| (*node.config).clone())
            .collect::<ModelIndex>();

        // Materialized relay state uses a start-version-qualified schema fingerprint. Install the
        // desired fingerprints before constructing state so the post-swap stale-state purge does
        // not discard the newly attached state instance.
        self.install_state_schema_fingerprints(&schedule);
        self.rebind_reassigned_nodes(domain, &schedule, reassignments, local_node_id.as_ref())
            .await?;

        for entity in entities {
            tokio::task::consume_budget().await;
            if entity.kind == ModelKind::Relay {
                let ScheduledModel {
                    config: desired_relay,
                    node: desired_node,
                } = schedule
                    .scheduled::<CreateRelay>(entity.identifier.clone())
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing desired relay '{}'", entity.identifier.as_str()),
                    })?;
                let desired_materialized = desired_relay.materialized_state.is_some();
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
                                desired_node.effective_branching.clone().unwrap_or_default(),
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
                    tokio::task::consume_budget().await;
                    task.abort();
                    task.join_after_shutdown("placement").await;
                }

                if desired_materialized {
                    let placement = self.build_scheduled_node_placement(
                        domain,
                        &shutdown,
                        desired_node,
                        local_node_id.as_ref().ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: "local node id is unavailable for relay transition"
                                    .to_string(),
                            }
                        })?,
                        Some(schema.arrow_schema()),
                    )?;
                    let state_task = if desired_node.executes_on(local_node_id.as_ref().verified(
                        "the resolution above returned an error unless the local node id is \
                         present",
                    )) {
                        Some(self.spawn_relay_state_task(
                            domain,
                            RelayStateTaskSpec {
                                relay: RelayName::from(&entity.identifier.clone()),
                                state: placement.materialized_state.clone().ok_or_else(|| {
                                    RuntimeError::BuildDomainExecution {
                                        domain: domain.as_str().to_string(),
                                        reason: format!(
                                            "missing materialized state for relay '{}'",
                                            entity.identifier.as_str()
                                        ),
                                    }
                                })?,
                                retention: RelayRetention::from_schedule(
                                    domain,
                                    &schedule,
                                    &RelayName::from(&entity.identifier),
                                )?,
                                receiver: services.add_local_runtime_consumer(AckMode::Detached),
                            },
                        ))
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
                self.bump_relay_state_epoch(domain);
                if was_materialized && !desired_materialized {
                    self.purge_materialized_relay_state(
                        domain,
                        &RelayName::from(&entity.identifier),
                    )?;
                }
                continue;
            }
            if entity.kind == ModelKind::Ingestor {
                let ScheduledModel {
                    config: desired_ingestor,
                    node: desired_node,
                } = schedule
                    .scheduled::<CreateIngestor>(entity.identifier.clone())
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing desired ingestor '{}'",
                            entity.identifier.as_str()
                        ),
                    })?;

                let key = entity.in_domain(domain);
                if self.inner.ingestors.contains_key(&key) {
                    self.stop_ingestor(domain, &IngestorName::from(&entity.identifier))
                        .await?;
                }

                // The ingestor builds its branch entrypoints from the specs the execution holds
                // for it, so an ingestor arriving on this node needs its desired specs installed
                // before it starts and one leaving needs them removed.
                let desired_entrypoint_specs = desired_specs
                    .entrypoints
                    .iter()
                    .filter(|spec| {
                        spec.kind == ModelKind::Ingestor && spec.identifier == entity.identifier
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                    if desired_entrypoint_specs.is_empty() {
                        execution.branched_ingestors.remove(&entity.identifier);
                    } else {
                        execution.branched_ingestors.insert(
                            ModelName::from(&BranchName::from(&entity.identifier)),
                            desired_entrypoint_specs,
                        );
                    }
                }

                if Self::scheduled_node_executes_locally(desired_node, local_node_id.as_ref()) {
                    let source_model =
                        Self::source_model_for_scheduled_ingestor(&schedule, desired_ingestor)
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing source model for swapped ingestor '{}'",
                                    entity.identifier.as_str()
                                ),
                            })?;
                    let plan = IngestorStartPlan::decide(domain, desired_node, &source_model)
                        .map_err(|error| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "cannot plan swapped ingestor '{}': {error}",
                                desired_ingestor.name.as_str()
                            ),
                        })?;
                    self.start_ingestor(plan).await?;
                }
                continue;
            }
            if entity.kind == ModelKind::Emitter {
                let ScheduledModel {
                    config: desired_emitter,
                    node: desired_node,
                } = schedule
                    .scheduled::<CreateEmitter>(entity.identifier.clone())
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing desired emitter '{}'", entity.identifier.as_str()),
                    })?;
                let desired_emitter = desired_emitter.clone();
                let (old_emitter, old_task) = {
                    let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution is unavailable for emitter swap".to_string(),
                        }
                    })?;
                    let old_node = execution
                        .schedule
                        .nodes
                        .get(&NodeRef::new(ModelKind::Emitter, entity.identifier.clone()))
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing existing emitter '{}'",
                                entity.identifier.as_str()
                            ),
                        })?;
                    let Model::Emitter(old_emitter) = old_node.config.as_ref() else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing existing emitter '{}'",
                                entity.identifier.as_str()
                            ),
                        });
                    };
                    let old_emitter = old_emitter.clone();
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

                let executes_locally = local_node_id
                    .as_ref()
                    .is_some_and(|node_id| desired_node.executes_on(node_id));
                /// What spawning the swapped emitter's task needs from the domain execution,
                /// taken while it is borrowed so the spawn itself runs without holding that
                /// borrow.
                struct EmitterSpawnInputs {
                    shutdown: watch::Sender<bool>,
                    codecs: HashMap<CodecName, Arc<CompiledCodec>>,
                    clients: HashMap<ClientName, Arc<Model>>,
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
                        for input_relay in old_emitter.from.relays() {
                            if let Some(services) = execution.relay_services.get(input_relay) {
                                services.remove_local_runtime_consumer(old_emitter.mode);
                            }
                        }
                    }
                    if !executes_locally {
                        None
                    } else {
                        let inputs = desired_emitter
                            .from
                            .relays()
                            .iter()
                            .map(|input_relay| {
                                let Some(services) = execution.relay_services.get(input_relay)
                                else {
                                    return Err(RuntimeError::BuildDomainExecution {
                                        domain: domain.as_str().to_string(),
                                        reason: format!(
                                            "missing relay services for swapped emitter input '{}'",
                                            input_relay.as_str()
                                        ),
                                    });
                                };
                                Ok((
                                    input_relay.clone(),
                                    services.add_local_runtime_consumer(desired_emitter.mode),
                                ))
                            })
                            .collect::<Result<Vec<_>, RuntimeError>>()?;
                        let deps = self.emitter_task_deps(
                            ExecutionBuildDeps {
                                domain,
                                relay_schemas: &execution.relay_schemas,
                                relay_branchings: &execution.relay_branchings,
                                materialized_relay_specs: &execution.materialized_stream_specs,
                                lookups: &execution.lookups,
                            },
                            &desired_emitter,
                        )?;
                        Some(EmitterSpawnInputs {
                            shutdown: execution.shutdown.clone(),
                            codecs: execution.codecs.clone(),
                            clients: execution.clients.clone(),
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
                            clients: &spawn.clients,
                            deps: spawn.deps,
                        },
                        desired_emitter,
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
                continue;
            }
            if entity.kind == ModelKind::Reingestor {
                let desired_node = schedule
                    .nodes
                    .get(&NodeRef::new(
                        ModelKind::Reingestor,
                        entity.identifier.clone(),
                    ))
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing desired reingestor '{}'",
                            entity.identifier.as_str()
                        ),
                    })?;
                let Model::Reingestor(desired_reingestor) = desired_node.config.as_ref() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "desired reingestor '{}' has the wrong model kind",
                            entity.identifier.as_str()
                        ),
                    });
                };
                let desired_reingestor = desired_reingestor.clone();
                /// What the outgoing reingestor left behind, taken out while the execution is
                /// borrowed so the tasks below are awaited without holding that borrow.
                struct RetiredReingestor {
                    tasks: Vec<JoinHandle<()>>,
                    entrypoints: Vec<Arc<IngestorRouteRuntime>>,
                    shutdown: watch::Sender<bool>,
                }

                let RetiredReingestor {
                    tasks: old_tasks,
                    entrypoints: old_entrypoints,
                    shutdown,
                } = {
                    let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution is unavailable for reingestor swap"
                                .to_string(),
                        }
                    })?;
                    let old_node = execution
                        .schedule
                        .nodes
                        .get(&NodeRef::new(
                            ModelKind::Reingestor,
                            entity.identifier.clone(),
                        ))
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing existing reingestor '{}'",
                                entity.identifier.as_str()
                            ),
                        })?;
                    let Model::Reingestor(old_reingestor) = old_node.config.as_ref() else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing existing reingestor '{}'",
                                entity.identifier.as_str()
                            ),
                        });
                    };
                    let old_reingestor = old_reingestor.clone();
                    let old_tasks = execution
                        .reingestor_tasks
                        .remove(entity)
                        .unwrap_or_default();
                    if !old_tasks.is_empty() {
                        for relay in old_reingestor.from.relays() {
                            if let Some(services) = execution.relay_services.get(relay) {
                                services.remove_local_runtime_consumer(old_reingestor.mode);
                            }
                        }
                    }
                    let old_entrypoints = execution
                        .branched_entrypoints
                        .remove(&entity.identifier)
                        .unwrap_or_default();
                    execution.branched_ingestors.remove(&entity.identifier);
                    RetiredReingestor {
                        tasks: old_tasks,
                        entrypoints: old_entrypoints,
                        shutdown: execution.shutdown.clone(),
                    }
                };
                for task in old_tasks {
                    tokio::task::consume_budget().await;
                    task.abort();
                    task.join_after_shutdown("scheduled node").await;
                }
                for runtime in old_entrypoints {
                    tokio::task::consume_budget().await;
                    runtime.shutdown().await;
                }

                if Self::scheduled_node_executes_locally(desired_node, local_node_id.as_ref()) {
                    let desired_entrypoint_specs = desired_specs
                        .entrypoints
                        .iter()
                        .filter(|spec| {
                            spec.kind == ModelKind::Reingestor
                                && spec.identifier == entity.identifier
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    let templates = {
                        let execution = self.inner.executions.get(domain).ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: "domain execution disappeared during reingestor swap"
                                    .to_string(),
                            }
                        })?;
                        desired_entrypoint_specs
                            .iter()
                            .map(|spec| {
                                materialize_ingestor_route_template(
                                    spec,
                                    &desired_model_index,
                                    &execution.relay_registries,
                                    &execution.relay_services,
                                )
                                .map(|template| (spec.clone(), template))
                                .map_err(|reason| {
                                    RuntimeError::BuildDomainExecution {
                                        domain: domain.as_str().to_string(),
                                        reason,
                                    }
                                })
                            })
                            .collect::<Result<Vec<_>, RuntimeError>>()?
                    };
                    let mut entrypoints = Vec::with_capacity(templates.len());
                    let mut entrypoint_senders = HashMap::default();
                    for (spec, template) in templates {
                        tokio::task::consume_budget().await;
                        let Some(runtime) = self.start_branched_entrypoint_runtime(
                            domain,
                            &entity.identifier,
                            Some((graph_handle.clone(), template)),
                        ) else {
                            continue;
                        };
                        entrypoint_senders.insert(spec.root_relay.clone(), runtime.sender());
                        entrypoints.push(runtime);
                    }

                    let receivers = {
                        let execution = self.inner.executions.get(domain).ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: "domain execution disappeared before reingestor spawn"
                                    .to_string(),
                            }
                        })?;
                        desired_reingestor
                            .from
                            .relays()
                            .iter()
                            .map(|relay| {
                                let Some(services) = execution.relay_services.get(relay) else {
                                    return Err(RuntimeError::BuildDomainExecution {
                                        domain: domain.as_str().to_string(),
                                        reason: format!(
                                            "missing reingestor input relay services '{}'",
                                            relay.as_str()
                                        ),
                                    });
                                };
                                Ok((
                                    relay.clone(),
                                    services.add_local_runtime_consumer(desired_reingestor.mode),
                                ))
                            })
                            .collect::<Result<Vec<_>, RuntimeError>>()?
                    };
                    let mut tasks = Vec::with_capacity(receivers.len());
                    for (from_relay, receiver) in receivers {
                        tokio::task::consume_budget().await;
                        tasks.push(self.spawn_reingestor_task(
                            domain,
                            &shutdown,
                            &entrypoint_senders,
                            desired_reingestor.clone(),
                            from_relay,
                            receiver,
                        )?);
                    }
                    let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution disappeared after reingestor spawn"
                                .to_string(),
                        }
                    })?;
                    execution.branched_ingestors.insert(
                        ModelName::from(&BranchName::from(&entity.identifier)),
                        desired_entrypoint_specs,
                    );
                    execution.branched_entrypoints.insert(
                        ModelName::from(&BranchName::from(&entity.identifier)),
                        entrypoints,
                    );
                    execution.reingestor_tasks.insert(entity.clone(), tasks);
                }
                continue;
            }
            if entity.kind == ModelKind::Generator {
                let desired_node = schedule
                    .nodes
                    .get(&NodeRef::new(
                        ModelKind::Generator,
                        entity.identifier.clone(),
                    ))
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing desired generator '{}'",
                            entity.identifier.as_str()
                        ),
                    })?;
                let Model::Generator(desired_generator) = desired_node.config.as_ref() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "desired generator '{}' has the wrong model kind",
                            entity.identifier.as_str()
                        ),
                    });
                };
                let desired_generator = desired_generator.clone();
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

                if Self::scheduled_node_executes_locally(desired_node, local_node_id.as_ref()) {
                    let (shutdown, spec) = {
                        let execution = self.inner.executions.get(domain).ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: "domain execution disappeared during generator swap"
                                    .to_string(),
                            }
                        })?;
                        let source_schema = execution
                            .relay_schemas
                            .get(&desired_generator.materialized_relay)
                            .cloned()
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing generator source relay schema '{}'",
                                    desired_generator.materialized_relay.as_str()
                                ),
                            })?;
                        let source_branch_schema = execution
                            .relay_branching_schemas
                            .get(&desired_generator.materialized_relay)
                            .cloned()
                            .flatten();
                        let source_branching = execution
                            .relay_branchings
                            .get(&desired_generator.materialized_relay)
                            .cloned()
                            .unwrap_or_default();
                        let mut routes =
                            Vec::with_capacity(desired_generator.output_routes.routes.len());
                        for output in desired_generator.output_routes.outputs() {
                            let output_schema = execution
                                .relay_schemas
                                .get(&output.relay)
                                .cloned()
                                .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                    domain: domain.as_str().to_string(),
                                    reason: format!(
                                        "missing generator output relay schema '{}'",
                                        output.relay.as_str()
                                    ),
                                })?;
                            let output_registry = execution
                                .relay_registries
                                .get(&output.relay)
                                .cloned()
                                .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                    domain: domain.as_str().to_string(),
                                    reason: format!(
                                        "missing generator output relay '{}'",
                                        output.relay.as_str()
                                    ),
                                })?;
                            let output_services = execution
                                .relay_services
                                .get(&output.relay)
                                .cloned()
                                .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                    domain: domain.as_str().to_string(),
                                    reason: format!(
                                        "missing generator output relay services '{}'",
                                        output.relay.as_str()
                                    ),
                                })?;
                            let program = compile_generator_set_program(
                                domain,
                                &desired_generator,
                                output,
                                GeneratorSetProgramSchemas {
                                    output: output_schema.arrow_schema(),
                                    output_sensitivity: output_schema.vm_sensitivity(),
                                    source: source_schema.arrow_schema(),
                                    branch: source_branch_schema.clone(),
                                },
                                Some(&execution.udfs),
                            )?;
                            routes.push(GeneratorTaskRouteSpec::new(
                                output.clone(),
                                program,
                                output_schema,
                                output_registry,
                                output_services,
                            ));
                        }
                        (
                            execution.shutdown.clone(),
                            GeneratorTaskSpec::new(
                                desired_generator.clone(),
                                source_schema,
                                source_branching,
                                source_branch_schema,
                                routes,
                            ),
                        )
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
            let desired_node = schedule
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
                let old_specs = branched_node_specs_from_scheduled_nodes(&execution.schedule.nodes);
                old_specs
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
            let state_purges = if let Some(execution) = self.inner.executions.get(domain)
                && let Some(old_node) = execution
                    .schedule
                    .nodes
                    .get(&NodeRef::new(entity.kind, entity.identifier.clone()))
                && let Some(desired_model) = desired_model_index.get(entity)
            {
                old_node
                    .config
                    .change_aspects_against(desired_model)
                    .state_purges()
            } else {
                Vec::new()
            };
            for purge in state_purges {
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

            let (old_task, mut template) = {
                let mut execution = self.inner.executions.get_mut(domain).ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: "domain execution is unavailable for entity swap".to_string(),
                    }
                })?;
                let template = materialize_processor_instance_template(
                    &desired_spec,
                    &desired_model_index,
                    &execution.relay_schemas,
                    &execution.relay_registries,
                    &execution.relay_services,
                    Some(&execution.udfs),
                )
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason,
                })?;
                let old_task = execution.node_tasks.remove(entity);
                (old_task, template)
            };

            template
                .prepare_wasm_processors(self, domain)
                .await
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason,
                })?;
            let had_old_task = old_task.is_some();
            let handoffs = if let Some(old_task) = old_task {
                old_task
                    .handoff()
                    .await
                    .map_err(|reason| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason,
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
            for relay in &old_spec.spec.input_relays {
                if let Some(services) = execution.relay_services.get(relay)
                    && had_old_task
                {
                    services.remove_local_runtime_consumer(old_spec.spec.mode);
                }
            }

            let executes_locally = local_node_id
                .as_ref()
                .is_some_and(|node_id| desired_node.executes_on(node_id));
            if executes_locally {
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
                    ProcessorRuntimeContext::new(
                        self.clone(),
                        domain.clone(),
                        execution.graph.clone(),
                    ),
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
        if let Some(mut execution) = self.inner.executions.get_mut(domain) {
            if let Some(local_node_id) = local_node_id.as_ref() {
                let remote_consumers =
                    Self::remote_runtime_consumers_for_schedule(&schedule, local_node_id);
                for (relay, services) in &execution.relay_services {
                    let owner_node = if let Some(node) = schedule
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
            execution.schedule = schedule;
        }
        local_gate_hold.release();
        Ok(())
    }

    pub(super) async fn apply_dynamic_schedule_update(
        &self,
        domain: &DomainName,
        schedule: DomainSchedule,
        updates: &[nervix_models::DynamicModelUpdate],
    ) -> Result<(), RuntimeError> {
        let graph = ActiveGraph::from_scheduled_models(&schedule).map_err(|error| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("failed to build dynamic schedule graph: {error}"),
            }
        })?;
        self.apply_dynamic_model_updates(domain, updates).await?;
        let graph_handle = self.domain_graph_handle(domain).await;
        graph_handle.store(Some(StdArc::new(graph)));
        if let Some(mut execution) = self.inner.executions.get_mut(domain) {
            execution.schedule = schedule;
        }
        self.force_flush_domain(domain);
        Ok(())
    }

    pub(super) async fn apply_dynamic_model_updates(
        &self,
        domain: &DomainName,
        updates: &[nervix_models::DynamicModelUpdate],
    ) -> Result<(), RuntimeError> {
        for update in updates {
            tokio::task::consume_budget().await;
            match update {
                nervix_models::DynamicModelUpdate::RelayCapacity { relay, capacity } => {
                    self.set_relay_capacity(domain, relay, *capacity);
                }
                nervix_models::DynamicModelUpdate::Processor { .. } => {}
                nervix_models::DynamicModelUpdate::Emitter { emitter, config } => {
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
                        ScheduledEmitterTask::reconfigure_via(&commands, config.clone())
                            .await
                            .map_err(|reason| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason,
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
        node: &ScheduledNode,
        local_node_id: &ClusterNodeName,
        materialized_schema: Option<StdArc<arrow_schema::Schema>>,
    ) -> Result<ScheduledNodePlacement, RuntimeError> {
        let mut placement = ScheduledNodePlacement::default();
        let executes_locally = node.executes_on(local_node_id);
        let assigned_locally = node.is_assigned_to(local_node_id);
        let execution_node = node.execution_node().cloned();

        if let Model::Relay(relay) = node.config.as_ref()
            && relay.materialized_state.is_some()
            && (executes_locally || assigned_locally)
        {
            let schema = materialized_schema.ok_or_else(|| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("missing materialized relay spec '{}'", relay.name.as_str()),
            })?;
            let replica_nodes = node
                .replica_nodes()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let mut assignment = self
                .replicated_materialized_stream_state(
                    self.state_placement(
                        domain,
                        RuntimeStateKind::MaterializedRelay,
                        ModelKind::Relay,
                        &relay.name,
                        None,
                    ),
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
                                relay.name.as_str()
                            ),
                        }
                    })?);
            } else {
                let installer = assignment.installer.take().ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "materialized relay '{}' lacks replica installation access",
                            relay.name.as_str()
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

        if let Model::Ingestor(ingestor) = node.config.as_ref()
            && let IngestSource::Kafka {
                offset_mode: KafkaOffsetMode::Domain,
                ..
            } = &ingestor.source
            && assigned_locally
        {
            let replica_nodes = node
                .replica_nodes()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let required_replica_acks = replica_nodes.len();
            let mut assignment = self
                .replicated_kafka_offset_state(
                    self.state_placement(
                        domain,
                        RuntimeStateKind::KafkaOffset,
                        node.kind(),
                        &node.identifier,
                        None,
                    ),
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
                                ingestor.name.as_str()
                            ),
                        }
                    })?);
            } else {
                let installer = assignment.installer.take().ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "Kafka ingestor '{}' lacks replica installation access",
                            ingestor.name.as_str()
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
                    self.state_placement(
                        domain,
                        RuntimeStateKind::BranchAggregated,
                        node.kind(),
                        &node.identifier,
                        None,
                    ),
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

    pub async fn apply_changes(&self, changes: RuntimeChanges) -> Result<(), RuntimeError> {
        let domain = changes.domain.clone();
        let graph = changes.graph;
        let starts_are_scheduled_by_graph = graph.is_some();
        let mut stops = Vec::new();
        let mut starts = Vec::new();
        for change in changes.changes {
            match change {
                RuntimeChange::StopIngestor { ingestor } => stops.push(ingestor),
                RuntimeChange::StartIngestor {
                    source_model,
                    ingestor,
                } => starts.push((*source_model, *ingestor)),
            }
        }

        for ingestor in stops {
            self.stop_ingestor(&domain, &ingestor).await?;
        }

        self.rebuild_domain_execution(&domain, graph).await?;

        if starts_are_scheduled_by_graph {
            return Ok(());
        }

        for (source_model, ingestor) in starts {
            let plan = IngestorStartPlan::decide_unscheduled(&domain, &ingestor, &source_model)
                .map_err(|error| RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            ingestors::IngestorStarter::start(self, plan).await?;
        }

        Ok(())
    }

    pub(super) fn scheduled_node_executes_locally(
        node: &ScheduledNode,
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
    use std::{collections::BTreeMap, sync::Arc as StdArc};

    use nervix_models::{
        AckMode, BranchSelection, ClusterNodeName, ClusterSchedule, CreateDeduplicator,
        CreateJunction, CreateRelay, CreateSchema, DeduplicatorName, DomainConfig, DomainPace,
        DomainSchedule, DomainState, DomainStatus, ModelKind, ModelName, NodeRef, ParseAsType,
        ProcessorInputs, ProcessorOutputs, RelayBranching, RelayName, ScheduledNode, SchemaField,
        SchemaName,
    };
    use nonzero_ext::nonzero;

    use super::*;

    #[tokio::test]
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
                    period: "1s".to_string(),
                    skew: "0s".to_string(),
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
                ScheduledNode::new(nervix_models::Model::Schema(CreateSchema {
                    name: schema.clone(),
                    fields: vec![SchemaField {
                        name: named("value"),
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

    #[tokio::test]
    async fn paused_schedule_keeps_full_execution_without_rebuilding_unchanged_graph() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let schema = named::<SchemaName>("notification");
        let relay = named::<RelayName>("notifications");
        let running = DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0s".to_string(),
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
        let graph_before_pause = runtime
            .inner
            .executions
            .get(&domain)
            .expect("execution should exist")
            .graph
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
        assert!(StdArc::ptr_eq(&graph_before_pause, &execution.graph));
        assert!(execution.relay_registries.contains_key(&relay));
    }

    #[tokio::test]
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
                    period: "1s".to_string(),
                    skew: "0s".to_string(),
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
                &current_schedule,
            )
            .await
            .expect("current cluster state should build");
        runtime
            .apply_cluster_state(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                1,
                &domains,
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
            Some(&execution.schedule),
            current_schedule.domains.get(&domain)
        );
        assert!(execution.relay_registries.contains_key(&relay));
    }

    #[tokio::test]
    async fn branch_preserving_processors_build_standalone_schedule_nodes() {
        let runtime = Runtime::default();
        let domain = domain("default");
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
                    output_routes: (ProcessorOutputs::single(named("projected_orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
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
                    output_routes: (ProcessorOutputs::single(named("joined_orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
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

    #[tokio::test]
    async fn scheduled_processor_entity_swap_is_not_junction_specific() {
        let runtime = Runtime::default();
        *runtime.inner.remote_dispatch.local_node_id.write() =
            Some(ClusterNodeName::parse("node-1").expect("valid name"));
        let domain = domain("default");
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

        runtime
            .swap_scheduled_nodes(&domain, desired.clone(), &[entity], &[], &[])
            .await
            .expect("non-junction scheduled processors must use the shared swap path");
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("domain execution must remain installed");
        assert_eq!(execution.schedule, desired);
        assert!(execution.node_tasks.contains_key(&NodeRef {
            kind: ModelKind::Deduplicator,
            identifier: ModelName::from(&processor),
        }));
    }

    #[tokio::test]
    async fn scheduled_entity_swap_reinstalls_state_schema_fingerprints() {
        let runtime = Runtime::default();
        *runtime.inner.remote_dispatch.local_node_id.write() =
            Some(ClusterNodeName::parse("node-1").expect("valid name"));
        let domain = domain("default");
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
        processor_node.schema_fingerprint = [7; 32];
        let nervix_models::Model::Deduplicator(config) = processor_node.config.as_mut() else {
            panic!("scheduled processor must contain a deduplicator model");
        };
        config.mode = AckMode::Detached;
        let entity = NodeRef {
            kind: ModelKind::Deduplicator,
            identifier: ModelName::from(&processor),
        };

        runtime
            .swap_scheduled_nodes(&domain, desired, &[entity], &[], &[])
            .await
            .expect("entity swap must apply");

        let installed = runtime
            .inner
            .state_schema_fingerprints
            .get(&DomainNodeRef::node_in(
                domain,
                ModelKind::Deduplicator,
                ModelName::from(&processor),
            ))
            .map(|entry| *entry.value());
        assert_eq!(
            installed,
            Some([7; 32]),
            "an entity swap must reinstall the schedule's state schema fingerprints so persisted \
             runtime state is not stranded under the pre-swap fingerprint"
        );
    }
}
