//! Swapping the runtime of one relay, ingestor, reingestor, generator or processor while a
//! schedule's entity swap applies.
//!
//! Layer: data plane.
//! - **Owns.** Stopping the outgoing runtime of one swapped entity on this node, moving its relay
//!   consumer registrations to the incoming plan, and starting the entity's replacement where it
//!   runs.
//! - **Depends on.** The installed domain execution, the desired execution revision and the task
//!   supervisors of each entity kind.
//! - **Must not know.** Placement decisions, connector drivers or the schedule's other entities.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "an entity swap of schedule publication replaces one entity's runtime once per \
                  published revision"
    )
)]

use error_stack::ResultExt as _;

use super::{
    entity_swap_restore::{
        EntitySwapJournal, EntitySwapStepIndex, GeneratorUnitSwap, IngestorSwap, PlacementSwap,
        ProcessorUnitSwap, ReingestorSwap,
    },
    ingestor_start::RetiringIngestorInput,
    partition_claims::UnitPartitionTransition,
    relay_repartition::InstalledRegistrations,
    *,
};

/// What every per-kind replacement of one swap reads: the domain, the revision it applies, the
/// registrations installed before it and this node's identity.
#[derive(Clone, Copy)]
pub(super) struct EntitySwapContext<'s, 'r> {
    pub(super) domain: &'s DomainName,
    pub(super) revision: &'s Arc<ExecutionRevision>,
    pub(super) registrations: &'s InstalledRegistrations<'r>,
    pub(super) local_node_id: Option<&'s ClusterNodeName>,
}

/// What a swap finishes only once nothing else of it can fail: deleting state that only the
/// previous execution reads, then starting the processor units whose state that was.
#[derive(Default)]
pub(super) struct DeferredSwapWork {
    purges: Vec<SwapStatePurge>,
    processor_units: Vec<ProcessorUnitAttach>,
}

/// State a swap deletes because only the previous execution reads it.
enum SwapStatePurge {
    /// A deduplicator whose key changed: its keyspace holds the values of the previous key.
    Deduplicator(DeduplicatorName),
    /// A relay that is no longer materialized.
    MaterializedRelay(RelayName),
}

/// A processor unit whose previous task has handed its branches over, with what starting its
/// replacement takes.
pub(super) struct ProcessorUnitAttach {
    owner: NodeUnitRef,
    step: EntitySwapStepIndex<ProcessorUnitSwap>,
    had_old_task: bool,
    replacement: Option<ProcessorReplacementPlan>,
    old_spec: Arc<BranchedProcessorNodeSpec>,
}

/// The replacement a processor unit starts where it runs on this node.
struct ProcessorReplacementPlan {
    template: BranchInstanceTemplate,
    spec: Arc<BranchedProcessorNodeSpec>,
    /// Whether the unit holds only keys the previous partition count placed in it.
    holding: bool,
}

impl DeferredSwapWork {
    fn purge(&mut self, purge: SwapStatePurge) {
        self.purges.push(purge);
    }

    fn attach_later(&mut self, attach: ProcessorUnitAttach) {
        self.processor_units.push(attach);
    }
}

impl Runtime {
    /// A test holds or fails the swap here, where this node replaces `entity`: its previous
    /// runtime has stopped, handing over what it held, and its replacement has not started.
    #[cfg(feature = "testing")]
    pub(super) async fn hold_or_fail_entity_swap(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        local_node_id: Option<&ClusterNodeName>,
    ) -> error_stack::Result<(), RuntimeError> {
        let Some(local_node_id) = local_node_id else {
            return Ok(());
        };
        self.inner
            .fault_injection
            .pause_entity_swap_while_replacing_if_armed(local_node_id, domain, entity)
            .await;
        if self
            .inner
            .fault_injection
            .entity_swap_fails_while_replacing(local_node_id, domain, entity)
        {
            return Err(Report::new(ExecutionBuildError::InjectedSwapFailure)
                .change_context(RuntimeError::build_domain_execution(domain)));
        }
        Ok(())
    }

    /// Finishes the deferred work of a swap once nothing else of it can fail. Every stored state
    /// is deleted before any state held in memory is dropped, so a deletion that fails leaves each
    /// deleted entity's state in memory, where its restored task reads it.
    pub(super) fn complete_deferred_swap_work(
        &self,
        domain: &DomainName,
        registrations: &InstalledRegistrations<'_>,
        deferred: DeferredSwapWork,
        journal: &mut EntitySwapJournal,
    ) -> error_stack::Result<(), RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let DeferredSwapWork {
            purges,
            processor_units,
        } = deferred;
        for purge in &purges {
            match purge {
                SwapStatePurge::Deduplicator(deduplicator) => self
                    .purge_stored_deduplicator_state(domain, deduplicator)
                    .change_context_lazy(build)?,
                SwapStatePurge::MaterializedRelay(relay) => self
                    .purge_stored_materialized_relay_state(domain, relay)
                    .change_context_lazy(build)?,
            }
        }
        for purge in &purges {
            match purge {
                SwapStatePurge::Deduplicator(deduplicator) => {
                    self.purge_deduplicator_state(domain, deduplicator);
                }
                SwapStatePurge::MaterializedRelay(relay) => {
                    self.purge_materialized_relay_state(domain, relay);
                }
            }
        }
        for attach in processor_units {
            self.attach_processor_unit(domain, registrations, attach, journal)?;
        }
        Ok(())
    }

    /// Replaces the relay `entity`'s materialized state runtime on this node: its state tasks and
    /// placement follow whether `revision` materializes the relay.
    pub(super) async fn swap_relay_entity(
        &self,
        context: EntitySwapContext<'_, '_>,
        entity: &NodeRef,
        journal: &mut EntitySwapJournal,
        deferred: &mut DeferredSwapWork,
    ) -> error_stack::Result<(), RuntimeError> {
        let EntitySwapContext {
            domain,
            revision,
            registrations,
            local_node_id,
        } = context;
        let build = || RuntimeError::build_domain_execution(domain);
        let activation_plan = &revision.activation;
        let relay = RelayName::from(&entity.identifier);
        let Some(desired_node) = revision.nodes.get(entity) else {
            return Err(Report::new(ExecutionBuildError::MissingDesiredNode {
                node: entity.clone(),
            })
            .change_context(build()));
        };
        let desired_materialized = desired_node.materialized_relay;
        let step = journal.record(PlacementSwap::new(entity.clone(), None));
        let (
            was_materialized,
            shutdown,
            schema,
            services,
            previous_state_task,
            previous_placement_tasks,
        ) = {
            let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                return Err(Report::new(ExecutionBuildError::ExecutionUnavailable {
                    step: ExecutionStep::RelayTransition,
                })
                .change_context(build()));
            };
            let was_materialized = execution.materialized_stream_specs.contains_key(&relay);
            let Some(schema) = execution.relay_schemas.get(&relay).cloned() else {
                return Err(
                    Report::new(ExecutionBuildError::MissingRelaySchema { relay })
                        .change_context(build()),
                );
            };
            let Some(services) = execution.relay_services.get(&relay).cloned() else {
                return Err(
                    Report::new(ExecutionBuildError::MissingRelayBoundary { relay })
                        .change_context(build()),
                );
            };
            if desired_materialized {
                let spec = RuntimeMaterializedRelaySpec::new(
                    schema.arrow_schema(),
                    schema.vm_sensitivity(),
                    desired_node
                        .resolved_branching
                        .clone()
                        .assured("the schedule resolves every relay branch declaration"),
                );
                let owners = RelayOwnerRoutes::new(desired_node.unit_execution_nodes());
                let publication =
                    self.materialized_relay_publication(domain, &relay, &spec, Some(&owners));
                execution
                    .materialized_stream_specs
                    .insert(relay.clone(), spec);
                execution
                    .materialized_stream_reads
                    .insert(relay.clone(), publication);
                execution
                    .materialized_stream_owner_nodes
                    .insert(RelayName::from(&entity.identifier), owners);
            } else {
                execution
                    .materialized_stream_specs
                    .remove(&RelayName::from(&entity.identifier));
                execution
                    .materialized_stream_owner_nodes
                    .remove(&RelayName::from(&entity.identifier));
                execution.materialized_stream_reads.remove(&relay);
            }
            let previous_state_tasks = execution.take_relay_state_tasks(&relay);
            (
                was_materialized,
                execution.shutdown.clone(),
                schema,
                services,
                previous_state_tasks,
                {
                    let keys = execution
                        .placement_tasks
                        .keys()
                        .filter(|owner| owner.node == *entity)
                        .cloned()
                        .collect::<Vec<_>>();
                    keys.into_iter()
                        .filter_map(|owner| execution.placement_tasks.remove(&owner))
                        .collect::<Vec<_>>()
                },
            )
        };

        // Every previous task ends, even after one fails to stop: a task that does not stop
        // within its grace is aborted, and a restore starts each of them again.
        let mut stop_failure = None;
        for task in previous_state_task {
            let unit = task.unit;
            let stopped = task.stop(self.planned_task_stop()).await;
            let withdrawn = registrations.counts(&relay, entity, Some(unit));
            if withdrawn {
                services.remove_local_unit_runtime_consumer(AckMode::Detached, unit);
            }
            journal
                .step(step)
                .record_state_task_stopped(unit, withdrawn);
            if let Err(error) = stopped
                && stop_failure.is_none()
            {
                stop_failure = Some(error);
            }
        }
        if !previous_placement_tasks.is_empty() {
            journal.step(step).record_aborted();
        }
        for previous in previous_placement_tasks {
            nervix_primitives::task::consume_budget().await;
            previous.abort().await;
        }
        if let Some(error) = stop_failure {
            return Err(error
                .change_context(ExecutionBuildError::StopRelayStateTask {
                    relay: relay.clone(),
                })
                .change_context(build()));
        }
        #[cfg(feature = "testing")]
        self.hold_or_fail_entity_swap(domain, entity, local_node_id)
            .await?;

        if desired_materialized {
            let Some(local_node) = local_node_id else {
                return Err(Report::new(ExecutionBuildError::LocalNodeUnknown { relay })
                    .change_context(build()));
            };
            let mut placement = self
                .build_scheduled_node_placement(
                    domain,
                    &shutdown,
                    desired_node,
                    local_node,
                    Some(PlacedNodeState::MaterializedRelay(schema.arrow_schema())),
                    None,
                )
                .change_context_lazy(build)?;
            // Every local unit must have its state before any task starts, so a placement that
            // lacks one leaves no task of it running.
            let mut states = Vec::new();
            let mut complete = true;
            for unit in desired_node.units_executing_on(local_node) {
                match placement.materialized_states.remove(&unit) {
                    Some(state) => states.push((unit, state)),
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
            if !complete {
                for (_, tasks) in placement.tasks {
                    nervix_primitives::task::consume_budget().await;
                    tasks.abort().await;
                }
                return Err(
                    Report::new(ExecutionBuildError::MissingMaterializedState { relay })
                        .change_context(build()),
                );
            }
            let retention = activation_plan
                .relays
                .get(&relay)
                .verified("the domain plan covers every scheduled relay")
                .retention;
            let mut state_tasks = Vec::with_capacity(states.len());
            for (unit, state) in states {
                let (receiver, registered) = if desired_node.is_holding_unit(unit) {
                    (services.dormant_unit_fan_in(AckMode::Detached, unit), None)
                } else {
                    (
                        services.add_local_unit_runtime_consumer(AckMode::Detached, unit),
                        Some(services.clone()),
                    )
                };
                let task = self.spawn_relay_state_task(
                    domain,
                    RelayStateTaskSpec {
                        relay: relay.clone(),
                        unit,
                        partitioning: services.partitioning,
                        state,
                        retention,
                        receiver,
                        transition: UnitPartitionTransition::of(desired_node, unit),
                        parked_capacity: services.owner_capacity(),
                    },
                );
                state_tasks.push((unit, task, registered));
            }
            if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                for (unit, tasks) in placement.tasks {
                    execution
                        .placement_tasks
                        .insert(NodeUnitRef::new(entity.clone(), unit), tasks);
                    journal.step(step).record_installed(unit);
                }
                for (unit, task, registered) in state_tasks {
                    execution
                        .relay_state_tasks
                        .insert(RelayUnitRef::new(relay.clone(), unit), task);
                    journal
                        .step(step)
                        .record_state_task_started(unit, registered);
                }
            }
        }
        if was_materialized && !desired_materialized {
            deferred.purge(SwapStatePurge::MaterializedRelay(relay));
        }
        Ok(())
    }

    /// Stops every local unit of the ingestor `entity` and prepares a replacement that requires
    /// the previous input's retirement, without touching what the previous input keeps at its
    /// broker: the swap retires that only once it can no longer be undone, so a failed swap
    /// starts the previous ingestor again on it. The swap starts no unit: an ingestor is a source,
    /// and the revision's activation starts the units `revision` plans for this node once every
    /// node has prepared it, so no unit runs on two nodes at once and a unit that moved here
    /// starts only once this node holds what it reads from, such as its Kafka DOMAIN offsets.
    pub(super) async fn swap_ingestor_entity(
        &self,
        context: EntitySwapContext<'_, '_>,
        entity: &NodeRef,
        retiring_ingestors: &mut BTreeMap<IngestorName, RetiringIngestorInput>,
        journal: &mut EntitySwapJournal,
    ) -> error_stack::Result<(), RuntimeError> {
        let domain = context.domain;
        let build = || RuntimeError::build_domain_execution(domain);
        if !context.revision.nodes.contains_key(entity) {
            return Err(Report::new(ExecutionBuildError::MissingDesiredNode {
                node: entity.clone(),
            })
            .change_context(build()));
        }
        let ingestor = IngestorName::from(&entity.identifier);
        let step = journal.record(IngestorSwap::new(ingestor.clone()));
        for unit in self.running_ingestor_units(domain, &ingestor) {
            nervix_primitives::task::consume_budget().await;
            self.stop_ingestor_unit(domain, &ingestor, unit, self.planned_task_stop())
                .await
                .change_context_lazy(build)?;
            journal.step(step).record_stopped(unit);
        }
        if let Some(retirement) = retiring_ingestors.get(&ingestor) {
            Box::pin(self.prepare_ingestor_replacement(retirement)).await?;
        }
        #[cfg(feature = "testing")]
        self.hold_or_fail_entity_swap(domain, entity, context.local_node_id)
            .await?;
        Ok(())
    }

    /// Replaces the tasks and branched entrypoints of the reingestor `entity` with those
    /// `revision` plans, where this node runs it.
    pub(super) async fn swap_reingestor_entity(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        revision: &Arc<ExecutionRevision>,
        registrations: &InstalledRegistrations<'_>,
        local_node_id: Option<&ClusterNodeName>,
        journal: &mut EntitySwapJournal,
    ) -> error_stack::Result<(), RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let entrypoints = &revision.entrypoints;
        let reingestor = ReingestorName::from(&entity.identifier);
        let Some(desired_plan) = entrypoints.reingestor(&reingestor).cloned() else {
            return Err(Report::new(ExecutionBuildError::MissingDesiredNode {
                node: entity.clone(),
            })
            .change_context(build()));
        };
        let reingestor_swap_unavailable = || {
            Report::new(ExecutionBuildError::ExecutionUnavailable {
                step: ExecutionStep::ReingestorSwap,
            })
            .change_context(build())
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

        let step = journal.record(ReingestorSwap::new(entity.clone()));
        let RetiredReingestor {
            tasks: old_tasks,
            entrypoints: old_entrypoints,
        } = {
            let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                return Err(reingestor_swap_unavailable());
            };
            let Some(old_plan) = execution
                .revision
                .entrypoints
                .reingestor(&reingestor)
                .cloned()
            else {
                return Err(Report::new(ExecutionBuildError::MissingInstalledNode {
                    node: entity.clone(),
                })
                .change_context(build()));
            };
            let old_tasks = execution
                .reingestor_tasks
                .remove(entity)
                .unwrap_or_default();
            let old_entrypoints = execution
                .branched_entrypoints
                .remove(&entity.identifier)
                .unwrap_or_default();
            if !old_tasks.is_empty() || !old_entrypoints.is_empty() {
                journal.step(step).record_previous_stopped();
            }
            if !old_tasks.is_empty() {
                for input in &old_plan.inputs {
                    if let Some(services) = execution.relay_services.get(&input.relay)
                        && registrations.counts(&input.relay, entity, None)
                    {
                        services.remove_local_runtime_consumer(old_plan.mode);
                        journal.step(step).record_withdrawn(input.relay.clone());
                    }
                }
            }
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
            runtime.shutdown(self.planned_task_stop()).await;
        }
        #[cfg(feature = "testing")]
        self.hold_or_fail_entity_swap(domain, entity, local_node_id)
            .await?;

        if Self::scheduled_node_executes_locally(desired_node, local_node_id) {
            let (routing, shutdown) = {
                let Some(execution) = self.inner.executions.get(domain) else {
                    return Err(reingestor_swap_unavailable());
                };
                (execution.routing.staged(), execution.shutdown.clone())
            };
            let mut inputs = Vec::with_capacity(desired_plan.inputs.len());
            let mut registered = Vec::with_capacity(desired_plan.inputs.len());
            for input in &desired_plan.inputs {
                let Some(services) = routing.relay_services.get(&input.relay) else {
                    return Err(Report::new(ExecutionBuildError::MissingRelayBoundary {
                        relay: input.relay.clone(),
                    })
                    .change_context(build()));
                };
                registered.push(services.clone());
                inputs.push(PlannedReingestorInput {
                    plan: desired_plan.clone(),
                    input: input.clone(),
                    consumer: ReingestorInputConsumer::Deferred(services.clone()),
                });
            }
            let runtimes = self
                .start_reingestor_runtimes(
                    ExecutionBuildDeps::from_routing(domain, &routing, self.inner.vm_settings),
                    &shutdown,
                    RelayRuntimeHandles {
                        services: &routing.relay_services,
                    },
                    inputs,
                )
                .change_context(ExecutionBuildError::StartReingestors)
                .change_context_lazy(build)?;
            journal
                .step(step)
                .record_replacement(desired_plan.mode, registered);
            let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                return Err(reingestor_swap_unavailable());
            };
            execution
                .branched_entrypoints
                .extend(runtimes.branched_entrypoints);
            execution.reingestor_tasks.extend(runtimes.tasks);
        }
        Ok(())
    }

    /// Replaces the task of each unit of the generator `entity` to swap with one of the generator
    /// `revision` plans, where the unit runs on this node.
    pub(super) async fn swap_generator_entity(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        revision: &Arc<ExecutionRevision>,
        replace_all: bool,
        local_node_id: Option<&ClusterNodeName>,
        journal: &mut EntitySwapJournal,
    ) -> error_stack::Result<(), RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let resource_plans = &revision.resources;
        let name = GeneratorName::from(&entity.identifier);
        let Some(generator) = resource_plans.generators.get(&name) else {
            return Err(Report::new(ExecutionBuildError::MissingDesiredNode {
                node: entity.clone(),
            })
            .change_context(build()));
        };
        let generator_swap_unavailable = || {
            Report::new(ExecutionBuildError::ExecutionUnavailable {
                step: ExecutionStep::GeneratorSwap,
            })
            .change_context(build())
        };
        let units =
            self.processor_units_to_swap(domain, entity, revision, replace_all, local_node_id);
        let mut shared_spec: Option<GeneratorTaskSpec> = None;
        let mut units = units.into_iter().peekable();
        while let Some(unit) = units.next() {
            nervix_primitives::task::consume_budget().await;
            let owner = NodeUnitRef::new(entity.clone(), unit);
            let step = journal.record(GeneratorUnitSwap::new(owner.clone()));
            let old_task = {
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(generator_swap_unavailable());
                };
                execution.generator_tasks.remove(&owner)
            };
            if let Some(task) = old_task {
                journal.step(step).record_previous_stopped();
                task.abort();
                task.join_after_shutdown("generator").await;
            }
            #[cfg(feature = "testing")]
            self.hold_or_fail_entity_swap(domain, entity, local_node_id)
                .await?;

            if generator.assignment.unit_executes_on(unit, local_node_id) {
                let (shutdown, spec) = {
                    let Some(execution) = self.inner.executions.get(domain) else {
                        return Err(generator_swap_unavailable());
                    };
                    if shared_spec.is_none() {
                        shared_spec = Some(
                            GeneratorTaskSpec::bind(
                                domain,
                                generator,
                                unit,
                                &execution.relay_services,
                                &execution.udfs,
                                self.inner.vm_settings,
                            )
                            .change_context_lazy(build)?,
                        );
                    }
                    // Programs are compiled once for this logical generator. Move the
                    // final specification, keeping the single-unit path free of clones.
                    let mut spec = if units.peek().is_some() {
                        shared_spec
                            .as_ref()
                            .verified("the specification was bound above")
                            .clone()
                    } else {
                        shared_spec
                            .take()
                            .verified("the specification was bound above")
                    };
                    spec.unit = unit;
                    (execution.shutdown.clone(), spec)
                };
                let task = self
                    .spawn_generator_task(domain, &shutdown, spec)
                    .change_context_lazy(build)?;
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(generator_swap_unavailable());
                };
                execution.generator_tasks.insert(owner, task);
                journal.step(step).record_replacement_started();
            }
        }
        Ok(())
    }

    /// Replaces the task of each unit of the processor `entity` to swap: the previous task hands
    /// its branches over, and the replacement `revision` plans takes them where the unit runs on
    /// this node.
    pub(super) async fn swap_processor_entity(
        &self,
        context: EntitySwapContext<'_, '_>,
        entity: &NodeRef,
        state_purges: &BTreeMap<NodeRef, Vec<nervix_models::StatePurge>>,
        replace_all: bool,
        journal: &mut EntitySwapJournal,
        deferred: &mut DeferredSwapWork,
    ) -> error_stack::Result<(), RuntimeError> {
        let EntitySwapContext {
            domain,
            revision,
            registrations,
            local_node_id,
        } = context;
        let build = || RuntimeError::build_domain_execution(domain);
        let desired_specs = &revision.processors;
        let missing_desired = || {
            Report::new(ExecutionBuildError::MissingDesiredNode {
                node: entity.clone(),
            })
            .change_context(build())
        };
        let processor_swap_unavailable = || {
            Report::new(ExecutionBuildError::ExecutionUnavailable {
                step: ExecutionStep::ProcessorSwap,
            })
            .change_context(build())
        };
        let Some(desired_node) = revision.nodes.get(entity) else {
            return Err(missing_desired());
        };
        let Some(desired_spec) = desired_specs
            .processor(entity.kind, &entity.identifier)
            .cloned()
        else {
            return Err(missing_desired());
        };
        let old_spec = {
            let Some(execution) = self.inner.executions.get(domain) else {
                return Err(processor_swap_unavailable());
            };
            execution
                .revision
                .processors
                .processor(entity.kind, &entity.identifier)
                .cloned()
        };
        let Some(old_spec) = old_spec else {
            return Err(Report::new(ExecutionBuildError::MissingInstalledNode {
                node: entity.clone(),
            })
            .change_context(build()));
        };
        // The change aspects own which node-local state a swap invalidates, so the runtime
        // applies that contract rather than re-deriving it per processor kind. A purge deletes
        // state the previous task reads, so it waits until nothing else of the swap can fail, and
        // the units whose state it deletes start their replacements only after it.
        let mut purges_state = false;
        for purge in state_purges.get(entity).into_iter().flatten() {
            match purge {
                nervix_models::StatePurge::DeduplicatorKeyspace => {
                    purges_state = true;
                    deferred.purge(SwapStatePurge::Deduplicator(DeduplicatorName::from(
                        &entity.identifier,
                    )));
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

        let units =
            self.processor_units_to_swap(domain, entity, revision, replace_all, local_node_id);
        for unit in units {
            nervix_primitives::task::consume_budget().await;
            let owner = NodeUnitRef::new(entity.clone(), unit);
            let executes_locally =
                Self::scheduled_unit_executes_locally(desired_node, unit, local_node_id);
            let published_plan = if executes_locally {
                Some(
                    self.bind_installed_processor_plan(domain, &desired_spec, revision, unit)
                        .await?,
                )
            } else {
                None
            };
            // Every input of the replacement must have its relay boundary before the previous
            // task stops, so starting the replacement cannot fail once it has.
            if executes_locally {
                let mut missing_boundary = None;
                {
                    let Some(execution) = self.inner.executions.get(domain) else {
                        return Err(processor_swap_unavailable());
                    };
                    for relay in &desired_spec.spec.input_relays {
                        if !execution.relay_services.contains_key(relay) {
                            missing_boundary = Some(relay.clone());
                            break;
                        }
                    }
                }
                if let Some(relay) = missing_boundary {
                    return Err(
                        Report::new(ExecutionBuildError::MissingRelayBoundary { relay })
                            .change_context(build()),
                    );
                }
            }
            let step = journal.record(ProcessorUnitSwap::new(owner.clone()));
            let (old_task, previous_plan) = {
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(processor_swap_unavailable());
                };
                let old_task = execution.node_tasks.remove(&owner);
                let previous_plan = execution.routing.processor_plans.get(&owner).cloned();
                (old_task, previous_plan)
            };
            let had_old_task = old_task.is_some();
            if let Some(old_task) = old_task {
                journal.step(step).record_previous(previous_plan);
                let handoffs = old_task
                    .handoff()
                    .await
                    .change_context_lazy(|| ExecutionBuildError::ProcessorHandoff {
                        node: entity.clone(),
                    })
                    .change_context_lazy(build)?;
                journal.step(step).record_handoffs(handoffs);
            }
            #[cfg(feature = "testing")]
            if had_old_task || executes_locally {
                self.hold_or_fail_entity_swap(domain, entity, local_node_id)
                    .await?;
            }

            {
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(processor_swap_unavailable());
                };
                // Staged only: a failed swap discards it with the rest of the routing it staged.
                match published_plan.as_ref() {
                    Some(published_plan) => {
                        execution
                            .routing
                            .processor_plans
                            .insert(owner.clone(), published_plan.clone());
                    }
                    None => {
                        execution.routing.processor_plans.remove(&owner);
                    }
                }
            }
            let replacement = published_plan.map(|published_plan| ProcessorReplacementPlan {
                template: published_plan.template.as_ref().clone(),
                spec: desired_spec.clone(),
                // A unit holding only keys the previous count placed in it takes no input.
                holding: desired_node.is_holding_unit(unit),
            });
            let attach = ProcessorUnitAttach {
                owner,
                step,
                had_old_task,
                replacement,
                old_spec: old_spec.clone(),
            };
            if purges_state {
                deferred.attach_later(attach);
            } else {
                self.attach_processor_unit(domain, registrations, attach, journal)?;
            }
        }
        Ok(())
    }

    /// Starts the replacement of a processor unit whose previous task handed its branches over:
    /// the unit's input registrations move to the replacement, which takes those branches.
    pub(super) fn attach_processor_unit(
        &self,
        domain: &DomainName,
        registrations: &InstalledRegistrations<'_>,
        attach: ProcessorUnitAttach,
        journal: &mut EntitySwapJournal,
    ) -> error_stack::Result<(), RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let ProcessorUnitAttach {
            owner,
            step,
            had_old_task,
            replacement,
            old_spec,
        } = attach;
        let Some(mut execution) = self.inner.executions.get_mut(domain) else {
            return Err(Report::new(ExecutionBuildError::ExecutionUnavailable {
                step: ExecutionStep::ProcessorSwap,
            })
            .change_context(build()));
        };
        let replacement = match replacement {
            Some(plan) => {
                let mut boundaries = Vec::with_capacity(plan.spec.spec.input_relays.len());
                for relay in &plan.spec.spec.input_relays {
                    let Some(services) = execution.relay_services.get(relay) else {
                        return Err(Report::new(ExecutionBuildError::MissingRelayBoundary {
                            relay: relay.clone(),
                        })
                        .change_context(build()));
                    };
                    boundaries.push((relay.clone(), services.clone()));
                }
                Some((plan, boundaries))
            }
            None => None,
        };
        let unit_swap = journal.step(step);
        if had_old_task {
            for relay in &old_spec.spec.input_relays {
                if let Some(services) = execution.relay_services.get(relay)
                    && registrations.counts(relay, &owner.node, Some(owner.unit))
                {
                    services.remove_local_unit_runtime_consumer(old_spec.spec.mode, owner.unit);
                    unit_swap.record_withdrawn(relay.clone());
                }
            }
        }
        let Some((plan, boundaries)) = replacement else {
            return Ok(());
        };
        let mode = plan.spec.spec.mode;
        let mut inputs = Vec::with_capacity(boundaries.len());
        let mut registered = Vec::with_capacity(boundaries.len());
        for (relay, services) in boundaries {
            let input = if plan.holding {
                services.dormant_unit_fan_in(mode, owner.unit)
            } else {
                let input = services.add_local_unit_runtime_consumer(mode, owner.unit);
                registered.push(services);
                input
            };
            inputs.push((relay, input));
        }
        let task = spawn_processor_node_runtime_with_handoffs(
            ProcessorRuntimeContext::new(self.clone(), domain.clone()),
            &execution.shutdown,
            plan.template,
            inputs,
            unit_swap.take_handoffs(),
            self.inner.branch_instance_expiration_scan_interval,
        );
        execution.node_tasks.insert(owner, task);
        unit_swap.record_replacement(mode, registered);
        Ok(())
    }
}
