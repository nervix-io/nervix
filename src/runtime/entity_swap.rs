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
    ingestor_start::RetiringIngestorInput, partition_claims::UnitPartitionTransition,
    relay_repartition::InstalledRegistrations, *,
};

impl Runtime {
    /// Replaces the relay `entity`'s materialized state runtime on this node: its state tasks and
    /// placement follow whether `revision` materializes the relay.
    pub(super) async fn swap_relay_entity(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        revision: &Arc<ExecutionRevision>,
        registrations: &InstalledRegistrations<'_>,
        local_node_id: Option<&ClusterNodeName>,
    ) -> error_stack::Result<(), RuntimeError> {
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
                return Err(Report::new(ExecutionBuildError::MissingRelayBoundary {
                    relay,
                })
                .change_context(build()));
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
                let publication = self.materialized_relay_publication(
                    domain,
                    &relay,
                    &spec,
                    Some(&owners),
                );
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

        for task in previous_state_task {
            let unit = task.unit;
            task.stop(self.task_stop_grace())
                .await
                .change_context_lazy(|| ExecutionBuildError::StopRelayStateTask {
                    relay: relay.clone(),
                })
                .change_context_lazy(build)?;
            if registrations.counts(&relay, entity, Some(unit)) {
                services.remove_local_unit_runtime_consumer(AckMode::Detached, unit);
            }
        }
        for previous in previous_placement_tasks {
            nervix_primitives::task::consume_budget().await;
            previous.abort().await;
        }

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
            let mut state_tasks = Vec::new();
            for unit in desired_node.units_executing_on(local_node) {
                let Some(state) = placement.materialized_states.remove(&unit) else {
                    return Err(Report::new(
                        ExecutionBuildError::MissingMaterializedState { relay },
                    )
                    .change_context(build()));
                };
                let task = self.spawn_relay_state_task(
                    domain,
                    RelayStateTaskSpec {
                        relay: relay.clone(),
                        unit,
                        partitioning: services.partitioning,
                        state,
                        retention: activation_plan
                            .relays
                            .get(&relay)
                            .verified("the domain plan covers every scheduled relay")
                            .retention,
                        receiver: if desired_node.is_holding_unit(unit) {
                            services.dormant_unit_fan_in(AckMode::Detached, unit)
                        } else {
                            services
                                .add_local_unit_runtime_consumer(AckMode::Detached, unit)
                        },
                        transition: UnitPartitionTransition::of(desired_node, unit),
                        parked_capacity: services.owner_capacity(),
                    },
                );
                state_tasks.push((RelayUnitRef::new(relay.clone(), unit), task));
            }
            if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                if !placement.tasks.is_empty() {
                    execution
                        .placement_tasks
                        .extend(placement.tasks.into_iter().map(|(unit, tasks)| {
                            (NodeUnitRef::new(entity.clone(), unit), tasks)
                        }));
                }
                for (key, state_task) in state_tasks {
                    execution.relay_state_tasks.insert(key, state_task);
                }
            }
        }
        if was_materialized && !desired_materialized {
            self.purge_materialized_relay_state(domain, &relay)
                .change_context_lazy(build)?;
        }
        Ok(())
    }

    /// Stops every local unit of the ingestor `entity` and starts the units `revision` plans for
    /// this node, retiring the previous input first where the replacement requires it.
    pub(super) async fn swap_ingestor_entity(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        revision: &Arc<ExecutionRevision>,
        retiring_ingestors: &mut BTreeMap<IngestorName, RetiringIngestorInput>,
        local_node_id: Option<&ClusterNodeName>,
    ) -> error_stack::Result<(), RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let entrypoints = &revision.entrypoints;
        let Some(desired_node) = revision.nodes.get(entity) else {
            return Err(Report::new(ExecutionBuildError::MissingDesiredNode {
                node: entity.clone(),
            })
            .change_context(build()));
        };
        let ingestor = IngestorName::from(&entity.identifier);
        self.stop_ingestor(domain, &ingestor)
            .await
            .change_context_lazy(build)?;
        if let Some(retirement) = retiring_ingestors.remove(&ingestor) {
            Box::pin(self.retire_ingestor_input(retirement)).await?;
        }
        for unit in desired_node.units.keys() {
            if Self::scheduled_unit_executes_locally(desired_node, *unit, local_node_id) {
                let plan = entrypoints
                    .ingestor(&ingestor, *unit)
                    .assured("each ingestor unit has its decided plan");
                self.start_ingestor(plan).await.change_context_lazy(build)?;
            }
        }
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
            if !old_tasks.is_empty() {
                for input in &old_plan.inputs {
                    if let Some(services) = execution.relay_services.get(&input.relay)
                        && registrations.counts(&input.relay, entity, None)
                    {
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
            runtime.shutdown(self.task_stop_grace()).await;
        }

        if Self::scheduled_node_executes_locally(desired_node, local_node_id) {
            let (routing, shutdown) = {
                let Some(execution) = self.inner.executions.get(domain) else {
                    return Err(reingestor_swap_unavailable());
                };
                (execution.routing.staged(), execution.shutdown.clone())
            };
            let mut inputs = Vec::with_capacity(desired_plan.inputs.len());
            for input in &desired_plan.inputs {
                let Some(services) = routing.relay_services.get(&input.relay) else {
                    return Err(Report::new(ExecutionBuildError::MissingRelayBoundary {
                        relay: input.relay.clone(),
                    })
                    .change_context(build()));
                };
                inputs.push(PlannedReingestorInput {
                    plan: desired_plan.clone(),
                    input: input.clone(),
                    consumer: ReingestorInputConsumer::Deferred(services.clone()),
                });
            }
            let runtimes = self
                .start_reingestor_runtimes(
                    ExecutionBuildDeps::from_routing(
                        domain,
                        &routing,
                        self.inner.vm_settings,
                    ),
                    &shutdown,
                    RelayRuntimeHandles {
                        services: &routing.relay_services,
                    },
                    inputs,
                )
                .change_context(ExecutionBuildError::StartReingestors)
                .change_context_lazy(build)?;
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
        let units = self.processor_units_to_swap(
            domain,
            entity,
            &revision,
            replace_all,
            local_node_id,
        );
        let mut shared_spec: Option<GeneratorTaskSpec> = None;
        let mut units = units.into_iter().peekable();
        while let Some(unit) = units.next() {
            nervix_primitives::task::consume_budget().await;
            let owner = NodeUnitRef::new(entity.clone(), unit);
            let old_task = {
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(generator_swap_unavailable());
                };
                execution.generator_tasks.remove(&owner)
            };
            if let Some(task) = old_task {
                task.abort();
                task.join_after_shutdown("generator").await;
            }

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
            }
        }
        Ok(())
    }

    /// Replaces the task of each unit of the processor `entity` to swap: the previous task hands
    /// its branches over, and the replacement `revision` plans takes them where the unit runs on
    /// this node.
    pub(super) async fn swap_processor_entity(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        revision: &Arc<ExecutionRevision>,
        registrations: &InstalledRegistrations<'_>,
        state_purges: &BTreeMap<NodeRef, Vec<nervix_models::StatePurge>>,
        replace_all: bool,
        local_node_id: Option<&ClusterNodeName>,
    ) -> error_stack::Result<(), RuntimeError> {
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
        // applies that contract rather than re-deriving it per processor kind.
        for purge in state_purges.get(entity).into_iter().flatten() {
            match purge {
                nervix_models::StatePurge::DeduplicatorKeyspace => {
                    self.purge_deduplicator_state(
                        domain,
                        &DeduplicatorName::from(&entity.identifier),
                    )
                    .change_context_lazy(build)?;
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

        let units = self.processor_units_to_swap(
            domain,
            entity,
            &revision,
            replace_all,
            local_node_id,
        );
        for unit in units {
            nervix_primitives::task::consume_budget().await;
            let owner = NodeUnitRef::new(entity.clone(), unit);
            let executes_locally =
                Self::scheduled_unit_executes_locally(desired_node, unit, local_node_id);
            let published_plan = if executes_locally {
                Some(
                    self.bind_installed_processor_plan(domain, &desired_spec, &revision, unit)
                        .await?,
                )
            } else {
                None
            };
            let template = published_plan
                .as_ref()
                .map(|plan| plan.template.as_ref().clone());
            let old_task = {
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(processor_swap_unavailable());
                };
                execution.node_tasks.remove(&owner)
            };
            let had_old_task = old_task.is_some();
            let handoffs = if let Some(old_task) = old_task {
                old_task
                    .handoff()
                    .await
                    .change_context_lazy(|| ExecutionBuildError::ProcessorHandoff {
                        node: entity.clone(),
                    })
                    .change_context_lazy(build)?
            } else {
                Vec::new()
            };
            // A scenario holds or fails the swap here, where this node replaces the unit: its
            // previous task, if any, has handed its branches over and its replacement, if any,
            // has not started.
            #[cfg(feature = "testing")]
            if (had_old_task || executes_locally)
                && let Some(local_node_id) = local_node_id
            {
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
                        .change_context(build()));
                }
            }

            let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                return Err(processor_swap_unavailable());
            };
            if let Some(published_plan) = published_plan {
                execution
                    .routing
                    .processor_plans
                    .insert(owner.clone(), published_plan);
            } else {
                execution.routing.processor_plans.remove(&owner);
            }
            for relay in &old_spec.spec.input_relays {
                if let Some(services) = execution.relay_services.get(relay)
                    && had_old_task
                    && registrations.counts(relay, entity, Some(unit))
                {
                    services.remove_local_unit_runtime_consumer(old_spec.spec.mode, unit);
                }
            }

            if executes_locally {
                let template = template.assured(
                    "a locally executing processor binds its template before its prior task \
                     stops",
                );
                let mut inputs = Vec::with_capacity(desired_spec.spec.input_relays.len());
                for relay in &desired_spec.spec.input_relays {
                    let Some(services) = execution.relay_services.get(relay) else {
                        return Err(Report::new(ExecutionBuildError::MissingRelayBoundary {
                            relay: relay.clone(),
                        })
                        .change_context(build()));
                    };
                    // A unit holding only keys the previous count placed in it takes no input.
                    let input = if desired_node.is_holding_unit(unit) {
                        services.dormant_unit_fan_in(desired_spec.spec.mode, unit)
                    } else {
                        services.add_local_unit_runtime_consumer(desired_spec.spec.mode, unit)
                    };
                    inputs.push((relay.clone(), input));
                }
                let task = spawn_processor_node_runtime_with_handoffs(
                    ProcessorRuntimeContext::new(self.clone(), domain.clone()),
                    &execution.shutdown,
                    template,
                    inputs,
                    handoffs,
                    self.inner.branch_instance_expiration_scan_interval,
                );
                execution.node_tasks.insert(owner, task);
            }
        }
        Ok(())
    }
}
