//! Rebuilding running and passive domain executions from typed activation decisions.
//!
//! Layer: data plane.
//! - **Owns.** Binding a domain's relay surfaces, programs and tasks during rebuild and recovery.
//! - **Depends on.** Typed registry plans, runtime capabilities and installed resources.
//! - **Must not know.** NSPL parsing, transaction decisions or connector internals.

use nervix_connector_websockets::CompiledSignalingProtocol;

use super::*;

pub(super) struct ActivatedDomainSurfaces {
    pub(super) codecs: HashMap<CodecName, Arc<CompiledCodec>>,
    pub(super) signaling_protocols: HashMap<SignalingProtocolName, Arc<CompiledSignalingProtocol>>,
    pub(super) endpoint_routes: HashMap<EndpointName, EndpointRoute>,
}

/// Every relay that keeps branch instances: each relay a planned ingestor or reingestor route
/// writes into a branch, and every relay a branched processor reads or writes.
pub(super) fn branch_relays_from_plans(
    specs: &BranchedNodeSpecs,
    entrypoints: &EntrypointPlans,
) -> HashSet<RelayName> {
    let mut relays = HashSet::default();
    for route in entrypoints.routes() {
        if route.branch.retention().is_some() {
            relays.insert(route.relay.clone());
        }
    }
    for node_spec in &specs.processors {
        if node_spec.branch_ttl.is_some() {
            relays.extend(node_spec.spec.relay_ids());
        }
    }
    relays
}

impl Runtime {
    /// Bind pinned resources and compile the codec and endpoint surfaces selected by one pure
    /// domain decision. Both running and passive builds use this exact installation path.
    pub(super) async fn activate_domain_surfaces(
        &self,
        domain: &DomainName,
        plan: &DomainActivationPlan,
    ) -> Result<ActivatedDomainSurfaces, RuntimeError> {
        let mut signaling_protocols = HashMap::new();
        for protocol in plan.signaling_protocols.values() {
            let compiled = Box::pin(self.compile_signaling_protocol(domain, protocol)).await?;
            signaling_protocols.insert(protocol.name.clone(), compiled);
        }

        let mut codecs = HashMap::new();
        for codec in plan.codecs.values() {
            let compiled = Box::pin(self.compile_domain_codec(domain, codec)).await?;
            codecs.insert(codec.name.clone(), compiled);
        }

        let mut endpoint_routes = HashMap::new();
        for endpoint in plan.endpoints.values() {
            let signaling_protocol = endpoint.signaling_protocol.as_ref().map(|name| {
                signaling_protocols
                    .get(name)
                    .cloned()
                    .assured("the domain plan resolved each endpoint signaling protocol")
            });
            endpoint_routes.insert(
                endpoint.name.clone(),
                EndpointRoute {
                    path: endpoint.path.clone(),
                    hostnames: endpoint.hostnames.clone(),
                    endpoint_type: endpoint.endpoint_type,
                    signaling_protocol,
                },
            );
        }

        Ok(ActivatedDomainSurfaces {
            codecs,
            signaling_protocols,
            endpoint_routes,
        })
    }

    /// The fan-out of `relay`, kept across rebuilds so buffered batches and attached consumers
    /// survive them. Its session subscribers are closed when the relay's rows change definition.
    pub(in crate::runtime) async fn relay_boundary_fanout_with_capacity(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        capacity: NonZeroUsize,
        definition: RelaySubscriptionDefinition,
    ) -> RelayBoundaryFanout {
        let use_branch_collapse = definition.is_branched();
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone());
        let existing = self
            .inner
            .relay_boundary_fanouts
            .get(&key)
            .map(|fanout| fanout.clone());
        if let Some(fanout) = existing {
            if fanout.uses_branch_collapse() == use_branch_collapse {
                fanout.set_capacity(capacity);
                fanout.subscriptions().declare(definition);
                return fanout;
            }
            // Branching changed, which is a change of definition too: the replaced fan-out's
            // subscribers end before the replacement carries a batch.
            fanout.subscriptions().withdraw();
        }

        let fanout = if use_branch_collapse {
            RelayBoundaryFanout::branch_collapse_with_capacity(capacity)
        } else {
            RelayBoundaryFanout::direct_with_capacity(capacity)
        };
        fanout.subscriptions().declare(definition);
        self.inner
            .relay_boundary_fanouts
            .insert(key, fanout.clone());
        fanout
    }

    /// Closes the session subscribers of every relay of `domain` this node no longer declares.
    /// A relay that is declared again later is a new relay to them, so they are not kept waiting
    /// for it.
    pub(in crate::runtime) fn withdraw_undeclared_relay_subscriptions<F>(
        &self,
        domain: &DomainName,
        declared: F,
    ) where
        F: Fn(&RelayName) -> bool,
    {
        for fanout in self.inner.relay_boundary_fanouts.iter() {
            let key = fanout.key();
            if &key.domain != domain {
                continue;
            }
            let relay = RelayName::from(key.identifier());
            if declared(&relay) {
                continue;
            }
            fanout.value().subscriptions().withdraw();
        }
    }

    pub(in crate::runtime) async fn domain_graph_handle(
        &self,
        domain: &DomainName,
    ) -> SharedActiveGraph {
        self.inner
            .domain_graphs
            .entry(domain.clone())
            .or_insert_with(|| StdArc::new(ArcSwapOption::from(None)))
            .clone()
    }

    pub(in crate::runtime) async fn clear_domain_graph_handle(&self, domain: &DomainName) {
        let handle = self
            .inner
            .domain_graphs
            .get(domain)
            .map(|entry| entry.clone());
        if let Some(handle) = handle {
            handle.store(None);
        }
    }

    /// Starts the branched entrypoint runtime every route of one ingestor or reingestor feeds, and
    /// returns them with their senders keyed by the relay each route writes.
    pub(in crate::runtime) fn start_branched_entrypoint_runtimes(
        &self,
        domain: &DomainName,
        identifier: &ModelName,
        templates: HashMap<RelayName, IngestorRouteTemplate>,
    ) -> IngestorRouteRuntimes {
        let mut roots = templates.into_iter().collect::<Vec<_>>();
        roots.sort_by(|left, right| left.0.cmp(&right.0));
        let mut runtimes = Vec::with_capacity(roots.len());
        let mut senders = HashMap::with_capacity(roots.len());
        for (root_relay, template) in roots {
            let runtime = IngestorRouteRuntime::new(
                self.clone(),
                domain.clone(),
                IngestorName::from(identifier),
                template,
                self.inner.branch_instance_expiration_scan_interval,
            );
            senders.insert(root_relay, runtime.sender());
            runtimes.push(runtime);
        }
        IngestorRouteRuntimes { runtimes, senders }
    }

    pub(super) fn set_relay_capacity(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        capacity: NonZeroUsize,
    ) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone());
        if let Some(fanout) = self.inner.relay_boundary_fanouts.get(&key) {
            fanout.set_capacity(capacity);
        }
        if let Some(execution) = self.inner.executions.get(domain)
            && let Some(services) = execution.relay_services.get(relay)
        {
            services.fanout.set_capacity(capacity);
        }
    }

    /// Installs a built execution and publishes its endpoint routes in one step, so the routing
    /// index can never drift from the executions it describes.
    pub(super) fn install_domain_execution(
        &self,
        domain: &DomainName,
        mut execution: DomainExecution,
    ) {
        self.withdraw_undeclared_relay_subscriptions(domain, |relay| {
            execution.relay_services.contains_key(relay)
        });
        self.publish_routed_endpoints(domain, &execution);
        execution.routing.publish();
        self.inner
            .domain_routings
            .insert(domain.clone(), execution.routing.shared());
        self.inner.executions.insert(domain.clone(), execution);
    }

    /// Publishes an execution's endpoint routes into the routing index. Called with the execution
    /// that is about to become live so inbound requests resolve it by host and path.
    pub(super) fn publish_routed_endpoints(
        &self,
        domain: &DomainName,
        execution: &DomainExecution,
    ) {
        if execution.passive_only {
            return;
        }
        for (key, endpoint) in execution.routed_endpoints() {
            self.inner
                .routed_endpoints
                .entry(key)
                .or_default()
                .insert(domain.clone(), endpoint);
        }
    }

    /// Withdraws an execution's endpoint routes from the routing index. Called with the execution
    /// that has just been removed, so only the routes that domain published are dropped.
    pub(super) fn withdraw_routed_endpoints(
        &self,
        domain: &DomainName,
        execution: &DomainExecution,
    ) {
        for (key, _) in execution.routed_endpoints() {
            let Some(mut domains) = self.inner.routed_endpoints.get_mut(&key) else {
                continue;
            };
            domains.remove(domain);
            let emptied = domains.is_empty();
            drop(domains);
            if emptied {
                self.inner
                    .routed_endpoints
                    .remove_if(&key, |_, domains| domains.is_empty());
            }
        }
    }

    pub(in crate::runtime) async fn rebuild_domain_from_schedule(
        &self,
        local_node_id: &ClusterNodeName,
        domain: &DomainName,
        schedule: Option<DomainSchedule>,
        start_ingestors: bool,
    ) -> Result<(), RuntimeError> {
        // Domain teardown, compilation, restoration, and startup are separate rebuild phases.
        // Their futures stay indirect to bound this coordinator's debug poll frame.
        Box::pin(self.stop_domain_ingestors(domain)).await;

        let desired_start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        if let Some((_, mut existing)) = self.inner.executions.remove(domain) {
            existing.routing.deactivate();
            Box::pin(self.stop_domain_execution(domain, existing)).await;
        }

        let Some(schedule) = schedule else {
            self.withdraw_undeclared_relay_subscriptions(domain, |_| false);
            self.clear_domain_ingestor_quiescence(domain);
            self.inner.compiled_domain_udfs.remove(domain);
            self.clear_state_identities(domain);
            Box::pin(self.clear_domain_graph_handle(domain)).await;
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        };
        self.install_state_identities(&schedule);
        let stopped = self
            .inner
            .domains
            .get(domain)
            .is_some_and(|state| matches!(state.status, nervix_models::DomainStatus::Stopped));
        let schedule_fingerprint = Self::ownership_handoff_schedule_fingerprint(&schedule)
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: reason.to_string(),
            })?;
        for node in schedule.nodes.values() {
            self.activate_prepared_forced_ownership_recovery_state(
                domain,
                node,
                local_node_id,
                schedule_fingerprint,
                !stopped,
            )
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "failed to activate forced recovery state for {} '{}': {error}",
                    node.kind().as_str(),
                    node.identifier.as_str()
                ),
            })?;
            self.activate_prepared_ownership_handoff_state(
                domain,
                node,
                local_node_id,
                schedule_fingerprint,
                !stopped,
            )
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "failed to activate prepared state for {} '{}': {error}",
                    node.kind().as_str(),
                    node.identifier.as_str()
                ),
            })?;
        }
        if stopped {
            self.clear_domain_ingestor_quiescence(domain);
            self.clear_expiring_stream_states_for_domain(domain);
            let execution =
                Box::pin(self.build_passive_execution_from_schedule(domain, &schedule)).await?;
            self.install_domain_execution(domain, execution);
            Box::pin(self.clear_domain_graph_handle(domain)).await;
            return Ok(());
        }
        let domain_clock =
            self.bind_domain_clock(domain)
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
        let domain_graph = Box::pin(self.domain_graph_handle(domain)).await;
        domain_graph.store(None);
        let (shutdown_tx, _) = watch::channel(false);
        let mut relay_builders = HashMap::new();
        let mut relay_branchings = HashMap::new();
        let mut relay_schemas = HashMap::new();
        let mut materialized_stream_specs = HashMap::new();
        let mut materialized_stream_owner_nodes = HashMap::new();
        let mut lookup_specs = Vec::new();
        let mut relay_state_specs = Vec::new();
        let mut emitter_specs = Vec::new();
        let mut reingestor_inputs = Vec::new();
        let mut local_ingestors = Vec::new();
        let mut node_tasks = HashMap::new();
        let mut emitter_tasks = HashMap::new();
        let mut generator_tasks = HashMap::new();
        let remote_dispatcher = self.inner.remote_dispatcher.load_full();
        let model_index = schedule
            .nodes
            .values()
            .map(|node| (*node.config).clone())
            .collect::<ModelIndex>();
        let activation_plan = DomainActivationPlan::from_scheduled_nodes(domain, &schedule.nodes)
            .map_err(|report| RuntimeError::activation_plan(domain, report))?;
        let resource_plans =
            ResourceExecutionPlans::from_scheduled_nodes(domain, &schedule.nodes, &activation_plan)
                .map_err(|report| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("failed to plan domain resources: {report:#}"),
                })?;
        let entrypoints = Arc::new(
            EntrypointPlans::from_scheduled_nodes(domain, &schedule.nodes, &activation_plan)
                .map_err(|report| RuntimeError::entrypoint_plan(domain, report))?,
        );
        let emitter_plans = Arc::new(
            EmitterExecutionPlans::from_scheduled_nodes(&schedule.nodes, &activation_plan)
                .map_err(|report| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("failed to plan emitters: {report:#}"),
                })?,
        );
        for plan in entrypoints.ingestors() {
            if let Err(error) = Self::parse_ingest_acknowledgement(
                domain,
                &plan.ingestor.name,
                plan.acknowledgement(),
            ) {
                self.record_ingestor_transient_error(
                    domain,
                    &plan.ingestor.name,
                    error.to_string(),
                );
                return Err(error);
            }
        }
        for wasm in resource_plans.wasm.values() {
            Box::pin(self.prepare_wasm_module(&wasm.module))
                .await
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("{reason:#}"),
                })?;
        }
        let udf_executor = Box::pin(self.compile_domain_udfs(domain, resource_plans.udfs.clone()))
            .await
            .map_err(|error| RuntimeError::CompileDomainUdfs {
                domain: domain.as_str().to_string(),
                report: error,
            })?;
        let all_branched_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        let branch_relays = branch_relays_from_plans(&all_branched_specs, &entrypoints);

        let ActivatedDomainSurfaces {
            codecs,
            signaling_protocols,
            endpoint_routes,
        } = Box::pin(self.activate_domain_surfaces(domain, &activation_plan)).await?;

        for relay in activation_plan.relays.values() {
            let node = schedule
                .nodes
                .get(&NodeRef::new(
                    ModelKind::Relay,
                    ModelName::from(&relay.name),
                ))
                .assured("the domain plan contains exactly the scheduled relays");
            let expiring_state =
                if node.executes_on(local_node_id) && branch_relays.contains(&relay.name) {
                    let state =
                        self.expiring_stream_state(domain, &relay.name)
                            .map_err(|error| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: error.to_string(),
                            })?;
                    Some(state)
                } else {
                    None
                };
            let fanout = Box::pin(self.relay_boundary_fanout_with_capacity(
                domain,
                &relay.name,
                relay.capacity,
                RelaySubscriptionDefinition::new(relay.schema.clone(), relay.branching.clone()),
            ))
            .await;
            let registry = match expiring_state.as_ref() {
                Some(state) => state.registry.clone(),
                None => RelayRegistry::new(),
            };
            relay_builders.insert(
                relay.name.clone(),
                RelayBoundaryBuilder {
                    fanout,
                    attached_runtime_consumer_count: 0,
                    detached_runtime_consumer_count: 0,
                    registry,
                    remote_runtime_consumers: Vec::new(),
                },
            );
            relay_branchings.insert(relay.name.clone(), relay.branching.clone());
            relay_schemas.insert(relay.name.clone(), relay.schema.clone());
            if relay.materialized {
                materialized_stream_specs.insert(
                    relay.name.clone(),
                    RuntimeMaterializedRelaySpec::new(
                        relay.schema.arrow_schema(),
                        relay.schema.vm_sensitivity(),
                        relay.branching.clone(),
                    ),
                );
                materialized_stream_owner_nodes.insert(relay.name.clone(), None);
            }
        }

        let mut placement_tasks = HashMap::<NodeRef, Vec<JoinHandle<()>>>::new();
        let mut kafka_offset_states = HashMap::new();
        let mut materialized_states = HashMap::new();
        for node in schedule.nodes.values() {
            tokio::task::consume_budget().await;
            let state = PlacedNodeState::of(
                node,
                ScheduledDomainPlans {
                    activation: &activation_plan,
                    entrypoints: &entrypoints,
                },
            );
            if let Some(PlacedNodeState::MaterializedRelay(schema)) = state.as_ref() {
                let state_placement = self
                    .state_placement(
                        domain,
                        RuntimeStateKind::MaterializedRelay,
                        ModelKind::Relay,
                        RelayName::from(&node.identifier),
                        None,
                    )
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: error.to_string(),
                    })?;
                Box::pin(self.prepare_materialized_stream_restore(&state_placement, schema))
                    .await
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: error.to_string(),
                    })?;
            }
            let placement = self.build_scheduled_node_placement(
                domain,
                &shutdown_tx,
                node,
                local_node_id,
                state,
            )?;
            if let Some(state) = placement.kafka_offset_state {
                kafka_offset_states.insert(RelayName::from(&node.identifier), state);
            }
            if let Some(state) = placement.materialized_state {
                materialized_states.insert(RelayName::from(&node.identifier), state);
            }
            if !placement.tasks.is_empty() {
                placement_tasks.insert(node.identity(), placement.tasks);
            }
        }

        for node in schedule.nodes.values() {
            if node.kind() == ModelKind::Relay {
                let relay_name = RelayName::from(&node.identifier);
                let planned = activation_plan
                    .relays
                    .get(&relay_name)
                    .verified("the domain plan covers every scheduled relay");
                if planned.materialized {
                    materialized_stream_owner_nodes
                        .insert(relay_name.clone(), node.execution_node().cloned());
                    let relay = relay_builders
                        .get_mut(&relay_name)
                        .verified("the relay boundary was installed from this domain plan");
                    if node.executes_on(local_node_id) {
                        let state =
                            materialized_states
                                .get(&relay_name)
                                .cloned()
                                .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                    domain: domain.as_str().to_string(),
                                    reason: format!(
                                        "missing materialized relay state '{}'",
                                        relay_name
                                    ),
                                })?;
                        relay_state_specs.push(RelayStateTaskSpec {
                            relay: relay_name,
                            state,
                            retention: planned.retention,
                            receiver: relay.runtime_consumer_fan_in_for_mode(AckMode::Detached),
                        });
                    }
                }
            }
            if node.kind() == ModelKind::Emitter {
                let emitter = emitter_plans
                    .emitter(&EmitterName::from(&node.identifier))
                    .assured("every scheduled emitter has a plan from this schedule");
                let mut inputs = Vec::with_capacity(emitter.inputs.len());
                for input in &emitter.inputs {
                    let Some(relay) = relay_builders.get_mut(&input.relay) else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!("missing emitter input relay '{}'", input.relay),
                        });
                    };
                    if node.executes_on(local_node_id) {
                        inputs.push((
                            input.relay.clone(),
                            relay.runtime_consumer_fan_in_for_mode(emitter.mode),
                        ));
                    }
                }
                if node.executes_on(local_node_id) {
                    emitter_specs.push((emitter.as_ref().clone(), inputs));
                }
            }
        }
        for lookup in resource_plans.lookups.values() {
            let Some(codec) = codecs.get(&lookup.codec).cloned() else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("missing compiled codec '{}'", lookup.codec),
                });
            };
            let runtime = Box::pin(self.load_lookup_runtime(lookup.clone(), codec))
                .await
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            lookup_specs.push((lookup.name.clone(), Arc::new(runtime)));
        }
        for plan in entrypoints.reingestors() {
            let identity = NodeRef::new(ModelKind::Reingestor, ModelName::from(&plan.name));
            let node = schedule
                .nodes
                .get(&identity)
                .assured("the entrypoint plans were decided from this same schedule");
            if !node.executes_on(local_node_id) {
                continue;
            }
            for input in &plan.inputs {
                let relay = relay_builders.get_mut(&input.relay).verified(
                    "the entrypoint plan resolved every input relay against the activation plan \
                     these relay boundaries were built from",
                );
                reingestor_inputs.push(PlannedReingestorInput {
                    plan: plan.clone(),
                    input: input.clone(),
                    consumer: ReingestorInputConsumer::Registered(
                        relay.runtime_consumer_fan_in_for_mode(plan.mode),
                    ),
                });
            }
        }
        for plan in entrypoints.ingestors() {
            let identity = NodeRef::new(ModelKind::Ingestor, ModelName::from(&plan.ingestor.name));
            let node = schedule
                .nodes
                .get(&identity)
                .assured("the entrypoint plans were decided from this same schedule");
            if node.executes_on(local_node_id) {
                local_ingestors.push(plan.clone());
            }
        }

        let mut processor_input_specs = Vec::new();
        for node_spec in &all_branched_specs.processors {
            let Some(node) = schedule.nodes.get(&NodeRef::new(
                node_spec.spec.kind,
                node_spec.spec.processor.clone(),
            )) else {
                continue;
            };
            let executes_locally = node.executes_on(local_node_id);
            let mut inputs = Vec::new();
            for input_relay in &node_spec.spec.input_relays {
                let Some(relay) = relay_builders.get_mut(input_relay) else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing {} '{}' input relay '{}'",
                            node_spec.spec.kind.as_str(),
                            node_spec.spec.processor.as_str(),
                            input_relay.as_str()
                        ),
                    });
                };
                if executes_locally {
                    inputs.push((
                        input_relay.clone(),
                        relay.runtime_consumer_fan_in_for_mode(node_spec.spec.mode),
                    ));
                }
            }
            if executes_locally {
                processor_input_specs.push((node_spec.clone(), inputs));
            }
        }

        // Remote delivery targets follow only from the published schedule, so the same derivation
        // seeds a freshly built domain and re-points the relays of an incrementally moved node.
        let remote_runtime_consumers = Self::remote_runtime_consumers_for_schedule(
            &schedule,
            &entrypoints,
            &emitter_plans,
            local_node_id,
        );
        for (relay, builder) in relay_builders.iter_mut() {
            builder.remote_runtime_consumers = remote_runtime_consumers
                .get(relay)
                .cloned()
                .unwrap_or_default();
        }

        let relay_registries = relay_builders
            .iter()
            .map(|(identifier, relay)| (identifier.clone(), relay.registry.clone()))
            .collect::<HashMap<_, _>>();
        for relay in relay_registries.keys() {
            if !schedule
                .nodes
                .get(&NodeRef::new(ModelKind::Relay, ModelName::from(relay)))
                .is_some_and(|node| node.executes_on(local_node_id))
            {
                continue;
            }
            self.inner
                .metrics
                .register_global_stream(domain, relay, Some(local_node_id));
        }
        let relay_services = relay_builders
            .into_iter()
            .map(|(identifier, relay)| {
                (
                    identifier,
                    Arc::new(RelayBoundaryServices::new(
                        relay.fanout,
                        relay.attached_runtime_consumer_count,
                        relay.detached_runtime_consumer_count,
                        relay.remote_runtime_consumers,
                        remote_dispatcher.clone(),
                    )),
                )
            })
            .collect::<HashMap<_, _>>();
        for (relay, services) in &relay_services {
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
        }
        let mut relay_owner_tasks = HashMap::new();
        for node in schedule.nodes.values() {
            if node.kind() != ModelKind::Relay || !node.executes_on(local_node_id) {
                continue;
            }
            let services = relay_services
                .get(&RelayName::from(&node.identifier))
                .cloned()
                .verified(
                    "these maps were built from the same scheduled relay nodes this loop walks",
                );
            let registry = relay_registries
                .get(&RelayName::from(&node.identifier))
                .cloned()
                .verified(
                    "these maps were built from the same scheduled relay nodes this loop walks",
                );
            relay_owner_tasks.insert(
                RelayName::from(&node.identifier),
                self.spawn_relay_owner_task(
                    domain,
                    &RelayName::from(&node.identifier),
                    registry,
                    services,
                    activation_plan
                        .relays
                        .get(&RelayName::from(&node.identifier))
                        .verified("the domain plan covers every scheduled relay")
                        .retention,
                ),
            );
        }

        let lookup_runtimes = lookup_specs.iter().cloned().collect::<HashMap<_, _>>();
        let local_processor_specs = processor_input_specs
            .iter()
            .map(|(spec, _)| spec.clone())
            .collect::<Vec<_>>();
        let previous_processor_plans = HashMap::default();
        let processor_plans = bind_published_processor_plans(
            &local_processor_specs,
            ProcessorPlanBindingContext {
                runtime: self,
                domain,
                model_index: &model_index,
                relay_schemas: &relay_schemas,
                relay_registries: &relay_registries,
                relay_services: &relay_services,
                relay_branchings: &relay_branchings,
                materialized_stream_specs: &materialized_stream_specs,
                lookups: &lookup_runtimes,
                udfs: Some(&udf_executor),
                previous: &previous_processor_plans,
            },
        )
        .await
        .map_err(|reason| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!("{reason:#}"),
        })?;
        let error_specs =
            MessageErrorRouteSpecs::from_scheduled_nodes(domain, &schedule.nodes, &activation_plan)
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("failed to plan message-error routes: {reason:#}"),
                })?;
        let message_error_plans = Arc::new(
            BoundMessageErrorRoutes::bind(
                error_specs,
                MessageErrorRouteBindingContext {
                    relay_registries: &relay_registries,
                    relay_services: &relay_services,
                    materialized_stream_specs: &materialized_stream_specs,
                    lookups: &lookup_runtimes,
                    udfs: &udf_executor,
                },
            )
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("failed to bind message-error routes: {reason:#}"),
            })?,
        );

        for (node_spec, inputs) in processor_input_specs {
            let entity = NodeRef {
                kind: node_spec.spec.kind,
                identifier: node_spec.spec.processor.clone(),
            };
            let template = processor_plans
                .get(&entity)
                .verified("the published plan binder returns every local processor")
                .template
                .as_ref()
                .clone();
            node_tasks.insert(
                entity,
                spawn_processor_node_runtime(
                    ProcessorRuntimeContext::new(self.clone(), domain.clone()),
                    &shutdown_tx,
                    template,
                    inputs,
                    self.inner.branch_instance_expiration_scan_interval,
                ),
            );
        }

        let execution_build_deps = ExecutionBuildDeps {
            domain,
            relay_schemas: &relay_schemas,
            relay_branchings: &relay_branchings,
            materialized_relay_specs: &materialized_stream_specs,
            lookups: &lookup_runtimes,
            udfs: Some(&udf_executor),
        };

        for generator in resource_plans.generators.values() {
            if !generator.assignment.executes_on(Some(local_node_id)) {
                continue;
            }
            let spec = GeneratorTaskSpec::bind(
                domain,
                generator,
                &relay_registries,
                &relay_services,
                &udf_executor,
            )
            .map_err(|report| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("generator binding failed: {report:#}"),
            })?;
            let entity = NodeRef::new(ModelKind::Generator, &generator.name);
            generator_tasks.insert(
                entity,
                self.spawn_generator_task(domain, &shutdown_tx, spec)?,
            );
        }

        let mut relay_state_tasks = HashMap::new();
        for spec in relay_state_specs {
            let relay = spec.relay.clone();
            let task = self.spawn_relay_state_task(domain, spec);
            relay_state_tasks.insert(relay, task);
        }

        for (emitter, inputs) in emitter_specs {
            let entity = NodeRef {
                kind: ModelKind::Emitter,
                identifier: ModelName::from(&emitter.name),
            };
            emitter_tasks.insert(
                entity,
                self.spawn_emitter_task(
                    EmitterTaskBuildDeps {
                        domain,
                        shutdown_tx: &shutdown_tx,
                        codecs: &codecs,
                        deps: self.emitter_task_deps(execution_build_deps, &emitter)?,
                    },
                    emitter,
                    inputs,
                )?,
            );
        }

        let ReingestorRuntimes {
            branched_entrypoints,
            tasks: reingestor_tasks,
        } = self
            .start_reingestor_runtimes(
                execution_build_deps,
                &shutdown_tx,
                RelayRuntimeHandles {
                    registries: &relay_registries,
                    services: &relay_services,
                },
                reingestor_inputs,
            )
            .map_err(|report| RuntimeError::entrypoint_binding(domain, report))?;

        self.install_domain_execution(
            domain,
            DomainExecution {
                schedule: schedule.clone(),
                start_version: desired_start_version,
                domain_clock,
                shutdown: shutdown_tx,
                routing: self.stage_domain_routing(
                    domain,
                    DomainRoutingSnapshot {
                        passive_only: false,
                        relay_registries,
                        relay_schemas,
                        relay_services,
                        lookups: lookup_runtimes,
                        udfs: udf_executor,
                        relay_branchings,
                        materialized_stream_specs,
                        materialized_stream_owner_nodes,
                        codecs,
                        signaling_protocols,
                        processor_plans,
                    },
                ),
                entrypoints,
                message_error_plans,
                branched_entrypoints,
                endpoint_routes,
                node_tasks,
                emitter_tasks,
                generator_tasks,
                reingestor_tasks,
                placement_tasks,
                relay_state_tasks,
                relay_owner_tasks,
                emitter_plans,
                tasks: Vec::new(),
            },
        );

        if self
            .inner
            .domains
            .get(domain)
            .is_some_and(|state| !matches!(state.status, nervix_models::DomainStatus::Running))
            || !start_ingestors
        {
            return Ok(());
        }

        for plan in local_ingestors {
            let ingestor_name = &plan.ingestor.name;
            self.clear_ingestor_transient_error(domain, ingestor_name);
            if let Err(error) = Box::pin(self.start_ingestor(&plan)).await {
                self.record_ingestor_transient_error(domain, ingestor_name, error.to_string());
                Box::pin(self.abort_domain_execution_start(domain)).await;
                return Err(error);
            }
        }

        Ok(())
    }

    pub(in crate::runtime) async fn build_passive_execution_from_schedule(
        &self,
        domain: &DomainName,
        schedule: &DomainSchedule,
    ) -> Result<DomainExecution, RuntimeError> {
        let domain_clock = self.bind_passive_domain_clock(domain).map_err(|error| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: error.to_string(),
            }
        })?;
        let mut lookups = HashMap::new();
        let activation_plan = DomainActivationPlan::from_scheduled_nodes(domain, &schedule.nodes)
            .map_err(|report| RuntimeError::activation_plan(domain, report))?;
        let resource_plans =
            ResourceExecutionPlans::from_scheduled_nodes(domain, &schedule.nodes, &activation_plan)
                .map_err(|report| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("failed to plan domain resources: {report:#}"),
                })?;
        let udf_executor = self
            .compile_domain_udfs(domain, resource_plans.udfs.clone())
            .await
            .map_err(|error| RuntimeError::CompileDomainUdfs {
                domain: domain.as_str().to_string(),
                report: error,
            })?;
        let entrypoints = Arc::new(
            EntrypointPlans::from_scheduled_nodes(domain, &schedule.nodes, &activation_plan)
                .map_err(|report| RuntimeError::entrypoint_plan(domain, report))?,
        );
        let emitter_plans = Arc::new(
            EmitterExecutionPlans::from_scheduled_nodes(&schedule.nodes, &activation_plan)
                .map_err(|report| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("failed to plan emitters: {report:#}"),
                })?,
        );
        let ActivatedDomainSurfaces {
            codecs,
            signaling_protocols,
            endpoint_routes,
        } = Box::pin(self.activate_domain_surfaces(domain, &activation_plan)).await?;
        let mut relay_builders = HashMap::new();
        let mut relay_branchings = HashMap::new();
        let mut relay_schemas = HashMap::new();
        let mut materialized_stream_specs = HashMap::new();
        let mut materialized_stream_owner_nodes = HashMap::new();

        for relay in activation_plan.relays.values() {
            let fanout = self
                .relay_boundary_fanout_with_capacity(
                    domain,
                    &relay.name,
                    relay.capacity,
                    RelaySubscriptionDefinition::new(relay.schema.clone(), relay.branching.clone()),
                )
                .await;
            relay_builders.insert(
                relay.name.clone(),
                RelayBoundaryBuilder {
                    fanout,
                    attached_runtime_consumer_count: 0,
                    detached_runtime_consumer_count: 0,
                    registry: RelayRegistry::new(),
                    remote_runtime_consumers: Vec::new(),
                },
            );
            relay_branchings.insert(relay.name.clone(), relay.branching.clone());
            relay_schemas.insert(relay.name.clone(), relay.schema.clone());
            if relay.materialized {
                materialized_stream_specs.insert(
                    relay.name.clone(),
                    RuntimeMaterializedRelaySpec::new(
                        relay.schema.arrow_schema(),
                        relay.schema.vm_sensitivity(),
                        relay.branching.clone(),
                    ),
                );
                let node = schedule
                    .nodes
                    .get(&NodeRef::new(
                        ModelKind::Relay,
                        ModelName::from(&relay.name),
                    ))
                    .assured("the domain plan contains exactly the scheduled relays");
                materialized_stream_owner_nodes
                    .insert(relay.name.clone(), node.execution_node().cloned());
            }
        }

        for lookup in resource_plans.lookups.values() {
            let Some(codec) = codecs.get(&lookup.codec).cloned() else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("missing compiled codec '{}'", lookup.codec),
                });
            };
            let runtime = self
                .load_lookup_runtime(lookup.clone(), codec)
                .await
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            lookups.insert(lookup.name.clone(), Arc::new(runtime));
        }

        let graph = self.domain_graph_handle(domain).await;
        graph.store(None);
        let (shutdown, _) = watch::channel(false);
        let relay_registries = relay_builders
            .iter()
            .map(|(identifier, relay)| (identifier.clone(), relay.registry.clone()))
            .collect::<HashMap<_, _>>();
        let relay_services = relay_builders
            .into_iter()
            .map(|(identifier, relay)| {
                (
                    identifier,
                    Arc::new(RelayBoundaryServices::new(
                        relay.fanout,
                        relay.attached_runtime_consumer_count,
                        relay.detached_runtime_consumer_count,
                        relay.remote_runtime_consumers,
                        None,
                    )),
                )
            })
            .collect::<HashMap<_, _>>();
        let error_specs =
            MessageErrorRouteSpecs::from_scheduled_nodes(domain, &schedule.nodes, &activation_plan)
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("failed to plan message-error routes: {reason:#}"),
                })?;
        let message_error_plans = Arc::new(
            BoundMessageErrorRoutes::bind(
                error_specs,
                MessageErrorRouteBindingContext {
                    relay_registries: &relay_registries,
                    relay_services: &relay_services,
                    materialized_stream_specs: &materialized_stream_specs,
                    lookups: &lookups,
                    udfs: &udf_executor,
                },
            )
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("failed to bind message-error routes: {reason:#}"),
            })?,
        );
        let start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        Ok(DomainExecution {
            schedule: schedule.clone(),
            start_version,
            domain_clock,
            shutdown,
            routing: self.stage_domain_routing(
                domain,
                DomainRoutingSnapshot {
                    passive_only: true,
                    relay_registries,
                    relay_schemas,
                    relay_services,
                    lookups,
                    udfs: udf_executor,
                    relay_branchings,
                    materialized_stream_specs,
                    materialized_stream_owner_nodes,
                    codecs,
                    signaling_protocols,
                    processor_plans: HashMap::default(),
                },
            ),
            entrypoints,
            message_error_plans,
            branched_entrypoints: HashMap::default(),
            endpoint_routes,
            node_tasks: HashMap::default(),
            emitter_tasks: HashMap::default(),
            generator_tasks: HashMap::default(),
            reingestor_tasks: HashMap::default(),
            placement_tasks: HashMap::default(),
            relay_state_tasks: HashMap::default(),
            relay_owner_tasks: HashMap::default(),
            emitter_plans,
            tasks: Vec::new(),
        })
    }
}
