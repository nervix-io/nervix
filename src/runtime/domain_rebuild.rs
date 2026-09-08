use super::*;

pub(super) fn branch_relays_from_branched_specs(specs: &BranchedNodeSpecs) -> HashSet<RelayName> {
    let mut relays = HashSet::default();
    for spec in &specs.entrypoints {
        if spec.branch_ttl.is_some() {
            relays.insert(spec.root_relay.clone());
        }
    }
    for node_spec in &specs.processors {
        if node_spec.branch_ttl.is_some() {
            relays.extend(node_spec.spec.relay_ids());
        }
    }
    relays
}

pub(super) fn relay_branching_schema_for_runtime(
    domain: &DomainName,
    relay_identifier: &RelayName,
    relay: &CreateRelay,
    effective_branching_schema: Option<&SchemaName>,
    schemas: &HashMap<SchemaName, Arc<CompiledSchema>>,
) -> Result<Option<StdArc<arrow_schema::Schema>>, RuntimeError> {
    let Some(schema_name) = effective_branching_schema else {
        if let Some(branch) = relay.branching.branch() {
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "missing effective branch branching schema for relay '{}' branched by '{}'",
                    relay_identifier.as_str(),
                    branch.as_str()
                ),
            });
        }
        return Ok(None);
    };
    let Some(schema) = schemas.get(schema_name) else {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "missing branch schema '{}' for relay '{}'",
                schema_name.as_str(),
                relay_identifier.as_str()
            ),
        });
    };
    Ok(Some(schema.arrow_schema()))
}

impl Runtime {
    pub(in crate::runtime) async fn relay_boundary_fanout_with_capacity(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        use_branch_collapse: bool,
        capacity: NonZeroUsize,
    ) -> RelayBoundaryFanout {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone());
        if let Some(fanout) = self.inner.relay_boundary_fanouts.get(&key)
            && fanout.uses_branch_collapse() == use_branch_collapse
        {
            fanout.set_capacity(capacity);
            return fanout.clone();
        }

        let fanout = if use_branch_collapse {
            RelayBoundaryFanout::branch_collapse_with_capacity(capacity)
        } else {
            RelayBoundaryFanout::direct_with_capacity(capacity)
        };
        self.inner
            .relay_boundary_fanouts
            .insert(key, fanout.clone());
        fanout
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

    pub(in crate::runtime) fn start_branched_entrypoint_runtime(
        &self,
        domain: &DomainName,
        identifier: impl Into<ModelName>,
        branched: Option<(SharedActiveGraph, IngestorRouteTemplate)>,
    ) -> Option<Arc<IngestorRouteRuntime>> {
        let identifier = identifier.into();
        branched.map(|(graph, template)| {
            IngestorRouteRuntime::new(
                self.clone(),
                domain.clone(),
                IngestorName::from(&identifier),
                graph,
                template,
                self.inner.branch_instance_expiration_scan_interval,
            )
        })
    }

    pub(in crate::runtime) fn start_branched_ingestor_runtime(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        branched: HashMap<RelayName, (SharedActiveGraph, IngestorRouteTemplate)>,
    ) -> IngestorRouteRuntimes {
        let mut roots = branched.into_iter().collect::<Vec<_>>();
        roots.sort_by(|left, right| left.0.cmp(&right.0));
        let mut runtimes = Vec::with_capacity(roots.len());
        let mut senders = HashMap::with_capacity(roots.len());
        for (root_relay, template) in roots {
            let Some(runtime) =
                self.start_branched_entrypoint_runtime(domain, ingestor, Some(template))
            else {
                continue;
            };
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
    pub(super) fn install_domain_execution(&self, domain: &DomainName, execution: DomainExecution) {
        self.publish_routed_endpoints(domain, &execution);
        self.inner.executions.insert(domain.clone(), execution);
    }

    /// Publishes an execution's endpoint routes into the routing index. Called with the execution
    /// that is about to become live so inbound requests resolve it by host and path.
    pub(super) fn publish_routed_endpoints(
        &self,
        domain: &DomainName,
        execution: &DomainExecution,
    ) {
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
        self.stop_domain_ingestors(domain).await;

        let desired_start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        let reset_for_start = if let Some((_, existing)) = self.inner.executions.remove(domain) {
            let reset_for_start =
                existing.passive_only || existing.start_version != desired_start_version;
            self.stop_domain_execution(domain, existing).await;
            reset_for_start
        } else {
            false
        };

        let Some(schedule) = schedule else {
            self.clear_domain_ingestor_quiescence(domain);
            self.inner.compiled_domain_udfs.remove(domain);
            self.clear_state_schema_fingerprints(domain);
            self.clear_domain_graph_handle(domain).await;
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        };
        self.install_state_schema_fingerprints(&schedule);
        if self
            .inner
            .domains
            .get(domain)
            .is_some_and(|state| matches!(state.status, nervix_models::DomainStatus::Stopped))
        {
            self.clear_domain_ingestor_quiescence(domain);
            self.purge_stopped_domain_runtime_state(domain)?;
            self.clear_expiring_stream_states_for_domain(domain);
            let execution = self
                .build_passive_execution_from_schedule(domain, &schedule)
                .await?;
            self.install_domain_execution(domain, execution);
            self.clear_domain_graph_handle(domain).await;
            return Ok(());
        }
        if reset_for_start {
            self.purge_stopped_domain_runtime_state(domain)?;
        }

        let domain_graph = self.domain_graph_handle(domain).await;
        domain_graph.store(None);
        let (shutdown_tx, _) = watch::channel(false);
        let mut relay_builders = HashMap::new();
        let mut relay_branchings = HashMap::new();
        let mut relay_branching_schemas = HashMap::new();
        let mut relay_schemas = HashMap::new();
        let mut materialized_stream_specs = HashMap::new();
        let mut materialized_stream_owner_nodes = HashMap::new();
        let mut schemas = HashMap::new();
        let mut wire_schemas = DomainWireSchemas::default();
        let mut codecs = HashMap::new();
        let mut signaling_protocols = HashMap::new();
        let mut transports = HashMap::new();
        let mut vhosts = HashMap::new();
        let mut endpoint_specs = Vec::new();
        let mut endpoint_routes = HashMap::new();
        let mut generator_specs = Vec::new();
        let mut lookup_specs = Vec::new();
        let mut relay_state_specs = Vec::new();
        let mut emitter_specs = Vec::new();
        let mut reingestor_specs = Vec::<ReingestorInputSpec>::new();
        let mut ingestor_specs = Vec::new();
        let mut node_tasks = HashMap::new();
        let mut emitter_tasks = HashMap::new();
        let mut generator_tasks = HashMap::new();
        let mut reingestor_tasks = HashMap::new();
        let remote_dispatcher = self.inner.remote_dispatcher.read().clone();
        let model_index = schedule
            .nodes
            .values()
            .map(|node| (*node.config).clone())
            .collect::<ModelIndex>();
        for node in schedule.nodes.values() {
            match node.config.as_ref() {
                Model::Ingestor(ingestor) => {
                    if let Err(error) = Self::validate_ingestor_start_settings(domain, ingestor) {
                        self.record_ingestor_transient_error(
                            domain,
                            &ingestor.name,
                            error.to_string(),
                        );
                        return Err(error);
                    }
                }
                Model::WasmProcessor(processor) => {
                    self.compile_wasm_processor_module(
                        domain,
                        &processor.name,
                        &processor.resource,
                        processor.resource_version,
                        &processor.file,
                    )
                    .await
                    .map_err(|reason| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason,
                    })?;
                }
                _ => {}
            }
        }
        let udf_executor = self
            .compile_domain_udfs(
                domain,
                model_index
                    .models()
                    .filter_map(|model| {
                        if let Model::Udf(udf) = model {
                            Some(udf.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
            )
            .await
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("failed to compile domain UDFs: {error}"),
            })?;
        let all_branched_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        let branch_relays = branch_relays_from_branched_specs(&all_branched_specs);
        let branched_specs = all_branched_specs
            .entrypoints
            .iter()
            .filter(|spec| {
                schedule
                    .nodes
                    .get(&NodeRef::new(spec.kind, spec.identifier.clone()))
                    .is_some_and(|node| node.executes_on(local_node_id))
            })
            .cloned()
            .collect::<Vec<_>>();

        for node in schedule.nodes.values() {
            match node.config.as_ref() {
                Model::Schema(schema) => {
                    schemas.insert(schema.name.clone(), Arc::new(compile_schema(schema)));
                }
                Model::WireJsonSchema(wire_schema) => {
                    wire_schemas.insert_json(wire_schema.clone());
                }
                Model::WireCborSchema(wire_schema) => {
                    wire_schemas.insert_cbor(wire_schema.clone());
                }
                Model::WireAvroSchema(wire_schema) => {
                    wire_schemas.insert_avro(wire_schema.clone());
                }
                model if model.kind() == ModelKind::Client => {
                    transports.insert(
                        ClientName::from(&node.identifier),
                        Arc::new((*node.config).clone()),
                    );
                }
                Model::Vhost(vhost) => {
                    vhosts.insert(vhost.name.clone(), vhost.clone());
                }
                Model::Endpoint(endpoint) => {
                    endpoint_specs.push(endpoint.clone());
                }
                Model::SignalingProtocol(protocol) => {
                    signaling_protocols.insert(
                        protocol.name.clone(),
                        self.compile_signaling_protocol(domain, protocol).await?,
                    );
                }
                Model::Generator(_) => {}
                _ => {}
            }
        }

        for endpoint in endpoint_specs {
            let Some(vhost) = vhosts.get(&endpoint.on_vhost) else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("missing vhost '{}'", endpoint.on_vhost.as_str()),
                });
            };
            let signaling_protocol = endpoint
                .signaling_protocol
                .as_ref()
                .map(|signaling_protocol| {
                    signaling_protocols
                        .get(signaling_protocol)
                        .cloned()
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing signaling protocol '{}'",
                                signaling_protocol.as_str()
                            ),
                        })
                })
                .transpose()?;
            endpoint_routes.insert(
                endpoint.name.clone(),
                EndpointRoute {
                    path: endpoint.path,
                    hostnames: vhost
                        .hostnames
                        .iter()
                        .map(|host| host.to_ascii_lowercase())
                        .collect(),
                    endpoint_type: endpoint.endpoint_type,
                    signaling_protocol,
                },
            );
        }

        for node in schedule.nodes.values() {
            if let Model::Codec(codec) = node.config.as_ref() {
                let Some(schema) = schemas.get(&codec.schema).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing compiled schema '{}'", codec.schema.as_str()),
                    });
                };
                let wire_format = wire_schemas.resolve(domain, &codec.wire_format)?;
                let compiled = self
                    .compile_domain_codec(domain, codec, schema, wire_format)
                    .await?;
                codecs.insert(codec.name.clone(), compiled);
            }
        }

        for node in schedule.nodes.values() {
            if let Model::Relay(relay) = node.config.as_ref() {
                let Some(schema) = schemas.get(&relay.schema).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing compiled relay schema '{}' for relay '{}'",
                            relay.schema.as_str(),
                            relay.name.as_str()
                        ),
                    });
                };
                let expiring_state = (node.executes_on(local_node_id)
                    && branch_relays.contains(&relay.name))
                .then(|| self.expiring_stream_state(domain, &relay.name));
                let fanout = self
                    .relay_boundary_fanout_with_capacity(
                        domain,
                        &relay.name,
                        !relay.branching.is_unbranched(),
                        relay.buffer,
                    )
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
                relay_branchings.insert(
                    relay.name.clone(),
                    node.effective_branching.clone().unwrap_or_default(),
                );
                let branching_schema = relay_branching_schema_for_runtime(
                    domain,
                    &relay.name,
                    relay,
                    node.effective_branching_schema.as_ref(),
                    &schemas,
                )?;
                relay_branching_schemas.insert(relay.name.clone(), branching_schema);
                relay_schemas.insert(relay.name.clone(), schema);
                if relay.materialized_state.is_some() {
                    materialized_stream_specs.insert(
                        relay.name.clone(),
                        RuntimeMaterializedRelaySpec::new(
                            relay_schemas
                                .get(&RelayName::from(&node.identifier))
                                .verified(
                                    "the schema was inserted under this identifier immediately \
                                     above",
                                )
                                .arrow_schema(),
                            relay_schemas
                                .get(&RelayName::from(&node.identifier))
                                .verified(
                                    "the schema was inserted under this identifier immediately \
                                     above",
                                )
                                .vm_sensitivity(),
                            node.effective_branching.clone().unwrap_or_default(),
                        ),
                    );
                    materialized_stream_owner_nodes.insert(relay.name.clone(), None);
                }
            }
        }

        let mut placement_tasks = HashMap::<NodeRef, Vec<JoinHandle<()>>>::new();
        let mut kafka_offset_states = HashMap::new();
        let mut materialized_states = HashMap::new();
        for node in schedule.nodes.values() {
            tokio::task::consume_budget().await;
            let materialized_schema = match node.config.as_ref() {
                Model::Relay(relay) if relay.materialized_state.is_some() => {
                    materialized_stream_specs
                        .get(&relay.name)
                        .map(|spec| spec.schema.clone())
                }
                _ => None,
            };
            let placement = self.build_scheduled_node_placement(
                domain,
                &shutdown_tx,
                node,
                local_node_id,
                materialized_schema,
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
            match node.config.as_ref() {
                Model::Relay(relay_model) if relay_model.materialized_state.is_some() => {
                    materialized_stream_owner_nodes
                        .insert(relay_model.name.clone(), node.execution_node().cloned());
                    let Some(relay) = relay_builders.get_mut(&relay_model.name) else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing materialized relay '{}'",
                                relay_model.name.as_str()
                            ),
                        });
                    };
                    if node.executes_on(local_node_id) {
                        let state = materialized_states
                            .get(&RelayName::from(&node.identifier))
                            .cloned()
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing materialized relay state '{}'",
                                    relay_model.name.as_str()
                                ),
                            })?;
                        relay_state_specs.push(RelayStateTaskSpec {
                            relay: relay_model.name.clone(),
                            state,
                            retention: RelayRetention::from_schedule(
                                domain,
                                &schedule,
                                &relay_model.name,
                            )?,
                            receiver: relay.runtime_consumer_fan_in_for_mode(AckMode::Detached),
                        });
                    }
                }
                Model::Generator(generator) if node.executes_on(local_node_id) => {
                    let Some(source_schema) =
                        relay_schemas.get(&generator.materialized_relay).cloned()
                    else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing generator materialized relay schema '{}'",
                                generator.materialized_relay
                            ),
                        });
                    };
                    let source_branch_schema = relay_branching_schemas
                        .get(&generator.materialized_relay)
                        .cloned()
                        .flatten();
                    let source_branching = relay_branchings
                        .get(&generator.materialized_relay)
                        .cloned()
                        .unwrap_or_default();
                    let mut routes = Vec::new();
                    for output in generator.output_routes.outputs() {
                        let Some(output_schema) = relay_schemas.get(&output.relay).cloned() else {
                            return Err(RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing generator output relay schema '{}'",
                                    output.relay
                                ),
                            });
                        };
                        let program = compile_generator_set_program(
                            domain,
                            generator,
                            output,
                            GeneratorSetProgramSchemas {
                                output: output_schema.arrow_schema(),
                                output_sensitivity: output_schema.vm_sensitivity(),
                                source: source_schema.arrow_schema(),
                                branch: source_branch_schema.clone(),
                            },
                            Some(&udf_executor),
                        )?;
                        routes.push((output.clone(), program, output_schema));
                    }
                    generator_specs.push((generator.clone(), source_branching, routes));
                }
                Model::Lookup(lookup) => {
                    let Some(codec) = codecs.get(&lookup.decode_using_codec).cloned() else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing compiled codec '{}'",
                                lookup.decode_using_codec.as_str()
                            ),
                        });
                    };
                    let runtime = self
                        .load_lookup_runtime(domain, lookup.clone(), codec)
                        .await
                        .map_err(|reason| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason,
                        })?;
                    lookup_specs.push((lookup.name.clone(), Arc::new(runtime)));
                }
                Model::Emitter(emitter) => {
                    let mut inputs = Vec::with_capacity(emitter.from.relays().len());
                    for input_relay in emitter.from.relays() {
                        let Some(relay) = relay_builders.get_mut(input_relay) else {
                            return Err(RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing emitter input relay '{}'",
                                    input_relay.as_str()
                                ),
                            });
                        };
                        if node.executes_on(local_node_id) {
                            inputs.push((
                                input_relay.clone(),
                                relay.runtime_consumer_fan_in_for_mode(emitter.mode),
                            ));
                        }
                    }
                    if node.executes_on(local_node_id) {
                        emitter_specs.push((emitter.clone(), inputs));
                    }
                }
                Model::Reingestor(reingestor) => {
                    for from_relay in reingestor.from.relays() {
                        let Some(relay) = relay_builders.get_mut(from_relay) else {
                            return Err(RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing reingestor input relay '{}'",
                                    from_relay.as_str()
                                ),
                            });
                        };
                        if node.executes_on(local_node_id) {
                            let receiver = relay.runtime_consumer_fan_in_for_mode(reingestor.mode);
                            reingestor_specs.push(ReingestorInputSpec {
                                reingestor: reingestor.clone(),
                                from_relay: from_relay.clone(),
                                receiver,
                            });
                        }
                    }
                }
                Model::Ingestor(_) if node.executes_on(local_node_id) => {
                    ingestor_specs.push(node.clone());
                }
                _ => {}
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
        let remote_runtime_consumers =
            Self::remote_runtime_consumers_for_schedule(&schedule, local_node_id);
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
                    RelayRetention::from_schedule(
                        domain,
                        &schedule,
                        &RelayName::from(&node.identifier),
                    )?,
                ),
            );
        }

        let mut branched_entrypoints = HashMap::new();
        let mut branched_entrypoint_senders = HashMap::new();
        for spec in &branched_specs {
            if spec.kind != ModelKind::Reingestor {
                continue;
            }
            let template = materialize_ingestor_route_template(
                spec,
                &model_index,
                &relay_registries,
                &relay_services,
            )
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            })?;
            let Some(runtime) = self.start_branched_entrypoint_runtime(
                domain,
                &spec.identifier,
                Some((domain_graph.clone(), template)),
            ) else {
                continue;
            };
            branched_entrypoint_senders.insert(spec.root_relay.clone(), runtime.sender());
            branched_entrypoints
                .entry(spec.identifier.clone())
                .or_insert_with(Vec::new)
                .push(runtime);
        }

        for (node_spec, inputs) in processor_input_specs {
            let mut template = materialize_processor_instance_template(
                &node_spec,
                &model_index,
                &relay_schemas,
                &relay_registries,
                &relay_services,
                Some(&udf_executor),
            )
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            })?;
            template
                .prepare_wasm_processors(self, domain)
                .await
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason,
                })?;
            let entity = NodeRef {
                kind: node_spec.spec.kind,
                identifier: node_spec.spec.processor.clone(),
            };
            node_tasks.insert(
                entity,
                spawn_processor_node_runtime(
                    ProcessorRuntimeContext::new(
                        self.clone(),
                        domain.clone(),
                        domain_graph.clone(),
                    ),
                    &shutdown_tx,
                    template,
                    inputs,
                    self.inner.branch_instance_expiration_scan_interval,
                ),
            );
        }

        let lookup_runtimes = lookup_specs.iter().cloned().collect::<HashMap<_, _>>();
        let execution_build_deps = ExecutionBuildDeps {
            domain,
            relay_schemas: &relay_schemas,
            relay_branchings: &relay_branchings,
            materialized_relay_specs: &materialized_stream_specs,
            lookups: &lookup_runtimes,
        };

        for (generator, source_branching, route_specs) in generator_specs {
            let source_schema = relay_schemas
                .get(&generator.materialized_relay)
                .cloned()
                .ok_or_else(|| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing generator materialized relay schema '{}'",
                        generator.materialized_relay
                    ),
                })?;
            let source_branch_schema = relay_branching_schemas
                .get(&generator.materialized_relay)
                .cloned()
                .flatten();
            let mut routes = Vec::with_capacity(route_specs.len());
            for (output, program, output_schema) in route_specs {
                let Some(output_registry) = relay_registries.get(&output.relay).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing generator output relay '{}'", output.relay),
                    });
                };
                let Some(output_services) = relay_services.get(&output.relay).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing generator output relay services '{}'",
                            output.relay
                        ),
                    });
                };
                routes.push(GeneratorTaskRouteSpec::new(
                    output,
                    program,
                    output_schema,
                    output_registry,
                    output_services,
                ));
            }
            let entity = NodeRef {
                kind: ModelKind::Generator,
                identifier: ModelName::from(&generator.name),
            };
            generator_tasks.insert(
                entity,
                self.spawn_generator_task(
                    domain,
                    &shutdown_tx,
                    GeneratorTaskSpec::new(
                        generator,
                        source_schema,
                        source_branching,
                        source_branch_schema,
                        routes,
                    ),
                )?,
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
                        clients: &transports,
                        deps: self.emitter_task_deps(execution_build_deps, &emitter)?,
                    },
                    emitter,
                    inputs,
                )?,
            );
        }

        for spec in reingestor_specs {
            let entity = NodeRef {
                kind: ModelKind::Reingestor,
                identifier: ModelName::from(&spec.reingestor.name),
            };
            reingestor_tasks
                .entry(entity)
                .or_insert_with(Vec::new)
                .push(self.spawn_reingestor_task(
                    domain,
                    &shutdown_tx,
                    &branched_entrypoint_senders,
                    spec.reingestor,
                    spec.from_relay,
                    spec.receiver,
                )?);
        }

        self.install_domain_execution(
            domain,
            DomainExecution {
                schedule: schedule.clone(),
                passive_only: false,
                start_version: desired_start_version,
                shutdown: shutdown_tx,
                graph: domain_graph.clone(),
                relay_registries,
                relay_schemas,
                relay_services,
                lookups: lookup_runtimes,
                udfs: udf_executor,
                relay_branchings,
                relay_branching_schemas,
                materialized_stream_specs,
                materialized_stream_owner_nodes,
                branched_ingestors: Self::branched_specs_by_identifier(&branched_specs),
                branched_entrypoints,
                codecs,
                signaling_protocols,
                endpoint_routes,
                node_tasks,
                emitter_tasks,
                generator_tasks,
                reingestor_tasks,
                placement_tasks,
                relay_state_tasks,
                relay_owner_tasks,
                clients: transports,
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

        for node in ingestor_specs {
            let Model::Ingestor(ingestor) = node.config.as_ref() else {
                continue;
            };
            let Some(source_model) = Self::source_model_for_scheduled_ingestor(&schedule, ingestor)
            else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("missing ingestor source for '{}'", ingestor.name.as_str()),
                });
            };
            let ingestor_name = ingestor.name.clone();
            let plan =
                IngestorStartPlan::decide(domain, &node, &source_model).map_err(|error| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "cannot plan ingestor '{}': {error}",
                            ingestor.name.as_str()
                        ),
                    }
                })?;
            self.clear_ingestor_transient_error(domain, &ingestor_name);
            if let Err(error) = self.start_ingestor(plan).await {
                self.record_ingestor_transient_error(domain, &ingestor_name, error.to_string());
                self.abort_domain_execution_start(domain).await;
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
        let udf_executor = self
            .compile_domain_udfs(
                domain,
                schedule
                    .nodes
                    .values()
                    .filter_map(|node| {
                        if let Model::Udf(udf) = node.config.as_ref() {
                            Some(udf.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
            )
            .await
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("failed to compile domain UDFs: {error}"),
            })?;
        let mut relay_builders = HashMap::new();
        let mut relay_branchings = HashMap::new();
        let mut relay_branching_schemas = HashMap::new();
        let mut relay_schemas = HashMap::new();
        let mut schemas = HashMap::new();
        let mut wire_schemas = DomainWireSchemas::default();
        let mut codecs = HashMap::new();
        let mut lookups = HashMap::new();

        for node in schedule.nodes.values() {
            match node.config.as_ref() {
                Model::Schema(schema) => {
                    schemas.insert(schema.name.clone(), Arc::new(compile_schema(schema)));
                }
                Model::WireJsonSchema(wire_schema) => {
                    wire_schemas.insert_json(wire_schema.clone());
                }
                Model::WireCborSchema(wire_schema) => {
                    wire_schemas.insert_cbor(wire_schema.clone());
                }
                Model::WireAvroSchema(wire_schema) => {
                    wire_schemas.insert_avro(wire_schema.clone());
                }
                _ => {}
            }
        }

        for node in schedule.nodes.values() {
            let Model::Relay(relay) = node.config.as_ref() else {
                continue;
            };
            let Some(schema) = schemas.get(&relay.schema).cloned() else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing compiled relay schema '{}' for relay '{}'",
                        relay.schema.as_str(),
                        node.identifier.as_str()
                    ),
                });
            };
            let fanout = self
                .relay_boundary_fanout_with_capacity(
                    domain,
                    &relay.name.clone(),
                    !relay.branching.is_unbranched(),
                    relay.buffer,
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
            relay_branchings.insert(
                relay.name.clone(),
                node.effective_branching.clone().unwrap_or_default(),
            );
            let branching_schema = relay_branching_schema_for_runtime(
                domain,
                &relay.name,
                relay,
                node.effective_branching_schema.as_ref(),
                &schemas,
            )?;
            relay_branching_schemas.insert(relay.name.clone(), branching_schema);
            relay_schemas.insert(relay.name.clone(), schema);
        }

        for node in schedule.nodes.values() {
            if let Model::Codec(codec) = node.config.as_ref() {
                let Some(schema) = schemas.get(&codec.schema).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing compiled schema '{}'", codec.schema.as_str()),
                    });
                };
                let wire_format = wire_schemas.resolve(domain, &codec.wire_format)?;
                let compiled = self
                    .compile_domain_codec(domain, codec, schema, wire_format)
                    .await?;
                codecs.insert(codec.name.clone(), compiled);
            }
        }

        for node in schedule.nodes.values() {
            if let Model::Lookup(lookup) = node.config.as_ref() {
                let Some(codec) = codecs.get(&lookup.decode_using_codec).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing compiled codec '{}'",
                            lookup.decode_using_codec.as_str()
                        ),
                    });
                };
                let runtime = self
                    .load_lookup_runtime(domain, lookup.clone(), codec)
                    .await
                    .map_err(|reason| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason,
                    })?;
                lookups.insert(lookup.name.clone(), Arc::new(runtime));
            }
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
        let start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        Ok(DomainExecution {
            schedule: schedule.clone(),
            passive_only: true,
            start_version,
            shutdown,
            graph,
            relay_registries,
            relay_schemas,
            relay_services,
            lookups,
            udfs: udf_executor,
            relay_branchings,
            relay_branching_schemas,
            materialized_stream_specs: HashMap::default(),
            materialized_stream_owner_nodes: HashMap::default(),
            branched_ingestors: HashMap::default(),
            branched_entrypoints: HashMap::default(),
            codecs,
            signaling_protocols: HashMap::default(),
            endpoint_routes: HashMap::default(),
            node_tasks: HashMap::default(),
            emitter_tasks: HashMap::default(),
            generator_tasks: HashMap::default(),
            reingestor_tasks: HashMap::default(),
            placement_tasks: HashMap::default(),
            relay_state_tasks: HashMap::default(),
            relay_owner_tasks: HashMap::default(),
            clients: HashMap::default(),
            tasks: Vec::new(),
        })
    }
}
