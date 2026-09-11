//! Installed domain execution and lifecycle state.
//!
//! Layer: data plane.
//! - **Owns.** Applying committed domain state to clocks and node-local execution ownership.
//! - **Depends on.** Vocabulary, installed plans and runtime infrastructure.
//! - **Must not know.** Parsing or control-plane placement and transaction decisions.
//!
//! Existing model-backed execution fields violate the data-plane plan boundary.

use super::*;

/// One resource as a domain owns it, which is how installed resource versions are tracked.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct DomainResourceKey {
    pub(super) domain: DomainName,
    pub(super) resource: ResourceName,
}

pub(super) struct DomainExecution {
    pub(super) schedule: DomainSchedule,
    pub(super) passive_only: bool,
    pub(super) start_version: u64,
    pub(super) domain_clock: DomainClock,
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) graph: SharedActiveGraph,
    pub(super) relay_registries: HashMap<RelayName, RelayRegistry>,
    pub(super) relay_schemas: HashMap<RelayName, Arc<CompiledSchema>>,
    pub(super) relay_services: HashMap<RelayName, Arc<RelayBoundaryServices>>,
    pub(super) lookups: HashMap<LookupName, Arc<LookupRuntime>>,
    pub(super) udfs: UdfExecutor,
    pub(super) relay_branchings: HashMap<RelayName, Vec<FieldName>>,
    pub(super) relay_branching_schemas: HashMap<RelayName, Option<StdArc<arrow_schema::Schema>>>,
    pub(super) materialized_stream_specs: HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    pub(super) materialized_stream_owner_nodes: HashMap<RelayName, Option<ClusterNodeName>>,
    pub(super) branched_ingestors: HashMap<ModelName, Vec<BranchedIngestorSpec>>,
    pub(super) branched_entrypoints: HashMap<ModelName, Vec<Arc<IngestorRouteRuntime>>>,
    pub(super) codecs: HashMap<CodecName, Arc<CompiledCodec>>,
    pub(super) signaling_protocols: HashMap<SignalingProtocolName, Arc<CompiledSignalingProtocol>>,
    pub(super) endpoint_routes: HashMap<EndpointName, EndpointRoute>,
    pub(super) node_tasks: HashMap<NodeRef, ScheduledNodeTask>,
    pub(super) emitter_tasks: HashMap<NodeRef, ScheduledEmitterTask>,
    pub(super) generator_tasks: HashMap<NodeRef, JoinHandle<()>>,
    pub(super) reingestor_tasks: HashMap<NodeRef, Vec<JoinHandle<()>>>,
    /// Placement-derived tasks grouped by the scheduled node whose assignment owns them, so a
    /// reassignment can replace one node's replication runtime without disturbing its siblings.
    pub(super) placement_tasks: HashMap<NodeRef, Vec<JoinHandle<()>>>,
    /// The task maintaining each locally owned materialized relay, keyed by that relay.
    pub(super) relay_state_tasks: HashMap<RelayName, RelayStateTask>,
    /// The single buffer-and-fan-out task for every relay owned by this cluster node.
    pub(super) relay_owner_tasks: HashMap<RelayName, RelayOwnerTask>,
    pub(super) clients: HashMap<ClientName, Arc<Model>>,
    pub(super) tasks: Vec<JoinHandle<()>>,
}

impl DomainExecution {
    /// The host and path keys inbound routing uses to reach this domain's endpoint routes.
    pub(super) fn routed_endpoints(
        &self,
    ) -> impl Iterator<Item = (HttpRouteKey, RoutedEndpoint)> + '_ {
        self.endpoint_routes.values().flat_map(|route| {
            route.hostnames.iter().map(|host| {
                (
                    HttpRouteKey {
                        host: host.clone(),
                        path: route.path.clone(),
                    },
                    RoutedEndpoint {
                        endpoint_type: route.endpoint_type,
                        signaling_protocol: route.signaling_protocol.clone(),
                    },
                )
            })
        })
    }
}

#[derive(Debug)]
pub(crate) struct LookupRuntime {
    pub(super) model: CreateLookup,
    pub(super) resource_version: u64,
    pub(super) schema: Arc<CompiledSchema>,
    pub(super) batch: Arc<RuntimeRecordBatch>,
    pub(super) entries: Arc<HashMap<String, usize>>,
}

#[derive(Debug, Clone)]
pub(super) struct ObservedDomainTick {
    pub(super) tick_id: u64,
    pub(super) wall_clock: Timestamp,
}

#[derive(Debug)]
pub(super) struct RuntimeDomainState {
    pub(super) config: DomainConfig,
    pub(super) status: nervix_models::DomainStatus,
    pub(super) start_version: u64,
    pub(super) last_start: nervix_models::DomainStartPoint,
    pub(super) clock_authority: DomainClockAuthority,
    pub(super) clock: DomainClockLifecycle,
    pub(super) progress: parking_lot::Mutex<Option<ObservedDomainTick>>,
}

impl Runtime {
    pub(crate) fn subscribe_domain_state(&self) -> watch::Receiver<u64> {
        self.inner.domain_status_changed.subscribe()
    }

    pub(crate) fn has_domain_clock_authority(
        &self,
        domain: &DomainName,
        generation: u64,
        authority: &DomainClockAuthority,
    ) -> bool {
        self.inner.domains.get(domain).is_some_and(|state| {
            !matches!(state.status, nervix_models::DomainStatus::Stopped)
                && state.start_version == generation
                && state.clock_authority == *authority
        })
    }

    #[cfg(test)]
    pub(super) fn sync_domains(&self, domains: &BTreeMap<DomainName, DomainState>) {
        let authority = DomainClockAuthority::assigned(
            nervix_models::DomainClockAuthorityRevision::INITIAL,
            nervix_models::ClusterNodeIdentity::new(
                ClusterNodeName::parse("test-clock-authority")
                    .assured("the fixed test authority name satisfies the name grammar"),
                nervix_models::ClusterNodeIncarnation::new(1),
            ),
        );
        let authorities = domains
            .keys()
            .map(|domain| (domain.clone(), authority.clone()))
            .collect::<BTreeMap<_, _>>();
        self.sync_committed_domains(domains, &authorities);
    }

    pub(super) fn sync_committed_domains(
        &self,
        domains: &BTreeMap<DomainName, DomainState>,
        authorities: &BTreeMap<DomainName, DomainClockAuthority>,
    ) {
        for domain in self
            .inner
            .domains
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>()
        {
            if !domains.contains_key(&domain) {
                if let Some((_, removed)) = self.inner.domains.remove(&domain) {
                    removed.clock.mark_missing();
                }
                self.inner.domain_instantiation_errors.remove(&domain);
                self.inner.in_flight_by_domain.remove(&domain);
                self.inner
                    .in_flight_by_ingestor
                    .retain(|key, _| key.domain != domain);
                self.inner.generator_activity_by_domain.remove(&domain);
                if let Some((_, force_flush)) = self.inner.force_flush_by_domain.remove(&domain) {
                    force_flush.close();
                }
            }
        }

        for (domain, state) in domains {
            let authority = authorities
                .get(domain)
                .cloned()
                .unwrap_or_else(DomainClockAuthority::initial);
            let mut entry = self.inner.domains.entry(domain.clone()).or_insert_with(|| {
                let clock = DomainClockLifecycle::new(domain.clone());
                clock.synchronize(state, &authority);
                RuntimeDomainState {
                    config: state.config.clone(),
                    status: state.status.clone(),
                    start_version: state.start_version,
                    last_start: state.last_start.clone(),
                    clock_authority: authority.clone(),
                    clock,
                    progress: parking_lot::Mutex::new(None),
                }
            });
            let generation_changed = entry.start_version != state.start_version;
            entry.config = state.config.clone();
            entry.status = state.status.clone();
            entry.start_version = state.start_version;
            entry.last_start = state.last_start.clone();
            entry.clock_authority = authority.clone();
            entry.clock.synchronize(state, &authority);
            if generation_changed || matches!(state.status, nervix_models::DomainStatus::Stopped) {
                *entry.progress.lock() = None;
            }
        }
        self.inner.domain_status_changed.send_modify(|version| {
            *version = version
                .checked_add(1)
                .assured("a node cannot apply 2^64 domain status changes");
        });
    }

    pub(in crate::runtime) async fn rebuild_domain_execution(
        &self,
        domain: &DomainName,
        graph: Option<ActiveGraph>,
    ) -> Result<(), RuntimeError> {
        if let Some((_, existing)) = self.inner.executions.remove(domain) {
            self.stop_domain_execution(domain, existing).await;
        }

        let Some(graph) = graph else {
            self.clear_domain_graph_handle(domain).await;
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        };
        let stopped = self
            .inner
            .domains
            .get(domain)
            .is_some_and(|state| matches!(state.status, nervix_models::DomainStatus::Stopped));
        if stopped || !self.inner.domains.contains_key(domain) {
            self.clear_domain_graph_handle(domain).await;
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        }
        let domain_clock =
            self.bind_domain_clock(domain)
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
        self.install_state_schema_fingerprints_from_graph(domain, &graph);

        let domain_graph = self.domain_graph_handle(domain).await;
        domain_graph.store(Some(StdArc::new(graph.clone())));
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
        let mut emitter_specs = Vec::new();
        let mut reingestor_specs = Vec::<ReingestorInputSpec>::new();
        let tasks = Vec::new();
        let mut node_tasks = HashMap::new();
        let mut emitter_tasks = HashMap::new();
        let mut generator_tasks = HashMap::new();
        let mut reingestor_tasks = HashMap::new();
        let branched_specs = branched_node_specs_from_active_graph(&graph);
        let branch_relays = branch_relays_from_branched_specs(&branched_specs);
        let model_index = graph
            .nodes()
            .into_iter()
            .map(|node| (*node.config).clone())
            .collect::<ModelIndex>();
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

        for node in graph.nodes() {
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
                    transports.insert(ClientName::from(&node.identifier), node.config.clone());
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

        for node in graph.nodes() {
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

        for node in graph.nodes() {
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
                let expiring_state = branch_relays
                    .contains(&relay.name)
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

        for node in graph.nodes() {
            match node.config.as_ref() {
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
                Model::Generator(generator) => {
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
                        inputs.push((
                            input_relay.clone(),
                            relay.runtime_consumer_fan_in_for_mode(emitter.mode),
                        ));
                    }
                    emitter_specs.push((emitter.clone(), inputs));
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
                        let receiver = relay.runtime_consumer_fan_in_for_mode(reingestor.mode);
                        reingestor_specs.push(ReingestorInputSpec {
                            reingestor: reingestor.clone(),
                            from_relay: from_relay.clone(),
                            receiver,
                        });
                    }
                }
                _ => {}
            }
        }

        let mut processor_input_specs = Vec::new();
        for node_spec in &branched_specs.processors {
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
                inputs.push((
                    input_relay.clone(),
                    relay.runtime_consumer_fan_in_for_mode(node_spec.spec.mode),
                ));
            }
            processor_input_specs.push((node_spec.clone(), inputs));
        }

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
        let relay_owner_tasks = relay_services
            .iter()
            .map(|(relay, services)| {
                let registry = relay_registries.get(relay).cloned().verified(
                    "the registries were built from the same relay set as the services this loop \
                     walks",
                );
                (
                    relay.clone(),
                    self.spawn_relay_owner_task(
                        domain,
                        relay,
                        registry,
                        services.clone(),
                        RelayRetention::default(),
                    ),
                )
            })
            .collect();

        let mut branched_entrypoints = HashMap::new();
        let mut branched_entrypoint_senders = HashMap::new();
        for spec in &branched_specs.entrypoints {
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

        let start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        self.install_domain_execution(
            domain,
            DomainExecution {
                schedule: DomainSchedule::new(
                    domain.clone(),
                    graph
                        .nodes()
                        .into_iter()
                        .map(|node| {
                            let fingerprint = graph
                                .schema_fingerprint(node.kind, &node.identifier)
                                .unwrap_or([0; 32]);
                            ScheduledNode::new((*node.config).clone())
                                .with_effective_branching(
                                    node.effective_branching,
                                    node.effective_branching_schema,
                                )
                                .with_schema_fingerprint(fingerprint)
                        })
                        .collect::<Vec<_>>(),
                    Vec::new(),
                ),
                passive_only: false,
                start_version,
                domain_clock,
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
                branched_ingestors: Self::branched_specs_by_identifier(&branched_specs.entrypoints),
                branched_entrypoints,
                codecs,
                signaling_protocols,
                endpoint_routes,
                node_tasks,
                emitter_tasks,
                generator_tasks,
                reingestor_tasks,
                placement_tasks: HashMap::default(),
                relay_state_tasks: HashMap::default(),
                relay_owner_tasks,
                clients: transports,
                tasks,
            },
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{
        DomainClockState, DomainConfig, DomainPace, DomainState, DomainStatus, DomainTick,
        DomainTimeRate, Timestamp,
    };

    use super::*;
    use crate::runtime::domain_clock::DomainClockAccessError;

    #[test]
    fn sync_domains_stops_ingestion_when_paced_domain_stops() {
        let runtime = Runtime::new();
        let mut domains = BTreeMap::new();
        domains.insert(domain("paced"), paced_domain_state("paced"));
        runtime.sync_domains(&domains);
        runtime
            .handle_domain_tick(
                &domain("paced"),
                &DomainTick {
                    tick_id: 1,
                    logical_timestamp: Timestamp::from_unix_nanos(0),
                    wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
                    period: "1s".parse().expect("fixture period is valid"),
                },
            )
            .expect("the fixture domain exists");

        domains.insert(
            domain("paced"),
            DomainState {
                id: domain("paced"),
                config: DomainConfig {
                    pace: DomainPace::Paced,
                    period: "1s".to_string(),
                    skew: "250ms".to_string(),
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Stopped,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            },
        );
        runtime.sync_domains(&domains);

        assert!(
            runtime
                .ingestion_time(&domain("paced"), &named("ing"))
                .is_err()
        );
    }

    #[test]
    fn sync_domains_preserves_clock_state_but_rejects_ingestion_while_paused() {
        let runtime = Runtime::new();
        let mut domains = BTreeMap::new();
        domains.insert(domain("paced"), paced_domain_state("paced"));
        runtime.sync_domains(&domains);
        runtime
            .handle_domain_tick(
                &domain("paced"),
                &DomainTick {
                    tick_id: 1,
                    logical_timestamp: Timestamp::from_unix_nanos(0),
                    wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
                    period: "1s".parse().expect("fixture period is valid"),
                },
            )
            .expect("the fixture domain exists");

        let mut paused = paced_domain_state("paced");
        paused.status = DomainStatus::Paused;
        domains.insert(domain("paced"), paused);
        runtime.sync_domains(&domains);

        assert_eq!(
            runtime
                .inner
                .domains
                .get(&domain("paced"))
                .expect("domain should remain")
                .progress
                .lock()
                .as_ref()
                .map(|progress| progress.tick_id),
            Some(1)
        );
        let error = runtime
            .ingestion_time(&domain("paced"), &named("ing"))
            .err()
            .assured("paused domain must reject ingestion");
        assert!(matches!(
            error.current_context(),
            super::ingestion_time::IngestionTimeError::Paused { .. }
        ));
    }

    #[test]
    fn sync_domains_installs_the_committed_generation_before_execution_binds() {
        let runtime = Runtime::new();
        let clock_domain = domain("paced");
        let logical_origin = "2000-01-01T00:00:00Z"
            .parse::<Timestamp>()
            .expect("fixture timestamp is valid");
        let mapping =
            DomainClockState::new(current_timestamp(), logical_origin, DomainTimeRate::ONE);
        let mut state = paced_domain_state("paced");
        state.start_version = 9;
        state.clock = Some(mapping);

        runtime.sync_domains(&BTreeMap::from([(clock_domain.clone(), state)]));
        let snapshot = runtime
            .bind_domain_clock(&clock_domain)
            .and_then(|clock| clock.snapshot())
            .expect("the replicated mapping must be installed before execution");

        assert_eq!(snapshot.generation(), 9);
        assert!(
            snapshot.now()
                < "2001-01-01T00:00:00Z"
                    .parse::<Timestamp>()
                    .expect("fixture timestamp is valid")
        );
    }

    #[tokio::test]
    async fn execution_rejects_an_uninstalled_paced_clock() {
        let runtime = Runtime::new();
        let clock_domain = domain("paced");
        runtime.sync_domains(&BTreeMap::from([(
            clock_domain.clone(),
            paced_domain_state("paced"),
        )]));
        let schedule = DomainSchedule::new(clock_domain.clone(), Vec::new(), Vec::new());

        let result = runtime
            .build_passive_execution_from_schedule(&clock_domain, &schedule)
            .await;

        let Err(RuntimeError::BuildDomainExecution { domain, reason }) = result else {
            panic!("execution must reject a paced domain without an installed mapping");
        };
        assert_eq!(domain, clock_domain.as_str());
        assert!(
            reason.contains("not installed"),
            "unexpected error: {reason}"
        );
    }

    #[tokio::test]
    async fn execution_rejects_a_mapping_without_a_committed_authority() {
        let runtime = Runtime::new();
        let clock_domain = domain("paced");
        let mut state = paced_domain_state("paced");
        state.start_version = 3;
        state.clock = Some(DomainClockState::new(
            current_timestamp(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ));
        runtime.sync_committed_domains(
            &BTreeMap::from([(clock_domain.clone(), state)]),
            &BTreeMap::new(),
        );
        let schedule = DomainSchedule::new(clock_domain.clone(), Vec::new(), Vec::new());

        let result = runtime
            .build_passive_execution_from_schedule(&clock_domain, &schedule)
            .await;

        let Err(RuntimeError::BuildDomainExecution { domain, reason }) = result else {
            panic!("execution must reject a paced clock without its committed authority");
        };
        assert_eq!(domain, clock_domain.as_str());
        assert!(
            reason.contains("not installed"),
            "unexpected error: {reason}"
        );
    }

    #[tokio::test]
    async fn passive_execution_keeps_a_stopped_clock_unreadable() {
        let runtime = Runtime::new();
        let clock_domain = domain("stopped");
        let mut stopped = unpaced_domain_state(clock_domain.as_str());
        stopped.status = DomainStatus::Stopped;
        runtime.sync_domains(&BTreeMap::from([(clock_domain.clone(), stopped)]));
        let schedule = DomainSchedule::new(clock_domain.clone(), Vec::new(), Vec::new());

        let execution = runtime
            .build_passive_execution_from_schedule(&clock_domain, &schedule)
            .await
            .expect("stopped domains retain passive model execution");
        let error = execution
            .domain_clock
            .snapshot()
            .expect_err("passive execution must not make a stopped clock readable");

        assert!(execution.passive_only);
        assert!(matches!(
            error.current_context(),
            DomainClockAccessError::Stopped { domain, generation: 0 }
                if domain == &clock_domain
        ));
    }
}
