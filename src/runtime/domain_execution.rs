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

pub(super) const DOMAIN_TICK_HISTORY_LIMIT: usize = 256;

#[derive(Debug, Clone)]
pub(super) struct ObservedDomainTick {
    pub(super) tick_id: u64,
    pub(super) logical_timestamp: Timestamp,
    pub(super) wall_clock: Timestamp,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeDomainClockState {
    pub(super) logical_started_at: Timestamp,
    pub(super) wall_started_at: Timestamp,
    pub(super) time_rate: String,
}

#[derive(Debug)]
pub(super) struct RuntimeDomainState {
    pub(super) config: DomainConfig,
    pub(super) status: nervix_models::DomainStatus,
    pub(super) start_version: u64,
    pub(super) last_start: nervix_models::DomainStartPoint,
    pub(super) clock: Option<RuntimeDomainClockState>,
    pub(super) ticks: parking_lot::Mutex<VecDeque<ObservedDomainTick>>,
}

impl Runtime {
    pub fn sync_domains(&self, domains: &BTreeMap<DomainName, DomainState>) {
        for domain in self
            .inner
            .domains
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>()
        {
            if !domains.contains_key(&domain) {
                self.inner.domains.remove(&domain);
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
            let mut entry =
                self.inner
                    .domains
                    .entry(domain.clone())
                    .or_insert_with(|| RuntimeDomainState {
                        config: state.config.clone(),
                        status: state.status.clone(),
                        start_version: state.start_version,
                        last_start: state.last_start.clone(),
                        clock: None,
                        ticks: parking_lot::Mutex::new(VecDeque::new()),
                    });
            entry.config = state.config.clone();
            entry.status = state.status.clone();
            entry.start_version = state.start_version;
            entry.last_start = state.last_start.clone();
            if let nervix_models::DomainStatus::Stopped = state.status {
                entry.clock = None;
                entry.ticks.lock().clear();
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
            if stopped {
                self.purge_stopped_domain_runtime_state(domain)?;
            }
            self.clear_domain_graph_handle(domain).await;
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        }
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
            .map(|node| (node.node_ref(), (*node.config).clone()))
            .collect::<HashMap<_, _>>();
        let udf_executor = self
            .compile_domain_udfs(
                domain,
                model_index
                    .values()
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
                Model::ClientKafka(_)
                | Model::ClientPulsar(_)
                | Model::ClientHttp(_)
                | Model::ClientSentry(_)
                | Model::ClientOtel(_)
                | Model::ClientPrometheus(_)
                | Model::ClientRabbitMq(_)
                | Model::ClientRedis(_)
                | Model::ClientMqtt(_)
                | Model::ClientNats(_)
                | Model::ClientZeroMq(_)
                | Model::ClientSqs(_)
                | Model::ClientWebsockets(_)
                | Model::ClientClickHouse(_)
                | Model::ClientPostgres(_)
                | Model::ClientMySql(_)
                | Model::ClientMongoDb(_)
                | Model::ClientS3(_)
                | Model::ClientGcs(_)
                | Model::ClientAzureBlob(_)
                | Model::ClientIcebergRest(_)
                | Model::ClientSyslog(_) => {
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
                let registry = expiring_state
                    .as_ref()
                    .map(|state| state.registry.clone())
                    .unwrap_or_else(RelayRegistry::new);
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

        self.install_domain_execution(
            domain,
            DomainExecution {
                schedule: DomainSchedule::new(
                    domain.clone(),
                    graph
                        .nodes()
                        .into_iter()
                        .map(|node| ScheduledNode {
                            schema_fingerprint: graph
                                .schema_fingerprint(node.kind, &node.identifier)
                                .unwrap_or([0; 32]),
                            identifier: node.identifier,
                            kind: node.kind,
                            config: Box::new((*node.config).clone()),
                            effective_branching: node.effective_branching,
                            effective_branching_schema: node.effective_branching_schema,
                            kafka_partition_schedule: None,
                            primary_node: None,
                            assigned_nodes: Vec::new(),
                        })
                        .collect::<Vec<_>>(),
                    Vec::new(),
                ),
                passive_only: false,
                start_version: self
                    .inner
                    .domains
                    .get(domain)
                    .map_or(0, |state| state.start_version),
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
        DomainConfig, DomainPace, DomainState, DomainStatus, DomainTick, Timestamp,
    };

    use super::*;

    #[test]
    fn sync_domains_clears_ticks_when_paced_domain_stops() {
        let runtime = Runtime::new();
        let mut domains = BTreeMap::new();
        domains.insert(domain("paced"), paced_domain_state("paced"));
        runtime.sync_domains(&domains);
        runtime.handle_domain_tick(
            &domain("paced"),
            &DomainTick {
                tick_id: 1,
                logical_timestamp: Timestamp::from_unix_nanos(0),
                wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
                duration_ms: 1_000,
            },
        );

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
                .ensure_domain_allows_ingestion(
                    &domain("paced"),
                    &named("ing"),
                    Timestamp::from_unix_nanos(10_000_000),
                )
                .is_err()
        );
    }

    #[test]
    fn sync_domains_preserves_clock_state_but_rejects_ingestion_while_paused() {
        let runtime = Runtime::new();
        let mut domains = BTreeMap::new();
        domains.insert(domain("paced"), paced_domain_state("paced"));
        runtime.sync_domains(&domains);
        runtime.handle_domain_tick(
            &domain("paced"),
            &DomainTick {
                tick_id: 1,
                logical_timestamp: Timestamp::from_unix_nanos(0),
                wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
                duration_ms: 1_000,
            },
        );

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
                .ticks
                .lock()
                .len(),
            1
        );
        let error = runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &named("ing"),
                Timestamp::from_unix_nanos(10_000_000_000),
            )
            .expect_err("paused domain must reject ingestion");
        assert!(error.contains("paused"));
    }
}
