//! Installed domain execution and lifecycle state.
//!
//! Layer: data plane.
//! - **Owns.** Applying committed domain state to clocks and node-local execution ownership.
//! - **Depends on.** Vocabulary, installed plans and runtime infrastructure.
//! - **Must not know.** Parsing or control-plane placement and transaction decisions.
//!
//! The installed revision owns the planned nodes, placement and executable decisions together.

use nervix_connector_websockets::CompiledSignalingProtocol;

use super::{domain_rebuild::ActivatedDomainSurfaces, *};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(super) enum DomainRoutingError {
    #[error("domain '{domain}' is not instantiated")]
    DomainNotInstantiated { domain: DomainName },
    #[error("domain '{domain}' routing was not resolved for this operation")]
    SnapshotNotResolved { domain: DomainName },
    #[error("stream '{relay}' schema is not instantiated in domain '{domain}'")]
    RelaySchemaNotInstantiated {
        domain: DomainName,
        relay: RelayName,
    },
}

/// The immutable routing state one installed domain publishes to its data-plane tasks.
///
/// Hot Path 06 keeps the schemas and placement descriptors beside the services they describe:
/// deriving them used to require several contended execution-map reads for every batch. Every
/// field that can change together during a schedule apply lives in this one value, so readers
/// observe either the preceding revision or its complete replacement.
#[derive(Clone)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct DomainRoutingSnapshot {
    pub(super) passive_only: bool,
    pub(super) relay_registries: HashMap<RelayName, RelayRegistry>,
    pub(super) relay_schemas: HashMap<RelayName, Arc<CompiledSchema>>,
    pub(super) relay_services: HashMap<RelayName, Arc<RelayBoundaryServices>>,
    pub(super) lookups: HashMap<LookupName, Arc<LookupRuntime>>,
    pub(super) udfs: UdfExecutor,
    pub(super) relay_branchings: HashMap<RelayName, ResolvedBranching>,
    pub(super) materialized_stream_specs: HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    pub(super) materialized_stream_owner_nodes: HashMap<RelayName, Option<ClusterNodeName>>,
    pub(super) codecs: HashMap<CodecName, Arc<CompiledCodec>>,
    pub(super) signaling_protocols: HashMap<SignalingProtocolName, Arc<CompiledSignalingProtocol>>,
    pub(super) processor_plans: HashMap<NodeRef, StdArc<PublishedProcessorPlan>>,
}

pub(crate) type SharedDomainRouting = StdArc<ArcSwap<DomainRoutingSnapshot>>;
pub(crate) type DomainRoutingCache = Cache<SharedDomainRouting, StdArc<DomainRoutingSnapshot>>;

/// Lifecycle-owned staging and publication for one domain's routing state.
///
/// `current` shares the published allocation until lifecycle code first mutates it. `Arc::make_mut`
/// then creates the next complete revision, which remains private until `publish` replaces the
/// ArcSwap value in one operation.
pub(super) struct DomainRouting {
    current: StdArc<DomainRoutingSnapshot>,
    published: SharedDomainRouting,
}

impl DomainRouting {
    pub(super) fn new(snapshot: DomainRoutingSnapshot) -> Self {
        let current = StdArc::new(snapshot);
        let published = StdArc::new(ArcSwap::from(current.clone()));
        Self { current, published }
    }

    pub(super) fn with_publisher(
        snapshot: DomainRoutingSnapshot,
        published: SharedDomainRouting,
    ) -> Self {
        Self {
            current: StdArc::new(snapshot),
            published,
        }
    }

    pub(super) fn shared(&self) -> SharedDomainRouting {
        self.published.clone()
    }

    /// The revision lifecycle code is staging, which is the published revision until it is first
    /// mutated.
    pub(super) fn staged(&self) -> StdArc<DomainRoutingSnapshot> {
        self.current.clone()
    }

    pub(super) fn publish(&mut self) {
        self.published.store(self.current.clone());
    }

    pub(super) fn deactivate(&mut self) {
        self.passive_only = true;
        self.publish();
    }
}

impl std::ops::Deref for DomainRouting {
    type Target = DomainRoutingSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.current
    }
}

impl std::ops::DerefMut for DomainRouting {
    fn deref_mut(&mut self) -> &mut Self::Target {
        StdArc::make_mut(&mut self.current)
    }
}

pub(super) struct DomainExecution {
    pub(super) revision: Arc<ExecutionRevision>,
    pub(super) start_version: u64,
    pub(super) domain_clock: DomainClock,
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) routing: DomainRouting,
    /// Fully bound error routes installed as one revision before failed records can use them.
    pub(super) message_error_plans: Arc<BoundMessageErrorRoutes>,
    pub(super) branched_entrypoints: HashMap<ModelName, Vec<Arc<IngestorRouteRuntime>>>,
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
    pub(super) tasks: Vec<JoinHandle<()>>,
}

impl std::ops::Deref for DomainExecution {
    type Target = DomainRoutingSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.routing
    }
}

impl std::ops::DerefMut for DomainExecution {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.routing
    }
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
    pub(super) plan: LookupResourcePlan,
    pub(super) schema: Arc<CompiledSchema>,
    pub(super) batch: Arc<RuntimeRecordBatch>,
    pub(super) entries: Arc<HashMap<String, usize>>,
    pub(super) metrics: MessageMetricsHandle,
}

#[derive(Debug, Clone)]
pub(super) struct ObservedDomainTick {
    pub(super) generation: u64,
    pub(super) tick_id: u64,
    pub(super) logical_boundary: Timestamp,
    pub(super) authority_utc: Timestamp,
}

#[derive(Debug)]
pub(super) struct RuntimeDomainState {
    pub(super) config: DomainConfig,
    pub(super) status: nervix_models::DomainStatus,
    pub(super) start_version: u64,
    pub(super) last_start: nervix_models::DomainStartPoint,
    pub(super) clock_authority: DomainClockAuthority,
    pub(super) clock: DomainClockLifecycle,
    pub(super) progress: watch::Sender<Option<ObservedDomainTick>>,
}

impl Runtime {
    /// Resolves the stable publication handle for a domain. Data-plane tasks call this once when
    /// they are created and retain a `DomainRoutingCache`; lifecycle and observability callers may
    /// resolve it directly because they are not batch paths.
    pub(crate) fn domain_routing(&self, domain: &DomainName) -> Option<SharedDomainRouting> {
        self.inner
            .domain_routings
            .get(domain)
            .map(|routing| routing.value().clone())
    }

    pub(super) fn stage_domain_routing(
        &self,
        domain: &DomainName,
        snapshot: DomainRoutingSnapshot,
    ) -> DomainRouting {
        match self.domain_routing(domain) {
            Some(published) => DomainRouting::with_publisher(snapshot, published),
            None => DomainRouting::new(snapshot),
        }
    }

    pub(crate) fn domain_routing_cache(&self, domain: &DomainName) -> Option<DomainRoutingCache> {
        self.domain_routing(domain).map(DomainRoutingCache::new)
    }

    pub(crate) fn subscribe_domain_state(&self) -> watch::Receiver<u64> {
        self.inner.domain_status_changed.subscribe()
    }

    /// Waits until this node has installed the cluster's committed domains since it started.
    ///
    /// Until then the node holds no domain, so a domain it lacks may still be one the cluster has,
    /// as right after a restart. From then on, a domain the node lacks is one the committed state
    /// it installed does not hold.
    pub(crate) async fn committed_domains_installed(&self) {
        let mut installations = self.inner.domain_status_changed.subscribe();
        let installed = installations
            .wait_for(|installations| *installations > 0)
            .await;
        installed.assured("the runtime holds the sender of its own domain installations");
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

    /// Installs `domains` as a committed revision would, each with the same assigned test clock
    /// authority, for tests of the runtime and of the edges that observe it.
    #[cfg(test)]
    pub(crate) fn sync_domains(&self, domains: &BTreeMap<DomainName, DomainState>) {
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
                    progress: watch::channel(None).0,
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
                entry.progress.send_replace(None);
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
        revision: Option<Arc<ExecutionRevision>>,
    ) -> Result<(), RuntimeError> {
        if let Some((_, mut existing)) = self.inner.executions.remove(domain) {
            existing.routing.deactivate();
            self.stop_domain_execution(domain, existing).await;
        }

        let Some(revision) = revision else {
            self.withdraw_undeclared_relay_subscriptions(domain, |_| false);
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        };
        let stopped = self
            .inner
            .domains
            .get(domain)
            .is_some_and(|state| matches!(state.status, nervix_models::DomainStatus::Stopped));
        if stopped || !self.inner.domains.contains_key(domain) {
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        }
        let domain_clock =
            self.bind_domain_clock(domain)
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
        self.install_state_identities_from_unplaced_revision(domain, &revision);
        let (shutdown_tx, _) = watch::channel(false);
        let mut relay_builders = HashMap::new();
        let mut relay_branchings = HashMap::new();
        let mut relay_schemas = HashMap::new();
        let mut materialized_stream_specs = HashMap::new();
        let mut materialized_stream_owner_nodes = HashMap::new();
        let mut lookup_specs = Vec::new();
        let mut emitter_specs = Vec::new();
        let mut reingestor_inputs = Vec::new();
        let tasks = Vec::new();
        let mut node_tasks = HashMap::new();
        let mut emitter_tasks = HashMap::new();
        let mut generator_tasks = HashMap::new();
        let branched_specs = &revision.processors;
        let activation_plan = &revision.activation;
        let resource_plans = &revision.resources;
        let udf_executor = self
            .compile_domain_udfs(domain, resource_plans.udfs.clone())
            .await
            .map_err(|error| RuntimeError::CompileDomainUdfs {
                domain: domain.as_str().to_string(),
                report: error,
            })?;
        let entrypoints = &revision.entrypoints;
        let emitter_plans = &revision.emitters;
        let branch_relays = branch_relays_from_plans(branched_specs, entrypoints);
        let ActivatedDomainSurfaces {
            codecs,
            signaling_protocols,
            endpoint_routes,
        } = self
            .activate_domain_surfaces(domain, activation_plan)
            .await?;

        for relay in activation_plan.relays.values() {
            let expiring_state = if branch_relays.contains(&relay.name) {
                let state = self
                    .expiring_stream_state(domain, &relay.name)
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: error.to_string(),
                    })?;
                Some(state)
            } else {
                None
            };
            let fanout = self
                .relay_boundary_fanout_with_capacity(
                    domain,
                    &relay.name,
                    relay.capacity,
                    RelaySubscriptionDefinition::new(relay.schema.clone(), relay.branching.clone()),
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
            lookup_specs.push((lookup.name.clone(), Arc::new(runtime)));
        }

        for emitter in emitter_plans.emitters() {
            let mut inputs = Vec::with_capacity(emitter.inputs.len());
            for input in &emitter.inputs {
                let Some(relay) = relay_builders.get_mut(&input.relay) else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing emitter input relay '{}'", input.relay),
                    });
                };
                inputs.push((
                    input.relay.clone(),
                    relay.runtime_consumer_fan_in_for_mode(emitter.mode),
                ));
            }
            emitter_specs.push((emitter.as_ref().clone(), inputs));
        }
        for plan in entrypoints.reingestors() {
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

        let lookup_runtimes = lookup_specs.iter().cloned().collect::<HashMap<_, _>>();
        let processor_specs = processor_input_specs
            .iter()
            .map(|(spec, _)| spec.clone())
            .collect::<Vec<_>>();
        let previous_processor_plans = HashMap::default();
        let processor_plans = bind_published_processor_plans(
            &processor_specs,
            ProcessorPlanBindingContext {
                runtime: self,
                domain,
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
            reason: format!("failed to bind published processor plans: {reason:#}"),
        })?;
        let message_error_plans = Arc::new(
            BoundMessageErrorRoutes::bind(
                revision.message_errors.clone(),
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
                .verified("the published plan binder returns every planned processor")
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

        let start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        self.install_domain_execution(
            domain,
            DomainExecution {
                revision,
                start_version,
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
                message_error_plans,
                branched_entrypoints,
                endpoint_routes,
                node_tasks,
                emitter_tasks,
                generator_tasks,
                reingestor_tasks,
                placement_tasks: HashMap::default(),
                relay_state_tasks: HashMap::default(),
                relay_owner_tasks,
                tasks,
            },
        );

        Ok(())
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "domain_routing_shuttle_tests.rs"]
mod shuttle_tests;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{
        AckMode, BranchSelection, CodecWireFormat, CreateBranch, CreateCodec, CreateEndpoint,
        CreateGenerator, CreateLookup, CreateReingestor, CreateRelay, CreateSchema,
        CreateSignalingProtocol, CreateVhost, CreateWireSchema, DomainClockState, DomainConfig,
        DomainPace, DomainState, DomainStatus, DomainTick, DomainTimeRate, EndpointType,
        FlushPolicy, JsonType, MaterializedRelayState, MessageErrorPolicy, Model, OutputBranch,
        ParseAsType, ProcessorInputs, ProcessorOutput, ProcessorOutputs, RelayBranching,
        SchemaField, SchemaFingerprint, SignalingProtocolOnConnect, SignalingWireFormat, Timestamp,
        WireSchemaField,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::runtime::domain_clock::DomainClockAccessError;

    #[test]
    fn routing_cache_observes_only_complete_published_revisions() {
        let relay = named::<RelayName>("state");
        let first_owner = named::<ClusterNodeName>("node-a");
        let second_owner = named::<ClusterNodeName>("node-b");
        let mut routing = DomainRouting::new(DomainRoutingSnapshot {
            passive_only: false,
            materialized_stream_owner_nodes: [(relay.clone(), Some(first_owner.clone()))]
                .into_iter()
                .collect(),
            ..DomainRoutingSnapshot::default()
        });
        let mut cache = DomainRoutingCache::new(routing.shared());
        let first_revision = cache.load().clone();

        routing.passive_only = true;
        routing
            .materialized_stream_owner_nodes
            .insert(relay.clone(), Some(second_owner.clone()));

        let staged_revision = cache.load().clone();
        assert!(StdArc::ptr_eq(&first_revision, &staged_revision));
        assert!(!staged_revision.passive_only);
        assert_eq!(
            staged_revision.materialized_stream_owner_nodes.get(&relay),
            Some(&Some(first_owner))
        );

        routing.publish();

        let second_revision = cache.load().clone();
        assert!(!StdArc::ptr_eq(&first_revision, &second_revision));
        assert!(second_revision.passive_only);
        assert_eq!(
            second_revision.materialized_stream_owner_nodes.get(&relay),
            Some(&Some(second_owner))
        );
    }

    #[test]
    fn committed_domains_are_installed_once_the_first_committed_state_is_applied() {
        use futures_util::FutureExt as _;

        let runtime = Runtime::new();
        let mut installed = std::pin::pin!(runtime.committed_domains_installed());
        assert!(
            (&mut installed).now_or_never().is_none(),
            "a node that has installed nothing waits"
        );
        runtime.sync_committed_domains(&BTreeMap::new(), &BTreeMap::new());
        assert!(
            installed.now_or_never().is_some(),
            "installing the committed state releases the wait, even when it holds no domain"
        );
    }

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
                    period: "1s"
                        .parse()
                        .assured("one second is a positive fixture cadence"),
                },
            )
            .expect("the fixture domain exists");

        domains.insert(
            domain("paced"),
            DomainState {
                id: domain("paced"),
                config: DomainConfig {
                    pace: DomainPace::Paced {
                        period: "1s"
                            .parse()
                            .assured("one second is a positive fixture cadence"),
                        skew: "250ms"
                            .parse()
                            .assured("250 milliseconds fits the fixture skew representation"),
                    },
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
                    period: "1s"
                        .parse()
                        .assured("one second is a positive fixture cadence"),
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
                .borrow()
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
        let mapping = DomainClockState::new(Timestamp::now(), logical_origin, DomainTimeRate::ONE);
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

        let Err(report) = result else {
            panic!("execution must reject a paced domain without an installed mapping");
        };
        let RuntimeError::BuildDomainExecution { domain, reason } = report.current_context() else {
            panic!("clock binding must report the failed domain execution");
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
            Timestamp::now(),
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

        let Err(report) = result else {
            panic!("execution must reject a paced clock without its committed authority");
        };
        let RuntimeError::BuildDomainExecution { domain, reason } = report.current_context() else {
            panic!("clock binding must report the failed domain execution");
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

    #[tokio::test]
    async fn passive_execution_installs_planned_surfaces_without_admitting_endpoint_traffic() {
        let runtime = Runtime::new();
        let domain = domain("stopped_surfaces");
        let mut stopped = unpaced_domain_state(domain.as_str());
        stopped.status = DomainStatus::Stopped;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), stopped)]));
        let schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_model(Model::Schema(CreateSchema {
                    name: named("payload"),
                    fields: vec![SchemaField {
                        name: named("value"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    }],
                })),
                scheduled_model(Model::WireJsonSchema(CreateWireSchema {
                    name: named("payload_wire"),
                    strictness: Default::default(),
                    fields: vec![WireSchemaField {
                        name: named("value"),
                        ty: JsonType::String,
                        optional: false,
                    }],
                })),
                scheduled_model(Model::Codec(CreateCodec {
                    name: named("payload_codec"),
                    wire_format: CodecWireFormat::Json {
                        wire_schema: named("payload_wire"),
                    },
                    schema: named("payload"),
                    encoding_rules: Vec::new(),
                })),
                scheduled_model(Model::Relay(CreateRelay {
                    name: named("events"),
                    schema: named("payload"),
                    buffer: nonzero!(7usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: Some(MaterializedRelayState::LastByTimestamp),
                })),
                scheduled_model(Model::Vhost(CreateVhost {
                    name: named("edge"),
                    hostnames: vec!["EVENTS.EXAMPLE.COM".to_string()],
                    tls: None,
                })),
                scheduled_model(Model::SignalingProtocol(CreateSignalingProtocol {
                    name: named("handshake"),
                    format: SignalingWireFormat::Json,
                    on_connect: SignalingProtocolOnConnect {
                        accept_data: true,
                        steps: Vec::new(),
                        fail_matchers: Vec::new(),
                        timeout: "1s".to_string(),
                    },
                })),
                scheduled_model(Model::Endpoint(CreateEndpoint {
                    name: named("receive"),
                    on_vhost: named("edge"),
                    path: "/ingest".to_string(),
                    endpoint_type: EndpointType::Websockets,
                    signaling_protocol: Some(named("handshake")),
                })),
            ],
            Vec::new(),
        );

        let mut execution = runtime
            .build_passive_execution_from_schedule(&domain, &schedule)
            .await
            .expect("a stopped domain can install its planned surfaces");
        assert!(execution.passive_only);
        assert!(execution.codecs.contains_key("payload_codec"));
        assert!(execution.relay_schemas.contains_key("events"));
        assert!(execution.materialized_stream_specs.contains_key("events"));
        assert!(execution.signaling_protocols.contains_key("handshake"));
        assert_eq!(
            execution.endpoint_routes["receive"].hostnames,
            vec!["events.example.com".to_string()]
        );
        runtime.publish_routed_endpoints(&domain, &execution);
        assert!(runtime.inner.routed_endpoints.is_empty());

        execution.routing.passive_only = false;
        runtime.publish_routed_endpoints(&domain, &execution);
        assert_eq!(runtime.inner.routed_endpoints.len(), 1);
        execution.routing.deactivate();
        runtime.withdraw_routed_endpoints(&domain, &execution);
        assert!(runtime.inner.routed_endpoints.is_empty());
    }

    fn tenant_schema(name: &str) -> CreateSchema {
        CreateSchema {
            name: named(name),
            fields: vec![SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        }
    }

    fn tenant_relay(name: &str, branching: RelayBranching) -> Model {
        Model::Relay(CreateRelay {
            name: named(name),
            schema: named("payload"),
            buffer: nonzero!(4usize),
            branching,
            materialized_state: None,
        })
    }

    /// A reingestor copying `incoming` into `outgoing`, whose route leaves its records unbranched.
    fn repartitioning_reingestor() -> Model {
        Model::Reingestor(CreateReingestor {
            name: named("repartition"),
            from: ProcessorInputs::single(named("incoming")),
            output_routes: with_inherit_all(ProcessorOutputs::single(named("outgoing")))
                .with_flush_policy(FlushPolicy::Immediate)
                .with_branch(OutputBranch::Unbranched),
            mode: AckMode::Attached,
            materialized_state: Vec::new(),
            filter_where: None,
        })
    }

    #[tokio::test]
    async fn a_graph_build_starts_each_planned_reingestor_input() {
        let runtime = Runtime::new();
        let domain = domain("graph_reingestor");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let models = [
            Model::Schema(tenant_schema("payload")),
            tenant_relay("incoming", RelayBranching::unbranched()),
            tenant_relay("outgoing", RelayBranching::unbranched()),
            repartitioning_reingestor(),
        ];
        let graph = ActiveGraph::from_scheduled_models(&DomainSchedule::new(
            domain.clone(),
            models.into_iter().map(scheduled_model),
            Vec::new(),
        ))
        .assured("two relays and the reingestor between them form a graph");

        runtime
            .rebuild_domain_execution(
                &domain,
                Some(
                    ExecutionRevision::from_graph(&domain, &graph)
                        .assured("the fixture graph has complete execution plans"),
                ),
            )
            .await
            .assured("a valid graph builds its domain execution");

        {
            let execution = runtime
                .inner
                .executions
                .get(&domain)
                .assured("the build installs the execution");
            let repartition = named::<ModelName>("repartition");
            assert!(
                execution
                    .revision
                    .entrypoints
                    .reingestor(&named("repartition"))
                    .is_some()
            );
            assert_eq!(execution.branched_entrypoints[&repartition].len(), 1);
            assert_eq!(
                execution.reingestor_tasks[&NodeRef::new(ModelKind::Reingestor, repartition)].len(),
                1
            );
        }
        runtime
            .rebuild_domain_execution(&domain, None)
            .await
            .assured("removing the graph stops the execution");
        assert!(!runtime.inner.executions.contains_key(&domain));
    }

    #[tokio::test]
    async fn an_unplaced_graph_binds_a_generator_from_its_materialized_source_plan() {
        let runtime = Runtime::new();
        let domain = domain("graph_generator");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let Model::Relay(mut source) = tenant_relay("notifications", RelayBranching::unbranched())
        else {
            unreachable!("the relay fixture returns a relay");
        };
        source.materialized_state = Some(MaterializedRelayState::LastByTimestamp);
        let generator = Model::Generator(CreateGenerator {
            name: named("synth"),
            materialized_relay: source.name.clone(),
            branched_by: BranchSelection::unbranched(),
            each: "100ms".parse().assured("the fixture cadence is positive"),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                relay: named("generated"),
                construction: nervix_nspl::parse_route_construction(
                    "SET tenant = relay_state.notifications.tenant",
                )
                .assured("the fixture route is valid NSPL"),
                flush_policy: Some(FlushPolicy::Immediate),
                message_error_policy: MessageErrorPolicy::Log,
                branch: None,
            }]),
        });
        let graph = ActiveGraph::from_scheduled_models(&DomainSchedule::new(
            domain.clone(),
            [
                Model::Schema(tenant_schema("payload")),
                Model::Relay(source),
                tenant_relay("generated", RelayBranching::unbranched()),
                generator,
            ]
            .into_iter()
            .map(scheduled_model),
            Vec::new(),
        ))
        .assured("the generator references the materialized source and output relay");

        runtime
            .rebuild_domain_execution(
                &domain,
                Some(
                    ExecutionRevision::from_graph(&domain, &graph)
                        .assured("the fixture graph has complete execution plans"),
                ),
            )
            .await
            .assured("the generator route binds from its planned source and output schemas");
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .assured("the running domain has an execution");
        assert!(execution.generator_tasks.contains_key(&NodeRef::new(
            ModelKind::Generator,
            named::<GeneratorName>("synth")
        )));
        drop(execution);
        runtime
            .rebuild_domain_execution(&domain, None)
            .await
            .assured("removing the graph stops its generator task");
    }

    #[tokio::test]
    async fn an_unplaced_lookup_graph_reports_a_missing_resource_store_before_activation() {
        let runtime = Runtime::new();
        let domain = domain("graph_lookup");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let graph = ActiveGraph::from_scheduled_models(&DomainSchedule::new(
            domain.clone(),
            [
                Model::Schema(tenant_schema("payload")),
                Model::WireJsonSchema(CreateWireSchema {
                    name: named("payload_wire"),
                    strictness: Default::default(),
                    fields: vec![WireSchemaField {
                        name: named("tenant"),
                        ty: JsonType::String,
                        optional: false,
                    }],
                }),
                Model::Codec(CreateCodec {
                    name: named("payload_codec"),
                    wire_format: CodecWireFormat::Json {
                        wire_schema: named("payload_wire"),
                    },
                    schema: named("payload"),
                    encoding_rules: Vec::new(),
                }),
                Model::Lookup(CreateLookup {
                    name: named("tenants"),
                    key_field: named("tenant"),
                    resource: named("tenant_bundle"),
                    resource_version: 3,
                    path: "tenants.jsonl".to_string(),
                    decode_using_codec: named("payload_codec"),
                }),
            ]
            .into_iter()
            .map(scheduled_model),
            Vec::new(),
        ))
        .assured("the lookup codec and key are declared");

        let error = runtime
            .rebuild_domain_execution(
                &domain,
                Some(
                    ExecutionRevision::from_graph(&domain, &graph)
                        .assured("the fixture graph has complete execution plans"),
                ),
            )
            .await
            .expect_err("a lookup cannot load before its resource store is attached");
        assert!(error.to_string().contains("resource store is not attached"));
        assert!(!runtime.inner.executions.contains_key(&domain));
    }

    #[tokio::test]
    async fn a_build_rejects_a_route_its_relay_is_not_branched_for() {
        let domain = domain("branch_mismatch");
        let branched = ScheduledNode::new(
            tenant_relay("outgoing", RelayBranching::branched_by(named("by_tenant"))),
            SchemaFingerprint::from_digest([1; 32]),
        )
        .with_resolved_branching(Some(ResolvedBranching::branched(
            named("by_tenant"),
            tenant_schema("tenant_key"),
        )));
        let schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_model(Model::Schema(tenant_schema("payload"))),
                scheduled_model(Model::Schema(tenant_schema("tenant_key"))),
                scheduled_model(Model::Branch(CreateBranch {
                    name: named("by_tenant"),
                    schema: named("tenant_key"),
                    ttl: "5m".to_string(),
                    eviction: None,
                })),
                scheduled_model(tenant_relay("incoming", RelayBranching::unbranched())),
                branched,
                scheduled_model(repartitioning_reingestor()),
            ],
            Vec::new(),
        );

        let error = ExecutionRevision::from_schedule(&schedule)
            .err()
            .assured("an unbranched route to a branched relay does not plan");

        assert!(matches!(
            error.current_context(),
            crate::registry::ExecutionRevisionError::Entrypoints { domain: failed }
                if failed == &domain
        ));
        assert!(format!("{error:#}").contains("outgoing"));
    }
}
