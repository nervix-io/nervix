use nervix_models::CreateUdf;
use rdkafka::consumer::StreamConsumer;

use super::{schedule_delta::ScheduleDelta, *};

struct ScheduledIngestorStartSpec {
    domain: Domain,
    source_model: Model,
    ingestor: CreateIngestor,
    kafka_offset_state: Option<Arc<ReplicatedKafkaOffsetState>>,
}

#[derive(Clone, Copy)]
struct ReingestorDispatchContext<'a> {
    domain: &'a Domain,
    reingestor: &'a Identifier,
    from_relay: &'a Identifier,
    from_where: Option<&'a nervix_models::Expression>,
    mode: AckMode,
    error_policies: &'a ErrorPolicies,
    branched_senders: &'a HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
}

#[derive(Clone, Copy)]
enum ReingestorOutputFlush {
    Due(Timestamp),
    All,
}

struct ReingestorOutputQuiesceGauge {
    counters: Arc<NodeQuiesceCounters>,
    output_buffers: usize,
}

impl ReingestorOutputQuiesceGauge {
    fn new(counters: Arc<NodeQuiesceCounters>) -> Self {
        Self {
            counters,
            output_buffers: 0,
        }
    }

    fn observe(&mut self, outputs: &RelayProcessorOutputsNode) {
        let output_buffers = outputs
            .routes
            .iter()
            .map(|output| output.pending.len())
            .fold(0usize, usize::saturating_add);
        if output_buffers > self.output_buffers {
            self.counters
                .output_buffers
                .fetch_add(output_buffers - self.output_buffers, Ordering::AcqRel);
        } else if output_buffers < self.output_buffers {
            self.counters
                .output_buffers
                .fetch_sub(self.output_buffers - output_buffers, Ordering::AcqRel);
        }
        self.output_buffers = output_buffers;
    }
}

impl Drop for ReingestorOutputQuiesceGauge {
    fn drop(&mut self) {
        self.counters
            .output_buffers
            .fetch_sub(self.output_buffers, Ordering::AcqRel);
    }
}

fn branch_relays_from_branched_specs(specs: &BranchedNodeSpecs) -> HashSet<Identifier> {
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

fn relay_branching_schema_for_runtime(
    domain: &Domain,
    relay_identifier: &Identifier,
    relay: &CreateRelay,
    effective_branching_schema: Option<&Identifier>,
    schemas: &HashMap<Identifier, Arc<CompiledSchema>>,
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

#[derive(Debug)]
struct ProtobufDescriptorCompileConfig {
    files: Vec<String>,
    includes: Vec<String>,
}

impl ProtobufDescriptorCompileConfig {
    fn from_entries(entries: &[ClientConfigEntry]) -> Result<Self, String> {
        let mut files = Vec::new();
        let mut includes = Vec::new();
        for entry in entries {
            match entry.key.to_ascii_lowercase().as_str() {
                "file" | "files" => Self::append_paths(&mut files, &entry.value),
                "include" | "includes" => Self::append_paths(&mut includes, &entry.value),
                other => {
                    return Err(format!(
                        "unsupported protobuf config key '{other}'; expected 'file', 'files', \
                         'include', or 'includes'"
                    ));
                }
            }
        }
        Ok(Self { files, includes })
    }

    fn append_paths(paths: &mut Vec<String>, value: &str) {
        paths.extend(
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_string),
        );
    }

    fn compile_descriptor_set(
        self,
        store: &ResourceStore,
        id: &ResourceId,
    ) -> Result<prost_types::FileDescriptorSet, String> {
        let files = if self.files.is_empty() {
            Self::collect_resource_proto_files(store, id)?
        } else {
            self.files
                .iter()
                .map(|path| {
                    store
                        .resolve_content_path(id, path)
                        .map_err(|error| format!("invalid protobuf source path '{path}': {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        if files.is_empty() {
            return Err(format!(
                "protobuf resource '{}' version {} contains no .proto files",
                id.identifier.as_str(),
                id.version
            ));
        }
        let includes = if self.includes.is_empty() {
            vec![store.content_root(id)]
        } else {
            self.includes
                .iter()
                .map(|path| {
                    store
                        .resolve_content_path(id, path)
                        .map_err(|error| format!("invalid protobuf include path '{path}': {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        protox::compile(files, includes)
            .map_err(|error| format!("failed to compile protobuf descriptors: {error}"))
    }

    fn collect_resource_proto_files(
        store: &ResourceStore,
        id: &ResourceId,
    ) -> Result<Vec<PathBuf>, String> {
        let root = store.content_root(id);
        let mut files = BTreeSet::new();
        Self::collect_proto_files_recursive(&root, &mut files)?;
        Ok(files.into_iter().collect())
    }

    fn collect_proto_files_recursive(
        directory: &PathBuf,
        files: &mut BTreeSet<PathBuf>,
    ) -> Result<(), String> {
        let entries = std::fs::read_dir(directory).map_err(|error| {
            format!(
                "failed to read protobuf resource directory '{}': {error}",
                directory.display()
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                format!(
                    "failed to read protobuf resource directory entry '{}': {error}",
                    directory.display()
                )
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|error| {
                format!(
                    "failed to inspect protobuf resource path '{}': {error}",
                    path.display()
                )
            })?;
            if file_type.is_dir() {
                Self::collect_proto_files_recursive(&path, files)?;
            } else if file_type.is_file()
                && path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| extension == "proto")
            {
                files.insert(path);
            }
        }
        Ok(())
    }
}

impl Runtime {
    async fn compile_domain_codec(
        &self,
        domain: &Domain,
        codec: &CreateCodec,
        schema: Arc<CompiledSchema>,
        wire_schema: Option<&WireSchemaDefinition>,
    ) -> Result<Arc<CompiledCodec>, RuntimeError> {
        let protobuf_descriptor = if let CodecWireFormat::Protobuf(config) = &codec.wire_format {
            let build_error = |reason: String| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            };
            let pool = self
                .compile_protobuf_descriptor_pool(
                    domain,
                    &config.resource,
                    config.resource_version,
                    &config.config,
                )
                .await
                .map_err(build_error)?;
            Some(pool.message(&config.message).map_err(build_error)?)
        } else {
            None
        };

        compile_codec_with_protobuf(codec, schema, wire_schema, protobuf_descriptor).map_err(
            |err| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: err.to_string(),
            },
        )
    }

    async fn compile_signaling_protocol(
        &self,
        domain: &Domain,
        protocol: &CreateSignalingProtocol,
    ) -> Result<Arc<CompiledSignalingProtocol>, RuntimeError> {
        let build_error = |reason: String| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason,
        };
        let descriptors = if let SignalingWireFormat::Protobuf(config) = &protocol.format {
            let pool = self
                .compile_protobuf_descriptor_pool(
                    domain,
                    &config.resource,
                    config.resource_version,
                    &config.config,
                )
                .await
                .map_err(build_error)?;
            Some(SignalingProtobufDescriptors {
                send: pool.message(&config.send_message).map_err(build_error)?,
                wait: pool.message(&config.wait_message).map_err(build_error)?,
            })
        } else {
            None
        };

        CompiledSignalingProtocol::compile(protocol, descriptors)
            .map(Arc::new)
            .map_err(|error| build_error(error.to_string()))
    }

    async fn compile_protobuf_descriptor_pool(
        &self,
        domain: &Domain,
        resource: &Identifier,
        resource_version: Option<u64>,
        config: &[ClientConfigEntry],
    ) -> Result<ProtobufDescriptorPool, String> {
        let store =
            self.resource_store.read().clone().ok_or_else(|| {
                "protobuf descriptors require an attached resource store".to_string()
            })?;
        let id = self.resolve_resource_id(domain, resource, resource_version, resource.as_str())?;
        let compile_config = ProtobufDescriptorCompileConfig::from_entries(config)?;
        let file_descriptor_set =
            tokio::task::spawn_blocking(move || compile_config.compile_descriptor_set(&store, &id))
                .await
                .map_err(|error| {
                    format!("failed to join protobuf descriptor compilation task: {error}")
                })??;

        ProtobufDescriptorPool::from_file_descriptor_set(file_descriptor_set)
    }

    pub(in crate::runtime) fn emitter_task_deps(
        &self,
        deps: ExecutionBuildDeps<'_>,
        emitter: &CreateEmitter,
    ) -> Result<EmitterTaskDeps, RuntimeError> {
        let Some(input_relay) = emitter.from.first() else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: deps.domain.as_str().to_string(),
                reason: format!("emitter '{}' has no input relay", emitter.name.as_str()),
            });
        };
        let Some(input_schema) = deps.relay_schemas.get(input_relay).cloned() else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: deps.domain.as_str().to_string(),
                reason: format!(
                    "missing emitter input relay schema '{}'",
                    input_relay.as_str()
                ),
            });
        };
        let Some(input_branching) = deps.relay_branchings.get(input_relay).cloned() else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: deps.domain.as_str().to_string(),
                reason: format!(
                    "missing emitter input relay branching '{}'",
                    input_relay.as_str()
                ),
            });
        };
        Ok(EmitterTaskDeps {
            input_schema,
            input_branching,
            materialized_relay_specs: deps.materialized_relay_specs.clone(),
            materialized_relay_owner_nodes: deps.materialized_relay_owner_nodes.clone(),
            lookups: deps.lookups.clone(),
        })
    }

    pub fn new() -> Self {
        Self::with_test_hooks(RuntimeTestHooks::default())
    }

    pub fn with_test_hooks(hooks: RuntimeTestHooks) -> Self {
        Self::with_persistence(None, DEFAULT_STATE_SNAPSHOT_INTERVAL, hooks)
            .expect("runtime without persistence should initialize")
    }

    pub fn with_persistence(
        db: Option<Database>,
        state_snapshot_interval: Duration,
        hooks: RuntimeTestHooks,
    ) -> Result<Self, RuntimePersistenceError> {
        Self::with_persistence_and_temp_dir(
            db,
            state_snapshot_interval,
            hooks,
            PathBuf::from(DEFAULT_TEMP_DIR),
        )
    }

    pub fn with_persistence_and_temp_dir(
        db: Option<Database>,
        state_snapshot_interval: Duration,
        hooks: RuntimeTestHooks,
        temp_dir: PathBuf,
    ) -> Result<Self, RuntimePersistenceError> {
        let (events, _) = broadcast::channel(256);
        let (domain_status_changed, _) = watch::channel(0);
        let state_store = db
            .map(RuntimeStateStore::from_database)
            .transpose()?
            .map(Arc::new);
        Ok(Self {
            ingestors: Arc::new(DashMap::default()),
            ingestor_quiescence: Arc::new(DashMap::default()),
            ingestors_paused_for_memory_pressure: Arc::new(AtomicBool::new(false)),
            ingestor_transient_errors: Arc::new(DashMap::default()),
            ingestor_reconnect_backoffs: Arc::new(DashMap::default()),
            ingestor_readiness: Arc::new(DashMap::default()),
            emitter_transient_errors: Arc::new(DashMap::default()),
            emitter_retry_statuses: Arc::new(DashMap::default()),
            emitter_confirmation_waits: Arc::new(DashMap::default()),
            executions: Arc::new(DashMap::default()),
            message_error_routes: Arc::new(DashMap::default()),
            compiled_domain_udfs: Arc::new(DashMap::default()),
            schedule_apply_lock: Arc::new(Mutex::new(())),
            applied_cluster_revision: Arc::new(AtomicU64::new(u64::MAX)),
            domain_instantiation_errors: Arc::new(DashMap::default()),
            domains: Arc::new(DashMap::default()),
            domain_status_changed,
            in_flight_by_domain: Arc::new(DashMap::default()),
            generator_activity_by_domain: Arc::new(DashMap::default()),
            emitter_buffers: Arc::new(DashMap::default()),
            force_flush_by_domain: Arc::new(DashMap::default()),
            node_quiesce_counters: Arc::new(DashMap::default()),
            entity_gate_holds: Arc::new(DashMap::default()),
            active_domain_alters: Arc::new(DashMap::default()),
            state_schema_fingerprints: Arc::new(DashMap::default()),
            domain_graphs: Arc::new(DashMap::default()),
            endpoint_bindings: Arc::new(DashMap::default()),
            relay_boundary_fanouts: Arc::new(DashMap::default()),
            events,
            emitter_faults: hooks.emitter_faults,
            ingestor_faults: hooks.ingestor_faults,
            otel_client_faults: hooks.otel_client_faults,
            #[cfg(feature = "testing")]
            schedule_publication_faults: hooks.schedule_publication_faults,
            #[cfg(feature = "testing")]
            transaction_binding_drops: hooks.transaction_binding_drops,
            transaction_commit_pauses: hooks.transaction_commit_pauses,
            #[cfg(feature = "testing")]
            entity_gate_pauses: hooks.entity_gate_pauses,
            resource_store: Arc::new(RwLock::new(None)),
            resource_versions: Arc::new(RwLock::new(ResourceVersionStatus::default())),
            remote_dispatcher: Arc::new(RwLock::new(None)),
            local_node_id: Arc::new(RwLock::new(None)),
            next_remote_ack_id: Arc::new(AtomicU64::new(1)),
            pending_remote_acks: Arc::new(DashMap::default()),
            next_state_sync_correlation_id: Arc::new(AtomicU64::new(1)),
            pending_state_syncs: Arc::new(DashMap::default()),
            expiring_stream_states: Arc::new(DashMap::default()),
            latest_resource_versions: Arc::new(DashMap::default()),
            replicated_deduplicator_states: Arc::new(DashMap::default()),
            replicated_kafka_offset_states: Arc::new(DashMap::default()),
            replicated_materialized_stream_states: Arc::new(DashMap::default()),
            materializer_epochs: Arc::new(DashMap::default()),
            materialized_state_changed: Arc::new(Notify::new()),
            replicated_window_processor_states: Arc::new(DashMap::default()),
            replicated_wasm_processor_states: Arc::new(DashMap::default()),
            replicated_branch_aggregated_states: Arc::new(DashMap::default()),
            wasm_runtime: Arc::new(
                WasmRuntime::new(WasmRuntimeConfig::default())
                    .expect("wasm runtime should initialize"),
            ),
            branch_instance_expiration_scan_interval: hooks
                .branch_instance_expiration_scan_interval
                .unwrap_or(BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL),
            state_store,
            state_snapshot_interval,
            state_replication_poll_interval: DEFAULT_STATE_REPLICATION_POLL_INTERVAL,
            domain_drain_timeout: hooks
                .domain_drain_timeout
                .unwrap_or(DEFAULT_DOMAIN_DRAIN_TIMEOUT),
            entity_gate_deadline: hooks
                .entity_gate_deadline
                .unwrap_or(DEFAULT_DOMAIN_DRAIN_TIMEOUT),
            temp_dir: Arc::new(temp_dir),
            metrics: RuntimeMetrics::default(),
        })
    }

    pub fn metrics(&self) -> RuntimeMetrics {
        self.metrics.clone()
    }

    pub fn domain_drain_timeout(&self) -> Duration {
        self.domain_drain_timeout
    }

    pub fn entity_gate_deadline(&self) -> Duration {
        self.entity_gate_deadline
    }

    pub fn entity_pause_relays(
        &self,
        domain: &Domain,
        affected_entities: &[RegistryEntity],
    ) -> Vec<Identifier> {
        let Some(execution) = self.executions.get(domain) else {
            return Vec::new();
        };
        Self::entity_pause_relays_for_schedule(&execution.schedule, affected_entities)
    }

    pub(in crate::runtime) fn entity_pause_relays_for_schedule(
        schedule: &DomainSchedule,
        affected_entities: &[RegistryEntity],
    ) -> Vec<Identifier> {
        let processor_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        let mut relays = affected_entities
            .iter()
            .flat_map(|entity| {
                if entity.kind == ModelKind::Relay {
                    return vec![entity.identifier.clone()];
                }
                if let Some(processor) = processor_specs.processor(entity.kind, &entity.identifier)
                {
                    return processor.spec.input_relays.clone();
                }
                schedule
                    .nodes
                    .iter()
                    .find(|node| node.kind == entity.kind && node.identifier == entity.identifier)
                    .and_then(|node| match node.config.as_ref() {
                        Model::Emitter(emitter) => Some(emitter.from.from.clone()),
                        Model::Reingestor(reingestor) => Some(reingestor.from.from.clone()),
                        Model::Generator(generator) => {
                            Some(vec![generator.materialized_relay.clone()])
                        }
                        _ => None,
                    })
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        relays.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        relays.dedup();
        relays
    }

    pub fn engage_entity_gates(
        &self,
        domain: &Domain,
        relays: &[Identifier],
        deadline: Instant,
        reason: &str,
    ) -> EntityGateHold {
        let mut gates = Vec::with_capacity(relays.len());
        for relay in relays {
            let key = (domain.clone(), relay.clone());
            let Some(fanout) = self.relay_boundary_fanouts.get(&key) else {
                continue;
            };
            let gate = fanout.dispatch_gate();
            gates.push(RelayDispatchGateLease::engage(gate, deadline, reason));
        }
        EntityGateHold { gates }
    }

    pub async fn engage_entity_gate_operation(
        &self,
        operation_id: u64,
        domain: &Domain,
        relays: &[Identifier],
        affected_entities: &[RegistryEntity],
        deadline: Instant,
        reason: &str,
    ) -> Result<(), String> {
        let hold_key = (domain.clone(), operation_id);
        if self.entity_gate_holds.contains_key(&hold_key) {
            return Ok(());
        }
        let mut gates = self.engage_entity_gates(domain, relays, deadline, reason);
        if !gates.wait_quiescent().await {
            gates.release();
            return Err(format!(
                "relay dispatch gate fence for domain '{}' did not complete before its deadline",
                domain.as_str()
            ));
        }
        if self.entity_gate_holds.contains_key(&hold_key) {
            gates.release();
            return Ok(());
        }
        self.entity_gate_holds.insert(
            hold_key.clone(),
            EntityAlterHold {
                gates,
                quiesced_ingestors: Vec::new(),
            },
        );
        let ingestors = affected_entities
            .iter()
            .filter(|entity| entity.kind == ModelKind::Ingestor)
            .map(|entity| entity.identifier.clone())
            .collect::<Vec<_>>();
        for ingestor in &ingestors {
            tokio::task::consume_budget().await;
            let key = RuntimeKey::new(domain.clone(), ingestor.clone());
            if !self.ingestors.contains_key(&key) {
                continue;
            }
            if self.engage_ingestor_quiesce(domain, ingestor, IngestorQuiesceCause::EntityHold)
                && let Some(mut hold) = self.entity_gate_holds.get_mut(&hold_key)
            {
                hold.quiesced_ingestors.push(ingestor.clone());
            }
        }
        self.force_flush_domain(domain);
        Ok(())
    }

    pub async fn release_entity_gate_operation(
        &self,
        operation_id: u64,
        domain: &Domain,
    ) -> Result<(), String> {
        let hold_key = (domain.clone(), operation_id);
        let Some(quiesced_ingestors) = self
            .entity_gate_holds
            .get(&hold_key)
            .map(|hold| hold.quiesced_ingestors.clone())
        else {
            return Ok(());
        };
        for ingestor in &quiesced_ingestors {
            tokio::task::consume_budget().await;
            self.release_ingestor_quiesce(domain, ingestor, IngestorQuiesceCause::EntityHold);
            if !self
                .ingestors
                .contains_key(&RuntimeKey::new(domain.clone(), ingestor.clone()))
            {
                self.remove_ingestor_quiescence(domain, ingestor);
            }
        }
        if let Some((_, hold)) = self.entity_gate_holds.remove(&hold_key) {
            hold.gates.release();
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn entity_gate_operation_is_held(&self, operation_id: u64, domain: &Domain) -> bool {
        self.entity_gate_holds
            .contains_key(&(domain.clone(), operation_id))
    }

    pub fn entity_drain_status(
        &self,
        domain: &Domain,
        relays: &[Identifier],
        affected_entities: &[RegistryEntity],
    ) -> EntityDrainStatus {
        let buffered_relay_batches = relays
            .iter()
            .filter_map(|relay| {
                self.relay_boundary_fanouts
                    .get(&(domain.clone(), relay.clone()))
                    .map(|fanout| fanout.runtime_consumer_buffer_len())
            })
            .sum();
        let node_work_items = affected_entities
            .iter()
            .map(|entity| {
                let quiesce_work = self
                    .node_quiesce_counters
                    .get(&RuntimeKey::new(domain.clone(), entity.identifier.clone()))
                    .map(|counters| counters.outstanding_work())
                    .unwrap_or(0);
                let emitter_work = if entity.kind == ModelKind::Emitter {
                    self.emitter_buffers
                        .get(&RuntimeKey::new(domain.clone(), entity.identifier.clone()))
                        .map(|buffered| buffered.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };
                quiesce_work.saturating_add(emitter_work)
            })
            .sum();
        let mut emitter_publishing = affected_entities
            .iter()
            .filter(|entity| entity.kind == ModelKind::Emitter)
            .filter_map(|entity| {
                self.emitter_publishing_drain_status(&RuntimeKey::new(
                    domain.clone(),
                    entity.identifier.clone(),
                ))
            })
            .collect::<Vec<_>>();
        emitter_publishing.sort_by(|left, right| left.emitter.cmp(&right.emitter));
        EntityDrainStatus {
            buffered_relay_batches,
            node_work_items,
            emitter_publishing,
        }
    }

    fn emitter_publishing_drain_status(
        &self,
        key: &RuntimeKey,
    ) -> Option<EmitterPublishingDrainStatus> {
        let pending_messages = self
            .emitter_buffers
            .get(key)
            .map(|buffered| buffered.load(Ordering::Acquire))
            .unwrap_or(0);
        let awaiting_confirmation = self
            .emitter_confirmation_waits
            .get(key)
            .is_some_and(|waits| waits.load(Ordering::Acquire) > 0);
        if awaiting_confirmation {
            return Some(EmitterPublishingDrainStatus {
                emitter: key.identifier.clone(),
                state: EmitterPublishingDrainState::AwaitingConfirmation,
                pending_messages,
                retry_backoff: None,
                retry_wait: None,
            });
        }
        let retry = self.emitter_retry_statuses.get(key)?;
        let state = match retry.kind {
            EmitterRetryKind::Infrastructure => EmitterPublishingDrainState::RetryingInfrastructure,
            EmitterRetryKind::IcebergCommit => EmitterPublishingDrainState::RetryingIcebergCommit,
        };
        Some(EmitterPublishingDrainStatus {
            emitter: key.identifier.clone(),
            state,
            pending_messages,
            retry_backoff: Some(retry.reconnect.backoff),
            retry_wait: Some(
                retry
                    .reconnect
                    .retry_at
                    .saturating_duration_since(Instant::now()),
            ),
        })
    }

    pub(super) fn node_quiesce_counters(
        &self,
        domain: &Domain,
        node: &Identifier,
    ) -> Arc<NodeQuiesceCounters> {
        self.node_quiesce_counters
            .entry(RuntimeKey::new(domain.clone(), node.clone()))
            .or_insert_with(|| Arc::new(NodeQuiesceCounters::default()))
            .clone()
    }

    pub(super) fn materializer_epoch(&self, domain: &Domain) -> Arc<AtomicU64> {
        self.materializer_epochs
            .entry(domain.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    fn bump_materializer_epoch(&self, domain: &Domain) {
        self.materializer_epoch(domain)
            .fetch_add(1, Ordering::AcqRel);
    }

    fn purge_materialized_relay_state(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<(), RuntimeError> {
        let placements = self
            .replicated_materialized_stream_states
            .iter()
            .filter(|entry| {
                entry.key().domain == *domain
                    && entry.key().kind == ModelKind::Materializer
                    && entry.key().identifier == *relay
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for placement in placements {
            self.replicated_materialized_stream_states
                .remove(&placement);
        }
        if let Some(store) = &self.state_store {
            store
                .purge_entity(
                    domain,
                    RuntimeStateKind::MaterializedRelay,
                    ModelKind::Materializer,
                    relay,
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "failed to purge materialized state for relay '{}': {error}",
                        relay.as_str()
                    ),
                })?;
        }
        Ok(())
    }

    fn purge_deduplicator_state(
        &self,
        domain: &Domain,
        deduplicator: &Identifier,
    ) -> Result<(), RuntimeError> {
        let placements = self
            .replicated_deduplicator_states
            .iter()
            .filter(|entry| {
                entry.key().domain == *domain
                    && entry.key().kind == ModelKind::Deduplicator
                    && entry.key().identifier == *deduplicator
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for placement in placements {
            self.replicated_deduplicator_states.remove(&placement);
        }
        if let Some(store) = &self.state_store {
            store
                .purge_entity(
                    domain,
                    RuntimeStateKind::Deduplicator,
                    ModelKind::Deduplicator,
                    deduplicator,
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "failed to purge state for deduplicator '{}': {error}",
                        deduplicator.as_str()
                    ),
                })?;
        }
        Ok(())
    }

    pub(crate) fn try_begin_domain_alter(&self, domain: &Domain) -> Option<DomainAlterGuard> {
        match self.active_domain_alters.entry(domain.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => None,
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(ActiveDomainAlter);
                Some(DomainAlterGuard {
                    domain: domain.clone(),
                    active_domain_alters: self.active_domain_alters.clone(),
                })
            }
        }
    }

    pub fn domain_alter_is_active(&self, domain: &Domain) -> bool {
        self.active_domain_alters.contains_key(domain)
    }

    #[cfg(feature = "testing")]
    pub fn take_armed_schedule_publication_fault(&self, domain: &Domain) -> bool {
        self.schedule_publication_faults.take_armed_fault(domain)
    }

    #[cfg(feature = "testing")]
    pub fn take_armed_transaction_binding_drop(&self, node_id: &str) -> bool {
        self.transaction_binding_drops.take(node_id)
    }

    #[cfg(feature = "testing")]
    pub async fn pause_transaction_commit_after_progress_if_armed(
        &self,
        node_id: &str,
        completed_statements: usize,
    ) {
        self.transaction_commit_pauses
            .pause_if_armed(node_id, completed_statements)
            .await;
    }

    #[cfg(feature = "testing")]
    pub async fn pause_entity_gate_if_armed(&self, domain: &Domain) {
        self.entity_gate_pauses.pause_if_armed(domain).await;
    }

    pub(in crate::runtime) fn record_ingestor_transient_error(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        error: impl Into<String>,
    ) {
        self.ingestor_transient_errors.insert(
            RuntimeKey::new(domain.clone(), ingestor.clone()),
            error.into(),
        );
    }

    pub(in crate::runtime) fn record_ingestor_transient_error_with_backoff(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        error: impl Into<String>,
        backoff: Duration,
    ) {
        let key = RuntimeKey::new(domain.clone(), ingestor.clone());
        self.ingestor_transient_errors
            .insert(key.clone(), error.into());
        self.ingestor_reconnect_backoffs.insert(
            key,
            RuntimeReconnectStatus {
                backoff,
                retry_at: Instant::now() + backoff,
            },
        );
    }

    pub(in crate::runtime) fn clear_ingestor_transient_error(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
    ) {
        self.ingestor_transient_errors
            .remove(&RuntimeKey::new(domain.clone(), ingestor.clone()));
        self.ingestor_reconnect_backoffs
            .remove(&RuntimeKey::new(domain.clone(), ingestor.clone()));
    }

    pub(in crate::runtime) fn prepare_ingestor_readiness(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        expected_instances: u64,
    ) {
        self.ingestor_readiness.insert(
            RuntimeKey::new(domain.clone(), ingestor.clone()),
            IngestorReadiness::new(expected_instances),
        );
    }

    pub(in crate::runtime) fn prepare_ingestor_quiescence(
        &self,
        domain: &Domain,
        ingestor: &CreateIngestor,
    ) -> Arc<IngestorQuiesceControl> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if let Some(control) = self.ingestor_quiescence.get(&key) {
            control.update_declared_source(&ingestor.source);
            return control.clone();
        }
        let metric_labels = self.metrics.register_ingestor_quiesce(
            domain,
            &ingestor.name,
            self.local_node_id.read().as_deref(),
        );
        let control = Arc::new(IngestorQuiesceControl::new(
            ingestor.source.quiesce().clone(),
            self.metrics.clone(),
            metric_labels,
        ));
        if self.ingestors_paused_for_memory_pressure() {
            control.engage(IngestorQuiesceCause::MemoryPressure);
        }
        self.ingestor_quiescence.insert(key, control.clone());
        control
    }

    pub(in crate::runtime) fn ingestor_quiesce_control(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
    ) -> Option<Arc<IngestorQuiesceControl>> {
        self.ingestor_quiescence
            .get(&RuntimeKey::new(domain.clone(), ingestor.clone()))
            .map(|control| control.clone())
    }

    fn engage_ingestor_quiesce(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        cause: IngestorQuiesceCause,
    ) -> bool {
        let Some(control) = self.ingestor_quiesce_control(domain, ingestor) else {
            return false;
        };
        control.engage(cause);
        info!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            cause = cause.as_str(),
            "ingestor entered quiesce"
        );
        true
    }

    fn release_ingestor_quiesce(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        cause: IngestorQuiesceCause,
    ) -> bool {
        let Some(control) = self.ingestor_quiesce_control(domain, ingestor) else {
            return false;
        };
        control.release(cause);
        info!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            cause = cause.as_str(),
            "ingestor left quiesce"
        );
        true
    }

    fn engage_domain_ingestor_quiesce(&self, domain: &Domain) {
        let ingestors = self
            .ingestors
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.key().identifier.clone())
            .collect::<Vec<_>>();
        for ingestor in ingestors {
            self.engage_ingestor_quiesce(domain, &ingestor, IngestorQuiesceCause::DomainPause);
        }
    }

    fn release_domain_ingestor_quiesce(&self, domain: &Domain) {
        let ingestors = self
            .ingestor_quiescence
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.key().identifier.clone())
            .collect::<Vec<_>>();
        for ingestor in ingestors {
            self.release_ingestor_quiesce(domain, &ingestor, IngestorQuiesceCause::DomainPause);
        }
    }

    fn remove_ingestor_quiescence(&self, domain: &Domain, ingestor: &Identifier) {
        let key = RuntimeKey::new(domain.clone(), ingestor.clone());
        if let Some((_, control)) = self.ingestor_quiescence.remove(&key) {
            control.terminate();
        }
    }

    fn clear_domain_ingestor_quiescence(&self, domain: &Domain) {
        let ingestors = self
            .ingestor_quiescence
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.key().identifier.clone())
            .collect::<Vec<_>>();
        for ingestor in ingestors {
            self.remove_ingestor_quiescence(domain, &ingestor);
        }
    }

    pub(in crate::runtime) fn mark_ingestor_instance_ready(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        instance_idx: u64,
    ) {
        let key = RuntimeKey::new(domain.clone(), ingestor.clone());
        if let Some(mut readiness) = self.ingestor_readiness.get_mut(&key) {
            readiness.ready_instances.insert(instance_idx);
        }
    }

    pub(in crate::runtime) fn mark_ingestor_instance_unready(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        instance_idx: u64,
    ) {
        let key = RuntimeKey::new(domain.clone(), ingestor.clone());
        if let Some(mut readiness) = self.ingestor_readiness.get_mut(&key) {
            readiness.ready_instances.remove(&instance_idx);
        }
    }

    pub(in crate::runtime) fn clear_ingestor_readiness(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
    ) {
        self.ingestor_readiness
            .remove(&RuntimeKey::new(domain.clone(), ingestor.clone()));
    }

    fn ingestor_ready(&self, domain: &Domain, ingestor: &Identifier) -> bool {
        self.ingestor_readiness
            .get(&RuntimeKey::new(domain.clone(), ingestor.clone()))
            .is_none_or(|readiness| readiness.is_ready())
    }

    fn ingestor_transient_error(&self, domain: &Domain, ingestor: &Identifier) -> Option<String> {
        self.ingestor_transient_errors
            .get(&RuntimeKey::new(domain.clone(), ingestor.clone()))
            .map(|error| error.value().clone())
    }

    fn ingestor_reconnect_backoff(&self, domain: &Domain, ingestor: &Identifier) -> Option<String> {
        self.ingestor_reconnect_backoffs
            .get(&RuntimeKey::new(domain.clone(), ingestor.clone()))
            .map(|status| humantime::format_duration(status.value().backoff).to_string())
    }

    fn ingestor_reconnect_wait_millis(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
    ) -> Option<u64> {
        self.ingestor_reconnect_backoffs
            .get(&RuntimeKey::new(domain.clone(), ingestor.clone()))
            .map(|status| {
                u64::try_from(
                    status
                        .value()
                        .retry_at
                        .saturating_duration_since(Instant::now())
                        .as_millis(),
                )
                .unwrap_or(u64::MAX)
            })
    }

    pub(in crate::runtime) fn record_emitter_transient_error(
        &self,
        domain: &Domain,
        emitter: &Identifier,
        error: impl Into<String>,
    ) {
        self.emitter_transient_errors.insert(
            RuntimeKey::new(domain.clone(), emitter.clone()),
            error.into(),
        );
    }

    pub(in crate::runtime) fn record_emitter_transient_error_with_backoff(
        &self,
        domain: &Domain,
        emitter: &Identifier,
        error: impl Into<String>,
        backoff: Duration,
    ) {
        self.record_emitter_retry_with_backoff(
            domain,
            emitter,
            error,
            backoff,
            EmitterRetryKind::Infrastructure,
        );
    }

    pub(in crate::runtime) fn record_iceberg_commit_failure_with_backoff(
        &self,
        domain: &Domain,
        emitter: &Identifier,
        error: impl Into<String>,
        backoff: Duration,
    ) {
        self.record_emitter_retry_with_backoff(
            domain,
            emitter,
            error,
            backoff,
            EmitterRetryKind::IcebergCommit,
        );
    }

    fn record_emitter_retry_with_backoff(
        &self,
        domain: &Domain,
        emitter: &Identifier,
        error: impl Into<String>,
        backoff: Duration,
        kind: EmitterRetryKind,
    ) {
        let key = RuntimeKey::new(domain.clone(), emitter.clone());
        self.emitter_transient_errors
            .insert(key.clone(), error.into());
        self.emitter_retry_statuses.insert(
            key,
            EmitterRetryStatus {
                kind,
                reconnect: RuntimeReconnectStatus {
                    backoff,
                    retry_at: Instant::now() + backoff,
                },
            },
        );
    }

    pub(in crate::runtime) fn begin_emitter_confirmation_wait(
        &self,
        domain: &Domain,
        emitter: &Identifier,
    ) -> EmitterConfirmationWaitGuard {
        let active_waits = self
            .emitter_confirmation_waits
            .entry(RuntimeKey::new(domain.clone(), emitter.clone()))
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        active_waits.fetch_add(1, Ordering::AcqRel);
        EmitterConfirmationWaitGuard { active_waits }
    }

    pub(in crate::runtime) fn clear_emitter_transient_error(
        &self,
        domain: &Domain,
        emitter: &Identifier,
    ) {
        self.emitter_transient_errors
            .remove(&RuntimeKey::new(domain.clone(), emitter.clone()));
        self.emitter_retry_statuses
            .remove(&RuntimeKey::new(domain.clone(), emitter.clone()));
    }

    fn emitter_transient_error(&self, domain: &Domain, emitter: &Identifier) -> Option<String> {
        self.emitter_transient_errors
            .get(&RuntimeKey::new(domain.clone(), emitter.clone()))
            .map(|error| error.value().clone())
    }

    pub fn emitter_reconnect_backoff(
        &self,
        domain: &Domain,
        emitter: &Identifier,
    ) -> Option<String> {
        self.emitter_retry_statuses
            .get(&RuntimeKey::new(domain.clone(), emitter.clone()))
            .map(|status| humantime::format_duration(status.value().reconnect.backoff).to_string())
    }

    fn emitter_reconnect_wait_millis(&self, domain: &Domain, emitter: &Identifier) -> Option<u64> {
        self.emitter_retry_statuses
            .get(&RuntimeKey::new(domain.clone(), emitter.clone()))
            .map(|status| {
                u64::try_from(
                    status
                        .value()
                        .reconnect
                        .retry_at
                        .saturating_duration_since(Instant::now())
                        .as_millis(),
                )
                .unwrap_or(u64::MAX)
            })
    }

    pub(in crate::runtime) async fn wait_if_ingestor_faulted(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> bool {
        if !self.ingestor_faults.is_failed(ingestor) {
            return false;
        }
        self.record_ingestor_transient_error_with_backoff(
            domain,
            ingestor,
            "ingestor fault injector failed source",
            Duration::from_millis(250),
        );
        tokio::select! {
            changed = shutdown_rx.changed() => changed.is_err() || *shutdown_rx.borrow(),
            _ = sleep(Duration::from_millis(250)) => false,
        }
    }

    pub(in crate::runtime) fn mark_branch_aggregated_metrics_updated(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
    ) {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::BranchAggregated,
            kind,
            identifier,
            None,
        );
        if let Some(state) = self.replicated_branch_aggregated_states.get(&placement) {
            state.mark_metrics_updated();
        }
    }

    pub fn attach_resource_store(&self, resource_store: Arc<ResourceStore>) {
        *self.resource_store.write() = Some(resource_store);
    }

    pub fn sync_resource_versions(&self, resources: &nervix_models::ResourceVersionStatus) {
        self.latest_resource_versions.clear();
        for resource in &resources.versions {
            let key = (resource.id.domain.clone(), resource.id.identifier.clone());
            if let Some(mut existing) = self.latest_resource_versions.get_mut(&key) {
                if resource.id.version > *existing {
                    *existing = resource.id.version;
                }
            } else {
                self.latest_resource_versions
                    .insert(key, resource.id.version);
            }
        }
    }

    pub fn attach_remote_dispatcher(
        &self,
        local_node_id: String,
        cluster: Arc<cluster::ClusterHandle>,
        interconnect: Arc<Transport>,
    ) {
        *self.local_node_id.write() = Some(local_node_id);
        *self.remote_dispatcher.write() = Some(Arc::new(RemoteDispatcher {
            cluster,
            interconnect,
            local_node_id: self.local_node_id.clone(),
            next_remote_ack_id: self.next_remote_ack_id.clone(),
            pending_remote_acks: self.pending_remote_acks.clone(),
        }));
    }

    pub fn attach_resources(
        &self,
        resource_store: Arc<ResourceStore>,
        resource_versions: ResourceVersionStatus,
    ) {
        *self.resource_store.write() = Some(resource_store);
        self.sync_resource_versions(&resource_versions);
        *self.resource_versions.write() = resource_versions;
    }

    pub fn update_resource_versions(&self, resource_versions: ResourceVersionStatus) {
        self.sync_resource_versions(&resource_versions);
        *self.resource_versions.write() = resource_versions;
    }

    /// Resolves a resource reference to the concrete version installed in `domain`. Resources are
    /// domain-owned, so the same name in another domain is a different resource with its own
    /// version sequence. `spec` may pin a version as `<name>@<version>`.
    pub(in crate::runtime) fn resolve_resource_id(
        &self,
        domain: &Domain,
        identifier: &Identifier,
        requested_version: Option<u64>,
        spec: &str,
    ) -> Result<ResourceId, String> {
        if let Some(version) = requested_version {
            return Ok(ResourceId::new(domain.clone(), identifier.clone(), version));
        }
        if let Some((name, version)) = spec.rsplit_once('@') {
            let parsed = Identifier::parse(name)
                .map_err(|_| format!("invalid client resource identifier '{name}'"))?;
            if &parsed != identifier {
                return Err(format!(
                    "client resource mount '{spec}' resolved to unexpected identifier '{}'",
                    parsed.as_str()
                ));
            }
            let version = version
                .parse::<u64>()
                .map_err(|_| format!("invalid client resource version '{version}'"))?;
            return Ok(ResourceId::new(domain.clone(), identifier.clone(), version));
        }

        let resources = self.resource_versions.read();
        resources
            .latest_version(domain, identifier)
            .map(|version| ResourceId::new(domain.clone(), identifier.clone(), version))
            .ok_or_else(|| {
                format!(
                    "resource '{}' has no installed versions in domain '{}'",
                    identifier.as_str(),
                    domain.as_str()
                )
            })
    }

    pub(crate) fn resolve_client_config(
        &self,
        domain: &Domain,
        mount: Option<&Identifier>,
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<ResolvedClientConfig, String> {
        self.resolve_client_config_with_template_vars(domain, mount, config, BTreeMap::default())
    }

    pub(in crate::runtime) fn resolve_client_config_with_instance(
        &self,
        domain: &Domain,
        mount: Option<&Identifier>,
        config: &[nervix_models::ClientConfigEntry],
        instance: u64,
    ) -> Result<ResolvedClientConfig, String> {
        self.resolve_client_config_with_template_vars(
            domain,
            mount,
            config,
            BTreeMap::from([("instance".to_string(), instance.to_string())]),
        )
    }

    fn resolve_client_config_with_template_vars(
        &self,
        domain: &Domain,
        mount: Option<&Identifier>,
        config: &[nervix_models::ClientConfigEntry],
        mut context: BTreeMap<String, String>,
    ) -> Result<ResolvedClientConfig, String> {
        let template_engine = TemplateEngine::new();
        let mut entries = Vec::with_capacity(config.len());
        for entry in config {
            entries.push(entry.clone());
        }

        let Some(mount) = mount else {
            for entry in &mut entries {
                entry.value = render_client_config_template(
                    &template_engine,
                    &entry.key,
                    &entry.value,
                    &context,
                )?;
            }
            return Ok(ResolvedClientConfig {
                entries,
                mounts: None,
            });
        };

        let resource_store = self
            .resource_store
            .read()
            .clone()
            .ok_or_else(|| "runtime resource store is not available".to_string())?;
        let mount_root = tempfile::tempdir()
            .map_err(|source| format!("failed to create client resource mount root: {source}"))?;
        let mut aliases = BTreeMap::new();
        let id = self.resolve_resource_id(domain, mount, None, mount.as_str())?;
        let source_root = resource_store.content_root(&id);
        if !source_root.exists() {
            return Err(format!(
                "client resource mount '{}' points to missing content root '{}'",
                mount.as_str(),
                source_root.display()
            ));
        }
        let mount_path = mount_root.path().join(mount.as_str());
        #[cfg(unix)]
        std::os::unix::fs::symlink(&source_root, &mount_path).map_err(|source| {
            format!(
                "failed to mount client resource '{}' at '{}': {source}",
                mount.as_str(),
                mount_path.display()
            )
        })?;
        #[cfg(not(unix))]
        {
            return Err("client resource mounts are only supported on unix targets".to_string());
        }
        aliases.insert(mount.as_str().to_string(), mount_path);

        for (resource_name, mount_path) in &aliases {
            context.insert(
                resource_name.clone(),
                mount_path.to_string_lossy().into_owned(),
            );
        }
        for entry in &mut entries {
            entry.value = render_client_config_template(
                &template_engine,
                &entry.key,
                &entry.value,
                &context,
            )?;
        }

        Ok(ResolvedClientConfig {
            entries,
            mounts: Some(Arc::new(ClientResourceMounts {
                _root: mount_root,
                _aliases: aliases,
            })),
        })
    }

    pub fn has_state_store(&self) -> bool {
        self.state_store.is_some()
    }

    pub fn state_snapshot_interval(&self) -> Duration {
        self.state_snapshot_interval
    }

    pub(crate) async fn handle_state_sync_request(
        &self,
        placement: &RuntimeStatePlacement,
        after_lsm: u64,
    ) -> Result<Option<PersistedRuntimeStateEntry>, String> {
        if let RuntimeStateKind::MaterializedRelay = placement.state {
            let mut entries = Vec::new();
            let mut latest_lsm = 0;
            let mut found = false;
            let mut metrics_snapshot = crate::metrics::RuntimeMetricsSnapshot::default();
            for state in self.replicated_materialized_stream_states.iter() {
                let concrete = state.key();
                if concrete.domain != placement.domain
                    || concrete.state != placement.state
                    || concrete.kind != placement.kind
                    || concrete.identifier != placement.identifier
                    || concrete.schema_fingerprint != placement.schema_fingerprint
                {
                    continue;
                }
                found = true;
                latest_lsm = latest_lsm.max(state.current_lsm.load(Ordering::SeqCst));
                if concrete.branch_key.is_none() {
                    metrics_snapshot = state.metrics_snapshot(&self.metrics);
                }
                entries.extend(
                    self.visible_materialized_stream_remote_entries(concrete, state.value())
                        .into_iter()
                        .filter(|(key, _)| {
                            placement
                                .branch_key
                                .as_ref()
                                .is_none_or(|requested| key.as_ref() == Some(requested))
                        }),
                );
            }
            if found {
                if latest_lsm <= after_lsm {
                    return Ok(None);
                }
                return Ok(Some(PersistedRuntimeStateEntry {
                    lsm: latest_lsm,
                    schema_fingerprint: placement.schema_fingerprint,
                    payload: encode_materialized_stream_snapshot_entries(
                        &entries,
                        metrics_snapshot,
                    )
                    .map_err(|error| error.to_string())?,
                }));
            }
        }
        if let Some(state) = self.replicated_deduplicator_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
            return Ok(None);
        }
        if let Some(state) = self.replicated_kafka_offset_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self.replicated_materialized_stream_states.get(placement) {
            let snapshot = PersistedRuntimeStateEntry {
                lsm: state.current_lsm.load(Ordering::SeqCst),
                schema_fingerprint: placement.schema_fingerprint,
                payload: encode_materialized_stream_snapshot_entries(
                    &self.visible_materialized_stream_remote_entries(placement, &state),
                    state.metrics_snapshot(&self.metrics),
                )
                .map_err(|error| error.to_string())?,
            };
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self.replicated_window_processor_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self.replicated_wasm_processor_states.get(placement) {
            let snapshot = state.latest_snapshot().map_err(|error| error.to_string())?;
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
        }
        if let Some(state) = self.replicated_branch_aggregated_states.get(placement) {
            let snapshot = state
                .latest_snapshot(&self.metrics)
                .map_err(|error| error.to_string())?;
            if snapshot.lsm > after_lsm {
                return Ok(Some(snapshot));
            }
        }
        Ok(None)
    }

    pub fn handle_state_sync_response(
        &self,
        correlation_id: u64,
        result: Result<Option<PersistedRuntimeStateEntry>, String>,
    ) {
        let Some((_, tx)) = self.pending_state_syncs.remove(&correlation_id) else {
            return;
        };
        let _ = tx.send(result);
    }

    pub(crate) fn handle_state_replication_ack(&self, node_id: &str, ack: StateSyncAck) {
        if let Some(state) = self.replicated_deduplicator_states.get(&ack.placement) {
            state.mark_replica_progress(node_id, ack.lsm);
        }
        if let Some(state) = self.replicated_kafka_offset_states.get(&ack.placement) {
            state.mark_replica_progress(node_id, ack.lsm);
        }
        if let Some(state) = self
            .replicated_materialized_stream_states
            .get(&ack.placement)
        {
            state.mark_replica_progress(node_id, ack.lsm);
        }
        if let Some(state) = self.replicated_window_processor_states.get(&ack.placement) {
            state.mark_replica_progress(node_id, ack.lsm);
        }
        if let Some(state) = self.replicated_wasm_processor_states.get(&ack.placement) {
            state.mark_replica_progress(node_id, ack.lsm);
        }
        if let Some(state) = self.replicated_branch_aggregated_states.get(&ack.placement) {
            state.mark_replica_progress(node_id, ack.lsm);
        }
    }

    pub(in crate::runtime) async fn request_state_sync(
        &self,
        target_node_id: &str,
        placement: &RuntimeStatePlacement,
        after_lsm: u64,
    ) -> Result<Option<PersistedRuntimeStateEntry>, String> {
        self.request_state_sync_with_timeout(
            target_node_id,
            placement,
            after_lsm,
            Duration::from_secs(5),
        )
        .await
    }

    async fn request_state_sync_with_timeout(
        &self,
        target_node_id: &str,
        placement: &RuntimeStatePlacement,
        after_lsm: u64,
        response_timeout: Duration,
    ) -> Result<Option<PersistedRuntimeStateEntry>, String> {
        let Some(dispatcher) = self.remote_dispatcher.read().clone() else {
            return Err("remote dispatcher unavailable".to_string());
        };
        let correlation_id = self
            .next_state_sync_correlation_id
            .fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending_state_syncs.insert(correlation_id, tx);
        let result = dispatcher
            .dispatch(
                target_node_id,
                Envelope::Control(nervix_interconnect::ControlEnvelope::StateSyncRequest(
                    nervix_interconnect::StateSyncRequest {
                        correlation_id,
                        placement: placement.to_remote(),
                        after_lsm,
                    },
                )),
            )
            .await;
        if let Err(error) = result {
            self.pending_state_syncs.remove(&correlation_id);
            return Err(error);
        }
        match tokio::time::timeout(response_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.pending_state_syncs.remove(&correlation_id);
                Err("state sync response channel closed".to_string())
            }
            Err(_) => {
                self.pending_state_syncs.remove(&correlation_id);
                Err("timed out waiting for state sync response".to_string())
            }
        }
    }

    pub(in crate::runtime) async fn wait_for_replica_quorum(
        &self,
        state: &ReplicatedDeduplicatorState,
        lsm: u64,
    ) -> Result<(), String> {
        if state.required_replica_acks == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            if state.replica_quorum_satisfied(lsm) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for replica quorum for '{}' at lsm {}",
                    state.placement.identifier.as_str(),
                    lsm
                ));
            }
            tokio::select! {
                _ = state.replication_notify.notified() => {}
                _ = sleep_until(deadline) => {}
            }
        }
    }

    pub(in crate::runtime) async fn persist_deduplicator_snapshot(
        &self,
        state: &ReplicatedDeduplicatorState,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Some(store) = &self.state_store {
            store
                .persist_latest_snapshot(&state.placement, lsm, payload)
                .map_err(|error| error.to_string())?;
            state.last_persisted_lsm.store(lsm, Ordering::SeqCst);
            state.dirty.store(false, Ordering::SeqCst);
        }
        self.wait_for_replica_quorum(state, lsm).await
    }

    pub(in crate::runtime) async fn wait_for_kafka_offset_replica_quorum(
        &self,
        state: &ReplicatedKafkaOffsetState,
        lsm: u64,
    ) -> Result<(), String> {
        if state.required_replica_acks == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            if state.replica_quorum_satisfied(lsm) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for replica quorum for '{}' at lsm {}",
                    state.placement.identifier.as_str(),
                    lsm
                ));
            }
            tokio::select! {
                _ = state.replication_notify.notified() => {}
                _ = sleep_until(deadline) => {}
            }
        }
    }

    pub(in crate::runtime) async fn wait_for_materialized_stream_replica_quorum(
        &self,
        state: &ReplicatedMaterializedRelayState,
        lsm: u64,
    ) -> Result<(), String> {
        if state.required_replica_acks == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            if state.replica_quorum_satisfied(lsm) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for replica quorum for '{}' at lsm {}",
                    state.placement.identifier.as_str(),
                    lsm
                ));
            }
            tokio::select! {
                _ = state.replication_notify.notified() => {}
                _ = sleep_until(deadline) => {}
            }
        }
    }

    pub(in crate::runtime) async fn wait_for_window_processor_replica_quorum(
        &self,
        state: &ReplicatedWindowProcessorState,
        lsm: u64,
    ) -> Result<(), String> {
        if state.required_replica_acks == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            if state.replica_quorum_satisfied(lsm) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for replica quorum for '{}' branch '{}' primary '{}' at \
                     lsm {}",
                    state.placement.identifier.as_str(),
                    state.placement.concrete_branch_key(),
                    state.primary_node.as_deref().unwrap_or("-"),
                    lsm
                ));
            }
            tokio::select! {
                _ = state.replication_notify.notified() => {}
                _ = sleep_until(deadline) => {}
            }
        }
    }

    pub(in crate::runtime) async fn wait_for_wasm_processor_replica_quorum(
        &self,
        state: &ReplicatedWasmProcessorState,
        lsm: u64,
    ) -> Result<(), String> {
        if state.required_replica_acks == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            if state.replica_quorum_satisfied(lsm) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for replica quorum for wasm processor '{}' branch '{}' at \
                     lsm {}",
                    state.placement.identifier.as_str(),
                    state.placement.concrete_branch_key(),
                    lsm
                ));
            }
            tokio::select! {
                _ = state.replication_notify.notified() => {}
                _ = sleep_until(deadline) => {}
            }
        }
    }

    pub(in crate::runtime) async fn persist_kafka_offset_snapshot(
        &self,
        state: &ReplicatedKafkaOffsetState,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Some(store) = &self.state_store {
            store
                .persist_latest_snapshot(&state.placement, lsm, payload)
                .map_err(|error| error.to_string())?;
            state.last_persisted_lsm.store(lsm, Ordering::SeqCst);
            state.dirty.store(false, Ordering::SeqCst);
        }
        self.wait_for_kafka_offset_replica_quorum(state, lsm).await
    }

    pub(in crate::runtime) async fn commit_domain_kafka_offset(
        &self,
        state: &ReplicatedKafkaOffsetState,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), String> {
        let (lsm, payload) = state
            .apply_committed_offset(topic, partition, next_offset)
            .map_err(|error| error.to_string())?;
        self.persist_kafka_offset_snapshot(state, lsm, &payload)
            .await
    }

    pub(in crate::runtime) async fn reset_domain_kafka_offsets(
        &self,
        state: &ReplicatedKafkaOffsetState,
        offsets: HashMap<(String, i32), i64>,
    ) -> Result<(), String> {
        let (lsm, payload) = state
            .replace_offsets(offsets)
            .map_err(|error| error.to_string())?;
        self.persist_kafka_offset_snapshot(state, lsm, &payload)
            .await
    }

    pub(in crate::runtime) async fn persist_materialized_stream_snapshot(
        &self,
        state: &ReplicatedMaterializedRelayState,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Some(store) = &self.state_store {
            store
                .persist_latest_snapshot(&state.placement, lsm, payload)
                .map_err(|error| error.to_string())?;
            state.last_persisted_lsm.store(lsm, Ordering::SeqCst);
            state.dirty.store(false, Ordering::SeqCst);
        }
        self.wait_for_materialized_stream_replica_quorum(state, lsm)
            .await
    }

    pub(in crate::runtime) async fn persist_window_processor_snapshot(
        &self,
        state: &ReplicatedWindowProcessorState,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Some(store) = &self.state_store {
            store
                .persist_latest_snapshot(&state.placement, lsm, payload)
                .map_err(|error| error.to_string())?;
            state.last_persisted_lsm.store(lsm, Ordering::SeqCst);
            state.dirty.store(false, Ordering::SeqCst);
        }
        self.wait_for_window_processor_replica_quorum(state, lsm)
            .await
    }

    pub(in crate::runtime) async fn persist_wasm_processor_snapshot(
        &self,
        state: &ReplicatedWasmProcessorState,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Some(store) = &self.state_store {
            store
                .persist_latest_snapshot(&state.placement, lsm, payload)
                .map_err(|error| error.to_string())?;
            state.last_persisted_lsm.store(lsm, Ordering::SeqCst);
            state.dirty.store(false, Ordering::SeqCst);
        }
        self.wait_for_wasm_processor_replica_quorum(state, lsm)
            .await
    }

    pub(in crate::runtime) async fn update_materialized_stream_last_by_timestamp(
        &self,
        state: &ReplicatedMaterializedRelayState,
        key: &Option<BranchKey>,
        record: &RuntimeRecord,
    ) -> Result<(), String> {
        let Some((lsm, payload)) = state
            .update_last_by_timestamp(&self.metrics, key, record)
            .map_err(|error| error.to_string())?
        else {
            return Ok(());
        };
        self.persist_materialized_stream_snapshot(state, lsm, &payload)
            .await?;
        self.materialized_state_changed.notify_waiters();
        Ok(())
    }

    pub(in crate::runtime) async fn delete_materialized_stream_key(
        &self,
        state: &ReplicatedMaterializedRelayState,
        key: &Option<BranchKey>,
    ) -> Result<(), String> {
        let Some((lsm, payload)) = state
            .remove_key(&self.metrics, key)
            .map_err(|error| error.to_string())?
        else {
            return Ok(());
        };
        self.persist_materialized_stream_snapshot(state, lsm, &payload)
            .await?;
        self.materialized_state_changed.notify_waiters();
        Ok(())
    }

    pub(in crate::runtime) fn replicated_deduplicator_state(
        &self,
        placement: RuntimeStatePlacement,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
    ) -> Result<Arc<ReplicatedDeduplicatorState>, RuntimePersistenceError> {
        if let Some(existing) = self.replicated_deduplicator_states.get(&placement) {
            return Ok(existing.clone());
        }
        let initial = self
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
        let state = Arc::new(ReplicatedDeduplicatorState::new(
            placement.clone(),
            replica_nodes,
            required_replica_acks,
            initial,
        )?);
        self.replicated_deduplicator_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_kafka_offset_state(
        &self,
        placement: RuntimeStatePlacement,
        primary_node: Option<String>,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
    ) -> Result<Arc<ReplicatedKafkaOffsetState>, RuntimePersistenceError> {
        if let Some(existing) = self.replicated_kafka_offset_states.get(&placement) {
            return Ok(existing.clone());
        }
        let initial = self
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
        let state = Arc::new(ReplicatedKafkaOffsetState::new(
            placement.clone(),
            primary_node,
            replica_nodes,
            required_replica_acks,
            initial,
        )?);
        self.replicated_kafka_offset_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_materialized_stream_state(
        &self,
        placement: RuntimeStatePlacement,
        primary_node: Option<String>,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
    ) -> Result<Arc<ReplicatedMaterializedRelayState>, RuntimePersistenceError> {
        if let Some(existing) = self.replicated_materialized_stream_states.get(&placement) {
            return Ok(existing.clone());
        }
        let initial = self
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement.clone(),
            primary_node,
            self.local_node_id
                .read()
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            replica_nodes,
            required_replica_acks,
            &self.metrics,
            initial,
        )?);
        self.replicated_materialized_stream_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_window_processor_state(
        &self,
        placement: RuntimeStatePlacement,
        primary_node: Option<String>,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
    ) -> Result<Arc<ReplicatedWindowProcessorState>, RuntimePersistenceError> {
        if let Some(existing) = self.replicated_window_processor_states.get(&placement) {
            return Ok(existing.clone());
        }
        let initial = self
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
        let state = Arc::new(ReplicatedWindowProcessorState::new(
            placement.clone(),
            primary_node,
            replica_nodes,
            required_replica_acks,
            initial,
        )?);
        self.replicated_window_processor_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_wasm_processor_state(
        &self,
        placement: RuntimeStatePlacement,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
    ) -> Result<Arc<ReplicatedWasmProcessorState>, RuntimePersistenceError> {
        if let Some(existing) = self.replicated_wasm_processor_states.get(&placement) {
            return Ok(existing.clone());
        }
        let initial = self
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
        let state = Arc::new(ReplicatedWasmProcessorState::new(
            placement.clone(),
            replica_nodes,
            required_replica_acks,
            initial,
        )?);
        self.replicated_wasm_processor_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn replicated_branch_aggregated_state(
        &self,
        placement: RuntimeStatePlacement,
        primary_node: Option<String>,
        physical_node_id: String,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
    ) -> Result<Arc<ReplicatedBranchAggregatedState>, RuntimePersistenceError> {
        if let Some(existing) = self.replicated_branch_aggregated_states.get(&placement) {
            if let Some(snapshot) = self
                .state_store
                .as_ref()
                .map(|store| store.latest_snapshot(&placement))
                .transpose()?
                .flatten()
            {
                existing.restore_persisted_snapshot(&self.metrics, snapshot)?;
            }
            return Ok(existing.clone());
        }
        let initial = self
            .state_store
            .as_ref()
            .map(|store| store.latest_snapshot(&placement))
            .transpose()?
            .flatten();
        let state = Arc::new(ReplicatedBranchAggregatedState::new(
            placement.clone(),
            primary_node,
            physical_node_id,
            replica_nodes,
            required_replica_acks,
            &self.metrics,
            initial,
        )?);
        self.replicated_branch_aggregated_states
            .insert(placement, state.clone());
        Ok(state)
    }

    pub(in crate::runtime) fn spawn_kafka_offset_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedKafkaOffsetState>,
    ) -> Option<JoinHandle<()>> {
        let store = self.state_store.as_ref()?.clone();
        let snapshot_interval = self.state_snapshot_interval;
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                |state: &ReplicatedKafkaOffsetState, store: &RuntimeStateStore| {
                    if !state.dirty.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    let snapshot = state.latest_snapshot()?;
                    if snapshot.lsm <= state.last_persisted_lsm.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    store.persist_latest_snapshot(
                        &state.placement,
                        snapshot.lsm,
                        &snapshot.payload,
                    )?;
                    state
                        .last_persisted_lsm
                        .store(snapshot.lsm, Ordering::SeqCst);
                    state.dirty.store(false, Ordering::SeqCst);
                    Ok::<(), RuntimePersistenceError>(())
                };
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            if let Err(error) = flush_latest_snapshot(&state, &store) {
                                warn!(error = %error, "failed to flush kafka offset snapshot during shutdown");
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        if let Err(error) = flush_latest_snapshot(&state, &store) {
                            warn!(error = %error, "failed to persist kafka offset snapshot");
                        }
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_materialized_stream_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedMaterializedRelayState>,
    ) -> Option<JoinHandle<()>> {
        let store = self.state_store.as_ref()?.clone();
        let metrics = self.metrics.clone();
        let snapshot_interval = self.state_snapshot_interval;
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                |state: &ReplicatedMaterializedRelayState,
                 metrics: &RuntimeMetrics,
                 store: &RuntimeStateStore| {
                    if !state.dirty.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    let snapshot = state.latest_snapshot(metrics)?;
                    if snapshot.lsm <= state.last_persisted_lsm.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    store.persist_latest_snapshot(
                        &state.placement,
                        snapshot.lsm,
                        &snapshot.payload,
                    )?;
                    state
                        .last_persisted_lsm
                        .store(snapshot.lsm, Ordering::SeqCst);
                    state.dirty.store(false, Ordering::SeqCst);
                    Ok::<(), RuntimePersistenceError>(())
                };
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            if let Err(error) = flush_latest_snapshot(&state, &metrics, &store) {
                                warn!(error = %error, "failed to flush materialized relay snapshot during shutdown");
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        if let Err(error) = flush_latest_snapshot(&state, &metrics, &store) {
                            warn!(error = %error, "failed to persist materialized relay snapshot");
                        }
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_branch_aggregated_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedBranchAggregatedState>,
    ) -> Option<JoinHandle<()>> {
        let store = self.state_store.as_ref()?.clone();
        let metrics = self.metrics.clone();
        let snapshot_interval = self.state_snapshot_interval;
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let flush_latest_snapshot =
                |state: &ReplicatedBranchAggregatedState,
                 metrics: &RuntimeMetrics,
                 store: &RuntimeStateStore| {
                    if !state.dirty.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    let snapshot = state.latest_snapshot(metrics)?;
                    if snapshot.lsm <= state.last_persisted_lsm.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    store.persist_latest_snapshot(
                        &state.placement,
                        snapshot.lsm,
                        &snapshot.payload,
                    )?;
                    state
                        .last_persisted_lsm
                        .store(snapshot.lsm, Ordering::SeqCst);
                    state.dirty.store(false, Ordering::SeqCst);
                    Ok::<(), RuntimePersistenceError>(())
                };
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            if let Err(error) = flush_latest_snapshot(&state, &metrics, &store) {
                                warn!(error = %error, "failed to flush branch-aggregated state snapshot during shutdown");
                            }
                            break;
                        }
                    }
                    _ = sleep(snapshot_interval) => {
                        if let Err(error) = flush_latest_snapshot(&state, &metrics, &store) {
                            warn!(error = %error, "failed to persist branch-aggregated state snapshot");
                        }
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_kafka_offset_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedKafkaOffsetState>,
    ) -> Option<JoinHandle<()>> {
        let primary_node = state.primary_node.clone()?;
        let poll_interval = self.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if initial_sync_pending {
                    initial_sync_pending = false;
                } else {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(poll_interval) => {}
                    }
                }
                let after_lsm = state.current_lsm.load(Ordering::SeqCst);
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &state.placement,
                        after_lsm,
                        poll_interval,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) = state.apply_snapshot(snapshot.lsm, &snapshot.payload) {
                            warn!(error = %error, "failed to apply replicated kafka offset snapshot");
                            continue;
                        }
                        let dispatcher = runtime.remote_dispatcher.read().clone();
                        if let Some(dispatcher) = dispatcher {
                            let local_node_id = runtime.local_node_id.read().clone();
                            let Some(local_node_id) = local_node_id else {
                                continue;
                            };
                            if let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: state.placement.to_remote(),
                                                lsm: snapshot.lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                            {
                                warn!(node_id = local_node_id, error = %error, "failed to acknowledge replicated kafka offset snapshot");
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %error, "failed to sync replicated kafka offsets");
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_materialized_stream_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedMaterializedRelayState>,
    ) -> Option<JoinHandle<()>> {
        let primary_node = state.primary_node.clone()?;
        let poll_interval = self.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if initial_sync_pending {
                    initial_sync_pending = false;
                } else {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(poll_interval) => {}
                    }
                }
                let after_lsm = state.current_lsm.load(Ordering::SeqCst);
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &state.placement,
                        after_lsm,
                        poll_interval,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) =
                            state.apply_snapshot(&runtime.metrics, snapshot.lsm, &snapshot.payload)
                        {
                            warn!(error = %error, "failed to apply replicated materialized relay snapshot");
                            continue;
                        }
                        runtime.materialized_state_changed.notify_waiters();
                        let dispatcher = runtime.remote_dispatcher.read().clone();
                        if let Some(dispatcher) = dispatcher {
                            let local_node_id = runtime.local_node_id.read().clone();
                            let Some(local_node_id) = local_node_id else {
                                continue;
                            };
                            if let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: state.placement.to_remote(),
                                                lsm: snapshot.lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                            {
                                warn!(node_id = local_node_id, error = %error, "failed to acknowledge replicated materialized relay snapshot");
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %error, "failed to sync replicated materialized relay state");
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_branch_aggregated_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedBranchAggregatedState>,
    ) -> Option<JoinHandle<()>> {
        let primary_node = state.primary_node.clone()?;
        let poll_interval = self.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                tokio::task::consume_budget().await;
                if initial_sync_pending {
                    initial_sync_pending = false;
                } else {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(poll_interval) => {}
                    }
                }
                let after_lsm = state.current_lsm.load(Ordering::SeqCst);
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &state.placement,
                        after_lsm,
                        poll_interval,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) =
                            state.apply_snapshot(&runtime.metrics, snapshot.lsm, &snapshot.payload)
                        {
                            warn!(error = %error, "failed to apply replicated branch-aggregated state snapshot");
                            continue;
                        }
                        let dispatcher = runtime.remote_dispatcher.read().clone();
                        if let Some(dispatcher) = dispatcher {
                            let local_node_id = runtime.local_node_id.read().clone();
                            let Some(local_node_id) = local_node_id else {
                                continue;
                            };
                            if let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: state.placement.to_remote(),
                                                lsm: snapshot.lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                            {
                                warn!(node_id = local_node_id, error = %error, "failed to acknowledge replicated branch-aggregated state snapshot");
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %error, "failed to sync replicated branch-aggregated state");
                    }
                }
            }
        }))
    }

    pub fn sync_domains(&self, domains: &BTreeMap<Domain, DomainState>) {
        for domain in self
            .domains
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>()
        {
            if !domains.contains_key(&domain) {
                self.domains.remove(&domain);
                self.domain_instantiation_errors.remove(&domain);
                self.in_flight_by_domain.remove(&domain);
                self.generator_activity_by_domain.remove(&domain);
                if let Some((_, force_flush)) = self.force_flush_by_domain.remove(&domain) {
                    force_flush.close();
                }
            }
        }

        for (domain, state) in domains {
            let mut entry =
                self.domains
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
        self.domain_status_changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    pub(in crate::runtime) fn tracked_ack_root(&self, domain: &Domain) -> (AckSet, AckCompletion) {
        let tracker = self
            .in_flight_by_domain
            .entry(domain.clone())
            .or_insert_with(|| Arc::new(AckRootTracker::default()))
            .clone();
        AckSet::tracked_root(tracker)
    }

    pub fn domain_outstanding_work(&self, domain: &Domain) -> usize {
        self.in_flight_by_domain
            .get(domain)
            .map_or(0, |tracker| tracker.outstanding())
    }

    fn generator_activity_tracker(&self, domain: &Domain) -> Arc<AtomicUsize> {
        self.generator_activity_by_domain
            .entry(domain.clone())
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone()
    }

    pub(in crate::runtime) fn force_flush_participant(
        &self,
        domain: &Domain,
        counters: Arc<NodeQuiesceCounters>,
    ) -> DomainForceFlushParticipant {
        let coordinator = self
            .force_flush_by_domain
            .entry(domain.clone())
            .or_insert_with(DomainForceFlush::new)
            .clone();
        DomainForceFlush::subscribe(&coordinator, Some(counters))
    }

    pub fn force_flush_domain(&self, domain: &Domain) -> u64 {
        self.force_flush_by_domain
            .entry(domain.clone())
            .or_insert_with(DomainForceFlush::new)
            .request()
    }

    pub fn force_flush_domain_if_idle(&self, domain: &Domain) -> u64 {
        self.force_flush_by_domain
            .entry(domain.clone())
            .or_insert_with(DomainForceFlush::new)
            .request_if_idle()
    }

    pub fn domain_drain_status(&self, domain: &Domain) -> DomainDrainStatus {
        let active_ingestors = self
            .ingestors
            .iter()
            .filter(|entry| {
                &entry.key().domain == domain
                    && self
                        .ingestor_quiescence
                        .get(entry.key())
                        .is_none_or(|control| !control.is_quiesced())
            })
            .count();
        let active_generators = self
            .generator_activity_by_domain
            .get(domain)
            .map_or(0, |counter| counter.load(Ordering::Acquire));
        let buffered_emitter_messages = self
            .emitter_buffers
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.value().load(Ordering::Acquire))
            .sum();
        let mut publishing_keys = self
            .emitter_confirmation_waits
            .iter()
            .filter(|entry| {
                &entry.key().domain == domain && entry.value().load(Ordering::Acquire) > 0
            })
            .map(|entry| entry.key().clone())
            .collect::<HashSet<_>>();
        publishing_keys.extend(
            self.emitter_retry_statuses
                .iter()
                .filter(|entry| &entry.key().domain == domain)
                .map(|entry| entry.key().clone()),
        );
        let mut emitter_publishing = publishing_keys
            .into_iter()
            .filter_map(|key| {
                let pending_messages = self
                    .emitter_buffers
                    .get(&key)
                    .map(|buffered| buffered.load(Ordering::Acquire))
                    .unwrap_or(0);
                let awaiting_confirmation = self
                    .emitter_confirmation_waits
                    .get(&key)
                    .is_some_and(|waits| waits.load(Ordering::Acquire) > 0);
                if awaiting_confirmation {
                    return Some(EmitterPublishingDrainStatus {
                        emitter: key.identifier,
                        state: EmitterPublishingDrainState::AwaitingConfirmation,
                        pending_messages,
                        retry_backoff: None,
                        retry_wait: None,
                    });
                }
                let retry = self.emitter_retry_statuses.get(&key)?;
                let state = match retry.kind {
                    EmitterRetryKind::Infrastructure => {
                        EmitterPublishingDrainState::RetryingInfrastructure
                    }
                    EmitterRetryKind::IcebergCommit => {
                        EmitterPublishingDrainState::RetryingIcebergCommit
                    }
                };
                Some(EmitterPublishingDrainStatus {
                    emitter: key.identifier,
                    state,
                    pending_messages,
                    retry_backoff: Some(retry.reconnect.backoff),
                    retry_wait: Some(
                        retry
                            .reconnect
                            .retry_at
                            .saturating_duration_since(Instant::now()),
                    ),
                })
            })
            .collect::<Vec<_>>();
        emitter_publishing.sort_by(|left, right| left.emitter.cmp(&right.emitter));
        DomainDrainStatus {
            active_ingestors,
            active_generators,
            outstanding_acks: self.domain_outstanding_work(domain),
            buffered_emitter_messages,
            emitter_publishing,
        }
    }

    pub(in crate::runtime) fn current_stream_expiration_time(
        &self,
        domain: &Domain,
    ) -> Result<Option<Timestamp>, String> {
        let wall_now = current_timestamp();
        let Some(state) = self.domains.get(domain) else {
            return Ok(Some(wall_now));
        };
        match state.config.pace {
            DomainPace::Unpaced => Ok(Some(wall_now)),
            DomainPace::Paced => {
                let latest_tick = state.ticks.lock().back().cloned();
                if let Some(clock) = state.clock.as_ref() {
                    current_domain_logical_time(clock, latest_tick.as_ref(), wall_now).map(Some)
                } else {
                    Ok(latest_tick.map(|tick| tick.logical_timestamp))
                }
            }
        }
    }

    pub(in crate::runtime) fn touch_stream_key(
        &self,
        domain: &Domain,
        relay: &Identifier,
        key: &Option<BranchKey>,
        now: Timestamp,
    ) {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Materializer,
            relay,
            None,
        );
        if let Some(state) = self.expiring_stream_states.get(&placement) {
            state.touch(key, now);
        }
    }

    pub(in crate::runtime) fn remove_stream_key_presence(
        &self,
        domain: &Domain,
        relay: &Identifier,
        key: &Option<BranchKey>,
    ) {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Materializer,
            relay,
            None,
        );
        if let Some(state) = self.expiring_stream_states.get(&placement) {
            state.remove(key);
        }
    }

    pub(in crate::runtime) async fn ingest_stream_boundary_message(
        &self,
        domain: &Domain,
        relay: &Identifier,
        registry: &RelayRegistry,
        services: &RelayBoundaryServices,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let now = self
            .current_stream_expiration_time(domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp);
        registry.touch(&batch.key, now);
        self.touch_stream_key(domain, relay, &batch.key, now);
        self.metrics.observe_global_stream_received(
            domain,
            relay,
            self.local_node_id.read().as_deref(),
            batch.message_count(),
            batch.estimated_bytes(),
            batch.domain_timestamp(),
        );
        self.mark_branch_aggregated_metrics_updated(domain, ModelKind::Relay, relay);
        let physical_node_id = self.local_node_id.read().clone();
        services
            .ingest_message(
                &self.metrics,
                domain,
                relay,
                physical_node_id.as_deref(),
                batch,
            )
            .await
    }

    pub(in crate::runtime) async fn inject_remote_stream_boundary_message(
        &self,
        domain: &Domain,
        relay: &Identifier,
        registry: &RelayRegistry,
        services: &RelayBoundaryServices,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let now = self
            .current_stream_expiration_time(domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp);
        registry.touch(&batch.key, now);
        self.touch_stream_key(domain, relay, &batch.key, now);
        self.metrics.observe_global_stream_received(
            domain,
            relay,
            self.local_node_id.read().as_deref(),
            batch.message_count(),
            batch.estimated_bytes(),
            batch.domain_timestamp(),
        );
        self.mark_branch_aggregated_metrics_updated(domain, ModelKind::Relay, relay);
        let physical_node_id = self.local_node_id.read().clone();
        services
            .inject_remote_message(
                &self.metrics,
                domain,
                relay,
                physical_node_id.as_deref(),
                batch,
            )
            .await
    }

    pub(in crate::runtime) fn expiring_stream_state(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Arc<ExpiringRelayState> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Materializer,
            relay,
            None,
        );
        if let Some(existing) = self.expiring_stream_states.get(&placement) {
            return existing.clone();
        }
        let state = Arc::new(ExpiringRelayState::new());
        self.expiring_stream_states.insert(placement, state.clone());
        state
    }

    pub(in crate::runtime) fn clear_expiring_stream_states_for_domain(&self, domain: &Domain) {
        let relays = self
            .expiring_stream_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in relays {
            self.expiring_stream_states.remove(&placement);
        }
    }

    pub fn handle_domain_clock_start(
        &self,
        domain: &Domain,
        logical_started_at: Timestamp,
        wall_started_at: Timestamp,
        time_rate: &str,
    ) {
        let mut entry = self
            .domains
            .entry(domain.clone())
            .or_insert_with(|| RuntimeDomainState {
                config: DomainConfig {
                    pace: DomainPace::Paced,
                    period: "1s".to_string(),
                    skew: "0ms".to_string(),
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: nervix_models::DomainStatus::Running,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
                ticks: parking_lot::Mutex::new(VecDeque::new()),
            });
        entry.clock = Some(RuntimeDomainClockState {
            logical_started_at,
            wall_started_at,
            time_rate: time_rate.to_string(),
        });
    }

    pub fn handle_domain_clock_stop(&self, domain: &Domain) {
        if let Some(mut entry) = self.domains.get_mut(domain) {
            entry.clock = None;
            entry.ticks.lock().clear();
        }
    }

    pub fn handle_domain_tick(&self, domain: &Domain, tick: &DomainTick) {
        let entry = self
            .domains
            .entry(domain.clone())
            .or_insert_with(|| RuntimeDomainState {
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    period: tick.duration_ms.to_string(),
                    skew: "0ms".to_string(),
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: nervix_models::DomainStatus::Running,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
                ticks: parking_lot::Mutex::new(VecDeque::new()),
            });
        let mut ticks = entry.ticks.lock();
        if ticks
            .back()
            .is_some_and(|observed| observed.tick_id == tick.tick_id)
        {
            return;
        }
        ticks.push_back(ObservedDomainTick {
            tick_id: tick.tick_id,
            logical_timestamp: tick.logical_timestamp,
            wall_clock: tick.wall_clock,
        });
        while ticks.len() > DOMAIN_TICK_HISTORY_LIMIT {
            ticks.pop_front();
        }
    }

    pub async fn handle_remote_stream(&self, payload: RelayPayload) -> Result<(), RuntimeError> {
        match payload.kind {
            RelayPayloadKind::Routed => self.handle_remote_stream_payload(payload).await,
            RelayPayloadKind::SubscriptionFanout => {
                self.handle_remote_subscription_payload(payload).await
            }
        }
    }

    pub(in crate::runtime) fn remote_stream_target(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<
        (
            RelayRegistry,
            Arc<RelayBoundaryServices>,
            Arc<CompiledSchema>,
        ),
        RuntimeError,
    > {
        let Some(execution) = self.executions.get(domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        if execution.passive_only {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        }
        let Some(registry) = execution.relay_registries.get(relay).cloned() else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        let Some(services) = execution.relay_services.get(relay).cloned() else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        let Some(schema) = execution.relay_schemas.get(relay).cloned() else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        Ok((registry, services, schema))
    }

    pub(in crate::runtime) async fn wait_for_remote_stream_target(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<
        (
            RelayRegistry,
            Arc<RelayBoundaryServices>,
            Arc<CompiledSchema>,
        ),
        RuntimeError,
    > {
        let deadline = Instant::now() + REMOTE_RELAY_INSTANTIATION_WAIT;
        loop {
            tokio::task::consume_budget().await;
            match self.remote_stream_target(domain, relay) {
                Ok(target) => return Ok(target),
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(error);
                    }
                }
            }
            sleep(REMOTE_RELAY_INSTANTIATION_POLL).await;
        }
    }

    pub(in crate::runtime) async fn handle_remote_stream_payload(
        &self,
        remote: RelayPayload,
    ) -> Result<(), RuntimeError> {
        let (registry, services, schema) = self
            .wait_for_remote_stream_target(&remote.domain, &remote.relay)
            .await?;
        let decoded_batch = schema
            .arrow_batch_from_ipc_bytes(&remote.batch_ipc)
            .map_err(|reason| RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason,
            })?;
        if remote.metadata.len() != decoded_batch.batch().num_rows() {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: format!(
                    "remote metadata count {} does not match batch row count {}",
                    remote.metadata.len(),
                    decoded_batch.batch().num_rows()
                ),
            });
        }
        if remote.acks.len() != decoded_batch.batch().num_rows() {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: format!(
                    "remote ack count {} does not match batch row count {}",
                    remote.acks.len(),
                    decoded_batch.batch().num_rows()
                ),
            });
        }
        let branch_key = BranchKey::from_remote_key(remote.key).map_err(|reason| {
            RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason,
            }
        })?;
        let acks = remote
            .acks
            .into_iter()
            .map(|ack| {
                if let Some(ack) = ack {
                    let (acks, completion) = self.tracked_ack_root(&remote.domain);
                    self.spawn_remote_ack_watcher(remote.domain.clone(), completion, Some(ack));
                    acks
                } else {
                    AckSet::empty()
                }
            })
            .collect::<Vec<_>>();
        let batch = RelayRecordBatch::from_runtime_batch(
            schema,
            branch_key,
            decoded_batch,
            remote
                .metadata
                .into_iter()
                .map(RuntimeRecordMetadata::from_remote)
                .collect(),
            acks,
        )
        .map_err(|reason| RuntimeError::DecodeRemoteRelay {
            domain: remote.domain.as_str().to_string(),
            relay: remote.relay.as_str().to_string(),
            reason,
        })?;
        if self
            .inject_remote_stream_boundary_message(
                &remote.domain,
                &remote.relay,
                &registry,
                &services,
                &batch,
            )
            .await
            .is_ok()
        {
            for ack in batch.acks.iter() {
                ack.ack_success();
            }
        } else {
            for ack in batch.acks.iter() {
                ack.no_ack("failed to inject remote relay message into local runtime");
            }
        }
        Ok(())
    }

    pub(in crate::runtime) async fn handle_remote_subscription_payload(
        &self,
        remote: RelayPayload,
    ) -> Result<(), RuntimeError> {
        let Some(execution) = self.executions.get(&remote.domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
            });
        };
        let Some(services) = execution.relay_services.get(&remote.relay) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
            });
        };
        let Some(schema) = execution.relay_schemas.get(&remote.relay).cloned() else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
            });
        };
        let decoded_batch = schema
            .arrow_batch_from_ipc_bytes(&remote.batch_ipc)
            .map_err(|reason| RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason,
            })?;
        if remote.metadata.len() != decoded_batch.batch().num_rows() {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: format!(
                    "remote metadata count {} does not match batch row count {}",
                    remote.metadata.len(),
                    decoded_batch.batch().num_rows()
                ),
            });
        }
        if remote.acks.len() != decoded_batch.batch().num_rows() {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: format!(
                    "remote ack count {} does not match batch row count {}",
                    remote.acks.len(),
                    decoded_batch.batch().num_rows()
                ),
            });
        }
        if remote.acks.iter().any(Option::is_some) {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: "subscription fanout payload must not carry remote ack registrations"
                    .to_string(),
            });
        }
        let branch_key = BranchKey::from_remote_key(remote.key).map_err(|reason| {
            RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason,
            }
        })?;
        let ack_count = remote.acks.len();
        let batch = RelayRecordBatch::from_runtime_batch(
            schema,
            branch_key,
            decoded_batch,
            remote
                .metadata
                .into_iter()
                .map(RuntimeRecordMetadata::from_remote)
                .collect(),
            vec![AckSet::empty(); ack_count],
        )
        .map_err(|reason| RuntimeError::DecodeRemoteRelay {
            domain: remote.domain.as_str().to_string(),
            relay: remote.relay.as_str().to_string(),
            reason,
        })?;
        services.fanout_local_subscriptions(&batch).await;
        Ok(())
    }

    pub(crate) fn handle_remote_ack_resolution(&self, ack: RemoteAckResolution) {
        if let RemoteAckOutcome::Alive = ack.outcome {
            let Some(pending) = self.pending_remote_acks.get(&ack.ack_id) else {
                warn!(
                    ack_id = ack.ack_id,
                    "received remote ack alive for unknown ack id"
                );
                return;
            };
            trace!(ack_id = ack.ack_id, "received remote ack alive");
            pending.ack_alive();
            return;
        }

        let Some((_, pending)) = self.pending_remote_acks.remove(&ack.ack_id) else {
            warn!(
                ack_id = ack.ack_id,
                "received remote ack resolution for unknown ack id"
            );
            return;
        };
        trace!(ack_id = ack.ack_id, outcome = ?ack.outcome, "resolving remote ack");
        match ack.outcome {
            RemoteAckOutcome::Ack => pending.ack_success(),
            RemoteAckOutcome::NoAck(error) => pending.no_ack(error),
            RemoteAckOutcome::Alive => unreachable!("alive ack outcome is handled before removal"),
        }
    }

    pub(in crate::runtime) fn spawn_remote_ack_watcher(
        &self,
        domain: Domain,
        completion: AckCompletion,
        ack: Option<RemoteAckRegistration>,
    ) {
        let Some(ack) = ack else {
            return;
        };
        let Some(dispatcher) = self.remote_dispatcher.read().clone() else {
            return;
        };
        tokio::spawn(async move {
            let mut completion = completion;
            loop {
                tokio::select! {
                    _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => {
                        trace!(
                            domain = domain.as_str(),
                            ack_id = ack.ack_id,
                            target_node = ack.reply_node_id,
                            "sending remote ack alive"
                        );
                        if let Err(error) = dispatcher
                            .dispatch(
                                &ack.reply_node_id,
                                Envelope::Ack(RemoteAckResolution {
                                    ack_id: ack.ack_id,
                                    outcome: RemoteAckOutcome::Alive,
                                }),
                            )
                            .await
                        {
                            warn!(
                                domain = domain.as_str(),
                                ack_id = ack.ack_id,
                                target_node = ack.reply_node_id,
                                error = %error,
                                "failed to return remote ack alive"
                            );
                        }
                    }
                    progress = completion.wait_for_progress() => {
                        match progress {
                            AckProgress::Alive => {
                                trace!(
                                    domain = domain.as_str(),
                                    ack_id = ack.ack_id,
                                    target_node = ack.reply_node_id,
                                    "forwarding remote ack alive"
                                );
                                if let Err(error) = dispatcher
                                    .dispatch(
                                        &ack.reply_node_id,
                                        Envelope::Ack(RemoteAckResolution {
                                            ack_id: ack.ack_id,
                                            outcome: RemoteAckOutcome::Alive,
                                        }),
                                    )
                                    .await
                                {
                                    warn!(
                                        domain = domain.as_str(),
                                        ack_id = ack.ack_id,
                                        target_node = ack.reply_node_id,
                                        error = %error,
                                        "failed to forward remote ack alive"
                                    );
                                }
                            }
                            AckProgress::Complete(outcome) => {
                                trace!(
                                    domain = domain.as_str(),
                                    ack_id = ack.ack_id,
                                    target_node = ack.reply_node_id,
                                    outcome = ?outcome,
                                    "sending remote ack resolution"
                                );
                                if let Err(error) = dispatcher
                                    .dispatch(
                                        &ack.reply_node_id,
                                        Envelope::Ack(RemoteAckResolution {
                                            ack_id: ack.ack_id,
                                            outcome: match outcome {
                                                AckOutcome::Ack => RemoteAckOutcome::Ack,
                                                AckOutcome::NoAck(error) => RemoteAckOutcome::NoAck(error),
                                            },
                                        }),
                                    )
                                    .await
                                {
                                    warn!(
                                        domain = domain.as_str(),
                                        ack_id = ack.ack_id,
                                        target_node = ack.reply_node_id,
                                        error = %error,
                                        "failed to return remote ack resolution"
                                    );
                                }
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    pub(in crate::runtime) async fn handle_message_error(
        &self,
        domain: &Domain,
        node_kind: &str,
        node: &Identifier,
        policies: &ErrorPolicies,
        message: RelayMessage,
        failure: MessageErrorFailure,
    ) {
        let MessageErrorFailure {
            source_route,
            reason,
            operation,
        } = failure;
        self.handle_structured_message_error(MessageErrorHandling {
            domain,
            node_kind,
            node,
            source_route: source_route.as_ref(),
            policy: &policies.message,
            message,
            error: structured_message_error(
                MessageErrorCode::External,
                reason,
                operation,
                None,
                std::iter::empty(),
            ),
            partial_output: None,
            materialized_state: HashMap::default(),
            ingest_metadata: None,
        })
        .await;
    }

    pub(in crate::runtime) async fn handle_message_error_with_policy(
        &self,
        domain: &Domain,
        node_kind: &str,
        node: &Identifier,
        policy: &MessageErrorPolicy,
        message: RelayMessage,
        failure: MessageErrorFailure,
    ) {
        let MessageErrorFailure {
            source_route,
            reason,
            operation,
        } = failure;
        self.handle_structured_message_error(MessageErrorHandling {
            domain,
            node_kind,
            node,
            source_route: source_route.as_ref(),
            policy,
            message,
            error: structured_message_error(
                MessageErrorCode::External,
                reason,
                operation,
                None,
                std::iter::empty(),
            ),
            partial_output: None,
            materialized_state: HashMap::default(),
            ingest_metadata: None,
        })
        .await;
    }

    pub(in crate::runtime) async fn handle_structured_message_error(
        &self,
        handling: MessageErrorHandling<'_>,
    ) {
        let MessageErrorHandling {
            domain,
            node_kind,
            node,
            source_route,
            policy,
            message,
            error,
            partial_output,
            materialized_state,
            ingest_metadata,
        } = handling;
        match policy {
            MessageErrorPolicy::Ignore => {
                message.acks.ack_success();
            }
            MessageErrorPolicy::Log => {
                let _ = self.events.send(RuntimeEvent::Error(format!(
                    "{} '{}' message error in domain '{}': {}",
                    node_kind,
                    node.as_str(),
                    domain.as_str(),
                    error.message
                )));
                warn!(
                    domain = domain.as_str(),
                    node_kind,
                    node = node.as_str(),
                    error_reference = %error.reference,
                    error_code = error.code.as_ref(),
                    error_operation = error.operation.as_ref(),
                    reason = %error.message,
                    "runtime node handled message error"
                );
                message.acks.no_ack(error.message);
            }
            MessageErrorPolicy::Dlq { relay, assignments } => {
                let context = MessageErrorContext {
                    domain,
                    node_kind,
                    node,
                    source_route,
                    message: &message,
                    error: &error,
                    partial_output: partial_output.as_ref(),
                    materialized_state: &materialized_state,
                    ingest_metadata,
                };
                if let Err(dispatch_error) = self
                    .dispatch_message_error_to_dlq(context, relay, assignments)
                    .await
                {
                    let _ = self.events.send(RuntimeEvent::Error(format!(
                        "{} '{}' failed to dispatch message error {} to DLQ '{}' in domain '{}': \
                         {}",
                        node_kind,
                        node.as_str(),
                        error.reference,
                        relay.as_str(),
                        domain.as_str(),
                        dispatch_error
                    )));
                    message.acks.no_ack(format!(
                        "{} '{}' failed to dispatch message error {} to DLQ '{}': {}",
                        node_kind,
                        node.as_str(),
                        error.reference,
                        relay.as_str(),
                        dispatch_error
                    ));
                }
            }
        }
    }

    pub(in crate::runtime) fn handle_general_error_for_acks<'a>(
        &self,
        domain: &Domain,
        node_kind: &str,
        node: &Identifier,
        policies: &ErrorPolicies,
        acks: impl IntoIterator<Item = &'a AckSet>,
        reason: String,
    ) {
        match policies.general {
            GeneralErrorPolicy::Ignore => {
                for ack in acks {
                    ack.ack_success();
                }
            }
            GeneralErrorPolicy::Log => {
                let _ = self.events.send(RuntimeEvent::Error(format!(
                    "{} '{}' general error in domain '{}': {}",
                    node_kind,
                    node.as_str(),
                    domain.as_str(),
                    reason
                )));
                warn!(
                    domain = domain.as_str(),
                    node_kind,
                    node = node.as_str(),
                    reason = %reason,
                    "runtime node handled general error"
                );
                for ack in acks {
                    ack.no_ack(reason.clone());
                }
            }
        }
    }

    pub(in crate::runtime) fn handle_internal_processor_error_for_acks<'a>(
        &self,
        domain: &Domain,
        node_kind: &str,
        node: &Identifier,
        _policies: &ErrorPolicies,
        acks: impl IntoIterator<Item = &'a AckSet>,
        reason: String,
    ) {
        let _ = self.events.send(RuntimeEvent::Error(format!(
            "{} '{}' internal error in domain '{}': {}",
            node_kind,
            node.as_str(),
            domain.as_str(),
            reason
        )));
        warn!(
            domain = domain.as_str(),
            node_kind,
            node = node.as_str(),
            reason = %reason,
            "runtime processor handled internal error"
        );
        for ack in acks {
            ack.no_ack(reason.clone());
        }
    }

    pub(in crate::runtime) async fn handle_planned_message_errors(
        &self,
        domain: &Domain,
        node_kind: &str,
        node: &Identifier,
        policies: &ErrorPolicies,
        errors: Vec<PlannedMessageError>,
    ) {
        for error in errors {
            self.handle_structured_message_error(MessageErrorHandling {
                domain,
                node_kind,
                node,
                source_route: None,
                policy: &policies.message,
                message: error.message,
                error: error.error,
                partial_output: error.partial_output,
                materialized_state: error.materialized_state,
                ingest_metadata: None,
            })
            .await;
        }
    }

    pub(in crate::runtime) async fn handle_planned_message_errors_with_policy(
        &self,
        domain: &Domain,
        node_kind: &str,
        node: &Identifier,
        source_route: Option<&Identifier>,
        policy: &MessageErrorPolicy,
        errors: Vec<PlannedMessageError>,
    ) {
        for error in errors {
            self.handle_structured_message_error(MessageErrorHandling {
                domain,
                node_kind,
                node,
                source_route,
                policy,
                message: error.message,
                error: error.error,
                partial_output: error.partial_output,
                materialized_state: error.materialized_state,
                ingest_metadata: None,
            })
            .await;
        }
    }

    pub(in crate::runtime) async fn dispatch_message_error_to_dlq(
        &self,
        context: MessageErrorContext<'_>,
        relay: &Identifier,
        assignments: &[Assignment],
    ) -> Result<(), String> {
        let MessageErrorContext {
            domain,
            node_kind,
            node,
            source_route,
            message,
            error,
            partial_output,
            materialized_state,
            ingest_metadata,
        } = context;
        let (schema, target, branching, program, flush_policy) = {
            let Some(execution) = self.executions.get(domain) else {
                return Err(format!("domain '{}' is not instantiated", domain.as_str()));
            };
            let schema = execution.relay_schemas.get(relay).cloned().ok_or_else(|| {
                format!(
                    "DLQ relay '{}' schema is not instantiated in domain '{}'",
                    relay.as_str(),
                    domain.as_str()
                )
            })?;
            let registry = execution
                .relay_registries
                .get(relay)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "DLQ relay '{}' is not instantiated in domain '{}'",
                        relay.as_str(),
                        domain.as_str()
                    )
                })?;
            let services = execution
                .relay_services
                .get(relay)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "DLQ relay '{}' services are not instantiated in domain '{}'",
                        relay.as_str(),
                        domain.as_str()
                    )
                })?;
            let branching = execution
                .relay_branchings
                .get(relay)
                .cloned()
                .unwrap_or_default();
            let flush_policy = Self::message_error_flush_policy(
                &execution,
                domain,
                node_kind,
                node,
                source_route,
                relay,
                assignments,
            )?;
            let schemas = Self::message_error_compile_schemas(
                &execution,
                node_kind,
                node,
                source_route,
                relay,
                assignments,
            )?;
            let program = compile_message_error_set_program(
                domain,
                node,
                assignments,
                schema.clone(),
                schemas,
                RuntimeVmCompileContext {
                    available_materialized_streams: &execution.materialized_stream_specs,
                    available_lookups: &execution.lookups,
                    current_branching: &branching,
                    current_branch_schema: None,
                    current_branch_sensitivity: None,
                    udfs: Some(&execution.udfs),
                },
            )?;
            (
                schema,
                MessageErrorRouteTarget { registry, services },
                branching,
                program,
                flush_policy,
            )
        };
        let dlq_record = Self::execute_message_error_set_program(
            &program,
            message,
            error,
            partial_output,
            materialized_state,
            ingest_metadata,
            self.current_stream_expiration_time(domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp),
        )
        .await?;
        let key = preserved_message_error_branch(&branching, &message.key, relay, error.reference)?;
        let batch = RelayRecordBatch::single(schema, key, dlq_record, AckSet::empty())?;
        if let Some(flush_policy) = flush_policy {
            self.enqueue_message_error_delivery(
                MessageErrorRouteKey {
                    domain: domain.clone(),
                    node_kind: node_kind.to_string(),
                    node: node.clone(),
                    source_route: source_route.cloned(),
                    error_relay: relay.clone(),
                },
                target,
                flush_policy,
                MessageErrorDelivery {
                    batch,
                    source_acks: vec![message.acks.clone()],
                },
            )
            .await?;
        } else {
            self.ingest_stream_boundary_message(
                domain,
                relay,
                &target.registry,
                &target.services,
                &batch,
            )
            .await
            .map_err(|_| {
                format!(
                    "DLQ relay '{}' rejected message error from {} '{}'",
                    relay.as_str(),
                    node_kind,
                    node.as_str()
                )
            })?;
            message.acks.ack_success();
        }
        Ok(())
    }

    fn message_error_flush_policy(
        execution: &DomainExecution,
        domain: &Domain,
        node_kind: &str,
        node: &Identifier,
        source_route: Option<&Identifier>,
        error_relay: &Identifier,
        assignments: &[Assignment],
    ) -> Result<Option<RuntimeFlushPolicy>, String> {
        let scheduled = execution
            .schedule
            .nodes
            .iter()
            .find(|scheduled| &scheduled.identifier == node && scheduled.kind.as_str() == node_kind)
            .ok_or_else(|| {
                format!(
                    "runtime model for {node_kind} '{}' is unavailable",
                    node.as_str()
                )
            })?;
        let outputs = match scheduled.config.as_ref() {
            Model::Ingestor(model) => &model.output_routes,
            Model::Reingestor(model) => &model.output_routes,
            Model::Junction(model) => &model.output_routes,
            Model::Deduplicator(model) => &model.output_routes,
            Model::Reorderer(model) => &model.output_routes,
            Model::WindowProcessor(model) => &model.output_routes,
            Model::Generator(model) => &model.output_routes,
            Model::Inferencer(model) => &model.output_routes,
            Model::WasmProcessor(model) => &model.output_routes,
            Model::Correlator(model) => &model.output_routes,
            Model::Emitter(model) => {
                return Self::parse_runtime_node_flush_policy(
                    domain,
                    node_kind,
                    node,
                    &model.flush_each,
                    model.max_batch_size.as_deref(),
                )
                .map(Some)
                .map_err(|error| error.to_string());
            }
            other => {
                return Err(format!(
                    "{} '{}' cannot own a message-error route",
                    other.kind().as_str(),
                    node.as_str()
                ));
            }
        };
        let output = matching_message_error_output(outputs, source_route, error_relay, assignments)
            .ok_or_else(|| {
                format!(
                    "{} '{}' message-error output route is unavailable",
                    node_kind,
                    node.as_str()
                )
            })?;
        let Some(policy) = output.flush_policy.as_ref() else {
            return Ok(None);
        };
        Self::parse_runtime_node_flush_policy(
            domain,
            node_kind,
            node,
            &policy.flush_each,
            policy.max_batch_size.as_deref(),
        )
        .map(Some)
        .map_err(|error| error.to_string())
    }

    fn message_error_compile_schemas(
        execution: &DomainExecution,
        node_kind: &str,
        node: &Identifier,
        source_route: Option<&Identifier>,
        error_relay: &Identifier,
        assignments: &[Assignment],
    ) -> Result<MessageErrorCompileSchemas, String> {
        let scheduled = execution
            .schedule
            .nodes
            .iter()
            .find(|scheduled| &scheduled.identifier == node && scheduled.kind.as_str() == node_kind)
            .ok_or_else(|| {
                format!(
                    "runtime model for {node_kind} '{}' is unavailable",
                    node.as_str()
                )
            })?;
        let relay_schema = |relay: &Identifier| {
            execution.relay_schemas.get(relay).cloned().ok_or_else(|| {
                format!(
                    "runtime schema for relay '{}' is unavailable",
                    relay.as_str()
                )
            })
        };
        let partial_output_schema = |outputs: &nervix_models::ProcessorOutputs| {
            matching_message_error_output(outputs, source_route, error_relay, assignments)
                .map(|output| relay_schema(&output.relay))
                .transpose()
        };
        let mut schemas = MessageErrorCompileSchemas {
            input: None,
            left: None,
            right: None,
            partial_output: None,
            current_branching: Vec::new(),
            allow_header_reads: false,
        };
        let mut current_branch_relay = None;
        match scheduled.config.as_ref() {
            Model::Ingestor(model) => {
                schemas.input = execution
                    .codecs
                    .get(&model.decode_using_codec)
                    .map(|codec| codec.schema())
                    .ok_or_else(|| {
                        format!(
                            "runtime codec '{}' is unavailable",
                            model.decode_using_codec.as_str()
                        )
                    })?
                    .into();
                schemas.allow_header_reads = ingest_source_supports_headers(&model.source);
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Reingestor(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("reingestor '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Junction(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("junction '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Deduplicator(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("deduplicator '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Reorderer(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("reorderer '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::WindowProcessor(model) => {
                current_branch_relay = model.from.first().cloned();
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Generator(model) => {
                current_branch_relay = Some(model.materialized_relay.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Inferencer(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("inferencer '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::WasmProcessor(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!(
                        "WASM processor '{}' has no input relay",
                        model.name.as_str()
                    )
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Correlator(model) => {
                let left = model.left.first().ok_or_else(|| {
                    format!("correlator '{}' has no left relay", model.name.as_str())
                })?;
                let right = model.right.first().ok_or_else(|| {
                    format!("correlator '{}' has no right relay", model.name.as_str())
                })?;
                schemas.left = Some(relay_schema(left)?);
                schemas.right = Some(relay_schema(right)?);
                current_branch_relay = Some(left.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Emitter(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("emitter '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = model
                    .encode_using_codec
                    .as_ref()
                    .map(|codec| {
                        execution
                            .codecs
                            .get(codec)
                            .map(|compiled| compiled.schema())
                            .ok_or_else(|| {
                                format!("runtime codec '{}' is unavailable", codec.as_str())
                            })
                    })
                    .transpose()?;
            }
            other => {
                return Err(format!(
                    "{} '{}' cannot own a message-error route",
                    other.kind().as_str(),
                    node.as_str()
                ));
            }
        }
        if let Some(relay) = current_branch_relay {
            schemas.current_branching = execution
                .relay_branchings
                .get(&relay)
                .cloned()
                .unwrap_or_default();
        }
        Ok(schemas)
    }

    pub(in crate::runtime) async fn execute_message_error_set_program(
        program: &CompiledProgramWithMaterializedInterest,
        message: &RelayMessage,
        error: &StructuredMessageError,
        partial_output: Option<&RuntimeRecord>,
        materialized_state: &HashMap<String, RuntimeValue>,
        ingest_metadata: Option<&IngestFilterMapMetadata>,
        execution_now: Timestamp,
    ) -> Result<RuntimeRecord, String> {
        let mut fields = message
            .record
            .fields()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect::<HashMap<_, _>>();
        if let Some(partial_output) = partial_output {
            for (name, value) in partial_output.fields() {
                fields.insert(format!("partial_output.{name}"), value.clone());
            }
        }
        fields.extend(
            materialized_state
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        );
        fields.insert(
            "error.reference".to_string(),
            RuntimeValue::String(error.reference.to_string()),
        );
        fields.insert(
            "error.code".to_string(),
            RuntimeValue::String(error.code.as_ref().to_string()),
        );
        fields.insert(
            "error.message".to_string(),
            RuntimeValue::String(error.message.clone()),
        );
        fields.insert(
            "error.operation".to_string(),
            RuntimeValue::String(error.operation.as_ref().to_string()),
        );
        if let Some(operation_index) = error.operation_index {
            fields.insert(
                "error.operation_index".to_string(),
                RuntimeValue::U32(operation_index),
            );
        }
        fields.insert(
            "error.fields".to_string(),
            RuntimeValue::Vec(
                error
                    .fields
                    .iter()
                    .map(|field| RuntimeValue::String(field.as_str().to_string()))
                    .collect(),
            ),
        );
        fields.insert(
            "error.occurred_at".to_string(),
            RuntimeValue::Datetime(error.occurred_at.as_datetime().fixed_offset()),
        );
        let record =
            RuntimeRecord::from_fields_with_metadata(fields, message.record.metadata().clone());
        let record =
            augment_runtime_records_with_lookup_hash_maps(vec![record], program, execution_now)
                .await?
                .into_iter()
                .next()
                .expect("one message-error input record must remain");
        let uninitialized = VmUninitializedInput {
            fields: program
                .compiled
                .input_schema
                .fields()
                .iter()
                .filter(|field| field.name().starts_with("error_output."))
                .map(|field| field.name().clone())
                .collect(),
        };
        let batch = vm_typed_batch_from_runtime_records_with_metadata_and_uninitialized(
            std::slice::from_ref(&record),
            ingest_metadata.map(std::slice::from_ref),
            &program.compiled.input_schema,
            Some(&uninitialized),
        )?;
        let result = execute_program_with_selection_in_context(
            &program.compiled,
            &batch,
            &VmExecutionContext {
                now: execution_now,
                injector: Some(IngestHeaderFunctionInjector::from_metadata(
                    ingest_metadata.map(std::slice::from_ref),
                    batch.row_count(),
                )),
            },
        )
        .await
        .map_err(|error| format!("message-error SET execution failed: {error}"))?;
        if result.batch.row_count() != 1 {
            return Err(format!(
                "message-error SET produced {} rows for one error",
                result.batch.row_count()
            ));
        }
        if let Some(side_error) = result.batch.errors().row(0).first() {
            return Err(format!(
                "message-error SET failed with {}: {} at {}",
                side_error.code.as_str(),
                side_error.message,
                side_error.span
            ));
        }
        vm_output_row_to_decoded_record(&result.batch, 0)
            .map(|record| record.into_runtime_record(message.record.metadata().clone()))
    }

    /// Builds one Arrow batch per (relay, branch key) from everything a poll group routed
    /// and forwards each to its branch entrypoint.
    ///
    /// This is the counterpart to `IngestGroupDispatch::collector`. Building the batch once per
    /// group replaces N single-row batch constructions, N channel sends, and the
    /// `spawn_blocking` hop the route task pays per message.
    pub(in crate::runtime) async fn flush_ingest_collector(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        branched_senders: &HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
        collector: &mut IngestRouteCollector,
    ) -> Result<(), String> {
        if collector.is_empty() {
            return Ok(());
        }
        let groups = collector.drain_groups();
        let Some(execution) = self.executions.get(domain) else {
            let error = format!("domain '{}' is not running", domain.as_str());
            for (_, messages) in &groups {
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor.as_str(),
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    messages.iter().map(|message| &message.acks),
                    error.clone(),
                );
            }
            return Err(error);
        };
        let relay_schemas = execution.relay_schemas.clone();
        drop(execution);

        let mut first_error = None;
        for (relay, messages) in groups {
            tokio::task::consume_budget().await;
            let acks = messages
                .iter()
                .map(|message| message.acks.clone())
                .collect::<Vec<_>>();
            let Some(schema) = relay_schemas.get(&relay).cloned() else {
                let error = format!(
                    "stream '{}' schema is not instantiated in domain '{}'",
                    relay.as_str(),
                    domain.as_str()
                );
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor.as_str(),
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    acks.iter(),
                    error.clone(),
                );
                first_error.get_or_insert(error);
                continue;
            };
            let batch = match RelayRecordBatch::from_messages(schema, messages) {
                Ok(batch) => batch,
                Err(error) => {
                    self.handle_general_error_for_acks(
                        domain,
                        ModelKind::Ingestor.as_str(),
                        ingestor,
                        &ErrorPolicies::handled_by_log(),
                        acks.iter(),
                        error.clone(),
                    );
                    first_error.get_or_insert(error);
                    continue;
                }
            };
            let Some(sender) = branched_senders.get(&relay) else {
                let error = format!(
                    "ingestor '{}' has no branch entrypoint for relay '{}'",
                    ingestor.as_str(),
                    relay.as_str()
                );
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor.as_str(),
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    batch.acks.iter(),
                    error.clone(),
                );
                first_error.get_or_insert(error);
                continue;
            };
            if let Err(error) = sender.send(batch).await {
                let batch = error.0;
                let reason = format!(
                    "ingestor '{}' failed to forward batch to branch entrypoint for relay '{}'",
                    ingestor.as_str(),
                    relay.as_str()
                );
                self.handle_general_error_for_acks(
                    domain,
                    ModelKind::Ingestor.as_str(),
                    ingestor,
                    &ErrorPolicies::handled_by_log(),
                    batch.acks.iter(),
                    reason.clone(),
                );
                first_error.get_or_insert(reason);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Dispatches a whole poll group of ingested messages.
    ///
    /// The ingestor `FILTER WHERE` runs once over the group and each route's filter-map
    /// runs once over the rows that survived it, so the columnar VM is entered a fixed
    /// number of times per group instead of twice per record. Records, ingest metadata
    /// and acks stay row-aligned throughout: that is what lets a message error still
    /// name the record that produced it and lets every record keep its own acks.
    pub(in crate::runtime) async fn dispatch_ingested_records(
        &self,
        dispatch: IngestGroupDispatch<'_>,
    ) -> Result<(), String> {
        let IngestGroupDispatch {
            domain,
            ingestor,
            timestamp_source,
            output_routes,
            filter_where,
            records,
            metadata,
            mut acks,
            ingested_at,
            collector,
        } = dispatch;
        if records.is_empty() {
            return Ok(());
        }
        if !metadata.is_empty() && metadata.len() != records.len() {
            return Err(format!(
                "ingestor '{}' received {} ingest metadata rows for {} records",
                ingestor.as_str(),
                metadata.len(),
                records.len()
            ));
        }
        if acks.len() != records.len() {
            return Err(format!(
                "ingestor '{}' received {} ack sets for {} records",
                ingestor.as_str(),
                acks.len(),
                records.len()
            ));
        }

        // Sources that do not track acks themselves still need a root for downstream
        // resolution to land on. Those completions are deliberately never observed.
        let mut _unobserved_completions = Vec::new();
        for slot in acks.iter_mut().filter(|slot| slot.is_empty()) {
            let (tracked, completion) = self.tracked_ack_root(domain);
            *slot = tracked;
            _unobserved_completions.push(completion);
        }
        let mut rows = IngestGroupRows {
            records: records
                .into_iter()
                .map(|record| {
                    record.into_runtime_record(RuntimeRecordMetadata::from_ingested_at_watermarks(
                        ingested_at,
                        ingested_at,
                    ))
                })
                .collect(),
            metadata,
            acks,
        };

        // One execution clock for the whole group: a batch is evaluated against the
        // state it was admitted with.
        let execution_now = self
            .current_stream_expiration_time(domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp);

        if let Some(filter_where) = filter_where {
            let side_inputs = self
                .load_materialized_side_inputs(
                    domain,
                    &None,
                    &filter_where.materialized_interest,
                    &self
                        .executions
                        .get(domain)
                        .map(|execution| execution.materialized_stream_owner_nodes.clone())
                        .unwrap_or_default(),
                )
                .await?;
            let outcomes = evaluate_filter_map_on_records(
                filter_where,
                augment_runtime_records_with_side_inputs(rows.records.clone(), &side_inputs),
                None,
                rows.metadata_rows(),
                execution_now,
            )
            .await?;
            let mut keep = vec![false; rows.len()];
            let mut transformed = Vec::new();
            for (row, outcome) in outcomes.into_iter().enumerate() {
                tokio::task::consume_budget().await;
                match outcome {
                    SingleRecordFilterMapOutcome::Filtered => rows.acks[row].ack_success(),
                    SingleRecordFilterMapOutcome::Output(record) => {
                        keep[row] = true;
                        transformed.push((row, record));
                    }
                    SingleRecordFilterMapOutcome::MessageError {
                        error,
                        materialized_state,
                        ..
                    } => {
                        let acks = std::mem::replace(&mut rows.acks[row], AckSet::empty());
                        self.handle_ingestor_filter_where_error(IngestorFilterWhereError {
                            domain,
                            ingestor,
                            output_routes,
                            record: &rows.records[row],
                            ingest_metadata: rows.metadata_row(row),
                            acks,
                            error,
                            materialized_state,
                        })
                        .await;
                    }
                }
            }
            for (row, record) in transformed {
                rows.records[row] = record;
            }
            rows = rows.select(&keep);
        }
        if rows.is_empty() {
            return Ok(());
        }

        let Some(execution) = self.executions.get(domain) else {
            return Err(format!("domain '{}' is not instantiated", domain.as_str()));
        };
        let owner_nodes = execution.materialized_stream_owner_nodes.clone();
        drop(execution);

        // Timestamp resolution and admission stay per record: `TIMESTAMP AT` reads a
        // field of the record itself, and a paced domain admits each event on its own
        // merits. Either rejection fails the group, exactly as the per-record path did.
        let mut event_timestamps = Vec::with_capacity(rows.len());
        for record in &rows.records {
            let event_timestamp =
                self.resolve_ingested_record_timestamp(domain, ingestor, timestamp_source, record)?;
            self.ensure_domain_allows_ingestion(domain, ingestor, event_timestamp)?;
            event_timestamps.push(event_timestamp);
        }
        rows.records = std::mem::take(&mut rows.records)
            .into_iter()
            .zip(&event_timestamps)
            .map(|(record, event_timestamp)| record.with_ingested_at_watermarks(*event_timestamp))
            .collect();
        let physical_node_id = self.local_node_id.read().clone();
        for (record, event_timestamp) in rows.records.iter().zip(&event_timestamps) {
            self.metrics
                .observe_global_node_without_stream_received(NodeWithoutRelayObservation {
                    domain,
                    kind: ModelKind::Ingestor,
                    node: ingestor,
                    physical_node_id: physical_node_id.as_deref(),
                    messages: 1,
                    bytes: record.estimated_bytes(),
                    domain_timestamp: Some(*event_timestamp),
                });
        }
        self.mark_branch_aggregated_metrics_updated(domain, ModelKind::Ingestor, ingestor);

        // Every route filters the same surviving group in one VM execution. Outcomes are
        // transposed back onto their originating row so each record's ack split still
        // counts only the routes that actually took it.
        let mut routed = (0..rows.len()).map(|_| Vec::new()).collect::<Vec<_>>();
        for (output_index, output) in output_routes.routes.iter().enumerate() {
            tokio::task::consume_budget().await;
            let outcomes = if let Some(filter_map) = output.compiled_program.as_ref() {
                let side_inputs = self
                    .load_materialized_side_inputs(
                        domain,
                        &None,
                        &filter_map.materialized_interest,
                        &owner_nodes,
                    )
                    .await?;
                evaluate_filter_map_on_records(
                    filter_map,
                    augment_runtime_records_with_side_inputs(rows.records.clone(), &side_inputs),
                    None,
                    rows.metadata_rows(),
                    execution_now,
                )
                .await?
            } else {
                rows.records
                    .iter()
                    .cloned()
                    .map(SingleRecordFilterMapOutcome::Output)
                    .collect()
            };
            for (row, outcome) in outcomes.into_iter().enumerate() {
                if let SingleRecordFilterMapOutcome::Filtered = outcome {
                    continue;
                }
                routed[row].push((output_index, outcome));
            }
        }

        for (row, outcomes) in routed.into_iter().enumerate() {
            tokio::task::consume_budget().await;
            let acks = std::mem::replace(&mut rows.acks[row], AckSet::empty());
            if outcomes.is_empty() {
                acks.ack_success();
                continue;
            }
            let mut route_errors = Vec::new();
            let mut route_outputs = Vec::new();
            for (output_index, outcome) in outcomes {
                match outcome {
                    SingleRecordFilterMapOutcome::Filtered => {}
                    SingleRecordFilterMapOutcome::Output(record) => {
                        route_outputs.push((output_index, record));
                    }
                    SingleRecordFilterMapOutcome::MessageError {
                        error,
                        partial_output,
                        materialized_state,
                    } => {
                        route_errors.push((output_index, error, partial_output, materialized_state))
                    }
                }
            }
            let routed_count = route_errors.len() + route_outputs.len();
            let mut ack_queue = VecDeque::with_capacity(routed_count);
            for _ in 1..routed_count {
                ack_queue.push_back(acks.attached());
            }
            ack_queue.push_front(acks);
            for (output_index, error, partial_output, materialized_state) in route_errors {
                let acks = ack_queue
                    .pop_front()
                    .expect("ack queue must match ingestor route outcomes");
                let output = &output_routes.routes[output_index];
                self.handle_structured_message_error(MessageErrorHandling {
                    domain,
                    node_kind: ModelKind::Ingestor.as_str(),
                    node: ingestor,
                    source_route: Some(&output.relay),
                    policy: &output.message_error_policy,
                    message: RelayMessage {
                        key: None,
                        record: rows.records[row].clone(),
                        acks,
                    },
                    error,
                    partial_output,
                    materialized_state,
                    ingest_metadata: rows.metadata_row(row),
                })
                .await;
            }
            for (output_index, output_record) in route_outputs {
                let acks = ack_queue
                    .pop_front()
                    .expect("ack queue must match ingestor route outcomes");
                let output = &output_routes.routes[output_index];
                let relay = output.relay.clone();
                let key = match output.branch.as_ref().ok_or_else(|| {
                    format!(
                        "ingestor '{}' output '{}' has no branch declaration",
                        ingestor.as_str(),
                        relay.as_str()
                    )
                })? {
                    nervix_models::OutputBranch::Unbranched => None,
                    nervix_models::OutputBranch::BranchedBy { assignments, .. } => {
                        match planning::resolve_concrete_branch_from_assignments_blocking(
                            &output_record,
                            Some(&rows.records[row]),
                            None,
                            assignments,
                            ingestor,
                            self.udf_executor(domain).as_ref(),
                        )
                        .await
                        {
                            Ok(branch) => branch.into_relay_key(),
                            Err(reason) => {
                                self.handle_structured_message_error(MessageErrorHandling {
                                    domain,
                                    node_kind: ModelKind::Ingestor.as_str(),
                                    node: ingestor,
                                    source_route: Some(&relay),
                                    policy: &output.message_error_policy,
                                    message: RelayMessage {
                                        key: None,
                                        record: rows.records[row].clone(),
                                        acks,
                                    },
                                    error: structured_message_error(
                                        MessageErrorCode::Evaluation,
                                        reason,
                                        MessageErrorOperation::BranchSet,
                                        None,
                                        std::iter::empty(),
                                    ),
                                    partial_output: Some(output_record),
                                    materialized_state: HashMap::default(),
                                    ingest_metadata: rows.metadata_row(row),
                                })
                                .await;
                                continue;
                            }
                        }
                    }
                };
                collector.push(
                    relay,
                    RelayMessage {
                        key,
                        record: output_record,
                        acks,
                    },
                );
            }
        }
        Ok(())
    }

    /// Fans an ingestor `FILTER WHERE` message error out to every output route's error
    /// policy, splitting acks the same way a routed message would have.
    async fn handle_ingestor_filter_where_error(&self, handling: IngestorFilterWhereError<'_>) {
        let IngestorFilterWhereError {
            domain,
            ingestor,
            output_routes,
            record,
            ingest_metadata,
            acks,
            error,
            materialized_state,
        } = handling;
        let route_count = output_routes.routes.len();
        if route_count == 0 {
            acks.no_ack(error.message);
            return;
        }
        let mut ack_queue = VecDeque::with_capacity(route_count);
        for _ in 1..route_count {
            ack_queue.push_back(acks.attached());
        }
        ack_queue.push_front(acks);
        for output in &output_routes.routes {
            let acks = ack_queue
                .pop_front()
                .expect("ack queue must match ingestor output routes");
            self.handle_structured_message_error(MessageErrorHandling {
                domain,
                node_kind: ModelKind::Ingestor.as_str(),
                node: ingestor,
                source_route: Some(&output.relay),
                policy: &output.message_error_policy,
                message: RelayMessage {
                    key: None,
                    record: record.clone(),
                    acks,
                },
                error: error.clone(),
                partial_output: None,
                materialized_state: materialized_state.clone(),
                ingest_metadata,
            })
            .await;
        }
    }

    pub(in crate::runtime) fn resolve_ingested_record_timestamp(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        timestamp_source: Option<&IngestTimestampSource>,
        record: &RuntimeRecord,
    ) -> Result<Timestamp, String> {
        match timestamp_source {
            Some(IngestTimestampSource::Now) => Ok(record.metadata().ingested_at_low_watermark()),
            Some(IngestTimestampSource::At(timestamp_field)) => {
                match record.value(timestamp_field.as_str()) {
                    Some(RuntimeValue::Datetime(value)) => Ok(Timestamp::from(value.to_utc())),
                    Some(_) => Err(format!(
                        "TIMESTAMP field '{}' for ingestor '{}' is not DATETIME at runtime",
                        timestamp_field.as_str(),
                        ingestor.as_str()
                    )),
                    None => Err(format!(
                        "TIMESTAMP field '{}' for ingestor '{}' is missing from decoded record",
                        timestamp_field.as_str(),
                        ingestor.as_str()
                    )),
                }
            }
            None => {
                let pace = self
                    .domains
                    .get(domain)
                    .map(|state| state.config.pace)
                    .unwrap_or(DomainPace::Unpaced);
                if let DomainPace::Paced = pace {
                    Err(format!(
                        "paced domain '{}' requires ingestor '{}' to declare TIMESTAMP NOW or \
                         TIMESTAMP AT <field>",
                        domain.as_str(),
                        ingestor.as_str()
                    ))
                } else {
                    Ok(record.metadata().ingested_at_low_watermark())
                }
            }
        }
    }

    pub(in crate::runtime) fn ensure_domain_allows_ingestion(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        event_timestamp: Timestamp,
    ) -> Result<(), String> {
        let Some(domain_state) = self.domains.get(domain) else {
            return Ok(());
        };
        match domain_state.status {
            nervix_models::DomainStatus::Stopped => {
                return Err(format!(
                    "domain '{}' is stopped; ingestor '{}' cannot accept events",
                    domain.as_str(),
                    ingestor.as_str()
                ));
            }
            nervix_models::DomainStatus::Paused => {
                return Err(format!(
                    "domain '{}' is paused; ingestor '{}' cannot accept events",
                    domain.as_str(),
                    ingestor.as_str()
                ));
            }
            nervix_models::DomainStatus::Running => {}
        }
        if let DomainPace::Unpaced = domain_state.config.pace {
            return Ok(());
        }

        let skew = humantime::parse_duration(&domain_state.config.skew).map_err(|error| {
            format!(
                "invalid skew '{}' for paced domain '{}': {error}",
                domain_state.config.skew,
                domain.as_str()
            )
        })?;
        let ticks = domain_state.ticks.lock();
        if ticks.iter().any(|tick| {
            event_timestamp
                .into_datetime()
                .signed_duration_since(tick.wall_clock.into_datetime())
                .abs()
                .to_std()
                .is_ok_and(|distance| distance <= skew)
        }) {
            return Ok(());
        }
        drop(ticks);

        let period = humantime::parse_duration(&domain_state.config.period).map_err(|error| {
            format!(
                "invalid period '{}' for paced domain '{}': {error}",
                domain_state.config.period,
                domain.as_str()
            )
        })?;
        if let Some(clock) = &domain_state.clock
            && domain_clock_window_matches(clock, period, skew, event_timestamp)?
        {
            return Ok(());
        }

        Err(format!(
            "paced domain '{}' rejected ingestor '{}' event outside any tick window",
            domain.as_str(),
            ingestor.as_str()
        ))
    }

    pub(in crate::runtime) async fn initialize_domain_kafka_consumer_offsets(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        topic: &str,
        consumer: &StreamConsumer,
        state: &ReplicatedKafkaOffsetState,
        instance_idx: u64,
    ) -> Result<(u64, bool), String> {
        let (start_version, last_start) = if let Some(domain_state) = self.domains.get(domain) {
            (domain_state.start_version, domain_state.last_start.clone())
        } else {
            (0, nervix_models::DomainStartPoint::Resume)
        };
        let scheduled_partition_schedule = self.executions.get(domain).and_then(|execution| {
            execution
                .schedule
                .nodes
                .iter()
                .find(|node| node.kind == ModelKind::Ingestor && node.identifier == *ingestor)
                .and_then(|node| node.kafka_partition_schedule.clone())
        });

        let offsets = if let nervix_models::DomainStartPoint::Resume = &last_start {
            let missing_partition_timestamp = self.current_paced_domain_time(domain)?;
            KafkaIngestor::resume_offsets_from_state(
                consumer,
                topic,
                state,
                missing_partition_timestamp,
            )?
        } else {
            let timestamp = match &last_start {
                nervix_models::DomainStartPoint::Now { .. } => current_timestamp(),
                nervix_models::DomainStartPoint::At { timestamp, .. } => {
                    chrono::DateTime::parse_from_rfc3339(timestamp)
                        .map(|value| Timestamp::from(value.to_utc()))
                        .map_err(|error| {
                            format!("invalid start timestamp '{timestamp}': {error}")
                        })?
                }
                nervix_models::DomainStartPoint::Resume => unreachable!("handled above"),
            };
            KafkaIngestor::offsets_by_timestamp(consumer, topic, timestamp)?
        };
        let has_assignment = KafkaIngestor::assign_offsets_for_instance(
            consumer,
            topic,
            &offsets,
            scheduled_partition_schedule.as_ref(),
            instance_idx,
        )?;

        if let nervix_models::DomainStartPoint::Resume = &last_start {
            return Ok((start_version, has_assignment));
        }

        let concrete_offsets =
            KafkaIngestor::concrete_next_offsets_from_assignment(consumer, topic, &offsets)?;
        self.reset_domain_kafka_offsets(state, concrete_offsets)
            .await?;
        Ok((start_version, has_assignment))
    }

    pub(crate) fn current_paced_domain_time(
        &self,
        domain: &Domain,
    ) -> Result<Option<Timestamp>, String> {
        let Some(domain_state) = self.domains.get(domain) else {
            return Ok(None);
        };
        if let DomainPace::Unpaced = domain_state.config.pace {
            return Ok(None);
        }
        let wall_now = current_timestamp();
        let latest_tick = domain_state.ticks.lock().back().cloned();
        if let Some(clock) = domain_state.clock.as_ref() {
            current_domain_logical_time(clock, latest_tick.as_ref(), wall_now).map(Some)
        } else {
            Ok(latest_tick.map(|tick| tick.logical_timestamp))
        }
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.events.subscribe()
    }

    pub(in crate::runtime) async fn relay_boundary_fanout_with_capacity(
        &self,
        domain: &Domain,
        relay: &Identifier,
        use_branch_collapse: bool,
        capacity: NonZeroUsize,
    ) -> RelayBoundaryFanout {
        let key = (domain.clone(), relay.clone());
        if let Some(fanout) = self.relay_boundary_fanouts.get(&key)
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
        self.relay_boundary_fanouts.insert(key, fanout.clone());
        fanout
    }

    fn relay_capacity(
        domain: &Domain,
        relay: &Identifier,
        capacity: usize,
    ) -> Result<NonZeroUsize, RuntimeError> {
        NonZeroUsize::new(capacity).ok_or_else(|| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!("relay '{}' capacity must be greater than 0", relay.as_str()),
        })
    }

    pub(in crate::runtime) async fn domain_graph_handle(
        &self,
        domain: &Domain,
    ) -> SharedActiveGraph {
        self.domain_graphs
            .entry(domain.clone())
            .or_insert_with(|| StdArc::new(ArcSwapOption::from(None)))
            .clone()
    }

    pub(in crate::runtime) async fn clear_domain_graph_handle(&self, domain: &Domain) {
        let handle = self.domain_graphs.get(domain).map(|entry| entry.clone());
        if let Some(handle) = handle {
            handle.store(None);
        }
    }

    pub(in crate::runtime) fn start_branched_entrypoint_runtime(
        &self,
        domain: &Domain,
        identifier: &Identifier,
        branched: Option<(SharedActiveGraph, IngestorRouteTemplate)>,
    ) -> Option<Arc<IngestorRouteRuntime>> {
        branched.map(|(graph, template)| {
            IngestorRouteRuntime::new(
                self.clone(),
                domain.clone(),
                identifier.clone(),
                graph,
                template,
                self.branch_instance_expiration_scan_interval,
            )
        })
    }

    fn branched_specs_by_identifier(
        specs: &[BranchedIngestorSpec],
    ) -> HashMap<Identifier, Vec<BranchedIngestorSpec>> {
        let mut specs_by_identifier = HashMap::default();
        for spec in specs {
            specs_by_identifier
                .entry(spec.identifier.clone())
                .or_insert_with(Vec::new)
                .push(spec.clone());
        }
        specs_by_identifier
    }

    pub(in crate::runtime) fn start_branched_ingestor_runtime(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
        branched: HashMap<Identifier, (SharedActiveGraph, IngestorRouteTemplate)>,
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

    pub async fn apply_cluster_schedule(
        &self,
        local_node_id: &str,
        schedule: &ClusterSchedule,
    ) -> Result<(), RuntimeError> {
        let _lock = self.schedule_apply_lock.lock().await;
        self.apply_cluster_schedule_locked(local_node_id, schedule, true)
            .await
    }

    pub async fn apply_cluster_state(
        &self,
        local_node_id: &str,
        revision: u64,
        domains: &BTreeMap<Domain, DomainState>,
        schedule: &ClusterSchedule,
    ) -> Result<(), RuntimeError> {
        let _lock = self.schedule_apply_lock.lock().await;
        let applied_revision = self.applied_cluster_revision.load(Ordering::Acquire);
        if applied_revision != u64::MAX && revision <= applied_revision {
            return Ok(());
        }

        self.sync_domains(domains);
        self.apply_cluster_schedule_locked(local_node_id, schedule, false)
            .await?;
        self.applied_cluster_revision
            .store(revision, Ordering::Release);
        Ok(())
    }

    async fn apply_cluster_schedule_locked(
        &self,
        local_node_id: &str,
        schedule: &ClusterSchedule,
        start_ingestors: bool,
    ) -> Result<(), RuntimeError> {
        let scheduled_domains = schedule
            .domains
            .iter()
            .map(|domain| domain.domain.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let existing_domains = {
            self.executions
                .iter()
                .map(|entry| entry.key().clone())
                .collect::<std::collections::BTreeSet<_>>()
        };
        let existing_schedules = {
            self.executions
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().schedule.clone()))
                .collect::<HashMap<_, _>>()
        };
        let existing_passive_only = {
            self.executions
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().passive_only))
                .collect::<HashMap<_, _>>()
        };
        let existing_start_versions = {
            self.executions
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
                    self.domain_instantiation_errors.remove(domain);
                }
                Err(error) => {
                    self.domain_instantiation_errors
                        .insert(domain.clone(), error.to_string());
                    return Err(error);
                }
            }
        }

        for domain in &schedule.domains {
            let Some(domain_state) = self.domains.get(&domain.domain) else {
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
                            self.domain_instantiation_errors.remove(&domain.domain);
                        }
                        Err(error) => {
                            self.domain_instantiation_errors
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
    async fn apply_schedule_delta(
        &self,
        local_node_id: &str,
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
                dynamic_updates,
            } => {
                if let Err(error) = self
                    .swap_scheduled_nodes(
                        &desired.domain,
                        desired.clone(),
                        &entities,
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

    pub(super) async fn swap_scheduled_nodes(
        &self,
        domain: &Domain,
        schedule: DomainSchedule,
        entities: &[RegistryEntity],
        dynamic_updates: &[nervix_models::DynamicModelUpdate],
    ) -> Result<(), RuntimeError> {
        let desired_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        let mut relays = self.entity_pause_relays(domain, entities);
        relays.extend(Self::entity_pause_relays_for_schedule(&schedule, entities));
        relays.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        relays.dedup();
        let mut local_gate_hold = self.engage_entity_gates(
            domain,
            &relays,
            Instant::now() + self.entity_gate_deadline,
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
            .iter()
            .map(|node| ((node.kind, node.identifier.clone()), (*node.config).clone()))
            .collect::<HashMap<_, _>>();
        let local_node_id = self.local_node_id.read().clone();

        for entity in entities {
            tokio::task::consume_budget().await;
            if entity.kind == ModelKind::Relay {
                let desired_node = schedule
                    .nodes
                    .iter()
                    .find(|node| {
                        node.kind == ModelKind::Relay && node.identifier == entity.identifier
                    })
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing desired relay '{}'", entity.identifier.as_str()),
                    })?;
                let Model::Relay(desired_relay) = desired_node.config.as_ref() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "desired relay '{}' has the wrong model kind",
                            entity.identifier.as_str()
                        ),
                    });
                };
                let dropped_materialized_state = {
                    let mut execution = self.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution is unavailable for relay transition"
                                .to_string(),
                        }
                    })?;
                    let was_materialized = execution
                        .materialized_stream_specs
                        .contains_key(&entity.identifier);
                    if desired_relay.materialized_state.is_some() {
                        let schema = execution
                            .relay_schemas
                            .get(&entity.identifier)
                            .cloned()
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing schema for materialized relay '{}'",
                                    entity.identifier.as_str()
                                ),
                            })?;
                        execution.materialized_stream_specs.insert(
                            entity.identifier.clone(),
                            RuntimeMaterializedRelaySpec {
                                schema: schema.arrow_schema(),
                                sensitivity: schema.vm_sensitivity(),
                                branching: desired_node
                                    .effective_branching
                                    .clone()
                                    .unwrap_or_default(),
                            },
                        );
                        execution
                            .materialized_stream_owner_nodes
                            .insert(entity.identifier.clone(), None);
                    } else {
                        execution
                            .materialized_stream_specs
                            .remove(&entity.identifier);
                        execution
                            .materialized_stream_owner_nodes
                            .remove(&entity.identifier);
                    }
                    was_materialized && desired_relay.materialized_state.is_none()
                };
                self.bump_materializer_epoch(domain);
                if dropped_materialized_state {
                    self.purge_materialized_relay_state(domain, &entity.identifier)?;
                }
                continue;
            }
            if entity.kind == ModelKind::Ingestor {
                let desired_node = schedule
                    .nodes
                    .iter()
                    .find(|node| {
                        node.kind == ModelKind::Ingestor && node.identifier == entity.identifier
                    })
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "missing desired ingestor '{}'",
                            entity.identifier.as_str()
                        ),
                    })?;
                let Model::Ingestor(desired_ingestor) = desired_node.config.as_ref() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "desired ingestor '{}' has the wrong model kind",
                            entity.identifier.as_str()
                        ),
                    });
                };

                let key = RuntimeKey::new(domain.clone(), entity.identifier.clone());
                if self.ingestors.contains_key(&key) {
                    self.stop_ingestor(domain, &entity.identifier).await?;
                }

                if Self::scheduled_node_executes_locally(desired_node, local_node_id.as_deref()) {
                    let source_model =
                        Self::source_model_for_scheduled_ingestor(&schedule, desired_ingestor)
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing source model for swapped ingestor '{}'",
                                    entity.identifier.as_str()
                                ),
                            })?;
                    let kafka_offset_state = self.kafka_offset_state_for_memory_pressure_resume(
                        domain,
                        desired_node,
                        desired_ingestor,
                        local_node_id.as_deref(),
                    );
                    self.start_scheduled_ingestor(
                        domain,
                        source_model,
                        desired_ingestor.clone(),
                        kafka_offset_state,
                    )
                    .await?;
                }
                continue;
            }
            if entity.kind == ModelKind::Emitter {
                let desired_node = schedule
                    .nodes
                    .iter()
                    .find(|node| {
                        node.kind == ModelKind::Emitter && node.identifier == entity.identifier
                    })
                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing desired emitter '{}'", entity.identifier.as_str()),
                    })?;
                let Model::Emitter(desired_emitter) = desired_node.config.as_ref() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "desired emitter '{}' has the wrong model kind",
                            entity.identifier.as_str()
                        ),
                    });
                };
                let desired_emitter = desired_emitter.clone();
                let (old_emitter, old_task) = {
                    let mut execution = self.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution is unavailable for emitter swap".to_string(),
                        }
                    })?;
                    let old_emitter = execution
                        .schedule
                        .nodes
                        .iter()
                        .find(|node| {
                            node.kind == ModelKind::Emitter && node.identifier == entity.identifier
                        })
                        .and_then(|node| {
                            if let Model::Emitter(emitter) = node.config.as_ref() {
                                Some(emitter.clone())
                            } else {
                                None
                            }
                        })
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
                        && let Some(mut execution) = self.executions.get_mut(domain)
                    {
                        execution.emitter_tasks.insert(entity.clone(), old_task);
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason,
                    });
                }

                let executes_locally = local_node_id
                    .as_deref()
                    .is_some_and(|node_id| desired_node.executes_on(node_id));
                let spawn = {
                    let execution = self.executions.get_mut(domain).ok_or_else(|| {
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
                                execution
                                    .relay_services
                                    .get(input_relay)
                                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                        domain: domain.as_str().to_string(),
                                        reason: format!(
                                            "missing relay services for swapped emitter input '{}'",
                                            input_relay.as_str()
                                        ),
                                    })
                                    .map(|services| {
                                        (
                                            input_relay.clone(),
                                            services
                                                .add_local_runtime_consumer(desired_emitter.mode),
                                        )
                                    })
                            })
                            .collect::<Result<Vec<_>, RuntimeError>>()?;
                        let deps = self.emitter_task_deps(
                            ExecutionBuildDeps {
                                domain,
                                relay_schemas: &execution.relay_schemas,
                                relay_branchings: &execution.relay_branchings,
                                materialized_relay_specs: &execution.materialized_stream_specs,
                                materialized_relay_owner_nodes: &execution
                                    .materialized_stream_owner_nodes,
                                lookups: &execution.lookups,
                            },
                            &desired_emitter,
                        )?;
                        Some((
                            execution.shutdown.clone(),
                            execution.codecs.clone(),
                            execution.clients.clone(),
                            deps,
                            inputs,
                        ))
                    }
                };
                if let Some((shutdown, codecs, clients, deps, inputs)) = spawn {
                    let task = self.spawn_emitter_task(
                        EmitterTaskBuildDeps {
                            domain,
                            shutdown_tx: &shutdown,
                            codecs: &codecs,
                            clients: &clients,
                            deps,
                        },
                        desired_emitter,
                        inputs,
                    )?;
                    self.executions
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
                    .iter()
                    .find(|node| {
                        node.kind == ModelKind::Reingestor && node.identifier == entity.identifier
                    })
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
                let (old_tasks, old_entrypoints, shutdown) = {
                    let mut execution = self.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution is unavailable for reingestor swap"
                                .to_string(),
                        }
                    })?;
                    let old_reingestor = execution
                        .schedule
                        .nodes
                        .iter()
                        .find(|node| {
                            node.kind == ModelKind::Reingestor
                                && node.identifier == entity.identifier
                        })
                        .and_then(|node| {
                            if let Model::Reingestor(reingestor) = node.config.as_ref() {
                                Some(reingestor.clone())
                            } else {
                                None
                            }
                        })
                        .ok_or_else(|| RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing existing reingestor '{}'",
                                entity.identifier.as_str()
                            ),
                        })?;
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
                    (old_tasks, old_entrypoints, execution.shutdown.clone())
                };
                for task in old_tasks {
                    tokio::task::consume_budget().await;
                    task.abort();
                    let _ = task.await;
                }
                for runtime in old_entrypoints {
                    tokio::task::consume_budget().await;
                    runtime.shutdown().await;
                }

                if Self::scheduled_node_executes_locally(desired_node, local_node_id.as_deref()) {
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
                        let execution = self.executions.get(domain).ok_or_else(|| {
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
                        let execution = self.executions.get(domain).ok_or_else(|| {
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
                                execution
                                    .relay_services
                                    .get(relay)
                                    .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                        domain: domain.as_str().to_string(),
                                        reason: format!(
                                            "missing reingestor input relay services '{}'",
                                            relay.as_str()
                                        ),
                                    })
                                    .map(|services| {
                                        (
                                            relay.clone(),
                                            services.add_local_runtime_consumer(
                                                desired_reingestor.mode,
                                            ),
                                        )
                                    })
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
                    let mut execution = self.executions.get_mut(domain).ok_or_else(|| {
                        RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: "domain execution disappeared after reingestor spawn"
                                .to_string(),
                        }
                    })?;
                    execution
                        .branched_ingestors
                        .insert(entity.identifier.clone(), desired_entrypoint_specs);
                    execution
                        .branched_entrypoints
                        .insert(entity.identifier.clone(), entrypoints);
                    execution.reingestor_tasks.insert(entity.clone(), tasks);
                }
                continue;
            }
            if entity.kind == ModelKind::Generator {
                let desired_node = schedule
                    .nodes
                    .iter()
                    .find(|node| {
                        node.kind == ModelKind::Generator && node.identifier == entity.identifier
                    })
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
                    let _ = task.await;
                }

                if Self::scheduled_node_executes_locally(desired_node, local_node_id.as_deref()) {
                    let (shutdown, spec) = {
                        let execution = self.executions.get(domain).ok_or_else(|| {
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
                            routes.push(GeneratorTaskRouteSpec {
                                output: output.clone(),
                                program,
                                output_schema,
                                output_registry,
                                output_services,
                            });
                        }
                        (
                            execution.shutdown.clone(),
                            GeneratorTaskSpec {
                                source_relay: desired_generator.materialized_relay.clone(),
                                generator: desired_generator.clone(),
                                source_branching,
                                routes,
                            },
                        )
                    };
                    let task = self.spawn_generator_task(domain, &shutdown, spec)?;
                    self.executions
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
                .iter()
                .find(|node| node.kind == entity.kind && node.identifier == entity.identifier)
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
                let execution = self.executions.get(domain).ok_or_else(|| {
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
            let state_purges = self
                .executions
                .get(domain)
                .and_then(|execution| {
                    execution
                        .schedule
                        .nodes
                        .iter()
                        .find(|node| {
                            node.kind == entity.kind && node.identifier == entity.identifier
                        })
                        .map(|node| (*node.config).clone())
                })
                .and_then(|old_model| {
                    desired_model_index
                        .get(&(entity.kind, entity.identifier.clone()))
                        .map(|desired_model| {
                            old_model
                                .change_aspects_against(desired_model)
                                .state_purges()
                        })
                })
                .unwrap_or_default();
            for purge in state_purges {
                match purge {
                    nervix_models::StatePurge::DeduplicatorKeyspace => {
                        self.purge_deduplicator_state(domain, &entity.identifier)?;
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
                let mut execution = self.executions.get_mut(domain).ok_or_else(|| {
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

            let mut execution = self.executions.get_mut(domain).ok_or_else(|| {
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
                .as_deref()
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
                    self.branch_instance_expiration_scan_interval,
                );
                execution.node_tasks.insert(entity.clone(), task);
            }
        }

        self.apply_dynamic_model_updates(domain, dynamic_updates)
            .await?;
        self.install_state_schema_fingerprints(&schedule);
        if let Some(mut execution) = self.executions.get_mut(domain) {
            if let Some(local_node_id) = local_node_id.as_deref() {
                let remote_consumers =
                    Self::remote_runtime_consumers_for_schedule(&schedule, local_node_id);
                for (relay, services) in &execution.relay_services {
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

    pub(in crate::runtime) fn remote_runtime_consumers_for_schedule(
        schedule: &DomainSchedule,
        local_node_id: &str,
    ) -> HashMap<Identifier, Vec<RemoteRuntimeConsumer>> {
        let mut consumers = HashMap::<Identifier, Vec<RemoteRuntimeConsumer>>::new();
        let processor_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        for spec in processor_specs.processors {
            let Some(node) = schedule
                .nodes
                .iter()
                .find(|node| node.kind == spec.spec.kind && node.identifier == spec.spec.processor)
            else {
                continue;
            };
            if node.executes_on(local_node_id) {
                continue;
            }
            let Some(target_node) = node.execution_node() else {
                continue;
            };
            for relay in &spec.spec.input_relays {
                push_remote_runtime_consumer(
                    consumers.entry(relay.clone()).or_default(),
                    target_node,
                    relay,
                    spec.spec.mode,
                );
            }
        }
        for node in &schedule.nodes {
            if node.executes_on(local_node_id) {
                continue;
            }
            let Some(target_node) = node.execution_node() else {
                continue;
            };
            match node.config.as_ref() {
                Model::Emitter(emitter) => {
                    for relay in emitter.from.relays() {
                        push_remote_runtime_consumer(
                            consumers.entry(relay.clone()).or_default(),
                            target_node,
                            relay,
                            emitter.mode,
                        );
                    }
                }
                Model::Reingestor(reingestor) => {
                    for relay in reingestor.from.relays() {
                        push_remote_runtime_consumer(
                            consumers.entry(relay.clone()).or_default(),
                            target_node,
                            relay,
                            reingestor.mode,
                        );
                    }
                }
                _ => {}
            }
        }
        consumers
    }

    async fn apply_dynamic_schedule_update(
        &self,
        domain: &Domain,
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
        if let Some(mut execution) = self.executions.get_mut(domain) {
            execution.schedule = schedule;
        }
        self.force_flush_domain(domain);
        Ok(())
    }

    async fn apply_dynamic_model_updates(
        &self,
        domain: &Domain,
        updates: &[nervix_models::DynamicModelUpdate],
    ) -> Result<(), RuntimeError> {
        for update in updates {
            tokio::task::consume_budget().await;
            match update {
                nervix_models::DynamicModelUpdate::RelayCapacity { relay, capacity } => {
                    let Some(capacity) = NonZeroUsize::new(*capacity) else {
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            "rejected zero-capacity dynamic relay update"
                        );
                        continue;
                    };
                    self.set_relay_capacity(domain, relay, capacity);
                }
                nervix_models::DynamicModelUpdate::Processor { .. } => {}
                nervix_models::DynamicModelUpdate::Emitter { emitter, config } => {
                    let commands = self.executions.get(domain).and_then(|execution| {
                        execution
                            .emitter_tasks
                            .get(&RegistryEntity {
                                kind: ModelKind::Emitter,
                                identifier: emitter.clone(),
                            })
                            .map(|task| task.commands.clone())
                    });
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

    fn set_relay_capacity(&self, domain: &Domain, relay: &Identifier, capacity: NonZeroUsize) {
        let key = (domain.clone(), relay.clone());
        if let Some(fanout) = self.relay_boundary_fanouts.get(&key) {
            fanout.set_capacity(capacity);
        }
        if let Some(execution) = self.executions.get(domain)
            && let Some(services) = execution.relay_services.get(relay)
        {
            services.fanout.set_capacity(capacity);
        }
    }

    pub async fn has_websocket_endpoint(&self, host: &str, path: &str) -> bool {
        self.has_endpoint(host, path, EndpointType::Websockets)
            .await
    }

    pub async fn websocket_endpoint_signaling_protocol(
        &self,
        host: &str,
        path: &str,
    ) -> Option<Arc<CompiledSignalingProtocol>> {
        let host = normalize_http_host(host);
        self.executions.iter().find_map(|execution| {
            execution
                .endpoint_routes
                .values()
                .find(|route| {
                    route.endpoint_type == EndpointType::Websockets
                        && route.path == path
                        && route.hostnames.iter().any(|hostname| hostname == &host)
                })
                .and_then(|route| route.signaling_protocol.clone())
        })
    }

    pub(in crate::runtime) async fn signaling_protocol(
        &self,
        domain: &Domain,
        signaling_protocol: &Identifier,
    ) -> Option<Arc<CompiledSignalingProtocol>> {
        self.executions.get(domain).and_then(|execution| {
            execution
                .signaling_protocols
                .get(signaling_protocol)
                .cloned()
        })
    }

    pub async fn has_http_endpoint(&self, host: &str, path: &str) -> bool {
        self.has_endpoint(host, path, EndpointType::Http).await
    }

    pub(in crate::runtime) async fn has_endpoint(
        &self,
        host: &str,
        path: &str,
        endpoint_type: EndpointType,
    ) -> bool {
        let host = normalize_http_host(host);
        self.executions.iter().any(|execution| {
            execution.endpoint_routes.values().any(|route| {
                route.endpoint_type == endpoint_type
                    && route.path == path
                    && route.hostnames.iter().any(|hostname| hostname == &host)
            })
        })
    }

    pub(in crate::runtime) async fn rebuild_domain_from_schedule(
        &self,
        local_node_id: &str,
        domain: &Domain,
        schedule: Option<DomainSchedule>,
        start_ingestors: bool,
    ) -> Result<(), RuntimeError> {
        self.stop_domain_ingestors(domain).await;

        let desired_start_version = self
            .domains
            .get(domain)
            .map_or(0, |state| state.start_version);
        let reset_for_start = if let Some((_, existing)) = self.executions.remove(domain) {
            let reset_for_start =
                existing.passive_only || existing.start_version != desired_start_version;
            self.stop_domain_execution(domain, existing).await;
            reset_for_start
        } else {
            false
        };

        let Some(schedule) = schedule else {
            self.clear_domain_ingestor_quiescence(domain);
            self.compiled_domain_udfs.remove(domain);
            self.clear_state_schema_fingerprints(domain);
            self.clear_domain_graph_handle(domain).await;
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        };
        self.install_state_schema_fingerprints(&schedule);
        if self
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
            self.executions.insert(domain.clone(), execution);
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
        let mut materialized_stream_branch_ttls = HashMap::new();
        let mut materialized_stream_branch_capacities = HashMap::new();
        let mut schemas = HashMap::new();
        let mut wire_schemas = HashMap::new();
        let mut codecs = HashMap::new();
        let mut signaling_protocols = HashMap::new();
        let mut transports = HashMap::new();
        let mut vhosts = HashMap::new();
        let mut endpoint_specs = Vec::new();
        let mut endpoint_routes = HashMap::new();
        let mut generator_specs = Vec::new();
        let mut lookup_specs = Vec::new();
        let mut materializer_specs = Vec::new();
        let mut emitter_specs = Vec::new();
        let mut reingestor_specs = Vec::new();
        let mut ingestor_specs = Vec::new();
        let mut tasks = Vec::new();
        let mut node_tasks = HashMap::new();
        let mut emitter_tasks = HashMap::new();
        let mut generator_tasks = HashMap::new();
        let mut reingestor_tasks = HashMap::new();
        let remote_dispatcher = self.remote_dispatcher.read().clone();
        let model_index = schedule
            .nodes
            .iter()
            .map(|node| ((node.kind, node.identifier.clone()), (*node.config).clone()))
            .collect::<HashMap<_, _>>();
        for node in &schedule.nodes {
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
        let all_branched_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        let branch_relays = branch_relays_from_branched_specs(&all_branched_specs);
        let branched_specs = all_branched_specs
            .entrypoints
            .iter()
            .filter(|spec| {
                schedule
                    .nodes
                    .iter()
                    .find(|node| node.kind == spec.kind && node.identifier == spec.identifier)
                    .is_some_and(|node| node.executes_on(local_node_id))
            })
            .cloned()
            .collect::<Vec<_>>();

        for node in &schedule.nodes {
            match node.config.as_ref() {
                Model::Schema(schema) => {
                    schemas.insert(node.identifier.clone(), Arc::new(compile_schema(schema)));
                }
                Model::WireJsonSchema(wire_schema) => {
                    wire_schemas.insert(
                        (node.kind, node.identifier.clone()),
                        WireSchemaDefinition::Json(wire_schema.clone()),
                    );
                }
                Model::WireCborSchema(wire_schema) => {
                    wire_schemas.insert(
                        (node.kind, node.identifier.clone()),
                        WireSchemaDefinition::Cbor(wire_schema.clone()),
                    );
                }
                Model::WireAvroSchema(wire_schema) => {
                    wire_schemas.insert(
                        (node.kind, node.identifier.clone()),
                        WireSchemaDefinition::Avro(wire_schema.clone()),
                    );
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
                | Model::ClientIcebergRest(_) => {
                    transports.insert(node.identifier.clone(), Arc::new((*node.config).clone()));
                }
                Model::Vhost(vhost) => {
                    vhosts.insert(node.identifier.clone(), vhost.clone());
                }
                Model::Endpoint(endpoint) => {
                    endpoint_specs.push(endpoint.clone());
                }
                Model::SignalingProtocol(protocol) => {
                    signaling_protocols.insert(
                        node.identifier.clone(),
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

        for node in &schedule.nodes {
            if let Model::Codec(codec) = node.config.as_ref() {
                let Some(schema) = schemas.get(&codec.schema).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing compiled schema '{}'", codec.schema.as_str()),
                    });
                };
                let wire_schema = codec
                    .wire_schema
                    .as_ref()
                    .map(|wire_schema| {
                        let kind = codec.wire_format.wire_schema_kind().ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: "codec wire format cannot reference a wire schema"
                                    .to_string(),
                            }
                        })?;
                        wire_schemas
                            .get(&(kind, wire_schema.clone()))
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing compiled wire schema '{}'",
                                    wire_schema.as_str()
                                ),
                            })
                    })
                    .transpose()?;
                let compiled = self
                    .compile_domain_codec(domain, codec, schema, wire_schema)
                    .await?;
                codecs.insert(node.identifier.clone(), compiled);
            }
        }

        for node in &schedule.nodes {
            if let Model::Relay(relay) = node.config.as_ref() {
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
                let expiring_state = branch_relays
                    .contains(&node.identifier)
                    .then(|| self.expiring_stream_state(domain, &node.identifier));
                let capacity = Self::relay_capacity(domain, &node.identifier, relay.buffer)?;
                let fanout = self
                    .relay_boundary_fanout_with_capacity(
                        domain,
                        &node.identifier,
                        !relay.branching.is_unbranched(),
                        capacity,
                    )
                    .await;
                let registry = expiring_state
                    .as_ref()
                    .map(|state| state.registry.clone())
                    .unwrap_or_else(RelayRegistry::new);
                relay_builders.insert(
                    node.identifier.clone(),
                    RelayBoundaryBuilder {
                        fanout,
                        attached_runtime_consumer_count: 0,
                        detached_runtime_consumer_count: 0,
                        registry,
                        remote_runtime_consumers: Vec::new(),
                    },
                );
                relay_branchings.insert(
                    node.identifier.clone(),
                    node.effective_branching.clone().unwrap_or_default(),
                );
                let branching_schema = relay_branching_schema_for_runtime(
                    domain,
                    &node.identifier,
                    relay,
                    node.effective_branching_schema.as_ref(),
                    &schemas,
                )?;
                relay_branching_schemas.insert(node.identifier.clone(), branching_schema);
                relay_schemas.insert(node.identifier.clone(), schema);
                if relay.materialized_state.is_some() {
                    if let Some(branch) = relay.branching.branch() {
                        let branch_model = schedule
                            .nodes
                            .iter()
                            .find_map(|node| {
                                let Model::Branch(candidate) = node.config.as_ref() else {
                                    return None;
                                };
                                (&candidate.name == branch).then_some(candidate)
                            })
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing branch '{}' for materialized relay '{}'",
                                    branch.as_str(),
                                    node.identifier.as_str()
                                ),
                            })?;
                        let branch_ttl =
                            humantime::parse_duration(&branch_model.ttl).map_err(|error| {
                                RuntimeError::BuildDomainExecution {
                                    domain: domain.as_str().to_string(),
                                    reason: format!(
                                        "invalid branch ttl '{}' for materialized relay '{}': \
                                         {error}",
                                        branch_model.ttl,
                                        node.identifier.as_str()
                                    ),
                                }
                            })?;
                        materialized_stream_branch_ttls.insert(node.identifier.clone(), branch_ttl);
                        if let Some(eviction) = branch_model.eviction.as_ref() {
                            let capacity =
                                usize::try_from(eviction.max_instances()).map_err(|_| {
                                    RuntimeError::BuildDomainExecution {
                                        domain: domain.as_str().to_string(),
                                        reason: format!(
                                            "branch '{}' max instances {} does not fit usize for \
                                             materialized relay '{}'",
                                            branch.as_str(),
                                            eviction.max_instances(),
                                            node.identifier.as_str()
                                        ),
                                    }
                                })?;
                            materialized_stream_branch_capacities
                                .insert(node.identifier.clone(), capacity);
                        }
                    }
                    materialized_stream_specs.insert(
                        node.identifier.clone(),
                        RuntimeMaterializedRelaySpec {
                            schema: relay_schemas
                                .get(&node.identifier)
                                .expect("inserted relay schema must exist")
                                .arrow_schema(),
                            sensitivity: relay_schemas
                                .get(&node.identifier)
                                .expect("inserted relay schema must exist")
                                .vm_sensitivity(),
                            branching: node.effective_branching.clone().unwrap_or_default(),
                        },
                    );
                    materialized_stream_owner_nodes.insert(node.identifier.clone(), None);
                }
            }
        }

        for node in &schedule.nodes {
            match node.config.as_ref() {
                Model::Materializer(materializer) => {
                    let primary_node = node.execution_node().map(str::to_string);
                    materialized_stream_owner_nodes
                        .insert(materializer.relay.clone(), primary_node.clone());
                    let Some(relay) = relay_builders.get_mut(&materializer.relay) else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing materialized relay '{}'",
                                materializer.relay.as_str()
                            ),
                        });
                    };
                    let replica_nodes = node
                        .replica_nodes()
                        .into_iter()
                        .map(str::to_string)
                        .collect::<Vec<_>>();
                    let placement = self.state_placement(
                        domain,
                        RuntimeStateKind::MaterializedRelay,
                        ModelKind::Materializer,
                        &materializer.relay,
                        None,
                    );
                    if node.executes_on(local_node_id) {
                        let state = self
                            .replicated_materialized_stream_state(
                                placement,
                                primary_node,
                                replica_nodes.clone(),
                                replica_nodes.len(),
                            )
                            .map_err(|error| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: error.to_string(),
                            })?;
                        if let Some(task) = self
                            .spawn_materialized_stream_snapshot_task(&shutdown_tx, state.clone())
                        {
                            tasks.push(task);
                        }
                        materializer_specs.push(MaterializerTaskSpec {
                            relay: materializer.relay.clone(),
                            state,
                            branch_ttl: materialized_stream_branch_ttls
                                .get(&materializer.relay)
                                .copied(),
                            branch_capacity: materialized_stream_branch_capacities
                                .get(&materializer.relay)
                                .copied(),
                            receiver: relay.runtime_consumer_fan_in_for_mode(AckMode::Detached),
                        });
                    } else {
                        if let Some(primary_node) = node.execution_node() {
                            push_remote_runtime_consumer(
                                &mut relay.remote_runtime_consumers,
                                primary_node,
                                &materializer.relay,
                                AckMode::Detached,
                            );
                        }
                        if node.is_assigned_to(local_node_id) {
                            let state = self
                                .replicated_materialized_stream_state(
                                    placement,
                                    primary_node,
                                    replica_nodes.clone(),
                                    replica_nodes.len(),
                                )
                                .map_err(|error| RuntimeError::BuildDomainExecution {
                                    domain: domain.as_str().to_string(),
                                    reason: error.to_string(),
                                })?;
                            if let Some(task) = self.spawn_materialized_stream_snapshot_task(
                                &shutdown_tx,
                                state.clone(),
                            ) {
                                tasks.push(task);
                            }
                            if let Some(task) = self
                                .spawn_materialized_stream_replica_poll_task(&shutdown_tx, state)
                            {
                                tasks.push(task);
                            }
                        }
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
                        } else if let Some(assigned_node) = node.execution_node() {
                            push_remote_runtime_consumer(
                                &mut relay.remote_runtime_consumers,
                                assigned_node,
                                input_relay,
                                emitter.mode,
                            );
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
                            reingestor_specs.push((
                                reingestor.clone(),
                                from_relay.clone(),
                                receiver,
                            ));
                        } else if let Some(assigned_node) = node.execution_node() {
                            push_remote_runtime_consumer(
                                &mut relay.remote_runtime_consumers,
                                assigned_node,
                                from_relay,
                                reingestor.mode,
                            );
                        }
                    }
                }
                Model::Ingestor(ingestor) => {
                    let kafka_offset_state = if let IngestSource::Kafka {
                        offset_mode: KafkaOffsetMode::Domain,
                        ..
                    } = &ingestor.source
                    {
                        let placement = self.state_placement(
                            domain,
                            RuntimeStateKind::KafkaOffset,
                            node.kind,
                            &node.identifier,
                            None,
                        );
                        if node.is_primary_on(local_node_id) {
                            Some(
                                self.replicated_kafka_offset_state(
                                    placement,
                                    node.primary_node.clone(),
                                    node.replica_nodes()
                                        .into_iter()
                                        .map(str::to_string)
                                        .collect(),
                                    node.replica_nodes().len(),
                                )
                                .map_err(|error| {
                                    RuntimeError::BuildDomainExecution {
                                        domain: domain.as_str().to_string(),
                                        reason: error.to_string(),
                                    }
                                })?,
                            )
                        } else if node.is_assigned_to(local_node_id) {
                            let state = self
                                .replicated_kafka_offset_state(
                                    placement,
                                    node.primary_node.clone(),
                                    node.replica_nodes()
                                        .into_iter()
                                        .map(str::to_string)
                                        .collect(),
                                    node.replica_nodes().len(),
                                )
                                .map_err(|error| RuntimeError::BuildDomainExecution {
                                    domain: domain.as_str().to_string(),
                                    reason: error.to_string(),
                                })?;
                            if let Some(task) =
                                self.spawn_kafka_offset_snapshot_task(&shutdown_tx, state.clone())
                            {
                                tasks.push(task);
                            }
                            if let Some(task) =
                                self.spawn_kafka_offset_replica_poll_task(&shutdown_tx, state)
                            {
                                tasks.push(task);
                            }
                            None
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if node.executes_on(local_node_id) {
                        if let Some(state) = kafka_offset_state.as_ref()
                            && let Some(task) =
                                self.spawn_kafka_offset_snapshot_task(&shutdown_tx, state.clone())
                        {
                            tasks.push(task);
                        }
                        ingestor_specs.push((ingestor.clone(), kafka_offset_state));
                    }
                }
                _ => {}
            }
        }

        let mut processor_input_specs = Vec::new();
        for node_spec in &all_branched_specs.processors {
            let Some(node) = schedule.nodes.iter().find(|node| {
                node.kind == node_spec.spec.kind && node.identifier == node_spec.spec.processor
            }) else {
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
                } else if let Some(assigned_node) = node.execution_node() {
                    push_remote_runtime_consumer(
                        &mut relay.remote_runtime_consumers,
                        assigned_node,
                        input_relay,
                        node_spec.spec.mode,
                    );
                }
            }
            if executes_locally {
                processor_input_specs.push((node_spec.clone(), inputs));
            }
        }

        let relay_registries = relay_builders
            .iter()
            .map(|(identifier, relay)| (identifier.clone(), relay.registry.clone()))
            .collect::<HashMap<_, _>>();
        for relay in relay_registries.keys() {
            let placement = self.state_placement(
                domain,
                RuntimeStateKind::BranchAggregated,
                ModelKind::Relay,
                relay,
                None,
            );
            let state = self
                .replicated_branch_aggregated_state(
                    placement,
                    Some(local_node_id.to_string()),
                    local_node_id.to_string(),
                    Vec::new(),
                    0,
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            if let Some(task) = self.spawn_branch_aggregated_snapshot_task(&shutdown_tx, state) {
                tasks.push(task);
            }
            self.metrics
                .register_global_stream(domain, relay, Some(local_node_id));
        }
        for node in &schedule.nodes {
            let primary_node = node.execution_node().map(str::to_string).or_else(|| {
                node.executes_on(local_node_id)
                    .then(|| local_node_id.to_string())
            });
            let physical_node_id = primary_node
                .clone()
                .unwrap_or_else(|| local_node_id.to_string());
            let replica_nodes = if node.execution_node().is_some() {
                node.replica_nodes()
                    .into_iter()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let required_replica_acks = replica_nodes.len();
            let placement = self.state_placement(
                domain,
                RuntimeStateKind::BranchAggregated,
                node.kind,
                &node.identifier,
                None,
            );
            if node.executes_on(local_node_id) {
                let state = self
                    .replicated_branch_aggregated_state(
                        placement,
                        primary_node,
                        physical_node_id,
                        replica_nodes,
                        required_replica_acks,
                    )
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: error.to_string(),
                    })?;
                if let Some(task) = self.spawn_branch_aggregated_snapshot_task(&shutdown_tx, state)
                {
                    tasks.push(task);
                }
            } else if node.is_assigned_to(local_node_id) && primary_node.is_some() {
                let state = self
                    .replicated_branch_aggregated_state(
                        placement,
                        primary_node,
                        physical_node_id,
                        replica_nodes,
                        required_replica_acks,
                    )
                    .map_err(|error| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: error.to_string(),
                    })?;
                if let Some(task) =
                    self.spawn_branch_aggregated_replica_poll_task(&shutdown_tx, state)
                {
                    tasks.push(task);
                }
            }
            self.metrics.register_global_node(
                domain,
                node.kind,
                &node.identifier,
                node.execution_node().or(Some(local_node_id)),
            );
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
            let entity = RegistryEntity {
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
                    self.branch_instance_expiration_scan_interval,
                ),
            );
        }

        let lookup_runtimes = lookup_specs.iter().cloned().collect::<HashMap<_, _>>();
        let execution_build_deps = ExecutionBuildDeps {
            domain,
            relay_schemas: &relay_schemas,
            relay_branchings: &relay_branchings,
            materialized_relay_specs: &materialized_stream_specs,
            materialized_relay_owner_nodes: &materialized_stream_owner_nodes,
            lookups: &lookup_runtimes,
        };

        for (generator, source_branching, route_specs) in generator_specs {
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
                routes.push(GeneratorTaskRouteSpec {
                    output,
                    program,
                    output_schema,
                    output_registry,
                    output_services,
                });
            }
            let entity = RegistryEntity {
                kind: ModelKind::Generator,
                identifier: generator.name.clone(),
            };
            generator_tasks.insert(
                entity,
                self.spawn_generator_task(
                    domain,
                    &shutdown_tx,
                    GeneratorTaskSpec {
                        source_relay: generator.materialized_relay.clone(),
                        generator,
                        source_branching,
                        routes,
                    },
                )?,
            );
        }

        for spec in materializer_specs {
            tasks.push(self.spawn_materializer_task(domain, &shutdown_tx, spec));
        }

        for (emitter, inputs) in emitter_specs {
            let entity = RegistryEntity {
                kind: ModelKind::Emitter,
                identifier: emitter.name.clone(),
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

        for (reingestor, from_relay, receiver) in reingestor_specs {
            let entity = RegistryEntity {
                kind: ModelKind::Reingestor,
                identifier: reingestor.name.clone(),
            };
            reingestor_tasks
                .entry(entity)
                .or_insert_with(Vec::new)
                .push(self.spawn_reingestor_task(
                    domain,
                    &shutdown_tx,
                    &branched_entrypoint_senders,
                    reingestor,
                    from_relay,
                    receiver,
                )?);
        }

        self.executions.insert(
            domain.clone(),
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
                clients: transports,
                tasks,
            },
        );

        if self
            .domains
            .get(domain)
            .is_some_and(|state| !matches!(state.status, nervix_models::DomainStatus::Running))
            || !start_ingestors
        {
            return Ok(());
        }

        for (ingestor, kafka_offset_state) in ingestor_specs {
            let Some(source_model) =
                Self::source_model_for_scheduled_ingestor(&schedule, &ingestor)
            else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("missing ingestor source for '{}'", ingestor.name.as_str()),
                });
            };
            let ingestor_name = ingestor.name.clone();
            self.clear_ingestor_transient_error(domain, &ingestor_name);
            if let Err(error) = self
                .start_scheduled_ingestor(domain, source_model, ingestor, kafka_offset_state)
                .await
            {
                self.record_ingestor_transient_error(domain, &ingestor_name, error.to_string());
                self.abort_domain_execution_start(domain).await;
                return Err(error);
            }
        }

        Ok(())
    }

    pub(in crate::runtime) fn install_state_schema_fingerprints(&self, schedule: &DomainSchedule) {
        self.clear_state_schema_fingerprints(&schedule.domain);
        let start_version = self
            .domains
            .get(&schedule.domain)
            .map_or(0, |state| state.start_version);
        for node in &schedule.nodes {
            let schema_fingerprint = if node.kind == ModelKind::Materializer {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"nervix/materialized-state/start-version");
                hasher.update(&node.schema_fingerprint);
                hasher.update(&start_version.to_be_bytes());
                *hasher.finalize().as_bytes()
            } else {
                node.schema_fingerprint
            };
            self.state_schema_fingerprints.insert(
                RuntimeStateSchemaKey::new(
                    schedule.domain.clone(),
                    node.kind,
                    node.identifier.clone(),
                ),
                schema_fingerprint,
            );
        }
    }

    fn install_state_schema_fingerprints_from_graph(&self, domain: &Domain, graph: &ActiveGraph) {
        self.clear_state_schema_fingerprints(domain);
        for node in graph.nodes() {
            self.state_schema_fingerprints.insert(
                RuntimeStateSchemaKey::new(domain.clone(), node.kind, node.identifier.clone()),
                graph
                    .schema_fingerprint(node.kind, &node.identifier)
                    .unwrap_or([0; 32]),
            );
        }
    }

    fn clear_state_schema_fingerprints(&self, domain: &Domain) {
        let keys = self
            .state_schema_fingerprints
            .iter()
            .filter_map(|entry| (&entry.key().domain == domain).then(|| entry.key().clone()))
            .collect::<Vec<_>>();
        for key in keys {
            self.state_schema_fingerprints.remove(&key);
        }
    }

    pub(in crate::runtime) fn state_placement(
        &self,
        domain: &Domain,
        state: RuntimeStateKind,
        kind: ModelKind,
        identifier: &Identifier,
        branch_key: Option<BranchKey>,
    ) -> RuntimeStatePlacement {
        let schema_fingerprint =
            if let RuntimeStateKind::BranchAggregated | RuntimeStateKind::KafkaOffset = state {
                [0; 32]
            } else {
                self.state_schema_fingerprints
                    .get(&RuntimeStateSchemaKey::new(
                        domain.clone(),
                        kind,
                        identifier.clone(),
                    ))
                    .map(|fingerprint| *fingerprint)
                    .unwrap_or([0; 32])
            };
        RuntimeStatePlacement {
            domain: domain.clone(),
            state,
            kind,
            identifier: identifier.clone(),
            schema_fingerprint,
            branch_key,
        }
    }

    fn runtime_state_placement_is_current(&self, placement: &RuntimeStatePlacement) -> bool {
        let Some(current) = self
            .state_schema_fingerprints
            .get(&RuntimeStateSchemaKey::new(
                placement.domain.clone(),
                placement.kind,
                placement.identifier.clone(),
            ))
            .map(|fingerprint| *fingerprint)
        else {
            return false;
        };
        let expected = if let RuntimeStateKind::BranchAggregated | RuntimeStateKind::KafkaOffset =
            placement.state
        {
            [0; 32]
        } else {
            current
        };
        placement.schema_fingerprint == expected
    }

    pub(in crate::runtime) fn purge_stale_runtime_state(
        &self,
        domain: &Domain,
    ) -> Result<(), RuntimePersistenceError> {
        let stale_deduplicators = self
            .replicated_deduplicator_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_deduplicators {
            self.replicated_deduplicator_states.remove(&placement);
        }
        let stale_materialized = self
            .replicated_materialized_stream_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_materialized {
            self.replicated_materialized_stream_states
                .remove(&placement);
        }
        let stale_windows = self
            .replicated_window_processor_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_windows {
            self.replicated_window_processor_states.remove(&placement);
        }
        let stale_wasm = self
            .replicated_wasm_processor_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_wasm {
            self.replicated_wasm_processor_states.remove(&placement);
        }
        let stale_offsets = self
            .replicated_kafka_offset_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_offsets {
            self.replicated_kafka_offset_states.remove(&placement);
        }
        let stale_aggregates = self
            .replicated_branch_aggregated_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_aggregates {
            self.replicated_branch_aggregated_states.remove(&placement);
        }
        let stale_expiring = self
            .expiring_stream_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_expiring {
            self.expiring_stream_states.remove(&placement);
        }

        if let Some(store) = self.state_store.as_ref() {
            let current = self
                .state_schema_fingerprints
                .iter()
                .filter_map(|entry| {
                    let key = entry.key();
                    (&key.domain == domain)
                        .then(|| ((key.kind, key.identifier.clone()), *entry.value()))
                })
                .collect::<HashMap<_, _>>();
            store.purge_stale_schema_fingerprints(domain, &current)?;
        }
        Ok(())
    }

    pub async fn dispatch_websocket_payload(
        &self,
        host: &str,
        path: &str,
        payload: &[u8],
        headers: IngestHeaders,
    ) -> EndpointDispatchOutcome {
        self.dispatch_endpoint_payload(host, path, payload, headers, "websocket")
            .await
    }

    pub async fn websocket_endpoint_admission(
        &self,
        host: &str,
        path: &str,
    ) -> EndpointDispatchOutcome {
        let route_key = HttpRouteKey {
            host: normalize_http_host(host),
            path: path.to_string(),
        };
        let bindings = self
            .endpoint_bindings
            .get(&route_key)
            .map(|bindings| bindings.clone())
            .unwrap_or_default();
        let mut outcome = EndpointDispatchOutcome::default();
        let mut retry_after = Vec::new();
        for binding in &bindings {
            match binding.quiesce.endpoint_admission() {
                Ok(()) => outcome.accepted = outcome.accepted.saturating_add(1),
                Err(duration) => {
                    outcome.rejected = outcome.rejected.saturating_add(1);
                    retry_after.push(duration);
                }
            }
        }
        if outcome.accepted == 0
            && !retry_after.is_empty()
            && retry_after.iter().all(Option::is_some)
        {
            outcome.retry_after = retry_after.into_iter().flatten().max();
        }
        outcome
    }

    pub async fn dispatch_http_payload(
        &self,
        host: &str,
        path: &str,
        payload: &[u8],
        headers: IngestHeaders,
    ) -> EndpointDispatchOutcome {
        self.dispatch_endpoint_payload(host, path, payload, headers, "http")
            .await
    }

    pub(in crate::runtime) async fn dispatch_endpoint_payload(
        &self,
        host: &str,
        path: &str,
        payload: &[u8],
        headers: IngestHeaders,
        protocol: &str,
    ) -> EndpointDispatchOutcome {
        let route_key = HttpRouteKey {
            host: normalize_http_host(host),
            path: path.to_string(),
        };
        let bindings = {
            self.endpoint_bindings
                .get(&route_key)
                .map(|bindings| bindings.clone())
                .unwrap_or_default()
        };

        let mut outcome = EndpointDispatchOutcome::default();
        let mut retry_after = Vec::new();
        for binding in &bindings {
            let payload = BufferedIngestPayload::new(
                payload,
                IngestFilterMapMetadata::from_headers(headers.clone()),
            );
            match binding.quiesce.intake(0, payload, true) {
                IngestorQuiesceIntake::Dispatch(payload) => {
                    outcome.accepted = outcome.accepted.saturating_add(1);
                    self.dispatch_endpoint_binding(binding, payload, protocol)
                        .await;
                }
                IngestorQuiesceIntake::Buffered => {
                    outcome.accepted = outcome.accepted.saturating_add(1);
                }
                IngestorQuiesceIntake::Dropped => {
                    outcome.rejected = outcome.rejected.saturating_add(1);
                    retry_after.push(None);
                }
                IngestorQuiesceIntake::Rejected {
                    retry_after: binding_retry_after,
                } => {
                    outcome.rejected = outcome.rejected.saturating_add(1);
                    retry_after.push(binding_retry_after);
                }
            }
        }
        if outcome.accepted == 0
            && !retry_after.is_empty()
            && retry_after.iter().all(Option::is_some)
        {
            outcome.retry_after = retry_after.into_iter().flatten().max();
        }
        outcome
    }

    pub(in crate::runtime) async fn dispatch_endpoint_binding(
        &self,
        binding: &EndpointIngestBinding,
        payload: BufferedIngestPayload,
        protocol: &str,
    ) {
        match decode_ingested_payload(binding.codec.clone(), payload.payload()).await {
            Ok(record) => {
                let mut collector = IngestRouteCollector::default();
                let dispatch_result = self
                    .dispatch_ingested_records(IngestGroupDispatch {
                        collector: &mut collector,
                        domain: &binding.domain,
                        ingestor: &binding.ingestor,
                        timestamp_source: binding.timestamp_source.as_ref(),
                        output_routes: &binding.output_routes,
                        filter_where: binding.filter_where.as_ref(),
                        records: vec![record],
                        metadata: vec![payload.metadata().clone()],
                        ingested_at: current_timestamp(),
                        acks: vec![AckSet::empty()],
                    })
                    .await;
                let flush_result = self
                    .flush_ingest_collector(
                        &binding.domain,
                        &binding.ingestor,
                        &binding.branched_senders,
                        &mut collector,
                    )
                    .await;
                if let Err(error) = dispatch_result.and(flush_result) {
                    let _ = self.events.send(RuntimeEvent::Error(format!(
                        "failed to dispatch {protocol} message for ingestor '{}' in domain '{}': \
                         {}",
                        binding.ingestor.as_str(),
                        binding.domain.as_str(),
                        error
                    )));
                    warn!(
                        domain = binding.domain.as_str(),
                        ingestor = binding.ingestor.as_str(),
                        error = %error,
                        protocol,
                        "failed to dispatch endpoint message"
                    );
                }
            }
            Err(error) => {
                let _ = self.events.send(RuntimeEvent::Error(format!(
                    "failed to decode {protocol} message for ingestor '{}' in domain '{}': {}",
                    binding.ingestor.as_str(),
                    binding.domain.as_str(),
                    error
                )));
                warn!(
                    domain = binding.domain.as_str(),
                    ingestor = binding.ingestor.as_str(),
                    error = %error,
                    protocol,
                    "failed to decode endpoint message"
                );
            }
        }
    }

    pub(in crate::runtime) async fn dispatch_raw_ingest_payload(
        &self,
        dispatch: RawIngestDispatch<'_>,
    ) -> Result<(), String> {
        let RawIngestDispatch {
            domain,
            ingestor,
            timestamp_source,
            output_routes,
            filter_where,
            branched_senders,
            codec,
            payload,
            collector,
            flush,
        } = dispatch;
        let mut records = Vec::new();
        let mut metadata = Vec::new();
        for (source_payload, source_metadata) in payload.entries() {
            tokio::task::consume_budget().await;
            records.push(
                decode_ingested_payload(codec.clone(), source_payload)
                    .await
                    .map_err(|error| error.to_string())?,
            );
            metadata.push(source_metadata.clone());
        }
        self.dispatch_ingested_records(IngestGroupDispatch {
            collector,
            domain,
            ingestor,
            timestamp_source,
            output_routes,
            filter_where,
            records,
            metadata,
            ingested_at: current_timestamp(),
            acks: vec![AckSet::empty()],
        })
        .await
        .map_err(|error| error.to_string())?;
        if flush {
            self.flush_ingest_collector(domain, ingestor, branched_senders, collector)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    pub(crate) async fn subscribe_stream(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<RelaySubscriptionReceiver<RelayRecordBatch>, RuntimeError> {
        let Some(execution) = self.executions.get(domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        if !execution.relay_registries.contains_key(relay) {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        }
        let Some(services) = execution.relay_services.get(relay) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        Ok(services.subscription_receiver())
    }

    pub(crate) fn describe_local_stream_exists(
        &self,
        domain: &Domain,
        relay: &Identifier,
        key: &Option<BranchKey>,
    ) -> Result<bool, RuntimeError> {
        let Some(execution) = self.executions.get(domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        if !execution.relay_registries.contains_key(relay) {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        }
        let relay_registry = execution
            .relay_registries
            .get(relay)
            .expect("checked above that relay exists");
        Ok(relay_registry.contains_key(key))
    }

    pub fn describe_metrics_for(
        &self,
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
    ) -> Vec<String> {
        if let Err(error) =
            self.refresh_branch_aggregated_metrics_for_target(domain, kind, identifier)
        {
            warn!(
                domain = domain.as_str(),
                kind,
                identifier = identifier.as_str(),
                error = %error,
                "failed to refresh branch-aggregated metrics before describe"
            );
        }
        self.metrics
            .describe_global_target(domain, kind, identifier)
    }

    pub fn describe_wasm_processor_state_for(
        &self,
        domain: &Domain,
        processor: &Identifier,
    ) -> Vec<String> {
        let mut branch_count = 0_usize;
        let mut dirty_count = 0_usize;
        let mut pending_replica_count = 0_usize;
        for state in self.replicated_wasm_processor_states.iter() {
            let placement = &state.placement;
            if &placement.domain != domain
                || placement.kind != ModelKind::WasmProcessor
                || placement.identifier != *processor
            {
                continue;
            }
            branch_count += 1;
            if state.dirty.load(Ordering::SeqCst) {
                dirty_count += 1;
            }
            let current_lsm = state.current_lsm.load(Ordering::SeqCst);
            if !state.replica_quorum_satisfied(current_lsm) {
                pending_replica_count += 1;
            }
        }
        vec![
            format!("state structures: {branch_count}"),
            format!("dirty state structures: {dirty_count}"),
            format!("replica pending state structures: {pending_replica_count}"),
        ]
    }

    pub fn describe_domain_statistics(&self, domain: &Domain) -> Vec<String> {
        self.metrics.describe_domain_statistics(domain)
    }

    pub fn dataflow_domain_statistics(
        &self,
        domain: &Domain,
    ) -> nervix_dataflow_graph::DataflowStatistics {
        self.metrics.dataflow_domain_statistics(domain)
    }

    pub fn dataflow_node_statistics(
        &self,
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
    ) -> nervix_dataflow_graph::DataflowStatistics {
        self.metrics
            .dataflow_node_statistics(domain, kind, identifier)
    }

    pub fn dataflow_edge_statistics(
        &self,
        domain: &Domain,
        metric: &nervix_dataflow_graph::DataflowMetricRef,
    ) -> nervix_dataflow_graph::DataflowStatistics {
        self.metrics.dataflow_edge_statistics(domain, metric)
    }

    pub fn dataflow_relay_buffer_statistics(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> nervix_dataflow_graph::DataflowStatistics {
        self.metrics.dataflow_relay_buffer_statistics(domain, relay)
    }

    pub fn dataflow_branch_statistics(
        &self,
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
    ) -> Vec<nervix_dataflow_graph::DataflowBranchStatistics> {
        self.metrics
            .dataflow_branch_statistics(domain, kind, identifier)
    }

    pub fn dataflow_edge_branch_statistics(
        &self,
        domain: &Domain,
        metric: &nervix_dataflow_graph::DataflowMetricRef,
    ) -> Vec<nervix_dataflow_graph::DataflowBranchStatistics> {
        self.metrics.dataflow_edge_branch_statistics(domain, metric)
    }

    pub fn dataflow_relay_branch_statistics(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Vec<nervix_dataflow_graph::DataflowBranchStatistics> {
        let Some(execution) = self.executions.get(domain) else {
            return Vec::new();
        };
        let Some(registry) = execution.relay_registries.get(relay) else {
            return Vec::new();
        };
        registry
            .keys()
            .into_iter()
            .map(|branch| nervix_dataflow_graph::DataflowBranchStatistics {
                branch,
                statistics: Default::default(),
            })
            .collect()
    }

    pub fn dataflow_node_status(
        &self,
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
    ) -> (
        nervix_dataflow_graph::DataflowNodeStatus,
        Option<String>,
        Option<u64>,
    ) {
        let reconnect_wait_millis = if kind.eq_ignore_ascii_case("INGESTOR") {
            self.ingestor_reconnect_wait_millis(domain, identifier)
        } else if kind.eq_ignore_ascii_case("EMITTER") {
            self.emitter_reconnect_wait_millis(domain, identifier)
        } else {
            None
        };
        let detail = if kind.eq_ignore_ascii_case("INGESTOR") {
            self.ingestor_transient_error(domain, identifier)
                .map(|error| {
                    if let Some(backoff) = self.ingestor_reconnect_backoff(domain, identifier) {
                        format!("{error}; reconnect backoff: {backoff}")
                    } else {
                        error
                    }
                })
                .or_else(|| {
                    self.ingestor_faults
                        .is_failed(identifier)
                        .then(|| "ingestor fault injector failed source".to_string())
                })
        } else if kind.eq_ignore_ascii_case("EMITTER") {
            self.emitter_transient_error(domain, identifier)
                .map(|error| {
                    if let Some(backoff) = self.emitter_reconnect_backoff(domain, identifier) {
                        format!("{error}; reconnect backoff: {backoff}")
                    } else {
                        error
                    }
                })
                .or_else(|| {
                    self.emitter_faults
                        .fault_mode(identifier)
                        .map(|_| "emitter fault injector failed publish".to_string())
                })
        } else {
            None
        };
        if let Some(detail) = detail {
            (
                nervix_dataflow_graph::DataflowNodeStatus::Error,
                Some(detail),
                reconnect_wait_millis,
            )
        } else {
            (nervix_dataflow_graph::DataflowNodeStatus::Ok, None, None)
        }
    }

    pub fn dataflow_node_transient_state(
        &self,
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
    ) -> (Option<String>, Option<String>, Option<u64>) {
        if kind.eq_ignore_ascii_case("INGESTOR") {
            (
                self.ingestor_transient_error(domain, identifier),
                self.ingestor_reconnect_backoff(domain, identifier),
                self.ingestor_reconnect_wait_millis(domain, identifier),
            )
        } else if kind.eq_ignore_ascii_case("EMITTER") {
            (
                self.emitter_transient_error(domain, identifier),
                self.emitter_reconnect_backoff(domain, identifier),
                self.emitter_reconnect_wait_millis(domain, identifier),
            )
        } else {
            (None, None, None)
        }
    }

    pub(in crate::runtime) fn refresh_branch_aggregated_metrics_for_target(
        &self,
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
    ) -> Result<(), RuntimePersistenceError> {
        let Some(store) = &self.state_store else {
            return Ok(());
        };
        let placements = self
            .replicated_branch_aggregated_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                if &placement.domain == domain
                    && placement.kind.as_str().eq_ignore_ascii_case(kind)
                    && &placement.identifier == identifier
                {
                    Some(placement.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for placement in placements {
            let Some(state) = self.replicated_branch_aggregated_states.get(&placement) else {
                continue;
            };
            if let Some(snapshot) = store.latest_snapshot(&placement)? {
                state.restore_persisted_snapshot(&self.metrics, snapshot)?;
            }
        }
        let Ok(kind) = kind.to_ascii_lowercase().parse::<ModelKind>() else {
            return Ok(());
        };
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::BranchAggregated,
            kind,
            identifier,
            None,
        );
        if !self
            .metrics
            .has_global_target_measurements(domain, kind, identifier)
            && let Some(snapshot) = store.latest_snapshot(&placement)?
        {
            let decoded = decode_branch_aggregated_snapshot(&snapshot.payload)?;
            self.metrics.apply_global_snapshot(decoded.metrics);
        }
        Ok(())
    }

    pub fn describe_local_ingestor(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
    ) -> Result<IngestorDescribe, String> {
        let memory_backpressure_paused = self.ingestors_paused_for_memory_pressure();
        let quiesce_control = self.ingestor_quiesce_control(domain, ingestor);
        let quiesce_state = quiesce_control
            .as_ref()
            .and_then(|control| control.cause())
            .map(|cause| cause.as_str().to_string());
        let quiesce_counters = quiesce_control
            .as_ref()
            .map(|control| control.counters())
            .unwrap_or_default();
        if !self.executions.contains_key(domain) {
            if let Some(error) = self.domain_instantiation_errors.get(domain) {
                return Err(error.value().clone());
            }
            return Ok(IngestorDescribe {
                running: false,
                ready: false,
                quiesce_state: quiesce_state.clone(),
                quiesce_counters,
                memory_backpressure_paused,
                transient_error: self.ingestor_transient_error(domain, ingestor),
                reconnect_backoff: self.ingestor_reconnect_backoff(domain, ingestor),
                reconnect_wait_millis: self.ingestor_reconnect_wait_millis(domain, ingestor),
                kafka_domain_offsets: None,
            });
        }

        let key = RuntimeKey::new(domain.clone(), ingestor.clone());
        let Some(runtime) = self.ingestors.get(&key) else {
            return Ok(IngestorDescribe {
                running: false,
                ready: false,
                quiesce_state: quiesce_state.clone(),
                quiesce_counters,
                memory_backpressure_paused,
                transient_error: self.ingestor_transient_error(domain, ingestor).or_else(|| {
                    self.domain_instantiation_errors
                        .get(domain)
                        .map(|error| error.value().clone())
                }),
                reconnect_backoff: self.ingestor_reconnect_backoff(domain, ingestor),
                reconnect_wait_millis: self.ingestor_reconnect_wait_millis(domain, ingestor),
                kafka_domain_offsets: None,
            });
        };
        let Some(execution) = self.executions.get(domain) else {
            return Ok(IngestorDescribe {
                running: true,
                ready: self.ingestor_ready(domain, ingestor),
                quiesce_state: quiesce_state.clone(),
                quiesce_counters,
                memory_backpressure_paused,
                transient_error: self.ingestor_transient_error(domain, ingestor),
                reconnect_backoff: self.ingestor_reconnect_backoff(domain, ingestor),
                reconnect_wait_millis: self.ingestor_reconnect_wait_millis(domain, ingestor),
                kafka_domain_offsets: None,
            });
        };
        let scheduled_ingestor = execution.schedule.nodes.iter().find_map(|node| {
            if node.kind == ModelKind::Ingestor && node.identifier == *ingestor {
                match node.config.as_ref() {
                    Model::Ingestor(ingestor) => Some((node, ingestor.clone())),
                    _ => None,
                }
            } else {
                None
            }
        });
        let kafka_domain_offsets = match runtime.value() {
            IngestorRuntime::Background { .. } => {
                scheduled_ingestor.and_then(|(node, ingestor)| match &ingestor.source {
                    IngestSource::Kafka {
                        topic,
                        offset_mode: KafkaOffsetMode::Domain,
                        instances,
                        ..
                    } => node.kafka_partition_schedule.as_ref().map(|schedule| {
                        kafka_domain_offset_describe_from_schedule(
                            topic.as_str(),
                            *instances,
                            schedule,
                        )
                    }),
                    _ => None,
                })
            }
            IngestorRuntime::Endpoint { .. } => None,
        };
        Ok(IngestorDescribe {
            running: true,
            ready: self.ingestor_ready(domain, ingestor),
            quiesce_state,
            quiesce_counters,
            memory_backpressure_paused,
            transient_error: self.ingestor_transient_error(domain, ingestor),
            reconnect_backoff: self.ingestor_reconnect_backoff(domain, ingestor),
            reconnect_wait_millis: self.ingestor_reconnect_wait_millis(domain, ingestor),
            kafka_domain_offsets,
        })
    }

    pub fn local_materialized_stream_state(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<Vec<(String, RuntimeRecord)>, String> {
        let mut entries = Vec::new();
        for state in self.replicated_materialized_stream_states.iter() {
            let placement = state.key();
            if placement.domain == *domain
                && placement.kind == ModelKind::Materializer
                && placement.identifier == *relay
            {
                entries.extend(
                    self.visible_materialized_stream_remote_entries(placement, state.value())
                        .into_iter()
                        .map(|(key, record)| {
                            (
                                branch_key_display(&key).to_string(),
                                RuntimeRecord::from_remote(record),
                            )
                        }),
                );
            }
        }
        if !entries.is_empty() {
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            return Ok(entries);
        }
        self.local_materialized_stream_state_for_branch(domain, relay, &None)
    }

    pub(in crate::runtime) fn materialized_relay_is_scheduled(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> bool {
        self.executions
            .get(domain)
            .and_then(|execution| {
                execution
                    .materialized_stream_owner_nodes
                    .get(relay)
                    .cloned()
            })
            .flatten()
            .is_some()
    }

    pub(in crate::runtime) fn local_materialized_stream_state_for_branch(
        &self,
        domain: &Domain,
        relay: &Identifier,
        branch_key: &Option<BranchKey>,
    ) -> Result<Vec<(String, RuntimeRecord)>, String> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Materializer,
            relay,
            branch_key.clone(),
        );
        if let Some(state) = self.replicated_materialized_stream_states.get(&placement) {
            let entries = self
                .visible_materialized_stream_remote_entries(&placement, &state)
                .into_iter()
                .map(|(key, record)| {
                    (
                        branch_key_display(&key).to_string(),
                        RuntimeRecord::from_remote(record),
                    )
                })
                .collect::<Vec<_>>();
            if !entries.is_empty() || branch_key.is_none() {
                return Ok(entries);
            }
        }
        if branch_key.is_some() {
            let aggregate_placement = RuntimeStatePlacement {
                branch_key: None,
                ..placement.clone()
            };
            if let Some(state) = self
                .replicated_materialized_stream_states
                .get(&aggregate_placement)
            {
                return Ok(self
                    .visible_materialized_stream_remote_entries(&aggregate_placement, &state)
                    .into_iter()
                    .filter(|(key, _)| key == branch_key)
                    .map(|(key, record)| {
                        (
                            branch_key_display(&key).to_string(),
                            RuntimeRecord::from_remote(record),
                        )
                    })
                    .collect());
            }
        }
        if let Some(store) = &self.state_store
            && let Some(snapshot) = store
                .latest_snapshot(&placement)
                .map_err(|error| error.to_string())?
        {
            return decode_materialized_stream_snapshot(&snapshot.payload)
                .map(|entries| {
                    let mut visible = entries
                        .into_iter()
                        .map(|(key, record)| {
                            (
                                branch_key_display(&key).to_string(),
                                RuntimeRecord::from_remote(record),
                            )
                        })
                        .collect::<Vec<_>>();
                    visible.sort_by(|left, right| left.0.cmp(&right.0));
                    visible
                })
                .map_err(|error| error.to_string());
        }
        if branch_key.is_some() {
            let aggregate_placement = RuntimeStatePlacement {
                branch_key: None,
                ..placement
            };
            if let Some(store) = &self.state_store
                && let Some(snapshot) = store
                    .latest_snapshot(&aggregate_placement)
                    .map_err(|error| error.to_string())?
            {
                return decode_materialized_stream_snapshot(&snapshot.payload)
                    .map(|entries| {
                        let mut visible = entries
                            .into_iter()
                            .filter(|(key, _)| key == branch_key)
                            .map(|(key, record)| {
                                (
                                    branch_key_display(&key).to_string(),
                                    RuntimeRecord::from_remote(record),
                                )
                            })
                            .collect::<Vec<_>>();
                        visible.sort_by(|left, right| left.0.cmp(&right.0));
                        visible
                    })
                    .map_err(|error| error.to_string());
            }
        }
        Ok(Vec::new())
    }

    pub(in crate::runtime) fn visible_materialized_stream_remote_entries(
        &self,
        placement: &RuntimeStatePlacement,
        state: &ReplicatedMaterializedRelayState,
    ) -> Vec<(Option<BranchKey>, nervix_models::RemoteRuntimeRecord)> {
        let expiring_placement = self.state_placement(
            &placement.domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Materializer,
            &placement.identifier,
            None,
        );
        let scheduled = self
            .executions
            .get(&placement.domain)
            .and_then(|execution| {
                execution
                    .materialized_stream_owner_nodes
                    .get(&placement.identifier)
                    .cloned()
            })
            .flatten()
            .is_some();
        let mut entries = if !scheduled
            && let Some(expiring_state) = self.expiring_stream_states.get(&expiring_placement)
        {
            state
                .entries
                .iter()
                .filter_map(|entry| {
                    expiring_state
                        .contains_key(entry.key())
                        .then(|| (entry.key().clone(), entry.value().clone()))
                })
                .collect::<Vec<_>>()
        } else {
            state
                .entries
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect::<Vec<_>>()
        };
        entries
            .sort_by(|left, right| branch_key_display(&left.0).cmp(branch_key_display(&right.0)));
        entries
    }

    pub async fn remote_materialized_stream_state(
        &self,
        target_node_id: &str,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<Vec<(String, RuntimeRecord)>, String> {
        self.remote_materialized_stream_state_for_branch(target_node_id, domain, relay, &None)
            .await
    }

    async fn materialized_stream_state_from_owner(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<Vec<(String, RuntimeRecord)>, String> {
        let owner = self
            .executions
            .get(domain)
            .and_then(|execution| {
                execution
                    .materialized_stream_owner_nodes
                    .get(relay)
                    .cloned()
            })
            .flatten();
        let local_node_id = self.local_node_id.read().clone();
        if let Some(owner) = owner
            && local_node_id.as_deref() != Some(owner.as_str())
        {
            return self
                .remote_materialized_stream_state(&owner, domain, relay)
                .await;
        }
        self.local_materialized_stream_state(domain, relay)
    }

    pub(in crate::runtime) async fn remote_materialized_stream_state_for_branch(
        &self,
        target_node_id: &str,
        domain: &Domain,
        relay: &Identifier,
        branch_key: &Option<BranchKey>,
    ) -> Result<Vec<(String, RuntimeRecord)>, String> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Materializer,
            relay,
            branch_key.clone(),
        );
        let Some(snapshot) = self
            .request_state_sync(target_node_id, &placement, 0)
            .await?
        else {
            return Ok(Vec::new());
        };
        decode_materialized_stream_snapshot(&snapshot.payload)
            .map(|entries| {
                let mut visible = entries
                    .into_iter()
                    .map(|(key, record)| {
                        (
                            branch_key_display(&key).to_string(),
                            RuntimeRecord::from_remote(record),
                        )
                    })
                    .collect::<Vec<_>>();
                visible.sort_by(|left, right| left.0.cmp(&right.0));
                visible
            })
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn load_materialized_side_inputs(
        &self,
        domain: &Domain,
        branch_key: &Option<BranchKey>,
        interest: &MaterializedProgramInterest,
        owner_nodes: &HashMap<Identifier, Option<String>>,
    ) -> Result<HashMap<String, RuntimeValue>, String> {
        let mut values = HashMap::default();
        if interest.relays.is_empty() {
            return Ok(values);
        }

        let local_node_id = self.local_node_id.read().clone();
        for relay_interest in &interest.relays {
            tokio::task::consume_budget().await;
            let (placement_branch_key, lookup_key) = match relay_interest.key_mode {
                MaterializedLookupKeyMode::CurrentBranch => {
                    let Some(key) = branch_key.as_ref() else {
                        return Err(format!(
                            "materialized relay '{}' requires a current branch key",
                            relay_interest.relay.as_str()
                        ));
                    };
                    (Some(key.clone()), Some(key.as_str().to_string()))
                }
                MaterializedLookupKeyMode::Root => (None, None),
            };
            let owner = owner_nodes
                .get(&relay_interest.relay)
                .and_then(|node| node.as_ref())
                .cloned();
            let entries = if let Some(owner) = owner {
                if local_node_id.as_deref() == Some(owner.as_str()) {
                    self.local_materialized_stream_state_for_branch(
                        domain,
                        &relay_interest.relay,
                        &placement_branch_key,
                    )
                } else {
                    self.remote_materialized_stream_state_for_branch(
                        &owner,
                        domain,
                        &relay_interest.relay,
                        &placement_branch_key,
                    )
                    .await
                }
            } else {
                self.local_materialized_stream_state_for_branch(
                    domain,
                    &relay_interest.relay,
                    &placement_branch_key,
                )
            }?;
            let Some(record) = materialized_record_from_entries(entries, lookup_key.as_deref())
            else {
                continue;
            };
            for field in &relay_interest.fields {
                let Some(value) = record.value(field) else {
                    continue;
                };
                values.insert(
                    format!("relay_state.{}.{}", relay_interest.relay.as_str(), field),
                    value.clone(),
                );
            }
        }

        Ok(values)
    }

    pub(crate) async fn load_materialized_dependency_values(
        &self,
        domain: &Domain,
        branch_key: &Option<BranchKey>,
        relay: &Identifier,
        owner_nodes: &HashMap<Identifier, Option<String>>,
    ) -> Result<Option<HashMap<String, RuntimeValue>>, String> {
        let Some(execution) = self.executions.get(domain) else {
            return Err(format!("domain '{}' is not instantiated", domain));
        };
        let Some(spec) = execution.materialized_stream_specs.get(relay).cloned() else {
            return Err(format!(
                "materialized relay '{}' is not instantiated in domain '{}'",
                relay, domain
            ));
        };
        drop(execution);

        let (placement_branch_key, lookup_key) = if spec.branching.is_empty() {
            (None, None)
        } else {
            let Some(key) = branch_key.as_ref() else {
                return Err(format!(
                    "materialized relay '{}' requires a current branch key",
                    relay
                ));
            };
            (Some(key.clone()), Some(key.as_str().to_string()))
        };
        let owner = owner_nodes
            .get(relay)
            .and_then(|node| node.as_ref())
            .cloned();
        let local_node_id = self.local_node_id.read().clone();
        let entries = if let Some(owner) = owner {
            if local_node_id.as_deref() == Some(owner.as_str()) {
                self.local_materialized_stream_state_for_branch(
                    domain,
                    relay,
                    &placement_branch_key,
                )
            } else {
                self.remote_materialized_stream_state_for_branch(
                    &owner,
                    domain,
                    relay,
                    &placement_branch_key,
                )
                .await
            }
        } else {
            self.local_materialized_stream_state_for_branch(domain, relay, &placement_branch_key)
        }?;
        let Some(record) = materialized_record_from_entries(entries, lookup_key.as_deref()) else {
            return Ok(None);
        };
        let values = spec
            .schema
            .fields()
            .iter()
            .filter_map(|field| {
                record.value(field.name()).cloned().map(|value| {
                    (
                        format!("relay_state.{}.{}", relay.as_str(), field.name()),
                        value,
                    )
                })
            })
            .collect();
        Ok(Some(values))
    }

    pub(in crate::runtime) async fn resolve_materialized_dependencies(
        &self,
        domain: &Domain,
        branch_key: &Option<BranchKey>,
        dependencies: &[nervix_models::MaterializedStateDependency],
    ) -> Result<MaterializedDependencyResolution, String> {
        let owner_nodes = self
            .executions
            .get(domain)
            .map(|execution| execution.materialized_stream_owner_nodes.clone())
            .unwrap_or_default();
        let mut resolved = HashMap::default();
        let udfs = self.udf_executor(domain);
        for dependency in dependencies {
            tokio::task::consume_budget().await;
            if let Some(values) = self
                .load_materialized_dependency_values(
                    domain,
                    branch_key,
                    &dependency.relay,
                    &owner_nodes,
                )
                .await?
            {
                resolved.extend(values);
                continue;
            }
            match &dependency.policy {
                MaterializedStatePolicy::RequiredSkip => {
                    return Ok(MaterializedDependencyResolution::Skip);
                }
                MaterializedStatePolicy::RequiredWait => {
                    return Ok(MaterializedDependencyResolution::Wait);
                }
                MaterializedStatePolicy::Default(assignments) => {
                    for assignment in assignments {
                        if matches!(
                            assignment.value,
                            nervix_models::Expression::Literal(ModelLiteral::Null)
                        ) {
                            continue;
                        }
                        let value = planning::evaluate_constant_expression_blocking(
                            &assignment.value,
                            udfs.as_ref(),
                        )
                        .await?;
                        resolved.insert(
                            format!(
                                "relay_state.{}.{}",
                                dependency.relay, assignment.target.field
                            ),
                            value,
                        );
                    }
                }
            }
        }
        Ok(MaterializedDependencyResolution::Ready(resolved))
    }

    pub(in crate::runtime) async fn resolve_materialized_dependencies_for_batch(
        &self,
        domain: &Domain,
        input_relay: &Identifier,
        dependencies: &[nervix_models::MaterializedStateDependency],
        batch: RelayRecordBatch,
        shutdown_rx: &mut watch::Receiver<bool>,
        wait_for_required_state: bool,
    ) -> Result<Option<RelayRecordBatch>, String> {
        loop {
            tokio::task::consume_budget().await;
            let changed = self.materialized_state_changed.notified();
            match self
                .resolve_materialized_dependencies(domain, &batch.key, dependencies)
                .await?
            {
                MaterializedDependencyResolution::Ready(_values) => return Ok(Some(batch)),
                MaterializedDependencyResolution::Skip => {
                    for ack in batch.acks.iter() {
                        ack.ack_success();
                    }
                    return Ok(None);
                }
                MaterializedDependencyResolution::Wait => {
                    if !wait_for_required_state {
                        for ack in batch.acks.iter() {
                            ack.no_ack(format!(
                                "node stopped while waiting for required materialized state at \
                                 relay '{}'",
                                input_relay
                            ));
                        }
                        return Ok(None);
                    }
                    if let Some(branch_key) = batch.key.as_ref()
                        && self
                            .executions
                            .get(domain)
                            .and_then(|execution| {
                                execution.relay_registries.get(input_relay).cloned()
                            })
                            .is_some_and(|registry| !registry.contains_key(&batch.key))
                    {
                        for ack in batch.acks.iter() {
                            ack.no_ack(format!(
                                "branch was evicted while waiting for materialized state at {} \
                                 '{}'",
                                input_relay, branch_key
                            ));
                        }
                        return Ok(None);
                    }
                    tokio::select! {
                        _ = changed => {}
                        _ = sleep(self.state_replication_poll_interval) => {}
                        result = shutdown_rx.changed() => {
                            if result.is_err() || *shutdown_rx.borrow() {
                                for ack in batch.acks.iter() {
                                    ack.no_ack(format!(
                                        "node stopped while waiting for required materialized state \
                                         at relay '{}'",
                                        input_relay
                                    ));
                                }
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn describe_local_lookup(
        &self,
        domain: &Domain,
        name: &Identifier,
    ) -> Result<(CreateLookup, u64, usize), String> {
        let Some(execution) = self.executions.get(domain) else {
            if let Some(error) = self.domain_instantiation_errors.get(domain) {
                return Err(error.value().clone());
            }
            return Err(format!("domain '{}' is not instantiated", domain.as_str()));
        };
        let Some(lookup) = execution.lookups.get(name) else {
            return Err(format!(
                "lookup '{}' is not instantiated in domain '{}'",
                name.as_str(),
                domain.as_str()
            ));
        };
        Ok((
            lookup.model.clone(),
            lookup.resource_version,
            lookup.entries.len(),
        ))
    }

    pub(crate) fn udf_executor(&self, domain: &Domain) -> Option<UdfExecutor> {
        self.executions
            .get(domain)
            .map(|execution| execution.udfs.clone())
    }

    pub(crate) async fn prepare_domain_udfs(
        &self,
        mut models: Vec<CreateUdf>,
    ) -> Result<CompiledDomainUdfs, nervix_roto::UdfError> {
        models.sort_by(|left, right| left.name.cmp(&right.name));
        let executor = UdfExecutor::compile(models.clone()).await?;
        Ok(CompiledDomainUdfs { models, executor })
    }

    pub(crate) fn install_prepared_domain_udfs(
        &self,
        domain: &Domain,
        prepared: CompiledDomainUdfs,
    ) {
        self.compiled_domain_udfs.insert(domain.clone(), prepared);
    }

    async fn compile_domain_udfs(
        &self,
        domain: &Domain,
        models: Vec<CreateUdf>,
    ) -> Result<UdfExecutor, nervix_roto::UdfError> {
        let mut sorted_models = models;
        sorted_models.sort_by(|left, right| left.name.cmp(&right.name));
        if let Some(cached) = self.compiled_domain_udfs.get(domain)
            && cached.models == sorted_models
        {
            return Ok(cached.executor.clone());
        }
        let prepared = self.prepare_domain_udfs(sorted_models).await?;
        let executor = prepared.executor.clone();
        self.install_prepared_domain_udfs(domain, prepared);
        Ok(executor)
    }

    pub fn query_local_lookup(
        &self,
        domain: &Domain,
        name: &Identifier,
        key: &str,
    ) -> Result<Option<DecodedRecord>, String> {
        let Some(execution) = self.executions.get(domain) else {
            if let Some(error) = self.domain_instantiation_errors.get(domain) {
                return Err(error.value().clone());
            }
            return Err(format!("domain '{}' is not instantiated", domain.as_str()));
        };
        let Some(lookup) = execution.lookups.get(name) else {
            return Err(format!(
                "lookup '{}' is not instantiated in domain '{}'",
                name.as_str(),
                domain.as_str()
            ));
        };
        self.metrics
            .observe_global_node_without_stream_received(NodeWithoutRelayObservation {
                domain,
                kind: ModelKind::Lookup,
                node: name,
                physical_node_id: self.local_node_id.read().as_deref(),
                messages: 1,
                bytes: u64::try_from(key.len()).unwrap_or(u64::MAX),
                domain_timestamp: Some(current_timestamp()),
            });
        self.mark_branch_aggregated_metrics_updated(domain, ModelKind::Lookup, name);
        Ok(lookup.entries.get(key).cloned())
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
            ingestors::IngestorStarter::start_scheduled(
                self,
                &domain,
                source_model,
                ingestor,
                None,
            )
            .await?;
        }

        Ok(())
    }

    pub(in crate::runtime) async fn rebuild_domain_execution(
        &self,
        domain: &Domain,
        graph: Option<ActiveGraph>,
    ) -> Result<(), RuntimeError> {
        if let Some((_, existing)) = self.executions.remove(domain) {
            self.stop_domain_execution(domain, existing).await;
        }

        let Some(graph) = graph else {
            self.clear_domain_graph_handle(domain).await;
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        };
        let stopped = self
            .domains
            .get(domain)
            .is_some_and(|state| matches!(state.status, nervix_models::DomainStatus::Stopped));
        if stopped || !self.domains.contains_key(domain) {
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
        let mut wire_schemas = HashMap::new();
        let mut codecs = HashMap::new();
        let mut signaling_protocols = HashMap::new();
        let mut transports = HashMap::new();
        let mut vhosts = HashMap::new();
        let mut endpoint_specs = Vec::new();
        let mut endpoint_routes = HashMap::new();
        let mut generator_specs = Vec::new();
        let mut lookup_specs = Vec::new();
        let mut emitter_specs = Vec::new();
        let mut reingestor_specs = Vec::new();
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
            .map(|node| ((node.kind, node.identifier.clone()), (*node.config).clone()))
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
                    schemas.insert(node.identifier.clone(), Arc::new(compile_schema(schema)));
                }
                Model::WireJsonSchema(wire_schema) => {
                    wire_schemas.insert(
                        (node.kind, node.identifier.clone()),
                        WireSchemaDefinition::Json(wire_schema.clone()),
                    );
                }
                Model::WireCborSchema(wire_schema) => {
                    wire_schemas.insert(
                        (node.kind, node.identifier.clone()),
                        WireSchemaDefinition::Cbor(wire_schema.clone()),
                    );
                }
                Model::WireAvroSchema(wire_schema) => {
                    wire_schemas.insert(
                        (node.kind, node.identifier.clone()),
                        WireSchemaDefinition::Avro(wire_schema.clone()),
                    );
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
                | Model::ClientIcebergRest(_) => {
                    transports.insert(node.identifier.clone(), node.config.clone());
                }
                Model::Vhost(vhost) => {
                    vhosts.insert(node.identifier.clone(), vhost.clone());
                }
                Model::Endpoint(endpoint) => {
                    endpoint_specs.push(endpoint.clone());
                }
                Model::SignalingProtocol(protocol) => {
                    signaling_protocols.insert(
                        node.identifier.clone(),
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
                let wire_schema = codec
                    .wire_schema
                    .as_ref()
                    .map(|wire_schema| {
                        let kind = codec.wire_format.wire_schema_kind().ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: "codec wire format cannot reference a wire schema"
                                    .to_string(),
                            }
                        })?;
                        wire_schemas
                            .get(&(kind, wire_schema.clone()))
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing compiled wire schema '{}'",
                                    wire_schema.as_str()
                                ),
                            })
                    })
                    .transpose()?;
                let compiled = self
                    .compile_domain_codec(domain, codec, schema, wire_schema)
                    .await?;
                codecs.insert(node.identifier.clone(), compiled);
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
                            node.identifier.as_str()
                        ),
                    });
                };
                let expiring_state = branch_relays
                    .contains(&node.identifier)
                    .then(|| self.expiring_stream_state(domain, &node.identifier));
                let capacity = Self::relay_capacity(domain, &node.identifier, relay.buffer)?;
                let fanout = self
                    .relay_boundary_fanout_with_capacity(
                        domain,
                        &node.identifier,
                        !relay.branching.is_unbranched(),
                        capacity,
                    )
                    .await;
                let registry = expiring_state
                    .as_ref()
                    .map(|state| state.registry.clone())
                    .unwrap_or_else(RelayRegistry::new);
                relay_builders.insert(
                    node.identifier.clone(),
                    RelayBoundaryBuilder {
                        fanout,
                        attached_runtime_consumer_count: 0,
                        detached_runtime_consumer_count: 0,
                        registry,
                        remote_runtime_consumers: Vec::new(),
                    },
                );
                relay_branchings.insert(
                    node.identifier.clone(),
                    node.effective_branching.clone().unwrap_or_default(),
                );
                let branching_schema = relay_branching_schema_for_runtime(
                    domain,
                    &node.identifier,
                    relay,
                    node.effective_branching_schema.as_ref(),
                    &schemas,
                )?;
                relay_branching_schemas.insert(node.identifier.clone(), branching_schema);
                relay_schemas.insert(node.identifier.clone(), schema);
                if relay.materialized_state.is_some() {
                    materialized_stream_specs.insert(
                        node.identifier.clone(),
                        RuntimeMaterializedRelaySpec {
                            schema: relay_schemas
                                .get(&node.identifier)
                                .expect("inserted relay schema must exist")
                                .arrow_schema(),
                            sensitivity: relay_schemas
                                .get(&node.identifier)
                                .expect("inserted relay schema must exist")
                                .vm_sensitivity(),
                            branching: node.effective_branching.clone().unwrap_or_default(),
                        },
                    );
                    materialized_stream_owner_nodes.insert(node.identifier.clone(), None);
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
                        reingestor_specs.push((reingestor.clone(), from_relay.clone(), receiver));
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
            let entity = RegistryEntity {
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
                    self.branch_instance_expiration_scan_interval,
                ),
            );
        }

        let lookup_runtimes = lookup_specs.iter().cloned().collect::<HashMap<_, _>>();
        let execution_build_deps = ExecutionBuildDeps {
            domain,
            relay_schemas: &relay_schemas,
            relay_branchings: &relay_branchings,
            materialized_relay_specs: &materialized_stream_specs,
            materialized_relay_owner_nodes: &materialized_stream_owner_nodes,
            lookups: &lookup_runtimes,
        };

        for (generator, source_branching, route_specs) in generator_specs {
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
                routes.push(GeneratorTaskRouteSpec {
                    output,
                    program,
                    output_schema,
                    output_registry,
                    output_services,
                });
            }
            let entity = RegistryEntity {
                kind: ModelKind::Generator,
                identifier: generator.name.clone(),
            };
            generator_tasks.insert(
                entity,
                self.spawn_generator_task(
                    domain,
                    &shutdown_tx,
                    GeneratorTaskSpec {
                        source_relay: generator.materialized_relay.clone(),
                        generator,
                        source_branching,
                        routes,
                    },
                )?,
            );
        }

        for (emitter, inputs) in emitter_specs {
            let entity = RegistryEntity {
                kind: ModelKind::Emitter,
                identifier: emitter.name.clone(),
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

        for (reingestor, from_relay, receiver) in reingestor_specs {
            let entity = RegistryEntity {
                kind: ModelKind::Reingestor,
                identifier: reingestor.name.clone(),
            };
            reingestor_tasks
                .entry(entity)
                .or_insert_with(Vec::new)
                .push(self.spawn_reingestor_task(
                    domain,
                    &shutdown_tx,
                    &branched_entrypoint_senders,
                    reingestor,
                    from_relay,
                    receiver,
                )?);
        }

        self.executions.insert(
            domain.clone(),
            DomainExecution {
                schedule: DomainSchedule {
                    domain: domain.clone(),
                    nodes: graph
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
                        .collect(),
                    placement_groups: Vec::new(),
                },
                passive_only: false,
                start_version: self
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
                clients: transports,
                tasks,
            },
        );

        Ok(())
    }

    pub(in crate::runtime) async fn build_passive_execution_from_schedule(
        &self,
        domain: &Domain,
        schedule: &DomainSchedule,
    ) -> Result<DomainExecution, RuntimeError> {
        let udf_executor = self
            .compile_domain_udfs(
                domain,
                schedule
                    .nodes
                    .iter()
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
        let mut wire_schemas = HashMap::new();
        let mut codecs = HashMap::new();
        let mut lookups = HashMap::new();

        for node in &schedule.nodes {
            match node.config.as_ref() {
                Model::Schema(schema) => {
                    schemas.insert(node.identifier.clone(), Arc::new(compile_schema(schema)));
                }
                Model::WireJsonSchema(wire_schema) => {
                    wire_schemas.insert(
                        (node.kind, node.identifier.clone()),
                        WireSchemaDefinition::Json(wire_schema.clone()),
                    );
                }
                Model::WireCborSchema(wire_schema) => {
                    wire_schemas.insert(
                        (node.kind, node.identifier.clone()),
                        WireSchemaDefinition::Cbor(wire_schema.clone()),
                    );
                }
                Model::WireAvroSchema(wire_schema) => {
                    wire_schemas.insert(
                        (node.kind, node.identifier.clone()),
                        WireSchemaDefinition::Avro(wire_schema.clone()),
                    );
                }
                _ => {}
            }
        }

        for node in &schedule.nodes {
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
            let capacity = Self::relay_capacity(domain, &node.identifier, relay.buffer)?;
            let fanout = self
                .relay_boundary_fanout_with_capacity(
                    domain,
                    &node.identifier,
                    !relay.branching.is_unbranched(),
                    capacity,
                )
                .await;
            relay_builders.insert(
                node.identifier.clone(),
                RelayBoundaryBuilder {
                    fanout,
                    attached_runtime_consumer_count: 0,
                    detached_runtime_consumer_count: 0,
                    registry: RelayRegistry::new(),
                    remote_runtime_consumers: Vec::new(),
                },
            );
            relay_branchings.insert(
                node.identifier.clone(),
                node.effective_branching.clone().unwrap_or_default(),
            );
            let branching_schema = relay_branching_schema_for_runtime(
                domain,
                &node.identifier,
                relay,
                node.effective_branching_schema.as_ref(),
                &schemas,
            )?;
            relay_branching_schemas.insert(node.identifier.clone(), branching_schema);
            relay_schemas.insert(node.identifier.clone(), schema);
        }

        for node in &schedule.nodes {
            if let Model::Codec(codec) = node.config.as_ref() {
                let Some(schema) = schemas.get(&codec.schema).cloned() else {
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!("missing compiled schema '{}'", codec.schema.as_str()),
                    });
                };
                let wire_schema = codec
                    .wire_schema
                    .as_ref()
                    .map(|wire_schema| {
                        let kind = codec.wire_format.wire_schema_kind().ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: "codec wire format cannot reference a wire schema"
                                    .to_string(),
                            }
                        })?;
                        wire_schemas
                            .get(&(kind, wire_schema.clone()))
                            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing compiled wire schema '{}'",
                                    wire_schema.as_str()
                                ),
                            })
                    })
                    .transpose()?;
                let compiled = self
                    .compile_domain_codec(domain, codec, schema, wire_schema)
                    .await?;
                codecs.insert(node.identifier.clone(), compiled);
            }
        }

        for node in &schedule.nodes {
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
                lookups.insert(node.identifier.clone(), Arc::new(runtime));
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
        Ok(DomainExecution {
            schedule: schedule.clone(),
            passive_only: true,
            start_version: self
                .domains
                .get(domain)
                .map_or(0, |state| state.start_version),
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
            clients: HashMap::default(),
            tasks: Vec::new(),
        })
    }

    pub(in crate::runtime) fn spawn_generator_task(
        &self,
        domain: &Domain,
        shutdown_tx: &watch::Sender<bool>,
        spec: GeneratorTaskSpec,
    ) -> Result<JoinHandle<()>, RuntimeError> {
        let GeneratorTaskSpec {
            generator,
            source_relay,
            source_branching,
            routes,
        } = spec;
        let interval = Self::parse_runtime_node_duration_setting(
            domain,
            "generator",
            &generator.name,
            "each",
            &generator.each,
        )?;
        let routes = routes
            .into_iter()
            .map(|route| {
                let policy = route.output.flush_policy.as_ref().ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "generator '{}' output '{}' has no flush policy",
                            generator.name, route.output.relay
                        ),
                    }
                })?;
                let flush_policy = Self::parse_runtime_node_flush_policy(
                    domain,
                    "generator",
                    &generator.name,
                    &policy.flush_each,
                    policy.max_batch_size.as_deref(),
                )?;
                Ok((route, flush_policy))
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        let task_domain = domain.clone();
        let task_generator = generator.name.clone();
        let source_gate = self
            .relay_boundary_fanouts
            .get(&(domain.clone(), source_relay.clone()))
            .map(|fanout| fanout.dispatch_gate())
            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "missing generator source relay gate '{}'",
                    source_relay.as_str()
                ),
            })?;
        let quiesce_counters = self.node_quiesce_counters(domain, &generator.name);
        let mut shutdown_rx = shutdown_tx.subscribe();
        let mut domain_status_rx = self.domain_status_changed.subscribe();
        let generator_activity = self.generator_activity_tracker(domain);
        let runtime = self.clone();
        let task_events = self.events.clone();

        Ok(tokio::spawn(async move {
            let mut activity = DomainActivityGuard::new(generator_activity);
            let mut quiesce_activity = Some(NodeQuiesceWorkGuard::begin(quiesce_counters.clone()));
            let mut next_state_refresh = None::<Timestamp>;
            let mut branch_states =
                HashMap::<Option<BranchKey>, GeneratorBranchTaskState>::default();

            loop {
                tokio::task::consume_budget().await;
                if source_gate.is_closed() {
                    for (route_index, (route, _)) in routes.iter().enumerate() {
                        tokio::task::consume_budget().await;
                        let mut pending_groups = Vec::new();
                        for (branch_key, state) in &mut branch_states {
                            let route_state = &mut state.routes[route_index];
                            route_state.next_flush = None;
                            if !route_state.pending.is_empty() {
                                pending_groups.push((
                                    branch_key.clone(),
                                    std::mem::take(&mut route_state.pending),
                                ));
                            }
                        }
                        if !pending_groups.is_empty() {
                            flush_generator_groups(
                                GeneratorFlushContext {
                                    runtime: &runtime,
                                    domain: &task_domain,
                                    generator: &task_generator,
                                    output_relay: &route.output.relay,
                                    output_schema: &route.output_schema,
                                    output_registry: &route.output_registry,
                                    output_services: &route.output_services,
                                    task_events: &task_events,
                                },
                                &mut pending_groups,
                            )
                            .await;
                        }
                    }
                    quiesce_activity.take();
                    activity.set_active(false);
                    tokio::select! {
                        _ = source_gate.wait_open() => {}
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                    if quiesce_activity.is_none() {
                        quiesce_activity =
                            Some(NodeQuiesceWorkGuard::begin(quiesce_counters.clone()));
                    }
                    continue;
                }
                if runtime.domains.get(&task_domain).is_some_and(|state| {
                    matches!(state.status, nervix_models::DomainStatus::Paused)
                }) {
                    for (route_index, (route, _)) in routes.iter().enumerate() {
                        tokio::task::consume_budget().await;
                        let mut pending_groups = Vec::new();
                        for (branch_key, state) in &mut branch_states {
                            let route_state = &mut state.routes[route_index];
                            route_state.next_flush = None;
                            if !route_state.pending.is_empty() {
                                pending_groups.push((
                                    branch_key.clone(),
                                    std::mem::take(&mut route_state.pending),
                                ));
                            }
                        }
                        if !pending_groups.is_empty() {
                            flush_generator_groups(
                                GeneratorFlushContext {
                                    runtime: &runtime,
                                    domain: &task_domain,
                                    generator: &task_generator,
                                    output_relay: &route.output.relay,
                                    output_schema: &route.output_schema,
                                    output_registry: &route.output_registry,
                                    output_services: &route.output_services,
                                    task_events: &task_events,
                                },
                                &mut pending_groups,
                            )
                            .await;
                        }
                    }
                    activity.set_active(false);
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        changed = domain_status_rx.changed() => {
                            if changed.is_err() {
                                break;
                            }
                        }
                        _ = source_gate.wait_closed() => {}
                    }
                    continue;
                }
                activity.set_active(true);
                let wall_now = current_timestamp();
                let execution_now;
                let paced_state = runtime.domains.get(&task_domain).map(|domain_state| {
                    (
                        domain_state.config.pace,
                        domain_state.clock.clone(),
                        domain_state.ticks.lock().back().cloned(),
                    )
                });
                let is_paced = paced_state
                    .as_ref()
                    .is_some_and(|(pace, _, _)| *pace == DomainPace::Paced);
                if let Some((DomainPace::Paced, ref clock, ref latest_tick)) = paced_state {
                    let Some(clock) = clock else {
                        next_state_refresh = None;
                        for state in branch_states.values_mut() {
                            state.next_generation = None;
                            for route in &mut state.routes {
                                route.next_flush = None;
                            }
                        }
                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = sleep(Duration::from_millis(50)) => {}
                            _ = source_gate.wait_closed() => {}
                        }
                        continue;
                    };
                    execution_now =
                        match current_domain_logical_time(clock, latest_tick.as_ref(), wall_now) {
                            Ok(value) => value,
                            Err(error) => {
                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                    "failed to resolve generator domain clock for '{}' in domain \
                                     '{}': {}",
                                    task_generator.as_str(),
                                    task_domain.as_str(),
                                    error
                                )));
                                tokio::select! {
                                    changed = shutdown_rx.changed() => {
                                        if changed.is_err() || *shutdown_rx.borrow() {
                                            break;
                                        }
                                    }
                                    _ = sleep(Duration::from_millis(100)) => {}
                                    _ = source_gate.wait_closed() => {}
                                }
                                continue;
                            }
                        };
                } else {
                    execution_now = current_timestamp();
                }

                if next_state_refresh.is_none() {
                    next_state_refresh = Some(execution_now);
                }
                let should_refresh_state =
                    next_state_refresh.is_some_and(|next| execution_now >= next);
                let mut did_scheduled_work = false;

                if should_refresh_state {
                    advance_scheduled_timestamp(&mut next_state_refresh, interval, execution_now);
                    did_scheduled_work = true;

                    let mut state_load_failed = false;
                    let state = match runtime
                        .materialized_stream_state_from_owner(&task_domain, &source_relay)
                        .await
                    {
                        Ok(state) => state,
                        Err(error) => {
                            state_load_failed = true;
                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                "failed to read materialized state for generator '{}' from relay \
                                 '{}' in domain '{}': {}",
                                task_generator.as_str(),
                                source_relay.as_str(),
                                task_domain.as_str(),
                                error
                            )));
                            Vec::new()
                        }
                    };

                    let mut source_state_by_branch =
                        HashMap::<Option<BranchKey>, Vec<RuntimeRecord>>::default();
                    if !state_load_failed {
                        let mut latest_state = HashMap::<String, RuntimeRecord>::default();
                        for (key, record) in state {
                            let replace = latest_state.get(&key).is_none_or(|existing| {
                                let existing = existing.metadata();
                                let candidate = record.metadata();
                                candidate.ingested_at_high_watermark()
                                    > existing.ingested_at_high_watermark()
                                    || (candidate.ingested_at_high_watermark()
                                        == existing.ingested_at_high_watermark()
                                        && candidate.ingested_at_low_watermark()
                                            > existing.ingested_at_low_watermark())
                            });
                            if replace {
                                latest_state.insert(key, record);
                            }
                        }
                        for record in latest_state.into_values() {
                            let branch_key = if source_branching.is_empty() {
                                None
                            } else {
                                match BranchKey::from_record(&record, source_branching.iter()) {
                                    Ok(Some(key)) => Some(key),
                                    Ok(None) => {
                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                            "generator '{}' source relay '{}' record is missing \
                                             concrete branch fields",
                                            task_generator.as_str(),
                                            source_relay.as_str(),
                                        )));
                                        continue;
                                    }
                                    Err(error) => {
                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                            "generator '{}' source relay '{}' has invalid \
                                             concrete branch fields: {}",
                                            task_generator.as_str(),
                                            source_relay.as_str(),
                                            error,
                                        )));
                                        continue;
                                    }
                                }
                            };
                            source_state_by_branch
                                .entry(branch_key)
                                .or_default()
                                .push(record);
                        }
                    }

                    if !state_load_failed {
                        let active_branch_keys = source_state_by_branch
                            .keys()
                            .cloned()
                            .collect::<HashSet<_>>();
                        branch_states
                            .retain(|branch_key, _| active_branch_keys.contains(branch_key));
                        for (branch_key, records) in source_state_by_branch {
                            tokio::task::consume_budget().await;
                            let branch_state = branch_states
                                .entry(branch_key.clone())
                                .or_insert_with(|| GeneratorBranchTaskState {
                                    next_generation: None,
                                    routes: routes
                                        .iter()
                                        .map(|_| GeneratorRouteBranchTaskState::default())
                                        .collect(),
                                });
                            if branch_state.next_generation.is_none() {
                                branch_state.next_generation = Some(execution_now);
                            }
                            for (route_state, (_, flush_policy)) in
                                branch_state.routes.iter_mut().zip(&routes)
                            {
                                if route_state.next_flush.is_none()
                                    && let RuntimeFlushPolicy::Each {
                                        interval: flush_each,
                                        ..
                                    } = flush_policy
                                {
                                    route_state.next_flush =
                                        Some(checked_add_duration_to_timestamp(
                                            execution_now,
                                            *flush_each,
                                        ));
                                }
                            }
                            if !branch_state
                                .next_generation
                                .is_some_and(|next| execution_now >= next)
                            {
                                continue;
                            }
                            advance_scheduled_timestamp(
                                &mut branch_state.next_generation,
                                interval,
                                execution_now,
                            );

                            for source_record in records {
                                tokio::task::consume_budget().await;
                                let mut values = HashMap::default();
                                for field in source_record.to_remote().fields {
                                    values.insert(
                                        format!(
                                            "relay_state.{}.{}",
                                            source_relay.as_str(),
                                            field.name
                                        ),
                                        RuntimeValue::from_remote(field.value),
                                    );
                                }
                                if let Some(branch_key) = branch_key.as_ref() {
                                    for (field, value) in branch_key.fields() {
                                        values.insert(
                                            format!("branch.{}", field.as_str()),
                                            value.clone(),
                                        );
                                    }
                                }
                                let materialized_state = values
                                    .iter()
                                    .filter(|(name, _)| name.starts_with("relay_state."))
                                    .map(|(name, value)| (name.clone(), value.clone()))
                                    .collect::<HashMap<_, _>>();

                                for (route_index, (route, flush_policy)) in
                                    routes.iter().enumerate()
                                {
                                    tokio::task::consume_budget().await;
                                    let input = match generator_context_batch(
                                        &route.program.compiled.input_schema,
                                        &values,
                                    ) {
                                        Ok(input) => input,
                                        Err(error) => {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "failed to prepare generator '{}' route '{}' \
                                                 input in domain '{}' branch '{}': {}",
                                                task_generator.as_str(),
                                                route.output.relay.as_str(),
                                                task_domain.as_str(),
                                                branch_key_display(&branch_key),
                                                error
                                            )));
                                            continue;
                                        }
                                    };
                                    match execute_generator_program_on_context(
                                        &route.program,
                                        &input,
                                        execution_now,
                                        &materialized_state,
                                    )
                                    .await
                                    {
                                        Ok(SingleRecordFilterMapOutcome::Filtered) => {}
                                        Ok(SingleRecordFilterMapOutcome::Output(record)) => {
                                            let (acks, _completion) =
                                                runtime.tracked_ack_root(&task_domain);
                                            let route_state = &mut branch_state.routes[route_index];
                                            route_state.pending.push(RelayMessage {
                                                key: branch_key.clone(),
                                                record,
                                                acks,
                                            });
                                            if route_state.next_flush.is_none() {
                                                route_state.next_flush =
                                                    Some(checked_add_duration_to_timestamp(
                                                        execution_now,
                                                        flush_policy.interval(),
                                                    ));
                                            }
                                        }
                                        Ok(SingleRecordFilterMapOutcome::MessageError {
                                            error,
                                            partial_output,
                                            materialized_state,
                                        }) => {
                                            let (acks, _completion) =
                                                runtime.tracked_ack_root(&task_domain);
                                            runtime
                                                .handle_structured_message_error(
                                                    MessageErrorHandling {
                                                        domain: &task_domain,
                                                        node_kind: "generator",
                                                        node: &task_generator,
                                                        source_route: Some(&route.output.relay),
                                                        policy: &route.output.message_error_policy,
                                                        message: RelayMessage {
                                                            key: branch_key.clone(),
                                                            record: source_record.clone(),
                                                            acks,
                                                        },
                                                        error,
                                                        partial_output,
                                                        materialized_state,
                                                        ingest_metadata: None,
                                                    },
                                                )
                                                .await;
                                        }
                                        Err(error) => {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "failed to execute generator '{}' route '{}' in \
                                                 domain '{}' branch '{}': {}",
                                                task_generator.as_str(),
                                                route.output.relay.as_str(),
                                                task_domain.as_str(),
                                                branch_key_display(&branch_key),
                                                error
                                            )));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let mut flushed_any_branch = false;
                for (branch_key, branch_state) in &mut branch_states {
                    tokio::task::consume_budget().await;
                    for ((route, flush_policy), route_state) in
                        routes.iter().zip(&mut branch_state.routes)
                    {
                        if !route_state
                            .next_flush
                            .is_some_and(|next| execution_now >= next)
                        {
                            continue;
                        }
                        match flush_policy {
                            RuntimeFlushPolicy::Each { interval, .. } => {
                                advance_scheduled_timestamp(
                                    &mut route_state.next_flush,
                                    *interval,
                                    execution_now,
                                );
                            }
                            RuntimeFlushPolicy::Immediate => {
                                route_state.next_flush = None;
                            }
                        }
                        if !route_state.pending.is_empty() {
                            let mut pending_group = vec![(
                                branch_key.clone(),
                                std::mem::take(&mut route_state.pending),
                            )];
                            flush_generator_groups(
                                GeneratorFlushContext {
                                    runtime: &runtime,
                                    domain: &task_domain,
                                    generator: &task_generator,
                                    output_relay: &route.output.relay,
                                    output_schema: &route.output_schema,
                                    output_registry: &route.output_registry,
                                    output_services: &route.output_services,
                                    task_events: &task_events,
                                },
                                &mut pending_group,
                            )
                            .await;
                        }
                        flushed_any_branch = true;
                    }
                }
                did_scheduled_work |= flushed_any_branch;

                if did_scheduled_work {
                    continue;
                }

                let next_deadline =
                    next_state_refresh
                        .into_iter()
                        .chain(
                            branch_states
                                .values()
                                .filter_map(|state| state.next_generation),
                        )
                        .chain(branch_states.values().flat_map(|state| {
                            state.routes.iter().filter_map(|route| route.next_flush)
                        }))
                        .min();
                let sleep_duration = next_deadline
                    .map(|next| {
                        if is_paced {
                            paced_state
                                .as_ref()
                                .and_then(|(_, clock, _)| clock.as_ref())
                                .map(|clock| {
                                    wall_duration_until_logical_target(clock, execution_now, next)
                                        .unwrap_or(Duration::from_millis(100))
                                })
                                .unwrap_or(Duration::from_millis(50))
                        } else {
                            wall_duration_until_timestamp(execution_now, next)
                        }
                    })
                    .unwrap_or(interval);

                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = sleep(sleep_duration) => {}
                    _ = source_gate.wait_closed() => {}
                }
            }
        }))
    }

    async fn evaluate_reingestor_output_events(
        &self,
        domain: &Domain,
        reingestor: &Identifier,
        from_relay: &Identifier,
        output: &mut RelayProcessorOutputNode,
        output_index: usize,
        batch: &RelayRecordBatch,
    ) -> Result<
        (
            Vec<PendingProcessorOutputMessage>,
            Vec<PendingProcessorOutputBatch>,
            Vec<PendingProcessorOutputMessageError>,
        ),
        PlannedGeneralError,
    > {
        if output.compiled_program.is_none() {
            let (
                input_schema,
                output_schema,
                materialized_stream_specs,
                available_lookups,
                udfs,
                current_branching,
                current_branch_schema,
            ) = {
                let Some(execution) = self.executions.get(domain) else {
                    return Err(PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: format!("domain '{}' is not instantiated", domain.as_str()),
                    });
                };
                let input_schema = execution
                    .relay_schemas
                    .get(from_relay)
                    .cloned()
                    .ok_or_else(|| PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: format!(
                            "stream '{}' schema is not instantiated in domain '{}'",
                            from_relay.as_str(),
                            domain.as_str()
                        ),
                    })?;
                let output_schema = execution
                    .relay_schemas
                    .get(&output.relay)
                    .cloned()
                    .ok_or_else(|| PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: format!(
                            "stream '{}' schema is not instantiated in domain '{}'",
                            output.relay.as_str(),
                            domain.as_str()
                        ),
                    })?;
                (
                    input_schema,
                    output_schema,
                    execution.materialized_stream_specs.clone(),
                    execution.lookups.clone(),
                    execution.udfs.clone(),
                    execution
                        .relay_branchings
                        .get(from_relay)
                        .cloned()
                        .unwrap_or_default(),
                    execution
                        .relay_branching_schemas
                        .get(from_relay)
                        .cloned()
                        .flatten(),
                )
            };
            match compile_processor_output_filter_map_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: reingestor,
                },
                std::slice::from_ref(from_relay),
                &output.relay,
                &output.construction,
                RuntimeVmSchemaPair {
                    input: batch.arrow_schema(),
                    input_sensitivity: input_schema.vm_sensitivity(),
                    output: output_schema.arrow_schema(),
                    output_sensitivity: output_schema.vm_sensitivity(),
                },
                None,
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
                    udfs: Some(&udfs),
                },
            ) {
                Ok(program) => output.compiled_program = program,
                Err(error) => {
                    return Err(PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: error.to_string(),
                    });
                }
            }
        }

        let Some(program) = output.compiled_program.as_ref() else {
            let can_forward_batch = self
                .executions
                .get(domain)
                .and_then(|execution| execution.relay_schemas.get(&output.relay).cloned())
                .map(|schema| schema.arrow_schema().as_ref() == batch.arrow_schema().as_ref())
                .unwrap_or(true);
            if can_forward_batch {
                return Ok((
                    Vec::new(),
                    vec![pending_passthrough_output_batch(output_index, batch)],
                    Vec::new(),
                ));
            }
            let records = batch
                .runtime_records()
                .map_err(|error| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "reingestor '{}' failed to materialize node-local output rows: {}",
                        reingestor.as_str(),
                        error
                    ),
                })?;
            let messages = records
                .into_iter()
                .enumerate()
                .map(|(row, record)| PendingProcessorOutputMessage {
                    row,
                    output_index,
                    key: batch.keys[row].clone(),
                    record,
                })
                .collect();
            return Ok((messages, Vec::new(), Vec::new()));
        };

        let (output_schema, owner_nodes) = {
            let Some(execution) = self.executions.get(domain) else {
                return Err(PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!("domain '{}' is not instantiated", domain.as_str()),
                });
            };
            let output_schema = execution
                .relay_schemas
                .get(&output.relay)
                .cloned()
                .ok_or_else(|| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "stream '{}' schema is not instantiated in domain '{}'",
                        output.relay.as_str(),
                        domain.as_str()
                    ),
                })?;
            (
                output_schema,
                execution.materialized_stream_owner_nodes.clone(),
            )
        };
        let side_inputs = self
            .load_materialized_side_inputs(
                domain,
                &batch.key,
                &program.materialized_interest,
                &owner_nodes,
            )
            .await
            .map_err(|error| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "reingestor '{}' failed to load materialized side inputs: {}",
                    reingestor.as_str(),
                    error
                ),
            })?;
        let execution_now = self
            .current_stream_expiration_time(domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp);
        let executed = execute_filter_map_program_on_batch(
            "reingestor",
            reingestor,
            program,
            FilterMapBatchInputs {
                carrier: &batch.batch,
                keys: &batch.keys,
                side_inputs: &side_inputs,
            },
            execution_now,
            batch.acks.clone(),
        )
        .await?;
        let state_snapshot = relay_state_snapshot_from_side_inputs(&side_inputs);
        let mut success_output_rows = Vec::new();
        let mut success_input_rows = Vec::new();
        let mut errors = Vec::new();
        for (output_row, &input_row) in executed.selected_rows.iter().enumerate() {
            if let Some(side_error) = executed.batch.errors().row(output_row).first() {
                let partial_output = vm_partial_output_row_to_runtime_record(
                    &executed.batch,
                    output_row,
                    batch.metadata[input_row].clone(),
                )
                .ok();
                let record =
                    batch
                        .runtime_record(input_row)
                        .map_err(|error| PlannedGeneralError {
                            acks: batch.acks.clone(),
                            reason: format!(
                                "reingestor '{}' failed to materialize FILTER-MAP error input \
                                 row: {}",
                                reingestor.as_str(),
                                error
                            ),
                        })?;
                errors.push(PendingProcessorOutputMessageError {
                    row: input_row,
                    key: batch.keys[input_row].clone(),
                    record,
                    error: program.structured_side_error(
                        format!(
                            "reingestor '{}' FILTER-MAP side error {}: {} at {}",
                            reingestor.as_str(),
                            side_error.code.as_str(),
                            side_error.message,
                            side_error.span
                        ),
                        side_error.span,
                        MessageErrorOperation::Set,
                    ),
                    partial_output,
                    materialized_state: state_snapshot.clone(),
                });
                continue;
            }
            success_output_rows.push(output_row);
            success_input_rows.push(input_row);
        }
        let output_batches = if success_output_rows.is_empty() {
            Vec::new()
        } else {
            let output_batch = vm_typed_batch_selected_rows_to_runtime_batch(
                &executed.batch,
                &success_output_rows,
            )
            .map_err(|error| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "reingestor '{}' failed to materialize successful FILTER-MAP rows: {}",
                    reingestor.as_str(),
                    error
                ),
            })?;
            if output_batch.schema().as_ref() != output_schema.arrow_schema().as_ref() {
                return Err(PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "reingestor '{}' FILTER-MAP output schema does not match relay '{}'",
                        reingestor.as_str(),
                        output.relay.as_str()
                    ),
                });
            }
            let metadata = success_input_rows
                .iter()
                .map(|input_row| batch.metadata[*input_row].clone())
                .collect::<Vec<_>>();
            vec![PendingProcessorOutputBatch {
                output_index,
                input_rows: success_input_rows,
                key: batch.key.clone(),
                batch: output_batch,
                metadata,
            }]
        };

        Ok((Vec::new(), output_batches, errors))
    }

    async fn dispatch_reingestor_outputs(
        &self,
        context: ReingestorDispatchContext<'_>,
        compiled_from_where: &mut Option<CompiledProgramWithMaterializedInterest>,
        output_routes: &mut RelayProcessorOutputsNode,
        output_quiesce_gauge: &mut ReingestorOutputQuiesceGauge,
        batch: RelayRecordBatch,
    ) {
        let ReingestorDispatchContext {
            domain,
            reingestor,
            from_relay,
            from_where: _,
            mode: _,
            error_policies,
            branched_senders,
        } = context;
        if batch.message_count() == 0 {
            return;
        }
        let Some(batch) = self
            .filter_reingestor_from_batch(context, compiled_from_where, batch)
            .await
        else {
            return;
        };
        if batch.message_count() == 0 {
            return;
        }

        let output_relays = output_routes
            .routes
            .iter()
            .map(|output| output.relay.clone())
            .collect::<Vec<_>>();

        let mut pending_messages = Vec::new();
        let mut pending_batches = Vec::new();
        let mut pending_errors = Vec::new();
        for (output_index, output) in output_routes.routes.iter_mut().enumerate() {
            let (messages, batches, errors) = match self
                .evaluate_reingestor_output_events(
                    domain,
                    reingestor,
                    from_relay,
                    output,
                    output_index,
                    &batch,
                )
                .await
            {
                Ok(events) => events,
                Err(error) => {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        "reingestor",
                        reingestor,
                        error_policies,
                        error.acks.iter(),
                        error.reason,
                    );
                    return;
                }
            };
            pending_messages.extend(messages);
            pending_batches.extend(batches);
            pending_errors.extend(errors.into_iter().map(|error| (output_index, error)));
        }

        let mut delivery_counts = vec![0usize; batch.acks.len()];
        for message in &pending_messages {
            delivery_counts[message.row] += 1;
        }
        for pending_batch in &pending_batches {
            for row in &pending_batch.input_rows {
                delivery_counts[*row] += 1;
            }
        }
        for (_, error) in &pending_errors {
            delivery_counts[error.row] += 1;
        }

        let RelayRecordBatch { acks, .. } = batch;
        let mut ack_queues = Vec::with_capacity(delivery_counts.len());
        for (row, ack) in acks.into_iter().enumerate() {
            let delivery_count = delivery_counts[row];
            if delivery_count == 0 {
                ack.ack_success();
                ack_queues.push(VecDeque::new());
                continue;
            }
            let mut queue = VecDeque::with_capacity(delivery_count);
            for _ in 1..delivery_count {
                queue.push_back(ack.attached());
            }
            queue.push_front(ack);
            ack_queues.push(queue);
        }

        let mut messages_by_output = vec![Vec::new(); output_relays.len()];
        let mut batches_by_output = vec![Vec::new(); output_relays.len()];
        for message in pending_messages {
            let Some(acks) = ack_queues[message.row].pop_front() else {
                continue;
            };
            messages_by_output[message.output_index].push(RelayMessage {
                key: message.key,
                record: message.record,
                acks,
            });
        }
        for pending_batch in pending_batches {
            let mut batch_acks = Vec::with_capacity(pending_batch.input_rows.len());
            for row in &pending_batch.input_rows {
                let Some(acks) = ack_queues[*row].pop_front() else {
                    continue;
                };
                batch_acks.push(acks);
            }
            if batch_acks.len() != pending_batch.input_rows.len() {
                self.handle_internal_processor_error_for_acks(
                    domain,
                    "reingestor",
                    reingestor,
                    error_policies,
                    batch_acks.iter(),
                    "reingestor output batch ack count does not match selected row count"
                        .to_string(),
                );
                return;
            }
            let output_index = pending_batch.output_index;
            let error_acks = batch_acks.clone();
            match pending_batch.into_relay_batch(batch_acks) {
                Ok(batch) => batches_by_output[output_index].push(batch),
                Err(error) => {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        "reingestor",
                        reingestor,
                        error_policies,
                        error_acks.iter(),
                        error,
                    );
                    return;
                }
            }
        }

        for (output_index, error) in pending_errors {
            let Some(acks) = ack_queues[error.row].pop_front() else {
                continue;
            };
            self.handle_structured_message_error(MessageErrorHandling {
                domain,
                node_kind: "reingestor",
                node: reingestor,
                source_route: Some(&output_routes.routes[output_index].relay),
                policy: &output_routes.routes[output_index].message_error_policy,
                message: RelayMessage {
                    key: error.key,
                    record: error.record,
                    acks,
                },
                error: error.error,
                partial_output: error.partial_output,
                materialized_state: error.materialized_state,
                ingest_metadata: None,
            })
            .await;
        }

        let execution_now = self
            .current_stream_expiration_time(domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp);
        for (output_index, (messages, mut batches)) in messages_by_output
            .into_iter()
            .zip(batches_by_output)
            .enumerate()
        {
            tokio::task::consume_budget().await;
            let relay = &output_relays[output_index];
            if !branched_senders.contains_key(relay) {
                for message in messages {
                    self.handle_message_error(
                        domain,
                        "reingestor",
                        reingestor,
                        error_policies,
                        message,
                        MessageErrorFailure::publish(
                            Some(relay),
                            format!(
                                "missing reingestor branched entrypoint for relay '{}'",
                                relay.as_str()
                            ),
                        ),
                    )
                    .await;
                }
                for batch in batches {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        "reingestor",
                        reingestor,
                        error_policies,
                        batch.acks.iter(),
                        format!(
                            "missing reingestor branched entrypoint for relay '{}'",
                            relay.as_str()
                        ),
                    );
                }
                continue;
            }
            if !messages.is_empty() {
                let output_schema = match relay_schema_for_runtime(self, domain, relay) {
                    Ok(schema) => schema,
                    Err(error) => {
                        for message in messages {
                            self.handle_message_error(
                                domain,
                                "reingestor",
                                reingestor,
                                error_policies,
                                message,
                                MessageErrorFailure::publish(Some(relay), error.to_string()),
                            )
                            .await;
                        }
                        continue;
                    }
                };
                match build_stream_record_batch_preserving_acks(output_schema, messages) {
                    Ok(batch) => batches.push(batch),
                    Err((error, acks)) => {
                        self.handle_internal_processor_error_for_acks(
                            domain,
                            "reingestor",
                            reingestor,
                            error_policies,
                            acks.iter(),
                            format!(
                                "reingestor '{}' failed to build output batch for relay '{}': {}",
                                reingestor.as_str(),
                                relay.as_str(),
                                error
                            ),
                        );
                        continue;
                    }
                }
            };
            if batches.is_empty() {
                continue;
            }
            let output = &mut output_routes.routes[output_index];
            let mut should_flush = false;
            for batch in batches.drain(..) {
                should_flush |= output.enqueue(batch, execution_now);
            }
            output_quiesce_gauge.observe(output_routes);
            if !should_flush {
                continue;
            }
            self.flush_reingestor_output(context, &mut output_routes.routes[output_index])
                .await;
            output_quiesce_gauge.observe(output_routes);
        }
    }

    async fn flush_reingestor_output(
        &self,
        context: ReingestorDispatchContext<'_>,
        output: &mut RelayProcessorOutputNode,
    ) {
        let ReingestorDispatchContext {
            domain,
            reingestor,
            mode,
            error_policies,
            branched_senders,
            ..
        } = context;
        let pending = output.take_pending();
        if pending.is_empty() {
            return;
        }
        let pending_acks = pending
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        let forwarded = match RelayRecordBatch::concat(pending) {
            Ok(batch) => batch,
            Err(error) => {
                self.handle_internal_processor_error_for_acks(
                    domain,
                    "reingestor",
                    reingestor,
                    error_policies,
                    pending_acks.iter(),
                    format!(
                        "reingestor '{}' failed to concat buffered output batches for relay '{}': \
                         {}",
                        reingestor.as_str(),
                        output.relay.as_str(),
                        error
                    ),
                );
                return;
            }
        };
        let Some(branched_sender) = branched_senders.get(&output.relay) else {
            self.handle_internal_processor_error_for_acks(
                domain,
                "reingestor",
                reingestor,
                error_policies,
                forwarded.acks.iter(),
                format!(
                    "missing reingestor branched entrypoint for relay '{}'",
                    output.relay.as_str()
                ),
            );
            return;
        };
        if let Err(error) = branched_sender.send(forwarded).await {
            let batch = error.0;
            if mode == AckMode::Detached {
                for ack in batch.acks {
                    ack.ack_success();
                }
                return;
            }
            self.handle_internal_processor_error_for_acks(
                domain,
                "reingestor",
                reingestor,
                error_policies,
                batch.acks.iter(),
                format!(
                    "reingestor '{}' failed to forward buffered batch to branch entrypoint for \
                     relay '{}'",
                    reingestor.as_str(),
                    output.relay.as_str()
                ),
            );
        }
    }

    async fn flush_reingestor_outputs(
        &self,
        context: ReingestorDispatchContext<'_>,
        output_routes: &mut RelayProcessorOutputsNode,
        flush: ReingestorOutputFlush,
        output_quiesce_gauge: &mut ReingestorOutputQuiesceGauge,
    ) {
        for output_index in 0..output_routes.routes.len() {
            tokio::task::consume_budget().await;
            let should_flush = match flush {
                ReingestorOutputFlush::Due(now) => {
                    output_routes.routes[output_index].flush_due(now)
                }
                ReingestorOutputFlush::All => {
                    !output_routes.routes[output_index].pending.is_empty()
                }
            };
            if !should_flush {
                continue;
            }
            self.flush_reingestor_output(context, &mut output_routes.routes[output_index])
                .await;
            output_quiesce_gauge.observe(output_routes);
        }
    }

    async fn filter_reingestor_from_batch(
        &self,
        context: ReingestorDispatchContext<'_>,
        compiled_from_where: &mut Option<CompiledProgramWithMaterializedInterest>,
        batch: RelayRecordBatch,
    ) -> Option<RelayRecordBatch> {
        let ReingestorDispatchContext {
            domain,
            reingestor,
            from_relay,
            from_where,
            error_policies,
            ..
        } = context;
        let Some(from_where) = from_where else {
            return Some(batch);
        };

        if compiled_from_where.is_none() {
            let (
                input_schema,
                materialized_stream_specs,
                available_lookups,
                udfs,
                current_branching,
                current_branch_schema,
            ) = {
                let Some(execution) = self.executions.get(domain) else {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        "reingestor",
                        reingestor,
                        error_policies,
                        batch.acks.iter(),
                        format!("domain '{}' is not instantiated", domain.as_str()),
                    );
                    return None;
                };
                let input_schema = match execution.relay_schemas.get(from_relay).cloned() {
                    Some(schema) => schema,
                    None => {
                        self.handle_internal_processor_error_for_acks(
                            domain,
                            "reingestor",
                            reingestor,
                            error_policies,
                            batch.acks.iter(),
                            format!(
                                "stream '{}' schema is not instantiated in domain '{}'",
                                from_relay.as_str(),
                                domain.as_str()
                            ),
                        );
                        return None;
                    }
                };
                (
                    input_schema,
                    execution.materialized_stream_specs.clone(),
                    execution.lookups.clone(),
                    execution.udfs.clone(),
                    execution
                        .relay_branchings
                        .get(from_relay)
                        .cloned()
                        .unwrap_or_default(),
                    execution
                        .relay_branching_schemas
                        .get(from_relay)
                        .cloned()
                        .flatten(),
                )
            };
            match compile_expression_filter_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: reingestor,
                },
                Some(from_where),
                RuntimeVmSchema {
                    schema: batch.arrow_schema(),
                    sensitivity: input_schema.vm_sensitivity(),
                },
                false,
                MessageErrorOperation::SourceWhere,
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
                    udfs: Some(&udfs),
                },
            ) {
                Ok(program) => *compiled_from_where = program,
                Err(error) => {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        "reingestor",
                        reingestor,
                        error_policies,
                        batch.acks.iter(),
                        format!("FROM WHERE compile failed: {}", error),
                    );
                    return None;
                }
            }
        }

        let Some(program) = compiled_from_where.clone() else {
            return Some(batch);
        };
        let owner_nodes = self
            .executions
            .get(domain)
            .map(|execution| execution.materialized_stream_owner_nodes.clone())
            .unwrap_or_default();
        let side_inputs = match self
            .load_materialized_side_inputs(
                domain,
                &batch.key,
                &program.materialized_interest,
                &owner_nodes,
            )
            .await
        {
            Ok(values) => values,
            Err(error) => {
                self.handle_internal_processor_error_for_acks(
                    domain,
                    "reingestor",
                    reingestor,
                    error_policies,
                    batch.acks.iter(),
                    format!(
                        "reingestor '{}' failed to load FROM WHERE side inputs: {}",
                        reingestor.as_str(),
                        error
                    ),
                );
                return None;
            }
        };
        let execution_now = self
            .current_stream_expiration_time(domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp);
        let plan = match plan_filter_map_messages(
            "reingestor",
            reingestor,
            "FROM WHERE",
            &program,
            batch,
            execution_now,
            &side_inputs,
        )
        .await
        {
            Ok(plan) => plan,
            Err(error) => {
                self.handle_internal_processor_error_for_acks(
                    domain,
                    "reingestor",
                    reingestor,
                    error_policies,
                    error.acks.iter(),
                    error.reason,
                );
                return None;
            }
        };
        self.handle_planned_message_errors(
            domain,
            "reingestor",
            reingestor,
            error_policies,
            plan.message_errors,
        )
        .await;
        plan.batch
    }

    pub(in crate::runtime) fn spawn_reingestor_task(
        &self,
        domain: &Domain,
        shutdown_tx: &watch::Sender<bool>,
        branched_entrypoint_senders: &HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
        reingestor: CreateReingestor,
        from_relay: Identifier,
        receiver: RelayRuntimeFanIn,
    ) -> Result<JoinHandle<()>, RuntimeError> {
        let input_collect_policy = Self::parse_runtime_node_input_collect_policy(
            domain,
            "reingestor",
            &reingestor.name,
            reingestor.from.collect_policy.as_ref(),
        )?;
        let mut task_output_routes = RelayProcessorOutputsNode {
            routes: reingestor
                .output_routes
                .routes
                .iter()
                .map(|output| {
                    let flush_policy = output
                        .flush_policy
                        .as_ref()
                        .map(|policy| {
                            Self::parse_runtime_node_flush_policy(
                                domain,
                                "reingestor output",
                                &output.relay,
                                &policy.flush_each,
                                policy.max_batch_size.as_deref(),
                            )
                        })
                        .transpose()?;
                    Ok(RelayProcessorOutputNode {
                        relay: output.relay.clone(),
                        construction: output.construction.clone(),
                        branch: output.branch.clone(),
                        flush_policy,
                        message_error_policy: output.message_error_policy.clone(),
                        pending: Vec::new(),
                        next_flush: None,
                        compiled_program: None,
                    })
                })
                .collect::<Result<Vec<_>, RuntimeError>>()?,
        };
        let mut task_branched_senders = HashMap::default();
        for output in reingestor.output_routes.outputs() {
            let Some(sender) = branched_entrypoint_senders.get(&output.relay).cloned() else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing reingestor branched entrypoint for relay '{}'",
                        output.relay.as_str()
                    ),
                });
            };
            task_branched_senders.insert(output.relay.clone(), sender);
        }
        let task_domain = domain.clone();
        let task_reingestor = reingestor.name.clone();
        let task_from_relay = from_relay;
        let task_from_where = reingestor
            .from
            .where_clauses()
            .iter()
            .find(|source_filter| source_filter.relay == task_from_relay)
            .map(|source_filter| source_filter.where_clause.clone());
        let task_materialized_state = reingestor.materialized_state.clone();
        let task_mode = reingestor.mode;
        let task_error_policies = internal_processor_error_policies(GeneralErrorPolicy::Log);
        let quiesce_counters = self.node_quiesce_counters(domain, &reingestor.name);
        let runtime = self.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let force_flush = self.force_flush_participant(domain, quiesce_counters.clone());

        Ok(tokio::spawn(async move {
            let mut output_quiesce_gauge =
                ReingestorOutputQuiesceGauge::new(quiesce_counters.clone());
            let interaction_input =
                RelayInteractionInput::new(task_from_relay.clone(), receiver, input_collect_policy);
            let mut interaction = RelayInteraction::new(
                vec![interaction_input],
                shutdown_rx,
                Some(force_flush),
                Some(quiesce_counters.clone()),
            )
            .expect("validated reingestor input must build a relay interaction");
            let mut compiled_from_where = None;
            loop {
                tokio::task::consume_budget().await;
                let execution_now = runtime
                    .current_stream_expiration_time(&task_domain)
                    .ok()
                    .flatten()
                    .unwrap_or_else(current_timestamp);
                let wake_at = task_output_routes.next_flush().map(|deadline| {
                    Instant::now()
                        + wall_duration_until_domain_deadline(
                            &runtime,
                            &task_domain,
                            execution_now,
                            deadline,
                        )
                });
                let work = match interaction.next(wake_at).await {
                    Ok(work) => work,
                    Err(error) => {
                        let reason = format!(
                            "reingestor '{}' relay interaction failed: {error}",
                            task_reingestor.as_str()
                        );
                        runtime.handle_internal_processor_error_for_acks(
                            &task_domain,
                            "reingestor",
                            &task_reingestor,
                            &task_error_policies,
                            error.acks(),
                            reason,
                        );
                        continue;
                    }
                };
                let (event, _work) = work.into_parts();
                match event {
                    RelayInteractionEvent::Stopped(reason) => {
                        runtime
                            .flush_reingestor_outputs(
                                ReingestorDispatchContext {
                                    domain: &task_domain,
                                    reingestor: &task_reingestor,
                                    from_relay: &task_from_relay,
                                    from_where: task_from_where.as_ref(),
                                    mode: task_mode,
                                    error_policies: &task_error_policies,
                                    branched_senders: &task_branched_senders,
                                },
                                &mut task_output_routes,
                                ReingestorOutputFlush::All,
                                &mut output_quiesce_gauge,
                            )
                            .await;
                        debug!(
                            domain = task_domain.as_str(),
                            reingestor = task_reingestor.as_str(),
                            ?reason,
                            "reingestor relay interaction stopped"
                        );
                        break;
                    }
                    RelayInteractionEvent::Wake => {
                        let now = runtime
                            .current_stream_expiration_time(&task_domain)
                            .ok()
                            .flatten()
                            .unwrap_or_else(current_timestamp);
                        runtime
                            .flush_reingestor_outputs(
                                ReingestorDispatchContext {
                                    domain: &task_domain,
                                    reingestor: &task_reingestor,
                                    from_relay: &task_from_relay,
                                    from_where: task_from_where.as_ref(),
                                    mode: task_mode,
                                    error_policies: &task_error_policies,
                                    branched_senders: &task_branched_senders,
                                },
                                &mut task_output_routes,
                                ReingestorOutputFlush::Due(now),
                                &mut output_quiesce_gauge,
                            )
                            .await;
                    }
                    RelayInteractionEvent::ForceFlush(completion) => {
                        runtime
                            .flush_reingestor_outputs(
                                ReingestorDispatchContext {
                                    domain: &task_domain,
                                    reingestor: &task_reingestor,
                                    from_relay: &task_from_relay,
                                    from_where: task_from_where.as_ref(),
                                    mode: task_mode,
                                    error_policies: &task_error_policies,
                                    branched_senders: &task_branched_senders,
                                },
                                &mut task_output_routes,
                                ReingestorOutputFlush::All,
                                &mut output_quiesce_gauge,
                            )
                            .await;
                        completion.complete();
                    }
                    RelayInteractionEvent::Command(command) => match command {},
                    RelayInteractionEvent::Batch {
                        relay: input_relay,
                        batch,
                    } => {
                        debug_assert_eq!(input_relay, task_from_relay);
                        let delivery_observation = batch.delivery_observation(current_timestamp());
                        let physical_node_id = runtime.local_node_id.read().clone();
                        runtime
                            .metrics
                            .observe_global_node_received(NodeBatchObservation {
                                domain: &task_domain,
                                kind: ModelKind::Reingestor,
                                node: &task_reingestor,
                                relay: &task_from_relay,
                                physical_node_id: physical_node_id.as_deref(),
                                messages: batch.message_count(),
                                bytes: batch.estimated_bytes(),
                                domain_timestamp: delivery_observation.domain_timestamp,
                            });
                        runtime.mark_branch_aggregated_metrics_updated(
                            &task_domain,
                            ModelKind::Reingestor,
                            &task_reingestor,
                        );
                        for seconds in delivery_observation.latency_seconds {
                            runtime
                                .metrics
                                .observe_global_delivery_latency_at_domain_time(
                                    NodeLatencyObservation {
                                        domain: &task_domain,
                                        kind: ModelKind::Reingestor,
                                        node: &task_reingestor,
                                        relay: &task_from_relay,
                                        physical_node_id: physical_node_id.as_deref(),
                                        seconds,
                                        domain_timestamp: delivery_observation.domain_timestamp,
                                    },
                                );
                        }
                        let dependency_error_acks = batch.acks.clone();
                        let wait_for_required_state = !interaction.is_terminal_drain();
                        let batch = match runtime
                            .resolve_materialized_dependencies_for_batch(
                                &task_domain,
                                &task_from_relay,
                                &task_materialized_state,
                                batch,
                                interaction.shutdown_receiver(),
                                wait_for_required_state,
                            )
                            .await
                        {
                            Ok(Some(batch)) => batch,
                            Ok(None) => continue,
                            Err(error) => {
                                runtime.handle_internal_processor_error_for_acks(
                                    &task_domain,
                                    "reingestor",
                                    &task_reingestor,
                                    &task_error_policies,
                                    dependency_error_acks.iter(),
                                    format!(
                                        "reingestor '{}' failed to resolve materialized \
                                         dependencies: {error}",
                                        task_reingestor.as_str()
                                    ),
                                );
                                continue;
                            }
                        };
                        runtime
                            .dispatch_reingestor_outputs(
                                ReingestorDispatchContext {
                                    domain: &task_domain,
                                    reingestor: &task_reingestor,
                                    from_relay: &task_from_relay,
                                    from_where: task_from_where.as_ref(),
                                    mode: task_mode,
                                    error_policies: &task_error_policies,
                                    branched_senders: &task_branched_senders,
                                },
                                &mut compiled_from_where,
                                &mut task_output_routes,
                                &mut output_quiesce_gauge,
                                batch,
                            )
                            .await;
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_emitter_task(
        &self,
        build: EmitterTaskBuildDeps<'_>,
        emitter: CreateEmitter,
        inputs: Vec<(Identifier, RelayRuntimeFanIn)>,
    ) -> Result<ScheduledEmitterTask, RuntimeError> {
        emitters::EmitterTask::spawn(self, build, emitter, inputs)
    }

    pub(in crate::runtime) fn spawn_materializer_task(
        &self,
        domain: &Domain,
        shutdown_tx: &watch::Sender<bool>,
        spec: MaterializerTaskSpec,
    ) -> JoinHandle<()> {
        let MaterializerTaskSpec {
            relay,
            state,
            branch_ttl,
            branch_capacity,
            receiver,
        } = spec;
        let runtime = self.clone();
        let domain = domain.clone();
        let expiration_scan_interval = self.branch_instance_expiration_scan_interval;
        let shutdown_rx = shutdown_tx.subscribe();
        let quiesce_counters = self.node_quiesce_counters(&domain, &relay);
        let force_flush = self.force_flush_participant(&domain, quiesce_counters.clone());
        tokio::spawn(async move {
            let interaction_input = RelayInteractionInput::new(relay.clone(), receiver, None);
            let mut interaction = RelayInteraction::new(
                vec![interaction_input],
                shutdown_rx,
                Some(force_flush),
                Some(quiesce_counters),
            )
            .expect("validated materializer input must build a relay interaction");
            let mut branch_instances = BranchInstanceRegistry::<Option<BranchKey>, ()>::new();
            let mut restored_branches = state
                .entries
                .iter()
                .map(|entry| {
                    (
                        entry.key().clone(),
                        entry.value().metadata.ingested_at_high_watermark,
                    )
                })
                .collect::<Vec<_>>();
            restored_branches.sort_by_key(|(_, last_ingestion)| *last_ingestion);
            for (key, last_ingestion) in restored_branches {
                branch_instances.insert_restored(key, last_ingestion, ());
            }
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            loop {
                tokio::task::consume_budget().await;
                if let Some(branch_ttl) = branch_ttl
                    && Instant::now() >= next_expiration_scan
                {
                    let now = runtime
                        .current_stream_expiration_time(&domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    for (key, _) in branch_instances.expire(now, branch_ttl) {
                        tokio::task::consume_budget().await;
                        runtime.remove_stream_key_presence(&domain, &relay, &key);
                        if let Err(error) =
                            runtime.delete_materialized_stream_key(&state, &key).await
                        {
                            warn!(
                                domain = domain.as_str(),
                                relay = relay.as_str(),
                                branch = branch_key_display(&key),
                                error = %error,
                                "failed to expire scheduled materialized relay state"
                            );
                        }
                    }
                    next_expiration_scan = Instant::now() + expiration_scan_interval;
                    continue;
                }
                let expiration_sleep = branch_ttl.map(|_| {
                    next_expiration_scan
                        .checked_duration_since(Instant::now())
                        .unwrap_or(Duration::ZERO)
                });
                let wake_at = expiration_sleep.map(|sleep| Instant::now() + sleep);
                let work = match interaction.next(wake_at).await {
                    Ok(work) => work,
                    Err(error) => {
                        if let Some(acks) = error.acks() {
                            acks.no_ack(format!(
                                "materializer for relay '{}' failed to collect input: {error}",
                                relay.as_str()
                            ));
                        }
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            error = %error,
                            "materializer relay interaction failed"
                        );
                        continue;
                    }
                };
                let (event, _work) = work.into_parts();
                let batch = match event {
                    RelayInteractionEvent::Batch {
                        relay: input_relay,
                        batch,
                    } => {
                        debug_assert_eq!(input_relay, relay);
                        batch
                    }
                    RelayInteractionEvent::Wake => continue,
                    RelayInteractionEvent::ForceFlush(completion) => {
                        completion.complete();
                        continue;
                    }
                    RelayInteractionEvent::Command(command) => match command {},
                    RelayInteractionEvent::Stopped(reason) => {
                        debug!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            ?reason,
                            "materializer relay interaction stopped"
                        );
                        break;
                    }
                };
                let branch_key = batch.key.clone();
                let now = runtime
                    .current_stream_expiration_time(&domain)
                    .ok()
                    .flatten()
                    .unwrap_or_else(current_timestamp);
                branch_instances
                    .get_or_try_create_with(branch_key.clone(), now, |_| {
                        Ok::<(), std::convert::Infallible>(())
                    })
                    .expect("infallible materialized branch tracking must succeed");
                if let Some(branch_capacity) = branch_capacity {
                    for (evicted_key, _) in branch_instances.evict_lru_to_capacity(branch_capacity)
                    {
                        tokio::task::consume_budget().await;
                        runtime.remove_stream_key_presence(&domain, &relay, &evicted_key);
                        if let Err(error) = runtime
                            .delete_materialized_stream_key(&state, &evicted_key)
                            .await
                        {
                            warn!(
                                domain = domain.as_str(),
                                relay = relay.as_str(),
                                branch = branch_key_display(&evicted_key),
                                error = %error,
                                "failed to evict scheduled materialized relay state by lru"
                            );
                        }
                    }
                }
                runtime.touch_stream_key(&domain, &relay, &branch_key, now);
                let messages = match batch.try_into_messages() {
                    Ok(messages) => messages,
                    Err(error_and_batch) => {
                        let (error, _) = *error_and_batch;
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            branch = branch_key_display(&branch_key),
                            error = %error,
                            "failed to decode scheduled materialized relay batch"
                        );
                        continue;
                    }
                };
                for message in messages {
                    tokio::task::consume_budget().await;
                    if let Err(error) = runtime
                        .update_materialized_stream_last_by_timestamp(
                            &state,
                            &branch_key,
                            &message.record,
                        )
                        .await
                    {
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            branch = branch_key_display(&branch_key),
                            error = %error,
                            "failed to update scheduled materialized relay state"
                        );
                    }
                }
            }
        })
    }

    pub(in crate::runtime) async fn start_scheduled_ingestor(
        &self,
        domain: &Domain,
        source_model: Model,
        ingestor: CreateIngestor,
        kafka_offset_state: Option<Arc<ReplicatedKafkaOffsetState>>,
    ) -> Result<(), RuntimeError> {
        ingestors::IngestorStarter::start_scheduled(
            self,
            domain,
            source_model,
            ingestor,
            kafka_offset_state,
        )
        .await
    }

    pub async fn pause_ingestors_for_memory_pressure(&self) -> usize {
        self.ingestors_paused_for_memory_pressure
            .store(true, Ordering::SeqCst);
        let ingestors = self
            .ingestors
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();

        let mut quiesced = 0;
        for key in ingestors {
            tokio::task::consume_budget().await;
            if self.engage_ingestor_quiesce(
                &key.domain,
                &key.identifier,
                IngestorQuiesceCause::MemoryPressure,
            ) {
                quiesced += 1;
            }
        }
        quiesced
    }

    pub async fn resume_one_ingestor_after_memory_pressure(&self) -> Result<bool, RuntimeError> {
        let mut keys = self
            .ingestor_quiescence
            .iter()
            .filter_map(|entry| {
                (entry.value().cause() == Some(IngestorQuiesceCause::MemoryPressure))
                    .then(|| entry.key().clone())
            })
            .collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            left.domain
                .as_str()
                .cmp(right.domain.as_str())
                .then_with(|| left.identifier.as_str().cmp(right.identifier.as_str()))
        });
        let Some(key) = keys.first() else {
            self.ingestors_paused_for_memory_pressure
                .store(false, Ordering::SeqCst);
            return Ok(false);
        };
        self.release_ingestor_quiesce(
            &key.domain,
            &key.identifier,
            IngestorQuiesceCause::MemoryPressure,
        );
        info!(
            domain = key.domain.as_str(),
            ingestor = key.identifier.as_str(),
            "resumed ingestor after memory pressure"
        );
        Ok(true)
    }

    pub fn ingestors_paused_for_memory_pressure(&self) -> bool {
        self.ingestors_paused_for_memory_pressure
            .load(Ordering::SeqCst)
    }

    async fn start_missing_domain_ingestors(&self, domain: &Domain) -> Result<(), RuntimeError> {
        while let Some(spec) = self.next_scheduled_ingestor_start_spec(Some(domain)) {
            tokio::task::consume_budget().await;
            self.start_scheduled_ingestor(
                &spec.domain,
                spec.source_model,
                spec.ingestor,
                spec.kafka_offset_state,
            )
            .await?;
        }
        Ok(())
    }

    pub async fn start_running_domain_ingestors(&self) -> Result<(), RuntimeError> {
        let _lock = self.schedule_apply_lock.lock().await;
        while let Some(spec) = self.next_scheduled_ingestor_start_spec(None) {
            tokio::task::consume_budget().await;
            self.start_scheduled_ingestor(
                &spec.domain,
                spec.source_model,
                spec.ingestor,
                spec.kafka_offset_state,
            )
            .await?;
        }
        Ok(())
    }

    fn next_scheduled_ingestor_start_spec(
        &self,
        requested_domain: Option<&Domain>,
    ) -> Option<ScheduledIngestorStartSpec> {
        let local_node_id = self.local_node_id.read().clone();
        let mut domains = self
            .executions
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|domain| requested_domain.is_none_or(|requested| requested == domain))
            .collect::<Vec<_>>();
        domains.sort_by(|left, right| left.as_str().cmp(right.as_str()));

        for domain in domains {
            if self
                .domains
                .get(&domain)
                .is_some_and(|state| !matches!(state.status, nervix_models::DomainStatus::Running))
            {
                continue;
            }
            let Some(execution) = self.executions.get(&domain) else {
                continue;
            };
            let passive_only = execution.passive_only;
            let schedule = execution.schedule.clone();
            drop(execution);

            if passive_only {
                continue;
            }

            for node in &schedule.nodes {
                if node.kind != ModelKind::Ingestor
                    || !Self::scheduled_node_executes_locally(node, local_node_id.as_deref())
                {
                    continue;
                }

                let key = RuntimeKey::new(domain.clone(), node.identifier.clone());
                if self.ingestors.contains_key(&key) {
                    continue;
                }

                let Model::Ingestor(ingestor) = node.config.as_ref() else {
                    continue;
                };
                let Some(source_model) =
                    Self::source_model_for_scheduled_ingestor(&schedule, ingestor)
                else {
                    warn!(
                        domain = domain.as_str(),
                        ingestor = ingestor.name.as_str(),
                        "cannot resume ingestor after memory pressure because its source model is \
                         missing"
                    );
                    continue;
                };

                return Some(ScheduledIngestorStartSpec {
                    domain: domain.clone(),
                    source_model,
                    ingestor: ingestor.clone(),
                    kafka_offset_state: self.kafka_offset_state_for_memory_pressure_resume(
                        &domain,
                        node,
                        ingestor,
                        local_node_id.as_deref(),
                    ),
                });
            }
        }

        None
    }

    fn scheduled_node_executes_locally(node: &ScheduledNode, local_node_id: Option<&str>) -> bool {
        if let Some(local_node_id) = local_node_id {
            return node.executes_on(local_node_id);
        }
        node.primary_node.is_none() && node.assigned_nodes.is_empty()
    }

    fn source_model_for_scheduled_ingestor(
        schedule: &DomainSchedule,
        ingestor: &CreateIngestor,
    ) -> Option<Model> {
        let source_ref = match &ingestor.source {
            IngestSource::Http { client, .. } => client,
            IngestSource::Kafka { client, .. } => client,
            IngestSource::Pulsar { client, .. } => client,
            IngestSource::Prometheus { client, .. } => client,
            IngestSource::RabbitMq { client, .. } => client,
            IngestSource::RedisPubSub { client, .. } => client,
            IngestSource::Mqtt { client, .. } => client,
            IngestSource::Nats { client, .. } => client,
            IngestSource::ZeroMq { client, .. } => client,
            IngestSource::Sqs { client, .. } => client,
            IngestSource::Websockets { client, .. } => client,
            IngestSource::Endpoint { endpoint, .. } => endpoint,
        };
        let source_kind = match &ingestor.source {
            IngestSource::Endpoint { .. } => ModelKind::Endpoint,
            _ => ModelKind::Client,
        };
        schedule
            .nodes
            .iter()
            .find(|node| node.kind == source_kind && node.identifier == *source_ref)
            .map(|node| (*node.config).clone())
    }

    fn kafka_offset_state_for_memory_pressure_resume(
        &self,
        domain: &Domain,
        node: &ScheduledNode,
        ingestor: &CreateIngestor,
        local_node_id: Option<&str>,
    ) -> Option<Arc<ReplicatedKafkaOffsetState>> {
        let IngestSource::Kafka {
            offset_mode: KafkaOffsetMode::Domain,
            ..
        } = &ingestor.source
        else {
            return None;
        };
        let local_node_id = local_node_id?;
        if !node.is_primary_on(local_node_id) {
            return None;
        }
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::KafkaOffset,
            node.kind,
            &node.identifier,
            None,
        );
        self.replicated_kafka_offset_states
            .get(&placement)
            .map(|state| state.value().clone())
    }

    pub(in crate::runtime) async fn stop_domain_execution(
        &self,
        domain: &Domain,
        execution: DomainExecution,
    ) {
        let _ = execution.shutdown.send(true);
        for (entity, node_task) in execution.node_tasks {
            Self::await_shutdown_task(
                node_task.task,
                domain,
                Some(&entity.identifier),
                "scheduled node",
            )
            .await;
        }
        for (entity, emitter_task) in execution.emitter_tasks {
            Self::await_shutdown_task_with_grace(
                emitter_task.task,
                domain,
                Some(&entity.identifier),
                "scheduled emitter",
                self.domain_drain_timeout()
                    .saturating_add(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE),
            )
            .await;
        }
        for (entity, task) in execution.generator_tasks {
            Self::await_shutdown_task(task, domain, Some(&entity.identifier), "generator").await;
        }
        for (entity, tasks) in execution.reingestor_tasks {
            for task in tasks {
                Self::await_shutdown_task(task, domain, Some(&entity.identifier), "reingestor")
                    .await;
            }
        }
        for task in execution.tasks {
            Self::await_shutdown_task(task, domain, None, "domain execution").await;
        }
        let quiesce_keys = self
            .node_quiesce_counters
            .iter()
            .filter(|entry| &entry.key().domain == domain)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in quiesce_keys {
            self.node_quiesce_counters.remove(&key);
        }
        for (identifier, runtimes) in execution.branched_entrypoints {
            for runtime in runtimes {
                runtime.shutdown().await;
                info!(
                    domain = domain.as_str(),
                    entrypoint = identifier.as_str(),
                    "stopped branched entrypoint runtime"
                );
            }
        }
        self.stop_message_error_routes_for_domain(domain).await;
        if !self
            .domains
            .get(domain)
            .is_some_and(|state| matches!(state.status, nervix_models::DomainStatus::Paused))
        {
            self.clear_runtime_state_for_domain(domain);
        }
    }

    fn clear_runtime_state_for_domain(&self, domain: &Domain) {
        let placements = self
            .replicated_deduplicator_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.replicated_deduplicator_states.remove(&placement);
        }
        let placements = self
            .replicated_kafka_offset_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.replicated_kafka_offset_states.remove(&placement);
        }
        let placements = self
            .replicated_materialized_stream_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.replicated_materialized_stream_states
                .remove(&placement);
        }
        let placements = self
            .replicated_window_processor_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.replicated_window_processor_states.remove(&placement);
        }
        let placements = self
            .replicated_wasm_processor_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.replicated_wasm_processor_states.remove(&placement);
        }
        let placements = self
            .replicated_branch_aggregated_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.replicated_branch_aggregated_states.remove(&placement);
        }
    }

    fn purge_stopped_domain_runtime_state(&self, domain: &Domain) -> Result<(), RuntimeError> {
        let Some(store) = self.state_store.as_ref() else {
            return Ok(());
        };
        store
            .purge_domain(domain)
            .map_err(|error| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: error.to_string(),
            })
    }

    async fn abort_domain_execution_start(&self, domain: &Domain) {
        self.stop_domain_ingestors(domain).await;
        if let Some((_, execution)) = self.executions.remove(domain) {
            self.stop_domain_execution(domain, execution).await;
        }
        self.clear_domain_graph_handle(domain).await;
    }

    pub(in crate::runtime) async fn stop_domain_ingestors(&self, domain: &Domain) {
        let ingestors = self
            .ingestors
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|key| &key.domain == domain)
            .collect::<Vec<_>>();

        for key in ingestors {
            if let Err(error) = self.stop_ingestor(domain, &key.identifier).await {
                warn!(
                    domain = domain.as_str(),
                    ingestor = key.identifier.as_str(),
                    error = %error,
                    "failed to stop domain ingestor during schedule rebuild"
                );
            }
        }
    }

    pub async fn shutdown(&self) {
        let domains = self
            .executions
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for domain in &domains {
            self.stop_domain_ingestors(domain).await;
        }
        for domain in &domains {
            if let Some((_, execution)) = self.executions.remove(domain) {
                self.stop_domain_execution(domain, execution).await;
            }
            self.clear_domain_ingestor_quiescence(domain);
        }
        self.endpoint_bindings.clear();
        self.compiled_domain_udfs.clear();
        self.ingestor_readiness.clear();
        self.expiring_stream_states.clear();
        self.replicated_deduplicator_states.clear();
        self.replicated_kafka_offset_states.clear();
        self.replicated_materialized_stream_states.clear();
        self.replicated_window_processor_states.clear();
        self.replicated_branch_aggregated_states.clear();
    }

    pub(in crate::runtime) async fn await_ack_completion(
        shutdown_rx: &mut watch::Receiver<bool>,
        mut completion: AckCompletion,
        timeout_duration: Duration,
    ) -> Option<AckOutcome> {
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    let _ = changed;
                    return None;
                }
                progress = tokio::time::timeout(timeout_duration, completion.wait_for_progress()) => {
                    match progress {
                        Ok(AckProgress::Alive) => {}
                        Ok(AckProgress::Complete(outcome)) => return Some(outcome),
                        Err(_) => {
                            return Some(AckOutcome::NoAck(format!(
                                "ack timeout elapsed after {}",
                                humantime::format_duration(timeout_duration)
                            )));
                        }
                    }
                }
            }
        }
    }

    pub(in crate::runtime) fn parse_ack_timeout(
        domain: &Domain,
        ingestor: &Identifier,
        timeout: &str,
    ) -> Result<Duration, RuntimeError> {
        humantime::parse_duration(timeout).map_err(|source| RuntimeError::StartIngestor {
            domain: domain.as_str().to_string(),
            ingestor: ingestor.as_str().to_string(),
            reason: format!("invalid ack timeout '{timeout}': {source}"),
        })
    }

    fn validate_ingestor_start_settings(
        domain: &Domain,
        ingestor: &CreateIngestor,
    ) -> Result<(), RuntimeError> {
        match &ingestor.source {
            IngestSource::Kafka { mode, .. } | IngestSource::Pulsar { mode, .. } => match mode {
                KafkaIngestMode::AckParallel {
                    batch_timeout,
                    timeout,
                    retry_policy,
                    ..
                } => {
                    Self::parse_duration_setting(
                        domain,
                        &ingestor.name,
                        "batch timeout",
                        batch_timeout,
                    )?;
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                    Self::parse_retry_policy(domain, &ingestor.name, retry_policy)?;
                }
                KafkaIngestMode::AckSequential {
                    timeout,
                    retry_policy,
                } => {
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                    Self::parse_retry_policy(domain, &ingestor.name, retry_policy)?;
                }
                KafkaIngestMode::NoAckParallel => {}
            },
            IngestSource::Mqtt { mode, .. } => match mode {
                MqttIngestMode::AckParallel {
                    batch_timeout,
                    timeout,
                    retry_policy,
                    ..
                } => {
                    Self::parse_duration_setting(
                        domain,
                        &ingestor.name,
                        "batch timeout",
                        batch_timeout,
                    )?;
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                    Self::parse_retry_policy(domain, &ingestor.name, retry_policy)?;
                }
                MqttIngestMode::AckSequential {
                    timeout,
                    retry_policy,
                } => {
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                    Self::parse_retry_policy(domain, &ingestor.name, retry_policy)?;
                }
                MqttIngestMode::NoAckParallel { .. } | MqttIngestMode::NoAckSequential { .. } => {}
            },
            IngestSource::RabbitMq { mode, .. } => match mode {
                RabbitMqIngestMode::AckSequential { timeout, .. } => {
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                }
            },
            IngestSource::Sqs { mode, .. } => match mode {
                SqsIngestMode::AckSequential { timeout, .. } => {
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                }
            },
            IngestSource::Http { .. }
            | IngestSource::Prometheus { .. }
            | IngestSource::RedisPubSub { .. }
            | IngestSource::Nats { .. }
            | IngestSource::ZeroMq { .. }
            | IngestSource::Websockets { .. }
            | IngestSource::Endpoint { .. } => {}
        }
        Ok(())
    }

    pub(in crate::runtime) fn parse_duration_setting(
        domain: &Domain,
        ingestor: &Identifier,
        field: &str,
        value: &str,
    ) -> Result<Duration, RuntimeError> {
        humantime::parse_duration(value).map_err(|source| RuntimeError::StartIngestor {
            domain: domain.as_str().to_string(),
            ingestor: ingestor.as_str().to_string(),
            reason: format!("invalid {field} '{value}': {source}"),
        })
    }

    pub(in crate::runtime) fn parse_runtime_node_duration_setting(
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
        field: &str,
        value: &str,
    ) -> Result<Duration, RuntimeError> {
        humantime::parse_duration(value).map_err(|source| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "invalid {field} '{value}' for {kind} '{}': {source}",
                identifier.as_str()
            ),
        })
    }

    pub(in crate::runtime) fn parse_runtime_node_flush_policy(
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
        value: &str,
        max_batch_size: Option<&str>,
    ) -> Result<RuntimeFlushPolicy, RuntimeError> {
        if value.eq_ignore_ascii_case("IMMEDIATE") {
            Ok(RuntimeFlushPolicy::Immediate)
        } else {
            let interval = Self::parse_runtime_node_duration_setting(
                domain,
                kind,
                identifier,
                "flush_each",
                value,
            )?;
            let max_batch_size =
                max_batch_size.ok_or_else(|| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "{} '{}' FLUSH EACH requires MAX BATCH SIZE",
                        kind,
                        identifier.as_str()
                    ),
                })?;
            let max_batch_size = max_batch_size
                .parse::<ubyte::ByteUnit>()
                .map_err(|source| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "invalid max_batch_size '{}' for {} '{}': {}",
                        max_batch_size,
                        kind,
                        identifier.as_str(),
                        source
                    ),
                })?;
            Ok(RuntimeFlushPolicy::Each {
                interval,
                max_batch_size: max_batch_size.as_u64(),
            })
        }
    }

    pub(in crate::runtime) fn parse_runtime_node_input_collect_policy(
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
        policy: Option<&nervix_models::InputCollectPolicy>,
    ) -> Result<Option<RuntimeInputCollectPolicy>, RuntimeError> {
        policy
            .map(|policy| {
                let interval = Self::parse_runtime_node_duration_setting(
                    domain,
                    kind,
                    identifier,
                    "collect_for",
                    &policy.collect_for,
                )?;
                let max_batch_size = policy
                    .max_batch_size
                    .as_deref()
                    .map(|max_batch_size| {
                        max_batch_size
                            .parse::<ubyte::ByteUnit>()
                            .map(|size| size.as_u64())
                            .map_err(|source| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "invalid input collection max_batch_size '{}' for {} '{}': {}",
                                    max_batch_size,
                                    kind,
                                    identifier.as_str(),
                                    source
                                ),
                            })
                    })
                    .transpose()?;
                Ok(RuntimeInputCollectPolicy {
                    interval,
                    max_batch_size,
                })
            })
            .transpose()
    }

    pub(in crate::runtime) fn parse_retry_policy(
        domain: &Domain,
        ingestor: &Identifier,
        policy: &RetryPolicy,
    ) -> Result<ParsedRetryPolicy, RuntimeError> {
        Ok(ParsedRetryPolicy {
            backoff: Self::parse_duration_setting(
                domain,
                ingestor,
                "retry backoff",
                &policy.backoff,
            )?,
            max_backoff: Self::parse_duration_setting(
                domain,
                ingestor,
                "retry max backoff",
                &policy.max_backoff,
            )?,
        })
    }

    pub(in crate::runtime) async fn stop_ingestor(
        &self,
        domain: &Domain,
        ingestor: &Identifier,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.clone());
        let Some((_, runtime)) = self.ingestors.remove(&key) else {
            return Err(RuntimeError::IngestorNotRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.as_str().to_string(),
            });
        };

        match runtime {
            IngestorRuntime::Background {
                shutdown,
                branched,
                tasks,
            } => {
                if shutdown.send(true).is_err() {
                    warn!(
                        domain = domain.as_str(),
                        ingestor = ingestor.as_str(),
                        "ingestor shutdown signal had no receiver"
                    );
                }
                for task in tasks {
                    Self::await_shutdown_task(task, domain, Some(ingestor), "ingestor").await;
                }
                for branched in branched {
                    branched.shutdown().await;
                }
            }
            IngestorRuntime::Endpoint {
                route_keys,
                branched,
                shutdown,
                tasks,
            } => {
                if shutdown.send(true).is_err() {
                    warn!(
                        domain = domain.as_str(),
                        ingestor = ingestor.as_str(),
                        "endpoint ingestor shutdown signal had no receiver"
                    );
                }
                for task in tasks {
                    Self::await_shutdown_task(task, domain, Some(ingestor), "endpoint ingestor")
                        .await;
                }
                for route_key in route_keys {
                    let remove_route =
                        if let Some(mut bindings) = self.endpoint_bindings.get_mut(&route_key) {
                            bindings.retain(|binding| binding.runtime_key != key);
                            bindings.is_empty()
                        } else {
                            false
                        };
                    if remove_route {
                        self.endpoint_bindings.remove(&route_key);
                    }
                }
                for branched in branched {
                    branched.shutdown().await;
                }
            }
        }

        self.clear_ingestor_readiness(domain, ingestor);
        if self
            .ingestor_quiesce_control(domain, ingestor)
            .is_some_and(|control| !control.is_quiesced())
        {
            self.remove_ingestor_quiescence(domain, ingestor);
        }
        Ok(())
    }

    pub(in crate::runtime) async fn await_shutdown_task(
        task: JoinHandle<()>,
        domain: &Domain,
        ingestor: Option<&Identifier>,
        task_kind: &str,
    ) {
        const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

        Self::await_shutdown_task_with_grace(
            task,
            domain,
            ingestor,
            task_kind,
            SHUTDOWN_GRACE_PERIOD,
        )
        .await;
    }

    async fn await_shutdown_task_with_grace(
        mut task: JoinHandle<()>,
        domain: &Domain,
        ingestor: Option<&Identifier>,
        task_kind: &str,
        grace_period: Duration,
    ) {
        match tokio::time::timeout(grace_period, &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if error.is_cancelled() {
                    warn!(
                        domain = domain.as_str(),
                        ingestor = ingestor.map(Identifier::as_str),
                        task_kind,
                        "shutdown task was cancelled"
                    );
                } else {
                    error!(
                        domain = domain.as_str(),
                        ingestor = ingestor.map(Identifier::as_str),
                        task_kind,
                        error = %error,
                        "shutdown task join failed"
                    );
                }
            }
            Err(_) => {
                warn!(
                    domain = domain.as_str(),
                    ingestor = ingestor.map(Identifier::as_str),
                    task_kind,
                    grace_period = %humantime::format_duration(grace_period),
                    "shutdown task exceeded grace period; aborting"
                );
                task.abort();
                if let Err(error) = task.await
                    && !error.is_cancelled()
                {
                    error!(
                        domain = domain.as_str(),
                        ingestor = ingestor.map(Identifier::as_str),
                        task_kind,
                        error = %error,
                        "aborted shutdown task join failed"
                    );
                }
            }
        }
    }

    pub(in crate::runtime) async fn ingestor_dependencies(
        &self,
        domain: &Domain,
        ingestor: &CreateIngestor,
    ) -> Result<IngestorDependencies, RuntimeError> {
        let Some(execution) = self.executions.get(domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: ingestor
                    .output_routes
                    .relays()
                    .next()
                    .map(|relay| relay.as_str().to_string())
                    .unwrap_or_else(|| "<missing>".to_string()),
            });
        };
        let Some(codec) = execution.codecs.get(&ingestor.decode_using_codec).cloned() else {
            return Err(RuntimeError::CodecNotInstantiated {
                domain: domain.as_str().to_string(),
                codec: ingestor.decode_using_codec.as_str().to_string(),
            });
        };
        let empty_branching = Vec::new();
        let filter_where = compile_expression_filter_program(
            RuntimeCompileTarget {
                domain,
                identifier: &ingestor.name,
            },
            ingestor.filter_where.as_ref(),
            RuntimeVmSchema {
                schema: codec.schema().arrow_schema(),
                sensitivity: codec.schema().vm_sensitivity(),
            },
            ingest_source_supports_headers(&ingestor.source),
            MessageErrorOperation::FilterWhere,
            RuntimeVmCompileContext {
                available_materialized_streams: &execution.materialized_stream_specs,
                available_lookups: &execution.lookups,
                current_branching: &empty_branching,
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: Some(&execution.udfs),
            },
        )?;
        let mut output_routes = RelayProcessorOutputsNode {
            routes: Vec::with_capacity(ingestor.output_routes.routes.len()),
        };
        for output in ingestor.output_routes.outputs() {
            if !execution.relay_services.contains_key(&output.relay) {
                return Err(RuntimeError::RelayNotInstantiated {
                    domain: domain.as_str().to_string(),
                    relay: output.relay.as_str().to_string(),
                });
            }
            let output_schema = execution
                .relay_schemas
                .get(&output.relay)
                .cloned()
                .ok_or_else(|| RuntimeError::RelayNotInstantiated {
                    domain: domain.as_str().to_string(),
                    relay: output.relay.as_str().to_string(),
                })?;
            let compiled_program = compile_ingestor_filter_map_program(
                domain,
                &ingestor.name,
                &ingestor.source,
                &output.construction,
                RuntimeVmSchemaPair {
                    input: codec.schema().arrow_schema(),
                    input_sensitivity: codec.schema().vm_sensitivity(),
                    output: output_schema.arrow_schema(),
                    output_sensitivity: output_schema.vm_sensitivity(),
                },
                RuntimeVmCompileContext {
                    available_materialized_streams: &execution.materialized_stream_specs,
                    available_lookups: &execution.lookups,
                    current_branching: &execution
                        .relay_branchings
                        .get(&output.relay)
                        .cloned()
                        .unwrap_or_default(),
                    current_branch_schema: None,
                    current_branch_sensitivity: None,
                    udfs: Some(&execution.udfs),
                },
            )?;
            let flush_policy = output
                .flush_policy
                .as_ref()
                .map(|policy| {
                    Self::parse_runtime_node_flush_policy(
                        domain,
                        "ingestor output",
                        &output.relay,
                        &policy.flush_each,
                        policy.max_batch_size.as_deref(),
                    )
                })
                .transpose()?;
            output_routes.routes.push(RelayProcessorOutputNode {
                relay: output.relay.clone(),
                construction: output.construction.clone(),
                branch: output.branch.clone(),
                flush_policy,
                message_error_policy: output.message_error_policy.clone(),
                pending: Vec::new(),
                next_flush: None,
                compiled_program,
            });
        }
        if output_routes.base_relay().is_none() {
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "ingestor '{}' must declare at least one output route",
                    ingestor.name.as_str()
                ),
            });
        }
        let model_index = execution
            .schedule
            .nodes
            .iter()
            .map(|node| ((node.kind, node.identifier.clone()), (*node.config).clone()))
            .collect::<HashMap<_, _>>();
        let mut branched_templates = HashMap::default();
        if let Some(specs) = execution.branched_ingestors.get(&ingestor.name) {
            for spec in specs {
                let template = materialize_ingestor_route_template(
                    spec,
                    &model_index,
                    &execution.relay_registries,
                    &execution.relay_services,
                )
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason,
                })?;
                branched_templates
                    .insert(spec.root_relay.clone(), (execution.graph.clone(), template));
            }
        }
        Ok(IngestorDependencies {
            output_routes,
            filter_where,
            codec,
            branched_templates,
        })
    }

    pub(in crate::runtime) async fn load_lookup_runtime(
        &self,
        domain: &Domain,
        lookup: CreateLookup,
        codec: Arc<CompiledCodec>,
    ) -> Result<LookupRuntime, String> {
        let Some(resource_store) = self.resource_store.read().clone() else {
            return Err("resource store is not attached".to_string());
        };
        let Some(resource_version) = self
            .latest_resource_versions
            .get(&(domain.clone(), lookup.resource.clone()))
            .map(|value| *value)
        else {
            return Err(format!(
                "resource '{}' has no uploaded versions for lookup '{}' in domain '{}'",
                lookup.resource.as_str(),
                lookup.name.as_str(),
                domain.as_str()
            ));
        };
        let resource_id =
            ResourceId::new(domain.clone(), lookup.resource.clone(), resource_version);
        let path = resource_store
            .resolve_content_path(&resource_id, &lookup.path)
            .map_err(|error| error.to_string())?;
        let file = tokio::fs::File::open(&path).await.map_err(|error| {
            format!(
                "failed to open lookup file '{}' for lookup '{}' in domain '{}': {}",
                path.display(),
                lookup.name.as_str(),
                domain.as_str(),
                error
            )
        })?;
        let mut lines = tokio::io::BufReader::new(file).lines();
        let mut entries = HashMap::new();
        let mut line_number = 0usize;
        while let Some(line) = lines.next_line().await.map_err(|error| {
            format!(
                "failed to read lookup file '{}' for lookup '{}': {}",
                path.display(),
                lookup.name.as_str(),
                error
            )
        })? {
            tokio::task::consume_budget().await;
            line_number += 1;
            if line.trim().is_empty() {
                continue;
            }
            let record = decode_ingested_payload_owned(codec.clone(), line.into_bytes())
                .await
                .map_err(|error| {
                    format!(
                        "failed to decode lookup '{}' line {}: {}",
                        lookup.name.as_str(),
                        line_number,
                        error
                    )
                })?;
            let Some(value) = record.value(lookup.key_field.as_str()) else {
                return Err(format!(
                    "lookup '{}' line {} is missing key field '{}'",
                    lookup.name.as_str(),
                    line_number,
                    lookup.key_field.as_str()
                ));
            };
            entries.insert(value.to_key_fragment(), record);
        }

        Ok(LookupRuntime {
            model: lookup,
            resource_version,
            schema: codec.schema(),
            entries: Arc::new(entries),
        })
    }
}
