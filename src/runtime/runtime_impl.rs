use rdkafka::consumer::StreamConsumer;

use super::*;

struct ScheduledIngestorStartSpec {
    domain: Domain,
    source_model: Model,
    ingestor: CreateIngestor,
    kafka_offset_state: Option<Arc<ReplicatedKafkaOffsetState>>,
}

fn branch_relays_from_branched_specs(specs: &[BranchedIngestorSpec]) -> HashSet<Identifier> {
    specs
        .iter()
        .filter(|spec| spec.branch_ttl.is_some())
        .flat_map(BranchedIngestorSpec::relay_ids)
        .collect()
}

fn relay_branching_schema_for_runtime(
    domain: &Domain,
    relay_identifier: &Identifier,
    relay: &CreateRelay,
    effective_branching_schema: Option<&Identifier>,
    schemas: &HashMap<Identifier, Arc<CompiledSchema>>,
) -> Result<Option<Arc<arrow_schema::Schema>>, RuntimeError> {
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
struct ProtobufCodecCompileConfig {
    files: Vec<String>,
    includes: Vec<String>,
}

impl ProtobufCodecCompileConfig {
    fn from_entries(entries: &[ClientConfigEntry]) -> Result<Self, String> {
        let mut files = Vec::new();
        let mut includes = Vec::new();
        for entry in entries {
            match entry.key.to_ascii_lowercase().as_str() {
                "file" | "files" => Self::append_paths(&mut files, &entry.value),
                "include" | "includes" => Self::append_paths(&mut includes, &entry.value),
                other => {
                    return Err(format!(
                        "unsupported protobuf codec config key '{other}'; expected 'file', \
                         'files', 'include', or 'includes'"
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
        resource: &Identifier,
        version: u64,
    ) -> Result<prost_types::FileDescriptorSet, String> {
        let files = if self.files.is_empty() {
            Self::collect_resource_proto_files(store, resource, version)?
        } else {
            self.files
                .iter()
                .map(|path| {
                    store
                        .resolve_content_path(resource, version, path)
                        .map_err(|error| format!("invalid protobuf source path '{path}': {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        if files.is_empty() {
            return Err(format!(
                "protobuf resource '{}' version {version} contains no .proto files",
                resource.as_str()
            ));
        }
        let includes = if self.includes.is_empty() {
            vec![store.content_root(resource, version)]
        } else {
            self.includes
                .iter()
                .map(|path| {
                    store
                        .resolve_content_path(resource, version, path)
                        .map_err(|error| format!("invalid protobuf include path '{path}': {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        protox::compile(files, includes)
            .map_err(|error| format!("failed to compile protobuf descriptors: {error}"))
    }

    fn collect_resource_proto_files(
        store: &ResourceStore,
        resource: &Identifier,
        version: u64,
    ) -> Result<Vec<PathBuf>, String> {
        let root = store.content_root(resource, version);
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
    pub(in crate::runtime) fn vm_program_error(
        error: nervix_nspl::vm_program::ParseFromSourceError,
    ) -> String {
        match error {
            nervix_nspl::vm_program::ParseFromSourceError::Lex { diagnostics, .. }
            | nervix_nspl::vm_program::ParseFromSourceError::Parse { diagnostics, .. } => {
                diagnostics
                    .first()
                    .map(|diagnostic| diagnostic.message.clone())
                    .unwrap_or_else(|| "unknown FILTER-MAP parse error".to_string())
            }
        }
    }

    async fn compile_domain_codec(
        &self,
        domain: &Domain,
        codec: &CreateCodec,
        schema: Arc<CompiledSchema>,
        wire_schema: Option<&CreateWireSchemaStmt>,
    ) -> Result<Arc<CompiledCodec>, RuntimeError> {
        let protobuf_descriptor = if let CodecWireFormat::Protobuf(config) = &codec.wire_format {
            Some(
                self.compile_protobuf_codec_descriptor(codec, config)
                    .await
                    .map_err(|reason| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason,
                    })?,
            )
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

    async fn compile_protobuf_codec_descriptor(
        &self,
        codec: &CreateCodec,
        config: &CodecProtobufConfig,
    ) -> Result<ProtobufCodecDescriptor, String> {
        let store = self
            .resource_store
            .read()
            .clone()
            .ok_or_else(|| "protobuf codec requires an attached resource store".to_string())?;
        let version = if let Some(version) = config.resource_version {
            version
        } else {
            self.resolve_resource_version(&config.resource, config.resource.as_str())?
        };
        let resource = config.resource.clone();
        let compile_config = ProtobufCodecCompileConfig::from_entries(&config.config)?;
        let store_for_task = store.clone();
        let file_descriptor_set = tokio::task::spawn_blocking(move || {
            compile_config.compile_descriptor_set(&store_for_task, &resource, version)
        })
        .await
        .map_err(|error| {
            format!("failed to join protobuf descriptor compilation task: {error}")
        })??;

        ProtobufCodecDescriptor::from_file_descriptor_set(
            codec,
            file_descriptor_set,
            &config.message,
        )
        .map_err(|error| error.to_string())
    }

    pub(in crate::runtime) fn emitter_task_deps(
        &self,
        deps: ExecutionBuildDeps<'_>,
        emitter: &CreateEmitter,
    ) -> Result<EmitterTaskDeps, RuntimeError> {
        let Some(input_schema) = deps.relay_schemas.get(&emitter.from_relay).cloned() else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: deps.domain.as_str().to_string(),
                reason: format!(
                    "missing emitter input relay schema '{}'",
                    emitter.from_relay.as_str()
                ),
            });
        };
        Ok(EmitterTaskDeps {
            input_schema,
            input_branching: deps
                .relay_branchings
                .get(&emitter.from_relay)
                .cloned()
                .unwrap_or_default(),
            input_branching_schema: deps
                .relay_branching_schemas
                .get(&emitter.from_relay)
                .cloned()
                .flatten(),
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
        let state_store = db
            .map(RuntimeStateStore::from_database)
            .transpose()?
            .map(Arc::new);
        Ok(Self {
            ingestors: Arc::new(DashMap::default()),
            ingestors_paused_for_memory_pressure: Arc::new(AtomicBool::new(false)),
            ingestor_transient_errors: Arc::new(DashMap::default()),
            ingestor_reconnect_backoffs: Arc::new(DashMap::default()),
            ingestor_readiness: Arc::new(DashMap::default()),
            emitter_transient_errors: Arc::new(DashMap::default()),
            emitter_reconnect_backoffs: Arc::new(DashMap::default()),
            executions: Arc::new(DashMap::default()),
            schedule_apply_lock: Arc::new(Mutex::new(())),
            domain_instantiation_errors: Arc::new(DashMap::default()),
            domains: Arc::new(DashMap::default()),
            domain_graphs: Arc::new(DashMap::default()),
            endpoint_bindings: Arc::new(DashMap::default()),
            relay_boundary_fanouts: Arc::new(DashMap::default()),
            events,
            emitter_faults: hooks.emitter_faults,
            ingestor_faults: hooks.ingestor_faults,
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
            replicated_window_processor_states: Arc::new(DashMap::default()),
            replicated_wasm_processor_states: Arc::new(DashMap::default()),
            replicated_branch_aggregated_states: Arc::new(DashMap::default()),
            correlator_states: Arc::new(DashMap::default()),
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
            temp_dir: Arc::new(temp_dir),
            metrics: RuntimeMetrics::default(),
        })
    }

    pub fn metrics(&self) -> RuntimeMetrics {
        self.metrics.clone()
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
        let key = RuntimeKey::new(domain.clone(), emitter.clone());
        self.emitter_transient_errors
            .insert(key.clone(), error.into());
        self.emitter_reconnect_backoffs.insert(
            key,
            RuntimeReconnectStatus {
                backoff,
                retry_at: Instant::now() + backoff,
            },
        );
    }

    pub(in crate::runtime) fn clear_emitter_transient_error(
        &self,
        domain: &Domain,
        emitter: &Identifier,
    ) {
        self.emitter_transient_errors
            .remove(&RuntimeKey::new(domain.clone(), emitter.clone()));
        self.emitter_reconnect_backoffs
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
        self.emitter_reconnect_backoffs
            .get(&RuntimeKey::new(domain.clone(), emitter.clone()))
            .map(|status| humantime::format_duration(status.value().backoff).to_string())
    }

    fn emitter_reconnect_wait_millis(&self, domain: &Domain, emitter: &Identifier) -> Option<u64> {
        self.emitter_reconnect_backoffs
            .get(&RuntimeKey::new(domain.clone(), emitter.clone()))
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
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeStateKind::BranchAggregated,
            kind,
            identifier: identifier.clone(),
            branch_key: None,
        };
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
            if let Some(mut existing) = self
                .latest_resource_versions
                .get_mut(&resource.id.identifier)
            {
                if resource.id.version > *existing {
                    *existing = resource.id.version;
                }
            } else {
                self.latest_resource_versions
                    .insert(resource.id.identifier.clone(), resource.id.version);
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

    pub(in crate::runtime) fn resolve_resource_version(
        &self,
        identifier: &Identifier,
        spec: &str,
    ) -> Result<u64, String> {
        if let Some((name, version)) = spec.rsplit_once('@') {
            let parsed = Identifier::parse(name)
                .map_err(|_| format!("invalid client resource identifier '{name}'"))?;
            if &parsed != identifier {
                return Err(format!(
                    "client resource mount '{spec}' resolved to unexpected identifier '{}'",
                    parsed.as_str()
                ));
            }
            return version
                .parse::<u64>()
                .map_err(|_| format!("invalid client resource version '{version}'"));
        }

        let resources = self.resource_versions.read();
        resources
            .next_version_by_identifier
            .iter()
            .find_map(|(known_identifier, next_version)| {
                (known_identifier == identifier).then_some(next_version.saturating_sub(1))
            })
            .filter(|version| *version > 0)
            .ok_or_else(|| {
                format!(
                    "resource '{}' has no installed versions",
                    identifier.as_str()
                )
            })
    }

    pub(crate) fn resolve_client_config(
        &self,
        mount: Option<&Identifier>,
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<ResolvedClientConfig, String> {
        self.resolve_client_config_with_template_vars(mount, config, BTreeMap::default())
    }

    pub(in crate::runtime) fn resolve_client_config_with_instance(
        &self,
        mount: Option<&Identifier>,
        config: &[nervix_models::ClientConfigEntry],
        instance: u64,
    ) -> Result<ResolvedClientConfig, String> {
        self.resolve_client_config_with_template_vars(
            mount,
            config,
            BTreeMap::from([("instance".to_string(), instance.to_string())]),
        )
    }

    fn resolve_client_config_with_template_vars(
        &self,
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
        let version = self.resolve_resource_version(mount, mount.as_str())?;
        let source_root = resource_store.content_root(mount, version);
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
        if let RuntimeStateKind::MaterializedRelay = placement.state
            && placement.branch_key.is_none()
        {
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
                {
                    continue;
                }
                found = true;
                latest_lsm = latest_lsm.max(state.current_lsm.load(Ordering::SeqCst));
                if concrete.branch_key.is_none() {
                    metrics_snapshot = state.metrics_snapshot(&self.metrics);
                }
                entries.extend(
                    self.visible_materialized_stream_remote_entries(concrete, state.value()),
                );
            }
            if found {
                if latest_lsm <= after_lsm {
                    return Ok(None);
                }
                return Ok(Some(PersistedRuntimeStateEntry {
                    lsm: latest_lsm,
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
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| "timed out waiting for state sync response".to_string())?
            .map_err(|_| "state sync response channel closed".to_string())?
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
            .await
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
            .await
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

    pub(in crate::runtime) fn correlator_state(
        &self,
        placement: RuntimeStatePlacement,
    ) -> SharedCorrelatorBranchState {
        if let Some(existing) = self.correlator_states.get(&placement) {
            return existing.clone();
        }
        let state = Arc::new(ParkingMutex::new(CorrelatorBranchState::default()));
        self.correlator_states.insert(placement, state.clone());
        state
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
                    .request_state_sync(&primary_node, &state.placement, after_lsm)
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
                    .request_state_sync(&primary_node, &state.placement, after_lsm)
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
            if let (DomainPace::Paced, nervix_models::DomainStatus::Running) =
                (state.config.pace, &state.status)
            {
            } else {
                entry.clock = None;
                entry.ticks.lock().clear();
            }
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
        let runtime_key = RuntimeKey::new(domain.clone(), relay.clone());
        if let Some(state) = self.expiring_stream_states.get(&runtime_key) {
            state.touch(key, now);
        }
    }

    pub(in crate::runtime) fn remove_stream_key_presence(
        &self,
        domain: &Domain,
        relay: &Identifier,
        key: &Option<BranchKey>,
    ) {
        let runtime_key = RuntimeKey::new(domain.clone(), relay.clone());
        if let Some(state) = self.expiring_stream_states.get(&runtime_key) {
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
    ) -> Result<(), RelayRecordBatch> {
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
    ) -> Result<(), RelayRecordBatch> {
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
        let runtime_key = RuntimeKey::new(domain.clone(), relay.clone());
        if let Some(existing) = self.expiring_stream_states.get(&runtime_key) {
            return existing.clone();
        }
        let state = Arc::new(ExpiringRelayState::new());
        self.expiring_stream_states
            .insert(runtime_key, state.clone());
        state
    }

    pub(in crate::runtime) fn clear_expiring_stream_states_for_domain(&self, domain: &Domain) {
        let relays = self
            .expiring_stream_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|runtime_key| &runtime_key.domain == domain)
            .collect::<Vec<_>>();
        for runtime_key in relays {
            self.expiring_stream_states.remove(&runtime_key);
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
                    let (acks, completion) = AckSet::root();
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
        reason: String,
    ) {
        self.handle_message_error_with_policy(
            domain,
            node_kind,
            node,
            &policies.message,
            message,
            reason,
        )
        .await;
    }

    pub(in crate::runtime) async fn handle_message_error_with_policy(
        &self,
        domain: &Domain,
        node_kind: &str,
        node: &Identifier,
        policy: &MessageErrorPolicy,
        message: RelayMessage,
        reason: String,
    ) {
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
                    reason
                )));
                warn!(
                    domain = domain.as_str(),
                    node_kind,
                    node = node.as_str(),
                    reason = %reason,
                    "runtime node handled message error"
                );
                message.acks.no_ack(reason);
            }
            MessageErrorPolicy::Dlq { relay, mappings } => {
                let context = MessageErrorContext {
                    domain,
                    node_kind,
                    node,
                    message: &message,
                    reason: &reason,
                };
                if let Err(error) = self
                    .dispatch_message_error_to_dlq(context, relay, mappings)
                    .await
                {
                    let _ = self.events.send(RuntimeEvent::Error(format!(
                        "{} '{}' failed to dispatch message error to DLQ '{}' in domain '{}': {}",
                        node_kind,
                        node.as_str(),
                        relay.as_str(),
                        domain.as_str(),
                        error
                    )));
                    message.acks.no_ack(format!(
                        "{} '{}' failed to dispatch message error to DLQ '{}': {}",
                        node_kind,
                        node.as_str(),
                        relay.as_str(),
                        error
                    ));
                    return;
                }
                message.acks.ack_success();
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
            self.handle_message_error(
                domain,
                node_kind,
                node,
                policies,
                error.message,
                error.reason,
            )
            .await;
        }
    }

    pub(in crate::runtime) async fn dispatch_message_error_to_dlq(
        &self,
        context: MessageErrorContext<'_>,
        relay: &Identifier,
        mappings: &[ErrorFieldMapping],
    ) -> Result<(), String> {
        let MessageErrorContext {
            domain,
            node_kind,
            node,
            message,
            reason,
        } = context;
        let Some(execution) = self.executions.get(domain) else {
            return Err(format!("domain '{}' is not instantiated", domain.as_str()));
        };
        let Some(schema) = execution.relay_schemas.get(relay).cloned() else {
            return Err(format!(
                "DLQ relay '{}' schema is not instantiated in domain '{}'",
                relay.as_str(),
                domain.as_str()
            ));
        };
        let Some(registry) = execution.relay_registries.get(relay).cloned() else {
            return Err(format!(
                "DLQ relay '{}' is not instantiated in domain '{}'",
                relay.as_str(),
                domain.as_str()
            ));
        };
        let Some(services) = execution.relay_services.get(relay).cloned() else {
            return Err(format!(
                "DLQ relay '{}' services are not instantiated in domain '{}'",
                relay.as_str(),
                domain.as_str()
            ));
        };
        let branching = execution
            .relay_branchings
            .get(relay)
            .cloned()
            .unwrap_or_default();
        drop(execution);

        let dlq_record = DecodedRecord::from_fields(mappings.iter().map(|mapping| {
            (
                mapping.field.as_str().to_string(),
                self.message_error_mapping_value(
                    domain,
                    node_kind,
                    node,
                    message,
                    reason,
                    &mapping.value,
                ),
            )
        }))
        .into_runtime_record(message.record.metadata().clone());
        let key = if branching.is_empty() {
            None
        } else {
            resolve_concrete_branch(&dlq_record, &branching, node)?.into_relay_key()
        };
        let batch = RelayRecordBatch::single(schema, key, dlq_record, AckSet::empty())?;
        self.ingest_stream_boundary_message(domain, relay, &registry, &services, &batch)
            .await
            .map_err(|_| {
                format!(
                    "DLQ relay '{}' rejected message error from {} '{}'",
                    relay.as_str(),
                    node_kind,
                    node.as_str()
                )
            })?;
        Ok(())
    }

    pub(in crate::runtime) fn message_error_mapping_value(
        &self,
        domain: &Domain,
        node_kind: &str,
        node: &Identifier,
        message: &RelayMessage,
        reason: &str,
        value: &str,
    ) -> RuntimeValue {
        match value {
            "message_error.message" => RuntimeValue::String(reason.to_string()),
            "message_error.domain" => RuntimeValue::String(domain.as_str().to_string()),
            "message_error.node" => RuntimeValue::String(node.as_str().to_string()),
            "message_error.node_kind" => RuntimeValue::String(node_kind.to_string()),
            "message_error.key" => {
                RuntimeValue::String(branch_key_display(&message.key).to_string())
            }
            "message_error.record" => RuntimeValue::String(message.record.to_json_string()),
            other => RuntimeValue::String(other.to_string()),
        }
    }

    pub(in crate::runtime) async fn dispatch_ingested_record(
        &self,
        dispatch: IngestDispatch<'_>,
    ) -> Result<(), String> {
        let mut record = dispatch.record.into_runtime_record(
            RuntimeRecordMetadata::from_ingested_at_watermarks(
                dispatch.ingested_at,
                dispatch.ingested_at,
            ),
        );
        if let Some(filter_where) = dispatch.filter_where {
            let branch_key = if let Some(mappings) = dispatch.branch_value_mappings {
                resolve_concrete_branch_from_mappings(&record, None, mappings, dispatch.ingestor)?
            } else {
                resolve_concrete_branch(&record, dispatch.branching, dispatch.ingestor)?
            }
            .into_relay_key();
            let side_inputs = self
                .load_materialized_side_inputs(
                    dispatch.domain,
                    &branch_key,
                    &filter_where.materialized_interest,
                    &self
                        .executions
                        .get(dispatch.domain)
                        .map(|execution| execution.materialized_stream_owner_nodes.clone())
                        .unwrap_or_default(),
                )
                .await?;
            let execution_now = self
                .current_stream_expiration_time(dispatch.domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp);
            let Some(transformed) = execute_filter_map_on_record(
                filter_where,
                augment_runtime_record_with_side_inputs(record, &side_inputs),
                None,
                dispatch.filter_map_metadata.as_ref(),
                execution_now,
            )
            .await?
            else {
                dispatch.acks.ack_success();
                return Ok(());
            };
            record = transformed;
        }
        let event_timestamp = self.resolve_ingested_record_timestamp(
            dispatch.domain,
            dispatch.ingestor,
            dispatch.timestamp_source,
            &record,
        )?;
        self.ensure_domain_allows_ingestion(dispatch.domain, dispatch.ingestor, event_timestamp)?;
        record = record.with_ingested_at_watermarks(event_timestamp);

        let Some(execution) = self.executions.get(dispatch.domain) else {
            return Err(format!(
                "domain '{}' is not instantiated",
                dispatch.domain.as_str()
            ));
        };
        let relay_schemas = execution.relay_schemas.clone();
        let relay_registries = execution.relay_registries.clone();
        let relay_services = execution.relay_services.clone();
        let relay_branchings = execution.relay_branchings.clone();
        let owner_nodes = execution.materialized_stream_owner_nodes.clone();
        drop(execution);
        self.metrics
            .observe_global_node_without_stream_received(NodeWithoutRelayObservation {
                domain: dispatch.domain,
                kind: ModelKind::Ingestor,
                node: dispatch.ingestor,
                physical_node_id: self.local_node_id.read().as_deref(),
                messages: 1,
                bytes: record.estimated_bytes(),
                domain_timestamp: Some(event_timestamp),
            });
        self.mark_branch_aggregated_metrics_updated(
            dispatch.domain,
            ModelKind::Ingestor,
            dispatch.ingestor,
        );
        let mut routed_records = Vec::new();
        for output in &dispatch.output_routes.routes {
            let output_record = if let Some(filter_map) = output.compiled_program.as_ref() {
                let empty_branching = Vec::new();
                let output_branching = relay_branchings
                    .get(&output.relay)
                    .unwrap_or(&empty_branching);
                let branch_key = if output_branching.is_empty() {
                    ConcreteBranch::Root
                } else if let Some(mappings) = dispatch.branch_value_mappings {
                    resolve_concrete_branch_from_mappings(
                        &record,
                        None,
                        mappings,
                        dispatch.ingestor,
                    )?
                } else {
                    resolve_concrete_branch(&record, output_branching, dispatch.ingestor)?
                }
                .into_relay_key();
                let side_inputs = self
                    .load_materialized_side_inputs(
                        dispatch.domain,
                        &branch_key,
                        &filter_map.materialized_interest,
                        &owner_nodes,
                    )
                    .await?;
                let execution_now = self
                    .current_stream_expiration_time(dispatch.domain)
                    .ok()
                    .flatten()
                    .unwrap_or_else(current_timestamp);
                execute_filter_map_on_record(
                    filter_map,
                    augment_runtime_record_with_side_inputs(record.clone(), &side_inputs),
                    None,
                    dispatch.filter_map_metadata.as_ref(),
                    execution_now,
                )
                .await?
            } else {
                Some(record.clone())
            };
            if let Some(output_record) = output_record {
                routed_records.push((output.relay.clone(), output_record));
            }
        }
        if routed_records.is_empty() {
            dispatch.acks.ack_success();
            return Ok(());
        }
        let mut ack_queue = VecDeque::with_capacity(routed_records.len());
        for _ in 1..routed_records.len() {
            ack_queue.push_back(dispatch.acks.attached());
        }
        ack_queue.push_front(dispatch.acks);
        for (relay, output_record) in routed_records {
            let acks = ack_queue
                .pop_front()
                .expect("ack queue must match routed output count");
            if let Some(sender) = dispatch.branched_senders.get(&relay) {
                if let Err(error) = sender
                    .send(BranchedEntrypointInput::UnresolvedRecord {
                        record: output_record,
                        acks,
                    })
                    .await
                {
                    let BranchedEntrypointInput::UnresolvedRecord { acks, .. } = error.0 else {
                        continue;
                    };
                    self.handle_general_error_for_acks(
                        dispatch.domain,
                        ModelKind::Ingestor.as_str(),
                        dispatch.ingestor,
                        &ErrorPolicies::handled_by_log(),
                        std::iter::once(&acks),
                        format!(
                            "ingestor '{}' failed to forward record to branch entrypoint for \
                             relay '{}'",
                            dispatch.ingestor.as_str(),
                            relay.as_str()
                        ),
                    );
                }
                continue;
            }
            let empty_branching = Vec::new();
            let output_branching = relay_branchings.get(&relay).unwrap_or(&empty_branching);
            let key = if output_branching.is_empty() {
                ConcreteBranch::Root
            } else if let Some(mappings) = dispatch.branch_value_mappings {
                resolve_concrete_branch_from_mappings(
                    &output_record,
                    None,
                    mappings,
                    dispatch.ingestor,
                )?
            } else {
                resolve_concrete_branch(&output_record, output_branching, dispatch.ingestor)?
            }
            .into_relay_key();
            let Some(schema) = relay_schemas.get(&relay).cloned() else {
                return Err(format!(
                    "stream '{}' schema is not instantiated in domain '{}'",
                    relay.as_str(),
                    dispatch.domain.as_str()
                ));
            };
            let batch = RelayRecordBatch::single(schema, key, output_record, acks)?;
            if let Some(branch_key) = batch.key.as_ref() {
                self.metrics.observe_branch_node_without_stream_received(
                    branch_key.as_str(),
                    NodeWithoutRelayObservation {
                        domain: dispatch.domain,
                        kind: ModelKind::Ingestor,
                        node: dispatch.ingestor,
                        physical_node_id: self.local_node_id.read().as_deref(),
                        messages: batch.message_count(),
                        bytes: batch.estimated_bytes(),
                        domain_timestamp: batch.domain_timestamp(),
                    },
                );
            }
            self.metrics.observe_global_node_sent(NodeBatchObservation {
                domain: dispatch.domain,
                kind: ModelKind::Ingestor,
                node: dispatch.ingestor,
                relay: &relay,
                physical_node_id: self.local_node_id.read().as_deref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            });
            self.mark_branch_aggregated_metrics_updated(
                dispatch.domain,
                ModelKind::Ingestor,
                dispatch.ingestor,
            );
            let Some(registry) = relay_registries.get(&relay) else {
                return Err(format!(
                    "stream '{}' is not instantiated in domain '{}'",
                    relay.as_str(),
                    dispatch.domain.as_str()
                ));
            };
            let Some(services) = relay_services.get(&relay) else {
                return Err(format!(
                    "stream '{}' services are not instantiated in domain '{}'",
                    relay.as_str(),
                    dispatch.domain.as_str()
                ));
            };
            let _ = self
                .ingest_stream_boundary_message(dispatch.domain, &relay, registry, services, &batch)
                .await;
        }
        Ok(())
    }

    pub(in crate::runtime) async fn select_ingested_batch_rows(
        &self,
        selection: IngestBatchSelection<'_>,
    ) -> Result<HashSet<usize>, String> {
        let Some(filter_where) = selection.filter_where else {
            return Ok((0..selection.records.len()).collect());
        };
        let Some(execution) = self.executions.get(selection.domain) else {
            return Err(format!(
                "domain '{}' is not instantiated",
                selection.domain.as_str()
            ));
        };
        let owner_nodes = execution.materialized_stream_owner_nodes.clone();
        drop(execution);

        let mut rows_by_branch = HashMap::<Option<BranchKey>, Vec<usize>>::default();
        for (row, record) in selection.records.iter().enumerate() {
            let branch_key = if let Some(mappings) = selection.branch_value_mappings {
                resolve_concrete_branch_from_mappings(record, None, mappings, selection.ingestor)?
            } else {
                resolve_concrete_branch(record, selection.branching, selection.ingestor)?
            }
            .into_relay_key();
            rows_by_branch.entry(branch_key).or_default().push(row);
        }

        let mut selected_rows = HashSet::default();
        for (branch_key, rows) in rows_by_branch {
            tokio::task::consume_budget().await;
            let side_inputs = self
                .load_materialized_side_inputs(
                    selection.domain,
                    &branch_key,
                    &filter_where.materialized_interest,
                    &owner_nodes,
                )
                .await?;
            let execution_now = self
                .current_stream_expiration_time(selection.domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp);
            let input_records = rows
                .iter()
                .map(|row| selection.records[*row].clone())
                .collect::<Vec<_>>();
            let input_records =
                augment_runtime_records_with_side_inputs(input_records, &side_inputs);
            let branch_keys = rows.iter().map(|_| branch_key.clone()).collect::<Vec<_>>();
            let input_records =
                augment_runtime_records_with_branch_keys(input_records, &branch_keys)?;
            let input_records = augment_runtime_records_with_lookup_hash_maps(
                input_records,
                filter_where,
                execution_now,
            )
            .await?;
            let filter_map_metadata = selection.filter_map_metadata.map(|metadata| {
                rows.iter()
                    .map(|row| metadata[*row].clone())
                    .collect::<Vec<_>>()
            });
            let vm_batch = vm_typed_batch_from_runtime_records_with_metadata(
                &input_records,
                filter_map_metadata.as_deref(),
                &filter_where.compiled.input_schema,
            )?;
            let result = execute_program_with_selection_in_context(
                filter_where.compiled.as_ref(),
                &vm_batch,
                &VmExecutionContext { now: execution_now },
            )
            .await
            .map_err(|error| format!("FILTER WHERE execution failed: {error}"))?;
            for (output_row, &input_row) in result.selected_rows.iter().enumerate() {
                if let Some(side_error) = result.batch.errors()[output_row].first() {
                    return Err(format!(
                        "FILTER WHERE side error {}: {} at {}",
                        side_error.code.as_str(),
                        side_error.message,
                        side_error.span
                    ));
                }
                let Some(original_row) = rows.get(input_row).copied() else {
                    return Err(format!(
                        "FILTER WHERE selected row {input_row} outside input batch"
                    ));
                };
                selected_rows.insert(original_row);
            }
        }

        Ok(selected_rows)
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
        if let nervix_models::DomainStatus::Stopped = domain_state.status {
            return Err(format!(
                "domain '{}' is stopped; ingestor '{}' cannot accept events",
                domain.as_str(),
                ingestor.as_str()
            ));
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

    pub(in crate::runtime) fn current_paced_domain_time(
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
            .or_insert_with(|| Arc::new(ArcSwapOption::from(None)))
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
        branched: Option<(SharedActiveGraph, BranchInstanceTemplate)>,
    ) -> Option<Arc<BranchedIngestorRuntime>> {
        branched.map(|(graph, template)| {
            BranchedIngestorRuntime::new(
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
        branched: HashMap<Identifier, (SharedActiveGraph, BranchInstanceTemplate)>,
    ) -> BranchedIngestorRuntimes {
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
        BranchedIngestorRuntimes { runtimes, senders }
    }

    pub async fn apply_cluster_schedule(
        &self,
        local_node_id: &str,
        schedule: &ClusterSchedule,
    ) -> Result<(), RuntimeError> {
        let _lock = self.schedule_apply_lock.lock().await;
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

        for domain in existing_domains.difference(&scheduled_domains) {
            match self
                .rebuild_domain_from_schedule(local_node_id, domain, None)
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
            let desired_passive_only = self
                .domains
                .get(&domain.domain)
                .is_some_and(|state| matches!(state.status, nervix_models::DomainStatus::Stopped));
            if existing_schedules.get(&domain.domain) != Some(domain)
                || existing_passive_only.get(&domain.domain) != Some(&desired_passive_only)
            {
                if existing_passive_only.get(&domain.domain) == Some(&desired_passive_only)
                    && let Some(existing_schedule) = existing_schedules.get(&domain.domain)
                    && let Some(updates) =
                        Self::relay_capacity_updates_for_schedule_change(existing_schedule, domain)
                {
                    self.apply_relay_capacity_schedule_update(
                        &domain.domain,
                        domain.clone(),
                        &updates,
                    );
                    continue;
                }
                match self
                    .rebuild_domain_from_schedule(
                        local_node_id,
                        &domain.domain,
                        Some(domain.clone()),
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

        Ok(())
    }

    fn relay_capacity_updates_for_schedule_change(
        existing: &DomainSchedule,
        desired: &DomainSchedule,
    ) -> Option<Vec<(Identifier, NonZeroUsize)>> {
        if existing.domain != desired.domain {
            return None;
        }

        let mut normalized = existing.clone();
        let mut updates = Vec::new();
        for desired_node in &desired.nodes {
            if desired_node.kind != ModelKind::Relay {
                continue;
            }
            let existing_node = normalized.nodes.iter_mut().find(|node| {
                node.kind == ModelKind::Relay && node.identifier == desired_node.identifier
            })?;
            let Model::Relay(existing_relay) = existing_node.config.as_mut() else {
                return None;
            };
            let Model::Relay(desired_relay) = desired_node.config.as_ref() else {
                return None;
            };
            if existing_relay.buffer != desired_relay.buffer {
                let capacity = NonZeroUsize::new(desired_relay.buffer)?;
                existing_relay.buffer = desired_relay.buffer;
                updates.push((desired_node.identifier.clone(), capacity));
            }
        }

        (normalized == *desired).then_some(updates)
    }

    fn apply_relay_capacity_schedule_update(
        &self,
        domain: &Domain,
        schedule: DomainSchedule,
        updates: &[(Identifier, NonZeroUsize)],
    ) {
        for (relay, capacity) in updates {
            self.set_relay_capacity(domain, relay, *capacity);
        }
        if let Some(mut execution) = self.executions.get_mut(domain) {
            execution.schedule = schedule;
        }
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
    ) -> Option<Arc<CreateSignalingProtocol>> {
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
    ) -> Option<Arc<CreateSignalingProtocol>> {
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
    ) -> Result<(), RuntimeError> {
        self.stop_domain_ingestors(domain).await;

        if let Some((_, existing)) = self.executions.remove(domain) {
            self.stop_domain_execution(domain, existing).await;
        }

        let Some(schedule) = schedule else {
            self.clear_domain_graph_handle(domain).await;
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        };
        if self
            .domains
            .get(domain)
            .is_some_and(|state| matches!(state.status, nervix_models::DomainStatus::Stopped))
        {
            self.clear_expiring_stream_states_for_domain(domain);
            let execution = self
                .build_passive_execution_from_schedule(domain, &schedule)
                .await?;
            self.executions.insert(domain.clone(), execution);
            self.clear_domain_graph_handle(domain).await;
            return Ok(());
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
        let mut ingestor_specs = Vec::new();
        let mut tasks = Vec::new();
        let remote_dispatcher = self.remote_dispatcher.read().clone();
        let model_index = schedule
            .nodes
            .iter()
            .map(|node| ((node.kind, node.identifier.clone()), (*node.config).clone()))
            .collect::<HashMap<_, _>>();
        let all_branched_specs = branched_ingestor_specs_from_scheduled_nodes(&schedule.nodes);
        let branch_relays = branch_relays_from_branched_specs(&all_branched_specs);
        let handled_processors = branched_processor_ids(&all_branched_specs);
        let branched_specs = all_branched_specs
            .into_iter()
            .filter(|spec| {
                schedule
                    .nodes
                    .iter()
                    .find(|node| node.kind == spec.kind && node.identifier == spec.identifier)
                    .is_some_and(|node| node.executes_on(local_node_id))
            })
            .collect::<Vec<_>>();

        for node in &schedule.nodes {
            match node.config.as_ref() {
                Model::Schema(schema) => {
                    schemas.insert(node.identifier.clone(), Arc::new(compile_schema(schema)));
                }
                Model::WireSchema(wire_schema) => {
                    wire_schemas.insert(node.identifier.clone(), wire_schema.clone());
                }
                Model::ClientKafka(_)
                | Model::ClientPulsar(_)
                | Model::ClientHttp(_)
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
                    signaling_protocols.insert(node.identifier.clone(), Arc::new(protocol.clone()));
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
                        wire_schemas.get(wire_schema).ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing compiled wire schema '{}'",
                                    wire_schema.as_str()
                                ),
                            }
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
                Model::Deduplicator(deduplicator) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "deduplicator '{}' is not attached to a branch root",
                            deduplicator.name.as_str()
                        ),
                    });
                }
                Model::Inferencer(processor) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "inferencer '{}' is not attached to a branch root",
                            processor.name.as_str()
                        ),
                    });
                }
                Model::Materializer(_) => {}
                Model::Generator(generator) if node.executes_on(local_node_id) => {
                    let Some(output_schema) = relay_schemas.get(&generator.into_relay).cloned()
                    else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing generator output relay schema '{}'",
                                generator.into_relay.as_str()
                            ),
                        });
                    };
                    let output_branching = relay_branchings
                        .get(&generator.into_relay)
                        .cloned()
                        .unwrap_or_default();
                    let source_streams = generator_source_streams(
                        &generator.name,
                        &generator.set,
                        &generator.into_relay,
                    )
                    .map_err(|reason| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason,
                    })?;
                    if source_streams.is_empty() {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "generator '{}' must reference at least one materialized relay",
                                generator.name.as_str()
                            ),
                        });
                    }
                    let mut source_schemas = Vec::with_capacity(source_streams.len());
                    for source_stream in &source_streams {
                        let Some(source_schema) = relay_schemas.get(source_stream).cloned() else {
                            return Err(RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing generator source relay schema '{}'",
                                    source_stream.as_str()
                                ),
                            });
                        };
                        source_schemas.push((source_stream.clone(), source_schema.arrow_schema()));
                    }
                    let program = compile_generator_set_program(
                        domain,
                        generator,
                        output_schema.arrow_schema(),
                        output_schema.vm_sensitivity(),
                        &source_schemas,
                    )?;
                    generator_specs.push((
                        generator.clone(),
                        program,
                        source_streams,
                        output_branching,
                        output_schema,
                    ));
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
                Model::Junction(junction) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "junction '{}' is not attached to a branch root",
                            junction.name.as_str()
                        ),
                    });
                }
                Model::Reorderer(reorderer) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "reorderer '{}' is not attached to a branch root",
                            reorderer.name.as_str()
                        ),
                    });
                }
                Model::Emitter(emitter) => {
                    let Some(relay) = relay_builders.get_mut(&emitter.from_relay) else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing emitter input relay '{}'",
                                emitter.from_relay.as_str()
                            ),
                        });
                    };
                    if node.executes_on(local_node_id) {
                        let receiver = relay.runtime_consumer_fan_in_for_mode(emitter.mode);
                        emitter_specs.push((emitter.clone(), receiver));
                    } else if let Some(assigned_node) = node.execution_node() {
                        push_remote_runtime_consumer(
                            &mut relay.remote_runtime_consumers,
                            assigned_node,
                            &emitter.from_relay,
                            emitter.mode,
                        );
                    }
                }
                Model::WindowProcessor(processor) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "window processor '{}' is not attached to a branch root",
                            processor.name.as_str()
                        ),
                    });
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
                        let placement = RuntimeStatePlacement {
                            domain: domain.clone(),
                            state: RuntimeStateKind::KafkaOffset,
                            kind: node.kind,
                            identifier: node.identifier.clone(),
                            branch_key: None,
                        };
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

        let relay_registries = relay_builders
            .iter()
            .map(|(identifier, relay)| (identifier.clone(), relay.registry.clone()))
            .collect::<HashMap<_, _>>();
        for relay in relay_registries.keys() {
            let placement = RuntimeStatePlacement {
                domain: domain.clone(),
                state: RuntimeStateKind::BranchAggregated,
                kind: ModelKind::Relay,
                identifier: relay.clone(),
                branch_key: None,
            };
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
            let placement = RuntimeStatePlacement {
                domain: domain.clone(),
                state: RuntimeStateKind::BranchAggregated,
                kind: node.kind,
                identifier: node.identifier.clone(),
                branch_key: None,
            };
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
                    Arc::new(RelayBoundaryServices {
                        fanout: relay.fanout,
                        attached_runtime_consumer_count: relay.attached_runtime_consumer_count,
                        detached_runtime_consumer_count: relay.detached_runtime_consumer_count,
                        remote_runtime_consumers: relay.remote_runtime_consumers.into(),
                        remote_dispatcher: remote_dispatcher.clone(),
                    }),
                )
            })
            .collect::<HashMap<_, _>>();

        let mut branched_entrypoints = HashMap::new();
        let mut branched_entrypoint_senders = HashMap::new();
        for spec in &branched_specs {
            if spec.kind != ModelKind::Reingestor {
                continue;
            }
            let mut template = materialize_branch_instance_template(
                spec,
                &model_index,
                &relay_schemas,
                &relay_registries,
                &relay_services,
            )
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            })?;
            template
                .prepare_wasm_processors(self)
                .await
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

        let lookup_runtimes = lookup_specs.iter().cloned().collect::<HashMap<_, _>>();
        let execution_build_deps = ExecutionBuildDeps {
            domain,
            relay_schemas: &relay_schemas,
            relay_branchings: &relay_branchings,
            relay_branching_schemas: &relay_branching_schemas,
            materialized_relay_specs: &materialized_stream_specs,
            materialized_relay_owner_nodes: &materialized_stream_owner_nodes,
            lookups: &lookup_runtimes,
        };

        for (generator, program, source_streams, output_branching, output_schema) in generator_specs
        {
            let Some(output_registry) = relay_registries.get(&generator.into_relay).cloned() else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing generator output relay '{}'",
                        generator.into_relay.as_str()
                    ),
                });
            };
            let Some(output_services) = relay_services.get(&generator.into_relay).cloned() else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing generator output relay services '{}'",
                        generator.into_relay.as_str()
                    ),
                });
            };
            tasks.push(self.spawn_generator_task(
                domain,
                &shutdown_tx,
                GeneratorTaskSpec {
                    generator,
                    program,
                    source_relays: source_streams,
                    output_branching,
                    output_schema,
                    output_registry,
                    output_services,
                },
            )?);
        }

        for (emitter, receiver) in emitter_specs {
            tasks.push(self.spawn_emitter_task(
                EmitterTaskBuildDeps {
                    domain,
                    shutdown_tx: &shutdown_tx,
                    codecs: &codecs,
                    clients: &transports,
                    deps: self.emitter_task_deps(execution_build_deps, &emitter)?,
                },
                emitter,
                receiver,
            )?);
        }

        for (reingestor, from_relay, receiver) in reingestor_specs {
            tasks.push(self.spawn_reingestor_task(
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
                shutdown: shutdown_tx,
                graph: domain_graph.clone(),
                relay_registries,
                relay_schemas,
                relay_services,
                lookups: lookup_runtimes,
                relay_branchings,
                relay_branching_schemas,
                materialized_stream_specs,
                materialized_stream_owner_nodes,
                branched_ingestors: Self::branched_specs_by_identifier(&branched_specs),
                branched_entrypoints,
                codecs,
                signaling_protocols,
                endpoint_routes,
                tasks,
            },
        );

        if self.ingestors_paused_for_memory_pressure() {
            info!(
                domain = domain.as_str(),
                ingestors = ingestor_specs.len(),
                "deferring scheduled ingestor starts because memory pressure is active"
            );
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

    pub async fn dispatch_websocket_payload(
        &self,
        host: &str,
        path: &str,
        payload: &[u8],
        headers: IngestHeaders,
    ) -> usize {
        self.dispatch_endpoint_payload(host, path, payload, headers, "websocket")
            .await
    }

    pub async fn dispatch_http_payload(
        &self,
        host: &str,
        path: &str,
        payload: &[u8],
        headers: IngestHeaders,
    ) -> usize {
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
    ) -> usize {
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

        for binding in &bindings {
            match decode_ingested_payload(binding.codec.clone(), payload).await {
                Ok(record) => {
                    let mut output_routes = binding.output_routes.clone();
                    if let Err(error) = self
                        .dispatch_ingested_record(IngestDispatch {
                            domain: &binding.domain,
                            ingestor: &binding.ingestor,
                            timestamp_source: binding.timestamp_source.as_ref(),
                            branching: &binding.branching,
                            branch_value_mappings: Some(&binding.branch_value_mappings),
                            output_routes: &mut output_routes,
                            filter_where: binding.filter_where.as_ref(),
                            branched_senders: &binding.branched_senders,
                            record,
                            filter_map_metadata: Some(IngestFilterMapMetadata::from_headers(
                                headers.clone(),
                            )),
                            ingested_at: current_timestamp(),
                            acks: AckSet::empty(),
                        })
                        .await
                    {
                        let _ = self.events.send(RuntimeEvent::Error(format!(
                            "failed to dispatch {protocol} message for ingestor '{}' in domain \
                             '{}': {}",
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

        bindings.len()
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
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeStateKind::BranchAggregated,
            kind,
            identifier: identifier.clone(),
            branch_key: None,
        };
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
        if !self.executions.contains_key(domain) {
            if let Some(error) = self.domain_instantiation_errors.get(domain) {
                return Err(error.value().clone());
            }
            return Ok(IngestorDescribe {
                running: false,
                ready: false,
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

    pub(in crate::runtime) fn local_materialized_stream_state_for_branch(
        &self,
        domain: &Domain,
        relay: &Identifier,
        branch_key: &Option<BranchKey>,
    ) -> Result<Vec<(String, RuntimeRecord)>, String> {
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeStateKind::MaterializedRelay,
            kind: ModelKind::Materializer,
            identifier: relay.clone(),
            branch_key: branch_key.clone(),
        };
        if let Some(state) = self.replicated_materialized_stream_states.get(&placement) {
            return Ok(self
                .visible_materialized_stream_remote_entries(&placement, &state)
                .into_iter()
                .map(|(key, record)| {
                    (
                        branch_key_display(&key).to_string(),
                        RuntimeRecord::from_remote(record),
                    )
                })
                .collect());
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
        Ok(Vec::new())
    }

    pub(in crate::runtime) fn visible_materialized_stream_remote_entries(
        &self,
        placement: &RuntimeStatePlacement,
        state: &ReplicatedMaterializedRelayState,
    ) -> Vec<(Option<BranchKey>, nervix_models::RemoteRuntimeRecord)> {
        let runtime_key = RuntimeKey::new(placement.domain.clone(), placement.identifier.clone());
        let mut entries =
            if let Some(expiring_state) = self.expiring_stream_states.get(&runtime_key) {
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

    pub(in crate::runtime) async fn remote_materialized_stream_state_for_branch(
        &self,
        target_node_id: &str,
        domain: &Domain,
        relay: &Identifier,
        branch_key: &Option<BranchKey>,
    ) -> Result<Vec<(String, RuntimeRecord)>, String> {
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeStateKind::MaterializedRelay,
            kind: ModelKind::Materializer,
            identifier: relay.clone(),
            branch_key: branch_key.clone(),
        };
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
                    format!("{}.{}", relay_interest.relay.as_str(), field),
                    value.clone(),
                );
            }
        }

        Ok(values)
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
        let mut relay_capacity_updates = Vec::new();
        for change in changes.changes {
            match change {
                RuntimeChange::StopIngestor { ingestor } => stops.push(ingestor),
                RuntimeChange::StartIngestor {
                    source_model,
                    ingestor,
                } => starts.push((*source_model, *ingestor)),
                RuntimeChange::SetRelayCapacity { relay, capacity } => {
                    relay_capacity_updates.push((relay, capacity));
                }
            }
        }

        if stops.is_empty() && starts.is_empty() && !relay_capacity_updates.is_empty() {
            for (relay, capacity) in relay_capacity_updates {
                self.set_relay_capacity(&domain, &relay, capacity);
            }
            return Ok(());
        }

        for ingestor in stops {
            self.stop_ingestor(&domain, &ingestor).await?;
        }

        self.rebuild_domain_execution(&domain, graph).await?;

        if starts_are_scheduled_by_graph {
            return Ok(());
        }

        if self.ingestors_paused_for_memory_pressure() {
            info!(
                domain = domain.as_str(),
                ingestors = starts.len(),
                "deferring ingestor starts because memory pressure is active"
            );
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
        if self
            .domains
            .get(domain)
            .is_none_or(|state| matches!(state.status, nervix_models::DomainStatus::Stopped))
        {
            self.clear_domain_graph_handle(domain).await;
            self.clear_expiring_stream_states_for_domain(domain);
            return Ok(());
        }

        let domain_graph = self.domain_graph_handle(domain).await;
        domain_graph.store(Some(Arc::new(graph.clone())));
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
        let mut tasks = Vec::new();
        let branched_specs = branched_ingestor_specs_from_active_graph(&graph);
        let branch_relays = branch_relays_from_branched_specs(&branched_specs);
        let model_index = graph
            .nodes()
            .into_iter()
            .map(|node| ((node.kind, node.identifier.clone()), (*node.config).clone()))
            .collect::<HashMap<_, _>>();
        let handled_processors = branched_processor_ids(&branched_specs);

        for node in graph.nodes() {
            match node.config.as_ref() {
                Model::Schema(schema) => {
                    schemas.insert(node.identifier.clone(), Arc::new(compile_schema(schema)));
                }
                Model::WireSchema(wire_schema) => {
                    wire_schemas.insert(node.identifier.clone(), wire_schema.clone());
                }
                Model::ClientKafka(_)
                | Model::ClientPulsar(_)
                | Model::ClientHttp(_)
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
                    signaling_protocols.insert(node.identifier.clone(), Arc::new(protocol.clone()));
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
                        wire_schemas.get(wire_schema).ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing compiled wire schema '{}'",
                                    wire_schema.as_str()
                                ),
                            }
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
                Model::Deduplicator(deduplicator) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "deduplicator '{}' is not attached to a branch root",
                            deduplicator.name.as_str()
                        ),
                    });
                }
                Model::Inferencer(processor) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "inferencer '{}' is not attached to a branch root",
                            processor.name.as_str()
                        ),
                    });
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
                Model::Generator(generator) => {
                    let Some(output_schema) = relay_schemas.get(&generator.into_relay).cloned()
                    else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing generator output relay schema '{}'",
                                generator.into_relay.as_str()
                            ),
                        });
                    };
                    let output_branching = relay_branchings
                        .get(&generator.into_relay)
                        .cloned()
                        .unwrap_or_default();
                    let source_streams = generator_source_streams(
                        &generator.name,
                        &generator.set,
                        &generator.into_relay,
                    )
                    .map_err(|reason| RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason,
                    })?;
                    if source_streams.is_empty() {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "generator '{}' must reference at least one materialized relay",
                                generator.name.as_str()
                            ),
                        });
                    }
                    let mut source_schemas = Vec::with_capacity(source_streams.len());
                    for source_stream in &source_streams {
                        let Some(source_schema) = relay_schemas.get(source_stream).cloned() else {
                            return Err(RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing generator source relay schema '{}'",
                                    source_stream.as_str()
                                ),
                            });
                        };
                        source_schemas.push((source_stream.clone(), source_schema.arrow_schema()));
                    }
                    let program = compile_generator_set_program(
                        domain,
                        generator,
                        output_schema.arrow_schema(),
                        output_schema.vm_sensitivity(),
                        &source_schemas,
                    )?;
                    generator_specs.push((
                        generator.clone(),
                        program,
                        source_streams,
                        output_branching,
                        output_schema,
                    ));
                }
                Model::Junction(junction) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "junction '{}' is not attached to a branch root",
                            junction.name.as_str()
                        ),
                    });
                }
                Model::Reorderer(reorderer) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "reorderer '{}' is not attached to a branch root",
                            reorderer.name.as_str()
                        ),
                    });
                }
                Model::Emitter(emitter) => {
                    let Some(relay) = relay_builders.get_mut(&emitter.from_relay) else {
                        return Err(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "missing emitter input relay '{}'",
                                emitter.from_relay.as_str()
                            ),
                        });
                    };
                    let receiver = relay.runtime_consumer_fan_in_for_mode(emitter.mode);
                    emitter_specs.push((emitter.clone(), receiver));
                }
                Model::WindowProcessor(processor) => {
                    if handled_processors.contains(&node.identifier) {
                        continue;
                    }
                    return Err(RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "window processor '{}' is not attached to a branch root",
                            processor.name.as_str()
                        ),
                    });
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

        let relay_registries = relay_builders
            .iter()
            .map(|(identifier, relay)| (identifier.clone(), relay.registry.clone()))
            .collect::<HashMap<_, _>>();
        let relay_services = relay_builders
            .into_iter()
            .map(|(identifier, relay)| {
                (
                    identifier,
                    Arc::new(RelayBoundaryServices {
                        fanout: relay.fanout,
                        attached_runtime_consumer_count: relay.attached_runtime_consumer_count,
                        detached_runtime_consumer_count: relay.detached_runtime_consumer_count,
                        remote_runtime_consumers: relay.remote_runtime_consumers.into(),
                        remote_dispatcher: None,
                    }),
                )
            })
            .collect::<HashMap<_, _>>();

        let mut branched_entrypoints = HashMap::new();
        let mut branched_entrypoint_senders = HashMap::new();
        for spec in &branched_specs {
            if spec.kind != ModelKind::Reingestor {
                continue;
            }
            let mut template = materialize_branch_instance_template(
                spec,
                &model_index,
                &relay_schemas,
                &relay_registries,
                &relay_services,
            )
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            })?;
            template
                .prepare_wasm_processors(self)
                .await
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

        let lookup_runtimes = lookup_specs.iter().cloned().collect::<HashMap<_, _>>();
        let execution_build_deps = ExecutionBuildDeps {
            domain,
            relay_schemas: &relay_schemas,
            relay_branchings: &relay_branchings,
            relay_branching_schemas: &relay_branching_schemas,
            materialized_relay_specs: &materialized_stream_specs,
            materialized_relay_owner_nodes: &materialized_stream_owner_nodes,
            lookups: &lookup_runtimes,
        };

        for (generator, program, source_streams, output_branching, output_schema) in generator_specs
        {
            let Some(output_registry) = relay_registries.get(&generator.into_relay).cloned() else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing generator output relay '{}'",
                        generator.into_relay.as_str()
                    ),
                });
            };
            let Some(output_services) = relay_services.get(&generator.into_relay).cloned() else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing generator output relay services '{}'",
                        generator.into_relay.as_str()
                    ),
                });
            };
            tasks.push(self.spawn_generator_task(
                domain,
                &shutdown_tx,
                GeneratorTaskSpec {
                    generator,
                    program,
                    source_relays: source_streams,
                    output_branching,
                    output_schema,
                    output_registry,
                    output_services,
                },
            )?);
        }

        for (emitter, receiver) in emitter_specs {
            tasks.push(self.spawn_emitter_task(
                EmitterTaskBuildDeps {
                    domain,
                    shutdown_tx: &shutdown_tx,
                    codecs: &codecs,
                    clients: &transports,
                    deps: self.emitter_task_deps(execution_build_deps, &emitter)?,
                },
                emitter,
                receiver,
            )?);
        }

        for (reingestor, from_relay, receiver) in reingestor_specs {
            tasks.push(self.spawn_reingestor_task(
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
                },
                passive_only: false,
                shutdown: shutdown_tx,
                graph: domain_graph.clone(),
                relay_registries,
                relay_schemas,
                relay_services,
                lookups: lookup_runtimes,
                relay_branchings,
                relay_branching_schemas,
                materialized_stream_specs,
                materialized_stream_owner_nodes,
                branched_ingestors: Self::branched_specs_by_identifier(&branched_specs),
                branched_entrypoints,
                codecs,
                signaling_protocols,
                endpoint_routes,
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
                Model::Relay(relay) => {
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
                Model::Schema(schema) => {
                    schemas.insert(node.identifier.clone(), Arc::new(compile_schema(schema)));
                }
                Model::WireSchema(wire_schema) => {
                    wire_schemas.insert(node.identifier.clone(), wire_schema.clone());
                }
                _ => {}
            }
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
                        wire_schemas.get(wire_schema).ok_or_else(|| {
                            RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "missing compiled wire schema '{}'",
                                    wire_schema.as_str()
                                ),
                            }
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
                    Arc::new(RelayBoundaryServices {
                        fanout: relay.fanout,
                        attached_runtime_consumer_count: relay.attached_runtime_consumer_count,
                        detached_runtime_consumer_count: relay.detached_runtime_consumer_count,
                        remote_runtime_consumers: relay.remote_runtime_consumers.into(),
                        remote_dispatcher: None,
                    }),
                )
            })
            .collect::<HashMap<_, _>>();
        Ok(DomainExecution {
            schedule: schedule.clone(),
            passive_only: true,
            shutdown,
            graph,
            relay_registries,
            relay_schemas,
            relay_services,
            lookups,
            relay_branchings,
            relay_branching_schemas,
            materialized_stream_specs: HashMap::default(),
            materialized_stream_owner_nodes: HashMap::default(),
            branched_ingestors: HashMap::default(),
            branched_entrypoints: HashMap::default(),
            codecs,
            signaling_protocols: HashMap::default(),
            endpoint_routes: HashMap::default(),
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
            program,
            source_relays: source_streams,
            output_branching,
            output_schema,
            output_registry,
            output_services,
        } = spec;
        let interval = Self::parse_runtime_node_duration_setting(
            domain,
            "generator",
            &generator.name,
            "each",
            &generator.each,
        )?;
        let flush_each = Self::parse_runtime_node_flush_policy(
            domain,
            "generator",
            &generator.name,
            &generator.flush_each,
            generator.max_batch_size.as_deref(),
        )?;
        let task_domain = domain.clone();
        let task_generator = generator.name.clone();
        let task_output_relay = generator.into_relay.clone();
        let task_source_streams = source_streams;
        let task_output_branching = output_branching;
        let mut shutdown_rx = shutdown_tx.subscribe();
        let runtime = self.clone();
        let task_events = self.events.clone();

        Ok(tokio::spawn(async move {
            let mut next_state_refresh = None::<Timestamp>;
            let mut branch_states =
                HashMap::<Option<BranchKey>, GeneratorBranchTaskState>::default();

            loop {
                tokio::task::consume_budget().await;
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
                            state.next_flush = None;
                        }
                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = sleep(Duration::from_millis(50)) => {}
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
                let mut generated_branches = HashSet::default();

                if should_refresh_state {
                    advance_scheduled_timestamp(&mut next_state_refresh, interval, execution_now);
                    did_scheduled_work = true;

                    let local_node_id = runtime.local_node_id.read().clone();
                    let mut source_state_by_branch = HashMap::<
                        Option<BranchKey>,
                        (
                            HashMap<Identifier, HashMap<String, RuntimeRecord>>,
                            BTreeSet<String>,
                        ),
                    >::default();
                    let mut state_load_failed = false;
                    let remote_dispatcher = { runtime.remote_dispatcher.read().clone() };
                    let remote_nodes = if let Some(dispatcher) = remote_dispatcher {
                        dispatcher.cluster.live_node_ids().await
                    } else {
                        Vec::new()
                    };

                    for source_stream in &task_source_streams {
                        tokio::task::consume_budget().await;
                        let mut state = match runtime
                            .local_materialized_stream_state(&task_domain, source_stream)
                        {
                            Ok(state) => state,
                            Err(error) => {
                                state_load_failed = true;
                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                    "failed to read materialized state for generator '{}' from \
                                     relay '{}' in domain '{}': {}",
                                    task_generator.as_str(),
                                    source_stream.as_str(),
                                    task_domain.as_str(),
                                    error
                                )));
                                break;
                            }
                        };
                        for remote_node in &remote_nodes {
                            tokio::task::consume_budget().await;
                            if local_node_id.as_deref() == Some(remote_node.as_str()) {
                                continue;
                            }
                            match runtime
                                .remote_materialized_stream_state(
                                    remote_node,
                                    &task_domain,
                                    source_stream,
                                )
                                .await
                            {
                                Ok(remote_state) => state.extend(remote_state),
                                Err(error) => {
                                    state_load_failed = true;
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "failed to read materialized state for generator '{}' \
                                         from relay '{}' on node '{}' in domain '{}': {}",
                                        task_generator.as_str(),
                                        source_stream.as_str(),
                                        remote_node,
                                        task_domain.as_str(),
                                        error
                                    )));
                                    break;
                                }
                            }
                        }
                        if state_load_failed {
                            break;
                        }
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
                        for (key, record) in latest_state {
                            let branch_key = if task_output_branching.is_empty() {
                                None
                            } else {
                                match BranchKey::from_record(&record, task_output_branching.iter())
                                {
                                    Ok(Some(key)) => Some(key),
                                    Ok(None) => {
                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                            "generator '{}' source relay '{}' record is missing \
                                             concrete branch fields",
                                            task_generator.as_str(),
                                            source_stream.as_str(),
                                        )));
                                        continue;
                                    }
                                    Err(error) => {
                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                            "generator '{}' source relay '{}' has invalid \
                                             concrete branch fields: {}",
                                            task_generator.as_str(),
                                            source_stream.as_str(),
                                            error,
                                        )));
                                        continue;
                                    }
                                }
                            };
                            let (state_by_stream, source_keys) = source_state_by_branch
                                .entry(branch_key)
                                .or_insert_with(|| (HashMap::default(), BTreeSet::new()));
                            source_keys.insert(key.clone());
                            state_by_stream
                                .entry(source_stream.clone())
                                .or_default()
                                .insert(key, record);
                        }
                    }

                    if !state_load_failed {
                        let active_branch_keys = source_state_by_branch
                            .keys()
                            .cloned()
                            .collect::<HashSet<_>>();
                        for (branch_key, branch_state) in &mut branch_states {
                            if !active_branch_keys.contains(branch_key) {
                                branch_state.next_generation = None;
                            }
                        }
                        for (branch_key, (source_state_by_stream, source_keys)) in
                            source_state_by_branch
                        {
                            tokio::task::consume_budget().await;
                            let branch_state = branch_states.entry(branch_key.clone()).or_default();
                            if branch_state.next_generation.is_none() {
                                branch_state.next_generation = Some(execution_now);
                            }
                            if branch_state.next_flush.is_none()
                                && let RuntimeFlushPolicy::Each {
                                    interval: flush_each,
                                    ..
                                } = flush_each
                            {
                                branch_state.next_flush = Some(checked_add_duration_to_timestamp(
                                    execution_now,
                                    flush_each,
                                ));
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
                            generated_branches.insert(branch_key.clone());

                            for key in source_keys {
                                tokio::task::consume_budget().await;
                                let mut values = HashMap::default();
                                for source_stream in &task_source_streams {
                                    let Some(record) = source_state_by_stream
                                        .get(source_stream)
                                        .and_then(|entries| entries.get(&key))
                                    else {
                                        continue;
                                    };
                                    for field in record.to_remote().fields {
                                        values.insert(
                                            format!("{}.{}", source_stream.as_str(), field.name),
                                            RuntimeValue::from_remote(field.value),
                                        );
                                    }
                                }

                                let input =
                                    match generator_context_batch(&program.input_schema, &values) {
                                        Ok(input) => input,
                                        Err(error) => {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "failed to prepare generator input for '{}' in \
                                                 domain '{}' branch '{}': {}",
                                                task_generator.as_str(),
                                                task_domain.as_str(),
                                                branch_key_display(&branch_key),
                                                error
                                            )));
                                            continue;
                                        }
                                    };
                                let record = match execute_generator_program_on_context(
                                    &program,
                                    &input,
                                    execution_now,
                                )
                                .await
                                {
                                    Ok(Some(record)) => record,
                                    Ok(None) => continue,
                                    Err(error) => {
                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                            "failed to execute generator '{}' in domain '{}' \
                                             branch '{}': {}",
                                            task_generator.as_str(),
                                            task_domain.as_str(),
                                            branch_key_display(&branch_key),
                                            error
                                        )));
                                        continue;
                                    }
                                };
                                let output_key = if task_output_branching.is_empty() {
                                    None
                                } else {
                                    match BranchKey::from_record(
                                        &record,
                                        task_output_branching.iter(),
                                    ) {
                                        Ok(Some(key)) => Some(key),
                                        Ok(None) => {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "generator '{}' produced record missing \
                                                 destination branching for relay '{}'",
                                                task_generator.as_str(),
                                                task_output_relay.as_str(),
                                            )));
                                            continue;
                                        }
                                        Err(error) => {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "generator '{}' produced invalid destination \
                                                 branching for relay '{}': {}",
                                                task_generator.as_str(),
                                                task_output_relay.as_str(),
                                                error
                                            )));
                                            continue;
                                        }
                                    }
                                };
                                if output_key != branch_key {
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "generator '{}' attempted to cross concrete branch '{}' \
                                         into '{}'",
                                        task_generator.as_str(),
                                        branch_key_display(&branch_key),
                                        branch_key_display(&output_key),
                                    )));
                                    continue;
                                }
                                branch_state.pending.push(RelayMessage {
                                    key: branch_key.clone(),
                                    record,
                                    acks: AckSet::empty(),
                                });
                            }
                        }
                        branch_states.retain(|branch_key, branch_state| {
                            active_branch_keys.contains(branch_key)
                                || !branch_state.pending.is_empty()
                        });
                    }
                }

                let mut flushed_any_branch = false;
                for (branch_key, branch_state) in &mut branch_states {
                    tokio::task::consume_budget().await;
                    let should_flush = match flush_each {
                        RuntimeFlushPolicy::Immediate => generated_branches.contains(branch_key),
                        RuntimeFlushPolicy::Each { .. } => branch_state
                            .next_flush
                            .is_some_and(|next| execution_now >= next),
                    };
                    if !should_flush {
                        continue;
                    }
                    if let RuntimeFlushPolicy::Each {
                        interval: flush_each,
                        ..
                    } = flush_each
                    {
                        advance_scheduled_timestamp(
                            &mut branch_state.next_flush,
                            flush_each,
                            execution_now,
                        );
                    }
                    if !branch_state.pending.is_empty() {
                        let mut pending_group = vec![(
                            branch_key.clone(),
                            std::mem::take(&mut branch_state.pending),
                        )];
                        flush_generator_groups(
                            GeneratorFlushContext {
                                runtime: &runtime,
                                domain: &task_domain,
                                generator: &task_generator,
                                output_relay: &task_output_relay,
                                output_schema: &output_schema,
                                output_registry: &output_registry,
                                output_services: &output_services,
                                task_events: &task_events,
                            },
                            &mut pending_group,
                        )
                        .await;
                    }
                    flushed_any_branch = true;
                }
                did_scheduled_work |= flushed_any_branch;

                if did_scheduled_work {
                    continue;
                }

                let next_deadline = next_state_refresh
                    .into_iter()
                    .chain(
                        branch_states
                            .values()
                            .filter_map(|state| state.next_generation),
                    )
                    .chain(branch_states.values().filter_map(|state| state.next_flush))
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
        if output.compiled_program.is_none() && output.filter_map.is_some() {
            let (
                input_schema,
                output_schema,
                materialized_stream_specs,
                available_lookups,
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
                domain,
                reingestor,
                std::slice::from_ref(from_relay),
                &output.relay,
                output.filter_map.as_deref(),
                batch.arrow_schema(),
                input_schema.vm_sensitivity(),
                output_schema.arrow_schema(),
                output_schema.vm_sensitivity(),
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
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
            let messages = batch
                .records
                .iter()
                .enumerate()
                .map(|(row, record)| PendingProcessorOutputMessage {
                    row,
                    output_index,
                    key: batch.keys[row].clone(),
                    record: record.clone(),
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
        let input_records = prepare_filter_map_input_records(
            "reingestor",
            reingestor,
            program,
            batch.records.clone(),
            execution_now,
            &side_inputs,
            &batch.keys,
            &batch.acks,
        )
        .await?;
        let executed = execute_filter_map_program(
            "reingestor",
            reingestor,
            program,
            &input_records,
            execution_now,
            batch.acks.clone(),
        )
        .await?;
        let output_batch = vm_typed_batch_to_runtime_batch(&executed.batch).map_err(|error| {
            PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "reingestor '{}' failed to materialize FILTER-MAP output batch: {}",
                    reingestor.as_str(),
                    error
                ),
            }
        })?;
        let mut success_output_rows = Vec::new();
        let mut success_input_rows = Vec::new();
        let mut errors = Vec::new();
        for (output_row, &input_row) in executed.selected_rows.iter().enumerate() {
            if let Some(side_error) = executed.batch.errors()[output_row].first() {
                errors.push(PendingProcessorOutputMessageError {
                    row: input_row,
                    key: batch.keys[input_row].clone(),
                    record: batch.records[input_row].clone(),
                    reason: format!(
                        "reingestor '{}' FILTER-MAP side error {}: {} at {}",
                        reingestor.as_str(),
                        side_error.code.as_str(),
                        side_error.message,
                        side_error.span
                    ),
                });
                continue;
            }
            success_output_rows.push(output_row);
            success_input_rows.push(input_row);
        }
        let output_batches = if success_output_rows.is_empty() {
            Vec::new()
        } else {
            let output_batch = if success_output_rows.len() == executed.batch.row_count() {
                output_batch
            } else {
                let success_output_rows =
                    success_output_rows.iter().copied().collect::<HashSet<_>>();
                let keep = BooleanArray::from_iter(
                    (0..executed.batch.row_count())
                        .map(|row| Some(success_output_rows.contains(&row))),
                );
                output_batch
                    .filter(&keep)
                    .map_err(|error| PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: format!(
                            "reingestor '{}' failed to filter FILTER-MAP output batch: {}",
                            reingestor.as_str(),
                            error
                        ),
                    })?
            };
            let records = output_schema
                .decoded_records_from_arrow_batch(&output_batch)
                .map_err(|error| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "reingestor '{}' failed to decode FILTER-MAP output sidecar records: {}",
                        reingestor.as_str(),
                        error
                    ),
                })?
                .into_iter()
                .zip(success_input_rows.iter())
                .map(|(record, input_row)| {
                    record.into_runtime_record(batch.metadata[*input_row].clone())
                })
                .collect::<Vec<_>>();
            let metadata = success_input_rows
                .iter()
                .map(|input_row| batch.metadata[*input_row].clone())
                .collect::<Vec<_>>();
            vec![PendingProcessorOutputBatch {
                output_index,
                input_rows: success_input_rows,
                key: batch.key.clone(),
                batch: output_batch,
                records,
                metadata,
            }]
        };

        Ok((Vec::new(), output_batches, errors))
    }

    async fn dispatch_reingestor_outputs(
        &self,
        domain: &Domain,
        reingestor: &Identifier,
        from_relay: &Identifier,
        from_where: Option<&str>,
        compiled_from_where: &mut Option<CompiledProgramWithMaterializedInterest>,
        mode: AckMode,
        error_policies: &ErrorPolicies,
        output_routes: &mut RelayProcessorOutputsNode,
        branched_senders: &HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
        batch: RelayRecordBatch,
    ) {
        if batch.message_count() == 0 {
            return;
        }
        let Some(batch) = self
            .filter_reingestor_from_batch(
                domain,
                reingestor,
                from_relay,
                from_where,
                compiled_from_where,
                error_policies,
                batch,
            )
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
            pending_errors.extend(errors);
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
        for error in &pending_errors {
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

        let mut planned_errors = Vec::new();
        for error in pending_errors {
            let Some(acks) = ack_queues[error.row].pop_front() else {
                continue;
            };
            planned_errors.push(PlannedMessageError {
                message: RelayMessage {
                    key: error.key,
                    record: error.record,
                    acks,
                },
                reason: error.reason,
            });
        }
        self.handle_planned_message_errors(
            domain,
            "reingestor",
            reingestor,
            error_policies,
            planned_errors,
        )
        .await;

        for ((relay, messages), mut batches) in output_relays
            .into_iter()
            .zip(messages_by_output)
            .zip(batches_by_output)
        {
            let Some(branched_sender) = branched_senders.get(&relay) else {
                for message in messages {
                    self.handle_message_error(
                        domain,
                        "reingestor",
                        reingestor,
                        error_policies,
                        message,
                        format!(
                            "missing reingestor branched entrypoint for relay '{}'",
                            relay.as_str()
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
            };
            if !messages.is_empty() {
                let output_schema = match relay_schema_for_runtime(self, domain, &relay) {
                    Ok(schema) => schema,
                    Err(error) => {
                        for message in messages {
                            self.handle_message_error(
                                domain,
                                "reingestor",
                                reingestor,
                                error_policies,
                                message,
                                error.to_string(),
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
            let concat_acks = batches
                .iter()
                .flat_map(|batch| batch.acks.iter().cloned())
                .collect::<Vec<_>>();
            let forwarded = match RelayRecordBatch::concat(batches) {
                Ok(batch) => batch,
                Err(error) => {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        "reingestor",
                        reingestor,
                        error_policies,
                        concat_acks.iter(),
                        format!(
                            "reingestor '{}' failed to concat output batches for relay '{}': {}",
                            reingestor.as_str(),
                            relay.as_str(),
                            error
                        ),
                    );
                    continue;
                }
            };
            match branched_sender
                .send(BranchedEntrypointInput::PendingBranchingBatch(forwarded))
                .await
            {
                Ok(()) => {}
                Err(error) => {
                    let BranchedEntrypointInput::PendingBranchingBatch(batch) = error.0 else {
                        continue;
                    };
                    if mode == AckMode::Detached {
                        for ack in batch.acks {
                            ack.ack_success();
                        }
                        continue;
                    }
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        "reingestor",
                        reingestor,
                        error_policies,
                        batch.acks.iter(),
                        format!(
                            "reingestor '{}' failed to forward batch to branch entrypoint for \
                             relay '{}'",
                            reingestor.as_str(),
                            relay.as_str()
                        ),
                    );
                }
            }
        }
    }

    async fn filter_reingestor_from_batch(
        &self,
        domain: &Domain,
        reingestor: &Identifier,
        from_relay: &Identifier,
        from_where: Option<&str>,
        compiled_from_where: &mut Option<CompiledProgramWithMaterializedInterest>,
        error_policies: &ErrorPolicies,
        batch: RelayRecordBatch,
    ) -> Option<RelayRecordBatch> {
        let Some(from_where) = from_where else {
            return Some(batch);
        };

        if compiled_from_where.is_none() {
            let (
                input_schema,
                materialized_stream_specs,
                available_lookups,
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
            match compile_filter_map_program(
                domain,
                reingestor,
                std::slice::from_ref(from_relay),
                Some(from_where),
                batch.arrow_schema(),
                input_schema.vm_sensitivity(),
                input_schema.arrow_schema(),
                input_schema.vm_sensitivity(),
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
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
        let mut task_output_routes = RelayProcessorOutputsNode {
            routes: reingestor
                .output_routes
                .routes
                .iter()
                .map(|output| RelayProcessorOutputNode {
                    relay: output.relay.clone(),
                    filter_map: output.filter_map.clone(),
                    compiled_program: None,
                })
                .collect(),
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
        let task_mode = reingestor.mode;
        let task_error_policies = message_only_error_policies(&reingestor.message_error_policy);
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        Ok(tokio::spawn(async move {
            let mut input = receiver;
            let mut compiled_from_where = None;
            loop {
                tokio::task::consume_budget().await;
                match Self::recv_runtime_consumer_batch(
                    &mut input,
                    &mut shutdown_rx,
                    RuntimeFlushPolicy::Immediate,
                )
                .await
                {
                    BatchedInput::Shutdown | BatchedInput::Closed => break,
                    BatchedInput::Batch(batch) => {
                        runtime
                            .metrics
                            .observe_global_node_received(NodeBatchObservation {
                                domain: &task_domain,
                                kind: ModelKind::Reingestor,
                                node: &task_reingestor,
                                relay: &task_from_relay,
                                physical_node_id: runtime.local_node_id.read().as_deref(),
                                messages: batch.message_count(),
                                bytes: batch.estimated_bytes(),
                                domain_timestamp: batch.domain_timestamp(),
                            });
                        runtime.mark_branch_aggregated_metrics_updated(
                            &task_domain,
                            ModelKind::Reingestor,
                            &task_reingestor,
                        );
                        let delivery_latencies =
                            batch.delivery_latency_seconds(current_timestamp());
                        for seconds in delivery_latencies {
                            runtime
                                .metrics
                                .observe_global_delivery_latency_at_domain_time(
                                    NodeLatencyObservation {
                                        domain: &task_domain,
                                        kind: ModelKind::Reingestor,
                                        node: &task_reingestor,
                                        relay: &task_from_relay,
                                        physical_node_id: runtime.local_node_id.read().as_deref(),
                                        seconds,
                                        domain_timestamp: batch.domain_timestamp(),
                                    },
                                );
                            runtime.mark_branch_aggregated_metrics_updated(
                                &task_domain,
                                ModelKind::Reingestor,
                                &task_reingestor,
                            );
                        }
                        runtime
                            .dispatch_reingestor_outputs(
                                &task_domain,
                                &task_reingestor,
                                &task_from_relay,
                                task_from_where.as_deref(),
                                &mut compiled_from_where,
                                task_mode,
                                &task_error_policies,
                                &mut task_output_routes,
                                &task_branched_senders,
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
        receiver: RelayRuntimeFanIn,
    ) -> Result<JoinHandle<()>, RuntimeError> {
        emitters::EmitterTask::spawn(self, build, emitter, receiver)
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

        let mut stopped = 0;
        for key in ingestors {
            match self.stop_ingestor(&key.domain, &key.identifier).await {
                Ok(()) => {
                    stopped += 1;
                }
                Err(error) => {
                    warn!(
                        domain = key.domain.as_str(),
                        ingestor = key.identifier.as_str(),
                        error = %error,
                        "failed to pause ingestor during memory pressure"
                    );
                }
            }
        }
        stopped
    }

    pub async fn resume_one_ingestor_after_memory_pressure(&self) -> Result<bool, RuntimeError> {
        let Some(spec) = self.next_memory_paused_ingestor_start_spec() else {
            self.ingestors_paused_for_memory_pressure
                .store(false, Ordering::SeqCst);
            return Ok(false);
        };
        let ingestor = spec.ingestor.name.clone();
        self.start_scheduled_ingestor(
            &spec.domain,
            spec.source_model,
            spec.ingestor,
            spec.kafka_offset_state,
        )
        .await?;
        info!(
            domain = spec.domain.as_str(),
            ingestor = ingestor.as_str(),
            "resumed ingestor after memory pressure"
        );
        Ok(true)
    }

    pub fn ingestors_paused_for_memory_pressure(&self) -> bool {
        self.ingestors_paused_for_memory_pressure
            .load(Ordering::SeqCst)
    }

    fn next_memory_paused_ingestor_start_spec(&self) -> Option<ScheduledIngestorStartSpec> {
        let local_node_id = self.local_node_id.read().clone();
        let mut domains = self
            .executions
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        domains.sort_by(|left, right| left.as_str().cmp(right.as_str()));

        for domain in domains {
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
            IngestSource::Kinesis { client, .. } => client,
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
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeStateKind::KafkaOffset,
            kind: node.kind,
            identifier: node.identifier.clone(),
            branch_key: None,
        };
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
        for task in execution.tasks {
            Self::await_shutdown_task(task, domain, None, "domain execution").await;
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
            .replicated_branch_aggregated_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.replicated_branch_aggregated_states.remove(&placement);
        }
        let placements = self
            .correlator_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.correlator_states.remove(&placement);
        }
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
        for domain in domains {
            if let Some((_, execution)) = self.executions.remove(&domain) {
                self.stop_domain_execution(&domain, execution).await;
            }
        }
        self.endpoint_bindings.clear();
        self.ingestor_readiness.clear();
        self.expiring_stream_states.clear();
        self.replicated_deduplicator_states.clear();
        self.replicated_kafka_offset_states.clear();
        self.replicated_materialized_stream_states.clear();
        self.replicated_window_processor_states.clear();
        self.replicated_branch_aggregated_states.clear();
        self.correlator_states.clear();
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

    #[cfg(test)]
    pub(in crate::runtime) async fn recv_stream_message_batch(
        receiver: &mut mpsc::Receiver<RelayRecordBatch>,
        shutdown_rx: &mut watch::Receiver<bool>,
        flush_each: RuntimeFlushPolicy,
    ) -> BatchedInput {
        let first = tokio::select! {
            biased;
            message = receiver.recv() => message,
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return BatchedInput::Shutdown;
                }
                return BatchedInput::Shutdown;
            }
        };

        let Some(first) = first else {
            return BatchedInput::Closed;
        };

        if flush_each == RuntimeFlushPolicy::Immediate {
            let mut batch = vec![first];
            loop {
                tokio::task::consume_budget().await;
                match receiver.try_recv() {
                    Ok(message) => batch.push(message),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        return relay_batches_into_batched_input(batch);
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        return relay_batches_into_batched_input(batch);
                    }
                }
            }
        }

        let RuntimeFlushPolicy::Each {
            interval: flush_each,
            max_batch_size,
        } = flush_each
        else {
            unreachable!("immediate flush policy returned before deadline calculation");
        };
        let deadline = Instant::now() + flush_each;
        let mut batch = vec![first];
        let mut batch_size = relay_batches_estimated_bytes(&batch);
        if batch_size >= max_batch_size {
            return relay_batches_into_batched_input(batch);
        }
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        // Draining must flush messages already accepted by this consumer.
                        // Returning them here keeps shutdown from silently skipping the
                        // tail of a batch after upstream has successfully enqueued it.
                        return relay_batches_into_batched_input(batch);
                    }
                    return relay_batches_into_batched_input(batch);
                }
                _ = sleep_until(deadline) => return relay_batches_into_batched_input(batch),
                message = receiver.recv() => {
                    let Some(message) = message else {
                        return relay_batches_into_batched_input(batch);
                    };
                    batch_size = batch_size.saturating_add(message.estimated_bytes());
                    batch.push(message);
                    if batch_size >= max_batch_size {
                        return relay_batches_into_batched_input(batch);
                    }
                }
            }
        }
    }

    pub(in crate::runtime) async fn recv_runtime_consumer_batch(
        receiver: &mut RelayRuntimeFanIn,
        shutdown_rx: &mut watch::Receiver<bool>,
        flush_each: RuntimeFlushPolicy,
    ) -> BatchedInput {
        let received = tokio::select! {
            biased;
            message = receiver.recv() => message,
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return BatchedInput::Shutdown;
                }
                return BatchedInput::Shutdown;
            }
        };
        let Some(first) = received else {
            return BatchedInput::Closed;
        };

        if flush_each == RuntimeFlushPolicy::Immediate {
            let mut batch = vec![first];
            loop {
                tokio::task::consume_budget().await;
                match receiver.try_recv() {
                    Ok(Some(message)) => batch.push(message),
                    Ok(None) => return relay_batches_into_batched_input(batch),
                    Err(()) => return relay_batches_into_batched_input(batch),
                }
            }
        }

        let RuntimeFlushPolicy::Each {
            interval: flush_each,
            max_batch_size,
        } = flush_each
        else {
            unreachable!("immediate flush policy returned before deadline calculation");
        };
        let deadline = Instant::now() + flush_each;
        let mut batch = vec![first];
        let mut batch_size = relay_batches_estimated_bytes(&batch);
        if batch_size >= max_batch_size {
            return relay_batches_into_batched_input(batch);
        }
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return relay_batches_into_batched_input(batch);
                    }
                    return relay_batches_into_batched_input(batch);
                }
                _ = sleep_until(deadline) => return relay_batches_into_batched_input(batch),
                message = receiver.recv() => {
                    match message {
                        Some(message) => {
                            batch_size = batch_size.saturating_add(message.estimated_bytes());
                            batch.push(message);
                            if batch_size >= max_batch_size {
                                return relay_batches_into_batched_input(batch);
                            }
                        }
                        None => return relay_batches_into_batched_input(batch),
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
            } => {
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
        Ok(())
    }

    pub(in crate::runtime) async fn await_shutdown_task(
        mut task: JoinHandle<()>,
        domain: &Domain,
        ingestor: Option<&Identifier>,
        task_kind: &str,
    ) {
        const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

        match tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, &mut task).await {
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
                    grace_period = %humantime::format_duration(SHUTDOWN_GRACE_PERIOD),
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
        let message_namespace =
            Identifier::parse(INGEST_MESSAGE_NAMESPACE).expect("static namespace is valid");
        let empty_branching = Vec::new();
        let filter_where = compile_filter_map_program(
            domain,
            &ingestor.name,
            &[message_namespace],
            ingestor.filter_where.as_deref(),
            codec.schema().arrow_schema(),
            codec.schema().vm_sensitivity(),
            codec.schema().arrow_schema(),
            codec.schema().vm_sensitivity(),
            RuntimeVmCompileContext {
                available_materialized_streams: &execution.materialized_stream_specs,
                available_lookups: &execution.lookups,
                current_branching: &empty_branching,
                current_branch_schema: None,
                current_branch_sensitivity: None,
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
                &output.relay,
                &ingestor.source,
                output.filter_map.as_deref(),
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
                },
            )?;
            output_routes.routes.push(RelayProcessorOutputNode {
                relay: output.relay.clone(),
                filter_map: output.filter_map.clone(),
                compiled_program,
            });
        }
        let Some(base_output_relay) = output_routes.base_relay() else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "ingestor '{}' must declare at least one output route",
                    ingestor.name.as_str()
                ),
            });
        };
        let branching = execution
            .relay_branchings
            .get(&base_output_relay)
            .cloned()
            .unwrap_or_default();
        let model_index = execution
            .schedule
            .nodes
            .iter()
            .map(|node| ((node.kind, node.identifier.clone()), (*node.config).clone()))
            .collect::<HashMap<_, _>>();
        let branch_value_mappings = ingestor.branched_by.values().to_vec();
        let mut branched_templates = HashMap::default();
        if let Some(specs) = execution.branched_ingestors.get(&ingestor.name) {
            for spec in specs {
                let mut template = materialize_branch_instance_template(
                    spec,
                    &model_index,
                    &execution.relay_schemas,
                    &execution.relay_registries,
                    &execution.relay_services,
                )
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason,
                })?;
                template
                    .prepare_wasm_processors(self)
                    .await
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
            branching,
            branch_value_mappings,
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
            .get(&lookup.resource)
            .map(|value| *value)
        else {
            return Err(format!(
                "resource '{}' has no uploaded versions for lookup '{}'",
                lookup.resource.as_str(),
                lookup.name.as_str()
            ));
        };
        let path = resource_store
            .resolve_content_path(&lookup.resource, resource_version, &lookup.path)
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
