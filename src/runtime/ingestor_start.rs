//! Ingestor runtime materialization.
//!
//! Layer: data plane.
//! - **Owns.** Choosing which planned ingestors this node starts, stopping a running ingestor,
//!   and binding the dependencies every ingestor start reads.
//! - **Depends on.** Installed ingestor plans, installed domain capabilities and the source start
//!   path.
//! - **Must not know.** NSPL parsing, registry validation, placement selection, or which connector
//!   a source runs on.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "source task installation resolves retained branch, clock and acknowledgement \
                  owners"
    )
)]

use error_stack::ResultExt as _;

use super::*;

pub(in crate::runtime) type LookupRuntimeResult<T> = Result<T, Report<LookupRuntimeError>>;

#[derive(Debug, Error)]
pub(in crate::runtime) enum LookupRuntimeError {
    #[error("resource store is not attached")]
    ResourceStoreUnavailable,
    #[error(
        "failed to resolve path '{path}' in resource '{resource}' for lookup '{lookup}' in domain \
         '{domain}'"
    )]
    ResolveContentPath {
        domain: DomainName,
        lookup: LookupName,
        resource: ResourceName,
        path: String,
    },
    #[error("failed to open lookup file '{path}' for lookup '{lookup}' in domain '{domain}'")]
    OpenFile {
        domain: DomainName,
        lookup: LookupName,
        path: PathBuf,
    },
    #[error("failed to read lookup file '{path}' for lookup '{lookup}'")]
    ReadFile { lookup: LookupName, path: PathBuf },
    #[error("failed to decode lookup '{lookup}' line {line}")]
    DecodeLine { lookup: LookupName, line: usize },
    #[error("the node's bounded execution did not unfold lookup '{lookup}' line {line}")]
    UnfoldLine { lookup: LookupName, line: usize },
    #[error("failed to build lookup '{lookup}' record batch")]
    BuildBatch { lookup: LookupName },
    #[error("failed to read lookup '{lookup}' key field '{key}' at line {line}")]
    ReadKey {
        lookup: LookupName,
        line: usize,
        key: FieldName,
    },
    #[error("lookup '{lookup}' line {line} is missing key field '{key}'")]
    MissingKey {
        lookup: LookupName,
        line: usize,
        key: FieldName,
    },
}

pub(super) enum ScheduledIngestorStart {
    Plan(Arc<IngestorStartPlan>),
    Complete,
}

/// One running ingestor: the tasks its source runs in and the branch runtimes its routes feed.
pub(super) struct IngestorRuntime {
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) branched: Vec<Arc<IngestorRouteRuntime>>,
    pub(super) tasks: Vec<JoinHandle<()>>,
}

impl Runtime {
    pub(in crate::runtime) fn syslog_ingestor_bind_addr(&self, configured: &str) -> String {
        let dispatcher = self.inner.remote_dispatcher.load();
        if let Some(dispatcher) = dispatcher.as_deref() {
            return self
                .inner
                .fault_injection
                .syslog_ingestor_bind_addr(dispatcher.local_node_id(), configured);
        }

        configured.to_string()
    }

    pub(super) async fn start_missing_domain_ingestors(
        &self,
        domain: &DomainName,
    ) -> error_stack::Result<(), IngestorStartError> {
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.next_scheduled_ingestor_start_plan(Some(domain)) {
                ScheduledIngestorStart::Plan(plan) => self.start_ingestor(&plan).await?,
                ScheduledIngestorStart::Complete => break,
            }
        }
        Ok(())
    }

    pub(crate) async fn start_running_domain_ingestors(
        &self,
    ) -> error_stack::Result<(), IngestorStartError> {
        let _application = self.inner.schedule_application.lock().await;
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.next_scheduled_ingestor_start_plan(None) {
                ScheduledIngestorStart::Plan(plan) => self.start_ingestor(&plan).await?,
                ScheduledIngestorStart::Complete => break,
            }
        }
        // Every ingestor this node should run is running now, so the endpoint of a client ingestor
        // that is not running here ends its producers for good.
        let dispatcher = self.inner.remote_dispatcher.load();
        if let Some(dispatcher) = dispatcher.as_deref() {
            self.reconcile_client_ingestor_endpoints(dispatcher.local_node_id())
                .await;
        }
        Ok(())
    }

    /// The plan of the next ingestor this node executes but does not run, from the plans its
    /// running domains installed with their schedules.
    pub(super) fn next_scheduled_ingestor_start_plan(
        &self,
        requested_domain: Option<&DomainName>,
    ) -> ScheduledIngestorStart {
        let dispatcher = self.inner.remote_dispatcher.load();
        let local_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        let mut domains = self
            .inner
            .executions
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|domain| requested_domain.is_none_or(|requested| requested == domain))
            .collect::<Vec<_>>();
        domains.sort_by(|left, right| left.as_str().cmp(right.as_str()));

        for domain in domains {
            if self
                .inner
                .domains
                .get(&domain)
                .is_some_and(|state| !matches!(state.status, nervix_models::DomainStatus::Running))
            {
                continue;
            }
            let Some(execution) = self.inner.executions.get(&domain) else {
                continue;
            };
            if execution.passive_only {
                continue;
            }
            let mut local_plans = Vec::new();
            for plan in execution.revision.entrypoints.ingestors() {
                let identity =
                    NodeRef::new(ModelKind::Ingestor, ModelName::from(&plan.ingestor.name));
                let node = execution.revision.nodes.get(&identity).assured(
                    "every domain execution is installed and updated with the entrypoint plans \
                     decided from its planned revision",
                );
                if Self::scheduled_node_executes_locally(node, local_node_id) {
                    local_plans.push(plan.clone());
                }
            }
            drop(execution);

            for plan in local_plans {
                if !self
                    .inner
                    .ingestors
                    .contains_key(&plan.ingestor.runtime_key())
                {
                    return ScheduledIngestorStart::Plan(plan);
                }
            }
        }

        ScheduledIngestorStart::Complete
    }

    pub(in crate::runtime) async fn stop_ingestor(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> Result<(), RuntimeError> {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let Some((_, runtime)) = self.inner.ingestors.remove(&key) else {
            return Err(RuntimeError::IngestorNotRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.as_str().to_string(),
            });
        };

        let IngestorRuntime {
            shutdown,
            branched,
            tasks,
        } = runtime;
        if shutdown.send(true).is_err() {
            warn!(
                domain = domain.as_str(),
                ingestor = ingestor.as_str(),
                "ingestor shutdown signal had no receiver"
            );
        }
        // Every source's tasks end by closing it, so an endpoint source has unbound its routes by
        // the time they have been awaited.
        for task in tasks {
            Self::await_shutdown_task(task, domain, Some(ingestor), "ingestor").await;
        }
        for branched in branched {
            branched.shutdown().await;
        }

        self.inner.ingestor_statuses.remove(&key);
        self.clear_ingestor_readiness(domain, ingestor);
        // A client ingestor's producers outlive this execution until the endpoint learns whether a
        // restart keeps their contract.
        self.uninstall_client_execution(domain, ingestor);
        if self
            .ingestor_quiesce_control(domain, ingestor)
            .is_some_and(|control| !control.is_quiesced())
        {
            self.remove_ingestor_quiescence(domain, ingestor);
        }
        if self
            .inner
            .in_flight_by_ingestor
            .get(&key)
            .is_some_and(|tracker| tracker.outstanding() == 0)
        {
            self.inner.in_flight_by_ingestor.remove(&key);
        }
        Ok(())
    }

    /// Binds what every execution of `ingestor` dispatches through: its compiled node filter and
    /// routes over its input schema, and the branched entrypoints its routes feed. The input
    /// schema is what a transport's codec decodes its payloads into, as the staged routing
    /// revision installed that codec, or the schema a client source's batches carry.
    pub(in crate::runtime) async fn ingestor_dependencies(
        &self,
        ingestor: &IngestorSpec,
        input: &IngestorInputPlan,
    ) -> error_stack::Result<BoundIngestor, IngestorStartError> {
        let domain = &ingestor.domain;
        /// What the ingestor binds from its domain's execution, read under one lookup.
        struct BindingExecution {
            routing: StdArc<DomainRoutingSnapshot>,
            generation: u64,
        }
        let BindingExecution {
            routing,
            generation,
        } = match self.inner.executions.get(domain) {
            Some(execution) => BindingExecution {
                routing: execution.routing.staged(),
                generation: execution.start_version,
            },
            None => {
                return Err(Report::new(
                    IngestorStartError::DomainExecutionUnavailable {
                        domain: domain.clone(),
                        ingestor: ingestor.name.clone(),
                    },
                ));
            }
        };
        let input = match input {
            IngestorInputPlan::Transport(transport) => {
                let Some(codec) = routing.codecs.get(&transport.codec).cloned() else {
                    return Err(Report::new(IngestorStartError::CodecNotInstantiated {
                        domain: domain.clone(),
                        codec: transport.codec.clone(),
                    }));
                };
                BoundIngestorInput::Transport {
                    codec,
                    source: transport.source.clone(),
                }
            }
            IngestorInputPlan::Client(plan) => BoundIngestorInput::Client {
                plan: plan.clone(),
                generation,
            },
        };
        let input_schema = input.schema();
        let programs = ExecutionBuildDeps::from_routing(domain, &routing)
            .bind_ingestor(ingestor, &input_schema)
            .change_context_lazy(|| IngestorStartError::Bind {
                domain: domain.clone(),
            })?;
        let relays = RelayRuntimeHandles {
            services: &routing.relay_services,
        };
        let branched_templates = relays
            .route_templates(
                ModelKind::Ingestor,
                &ModelName::from(&ingestor.name),
                &ingestor.routes,
            )
            .change_context_lazy(|| IngestorStartError::Bind {
                domain: domain.clone(),
            })?;
        let dispatcher = self.inner.remote_dispatcher.load();
        let physical_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        let metrics = self.inner.metrics.resolve_global_node_message_metrics(
            domain,
            ModelKind::Ingestor,
            &ModelName::from(&ingestor.name),
            physical_node_id,
            "received",
        );
        Ok(BoundIngestor {
            input,
            dependencies: IngestorDependencies {
                handles: self
                    .ingest_task_handles(domain, &ingestor.name)
                    .change_context_lazy(|| IngestorStartError::Bind {
                        domain: domain.clone(),
                    })?,
                output_routes: programs.routes,
                filter_where: programs.filter_where,
                branched_templates,
                metrics,
            },
        })
    }

    pub(in crate::runtime) async fn load_lookup_runtime(
        &self,
        lookup: LookupResourcePlan,
        codec: Arc<CompiledCodec>,
    ) -> LookupRuntimeResult<LookupRuntime> {
        let domain = &lookup.resource.domain;
        let Some(resource_store) = self.inner.resource_store.load_full() else {
            return Err(Report::new(LookupRuntimeError::ResourceStoreUnavailable));
        };
        let path = resource_store
            .resolve_content_path(&lookup.resource, &lookup.path)
            .change_context(LookupRuntimeError::ResolveContentPath {
                domain: domain.clone(),
                lookup: lookup.name.clone(),
                resource: lookup.resource.identifier.clone(),
                path: lookup.path.clone(),
            })?;
        let file = tokio::fs::File::open(&path).await.map_err(|source| {
            Report::new(LookupRuntimeError::OpenFile {
                domain: domain.clone(),
                lookup: lookup.name.clone(),
                path: path.clone(),
            })
            .attach_printable(source.to_string())
        })?;
        let mut lines = tokio::io::BufReader::new(file).lines();
        let schema = codec.schema();
        // The whole file is one batch, so it is decoded into one set of Arrow columns rather than
        // one batch per line. Blank lines are skipped and a JAQ-backed codec may unfold one line
        // into any number of rows, so each row remembers the line it came from for the diagnostics
        // below.
        let mut builder = schema.batch_builder(0);
        let mut decoder = JsonDecoder::default();
        let mut row_lines = Vec::new();
        let mut line_number = 0usize;
        while let Some(line) = lines.next_line().await.map_err(|source| {
            Report::new(LookupRuntimeError::ReadFile {
                lookup: lookup.name.clone(),
                path: path.clone(),
            })
            .attach_printable(source.to_string())
        })? {
            nervix_primitives::task::consume_budget().await;
            line_number += 1;
            if line.trim().is_empty() {
                continue;
            }
            let decoded = decode_ingested_payload(
                self.executor(),
                QueueAdmission::RefuseWhenFull,
                &codec,
                line.as_bytes(),
                &mut decoder,
                &mut builder,
            )
            .await;
            let messages = match decoded {
                Ok(messages) => messages,
                Err(PayloadDecodeFailure::Codec(report)) => {
                    return Err(report.change_context(LookupRuntimeError::DecodeLine {
                        lookup: lookup.name.clone(),
                        line: line_number,
                    }));
                }
                Err(PayloadDecodeFailure::NotAdmitted(report)) => {
                    return Err(report.change_context(LookupRuntimeError::UnfoldLine {
                        lookup: lookup.name.clone(),
                        line: line_number,
                    }));
                }
            };
            row_lines.extend(std::iter::repeat_n(line_number, messages));
        }

        let batch = builder
            .finish()
            .change_context(LookupRuntimeError::BuildBatch {
                lookup: lookup.name.clone(),
            })?;
        let mut entries = HashMap::new();
        for (row, line_number) in row_lines.into_iter().enumerate() {
            nervix_primitives::task::consume_budget().await;
            let Some(value) = batch.value(row, lookup.key_field.as_str()).change_context(
                LookupRuntimeError::ReadKey {
                    lookup: lookup.name.clone(),
                    line: line_number,
                    key: lookup.key_field.clone(),
                },
            )?
            else {
                return Err(Report::new(LookupRuntimeError::MissingKey {
                    lookup: lookup.name.clone(),
                    line: line_number,
                    key: lookup.key_field.clone(),
                }));
            };
            entries.insert(value.to_key_fragment(), row);
        }

        let dispatcher = self.inner.remote_dispatcher.load();
        let physical_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        let metrics = self.inner.metrics.resolve_global_node_message_metrics(
            domain,
            ModelKind::Lookup,
            &ModelName::from(&lookup.name),
            physical_node_id,
            "received",
        );
        Ok(LookupRuntime {
            plan: lookup,
            schema,
            batch: Arc::new(batch),
            entries: Arc::new(entries),
            metrics,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{
        ClientConfigEntry, ClientName, ClusterNodeName, ClusterSchedule, CodecName,
        CodecWireFormat, CreateClientMqtt, CreateCodec, CreateIngestor, CreateJsonWireSchema,
        CreateRelay, CreateSchema, DomainConfig, DomainPace, DomainSchedule, DomainState,
        DomainStatus, GeneralErrorPolicy, IngestSource, IngestorName, JsonType, ModelKind,
        MqttIngestMode, MqttQos, MqttSession, OutputBranch, ParseAsType, ProcessorOutputs,
        RelayBranching, RelayName, RetryPolicy, SchemaField, SchemaName, WireSchemaField,
        WireSchemaName,
    };
    use nonzero_ext::nonzero;

    use super::*;

    /// A runtime with the host's resolver installed, as every node's runtime has one.
    async fn runtime_with_dns() -> Runtime {
        let dns = nervix_dns::DnsResolver::load(nervix_dns::DnsConfiguration::system())
            .await
            .expect("the host's resolver configuration should load");
        Runtime::with_persistence_and_temp_dir(
            nervix_execution::Executor::default(),
            Some(dns),
            None,
            DEFAULT_STATE_SNAPSHOT_INTERVAL,
            ConfiguredFaultInjection::default(),
            PathBuf::from(DEFAULT_TEMP_DIR),
            DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
        .expect("a runtime without persistence builds")
    }

    #[nervix_primitives::test]
    async fn scheduled_mqtt_client_id_conflicts_are_visible_on_describe() {
        let runtime = runtime_with_dns().await;
        let domain = domain("default");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Running,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            },
        )]));

        let schema = named::<SchemaName>("notification");
        let wire_schema = named::<WireSchemaName>("notification_wire");
        let codec = named::<CodecName>("notification_json");
        let relay = named::<RelayName>("notifications");
        let client = named::<ClientName>("mqtt_main");
        let ingestor = named::<IngestorName>("mqtt_notifications");
        let result = runtime
            .apply_cluster_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &ClusterSchedule::from_iter([DomainSchedule::new(
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
                        scheduled_model(nervix_models::Model::WireJsonSchema(
                            CreateJsonWireSchema {
                                name: wire_schema.clone(),
                                strictness: Default::default(),
                                fields: vec![WireSchemaField {
                                    name: named("user_id"),
                                    ty: JsonType::Integer,
                                    optional: false,
                                }],
                            },
                        )),
                        scheduled_model(nervix_models::Model::Codec(CreateCodec {
                            name: codec.clone(),
                            wire_format: CodecWireFormat::Json {
                                wire_schema: wire_schema.clone(),
                            },
                            schema: schema.clone(),
                            encoding_rules: Vec::new(),
                        })),
                        scheduled_model(nervix_models::Model::Relay(CreateRelay {
                            name: relay.clone(),
                            schema: schema.clone(),
                            buffer: nonzero!(2usize),
                            branching: RelayBranching::unbranched(),
                            materialized_state: None,
                        })),
                        scheduled_model(nervix_models::Model::ClientMqtt(CreateClientMqtt {
                            name: client.clone(),
                            mount: None,
                            config: vec![
                                ClientConfigEntry {
                                    key: "addr".to_string(),
                                    value: "mqtt://127.0.0.1:1883".to_string(),
                                },
                                ClientConfigEntry {
                                    key: "client_id".to_string(),
                                    value: "fixed-client".to_string(),
                                },
                            ],
                        })),
                        scheduled_model(nervix_models::Model::Ingestor(CreateIngestor {
                            name: ingestor.clone(),
                            output_routes: with_inherit_all(ProcessorOutputs::single(
                                relay.clone(),
                            ))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(OutputBranch::Unbranched),
                            input: nervix_models::IngestorInput::Transport(
                                nervix_models::TransportIngestorInput {
                                    source: IngestSource::Mqtt {
                                        client,
                                        topic: "notifications".to_string(),
                                        instances: nonzero!(2u64),
                                        mode: MqttIngestMode::NoAckSequential {
                                            session: MqttSession::Clean,
                                            qos: MqttQos::AtMostOnce,
                                        },
                                        quiesce: nervix_models::IngestQuiesceMode::Drop,
                                    },
                                    codec: codec.clone(),
                                },
                            ),
                            timestamp_source: None,
                            general_error_policy: GeneralErrorPolicy::Log,
                            filter_where: None,
                        })),
                    ],
                    Vec::new(),
                )]),
            )
            .await;

        result.expect("fixed mqtt client_id conflict should be reported by ingestor state");

        // Each instance reports the conflict when its source loop first tries to resume, so the
        // status is read until it shows it, within a bound generous enough for a loaded machine.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let describe = runtime
                .describe_local_ingestor(&domain, &ingestor)
                .expect("describe should succeed for scheduled ingestor");
            assert!(describe.running);
            let reported = describe.transient_error.as_deref().is_some_and(|error| {
                error.contains("MQTT client_id 'fixed-client' is shared by 2 instances")
            });
            if reported {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "describe should expose mqtt client_id conflict, got {:?}",
                describe.transient_error
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    #[nervix_primitives::test]
    async fn scheduled_ingestor_start_failure_removes_partial_domain_execution() {
        let runtime = Runtime::default();
        let domain = domain("default");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Running,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            },
        )]));

        let schema = named::<SchemaName>("notification");
        let wire_schema = named::<WireSchemaName>("notification_wire");
        let codec = named::<CodecName>("notification_json");
        let relay = named::<RelayName>("notifications");
        let client = named::<ClientName>("mqtt_main");
        let ingestor = named::<IngestorName>("mqtt_notifications");
        let result = runtime
            .apply_cluster_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &ClusterSchedule::from_iter([DomainSchedule::new(
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
                        scheduled_model(nervix_models::Model::WireJsonSchema(
                            CreateJsonWireSchema {
                                name: wire_schema.clone(),
                                strictness: Default::default(),
                                fields: vec![WireSchemaField {
                                    name: named("user_id"),
                                    ty: JsonType::Integer,
                                    optional: false,
                                }],
                            },
                        )),
                        scheduled_model(nervix_models::Model::Codec(CreateCodec {
                            name: codec.clone(),
                            wire_format: CodecWireFormat::Json {
                                wire_schema: wire_schema.clone(),
                            },
                            schema: schema.clone(),
                            encoding_rules: Vec::new(),
                        })),
                        scheduled_model(nervix_models::Model::Relay(CreateRelay {
                            name: relay.clone(),
                            schema: schema.clone(),
                            buffer: nonzero!(2usize),
                            branching: RelayBranching::unbranched(),
                            materialized_state: None,
                        })),
                        scheduled_model(nervix_models::Model::ClientMqtt(CreateClientMqtt {
                            name: client.clone(),
                            mount: None,
                            config: vec![ClientConfigEntry {
                                key: "addr".to_string(),
                                value: "mqtt://127.0.0.1:1883".to_string(),
                            }],
                        })),
                        scheduled_model(nervix_models::Model::Ingestor(CreateIngestor {
                            name: ingestor.clone(),
                            output_routes: with_inherit_all(ProcessorOutputs::single(
                                relay.clone(),
                            ))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(OutputBranch::Unbranched),
                            input: nervix_models::IngestorInput::Transport(
                                nervix_models::TransportIngestorInput {
                                    source: IngestSource::Mqtt {
                                        client,
                                        topic: "notifications".to_string(),
                                        instances: nonzero!(1u64),
                                        mode: MqttIngestMode::AckSequential {
                                            timeout: "oops".to_string(),
                                            retry_policy: RetryPolicy {
                                                backoff: "100ms".to_string(),
                                                max_backoff: "200ms".to_string(),
                                            },
                                        },
                                        quiesce: nervix_models::IngestQuiesceMode::Drop,
                                    },
                                    codec: codec.clone(),
                                },
                            ),
                            timestamp_source: None,
                            general_error_policy: GeneralErrorPolicy::Log,
                            filter_where: None,
                        })),
                    ],
                    Vec::new(),
                )]),
            )
            .await;

        let error = result.expect_err("invalid ACK timeout must fail schedule application");
        let start_error = error.to_string();
        assert!(
            start_error.contains("invalid ack timeout 'oops'"),
            "unexpected start error: {start_error}"
        );
        assert!(
            !runtime.inner.executions.contains_key(&domain),
            "failed scheduled ingestor start must not leave a partial domain execution"
        );
        assert!(
            !runtime
                .inner
                .ingestors
                .contains_key(&DomainNodeRef::node_in(
                    domain.clone(),
                    ModelKind::Ingestor,
                    ingestor.clone()
                )),
            "failed scheduled ingestor start must not leave an ingestor runtime"
        );
        let describe = runtime
            .describe_local_ingestor(&domain, &ingestor)
            .expect("describe should represent the stopped ingestor");
        assert!(!describe.running);
        assert!(!describe.ready);
        assert_eq!(
            describe.transient_error.as_deref(),
            Some(start_error.as_str())
        );
    }
}
