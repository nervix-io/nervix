use std::borrow::Cow;

use super::*;

pub(super) enum ScheduledIngestorStart {
    Plan(Box<IngestorStartPlan>),
    Complete,
    Error(RuntimeError),
}

impl Runtime {
    pub(in crate::runtime) fn syslog_ingestor_bind_addr(&self, configured: &str) -> String {
        #[cfg(feature = "testing")]
        if let Some(node_id) = self.inner.remote_dispatch.local_node_id.read().as_ref() {
            return self
                .inner
                .syslog_ingestor_bind_address_overrides
                .resolve(node_id, configured);
        }

        configured.to_string()
    }

    pub(in crate::runtime) async fn start_ingestor(
        &self,
        plan: IngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        ingestors::IngestorStarter::start(self, plan).await
    }

    pub(super) async fn start_missing_domain_ingestors(
        &self,
        domain: &DomainName,
    ) -> Result<(), RuntimeError> {
        loop {
            tokio::task::consume_budget().await;
            match self.next_scheduled_ingestor_start_plan(Some(domain)) {
                ScheduledIngestorStart::Plan(plan) => self.start_ingestor(*plan).await?,
                ScheduledIngestorStart::Complete => break,
                ScheduledIngestorStart::Error(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub async fn start_running_domain_ingestors(&self) -> Result<(), RuntimeError> {
        let _lock = self.inner.schedule_apply_lock.lock().await;
        loop {
            tokio::task::consume_budget().await;
            match self.next_scheduled_ingestor_start_plan(None) {
                ScheduledIngestorStart::Plan(plan) => self.start_ingestor(*plan).await?,
                ScheduledIngestorStart::Complete => break,
                ScheduledIngestorStart::Error(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(super) fn next_scheduled_ingestor_start_plan(
        &self,
        requested_domain: Option<&DomainName>,
    ) -> ScheduledIngestorStart {
        let local_node_id = self.inner.remote_dispatch.local_node_id.read().clone();
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
            let passive_only = execution.passive_only;
            let schedule = execution.schedule.clone();
            drop(execution);

            if passive_only {
                continue;
            }

            for node in schedule.nodes.values() {
                if node.kind() != ModelKind::Ingestor
                    || !Self::scheduled_node_executes_locally(node, local_node_id.as_ref())
                {
                    continue;
                }

                let key = node.identity().in_domain(&domain);
                if self.inner.ingestors.contains_key(&key) {
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

                let plan = match IngestorStartPlan::decide(&domain, node, &source_model) {
                    Ok(plan) => plan,
                    Err(error) => {
                        return ScheduledIngestorStart::Error(RuntimeError::BuildDomainExecution {
                            domain: domain.as_str().to_string(),
                            reason: format!(
                                "cannot plan ingestor '{}': {error}",
                                ingestor.name.as_str()
                            ),
                        });
                    }
                };
                return ScheduledIngestorStart::Plan(Box::new(plan));
            }
        }

        ScheduledIngestorStart::Complete
    }

    pub(super) fn source_model_for_scheduled_ingestor(
        schedule: &DomainSchedule,
        ingestor: &CreateIngestor,
    ) -> Option<Model> {
        let source_ref = ingestor.source.source_ref();
        let source_kind = match &ingestor.source {
            IngestSource::Endpoint { .. } => ModelKind::Endpoint,
            _ => ModelKind::Client,
        };
        schedule
            .nodes
            .get(&NodeRef::new(source_kind, source_ref))
            .map(|node| (*node.config).clone())
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
                    let remove_route = if let Some(mut bindings) =
                        self.inner.endpoint_bindings.get_mut(&route_key)
                    {
                        bindings.retain(|binding| binding.runtime_key != key);
                        bindings.is_empty()
                    } else {
                        false
                    };
                    if remove_route {
                        self.inner.endpoint_bindings.remove(&route_key);
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

    pub(in crate::runtime) async fn ingestor_dependencies(
        &self,
        domain: &DomainName,
        ingestor: &IngestorSpec,
    ) -> Result<IngestorDependencies, RuntimeError> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: ingestor
                    .routes
                    .first()
                    .map(|route| &route.relay)
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
                identifier: &ModelName::from(&ingestor.name),
            },
            ingestor.filter_where.as_ref(),
            RuntimeVmSchema {
                schema: codec.schema().arrow_schema(),
                sensitivity: codec.schema().vm_sensitivity(),
            },
            ingestor.allow_header_reads,
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
            routes: Vec::with_capacity(ingestor.routes.len()),
        };
        for output in &ingestor.routes {
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
                ingestor.metadata_kind,
                ingestor.allow_header_reads,
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
            let target_branch_schema = execution
                .relay_branching_schemas
                .get(&output.relay)
                .cloned()
                .flatten();
            let compiled_branch_program = compile_output_branch_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: &ModelName::from(&ingestor.name),
                },
                output.branch.as_ref(),
                RuntimeVmSchema {
                    schema: codec.schema().arrow_schema(),
                    sensitivity: codec.schema().vm_sensitivity(),
                },
                RuntimeVmSchema {
                    schema: output_schema.arrow_schema(),
                    sensitivity: output_schema.vm_sensitivity(),
                },
                target_branch_schema,
                RuntimeVmCompileContext {
                    available_materialized_streams: &execution.materialized_stream_specs,
                    available_lookups: &execution.lookups,
                    current_branching: &empty_branching,
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
                        policy,
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
                compiled_branch_program,
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
            .values()
            .map(|node| (*node.config).clone())
            .collect::<ModelIndex>();
        let mut branched_templates = HashMap::default();
        if let Some(specs) = execution
            .branched_ingestors
            .get(&ModelName::from(&ingestor.name))
        {
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
        domain: &DomainName,
        lookup: CreateLookup,
        codec: Arc<CompiledCodec>,
    ) -> Result<LookupRuntime, String> {
        let Some(resource_store) = self.inner.resource_store.read().clone() else {
            return Err("resource store is not attached".to_string());
        };
        let Some(resource_version) = self
            .inner
            .latest_resource_versions
            .get(&DomainResourceKey {
                domain: domain.clone(),
                resource: lookup.resource.clone(),
            })
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
        let schema = codec.schema();
        // The whole file is one batch, so it is decoded into one set of Arrow columns rather than
        // one batch per line. Blank lines are skipped, so each row remembers the line it came from
        // for the diagnostics below.
        let mut builder = schema.batch_builder(0);
        let mut row_lines = Vec::new();
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
            decode_ingested_payload(&codec, Cow::Owned(line.into_bytes()), &mut builder)
                .await
                .map_err(|error| {
                    format!(
                        "failed to decode lookup '{}' line {}: {}",
                        lookup.name.as_str(),
                        line_number,
                        error
                    )
                })?;
            row_lines.push(line_number);
        }

        let batch = builder.finish()?;
        let mut entries = HashMap::new();
        for (row, line_number) in row_lines.into_iter().enumerate() {
            tokio::task::consume_budget().await;
            let Some(value) = batch.value(row, lookup.key_field.as_str())? else {
                return Err(format!(
                    "lookup '{}' line {} is missing key field '{}'",
                    lookup.name.as_str(),
                    line_number,
                    lookup.key_field.as_str()
                ));
            };
            entries.insert(value.to_key_fragment(), row);
        }

        Ok(LookupRuntime {
            model: lookup,
            resource_version,
            schema,
            batch: Arc::new(batch),
            entries: Arc::new(entries),
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

    #[tokio::test]
    async fn scheduled_mqtt_client_id_conflicts_are_visible_on_describe() {
        let runtime = Runtime::default();
        let domain = domain("default");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    period: "1s".to_string(),
                    skew: "0s".to_string(),
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
                            decode_using_codec: codec.clone(),
                            timestamp_source: None,
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
                            general_error_policy: GeneralErrorPolicy::Log,
                            filter_where: None,
                        })),
                    ],
                    Vec::new(),
                )]),
            )
            .await;

        result.expect("fixed mqtt client_id conflict should be reported by ingestor state");

        let describe = runtime
            .describe_local_ingestor(&domain, &ingestor)
            .expect("describe should succeed for scheduled ingestor");
        assert!(describe.running);
        assert!(
            describe
                .transient_error
                .as_deref()
                .is_some_and(|error| error
                    .contains("MQTT client_id 'fixed-client' is shared by 2 instances")),
            "describe should expose mqtt client_id conflict, got {:?}",
            describe.transient_error
        );
    }

    #[tokio::test]
    async fn scheduled_ingestor_start_failure_removes_partial_domain_execution() {
        let runtime = Runtime::default();
        let domain = domain("default");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    period: "1s".to_string(),
                    skew: "0s".to_string(),
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
                            decode_using_codec: codec.clone(),
                            timestamp_source: None,
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
                            general_error_policy: GeneralErrorPolicy::Log,
                            filter_where: None,
                        })),
                    ],
                    Vec::new(),
                )]),
            )
            .await;

        let error = result.expect_err("invalid ACK timeout must fail schedule application");
        assert!(
            error.to_string().contains("invalid ack timeout 'oops'"),
            "unexpected start error: {error}"
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
        let describe_error = runtime
            .describe_local_ingestor(&domain, &ingestor)
            .expect_err("describe should expose the domain instantiation error");
        assert!(
            describe_error.contains("invalid ack timeout 'oops'"),
            "describe should expose start error, got {describe_error}"
        );
    }
}
