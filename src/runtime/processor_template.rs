use super::*;

macro_rules! declare_processor_input_filter_kinds {
    ($($Kind:ident => $label:literal, $operation:ident;)+) => {
        #[derive(Debug, Clone, Copy)]
        pub(super) enum ProcessorInputFilterKind {
            $($Kind,)+
        }

        impl ProcessorInputFilterKind {
            pub(super) fn label(self) -> &'static str {
                match self {
                    $(Self::$Kind => $label,)+
                }
            }

            pub(super) fn error_operation(self) -> MessageErrorOperation {
                match self {
                    $(Self::$Kind => MessageErrorOperation::$operation,)+
                }
            }
        }
    };
}

declare_processor_input_filter_kinds! {
    FromWhere => "FROM WHERE", SourceWhere;
    FilterWhere => "FILTER WHERE", FilterWhere;
}

pub(super) enum MaterializedDependencyResolution {
    Ready(HashMap<String, RuntimeValue>),
    Skip,
    Wait,
}

impl RelayProcessorOutputsNode {
    pub(super) fn matches_template(&self, template: &RelayProcessorOutputsTemplate) -> bool {
        self.routes.len() == template.routes.len()
            && self
                .routes
                .iter()
                .zip(&template.routes)
                .all(|(runtime, desired)| {
                    runtime.relay == desired.output_relay
                        && runtime.construction == desired.construction
                        && runtime.flush_policy == desired.flush_policy
                        && runtime.message_error_policy == desired.message_error_policy
                })
    }

    pub(super) fn apply_template(
        &mut self,
        template: &RelayProcessorOutputsTemplate,
    ) -> Result<bool, String> {
        if self.routes.len() != template.routes.len()
            || self
                .routes
                .iter()
                .zip(&template.routes)
                .any(|(runtime, desired)| runtime.relay != desired.output_relay)
        {
            return Err("dynamic processor update changed its route topology".to_string());
        }

        let mut changed = false;
        for (runtime, desired) in self.routes.iter_mut().zip(&template.routes) {
            let program_changed = runtime.construction != desired.construction
                || runtime.message_error_policy != desired.message_error_policy;
            changed |= program_changed || runtime.flush_policy != desired.flush_policy;
            if program_changed {
                runtime.compiled_program = None;
            }
            runtime.construction = desired.construction.clone();
            runtime.flush_policy = desired.flush_policy;
            runtime.message_error_policy = desired.message_error_policy.clone();
        }
        Ok(changed)
    }
}

impl RelayProcessorOperationNode {
    pub(super) fn apply_template(
        &mut self,
        template: &RelayProcessorOperationTemplate,
    ) -> Result<(), String> {
        match (self, template) {
            (
                Self::Deduplicator {
                    output_routes,
                    deduplicate_on,
                    max_time,
                    ..
                },
                RelayProcessorOperationTemplate::Deduplicator {
                    output_routes: desired_outputs,
                    deduplicate_on: desired_deduplicate_on,
                    max_time: desired_max_time,
                },
            ) => {
                if deduplicate_on != desired_deduplicate_on {
                    return Err(
                        "dynamic deduplicator update changed its state keyspace".to_string()
                    );
                }
                output_routes.apply_template(desired_outputs)?;
                *max_time = *desired_max_time;
                Ok(())
            }
            (
                Self::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
                    ..
                },
                RelayProcessorOperationTemplate::WindowProcessor {
                    output_routes: desired_outputs,
                    width_messages: desired_width_messages,
                    step_messages: desired_step_messages,
                    width_duration: desired_width_duration,
                    step_duration: desired_step_duration,
                    aggregate: desired_aggregate,
                    ..
                },
            ) => {
                if width_messages != desired_width_messages
                    || step_messages != desired_step_messages
                    || width_duration != desired_width_duration
                    || step_duration != desired_step_duration
                    || aggregate != desired_aggregate
                {
                    return Err(
                        "dynamic window processor update changed its state shape".to_string()
                    );
                }
                output_routes.apply_template(desired_outputs)?;
                Ok(())
            }
            (
                Self::Reorderer {
                    output_routes,
                    order_by,
                    max_time,
                    ..
                },
                RelayProcessorOperationTemplate::Reorderer {
                    output_routes: desired_outputs,
                    order_by: desired_order_by,
                    max_time: desired_max_time,
                },
            ) => {
                if order_by != desired_order_by {
                    return Err("dynamic reorderer update changed its ordering key".to_string());
                }
                output_routes.apply_template(desired_outputs)?;
                *max_time = *desired_max_time;
                Ok(())
            }
            (
                Self::Correlator {
                    output_routes,
                    left_relays,
                    right_relays,
                    correlate_where,
                    match_policy,
                    max_time,
                    timeout_policy,
                    compiled_where_program,
                    compiled_output_programs,
                    ..
                },
                RelayProcessorOperationTemplate::Correlator {
                    output_routes: desired_outputs,
                    left_relays: desired_left_relays,
                    right_relays: desired_right_relays,
                    correlate_where: desired_correlate_where,
                    match_policy: desired_match_policy,
                    max_time: desired_max_time,
                    timeout_policy: desired_timeout_policy,
                },
            ) => {
                if left_relays != desired_left_relays || right_relays != desired_right_relays {
                    return Err("dynamic correlator update changed its input sides".to_string());
                }
                if timeout_policy != desired_timeout_policy {
                    return Err("dynamic correlator update changed its timeout wiring".to_string());
                }
                if correlate_where != desired_correlate_where {
                    *compiled_where_program = None;
                }
                if output_routes.apply_template(desired_outputs)? {
                    for program in compiled_output_programs {
                        *program = None;
                    }
                }
                *correlate_where = desired_correlate_where.clone();
                *match_policy = *desired_match_policy;
                *max_time = *desired_max_time;
                Ok(())
            }
            (
                Self::Junction { output_routes },
                RelayProcessorOperationTemplate::Junction {
                    output_routes: desired_outputs,
                },
            ) => output_routes.apply_template(desired_outputs).map(|_| ()),
            (
                Self::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                    compiled_input_program,
                    ..
                },
                RelayProcessorOperationTemplate::Inferencer {
                    output_routes: desired_outputs,
                    resource: desired_resource,
                    resource_version: desired_resource_version,
                    file: desired_file,
                    inputs: desired_inputs,
                    output_schema: desired_output_schema,
                    compiled_input_program: desired_compiled_input_program,
                },
            ) => {
                if resource != desired_resource
                    || resource_version != desired_resource_version
                    || file != desired_file
                    || inputs != desired_inputs
                    || output_schema != desired_output_schema
                {
                    return Err(
                        "dynamic inferencer update changed its inference session".to_string()
                    );
                }
                *compiled_input_program = desired_compiled_input_program.clone();
                output_routes.apply_template(desired_outputs)?;
                Ok(())
            }
            (
                Self::WasmProcessor {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    limits,
                    ..
                },
                RelayProcessorOperationTemplate::WasmProcessor {
                    output_routes: desired_outputs,
                    resource: desired_resource,
                    resource_version: desired_resource_version,
                    file: desired_file,
                    limits: desired_limits,
                    ..
                },
            ) if resource == desired_resource
                && resource_version == desired_resource_version
                && file == desired_file
                && limits == desired_limits
                && output_routes.matches_template(desired_outputs) =>
            {
                Ok(())
            }
            (Self::WasmProcessor { .. }, RelayProcessorOperationTemplate::WasmProcessor { .. }) => {
                Err("WASM processors do not support dynamic configuration refresh".to_string())
            }
            _ => Err("dynamic processor update changed its operation kind".to_string()),
        }
    }
}

/// The exact relay schemas one WASM guest call encodes against.
pub(super) struct WasmGuestCallSchemas {
    pub(super) input: Arc<CompiledSchema>,
    pub(super) outputs: Vec<(RelayName, Arc<CompiledSchema>)>,
}

/// Resolves the input and output relay schemas a WASM guest call needs. Returns `None` after
/// NACKing everything the branch is holding when the graph can no longer describe them.
pub(super) fn wasm_guest_call_schemas(
    branch: &BranchRuntime,
    processor: &ModelName,
    input_relays: &[RelayName],
    output_routes: &RelayProcessorOutputsNode,
    ack_map: &mut WasmAckMap,
) -> Option<WasmGuestCallSchemas> {
    let Some(input_relay) = input_relays.first() else {
        for (_, context) in std::mem::take(ack_map) {
            context.acks.no_ack(format!(
                "wasm processor '{}' has no input relays",
                processor.as_str()
            ));
        }
        return None;
    };
    let input_schema = match relay_schema_for_runtime(&branch.runtime, &branch.domain, input_relay)
    {
        Ok(schema) => schema,
        Err(error) => {
            for (_, context) in std::mem::take(ack_map) {
                context.acks.no_ack(error.clone());
            }
            return None;
        }
    };
    let mut output_schemas = Vec::with_capacity(output_routes.routes.len());
    for output in &output_routes.routes {
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, &output.relay) {
            Ok(schema) => output_schemas.push((output.relay.clone(), schema)),
            Err(error) => {
                for (_, context) in std::mem::take(ack_map) {
                    context.acks.no_ack(error.clone());
                }
                return None;
            }
        }
    }
    Some(WasmGuestCallSchemas {
        input: input_schema,
        outputs: output_schemas,
    })
}

pub(super) fn wasm_instance_next_deadline(
    instance: Option<&nervix_wasm::WasmBranchInstance>,
) -> Option<Timestamp> {
    let instance = instance?;
    let mut next_deadline: Option<Timestamp> = None;
    for request in instance.timeout_requests() {
        let Ok(delay_nanos) = i64::try_from(request.delay.as_nanos()) else {
            continue;
        };
        let Some(deadline) = request
            .requested_at
            .unix_nanos()
            .checked_add(delay_nanos)
            .map(Timestamp::from_unix_nanos)
        else {
            // A guest-requested delay can run past the representable timestamp range. Such a
            // request has no deadline this clock can reach, so it never becomes the next one.
            continue;
        };
        next_deadline = match next_deadline {
            Some(current) => Some(current.min(deadline)),
            None => Some(deadline),
        };
    }
    next_deadline
}

impl RelayProcessorTemplate {
    pub(super) fn instantiate_output(
        output: &RelayProcessorOutputTemplate,
    ) -> RelayProcessorOutputNode {
        RelayProcessorOutputNode {
            relay: output.output_relay.clone(),
            construction: output.construction.clone(),
            branch: None,
            flush_policy: output.flush_policy,
            message_error_policy: output.message_error_policy.clone(),
            pending: Vec::new(),
            next_flush: None,
            compiled_program: None,
            compiled_branch_program: None,
        }
    }

    pub(super) fn instantiate_outputs(
        outputs: &RelayProcessorOutputsTemplate,
    ) -> RelayProcessorOutputsNode {
        RelayProcessorOutputsNode {
            routes: outputs
                .routes
                .iter()
                .map(Self::instantiate_output)
                .collect(),
        }
    }

    pub(super) fn instantiate(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        key: &Option<BranchKey>,
    ) -> Result<RelayProcessorNode, String> {
        Ok(RelayProcessorNode {
            kind: self.kind,
            processor: self.processor.clone(),
            input_relays: self.input_relays.clone(),
            input_collectors: self
                .input_collect_policies
                .iter()
                .map(|(relay, policy)| (relay.clone(), RuntimeInputCollector::new(*policy)))
                .collect(),
            error_policies: self.error_policies.clone(),
            from_where: self.from_where.clone(),
            compiled_from_where: HashMap::default(),
            filter_where: self.filter_where.clone(),
            materialized_state: self.materialized_state.clone(),
            pending_materialized: VecDeque::new(),
            compiled_filter_where: HashMap::default(),
            operation: match &self.operation {
                RelayProcessorOperationTemplate::Deduplicator {
                    output_routes,
                    deduplicate_on,
                    max_time,
                } => RelayProcessorOperationNode::Deduplicator {
                    output_routes: Self::instantiate_outputs(output_routes),
                    deduplicate_on: deduplicate_on.clone(),
                    max_time: *max_time,
                    compiled_key_program: None,
                    state: runtime
                        .replicated_deduplicator_state(runtime.state_placement(
                            domain,
                            RuntimeStateKind::Deduplicator,
                            self.kind,
                            &self.processor,
                            key.clone(),
                        ))
                        .map_err(|error| error.to_string())?,
                },
                RelayProcessorOperationTemplate::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
                    compiled_aggregates,
                } => {
                    let replicated_state = runtime
                        .replicated_window_processor_state(runtime.state_placement(
                            domain,
                            RuntimeStateKind::WindowProcessor,
                            self.kind,
                            &self.processor,
                            key.clone(),
                        ))
                        .map_err(|error| error.to_string())?;
                    let input_relay = self.input_relays.first().ok_or_else(|| {
                        format!(
                            "window processor '{}' requires an input relay",
                            self.processor.as_str()
                        )
                    })?;
                    let input_schema = relay_schema_for_runtime(runtime, domain, input_relay)?;
                    let state = replicated_state.restore_state(aggregate, &input_schema)?;
                    RelayProcessorOperationNode::WindowProcessor {
                        output_routes: Self::instantiate_outputs(output_routes),
                        width_messages: *width_messages,
                        step_messages: *step_messages,
                        width_duration: *width_duration,
                        step_duration: *step_duration,
                        aggregate: aggregate.clone(),
                        compiled_aggregates: compiled_aggregates.clone(),
                        state,
                        replicated_state,
                    }
                }
                RelayProcessorOperationTemplate::Reorderer {
                    output_routes,
                    order_by,
                    max_time,
                } => {
                    let output_routes = Self::instantiate_outputs(output_routes);
                    let output_buffers = (0..output_routes.routes.len())
                        .map(|_| ReordererOutputBuffer::default())
                        .collect();
                    RelayProcessorOperationNode::Reorderer {
                        output_routes,
                        order_by: order_by.clone(),
                        max_time: *max_time,
                        compiled_program: None,
                        output_buffers,
                        arrival_sequence: 0,
                    }
                }
                RelayProcessorOperationTemplate::Correlator {
                    output_routes,
                    left_relays,
                    right_relays,
                    correlate_where,
                    match_policy,
                    max_time,
                    timeout_policy,
                } => {
                    let output_routes = Self::instantiate_outputs(output_routes);
                    let compiled_output_programs =
                        (0..output_routes.routes.len()).map(|_| None).collect();
                    RelayProcessorOperationNode::Correlator {
                        output_routes,
                        left_relays: left_relays.clone(),
                        right_relays: right_relays.clone(),
                        correlate_where: correlate_where.clone(),
                        match_policy: *match_policy,
                        max_time: *max_time,
                        timeout_policy: timeout_policy.clone(),
                        compiled_where_program: None,
                        compiled_output_programs,
                        state: CorrelatorBranchState::default(),
                    }
                }
                RelayProcessorOperationTemplate::Junction { output_routes } => {
                    RelayProcessorOperationNode::Junction {
                        output_routes: Self::instantiate_outputs(output_routes),
                    }
                }
                RelayProcessorOperationTemplate::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                    compiled_input_program,
                } => {
                    let output_routes = Self::instantiate_outputs(output_routes);
                    let output_buffers = (0..output_routes.routes.len())
                        .map(|_| InferencerOutputBuffer::default())
                        .collect();
                    RelayProcessorOperationNode::Inferencer {
                        output_routes,
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
                        inputs: inputs.clone(),
                        output_schema: output_schema.clone(),
                        compiled_input_program: compiled_input_program.clone(),
                        output_buffers,
                        session: None,
                    }
                }
                RelayProcessorOperationTemplate::WasmProcessor {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    limits,
                    compiled,
                } => {
                    let replicated_state = runtime
                        .replicated_wasm_processor_state(
                            runtime.state_placement(
                                domain,
                                RuntimeStateKind::WasmProcessor,
                                self.kind,
                                &self.processor,
                                key.clone(),
                            ),
                            Vec::new(),
                            0,
                        )
                        .map_err(|error| error.to_string())?;
                    RelayProcessorOperationNode::WasmProcessor {
                        output_routes: Self::instantiate_outputs(output_routes),
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
                        limits: *limits,
                        compiled: compiled.clone(),
                        instance: None,
                        replicated_state,
                        ack_map: HashMap::default(),
                        next_ack_token: 1,
                        pending: Vec::new(),
                    }
                }
            },
            last_graph: None,
            applied_generation: 0,
        })
    }
}

impl BranchInstanceTemplate {
    pub(super) async fn prepare_wasm_processors(
        &mut self,
        runtime: &Runtime,
        domain: &DomainName,
    ) -> Result<(), String> {
        for processor in self.processors.values_mut() {
            tokio::task::consume_budget().await;
            if let RelayProcessorOperationTemplate::WasmProcessor {
                resource,
                resource_version,
                file,
                compiled,
                ..
            } = &mut processor.operation
            {
                *compiled = Some(
                    runtime
                        .compile_wasm_processor_module(
                            domain,
                            &processor.processor,
                            resource,
                            *resource_version,
                            file,
                        )
                        .await?,
                );
            }
        }
        Ok(())
    }

    pub(super) fn instantiate(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        key: Option<BranchKey>,
    ) -> Result<Mutex<BranchRuntime>, String> {
        let relays = self
            .relays
            .iter()
            .map(|(relay, template)| {
                (
                    relay.clone(),
                    ConcreteRelayRuntime::new(ConcreteRelayRuntimeBuild {
                        runtime: runtime.clone(),
                        domain: domain.clone(),
                        relay: relay.clone(),
                        registry: template.registry.clone(),
                        services: template.services.clone(),
                        key: key.clone(),
                    }),
                )
            })
            .collect::<HashMap<_, _>>();
        let materialized_states = self
            .materialized_streams
            .iter()
            .filter(|relay| !runtime.relay_is_cluster_scheduled(domain, relay))
            .map(|relay| {
                let execution = runtime.inner.executions.get(domain).ok_or_else(|| {
                    format!(
                        "materialized relay '{}' is not instantiated in domain '{}'",
                        relay.as_str(),
                        domain.as_str()
                    )
                })?;
                let Some(spec) = execution.materialized_stream_specs.get(relay) else {
                    return Err(format!(
                        "materialized relay '{}' is not instantiated in domain '{}'",
                        relay.as_str(),
                        domain.as_str()
                    ));
                };
                let schema = spec.schema.clone();
                let placement = runtime.state_placement(
                    domain,
                    RuntimeStateKind::MaterializedRelay,
                    ModelKind::Relay,
                    relay,
                    key.clone(),
                );
                runtime
                    .replicated_materialized_stream_state(placement, schema, None)
                    .map(|state| (relay.clone(), state))
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<HashMap<_, _>, String>>()?;
        let processors = self
            .processors
            .iter()
            .map(|(processor, template)| {
                Ok((
                    processor.clone(),
                    template.instantiate(runtime, domain, &key)?,
                ))
            })
            .collect::<Result<HashMap<_, _>, String>>()?;
        Ok(Mutex::new(BranchRuntime {
            key,
            runtime: runtime.clone(),
            domain: domain.clone(),
            source_kind: self.source_kind,
            source: self.source.clone(),
            root_relay: self.root_relay.clone(),
            relays,
            materialized_states,
            relay_state_epoch: None,
            processors,
            error_policies: self.error_policies.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use nervix_models::{ErrorPolicies, MessageErrorPolicy, ModelKind, ModelName, RelayName};
    use tokio::time::Duration;

    use super::*;

    #[test]
    fn processor_template_refresh_is_not_junction_specific() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let input = named::<RelayName>("events");
        let output = named::<RelayName>("unique_events");
        let processor = named::<ModelName>("deduplicate_events");
        let collect_policy = RuntimeInputCollectPolicy {
            interval: Duration::from_secs(1),
            max_batch_size: Some(1024),
        };
        let template = RelayProcessorTemplate {
            kind: ModelKind::Deduplicator,
            processor: processor.clone(),
            input_relays: vec![input.clone()],
            input_collect_policies: [(input.clone(), collect_policy)].into_iter().collect(),
            error_policies: ErrorPolicies::handled_by_log(),
            from_where: HashMap::default(),
            filter_where: None,
            materialized_state: Vec::new(),
            operation: RelayProcessorOperationTemplate::Deduplicator {
                output_routes: RelayProcessorOutputsTemplate {
                    routes: vec![RelayProcessorOutputTemplate {
                        output_relay: output,
                        construction: nervix_models::RouteConstruction::default(),
                        flush_policy: Some(RuntimeFlushPolicy::Immediate),
                        message_error_policy: MessageErrorPolicy::Log,
                    }],
                },
                deduplicate_on: vec![expression("input.event_id")],
                max_time: Duration::from_secs(600),
            },
        };
        let mut node = template
            .instantiate(&runtime, &domain, &None)
            .expect("deduplicator template must instantiate");

        let mut desired = template.clone();
        desired.filter_where = Some(expression("input.event_id > 0"));
        desired.input_collect_policies.insert(
            input.clone(),
            RuntimeInputCollectPolicy {
                interval: Duration::from_secs(2),
                max_batch_size: None,
            },
        );
        let RelayProcessorOperationTemplate::Deduplicator { max_time, .. } = &mut desired.operation
        else {
            panic!("test template must remain a deduplicator");
        };
        *max_time = Duration::from_secs(30);

        node.apply_node_template(desired)
            .expect("non-junction dynamic template fields must refresh in place");
        assert_eq!(node.filter_where, Some(expression("input.event_id > 0")));
        assert_eq!(
            node.input_collectors
                .get(&input)
                .expect("collector must remain installed")
                .policy
                .interval,
            Duration::from_secs(2)
        );
        let RelayProcessorOperationNode::Deduplicator { max_time, .. } = &node.operation else {
            panic!("runtime node must remain a deduplicator");
        };
        assert_eq!(*max_time, Duration::from_secs(30));

        let mut incompatible = template;
        let RelayProcessorOperationTemplate::Deduplicator { deduplicate_on, .. } =
            &mut incompatible.operation
        else {
            panic!("test template must remain a deduplicator");
        };
        *deduplicate_on = vec![expression("input.other_id")];
        assert!(
            node.apply_node_template(incompatible)
                .expect_err("a keyspace change must not hot-refresh")
                .contains("state keyspace")
        );
    }
}
