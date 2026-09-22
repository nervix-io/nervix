use error_stack::{Report, ResultExt as _};

use super::*;

/// Every way applying a refreshed template to a running processor, or instantiating a processor
/// for a branch, fails.
#[derive(Debug, thiserror::Error)]
pub(super) enum ProcessorTemplateError {
    #[error("dynamic processor update changed its route topology")]
    RouteTopology,
    #[error("dynamic deduplicator update changed its state keyspace")]
    DeduplicatorKeyspace,
    #[error("dynamic window processor update changed its state shape")]
    WindowStateShape,
    #[error("dynamic reorderer update changed its ordering key")]
    ReordererOrderingKey,
    #[error("dynamic correlator update changed its input sides")]
    CorrelatorInputSides,
    #[error("dynamic correlator update changed its timeout wiring")]
    CorrelatorTimeoutWiring,
    #[error("dynamic inferencer update changed its inference session")]
    InferencerSession,
    #[error("WASM processors do not support dynamic configuration refresh")]
    WasmRefresh,
    #[error("dynamic processor update changed its operation kind")]
    OperationKind,
    #[error(
        "processor template targets {} '{}', not {} '{}'",
        .template_kind.as_str(),
        .template_processor.as_str(),
        .kind.as_str(),
        .processor.as_str()
    )]
    TargetMismatch {
        template_kind: ModelKind,
        template_processor: ModelName,
        kind: ModelKind,
        processor: ModelName,
    },
    #[error("dynamic {} update changed processor input topology", .kind.as_str())]
    InputTopology { kind: ModelKind },
    #[error("dynamic {} update changed materialized-state dependencies", .kind.as_str())]
    MaterializedDependencies { kind: ModelKind },
    #[error(
        "failed to open {} '{}' replicated state for branch '{}'",
        .kind.as_str(),
        .processor.as_str(),
        branch_key_display(.branch)
    )]
    ReplicatedState {
        kind: ModelKind,
        processor: ModelName,
        branch: Option<BranchKey>,
    },
    #[error("window processor '{}' requires an input relay", .processor.as_str())]
    WindowInputRelay { processor: ModelName },
    #[error("failed to resolve window processor '{}' input schema", .processor.as_str())]
    WindowInputSchema { processor: ModelName },
    #[error(
        "failed to restore window processor '{}' state for branch '{}'",
        .processor.as_str(),
        branch_key_display(.branch)
    )]
    WindowRestore {
        processor: ModelName,
        branch: Option<BranchKey>,
    },
    #[error("could not bind branch domain clock")]
    BindDomainClock,
}

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
    ) -> error_stack::Result<bool, ProcessorTemplateError> {
        if self.routes.len() != template.routes.len()
            || self
                .routes
                .iter()
                .zip(&template.routes)
                .any(|(runtime, desired)| runtime.relay != desired.output_relay)
        {
            return Err(Report::new(ProcessorTemplateError::RouteTopology));
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
    ) -> error_stack::Result<(), ProcessorTemplateError> {
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
                    return Err(Report::new(ProcessorTemplateError::DeduplicatorKeyspace));
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
                    return Err(Report::new(ProcessorTemplateError::WindowStateShape));
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
                    return Err(Report::new(ProcessorTemplateError::ReordererOrderingKey));
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
                    return Err(Report::new(ProcessorTemplateError::CorrelatorInputSides));
                }
                if timeout_policy != desired_timeout_policy {
                    return Err(Report::new(ProcessorTemplateError::CorrelatorTimeoutWiring));
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
                    return Err(Report::new(ProcessorTemplateError::InferencerSession));
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
                Err(Report::new(ProcessorTemplateError::WasmRefresh))
            }
            _ => Err(Report::new(ProcessorTemplateError::OperationKind)),
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
    let input_schema = match branch.relay_schema(input_relay) {
        Ok(schema) => schema,
        Err(error) => {
            for (_, context) in std::mem::take(ack_map) {
                context.acks.no_ack(error.to_string());
            }
            return None;
        }
    };
    let mut output_schemas = Vec::with_capacity(output_routes.routes.len());
    for output in &output_routes.routes {
        match branch.relay_schema(&output.relay) {
            Ok(schema) => output_schemas.push((output.relay.clone(), schema)),
            Err(error) => {
                for (_, context) in std::mem::take(ack_map) {
                    context.acks.no_ack(error.to_string());
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
    instance: Option<&WasmLiveInstance>,
) -> Option<Timestamp> {
    let instance = instance?;
    let mut next_deadline: Option<Timestamp> = None;
    for request in instance.guest.timeout_requests() {
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
            flush_timer: BranchBufferTimer::default(),
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
    ) -> error_stack::Result<RelayProcessorNode, ProcessorTemplateError> {
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
                } => {
                    let placement = runtime
                        .state_placement(
                            domain,
                            RuntimeStateKind::Deduplicator,
                            self.kind,
                            &self.processor,
                            key.clone(),
                        )
                        .change_context_lazy(|| ProcessorTemplateError::ReplicatedState {
                            kind: self.kind,
                            processor: self.processor.clone(),
                            branch: key.clone(),
                        })?;
                    let state = runtime
                        .replicated_deduplicator_state(placement)
                        .change_context_lazy(|| ProcessorTemplateError::ReplicatedState {
                            kind: self.kind,
                            processor: self.processor.clone(),
                            branch: key.clone(),
                        })?;
                    RelayProcessorOperationNode::Deduplicator {
                        output_routes: Self::instantiate_outputs(output_routes),
                        deduplicate_on: deduplicate_on.clone(),
                        max_time: *max_time,
                        compiled_key_program: None,
                        keyspace: ReplicatedDeduplicatorState::keyspace(&state),
                    }
                }
                RelayProcessorOperationTemplate::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
                    plan,
                    compiled_aggregates,
                } => {
                    let placement = runtime
                        .state_placement(
                            domain,
                            RuntimeStateKind::WindowProcessor,
                            self.kind,
                            &self.processor,
                            key.clone(),
                        )
                        .change_context_lazy(|| ProcessorTemplateError::ReplicatedState {
                            kind: self.kind,
                            processor: self.processor.clone(),
                            branch: key.clone(),
                        })?;
                    let replicated_state = runtime
                        .replicated_window_processor_state(placement)
                        .change_context_lazy(|| ProcessorTemplateError::ReplicatedState {
                            kind: self.kind,
                            processor: self.processor.clone(),
                            branch: key.clone(),
                        })?;
                    let input_relay = self.input_relays.first().ok_or_else(|| {
                        Report::new(ProcessorTemplateError::WindowInputRelay {
                            processor: self.processor.clone(),
                        })
                    })?;
                    let input_schema = relay_schema_for_runtime(runtime, domain, input_relay)
                        .change_context_lazy(|| ProcessorTemplateError::WindowInputSchema {
                            processor: self.processor.clone(),
                        })?;
                    let state = replicated_state
                        .restore_state(plan, &input_schema)
                        .change_context_lazy(|| ProcessorTemplateError::WindowRestore {
                            processor: self.processor.clone(),
                            branch: key.clone(),
                        })?;
                    RelayProcessorOperationNode::WindowProcessor {
                        output_routes: Self::instantiate_outputs(output_routes),
                        width_messages: *width_messages,
                        step_messages: *step_messages,
                        width_duration: *width_duration,
                        step_duration: *step_duration,
                        aggregate: aggregate.clone(),
                        plan: plan.clone(),
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
                    let placement = runtime
                        .state_placement(
                            domain,
                            RuntimeStateKind::WasmProcessor,
                            self.kind,
                            &self.processor,
                            key.clone(),
                        )
                        .change_context_lazy(|| ProcessorTemplateError::ReplicatedState {
                            kind: self.kind,
                            processor: self.processor.clone(),
                            branch: key.clone(),
                        })?;
                    let replicated_state = runtime
                        .replicated_wasm_processor_state(placement)
                        .change_context_lazy(|| ProcessorTemplateError::ReplicatedState {
                            kind: self.kind,
                            processor: self.processor.clone(),
                            branch: key.clone(),
                        })?;
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
                        state_reset: WasmGuestStateResetFence::Open,
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
    ) -> error_stack::Result<(), WasmInstanceError> {
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
    ) -> error_stack::Result<Mutex<BranchRuntime>, ProcessorTemplateError> {
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
        // Branch-local materialized states are opened as their relays first materialize a batch,
        // because opening a persisted snapshot is admitted, charged work that this synchronous
        // instantiation cannot wait for.
        let materialized_states = HashMap::default();
        let mut processors = HashMap::default();
        for (processor, template) in &self.processors {
            let node = template.instantiate(runtime, domain, &key)?;
            processors.insert(processor.clone(), node);
        }
        let dispatcher = runtime.inner.remote_dispatcher.load();
        let physical_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        let branch_key = branch_key_display(&key);
        let source_metrics =
            runtime
                .inner
                .metrics
                .resolve_node_batch_metrics(NodeBatchMetricsSpec {
                    domain,
                    kind: self.source_kind,
                    node: &ModelName::from(&self.source),
                    relay: &self.root_relay,
                    physical_node_id,
                    direction: "sent",
                    branch_key: Some(branch_key),
                });
        let source_input_metrics = if self.source_kind == ModelKind::Ingestor {
            Some(runtime.inner.metrics.resolve_branch_node_message_metrics(
                domain,
                self.source_kind,
                &ModelName::from(&self.source),
                physical_node_id,
                "received",
                branch_key,
            ))
        } else {
            None
        };
        let processor_inputs = processors
            .iter()
            .map(|(identifier, processor)| {
                let inputs = processor
                    .input_relays
                    .iter()
                    .map(|relay| {
                        let metrics = runtime.inner.metrics.resolve_node_input_metrics(
                            domain,
                            processor.kind,
                            &processor.processor,
                            relay,
                            physical_node_id,
                            Some(branch_key),
                        );
                        (relay.clone(), metrics)
                    })
                    .collect();
                (identifier.clone(), inputs)
            })
            .collect();
        let processor_outputs = processors
            .iter()
            .map(|(identifier, processor)| {
                let outputs = processor
                    .operation
                    .output_routes()
                    .routes
                    .iter()
                    .map(|output| {
                        let metrics = runtime.inner.metrics.resolve_node_batch_metrics(
                            NodeBatchMetricsSpec {
                                domain,
                                kind: processor.kind,
                                node: &processor.processor,
                                relay: &output.relay,
                                physical_node_id,
                                direction: "sent",
                                branch_key: Some(branch_key),
                            },
                        );
                        (output.relay.clone(), metrics)
                    })
                    .collect();
                (identifier.clone(), outputs)
            })
            .collect();
        let domain_clock = runtime
            .bind_domain_clock(domain)
            .change_context(ProcessorTemplateError::BindDomainClock)?;
        Ok(Mutex::new(BranchRuntime {
            key,
            runtime: runtime.clone(),
            domain: domain.clone(),
            routing: runtime.domain_routing_cache(domain),
            routing_snapshot: None,
            domain_clock,
            source_kind: self.source_kind,
            source: self.source.clone(),
            root_relay: self.root_relay.clone(),
            relays,
            materialized_states,
            relay_state_epoch: None,
            processors,
            error_policies: self.error_policies.clone(),
            metrics: BranchRuntimeMetrics {
                source: source_metrics,
                source_input: source_input_metrics,
                processor_inputs,
                processor_outputs,
            },
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
        publish_state_identity(
            &runtime,
            &domain,
            ModelKind::Deduplicator,
            named::<ModelName>("deduplicate_events"),
        );
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
                .policy()
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
        let error = node
            .apply_node_template(incompatible)
            .expect_err("a keyspace change must not hot-refresh");
        assert_eq!(
            error.current_context().to_string(),
            "dynamic deduplicator update changed its state keyspace"
        );
    }

    #[test]
    fn processor_template_refresh_rejects_other_targets_topologies_and_kinds() {
        let runtime = Runtime::default();
        let domain = domain("default");
        publish_state_identity(
            &runtime,
            &domain,
            ModelKind::Deduplicator,
            named::<ModelName>("deduplicate_events"),
        );
        let input = named::<RelayName>("events");
        let template = RelayProcessorTemplate {
            kind: ModelKind::Deduplicator,
            processor: named("deduplicate_events"),
            input_relays: vec![input.clone()],
            input_collect_policies: [(
                input.clone(),
                RuntimeInputCollectPolicy {
                    interval: Duration::from_secs(1),
                    max_batch_size: Some(1024),
                },
            )]
            .into_iter()
            .collect(),
            error_policies: ErrorPolicies::handled_by_log(),
            from_where: HashMap::default(),
            filter_where: None,
            materialized_state: Vec::new(),
            operation: RelayProcessorOperationTemplate::Deduplicator {
                output_routes: RelayProcessorOutputsTemplate {
                    routes: vec![RelayProcessorOutputTemplate {
                        output_relay: named("unique_events"),
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
        let refusal = |node: &mut RelayProcessorNode, desired: RelayProcessorTemplate| {
            node.apply_node_template(desired)
                .expect_err("an incompatible template must not hot-refresh")
                .current_context()
                .to_string()
        };

        let mut other_target = template.clone();
        other_target.processor = named("other_events");
        assert_eq!(
            refusal(&mut node, other_target),
            "processor template targets deduplicator 'other_events', not deduplicator \
             'deduplicate_events'"
        );

        let mut other_input = template.clone();
        other_input.input_relays = vec![named("other_events")];
        assert_eq!(
            refusal(&mut node, other_input),
            "dynamic deduplicator update changed processor input topology"
        );

        let mut other_routes = template.clone();
        if let RelayProcessorOperationTemplate::Deduplicator { output_routes, .. } =
            &mut other_routes.operation
        {
            output_routes.routes.clear();
        }
        assert_eq!(
            refusal(&mut node, other_routes),
            "dynamic processor update changed its route topology"
        );

        let mut other_kind = template;
        other_kind.operation = RelayProcessorOperationTemplate::Junction {
            output_routes: RelayProcessorOutputsTemplate { routes: Vec::new() },
        };
        assert_eq!(
            refusal(&mut node, other_kind),
            "dynamic processor update changed its operation kind"
        );
    }
}
