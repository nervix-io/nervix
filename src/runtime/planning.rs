use error_stack::ResultExt as _;
use nervix_models::ModelName;
#[cfg(test)]
use nervix_models::{ProcessorInputWhere, ProcessorInputs};

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum WindowDurationSetting {
    Width,
    Step,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum FlushPolicyRequirement {
    Required,
    NotUsed,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(in crate::runtime) enum PlanningError {
    #[error("window processor '{node}' has an invalid {setting:?} duration")]
    InvalidWindowDuration {
        node: ModelName,
        setting: WindowDurationSetting,
    },
    #[error("{kind:?} '{node}' output route '{route}' must declare a flush policy")]
    MissingFlushPolicy {
        kind: ModelKind,
        node: ModelName,
        route: RelayName,
    },
    #[error("{kind:?} '{node}' output route '{route}' has an invalid flush interval")]
    InvalidFlushInterval {
        kind: ModelKind,
        node: ModelName,
        route: RelayName,
    },
    #[error("{kind:?} '{node}' output route '{route}' has an invalid maximum batch size")]
    InvalidFlushMaxBatchSize {
        kind: ModelKind,
        node: ModelName,
        route: RelayName,
    },
    #[error("{kind:?} '{node}' input relay '{relay}' has an invalid collection interval")]
    InvalidCollectInterval {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("{kind:?} '{node}' input relay '{relay}' has an invalid collection batch size")]
    InvalidCollectMaxBatchSize {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("{kind:?} '{node}' has an invalid maximum retention time")]
    InvalidMaxTime { kind: ModelKind, node: ModelName },
    #[error("window processor '{node}' must declare an output route")]
    MissingWindowOutput { node: ModelName },
    #[error("window processor '{node}' output route '{route}' has invalid construction")]
    InvalidWindowConstruction { node: ModelName, route: RelayName },
    #[error("window processor '{node}' output route '{route}' could not be compiled")]
    WindowOutputCompilation { node: ModelName, route: RelayName },
    #[error("{kind:?} '{node}' must declare an input relay")]
    MissingInputRelay { kind: ModelKind, node: ModelName },
    #[error("{kind:?} '{node}' input relay '{relay}' has no runtime schema")]
    MissingInputSchema {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("inferencer '{node}' input mappings could not be compiled for relay '{relay}'")]
    InferencerInputCompilation { node: ModelName, relay: RelayName },
    #[error("{kind:?} '{node}' has an invalid branch TTL")]
    InvalidBranchTtl { kind: ModelKind, node: ModelName },
    #[error("{kind:?} '{node}' output route '{route}' has no relay registry")]
    MissingRelayRegistry {
        kind: ModelKind,
        node: ModelName,
        route: RelayName,
    },
    #[error("{kind:?} '{node}' output route '{route}' has no relay services")]
    MissingRelayServices {
        kind: ModelKind,
        node: ModelName,
        route: RelayName,
    },
    #[error("failed to prepare the bound WASM module for processor '{node}'")]
    PrepareWasmProcessor { node: ModelName },
    #[error("{kind:?} '{node}' input relay '{relay}' has no resolved branching")]
    MissingInputBranching {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("{kind:?} '{node}' output relay '{relay}' has no runtime schema")]
    MissingOutputSchema {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("failed to bind prepared VM programs for {kind:?} '{node}'")]
    ProcessorProgramCompilation { kind: ModelKind, node: ModelName },
}

fn parse_optional_window_duration(
    processor: &ModelName,
    setting: WindowDurationSetting,
    value: Option<&str>,
) -> error_stack::Result<Option<Duration>, PlanningError> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let duration = humantime::parse_duration(raw).map_err(|error| {
        Report::new(PlanningError::InvalidWindowDuration {
            node: processor.clone(),
            setting,
        })
        .attach_printable(error)
    })?;
    Ok(Some(duration))
}

pub(in crate::runtime) fn materialize_output(
    kind: ModelKind,
    processor: &ModelName,
    output: &BranchedProcessorOutputSpec,
    requirement: FlushPolicyRequirement,
) -> error_stack::Result<RelayProcessorOutputTemplate, PlanningError> {
    let flush_policy = match output.flush_policy.as_ref() {
        Some(policy) => Some(parse_branch_flush_policy(
            kind,
            processor,
            &output.relay,
            policy,
        )?),
        None if requirement == FlushPolicyRequirement::Required => {
            return Err(Report::new(PlanningError::MissingFlushPolicy {
                kind,
                node: processor.clone(),
                route: output.relay.clone(),
            }));
        }
        None => None,
    };
    Ok(RelayProcessorOutputTemplate {
        output_relay: output.relay.clone(),
        construction: output.construction.clone(),
        flush_policy,
        message_error_policy: output.message_error_policy.clone(),
        compiled_program: None,
    })
}

fn materialize_outputs(
    kind: ModelKind,
    processor: &ModelName,
    outputs: &BranchedProcessorOutputsSpec,
    requirement: FlushPolicyRequirement,
) -> error_stack::Result<RelayProcessorOutputsTemplate, PlanningError> {
    let mut routes = Vec::with_capacity(outputs.routes.len());
    for output in &outputs.routes {
        routes.push(materialize_output(kind, processor, output, requirement)?);
    }
    Ok(RelayProcessorOutputsTemplate { routes })
}

pub(in crate::runtime) fn parse_branch_flush_policy(
    kind: ModelKind,
    processor: &ModelName,
    route: &RelayName,
    policy: &FlushPolicy,
) -> error_stack::Result<RuntimeFlushPolicy, PlanningError> {
    let FlushPolicy::Each {
        interval,
        max_batch_size,
    } = policy
    else {
        return Ok(RuntimeFlushPolicy::Immediate);
    };
    let parsed_interval = humantime::parse_duration(interval).map_err(|error| {
        Report::new(PlanningError::InvalidFlushInterval {
            kind,
            node: processor.clone(),
            route: route.clone(),
        })
        .attach_printable(error)
    })?;
    let parsed_max_batch_size = max_batch_size.parse::<ubyte::ByteUnit>().map_err(|error| {
        Report::new(PlanningError::InvalidFlushMaxBatchSize {
            kind,
            node: processor.clone(),
            route: route.clone(),
        })
        .attach_printable(error)
    })?;
    Ok(RuntimeFlushPolicy::Each {
        interval: parsed_interval,
        max_batch_size: parsed_max_batch_size.as_u64(),
    })
}

pub(in crate::runtime) fn parse_input_collect_policy(
    kind: ModelKind,
    processor: &ModelName,
    relay: &RelayName,
    policy: &nervix_models::InputCollectPolicy,
) -> error_stack::Result<RuntimeInputCollectPolicy, PlanningError> {
    let interval = humantime::parse_duration(&policy.collect_for).map_err(|error| {
        Report::new(PlanningError::InvalidCollectInterval {
            kind,
            node: processor.clone(),
            relay: relay.clone(),
        })
        .attach_printable(error)
    })?;
    let max_batch_size = if let Some(max_batch_size) = policy.max_batch_size.as_deref() {
        let size = max_batch_size.parse::<ubyte::ByteUnit>().map_err(|error| {
            Report::new(PlanningError::InvalidCollectMaxBatchSize {
                kind,
                node: processor.clone(),
                relay: relay.clone(),
            })
            .attach_printable(error)
        })?;
        Some(size.as_u64())
    } else {
        None
    };
    Ok(RuntimeInputCollectPolicy {
        interval,
        max_batch_size,
    })
}

fn parse_max_time(
    kind: ModelKind,
    processor: &ModelName,
    value: &str,
) -> error_stack::Result<Duration, PlanningError> {
    humantime::parse_duration(value).map_err(|error| {
        Report::new(PlanningError::InvalidMaxTime {
            kind,
            node: processor.clone(),
        })
        .attach_printable(error)
    })
}

fn materialize_nodes(
    nodes: &[BranchedProcessorSpec],
    relay_schemas: &HashMap<RelayName, Arc<CompiledSchema>>,
    udfs: Option<&UdfExecutor>,
) -> error_stack::Result<Vec<RelayProcessorTemplate>, PlanningError> {
    let mut out = Vec::new();
    for node in nodes {
        let mut input_collect_policies = HashMap::with_capacity(node.input_collect_policies.len());
        for (relay, policy) in &node.input_collect_policies {
            let parsed = parse_input_collect_policy(node.kind, &node.processor, relay, policy)?;
            input_collect_policies.insert(relay.clone(), parsed);
        }
        out.push(RelayProcessorTemplate {
            kind: node.kind,
            processor: node.processor.clone(),
            input_relays: node.input_relays.clone(),
            input_collect_policies,
            error_policies: node.error_policies.clone(),
            from_where: node.from_where.clone(),
            compiled_from_where: HashMap::default(),
            filter_where: node.filter_where.clone(),
            compiled_filter_where: HashMap::default(),
            materialized_state: node.materialized_state.clone(),
            operation: match &node.operation {
                BranchedProcessorOperationSpec::Deduplicator {
                    output_routes,
                    deduplicate_on,
                    max_time,
                } => RelayProcessorOperationTemplate::Deduplicator {
                    output_routes: materialize_outputs(
                        node.kind,
                        &node.processor,
                        output_routes,
                        FlushPolicyRequirement::Required,
                    )?,
                    deduplicate_on: deduplicate_on.clone(),
                    max_time: parse_max_time(node.kind, &node.processor, max_time)?,
                    compiled_key_program: None,
                },
                BranchedProcessorOperationSpec::WindowProcessor {
                    output_routes,
                    width,
                    step,
                    state_limit,
                } => {
                    if output_routes.outputs().next().is_none() {
                        return Err(Report::new(PlanningError::MissingWindowOutput {
                            node: node.processor.clone(),
                        }));
                    }
                    // Lower each written route's construction into its own aggregate program.
                    // Written route order is the order every later step counts in.
                    let mut route_aggregates = Vec::with_capacity(output_routes.routes.len());
                    for output in output_routes.outputs() {
                        let lowered =
                            lower_window_assignments(&output.construction).map_err(|reason| {
                                Report::new(PlanningError::InvalidWindowConstruction {
                                    node: node.processor.clone(),
                                    route: output.relay.clone(),
                                })
                                .attach_printable(reason)
                            })?;
                        route_aggregates.push(lowered.inner);
                    }

                    // Compile the routes in that same order. Each route's demands land in the
                    // shared accumulator plan after the demands of every route written before it,
                    // so a route's offset is the number of demands those routes already claimed.
                    let mut compiled_aggregates = Vec::with_capacity(route_aggregates.len());
                    let mut demand_offset = 0;
                    for (output, route_aggregate) in output_routes.outputs().zip(&route_aggregates)
                    {
                        let compiled = CompiledWindowAggregateProgram::compile(
                            route_aggregate,
                            &node.input_relays,
                            &output.relay,
                            relay_schemas,
                            udfs,
                        )
                        .map_err(|reason| {
                            reason.change_context(PlanningError::WindowOutputCompilation {
                                node: node.processor.clone(),
                                route: output.relay.clone(),
                            })
                        })?;
                        compiled_aggregates.push(compiled.with_demand_offset(demand_offset));
                        demand_offset += route_aggregate.demands().len();
                    }

                    // The shared accumulator plan the branch-local window state is built from,
                    // whose demands follow the same written route order as the offsets above.
                    let aggregate =
                        WindowAggregateProgram::combine_route_programs(&route_aggregates);

                    // Compilation is done, so the compiled programs now own the assignments and
                    // the materialized routes keep only their relay, flush, and error contracts.
                    let mut materialized_outputs = materialize_outputs(
                        node.kind,
                        &node.processor,
                        output_routes,
                        FlushPolicyRequirement::NotUsed,
                    )?;
                    for output in &mut materialized_outputs.routes {
                        output.construction.assignments.clear();
                    }

                    let width_messages = width.messages.map(|messages| messages.arch_into());
                    let step_messages = step.messages.map(|messages| messages.arch_into());
                    let width_duration = parse_optional_window_duration(
                        &node.processor,
                        WindowDurationSetting::Width,
                        width.duration.as_deref(),
                    )?;
                    let step_duration = parse_optional_window_duration(
                        &node.processor,
                        WindowDurationSetting::Step,
                        step.duration.as_deref(),
                    )?;
                    let sketch_layout = match (width_duration, step_duration) {
                        (Some(width), Some(step))
                            if aggregate
                                .demands()
                                .iter()
                                .any(|demand| demand.sketch.is_some()) =>
                        {
                            WindowPaneLayout::for_width_and_step(width, step)
                        }
                        _ => None,
                    };
                    let max_state_bytes = match state_limit {
                        nervix_models::WindowStateLimit::Unbounded => None,
                        nervix_models::WindowStateLimit::MaxBytes(bytes) => Some(*bytes),
                    };
                    let plan = WindowAccumulatorPlan::new(
                        compiled_aggregates.iter().map(|compiled| &compiled.route),
                        sketch_layout,
                        max_state_bytes,
                    );

                    RelayProcessorOperationTemplate::WindowProcessor {
                        output_routes: materialized_outputs,
                        width_messages,
                        step_messages,
                        width_duration,
                        step_duration,
                        aggregate,
                        plan,
                        compiled_aggregates,
                    }
                }
                BranchedProcessorOperationSpec::Reorderer {
                    output_routes,
                    order_by,
                    max_time,
                } => RelayProcessorOperationTemplate::Reorderer {
                    output_routes: materialize_outputs(
                        node.kind,
                        &node.processor,
                        output_routes,
                        FlushPolicyRequirement::Required,
                    )?,
                    order_by: order_by.clone(),
                    max_time: parse_max_time(node.kind, &node.processor, max_time)?,
                    compiled_program: None,
                },
                BranchedProcessorOperationSpec::Correlator {
                    output_routes,
                    left_relays,
                    right_relays,
                    correlate_where,
                    match_policy,
                    max_time,
                    timeout_policy,
                } => RelayProcessorOperationTemplate::Correlator {
                    output_routes: materialize_outputs(
                        node.kind,
                        &node.processor,
                        output_routes,
                        FlushPolicyRequirement::Required,
                    )?,
                    left_relays: left_relays.clone(),
                    right_relays: right_relays.clone(),
                    correlate_where: correlate_where.clone(),
                    match_policy: *match_policy,
                    max_time: parse_max_time(node.kind, &node.processor, max_time)?,
                    timeout_policy: timeout_policy.clone(),
                    compiled_where_program: None,
                    compiled_output_programs: (0..output_routes.routes.len())
                        .map(|_| None)
                        .collect(),
                },
                BranchedProcessorOperationSpec::Junction { output_routes } => {
                    RelayProcessorOperationTemplate::Junction {
                        output_routes: materialize_outputs(
                            node.kind,
                            &node.processor,
                            output_routes,
                            FlushPolicyRequirement::Required,
                        )?,
                    }
                }
                BranchedProcessorOperationSpec::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                } => {
                    let Some(input_relay) = node.input_relays.first() else {
                        return Err(Report::new(PlanningError::MissingInputRelay {
                            kind: node.kind,
                            node: node.processor.clone(),
                        }));
                    };
                    let Some(input_schema) = relay_schemas.get(input_relay) else {
                        return Err(Report::new(PlanningError::MissingInputSchema {
                            kind: node.kind,
                            node: node.processor.clone(),
                            relay: input_relay.clone(),
                        }));
                    };
                    let compiled_input_program = CompiledInferencerInputProgram::compile(
                        &node.processor,
                        inputs,
                        input_schema,
                        udfs,
                    )
                    .map_err(|reason| {
                        reason.change_context(PlanningError::InferencerInputCompilation {
                            node: node.processor.clone(),
                            relay: input_relay.clone(),
                        })
                    })?;
                    RelayProcessorOperationTemplate::Inferencer {
                        output_routes: materialize_outputs(
                            node.kind,
                            &node.processor,
                            output_routes,
                            FlushPolicyRequirement::Required,
                        )?,
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
                        inputs: inputs.clone(),
                        output_schema: output_schema.clone(),
                        compiled_input_program,
                    }
                }
                BranchedProcessorOperationSpec::WasmProcessor {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    limits,
                    rejected_state_policy,
                } => RelayProcessorOperationTemplate::WasmProcessor {
                    output_routes: materialize_outputs(
                        node.kind,
                        &node.processor,
                        output_routes,
                        FlushPolicyRequirement::NotUsed,
                    )?,
                    resource: resource.clone(),
                    resource_version: *resource_version,
                    file: file.clone(),
                    limits: *limits,
                    rejected_state_policy: *rejected_state_policy,
                    compiled: None,
                },
            },
        });
    }
    Ok(out)
}

fn parse_branch_ttl_setting(
    ttl: Option<&str>,
    kind: ModelKind,
    identifier: &ModelName,
) -> error_stack::Result<Option<Duration>, PlanningError> {
    let Some(ttl) = ttl else {
        return Ok(None);
    };
    let duration = humantime::parse_duration(ttl).map_err(|error| {
        Report::new(PlanningError::InvalidBranchTtl {
            kind,
            node: identifier.clone(),
        })
        .attach_printable(error)
    })?;
    Ok(Some(duration))
}

fn resolve_branch_relay_templates(
    kind: ModelKind,
    node: &ModelName,
    branch_relay_ids: HashSet<RelayName>,
    relay_registries: &HashMap<RelayName, RelayRegistry>,
    relay_services: &HashMap<RelayName, Arc<RelayBoundaryServices>>,
) -> error_stack::Result<HashMap<RelayName, RelayProcessorRelayTemplate>, PlanningError> {
    let mut templates = HashMap::with_capacity(branch_relay_ids.len());
    for relay in branch_relay_ids {
        let Some(registry) = relay_registries.get(&relay).cloned() else {
            return Err(Report::new(PlanningError::MissingRelayRegistry {
                kind,
                node: node.clone(),
                route: relay,
            }));
        };
        let Some(services) = relay_services.get(&relay).cloned() else {
            return Err(Report::new(PlanningError::MissingRelayServices {
                kind,
                node: node.clone(),
                route: relay,
            }));
        };
        templates.insert(relay, RelayProcessorRelayTemplate { registry, services });
    }
    Ok(templates)
}

/// Binds the branched entrypoint one planned ingestor or reingestor route feeds to the relay
/// registry and services its records publish through.
/// The template of the branched entrypoint one planned route of the ingestor or reingestor `kind`
/// `identifier` feeds.
pub(in crate::runtime) fn materialize_ingestor_route_template(
    kind: ModelKind,
    identifier: &ModelName,
    route: &PlannedEntryRoute,
    relay_registries: &HashMap<RelayName, RelayRegistry>,
    relay_services: &HashMap<RelayName, Arc<RelayBoundaryServices>>,
) -> error_stack::Result<IngestorRouteTemplate, PlanningError> {
    let Some(registry) = relay_registries.get(&route.relay).cloned() else {
        return Err(Report::new(PlanningError::MissingRelayRegistry {
            kind,
            node: identifier.clone(),
            route: route.relay.clone(),
        }));
    };
    let Some(services) = relay_services.get(&route.relay).cloned() else {
        return Err(Report::new(PlanningError::MissingRelayServices {
            kind,
            node: identifier.clone(),
            route: route.relay.clone(),
        }));
    };
    let mut relays = HashMap::default();
    relays.insert(
        route.relay.clone(),
        RelayProcessorRelayTemplate { registry, services },
    );
    let flush_policy =
        parse_branch_flush_policy(kind, identifier, &route.relay, &route.flush_policy)?;
    let mut branch = BranchInstanceTemplate {
        revision: ProcessorPlanRevision::new(),
        source_kind: kind,
        source: RelayName::from(identifier),
        root_relay: route.relay.clone(),
        branch: None,
        branch_ttl: None,
        branch_max_instances: None,
        error_policies: route.error_policies.clone(),
        relays,
        processors: HashMap::default(),
        wasm_state_reset: None,
    };
    if let Some(retention) = route.branch.retention() {
        branch.branch = Some(retention.branch.clone());
        branch.branch_ttl = Some(retention.ttl);
        branch.branch_max_instances = retention.max_instances;
    }
    Ok(IngestorRouteTemplate {
        branch,
        ack_boundary: route.ack_boundary,
        flush_policy,
    })
}

pub(in crate::runtime) fn materialize_processor_instance_template(
    node: &BranchedProcessorNodeSpec,
    relay_schemas: &HashMap<RelayName, Arc<CompiledSchema>>,
    relay_registries: &HashMap<RelayName, RelayRegistry>,
    relay_services: &HashMap<RelayName, Arc<RelayBoundaryServices>>,
    udfs: Option<&UdfExecutor>,
) -> error_stack::Result<BranchInstanceTemplate, PlanningError> {
    let spec = &node.spec;
    let Some(root_relay) = spec.input_relays.first().cloned() else {
        return Err(Report::new(PlanningError::MissingInputRelay {
            kind: spec.kind,
            node: spec.processor.clone(),
        }));
    };
    let relays = resolve_branch_relay_templates(
        spec.kind,
        &spec.processor,
        spec.output_relays(),
        relay_registries,
        relay_services,
    )?;
    let template = materialize_nodes(std::slice::from_ref(spec), relay_schemas, udfs)?
        .pop()
        .verified("materialize_nodes answers one template per spec and this call passes one spec");
    let mut processors = HashMap::default();
    processors.insert(spec.processor.clone(), template);
    Ok(BranchInstanceTemplate {
        revision: ProcessorPlanRevision::new(),
        source_kind: spec.kind,
        source: RelayName::from(&spec.processor),
        root_relay,
        branch: node.branch.clone(),
        branch_ttl: parse_branch_ttl_setting(
            node.branch_ttl.as_deref(),
            spec.kind,
            &spec.processor,
        )?,
        branch_max_instances: node.branch_max_instances.map(addressable_count),
        error_policies: spec.error_policies.clone(),
        relays,
        processors,
        wasm_state_reset: node.wasm_state_reset.clone(),
    })
}

fn bind_processor_template_programs(
    domain: &DomainName,
    template: &mut BranchInstanceTemplate,
    relay_schemas: &HashMap<RelayName, Arc<CompiledSchema>>,
    relay_branchings: &HashMap<RelayName, ResolvedBranching>,
    materialized_stream_specs: &HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    lookups: &HashMap<LookupName, Arc<LookupRuntime>>,
    udfs: Option<&UdfExecutor>,
) -> error_stack::Result<(), PlanningError> {
    for processor in template.processors.values_mut() {
        let compilation_error = || PlanningError::ProcessorProgramCompilation {
            kind: processor.kind,
            node: processor.processor.clone(),
        };
        let primary_input = processor.input_relays.first().ok_or_else(|| {
            Report::new(PlanningError::MissingInputRelay {
                kind: processor.kind,
                node: processor.processor.clone(),
            })
        })?;
        let primary_schema = relay_schemas.get(primary_input).ok_or_else(|| {
            Report::new(PlanningError::MissingInputSchema {
                kind: processor.kind,
                node: processor.processor.clone(),
                relay: primary_input.clone(),
            })
        })?;
        let primary_branching = relay_branchings.get(primary_input).ok_or_else(|| {
            Report::new(PlanningError::MissingInputBranching {
                kind: processor.kind,
                node: processor.processor.clone(),
                relay: primary_input.clone(),
            })
        })?;

        processor.compiled_from_where.clear();
        processor.compiled_filter_where.clear();
        for input_relay in &processor.input_relays {
            let input_schema = relay_schemas.get(input_relay).ok_or_else(|| {
                Report::new(PlanningError::MissingInputSchema {
                    kind: processor.kind,
                    node: processor.processor.clone(),
                    relay: input_relay.clone(),
                })
            })?;
            let input_branching = relay_branchings.get(input_relay).ok_or_else(|| {
                Report::new(PlanningError::MissingInputBranching {
                    kind: processor.kind,
                    node: processor.processor.clone(),
                    relay: input_relay.clone(),
                })
            })?;
            let source_scope = match &processor.operation {
                RelayProcessorOperationTemplate::Correlator {
                    left_relays,
                    right_relays,
                    ..
                } if left_relays.contains(input_relay) => RuntimeFilterScope::Source {
                    namespace: "left",
                    allow_header_reads: false,
                    allow_metadata: false,
                },
                RelayProcessorOperationTemplate::Correlator { right_relays, .. }
                    if right_relays.contains(input_relay) =>
                {
                    RuntimeFilterScope::Source {
                        namespace: "right",
                        allow_header_reads: false,
                        allow_metadata: false,
                    }
                }
                _ => RuntimeFilterScope::Source {
                    namespace: "input",
                    allow_header_reads: false,
                    allow_metadata: false,
                },
            };
            let context = || RuntimeVmCompileContext {
                available_materialized_streams: materialized_stream_specs,
                available_lookups: lookups,
                current_branching: input_branching,
                udfs,
            };
            if let Some(expression) = processor.from_where.get(input_relay)
                && let Some(program) = compile_scoped_filter_program(
                    RuntimeCompileTarget {
                        domain,
                        identifier: &processor.processor,
                    },
                    Some(expression),
                    RuntimeVmSchema {
                        schema: input_schema.arrow_schema(),
                        sensitivity: input_schema.vm_sensitivity(),
                    },
                    MessageErrorOperation::SourceWhere,
                    context(),
                    source_scope,
                )
                .change_context_lazy(compilation_error)?
            {
                processor
                    .compiled_from_where
                    .insert(input_relay.clone(), program);
            }
            if let Some(expression) = processor.filter_where.as_ref()
                && let Some(program) = compile_scoped_filter_program(
                    RuntimeCompileTarget {
                        domain,
                        identifier: &processor.processor,
                    },
                    Some(expression),
                    RuntimeVmSchema {
                        schema: input_schema.arrow_schema(),
                        sensitivity: input_schema.vm_sensitivity(),
                    },
                    MessageErrorOperation::FilterWhere,
                    context(),
                    RuntimeFilterScope::Source {
                        namespace: "input",
                        allow_header_reads: false,
                        allow_metadata: false,
                    },
                )
                .change_context_lazy(compilation_error)?
            {
                processor
                    .compiled_filter_where
                    .insert(input_relay.clone(), program);
            }
        }

        let output_context = || RuntimeVmCompileContext {
            available_materialized_streams: materialized_stream_specs,
            available_lookups: lookups,
            current_branching: primary_branching,
            udfs,
        };
        match &mut processor.operation {
            RelayProcessorOperationTemplate::Deduplicator {
                output_routes,
                deduplicate_on,
                compiled_key_program,
                ..
            } => {
                *compiled_key_program = Some(
                    compile_deduplicator_key_program(
                        &processor.processor,
                        &processor.input_relays,
                        deduplicate_on,
                        primary_schema.arrow_schema(),
                        udfs,
                    )
                    .change_context_lazy(compilation_error)?,
                );
                bind_transforming_output_programs(
                    TransformingOutputProgramBinding {
                        domain,
                        kind: processor.kind,
                        processor: &processor.processor,
                        input_relays: &processor.input_relays,
                        input_schema: primary_schema,
                        relay_schemas,
                    },
                    output_routes,
                    output_context,
                    None,
                )?;
            }
            RelayProcessorOperationTemplate::Reorderer {
                output_routes,
                order_by,
                compiled_program,
                ..
            } => {
                *compiled_program = Some(
                    compile_reorderer_program(
                        &processor.processor,
                        &processor.input_relays,
                        order_by,
                        primary_schema.arrow_schema(),
                        udfs,
                    )
                    .change_context_lazy(compilation_error)?,
                );
                bind_transforming_output_programs(
                    TransformingOutputProgramBinding {
                        domain,
                        kind: processor.kind,
                        processor: &processor.processor,
                        input_relays: &processor.input_relays,
                        input_schema: primary_schema,
                        relay_schemas,
                    },
                    output_routes,
                    output_context,
                    None,
                )?;
            }
            RelayProcessorOperationTemplate::Junction { output_routes } => {
                bind_transforming_output_programs(
                    TransformingOutputProgramBinding {
                        domain,
                        kind: processor.kind,
                        processor: &processor.processor,
                        input_relays: &processor.input_relays,
                        input_schema: primary_schema,
                        relay_schemas,
                    },
                    output_routes,
                    output_context,
                    None,
                )?;
            }
            RelayProcessorOperationTemplate::WindowProcessor { output_routes, .. } => {
                for output in &mut output_routes.routes {
                    let output_schema =
                        relay_schemas.get(&output.output_relay).ok_or_else(|| {
                            Report::new(PlanningError::MissingOutputSchema {
                                kind: processor.kind,
                                node: processor.processor.clone(),
                                relay: output.output_relay.clone(),
                            })
                        })?;
                    output.compiled_program = compile_finalized_output_filter_program(
                        domain,
                        &processor.processor,
                        output.construction.where_clause.as_ref(),
                        output_schema.arrow_schema(),
                        output_schema.vm_sensitivity(),
                        output_context(),
                    )
                    .change_context_lazy(compilation_error)?;
                }
            }
            RelayProcessorOperationTemplate::Inferencer {
                output_routes,
                output_schema,
                ..
            } => {
                let tensors = InferencerFilterMapTensors { output_schema };
                bind_transforming_output_programs(
                    TransformingOutputProgramBinding {
                        domain,
                        kind: processor.kind,
                        processor: &processor.processor,
                        input_relays: &processor.input_relays,
                        input_schema: primary_schema,
                        relay_schemas,
                    },
                    output_routes,
                    output_context,
                    Some(tensors),
                )?;
            }
            RelayProcessorOperationTemplate::WasmProcessor { output_routes, .. } => {
                for output in &mut output_routes.routes {
                    let output_schema =
                        relay_schemas.get(&output.output_relay).ok_or_else(|| {
                            Report::new(PlanningError::MissingOutputSchema {
                                kind: processor.kind,
                                node: processor.processor.clone(),
                                relay: output.output_relay.clone(),
                            })
                        })?;
                    output.compiled_program = compile_wasm_output_filter_map_program(
                        domain,
                        &processor.processor,
                        &output.construction,
                        output_schema.arrow_schema(),
                        output_schema.vm_sensitivity(),
                        output_context(),
                    )
                    .map_err(|error| Report::new(compilation_error()).attach_printable(error))?;
                }
            }
            RelayProcessorOperationTemplate::Correlator {
                output_routes,
                left_relays,
                right_relays,
                correlate_where,
                compiled_where_program,
                compiled_output_programs,
                ..
            } => {
                let left_relay = left_relays.first().ok_or_else(|| {
                    Report::new(PlanningError::MissingInputRelay {
                        kind: processor.kind,
                        node: processor.processor.clone(),
                    })
                })?;
                let right_relay = right_relays.first().ok_or_else(|| {
                    Report::new(PlanningError::MissingInputRelay {
                        kind: processor.kind,
                        node: processor.processor.clone(),
                    })
                })?;
                let left_schema = relay_schemas.get(left_relay).ok_or_else(|| {
                    Report::new(PlanningError::MissingInputSchema {
                        kind: processor.kind,
                        node: processor.processor.clone(),
                        relay: left_relay.clone(),
                    })
                })?;
                let right_schema = relay_schemas.get(right_relay).ok_or_else(|| {
                    Report::new(PlanningError::MissingInputSchema {
                        kind: processor.kind,
                        node: processor.processor.clone(),
                        relay: right_relay.clone(),
                    })
                })?;
                *compiled_where_program = Some(
                    compile_correlator_where_program(
                        &processor.processor,
                        correlate_where,
                        left_relays,
                        left_schema.arrow_schema(),
                        right_relays,
                        right_schema.arrow_schema(),
                        udfs,
                    )
                    .change_context_lazy(compilation_error)?,
                );
                compiled_output_programs.clear();
                for output in &output_routes.routes {
                    let output_schema =
                        relay_schemas.get(&output.output_relay).ok_or_else(|| {
                            Report::new(PlanningError::MissingOutputSchema {
                                kind: processor.kind,
                                node: processor.processor.clone(),
                                relay: output.output_relay.clone(),
                            })
                        })?;
                    let program = CorrelatorOutputCompileContext {
                        processor: &processor.processor,
                        left_schema: left_schema.arrow_schema(),
                        left_sensitivity: left_schema.vm_sensitivity(),
                        right_schema: right_schema.arrow_schema(),
                        right_sensitivity: right_schema.vm_sensitivity(),
                        output_relay: &output.output_relay,
                        output_schema: output_schema.arrow_schema(),
                        output_sensitivity: output_schema.vm_sensitivity(),
                        construction: &output.construction,
                        runtime: output_context(),
                    }
                    .compile()
                    .change_context_lazy(compilation_error)?;
                    compiled_output_programs.push(Some(program));
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct TransformingOutputProgramBinding<'a> {
    domain: &'a DomainName,
    kind: ModelKind,
    processor: &'a ModelName,
    input_relays: &'a [RelayName],
    input_schema: &'a CompiledSchema,
    relay_schemas: &'a HashMap<RelayName, Arc<CompiledSchema>>,
}

fn bind_transforming_output_programs<'a>(
    binding: TransformingOutputProgramBinding<'_>,
    outputs: &mut RelayProcessorOutputsTemplate,
    context: impl Fn() -> RuntimeVmCompileContext<'a>,
    inferencer_tensors: Option<InferencerFilterMapTensors<'_>>,
) -> error_stack::Result<(), PlanningError> {
    for output in &mut outputs.routes {
        let output_schema = binding
            .relay_schemas
            .get(&output.output_relay)
            .ok_or_else(|| {
                Report::new(PlanningError::MissingOutputSchema {
                    kind: binding.kind,
                    node: binding.processor.clone(),
                    relay: output.output_relay.clone(),
                })
            })?;
        let carrier_schema = match inferencer_tensors {
            Some(tensors) => tensors.output_arrow_schema(),
            None => binding.input_schema.arrow_schema(),
        };
        output.compiled_program = compile_processor_output_filter_map_program(
            RuntimeCompileTarget {
                domain: binding.domain,
                identifier: binding.processor,
            },
            binding.input_relays,
            &output.output_relay,
            &output.construction,
            RuntimeVmSchemaPair {
                input: carrier_schema,
                input_sensitivity: binding.input_schema.vm_sensitivity(),
                output: output_schema.arrow_schema(),
                output_sensitivity: output_schema.vm_sensitivity(),
            },
            inferencer_tensors,
            context(),
        )
        .map_err(|error| {
            Report::new(PlanningError::ProcessorProgramCompilation {
                kind: binding.kind,
                node: binding.processor.clone(),
            })
            .attach_printable(error)
        })?;
    }
    Ok(())
}

/// Inputs required to bind one complete node-local processor-plan revision.
pub(in crate::runtime) struct ProcessorPlanBindingContext<'a> {
    pub runtime: &'a Runtime,
    pub domain: &'a DomainName,
    pub relay_schemas: &'a HashMap<RelayName, Arc<CompiledSchema>>,
    pub relay_registries: &'a HashMap<RelayName, RelayRegistry>,
    pub relay_services: &'a HashMap<RelayName, Arc<RelayBoundaryServices>>,
    pub relay_branchings: &'a HashMap<RelayName, ResolvedBranching>,
    pub materialized_stream_specs: &'a HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    pub lookups: &'a HashMap<LookupName, Arc<LookupRuntime>>,
    pub udfs: Option<&'a UdfExecutor>,
    pub previous: &'a HashMap<NodeRef, StdArc<PublishedProcessorPlan>>,
}

/// Binds the node-local artifacts for every locally installed processor before their complete map
/// is published. An unchanged specification reuses the exact published allocation, including all
/// prepared VM and WASM artifacts it owns.
pub(in crate::runtime) async fn bind_published_processor_plans(
    specs: &[BranchedProcessorNodeSpec],
    context: ProcessorPlanBindingContext<'_>,
) -> error_stack::Result<HashMap<NodeRef, StdArc<PublishedProcessorPlan>>, PlanningError> {
    let ProcessorPlanBindingContext {
        runtime,
        domain,
        relay_schemas,
        relay_registries,
        relay_services,
        relay_branchings,
        materialized_stream_specs,
        lookups,
        udfs,
        previous,
    } = context;
    let mut plans = HashMap::with_capacity(specs.len());
    for spec in specs {
        tokio::task::consume_budget().await;
        let node = NodeRef::new(spec.spec.kind, spec.spec.processor.clone());
        if let Some(published) = previous.get(&node)
            && spec.reuses_prepared_revision(Some(&published.source))
        {
            plans.insert(node, published.clone());
            continue;
        }

        let mut template = materialize_processor_instance_template(
            spec,
            relay_schemas,
            relay_registries,
            relay_services,
            udfs,
        )?;
        bind_processor_template_programs(
            domain,
            &mut template,
            relay_schemas,
            relay_branchings,
            materialized_stream_specs,
            lookups,
            udfs,
        )?;
        template
            .prepare_wasm_processors(runtime, domain)
            .await
            .change_context_lazy(|| PlanningError::PrepareWasmProcessor {
                node: spec.spec.processor.clone(),
            })?;
        plans.insert(
            node,
            StdArc::new(PublishedProcessorPlan {
                source: spec.clone(),
                template: StdArc::new(template),
            }),
        );
    }
    Ok(plans)
}

#[cfg(test)]
#[path = "planning_tests.rs"]
mod tests;
