use std::num::NonZeroU64;

use nervix_models::{
    BranchName, CreateBranch, CreateRelay, ModelIndex, ModelName, ProcessorInputWhere,
    ProcessorInputs, ProcessorOutput as ModelProcessorOutput,
    ProcessorOutputs as ModelProcessorOutputs,
};

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
    #[error("{kind:?} '{node}' has no scheduled processor specification")]
    MissingProcessorSpecification { kind: ModelKind, node: ModelName },
    #[error("{kind:?} '{node}' did not produce a processor template")]
    MissingProcessorTemplate { kind: ModelKind, node: ModelName },
    #[error("{kind:?} '{node}' has an invalid branch TTL")]
    InvalidBranchTtl { kind: ModelKind, node: ModelName },
    #[error("{kind:?} '{node}' output route '{route}' has no configured relay")]
    MissingRelayModel {
        kind: ModelKind,
        node: ModelName,
        route: RelayName,
    },
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
}

fn branched_output(output: &ModelProcessorOutput) -> BranchedProcessorOutputSpec {
    BranchedProcessorOutputSpec {
        relay: output.relay.clone(),
        construction: output.construction.clone(),
        flush_policy: output.flush_policy.clone(),
        message_error_policy: output.message_error_policy.clone(),
    }
}

fn branched_outputs(outputs: &ModelProcessorOutputs) -> BranchedProcessorOutputsSpec {
    BranchedProcessorOutputsSpec {
        routes: outputs.routes.iter().map(branched_output).collect(),
    }
}

pub(in crate::runtime) fn processor_input_where_by_relay(
    from_where: &[ProcessorInputWhere],
) -> HashMap<RelayName, nervix_models::Expression> {
    from_where
        .iter()
        .map(|source_filter| {
            (
                source_filter.relay.clone(),
                source_filter.where_clause.clone(),
            )
        })
        .collect()
}

fn processor_input_where_by_inputs(
    inputs: &ProcessorInputs,
) -> HashMap<RelayName, nervix_models::Expression> {
    processor_input_where_by_relay(inputs.where_clauses())
}

fn processor_input_collect_policies(
    inputs: &ProcessorInputs,
) -> HashMap<RelayName, nervix_models::InputCollectPolicy> {
    let Some(policy) = inputs.collect_policy.as_ref() else {
        return HashMap::default();
    };
    inputs
        .relays()
        .iter()
        .cloned()
        .map(|relay| (relay, policy.clone()))
        .collect()
}

/// The branch a node runs in together with the retention the branch declares. An unbranched node
/// carries none of the three, which is how absent branch identity is represented.
struct BranchPolicy {
    branch: Option<BranchName>,
    ttl: Option<String>,
    max_instances: Option<NonZeroU64>,
}

fn branch_policy(
    branch_ref: Option<&BranchName>,
    branches: &HashMap<BranchName, CreateBranch>,
) -> BranchPolicy {
    let Some(branch_ref) = branch_ref else {
        return BranchPolicy {
            branch: None,
            ttl: None,
            max_instances: None,
        };
    };
    let branch = branches.get(branch_ref).verified(
        "the registry resolved every branch reference before the schedule reached planning",
    );
    BranchPolicy {
        branch: Some(branch_ref.clone()),
        ttl: Some(branch.ttl.clone()),
        max_instances: branch
            .eviction
            .as_ref()
            .map(|eviction| eviction.max_instances()),
    }
}

fn processor_node_spec(
    spec: BranchedProcessorSpec,
    branched_by: &nervix_models::BranchSelection,
    branches: &HashMap<BranchName, CreateBranch>,
) -> BranchedProcessorNodeSpec {
    let policy = branch_policy(branched_by.branch(), branches);
    BranchedProcessorNodeSpec {
        spec,
        branch: policy.branch,
        branch_ttl: policy.ttl,
        branch_max_instances: policy.max_instances,
        wasm_state_reset: None,
    }
}

/// One model the planner turns into node specs, named the way the registry registered it.
pub(in crate::runtime) struct PlannedModel {
    pub(in crate::runtime) kind: ModelKind,
    pub(in crate::runtime) identifier: ModelName,
    pub(in crate::runtime) model: Model,
}

pub(in crate::runtime) fn branched_node_specs_from_scheduled_nodes(
    nodes: &ScheduledNodes,
) -> BranchedNodeSpecs {
    let resets = nodes
        .values()
        .filter_map(|node| {
            node.wasm_state_reset()
                .cloned()
                .map(|reset| (node.identifier.clone(), reset))
        })
        .collect::<HashMap<_, _>>();
    let mut specs = branched_node_specs_from_models(nodes.values().map(|node| PlannedModel {
        kind: node.kind(),
        identifier: node.identifier.clone(),
        model: (*node.config).clone(),
    }));
    for processor in &mut specs.processors {
        processor.wasm_state_reset = resets.get(&processor.spec.processor).cloned();
    }
    specs
}

pub(in crate::runtime) fn branched_node_specs_from_active_graph(
    graph: &ActiveGraph,
) -> BranchedNodeSpecs {
    branched_node_specs_from_models(graph.nodes().into_iter().map(|node| PlannedModel {
        kind: node.kind,
        identifier: node.identifier,
        model: (*node.config).clone(),
    }))
}

pub(in crate::runtime) fn branched_node_specs_from_models(
    nodes: impl Iterator<Item = PlannedModel>,
) -> BranchedNodeSpecs {
    let nodes = nodes.collect::<Vec<_>>();
    let branches = nodes
        .iter()
        .filter_map(|planned| {
            if let Model::Branch(branch) = &planned.model {
                Some((branch.name.clone(), branch.clone()))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();
    let mut processors = Vec::new();
    let mut entrypoints = Vec::new();

    for PlannedModel {
        kind,
        identifier,
        model,
    } in nodes
    {
        match &model {
            Model::Deduplicator(deduplicator) => {
                if deduplicator.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: deduplicator.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&deduplicator.from),
                    mode: deduplicator.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&deduplicator.from),
                    filter_where: deduplicator.filter_where.clone(),
                    materialized_state: deduplicator.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Deduplicator {
                        output_routes: branched_outputs(&deduplicator.output_routes),
                        deduplicate_on: deduplicator.deduplicate_on.clone(),
                        max_time: deduplicator.max_time.clone(),
                    },
                };
                processors.push(processor_node_spec(
                    spec,
                    &deduplicator.branched_by,
                    &branches,
                ));
            }
            Model::Reorderer(reorderer) => {
                if reorderer.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: reorderer.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&reorderer.from),
                    mode: reorderer.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&reorderer.from),
                    filter_where: reorderer.filter_where.clone(),
                    materialized_state: reorderer.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Reorderer {
                        output_routes: branched_outputs(&reorderer.output_routes),
                        order_by: reorderer.order_by.clone(),
                        max_time: reorderer.max_time.clone(),
                    },
                };
                processors.push(processor_node_spec(spec, &reorderer.branched_by, &branches));
            }
            Model::Correlator(correlator) => {
                let mut input_relays = Vec::with_capacity(
                    correlator.left.relays().len() + correlator.right.relays().len(),
                );
                input_relays.extend(correlator.left.relays().iter().cloned());
                input_relays.extend(correlator.right.relays().iter().cloned());
                let mut from_where = processor_input_where_by_inputs(&correlator.left);
                from_where.extend(processor_input_where_by_inputs(&correlator.right));
                let mut input_collect_policies = processor_input_collect_policies(&correlator.left);
                input_collect_policies.extend(processor_input_collect_policies(&correlator.right));
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays,
                    input_collect_policies,
                    mode: correlator.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where,
                    filter_where: correlator.filter_where.clone(),
                    materialized_state: correlator.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Correlator {
                        output_routes: branched_outputs(&correlator.output_routes),
                        left_relays: correlator.left.relays().to_vec(),
                        right_relays: correlator.right.relays().to_vec(),
                        correlate_where: correlator.correlate_where.clone(),
                        match_policy: correlator.match_policy,
                        max_time: correlator.max_time.clone(),
                        timeout_policy: correlator.timeout_policy.clone(),
                    },
                };
                processors.push(processor_node_spec(
                    spec,
                    &correlator.branched_by,
                    &branches,
                ));
            }
            Model::WindowProcessor(window_processor) => {
                if window_processor.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: window_processor.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(
                        &window_processor.from,
                    ),
                    mode: window_processor.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&window_processor.from),
                    filter_where: window_processor.filter_where.clone(),
                    materialized_state: window_processor.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::WindowProcessor {
                        output_routes: branched_outputs(&window_processor.output_routes),
                        width: window_processor.width.clone(),
                        step: window_processor.step.clone(),
                        state_limit: window_processor.state_limit,
                    },
                };
                processors.push(processor_node_spec(
                    spec,
                    &window_processor.branched_by,
                    &branches,
                ));
            }
            Model::Junction(junction) => {
                if junction.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: junction.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&junction.from),
                    mode: junction.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&junction.from),
                    filter_where: junction.filter_where.clone(),
                    materialized_state: junction.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Junction {
                        output_routes: branched_outputs(&junction.output_routes),
                    },
                };
                processors.push(processor_node_spec(spec, &junction.branched_by, &branches));
            }
            Model::Inferencer(inferencer) => {
                if inferencer.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: inferencer.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&inferencer.from),
                    mode: inferencer.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&inferencer.from),
                    filter_where: inferencer.filter_where.clone(),
                    materialized_state: inferencer.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Inferencer {
                        output_routes: branched_outputs(&inferencer.output_routes),
                        resource: inferencer.resource.clone(),
                        resource_version: inferencer.resource_version,
                        file: inferencer.file.clone(),
                        inputs: inferencer.inputs.clone(),
                        output_schema: inferencer.output_schema.clone(),
                    },
                };
                processors.push(processor_node_spec(
                    spec,
                    &inferencer.branched_by,
                    &branches,
                ));
            }
            Model::WasmProcessor(processor) => {
                if processor.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: processor.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&processor.from),
                    mode: processor.mode,
                    error_policies: internal_processor_error_policies(
                        processor.global_error_policy.clone(),
                    ),
                    from_where: processor_input_where_by_inputs(&processor.from),
                    filter_where: processor.filter_where.clone(),
                    materialized_state: processor.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::WasmProcessor {
                        output_routes: branched_outputs(&processor.output_routes),
                        resource: processor.resource.clone(),
                        resource_version: processor.resource_version,
                        file: processor.file.clone(),
                        limits: processor.limits,
                        rejected_state_policy: processor.rejected_state_policy,
                    },
                };
                processors.push(processor_node_spec(spec, &processor.branched_by, &branches));
            }
            Model::Ingestor(ingestor) => {
                for output in ingestor.output_routes.outputs() {
                    let branch_action = output.branch.as_ref().verified(
                        "the registry requires every route of these nodes to declare its branch \
                         behavior",
                    );
                    let policy = branch_policy(branch_action.branch(), &branches);
                    entrypoints.push(BranchedIngestorSpec {
                        kind,
                        identifier: identifier.clone(),
                        root_relay: output.relay.clone(),
                        branch: policy.branch,
                        branch_ttl: policy.ttl,
                        branch_max_instances: policy.max_instances,
                        output_ack_boundary: BranchInstanceAckBoundary::Preserve,
                        output_flush_policy: output.flush_policy.clone().verified(
                            "the registry requires a flush policy on every flush-based output \
                             route",
                        ),
                        error_policies: output_error_policies(
                            &output.message_error_policy,
                            ingestor.general_error_policy.clone(),
                        ),
                    });
                }
            }
            Model::Reingestor(reingestor) => {
                for output in reingestor.output_routes.outputs() {
                    let branch_action = output.branch.as_ref().verified(
                        "the registry requires every route of these nodes to declare its branch \
                         behavior",
                    );
                    let policy = branch_policy(branch_action.branch(), &branches);
                    entrypoints.push(BranchedIngestorSpec {
                        kind,
                        identifier: identifier.clone(),
                        root_relay: output.relay.clone(),
                        branch: policy.branch,
                        branch_ttl: policy.ttl,
                        branch_max_instances: policy.max_instances,
                        output_ack_boundary: BranchInstanceAckBoundary::Reingestor(reingestor.mode),
                        output_flush_policy: output.flush_policy.clone().verified(
                            "the registry requires a flush policy on every flush-based output \
                             route",
                        ),
                        error_policies: output_error_policies(
                            &output.message_error_policy,
                            GeneralErrorPolicy::Log,
                        ),
                    });
                }
            }
            _ => {}
        }
    }

    processors.sort_by(|left, right| left.spec.processor.cmp(&right.spec.processor));

    BranchedNodeSpecs {
        entrypoints,
        processors,
    }
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

fn parse_branch_flush_policy(
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
            filter_where: node.filter_where.clone(),
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

pub(in crate::runtime) fn processor_template_for_graph_node(
    graph: &ActiveGraph,
    kind: ModelKind,
    processor: &ModelName,
    relay_schemas: &HashMap<RelayName, Arc<CompiledSchema>>,
    udfs: Option<&UdfExecutor>,
) -> error_stack::Result<RelayProcessorTemplate, PlanningError> {
    let specs = branched_node_specs_from_active_graph(graph);
    let Some(node) = specs.processor(kind, processor) else {
        return Err(Report::new(PlanningError::MissingProcessorSpecification {
            kind,
            node: processor.clone(),
        }));
    };
    let mut templates = materialize_nodes(std::slice::from_ref(&node.spec), relay_schemas, udfs)?;
    templates.pop().ok_or_else(|| {
        Report::new(PlanningError::MissingProcessorTemplate {
            kind,
            node: processor.clone(),
        })
    })
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
    model_index: &ModelIndex,
    relay_registries: &HashMap<RelayName, RelayRegistry>,
    relay_services: &HashMap<RelayName, Arc<RelayBoundaryServices>>,
) -> error_stack::Result<HashMap<RelayName, RelayProcessorRelayTemplate>, PlanningError> {
    let mut templates = HashMap::with_capacity(branch_relay_ids.len());
    for relay in branch_relay_ids {
        if model_index.configured::<CreateRelay>(&relay).is_none() {
            return Err(Report::new(PlanningError::MissingRelayModel {
                kind,
                node: node.clone(),
                route: relay,
            }));
        }
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

pub(in crate::runtime) fn materialize_ingestor_route_template(
    spec: &BranchedIngestorSpec,
    model_index: &ModelIndex,
    relay_registries: &HashMap<RelayName, RelayRegistry>,
    relay_services: &HashMap<RelayName, Arc<RelayBoundaryServices>>,
) -> error_stack::Result<IngestorRouteTemplate, PlanningError> {
    let mut branch_relay_ids = HashSet::default();
    branch_relay_ids.insert(spec.root_relay.clone());
    let relays = resolve_branch_relay_templates(
        spec.kind,
        &spec.identifier,
        branch_relay_ids,
        model_index,
        relay_registries,
        relay_services,
    )?;
    Ok(IngestorRouteTemplate {
        branch: BranchInstanceTemplate {
            source_kind: spec.kind,
            source: RelayName::from(&spec.identifier),
            root_relay: spec.root_relay.clone(),
            branch: spec.branch.clone(),
            branch_ttl: parse_branch_ttl_setting(
                spec.branch_ttl.as_deref(),
                spec.kind,
                &spec.identifier,
            )?,
            branch_max_instances: spec.branch_max_instances.map(addressable_count),
            error_policies: spec.error_policies.clone(),
            relays,
            processors: HashMap::default(),
            wasm_state_reset: None,
        },
        ack_boundary: spec.output_ack_boundary,
        flush_policy: parse_branch_flush_policy(
            spec.kind,
            &spec.identifier,
            &spec.root_relay,
            &spec.output_flush_policy,
        )?,
    })
}

pub(in crate::runtime) fn materialize_processor_instance_template(
    node: &BranchedProcessorNodeSpec,
    model_index: &ModelIndex,
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
        model_index,
        relay_registries,
        relay_services,
    )?;
    let template = materialize_nodes(std::slice::from_ref(spec), relay_schemas, udfs)?
        .pop()
        .verified("materialize_nodes answers one template per spec and this call passes one spec");
    let mut processors = HashMap::default();
    processors.insert(spec.processor.clone(), template);
    Ok(BranchInstanceTemplate {
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

#[cfg(test)]
#[path = "planning_tests.rs"]
mod tests;
