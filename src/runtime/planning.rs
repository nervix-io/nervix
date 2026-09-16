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

/// Which pooled transport a named client speaks, and the two things opening it needs.
///
/// Reading the Model to decide this is a planning concern: the data plane opens what it is handed
/// and never works out which driver it is talking to from a `Model` of its own.
pub(in crate::runtime) struct PooledClientPlan<'a> {
    pub(in crate::runtime) transport: PooledTransport,
    pub(in crate::runtime) bounds: ClientPoolBounds,
    pub(in crate::runtime) config: &'a [ClientConfigEntry],
}

/// The transports whose drivers own a connection pool sized by declared bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum PooledTransport {
    Postgres,
    MySql,
    MongoDb,
    Redis,
}

impl<'a> PooledClientPlan<'a> {
    /// The plan for `model`, or `None` when that client owns no connection pool.
    pub(in crate::runtime) fn for_model(model: &'a Model) -> Option<Self> {
        let (transport, bounds, config) = match model {
            Model::ClientPostgres(client) => {
                (PooledTransport::Postgres, client.pool, &client.config)
            }
            Model::ClientMySql(client) => (PooledTransport::MySql, client.pool, &client.config),
            Model::ClientMongoDb(client) => (PooledTransport::MongoDb, client.pool, &client.config),
            Model::ClientRedis(client) => (PooledTransport::Redis, client.pool, &client.config),
            _ => return None,
        };
        Some(Self {
            transport,
            bounds,
            config: config.as_slice(),
        })
    }
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
    branched_node_specs_from_models(nodes.values().map(|node| PlannedModel {
        kind: node.kind(),
        identifier: node.identifier.clone(),
        model: (*node.config).clone(),
    }))
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
                            Report::new(PlanningError::WindowOutputCompilation {
                                node: node.processor.clone(),
                                route: output.relay.clone(),
                            })
                            .attach_printable(reason)
                        })?;
                        compiled_aggregates.push(compiled.with_demand_offset(demand_offset));
                        demand_offset += route_aggregate.demands().len();
                    }

                    // The shared accumulator plan the branch-local window state is built from.
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

                    RelayProcessorOperationTemplate::WindowProcessor {
                        output_routes: materialized_outputs,
                        width_messages,
                        step_messages,
                        width_duration,
                        step_duration,
                        aggregate,
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
                        Report::new(PlanningError::InferencerInputCompilation {
                            node: node.processor.clone(),
                            relay: input_relay.clone(),
                        })
                        .attach_printable(reason)
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
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use nervix_models::{
        BranchSelection, CreateDeduplicator, CreateInferencer, CreateJunction, CreateSchema,
        CreateWasmProcessor, CreateWindowProcessor, InferencerTensorDeclaration,
        InferencerTensorDimension, InferencerTensorElementType, InferencerTensorMapping,
        InferencerTensorRepresentation, InferencerTensorSchema, ParseAsType, ProcessorOutputs,
        RelayBranching, SchemaField, WindowBound, ZeroMqIngestMode,
    };
    use nonzero_ext::nonzero;
    use triomphe::Arc;

    use super::*;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn inferencer_tensor_schema(size: NonZeroU32) -> InferencerTensorSchema {
        InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![InferencerTensorDimension::Fixed(size)],
        }
    }

    fn inferencer_node(input_relays: Vec<RelayName>) -> BranchedProcessorSpec {
        BranchedProcessorSpec {
            kind: ModelKind::Inferencer,
            processor: named("score_model"),
            input_relays,
            input_collect_policies: HashMap::default(),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),
            from_where: HashMap::default(),
            filter_where: None,
            materialized_state: Vec::new(),
            operation: BranchedProcessorOperationSpec::Inferencer {
                output_routes: BranchedProcessorOutputsSpec {
                    routes: vec![BranchedProcessorOutputSpec {
                        relay: named("scores"),
                        construction: RouteConstruction::default(),
                        flush_policy: Some(FlushPolicy::Immediate),
                        message_error_policy: MessageErrorPolicy::Log,
                    }],
                },
                resource: named("fraud_model"),
                resource_version: Some(1),
                file: "models/fraud.onnx".to_string(),
                inputs: Vec::new(),
                output_schema: Vec::new(),
            },
        }
    }

    fn window_node(output_routes: BranchedProcessorOutputsSpec) -> BranchedProcessorSpec {
        BranchedProcessorSpec {
            kind: ModelKind::WindowProcessor,
            processor: named("metric_window"),
            input_relays: vec![named("metrics")],
            input_collect_policies: HashMap::default(),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),
            from_where: HashMap::default(),
            filter_where: None,
            materialized_state: Vec::new(),
            operation: BranchedProcessorOperationSpec::WindowProcessor {
                output_routes,
                width: WindowBound::of_messages(10),
                step: WindowBound::of_messages(5),
            },
        }
    }

    #[test]
    fn planning_parsers_preserve_typed_contract_failures() {
        let processor = named::<ModelName>("orders_processor");
        let relay = named::<RelayName>("orders");

        let window_duration = parse_optional_window_duration(
            &processor,
            WindowDurationSetting::Width,
            Some("not-a-duration"),
        )
        .expect_err("an invalid window width must fail");
        assert!(matches!(
            window_duration.current_context(),
            PlanningError::InvalidWindowDuration {
                node,
                setting: WindowDurationSetting::Width,
            } if node == &processor
        ));

        let output = BranchedProcessorOutputSpec {
            relay: relay.clone(),
            construction: RouteConstruction::default(),
            flush_policy: Some(FlushPolicy::Each {
                interval: "1s".to_string(),
                max_batch_size: "not-a-size".to_string(),
            }),
            message_error_policy: MessageErrorPolicy::Log,
        };
        let flush_size = materialize_output(
            ModelKind::Deduplicator,
            &processor,
            &output,
            FlushPolicyRequirement::Required,
        )
        .expect_err("an invalid flush batch size must fail");
        assert!(matches!(
            flush_size.current_context(),
            PlanningError::InvalidFlushMaxBatchSize {
                kind: ModelKind::Deduplicator,
                node,
                route,
            } if node == &processor && route == &relay
        ));

        let collect_interval = parse_input_collect_policy(
            ModelKind::Junction,
            &processor,
            &relay,
            &nervix_models::InputCollectPolicy {
                collect_for: "not-a-duration".to_string(),
                max_batch_size: None,
            },
        )
        .expect_err("an invalid collection interval must fail");
        assert!(matches!(
            collect_interval.current_context(),
            PlanningError::InvalidCollectInterval {
                kind: ModelKind::Junction,
                node,
                relay: error_relay,
            } if node == &processor && error_relay == &relay
        ));

        let collect_size = parse_input_collect_policy(
            ModelKind::Junction,
            &processor,
            &relay,
            &nervix_models::InputCollectPolicy {
                collect_for: "1s".to_string(),
                max_batch_size: Some("not-a-size".to_string()),
            },
        )
        .expect_err("an invalid collection batch size must fail");
        assert!(matches!(
            collect_size.current_context(),
            PlanningError::InvalidCollectMaxBatchSize {
                kind: ModelKind::Junction,
                node,
                relay: error_relay,
            } if node == &processor && error_relay == &relay
        ));

        let unbounded_collection = parse_input_collect_policy(
            ModelKind::Junction,
            &processor,
            &relay,
            &nervix_models::InputCollectPolicy {
                collect_for: "1s".to_string(),
                max_batch_size: None,
            },
        )
        .expect("a collection policy may omit its byte bound");
        assert_eq!(unbounded_collection.max_batch_size, None);

        let max_time = parse_max_time(ModelKind::Deduplicator, &processor, "not-a-duration")
            .expect_err("an invalid maximum retention time must fail");
        assert!(matches!(
            max_time.current_context(),
            PlanningError::InvalidMaxTime {
                kind: ModelKind::Deduplicator,
                node,
            } if node == &processor
        ));

        let branch_ttl =
            parse_branch_ttl_setting(Some("not-a-duration"), ModelKind::Deduplicator, &processor)
                .expect_err("an invalid branch TTL must fail");
        assert!(matches!(
            branch_ttl.current_context(),
            PlanningError::InvalidBranchTtl {
                kind: ModelKind::Deduplicator,
                node,
            } if node == &processor
        ));
    }

    #[test]
    fn window_materialization_classifies_output_contract_failures() {
        let missing_output = materialize_nodes(
            &[window_node(BranchedProcessorOutputsSpec {
                routes: Vec::new(),
            })],
            &HashMap::default(),
            None,
        )
        .expect_err("a window processor without an output must fail");
        assert!(matches!(
            missing_output.current_context(),
            PlanningError::MissingWindowOutput { node } if node.as_str() == "metric_window"
        ));

        let inherited = RouteConstruction {
            inherit: Some(nervix_models::Inheritance::All),
            ..RouteConstruction::default()
        };
        let invalid_construction = materialize_nodes(
            &[window_node(BranchedProcessorOutputsSpec {
                routes: vec![BranchedProcessorOutputSpec {
                    relay: named("metric_summary"),
                    construction: inherited,
                    flush_policy: None,
                    message_error_policy: MessageErrorPolicy::Log,
                }],
            })],
            &HashMap::default(),
            None,
        )
        .expect_err("window output inheritance must fail lowering");
        assert!(matches!(
            invalid_construction.current_context(),
            PlanningError::InvalidWindowConstruction { node, route }
                if node.as_str() == "metric_window" && route.as_str() == "metric_summary"
        ));

        let compilation = materialize_nodes(
            &[window_node(BranchedProcessorOutputsSpec {
                routes: vec![BranchedProcessorOutputSpec {
                    relay: named("metric_summary"),
                    construction: construction("SET count = COUNT(input.value)"),
                    flush_policy: None,
                    message_error_policy: MessageErrorPolicy::Log,
                }],
            })],
            &HashMap::default(),
            None,
        )
        .expect_err("window output without runtime schemas must fail compilation");
        assert!(matches!(
            compilation.current_context(),
            PlanningError::WindowOutputCompilation { node, route }
                if node.as_str() == "metric_window" && route.as_str() == "metric_summary"
        ));
    }

    #[test]
    fn inferencer_materialization_requires_an_input_and_schema() {
        let missing_input =
            materialize_nodes(&[inferencer_node(Vec::new())], &HashMap::default(), None)
                .expect_err("an inferencer without input must fail");
        assert!(matches!(
            missing_input.current_context(),
            PlanningError::MissingInputRelay {
                kind: ModelKind::Inferencer,
                node,
            } if node.as_str() == "score_model"
        ));

        let missing_schema = materialize_nodes(
            &[inferencer_node(vec![named("features")])],
            &HashMap::default(),
            None,
        )
        .expect_err("an inferencer input without a runtime schema must fail");
        assert!(matches!(
            missing_schema.current_context(),
            PlanningError::MissingInputSchema {
                kind: ModelKind::Inferencer,
                node,
                relay,
            } if node.as_str() == "score_model" && relay.as_str() == "features"
        ));
    }

    #[test]
    fn relay_template_resolution_classifies_each_missing_owner() {
        let node = named::<ModelName>("orders_junction");
        let relay = named::<RelayName>("orders");
        let relay_ids = || std::iter::once(relay.clone()).collect();

        let missing_model = resolve_branch_relay_templates(
            ModelKind::Junction,
            &node,
            relay_ids(),
            &ModelIndex::default(),
            &HashMap::default(),
            &HashMap::default(),
        )
        .expect_err("an unconfigured relay must fail planning");
        assert!(matches!(
            missing_model.current_context(),
            PlanningError::MissingRelayModel {
                kind: ModelKind::Junction,
                node: error_node,
                route,
            } if error_node == &node && route == &relay
        ));

        let model_index = [Model::Relay(CreateRelay {
            name: relay.clone(),
            schema: named("orders_schema"),
            buffer: nonzero!(1usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        })]
        .into_iter()
        .collect::<ModelIndex>();
        let missing_registry = resolve_branch_relay_templates(
            ModelKind::Junction,
            &node,
            relay_ids(),
            &model_index,
            &HashMap::default(),
            &HashMap::default(),
        )
        .expect_err("a relay without a registry must fail planning");
        assert!(matches!(
            missing_registry.current_context(),
            PlanningError::MissingRelayRegistry {
                kind: ModelKind::Junction,
                node: error_node,
                route,
            } if error_node == &node && route == &relay
        ));

        let relay_registries = [(relay.clone(), RelayRegistry::new())]
            .into_iter()
            .collect();
        let missing_services = resolve_branch_relay_templates(
            ModelKind::Junction,
            &node,
            relay_ids(),
            &model_index,
            &relay_registries,
            &HashMap::default(),
        )
        .expect_err("a relay without boundary services must fail planning");
        assert!(matches!(
            missing_services.current_context(),
            PlanningError::MissingRelayServices {
                kind: ModelKind::Junction,
                node: error_node,
                route,
            } if error_node == &node && route == &relay
        ));
    }

    #[test]
    fn processor_instance_materialization_requires_an_input_relay() {
        let node = BranchedProcessorNodeSpec {
            spec: BranchedProcessorSpec {
                kind: ModelKind::Junction,
                processor: named("orders_junction"),
                input_relays: Vec::new(),
                input_collect_policies: HashMap::default(),
                mode: AckMode::Attached,
                error_policies: ErrorPolicies::handled_by_log(),
                from_where: HashMap::default(),
                filter_where: None,
                materialized_state: Vec::new(),
                operation: BranchedProcessorOperationSpec::Junction {
                    output_routes: BranchedProcessorOutputsSpec { routes: Vec::new() },
                },
            },
            branch: None,
            branch_ttl: None,
            branch_max_instances: None,
        };
        let error = materialize_processor_instance_template(
            &node,
            &ModelIndex::default(),
            &HashMap::default(),
            &HashMap::default(),
            &HashMap::default(),
            None,
        )
        .expect_err("a processor instance without input must fail planning");

        assert!(matches!(
            error.current_context(),
            PlanningError::MissingInputRelay {
                kind: ModelKind::Junction,
                node,
            } if node.as_str() == "orders_junction"
        ));
    }

    #[test]
    fn inferencer_input_mappings_compile_when_template_is_materialized() {
        let input_relay = named::<RelayName>("features");
        let processor = named::<ModelName>("score_model");
        let input_schema = Arc::new(compile_schema(&CreateSchema {
            name: named("feature_schema"),
            fields: vec![SchemaField {
                name: named("vector"),
                ty: ParseAsType::Array {
                    element: Box::new(ParseAsType::F32),
                    len: nonzero!(2u32),
                },
                optional: false,
                sensitive: false,
            }],
        }));
        let node = BranchedProcessorSpec {
            kind: ModelKind::Inferencer,
            processor: processor.clone(),
            input_relays: vec![input_relay.clone()],
            input_collect_policies: HashMap::default(),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),
            from_where: HashMap::default(),
            filter_where: None,
            materialized_state: Vec::new(),
            operation: BranchedProcessorOperationSpec::Inferencer {
                output_routes: BranchedProcessorOutputsSpec {
                    routes: vec![BranchedProcessorOutputSpec {
                        relay: named("scores"),
                        construction: RouteConstruction::default(),
                        flush_policy: Some(FlushPolicy::Immediate),
                        message_error_policy: MessageErrorPolicy::Log,
                    }],
                },
                resource: named("fraud_model"),
                resource_version: Some(1),
                file: "models/fraud.onnx".to_string(),
                inputs: vec![InferencerTensorMapping {
                    tensor: "features".to_string(),
                    schema: inferencer_tensor_schema(nonzero!(2u32)),
                    expression: nervix_nspl::parse_expression("input.missing")
                        .expect("test expression must parse"),
                }],
                output_schema: vec![InferencerTensorDeclaration {
                    tensor: "score".to_string(),
                    schema: inferencer_tensor_schema(nonzero!(1u32)),
                }],
            },
        };
        let mut relay_schemas = HashMap::default();
        relay_schemas.insert(input_relay, input_schema);

        let error = materialize_nodes(&[node], &relay_schemas, None)
            .expect_err("invalid INPUTS mapping must fail template materialization");

        assert!(matches!(
            error.current_context(),
            PlanningError::InferencerInputCompilation { node, relay }
                if node == &processor && relay.as_str() == "features"
        ));
    }

    #[test]
    fn missing_flush_policy_identifies_the_node_and_route() {
        let processor = named::<ModelName>("orders_deduplicator");
        let route = named::<RelayName>("deduplicated_orders");
        let output = BranchedProcessorOutputSpec {
            relay: route.clone(),
            construction: RouteConstruction::default(),
            flush_policy: None,
            message_error_policy: MessageErrorPolicy::Log,
        };

        let error = materialize_output(
            ModelKind::Deduplicator,
            &processor,
            &output,
            FlushPolicyRequirement::Required,
        )
        .expect_err("a flush-based route must declare its flush policy");

        assert!(matches!(
            error.current_context(),
            PlanningError::MissingFlushPolicy {
                kind,
                node,
                route: error_route,
            } if *kind == ModelKind::Deduplicator
                && node == &processor
                && error_route == &route
        ));
    }

    #[test]
    fn invalid_flush_interval_identifies_the_node_and_route() {
        let processor = named::<ModelName>("orders_reorderer");
        let route = named::<RelayName>("ordered_orders");
        let output = BranchedProcessorOutputSpec {
            relay: route.clone(),
            construction: RouteConstruction::default(),
            flush_policy: Some(FlushPolicy::Each {
                interval: "not-a-duration".to_string(),
                max_batch_size: "1MiB".to_string(),
            }),
            message_error_policy: MessageErrorPolicy::Log,
        };

        let error = materialize_output(
            ModelKind::Reorderer,
            &processor,
            &output,
            FlushPolicyRequirement::Required,
        )
        .expect_err("an invalid flush interval must fail planning");

        assert!(matches!(
            error.current_context(),
            PlanningError::InvalidFlushInterval {
                kind,
                node,
                route: error_route,
            } if *kind == ModelKind::Reorderer
                && node == &processor
                && error_route == &route
        ));
    }

    #[test]
    fn branched_node_specs_capture_downstream_processing_tree() {
        let specs = branched_node_specs_from_models(
            [
                branch_model("tenant", "orders", &["tenant"]),
                branch_model("tenant", "projected_orders", &["tenant"]),
                PlannedModel {
                    kind: ModelKind::Ingestor,
                    identifier: named("orders_ingestor"),
                    model: nervix_models::Model::Ingestor(CreateIngestor {
                        name: named("orders_ingestor"),
                        output_routes: (ProcessorOutputs::single(named("orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(branched_by("orders", &["tenant"])),
                        decode_using_codec: named("orders_codec"),
                        timestamp_source: None,
                        source: IngestSource::ZeroMq {
                            client: named("zmq_client"),
                            mode: ZeroMqIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_orders"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_orders"),
                        from: ProcessorInputs::single(named("orders"))
                            .with_collect_policy("25ms".to_string(), Some("2MiB".to_string())),
                        output_routes: (ProcessorOutputs::single(named("projected_orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("orders", &["tenant"]),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_projected_orders"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_projected_orders"),
                        from: ProcessorInputs::single(named("projected_orders")),
                        output_routes: (ProcessorOutputs::single(named("aggregated_orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("projected_orders", &["tenant"]),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Emitter,
                    identifier: named("orders_emitter"),
                    model: nervix_models::Model::Emitter(CreateEmitter {
                        name: named("orders_emitter"),
                        from: ProcessorInputs::single(named("aggregated_orders")),
                        encode_using_codec: Some(named("orders_codec")),
                        sink: Box::new(EmitSink::ZeroMq {
                            client: named("zmq_client"),
                        }),
                        flush_policy: FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        },
                        mode: AckMode::Attached,
                        error_policies: ErrorPolicies::handled_by_log(),
                        publishing_mode: EmitterPublishingMode::NoAck {
                            retry_policy: RetryPolicy {
                                backoff: "250ms".to_string(),
                                max_backoff: "30s".to_string(),
                            },
                        },
                        construction: nervix_models::RouteConstruction::default(),
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.entrypoints.len(), 1);
        let spec = &specs.entrypoints[0];
        assert_eq!(spec.identifier, named("orders_ingestor"));
        assert_eq!(spec.root_relay, named("orders"));
        assert_eq!(spec.branch.as_ref(), Some(&named("by_orders")));
        assert_eq!(specs.processors.len(), 2);
        let dedup_orders = &specs.processors[0];
        assert_eq!(dedup_orders.spec.processor, named("dedup_orders"));
        assert_eq!(dedup_orders.spec.input_relays, vec![named("orders")]);
        let collect_policy = dedup_orders
            .spec
            .input_collect_policies
            .get(&RelayName::from(&named::<ModelName>("orders")))
            .expect("input collection policy must be planned for its source relay");
        assert_eq!(collect_policy.collect_for, "25ms");
        assert_eq!(collect_policy.max_batch_size.as_deref(), Some("2MiB"));
        assert_eq!(dedup_orders.branch.as_ref(), Some(&named("by_orders")));
        assert_eq!(dedup_orders.branch_ttl.as_deref(), Some("5m"));
        assert_eq!(dedup_orders.branch_max_instances, None);
        let BranchedProcessorOperationSpec::Deduplicator { output_routes, .. } =
            &dedup_orders.spec.operation
        else {
            panic!("expected deduplicator output");
        };
        let output = output_routes
            .routes
            .first()
            .expect("deduplicator should have output route");
        assert_eq!(output.relay, named("projected_orders"));
        let dedup_projected = &specs.processors[1];
        assert_eq!(
            dedup_projected.spec.processor,
            named("dedup_projected_orders")
        );
        assert_eq!(
            dedup_projected.spec.input_relays,
            vec![named("projected_orders")]
        );
        assert_eq!(dedup_projected.branch_ttl.as_deref(), Some("5m"));
    }

    #[test]
    fn branched_node_specs_capture_window_processor_as_branch_node() {
        let specs = branched_node_specs_from_models(
            [
                branch_model("host", "metrics", &["host"]),
                branch_model("host", "metric_summary", &["host"]),
                PlannedModel {
                    kind: ModelKind::Ingestor,
                    identifier: named("metrics_ingestor"),
                    model: nervix_models::Model::Ingestor(CreateIngestor {
                        name: named("metrics_ingestor"),
                        output_routes: (ProcessorOutputs::single(named("metrics")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(branched_by("metrics", &["host"])),
                        decode_using_codec: named("metrics_codec"),
                        timestamp_source: None,
                        source: IngestSource::ZeroMq {
                            client: named("zmq_client"),
                            mode: ZeroMqIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::WindowProcessor,
                    identifier: named("metric_window"),
                    model: nervix_models::Model::WindowProcessor(CreateWindowProcessor {
                        name: named("metric_window"),
                        from: ProcessorInputs::single(named("metrics")),
                        output_routes: window_outputs(
                            "metric_summary",
                            "SET count = COUNT(input.latency)",
                        ),
                        branched_by: processor_branched_by("metrics", &["host"]),
                        width: WindowBound {
                            messages: Some(100),
                            duration: None,
                        },
                        step: WindowBound {
                            messages: Some(10),
                            duration: None,
                        },
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_summary"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_summary"),
                        from: ProcessorInputs::single(named("metric_summary")),
                        output_routes: (ProcessorOutputs::single(named("projected_summary")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("metric_summary", &["host"]),
                        deduplicate_on: vec![expression("input.count")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.entrypoints.len(), 1);
        let spec = &specs.entrypoints[0];
        assert_eq!(spec.root_relay, named("metrics"));
        assert_eq!(specs.processors.len(), 2);
        let window = specs
            .processors
            .iter()
            .find(|node| node.spec.processor == named("metric_window"))
            .expect("window processor spec must exist");
        let BranchedProcessorOperationSpec::WindowProcessor {
            output_routes,
            width,
            step,
        } = &window.spec.operation
        else {
            panic!("expected window processor branch node");
        };
        let output = output_routes
            .routes
            .first()
            .expect("window processor should have output route");
        assert_eq!(output.relay, named("metric_summary"));
        assert_eq!(width.messages, Some(100));
        assert_eq!(step.messages, Some(10));
        assert_eq!(output.construction.assignments.len(), 1);
        assert!(
            specs
                .processors
                .iter()
                .any(|node| node.spec.processor == named("dedup_summary")
                    && node.spec.input_relays == vec![named("metric_summary")])
        );
    }

    #[test]
    fn branched_node_specs_capture_inferencer_as_branch_node() {
        let specs = branched_node_specs_from_models(
            [
                branch_model("tenant", "features", &["tenant"]),
                branch_model("tenant", "scores", &["tenant"]),
                PlannedModel {
                    kind: ModelKind::Ingestor,
                    identifier: named("features_ingestor"),
                    model: nervix_models::Model::Ingestor(CreateIngestor {
                        name: named("features_ingestor"),
                        output_routes: (ProcessorOutputs::single(named("features")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(branched_by("features", &["tenant"])),
                        decode_using_codec: named("features_codec"),
                        timestamp_source: None,
                        source: IngestSource::ZeroMq {
                            client: named("zmq_client"),
                            mode: ZeroMqIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Inferencer,
                    identifier: named("score_model"),
                    model: nervix_models::Model::Inferencer(CreateInferencer {
                        name: named("score_model"),
                        from: ProcessorInputs::single(named("features")),
                        output_routes: (ProcessorOutputs::single(named("scores")))
                            .with_flush_policy(FlushPolicy::Immediate),
                        branched_by: processor_branched_by("features", &["tenant"]),
                        resource: named("fraud_model"),
                        resource_version: Some(3),
                        file: "models/fraud.onnx".to_string(),
                        inputs: vec![InferencerTensorMapping {
                            tensor: "features".to_string(),
                            schema: inferencer_tensor_schema(nonzero!(2u32)),
                            expression: expression("input.vector"),
                        }],
                        output_schema: vec![InferencerTensorDeclaration {
                            tensor: "score".to_string(),
                            schema: inferencer_tensor_schema(nonzero!(1u32)),
                        }],
                        mode: AckMode::Attached,
                        filter_where: Some(expression("input.active")),
                        materialized_state: Vec::new(),
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_scores"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_scores"),
                        from: ProcessorInputs::single(named("scores")),
                        output_routes: (ProcessorOutputs::single(named("projected_scores")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("scores", &["tenant"]),
                        deduplicate_on: vec![expression("input.score")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.entrypoints.len(), 1);
        let spec = &specs.entrypoints[0];
        assert_eq!(spec.root_relay, named("features"));
        assert_eq!(specs.processors.len(), 2);
        let inferencer = specs
            .processors
            .iter()
            .find(|node| node.spec.processor == named("score_model"))
            .expect("inferencer spec must exist");
        let BranchedProcessorOperationSpec::Inferencer {
            output_routes,
            resource,
            resource_version,
            file,
            inputs,
            output_schema,
            ..
        } = &inferencer.spec.operation
        else {
            panic!("expected inferencer branch node");
        };
        let output = output_routes
            .routes
            .first()
            .expect("inferencer should have output route");
        assert_eq!(output.relay, named("scores"));
        assert_eq!(resource, &named("fraud_model"));
        assert_eq!(*resource_version, Some(3));
        assert_eq!(file, "models/fraud.onnx");
        assert_eq!(inputs.len(), 1);
        assert_eq!(output_schema.len(), 1);
        assert_eq!(output.flush_policy, Some(FlushPolicy::Immediate));
        assert_eq!(
            inferencer.spec.filter_where,
            Some(expression("input.active"))
        );
        assert!(
            specs
                .processors
                .iter()
                .any(|node| node.spec.processor == named("dedup_scores")
                    && node.spec.input_relays == vec![named("scores")])
        );
    }

    #[test]
    fn branched_node_specs_capture_reingestor_entrypoint_tree() {
        let specs = branched_node_specs_from_models(
            [
                branch_model("tenant", "tenant_orders", &["tenant"]),
                PlannedModel {
                    kind: ModelKind::Reingestor,
                    identifier: named("tenant_partition"),
                    model: nervix_models::Model::Reingestor(CreateReingestor {
                        name: named("tenant_partition"),
                        from: ProcessorInputs::single(named("orders")),
                        output_routes: with_inherit_all(ProcessorOutputs::single(named(
                            "tenant_orders",
                        )))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(branched_by("tenant_orders", &["tenant"])),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_orders"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_orders"),
                        from: ProcessorInputs::single(named("tenant_orders")),
                        output_routes: (ProcessorOutputs::single(named("projected_orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("tenant_orders", &["tenant"]),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.entrypoints.len(), 1);
        let spec = &specs.entrypoints[0];
        assert_eq!(spec.kind, ModelKind::Reingestor);
        assert_eq!(spec.identifier, named("tenant_partition"));
        assert_eq!(spec.root_relay, named("tenant_orders"));
        assert_eq!(spec.branch.as_ref(), Some(&named("by_tenant_orders")));
        assert_eq!(specs.processors.len(), 1);
        assert_eq!(specs.processors[0].spec.processor, named("dedup_orders"));
        assert_eq!(
            specs.processors[0].spec.input_relays,
            vec![named("tenant_orders")]
        );
        assert_eq!(
            specs.processors[0].branch.as_ref(),
            Some(&named("by_tenant_orders"))
        );
        assert_eq!(specs.processors[0].branch_ttl.as_deref(), Some("5m"));
    }

    #[test]
    fn branched_node_specs_capture_processor_output_route_tree() {
        let specs = branched_node_specs_from_models(
            [
                branch_model("tenant", "orders", &["tenant"]),
                branch_model("tenant", "urgent_orders", &["tenant"]),
                branch_model("tenant", "default_orders", &["tenant"]),
                PlannedModel {
                    kind: ModelKind::Ingestor,
                    identifier: named("orders_ingestor"),
                    model: nervix_models::Model::Ingestor(CreateIngestor {
                        name: named("orders_ingestor"),
                        output_routes: (ProcessorOutputs::single(named("orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(branched_by("orders", &["tenant"])),
                        decode_using_codec: named("orders_codec"),
                        timestamp_source: None,
                        source: IngestSource::ZeroMq {
                            client: named("zmq_client"),
                            mode: ZeroMqIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("orders_splitter"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("orders_splitter"),
                        from: ProcessorInputs::single(named("orders")),
                        output_routes: (ProcessorOutputs::new(vec![
                            ProcessorOutput {
                                relay: named("urgent_orders"),
                                construction: nervix_nspl::parse_route_construction(
                                    "WHERE output.urgent",
                                )
                                .expect("route construction must parse"),
                                flush_policy: None,
                                message_error_policy: MessageErrorPolicy::Log,
                                branch: None,
                            },
                            ProcessorOutput {
                                relay: named("default_orders"),
                                construction: nervix_models::RouteConstruction::default(),
                                flush_policy: None,
                                message_error_policy: MessageErrorPolicy::Log,
                                branch: None,
                            },
                        ]))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                        branched_by: processor_branched_by("orders", &["tenant"]),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: Some(expression("input.active")),
                        materialized_state: Vec::new(),
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_urgent"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_urgent"),
                        from: ProcessorInputs::single(named("urgent_orders")),
                        output_routes: (ProcessorOutputs::single(named("urgent_projected")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("urgent_orders", &["tenant"]),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_default"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_default"),
                        from: ProcessorInputs::single(named("default_orders")),
                        output_routes: (ProcessorOutputs::single(named("default_projected")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("default_orders", &["tenant"]),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.entrypoints.len(), 1);
        assert_eq!(specs.processors.len(), 3);
        let splitter = specs
            .processors
            .iter()
            .find(|node| node.spec.processor == named("orders_splitter"))
            .expect("splitter spec must exist");
        let BranchedProcessorOperationSpec::Deduplicator { output_routes, .. } =
            &splitter.spec.operation
        else {
            panic!("expected deduplicator output routes");
        };
        assert_eq!(splitter.spec.filter_where, Some(expression("input.active")));
        assert_eq!(output_routes.routes.len(), 2);
        assert_eq!(
            output_routes.routes[0].construction.where_clause,
            Some(expression("output.urgent"))
        );
        assert_eq!(output_routes.routes[0].relay, named("urgent_orders"));
        assert_eq!(output_routes.routes[1].relay, named("default_orders"));
        assert!(
            specs
                .processors
                .iter()
                .any(|node| node.spec.processor == named("dedup_urgent")
                    && node.spec.input_relays == vec![named("urgent_orders")])
        );
        assert!(
            specs
                .processors
                .iter()
                .any(|node| node.spec.processor == named("dedup_default")
                    && node.spec.input_relays == vec![named("default_orders")])
        );
    }

    #[test]
    fn branched_node_specs_capture_junction_as_single_branch_processor() {
        let specs = branched_node_specs_from_models(
            [
                branch_model("tenant", "left_stream", &["tenant"]),
                branch_model("tenant", "right_stream", &["tenant"]),
                branch_model("tenant", "joined_stream", &["tenant"]),
                PlannedModel {
                    kind: ModelKind::Ingestor,
                    identifier: named("left_ingestor"),
                    model: nervix_models::Model::Ingestor(CreateIngestor {
                        name: named("left_ingestor"),
                        output_routes: (ProcessorOutputs::single(named("left_stream")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(branched_by("left_stream", &["tenant"])),
                        decode_using_codec: named("notification_codec"),
                        timestamp_source: None,
                        source: IngestSource::ZeroMq {
                            client: named("zmq_client"),
                            mode: ZeroMqIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,

                        filter_where: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Ingestor,
                    identifier: named("right_ingestor"),
                    model: nervix_models::Model::Ingestor(CreateIngestor {
                        name: named("right_ingestor"),
                        output_routes: (ProcessorOutputs::single(named("right_stream")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(branched_by("right_stream", &["tenant"])),
                        decode_using_codec: named("notification_codec"),
                        timestamp_source: None,
                        source: IngestSource::ZeroMq {
                            client: named("zmq_client"),
                            mode: ZeroMqIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,

                        filter_where: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Junction,
                    identifier: named("join_streams"),
                    model: nervix_models::Model::Junction(CreateJunction {
                        name: named("join_streams"),
                        from: ProcessorInputs::new(
                            vec![named("left_stream"), named("right_stream")],
                            Vec::new(),
                        ),
                        output_routes: (ProcessorOutputs::single(named("joined_stream")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("left_stream", &["tenant"]),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_joined"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_joined"),
                        from: ProcessorInputs::single(named("joined_stream")),
                        output_routes: (ProcessorOutputs::single(named("projected_joined")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("joined_stream", &["tenant"]),
                        deduplicate_on: vec![expression("input.tenant")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.entrypoints.len(), 2);
        assert_eq!(
            specs
                .processors
                .iter()
                .filter(|node| node.spec.processor == named("join_streams"))
                .count(),
            1
        );
        let junction = specs
            .processors
            .iter()
            .find(|node| node.spec.processor == named("join_streams"))
            .expect("junction spec must exist");
        assert_eq!(
            junction.spec.input_relays,
            vec![named("left_stream"), named("right_stream")]
        );
        let BranchedProcessorOperationSpec::Junction { output_routes, .. } =
            &junction.spec.operation
        else {
            panic!("expected junction processor");
        };
        let output = output_routes
            .routes
            .first()
            .expect("junction should have output route");
        assert_eq!(output.relay, named("joined_stream"));
        assert!(
            specs
                .processors
                .iter()
                .any(|node| node.spec.processor == named("dedup_joined"))
        );
    }

    #[test]
    fn branched_node_specs_capture_single_processor_output_route_tree() {
        let specs = branched_node_specs_from_models(
            [
                branch_model("tenant", "orders", &["tenant"]),
                branch_model("tenant", "projected_orders", &["tenant"]),
                PlannedModel {
                    kind: ModelKind::Ingestor,
                    identifier: named("orders_ingestor"),
                    model: nervix_models::Model::Ingestor(CreateIngestor {
                        name: named("orders_ingestor"),
                        output_routes: (ProcessorOutputs::single(named("orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(branched_by("orders", &["tenant"])),
                        decode_using_codec: named("orders_codec"),
                        timestamp_source: None,
                        source: IngestSource::ZeroMq {
                            client: named("zmq_client"),
                            mode: ZeroMqIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,

                        filter_where: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("orders_filter"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("orders_filter"),
                        from: ProcessorInputs::new(
                            vec![named("orders")],
                            vec![ProcessorInputWhere {
                                relay: named("orders"),
                                where_clause: expression("input.active"),
                            }],
                        ),
                        output_routes: (ProcessorOutputs::single(named("projected_orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("orders", &["tenant"]),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: Some(expression("input.active")),
                        materialized_state: Vec::new(),
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_projected"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_projected"),
                        from: ProcessorInputs::single(named("projected_orders")),
                        output_routes: (ProcessorOutputs::single(named("aggregated_orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("projected_orders", &["tenant"]),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.entrypoints.len(), 1);
        let orders_filter = specs
            .processors
            .iter()
            .find(|node| node.spec.processor == named("orders_filter"))
            .expect("orders filter spec must exist");
        assert_eq!(
            orders_filter
                .spec
                .from_where
                .get(&RelayName::from(&named::<ModelName>("orders"))),
            Some(&expression("input.active"))
        );
        let BranchedProcessorOperationSpec::Deduplicator { output_routes, .. } =
            &orders_filter.spec.operation
        else {
            panic!("expected processor output routes");
        };
        assert_eq!(
            orders_filter.spec.filter_where,
            Some(expression("input.active"))
        );
        assert_eq!(output_routes.routes.len(), 1);
        assert_eq!(output_routes.routes[0].relay, named("projected_orders"));
        assert!(
            specs
                .processors
                .iter()
                .any(|node| node.spec.processor == named("dedup_projected")
                    && node.spec.input_relays == vec![named("projected_orders")])
        );
    }

    #[test]
    fn branched_node_specs_include_singleton_branch_for_empty_branching() {
        let specs = branched_node_specs_from_models(
            [
                PlannedModel {
                    kind: ModelKind::Ingestor,
                    identifier: named("orders_ingestor"),
                    model: nervix_models::Model::Ingestor(CreateIngestor {
                        name: named("orders_ingestor"),
                        output_routes: (ProcessorOutputs::single(named("orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(OutputBranch::Unbranched),
                        decode_using_codec: named("orders_codec"),
                        timestamp_source: None,
                        source: IngestSource::ZeroMq {
                            client: named("zmq_client"),
                            mode: ZeroMqIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,

                        filter_where: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_orders"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_orders"),
                        from: ProcessorInputs::single(named("orders")),
                        output_routes: (ProcessorOutputs::single(named("projected_orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: processor_branched_by("orders", &[]),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.entrypoints.len(), 1);
        assert_eq!(specs.entrypoints[0].identifier, named("orders_ingestor"));
        assert_eq!(specs.entrypoints[0].root_relay, named("orders"));
        assert_eq!(specs.entrypoints[0].branch, None);
        assert_eq!(specs.entrypoints[0].branch_ttl, None);
        assert_eq!(specs.processors.len(), 1);
        assert_eq!(specs.processors[0].spec.processor, named("dedup_orders"));
        assert_eq!(specs.processors[0].branch_ttl, None);
        assert_eq!(specs.processors[0].branch, None);
        assert_eq!(specs.processors[0].branch_max_instances, None);
    }

    #[test]
    fn branched_processor_specs_do_not_require_an_entrypoint() {
        let specs = branched_node_specs_from_models(
            [
                PlannedModel {
                    kind: ModelKind::Relay,
                    identifier: named("orders"),
                    model: nervix_models::Model::Relay(CreateRelay {
                        name: named("orders"),
                        schema: named("order_event"),
                        buffer: nonzero!(1usize),
                        branching: RelayBranching::unbranched(),
                        materialized_state: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::Deduplicator,
                    identifier: named("dedup_orders"),
                    model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_orders"),
                        from: ProcessorInputs::single(named("orders")),
                        output_routes: (ProcessorOutputs::single(named("projected_orders")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                        branched_by: BranchSelection::unbranched(),
                        deduplicate_on: vec![expression("input.order_id")],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert!(specs.entrypoints.is_empty());
        assert_eq!(specs.processors.len(), 1);
        assert_eq!(specs.processors[0].spec.processor, named("dedup_orders"));
        assert_eq!(specs.processors[0].spec.input_relays, vec![named("orders")]);
        assert_eq!(specs.processors[0].branch_ttl, None);
    }

    #[test]
    fn branched_wasm_processor_specs_preserve_global_error_policy() {
        let specs = branched_node_specs_from_models(
            [
                PlannedModel {
                    kind: ModelKind::Relay,
                    identifier: named("orders"),
                    model: nervix_models::Model::Relay(CreateRelay {
                        name: named("orders"),
                        schema: named("order_event"),
                        buffer: nonzero!(1usize),
                        branching: RelayBranching::unbranched(),
                        materialized_state: None,
                    }),
                },
                PlannedModel {
                    kind: ModelKind::WasmProcessor,
                    identifier: named("filter_orders"),
                    model: nervix_models::Model::WasmProcessor(CreateWasmProcessor {
                        name: named("filter_orders"),
                        from: ProcessorInputs::single(named("orders")),
                        output_routes: ProcessorOutputs::single(named("filtered_orders")),
                        branched_by: BranchSelection::unbranched(),
                        resource: named("filter_resource"),
                        resource_version: None,
                        file: "filter.wasm".to_string(),
                        limits: nervix_models::WasmProcessorLimits {
                            max_fuel: nonzero!(1_000_000_000u64),
                            max_memory_bytes: nonzero!(67_108_864u64),
                        },
                        global_error_policy: GeneralErrorPolicy::Ignore,
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.processors.len(), 1);
        assert_eq!(
            specs.processors[0].spec.error_policies.general,
            GeneralErrorPolicy::Ignore
        );
        assert_eq!(
            specs.processors[0].spec.error_policies.message,
            MessageErrorPolicy::Log
        );
    }

    #[test]
    fn branched_node_specs_include_reingestor_with_declared_branching() {
        let specs = branched_node_specs_from_models(
            [
                branch_model("tenant", "tenant_notifications", &["tenant"]),
                PlannedModel {
                    kind: ModelKind::Reingestor,
                    identifier: named("tenant_partition"),
                    model: nervix_models::Model::Reingestor(CreateReingestor {
                        name: named("tenant_partition"),
                        from: ProcessorInputs::single(named("notifications")),
                        output_routes: (ProcessorOutputs::single(named("tenant_notifications")))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            })
                            .with_branch(branched_by("tenant_notifications", &["tenant"])),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                },
            ]
            .into_iter(),
        );

        assert_eq!(specs.entrypoints.len(), 1);
        assert_eq!(specs.entrypoints[0].identifier, named("tenant_partition"));
        assert_eq!(
            specs.entrypoints[0].root_relay,
            named("tenant_notifications")
        );
    }

    #[test]
    fn window_route_demands_are_offset_by_the_routes_written_before_them() {
        let input_relay = named::<RelayName>("metrics");
        let totals_relay = named::<RelayName>("metric_totals");
        let extremes_relay = named::<RelayName>("metric_extremes");
        let metric_schema = Arc::new(compile_schema(&CreateSchema {
            name: named("metric"),
            fields: vec![
                SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
            ],
        }));
        let totals_schema = Arc::new(compile_schema(&CreateSchema {
            name: named("metric_total"),
            fields: vec![
                SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("sample_count"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("first_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("total_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
            ],
        }));
        let extremes_schema = Arc::new(compile_schema(&CreateSchema {
            name: named("metric_extreme"),
            fields: vec![
                SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("max_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("min_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
            ],
        }));
        let totals_set = "SET tenant = FIRST(input.tenant), sample_count = COUNT(input.latency), \
                          first_latency = FIRST(input.latency), total_latency = SUM(input.latency)";
        let extremes_set = "SET tenant = LAST(input.tenant), max_latency = MAX(input.latency), \
                            min_latency = MIN(input.latency)";
        let node = BranchedProcessorSpec {
            kind: ModelKind::WindowProcessor,
            processor: named("route_scoped_latency"),
            input_relays: vec![input_relay.clone()],
            input_collect_policies: HashMap::default(),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),
            from_where: HashMap::default(),
            filter_where: None,
            materialized_state: Vec::new(),
            operation: BranchedProcessorOperationSpec::WindowProcessor {
                output_routes: BranchedProcessorOutputsSpec {
                    routes: vec![
                        BranchedProcessorOutputSpec {
                            relay: totals_relay.clone(),
                            construction: construction(totals_set),
                            flush_policy: Some(FlushPolicy::Immediate),
                            message_error_policy: MessageErrorPolicy::Log,
                        },
                        BranchedProcessorOutputSpec {
                            relay: extremes_relay.clone(),
                            construction: construction(extremes_set),
                            flush_policy: Some(FlushPolicy::Immediate),
                            message_error_policy: MessageErrorPolicy::Log,
                        },
                    ],
                },
                width: WindowBound::of_messages(3),
                step: WindowBound::of_messages(3),
            },
        };
        let mut relay_schemas = HashMap::default();
        relay_schemas.insert(input_relay, metric_schema);
        relay_schemas.insert(totals_relay.clone(), totals_schema);
        relay_schemas.insert(extremes_relay.clone(), extremes_schema);

        let mut templates = materialize_nodes(&[node], &relay_schemas, None)
            .expect("two-route window processor must materialize");

        let template = templates.pop().expect("one template per spec");
        let RelayProcessorOperationTemplate::WindowProcessor {
            output_routes,
            aggregate,
            compiled_aggregates,
            ..
        } = &template.operation
        else {
            panic!("expected a window processor template");
        };

        // The routes keep their written order, and each compiled program stays aligned with the
        // route it was compiled for.
        let route_relays: Vec<_> = output_routes
            .routes
            .iter()
            .map(|route| route.output_relay.clone())
            .collect();
        assert_eq!(route_relays, vec![totals_relay, extremes_relay]);
        assert_eq!(compiled_aggregates.len(), 2);

        // `FIRST(input.tenant)`, `COUNT(input.latency)`, `FIRST(input.latency)` and
        // `SUM(input.latency)` need four separate structures; `MAX` and `MIN` over the same input
        // deduplicate into one, so the second route claims two.
        assert_eq!(compiled_aggregates[0].demand_offset, 0);
        assert_eq!(compiled_aggregates[1].demand_offset, 4);
        assert_eq!(aggregate.demands().len(), 6);

        // The compiled programs own the assignments once compilation has run.
        for route in &output_routes.routes {
            assert!(route.construction.assignments.is_empty());
        }
    }
}
