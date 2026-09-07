use std::num::NonZeroU64;

use nervix_models::{
    BranchName, CreateBranch, ModelName, ProcessorInputWhere, ProcessorInputs,
    ProcessorOutput as ModelProcessorOutput, ProcessorOutputs as ModelProcessorOutputs,
};

use super::*;

fn branched_output(output: &ModelProcessorOutput) -> BranchedProcessorOutputSpec {
    BranchedProcessorOutputSpec {
        relay: output.relay.clone(),
        construction: output.construction.clone(),
        flush_each: output
            .flush_policy
            .as_ref()
            .map(|policy| policy.flush_each.clone()),
        max_batch_size: output
            .flush_policy
            .as_ref()
            .and_then(|policy| policy.max_batch_size.clone()),
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
        kind: node.kind,
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
                        output_flush_each: output
                            .flush_policy
                            .as_ref()
                            .verified(
                                "the registry requires a flush policy on every flush-based output \
                                 route",
                            )
                            .flush_each
                            .clone(),
                        output_max_batch_size: output
                            .flush_policy
                            .as_ref()
                            .and_then(|policy| policy.max_batch_size.clone()),
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
                        output_flush_each: output
                            .flush_policy
                            .as_ref()
                            .verified(
                                "the registry requires a flush policy on every flush-based output \
                                 route",
                            )
                            .flush_each
                            .clone(),
                        output_max_batch_size: output
                            .flush_policy
                            .as_ref()
                            .and_then(|policy| policy.max_batch_size.clone()),
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
    setting: &str,
    value: Option<&str>,
) -> Result<Option<Duration>, String> {
    value
        .map(|raw| {
            humantime::parse_duration(raw).map_err(|error| {
                format!(
                    "invalid window processor '{}' {} duration '{}': {}",
                    processor.as_str(),
                    setting,
                    raw,
                    error
                )
            })
        })
        .transpose()
}

pub(in crate::runtime) fn materialize_output(
    output: &BranchedProcessorOutputSpec,
) -> Result<RelayProcessorOutputTemplate, String> {
    Ok(RelayProcessorOutputTemplate {
        output_relay: output.relay.clone(),
        construction: output.construction.clone(),
        flush_policy: output
            .flush_each
            .as_deref()
            .map(|flush_each| {
                parse_branch_flush_policy(
                    "processor output",
                    &output.relay,
                    flush_each,
                    output.max_batch_size.as_deref(),
                )
            })
            .transpose()?,
        message_error_policy: output.message_error_policy.clone(),
    })
}

fn materialize_outputs(
    outputs: &BranchedProcessorOutputsSpec,
) -> Result<RelayProcessorOutputsTemplate, String> {
    Ok(RelayProcessorOutputsTemplate {
        routes: outputs
            .routes
            .iter()
            .map(materialize_output)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn parse_branch_flush_policy(
    kind: &str,
    processor: impl Into<ModelName>,
    value: &str,
    max_batch_size: Option<&str>,
) -> Result<RuntimeFlushPolicy, String> {
    let processor = processor.into();
    if value.eq_ignore_ascii_case("IMMEDIATE") {
        return Ok(RuntimeFlushPolicy::Immediate);
    }
    let interval = humantime::parse_duration(value).map_err(|error| {
        format!(
            "invalid {} '{}' flush_each duration '{}': {}",
            kind,
            processor.as_str(),
            value,
            error
        )
    })?;
    let max_batch_size = max_batch_size.ok_or_else(|| {
        format!(
            "{} '{}' FLUSH EACH requires MAX BATCH SIZE",
            kind,
            processor.as_str()
        )
    })?;
    let max_batch_size = max_batch_size.parse::<ubyte::ByteUnit>().map_err(|error| {
        format!(
            "invalid {} '{}' max_batch_size '{}': {}",
            kind,
            processor.as_str(),
            max_batch_size,
            error
        )
    })?;
    Ok(RuntimeFlushPolicy::Each {
        interval,
        max_batch_size: max_batch_size.as_u64(),
    })
}

pub(in crate::runtime) fn parse_input_collect_policy(
    kind: &str,
    processor: &ModelName,
    policy: &nervix_models::InputCollectPolicy,
) -> Result<RuntimeInputCollectPolicy, String> {
    let interval = humantime::parse_duration(&policy.collect_for).map_err(|error| {
        format!(
            "invalid {} '{}' COLLECT FOR duration '{}': {}",
            kind,
            processor.as_str(),
            policy.collect_for,
            error
        )
    })?;
    let max_batch_size = policy
        .max_batch_size
        .as_deref()
        .map(|max_batch_size| {
            max_batch_size
                .parse::<ubyte::ByteUnit>()
                .map(|size| size.as_u64())
                .map_err(|error| {
                    format!(
                        "invalid {} '{}' COLLECT MAX BATCH SIZE '{}': {}",
                        kind,
                        processor.as_str(),
                        max_batch_size,
                        error
                    )
                })
        })
        .transpose()?;
    Ok(RuntimeInputCollectPolicy {
        interval,
        max_batch_size,
    })
}

fn materialize_nodes(
    nodes: &[BranchedProcessorSpec],
    relay_schemas: &HashMap<RelayName, Arc<CompiledSchema>>,
    udfs: Option<&UdfExecutor>,
) -> Result<Vec<RelayProcessorTemplate>, String> {
    let mut out = Vec::new();
    for node in nodes {
        out.push(RelayProcessorTemplate {
            kind: node.kind,
            processor: node.processor.clone(),
            input_relays: node.input_relays.clone(),
            input_collect_policies: node
                .input_collect_policies
                .iter()
                .map(|(relay, policy)| {
                    parse_input_collect_policy(node.kind.as_str(), &node.processor, policy)
                        .map(|policy| (relay.clone(), policy))
                })
                .collect::<Result<HashMap<_, _>, _>>()?,
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
                    output_routes: materialize_outputs(output_routes)?,
                    deduplicate_on: deduplicate_on.clone(),
                    max_time: humantime::parse_duration(max_time).map_err(|error| {
                        format!(
                            "invalid deduplicator '{}' MAX TIME duration '{}': {}",
                            node.processor.as_str(),
                            max_time,
                            error
                        )
                    })?,
                },
                BranchedProcessorOperationSpec::WindowProcessor {
                    output_routes,
                    width,
                    step,
                } => {
                    if output_routes.outputs().next().is_none() {
                        return Err(format!(
                            "window processor '{}' requires an output relay",
                            node.processor.as_str()
                        ));
                    }
                    let route_aggregates = output_routes
                        .outputs()
                        .map(|output| {
                            lower_window_assignments(&output.construction)
                                .map(|aggregate| aggregate.inner)
                                .map_err(|reason| {
                                    format!(
                                        "window processor '{}' output '{}' construction is \
                                         invalid: {}",
                                        node.processor.as_str(),
                                        output.relay.as_str(),
                                        reason
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let mut demand_offset = 0;
                    let compiled_aggregates = output_routes
                        .outputs()
                        .zip(&route_aggregates)
                        .map(|(output, aggregate)| {
                            let compiled = CompiledWindowAggregateProgram::compile(
                                aggregate,
                                &node.input_relays,
                                &output.relay,
                                relay_schemas,
                                udfs,
                            )?
                            .with_demand_offset(demand_offset);
                            demand_offset += aggregate.demands().len();
                            Ok(compiled)
                        })
                        .collect::<Result<Vec<_>, String>>()?;
                    let aggregate =
                        WindowAggregateProgram::combine_route_programs(&route_aggregates);
                    let mut materialized_outputs = materialize_outputs(output_routes)?;
                    for output in &mut materialized_outputs.routes {
                        output.construction.assignments.clear();
                    }
                    RelayProcessorOperationTemplate::WindowProcessor {
                        output_routes: materialized_outputs,
                        width_messages: width.messages.map(|messages| messages.arch_into()),
                        step_messages: step.messages.map(|messages| messages.arch_into()),
                        width_duration: parse_optional_window_duration(
                            &node.processor,
                            "width",
                            width.duration.as_deref(),
                        )?,
                        step_duration: parse_optional_window_duration(
                            &node.processor,
                            "step",
                            step.duration.as_deref(),
                        )?,
                        aggregate,
                        compiled_aggregates,
                    }
                }
                BranchedProcessorOperationSpec::Reorderer {
                    output_routes,
                    order_by,
                    max_time,
                } => RelayProcessorOperationTemplate::Reorderer {
                    output_routes: materialize_outputs(output_routes)?,
                    order_by: order_by.clone(),
                    max_time: humantime::parse_duration(max_time).map_err(|error| {
                        format!(
                            "invalid reorderer '{}' MAX TIME duration '{}': {}",
                            node.processor.as_str(),
                            max_time,
                            error
                        )
                    })?,
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
                    output_routes: materialize_outputs(output_routes)?,
                    left_relays: left_relays.clone(),
                    right_relays: right_relays.clone(),
                    correlate_where: correlate_where.clone(),
                    match_policy: *match_policy,
                    max_time: humantime::parse_duration(max_time).map_err(|error| {
                        format!(
                            "invalid correlator '{}' MAX TIME duration '{}': {}",
                            node.processor.as_str(),
                            max_time,
                            error
                        )
                    })?,
                    timeout_policy: timeout_policy.clone(),
                },
                BranchedProcessorOperationSpec::Junction { output_routes } => {
                    RelayProcessorOperationTemplate::Junction {
                        output_routes: materialize_outputs(output_routes)?,
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
                    let input_relay = node.input_relays.first().ok_or_else(|| {
                        format!(
                            "inferencer '{}' requires an input relay",
                            node.processor.as_str()
                        )
                    })?;
                    let input_schema = relay_schemas.get(input_relay).ok_or_else(|| {
                        format!(
                            "inferencer '{}' input relay '{}' has no runtime schema",
                            node.processor.as_str(),
                            input_relay.as_str()
                        )
                    })?;
                    let compiled_input_program = CompiledInferencerInputProgram::compile(
                        &node.processor,
                        inputs,
                        input_schema,
                        udfs,
                    )?;
                    RelayProcessorOperationTemplate::Inferencer {
                        output_routes: materialize_outputs(output_routes)?,
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
                    output_routes: materialize_outputs(output_routes)?,
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
) -> Result<RelayProcessorTemplate, String> {
    let specs = branched_node_specs_from_active_graph(graph);
    let node = specs.processor(kind, processor).ok_or_else(|| {
        format!(
            "{} '{}' has no scheduled processor specification",
            kind.as_str(),
            processor.as_str()
        )
    })?;
    materialize_nodes(std::slice::from_ref(&node.spec), relay_schemas, udfs)?
        .pop()
        .ok_or_else(|| {
            format!(
                "{} '{}' did not produce a processor template",
                kind.as_str(),
                processor.as_str()
            )
        })
}

fn parse_branch_ttl_setting(
    ttl: Option<&str>,
    kind: ModelKind,
    identifier: &ModelName,
) -> Result<Option<Duration>, String> {
    ttl.map(|ttl| {
        humantime::parse_duration(ttl).map_err(|error| {
            format!(
                "invalid branch ttl '{}' for {} '{}': {}",
                ttl,
                kind.as_str(),
                identifier.as_str(),
                error
            )
        })
    })
    .transpose()
}

fn resolve_branch_relay_templates(
    branch_relay_ids: HashSet<RelayName>,
    model_index: &HashMap<NodeRef, Model>,
    relay_registries: &HashMap<RelayName, RelayRegistry>,
    relay_services: &HashMap<RelayName, Arc<RelayBoundaryServices>>,
) -> Result<
    (
        HashMap<RelayName, RelayProcessorRelayTemplate>,
        HashSet<RelayName>,
    ),
    String,
> {
    let materialized_streams = branch_relay_ids
        .iter()
        .filter_map(|relay| {
            match model_index.get(&NodeRef {
                kind: ModelKind::Relay,
                identifier: ModelName::from(relay),
            }) {
                Some(Model::Relay(model)) if model.materialized_state.is_some() => {
                    Some(relay.clone())
                }
                _ => None,
            }
        })
        .collect::<HashSet<_>>();
    let relays = branch_relay_ids
        .into_iter()
        .map(|relay| {
            match model_index.get(&NodeRef {
                kind: ModelKind::Relay,
                identifier: ModelName::from(&relay),
            }) {
                Some(Model::Relay(_)) => {}
                Some(model) => {
                    return Err(format!(
                        "expected relay model for '{}', found '{}'",
                        relay.as_str(),
                        model.kind().as_str()
                    ));
                }
                None => {
                    return Err(format!("missing branched relay '{}'", relay.as_str()));
                }
            }
            let registry = relay_registries
                .get(&relay)
                .cloned()
                .ok_or_else(|| format!("missing branched relay '{}'", relay.as_str()))?;
            let services = relay_services
                .get(&relay)
                .cloned()
                .ok_or_else(|| format!("missing branched relay services '{}'", relay.as_str()))?;
            Ok((relay, RelayProcessorRelayTemplate { registry, services }))
        })
        .collect::<Result<HashMap<_, _>, String>>()?;
    Ok((relays, materialized_streams))
}

pub(in crate::runtime) fn materialize_ingestor_route_template(
    spec: &BranchedIngestorSpec,
    model_index: &HashMap<NodeRef, Model>,
    relay_registries: &HashMap<RelayName, RelayRegistry>,
    relay_services: &HashMap<RelayName, Arc<RelayBoundaryServices>>,
) -> Result<IngestorRouteTemplate, String> {
    let mut branch_relay_ids = HashSet::default();
    branch_relay_ids.insert(spec.root_relay.clone());
    let (relays, materialized_streams) = resolve_branch_relay_templates(
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
            materialized_streams,
            processors: HashMap::default(),
        },
        ack_boundary: spec.output_ack_boundary,
        flush_policy: parse_branch_flush_policy(
            spec.kind.as_str(),
            &spec.identifier,
            &spec.output_flush_each,
            spec.output_max_batch_size.as_deref(),
        )?,
    })
}

pub(in crate::runtime) fn materialize_processor_instance_template(
    node: &BranchedProcessorNodeSpec,
    model_index: &HashMap<NodeRef, Model>,
    relay_schemas: &HashMap<RelayName, Arc<CompiledSchema>>,
    relay_registries: &HashMap<RelayName, RelayRegistry>,
    relay_services: &HashMap<RelayName, Arc<RelayBoundaryServices>>,
    udfs: Option<&UdfExecutor>,
) -> Result<BranchInstanceTemplate, String> {
    let spec = &node.spec;
    let root_relay = spec.input_relays.first().cloned().ok_or_else(|| {
        format!(
            "{} '{}' requires at least one input relay",
            spec.kind.as_str(),
            spec.processor.as_str()
        )
    })?;
    let (relays, materialized_streams) = resolve_branch_relay_templates(
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
        materialized_streams,
        processors,
    })
}

pub(in crate::runtime) fn format_branched_by(branched_by: &[FieldName]) -> String {
    if branched_by.is_empty() {
        "()".to_string()
    } else {
        format!(
            "({})",
            branched_by
                .iter()
                .map(|field| field.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
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
                        flush_each: Some("IMMEDIATE".to_string()),
                        max_batch_size: None,
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

        assert!(
            error.contains("inferencer 'score_model' INPUTS compile failed"),
            "unexpected error: {error}"
        );
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                        flush_each: "100ms".to_string(),
                        max_batch_size: Some("1MiB".to_string()),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
                            .with_flush_policy("IMMEDIATE".to_string(), None),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
        assert_eq!(output.flush_each.as_deref(), Some("IMMEDIATE"));
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
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
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
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
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
}
