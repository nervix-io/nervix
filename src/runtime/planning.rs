use nervix_models::{
    CreateBranch, OutputBranch, ProcessorInputWhere, ProcessorInputs,
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
) -> HashMap<Identifier, nervix_models::Expression> {
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
) -> HashMap<Identifier, nervix_models::Expression> {
    processor_input_where_by_relay(inputs.where_clauses())
}

fn processor_input_collect_policies(
    inputs: &ProcessorInputs,
) -> HashMap<Identifier, nervix_models::InputCollectPolicy> {
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

struct BranchEntrypoint {
    branch: Option<Identifier>,
    ttl: Option<String>,
    max_instances: Option<u64>,
}

fn branch_policy(
    branch_ref: Option<&Identifier>,
    branches: &HashMap<Identifier, CreateBranch>,
) -> (Option<Identifier>, Option<String>, Option<u64>) {
    let Some(branch_ref) = branch_ref else {
        return (None, None, None);
    };
    let branch = branches
        .get(branch_ref)
        .expect("branch references must be validated before runtime planning");
    (
        Some(branch_ref.clone()),
        Some(branch.ttl.clone()),
        branch
            .eviction
            .as_ref()
            .map(|eviction| eviction.max_instances()),
    )
}

fn branch_entrypoint(
    branch_action: &OutputBranch,
    branches: &HashMap<Identifier, CreateBranch>,
) -> BranchEntrypoint {
    let (branch, ttl, max_instances) = branch_policy(branch_action.branch(), branches);
    BranchEntrypoint {
        branch,
        ttl,
        max_instances,
    }
}

fn processor_node_spec(
    spec: BranchedProcessorSpec,
    branched_by: &nervix_models::BranchSelection,
    branches: &HashMap<Identifier, CreateBranch>,
) -> BranchedProcessorNodeSpec {
    let (branch, branch_ttl, branch_max_instances) = branch_policy(branched_by.branch(), branches);
    BranchedProcessorNodeSpec {
        spec,
        branch,
        branch_ttl,
        branch_max_instances,
    }
}

pub(in crate::runtime) fn branched_node_specs_from_scheduled_nodes(
    nodes: &[ScheduledNode],
) -> BranchedNodeSpecs {
    branched_node_specs_from_models(
        nodes
            .iter()
            .map(|node| (node.kind, node.identifier.clone(), (*node.config).clone())),
    )
}

pub(in crate::runtime) fn branched_node_specs_from_active_graph(
    graph: &ActiveGraph,
) -> BranchedNodeSpecs {
    branched_node_specs_from_models(
        graph
            .nodes()
            .into_iter()
            .map(|node| (node.kind, node.identifier, (*node.config).clone())),
    )
}

pub(in crate::runtime) fn branched_node_specs_from_models(
    nodes: impl Iterator<Item = (ModelKind, Identifier, Model)>,
) -> BranchedNodeSpecs {
    let nodes = nodes.collect::<Vec<_>>();
    let branches = nodes
        .iter()
        .filter_map(|(_, _, model)| {
            if let Model::Branch(branch) = model {
                Some((branch.name.clone(), branch.clone()))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();
    let mut processors = Vec::new();
    let mut ingestors = Vec::new();

    for (kind, identifier, model) in nodes {
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
                    let branch_action = output
                        .branch
                        .as_ref()
                        .expect("validated ingestor route must declare branch behavior");
                    let entrypoint = branch_entrypoint(branch_action, &branches);
                    ingestors.push((
                        kind,
                        identifier.clone(),
                        output.relay.clone(),
                        entrypoint.branch,
                        entrypoint.ttl,
                        entrypoint.max_instances,
                        BranchInstanceAckBoundary::Preserve,
                        output
                            .flush_policy
                            .as_ref()
                            .expect("validated ingestor output must have a flush policy")
                            .flush_each
                            .clone(),
                        output
                            .flush_policy
                            .as_ref()
                            .and_then(|policy| policy.max_batch_size.clone()),
                        output_error_policies(
                            &output.message_error_policy,
                            ingestor.general_error_policy.clone(),
                        ),
                    ));
                }
            }
            Model::Reingestor(reingestor) => {
                for output in reingestor.output_routes.outputs() {
                    let branch_action = output
                        .branch
                        .as_ref()
                        .expect("validated reingestor route must declare branch behavior");
                    let entrypoint = branch_entrypoint(branch_action, &branches);
                    ingestors.push((
                        kind,
                        identifier.clone(),
                        output.relay.clone(),
                        entrypoint.branch,
                        entrypoint.ttl,
                        entrypoint.max_instances,
                        BranchInstanceAckBoundary::Reingestor(reingestor.mode),
                        output
                            .flush_policy
                            .as_ref()
                            .expect("validated reingestor output must have a flush policy")
                            .flush_each
                            .clone(),
                        output
                            .flush_policy
                            .as_ref()
                            .and_then(|policy| policy.max_batch_size.clone()),
                        output_error_policies(
                            &output.message_error_policy,
                            GeneralErrorPolicy::Log,
                        ),
                    ));
                }
            }
            _ => {}
        }
    }

    processors.sort_by(|left, right| left.spec.processor.cmp(&right.spec.processor));

    BranchedNodeSpecs {
        entrypoints: ingestors
            .into_iter()
            .map(
                |(
                    kind,
                    identifier,
                    root_relay,
                    branch,
                    branch_ttl,
                    branch_max_instances,
                    output_ack_boundary,
                    output_flush_each,
                    output_max_batch_size,
                    error_policies,
                )| {
                    BranchedIngestorSpec {
                        kind,
                        identifier,
                        root_relay,
                        branch,
                        branch_ttl,
                        branch_max_instances,
                        output_ack_boundary,
                        output_flush_each,
                        output_max_batch_size,
                        error_policies,
                    }
                },
            )
            .collect(),
        processors,
    }
}

fn parse_optional_window_duration(
    processor: &Identifier,
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
    processor: &Identifier,
    value: &str,
    max_batch_size: Option<&str>,
) -> Result<RuntimeFlushPolicy, String> {
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
    processor: &Identifier,
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
    relay_schemas: &HashMap<Identifier, Arc<CompiledSchema>>,
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
                        width_messages: width.messages.map(|messages| messages as usize),
                        step_messages: step.messages.map(|messages| messages as usize),
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
    processor: &Identifier,
    relay_schemas: &HashMap<Identifier, Arc<CompiledSchema>>,
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
    identifier: &Identifier,
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

fn parse_branch_max_instances_setting(
    max_instances: Option<u64>,
    kind: ModelKind,
    identifier: &Identifier,
) -> Result<Option<usize>, String> {
    max_instances
        .map(|max_instances| {
            if max_instances == 0 {
                return Err(format!(
                    "invalid branch MAX INSTANCES '0' for {} '{}'",
                    kind.as_str(),
                    identifier.as_str()
                ));
            }
            usize::try_from(max_instances).map_err(|_| {
                format!(
                    "branch MAX INSTANCES '{}' for {} '{}' is too large for this runtime",
                    max_instances,
                    kind.as_str(),
                    identifier.as_str()
                )
            })
        })
        .transpose()
}

fn resolve_branch_relay_templates(
    branch_relay_ids: HashSet<Identifier>,
    model_index: &HashMap<(ModelKind, Identifier), Model>,
    relay_registries: &HashMap<Identifier, RelayRegistry>,
    relay_services: &HashMap<Identifier, Arc<RelayBoundaryServices>>,
) -> Result<
    (
        HashMap<Identifier, RelayProcessorRelayTemplate>,
        HashSet<Identifier>,
    ),
    String,
> {
    let materialized_streams = branch_relay_ids
        .iter()
        .filter_map(
            |relay| match model_index.get(&(ModelKind::Relay, relay.clone())) {
                Some(Model::Relay(model)) if model.materialized_state.is_some() => {
                    Some(relay.clone())
                }
                _ => None,
            },
        )
        .collect::<HashSet<_>>();
    let relays = branch_relay_ids
        .into_iter()
        .map(|relay| {
            match model_index.get(&(ModelKind::Relay, relay.clone())) {
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
    model_index: &HashMap<(ModelKind, Identifier), Model>,
    relay_registries: &HashMap<Identifier, RelayRegistry>,
    relay_services: &HashMap<Identifier, Arc<RelayBoundaryServices>>,
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
            source: spec.identifier.clone(),
            root_relay: spec.root_relay.clone(),
            branch: spec.branch.clone(),
            branch_ttl: parse_branch_ttl_setting(
                spec.branch_ttl.as_deref(),
                spec.kind,
                &spec.identifier,
            )?,
            branch_max_instances: parse_branch_max_instances_setting(
                spec.branch_max_instances,
                spec.kind,
                &spec.identifier,
            )?,
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
    model_index: &HashMap<(ModelKind, Identifier), Model>,
    relay_schemas: &HashMap<Identifier, Arc<CompiledSchema>>,
    relay_registries: &HashMap<Identifier, RelayRegistry>,
    relay_services: &HashMap<Identifier, Arc<RelayBoundaryServices>>,
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
        .expect("single processor spec must materialize one template");
    let mut processors = HashMap::default();
    processors.insert(spec.processor.clone(), template);
    Ok(BranchInstanceTemplate {
        source_kind: spec.kind,
        source: spec.processor.clone(),
        root_relay,
        branch: node.branch.clone(),
        branch_ttl: parse_branch_ttl_setting(
            node.branch_ttl.as_deref(),
            spec.kind,
            &spec.processor,
        )?,
        branch_max_instances: parse_branch_max_instances_setting(
            node.branch_max_instances,
            spec.kind,
            &spec.processor,
        )?,
        error_policies: spec.error_policies.clone(),
        relays,
        materialized_streams,
        processors,
    })
}

pub(in crate::runtime) fn format_branched_by(branched_by: &[Identifier]) -> String {
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
    use nervix_models::{
        CreateSchema, InferencerTensorDeclaration, InferencerTensorDimension,
        InferencerTensorElementType, InferencerTensorMapping, InferencerTensorRepresentation,
        InferencerTensorSchema, ParseAsType, SchemaField,
    };
    use triomphe::Arc;

    use super::*;

    fn identifier(value: &str) -> Identifier {
        Identifier::parse(value).expect("test identifier must be valid")
    }

    fn inferencer_tensor_schema(size: u32) -> InferencerTensorSchema {
        InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![InferencerTensorDimension::Fixed(size)],
        }
    }

    #[test]
    fn inferencer_input_mappings_compile_when_template_is_materialized() {
        let input_relay = identifier("features");
        let processor = identifier("score_model");
        let input_schema = Arc::new(compile_schema(&CreateSchema {
            name: identifier("feature_schema"),
            fields: vec![SchemaField {
                name: identifier("vector"),
                ty: ParseAsType::Array {
                    element: Box::new(ParseAsType::F32),
                    len: 2,
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
                        relay: identifier("scores"),
                        construction: RouteConstruction::default(),
                        flush_each: Some("IMMEDIATE".to_string()),
                        max_batch_size: None,
                        message_error_policy: MessageErrorPolicy::Log,
                    }],
                },
                resource: identifier("fraud_model"),
                resource_version: Some(1),
                file: "models/fraud.onnx".to_string(),
                inputs: vec![InferencerTensorMapping {
                    tensor: "features".to_string(),
                    schema: inferencer_tensor_schema(2),
                    expression: nervix_nspl::parse_expression("input.missing")
                        .expect("test expression must parse"),
                }],
                output_schema: vec![InferencerTensorDeclaration {
                    tensor: "score".to_string(),
                    schema: inferencer_tensor_schema(1),
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
}
