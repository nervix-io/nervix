use nervix_models::{
    Assignment, CorrelationTimeoutAction, CreateBranch, OutputBranch, ProcessorInputWhere,
    ProcessorInputs, ProcessorOutput as ModelProcessorOutput,
    ProcessorOutputs as ModelProcessorOutputs,
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
        children: Vec::new(),
    }
}

fn branched_outputs(outputs: &ModelProcessorOutputs) -> BranchedProcessorOutputsSpec {
    BranchedProcessorOutputsSpec {
        routes: outputs.routes.iter().map(branched_output).collect(),
    }
}

fn processor_input_where_by_relay(
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

struct BranchEntrypoint {
    ttl: Option<String>,
    max_instances: Option<u64>,
    assignments: Vec<Assignment>,
}

fn branch_entrypoint(
    branch_action: &OutputBranch,
    branches: &HashMap<Identifier, CreateBranch>,
) -> BranchEntrypoint {
    let Some(branch_ref) = branch_action.branch() else {
        return BranchEntrypoint {
            ttl: None,
            max_instances: None,
            assignments: Vec::new(),
        };
    };
    let branch = branches
        .get(branch_ref)
        .expect("branch references must be validated before runtime planning");
    BranchEntrypoint {
        ttl: Some(branch.ttl.clone()),
        max_instances: branch
            .eviction
            .as_ref()
            .map(|eviction| eviction.max_instances()),
        assignments: branch_action.assignments().to_vec(),
    }
}

pub(in crate::runtime) fn branched_ingestor_specs_from_scheduled_nodes(
    nodes: &[ScheduledNode],
) -> Vec<BranchedIngestorSpec> {
    branched_ingestor_specs_from_models(nodes.iter().map(|node| {
        (
            node.kind,
            node.identifier.clone(),
            (*node.config).clone(),
            node.effective_branching.clone(),
        )
    }))
}

pub(in crate::runtime) fn branched_ingestor_specs_from_active_graph(
    graph: &ActiveGraph,
) -> Vec<BranchedIngestorSpec> {
    branched_ingestor_specs_from_models(graph.nodes().into_iter().map(|node| {
        (
            node.kind,
            node.identifier,
            (*node.config).clone(),
            node.effective_branching,
        )
    }))
}

pub(in crate::runtime) fn branched_ingestor_specs_from_models(
    nodes: impl Iterator<Item = (ModelKind, Identifier, Model, Option<Vec<Identifier>>)>,
) -> Vec<BranchedIngestorSpec> {
    let nodes = nodes.collect::<Vec<_>>();
    let branches = nodes
        .iter()
        .filter_map(|(_, _, model, _)| {
            if let Model::Branch(branch) = model {
                Some((branch.name.clone(), branch.clone()))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();
    let mut processors_by_input = HashMap::<Identifier, Vec<BranchedProcessorSpec>>::new();
    let mut ingestors = Vec::new();
    let mut relay_roots = Vec::new();

    for (kind, identifier, model, effective_branching) in nodes {
        match &model {
            Model::Deduplicator(deduplicator) => {
                if deduplicator.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: deduplicator.from.relays().to_vec(),
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
                for from_relay in deduplicator.from.relays() {
                    processors_by_input
                        .entry(from_relay.clone())
                        .or_default()
                        .push(spec.clone());
                }
            }
            Model::Reorderer(reorderer) => {
                if reorderer.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: reorderer.from.relays().to_vec(),
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
                for from_relay in reorderer.from.relays() {
                    processors_by_input
                        .entry(from_relay.clone())
                        .or_default()
                        .push(spec.clone());
                }
            }
            Model::Correlator(correlator) => {
                let mut input_relays = Vec::with_capacity(
                    correlator.left.relays().len() + correlator.right.relays().len(),
                );
                input_relays.extend(correlator.left.relays().iter().cloned());
                input_relays.extend(correlator.right.relays().iter().cloned());
                let mut from_where = processor_input_where_by_inputs(&correlator.left);
                from_where.extend(processor_input_where_by_inputs(&correlator.right));
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: input_relays.clone(),
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
                for input_relay in input_relays {
                    processors_by_input
                        .entry(input_relay)
                        .or_default()
                        .push(spec.clone());
                }
            }
            Model::WindowProcessor(window_processor) => {
                if window_processor.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: window_processor.from.relays().to_vec(),
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
                for from_relay in window_processor.from.relays() {
                    processors_by_input
                        .entry(from_relay.clone())
                        .or_default()
                        .push(spec.clone());
                }
            }
            Model::Junction(junction) => {
                if junction.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: junction.from.relays().to_vec(),
                    mode: junction.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&junction.from),
                    filter_where: junction.filter_where.clone(),
                    materialized_state: junction.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Junction {
                        output_routes: branched_outputs(&junction.output_routes),
                    },
                };
                for from_relay in junction.from.relays() {
                    processors_by_input
                        .entry(from_relay.clone())
                        .or_default()
                        .push(spec.clone());
                }
            }
            Model::Inferencer(inferencer) => {
                if inferencer.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: inferencer.from.relays().to_vec(),
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
                for from_relay in inferencer.from.relays() {
                    processors_by_input
                        .entry(from_relay.clone())
                        .or_default()
                        .push(spec.clone());
                }
            }
            Model::WasmProcessor(processor) => {
                if processor.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: processor.from.relays().to_vec(),
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
                    },
                };
                for from_relay in processor.from.relays() {
                    processors_by_input
                        .entry(from_relay.clone())
                        .or_default()
                        .push(spec.clone());
                }
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
                        entrypoint.ttl,
                        entrypoint.max_instances,
                        Vec::new(),
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
                        entrypoint.ttl,
                        entrypoint.max_instances,
                        entrypoint.assignments,
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
            Model::Relay(_) if effective_branching.is_some() => {
                relay_roots.push((kind, identifier.clone(), identifier));
            }
            _ => {}
        }
    }

    fn build_nodes(
        relay: &Identifier,
        processors_by_input: &HashMap<Identifier, Vec<BranchedProcessorSpec>>,
    ) -> Vec<BranchedProcessorSpec> {
        let mut nodes = Vec::new();

        if let Some(processors) = processors_by_input.get(relay) {
            let mut processors = processors.clone();
            processors.sort_by(|left, right| left.processor.cmp(&right.processor));
            for mut processor in processors {
                match &mut processor.operation {
                    BranchedProcessorOperationSpec::Deduplicator { output_routes, .. }
                    | BranchedProcessorOperationSpec::WindowProcessor { output_routes, .. }
                    | BranchedProcessorOperationSpec::Reorderer { output_routes, .. }
                    | BranchedProcessorOperationSpec::Correlator { output_routes, .. }
                    | BranchedProcessorOperationSpec::Junction { output_routes, .. }
                    | BranchedProcessorOperationSpec::Inferencer { output_routes, .. }
                    | BranchedProcessorOperationSpec::WasmProcessor { output_routes, .. } => {
                        for output in output_routes.outputs_mut() {
                            output.children = build_nodes(&output.relay, processors_by_input);
                        }
                    }
                }
                nodes.push(processor);
            }
        }

        nodes
    }

    let entrypoint_relays = ingestors
        .iter()
        .map(|(_, _, root_relay, _, _, _, _, _, _, _)| root_relay.clone())
        .collect::<HashSet<_>>();
    let relay_roots = relay_roots
        .into_iter()
        .filter(|(_, _, root_relay)| {
            !entrypoint_relays.contains(root_relay) && processors_by_input.contains_key(root_relay)
        })
        .map(|(kind, identifier, root_relay)| {
            (
                kind,
                identifier,
                root_relay,
                None,
                None,
                Vec::new(),
                BranchInstanceAckBoundary::Preserve,
                "IMMEDIATE".to_string(),
                None,
                ErrorPolicies::handled_by_log(),
            )
        });

    ingestors
        .into_iter()
        .chain(relay_roots)
        .map(
            |(
                kind,
                identifier,
                root_relay,
                branch_ttl,
                branch_max_instances,
                entrypoint_branch_assignments,
                entrypoint_ack_boundary,
                entrypoint_flush_each,
                entrypoint_max_batch_size,
                error_policies,
            )| {
                BranchedIngestorSpec {
                    kind,
                    identifier,
                    root_relay: root_relay.clone(),
                    branch_ttl,
                    branch_max_instances,
                    entrypoint_branch_assignments,
                    entrypoint_ack_boundary,
                    entrypoint_flush_each,
                    entrypoint_max_batch_size,
                    error_policies,
                    processors: collect_reachable_processors(&root_relay, &processors_by_input),
                    roots: build_nodes(&root_relay, &processors_by_input),
                }
            },
        )
        .collect()
}

pub(in crate::runtime) fn collect_reachable_processors(
    root_relay: &Identifier,
    processors_by_input: &HashMap<Identifier, Vec<BranchedProcessorSpec>>,
) -> Vec<BranchedProcessorSpec> {
    fn visit_stream(
        relay: &Identifier,
        processors_by_input: &HashMap<Identifier, Vec<BranchedProcessorSpec>>,
        seen_processors: &mut HashSet<Identifier>,
        out: &mut Vec<BranchedProcessorSpec>,
    ) {
        let Some(processors) = processors_by_input.get(relay) else {
            return;
        };
        let mut processors = processors.clone();
        processors.sort_by(|left, right| left.processor.cmp(&right.processor));
        for processor in processors {
            if !seen_processors.insert(processor.processor.clone()) {
                continue;
            }
            match &processor.operation {
                BranchedProcessorOperationSpec::Deduplicator { output_routes, .. }
                | BranchedProcessorOperationSpec::Reorderer { output_routes, .. }
                | BranchedProcessorOperationSpec::Correlator { output_routes, .. }
                | BranchedProcessorOperationSpec::WindowProcessor { output_routes, .. }
                | BranchedProcessorOperationSpec::Junction { output_routes, .. }
                | BranchedProcessorOperationSpec::Inferencer { output_routes, .. }
                | BranchedProcessorOperationSpec::WasmProcessor { output_routes, .. } => {
                    for output in output_routes.outputs() {
                        visit_stream(&output.relay, processors_by_input, seen_processors, out);
                    }
                }
            }
            out.push(processor);
        }
    }

    let mut seen_processors = HashSet::default();
    let mut out = Vec::new();
    visit_stream(
        root_relay,
        processors_by_input,
        &mut seen_processors,
        &mut out,
    );
    out.sort_by(|left, right| left.processor.cmp(&right.processor));
    out
}

pub(in crate::runtime) fn branched_processor_ids(
    specs: &[BranchedIngestorSpec],
) -> HashSet<Identifier> {
    let mut ids = HashSet::default();
    for spec in specs {
        ids.extend(spec.processors.iter().map(|node| node.processor.clone()));
    }
    ids
}

pub(in crate::runtime) fn materialize_branch_instance_template(
    spec: &BranchedIngestorSpec,
    model_index: &HashMap<(ModelKind, Identifier), Model>,
    relay_schemas: &HashMap<Identifier, Arc<CompiledSchema>>,
    relay_registries: &HashMap<Identifier, RelayRegistry>,
    relay_services: &HashMap<Identifier, Arc<RelayBoundaryServices>>,
) -> Result<BranchInstanceTemplate, String> {
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

    fn materialize_output(
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

    fn materialize_nodes(
        nodes: &[BranchedProcessorSpec],
        relay_schemas: &HashMap<Identifier, Arc<CompiledSchema>>,
    ) -> Result<Vec<RelayProcessorTemplate>, String> {
        let mut out = Vec::new();
        for node in nodes {
            out.push(RelayProcessorTemplate {
                kind: node.kind,
                processor: node.processor.clone(),
                input_relays: node.input_relays.clone(),
                mode: node.mode,
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
                    } => RelayProcessorOperationTemplate::Inferencer {
                        output_routes: materialize_outputs(output_routes)?,
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
                        inputs: inputs.clone(),
                        output_schema: output_schema.clone(),
                    },
                    BranchedProcessorOperationSpec::WasmProcessor {
                        output_routes,
                        resource,
                        resource_version,
                        file,
                    } => RelayProcessorOperationTemplate::WasmProcessor {
                        output_routes: materialize_outputs(output_routes)?,
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
                        compiled: None,
                    },
                },
            });
        }
        Ok(out)
    }

    let mut branch_relay_ids = HashSet::default();
    branch_relay_ids.insert(spec.root_relay.clone());
    for processor in &spec.processors {
        branch_relay_ids.extend(processor.input_relays.iter().cloned());
        match &processor.operation {
            BranchedProcessorOperationSpec::Deduplicator { output_routes, .. }
            | BranchedProcessorOperationSpec::Reorderer { output_routes, .. }
            | BranchedProcessorOperationSpec::WindowProcessor { output_routes, .. }
            | BranchedProcessorOperationSpec::Junction { output_routes, .. }
            | BranchedProcessorOperationSpec::Inferencer { output_routes, .. }
            | BranchedProcessorOperationSpec::WasmProcessor { output_routes, .. } => {
                branch_relay_ids.extend(output_routes.outputs().map(|output| output.relay.clone()));
            }
            BranchedProcessorOperationSpec::Correlator {
                output_routes,
                timeout_policy,
                ..
            } => {
                branch_relay_ids.extend(output_routes.outputs().map(|output| output.relay.clone()));
                if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.left {
                    branch_relay_ids.insert(relay.clone());
                }
                if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.right {
                    branch_relay_ids.insert(relay.clone());
                }
            }
        }
    }
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
    let branch_ttl = spec
        .branch_ttl
        .as_deref()
        .map(|ttl| {
            humantime::parse_duration(ttl).map_err(|error| {
                format!(
                    "invalid branch ttl '{}' for {} '{}': {}",
                    ttl,
                    spec.kind.as_str(),
                    spec.identifier.as_str(),
                    error
                )
            })
        })
        .transpose()?;
    let branch_max_instances = spec
        .branch_max_instances
        .map(|max_instances| {
            if max_instances == 0 {
                return Err(format!(
                    "invalid branch MAX INSTANCES '0' for {} '{}'",
                    spec.kind.as_str(),
                    spec.identifier.as_str()
                ));
            }
            usize::try_from(max_instances).map_err(|_| {
                format!(
                    "branch MAX INSTANCES '{}' for {} '{}' is too large for this runtime",
                    max_instances,
                    spec.kind.as_str(),
                    spec.identifier.as_str()
                )
            })
        })
        .transpose()?;
    let processors = materialize_nodes(&spec.processors, relay_schemas)?
        .into_iter()
        .map(|processor| (processor.processor.clone(), processor))
        .collect::<HashMap<_, _>>();
    let mut processors_by_input = HashMap::<Identifier, Vec<Identifier>>::new();
    for processor in &spec.processors {
        for input_relay in &processor.input_relays {
            processors_by_input
                .entry(input_relay.clone())
                .or_default()
                .push(processor.processor.clone());
        }
    }
    for processors in processors_by_input.values_mut() {
        processors.sort();
        processors.dedup();
    }
    Ok(BranchInstanceTemplate {
        source_kind: spec.kind,
        source: spec.identifier.clone(),
        root_relay: spec.root_relay.clone(),
        branch_ttl,
        branch_max_instances,
        entrypoint_branch_assignments: spec.entrypoint_branch_assignments.clone(),
        entrypoint_ack_boundary: spec.entrypoint_ack_boundary,
        entrypoint_flush_each: parse_branch_flush_policy(
            spec.kind.as_str(),
            &spec.identifier,
            &spec.entrypoint_flush_each,
            spec.entrypoint_max_batch_size.as_deref(),
        )?,
        error_policies: spec.error_policies.clone(),
        relays,
        materialized_streams,
        processors,
        processors_by_input,
    })
}

#[cfg(test)]
pub(in crate::runtime) fn resolve_concrete_branch(
    record: &RuntimeRecord,
    branched_by: &[Identifier],
    owner: &Identifier,
) -> Result<ConcreteBranch, String> {
    if branched_by.is_empty() {
        return Ok(ConcreteBranch::Root);
    }

    let mut fields = Vec::with_capacity(branched_by.len());
    for field_name in branched_by {
        let Some(value) = record.value(field_name.as_str()) else {
            return Err(format!(
                "branch field '{}' is missing for '{}'",
                field_name.as_str(),
                owner.as_str()
            ));
        };
        fields.push((field_name.clone(), value.clone()));
    }

    BranchKey::from_fields(fields).map(ConcreteBranch::Key)
}

pub(in crate::runtime) fn resolve_concrete_branch_from_assignments(
    output: &RuntimeRecord,
    input: Option<&RuntimeRecord>,
    branch_key: Option<&BranchKey>,
    assignments: &[Assignment],
    owner: &Identifier,
) -> Result<ConcreteBranch, String> {
    if assignments.is_empty() {
        return Ok(branch_key
            .cloned()
            .map(ConcreteBranch::Key)
            .unwrap_or(ConcreteBranch::Root));
    }

    let mut fields = Vec::<(Identifier, RuntimeValue)>::with_capacity(assignments.len());
    let mut initialized = HashMap::<Identifier, RuntimeValue>::default();
    for (index, assignment) in assignments.iter().enumerate() {
        let value = evaluate_branch_expression(&assignment.value, output, input, &initialized)
            .map_err(|reason| {
                format!(
                    "branch SET assignment {index} for '{}' failed: {reason}",
                    owner.as_str()
                )
            })?;
        if let Some((_, current)) = fields
            .iter_mut()
            .find(|(field, _)| field == &assignment.target.field)
        {
            *current = value.clone();
        } else {
            fields.push((assignment.target.field.clone(), value.clone()));
        }
        initialized.insert(assignment.target.field.clone(), value);
    }

    BranchKey::from_fields(fields).map(ConcreteBranch::Key)
}

fn evaluate_branch_expression(
    expression: &nervix_models::Expression,
    output: &RuntimeRecord,
    input: Option<&RuntimeRecord>,
    initialized: &HashMap<Identifier, RuntimeValue>,
) -> Result<RuntimeValue, String> {
    use nervix_models::{Expression, FieldScope, Literal, ParseAsType, UnaryOperator};

    match expression {
        Expression::Literal(Literal::I64(value)) => Ok(RuntimeValue::I64(*value)),
        Expression::Literal(Literal::F64(value)) => {
            Ok(RuntimeValue::F64(OrderedFloat(value.value())))
        }
        Expression::Literal(Literal::Bool(value)) => Ok(RuntimeValue::Bool(*value)),
        Expression::Literal(Literal::String(value)) => Ok(RuntimeValue::String(value.clone())),
        Expression::Literal(Literal::Null) => {
            Err("NULL cannot be used as a concrete branch-key value".to_string())
        }
        Expression::Field(reference) => match &reference.scope {
            FieldScope::Bare | FieldScope::Branch => {
                initialized.get(&reference.field).cloned().ok_or_else(|| {
                    format!(
                        "branch field '{}' has not been initialized",
                        reference.field.as_str()
                    )
                })
            }
            FieldScope::Message | FieldScope::Output => output
                .value(reference.field.as_str())
                .cloned()
                .ok_or_else(|| format!("field '{}' is missing", reference.field.as_str())),
            FieldScope::Input => input
                .unwrap_or(output)
                .value(reference.field.as_str())
                .cloned()
                .ok_or_else(|| format!("input field '{}' is missing", reference.field.as_str())),
            FieldScope::RelayState { relay } => output
                .value(&format!(
                    "relay_state.{}.{}",
                    relay.as_str(),
                    reference.field.as_str()
                ))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "materialized state field 'relay_state.{}.{}' is missing",
                        relay.as_str(),
                        reference.field.as_str()
                    )
                }),
            scope => Err(format!(
                "scope '{scope:?}' is unavailable during branch construction"
            )),
        },
        Expression::Unary {
            operator,
            expression,
        } => {
            let value = evaluate_branch_expression(expression, output, input, initialized)?;
            match operator {
                UnaryOperator::Negate => negate_runtime_value(value),
                UnaryOperator::Not => match value {
                    RuntimeValue::Bool(value) => Ok(RuntimeValue::Bool(!value)),
                    other => Err(format!(
                        "NOT expects BOOL, found {}",
                        runtime_value_type_name(&other)
                    )),
                },
            }
        }
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            let left = evaluate_branch_expression(left, output, input, initialized)?;
            let right = evaluate_branch_expression(right, output, input, initialized)?;
            let operator = match operator {
                nervix_models::BinaryOperator::Add => BinaryOp::Add,
                nervix_models::BinaryOperator::Subtract => BinaryOp::Sub,
                nervix_models::BinaryOperator::Multiply => BinaryOp::Mul,
                nervix_models::BinaryOperator::Divide => BinaryOp::Div,
                nervix_models::BinaryOperator::Remainder => BinaryOp::Rem,
                nervix_models::BinaryOperator::Equal => BinaryOp::Eq,
                nervix_models::BinaryOperator::NotEqual => BinaryOp::NotEq,
                nervix_models::BinaryOperator::GreaterThan => BinaryOp::Gt,
                nervix_models::BinaryOperator::LessThan => BinaryOp::Lt,
                nervix_models::BinaryOperator::GreaterThanOrEqual => BinaryOp::GtEq,
                nervix_models::BinaryOperator::LessThanOrEqual => BinaryOp::LtEq,
                nervix_models::BinaryOperator::And => BinaryOp::And,
                nervix_models::BinaryOperator::Or => BinaryOp::Or,
            };
            evaluate_runtime_binary(operator, left, right)
        }
        Expression::Cast { expression, target } => {
            let value = evaluate_branch_expression(expression, output, input, initialized)?;
            let target = match target {
                ParseAsType::U8 => ArrowDataType::UInt8,
                ParseAsType::I8 => ArrowDataType::Int8,
                ParseAsType::U16 => ArrowDataType::UInt16,
                ParseAsType::I16 => ArrowDataType::Int16,
                ParseAsType::U32 => ArrowDataType::UInt32,
                ParseAsType::I32 => ArrowDataType::Int32,
                ParseAsType::U64 => ArrowDataType::UInt64,
                ParseAsType::I64 => ArrowDataType::Int64,
                ParseAsType::Bool => ArrowDataType::Boolean,
                ParseAsType::String => ArrowDataType::Utf8,
                ParseAsType::Datetime => ArrowDataType::Timestamp(
                    arrow_schema::TimeUnit::Nanosecond,
                    Some("+00:00".into()),
                ),
                ParseAsType::F32 => ArrowDataType::Float32,
                ParseAsType::F64 => ArrowDataType::Float64,
                other => return Err(format!("branch SET cannot cast to {other:?}")),
            };
            cast_runtime_value(value, &target)
        }
        Expression::Call {
            function,
            arguments,
        } => {
            let values = arguments
                .iter()
                .map(|argument| evaluate_branch_expression(argument, output, input, initialized))
                .collect::<Result<Vec<_>, _>>()?;
            match function.as_str().to_ascii_lowercase().as_str() {
                "leak_sensitive" => values
                    .into_iter()
                    .next()
                    .ok_or_else(|| "leak_sensitive expects one argument".to_string()),
                "lower" => unary_string_function(values, "lower", str::to_ascii_lowercase),
                "upper" => unary_string_function(values, "upper", str::to_ascii_uppercase),
                "trim" => unary_string_function(values, "trim", |value| value.trim().to_string()),
                "abs" => values
                    .into_iter()
                    .next()
                    .ok_or_else(|| "abs expects one argument".to_string())
                    .and_then(abs_runtime_value),
                "length" => match values.as_slice() {
                    [RuntimeValue::String(value)] => {
                        Ok(RuntimeValue::I64(value.chars().count() as i64))
                    }
                    [other] => Err(format!(
                        "length expects STRING, found {}",
                        runtime_value_type_name(other)
                    )),
                    _ => Err("length expects one argument".to_string()),
                },
                "concat" => Ok(RuntimeValue::String(
                    values
                        .iter()
                        .map(RuntimeValue::to_key_fragment)
                        .collect::<String>(),
                )),
                other => Err(format!(
                    "function '{other}' is unavailable during branch construction"
                )),
            }
        }
        Expression::Array(_) => {
            Err("array expressions are unavailable during branch construction".to_string())
        }
    }
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
