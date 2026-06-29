use nervix_models::{
    CorrelationTimeoutAction, ParameterValueMapping, ProcessorInputWhere,
    ProcessorOutput as ModelProcessorOutput, ProcessorOutputs as ModelProcessorOutputs,
};

use super::*;

fn parameterized_output(output: &ModelProcessorOutput) -> ParameterizedProcessorOutputSpec {
    ParameterizedProcessorOutputSpec {
        relay: output.relay.clone(),
        filter_map: output.filter_map.clone(),
        children: Vec::new(),
    }
}

fn parameterized_outputs(outputs: &ModelProcessorOutputs) -> ParameterizedProcessorOutputsSpec {
    ParameterizedProcessorOutputsSpec {
        routes: outputs.routes.iter().map(parameterized_output).collect(),
    }
}

fn processor_input_where_by_relay(
    from_where: &[ProcessorInputWhere],
) -> HashMap<Identifier, String> {
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

pub(in crate::runtime) fn parameterized_ingestor_specs_from_scheduled_nodes(
    nodes: &[ScheduledNode],
) -> Vec<ParameterizedIngestorSpec> {
    parameterized_ingestor_specs_from_models(nodes.iter().map(|node| {
        (
            node.kind,
            node.identifier.clone(),
            (*node.config).clone(),
            node.effective_parameterization.clone(),
        )
    }))
}

pub(in crate::runtime) fn parameterized_ingestor_specs_from_active_graph(
    graph: &ActiveGraph,
) -> Vec<ParameterizedIngestorSpec> {
    parameterized_ingestor_specs_from_models(graph.nodes().into_iter().map(|node| {
        (
            node.kind,
            node.identifier,
            (*node.config).clone(),
            node.effective_parameterization,
        )
    }))
}

pub(in crate::runtime) fn parameterized_ingestor_specs_from_models(
    nodes: impl Iterator<Item = (ModelKind, Identifier, Model, Option<Vec<Identifier>>)>,
) -> Vec<ParameterizedIngestorSpec> {
    let mut processors_by_input = HashMap::<Identifier, Vec<ParameterizedProcessorSpec>>::new();
    let mut ingestors = Vec::new();
    let mut relay_roots = Vec::new();

    for (kind, identifier, model, effective_parameterization) in nodes {
        match &model {
            Model::Deduplicator(deduplicator) => {
                processors_by_input
                    .entry(deduplicator.from_relay.clone())
                    .or_default()
                    .push(ParameterizedProcessorSpec {
                        kind,
                        processor: identifier,
                        input_relay: deduplicator.from_relay.clone(),
                        input_relays: vec![deduplicator.from_relay.clone()],
                        mode: deduplicator.mode,
                        error_policies: message_only_error_policies(
                            &deduplicator.message_error_policy,
                        ),
                        from_where: processor_input_where_by_relay(&deduplicator.from_where),
                        filter_where: deduplicator.filter_where.clone(),
                        operation: ParameterizedProcessorOperationSpec::Deduplicator {
                            output_routes: parameterized_outputs(&deduplicator.output_routes),
                            deduplicate_on: deduplicator.deduplicate_on.clone(),
                            max_time: deduplicator.max_time.clone(),
                        },
                    });
            }
            Model::Reorderer(reorderer) => {
                processors_by_input
                    .entry(reorderer.from_relay.clone())
                    .or_default()
                    .push(ParameterizedProcessorSpec {
                        kind,
                        processor: identifier,
                        input_relay: reorderer.from_relay.clone(),
                        input_relays: vec![reorderer.from_relay.clone()],
                        mode: reorderer.mode,
                        error_policies: message_only_error_policies(
                            &reorderer.message_error_policy,
                        ),
                        from_where: processor_input_where_by_relay(&reorderer.from_where),
                        filter_where: reorderer.filter_where.clone(),
                        operation: ParameterizedProcessorOperationSpec::Reorderer {
                            output_routes: parameterized_outputs(&reorderer.output_routes),
                            order_by: reorderer.order_by.clone(),
                            max_time: reorderer.max_time.clone(),
                            flush_each: reorderer.flush_each.clone(),
                            max_batch_size: reorderer.max_batch_size.clone(),
                        },
                    });
            }
            Model::Correlator(correlator) => {
                let spec = ParameterizedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relay: correlator.left_relay.clone(),
                    input_relays: vec![
                        correlator.left_relay.clone(),
                        correlator.right_relay.clone(),
                    ],
                    mode: correlator.mode,
                    error_policies: message_only_error_policies(&correlator.message_error_policy),
                    from_where: processor_input_where_by_relay(&correlator.from_where),
                    filter_where: correlator.filter_where.clone(),
                    operation: ParameterizedProcessorOperationSpec::Correlator {
                        output_routes: parameterized_outputs(&correlator.output_routes),
                        left_relay: correlator.left_relay.clone(),
                        right_relay: correlator.right_relay.clone(),
                        correlate_where: correlator.correlate_where.clone(),
                        match_policy: correlator.match_policy,
                        output_assignments: correlator.output.clone(),
                        max_time: correlator.max_time.clone(),
                        flush_each: correlator.flush_each.clone(),
                        max_batch_size: correlator.max_batch_size.clone(),
                        timeout_policy: correlator.timeout_policy.clone(),
                    },
                };
                processors_by_input
                    .entry(correlator.left_relay.clone())
                    .or_default()
                    .push(spec.clone());
                processors_by_input
                    .entry(correlator.right_relay.clone())
                    .or_default()
                    .push(spec);
            }
            Model::WindowProcessor(window_processor) => {
                processors_by_input
                    .entry(window_processor.from_relay.clone())
                    .or_default()
                    .push(ParameterizedProcessorSpec {
                        kind,
                        processor: identifier,
                        input_relay: window_processor.from_relay.clone(),
                        input_relays: vec![window_processor.from_relay.clone()],
                        mode: window_processor.mode,
                        error_policies: message_only_error_policies(
                            &window_processor.message_error_policy,
                        ),
                        from_where: processor_input_where_by_relay(&window_processor.from_where),
                        filter_where: window_processor.filter_where.clone(),
                        operation: ParameterizedProcessorOperationSpec::WindowProcessor {
                            output_routes: parameterized_outputs(&window_processor.output_routes),
                            width: window_processor.width.clone(),
                            step: window_processor.step.clone(),
                            aggregate: window_processor.aggregate.clone(),
                        },
                    });
            }
            Model::Unifier(unifier) => {
                let Some(input_relay) = unifier.from_relays.first().cloned() else {
                    continue;
                };
                let spec = ParameterizedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relay,
                    input_relays: unifier.from_relays.clone(),
                    mode: unifier.mode,
                    error_policies: message_only_error_policies(&unifier.message_error_policy),
                    from_where: processor_input_where_by_relay(&unifier.from_where),
                    filter_where: unifier.filter_where.clone(),
                    operation: ParameterizedProcessorOperationSpec::Unifier {
                        output_routes: parameterized_outputs(&unifier.output_routes),
                        flush_each: unifier.flush_each.clone(),
                        max_batch_size: unifier.max_batch_size.clone(),
                    },
                };
                for from_relay in &unifier.from_relays {
                    processors_by_input
                        .entry(from_relay.clone())
                        .or_default()
                        .push(spec.clone());
                }
            }
            Model::Inferencer(inferencer) => {
                processors_by_input
                    .entry(inferencer.from_relay.clone())
                    .or_default()
                    .push(ParameterizedProcessorSpec {
                        kind,
                        processor: identifier,
                        input_relay: inferencer.from_relay.clone(),
                        input_relays: vec![inferencer.from_relay.clone()],
                        mode: inferencer.mode,
                        error_policies: message_only_error_policies(
                            &inferencer.message_error_policy,
                        ),
                        from_where: processor_input_where_by_relay(&inferencer.from_where),
                        filter_where: inferencer.filter_where.clone(),
                        operation: ParameterizedProcessorOperationSpec::Inferencer {
                            output_routes: parameterized_outputs(&inferencer.output_routes),
                            resource: inferencer.resource.clone(),
                            resource_version: inferencer.resource_version,
                            file: inferencer.file.clone(),
                            inputs: inferencer.inputs.clone(),
                            outputs: inferencer.outputs.clone(),
                            flush_each: inferencer.flush_each.clone(),
                            max_batch_size: inferencer.max_batch_size.clone(),
                        },
                    });
            }
            Model::WasmProcessor(processor) => {
                processors_by_input
                    .entry(processor.from_relay.clone())
                    .or_default()
                    .push(ParameterizedProcessorSpec {
                        kind,
                        processor: identifier,
                        input_relay: processor.from_relay.clone(),
                        input_relays: vec![processor.from_relay.clone()],
                        mode: processor.mode,
                        error_policies: super::wasm_error_policies(
                            &processor.message_error_policy,
                            &processor.global_error_policy,
                        ),
                        from_where: processor_input_where_by_relay(&processor.from_where),
                        filter_where: processor.filter_where.clone(),
                        operation: ParameterizedProcessorOperationSpec::WasmProcessor {
                            output_routes: parameterized_outputs(&processor.output_routes),
                            resource: processor.resource.clone(),
                            resource_version: processor.resource_version,
                            file: processor.file.clone(),
                        },
                    });
            }
            Model::Ingestor(ingestor) => {
                for output in ingestor.output_routes.outputs() {
                    ingestors.push((
                        kind,
                        identifier.clone(),
                        output.relay.clone(),
                        ingestor.parameterized_by.ttl().map(str::to_string),
                        ingestor.parameterized_by.values().to_vec(),
                        ParametrizerAckBoundary::Preserve,
                        ingestor.flush_each.clone(),
                        ingestor.max_batch_size.clone(),
                        ingestor.error_policies.clone(),
                    ));
                }
            }
            Model::Reingestor(reingestor) => {
                for output in reingestor.output_routes.outputs() {
                    ingestors.push((
                        kind,
                        identifier.clone(),
                        output.relay.clone(),
                        reingestor.parameterized_by.ttl().map(str::to_string),
                        reingestor.parameterized_by.values().to_vec(),
                        ParametrizerAckBoundary::Reingestor(reingestor.mode),
                        reingestor.flush_each.clone(),
                        reingestor.max_batch_size.clone(),
                        message_only_error_policies(&reingestor.message_error_policy),
                    ));
                }
            }
            Model::Relay(_) if effective_parameterization.is_some() => {
                relay_roots.push((kind, identifier.clone(), identifier));
            }
            _ => {}
        }
    }

    fn build_nodes(
        relay: &Identifier,
        processors_by_input: &HashMap<Identifier, Vec<ParameterizedProcessorSpec>>,
    ) -> Vec<ParameterizedProcessorSpec> {
        let mut nodes = Vec::new();

        if let Some(processors) = processors_by_input.get(relay) {
            let mut processors = processors.clone();
            processors.sort_by(|left, right| left.processor.cmp(&right.processor));
            for mut processor in processors {
                match &mut processor.operation {
                    ParameterizedProcessorOperationSpec::Deduplicator { output_routes, .. }
                    | ParameterizedProcessorOperationSpec::WindowProcessor {
                        output_routes, ..
                    }
                    | ParameterizedProcessorOperationSpec::Reorderer { output_routes, .. }
                    | ParameterizedProcessorOperationSpec::Correlator { output_routes, .. }
                    | ParameterizedProcessorOperationSpec::Unifier { output_routes, .. }
                    | ParameterizedProcessorOperationSpec::Inferencer { output_routes, .. }
                    | ParameterizedProcessorOperationSpec::WasmProcessor {
                        output_routes, ..
                    } => {
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
        .map(|(_, _, root_relay, _, _, _, _, _, _)| root_relay.clone())
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
                Vec::new(),
                ParametrizerAckBoundary::Preserve,
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
                entrypoint_parameter_mappings,
                entrypoint_ack_boundary,
                entrypoint_flush_each,
                entrypoint_max_batch_size,
                error_policies,
            )| {
                ParameterizedIngestorSpec {
                    kind,
                    identifier,
                    root_relay: root_relay.clone(),
                    branch_ttl,
                    entrypoint_parameter_mappings,
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
    processors_by_input: &HashMap<Identifier, Vec<ParameterizedProcessorSpec>>,
) -> Vec<ParameterizedProcessorSpec> {
    fn visit_stream(
        relay: &Identifier,
        processors_by_input: &HashMap<Identifier, Vec<ParameterizedProcessorSpec>>,
        seen_processors: &mut HashSet<Identifier>,
        out: &mut Vec<ParameterizedProcessorSpec>,
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
                ParameterizedProcessorOperationSpec::Deduplicator { output_routes, .. }
                | ParameterizedProcessorOperationSpec::Reorderer { output_routes, .. }
                | ParameterizedProcessorOperationSpec::Correlator { output_routes, .. }
                | ParameterizedProcessorOperationSpec::WindowProcessor { output_routes, .. }
                | ParameterizedProcessorOperationSpec::Unifier { output_routes, .. }
                | ParameterizedProcessorOperationSpec::Inferencer { output_routes, .. }
                | ParameterizedProcessorOperationSpec::WasmProcessor { output_routes, .. } => {
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

pub(in crate::runtime) fn parameterized_processor_ids(
    specs: &[ParameterizedIngestorSpec],
) -> HashSet<Identifier> {
    let mut ids = HashSet::default();
    for spec in specs {
        ids.extend(spec.processors.iter().map(|node| node.processor.clone()));
    }
    ids
}

pub(in crate::runtime) fn materialize_parametrizer_template(
    spec: &ParameterizedIngestorSpec,
    model_index: &HashMap<(ModelKind, Identifier), Model>,
    relay_schemas: &HashMap<Identifier, Arc<CompiledSchema>>,
    relay_registries: &HashMap<Identifier, RelayRegistry>,
    relay_services: &HashMap<Identifier, Arc<RelayBoundaryServices>>,
) -> Result<ParametrizerTemplate, String> {
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
        output: &ParameterizedProcessorOutputSpec,
    ) -> Result<RelayProcessorOutputTemplate, String> {
        Ok(RelayProcessorOutputTemplate {
            output_relay: output.relay.clone(),
            filter_map: output.filter_map.clone(),
        })
    }

    fn materialize_outputs(
        outputs: &ParameterizedProcessorOutputsSpec,
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
        nodes: &[ParameterizedProcessorSpec],
    ) -> Result<Vec<RelayProcessorTemplate>, String> {
        let mut out = Vec::new();
        for node in nodes {
            out.push(RelayProcessorTemplate {
                kind: node.kind,
                processor: node.processor.clone(),
                input_relay: node.input_relay.clone(),
                input_relays: node.input_relays.clone(),
                mode: node.mode,
                error_policies: node.error_policies.clone(),
                from_where: node.from_where.clone(),
                filter_where: node.filter_where.clone(),
                operation: match &node.operation {
                    ParameterizedProcessorOperationSpec::Deduplicator {
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
                    ParameterizedProcessorOperationSpec::WindowProcessor {
                        output_routes,
                        width,
                        step,
                        aggregate,
                    } => RelayProcessorOperationTemplate::WindowProcessor {
                        output_routes: materialize_outputs(output_routes)?,
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
                        aggregate: parse_aggregate_program(aggregate)
                            .map_err(|error| {
                                format!(
                                    "window processor '{}' aggregate parse failed: {}",
                                    node.processor.as_str(),
                                    Runtime::vm_program_error(error)
                                )
                            })?
                            .inner,
                    },
                    ParameterizedProcessorOperationSpec::Reorderer {
                        output_routes,
                        order_by,
                        max_time,
                        flush_each,
                        max_batch_size,
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
                        flush_each: parse_branch_flush_policy(
                            "reorderer",
                            &node.processor,
                            flush_each,
                            max_batch_size.as_deref(),
                        )?,
                    },
                    ParameterizedProcessorOperationSpec::Correlator {
                        output_routes,
                        left_relay,
                        right_relay,
                        correlate_where,
                        match_policy,
                        output_assignments,
                        max_time,
                        flush_each,
                        max_batch_size,
                        timeout_policy,
                    } => RelayProcessorOperationTemplate::Correlator {
                        output_routes: materialize_outputs(output_routes)?,
                        left_relay: left_relay.clone(),
                        right_relay: right_relay.clone(),
                        correlate_where: correlate_where.clone(),
                        match_policy: *match_policy,
                        output_assignments: output_assignments.clone(),
                        max_time: humantime::parse_duration(max_time).map_err(|error| {
                            format!(
                                "invalid correlator '{}' MAX TIME duration '{}': {}",
                                node.processor.as_str(),
                                max_time,
                                error
                            )
                        })?,
                        flush_each: parse_branch_flush_policy(
                            "correlator",
                            &node.processor,
                            flush_each,
                            max_batch_size.as_deref(),
                        )?,
                        timeout_policy: timeout_policy.clone(),
                    },
                    ParameterizedProcessorOperationSpec::Unifier {
                        output_routes,
                        flush_each,
                        max_batch_size,
                    } => RelayProcessorOperationTemplate::Unifier {
                        output_routes: materialize_outputs(output_routes)?,
                        flush_each: parse_branch_flush_policy(
                            "unifier",
                            &node.processor,
                            flush_each,
                            max_batch_size.as_deref(),
                        )?,
                    },
                    ParameterizedProcessorOperationSpec::Inferencer {
                        output_routes,
                        resource,
                        resource_version,
                        file,
                        inputs,
                        outputs,
                        flush_each,
                        max_batch_size,
                    } => RelayProcessorOperationTemplate::Inferencer {
                        output_routes: materialize_outputs(output_routes)?,
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
                        inputs: inputs.clone(),
                        outputs: outputs.clone(),
                        flush_each: parse_branch_flush_policy(
                            "inferencer",
                            &node.processor,
                            flush_each,
                            max_batch_size.as_deref(),
                        )?,
                    },
                    ParameterizedProcessorOperationSpec::WasmProcessor {
                        output_routes,
                        resource,
                        resource_version,
                        file,
                    } => RelayProcessorOperationTemplate::WasmProcessor {
                        output_routes: materialize_outputs(output_routes)?,
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
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
            ParameterizedProcessorOperationSpec::Deduplicator { output_routes, .. }
            | ParameterizedProcessorOperationSpec::Reorderer { output_routes, .. }
            | ParameterizedProcessorOperationSpec::WindowProcessor { output_routes, .. }
            | ParameterizedProcessorOperationSpec::Unifier { output_routes, .. }
            | ParameterizedProcessorOperationSpec::Inferencer { output_routes, .. }
            | ParameterizedProcessorOperationSpec::WasmProcessor { output_routes, .. } => {
                branch_relay_ids.extend(output_routes.outputs().map(|output| output.relay.clone()));
            }
            ParameterizedProcessorOperationSpec::Correlator {
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
                    return Err(format!(
                        "missing parameterized branch relay '{}'",
                        relay.as_str()
                    ));
                }
            }
            let registry = relay_registries.get(&relay).cloned().ok_or_else(|| {
                format!("missing parameterized branch relay '{}'", relay.as_str())
            })?;
            let services = relay_services.get(&relay).cloned().ok_or_else(|| {
                format!(
                    "missing parameterized branch relay services '{}'",
                    relay.as_str()
                )
            })?;
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
    let entrypoint_schema_relay = match model_index.get(&(spec.kind, spec.identifier.clone())) {
        Some(Model::Reingestor(reingestor)) => &reingestor.from_relay,
        _ => &spec.root_relay,
    };
    let entrypoint_schema = relay_schemas
        .get(entrypoint_schema_relay)
        .cloned()
        .ok_or_else(|| {
            format!(
                "missing parameterized ingestor entrypoint relay schema '{}'",
                entrypoint_schema_relay.as_str()
            )
        })?;
    let processors = materialize_nodes(&spec.processors)?
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
    Ok(ParametrizerTemplate {
        source_kind: spec.kind,
        source: spec.identifier.clone(),
        root_relay: spec.root_relay.clone(),
        branch_ttl,
        entrypoint_schema,
        entrypoint_parameter_mappings: spec.entrypoint_parameter_mappings.clone(),
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

pub(in crate::runtime) fn resolve_concrete_branch(
    record: &RuntimeRecord,
    parameterized_by: &[Identifier],
    owner: &Identifier,
) -> Result<ConcreteBranch, String> {
    if parameterized_by.is_empty() {
        return Ok(ConcreteBranch::Root);
    }

    let mut fields = Vec::with_capacity(parameterized_by.len());
    for field_name in parameterized_by {
        let Some(value) = record.value(field_name.as_str()) else {
            return Err(format!(
                "parameterized field '{}' is missing for '{}'",
                field_name.as_str(),
                owner.as_str()
            ));
        };
        fields.push((field_name.clone(), value.clone()));
    }

    BranchKey::from_fields(fields).map(ConcreteBranch::Key)
}

pub(in crate::runtime) fn resolve_concrete_branch_from_mappings(
    record: &RuntimeRecord,
    branch_key: Option<&BranchKey>,
    mappings: &[ParameterValueMapping],
    owner: &Identifier,
) -> Result<ConcreteBranch, String> {
    if mappings.is_empty() {
        return Ok(ConcreteBranch::Root);
    }

    let mut fields = Vec::with_capacity(mappings.len());
    for mapping in mappings {
        let value = if mapping.relay.as_str() == super::BRANCH_NAMESPACE {
            branch_key.and_then(|key| key.value(mapping.relay_field.as_str()))
        } else {
            record.value(mapping.relay_field.as_str())
        };
        let Some(value) = value else {
            return Err(format!(
                "parameterized source field '{}.{}' is missing for '{}'",
                mapping.relay.as_str(),
                mapping.relay_field.as_str(),
                owner.as_str()
            ));
        };
        fields.push((mapping.field.clone(), value.clone()));
    }

    BranchKey::from_fields(fields).map(ConcreteBranch::Key)
}

pub(in crate::runtime) fn format_parameterized_by(parameterized_by: &[Identifier]) -> String {
    if parameterized_by.is_empty() {
        "()".to_string()
    } else {
        format!(
            "({})",
            parameterized_by
                .iter()
                .map(|field| field.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
