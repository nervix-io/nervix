use super::*;

impl RelayProcessorNode {
    pub(super) fn source_filter_scope(&self, incoming_relay: &RelayName) -> RuntimeFilterScope {
        match &self.operation {
            RelayProcessorOperationNode::Correlator {
                left_relays,
                right_relays,
                ..
            } if left_relays.contains(incoming_relay) => RuntimeFilterScope::Source {
                namespace: "left",
                allow_header_reads: false,
                allow_metadata: false,
            },
            RelayProcessorOperationNode::Correlator { right_relays, .. }
                if right_relays.contains(incoming_relay) =>
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
        }
    }

    pub(super) async fn resolve_materialized_dependencies(
        &self,
        branch: &BranchRuntime,
        branch_key: &Option<BranchKey>,
    ) -> Result<MaterializedDependencyResolution, String> {
        branch
            .runtime
            .resolve_materialized_dependencies(&branch.domain, branch_key, &self.materialized_state)
            .await
    }

    pub(super) fn refresh(
        &mut self,
        runtime: &Runtime,
        domain: &DomainName,
        graph: Option<StdArc<ActiveGraph>>,
    ) {
        let changed = match (&self.last_graph, &graph) {
            (Some(previous), Some(current)) => !StdArc::ptr_eq(previous, current),
            (None, None) => false,
            _ => true,
        };
        if !changed {
            return;
        }

        let requires_reinitialization = match (self.last_graph.as_ref(), graph.as_ref()) {
            (Some(previous), Some(current)) => {
                previous
                    .node(self.kind, &self.processor)
                    .map(|node| node.config.as_ref().clone())
                    != current
                        .node(self.kind, &self.processor)
                        .map(|node| node.config.as_ref().clone())
            }
            (None, Some(_)) | (Some(_), None) => true,
            (None, None) => false,
        };

        if requires_reinitialization {
            if let Some(error) = self.apply_refreshed_graph(runtime, domain, graph.as_ref()) {
                warn!(
                    kind = self.kind.as_str(),
                    processor = self.processor.as_str(),
                    error = %error,
                    "failed to refresh dynamic processor configuration"
                );
                return;
            }
            self.applied_generation = self
                .applied_generation
                .checked_add(1)
                .assured("a processor cannot apply 2^64 configuration refreshes");
        }
        self.last_graph = graph;
    }

    pub(super) fn apply_refreshed_graph(
        &mut self,
        runtime: &Runtime,
        domain: &DomainName,
        graph: Option<&StdArc<ActiveGraph>>,
    ) -> Option<String> {
        let Some(graph) = graph else {
            return Some(format!(
                "{} '{}' is absent from the refreshed graph",
                self.kind.as_str(),
                self.processor.as_str()
            ));
        };
        let Some(execution) = runtime.inner.executions.get(domain) else {
            return Some(format!(
                "domain '{}' has no execution for processor refresh",
                domain.as_str()
            ));
        };
        let template = match processor_template_for_graph_node(
            graph,
            self.kind,
            &self.processor,
            &execution.relay_schemas,
            Some(&execution.udfs),
        ) {
            Ok(template) => template,
            Err(error) => return Some(error),
        };
        self.apply_node_template(template).err()
    }

    pub(super) fn apply_node_template(
        &mut self,
        template: RelayProcessorTemplate,
    ) -> Result<(), String> {
        if self.kind != template.kind || self.processor != template.processor {
            return Err(format!(
                "processor template targets {} '{}', not {} '{}'",
                template.kind.as_str(),
                template.processor.as_str(),
                self.kind.as_str(),
                self.processor.as_str()
            ));
        }
        if self.input_relays != template.input_relays {
            return Err(format!(
                "dynamic {} update changed processor input topology",
                self.kind.as_str()
            ));
        }
        if self.materialized_state != template.materialized_state {
            return Err(format!(
                "dynamic {} update changed materialized-state dependencies",
                self.kind.as_str()
            ));
        }
        self.operation.apply_template(&template.operation)?;

        let mut previous_collectors = std::mem::take(&mut self.input_collectors);
        self.input_collectors = template
            .input_collect_policies
            .into_iter()
            .map(|(relay, policy)| {
                let mut collector = previous_collectors
                    .remove(&relay)
                    .unwrap_or_else(|| RuntimeInputCollector::new(policy));
                collector.policy = policy;
                (relay, collector)
            })
            .collect();

        if self.from_where != template.from_where {
            self.from_where = template.from_where;
            self.compiled_from_where.clear();
        }
        if self.filter_where != template.filter_where {
            self.filter_where = template.filter_where;
            self.compiled_filter_where.clear();
        }
        self.error_policies = template.error_policies;
        Ok(())
    }

    pub(super) async fn filter_input_batch(
        &mut self,
        graph: &SharedActiveGraph,
        branch: &mut BranchRuntime,
        incoming_relay: &RelayName,
        batch: RelayRecordBatch,
        materialized_state: &HashMap<String, RuntimeValue>,
    ) -> Option<RelayRecordBatch> {
        let batch = self
            .filter_input_batch_with_kind(
                graph,
                branch,
                incoming_relay,
                batch,
                ProcessorInputFilterKind::FromWhere,
                materialized_state,
            )
            .await?;
        self.filter_input_batch_with_kind(
            graph,
            branch,
            incoming_relay,
            batch,
            ProcessorInputFilterKind::FilterWhere,
            materialized_state,
        )
        .await
    }

    pub(super) fn concat_collected_input(
        &self,
        branch: &BranchRuntime,
        incoming_relay: &RelayName,
        batches: Vec<RelayRecordBatch>,
    ) -> Option<RelayRecordBatch> {
        let acks = batches
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        match RelayRecordBatch::concat(batches) {
            Ok(batch) => Some(batch),
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    self.kind,
                    &self.processor,
                    &self.error_policies,
                    acks.iter(),
                    format!(
                        "{} '{}' failed to concatenate collected input from relay '{}': {error}",
                        self.kind.as_str(),
                        self.processor.as_str(),
                        incoming_relay.as_str(),
                    ),
                );
                None
            }
        }
    }

    pub(super) fn accept_input<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
        incoming_relay: &'a RelayName,
        batch: RelayRecordBatch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let now = branch
                .runtime
                .current_stream_expiration_time(&branch.domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp);
            let Some(collector) = self.input_collectors.get_mut(incoming_relay) else {
                self.execute(graph, branch, incoming_relay, batch).await;
                return;
            };
            if !collector.push(batch, now) {
                return;
            }
            let batches = collector.take_pending();
            let Some(batch) = self.concat_collected_input(branch, incoming_relay, batches) else {
                return;
            };
            self.execute(graph, branch, incoming_relay, batch).await;
        })
    }

    pub(super) fn flush_due_collected_inputs<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
        now: Timestamp,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let due_relays = self
                .input_collectors
                .iter()
                .filter_map(|(relay, collector)| collector.is_due(now).then_some(relay.clone()))
                .collect::<Vec<_>>();
            for relay in due_relays {
                let batches = self
                    .input_collectors
                    .get_mut(&relay)
                    .map(RuntimeInputCollector::take_pending)
                    .unwrap_or_default();
                let Some(batch) = self.concat_collected_input(branch, &relay, batches) else {
                    continue;
                };
                self.execute(graph, branch, &relay, batch).await;
            }
        })
    }

    pub(super) fn flush_all_collected_inputs<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let pending_relays = self
                .input_collectors
                .iter()
                .filter_map(|(relay, collector)| {
                    (!collector.pending.is_empty()).then_some(relay.clone())
                })
                .collect::<Vec<_>>();
            for relay in pending_relays {
                let batches = self
                    .input_collectors
                    .get_mut(&relay)
                    .map(RuntimeInputCollector::take_pending)
                    .unwrap_or_default();
                let Some(batch) = self.concat_collected_input(branch, &relay, batches) else {
                    continue;
                };
                self.execute(graph, branch, &relay, batch).await;
            }
        })
    }

    pub(super) fn drop_collected_inputs(&mut self, reason: &str) {
        for collector in self.input_collectors.values_mut() {
            for batch in collector.take_pending() {
                for ack in batch.acks {
                    ack.no_ack(reason.to_string());
                }
            }
        }
    }

    pub(super) async fn filter_input_batch_with_kind(
        &mut self,
        graph: &SharedActiveGraph,
        branch: &mut BranchRuntime,
        incoming_relay: &RelayName,
        batch: RelayRecordBatch,
        kind: ProcessorInputFilterKind,
        materialized_state: &HashMap<String, RuntimeValue>,
    ) -> Option<RelayRecordBatch> {
        let Some(filter_where) = (match kind {
            ProcessorInputFilterKind::FromWhere => self.from_where.get(incoming_relay),
            ProcessorInputFilterKind::FilterWhere => self.filter_where.as_ref(),
        }) else {
            return Some(batch);
        };
        let filter_where = filter_where.clone();

        let needs_compile = match kind {
            ProcessorInputFilterKind::FromWhere => {
                !self.compiled_from_where.contains_key(incoming_relay)
            }
            ProcessorInputFilterKind::FilterWhere => {
                !self.compiled_filter_where.contains_key(incoming_relay)
            }
        };
        if needs_compile {
            let input_schema =
                match relay_schema_for_runtime(&branch.runtime, &branch.domain, incoming_relay) {
                    Ok(schema) => schema,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind,
                            &self.processor,
                            &self.error_policies,
                            batch.acks.iter(),
                            error,
                        );
                        return None;
                    }
                };
            let materialized_stream_specs =
                materialized_stream_specs_for_graph(&branch.runtime, &branch.domain, graph);
            let current_branching = branch
                .runtime
                .inner
                .executions
                .get(&branch.domain)
                .and_then(|execution| execution.relay_branchings.get(incoming_relay).cloned())
                .unwrap_or_default();
            let current_branch_schema =
                relay_branch_schema_for_runtime(&branch.runtime, &branch.domain, incoming_relay);
            let available_lookups = branch
                .runtime
                .inner
                .executions
                .get(&branch.domain)
                .map(|execution| execution.lookups.clone())
                .unwrap_or_default();
            let udfs = branch
                .runtime
                .inner
                .executions
                .get(&branch.domain)
                .map(|execution| execution.udfs.clone());
            let filter_scope = match kind {
                ProcessorInputFilterKind::FromWhere => self.source_filter_scope(incoming_relay),
                ProcessorInputFilterKind::FilterWhere => RuntimeFilterScope::Source {
                    namespace: "input",
                    allow_header_reads: false,
                    allow_metadata: false,
                },
            };
            match compile_scoped_filter_program(
                RuntimeCompileTarget {
                    domain: &branch.domain,
                    identifier: &self.processor,
                },
                Some(&filter_where),
                RuntimeVmSchema {
                    schema: batch.arrow_schema(),
                    sensitivity: input_schema.vm_sensitivity(),
                },
                kind.error_operation(),
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
                    udfs: udfs.as_ref(),
                },
                filter_scope,
            ) {
                Ok(Some(program)) => match kind {
                    ProcessorInputFilterKind::FromWhere => {
                        self.compiled_from_where
                            .insert(incoming_relay.clone(), program);
                    }
                    ProcessorInputFilterKind::FilterWhere => {
                        self.compiled_filter_where
                            .insert(incoming_relay.clone(), program);
                    }
                },
                Ok(None) => {}
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        self.kind,
                        &self.processor,
                        &self.error_policies,
                        batch.acks.iter(),
                        format!("{} compile failed: {}", kind.label(), error),
                    );
                    return None;
                }
            }
        }

        let program = match kind {
            ProcessorInputFilterKind::FromWhere => self.compiled_from_where.get(incoming_relay),
            ProcessorInputFilterKind::FilterWhere => self.compiled_filter_where.get(incoming_relay),
        }
        .cloned();
        let Some(program) = program else {
            return Some(batch);
        };
        let plan = match plan_filter_map_messages(
            self.kind.as_str(),
            &self.processor,
            kind.label(),
            &program,
            batch,
            branch
                .runtime
                .current_stream_expiration_time(&branch.domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp),
            materialized_state,
        )
        .await
        {
            Ok(plan) => plan,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    self.kind,
                    &self.processor,
                    &self.error_policies,
                    error.acks.iter(),
                    error.reason,
                );
                return None;
            }
        };
        branch
            .runtime
            .handle_planned_message_errors(
                &branch.domain,
                self.kind,
                &self.processor,
                &self.error_policies,
                plan.message_errors,
            )
            .await;
        plan.batch
    }

    pub(super) fn execute<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
        incoming_relay: &'a RelayName,
        batch: RelayRecordBatch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let current = graph.load_full();
            let current = current.as_ref().map(StdArc::clone);
            self.refresh(&branch.runtime, &branch.domain, current);
            let materialized_values = match self
                .resolve_materialized_dependencies(branch, &batch.key)
                .await
            {
                Ok(MaterializedDependencyResolution::Ready(values)) => values,
                Ok(MaterializedDependencyResolution::Skip) => {
                    for ack in batch.acks.iter() {
                        ack.ack_success();
                    }
                    return;
                }
                Ok(MaterializedDependencyResolution::Wait) => {
                    self.pending_materialized
                        .push_back(PendingMaterializedBatch::new(incoming_relay.clone(), batch));
                    return;
                }
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        self.kind,
                        &self.processor,
                        &self.error_policies,
                        batch.acks.iter(),
                        format!(
                            "{} '{}' failed to resolve materialized dependencies: {error}",
                            self.kind.as_str(),
                            self.processor
                        ),
                    );
                    return;
                }
            };
            let Some(batch) = self
                .filter_input_batch(graph, branch, incoming_relay, batch, &materialized_values)
                .await
            else {
                return;
            };
            match &mut self.operation {
                RelayProcessorOperationNode::Deduplicator {
                    output_routes,
                    deduplicate_on,
                    max_time,
                    compiled_key_program,
                    state,
                } => {
                    let input_arrow_schema = batch.arrow_schema();
                    let key_input_batch = batch.batch.clone();
                    let key_input_keys = batch.keys.clone();
                    let execution_now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);

                    if compiled_key_program.is_none() {
                        let udfs = branch.runtime.udf_executor(&branch.domain);
                        match compile_deduplicator_key_program(
                            &self.processor,
                            &self.input_relays,
                            deduplicate_on,
                            input_arrow_schema.clone(),
                            udfs.as_ref(),
                        ) {
                            Ok(program) => *compiled_key_program = Some(Box::new(program)),
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    error,
                                );
                                return;
                            }
                        }
                    }
                    let Some(key_program) = compiled_key_program.as_ref() else {
                        return;
                    };
                    let lookup_columns = HashMap::default();
                    let vm_batch = match project_vm_input_batch(
                        &key_program.program.input_schema,
                        &VmInputProjectionSources {
                            carrier: &key_input_batch,
                            namespace_batches: &[],
                            strict_namespaces: &[],
                            keys: &key_input_keys,
                            side_inputs: &materialized_values,
                            ingest_metadata: None,
                            lookup_columns: &lookup_columns,
                            uninitialized: None,
                        },
                        None,
                    ) {
                        Ok(batch) => batch,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "deduplicator '{}' failed to build DEDUPLICATE ON input \
                                     batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };
                    let key_result = execute_program_with_selection_in_context(
                        &key_program.program,
                        &vm_batch,
                        &VmExecutionContext {
                            now: execution_now,
                            injector: None,
                        },
                    )
                    .await;
                    let key_result = match key_result {
                        Ok(result) => result,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "deduplicator '{}' failed to evaluate DEDUPLICATE ON \
                                     expressions: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };

                    let mut dedup_keys = Vec::new();
                    let mut forwarded_rows = Vec::new();
                    for (row, acks) in batch.acks.iter().enumerate() {
                        trace!(
                            processor = self.processor.as_str(),
                            operator = "deduplicator",
                            "branched relay operator received message"
                        );

                        let dedup_key = DeduplicatorKey::new(
                            (0..key_program.key_count)
                                .map(|index| {
                                    reorder_key_part(
                                        key_result
                                            .batch
                                            .column(key_program.key_column_offset + index),
                                        row,
                                    )
                                })
                                .collect(),
                        );
                        if state.reserve_new_key(dedup_key.clone(), execution_now, *max_time) {
                            dedup_keys.push(dedup_key);
                            forwarded_rows.push(row);
                        } else {
                            debug!(
                                deduplicator = self.processor.as_str(),
                                "branched deduplicator dropped duplicate message"
                            );
                            acks.ack_success();
                        }
                    }

                    if forwarded_rows.is_empty() {
                        return;
                    }

                    let forwarded = match batch.take(&forwarded_rows) {
                        Ok(batch) => batch,
                        Err((error, acks)) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                acks.iter(),
                                format!(
                                    "deduplicator '{}' failed to build output batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };

                    let Some(dispatched_acks) = dispatch_processor_outputs(
                        ProcessorOutputDispatchContext {
                            graph,
                            branch,
                            node_kind: self.kind,
                            source_kind: self.kind,
                            processor: &self.processor,
                            error_policies: &self.error_policies,
                            input_relays: &self.input_relays,
                            filter_source: ProcessorOutputFilterSource::InputRelays,
                            materialized_state: ProcessorMaterializedState::Admitted(
                                &materialized_values,
                            ),
                        },
                        output_routes,
                        forwarded,
                    )
                    .await
                    else {
                        state.remove_reserved_keys(&dedup_keys);
                        return;
                    };

                    for ack in dispatched_acks {
                        ack.ack_success();
                    }
                }
                RelayProcessorOperationNode::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
                    compiled_aggregates,
                    state,
                    replicated_state,
                } => {
                    let messages = match batch.try_into_messages() {
                        Ok(messages) => messages,
                        Err(error_and_batch) => {
                            let (error, batch) = *error_and_batch;
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "window processor '{}' failed to decode arrow batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };
                    let Some(first_message) = messages.first() else {
                        return;
                    };
                    let execution_now = message_timestamp(first_message);
                    let row_count = messages.len();
                    let mut aggregate_inputs_by_row = (0..row_count)
                        .map(|_| Ok(Vec::new()))
                        .collect::<Vec<Result<Vec<WindowAggregateInput>, String>>>();
                    for compiled in compiled_aggregates.iter() {
                        tokio::task::consume_budget().await;
                        let evaluated = match evaluate_window_aggregate_inputs(
                            compiled,
                            first_message.record.batch(),
                            execution_now,
                        )
                        .await
                        {
                            Ok(evaluated) => evaluated,
                            Err(error) => {
                                for inputs in &mut aggregate_inputs_by_row {
                                    if inputs.is_ok() {
                                        *inputs = Err(error.clone());
                                    }
                                }
                                break;
                            }
                        };
                        if evaluated.len() != row_count {
                            let error = format!(
                                "window aggregate input VM produced {} rows for {row_count} input \
                                 rows",
                                evaluated.len()
                            );
                            for inputs in &mut aggregate_inputs_by_row {
                                if inputs.is_ok() {
                                    *inputs = Err(error.clone());
                                }
                            }
                            break;
                        }
                        for (inputs, evaluated) in aggregate_inputs_by_row.iter_mut().zip(evaluated)
                        {
                            match evaluated {
                                Ok(evaluated) => {
                                    if let Ok(inputs) = inputs {
                                        inputs.extend(evaluated);
                                    }
                                }
                                Err(error) => {
                                    if inputs.is_ok() {
                                        *inputs = Err(error);
                                    }
                                }
                            }
                        }
                    }
                    for (message, aggregate_inputs) in
                        messages.into_iter().zip(aggregate_inputs_by_row)
                    {
                        tokio::task::consume_budget().await;
                        let timestamp = message_timestamp(&message);
                        let aggregate_inputs = match aggregate_inputs {
                            Ok(aggregate_inputs) => aggregate_inputs,
                            Err(error) => {
                                branch
                                    .runtime
                                    .handle_message_error(
                                        &branch.domain,
                                        self.kind,
                                        &self.processor,
                                        &self.error_policies,
                                        message,
                                        MessageErrorFailure::publish(
                                            None,
                                            format!(
                                                "window processor '{}' aggregate input failed: {}",
                                                self.processor.as_str(),
                                                error
                                            ),
                                        ),
                                    )
                                    .await;
                                continue;
                            }
                        };
                        if let Err(error_and_message) =
                            state.push_message(aggregate, timestamp, message, aggregate_inputs)
                        {
                            let (error, message) = *error_and_message;
                            branch
                                .runtime
                                .handle_message_error(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    message,
                                    MessageErrorFailure::publish(
                                        None,
                                        format!(
                                            "window processor '{}' aggregate input failed: {}",
                                            self.processor.as_str(),
                                            error
                                        ),
                                    ),
                                )
                                .await;
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                state.entries.iter().map(|entry| &entry.message.acks),
                                format!(
                                    "window processor '{}' aggregate state failed: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            state.clear(aggregate);
                            replicated_state.mark_live_dirty();
                            continue;
                        }
                        replicated_state.mark_live_dirty();
                        let due =
                            window_width_met(state, *width_messages, *width_duration, timestamp);
                        let changed = flush_ready_window_processor(
                            WindowFlushContext {
                                graph,
                                node_kind: self.kind,
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                branch,
                                output_routes,
                                materialized_state: &self.materialized_state,
                            },
                            state,
                            aggregate,
                            compiled_aggregates,
                            WindowBounds {
                                width_messages: *width_messages,
                                step_messages: *step_messages,
                                width_duration: *width_duration,
                                step_duration: *step_duration,
                            },
                            timestamp,
                        )
                        .await;
                        if due || changed {
                            replicated_state.mark_live_dirty();
                            if let Err(error) = snapshot_window_processor_live_state(
                                &self.processor,
                                replicated_state,
                                state,
                            ) {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    state.entries.iter().map(|entry| &entry.message.acks),
                                    error,
                                );
                                state.clear(aggregate);
                                replicated_state.mark_live_dirty();
                            }
                        }
                    }
                }
                RelayProcessorOperationNode::Reorderer {
                    output_routes,
                    order_by,
                    max_time: _,
                    compiled_program,
                    output_buffers,
                    arrival_sequence,
                } => {
                    if compiled_program.is_none() {
                        let udfs = branch.runtime.udf_executor(&branch.domain);
                        match compile_reorderer_program(
                            &self.processor,
                            &self.input_relays,
                            order_by,
                            batch.arrow_schema(),
                            udfs.as_ref(),
                        ) {
                            Ok(program) => *compiled_program = Some(Box::new(program)),
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    error,
                                );
                                return;
                            }
                        }
                    }
                    let Some(program) = compiled_program.as_ref() else {
                        return;
                    };
                    let execution_now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    let lookup_columns = HashMap::default();
                    let vm_batch = match project_vm_input_batch(
                        &program.program.input_schema,
                        &VmInputProjectionSources {
                            carrier: &batch.batch,
                            namespace_batches: &[],
                            strict_namespaces: &[],
                            keys: &batch.keys,
                            side_inputs: &materialized_values,
                            ingest_metadata: None,
                            lookup_columns: &lookup_columns,
                            uninitialized: None,
                        },
                        None,
                    ) {
                        Ok(batch) => batch,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "reorderer '{}' failed to build BY input batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };
                    let key_result = execute_program_with_selection_in_context(
                        &program.program,
                        &vm_batch,
                        &VmExecutionContext {
                            now: execution_now,
                            injector: None,
                        },
                    )
                    .await;
                    let key_result = match key_result {
                        Ok(result) => result,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "reorderer '{}' failed to evaluate BY expressions: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };
                    if output_buffers.len() != output_routes.routes.len() {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind,
                            &self.processor,
                            &self.error_policies,
                            batch.acks.iter(),
                            format!(
                                "reorderer '{}' output buffer count does not match its routes",
                                self.processor.as_str()
                            ),
                        );
                        return;
                    }
                    let row_count = batch.batch.batch().num_rows();
                    let mut row_ordering = Vec::with_capacity(row_count);
                    for row in 0..row_count {
                        let key = (0..program.key_count)
                            .map(|index| {
                                reorder_key_part(
                                    key_result.batch.column(program.key_column_offset + index),
                                    row,
                                )
                            })
                            .collect::<Vec<_>>();
                        let sequence = *arrival_sequence;
                        *arrival_sequence = arrival_sequence
                            .checked_add(1)
                            .assured("a reorderer cannot admit 2^64 rows in one branch");
                        row_ordering.push(ReordererRowOrder {
                            key,
                            arrival_sequence: sequence,
                        });
                    }
                    let row_ordering = Arc::new(row_ordering);
                    let route_batches = batch.into_attached_fanout(output_routes.routes.len());
                    let mut due_outputs = Vec::new();
                    for (output_index, route_batch) in route_batches.into_iter().enumerate() {
                        let output_buffer = &mut output_buffers[output_index];
                        output_buffer.push(route_batch, Arc::clone(&row_ordering), execution_now);
                        let output = &mut output_routes.routes[output_index];
                        match output
                            .schedule_input_flush(execution_now, output_buffer.estimated_bytes())
                        {
                            Some(true) => {
                                output.force_flush_at(execution_now);
                                due_outputs.push(output_index);
                            }
                            Some(false) => {}
                            None => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    output_buffer.acks(),
                                    format!(
                                        "reorderer '{}' output '{}' has no flush policy",
                                        self.processor.as_str(),
                                        output.relay.as_str()
                                    ),
                                );
                                output_buffer.clear();
                            }
                        }
                    }
                    for output_index in due_outputs {
                        flush_branch_reorderer_output(
                            ReordererFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind,
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                input_relays: &self.input_relays,
                                materialized_state: &self.materialized_state,
                            },
                            &mut output_buffers[output_index],
                            output_index,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Correlator {
                    output_routes,
                    left_relays,
                    right_relays,
                    correlate_where,
                    match_policy,
                    max_time: _,
                    timeout_policy: _,
                    compiled_where_program,
                    compiled_output_programs,
                    state,
                } => {
                    let side = if left_relays.contains(incoming_relay) {
                        CorrelatorSide::Left
                    } else if right_relays.contains(incoming_relay) {
                        CorrelatorSide::Right
                    } else {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind,
                            &self.processor,
                            &self.error_policies,
                            batch.acks.iter(),
                            format!(
                                "correlator '{}' received unexpected relay '{}'",
                                self.processor.as_str(),
                                incoming_relay.as_str()
                            ),
                        );
                        return;
                    };
                    let execution_now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    if compiled_where_program.is_none() {
                        let Some(left_relay) = left_relays.first() else {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "correlator '{}' has no LEFT input relays",
                                    self.processor.as_str()
                                ),
                            );
                            return;
                        };
                        let Some(right_relay) = right_relays.first() else {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "correlator '{}' has no RIGHT input relays",
                                    self.processor.as_str()
                                ),
                            );
                            return;
                        };
                        let left_schema = match relay_schema_for_runtime(
                            &branch.runtime,
                            &branch.domain,
                            left_relay,
                        ) {
                            Ok(schema) => schema,
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    error.to_string(),
                                );
                                return;
                            }
                        };
                        let right_schema = match relay_schema_for_runtime(
                            &branch.runtime,
                            &branch.domain,
                            right_relay,
                        ) {
                            Ok(schema) => schema,
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    error.to_string(),
                                );
                                return;
                            }
                        };
                        match compile_correlator_where_program(
                            &self.processor,
                            correlate_where,
                            left_relays,
                            left_schema.arrow_schema(),
                            right_relays,
                            right_schema.arrow_schema(),
                            branch.runtime.udf_executor(&branch.domain).as_ref(),
                        ) {
                            Ok(program) => *compiled_where_program = Some(Box::new(program)),
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    error,
                                );
                                return;
                            }
                        }
                    }
                    let Some(where_program) = compiled_where_program.as_ref() else {
                        return;
                    };
                    let messages = match batch.clone().try_into_messages() {
                        Ok(messages) => messages,
                        Err(error_and_batch) => {
                            let (error, batch) = *error_and_batch;
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "correlator '{}' failed to decode arrow batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };

                    let mut correlations =
                        Vec::<(CorrelatorPendingMessage, CorrelatorPendingMessage)>::new();
                    let materialized_state = Arc::new(materialized_values);
                    for message in messages {
                        tokio::task::consume_budget().await;
                        let incoming = CorrelatorPendingMessage {
                            received_at: execution_now,
                            message,
                            materialized_state: materialized_state.clone(),
                        };
                        match correlate_incoming_message(
                            &self.processor,
                            where_program,
                            side,
                            *match_policy,
                            state,
                            incoming,
                            execution_now,
                        )
                        .await
                        {
                            Ok(Some(pair)) => correlations.push(pair),
                            Ok(None) => {}
                            Err((reason, acks)) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    acks.iter(),
                                    reason,
                                );
                            }
                        }
                    }
                    if correlations.is_empty() {
                        return;
                    }

                    if output_routes.routes.is_empty()
                        || compiled_output_programs.len() != output_routes.routes.len()
                    {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind,
                            &self.processor,
                            &self.error_policies,
                            correlations.iter().flat_map(|(left, right)| {
                                [&left.message.acks, &right.message.acks]
                            }),
                            format!(
                                "correlator '{}' output programs do not match its destinations",
                                self.processor.as_str()
                            ),
                        );
                        return;
                    }
                    let Some(left_relay) = left_relays.first() else {
                        return;
                    };
                    let Some(right_relay) = right_relays.first() else {
                        return;
                    };
                    let left_schema =
                        match relay_schema_for_runtime(&branch.runtime, &branch.domain, left_relay)
                        {
                            Ok(schema) => schema,
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    correlations.iter().flat_map(|(left, right)| {
                                        [&left.message.acks, &right.message.acks]
                                    }),
                                    error,
                                );
                                return;
                            }
                        };
                    let right_schema = match relay_schema_for_runtime(
                        &branch.runtime,
                        &branch.domain,
                        right_relay,
                    ) {
                        Ok(schema) => schema,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                correlations.iter().flat_map(|(left, right)| {
                                    [&left.message.acks, &right.message.acks]
                                }),
                                error,
                            );
                            return;
                        }
                    };
                    let materialized_stream_specs =
                        materialized_stream_specs_for_graph(&branch.runtime, &branch.domain, graph);
                    let current_branching = branch
                        .runtime
                        .inner
                        .executions
                        .get(&branch.domain)
                        .and_then(|execution| execution.relay_branchings.get(left_relay).cloned())
                        .unwrap_or_default();
                    let current_branch_schema = relay_branch_schema_for_runtime(
                        &branch.runtime,
                        &branch.domain,
                        left_relay,
                    );
                    let available_lookups = branch
                        .runtime
                        .inner
                        .executions
                        .get(&branch.domain)
                        .map(|execution| execution.lookups.clone())
                        .unwrap_or_default();
                    let udfs = branch
                        .runtime
                        .inner
                        .executions
                        .get(&branch.domain)
                        .map(|execution| execution.udfs.clone());
                    for (output_index, compiled_output_program) in compiled_output_programs
                        .iter_mut()
                        .enumerate()
                        .take(output_routes.routes.len())
                    {
                        if compiled_output_program.is_some() {
                            continue;
                        }
                        let output = &output_routes.routes[output_index];
                        if output.construction.assignments.is_empty() {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                correlations.iter().flat_map(|(left, right)| {
                                    [&left.message.acks, &right.message.acks]
                                }),
                                format!(
                                    "correlator '{}' TO output '{}' has no SET assignments",
                                    self.processor.as_str(),
                                    output.relay.as_str()
                                ),
                            );
                            return;
                        }
                        let output_schema = match relay_schema_for_runtime(
                            &branch.runtime,
                            &branch.domain,
                            &output.relay,
                        ) {
                            Ok(schema) => schema,
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    correlations.iter().flat_map(|(left, right)| {
                                        [&left.message.acks, &right.message.acks]
                                    }),
                                    error,
                                );
                                return;
                            }
                        };
                        let compiled = CorrelatorOutputCompileContext {
                            processor: &self.processor,
                            left_schema: left_schema.arrow_schema(),
                            left_sensitivity: left_schema.vm_sensitivity(),
                            right_schema: right_schema.arrow_schema(),
                            right_sensitivity: right_schema.vm_sensitivity(),
                            output_relay: &output.relay,
                            output_schema: output_schema.arrow_schema(),
                            output_sensitivity: output_schema.vm_sensitivity(),
                            construction: &output.construction,
                            runtime: RuntimeVmCompileContext {
                                available_materialized_streams: &materialized_stream_specs,
                                available_lookups: &available_lookups,
                                current_branching: &current_branching,
                                current_branch_schema: current_branch_schema.as_ref(),
                                current_branch_sensitivity: None,
                                udfs: udfs.as_ref(),
                            },
                        }
                        .compile();
                        match compiled {
                            Ok(program) => {
                                *compiled_output_program = Some(Box::new(program));
                            }
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    correlations.iter().flat_map(|(left, right)| {
                                        [&left.message.acks, &right.message.acks]
                                    }),
                                    error,
                                );
                                return;
                            }
                        }
                    }

                    let output_count = output_routes.routes.len();
                    let Some(output_programs) = compiled_output_programs
                        .iter()
                        .map(|program| program.as_deref())
                        .collect::<Option<Vec<_>>>()
                    else {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind,
                            &self.processor,
                            &self.error_policies,
                            correlations.iter().flat_map(|(left, right)| {
                                [&left.message.acks, &right.message.acks]
                            }),
                            format!(
                                "correlator '{}' output program is unavailable",
                                self.processor.as_str()
                            ),
                        );
                        return;
                    };
                    let matched = match CorrelatorMatchedBatch::from_correlations(
                        &correlations,
                        &output_programs,
                    ) {
                        Ok(matched) => matched,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                correlations.iter().flat_map(|(left, right)| {
                                    [&left.message.acks, &right.message.acks]
                                }),
                                format!(
                                    "correlator '{}' failed to build matched Arrow batches: \
                                     {error}",
                                    self.processor.as_str()
                                ),
                            );
                            return;
                        }
                    };
                    let mut pair_acks = correlations
                        .iter()
                        .map(|(left, right)| {
                            AckSet::merged([
                                left.message.acks.attached(),
                                right.message.acks.attached(),
                            ])
                        })
                        .collect::<Vec<_>>();
                    for (output_index, output_program) in output_programs.into_iter().enumerate() {
                        tokio::task::consume_budget().await;
                        let route_acks = if output_index + 1 == output_count {
                            std::mem::take(&mut pair_acks)
                        } else {
                            pair_acks.iter().map(AckSet::attached).collect()
                        };
                        let outcomes = match evaluate_correlator_output_batch(
                            &self.processor,
                            output_program,
                            &matched,
                            route_acks,
                            execution_now,
                        )
                        .await
                        {
                            Ok(outcomes) => outcomes,
                            Err((error, acks)) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    acks.iter(),
                                    format!(
                                        "correlator '{}' failed to evaluate batched output: \
                                         {error}",
                                        self.processor.as_str()
                                    ),
                                );
                                continue;
                            }
                        };
                        let policy = output_routes.routes[output_index]
                            .message_error_policy
                            .clone();
                        let output_relay = output_routes.routes[output_index].relay.clone();
                        let mut messages = Vec::new();
                        for outcome in outcomes {
                            tokio::task::consume_budget().await;
                            match outcome {
                                Ok(Some(message)) => messages.push(message),
                                Ok(None) => {}
                                Err(error) => {
                                    branch
                                        .runtime
                                        .handle_structured_message_error(MessageErrorHandling {
                                            domain: &branch.domain,
                                            node_kind: self.kind,
                                            node: &self.processor,
                                            source_route: Some(&output_relay),
                                            policy: &policy,
                                            message: error.message,
                                            error: error.error,
                                            partial_output: error.partial_output,
                                            materialized_state: error.materialized_state,
                                            ingest_metadata: None,
                                        })
                                        .await;
                                }
                            }
                        }
                        enqueue_correlator_output(
                            CorrelatorOutputContext {
                                graph,
                                branch,
                                node_kind: self.kind,
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                            },
                            output_index,
                            messages,
                            execution_now,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Junction { output_routes } => {
                    flush_branch_junction(
                        JunctionFlushContext {
                            graph,
                            branch,
                            node_kind: self.kind,
                            processor: &self.processor,
                            error_policies: &self.error_policies,
                            input_relays: &self.input_relays,
                            output_routes,
                            materialized_values: &materialized_values,
                        },
                        batch,
                    )
                    .await;
                }
                RelayProcessorOperationNode::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                    compiled_input_program,
                    output_buffers,
                    session,
                } => {
                    if output_buffers.len() != output_routes.routes.len() {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind,
                            &self.processor,
                            &self.error_policies,
                            batch.acks.iter(),
                            format!(
                                "inferencer '{}' output buffer count does not match its routes",
                                self.processor.as_str()
                            ),
                        );
                        return;
                    }
                    let now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    let route_batches = batch.into_attached_fanout(output_routes.routes.len());
                    let mut due_outputs = Vec::new();
                    for (output_index, route_batch) in route_batches.into_iter().enumerate() {
                        let output_buffer = &mut output_buffers[output_index];
                        output_buffer.push(route_batch);
                        let output = &mut output_routes.routes[output_index];
                        match output.schedule_input_flush(now, output_buffer.estimated_bytes()) {
                            Some(true) => {
                                output.force_flush_at(now);
                                due_outputs.push(output_index);
                            }
                            Some(false) => {}
                            None => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    output_buffer
                                        .pending
                                        .iter()
                                        .flat_map(|batch| batch.acks.iter()),
                                    format!(
                                        "inferencer '{}' output '{}' has no flush policy",
                                        self.processor.as_str(),
                                        output.relay.as_str()
                                    ),
                                );
                                output_buffer.clear();
                            }
                        }
                    }
                    for output_index in due_outputs {
                        flush_branch_inferencer_output(
                            InferencerFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind,
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                resource,
                                resource_version: *resource_version,
                                file,
                                inputs,
                                output_schema,
                                compiled_input_program,
                                input_relays: &self.input_relays,
                                session,
                                materialized_state: &self.materialized_state,
                            },
                            &mut output_buffers[output_index],
                            output_index,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::WasmProcessor {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    limits,
                    compiled,
                    instance,
                    replicated_state,
                    ack_map,
                    next_ack_token,
                    pending,
                } => {
                    pending.push(batch);
                    flush_branch_wasm_processor(
                        WasmFlushContext {
                            graph,
                            branch,
                            node_kind: self.kind,
                            processor: &self.processor,
                            error_policies: &self.error_policies,
                            input_relays: &self.input_relays,
                            output_routes,
                            resource,
                            resource_version: *resource_version,
                            file,
                            limits: *limits,
                            replicated_state,
                        },
                        compiled,
                        instance,
                        ack_map,
                        next_ack_token,
                        pending,
                    )
                    .await;
                }
            }
        })
    }

    pub(super) fn tick<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
        now: Timestamp,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.flush_due_collected_inputs(graph, branch, now).await;
            flush_due_processor_outputs(
                ProcessorOutputDispatchContext {
                    graph,
                    branch,
                    node_kind: self.kind,
                    source_kind: self.kind,
                    processor: &self.processor,
                    error_policies: &self.error_policies,
                    input_relays: &self.input_relays,
                    filter_source: ProcessorOutputFilterSource::InputRelays,
                    materialized_state: ProcessorMaterializedState::ResolvedAtDispatch(
                        &self.materialized_state,
                    ),
                },
                self.operation.output_routes_mut(),
                now,
            )
            .await;
            match &mut self.operation {
                RelayProcessorOperationNode::Deduplicator { .. } => {}
                RelayProcessorOperationNode::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
                    compiled_aggregates,
                    state,
                    replicated_state,
                } => {
                    let due = window_width_met(state, *width_messages, *width_duration, now);
                    let changed = flush_ready_window_processor(
                        WindowFlushContext {
                            graph,
                            node_kind: self.kind,
                            processor: &self.processor,
                            error_policies: &self.error_policies,
                            branch,
                            output_routes,
                            materialized_state: &self.materialized_state,
                        },
                        state,
                        aggregate,
                        compiled_aggregates,
                        WindowBounds {
                            width_messages: *width_messages,
                            step_messages: *step_messages,
                            width_duration: *width_duration,
                            step_duration: *step_duration,
                        },
                        now,
                    )
                    .await;
                    if due || changed {
                        replicated_state.mark_live_dirty();
                        if let Err(error) = snapshot_window_processor_live_state(
                            &self.processor,
                            replicated_state,
                            state,
                        ) {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind,
                                &self.processor,
                                &self.error_policies,
                                state.entries.iter().map(|entry| &entry.message.acks),
                                error,
                            );
                            state.clear(aggregate);
                            replicated_state.mark_live_dirty();
                        }
                    }
                }
                RelayProcessorOperationNode::Junction { .. } => {}
                RelayProcessorOperationNode::Reorderer {
                    output_routes,
                    max_time,
                    output_buffers,
                    ..
                } => {
                    let mut due_outputs = Vec::new();
                    for (output_index, output_buffer) in output_buffers.iter().enumerate() {
                        if output_buffer.is_empty() {
                            continue;
                        }
                        let max_time_due =
                            output_buffer
                                .first_received_at()
                                .is_some_and(|received_at| {
                                    checked_add_duration_to_timestamp(received_at, *max_time) <= now
                                });
                        let flush_due = output_routes.routes[output_index].flush_deadline_due(now);
                        if max_time_due || flush_due {
                            output_routes.routes[output_index].force_flush_at(now);
                            due_outputs.push(output_index);
                        }
                    }
                    for output_index in due_outputs {
                        flush_branch_reorderer_output(
                            ReordererFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind,
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                input_relays: &self.input_relays,
                                materialized_state: &self.materialized_state,
                            },
                            &mut output_buffers[output_index],
                            output_index,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Correlator {
                    max_time,
                    timeout_policy,
                    state,
                    ..
                } => {
                    let timed_out = {
                        let mut timed_out = Vec::new();

                        let mut left_remaining = Vec::new();
                        for entry in std::mem::take(&mut state.pending_left) {
                            if checked_add_duration_to_timestamp(entry.received_at, *max_time)
                                <= now
                            {
                                timed_out.push((timeout_policy.left.clone(), entry.message));
                            } else {
                                left_remaining.push(entry);
                            }
                        }
                        state.pending_left = left_remaining;

                        let mut right_remaining = Vec::new();
                        for entry in std::mem::take(&mut state.pending_right) {
                            if checked_add_duration_to_timestamp(entry.received_at, *max_time)
                                <= now
                            {
                                timed_out.push((timeout_policy.right.clone(), entry.message));
                            } else {
                                right_remaining.push(entry);
                            }
                        }
                        state.pending_right = right_remaining;

                        timed_out
                    };
                    for (action, message) in timed_out {
                        handle_correlator_timeout_action(
                            graph,
                            branch,
                            self.kind,
                            &self.processor,
                            &self.error_policies,
                            &action,
                            message,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                    compiled_input_program,
                    output_buffers,
                    session,
                } => {
                    let due_outputs = output_buffers
                        .iter()
                        .enumerate()
                        .filter_map(|(output_index, output_buffer)| {
                            (!output_buffer.pending.is_empty()
                                && output_routes.routes[output_index].flush_deadline_due(now))
                            .then_some(output_index)
                        })
                        .collect::<Vec<_>>();
                    for output_index in due_outputs {
                        output_routes.routes[output_index].force_flush_at(now);
                        flush_branch_inferencer_output(
                            InferencerFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind,
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                resource,
                                resource_version: *resource_version,
                                file,
                                inputs,
                                output_schema,
                                compiled_input_program,
                                input_relays: &self.input_relays,
                                session,
                                materialized_state: &self.materialized_state,
                            },
                            &mut output_buffers[output_index],
                            output_index,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::WasmProcessor {
                    output_routes,
                    instance,
                    replicated_state,
                    ack_map,
                    ..
                } => {
                    let Some(branch_instance) = instance.as_mut() else {
                        return;
                    };
                    let due_timeouts = branch_instance.take_due_timeout_requests(now);
                    if due_timeouts.is_empty() {
                        return;
                    }
                    if output_routes.routes.is_empty() {
                        for (_, context) in std::mem::take(ack_map) {
                            context.acks.no_ack(format!(
                                "wasm processor '{}' has no output destinations",
                                self.processor.as_str()
                            ));
                        }
                        return;
                    }
                    let Some(schemas) = wasm_guest_call_schemas(
                        branch,
                        &self.processor,
                        &self.input_relays,
                        output_routes,
                        ack_map,
                    ) else {
                        return;
                    };
                    let output_key = branch.key.clone();
                    for timeout in due_timeouts {
                        let timeout_result = instance
                            .as_mut()
                            .verified(
                                "the let-else above returned unless this branch holds an instance",
                            )
                            .on_timeout(timeout.handle)
                            .await;
                        let outputs = match timeout_result {
                            Ok(outputs) => outputs,
                            Err(error) => {
                                let resource_limit_exceeded = error.is_resource_limit_exceeded();
                                let reason = format!(
                                    "wasm processor '{}' failed timeout callback: {}",
                                    self.processor.as_str(),
                                    error
                                );
                                branch.runtime.handle_general_error_for_acks(
                                    &branch.domain,
                                    self.kind,
                                    &self.processor,
                                    &self.error_policies,
                                    ack_map.values().map(|context| &context.acks),
                                    reason,
                                );
                                ack_map.clear();
                                if resource_limit_exceeded {
                                    *instance = None;
                                }
                                return;
                            }
                        };
                        if dispatch_wasm_output_envelopes(
                            WasmOutputContext {
                                graph,
                                branch,
                                node_kind: self.kind,
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                input_relays: &self.input_relays,
                                input_schema: &schemas.input,
                                output_schemas: &schemas.outputs,
                                key: &output_key,
                                dispatch_error: "failed to forward timeout output",
                            },
                            outputs,
                            ack_map,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                    }
                    if let Err(error) = persist_wasm_guest_state(
                        &branch.runtime,
                        &self.processor,
                        replicated_state,
                        instance,
                    )
                    .await
                    {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind,
                            &self.processor,
                            &self.error_policies,
                            std::iter::empty::<&AckSet>(),
                            error,
                        );
                    }
                }
            }
        })
    }

    /// Asks a WASM guest to release the output it is still buffering, because the host is
    /// quiescing this branch for a handoff or shutdown.
    ///
    /// Native processors buffer inside runtime-owned route state that the caller force-flushes
    /// directly; a guest owns its own buffering, so the host has to ask before it can conclude the
    /// branch has drained.
    pub(super) fn flush_guest_buffers<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let RelayProcessorOperationNode::WasmProcessor {
                output_routes,
                instance,
                ack_map,
                ..
            } = &mut self.operation
            else {
                return;
            };
            if instance.is_none() {
                return;
            }
            if output_routes.routes.is_empty() {
                return;
            }
            let Some(schemas) = wasm_guest_call_schemas(
                branch,
                &self.processor,
                &self.input_relays,
                output_routes,
                ack_map,
            ) else {
                return;
            };
            let flush_result = instance
                .as_mut()
                .verified("the let-else above returned unless this branch holds an instance")
                .flush()
                .await;
            let outputs = match flush_result {
                Ok(outputs) => outputs,
                Err(error) => {
                    let resource_limit_exceeded = error.is_resource_limit_exceeded();
                    let reason = format!(
                        "wasm processor '{}' failed quiesce flush: {}",
                        self.processor.as_str(),
                        error
                    );
                    branch.runtime.handle_general_error_for_acks(
                        &branch.domain,
                        self.kind,
                        &self.processor,
                        &self.error_policies,
                        ack_map.values().map(|context| &context.acks),
                        reason,
                    );
                    ack_map.clear();
                    if resource_limit_exceeded {
                        *instance = None;
                    }
                    return;
                }
            };
            if outputs.is_empty() {
                return;
            }
            let output_key = branch.key.clone();
            dispatch_wasm_output_envelopes(
                WasmOutputContext {
                    graph,
                    branch,
                    node_kind: self.kind,
                    processor: &self.processor,
                    error_policies: &self.error_policies,
                    output_routes,
                    input_relays: &self.input_relays,
                    input_schema: &schemas.input,
                    output_schemas: &schemas.outputs,
                    key: &output_key,
                    dispatch_error: "failed to forward quiesce flush output",
                },
                outputs,
                ack_map,
            )
            .await
            .discarded(
                "the processor error policy already handled every failure this dispatch produced",
            );
        })
    }

    pub(super) fn snapshot_live_state(&mut self, branch: &mut BranchRuntime) -> Result<(), String> {
        let RelayProcessorOperationNode::WindowProcessor {
            aggregate,
            state,
            replicated_state,
            ..
        } = &mut self.operation
        else {
            return Ok(());
        };
        if !replicated_state.live_dirty.load(Ordering::SeqCst) {
            return Ok(());
        }
        if let Err(error) =
            snapshot_window_processor_live_state(&self.processor, replicated_state, state)
        {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                self.kind,
                &self.processor,
                &self.error_policies,
                state.entries.iter().map(|entry| &entry.message.acks),
                error.clone(),
            );
            state.clear(aggregate);
            replicated_state.mark_live_dirty();
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn spawn_snapshot_task(
        &self,
        runtime: &Runtime,
        shutdown_tx: &watch::Sender<bool>,
    ) -> SpawnedSnapshotTask {
        match &self.operation {
            RelayProcessorOperationNode::Deduplicator { state, .. } => SpawnedSnapshotTask {
                task: runtime.spawn_deduplicator_snapshot_task(shutdown_tx, state.clone()),
                requests: None,
            },
            RelayProcessorOperationNode::WindowProcessor {
                replicated_state, ..
            } => {
                let (request_tx, request_rx) = mpsc::channel(1);
                let task = runtime.spawn_window_processor_snapshot_task(
                    shutdown_tx,
                    replicated_state.clone(),
                    request_tx,
                );
                let requests = task.is_some().then_some(request_rx);
                SpawnedSnapshotTask { task, requests }
            }
            RelayProcessorOperationNode::Junction { .. }
            | RelayProcessorOperationNode::Reorderer { .. }
            | RelayProcessorOperationNode::Correlator { .. }
            | RelayProcessorOperationNode::Inferencer { .. }
            | RelayProcessorOperationNode::WasmProcessor { .. } => SpawnedSnapshotTask {
                task: None,
                requests: None,
            },
        }
    }

    pub(super) fn next_deadline(&self) -> Option<Timestamp> {
        let operation_deadline = match &self.operation {
            RelayProcessorOperationNode::Deduplicator { .. } => None,
            RelayProcessorOperationNode::WindowProcessor {
                width_duration,
                state,
                ..
            } => window_next_deadline(state, *width_duration),
            RelayProcessorOperationNode::Junction { .. } => None,
            RelayProcessorOperationNode::Reorderer {
                max_time,
                output_buffers,
                ..
            } => output_buffers
                .iter()
                .filter_map(ReordererOutputBuffer::first_received_at)
                .map(|received_at| checked_add_duration_to_timestamp(received_at, *max_time))
                .min(),
            RelayProcessorOperationNode::Correlator {
                max_time, state, ..
            } => state
                .pending_left
                .iter()
                .chain(state.pending_right.iter())
                .map(|entry| checked_add_duration_to_timestamp(entry.received_at, *max_time))
                .min(),
            RelayProcessorOperationNode::Inferencer { .. } => None,
            RelayProcessorOperationNode::WasmProcessor { instance, .. } => {
                wasm_instance_next_deadline(instance.as_deref())
            }
        };
        operation_deadline
            .into_iter()
            .chain(
                self.input_collectors
                    .values()
                    .filter_map(|collector| collector.deadline),
            )
            .chain(self.operation.output_routes().next_flush())
            .min()
    }
}
