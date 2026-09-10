use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) enum ProcessorOutputFilterSource<'a> {
    InputRelays,
    OutputRelay,
    Inferencer(InferencerFilterMapTensors<'a>),
}

impl ProcessorOutputFilterSource<'_> {
    pub(super) fn relays(&self, input_relays: &[RelayName]) -> Vec<RelayName> {
        match self {
            Self::InputRelays | Self::OutputRelay | Self::Inferencer(_) => input_relays.to_vec(),
        }
    }

    pub(super) fn inferencer_tensors(&self) -> Option<InferencerFilterMapTensors<'_>> {
        if let Self::Inferencer(tensors) = self {
            Some(*tensors)
        } else {
            None
        }
    }
}

pub(super) struct ProcessorOutputDispatchContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) source_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) input_relays: &'a [RelayName],
    pub(super) filter_source: ProcessorOutputFilterSource<'a>,
    pub(super) materialized_state: ProcessorMaterializedState<'a>,
    pub(super) execution_now: Timestamp,
}

/// How a dispatched batch obtains the node-wide materialized state its output routes read.
///
/// Materialized dependencies are declared once per node, so every route of a batch reads one
/// snapshot. Resolving them per route would repeat state-store reads and, because a raw read
/// applies no policy, would silently drop `DEFAULT` values.
pub(super) enum ProcessorMaterializedState<'a> {
    /// The node resolved its declared dependencies while admitting this batch, so route
    /// construction reuses that exact snapshot.
    Admitted(&'a HashMap<String, RuntimeValue>),
    /// The batch was buffered past admission, so the node-wide dependencies are resolved again
    /// against the dispatched batch's own branch.
    ResolvedAtDispatch(&'a [nervix_models::MaterializedStateDependency]),
}

impl ProcessorMaterializedState<'_> {
    /// Resolves the node-wide dependencies once for a dispatched batch.
    ///
    /// `REQUIRED SKIP` and `REQUIRED WAIT` gate a node's *input*; reaching either here means the
    /// state backing already-admitted work disappeared, which branch eviction is expected to
    /// prevent by dropping that buffered work with the branch.
    pub(super) async fn resolve(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        node_kind: ModelKind,
        node: &ModelName,
        branch_key: &Option<BranchKey>,
        execution_now: Timestamp,
    ) -> Result<HashMap<String, RuntimeValue>, String> {
        match self {
            Self::Admitted(values) => Ok((*values).clone()),
            Self::ResolvedAtDispatch(dependencies) => {
                match runtime
                    .resolve_materialized_dependencies(
                        domain,
                        branch_key,
                        dependencies,
                        execution_now,
                    )
                    .await?
                {
                    MaterializedDependencyResolution::Ready(values) => Ok(values),
                    MaterializedDependencyResolution::Skip => Err(format!(
                        "{} '{}' requires materialized state that was evicted after the batch was \
                         admitted",
                        node_kind.as_str(),
                        node.as_str()
                    )),
                    MaterializedDependencyResolution::Wait => Err(format!(
                        "{} '{}' awaits materialized state that was evicted after the batch was \
                         admitted",
                        node_kind.as_str(),
                        node.as_str()
                    )),
                }
            }
        }
    }
}

pub(super) struct PendingProcessorOutputBatch {
    pub(super) output_index: usize,
    pub(super) input_rows: Vec<usize>,
    pub(super) key: Option<BranchKey>,
    pub(super) batch: RuntimeRecordBatch,
    pub(super) metadata: Vec<RuntimeRecordMetadata>,
}

impl PendingProcessorOutputBatch {
    pub(super) fn into_relay_batch(self, acks: Vec<AckSet>) -> Result<RelayRecordBatch, String> {
        RelayRecordBatch::from_filtered_parts(self.key, self.batch, self.metadata, acks)
    }
}

pub(super) fn pending_output_batches_by_key(
    output_index: usize,
    input_rows: &[usize],
    keys: Vec<Option<BranchKey>>,
    batch: RuntimeRecordBatch,
    metadata: &[RuntimeRecordMetadata],
) -> Result<Vec<PendingProcessorOutputBatch>, String> {
    if input_rows.len() != keys.len()
        || input_rows.len() != batch.batch().num_rows()
        || input_rows.len() != metadata.len()
    {
        return Err(format!(
            "pending output has {} input rows, {} keys, {} Arrow rows, and {} metadata rows",
            input_rows.len(),
            keys.len(),
            batch.batch().num_rows(),
            metadata.len()
        ));
    }
    let mut groups = Vec::<(Option<BranchKey>, Vec<usize>)>::new();
    let mut positions = HashMap::<Option<BranchKey>, usize>::default();
    for (row, key) in keys.into_iter().enumerate() {
        if let Some(position) = positions.get(&key).copied() {
            groups[position].1.push(row);
        } else {
            positions.insert(key.clone(), groups.len());
            groups.push((key, vec![row]));
        }
    }
    groups
        .into_iter()
        .map(|(key, rows)| {
            Ok(PendingProcessorOutputBatch {
                output_index,
                input_rows: rows.iter().map(|row| input_rows[*row]).collect(),
                key,
                batch: batch.take(&rows)?,
                metadata: rows.iter().map(|row| metadata[*row].clone()).collect(),
            })
        })
        .collect()
}

pub(super) struct PendingProcessorOutputMessageError {
    pub(super) row: usize,
    pub(super) key: Option<BranchKey>,
    pub(super) record: RuntimeRow,
    pub(super) error: StructuredMessageError,
    pub(super) partial_output: Option<RuntimeRecordBatch>,
    pub(super) materialized_state: HashMap<String, RuntimeValue>,
}

pub(super) fn processor_output_input_sensitivity(
    branch: &BranchRuntime,
    relays: &[RelayName],
) -> VmSchemaSensitivity {
    let Some(relay) = relays.first() else {
        return VmSchemaSensitivity::default();
    };
    let Ok(schema) = relay_schema_for_runtime(&branch.runtime, &branch.domain, relay) else {
        return VmSchemaSensitivity::default();
    };
    schema.vm_sensitivity()
}

/// Work that every output route of one dispatched batch shares.
///
/// Output routes differ only in their construction: the node-wide materialized snapshot, the
/// execution clock, the relay schemas and the columns projected from the carrier batch are the
/// same for all of them. Resolving those once per batch keeps a fan-out node from repeating
/// state-store reads, relay-schema lookups and lookup-key programs per route.
pub(super) struct ProcessorOutputBatchScope {
    pub(super) side_inputs: HashMap<String, RuntimeValue>,
    pub(super) state_snapshot: HashMap<String, RuntimeValue>,
    pub(super) execution_now: Timestamp,
    /// Indexed by output route. Only routes this dispatch selected are resolved; a single-route
    /// flush must not be aborted by an unrelated route whose relay schema is missing.
    pub(super) output_schemas: Vec<Option<Arc<CompiledSchema>>>,
    pub(super) shared: SharedBatchColumns,
}

pub(super) async fn evaluate_processor_output_events(
    context: &mut ProcessorOutputDispatchContext<'_>,
    output: &mut RelayProcessorOutputNode,
    output_index: usize,
    batch: &RelayRecordBatch,
    scope: &mut ProcessorOutputBatchScope,
) -> Result<
    (
        Vec<PendingProcessorOutputBatch>,
        Vec<PendingProcessorOutputMessageError>,
    ),
    PlannedGeneralError,
> {
    let Some(output_schema) = scope.output_schemas[output_index].clone() else {
        return Err(PlannedGeneralError {
            acks: batch.acks.clone(),
            reason: format!(
                "{} '{}' evaluated output route '{}' without preparing its relay schema",
                context.node_kind.as_str(),
                context.processor.as_str(),
                output.relay.as_str()
            ),
        });
    };
    let Some(program) = output.compiled_program.as_ref() else {
        let projected = batch
            .batch
            .project(output_schema.arrow_schema())
            .map_err(|error| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "{} '{}' failed to project output relay '{}': {}",
                    context.node_kind.as_str(),
                    context.processor.as_str(),
                    output.relay.as_str(),
                    error
                ),
            })?;
        return Ok((
            vec![PendingProcessorOutputBatch {
                output_index,
                input_rows: (0..projected.batch().num_rows()).collect(),
                key: batch.key.clone(),
                batch: projected,
                metadata: batch.metadata.clone(),
            }],
            Vec::new(),
        ));
    };

    let executed = execute_filter_map_program_on_batch(
        context.node_kind.as_str(),
        context.processor,
        program,
        FilterMapBatchInputs {
            carrier: &batch.batch,
            namespace_batches: &[],
            keys: &batch.keys,
            side_inputs: &scope.side_inputs,
            ingest_metadata: None,
        },
        scope.execution_now,
        batch.acks.clone(),
        Some(&mut scope.shared),
    )
    .await?;
    let mut success_output_rows = Vec::new();
    let mut success_input_rows = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, input_row) in executed.selected_rows.iter().enumerate() {
        if let Some(side_error) = executed.batch.errors().row(output_row).first() {
            let partial_output = captured_partial_output(&executed.batch, output_row);
            let record = batch
                .runtime_row(input_row)
                .map_err(|error| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "{} '{}' failed to materialize FILTER-MAP error input row: {}",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        error
                    ),
                })?;
            message_errors.push(PendingProcessorOutputMessageError {
                row: input_row,
                key: batch.keys[input_row].clone(),
                record,
                error: program.structured_side_error(
                    scope.execution_now,
                    format!(
                        "{} '{}' FILTER-MAP side error {}: {} at {}",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        side_error.code.as_str(),
                        side_error.message,
                        side_error.span
                    ),
                    side_error.span,
                    MessageErrorOperation::Set,
                ),
                partial_output,
                materialized_state: scope.state_snapshot.clone(),
            });
            continue;
        }
        success_output_rows.push(output_row);
        success_input_rows.push(input_row);
    }
    let output_batches = if success_output_rows.is_empty() {
        Vec::new()
    } else {
        let output_batch =
            vm_typed_batch_selected_rows_to_runtime_batch(&executed.batch, &success_output_rows)
                .map_err(|error| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "{} '{}' failed to materialize successful FILTER-MAP rows: {}",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        error
                    ),
                })?;
        if output_batch.schema().as_ref() != output_schema.arrow_schema().as_ref() {
            return Err(PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "{} '{}' FILTER-MAP output schema does not match relay '{}'",
                    context.node_kind.as_str(),
                    context.processor.as_str(),
                    output.relay.as_str()
                ),
            });
        }
        let metadata = success_input_rows
            .iter()
            .map(|input_row| batch.metadata[*input_row].clone())
            .collect::<Vec<_>>();
        vec![PendingProcessorOutputBatch {
            output_index,
            input_rows: success_input_rows,
            key: batch.key.clone(),
            batch: output_batch,
            metadata,
        }]
    };
    Ok((output_batches, message_errors))
}

pub(super) async fn dispatch_processor_outputs(
    context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    batch: RelayRecordBatch,
) -> Option<Vec<AckSet>> {
    dispatch_selected_processor_outputs(context, outputs, batch, None, false).await
}

pub(super) async fn dispatch_processor_output(
    context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    batch: RelayRecordBatch,
    output_index: usize,
) -> Option<Vec<AckSet>> {
    dispatch_selected_processor_outputs(context, outputs, batch, Some(output_index), true).await
}

pub(super) async fn dispatch_selected_processor_outputs(
    mut context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    batch: RelayRecordBatch,
    selected_output: Option<usize>,
    flush_selected_immediately: bool,
) -> Option<Vec<AckSet>> {
    if batch.message_count() == 0 {
        return Some(Vec::new());
    }

    let output_relays = outputs
        .routes
        .iter()
        .map(|output| output.relay.clone())
        .collect::<Vec<_>>();
    let selects_output =
        |output_index: usize| selected_output.is_none_or(|selected| selected == output_index);

    let mut output_schemas = vec![None; output_relays.len()];
    for (output_index, output) in outputs.routes.iter_mut().enumerate() {
        if !selects_output(output_index) {
            continue;
        }
        tokio::task::consume_budget().await;
        let output_schema = match relay_schema_for_runtime(
            &context.branch.runtime,
            &context.branch.domain,
            &output_relays[output_index],
        ) {
            Ok(schema) => schema,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        batch.acks.iter(),
                        error.to_string(),
                    );
                return None;
            }
        };
        if let Err(error) =
            compile_processor_output_program(&mut context, output, &batch, &output_schema)
        {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    error.acks.iter(),
                    error.reason,
                );
            return None;
        }
        output_schemas[output_index] = Some(output_schema);
    }

    // Resolved before any route is evaluated so a state failure cannot discard routes that were
    // already evaluated, and so every route of this batch observes one snapshot.
    let side_inputs = match context
        .materialized_state
        .resolve(
            &context.branch.runtime,
            &context.branch.domain,
            context.node_kind,
            context.processor,
            &batch.key,
            context.execution_now,
        )
        .await
    {
        Ok(side_inputs) => side_inputs,
        Err(reason) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    batch.acks.iter(),
                    reason,
                );
            return None;
        }
    };
    let mut scope = ProcessorOutputBatchScope {
        state_snapshot: relay_state_snapshot_from_side_inputs(&side_inputs),
        side_inputs,
        execution_now: context.execution_now,
        output_schemas,
        shared: SharedBatchColumns::default(),
    };

    let mut pending_batches = Vec::new();
    let mut pending_errors = Vec::new();
    for (output_index, output) in outputs.routes.iter_mut().enumerate() {
        if !selects_output(output_index) {
            continue;
        }
        tokio::task::consume_budget().await;
        let (batches, errors) = match evaluate_processor_output_events(
            &mut context,
            output,
            output_index,
            &batch,
            &mut scope,
        )
        .await
        {
            Ok(events) => events,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        error.acks.iter(),
                        error.reason,
                    );
                return None;
            }
        };
        pending_batches.extend(batches);
        pending_errors.extend(errors.into_iter().map(|error| (output_index, error)));
    }

    let mut delivery_counts = vec![0usize; batch.acks.len()];
    for pending_batch in &pending_batches {
        for row in &pending_batch.input_rows {
            delivery_counts[*row] += 1;
        }
    }
    for (_, error) in &pending_errors {
        delivery_counts[error.row] += 1;
    }

    let RelayRecordBatch { acks, .. } = batch;
    let mut ack_queues = Vec::with_capacity(delivery_counts.len());
    for (row, ack) in acks.into_iter().enumerate() {
        let delivery_count = delivery_counts[row];
        if delivery_count == 0 {
            ack.ack_success();
            ack_queues.push(VecDeque::new());
            continue;
        }
        let mut queue = VecDeque::with_capacity(delivery_count);
        for _ in 1..delivery_count {
            queue.push_back(ack.attached());
        }
        queue.push_front(ack);
        ack_queues.push(queue);
    }

    let mut batches_by_output = vec![Vec::new(); output_relays.len()];
    for pending_batch in pending_batches {
        let mut batch_acks = Vec::with_capacity(pending_batch.input_rows.len());
        for row in &pending_batch.input_rows {
            let Some(acks) = ack_queues[*row].pop_front() else {
                continue;
            };
            batch_acks.push(acks);
        }
        if batch_acks.len() != pending_batch.input_rows.len() {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    batch_acks.iter(),
                    "processor output batch ack count does not match selected row count"
                        .to_string(),
                );
            return None;
        }
        let output_index = pending_batch.output_index;
        let error_acks = batch_acks.clone();
        match pending_batch.into_relay_batch(batch_acks) {
            Ok(batch) => batches_by_output[output_index].push(batch),
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        error_acks.iter(),
                        error,
                    );
                return None;
            }
        }
    }

    for (output_index, error) in pending_errors {
        let Some(acks) = ack_queues[error.row].pop_front() else {
            continue;
        };
        context
            .branch
            .runtime
            .handle_structured_message_error(MessageErrorHandling {
                domain: &context.branch.domain,
                node_kind: context.node_kind,
                node: context.processor,
                source_route: Some(&outputs.routes[output_index].relay),
                policy: &outputs.routes[output_index].message_error_policy,
                message: RelayMessage {
                    key: error.key,
                    record: error.record,
                    acks,
                },
                error: error.error,
                partial_output: error.partial_output,
                materialized_state: error.materialized_state,
                ingest_metadata: None,
                execution_now: scope.execution_now,
            })
            .await;
    }

    let execution_now = scope.execution_now;
    let mut dispatched_acks = Vec::new();
    for (output_index, mut batches) in batches_by_output.into_iter().enumerate() {
        let output = &mut outputs.routes[output_index];
        let relay = &output_relays[output_index];
        if batches.is_empty() {
            continue;
        }
        let mut should_flush = false;
        for batch in batches.drain(..) {
            should_flush |= output.enqueue(batch, execution_now);
        }
        if flush_selected_immediately
            && selected_output.is_some_and(|selected| selected == output_index)
        {
            output.force_flush_at(execution_now);
            should_flush = true;
        }
        if !should_flush {
            continue;
        }
        let pending = output.take_pending();
        let pending_acks = pending
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        let forwarded = match RelayRecordBatch::concat(pending) {
            Ok(batch) => batch,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        pending_acks.iter(),
                        format!(
                            "{} '{}' failed to concat output batches for relay '{}': {}",
                            context.node_kind.as_str(),
                            context.processor.as_str(),
                            relay.as_str(),
                            error
                        ),
                    );
                return None;
            }
        };
        if context
            .branch
            .dispatch_output(
                context.graph,
                output,
                context.source_kind,
                context.processor,
                &forwarded,
            )
            .await
            .is_ok()
        {
            dispatched_acks.extend(forwarded.acks.iter().cloned());
        } else {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    forwarded.acks.iter(),
                    format!(
                        "{} '{}' failed to forward message to relay '{}'",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        relay.as_str()
                    ),
                );
            return None;
        }
    }
    Some(dispatched_acks)
}

pub(super) async fn flush_due_processor_outputs(
    context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    now: Timestamp,
) {
    for output in &mut outputs.routes {
        if !output.flush_due(now) {
            continue;
        }
        let pending = output.take_pending();
        let pending_acks = pending
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        let forwarded = match RelayRecordBatch::concat(pending) {
            Ok(batch) => batch,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        pending_acks.iter(),
                        format!(
                            "{} '{}' failed to concat buffered output batches for relay '{}': {}",
                            context.node_kind.as_str(),
                            context.processor.as_str(),
                            output.relay.as_str(),
                            error
                        ),
                    );
                continue;
            }
        };
        if context
            .branch
            .dispatch_output(
                context.graph,
                output,
                context.source_kind,
                context.processor,
                &forwarded,
            )
            .await
            .is_ok()
        {
            for ack in &forwarded.acks {
                ack.ack_success();
            }
        } else {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    forwarded.acks.iter(),
                    format!(
                        "{} '{}' failed to forward buffered output to relay '{}'",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        output.relay.as_str()
                    ),
                );
        }
    }
}
