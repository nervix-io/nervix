use error_stack::{Report, ResultExt as _};

use super::*;

/// Every way dispatching a stateful processor's output fails before it reaches its routes.
#[derive(Debug, thiserror::Error)]
pub(super) enum ProcessorOutputError {
    #[error("failed to build the processor output relay batch")]
    RelayBatch,
    #[error(
        "pending output has {input_rows} input rows, {keys} keys, {arrow_rows} Arrow rows, and \
         {metadata_rows} metadata rows"
    )]
    PendingShape {
        input_rows: usize,
        keys: usize,
        arrow_rows: usize,
        metadata_rows: usize,
    },
    #[error("failed to select the pending output rows of one branch")]
    TakeBranchRows,
    #[error("an output batch holds fewer acknowledgements than the rows it selected")]
    SelectedRowAcks,
}

pub(super) struct ProcessorOutputDispatchContext<'a> {
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) source_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
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
        routing: &DomainRoutingSnapshot,
        context: &ProcessorOutputDispatchContext<'_>,
        branch_key: &Option<BranchKey>,
    ) -> error_stack::Result<HashMap<String, RuntimeValue>, ProcessorMaterializedError> {
        let dependencies = match self {
            Self::Admitted(values) => return Ok((*values).clone()),
            Self::ResolvedAtDispatch(dependencies) => dependencies,
        };
        let resolution = context
            .branch
            .runtime
            .resolve_materialized_dependencies(
                routing,
                &context.branch.domain,
                branch_key,
                dependencies,
                context.execution_now,
            )
            .await
            .change_context_lazy(|| ProcessorMaterializedError::Resolve {
                branch: BranchScope::from(branch_key),
            })?;
        match resolution {
            MaterializedDependencyResolution::Ready(values) => Ok(values),
            MaterializedDependencyResolution::Skip => Err(Report::new(
                ProcessorMaterializedError::EvictedRequiredSkip {
                    branch: BranchScope::from(branch_key),
                },
            )),
            MaterializedDependencyResolution::Wait => Err(Report::new(
                ProcessorMaterializedError::EvictedRequiredWait {
                    branch: BranchScope::from(branch_key),
                },
            )),
        }
    }
}

pub(super) struct PendingProcessorOutputBatch {
    pub(super) output_index: usize,
    pub(super) input_rows: Vec<usize>,
    pub(super) key: Option<BranchKey>,
    pub(super) batch: RuntimeRecordBatch,
    pub(super) metadata: RecordMetadataColumns,
}

impl PendingProcessorOutputBatch {
    pub(super) fn into_relay_batch(
        self,
        acks: Vec<AckSet>,
    ) -> error_stack::Result<RelayRecordBatch, ProcessorOutputError> {
        RelayRecordBatch::from_filtered_parts(self.key, self.batch, self.metadata, acks)
            .change_context(ProcessorOutputError::RelayBatch)
    }
}

pub(super) fn pending_output_batches_by_key(
    output_index: usize,
    input_rows: &[usize],
    keys: Vec<Option<BranchKey>>,
    batch: RuntimeRecordBatch,
    metadata: &RecordMetadataColumns,
) -> error_stack::Result<Vec<PendingProcessorOutputBatch>, ProcessorOutputError> {
    if input_rows.len() != keys.len()
        || input_rows.len() != batch.batch().num_rows()
        || input_rows.len() != metadata.len()
    {
        return Err(Report::new(ProcessorOutputError::PendingShape {
            input_rows: input_rows.len(),
            keys: keys.len(),
            arrow_rows: batch.batch().num_rows(),
            metadata_rows: metadata.len(),
        }));
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
    let mut pending = Vec::with_capacity(groups.len());
    for (key, rows) in groups {
        let branch_batch = batch
            .take(&rows)
            .change_context(ProcessorOutputError::TakeBranchRows)?;
        let branch_metadata = metadata.take(&rows).verified(
            "the shape check above proved the metadata has one row for every grouped key",
        );
        pending.push(PendingProcessorOutputBatch {
            output_index,
            input_rows: rows.iter().map(|row| input_rows[*row]).collect(),
            key,
            batch: branch_batch,
            metadata: branch_metadata,
        });
    }
    Ok(pending)
}

pub(super) struct PendingProcessorOutputMessageError {
    pub(super) row: usize,
    pub(super) key: Option<BranchKey>,
    pub(super) record: RuntimeRow,
    pub(super) error: StructuredMessageError,
    pub(super) partial_output: Option<RuntimeRecordBatch>,
    pub(super) materialized_state: HashMap<String, RuntimeValue>,
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

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(reason = "the admitted expression executor owns its generic effects")
)]
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
    PlannedGeneralFailure,
> {
    let operation = MessageErrorOperation::Set;
    let Some(output_schema) = scope.output_schemas[output_index].clone() else {
        let error = Report::new(PlannedGeneralError::OutputSchemaUnprepared {
            relay: output.relay.clone(),
        });
        return Err(PlannedGeneralFailure::new(error, batch.acks.clone()));
    };
    let Some(program) = output.compiled_program.as_ref() else {
        let projected = batch
            .batch
            .project(output_schema.arrow_schema())
            .map_err(|error| {
                PlannedGeneralFailure::new(
                    error.change_context(PlannedGeneralError::ProjectOutput {
                        relay: output.relay.clone(),
                    }),
                    batch.acks.clone(),
                )
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
        ProgramRun {
            executor: context.branch.runtime.executor(),
            now: scope.execution_now,
        },
        program,
        FilterMapBatchInputs {
            carrier: &batch.batch,
            namespace_batches: &[],
            keys: &batch.keys,
            side_inputs: &scope.side_inputs,
            ingest_metadata: None,
        },
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
            let record = batch.runtime_row(input_row).map_err(|error| {
                PlannedGeneralFailure::new(
                    error.change_context(PlannedGeneralError::MaterializeErrorInput {
                        operation,
                        row: input_row,
                    }),
                    batch.acks.clone(),
                )
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
                        side_error.code().as_str(),
                        side_error.reason,
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
                .map_err(|error| {
                    PlannedGeneralFailure::new(
                        error.change_context(PlannedGeneralError::MaterializeOutput { operation }),
                        batch.acks.clone(),
                    )
                })?;
        if output_batch.schema().as_ref() != output_schema.arrow_schema().as_ref() {
            let error = Report::new(PlannedGeneralError::OutputSchemaMismatch {
                relay: output.relay.clone(),
            });
            return Err(PlannedGeneralFailure::new(error, batch.acks.clone()));
        }
        let metadata = batch.metadata.take(&success_input_rows).verified(
            "the program selects rows of this batch, whose metadata has one entry for every row",
        );
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

    let routing = match context.branch.routing_snapshot.as_ref().cloned() {
        Some(routing) => routing,
        None => {
            let error = Report::new(DomainRoutingError::SnapshotNotResolved {
                domain: context.branch.domain.clone(),
            });
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    batch.acks.iter(),
                    &error,
                );
            return None;
        }
    };

    let output_relays = outputs
        .routes
        .iter()
        .map(|output| output.relay.clone())
        .collect::<Vec<_>>();
    let selects_output =
        |output_index: usize| selected_output.is_none_or(|selected| selected == output_index);

    let mut output_schemas = vec![None; output_relays.len()];
    for (output_index, _) in outputs.routes.iter_mut().enumerate() {
        if !selects_output(output_index) {
            continue;
        }
        nervix_primitives::task::consume_budget().await;
        let output_schema = match relay_schema_for_routing(
            &routing,
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
                        &error,
                    );
                return None;
            }
        };
        output_schemas[output_index] = Some(output_schema);
    }

    // Resolved before any route is evaluated so a state failure cannot discard routes that were
    // already evaluated, and so every route of this batch observes one snapshot.
    let side_inputs = match context
        .materialized_state
        .resolve(&routing, &context, &batch.key)
        .await
    {
        Ok(side_inputs) => side_inputs,
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
                    &error,
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
        nervix_primitives::task::consume_budget().await;
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
            Err(failure) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        failure.acks.iter(),
                        &failure.error,
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
                    &Report::new(ProcessorOutputError::SelectedRowAcks),
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
                        &error,
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
                routing: context.branch.routing_snapshot.as_deref(),
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

    let domain_clock = context.branch.domain_clock.clone();
    let flush_snapshot = match domain_clock.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let acks = batches_by_output
                .iter()
                .flatten()
                .flat_map(|batch| batch.acks.iter().cloned())
                .collect::<Vec<_>>();
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    acks.iter(),
                    &error.change_context(RouteOutputError::BufferClock),
                );
            return None;
        }
    };
    let mut dispatched_acks = Vec::new();
    for (output_index, mut batches) in batches_by_output.into_iter().enumerate() {
        let output = &mut outputs.routes[output_index];
        let relay = &output_relays[output_index];
        if batches.is_empty() {
            continue;
        }
        let mut should_flush = false;
        for batch in batches.drain(..) {
            match output.enqueue(batch, &domain_clock, &flush_snapshot) {
                Ok(flush) => should_flush |= flush,
                Err(error) => {
                    let pending = output.take_pending();
                    let acks = pending
                        .iter()
                        .flat_map(|batch| batch.acks.iter().cloned())
                        .collect::<Vec<_>>();
                    context
                        .branch
                        .runtime
                        .handle_internal_processor_error_for_acks(
                            &context.branch.domain,
                            context.node_kind,
                            context.processor,
                            context.error_policies,
                            acks.iter(),
                            &error.change_context(RouteOutputError::StartFlushDeadline {
                                relay: relay.clone(),
                            }),
                        );
                    return None;
                }
            }
        }
        if flush_selected_immediately
            && selected_output.is_some_and(|selected| selected == output_index)
        {
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
                        &error.change_context(RouteOutputError::Concatenate {
                            relay: relay.clone(),
                        }),
                    );
                return None;
            }
        };
        if context
            .branch
            .dispatch_output(output, context.source_kind, context.processor, &forwarded)
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
                    &Report::new(RouteOutputError::Forward {
                        relay: relay.clone(),
                    }),
                );
            return None;
        }
    }
    Some(dispatched_acks)
}

pub(super) async fn flush_due_processor_outputs(
    context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    _now: Timestamp,
) {
    flush_processor_outputs(context, outputs, ProcessorOutputFlush::Due).await;
}

pub(super) async fn flush_all_processor_outputs(
    context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
) {
    flush_processor_outputs(context, outputs, ProcessorOutputFlush::All).await;
}

#[derive(Clone, Copy)]
enum ProcessorOutputFlush {
    Due,
    All,
}

async fn flush_processor_outputs(
    context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    flush: ProcessorOutputFlush,
) {
    let domain_clock = context.branch.domain_clock.clone();
    let snapshot = match flush {
        ProcessorOutputFlush::All => None,
        ProcessorOutputFlush::Due => match domain_clock.snapshot() {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                // The clock failed for every route at once, so the output all of them hold fails
                // as one.
                let mut acks = Vec::new();
                for output in &mut outputs.routes {
                    let pending = output.take_pending();
                    acks.extend(pending.iter().flat_map(|batch| batch.acks.iter().cloned()));
                }
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        acks.iter(),
                        &error.change_context(RouteOutputError::ReleaseClock),
                    );
                return;
            }
        },
    };
    for output in &mut outputs.routes {
        let flush_due = match flush {
            ProcessorOutputFlush::All => !output.pending.is_empty(),
            ProcessorOutputFlush::Due => {
                let snapshot = snapshot
                    .as_ref()
                    .assured("due output flushing captures a clock snapshot above");
                match output.flush_due(&domain_clock, snapshot) {
                    Ok(due) => due,
                    Err(error) => {
                        let pending = output.take_pending();
                        let acks = pending
                            .iter()
                            .flat_map(|batch| batch.acks.iter().cloned())
                            .collect::<Vec<_>>();
                        context
                            .branch
                            .runtime
                            .handle_internal_processor_error_for_acks(
                                &context.branch.domain,
                                context.node_kind,
                                context.processor,
                                context.error_policies,
                                acks.iter(),
                                &error.change_context(RouteOutputError::InspectFlushDeadline {
                                    relay: output.relay.clone(),
                                }),
                            );
                        continue;
                    }
                }
            }
        };
        if !flush_due {
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
                        &error.change_context(RouteOutputError::Concatenate {
                            relay: output.relay.clone(),
                        }),
                    );
                continue;
            }
        };
        if context
            .branch
            .dispatch_output(output, context.source_kind, context.processor, &forwarded)
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
                    &Report::new(RouteOutputError::Forward {
                        relay: output.relay.clone(),
                    }),
                );
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::ParseAsType;

    use super::*;

    #[test]
    fn pending_output_batches_reject_rows_keys_and_metadata_that_disagree() {
        let batch = test_schema(&[("id", ParseAsType::U32)])
            .batch_from_test_rows([[("id".to_string(), RuntimeValue::U32(7))]])
            .expect("one test row should form a batch");
        let no_metadata = RecordMetadataColumns::from_rows([]);
        let Err(error) = pending_output_batches_by_key(0, &[0, 1], vec![None], batch, &no_metadata)
        else {
            panic!("rows, keys and metadata that disagree must not form pending batches");
        };
        assert_eq!(
            error.current_context().to_string(),
            "pending output has 2 input rows, 1 keys, 1 Arrow rows, and 0 metadata rows"
        );
    }
}
