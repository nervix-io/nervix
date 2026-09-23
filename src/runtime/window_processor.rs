//! Branch-local window processor execution.
//!
//! Layer: data plane.
//! - **Owns.** The rows a branch's window retains, admitting evaluated batches into the window's
//!   accumulators, due-window emission, stepping, and eviction.
//! - **Depends on.** Validated window plans, Arrow batches, the window accumulators, and bound
//!   domain time.
//! - **Must not know.** NSPL parsing, placement decisions or connector transports.

use error_stack::{Report, ResultExt as _};

use super::*;

/// Every way a window processor fails, from evaluating one batch's aggregate arguments to
/// publishing the branch-local window it owns.
#[derive(Debug, thiserror::Error)]
pub(super) enum WindowProcessorError {
    #[error("window aggregate requires a non-empty window")]
    EmptyWindow,
    #[error("window processor '{}' failed to snapshot branch state", .processor.as_str())]
    Snapshot { processor: ModelName },
    #[error(
        "window snapshot accumulator count {accumulators} does not match aggregate demand count \
         {demands}"
    )]
    SnapshotDemandCount { accumulators: usize, demands: usize },
    #[error("window snapshot does not carry the {storage:?} state of aggregate structure {demand}")]
    SnapshotAccumulator {
        demand: usize,
        storage: WindowAggregateStorageKind,
    },
    #[error("window snapshot row carries {values} argument values for {arguments} arguments")]
    SnapshotArgumentCount { values: usize, arguments: usize },
    #[error("window snapshot row sequence {found} does not follow sequence {previous}")]
    SnapshotSequence { previous: u64, found: u64 },
    #[error("window snapshot delays a removal from bucket {bucket} of {buckets} buckets")]
    SnapshotHistogramBucket { bucket: usize, buckets: usize },
    #[error("failed to encode a window entry for the branch snapshot")]
    EncodeSnapshotEntry,
    #[error("failed to restore a window entry from the branch snapshot")]
    RestoreSnapshotEntry,
    #[error("failed to restore a window entry branch key: {reason}")]
    RestoreSnapshotBranchKey { reason: String },
    #[error("failed to project the window aggregate input batch")]
    ProjectAggregateInput,
    #[error("window aggregate input VM execution failed")]
    AggregateInputExecution,
    #[error("window aggregate input VM produced {rows} rows for {expected} input rows")]
    AggregateInputRowCount { rows: usize, expected: usize },
    #[error("window aggregate input VM did not preserve all {expected} input rows")]
    AggregateInputRowsDropped { expected: usize },
    #[error("window aggregate input VM produced no '{field}' field")]
    AggregateInputFieldMissing { field: String },
    #[error("window aggregate input VM failed with {}: {reason}", .reason.code().as_str())]
    AggregateInputRow { reason: nervix_vm::SideErrorReason },
    #[error(
        "window aggregate arguments evaluated for {evaluated} structures, the window has {demands}"
    )]
    ArgumentDemandCount { evaluated: usize, demands: usize },
    #[error("window aggregate structure {demand} was evaluated with the wrong number of arguments")]
    ArgumentShape { demand: usize },
    #[error(
        "window aggregate argument '{field}' of structure {demand} evaluated to {found:?}, \
         expected {expected:?}"
    )]
    ArgumentColumnType {
        demand: usize,
        field: String,
        expected: ArrowDataType,
        found: ArrowDataType,
    },
    #[error("{} requires finite floating-point arguments", .function.nspl_name())]
    NonFiniteArgument { function: WindowAggregateFunction },
    #[error("SUM of the window does not fit {data_type:?}")]
    SumOverflow { data_type: ArrowDataType },
    #[error("{} of the window is not finite", .function.nspl_name())]
    StatisticNotFinite { function: WindowAggregateFunction },
    #[error("window aggregate did not initialize required output field '{field}'")]
    UninitializedOutputField { field: String },
    #[error("failed to build the window aggregate output batch")]
    BuildAggregateOutput,
    #[error("window aggregate VM compile input is invalid")]
    AggregateExprInput,
    #[error("window aggregate VM execution failed")]
    AggregateExprExecution,
    #[error("window aggregate VM produced no '{field}' output field")]
    AggregateExprFieldMissing { field: String },
    #[error("window aggregate VM produced null '{field}' output")]
    AggregateExprNullOutput { field: String },
}

/// One row the window retains, with the message it arrived in.
#[derive(Debug)]
pub(super) struct WindowEntry {
    pub(super) row: WindowRow,
    pub(super) message: RelayMessage,
}

/// A row of an evaluated batch that the window admits.
#[derive(Debug)]
pub(super) struct WindowAdmission {
    pub(super) message: RelayMessage,
    /// The row's index within the batch's evaluated argument columns.
    pub(super) row: usize,
}

#[derive(Debug)]
pub(super) struct WindowProcessorState {
    pub(super) entries: VecDeque<WindowEntry>,
    pub(super) next_sequence: u64,
    pub(super) accumulators: Vec<WindowAccumulator>,
}

impl RetainedWindowRows for VecDeque<WindowEntry> {
    fn retained(&self) -> usize {
        self.len()
    }

    fn retained_row(&self, position: usize) -> &WindowRow {
        &self
            .get(position)
            .verified("accumulators address only positions of rows the window retains")
            .row
    }
}

pub(super) fn message_timestamp(message: &RelayMessage) -> Timestamp {
    message.record.metadata().ingested_at_low_watermark()
}

pub(super) fn window_output_metadata(
    state: &WindowProcessorState,
    emit_high_watermark: Timestamp,
) -> error_stack::Result<RuntimeRecordMetadata, WindowProcessorError> {
    let low = state
        .entries
        .iter()
        .map(|entry| entry.row.timestamp)
        .min()
        .ok_or_else(|| Report::new(WindowProcessorError::EmptyWindow))?;
    Ok(RuntimeRecordMetadata::from_ingested_at_watermarks(
        low,
        emit_high_watermark,
    ))
}

pub(super) async fn flush_ready_window_processor(
    context: WindowFlushContext<'_>,
    state: &mut WindowProcessorState,
    plan: &WindowAccumulatorPlan,
    compiled_aggregates: &[CompiledWindowAggregateProgram],
    bounds: WindowBounds,
    now: Timestamp,
) -> bool {
    let WindowFlushContext {
        graph,
        node_kind,
        processor,
        error_policies,
        branch,
        output_routes,
        materialized_state,
        execution_now,
    } = context;
    if output_routes.routes.is_empty() {
        state.clear(plan);
        return true;
    }
    if output_routes.routes.len() != compiled_aggregates.len() {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            state.entries.iter().map(|entry| &entry.message.acks),
            format!(
                "window processor '{}' has {} output routes but {} compiled aggregate programs",
                processor.as_str(),
                output_routes.routes.len(),
                compiled_aggregates.len()
            ),
        );
        state.clear(plan);
        return true;
    }
    let mut changed = state.purge_timeouts(now);
    while window_width_met(state, bounds.width_messages, bounds.width_duration, now) {
        let Some(first_entry) = state.entries.front() else {
            break;
        };
        let output_metadata = match window_output_metadata(state, execution_now) {
            Ok(metadata) => metadata,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    state.entries.iter().map(|entry| &entry.message.acks),
                    format!(
                        "window processor '{}' cannot emit aggregate: {error:#}",
                        processor.as_str(),
                    ),
                );
                state.clear(plan);
                changed = true;
                break;
            }
        };
        let mut route_failed = false;
        for (output_index, compiled_aggregate) in compiled_aggregates.iter().enumerate() {
            let output_relay = output_routes.routes[output_index].relay.clone();
            let output_schema = match branch.relay_schema(&output_relay) {
                Ok(schema) => schema,
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        state.entries.iter().map(|entry| &entry.message.acks),
                        error.to_string(),
                    );
                    route_failed = true;
                    break;
                }
            };
            let output_batch = match evaluate_window_aggregate(
                compiled_aggregate,
                state,
                &output_schema,
                execution_now,
            )
            .await
            {
                Ok(record) => record,
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        state.entries.iter().map(|entry| &entry.message.acks),
                        format!(
                            "window processor '{}' output route '{}' aggregate failed: {error:#}",
                            processor.as_str(),
                            output_relay.as_str(),
                        ),
                    );
                    route_failed = true;
                    break;
                }
            };
            let output_message = RelayMessage {
                key: first_entry.message.key.clone(),
                record: match RuntimeRow::new(Arc::new(output_batch), 0, output_metadata.clone()) {
                    Ok(record) => record,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            node_kind,
                            processor,
                            error_policies,
                            state.entries.iter().map(|entry| &entry.message.acks),
                            format!(
                                "window processor '{}' failed to construct output route '{}' row: \
                                 {}",
                                processor.as_str(),
                                output_relay.as_str(),
                                error
                            ),
                        );
                        route_failed = true;
                        break;
                    }
                },
                acks: AckSet::merged(
                    state
                        .entries
                        .iter()
                        .map(|entry| entry.message.acks.attached()),
                ),
            };
            let forwarded =
                match RelayRecordBatch::from_messages(output_schema, vec![output_message]) {
                    Ok(batch) => batch,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            node_kind,
                            processor,
                            error_policies,
                            state.entries.iter().map(|entry| &entry.message.acks),
                            format!(
                                "window processor '{}' failed to build output route '{}' batch: {}",
                                processor.as_str(),
                                output_relay.as_str(),
                                error
                            ),
                        );
                        route_failed = true;
                        break;
                    }
                };
            if let Some(acks) = dispatch_processor_output(
                ProcessorOutputDispatchContext {
                    graph,
                    branch,
                    node_kind,
                    source_kind: ModelKind::WindowProcessor,
                    processor,
                    error_policies,
                    input_relays: std::slice::from_ref(&output_relay),
                    filter_source: ProcessorOutputFilterSource::OutputRelay,
                    materialized_state: ProcessorMaterializedState::ResolvedAtDispatch(
                        materialized_state,
                    ),
                    execution_now,
                },
                output_routes,
                forwarded,
                output_index,
            )
            .await
            {
                for ack in acks {
                    ack.ack_success();
                }
            }
        }
        if route_failed {
            state.clear(plan);
            changed = true;
            break;
        }
        advance_window(state, bounds.step_messages, bounds.step_duration, now);
        changed = true;
        if state.entries.is_empty() {
            break;
        }
    }
    changed
}

pub(super) fn snapshot_window_processor_live_state(
    processor: &ModelName,
    replicated_state: &ReplicatedWindowProcessorState,
    state: &WindowProcessorState,
) -> error_stack::Result<(), WindowProcessorError> {
    replicated_state
        .replace_state(state)
        .change_context_lazy(|| WindowProcessorError::Snapshot {
            processor: processor.clone(),
        })
}

impl WindowProcessorState {
    pub(super) fn new(plan: &WindowAccumulatorPlan) -> Self {
        Self {
            entries: VecDeque::new(),
            next_sequence: 0,
            accumulators: plan.empty_accumulators(),
        }
    }

    pub(super) fn to_snapshot(
        &self,
    ) -> error_stack::Result<WindowProcessorStateSnapshot, WindowProcessorError> {
        let mut entries = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            let record = entry
                .message
                .record
                .to_remote()
                .change_context(WindowProcessorError::EncodeSnapshotEntry)?;
            let arguments = entry.row.arguments.published_values(entry.row.row)?;
            entries.push(WindowEntrySnapshot {
                sequence: entry.row.sequence,
                timestamp: entry.row.timestamp,
                key: BranchKey::to_remote_key(&entry.message.key),
                record,
                arguments,
            });
        }
        Ok(WindowProcessorStateSnapshot {
            entries,
            next_sequence: self.next_sequence,
            accumulators: self
                .accumulators
                .iter()
                .map(WindowAccumulator::to_snapshot)
                .collect(),
        })
    }

    /// Rebuild a branch's live window from a published snapshot, which stays shared with every
    /// other reader and is only read here.
    pub(super) fn from_snapshot(
        plan: &WindowAccumulatorPlan,
        input_schema: &CompiledSchema,
        snapshot: &WindowProcessorStateSnapshot,
    ) -> error_stack::Result<Self, WindowProcessorError> {
        if snapshot.accumulators.len() != plan.demands().len() {
            return Err(Report::new(WindowProcessorError::SnapshotDemandCount {
                accumulators: snapshot.accumulators.len(),
                demands: plan.demands().len(),
            }));
        }
        let published_arguments = snapshot
            .entries
            .iter()
            .map(|entry| entry.arguments.as_slice())
            .collect::<Vec<_>>();
        let arguments = Arc::new(WindowArgumentColumns::restored(plan, &published_arguments)?);
        let mut entries = VecDeque::with_capacity(snapshot.entries.len());
        let mut previous_sequence: Option<u64> = None;
        for (row, entry) in snapshot.entries.iter().enumerate() {
            if let Some(previous) = previous_sequence
                && previous.checked_add(1) != Some(entry.sequence)
            {
                return Err(Report::new(WindowProcessorError::SnapshotSequence {
                    previous,
                    found: entry.sequence,
                }));
            }
            previous_sequence = Some(entry.sequence);
            let key = BranchKey::from_remote_key(entry.key.clone()).map_err(|reason| {
                Report::new(WindowProcessorError::RestoreSnapshotBranchKey { reason })
            })?;
            let record = input_schema
                .runtime_row_from_remote(&entry.record)
                .change_context(WindowProcessorError::RestoreSnapshotEntry)?;
            entries.push_back(WindowEntry {
                row: WindowRow {
                    sequence: entry.sequence,
                    timestamp: entry.timestamp,
                    arguments: arguments.clone(),
                    row,
                },
                message: RelayMessage {
                    key,
                    record,
                    acks: AckSet::empty(),
                },
            });
        }
        if let Some(last) = previous_sequence
            && last >= snapshot.next_sequence
        {
            return Err(Report::new(WindowProcessorError::SnapshotSequence {
                previous: last,
                found: snapshot.next_sequence,
            }));
        }
        let mut accumulators = Vec::with_capacity(plan.demands().len());
        for (demand, (compiled, published)) in plan
            .demands()
            .iter()
            .zip(&snapshot.accumulators)
            .enumerate()
        {
            accumulators.push(WindowAccumulator::restore(
                compiled, demand, &entries, published,
            )?);
        }
        Ok(Self {
            entries,
            next_sequence: snapshot.next_sequence,
            accumulators,
        })
    }

    /// Admit `run`, consecutive rows of one evaluated batch in arrival order, into the window and
    /// every accumulator.
    pub(super) fn admit(
        &mut self,
        arguments: &Arc<WindowArgumentColumns>,
        run: Vec<WindowAdmission>,
    ) {
        let start = self.entries.len();
        let mut latest: Option<Timestamp> = None;
        for admission in run {
            let timestamp = message_timestamp(&admission.message);
            latest = match latest {
                Some(latest) => Some(latest.max(timestamp)),
                None => Some(timestamp),
            };
            self.entries.push_back(WindowEntry {
                row: WindowRow {
                    sequence: self.next_sequence,
                    timestamp,
                    arguments: arguments.clone(),
                    row: admission.row,
                },
                message: admission.message,
            });
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .assured("a window cannot admit 2^64 rows in one branch");
        }
        let Some(admitted_at) = latest else {
            return;
        };
        let end = self.entries.len();
        for (demand, accumulator) in self.accumulators.iter_mut().enumerate() {
            accumulator.admit(demand, &self.entries, start..end, admitted_at);
        }
    }

    /// How many of `pending`, admitted in order, fill the window: the rows up to and including the
    /// first one whose admission meets the width, or every pending row when none does.
    pub(super) fn admission_run_len(
        &self,
        pending: &VecDeque<WindowAdmission>,
        width_messages: Option<usize>,
        width_duration: Option<Duration>,
    ) -> usize {
        let mut window_start = self.entries.front().map(|entry| entry.row.timestamp);
        let mut rows = self.entries.len();
        for (index, admission) in pending.iter().enumerate() {
            let timestamp = message_timestamp(&admission.message);
            let start = *window_start.get_or_insert(timestamp);
            rows = rows
                .checked_add(1)
                .assured("pending rows are already held in memory");
            let messages_met = width_messages.is_some_and(|width| rows >= width);
            let duration_met =
                width_duration.is_some_and(|width| timestamp_elapsed(start, timestamp) >= width);
            if messages_met || duration_met {
                return index
                    .checked_add(1)
                    .assured("pending rows are already held in memory");
            }
        }
        pending.len()
    }

    pub(super) fn clear(&mut self, plan: &WindowAccumulatorPlan) {
        self.entries.clear();
        self.accumulators = plan.empty_accumulators();
    }

    pub(super) fn purge_timeouts(&mut self, now: Timestamp) -> bool {
        let mut changed = false;
        for accumulator in &mut self.accumulators {
            changed |= accumulator.purge_expired(now);
        }
        changed
    }

    pub(super) fn next_timeout_deadline(&self) -> Option<Timestamp> {
        self.accumulators
            .iter()
            .filter_map(WindowAccumulator::next_deadline)
            .min()
    }

    /// Remove the `count` oldest rows from every accumulator and from the window, answering the
    /// removed entries oldest first. `removed_at` is the watermark the window stepped at.
    pub(super) fn retract_oldest(
        &mut self,
        count: usize,
        removed_at: Timestamp,
    ) -> Vec<WindowEntry> {
        let count = count.min(self.entries.len());
        for (demand, accumulator) in self.accumulators.iter_mut().enumerate() {
            accumulator.retract_oldest(demand, &self.entries, count, removed_at);
        }
        self.entries.drain(..count).collect()
    }
}

/// The aggregate arguments one input batch evaluated to, and why rows that cannot be admitted are
/// refused.
#[derive(Debug)]
pub(super) struct EvaluatedWindowArguments {
    pub(super) columns: Arc<WindowArgumentColumns>,
    /// One entry per row once any row is refused; empty while every row is admissible.
    refusals: Vec<Option<Report<WindowProcessorError>>>,
}

impl EvaluatedWindowArguments {
    fn refuse(&mut self, rows: usize, row: usize, refusal: Report<WindowProcessorError>) {
        if self.refusals.is_empty() {
            self.refusals.resize_with(rows, || None);
        }
        let slot = self
            .refusals
            .get_mut(row)
            .verified("refusals hold one slot for every row of the evaluated batch");
        if slot.is_none() {
            *slot = Some(refusal);
        }
    }

    /// Why the row at `row` cannot be admitted, if it cannot.
    pub(super) fn take_refusal(&mut self, row: usize) -> Option<Report<WindowProcessorError>> {
        self.refusals.get_mut(row)?.take()
    }
}

// Counted per thread so a test observes only the argument programs it ran itself, while the rest
// of the suite evaluates windows in parallel.
#[cfg(test)]
thread_local! {
    pub(super) static WINDOW_ARGUMENT_VM_EXECUTIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// Evaluate every aggregate argument of the window's routes over `carrier` once, and find the rows
/// whose arguments cannot be admitted.
pub(super) async fn evaluate_window_arguments(
    plan: &WindowAccumulatorPlan,
    programs: &[CompiledWindowAggregateProgram],
    carrier: &RuntimeRecordBatch,
    execution_now: Timestamp,
) -> error_stack::Result<EvaluatedWindowArguments, WindowProcessorError> {
    let row_count = carrier.batch().num_rows();
    let mut arrays = Vec::with_capacity(plan.demands().len());
    let mut row_failures: Vec<Option<Report<WindowProcessorError>>> = Vec::new();
    for program in programs {
        tokio::task::consume_budget().await;
        let result = evaluate_route_arguments(program, carrier, execution_now).await?;
        for demand in &program.route.demands {
            let columns = demand
                .arguments
                .try_convert(|column| route_argument_array(&result, column))?;
            arrays.push(columns);
        }
        if result.batch.errors().is_error_free() {
            continue;
        }
        if row_failures.is_empty() {
            row_failures.resize_with(row_count, || None);
        }
        for (row, failure) in row_failures.iter_mut().enumerate() {
            if failure.is_some() {
                continue;
            }
            if let Some(error) = result.batch.errors().row(row).first() {
                *failure = Some(Report::new(WindowProcessorError::AggregateInputRow {
                    reason: error.reason.clone(),
                }));
            }
        }
    }
    let columns = WindowArgumentColumns::new(plan, arrays, row_count)?;
    let mut evaluated = EvaluatedWindowArguments {
        columns: Arc::new(columns),
        refusals: Vec::new(),
    };
    for (row, failure) in row_failures.into_iter().enumerate() {
        if let Some(failure) = failure {
            evaluated.refuse(row_count, row, failure);
        }
    }
    for row in 0..row_count {
        if let Some(function) = evaluated.columns.refused_function(plan, row) {
            evaluated.refuse(
                row_count,
                row,
                Report::new(WindowProcessorError::NonFiniteArgument { function }),
            );
        }
    }
    Ok(evaluated)
}

/// The evaluated array of one argument column in a route's argument program result.
fn route_argument_array(
    result: &nervix_vm::ExecutionResult,
    column: &WindowArgumentColumn,
) -> error_stack::Result<ArrayRef, WindowProcessorError> {
    let index = result.batch.schema().index_of(&column.field).map_err(|_| {
        Report::new(WindowProcessorError::AggregateInputFieldMissing {
            field: column.field.clone(),
        })
    })?;
    Ok(result.batch.column(index).to_array_ref())
}

/// Whether `field` is a column the argument program writes, which enters its input uninitialized.
fn is_argument_output_field(field: &str) -> bool {
    match field.strip_prefix(WINDOW_ARGUMENT_NAMESPACE) {
        Some(rest) => rest.starts_with('.'),
        None => false,
    }
}

/// Run one route's argument program over every row of `carrier`.
async fn evaluate_route_arguments(
    program: &CompiledWindowAggregateProgram,
    carrier: &RuntimeRecordBatch,
    execution_now: Timestamp,
) -> error_stack::Result<nervix_vm::ExecutionResult, WindowProcessorError> {
    let argument_program = &program.route.argument_program;
    let row_count = carrier.batch().num_rows();
    let keys = vec![None; row_count];
    let side_inputs = HashMap::new();
    let lookup_columns = HashMap::new();
    let uninitialized = VmUninitializedInput {
        fields: argument_program
            .input_schema
            .fields()
            .iter()
            .filter(|field| is_argument_output_field(field.name()))
            .map(|field| field.name().clone())
            .collect(),
    };
    let input = project_vm_input_batch(
        &argument_program.input_schema,
        &VmInputProjectionSources {
            carrier,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized),
        },
        None,
    )
    .change_context(WindowProcessorError::ProjectAggregateInput)?;
    #[cfg(test)]
    WINDOW_ARGUMENT_VM_EXECUTIONS.with(|executions| executions.set(executions.get() + 1));
    let result = execute_program_with_selection_in_context(
        argument_program,
        &input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .change_context(WindowProcessorError::AggregateInputExecution)?;
    if result.batch.row_count() != row_count {
        return Err(Report::new(WindowProcessorError::AggregateInputRowCount {
            rows: result.batch.row_count(),
            expected: row_count,
        }));
    }
    if result.selected_rows.len() != row_count || !result.selected_rows.iter().eq(0..row_count) {
        return Err(Report::new(
            WindowProcessorError::AggregateInputRowsDropped {
                expected: row_count,
            },
        ));
    }
    Ok(result)
}

pub(super) fn window_width_met(
    state: &WindowProcessorState,
    width_messages: Option<usize>,
    width_duration: Option<Duration>,
    now: Timestamp,
) -> bool {
    if state.entries.is_empty() {
        return false;
    }
    if let Some(width_messages) = width_messages
        && state.entries.len() >= width_messages
    {
        return true;
    }
    if let Some(width_duration) = width_duration
        && let Some(first) = state.entries.front()
        && timestamp_elapsed(first.row.timestamp, now) >= width_duration
    {
        return true;
    }
    false
}

pub(super) fn window_next_deadline(
    state: &WindowProcessorState,
    width_duration: Option<Duration>,
) -> Option<Timestamp> {
    let width_deadline = if let Some(width_duration) = width_duration
        && let Some(first) = state.entries.front()
    {
        Some(checked_add_duration_to_timestamp(
            first.row.timestamp,
            width_duration,
        ))
    } else {
        None
    };
    match (width_deadline, state.next_timeout_deadline()) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

pub(super) fn timestamp_elapsed(start: Timestamp, end: Timestamp) -> Duration {
    end.into_datetime()
        .signed_duration_since(start.into_datetime())
        .to_std()
        .unwrap_or(Duration::ZERO)
}

/// Step the window: first by `step_messages` rows, then past every row older than `step_duration`
/// after the new first row. Stepped rows are acknowledged.
pub(super) fn advance_window(
    state: &mut WindowProcessorState,
    step_messages: Option<usize>,
    step_duration: Option<Duration>,
    removed_at: Timestamp,
) {
    let by_messages = step_messages.unwrap_or(0);
    for entry in state.retract_oldest(by_messages, removed_at) {
        entry.message.acks.ack_success();
    }
    let Some(step_duration) = step_duration else {
        return;
    };
    let Some(first) = state.entries.front() else {
        return;
    };
    let cutoff = checked_add_duration_to_timestamp(first.row.timestamp, step_duration);
    let by_duration = state
        .entries
        .iter()
        .take_while(|entry| entry.row.timestamp < cutoff)
        .count();
    for entry in state.retract_oldest(by_duration, removed_at) {
        entry.message.acks.ack_success();
    }
}

/// The results of one emission's aggregate invocations, which the route's output programs read.
#[derive(Debug)]
pub(super) struct WindowAggregateResults {
    results: BTreeMap<WindowAggregateInvocation, ArrayRef>,
}

impl VmFunctionInjector for WindowAggregateResults {
    /// Answers an aggregate for the selected output rows. The results hold one value per output
    /// row of the emission, so a conditional arm that selects some rows reads those rows' values
    /// by their identity in the output batch.
    fn inject_with_context(
        &self,
        function: &FunctionName,
        _arguments: &[VmTypedArray],
        rows: &nervix_vm::RowSelection,
        _span: nervix_vm::program::Span,
        _now: Timestamp,
        _prior_error_rows: nervix_vm::RowErrorMask<'_>,
    ) -> Result<nervix_vm::InjectedResult, nervix_vm::RuntimeError> {
        let FunctionName::WindowAggregate(invocation) = function else {
            return Err(nervix_vm::RuntimeError::InvalidBatch {
                message: format!("function '{}' is not a window aggregate", function.as_str()),
            });
        };
        let Some(result) = self.results.get(invocation) else {
            return Err(nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "window aggregate {} of structure {} was not evaluated for this emission",
                    invocation.function.nspl_name(),
                    invocation.demand_id
                ),
            });
        };
        if !rows.fits(result.len()) {
            return Err(nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "window aggregate {} evaluated {} rows for output rows selected as {rows:?}",
                    invocation.function.nspl_name(),
                    result.len()
                ),
            });
        }
        let output = match rows {
            nervix_vm::RowSelection::All(_) => result.clone(),
            nervix_vm::RowSelection::Selected(selected) => {
                let indices = UInt64Array::from_iter_values(
                    selected.iter().map(|row| -> u64 { (*row).arch_into() }),
                );
                take_arrow_array(result.as_ref(), &indices, None).map_err(|error| {
                    nervix_vm::RuntimeError::InvalidBatch {
                        message: format!(
                            "window aggregate {} could not be read for the selected output rows: \
                             {error}",
                            invocation.function.nspl_name()
                        ),
                    }
                })?
            }
        };
        let output = VmTypedArray::try_from_array_ref(output)?;
        Ok(nervix_vm::InjectedResult::success(output))
    }
}

/// Build the one-row output batch of one route from the window's accumulators.
pub(super) async fn evaluate_window_aggregate(
    program: &CompiledWindowAggregateProgram,
    state: &WindowProcessorState,
    output_schema: &CompiledSchema,
    execution_now: Timestamp,
) -> error_stack::Result<RuntimeRecordBatch, WindowProcessorError> {
    let mut results = BTreeMap::new();
    for compiled in &program.route.invocations {
        let demand = program
            .demand_offset
            .checked_add(compiled.invocation.demand_id)
            .assured("both index into the accumulators this program already holds in memory");
        let accumulator = state
            .accumulators
            .get(demand)
            .verified("a route's demands follow the demands of every route written before it");
        let result = accumulator.evaluate(
            demand,
            &compiled.invocation,
            &compiled.output_type,
            &state.entries,
        )?;
        results.insert(compiled.invocation.clone(), result);
    }
    let injector: Arc<Box<dyn VmFunctionInjector>> =
        Arc::new(Box::new(WindowAggregateResults { results }));
    let schema = output_schema.arrow_schema();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (field, assignment) in schema.fields().iter().zip(&program.field_assignments) {
        let column = match assignment {
            Some(index) => {
                let assignment = program
                    .route
                    .assignments
                    .get(*index)
                    .verified("field assignments index the route's own assignments");
                let column = evaluate_window_value(
                    &assignment.value,
                    &assignment.field,
                    field.data_type(),
                    injector.clone(),
                    execution_now,
                )
                .await?;
                if column.is_null(0) && !field.is_nullable() {
                    return Err(Report::new(WindowProcessorError::AggregateExprNullOutput {
                        field: field.name().clone(),
                    }));
                }
                column
            }
            None if field.is_nullable() => new_null_array(field.data_type(), 1),
            None => {
                return Err(Report::new(
                    WindowProcessorError::UninitializedOutputField {
                        field: field.name().clone(),
                    },
                ));
            }
        };
        columns.push(column);
    }
    let batch = RecordBatch::try_new(StdArc::clone(&schema), columns)
        .change_context(WindowProcessorError::BuildAggregateOutput)?;
    RuntimeRecordBatch::from_record_batch(schema, batch)
        .change_context(WindowProcessorError::BuildAggregateOutput)
}

/// Evaluate one assigned value as a one-row array of `data_type`.
fn evaluate_window_value<'a>(
    value: &'a CompiledWindowExpr,
    target_field: &'a str,
    data_type: &'a ArrowDataType,
    injector: Arc<Box<dyn VmFunctionInjector>>,
    execution_now: Timestamp,
) -> BoxFuture<'a, error_stack::Result<ArrayRef, WindowProcessorError>> {
    Box::pin(async move {
        match value {
            CompiledWindowExpr::Scalar(program) => {
                let input = VmTypedBatch::try_new(
                    program.input_schema.clone(),
                    program
                        .input_schema
                        .fields()
                        .iter()
                        .map(|field| VmTypedArray::uninitialized(field.data_type().clone(), 1))
                        .collect(),
                )
                .change_context(WindowProcessorError::AggregateExprInput)?;
                let result = execute_program_with_selection_in_context(
                    program,
                    &input,
                    &VmExecutionContext {
                        now: execution_now,
                        injector: Some(injector),
                    },
                )
                .await
                .change_context(WindowProcessorError::AggregateExprExecution)?;
                let column_index = result.batch.schema().index_of(target_field).map_err(|_| {
                    Report::new(WindowProcessorError::AggregateExprFieldMissing {
                        field: target_field.to_string(),
                    })
                })?;
                Ok(result.batch.column(column_index).to_array_ref())
            }
            CompiledWindowExpr::Array { items, fixed_size } => {
                let element = match data_type {
                    ArrowDataType::FixedSizeList(element, _) | ArrowDataType::List(element) => {
                        Some(element)
                    }
                    _ => None,
                };
                let element = element.verified(
                    "an array value compiles only against a fixed-size or variable list field",
                );
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(
                        evaluate_window_value(
                            item,
                            target_field,
                            element.data_type(),
                            injector.clone(),
                            execution_now,
                        )
                        .await?,
                    );
                }
                let value_refs = values
                    .iter()
                    .map(AsRef::as_ref)
                    .collect::<Vec<&dyn Array>>();
                let values = concat_arrow_arrays(&value_refs)
                    .change_context(WindowProcessorError::BuildAggregateOutput)?;
                let array: ArrayRef = if *fixed_size {
                    let length = i32::try_from(items.len())
                        .change_context(WindowProcessorError::BuildAggregateOutput)?;
                    StdArc::new(
                        arrow_array::FixedSizeListArray::try_new(
                            StdArc::clone(element),
                            length,
                            values,
                            None,
                        )
                        .change_context(WindowProcessorError::BuildAggregateOutput)?,
                    )
                } else {
                    StdArc::new(
                        ListArray::try_new(
                            StdArc::clone(element),
                            arrow_buffer::OffsetBuffer::from_lengths([items.len()]),
                            values,
                            None,
                        )
                        .change_context(WindowProcessorError::BuildAggregateOutput)?,
                    )
                };
                Ok(array)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float64Array, Int64Array, TimestampNanosecondArray};
    use nervix_models::{ParseAsType, Timestamp};
    use nonzero_ext::nonzero;
    use ordered_float::OrderedFloat;

    use super::*;
    use crate::{
        runtime_ack::AckSet,
        runtime_schema::{RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeValue},
    };

    fn field(name: &'static str, ty: ParseAsType) -> OptionalTestField {
        OptionalTestField {
            name,
            ty,
            optional: false,
        }
    }

    fn optional(name: &'static str, ty: ParseAsType) -> OptionalTestField {
        OptionalTestField {
            name,
            ty,
            optional: true,
        }
    }

    fn at(nanos: i64) -> Timestamp {
        Timestamp::from_unix_nanos(nanos)
    }

    /// One compiled single-route window processor state, driven the way its branch task drives it.
    struct TestWindow {
        plan: WindowAccumulatorPlan,
        compiled: CompiledWindowAggregateProgram,
        input_schema: Arc<CompiledSchema>,
        output_schema: Arc<CompiledSchema>,
        state: WindowProcessorState,
    }

    impl TestWindow {
        fn new(set: &str, input: &[OptionalTestField], output: &[OptionalTestField]) -> Self {
            let input_schema = test_optional_schema(input);
            let output_schema = test_optional_schema(output);
            let input_relay = named::<RelayName>("events");
            let output_relay = named::<RelayName>("summary");
            let relay_schemas = HashMap::from_iter([
                (input_relay.clone(), input_schema.clone()),
                (output_relay.clone(), output_schema.clone()),
            ]);
            let compiled = CompiledWindowAggregateProgram::compile(
                &window_aggregate(set),
                &[input_relay],
                &output_relay,
                &relay_schemas,
                None,
            )
            .expect("the test window route should compile");
            let plan = WindowAccumulatorPlan::new([&compiled.route]);
            let state = WindowProcessorState::new(&plan);
            Self {
                plan,
                compiled,
                input_schema,
                output_schema,
                state,
            }
        }

        /// Evaluate one batch of `columns`, in input schema order, whose rows carry `timestamps`
        /// as watermarks, admit every admissible row, and answer why the others were refused.
        async fn admit(&mut self, columns: Vec<ArrayRef>, timestamps: &[i64]) -> Vec<String> {
            let schema = self.input_schema.arrow_schema();
            let batch = RecordBatch::try_new(StdArc::clone(&schema), columns)
                .expect("the test columns should match the input schema");
            let carrier = Arc::new(
                RuntimeRecordBatch::from_record_batch(schema, batch)
                    .expect("the test batch should be a valid relay batch"),
            );
            let mut evaluated = evaluate_window_arguments(
                &self.plan,
                std::slice::from_ref(&self.compiled),
                &carrier,
                at(1),
            )
            .await
            .expect("the test batch's arguments should evaluate");
            let mut refused = Vec::new();
            let mut run = Vec::new();
            for (row, timestamp) in timestamps.iter().enumerate() {
                if let Some(refusal) = evaluated.take_refusal(row) {
                    refused.push(refusal.current_context().to_string());
                    continue;
                }
                let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(
                    at(*timestamp),
                    at(*timestamp),
                );
                let record = RuntimeRow::new(carrier.clone(), row, metadata)
                    .expect("every test row is inside its batch");
                let message = RelayMessage {
                    key: None,
                    record,
                    acks: AckSet::empty(),
                };
                run.push(WindowAdmission { message, row });
            }
            self.state.admit(&evaluated.columns, run);
            refused
        }

        fn step(&mut self, count: usize, removed_at: i64) {
            let removed = self.state.retract_oldest(count, at(removed_at));
            assert_eq!(removed.len(), count, "a test steps only over retained rows");
        }

        async fn emit(&self) -> error_stack::Result<RuntimeRecordBatch, WindowProcessorError> {
            evaluate_window_aggregate(&self.compiled, &self.state, &self.output_schema, at(42))
                .await
        }

        async fn emitted(&self) -> RuntimeRecordBatch {
            self.emit()
                .await
                .expect("the window aggregate should evaluate")
        }
    }

    fn int64(values: impl IntoIterator<Item = Option<i64>>) -> ArrayRef {
        StdArc::new(Int64Array::from_iter(values))
    }

    fn float64(values: impl IntoIterator<Item = Option<f64>>) -> ArrayRef {
        StdArc::new(Float64Array::from_iter(values))
    }

    fn booleans(values: impl IntoIterator<Item = Option<bool>>) -> ArrayRef {
        StdArc::new(BooleanArray::from_iter(values))
    }

    fn strings(values: impl IntoIterator<Item = Option<&'static str>>) -> ArrayRef {
        StdArc::new(StringArray::from_iter(values))
    }

    fn f64_value(value: f64) -> Option<RuntimeValue> {
        Some(RuntimeValue::F64(OrderedFloat(value)))
    }

    #[tokio::test]
    async fn statistics_follow_sliding_admission_and_retraction() {
        let mut window = TestWindow::new(
            "SET healthy_samples = COUNT_IF(input.healthy), all_healthy = \
             BOOL_AND(input.healthy), any_healthy = BOOL_OR(input.healthy), mean_value = \
             AVG(input.value), value_var_samp = VAR_SAMP(input.value), value_var_pop = \
             VAR_POP(input.value), value_stddev_samp = STDDEV_SAMP(input.value), load_covar_pop = \
             COVAR_POP(input.value, input.load), load_covar_samp = COVAR_SAMP(input.value, \
             input.load), load_corr = CORR(input.value, input.load), lowest_sensor = \
             ARG_MIN(input.sensor, input.value), highest_sensor = ARG_MAX(input.sensor, \
             input.value)",
            &[
                field("sensor", ParseAsType::String),
                field("value", ParseAsType::I64),
                field("load", ParseAsType::F64),
                field("healthy", ParseAsType::Bool),
            ],
            &[
                field("healthy_samples", ParseAsType::I64),
                field("all_healthy", ParseAsType::Bool),
                field("any_healthy", ParseAsType::Bool),
                field("mean_value", ParseAsType::F64),
                optional("value_var_samp", ParseAsType::F64),
                field("value_var_pop", ParseAsType::F64),
                optional("value_stddev_samp", ParseAsType::F64),
                field("load_covar_pop", ParseAsType::F64),
                optional("load_covar_samp", ParseAsType::F64),
                optional("load_corr", ParseAsType::F64),
                field("lowest_sensor", ParseAsType::String),
                field("highest_sensor", ParseAsType::String),
            ],
        );
        let rows = [
            ("north", 2, 10.0, true),
            ("south", 8, 40.0, true),
            ("east", 5, 25.0, false),
            ("west", 8, 40.0, true),
            ("center", 5, 25.0, true),
            ("top", 2, 10.0, true),
        ];
        struct Expected {
            healthy_samples: i64,
            all_healthy: bool,
            mean: f64,
            var_samp: f64,
            var_pop: f64,
            covar_pop: f64,
            covar_samp: f64,
            lowest: &'static str,
            highest: &'static str,
        }
        let windows = [
            Expected {
                healthy_samples: 2,
                all_healthy: false,
                mean: 5.0,
                var_samp: 9.0,
                var_pop: 6.0,
                covar_pop: 30.0,
                covar_samp: 45.0,
                lowest: "north",
                highest: "south",
            },
            Expected {
                healthy_samples: 2,
                all_healthy: false,
                mean: 7.0,
                var_samp: 3.0,
                var_pop: 2.0,
                covar_pop: 10.0,
                covar_samp: 15.0,
                lowest: "east",
                highest: "south",
            },
            Expected {
                healthy_samples: 2,
                all_healthy: false,
                mean: 6.0,
                var_samp: 3.0,
                var_pop: 2.0,
                covar_pop: 10.0,
                covar_samp: 15.0,
                lowest: "east",
                highest: "west",
            },
            Expected {
                healthy_samples: 3,
                all_healthy: true,
                mean: 5.0,
                var_samp: 9.0,
                var_pop: 6.0,
                covar_pop: 30.0,
                covar_samp: 45.0,
                lowest: "top",
                highest: "west",
            },
        ];
        let mut emitted = 0;
        for (index, (sensor, value, load, healthy)) in rows.into_iter().enumerate() {
            let timestamp = i64::try_from(index).expect("few test rows");
            let refused = window
                .admit(
                    vec![
                        strings([Some(sensor)]),
                        int64([Some(value)]),
                        float64([Some(load)]),
                        booleans([Some(healthy)]),
                    ],
                    &[timestamp],
                )
                .await;
            assert!(refused.is_empty());
            if window.state.entries.len() < 3 {
                continue;
            }
            let expected = &windows[emitted];
            let record = window.emitted().await;
            assert_eq!(
                batch_value(&record, "healthy_samples"),
                Some(RuntimeValue::I64(expected.healthy_samples))
            );
            assert_eq!(
                batch_value(&record, "all_healthy"),
                Some(RuntimeValue::Bool(expected.all_healthy))
            );
            assert_eq!(
                batch_value(&record, "any_healthy"),
                Some(RuntimeValue::Bool(true))
            );
            assert_eq!(batch_value(&record, "mean_value"), f64_value(expected.mean));
            assert_eq!(
                batch_value(&record, "value_var_samp"),
                f64_value(expected.var_samp)
            );
            assert_eq!(
                batch_value(&record, "value_var_pop"),
                f64_value(expected.var_pop)
            );
            assert_eq!(
                batch_value(&record, "value_stddev_samp"),
                f64_value(expected.var_samp.sqrt())
            );
            assert_eq!(
                batch_value(&record, "load_covar_pop"),
                f64_value(expected.covar_pop)
            );
            assert_eq!(
                batch_value(&record, "load_covar_samp"),
                f64_value(expected.covar_samp)
            );
            assert_eq!(batch_value(&record, "load_corr"), f64_value(1.0));
            assert_eq!(
                batch_value(&record, "lowest_sensor"),
                Some(RuntimeValue::String(expected.lowest.to_string()))
            );
            assert_eq!(
                batch_value(&record, "highest_sensor"),
                Some(RuntimeValue::String(expected.highest.to_string()))
            );
            emitted += 1;
            window.step(1, timestamp);
        }
        assert_eq!(emitted, windows.len());
    }

    #[tokio::test]
    async fn null_arguments_contribute_nothing_and_undefined_results_are_typed_nulls() {
        let set = "SET samples = COUNT(input.value), total = SUM(input.value), mean_value = \
                   AVG(input.value), lowest = MIN(input.value), first_value = FIRST(input.value), \
                   value_var_samp = VAR_SAMP(input.value), healthy_samples = \
                   COUNT_IF(input.healthy), all_healthy = BOOL_AND(input.healthy), lowest_label = \
                   ARG_MIN(input.label, input.value), correlation = CORR(input.value, input.value)";
        let input = [
            optional("value", ParseAsType::I64),
            optional("healthy", ParseAsType::Bool),
            optional("label", ParseAsType::String),
        ];
        let output = [
            field("samples", ParseAsType::I64),
            optional("total", ParseAsType::I64),
            optional("mean_value", ParseAsType::F64),
            optional("lowest", ParseAsType::I64),
            optional("first_value", ParseAsType::I64),
            optional("value_var_samp", ParseAsType::F64),
            field("healthy_samples", ParseAsType::I64),
            optional("all_healthy", ParseAsType::Bool),
            optional("lowest_label", ParseAsType::String),
            optional("correlation", ParseAsType::F64),
        ];

        let mut sparse = TestWindow::new(set, &input, &output);
        sparse
            .admit(
                vec![
                    int64([None, Some(4), Some(10)]),
                    booleans([None, Some(true), Some(false)]),
                    strings([Some("missing"), None, Some("ten")]),
                ],
                &[1, 2, 3],
            )
            .await;
        let record = sparse.emitted().await;
        assert_eq!(batch_value(&record, "samples"), Some(RuntimeValue::I64(3)));
        assert_eq!(batch_value(&record, "total"), Some(RuntimeValue::I64(14)));
        assert_eq!(batch_value(&record, "mean_value"), f64_value(7.0));
        assert_eq!(batch_value(&record, "lowest"), Some(RuntimeValue::I64(4)));
        assert_eq!(
            batch_value(&record, "first_value"),
            Some(RuntimeValue::I64(4))
        );
        assert_eq!(batch_value(&record, "value_var_samp"), f64_value(18.0));
        assert_eq!(
            batch_value(&record, "healthy_samples"),
            Some(RuntimeValue::I64(1))
        );
        assert_eq!(
            batch_value(&record, "all_healthy"),
            Some(RuntimeValue::Bool(false))
        );
        assert_eq!(
            batch_value(&record, "lowest_label"),
            Some(RuntimeValue::String("ten".to_string()))
        );
        assert_eq!(batch_value(&record, "correlation"), f64_value(1.0));

        let mut empty = TestWindow::new(set, &input, &output);
        empty
            .admit(
                vec![
                    int64([None, None]),
                    booleans([None, None]),
                    strings([None, Some("orphan")]),
                ],
                &[1, 2],
            )
            .await;
        let record = empty.emitted().await;
        assert_eq!(batch_value(&record, "samples"), Some(RuntimeValue::I64(2)));
        assert_eq!(
            batch_value(&record, "healthy_samples"),
            Some(RuntimeValue::I64(0))
        );
        for field in [
            "total",
            "mean_value",
            "lowest",
            "first_value",
            "value_var_samp",
            "all_healthy",
            "lowest_label",
            "correlation",
        ] {
            assert_eq!(batch_value(&record, field), None, "{field} should be null");
        }
    }

    #[tokio::test]
    async fn rows_with_non_finite_statistic_arguments_are_refused_before_admission() {
        let mut window = TestWindow::new(
            "SET mean_reading = AVG(input.reading), highest = MAX(input.reading), samples = \
             COUNT(input.reading)",
            &[field("reading", ParseAsType::F64)],
            &[
                field("mean_reading", ParseAsType::F64),
                field("highest", ParseAsType::F64),
                field("samples", ParseAsType::I64),
            ],
        );
        let refused = window
            .admit(
                vec![float64([
                    Some(1.0),
                    Some(f64::NAN),
                    Some(3.0),
                    Some(f64::INFINITY),
                ])],
                &[1, 2, 3, 4],
            )
            .await;
        assert_eq!(
            refused,
            vec![
                "AVG requires finite floating-point arguments".to_string(),
                "AVG requires finite floating-point arguments".to_string(),
            ]
        );
        let record = window.emitted().await;
        assert_eq!(batch_value(&record, "mean_reading"), f64_value(2.0));
        assert_eq!(batch_value(&record, "highest"), f64_value(3.0));
        assert_eq!(batch_value(&record, "samples"), Some(RuntimeValue::I64(2)));
    }

    #[tokio::test]
    async fn argument_evaluation_failures_refuse_only_their_rows_in_one_vm_execution() {
        let mut window = TestWindow::new(
            "SET adjusted_total = SUM(120 / input.latency)",
            &[field("latency", ParseAsType::I64)],
            &[field("adjusted_total", ParseAsType::I64)],
        );
        WINDOW_ARGUMENT_VM_EXECUTIONS.with(|executions| executions.set(0));
        let refused = window
            .admit(vec![int64([Some(10), Some(0), Some(30)])], &[1, 2, 3])
            .await;
        assert_eq!(
            WINDOW_ARGUMENT_VM_EXECUTIONS.with(std::cell::Cell::get),
            1,
            "all input rows must share one aggregate-argument VM execution"
        );
        assert_eq!(refused.len(), 1);
        assert!(
            refused[0].contains("division_by_zero"),
            "the failed row should carry the division_by_zero side error, got {refused:?}"
        );
        assert_eq!(window.state.entries.len(), 2);
        let record = window.emitted().await;
        assert_eq!(
            batch_value(&record, "adjusted_total"),
            Some(RuntimeValue::I64(16))
        );
    }

    #[tokio::test]
    async fn integer_sums_are_exact_through_retraction_and_report_overflow_of_their_type() {
        let mut window = TestWindow::new(
            "SET total = SUM(input.value)",
            &[field("value", ParseAsType::I64)],
            &[field("total", ParseAsType::I64)],
        );
        window
            .admit(vec![int64([Some(i64::MAX), Some(1)])], &[1, 2])
            .await;
        let overflow = window
            .emit()
            .await
            .expect_err("MAX + 1 does not fit an I64 sum");
        assert!(matches!(
            overflow.current_context(),
            WindowProcessorError::SumOverflow { .. }
        ));
        window.step(1, 3);
        window.admit(vec![int64([Some(-1)])], &[3]).await;
        let record = window.emitted().await;
        assert_eq!(
            batch_value(&record, "total"),
            Some(RuntimeValue::I64(0)),
            "a sum that returns to zero is zero, not an empty window"
        );
    }

    #[tokio::test]
    async fn float_sums_forget_an_evicted_outlier_exactly() {
        let mut window = TestWindow::new(
            "SET total = SUM(input.reading), mean_reading = AVG(input.reading), spread = \
             VAR_POP(input.reading)",
            &[field("reading", ParseAsType::F64)],
            &[
                field("total", ParseAsType::F64),
                field("mean_reading", ParseAsType::F64),
                field("spread", ParseAsType::F64),
            ],
        );
        window
            .admit(vec![float64([Some(1e16), Some(1.0)])], &[1, 2])
            .await;
        window.step(1, 2);
        window.admit(vec![float64([Some(3.0)])], &[3]).await;
        let record = window.emitted().await;
        assert_eq!(batch_value(&record, "total"), f64_value(4.0));
        assert_eq!(batch_value(&record, "mean_reading"), f64_value(2.0));
        assert_eq!(batch_value(&record, "spread"), f64_value(1.0));
    }

    #[tokio::test]
    async fn extremes_prefer_the_earliest_row_and_order_arrival_by_watermark() {
        let mut window = TestWindow::new(
            "SET lowest_label = ARG_MIN(input.label, input.value), highest_label = \
             ARG_MAX(input.label, input.value), lowest = MIN(input.value), first_label = \
             FIRST(input.label), last_label = LAST(input.label)",
            &[
                field("label", ParseAsType::String),
                field("value", ParseAsType::I64),
            ],
            &[
                field("lowest_label", ParseAsType::String),
                field("highest_label", ParseAsType::String),
                field("lowest", ParseAsType::I64),
                field("first_label", ParseAsType::String),
                field("last_label", ParseAsType::String),
            ],
        );
        window
            .admit(
                vec![
                    strings([Some("a"), Some("b"), Some("c"), Some("d")]),
                    int64([Some(5), Some(1), Some(9), Some(1)]),
                ],
                &[30, 10, 20, 40],
            )
            .await;
        let string = |value: &str| Some(RuntimeValue::String(value.to_string()));
        let record = window.emitted().await;
        assert_eq!(batch_value(&record, "lowest_label"), string("b"));
        assert_eq!(batch_value(&record, "highest_label"), string("c"));
        assert_eq!(batch_value(&record, "lowest"), Some(RuntimeValue::I64(1)));
        assert_eq!(batch_value(&record, "first_label"), string("b"));
        assert_eq!(batch_value(&record, "last_label"), string("d"));

        window.step(2, 40);
        let record = window.emitted().await;
        assert_eq!(batch_value(&record, "lowest_label"), string("d"));
        assert_eq!(batch_value(&record, "highest_label"), string("c"));
        assert_eq!(batch_value(&record, "first_label"), string("c"));
    }

    #[tokio::test]
    async fn admission_runs_end_at_the_row_that_fills_the_window() {
        let mut window = TestWindow::new(
            "SET samples = COUNT(input.value)",
            &[field("value", ParseAsType::I64)],
            &[field("samples", ParseAsType::I64)],
        );
        window
            .admit(vec![int64([Some(1), Some(2)])], &[1_000, 2_000])
            .await;
        let pending = |timestamps: &[i64]| {
            let values = int64(timestamps.iter().map(|_| Some(0)));
            let schema = window.input_schema.arrow_schema();
            let batch = RecordBatch::try_new(StdArc::clone(&schema), vec![values])
                .expect("the pending rows match the input schema");
            let carrier = Arc::new(
                RuntimeRecordBatch::from_record_batch(schema, batch)
                    .expect("the pending rows form a relay batch"),
            );
            timestamps
                .iter()
                .enumerate()
                .map(|(row, timestamp)| WindowAdmission {
                    message: RelayMessage {
                        key: None,
                        record: RuntimeRow::new(
                            carrier.clone(),
                            row,
                            RuntimeRecordMetadata::from_ingested_at_watermarks(
                                at(*timestamp),
                                at(*timestamp),
                            ),
                        )
                        .expect("every pending row is inside its batch"),
                        acks: AckSet::empty(),
                    },
                    row,
                })
                .collect::<VecDeque<_>>()
        };
        let three = pending(&[3_000, 4_000, 5_000]);
        assert_eq!(window.state.admission_run_len(&three, Some(3), None), 1);
        assert_eq!(window.state.admission_run_len(&three, Some(10), None), 3);
        assert_eq!(
            window
                .state
                .admission_run_len(&three, None, Some(Duration::from_nanos(3_500))),
            3,
            "the row at 5_000 is the first one 3_500 or more after the window's first row at 1_000"
        );
        assert_eq!(
            window
                .state
                .admission_run_len(&three, None, Some(Duration::from_nanos(2_500))),
            2,
            "the row at 4_000 is the first one 2_500 or more after the window's first row at 1_000"
        );
        let early = pending(&[500, 600]);
        assert_eq!(
            window
                .state
                .admission_run_len(&early, None, Some(Duration::from_nanos(1_000))),
            2
        );
    }

    #[tokio::test]
    async fn restored_windows_answer_every_aggregate_as_before_they_were_published() {
        let set = "SET count = COUNT(input.latency), total = SUM(input.latency), first_latency = \
                   FIRST(input.latency), highest = MAX(input.latency), mean_latency = \
                   AVG(input.latency), healthy = BOOL_AND(input.healthy), p0 = \
                   PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '2s')";
        let input = [
            field("latency", ParseAsType::I64),
            field("healthy", ParseAsType::Bool),
        ];
        let output = [
            field("count", ParseAsType::I64),
            field("total", ParseAsType::I64),
            field("first_latency", ParseAsType::I64),
            field("highest", ParseAsType::I64),
            field("mean_latency", ParseAsType::F64),
            field("healthy", ParseAsType::Bool),
            field("p0", ParseAsType::F64),
        ];
        let mut window = TestWindow::new(set, &input, &output);
        window
            .admit(
                vec![
                    int64([Some(10), Some(30), Some(90)]),
                    booleans([Some(true), Some(false), Some(true)]),
                ],
                &[10, 20, 30],
            )
            .await;
        window.step(1, 30);
        window
            .admit(vec![int64([Some(50)]), booleans([Some(true)])], &[40])
            .await;
        let before = window.emitted().await;

        let snapshot = window
            .state
            .to_snapshot()
            .expect("the window should publish");
        let restored = WindowProcessorState::from_snapshot(
            &window.plan,
            window.input_schema.as_ref(),
            &snapshot,
        )
        .expect("the published window should restore");
        assert_eq!(restored.entries.len(), 3);
        assert_eq!(restored.next_sequence, window.state.next_sequence);
        assert_eq!(
            restored.next_timeout_deadline(),
            window.state.next_timeout_deadline(),
            "the histogram's delayed removal survives publication"
        );
        window.state = restored;
        let after = window.emitted().await;
        for field in [
            "count",
            "total",
            "first_latency",
            "highest",
            "mean_latency",
            "healthy",
            "p0",
        ] {
            assert_eq!(
                batch_value(&after, field),
                batch_value(&before, field),
                "{field} should survive restoring the published window"
            );
        }
        assert_eq!(batch_value(&after, "p0"), f64_value(15.0));
    }

    #[tokio::test]
    async fn window_aggregate_evaluator_computes_vm_expression_percentile_and_array() {
        let mut window = TestWindow::new(
            "SET count = COUNT(input.latency), adjusted_count = COUNT(input.latency) + 2, p50 = \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), latencies = \
             [PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 100, 10, 0, 100, '2s')], observed_at = \
             now()",
            &[field("latency", ParseAsType::F64)],
            &[
                field("count", ParseAsType::I64),
                field("adjusted_count", ParseAsType::I64),
                field("p50", ParseAsType::F64),
                field(
                    "latencies",
                    ParseAsType::Array {
                        element: Box::new(ParseAsType::F64),
                        len: nonzero!(2u32),
                    },
                ),
                field("observed_at", ParseAsType::Datetime),
            ],
        );
        window
            .admit(
                vec![float64([Some(10.0), Some(20.0), Some(30.0)])],
                &[1, 2, 3],
            )
            .await;
        let record = window.emitted().await;

        assert_eq!(batch_value(&record, "count"), Some(RuntimeValue::I64(3)));
        assert_eq!(
            batch_value(&record, "adjusted_count"),
            Some(RuntimeValue::I64(5))
        );
        assert_eq!(batch_value(&record, "p50"), f64_value(25.0));
        assert_eq!(
            batch_value(&record, "latencies"),
            Some(RuntimeValue::Array(vec![
                RuntimeValue::F64(OrderedFloat(25.0)),
                RuntimeValue::F64(OrderedFloat(35.0)),
            ]))
        );
        assert_eq!(
            batch_value(&record, "observed_at"),
            Some(RuntimeValue::Datetime(at(42).as_datetime().fixed_offset()))
        );
    }

    #[tokio::test]
    async fn window_linear_histogram_percentiles_share_accumulator_by_config() {
        let mut window = TestWindow::new(
            "SET p50 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), p90 = \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 90, 10, 0, 100, '2s'), p50_other_range = \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 200, '2s')",
            &[field("latency", ParseAsType::I64)],
            &[
                field("p50", ParseAsType::F64),
                field("p90", ParseAsType::F64),
                field("p50_other_range", ParseAsType::F64),
            ],
        );
        assert_eq!(
            window.state.accumulators.len(),
            2,
            "same input and histogram config should share one accumulator"
        );
        window
            .admit(vec![int64([Some(10), Some(20), Some(30)])], &[1, 2, 3])
            .await;
        let record = window.emitted().await;
        assert_eq!(batch_value(&record, "p50"), f64_value(25.0));
        assert_eq!(batch_value(&record, "p90"), f64_value(35.0));
        assert_eq!(batch_value(&record, "p50_other_range"), f64_value(30.0));
    }

    #[tokio::test]
    async fn window_advance_removes_step_messages() {
        let mut window = TestWindow::new(
            "SET count = COUNT(input.latency)",
            &[field("latency", ParseAsType::I64)],
            &[field("count", ParseAsType::I64)],
        );
        window
            .admit(vec![int64((0..5).map(Some))], &[1, 2, 3, 4, 5])
            .await;

        advance_window(&mut window.state, Some(2), None, at(5));

        assert_eq!(window.state.entries.len(), 3);
        assert_eq!(
            window.state.entries.front().map(|entry| entry.row.sequence),
            Some(2)
        );
        let record = window.emitted().await;
        assert_eq!(batch_value(&record, "count"), Some(RuntimeValue::I64(3)));
    }

    #[tokio::test]
    async fn window_advance_steps_by_duration_after_messages() {
        let mut window = TestWindow::new(
            "SET count = COUNT(input.latency)",
            &[field("latency", ParseAsType::I64)],
            &[field("count", ParseAsType::I64)],
        );
        window
            .admit(vec![int64((0..5).map(Some))], &[0, 10, 20, 30, 40])
            .await;

        advance_window(
            &mut window.state,
            Some(1),
            Some(Duration::from_nanos(15)),
            at(40),
        );

        assert_eq!(
            window
                .state
                .entries
                .front()
                .map(|entry| entry.row.timestamp),
            Some(at(30)),
            "one message steps past 0, then the duration steps past everything before 10 + 15"
        );
        let record = window.emitted().await;
        assert_eq!(batch_value(&record, "count"), Some(RuntimeValue::I64(2)));
    }

    #[tokio::test]
    async fn linear_histogram_zero_delay_removes_step_values_immediately() {
        let mut window = TestWindow::new(
            "SET p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '0ms')",
            &[field("latency", ParseAsType::I64)],
            &[field("p0", ParseAsType::F64)],
        );
        window
            .admit(vec![int64([Some(10), Some(90)])], &[0, 1_000_000_000])
            .await;
        advance_window(&mut window.state, Some(1), None, at(1_000_000_000));
        let record = window.emitted().await;
        assert_eq!(batch_value(&record, "p0"), f64_value(95.0));
    }

    #[tokio::test]
    async fn linear_histogram_delay_retains_removed_step_values_until_expired() {
        let mut window = TestWindow::new(
            "SET p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '2s')",
            &[field("latency", ParseAsType::I64)],
            &[field("p0", ParseAsType::F64)],
        );
        window
            .admit(vec![int64([Some(10), Some(90)])], &[0, 1_000_000_000])
            .await;
        advance_window(&mut window.state, Some(1), None, at(1_000_000_000));
        let retained = window.emitted().await;
        assert_eq!(batch_value(&retained, "p0"), f64_value(15.0));

        window
            .admit(vec![int64([Some(90)])], &[2_000_000_000])
            .await;
        let still_retained = window.emitted().await;
        assert_eq!(batch_value(&still_retained, "p0"), f64_value(15.0));

        window
            .admit(vec![int64([Some(90)])], &[4_000_000_000])
            .await;
        let expired = window.emitted().await;
        assert_eq!(batch_value(&expired, "p0"), f64_value(95.0));
    }

    #[tokio::test]
    async fn linear_histogram_delay_exposes_timeout_deadline_without_new_messages() {
        let mut window = TestWindow::new(
            "SET p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '2s')",
            &[field("latency", ParseAsType::I64)],
            &[field("p0", ParseAsType::F64)],
        );
        window
            .admit(vec![int64([Some(10), Some(90)])], &[0, 1_000_000_000])
            .await;
        advance_window(&mut window.state, Some(1), None, at(1_000_000_000));
        assert_eq!(
            window.state.next_timeout_deadline(),
            Some(at(3_000_000_000))
        );

        assert!(!window.state.purge_timeouts(at(2_999_999_999)));
        assert!(window.state.purge_timeouts(at(3_000_000_000)));
        assert_eq!(window.state.next_timeout_deadline(), None);

        let record = window.emitted().await;
        assert_eq!(batch_value(&record, "p0"), f64_value(95.0));
    }

    #[tokio::test]
    async fn window_aggregate_state_updates_first_last_min_max_and_sum() {
        let mut window = TestWindow::new(
            "SET first_latency = FIRST(input.latency), last_latency = LAST(input.latency), \
             min_latency = MIN(input.latency), max_latency = MAX(input.latency), total_latency = \
             SUM(input.latency)",
            &[field("latency", ParseAsType::I64)],
            &[
                field("first_latency", ParseAsType::I64),
                field("last_latency", ParseAsType::I64),
                field("min_latency", ParseAsType::I64),
                field("max_latency", ParseAsType::I64),
                field("total_latency", ParseAsType::I64),
            ],
        );
        assert_eq!(
            window.state.accumulators.len(),
            3,
            "FIRST/LAST and MIN/MAX should each share one physical structure"
        );
        window
            .admit(vec![int64([Some(30), Some(10), Some(20)])], &[1, 2, 3])
            .await;
        let record = window.emitted().await;
        assert_eq!(
            batch_value(&record, "first_latency"),
            Some(RuntimeValue::I64(30))
        );
        assert_eq!(
            batch_value(&record, "last_latency"),
            Some(RuntimeValue::I64(20))
        );
        assert_eq!(
            batch_value(&record, "min_latency"),
            Some(RuntimeValue::I64(10))
        );
        assert_eq!(
            batch_value(&record, "max_latency"),
            Some(RuntimeValue::I64(30))
        );
        assert_eq!(
            batch_value(&record, "total_latency"),
            Some(RuntimeValue::I64(60))
        );

        advance_window(&mut window.state, Some(1), None, at(3));
        let record = window.emitted().await;
        assert_eq!(
            batch_value(&record, "first_latency"),
            Some(RuntimeValue::I64(10))
        );
        assert_eq!(
            batch_value(&record, "last_latency"),
            Some(RuntimeValue::I64(20))
        );
        assert_eq!(
            batch_value(&record, "min_latency"),
            Some(RuntimeValue::I64(10))
        );
        assert_eq!(
            batch_value(&record, "max_latency"),
            Some(RuntimeValue::I64(20))
        );
        assert_eq!(
            batch_value(&record, "total_latency"),
            Some(RuntimeValue::I64(30))
        );
    }

    #[test]
    fn window_message_timestamp_uses_low_watermark() {
        let message = RelayMessage {
            key: None,
            record: crate::runtime_schema::test_runtime_row([]).with_metadata(
                RuntimeRecordMetadata::from_ingested_at_watermarks(at(10), at(20)),
            ),
            acks: AckSet::empty(),
        };

        assert_eq!(message_timestamp(&message), at(10));
    }

    #[tokio::test]
    async fn window_output_metadata_uses_window_low_and_emit_high_watermark() {
        let mut window = TestWindow::new(
            "SET count = COUNT(input.latency)",
            &[field("latency", ParseAsType::I64)],
            &[field("count", ParseAsType::I64)],
        );
        window
            .admit(vec![int64([Some(1), Some(2), Some(3)])], &[30, 10, 20])
            .await;

        let metadata = window_output_metadata(&window.state, at(40))
            .expect("non-empty window should emit metadata");

        assert_eq!(metadata.ingested_at_low_watermark(), at(10));
        assert_eq!(metadata.ingested_at_high_watermark(), at(40));
    }

    /// The message a failed operation reports to the processor that called it.
    fn failure<T, C: error_stack::Context>(result: error_stack::Result<T, C>) -> String {
        let Err(error) = result else {
            panic!("the operation must fail");
        };
        error.current_context().to_string()
    }

    #[tokio::test]
    async fn restoring_a_window_rejects_snapshots_that_disagree_with_its_plan() {
        let mut window = TestWindow::new(
            "SET count = COUNT(input.latency)",
            &[field("latency", ParseAsType::I64)],
            &[field("count", ParseAsType::I64)],
        );
        window
            .admit(vec![int64([Some(10), Some(20)])], &[1, 2])
            .await;
        let snapshot = window
            .state
            .to_snapshot()
            .expect("the window should publish");
        let wider = TestWindow::new(
            "SET count = COUNT(input.latency), total = SUM(input.latency)",
            &[field("latency", ParseAsType::I64)],
            &[
                field("count", ParseAsType::I64),
                field("total", ParseAsType::I64),
            ],
        );
        assert_eq!(
            failure(WindowProcessorState::from_snapshot(
                &wider.plan,
                window.input_schema.as_ref(),
                &snapshot
            )),
            "window snapshot accumulator count 1 does not match aggregate demand count 2"
        );

        let mut keyed = snapshot.clone();
        keyed.entries[0].key = Some(Vec::new());
        assert_eq!(
            failure(WindowProcessorState::from_snapshot(
                &window.plan,
                window.input_schema.as_ref(),
                &keyed
            )),
            "failed to restore a window entry branch key: branch key must contain at least one \
             field"
        );

        let mut gapped = snapshot.clone();
        gapped.entries[1].sequence = 5;
        assert_eq!(
            failure(WindowProcessorState::from_snapshot(
                &window.plan,
                window.input_schema.as_ref(),
                &gapped
            )),
            "window snapshot row sequence 5 does not follow sequence 0"
        );

        let mut truncated = snapshot;
        truncated.entries[0].arguments.clear();
        assert_eq!(
            failure(WindowProcessorState::from_snapshot(
                &window.plan,
                window.input_schema.as_ref(),
                &truncated
            )),
            "window snapshot row carries 0 argument values for 1 arguments"
        );
    }

    #[tokio::test]
    async fn window_aggregate_reports_uninitialized_outputs_and_null_required_results() {
        let window = TestWindow::new(
            "SET count = COUNT(input.latency)",
            &[field("latency", ParseAsType::I64)],
            &[
                field("count", ParseAsType::I64),
                field("extra", ParseAsType::I64),
            ],
        );
        assert_eq!(
            failure(window.emit().await),
            "window aggregate did not initialize required output field 'extra'"
        );

        let empty = TestWindow::new(
            "SET first_latency = FIRST(input.latency)",
            &[field("latency", ParseAsType::I64)],
            &[field("first_latency", ParseAsType::I64)],
        );
        assert_eq!(
            failure(empty.emit().await),
            "window aggregate VM produced null 'first_latency' output"
        );
    }

    #[test]
    fn window_aggregate_compilation_reports_missing_schemas_and_targets() {
        let input = named::<RelayName>("latencies");
        let output = named::<RelayName>("summaries");
        let input_schema = test_schema(&[("latency", ParseAsType::I64)]);
        let aggregate = window_aggregate("SET count = COUNT(input.latency)");

        let output_only =
            HashMap::from_iter([(output.clone(), test_schema(&[("count", ParseAsType::I64)]))]);
        assert_eq!(
            failure(CompiledWindowAggregateProgram::compile(
                &aggregate,
                std::slice::from_ref(&input),
                &output,
                &output_only,
                None,
            )),
            "window aggregate input relay 'latencies' has no runtime schema"
        );

        let unrelated_output = HashMap::from_iter([
            (input.clone(), input_schema.clone()),
            (output.clone(), test_schema(&[("total", ParseAsType::I64)])),
        ]);
        let Err(missing_field) = CompiledWindowAggregateProgram::compile(
            &aggregate,
            std::slice::from_ref(&input),
            &output,
            &unrelated_output,
            None,
        ) else {
            panic!("an assignment to an undeclared field must not compile");
        };
        assert!(
            format!("{missing_field:#}")
                .contains("window aggregate output schema is missing field 'count'"),
            "{missing_field:#}"
        );

        let array = window_aggregate("SET count = [COUNT(input.latency), COUNT(input.latency)]");
        let scalar_output = HashMap::from_iter([
            (input.clone(), input_schema),
            (output.clone(), test_schema(&[("count", ParseAsType::I64)])),
        ]);
        let Err(array_target) = CompiledWindowAggregateProgram::compile(
            &array,
            std::slice::from_ref(&input),
            &output,
            &scalar_output,
            None,
        ) else {
            panic!("an array cannot be assigned to a scalar field");
        };
        assert!(
            format!("{array_target:#}")
                .contains("window aggregate array cannot be assigned to Int64 field 'count'"),
            "{array_target:#}"
        );
    }

    #[tokio::test]
    async fn datetime_extremes_keep_their_timezone() {
        let mut window = TestWindow::new(
            "SET latest = MAX(input.observed_at)",
            &[field("observed_at", ParseAsType::Datetime)],
            &[field("latest", ParseAsType::Datetime)],
        );
        let observed: ArrayRef = StdArc::new(
            TimestampNanosecondArray::from(vec![Some(5), Some(9), Some(7)]).with_timezone("+00:00"),
        );
        window.admit(vec![observed], &[1, 2, 3]).await;
        let record = window.emitted().await;
        assert_eq!(
            batch_value(&record, "latest"),
            Some(RuntimeValue::Datetime(at(9).as_datetime().fixed_offset()))
        );
    }
}
