//! Branch-local window processor execution.
//!
//! Layer: data plane.
//! - **Owns.** Window state, aggregate accumulation, due-window emission and eviction.
//! - **Depends on.** Validated window plans, Arrow batches and bound domain time.
//! - **Must not know.** NSPL parsing, placement decisions or connector transports.

use error_stack::{Report, ResultExt as _};

use super::*;

/// Every way a window processor fails, from accumulating one aggregate input to publishing the
/// branch-local window it owns.
#[derive(Debug, thiserror::Error)]
pub(super) enum WindowProcessorError {
    #[error("window aggregate requires a non-empty window")]
    EmptyWindow,
    #[error("window processor '{}' failed to snapshot branch state", .processor.as_str())]
    Snapshot { processor: ModelName },
    #[error("linear histogram delayed removal bucket is out of range")]
    DelayedRemovalBucketOutOfRange,
    #[error("linear histogram accumulator is missing delayed removed value")]
    MissingDelayedRemovedValue,
    #[error("linear histogram bucket is out of range")]
    BucketOutOfRange,
    #[error("linear histogram accumulator is missing removed value")]
    MissingRemovedValue,
    #[error("sequence aggregate structure requires a value")]
    SequenceRequiresValue,
    #[error("ordered aggregate structure requires a value")]
    OrderedRequiresValue,
    #[error("PERCENTILE_LINEAR_HISTOGRAM requires a value")]
    HistogramRequiresValue,
    #[error("SUM requires a value")]
    SumRequiresValue,
    #[error("sequence accumulator is missing removed window entry")]
    MissingSequenceEntry,
    #[error("sorted accumulator is missing removed window value")]
    MissingSortedValue,
    #[error("FIRST requires a non-empty window")]
    FirstRequiresWindow,
    #[error("LAST requires a non-empty window")]
    LastRequiresWindow,
    #[error("MAX requires a non-empty window")]
    MaxRequiresWindow,
    #[error("MIN requires a non-empty window")]
    MinRequiresWindow,
    #[error("SUM requires a non-empty window")]
    SumRequiresWindow,
    #[error("PERCENTILE_LINEAR_HISTOGRAM requires a constant percentile")]
    HistogramRequiresPercentile,
    #[error("{function:?} aggregate is backed by an incompatible accumulator")]
    IncompatibleAccumulator { function: WindowAggregateFunction },
    #[error(
        "window snapshot accumulator count {accumulators} does not match aggregate demand count \
         {demands}"
    )]
    SnapshotDemandCount { accumulators: usize, demands: usize },
    #[error("failed to encode a window entry for the branch snapshot")]
    EncodeSnapshotEntry,
    #[error("failed to restore a window entry from the branch snapshot")]
    RestoreSnapshotEntry,
    #[error("failed to restore a window entry branch key: {reason}")]
    RestoreSnapshotBranchKey { reason: String },
    #[error(
        "window aggregate input count {inputs} does not match accumulator count {accumulators}"
    )]
    AggregateInputCount { inputs: usize, accumulators: usize },
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
    #[error("failed to read the window aggregate input column '{field}'")]
    AggregateInputColumn { field: String },
    #[error("window aggregate input VM failed with {}: {message}", .code.as_str())]
    AggregateInputRow {
        code: nervix_vm::ErrorCode,
        message: String,
    },
    #[error("PERCENTILE_LINEAR_HISTOGRAM requires finite numeric values")]
    HistogramRequiresFinite,
    #[error("PERCENTILE_LINEAR_HISTOGRAM requires at least one bucket")]
    HistogramRequiresBucket,
    #[error("PERCENTILE_LINEAR_HISTOGRAM value {value} falls outside the bucket range")]
    HistogramValueOutOfRange { value: f64 },
    #[error("PERCENTILE_LINEAR_HISTOGRAM requires a non-empty window")]
    HistogramRequiresWindow,
    #[error(
        "PERCENTILE_LINEAR_HISTOGRAM percentile {percentile} has no rank in a window of {total} \
         samples"
    )]
    HistogramPercentileRank { percentile: f64, total: usize },
    #[error("PERCENTILE_LINEAR_HISTOGRAM histogram is empty")]
    HistogramEmpty,
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
    #[error("failed to read the window aggregate VM output '{field}'")]
    AggregateExprOutput { field: String },
    #[error("window aggregate VM produced null '{field}' output")]
    AggregateExprNullOutput { field: String },
    #[error("expected numeric value, found {type_name}")]
    NotNumeric { type_name: &'static str },
    #[error("SUM cannot combine {left} and {right}")]
    SumIncompatible {
        left: &'static str,
        right: &'static str,
    },
    #[error("SUM cannot remove {right} from {left}")]
    SumRemoveIncompatible {
        left: &'static str,
        right: &'static str,
    },
}

/// The window entry a failed accumulation belongs to, handed back so its ACKs stay resolvable.
#[derive(Debug)]
pub(super) struct WindowPushFailure {
    pub(super) error: Report<WindowProcessorError>,
    pub(super) message: RelayMessage,
}

#[derive(Debug)]
pub(super) struct WindowEntry {
    pub(super) sequence: u64,
    pub(super) timestamp: Timestamp,
    pub(super) message: RelayMessage,
    pub(super) aggregate_inputs: Vec<WindowAggregateInput>,
}

#[derive(Debug, Clone)]
pub(super) struct LinearHistogramDelayedRemoval {
    pub(super) expires_at: Timestamp,
    pub(super) bucket: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RuntimeValueSortKey(pub(super) RuntimeValue);

impl PartialOrd for RuntimeValueSortKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RuntimeValueSortKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_runtime_values(&self.0, &other.0)
    }
}

#[derive(Debug, Clone)]
pub(super) enum WindowAggregateAccumulator {
    Counter {
        count: usize,
    },
    Sequence {
        values: VecDeque<WindowSequenceValue>,
    },
    SortedMap {
        counts: BTreeMap<RuntimeValueSortKey, usize>,
    },
    LinearHistogram {
        buckets: Vec<usize>,
        total: usize,
        min: f64,
        max: f64,
        width: f64,
        delay: Duration,
        delayed_removals: VecDeque<LinearHistogramDelayedRemoval>,
    },
    Sum {
        total: Option<RuntimeValue>,
    },
}

/// One value held in a sequence accumulator, kept with the window entry it arrived from so
/// `FIRST` and `LAST` can order by arrival and `remove` can find the entry that left the window.
#[derive(Debug, Clone)]
pub(super) struct WindowSequenceValue {
    pub(super) timestamp: Timestamp,
    pub(super) sequence: u64,
    pub(super) value: RuntimeValue,
}

#[derive(Debug)]
pub(super) struct WindowProcessorState {
    pub(super) entries: VecDeque<WindowEntry>,
    pub(super) next_sequence: u64,
    pub(super) accumulators: Vec<WindowAggregateAccumulator>,
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
        .map(|entry| entry.timestamp)
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
    aggregate: &WindowAggregateProgram,
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
        state.clear(aggregate);
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
        state.clear(aggregate);
        return true;
    }
    let mut changed = false;
    match state.purge_timeouts(now) {
        Ok(purged) => {
            changed |= purged;
        }
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                state.entries.iter().map(|entry| &entry.message.acks),
                format!(
                    "window processor '{}' failed to purge timed aggregate state: {error:#}",
                    processor.as_str(),
                ),
            );
            state.clear(aggregate);
            return true;
        }
    }
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
                state.clear(aggregate);
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
            state.clear(aggregate);
            changed = true;
            break;
        }
        if let Err(error) = advance_window(
            state,
            aggregate,
            bounds.step_messages,
            bounds.step_duration,
            now,
        ) {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                state.entries.iter().map(|entry| &entry.message.acks),
                format!(
                    "window processor '{}' failed to advance window: {error:#}",
                    processor.as_str(),
                ),
            );
            state.clear(aggregate);
            changed = true;
            break;
        }
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

impl WindowAggregateAccumulator {
    pub(super) fn new(demand: &WindowAggregateDemand) -> Self {
        match demand.storage {
            WindowAggregateStorageKind::Counter => Self::Counter { count: 0 },
            WindowAggregateStorageKind::Sequence => Self::Sequence {
                values: VecDeque::new(),
            },
            WindowAggregateStorageKind::SortedMap => Self::SortedMap {
                counts: BTreeMap::new(),
            },
            WindowAggregateStorageKind::Histogram => {
                let config = demand.linear_histogram.as_ref().verified(
                    "the histogram storage kind is only chosen for a demand that carries the \
                     histogram config",
                );
                Self::LinearHistogram {
                    buckets: vec![0; config.buckets.get()],
                    total: 0,
                    min: config.min,
                    max: config.max,
                    width: (config.max - config.min) / config.buckets.get().approx_into::<f64>(),
                    delay: config.delay,
                    delayed_removals: VecDeque::new(),
                }
            }
            WindowAggregateStorageKind::Sum => Self::Sum { total: None },
        }
    }

    pub(super) fn to_snapshot(&self) -> WindowAggregateAccumulatorSnapshot {
        match self {
            Self::Counter { count } => {
                WindowAggregateAccumulatorSnapshot::Counter { count: *count }
            }
            Self::Sequence { values } => WindowAggregateAccumulatorSnapshot::Sequence {
                values: values
                    .iter()
                    .map(|entry| WindowSequenceValueSnapshot {
                        timestamp: entry.timestamp,
                        sequence: entry.sequence,
                        value: entry.value.to_remote(),
                    })
                    .collect(),
            },
            Self::SortedMap { counts } => WindowAggregateAccumulatorSnapshot::SortedMap {
                counts: counts
                    .iter()
                    .map(|(value, count)| WindowSortedCountSnapshot {
                        value: value.0.to_remote(),
                        count: *count,
                    })
                    .collect(),
            },
            Self::LinearHistogram {
                buckets,
                total,
                min,
                max,
                width,
                delay,
                delayed_removals,
            } => WindowAggregateAccumulatorSnapshot::LinearHistogram {
                buckets: buckets.clone(),
                total: *total,
                min: *min,
                max: *max,
                width: *width,
                delay_nanos: u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX),
                delayed_removals: delayed_removals
                    .iter()
                    .map(|removal| LinearHistogramDelayedRemovalSnapshot {
                        expires_at: removal.expires_at,
                        bucket: removal.bucket,
                    })
                    .collect(),
            },
            Self::Sum { total } => WindowAggregateAccumulatorSnapshot::Sum {
                total: total.as_ref().map(RuntimeValue::to_remote),
            },
        }
    }

    pub(super) fn from_snapshot(snapshot: &WindowAggregateAccumulatorSnapshot) -> Self {
        match snapshot {
            WindowAggregateAccumulatorSnapshot::Counter { count } => {
                Self::Counter { count: *count }
            }
            WindowAggregateAccumulatorSnapshot::Sequence { values } => Self::Sequence {
                values: values
                    .iter()
                    .map(|snapshot| WindowSequenceValue {
                        timestamp: snapshot.timestamp,
                        sequence: snapshot.sequence,
                        value: RuntimeValue::from_remote(snapshot.value.clone()),
                    })
                    .collect(),
            },
            WindowAggregateAccumulatorSnapshot::SortedMap { counts } => Self::SortedMap {
                counts: counts
                    .iter()
                    .map(|entry| {
                        (
                            RuntimeValueSortKey(RuntimeValue::from_remote(entry.value.clone())),
                            entry.count,
                        )
                    })
                    .collect(),
            },
            WindowAggregateAccumulatorSnapshot::LinearHistogram {
                buckets,
                total,
                min,
                max,
                width,
                delay_nanos,
                delayed_removals,
            } => Self::LinearHistogram {
                buckets: buckets.clone(),
                total: *total,
                min: *min,
                max: *max,
                width: *width,
                delay: Duration::from_nanos(*delay_nanos),
                delayed_removals: delayed_removals
                    .iter()
                    .map(|removal| LinearHistogramDelayedRemoval {
                        expires_at: removal.expires_at,
                        bucket: removal.bucket,
                    })
                    .collect(),
            },
            WindowAggregateAccumulatorSnapshot::Sum { total } => Self::Sum {
                total: total.clone().map(RuntimeValue::from_remote),
            },
        }
    }

    pub(super) fn purge_expired(
        &mut self,
        now: Timestamp,
    ) -> error_stack::Result<(), WindowProcessorError> {
        let Self::LinearHistogram {
            buckets,
            total,
            delayed_removals,
            ..
        } = self
        else {
            return Ok(());
        };
        while delayed_removals
            .front()
            .is_some_and(|removal| removal.expires_at <= now)
        {
            let removal = delayed_removals.pop_front().verified(
                "the loop condition just observed a front entry and nothing else pops the queue",
            );
            let Some(count) = buckets.get_mut(removal.bucket) else {
                return Err(Report::new(
                    WindowProcessorError::DelayedRemovalBucketOutOfRange,
                ));
            };
            if *count == 0 {
                return Err(Report::new(
                    WindowProcessorError::MissingDelayedRemovedValue,
                ));
            }
            *count = count
                .checked_sub(1)
                .verified("the check above returned for a bucket that holds no value");
            *total = total.checked_sub(1).verified(
                "the bucket count checked above is non-zero, and the total sums every bucket",
            );
        }
        Ok(())
    }

    pub(super) fn next_deadline(&self) -> Option<Timestamp> {
        let Self::LinearHistogram {
            delayed_removals, ..
        } = self
        else {
            return None;
        };
        delayed_removals.front().map(|removal| removal.expires_at)
    }

    pub(super) fn add(
        &mut self,
        _demand: &WindowAggregateDemand,
        timestamp: Timestamp,
        sequence: u64,
        value: Option<RuntimeValue>,
    ) -> error_stack::Result<(), WindowProcessorError> {
        self.purge_expired(timestamp)?;
        match self {
            Self::Counter { count } => {
                *count = count
                    .checked_add(1)
                    .assured("a window cannot admit 2^64 rows before they expire");
                Ok(())
            }
            Self::Sequence { values } => {
                let value = value
                    .ok_or_else(|| Report::new(WindowProcessorError::SequenceRequiresValue))?;
                values.push_back(WindowSequenceValue {
                    timestamp,
                    sequence,
                    value,
                });
                Ok(())
            }
            Self::SortedMap { counts } => {
                let value =
                    value.ok_or_else(|| Report::new(WindowProcessorError::OrderedRequiresValue))?;
                *counts.entry(RuntimeValueSortKey(value)).or_insert(0) += 1;
                Ok(())
            }
            Self::LinearHistogram {
                buckets,
                total,
                min,
                max,
                width,
                delay: _,
                delayed_removals: _,
            } => {
                let value = value
                    .ok_or_else(|| Report::new(WindowProcessorError::HistogramRequiresValue))?;
                let value = runtime_value_to_f64(&value)?;
                let bucket = linear_histogram_bucket(value, *min, *max, *width, buckets.len())?;
                buckets[bucket] = buckets[bucket]
                    .checked_add(1)
                    .assured("a window cannot admit 2^64 rows before they expire");
                *total = total
                    .checked_add(1)
                    .assured("a window cannot admit 2^64 rows before they expire");
                Ok(())
            }
            Self::Sum { total } => {
                let value =
                    value.ok_or_else(|| Report::new(WindowProcessorError::SumRequiresValue))?;
                *total = Some(match total.take() {
                    Some(current) => sum_runtime_values(current, value)?,
                    None => value,
                });
                Ok(())
            }
        }
    }

    pub(super) fn remove(
        &mut self,
        _demand: &WindowAggregateDemand,
        removal_time: Timestamp,
        timestamp: Timestamp,
        sequence: u64,
        value: Option<RuntimeValue>,
    ) -> error_stack::Result<(), WindowProcessorError> {
        self.purge_expired(removal_time)?;
        match self {
            Self::Counter { count } => {
                *count = count
                    .checked_sub(1)
                    .verified("a row is only removed from the window that admitted it");
                Ok(())
            }
            Self::Sequence { values } => {
                let Some(index) = values
                    .iter()
                    .position(|entry| entry.timestamp == timestamp && entry.sequence == sequence)
                else {
                    return Err(Report::new(WindowProcessorError::MissingSequenceEntry));
                };
                values.remove(index);
                Ok(())
            }
            Self::SortedMap { counts } => {
                let value =
                    value.ok_or_else(|| Report::new(WindowProcessorError::OrderedRequiresValue))?;
                decrement_runtime_value_count(counts, value)
            }
            Self::LinearHistogram {
                buckets,
                total,
                min,
                max,
                width,
                delay,
                delayed_removals,
            } => {
                let value = value
                    .ok_or_else(|| Report::new(WindowProcessorError::HistogramRequiresValue))?;
                let value = runtime_value_to_f64(&value)?;
                let bucket = linear_histogram_bucket(value, *min, *max, *width, buckets.len())?;
                if delay.is_zero() {
                    let Some(count) = buckets.get_mut(bucket) else {
                        return Err(Report::new(WindowProcessorError::BucketOutOfRange));
                    };
                    if *count == 0 {
                        return Err(Report::new(WindowProcessorError::MissingRemovedValue));
                    }
                    *count = count
                        .checked_sub(1)
                        .verified("the check above returned for a bucket that holds no value");
                    *total = total.checked_sub(1).verified(
                        "the bucket count checked above is non-zero, and the total sums every \
                         bucket",
                    );
                    return Ok(());
                }
                delayed_removals.push_back(LinearHistogramDelayedRemoval {
                    expires_at: checked_add_duration_to_timestamp(removal_time, *delay),
                    bucket,
                });
                Ok(())
            }
            Self::Sum { total } => {
                let value =
                    value.ok_or_else(|| Report::new(WindowProcessorError::SumRequiresValue))?;
                *total = match total.take() {
                    Some(current) => subtract_runtime_values(current, value)?,
                    None => None,
                };
                Ok(())
            }
        }
    }

    pub(super) fn evaluate(
        &self,
        function: WindowAggregateFunction,
        percentile: Option<f64>,
    ) -> error_stack::Result<RuntimeValue, WindowProcessorError> {
        match (function, self) {
            (WindowAggregateFunction::Count, Self::Counter { count }) => {
                Ok(RuntimeValue::I64(i64::try_from(*count).assured(
                    "a window counter cannot exceed the allocation limit of its retained entries",
                )))
            }
            (WindowAggregateFunction::First, Self::Sequence { values }) => {
                match values
                    .iter()
                    .min_by_key(|entry| (entry.timestamp, entry.sequence))
                {
                    Some(entry) => Ok(entry.value.clone()),
                    None => Err(Report::new(WindowProcessorError::FirstRequiresWindow)),
                }
            }
            (WindowAggregateFunction::Last, Self::Sequence { values }) => {
                match values
                    .iter()
                    .max_by_key(|entry| (entry.timestamp, entry.sequence))
                {
                    Some(entry) => Ok(entry.value.clone()),
                    None => Err(Report::new(WindowProcessorError::LastRequiresWindow)),
                }
            }
            (WindowAggregateFunction::Max, Self::SortedMap { counts }) => {
                match counts.last_key_value() {
                    Some((value, _)) => Ok(value.0.clone()),
                    None => Err(Report::new(WindowProcessorError::MaxRequiresWindow)),
                }
            }
            (WindowAggregateFunction::Min, Self::SortedMap { counts }) => {
                match counts.first_key_value() {
                    Some((value, _)) => Ok(value.0.clone()),
                    None => Err(Report::new(WindowProcessorError::MinRequiresWindow)),
                }
            }
            (
                WindowAggregateFunction::PercentileLinearHistogram,
                Self::LinearHistogram {
                    buckets,
                    total,
                    min,
                    max,
                    width,
                    ..
                },
            ) => {
                let percentile = percentile.ok_or_else(|| {
                    Report::new(WindowProcessorError::HistogramRequiresPercentile)
                })?;
                percentile_from_linear_histogram(buckets, *total, *min, *max, *width, percentile)
            }
            (WindowAggregateFunction::Sum, Self::Sum { total }) => total
                .clone()
                .ok_or_else(|| Report::new(WindowProcessorError::SumRequiresWindow)),
            _ => Err(Report::new(WindowProcessorError::IncompatibleAccumulator {
                function,
            })),
        }
    }
}

impl WindowProcessorState {
    pub(super) fn new(program: &WindowAggregateProgram) -> Self {
        let accumulators = program
            .demands()
            .iter()
            .map(WindowAggregateAccumulator::new)
            .collect();
        Self {
            entries: VecDeque::new(),
            next_sequence: 0,
            accumulators,
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
            entries.push(WindowEntrySnapshot {
                sequence: entry.sequence,
                timestamp: entry.timestamp,
                key: BranchKey::to_remote_key(&entry.message.key),
                record,
                aggregate_inputs: entry
                    .aggregate_inputs
                    .iter()
                    .map(|input| input.value.as_ref().map(RuntimeValue::to_remote))
                    .collect(),
            });
        }
        Ok(WindowProcessorStateSnapshot {
            entries,
            next_sequence: self.next_sequence,
            accumulators: self
                .accumulators
                .iter()
                .map(WindowAggregateAccumulator::to_snapshot)
                .collect(),
        })
    }

    /// Rebuild a branch's live window from a published snapshot, which stays shared with every
    /// other reader and is only read here.
    pub(super) fn from_snapshot(
        program: &WindowAggregateProgram,
        input_schema: &CompiledSchema,
        snapshot: &WindowProcessorStateSnapshot,
    ) -> error_stack::Result<Self, WindowProcessorError> {
        if snapshot.accumulators.len() != program.demands().len() {
            return Err(Report::new(WindowProcessorError::SnapshotDemandCount {
                accumulators: snapshot.accumulators.len(),
                demands: program.demands().len(),
            }));
        }
        let mut entries = VecDeque::with_capacity(snapshot.entries.len());
        for entry in &snapshot.entries {
            let key = BranchKey::from_remote_key(entry.key.clone()).map_err(|reason| {
                Report::new(WindowProcessorError::RestoreSnapshotBranchKey { reason })
            })?;
            let record = input_schema
                .runtime_row_from_remote(&entry.record)
                .change_context(WindowProcessorError::RestoreSnapshotEntry)?;
            entries.push_back(WindowEntry {
                sequence: entry.sequence,
                timestamp: entry.timestamp,
                message: RelayMessage {
                    key,
                    record,
                    acks: AckSet::empty(),
                },
                aggregate_inputs: entry
                    .aggregate_inputs
                    .iter()
                    .map(|value| WindowAggregateInput {
                        value: value.clone().map(RuntimeValue::from_remote),
                    })
                    .collect(),
            });
        }
        Ok(Self {
            entries,
            next_sequence: snapshot.next_sequence,
            accumulators: snapshot
                .accumulators
                .iter()
                .map(WindowAggregateAccumulator::from_snapshot)
                .collect(),
        })
    }

    pub(super) fn push_message(
        &mut self,
        program: &WindowAggregateProgram,
        timestamp: Timestamp,
        message: RelayMessage,
        inputs: Vec<WindowAggregateInput>,
    ) -> Result<(), Box<WindowPushFailure>> {
        let sequence = self.next_sequence;
        if let Err(error) = self.apply_aggregate_inputs(
            program.demands(),
            timestamp,
            sequence,
            &inputs,
            WindowAccumulatorAction::Add,
        ) {
            return Err(Box::new(WindowPushFailure { error, message }));
        }
        self.entries.push_back(WindowEntry {
            sequence,
            timestamp,
            message,
            aggregate_inputs: inputs,
        });
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .assured("a window cannot admit 2^64 rows in one branch");
        Ok(())
    }

    pub(super) fn clear(&mut self, program: &WindowAggregateProgram) {
        self.entries.clear();
        self.accumulators = program
            .demands()
            .iter()
            .map(WindowAggregateAccumulator::new)
            .collect();
    }

    pub(super) fn purge_timeouts(
        &mut self,
        now: Timestamp,
    ) -> error_stack::Result<bool, WindowProcessorError> {
        let mut changed = false;
        for accumulator in &mut self.accumulators {
            if accumulator
                .next_deadline()
                .is_some_and(|deadline| deadline <= now)
            {
                accumulator.purge_expired(now)?;
                changed = true;
            }
        }
        Ok(changed)
    }

    pub(super) fn next_timeout_deadline(&self) -> Option<Timestamp> {
        self.accumulators
            .iter()
            .filter_map(WindowAggregateAccumulator::next_deadline)
            .min()
    }

    pub(super) fn pop_front_entry(
        &mut self,
        program: &WindowAggregateProgram,
        removal_time: Timestamp,
    ) -> error_stack::Result<Option<WindowEntry>, WindowProcessorError> {
        let Some(entry) = self.entries.pop_front() else {
            return Ok(None);
        };
        self.apply_aggregate_inputs(
            program.demands(),
            entry.timestamp,
            entry.sequence,
            &entry.aggregate_inputs,
            WindowAccumulatorAction::Remove { at: removal_time },
        )?;
        Ok(Some(entry))
    }

    pub(super) fn apply_aggregate_inputs(
        &mut self,
        demands: &[WindowAggregateDemand],
        timestamp: Timestamp,
        sequence: u64,
        inputs: &[WindowAggregateInput],
        action: WindowAccumulatorAction,
    ) -> error_stack::Result<(), WindowProcessorError> {
        if inputs.len() != self.accumulators.len() {
            return Err(Report::new(WindowProcessorError::AggregateInputCount {
                inputs: inputs.len(),
                accumulators: self.accumulators.len(),
            }));
        }
        for ((input, accumulator), demand) in inputs.iter().zip(&mut self.accumulators).zip(demands)
        {
            match action {
                WindowAccumulatorAction::Add => {
                    accumulator.add(demand, timestamp, sequence, input.value.clone())?
                }
                WindowAccumulatorAction::Remove { at } => {
                    accumulator.remove(demand, at, timestamp, sequence, input.value.clone())?
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(super) struct WindowAggregateInput {
    pub(super) value: Option<RuntimeValue>,
}

#[cfg(test)]
pub(super) static WINDOW_AGGREGATE_INPUT_VM_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

pub(super) async fn evaluate_window_aggregate_inputs(
    program: &CompiledWindowAggregateProgram,
    carrier: &RuntimeRecordBatch,
    execution_now: Timestamp,
) -> error_stack::Result<
    Vec<error_stack::Result<Vec<WindowAggregateInput>, WindowProcessorError>>,
    WindowProcessorError,
> {
    let row_count = carrier.batch().num_rows();
    let keys = vec![None; row_count];
    let side_inputs = HashMap::new();
    let lookup_columns = HashMap::new();
    let uninitialized = VmUninitializedInput {
        fields: program
            .input_program
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("window_input."))
            .map(|field| field.name().clone())
            .collect(),
    };
    let input = project_vm_input_batch(
        &program.input_program.input_schema,
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
    WINDOW_AGGREGATE_INPUT_VM_EXECUTIONS.fetch_add(1, Ordering::Relaxed);
    let result = execute_program_with_selection_in_context(
        &program.input_program,
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
    let mut input_columns = Vec::with_capacity(program.input_fields.len());
    for field_name in &program.input_fields {
        let Some(field_name) = field_name else {
            input_columns.push(None);
            continue;
        };
        let column_index = result.batch.schema().index_of(field_name).map_err(|_| {
            Report::new(WindowProcessorError::AggregateInputFieldMissing {
                field: field_name.clone(),
            })
        })?;
        let array = result.batch.column(column_index).to_array_ref();
        let column =
            RuntimeValueColumn::new(field_name.as_str(), array).change_context_lazy(|| {
                WindowProcessorError::AggregateInputColumn {
                    field: field_name.clone(),
                }
            })?;
        input_columns.push(Some(column));
    }
    let mut rows = Vec::with_capacity(row_count);
    for row in 0..row_count {
        if let Some(error) = result.batch.errors().row(row).first() {
            rows.push(Err(Report::new(WindowProcessorError::AggregateInputRow {
                code: error.code,
                message: error.message.clone(),
            })));
            continue;
        }
        let mut inputs = Vec::with_capacity(input_columns.len());
        let mut row_failure = None;
        for (field_name, column) in program.input_fields.iter().zip(&input_columns) {
            let (Some(field_name), Some(column)) = (field_name, column) else {
                inputs.push(WindowAggregateInput { value: None });
                continue;
            };
            match column.nullable_value_at(row) {
                Ok(value) => inputs.push(WindowAggregateInput { value }),
                Err(error) => {
                    row_failure = Some(error.change_context(
                        WindowProcessorError::AggregateInputColumn {
                            field: field_name.clone(),
                        },
                    ));
                    break;
                }
            }
        }
        match row_failure {
            Some(failure) => rows.push(Err(failure)),
            None => rows.push(Ok(inputs)),
        }
    }
    Ok(rows)
}

#[derive(Debug, Clone, Copy)]
pub(super) enum WindowAccumulatorAction {
    Add,
    Remove { at: Timestamp },
}

pub(super) fn decrement_runtime_value_count(
    counts: &mut BTreeMap<RuntimeValueSortKey, usize>,
    value: RuntimeValue,
) -> error_stack::Result<(), WindowProcessorError> {
    let key = RuntimeValueSortKey(value);
    let Some(count) = counts.get_mut(&key) else {
        return Err(Report::new(WindowProcessorError::MissingSortedValue));
    };
    *count = count
        .checked_sub(1)
        .verified("the map drops an entry when its count reaches zero");
    if *count == 0 {
        counts.remove(&key);
    }
    Ok(())
}

pub(super) fn linear_histogram_bucket(
    value: f64,
    min: f64,
    max: f64,
    width: f64,
    bucket_count: usize,
) -> error_stack::Result<usize, WindowProcessorError> {
    if !value.is_finite() {
        return Err(Report::new(WindowProcessorError::HistogramRequiresFinite));
    }
    if bucket_count == 0 {
        return Err(Report::new(WindowProcessorError::HistogramRequiresBucket));
    }
    if value <= min {
        return Ok(0);
    }
    if value >= max {
        return Ok(bucket_count - 1);
    }
    ((value - min) / width)
        .floor()
        .checked_approx_into()
        .ok_or_else(|| Report::new(WindowProcessorError::HistogramValueOutOfRange { value }))
}

pub(super) fn percentile_from_linear_histogram(
    buckets: &[usize],
    total: usize,
    min: f64,
    max: f64,
    width: f64,
    percentile: f64,
) -> error_stack::Result<RuntimeValue, WindowProcessorError> {
    if total == 0 {
        return Err(Report::new(WindowProcessorError::HistogramRequiresWindow));
    }
    let rank: usize = ((percentile / 100.0) * (total - 1).approx_into::<f64>())
        .round()
        .checked_approx_into()
        .ok_or_else(|| {
            Report::new(WindowProcessorError::HistogramPercentileRank { percentile, total })
        })?;
    let mut seen = 0usize;
    for (index, count) in buckets.iter().enumerate() {
        seen += *count;
        if seen > rank {
            let midpoint = min + (index.approx_into::<f64>() + 0.5) * width;
            return Ok(RuntimeValue::F64(OrderedFloat(midpoint.clamp(min, max))));
        }
    }
    Err(Report::new(WindowProcessorError::HistogramEmpty))
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
        && timestamp_elapsed(first.timestamp, now) >= width_duration
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
            first.timestamp,
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

pub(super) fn advance_window(
    state: &mut WindowProcessorState,
    program: &WindowAggregateProgram,
    step_messages: Option<usize>,
    step_duration: Option<Duration>,
    removal_time: Timestamp,
) -> error_stack::Result<(), WindowProcessorError> {
    let remove_messages = step_messages.unwrap_or(0).min(state.entries.len());
    for _ in 0..remove_messages {
        if let Some(entry) = state.pop_front_entry(program, removal_time)? {
            entry.message.acks.ack_success();
        }
    }
    if let Some(step_duration) = step_duration
        && let Some(first) = state.entries.front()
    {
        let cutoff = checked_add_duration_to_timestamp(first.timestamp, step_duration);
        while state
            .entries
            .front()
            .is_some_and(|entry| entry.timestamp < cutoff)
        {
            if let Some(entry) = state.pop_front_entry(program, removal_time)? {
                entry.message.acks.ack_success();
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct WindowAggregateFunctionInjector {
    pub(super) accumulators: Vec<WindowAggregateAccumulator>,
    pub(super) demand_types: Vec<ArrowDataType>,
    pub(super) demand_offset: usize,
}

impl VmFunctionInjector for WindowAggregateFunctionInjector {
    fn inject_with_context(
        &self,
        function: &FunctionName,
        _arguments: &[VmTypedArray],
        row_count: usize,
        _span: nervix_vm::program::Span,
        _now: Timestamp,
        _prior_error_rows: nervix_vm::RowErrorMask<'_>,
    ) -> Result<nervix_vm::InjectedResult, nervix_vm::RuntimeError> {
        let FunctionName::WindowAggregate(invocation) = function else {
            return Err(nervix_vm::RuntimeError::InvalidBatch {
                message: format!("function '{}' is not a window aggregate", function.as_str()),
            });
        };
        let accumulator_id = self
            .demand_offset
            .checked_add(invocation.demand_id)
            .assured("both index into the accumulators this program already holds in memory");
        let accumulator = self.accumulators.get(accumulator_id).ok_or_else(|| {
            nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "window aggregate is missing accumulator for route demand {} (shared demand \
                     {})",
                    invocation.demand_id, accumulator_id
                ),
            }
        })?;
        let value = accumulator
            .evaluate(invocation.function, invocation.percentile)
            .map_err(|error| nervix_vm::RuntimeError::InvalidBatch {
                message: format!("{error:#}"),
            })?;
        let data_type = self.demand_types.get(invocation.demand_id).ok_or_else(|| {
            nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "window aggregate is missing output type for demand {}",
                    invocation.demand_id
                ),
            }
        })?;
        let array =
            runtime_value_arrow_array(data_type, Some(&value), row_count).map_err(|message| {
                nervix_vm::RuntimeError::InvalidBatch {
                    message: message.to_string(),
                }
            })?;
        let output = VmTypedArray::try_from_array_ref(array).map_err(|error| {
            nervix_vm::RuntimeError::InvalidBatch {
                message: error.to_string(),
            }
        })?;
        Ok(nervix_vm::InjectedResult::success(output))
    }
}

pub(super) async fn evaluate_window_aggregate(
    program: &CompiledWindowAggregateProgram,
    state: &WindowProcessorState,
    output_schema: &CompiledSchema,
    execution_now: Timestamp,
) -> error_stack::Result<RuntimeRecordBatch, WindowProcessorError> {
    let injector: Arc<Box<dyn VmFunctionInjector>> =
        Arc::new(Box::new(WindowAggregateFunctionInjector {
            accumulators: state.accumulators.clone(),
            demand_types: program.demand_types.clone(),
            demand_offset: program.demand_offset,
        }));
    let mut columns = Vec::with_capacity(output_schema.arrow_schema().fields().len());
    for field in output_schema.arrow_schema().fields() {
        let value = if let Some(assignment) = program
            .assignments
            .iter()
            .find(|assignment| assignment.target.field == *field.name())
        {
            Some(
                evaluate_window_aggregate_expr(
                    &assignment.value,
                    &assignment.target.field,
                    injector.clone(),
                    execution_now,
                )
                .await?,
            )
        } else if field.is_nullable() {
            None
        } else {
            return Err(Report::new(
                WindowProcessorError::UninitializedOutputField {
                    field: field.name().clone(),
                },
            ));
        };
        columns.push(
            runtime_value_arrow_array(field.data_type(), value.as_ref(), 1)
                .change_context(WindowProcessorError::BuildAggregateOutput)?,
        );
    }
    let batch = RecordBatch::try_new(output_schema.arrow_schema(), columns)
        .change_context(WindowProcessorError::BuildAggregateOutput)?;
    RuntimeRecordBatch::from_record_batch(output_schema.arrow_schema(), batch)
        .change_context(WindowProcessorError::BuildAggregateOutput)
}

pub(super) fn evaluate_window_aggregate_expr<'a>(
    expr: &'a CompiledWindowAggregateExpr,
    target_field: &'a str,
    injector: Arc<Box<dyn VmFunctionInjector>>,
    execution_now: Timestamp,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = error_stack::Result<RuntimeValue, WindowProcessorError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        match expr {
            CompiledWindowAggregateExpr::Scalar(program) => {
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
                let field = result.batch.schema().field(column_index);
                let array = result.batch.column(column_index).to_array_ref();
                let output_type =
                    parse_as_type_from_arrow(field.data_type()).change_context_lazy(|| {
                        WindowProcessorError::AggregateExprOutput {
                            field: target_field.to_string(),
                        }
                    })?;
                let value = runtime_value_from_arrow_array(
                    array.as_ref(),
                    &output_type,
                    false,
                    0,
                    target_field,
                )
                .change_context_lazy(|| {
                    WindowProcessorError::AggregateExprOutput {
                        field: target_field.to_string(),
                    }
                })?;
                value.ok_or_else(|| {
                    Report::new(WindowProcessorError::AggregateExprNullOutput {
                        field: target_field.to_string(),
                    })
                })
            }
            CompiledWindowAggregateExpr::Array { items, fixed_size } => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(
                        evaluate_window_aggregate_expr(
                            item,
                            target_field,
                            injector.clone(),
                            execution_now,
                        )
                        .await?,
                    );
                }
                if *fixed_size {
                    Ok(RuntimeValue::Array(values))
                } else {
                    Ok(RuntimeValue::Vec(values))
                }
            }
        }
    })
}

pub(super) fn runtime_value_to_f64(
    value: &RuntimeValue,
) -> error_stack::Result<f64, WindowProcessorError> {
    match value {
        RuntimeValue::U8(value) => Ok(f64::from(*value)),
        RuntimeValue::I8(value) => Ok(f64::from(*value)),
        RuntimeValue::U16(value) => Ok(f64::from(*value)),
        RuntimeValue::I16(value) => Ok(f64::from(*value)),
        RuntimeValue::U32(value) => Ok(f64::from(*value)),
        RuntimeValue::I32(value) => Ok(f64::from(*value)),
        RuntimeValue::U64(value) => Ok((*value).approx_into()),
        RuntimeValue::I64(value) => Ok((*value).approx_into()),
        RuntimeValue::F32(value) => Ok(f64::from(value.0)),
        RuntimeValue::F64(value) => Ok(value.0),
        other => Err(Report::new(WindowProcessorError::NotNumeric {
            type_name: runtime_value_type_name(other),
        })),
    }
}

pub(super) fn sum_runtime_values(
    left: RuntimeValue,
    right: RuntimeValue,
) -> error_stack::Result<RuntimeValue, WindowProcessorError> {
    match (left, right) {
        (RuntimeValue::U8(left), RuntimeValue::U8(right)) => Ok(RuntimeValue::U8(left + right)),
        (RuntimeValue::I8(left), RuntimeValue::I8(right)) => Ok(RuntimeValue::I8(left + right)),
        (RuntimeValue::U16(left), RuntimeValue::U16(right)) => Ok(RuntimeValue::U16(left + right)),
        (RuntimeValue::I16(left), RuntimeValue::I16(right)) => Ok(RuntimeValue::I16(left + right)),
        (RuntimeValue::U32(left), RuntimeValue::U32(right)) => Ok(RuntimeValue::U32(left + right)),
        (RuntimeValue::I32(left), RuntimeValue::I32(right)) => Ok(RuntimeValue::I32(left + right)),
        (RuntimeValue::U64(left), RuntimeValue::U64(right)) => Ok(RuntimeValue::U64(left + right)),
        (RuntimeValue::I64(left), RuntimeValue::I64(right)) => Ok(RuntimeValue::I64(left + right)),
        (RuntimeValue::F32(left), RuntimeValue::F32(right)) => {
            Ok(RuntimeValue::F32(OrderedFloat(left.0 + right.0)))
        }
        (RuntimeValue::F64(left), RuntimeValue::F64(right)) => {
            Ok(RuntimeValue::F64(OrderedFloat(left.0 + right.0)))
        }
        (left, right) => Err(Report::new(WindowProcessorError::SumIncompatible {
            left: runtime_value_type_name(&left),
            right: runtime_value_type_name(&right),
        })),
    }
}

pub(super) fn subtract_runtime_values(
    left: RuntimeValue,
    right: RuntimeValue,
) -> error_stack::Result<Option<RuntimeValue>, WindowProcessorError> {
    let value = match (left, right) {
        (RuntimeValue::U8(left), RuntimeValue::U8(right)) => RuntimeValue::U8(left - right),
        (RuntimeValue::I8(left), RuntimeValue::I8(right)) => RuntimeValue::I8(left - right),
        (RuntimeValue::U16(left), RuntimeValue::U16(right)) => RuntimeValue::U16(left - right),
        (RuntimeValue::I16(left), RuntimeValue::I16(right)) => RuntimeValue::I16(left - right),
        (RuntimeValue::U32(left), RuntimeValue::U32(right)) => RuntimeValue::U32(left - right),
        (RuntimeValue::I32(left), RuntimeValue::I32(right)) => RuntimeValue::I32(left - right),
        (RuntimeValue::U64(left), RuntimeValue::U64(right)) => RuntimeValue::U64(left - right),
        (RuntimeValue::I64(left), RuntimeValue::I64(right)) => RuntimeValue::I64(left - right),
        (RuntimeValue::F32(left), RuntimeValue::F32(right)) => {
            RuntimeValue::F32(OrderedFloat(left.0 - right.0))
        }
        (RuntimeValue::F64(left), RuntimeValue::F64(right)) => {
            RuntimeValue::F64(OrderedFloat(left.0 - right.0))
        }
        (left, right) => {
            return Err(Report::new(WindowProcessorError::SumRemoveIncompatible {
                left: runtime_value_type_name(&left),
                right: runtime_value_type_name(&right),
            }));
        }
    };
    if runtime_value_is_zero(&value) {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

pub(super) fn runtime_value_is_zero(value: &RuntimeValue) -> bool {
    match value {
        RuntimeValue::U8(value) => *value == 0,
        RuntimeValue::I8(value) => *value == 0,
        RuntimeValue::U16(value) => *value == 0,
        RuntimeValue::I16(value) => *value == 0,
        RuntimeValue::U32(value) => *value == 0,
        RuntimeValue::I32(value) => *value == 0,
        RuntimeValue::U64(value) => *value == 0,
        RuntimeValue::I64(value) => *value == 0,
        RuntimeValue::F32(value) => value.0 == 0.0,
        RuntimeValue::F64(value) => value.0 == 0.0,
        _ => false,
    }
}

pub(super) fn compare_runtime_values(
    left: &RuntimeValue,
    right: &RuntimeValue,
) -> std::cmp::Ordering {
    match (left, right) {
        (RuntimeValue::U8(left), RuntimeValue::U8(right)) => left.cmp(right),
        (RuntimeValue::I8(left), RuntimeValue::I8(right)) => left.cmp(right),
        (RuntimeValue::U16(left), RuntimeValue::U16(right)) => left.cmp(right),
        (RuntimeValue::I16(left), RuntimeValue::I16(right)) => left.cmp(right),
        (RuntimeValue::U32(left), RuntimeValue::U32(right)) => left.cmp(right),
        (RuntimeValue::I32(left), RuntimeValue::I32(right)) => left.cmp(right),
        (RuntimeValue::U64(left), RuntimeValue::U64(right)) => left.cmp(right),
        (RuntimeValue::I64(left), RuntimeValue::I64(right)) => left.cmp(right),
        (RuntimeValue::F32(left), RuntimeValue::F32(right)) => left.cmp(right),
        (RuntimeValue::F64(left), RuntimeValue::F64(right)) => left.cmp(right),
        (RuntimeValue::String(left), RuntimeValue::String(right)) => left.cmp(right),
        (RuntimeValue::Datetime(left), RuntimeValue::Datetime(right)) => left.cmp(right),
        (RuntimeValue::Bool(left), RuntimeValue::Bool(right)) => left.cmp(right),
        _ => left.to_key_fragment().cmp(&right.to_key_fragment()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use nervix_models::{CreateSchema, ParseAsType, Timestamp};
    use nonzero_ext::nonzero;
    use ordered_float::OrderedFloat;

    use super::*;
    use crate::{
        runtime_ack::AckSet,
        runtime_schema::{
            RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeValue, compile_schema,
            test_runtime_row,
        },
    };
    #[tokio::test]
    async fn window_aggregate_evaluator_computes_vm_expression_percentile_and_array() {
        let output_schema = compile_schema(&CreateSchema {
            name: named("summary"),
            fields: vec![
                nervix_models::SchemaField {
                    name: named("count"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("adjusted_count"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("p50"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("latencies"),
                    ty: ParseAsType::Array {
                        element: Box::new(ParseAsType::F64),
                        len: nonzero!(2u32),
                    },
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("observed_at"),
                    ty: ParseAsType::Datetime,
                    optional: false,
                    sensitive: false,
                },
            ],
        });
        let aggregate = window_aggregate(
            "SET count = COUNT(input.latency), adjusted_count = COUNT(input.latency) + 2, p50 = \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), latencies = \
             [PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 100, 10, 0, 100, '2s')], observed_at = \
             now()",
        );
        let mut state = WindowProcessorState::new(&aggregate);
        for value in [10.0, 20.0, 30.0] {
            state
                .push_message(
                    &aggregate,
                    Timestamp::now(),
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([(
                            "latency".to_string(),
                            RuntimeValue::F64(OrderedFloat(value)),
                        )]),
                        acks: AckSet::empty(),
                    },
                    window_inputs(&aggregate, RuntimeValue::F64(OrderedFloat(value))),
                )
                .expect("aggregate state should accept message");
        }

        let compiled =
            compile_window_aggregate_for_test(&aggregate, ParseAsType::F64, &output_schema);
        let execution_now = Timestamp::from_unix_nanos(946_684_800_000_000_000);
        let record = evaluate_window_aggregate(&compiled, &state, &output_schema, execution_now)
            .await
            .expect("aggregate should evaluate");

        assert_eq!(batch_value(&record, "count"), Some(RuntimeValue::I64(3)));
        assert_eq!(
            batch_value(&record, "adjusted_count"),
            Some(RuntimeValue::I64(5))
        );
        assert_eq!(
            batch_value(&record, "p50"),
            Some(RuntimeValue::F64(OrderedFloat(25.0)))
        );
        assert_eq!(
            batch_value(&record, "latencies"),
            Some(RuntimeValue::Array(vec![
                RuntimeValue::F64(OrderedFloat(25.0)),
                RuntimeValue::F64(OrderedFloat(35.0)),
            ]))
        );
        assert_eq!(
            batch_value(&record, "observed_at"),
            Some(RuntimeValue::Datetime(
                execution_now.as_datetime().fixed_offset()
            ))
        );
    }

    #[tokio::test]
    async fn window_aggregate_inputs_evaluate_one_batch_in_one_vm_execution() {
        let output_schema = test_schema(&[("adjusted_total", ParseAsType::I64)]);
        let aggregate = window_aggregate("SET adjusted_total = SUM(120 / input.latency)");
        let compiled =
            compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
        let rows = [10, 0, 30]
            .into_iter()
            .map(|latency| test_runtime_row([("latency".to_string(), RuntimeValue::I64(latency))]))
            .collect::<Vec<_>>();
        let carrier = RuntimeRecordBatch::from_rows(rows[0].batch().schema(), rows.iter())
            .expect("window input rows should form one Arrow batch");
        WINDOW_AGGREGATE_INPUT_VM_EXECUTIONS.store(0, Ordering::Relaxed);

        let evaluated =
            evaluate_window_aggregate_inputs(&compiled, &carrier, Timestamp::from_unix_nanos(1))
                .await
                .expect("batched window aggregate inputs should evaluate");

        assert_eq!(
            WINDOW_AGGREGATE_INPUT_VM_EXECUTIONS.load(Ordering::Relaxed),
            1,
            "all input rows must share one aggregate-input VM execution"
        );
        assert_eq!(evaluated.len(), 3);
        let first = evaluated[0]
            .as_ref()
            .expect("the first window input row should evaluate");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].value, Some(RuntimeValue::I64(12)));
        let row_error = evaluated[1]
            .as_ref()
            .expect_err("division by zero should fail only its input row");
        assert!(
            matches!(
                row_error.current_context(),
                WindowProcessorError::AggregateInputRow { code, .. }
                    if code.as_str() == "division_by_zero"
            ),
            "the failed row should carry the division_by_zero side error, got {row_error:?}"
        );
        let third = evaluated[2]
            .as_ref()
            .expect("the third window input row should evaluate");
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].value, Some(RuntimeValue::I64(4)));
    }

    #[tokio::test]
    async fn window_linear_histogram_percentiles_share_accumulator_by_config() {
        let output_schema = compile_schema(&CreateSchema {
            name: named("summary"),
            fields: vec![
                nervix_models::SchemaField {
                    name: named("p50"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("p90"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("p50_other_range"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
            ],
        });
        let aggregate = window_aggregate(
            "SET p50 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), p90 = \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 90, 10, 0, 100, '2s'), p50_other_range = \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 200, '2s')",
        );
        let mut state = WindowProcessorState::new(&aggregate);

        assert_eq!(
            state.accumulators.len(),
            2,
            "same input and histogram config should share one accumulator"
        );

        for value in [10, 20, 30] {
            state
                .push_message(
                    &aggregate,
                    Timestamp::now(),
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([(
                            "latency".to_string(),
                            RuntimeValue::I64(value),
                        )]),
                        acks: AckSet::empty(),
                    },
                    window_inputs(&aggregate, RuntimeValue::I64(value)),
                )
                .expect("aggregate state should accept message");
        }

        let compiled =
            compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
        let record = evaluate_window_aggregate(
            &compiled,
            &state,
            &output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("aggregate should evaluate");

        assert_eq!(
            batch_value(&record, "p50"),
            Some(RuntimeValue::F64(OrderedFloat(25.0)))
        );
        assert_eq!(
            batch_value(&record, "p90"),
            Some(RuntimeValue::F64(OrderedFloat(35.0)))
        );
        assert_eq!(
            batch_value(&record, "p50_other_range"),
            Some(RuntimeValue::F64(OrderedFloat(30.0)))
        );
    }

    #[test]
    fn window_advance_removes_step_messages() {
        let aggregate = window_aggregate("SET count = COUNT(input.latency)");
        let mut state = WindowProcessorState::new(&aggregate);
        for sequence in 0_i64..5 {
            state
                .push_message(
                    &aggregate,
                    Timestamp::now(),
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([(
                            "latency".to_string(),
                            RuntimeValue::I64(sequence),
                        )]),
                        acks: AckSet::empty(),
                    },
                    window_inputs(&aggregate, RuntimeValue::I64(sequence)),
                )
                .expect("aggregate state should accept message");
        }

        advance_window(&mut state, &aggregate, Some(2), None, Timestamp::now())
            .expect("window should advance");

        assert_eq!(state.entries.len(), 3);
        assert_eq!(state.entries.front().map(|entry| entry.sequence), Some(2));
        assert_eq!(
            state.accumulators[0]
                .evaluate(WindowAggregateFunction::Count, None)
                .expect("count should evaluate"),
            RuntimeValue::I64(3)
        );
    }

    #[tokio::test]
    async fn linear_histogram_zero_delay_removes_step_values_immediately() {
        let output_schema = compile_schema(&CreateSchema {
            name: named("summary"),
            fields: vec![nervix_models::SchemaField {
                name: named("p0"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            }],
        });
        let aggregate = window_aggregate(
            "SET p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '0ms')",
        );
        let mut state = WindowProcessorState::new(&aggregate);
        for (timestamp, value) in [
            (Timestamp::from_unix_nanos(0), 10),
            (Timestamp::from_unix_nanos(1_000_000_000), 90),
        ] {
            state
                .push_message(
                    &aggregate,
                    timestamp,
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([(
                            "latency".to_string(),
                            RuntimeValue::I64(value),
                        )]),
                        acks: AckSet::empty(),
                    },
                    window_inputs(&aggregate, RuntimeValue::I64(value)),
                )
                .expect("aggregate state should accept message");
        }

        advance_window(
            &mut state,
            &aggregate,
            Some(1),
            None,
            Timestamp::from_unix_nanos(1_000_000_000),
        )
        .expect("window should advance");
        let compiled =
            compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
        let record = evaluate_window_aggregate(
            &compiled,
            &state,
            &output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("aggregate should evaluate");

        assert_eq!(
            batch_value(&record, "p0"),
            Some(RuntimeValue::F64(OrderedFloat(95.0)))
        );
    }

    #[tokio::test]
    async fn linear_histogram_delay_retains_removed_step_values_until_expired() {
        let output_schema = compile_schema(&CreateSchema {
            name: named("summary"),
            fields: vec![nervix_models::SchemaField {
                name: named("p0"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            }],
        });
        let aggregate = window_aggregate(
            "SET p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '2s')",
        );
        let mut state = WindowProcessorState::new(&aggregate);
        for (timestamp, value) in [
            (Timestamp::from_unix_nanos(0), 10),
            (Timestamp::from_unix_nanos(1_000_000_000), 90),
        ] {
            state
                .push_message(
                    &aggregate,
                    timestamp,
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([(
                            "latency".to_string(),
                            RuntimeValue::I64(value),
                        )]),
                        acks: AckSet::empty(),
                    },
                    window_inputs(&aggregate, RuntimeValue::I64(value)),
                )
                .expect("aggregate state should accept message");
        }

        advance_window(
            &mut state,
            &aggregate,
            Some(1),
            None,
            Timestamp::from_unix_nanos(1_000_000_000),
        )
        .expect("window should advance");
        let compiled =
            compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
        let retained = evaluate_window_aggregate(
            &compiled,
            &state,
            &output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("aggregate should evaluate while delay retains value");
        assert_eq!(
            batch_value(&retained, "p0"),
            Some(RuntimeValue::F64(OrderedFloat(15.0)))
        );

        state
            .push_message(
                &aggregate,
                Timestamp::from_unix_nanos(2_000_000_000),
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(90))]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(90)),
            )
            .expect("aggregate state should accept message before delay expires");
        let still_retained = evaluate_window_aggregate(
            &compiled,
            &state,
            &output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("aggregate should evaluate before delay expires");
        assert_eq!(
            batch_value(&still_retained, "p0"),
            Some(RuntimeValue::F64(OrderedFloat(15.0)))
        );

        state
            .push_message(
                &aggregate,
                Timestamp::from_unix_nanos(4_000_000_000),
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(90))]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(90)),
            )
            .expect("aggregate state should accept message after delay expires");
        let expired = evaluate_window_aggregate(
            &compiled,
            &state,
            &output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("aggregate should evaluate after delay expires");
        assert_eq!(
            batch_value(&expired, "p0"),
            Some(RuntimeValue::F64(OrderedFloat(95.0)))
        );
    }

    #[tokio::test]
    async fn linear_histogram_delay_exposes_timeout_deadline_without_new_messages() {
        let output_schema = compile_schema(&CreateSchema {
            name: named("summary"),
            fields: vec![nervix_models::SchemaField {
                name: named("p0"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            }],
        });
        let aggregate = window_aggregate(
            "SET p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '2s')",
        );
        let mut state = WindowProcessorState::new(&aggregate);
        for (timestamp, value) in [
            (Timestamp::from_unix_nanos(0), 10),
            (Timestamp::from_unix_nanos(1_000_000_000), 90),
        ] {
            state
                .push_message(
                    &aggregate,
                    timestamp,
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([(
                            "latency".to_string(),
                            RuntimeValue::I64(value),
                        )]),
                        acks: AckSet::empty(),
                    },
                    window_inputs(&aggregate, RuntimeValue::I64(value)),
                )
                .expect("aggregate state should accept message");
        }

        advance_window(
            &mut state,
            &aggregate,
            Some(1),
            None,
            Timestamp::from_unix_nanos(1_000_000_000),
        )
        .expect("window should advance");
        assert_eq!(
            state.next_timeout_deadline(),
            Some(Timestamp::from_unix_nanos(3_000_000_000))
        );

        assert!(
            !state
                .purge_timeouts(Timestamp::from_unix_nanos(2_999_999_999))
                .expect("early purge check should succeed")
        );
        assert!(
            state
                .purge_timeouts(Timestamp::from_unix_nanos(3_000_000_000))
                .expect("due purge should succeed")
        );
        assert_eq!(state.next_timeout_deadline(), None);

        let compiled =
            compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
        let record = evaluate_window_aggregate(
            &compiled,
            &state,
            &output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("aggregate should evaluate after timeout purge");
        assert_eq!(
            batch_value(&record, "p0"),
            Some(RuntimeValue::F64(OrderedFloat(95.0)))
        );
    }

    #[tokio::test]
    async fn window_aggregate_state_updates_first_last_min_max_and_sum() {
        let output_schema = compile_schema(&CreateSchema {
            name: named("summary"),
            fields: vec![
                nervix_models::SchemaField {
                    name: named("first_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("last_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("min_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("max_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("total_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
            ],
        });
        let aggregate = window_aggregate(
            "SET first_latency = FIRST(input.latency), last_latency = LAST(input.latency), \
             min_latency = MIN(input.latency), max_latency = MAX(input.latency), total_latency = \
             SUM(input.latency)",
        );
        let mut state = WindowProcessorState::new(&aggregate);
        for value in [30, 10, 20] {
            state
                .push_message(
                    &aggregate,
                    Timestamp::now(),
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([(
                            "latency".to_string(),
                            RuntimeValue::I64(value),
                        )]),
                        acks: AckSet::empty(),
                    },
                    window_inputs(&aggregate, RuntimeValue::I64(value)),
                )
                .expect("aggregate state should accept message");
        }

        assert_eq!(
            state.accumulators.len(),
            3,
            "FIRST/LAST and MIN/MAX should each share one physical structure"
        );
        let compiled =
            compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
        let record = evaluate_window_aggregate(
            &compiled,
            &state,
            &output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("aggregate should evaluate");

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

        advance_window(&mut state, &aggregate, Some(1), None, Timestamp::now())
            .expect("window should advance");
        let record = evaluate_window_aggregate(
            &compiled,
            &state,
            &output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("aggregate should evaluate after removal");

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
            record: test_runtime_row([]).with_metadata(
                RuntimeRecordMetadata::from_ingested_at_watermarks(
                    Timestamp::from_unix_nanos(10),
                    Timestamp::from_unix_nanos(20),
                ),
            ),
            acks: AckSet::empty(),
        };

        let timestamp = message_timestamp(&message);

        assert_eq!(timestamp, Timestamp::from_unix_nanos(10));
    }

    #[test]
    fn window_output_metadata_uses_window_low_and_emit_high_watermark() {
        let aggregate = window_aggregate("SET count = COUNT(input.latency)");
        let mut state = WindowProcessorState::new(&aggregate);
        for timestamp in [
            Timestamp::from_unix_nanos(30),
            Timestamp::from_unix_nanos(10),
            Timestamp::from_unix_nanos(20),
        ] {
            state
                .push_message(
                    &aggregate,
                    timestamp,
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([(
                            "latency".to_string(),
                            RuntimeValue::I64(timestamp.unix_nanos()),
                        )]),
                        acks: AckSet::empty(),
                    },
                    window_inputs(&aggregate, RuntimeValue::I64(timestamp.unix_nanos())),
                )
                .expect("aggregate state should accept message");
        }

        let metadata = window_output_metadata(&state, Timestamp::from_unix_nanos(40))
            .expect("non-empty window should emit metadata");

        assert_eq!(
            metadata.ingested_at_low_watermark(),
            Timestamp::from_unix_nanos(10)
        );
        assert_eq!(
            metadata.ingested_at_high_watermark(),
            Timestamp::from_unix_nanos(40)
        );
    }

    #[tokio::test]
    async fn window_processor_state_snapshot_roundtrips_entries_and_accumulators() {
        let output_schema = compile_schema(&CreateSchema {
            name: named("summary"),
            fields: vec![
                nervix_models::SchemaField {
                    name: named("count"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("first_latency"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("p50"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
            ],
        });
        let aggregate = window_aggregate(
            "SET count = COUNT(input.latency), first_latency = FIRST(input.latency), p50 = \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s')",
        );
        let mut state = WindowProcessorState::new(&aggregate);
        for (timestamp, value) in [
            (Timestamp::from_unix_nanos(10), 10),
            (Timestamp::from_unix_nanos(20), 30),
        ] {
            state
                .push_message(
                    &aggregate,
                    timestamp,
                    RelayMessage {
                        key: string_branch_key("tenant", "acme"),
                        record: test_runtime_row([(
                            "latency".to_string(),
                            RuntimeValue::I64(value),
                        )])
                        .with_metadata(
                            RuntimeRecordMetadata::from_ingested_at_watermarks(
                                timestamp, timestamp,
                            ),
                        ),
                        acks: AckSet::empty(),
                    },
                    window_inputs(&aggregate, RuntimeValue::I64(value)),
                )
                .expect("window should accept message");
        }

        let input_schema = test_schema(&[("latency", ParseAsType::I64)]);
        let snapshot = state.to_snapshot().expect("snapshot should encode");
        let restored =
            WindowProcessorState::from_snapshot(&aggregate, input_schema.as_ref(), &snapshot)
                .expect("snapshot should restore");
        let compiled =
            compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
        let record = evaluate_window_aggregate(
            &compiled,
            &restored,
            &output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("restored aggregate should evaluate");

        assert_eq!(restored.entries.len(), 2);
        assert_eq!(
            key_label(&restored.entries.front().unwrap().message.key),
            r#"{"tenant":"acme"}"#
        );
        assert_eq!(batch_value(&record, "count"), Some(RuntimeValue::I64(2)));
        assert_eq!(
            batch_value(&record, "first_latency"),
            Some(RuntimeValue::I64(10))
        );
        assert_eq!(
            batch_value(&record, "p50"),
            Some(RuntimeValue::F64(OrderedFloat(35.0)))
        );
    }
}
