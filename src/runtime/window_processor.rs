use super::*;

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

pub(super) fn current_window_emit_high_watermark(
    runtime: &Runtime,
    domain: &DomainName,
) -> Result<Timestamp, String> {
    runtime
        .current_stream_expiration_time(domain)?
        .ok_or_else(|| format!("domain '{}' has no current timestamp", domain.as_str()))
}

pub(super) fn window_output_metadata(
    state: &WindowProcessorState,
    emit_high_watermark: Timestamp,
) -> Result<RuntimeRecordMetadata, String> {
    let low = state
        .entries
        .iter()
        .map(|entry| entry.timestamp)
        .min()
        .ok_or_else(|| "window aggregate requires a non-empty window".to_string())?;
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
                    "window processor '{}' failed to purge timed aggregate state: {}",
                    processor.as_str(),
                    error
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
        let emit_high_watermark =
            match current_window_emit_high_watermark(&branch.runtime, &branch.domain) {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        state.entries.iter().map(|entry| &entry.message.acks),
                        format!(
                            "window processor '{}' cannot emit aggregate: {}",
                            processor.as_str(),
                            error
                        ),
                    );
                    state.clear(aggregate);
                    changed = true;
                    break;
                }
            };
        let output_metadata = match window_output_metadata(state, emit_high_watermark) {
            Ok(metadata) => metadata,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    state.entries.iter().map(|entry| &entry.message.acks),
                    format!(
                        "window processor '{}' cannot emit aggregate: {}",
                        processor.as_str(),
                        error
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
            let output_schema =
                match relay_schema_for_runtime(&branch.runtime, &branch.domain, &output_relay) {
                    Ok(schema) => schema,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            node_kind,
                            processor,
                            error_policies,
                            state.entries.iter().map(|entry| &entry.message.acks),
                            error,
                        );
                        route_failed = true;
                        break;
                    }
                };
            let output_batch =
                match evaluate_window_aggregate(compiled_aggregate, state, &output_schema).await {
                    Ok(record) => record,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            node_kind,
                            processor,
                            error_policies,
                            state.entries.iter().map(|entry| &entry.message.acks),
                            format!(
                                "window processor '{}' output route '{}' aggregate failed: {}",
                                processor.as_str(),
                                output_relay.as_str(),
                                error
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
                    "window processor '{}' failed to advance window: {}",
                    processor.as_str(),
                    error
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
) -> Result<(), String> {
    replicated_state.replace_state(state).map_err(|error| {
        format!(
            "window processor '{}' failed to snapshot branch state: {}",
            processor.as_str(),
            error
        )
    })?;
    Ok(())
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

    pub(super) fn from_snapshot(snapshot: WindowAggregateAccumulatorSnapshot) -> Self {
        match snapshot {
            WindowAggregateAccumulatorSnapshot::Counter { count } => Self::Counter { count },
            WindowAggregateAccumulatorSnapshot::Sequence { values } => Self::Sequence {
                values: values
                    .into_iter()
                    .map(|snapshot| WindowSequenceValue {
                        timestamp: snapshot.timestamp,
                        sequence: snapshot.sequence,
                        value: RuntimeValue::from_remote(snapshot.value),
                    })
                    .collect(),
            },
            WindowAggregateAccumulatorSnapshot::SortedMap { counts } => Self::SortedMap {
                counts: counts
                    .into_iter()
                    .map(|entry| {
                        (
                            RuntimeValueSortKey(RuntimeValue::from_remote(entry.value)),
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
                buckets,
                total,
                min,
                max,
                width,
                delay: Duration::from_nanos(delay_nanos),
                delayed_removals: delayed_removals
                    .into_iter()
                    .map(|removal| LinearHistogramDelayedRemoval {
                        expires_at: removal.expires_at,
                        bucket: removal.bucket,
                    })
                    .collect(),
            },
            WindowAggregateAccumulatorSnapshot::Sum { total } => Self::Sum {
                total: total.map(RuntimeValue::from_remote),
            },
        }
    }

    pub(super) fn purge_expired(&mut self, now: Timestamp) -> Result<(), String> {
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
                return Err("linear histogram delayed removal bucket is out of range".to_string());
            };
            if *count == 0 {
                return Err(
                    "linear histogram accumulator is missing delayed removed value".to_string(),
                );
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
    ) -> Result<(), String> {
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
                    .ok_or_else(|| "sequence aggregate structure requires a value".to_string())?;
                values.push_back(WindowSequenceValue {
                    timestamp,
                    sequence,
                    value,
                });
                Ok(())
            }
            Self::SortedMap { counts } => {
                let value = value
                    .ok_or_else(|| "ordered aggregate structure requires a value".to_string())?;
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
                    .ok_or_else(|| "PERCENTILE_LINEAR_HISTOGRAM requires a value".to_string())?;
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
                let value = value.ok_or_else(|| "SUM requires a value".to_string())?;
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
    ) -> Result<(), String> {
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
                    return Err("sequence accumulator is missing removed window entry".to_string());
                };
                values.remove(index);
                Ok(())
            }
            Self::SortedMap { counts } => {
                let value = value
                    .ok_or_else(|| "ordered aggregate structure requires a value".to_string())?;
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
                    .ok_or_else(|| "PERCENTILE_LINEAR_HISTOGRAM requires a value".to_string())?;
                let value = runtime_value_to_f64(&value)?;
                let bucket = linear_histogram_bucket(value, *min, *max, *width, buckets.len())?;
                if delay.is_zero() {
                    let Some(count) = buckets.get_mut(bucket) else {
                        return Err("linear histogram bucket is out of range".to_string());
                    };
                    if *count == 0 {
                        return Err(
                            "linear histogram accumulator is missing removed value".to_string()
                        );
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
                let value = value.ok_or_else(|| "SUM requires a value".to_string())?;
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
    ) -> Result<RuntimeValue, String> {
        match (function, self) {
            (WindowAggregateFunction::Count, Self::Counter { count }) => {
                Ok(RuntimeValue::I64(i64::try_from(*count).assured(
                    "a window counter cannot exceed the allocation limit of its retained entries",
                )))
            }
            (WindowAggregateFunction::First, Self::Sequence { values }) => values
                .iter()
                .min_by_key(|entry| (entry.timestamp, entry.sequence))
                .map(|entry| entry.value.clone())
                .ok_or_else(|| "FIRST requires a non-empty window".to_string()),
            (WindowAggregateFunction::Last, Self::Sequence { values }) => values
                .iter()
                .max_by_key(|entry| (entry.timestamp, entry.sequence))
                .map(|entry| entry.value.clone())
                .ok_or_else(|| "LAST requires a non-empty window".to_string()),
            (WindowAggregateFunction::Max, Self::SortedMap { counts }) => counts
                .last_key_value()
                .map(|(value, _)| value.0.clone())
                .ok_or_else(|| "MAX requires a non-empty window".to_string()),
            (WindowAggregateFunction::Min, Self::SortedMap { counts }) => counts
                .first_key_value()
                .map(|(value, _)| value.0.clone())
                .ok_or_else(|| "MIN requires a non-empty window".to_string()),
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
                    "PERCENTILE_LINEAR_HISTOGRAM requires a constant percentile".to_string()
                })?;
                percentile_from_linear_histogram(buckets, *total, *min, *max, *width, percentile)
            }
            (WindowAggregateFunction::Sum, Self::Sum { total }) => total
                .clone()
                .ok_or_else(|| "SUM requires a non-empty window".to_string()),
            _ => Err(format!(
                "{function:?} aggregate is backed by an incompatible accumulator"
            )),
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

    pub(super) fn to_snapshot(&self) -> Result<WindowProcessorStateSnapshot, String> {
        Ok(WindowProcessorStateSnapshot {
            entries: self
                .entries
                .iter()
                .map(|entry| {
                    Ok(WindowEntrySnapshot {
                        sequence: entry.sequence,
                        timestamp: entry.timestamp,
                        key: BranchKey::to_remote_key(&entry.message.key),
                        record: entry.message.record.to_remote()?,
                        aggregate_inputs: entry
                            .aggregate_inputs
                            .iter()
                            .map(|input| input.value.as_ref().map(RuntimeValue::to_remote))
                            .collect(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            next_sequence: self.next_sequence,
            accumulators: self
                .accumulators
                .iter()
                .map(WindowAggregateAccumulator::to_snapshot)
                .collect(),
        })
    }

    pub(super) fn from_snapshot(
        program: &WindowAggregateProgram,
        input_schema: &CompiledSchema,
        snapshot: WindowProcessorStateSnapshot,
    ) -> Result<Self, String> {
        if snapshot.accumulators.len() != program.demands().len() {
            return Err(format!(
                "window snapshot accumulator count {} does not match aggregate demand count {}",
                snapshot.accumulators.len(),
                program.demands().len()
            ));
        }
        Ok(Self {
            entries: snapshot
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(WindowEntry {
                        sequence: entry.sequence,
                        timestamp: entry.timestamp,
                        message: RelayMessage {
                            key: BranchKey::from_remote_key(entry.key)?,
                            record: input_schema.runtime_row_from_remote(entry.record)?,
                            acks: AckSet::empty(),
                        },
                        aggregate_inputs: entry
                            .aggregate_inputs
                            .into_iter()
                            .map(|value| WindowAggregateInput {
                                value: value.map(RuntimeValue::from_remote),
                            })
                            .collect(),
                    })
                })
                .collect::<Result<VecDeque<_>, String>>()?,
            next_sequence: snapshot.next_sequence,
            accumulators: snapshot
                .accumulators
                .into_iter()
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
    ) -> Result<(), Box<(String, RelayMessage)>> {
        let sequence = self.next_sequence;
        self.apply_aggregate_inputs(
            program.demands(),
            timestamp,
            sequence,
            &inputs,
            WindowAccumulatorAction::Add,
        )
        .map_err(|error| Box::new((error, message.clone())))?;
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

    pub(super) fn purge_timeouts(&mut self, now: Timestamp) -> Result<bool, String> {
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
    ) -> Result<Option<WindowEntry>, String> {
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
    ) -> Result<(), String> {
        if inputs.len() != self.accumulators.len() {
            return Err(format!(
                "window aggregate input count {} does not match accumulator count {}",
                inputs.len(),
                self.accumulators.len()
            ));
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
) -> Result<Vec<Result<Vec<WindowAggregateInput>, String>>, String> {
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
    )?;
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
    .map_err(|error| error.to_string())?;
    if result.batch.row_count() != row_count {
        return Err(format!(
            "window aggregate input VM produced {} rows for {row_count} input rows",
            result.batch.row_count()
        ));
    }
    if result.selected_rows.len() != row_count || !result.selected_rows.iter().eq(0..row_count) {
        return Err(format!(
            "window aggregate input VM did not preserve all {row_count} input rows"
        ));
    }
    let input_columns = program
        .input_fields
        .iter()
        .map(|field_name| {
            let Some(field_name) = field_name else {
                return Ok(None);
            };
            let column_index = result.batch.schema().index_of(field_name).map_err(|_| {
                format!("window aggregate input VM produced no '{field_name}' field")
            })?;
            let array = result.batch.column(column_index).to_array_ref();
            RuntimeValueColumn::new(field_name.as_str(), array)
                .map(Some)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok((0..row_count)
        .map(|row| {
            if let Some(error) = result.batch.errors().row(row).first() {
                return Err(format!(
                    "window aggregate input VM failed with {}: {}",
                    error.code.as_str(),
                    error.message
                ));
            }
            input_columns
                .iter()
                .map(|column| {
                    let Some(column) = column else {
                        return Ok(WindowAggregateInput { value: None });
                    };
                    column
                        .nullable_value_at(row)
                        .map(|value| WindowAggregateInput { value })
                })
                .collect()
        })
        .collect())
}

#[derive(Debug, Clone, Copy)]
pub(super) enum WindowAccumulatorAction {
    Add,
    Remove { at: Timestamp },
}

pub(super) fn decrement_runtime_value_count(
    counts: &mut BTreeMap<RuntimeValueSortKey, usize>,
    value: RuntimeValue,
) -> Result<(), String> {
    let key = RuntimeValueSortKey(value);
    let Some(count) = counts.get_mut(&key) else {
        return Err("sorted accumulator is missing removed window value".to_string());
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
) -> Result<usize, String> {
    if !value.is_finite() {
        return Err("PERCENTILE_LINEAR_HISTOGRAM requires finite numeric values".to_string());
    }
    if bucket_count == 0 {
        return Err("PERCENTILE_LINEAR_HISTOGRAM requires at least one bucket".to_string());
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
        .ok_or_else(|| {
            format!("PERCENTILE_LINEAR_HISTOGRAM value {value} falls outside the bucket range")
        })
}

pub(super) fn percentile_from_linear_histogram(
    buckets: &[usize],
    total: usize,
    min: f64,
    max: f64,
    width: f64,
    percentile: f64,
) -> Result<RuntimeValue, String> {
    if total == 0 {
        return Err("PERCENTILE_LINEAR_HISTOGRAM requires a non-empty window".to_string());
    }
    let rank: usize = ((percentile / 100.0) * (total - 1).approx_into::<f64>())
        .round()
        .checked_approx_into()
        .ok_or_else(|| {
            format!(
                "PERCENTILE_LINEAR_HISTOGRAM percentile {percentile} has no rank in a window of \
                 {total} samples"
            )
        })?;
    let mut seen = 0usize;
    for (index, count) in buckets.iter().enumerate() {
        seen += *count;
        if seen > rank {
            let midpoint = min + (index.approx_into::<f64>() + 0.5) * width;
            return Ok(RuntimeValue::F64(OrderedFloat(midpoint.clamp(min, max))));
        }
    }
    Err("PERCENTILE_LINEAR_HISTOGRAM histogram is empty".to_string())
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
) -> Result<(), String> {
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
    fn inject(
        &self,
        function: &FunctionName,
        _arguments: &[VmTypedArray],
        row_count: usize,
        _span: nervix_vm::program::Span,
    ) -> Result<VmTypedArray, nervix_vm::RuntimeError> {
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
            .map_err(|message| nervix_vm::RuntimeError::InvalidBatch { message })?;
        let data_type = self.demand_types.get(invocation.demand_id).ok_or_else(|| {
            nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "window aggregate is missing output type for demand {}",
                    invocation.demand_id
                ),
            }
        })?;
        let array = runtime_value_arrow_array(data_type, Some(&value), row_count)
            .map_err(|message| nervix_vm::RuntimeError::InvalidBatch { message })?;
        VmTypedArray::try_from_array_ref(array).map_err(|error| {
            nervix_vm::RuntimeError::InvalidBatch {
                message: error.to_string(),
            }
        })
    }
}

pub(super) async fn evaluate_window_aggregate(
    program: &CompiledWindowAggregateProgram,
    state: &WindowProcessorState,
    output_schema: &CompiledSchema,
) -> Result<RuntimeRecordBatch, String> {
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
                )
                .await?,
            )
        } else if field.is_nullable() {
            None
        } else {
            return Err(format!(
                "window aggregate did not initialize required output field '{}'",
                field.name()
            ));
        };
        columns.push(runtime_value_arrow_array(
            field.data_type(),
            value.as_ref(),
            1,
        )?);
    }
    let batch = RecordBatch::try_new(output_schema.arrow_schema(), columns)
        .map_err(|error| error.to_string())?;
    RuntimeRecordBatch::from_record_batch(output_schema.arrow_schema(), batch)
}

pub(super) fn evaluate_window_aggregate_expr<'a>(
    expr: &'a CompiledWindowAggregateExpr,
    target_field: &'a str,
    injector: Arc<Box<dyn VmFunctionInjector>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RuntimeValue, String>> + Send + 'a>>
{
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
                .map_err(|error| error.to_string())?;
                let result = execute_program_with_selection_in_context(
                    program,
                    &input,
                    &VmExecutionContext {
                        now: Timestamp::now(),
                        injector: Some(injector),
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
                let column_index = result.batch.schema().index_of(target_field).map_err(|_| {
                    format!("window aggregate VM produced no '{target_field}' output field")
                })?;
                let field = result.batch.schema().field(column_index);
                let array = result.batch.column(column_index).to_array_ref();
                runtime_value_from_arrow_array(
                    array.as_ref(),
                    &parse_as_type_from_arrow(field.data_type())
                        .map_err(|error| error.to_string())?,
                    false,
                    0,
                    target_field,
                )?
                .ok_or_else(|| format!("window aggregate VM produced null '{target_field}' output"))
            }
            CompiledWindowAggregateExpr::Array { items, fixed_size } => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(
                        evaluate_window_aggregate_expr(item, target_field, injector.clone())
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

pub(super) fn runtime_value_to_f64(value: &RuntimeValue) -> Result<f64, String> {
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
        other => Err(format!(
            "expected numeric value, found {}",
            runtime_value_type_name(other)
        )),
    }
}

pub(super) fn sum_runtime_values(
    left: RuntimeValue,
    right: RuntimeValue,
) -> Result<RuntimeValue, String> {
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
        (left, right) => Err(format!(
            "SUM cannot combine {} and {}",
            runtime_value_type_name(&left),
            runtime_value_type_name(&right)
        )),
    }
}

pub(super) fn subtract_runtime_values(
    left: RuntimeValue,
    right: RuntimeValue,
) -> Result<Option<RuntimeValue>, String> {
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
            return Err(format!(
                "SUM cannot remove {} from {}",
                runtime_value_type_name(&right),
                runtime_value_type_name(&left)
            ));
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
            ],
        });
        let aggregate = window_aggregate(
            "SET count = COUNT(input.latency), adjusted_count = COUNT(input.latency) + 2, p50 = \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), latencies = \
             [PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 100, 10, 0, 100, '2s')]",
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
        let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
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
        assert!(
            evaluated[1]
                .as_ref()
                .expect_err("division by zero should fail only its input row")
                .contains("division_by_zero")
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
        let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
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
        let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
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
        let retained = evaluate_window_aggregate(&compiled, &state, &output_schema)
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
        let still_retained = evaluate_window_aggregate(&compiled, &state, &output_schema)
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
        let expired = evaluate_window_aggregate(&compiled, &state, &output_schema)
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
        let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
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
        let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
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
        let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
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
        let restored = WindowProcessorState::from_snapshot(
            &aggregate,
            input_schema.as_ref(),
            state.to_snapshot().expect("snapshot should encode"),
        )
        .expect("snapshot should restore");
        let compiled =
            compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
        let record = evaluate_window_aggregate(&compiled, &restored, &output_schema)
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
