use indexmap::IndexMap;

use super::*;

/// One reingestor input this node runs: the reingestor model, the relay that input reads from,
/// and the fan-in that delivers that relay's batches.
pub(super) struct ReingestorInputSpec {
    pub(super) reingestor: CreateReingestor,
    pub(super) from_relay: RelayName,
    pub(super) receiver: RelayRuntimeFanIn,
}

#[derive(Clone, Copy)]
pub(super) struct ReingestorDispatchContext<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) reingestor: &'a ReingestorName,
    pub(super) from_relay: &'a RelayName,
    pub(super) from_where: Option<&'a nervix_models::Expression>,
    pub(super) mode: AckMode,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) branched_senders: &'a HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    pub(super) domain_clock: &'a DomainClock,
    pub(super) execution_now: Timestamp,
}

impl<'a> ReingestorDispatchContext<'a> {
    fn output_flush(self) -> ReingestorOutputFlushContext<'a> {
        ReingestorOutputFlushContext {
            domain: self.domain,
            reingestor: self.reingestor,
            mode: self.mode,
            error_policies: self.error_policies,
            branched_senders: self.branched_senders,
            domain_clock: self.domain_clock,
        }
    }
}

#[derive(Clone, Copy)]
struct ReingestorOutputFlushContext<'a> {
    domain: &'a DomainName,
    reingestor: &'a ReingestorName,
    mode: AckMode,
    error_policies: &'a ErrorPolicies,
    branched_senders: &'a HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    domain_clock: &'a DomainClock,
}

#[derive(Clone, Copy)]
pub(super) enum ReingestorOutputFlush {
    Due,
    All,
}

#[derive(Default)]
struct ReingestorBranchOutputBuffer {
    pending: Vec<RelayRecordBatch>,
    estimated_bytes: u64,
    flush_timer: BranchBufferTimer,
}

impl ReingestorBranchOutputBuffer {
    fn enqueue(
        &mut self,
        batch: RelayRecordBatch,
        policy: RuntimeFlushPolicy,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
    ) -> BranchBufferTimingResult<bool> {
        self.flush_timer.arm_flush(policy, clock, snapshot)?;
        let deadline_reached = self.flush_timer.is_due(clock, snapshot)?;
        let estimated_bytes = self
            .estimated_bytes
            .checked_add(batch.estimated_bytes())
            .assured("both counts estimate batches this reingestor already holds in memory");
        self.pending.push(batch);
        self.estimated_bytes = estimated_bytes;
        Ok(deadline_reached || policy.size_boundary_reached(self.estimated_bytes))
    }
}

#[derive(Clone)]
struct ReingestorOutputBufferKey {
    output_index: usize,
    branch: Option<BranchKey>,
}

struct ReingestorOutputBuffers {
    routes: Vec<IndexMap<Option<BranchKey>, ReingestorBranchOutputBuffer>>,
    quiesce: ReingestorOutputQuiesceGauge,
}

impl ReingestorOutputBuffers {
    fn new(route_count: usize, quiesce_counters: Arc<NodeQuiesceCounters>) -> Self {
        Self {
            routes: std::iter::repeat_with(IndexMap::new)
                .take(route_count)
                .collect(),
            quiesce: ReingestorOutputQuiesceGauge::new(quiesce_counters),
        }
    }

    fn enqueue(
        &mut self,
        output_index: usize,
        batch: RelayRecordBatch,
        policy: RuntimeFlushPolicy,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
    ) -> BranchBufferTimingResult<(ReingestorOutputBufferKey, bool)> {
        let branch = batch.key.clone();
        let route = self
            .routes
            .get_mut(output_index)
            .verified("the output index came from this reingestor's route list");
        let should_flush = if let Some(buffer) = route.get_mut(&branch) {
            buffer.enqueue(batch, policy, clock, snapshot)?
        } else {
            let mut buffer = ReingestorBranchOutputBuffer::default();
            let should_flush = buffer.enqueue(batch, policy, clock, snapshot)?;
            route.insert(branch.clone(), buffer);
            should_flush
        };
        self.quiesce.add_batch();
        Ok((
            ReingestorOutputBufferKey {
                output_index,
                branch,
            },
            should_flush,
        ))
    }

    fn keys(&self) -> Vec<ReingestorOutputBufferKey> {
        self.routes
            .iter()
            .enumerate()
            .flat_map(|(output_index, branches)| {
                branches
                    .keys()
                    .cloned()
                    .map(move |branch| ReingestorOutputBufferKey {
                        output_index,
                        branch,
                    })
            })
            .collect()
    }

    fn get(&self, key: &ReingestorOutputBufferKey) -> &ReingestorBranchOutputBuffer {
        self.routes
            .get(key.output_index)
            .verified("the buffer key came from this reingestor's route list")
            .get(&key.branch)
            .verified("buffer keys are collected from the same route map immediately before use")
    }

    fn take(&mut self, key: &ReingestorOutputBufferKey) -> Vec<RelayRecordBatch> {
        let pending = self
            .routes
            .get_mut(key.output_index)
            .verified("the buffer key came from this reingestor's route list")
            .shift_remove(&key.branch)
            .verified("buffer keys are collected from the same route map immediately before use")
            .pending;
        self.quiesce.remove_batches(pending.len());
        pending
    }

    fn deadlines(&self) -> Vec<BranchBufferDeadline> {
        self.routes
            .iter()
            .flat_map(IndexMap::values)
            .filter_map(|buffer| buffer.flush_timer.deadline())
            .collect()
    }
}

struct ReingestorOutputQuiesceGauge {
    counters: Arc<NodeQuiesceCounters>,
    output_buffers: usize,
}

impl ReingestorOutputQuiesceGauge {
    fn new(counters: Arc<NodeQuiesceCounters>) -> Self {
        Self {
            counters,
            output_buffers: 0,
        }
    }

    fn add_batch(&mut self) {
        self.output_buffers = self
            .output_buffers
            .checked_add(1)
            .assured("the count cannot exceed the batches this reingestor holds in memory");
        self.counters.output_buffers.fetch_add(1, Ordering::AcqRel);
    }

    fn remove_batches(&mut self, count: usize) {
        self.output_buffers = self
            .output_buffers
            .checked_sub(count)
            .verified("only batches counted when they entered this buffer can be removed");
        self.counters
            .output_buffers
            .fetch_sub(count, Ordering::AcqRel);
    }
}

impl Drop for ReingestorOutputQuiesceGauge {
    fn drop(&mut self) {
        self.counters
            .output_buffers
            .fetch_sub(self.output_buffers, Ordering::AcqRel);
    }
}

impl Runtime {
    pub(super) async fn evaluate_reingestor_output_events(
        &self,
        context: ReingestorDispatchContext<'_>,
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
        let ReingestorDispatchContext {
            domain,
            reingestor,
            from_relay,
            ..
        } = context;
        if output.compiled_program.is_none() {
            let (
                input_schema,
                output_schema,
                materialized_stream_specs,
                available_lookups,
                udfs,
                current_branching,
                current_branch_schema,
                target_branch_schema,
            ) = {
                let Some(execution) = self.inner.executions.get(domain) else {
                    return Err(PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: format!("domain '{}' is not instantiated", domain.as_str()),
                    });
                };
                let input_schema = execution
                    .relay_schemas
                    .get(from_relay)
                    .cloned()
                    .ok_or_else(|| PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: format!(
                            "stream '{}' schema is not instantiated in domain '{}'",
                            from_relay.as_str(),
                            domain.as_str()
                        ),
                    })?;
                let output_schema = execution
                    .relay_schemas
                    .get(&output.relay)
                    .cloned()
                    .ok_or_else(|| PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: format!(
                            "stream '{}' schema is not instantiated in domain '{}'",
                            output.relay.as_str(),
                            domain.as_str()
                        ),
                    })?;
                (
                    input_schema,
                    output_schema,
                    execution.materialized_stream_specs.clone(),
                    execution.lookups.clone(),
                    execution.udfs.clone(),
                    execution
                        .relay_branchings
                        .get(from_relay)
                        .cloned()
                        .unwrap_or_default(),
                    execution
                        .relay_branching_schemas
                        .get(from_relay)
                        .cloned()
                        .flatten(),
                    execution
                        .relay_branching_schemas
                        .get(&output.relay)
                        .cloned()
                        .flatten(),
                )
            };
            match compile_processor_output_filter_map_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: &ModelName::from(reingestor),
                },
                std::slice::from_ref(from_relay),
                &output.relay,
                &output.construction,
                RuntimeVmSchemaPair {
                    input: batch.arrow_schema(),
                    input_sensitivity: input_schema.vm_sensitivity(),
                    output: output_schema.arrow_schema(),
                    output_sensitivity: output_schema.vm_sensitivity(),
                },
                None,
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
                    udfs: Some(&udfs),
                },
            ) {
                Ok(program) => output.compiled_program = program,
                Err(error) => {
                    return Err(PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: error.to_string(),
                    });
                }
            }
            output.compiled_branch_program = compile_output_branch_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: &ModelName::from(reingestor),
                },
                output.branch.as_ref(),
                RuntimeVmSchema {
                    schema: batch.arrow_schema(),
                    sensitivity: input_schema.vm_sensitivity(),
                },
                RuntimeVmSchema {
                    schema: output_schema.arrow_schema(),
                    sensitivity: output_schema.vm_sensitivity(),
                },
                target_branch_schema,
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
                    udfs: Some(&udfs),
                },
            )
            .map_err(|error| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: error.to_string(),
            })?;
        }

        let Some(program) = output.compiled_program.as_ref() else {
            let output_schema = match self.inner.executions.get(domain) {
                Some(execution) => execution.relay_schemas.get(&output.relay).cloned(),
                None => None,
            };
            let output_schema = output_schema.ok_or_else(|| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "reingestor '{}' output relay '{}' is not instantiated",
                    reingestor.as_str(),
                    output.relay.as_str()
                ),
            })?;
            let projected = batch
                .batch
                .project(output_schema.arrow_schema())
                .map_err(|error| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "reingestor '{}' failed to project output relay '{}': {error}",
                        reingestor.as_str(),
                        output.relay.as_str()
                    ),
                })?;
            let keys = if let Some(branch_program) = output.compiled_branch_program.as_ref() {
                evaluate_output_branch_program(
                    reingestor,
                    branch_program,
                    &batch.batch,
                    &projected,
                    &batch.keys,
                    &scope.side_inputs,
                    scope.execution_now,
                )
                .await
                .map_err(|reason| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason,
                })?
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|reason| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason,
                })?
            } else {
                match output.branch.as_ref() {
                    Some(OutputBranch::Unbranched) => vec![None; projected.batch().num_rows()],
                    Some(OutputBranch::BranchedBy { assignments, .. })
                        if assignments.is_empty() =>
                    {
                        batch.keys.clone()
                    }
                    Some(OutputBranch::BranchedBy { .. }) => {
                        return Err(PlannedGeneralError {
                            acks: batch.acks.clone(),
                            reason: format!(
                                "reingestor '{}' output '{}' has no compiled branch program",
                                reingestor.as_str(),
                                output.relay.as_str()
                            ),
                        });
                    }
                    None => batch.keys.clone(),
                }
            };
            let input_rows = (0..projected.batch().num_rows()).collect::<Vec<_>>();
            let pending = pending_output_batches_by_key(
                output_index,
                &input_rows,
                keys,
                projected,
                &batch.metadata,
            )
            .map_err(|reason| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason,
            })?;
            return Ok((pending, Vec::new()));
        };

        let Some(output_schema) = scope.output_schemas[output_index].clone() else {
            return Err(PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "reingestor '{}' evaluated output route '{}' without preparing its relay \
                     schema",
                    reingestor.as_str(),
                    output.relay.as_str()
                ),
            });
        };
        let execution_now = scope.execution_now;
        let executed = execute_filter_map_program_on_batch(
            "reingestor",
            reingestor,
            program,
            FilterMapBatchInputs {
                carrier: &batch.batch,
                namespace_batches: &[],
                keys: &batch.keys,
                side_inputs: &scope.side_inputs,
                ingest_metadata: None,
            },
            execution_now,
            batch.acks.clone(),
            Some(&mut scope.shared),
        )
        .await?;
        let mut success_output_rows = Vec::new();
        let mut success_input_rows = Vec::new();
        let mut errors = Vec::new();
        for (output_row, input_row) in executed.selected_rows.iter().enumerate() {
            if let Some(side_error) = executed.batch.errors().row(output_row).first() {
                let partial_output = captured_partial_output(&executed.batch, output_row);
                let record = batch
                    .runtime_row(input_row)
                    .map_err(|error| PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason: format!(
                            "reingestor '{}' failed to materialize FILTER-MAP error input row: {}",
                            reingestor.as_str(),
                            error
                        ),
                    })?;
                errors.push(PendingProcessorOutputMessageError {
                    row: input_row,
                    key: batch.keys[input_row].clone(),
                    record,
                    error: program.structured_side_error(
                        scope.execution_now,
                        format!(
                            "reingestor '{}' FILTER-MAP side error {}: {} at {}",
                            reingestor.as_str(),
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
            let output_batch = vm_typed_batch_selected_rows_to_runtime_batch(
                &executed.batch,
                &success_output_rows,
            )
            .map_err(|error| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "reingestor '{}' failed to materialize successful FILTER-MAP rows: {}",
                    reingestor.as_str(),
                    error
                ),
            })?;
            if output_batch.schema().as_ref() != output_schema.arrow_schema().as_ref() {
                return Err(PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "reingestor '{}' FILTER-MAP output schema does not match relay '{}'",
                        reingestor.as_str(),
                        output.relay.as_str()
                    ),
                });
            }
            let metadata = success_input_rows
                .iter()
                .map(|input_row| batch.metadata[*input_row].clone())
                .collect::<Vec<_>>();
            let input_batch =
                batch
                    .batch
                    .take(&success_input_rows)
                    .map_err(|reason| PlannedGeneralError {
                        acks: batch.acks.clone(),
                        reason,
                    })?;
            let input_keys = success_input_rows
                .iter()
                .map(|row| batch.keys[*row].clone())
                .collect::<Vec<_>>();
            let keys = if let Some(branch_program) = output.compiled_branch_program.as_ref() {
                evaluate_output_branch_program(
                    reingestor,
                    branch_program,
                    &input_batch,
                    &output_batch,
                    &input_keys,
                    &scope.side_inputs,
                    execution_now,
                )
                .await
                .map_err(|reason| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason,
                })?
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|reason| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason,
                })?
            } else {
                match output.branch.as_ref() {
                    Some(OutputBranch::Unbranched) => vec![None; output_batch.batch().num_rows()],
                    Some(OutputBranch::BranchedBy { assignments, .. })
                        if assignments.is_empty() =>
                    {
                        input_keys
                    }
                    Some(OutputBranch::BranchedBy { .. }) => {
                        return Err(PlannedGeneralError {
                            acks: batch.acks.clone(),
                            reason: format!(
                                "reingestor '{}' output '{}' has no compiled branch program",
                                reingestor.as_str(),
                                output.relay.as_str()
                            ),
                        });
                    }
                    None => input_keys,
                }
            };
            pending_output_batches_by_key(
                output_index,
                &success_input_rows,
                keys,
                output_batch,
                &metadata,
            )
            .map_err(|reason| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason,
            })?
        };

        Ok((output_batches, errors))
    }

    /// Dispatches one admitted batch through every reingestor output route.
    ///
    /// `materialized_values` is the snapshot resolved when the batch was admitted; `FROM WHERE`,
    /// the FILTER-MAP program and each route's branch program read it instead of re-reading the
    /// state store, so the whole batch observes one consistent view of its dependencies.
    async fn dispatch_reingestor_outputs(
        &self,
        context: ReingestorDispatchContext<'_>,
        compiled_from_where: &mut Option<CompiledProgramWithMaterializedInterest>,
        output_routes: &mut RelayProcessorOutputsNode,
        output_buffers: &mut ReingestorOutputBuffers,
        batch: RelayRecordBatch,
        materialized_values: &HashMap<String, RuntimeValue>,
    ) {
        let ReingestorDispatchContext {
            domain,
            reingestor,
            error_policies,
            branched_senders,
            execution_now,
            ..
        } = context;
        if batch.message_count() == 0 {
            return;
        }
        let Some(batch) = self
            .filter_reingestor_from_batch(context, compiled_from_where, batch, materialized_values)
            .await
        else {
            return;
        };
        if batch.message_count() == 0 {
            return;
        }

        let output_relays = output_routes
            .routes
            .iter()
            .map(|output| output.relay.clone())
            .collect::<Vec<_>>();

        let mut output_schemas = Vec::with_capacity(output_relays.len());
        for relay in &output_relays {
            match relay_schema_for_runtime(self, domain, relay) {
                Ok(schema) => output_schemas.push(Some(schema)),
                Err(error) => {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        ModelKind::Reingestor,
                        reingestor,
                        error_policies,
                        batch.acks.iter(),
                        error.to_string(),
                    );
                    return;
                }
            }
        }
        let mut scope = ProcessorOutputBatchScope {
            state_snapshot: relay_state_snapshot_from_side_inputs(materialized_values),
            side_inputs: materialized_values.clone(),
            execution_now,
            output_schemas,
            shared: SharedBatchColumns::default(),
        };

        let mut pending_batches = Vec::new();
        let mut pending_errors = Vec::new();
        for (output_index, output) in output_routes.routes.iter_mut().enumerate() {
            tokio::task::consume_budget().await;
            let (batches, errors) = match self
                .evaluate_reingestor_output_events(
                    context,
                    output,
                    output_index,
                    &batch,
                    &mut scope,
                )
                .await
            {
                Ok(events) => events,
                Err(error) => {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        ModelKind::Reingestor,
                        reingestor,
                        error_policies,
                        error.acks.iter(),
                        error.reason,
                    );
                    return;
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
                self.handle_internal_processor_error_for_acks(
                    domain,
                    ModelKind::Reingestor,
                    reingestor,
                    error_policies,
                    batch_acks.iter(),
                    "reingestor output batch ack count does not match selected row count"
                        .to_string(),
                );
                return;
            }
            let output_index = pending_batch.output_index;
            let error_acks = batch_acks.clone();
            match pending_batch.into_relay_batch(batch_acks) {
                Ok(batch) => batches_by_output[output_index].push(batch),
                Err(error) => {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        ModelKind::Reingestor,
                        reingestor,
                        error_policies,
                        error_acks.iter(),
                        error,
                    );
                    return;
                }
            }
        }

        for (output_index, error) in pending_errors {
            let Some(acks) = ack_queues[error.row].pop_front() else {
                continue;
            };
            self.handle_structured_message_error(MessageErrorHandling {
                domain,
                node_kind: ModelKind::Reingestor,
                node: &ModelName::from(reingestor),
                source_route: Some(&output_routes.routes[output_index].relay),
                policy: &output_routes.routes[output_index].message_error_policy,
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

        let domain_clock = context.domain_clock;
        let flush_snapshot = match domain_clock.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                for batches in batches_by_output {
                    for batch in batches {
                        self.handle_internal_processor_error_for_acks(
                            domain,
                            ModelKind::Reingestor,
                            reingestor,
                            error_policies,
                            batch.acks.iter(),
                            format!(
                                "reingestor '{}' could not read the domain clock while buffering \
                                 output: {error}",
                                reingestor.as_str(),
                            ),
                        );
                    }
                }
                return;
            }
        };
        for (output_index, mut batches) in batches_by_output.into_iter().enumerate() {
            tokio::task::consume_budget().await;
            let relay = &output_relays[output_index];
            if !branched_senders.contains_key(relay) {
                for batch in batches {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        ModelKind::Reingestor,
                        reingestor,
                        error_policies,
                        batch.acks.iter(),
                        format!(
                            "missing reingestor branched entrypoint for relay '{}'",
                            relay.as_str()
                        ),
                    );
                }
                continue;
            }
            if batches.is_empty() {
                continue;
            }
            let policy = output_routes.routes[output_index]
                .flush_policy
                .verified("the registry requires every reingestor output to declare FLUSH");
            for batch in batches.drain(..) {
                let batch_acks = batch.acks.clone();
                match output_buffers.enqueue(
                    output_index,
                    batch,
                    policy,
                    domain_clock,
                    &flush_snapshot,
                ) {
                    Ok((key, should_flush)) => {
                        if should_flush {
                            let pending = output_buffers.take(&key);
                            self.flush_reingestor_output(context.output_flush(), relay, pending)
                                .await;
                        }
                    }
                    Err(error) => {
                        self.handle_internal_processor_error_for_acks(
                            domain,
                            ModelKind::Reingestor,
                            reingestor,
                            error_policies,
                            batch_acks.iter(),
                            format!(
                                "reingestor '{}' could not start output '{}' flush deadline: \
                                 {error}",
                                reingestor.as_str(),
                                relay.as_str(),
                            ),
                        );
                    }
                }
            }
        }
    }

    async fn flush_reingestor_output(
        &self,
        context: ReingestorOutputFlushContext<'_>,
        output_relay: &RelayName,
        pending: Vec<RelayRecordBatch>,
    ) {
        let ReingestorOutputFlushContext {
            domain,
            reingestor,
            mode,
            error_policies,
            branched_senders,
            ..
        } = context;
        if pending.is_empty() {
            return;
        }
        let pending_acks = pending
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        let forwarded = match RelayRecordBatch::concat(pending) {
            Ok(batch) => batch,
            Err(error) => {
                self.handle_internal_processor_error_for_acks(
                    domain,
                    ModelKind::Reingestor,
                    reingestor,
                    error_policies,
                    pending_acks.iter(),
                    format!(
                        "reingestor '{}' failed to concat buffered output batches for relay '{}': \
                         {}",
                        reingestor.as_str(),
                        output_relay.as_str(),
                        error
                    ),
                );
                return;
            }
        };
        let Some(branched_sender) = branched_senders.get(output_relay) else {
            self.handle_internal_processor_error_for_acks(
                domain,
                ModelKind::Reingestor,
                reingestor,
                error_policies,
                forwarded.acks.iter(),
                format!(
                    "missing reingestor branched entrypoint for relay '{}'",
                    output_relay.as_str()
                ),
            );
            return;
        };
        if let Err(error) = branched_sender.send(forwarded).await {
            let batch = error.0;
            if mode == AckMode::Detached {
                for ack in batch.acks {
                    ack.ack_success();
                }
                return;
            }
            self.handle_internal_processor_error_for_acks(
                domain,
                ModelKind::Reingestor,
                reingestor,
                error_policies,
                batch.acks.iter(),
                format!(
                    "reingestor '{}' failed to forward buffered batch to branch entrypoint for \
                     relay '{}'",
                    reingestor.as_str(),
                    output_relay.as_str()
                ),
            );
        }
    }

    async fn flush_reingestor_outputs(
        &self,
        context: ReingestorOutputFlushContext<'_>,
        output_routes: &RelayProcessorOutputsNode,
        output_buffers: &mut ReingestorOutputBuffers,
        flush: ReingestorOutputFlush,
    ) {
        let snapshot = match flush {
            ReingestorOutputFlush::Due => match context.domain_clock.snapshot() {
                Ok(snapshot) => Some(snapshot),
                Err(error) => {
                    let keys = output_buffers.keys();
                    for key in keys {
                        tokio::task::consume_budget().await;
                        let relay = &output_routes
                            .routes
                            .get(key.output_index)
                            .verified("the buffer key came from this reingestor's route list")
                            .relay;
                        let pending = output_buffers.take(&key);
                        let acks = pending
                            .iter()
                            .flat_map(|batch| batch.acks.iter().cloned())
                            .collect::<Vec<_>>();
                        self.handle_internal_processor_error_for_acks(
                            context.domain,
                            ModelKind::Reingestor,
                            context.reingestor,
                            context.error_policies,
                            acks.iter(),
                            format!(
                                "reingestor '{}' could not read the domain clock while releasing \
                                 output '{}': {error}",
                                context.reingestor.as_str(),
                                relay.as_str(),
                            ),
                        );
                    }
                    return;
                }
            },
            ReingestorOutputFlush::All => None,
        };
        let keys = output_buffers.keys();
        for key in keys {
            tokio::task::consume_budget().await;
            let relay = &output_routes
                .routes
                .get(key.output_index)
                .verified("the buffer key came from this reingestor's route list")
                .relay;
            let should_flush = if let Some(snapshot) = &snapshot {
                match output_buffers
                    .get(&key)
                    .flush_timer
                    .is_due(context.domain_clock, snapshot)
                {
                    Ok(due) => due,
                    Err(error) => {
                        let pending = output_buffers.take(&key);
                        let acks = pending
                            .iter()
                            .flat_map(|batch| batch.acks.iter().cloned())
                            .collect::<Vec<_>>();
                        self.handle_internal_processor_error_for_acks(
                            context.domain,
                            ModelKind::Reingestor,
                            context.reingestor,
                            context.error_policies,
                            acks.iter(),
                            format!(
                                "reingestor '{}' could not inspect output '{}' flush deadline: \
                                 {error}",
                                context.reingestor.as_str(),
                                relay.as_str(),
                            ),
                        );
                        continue;
                    }
                }
            } else {
                true
            };
            if !should_flush {
                continue;
            }
            let pending = output_buffers.take(&key);
            self.flush_reingestor_output(context, relay, pending).await;
        }
    }

    pub(super) async fn filter_reingestor_from_batch(
        &self,
        context: ReingestorDispatchContext<'_>,
        compiled_from_where: &mut Option<CompiledProgramWithMaterializedInterest>,
        batch: RelayRecordBatch,
        materialized_values: &HashMap<String, RuntimeValue>,
    ) -> Option<RelayRecordBatch> {
        let ReingestorDispatchContext {
            domain,
            reingestor,
            from_relay,
            from_where,
            error_policies,
            execution_now,
            ..
        } = context;
        let Some(from_where) = from_where else {
            return Some(batch);
        };

        if compiled_from_where.is_none() {
            let (
                input_schema,
                materialized_stream_specs,
                available_lookups,
                udfs,
                current_branching,
                current_branch_schema,
            ) = {
                let Some(execution) = self.inner.executions.get(domain) else {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        ModelKind::Reingestor,
                        reingestor,
                        error_policies,
                        batch.acks.iter(),
                        format!("domain '{}' is not instantiated", domain.as_str()),
                    );
                    return None;
                };
                let input_schema = match execution.relay_schemas.get(from_relay).cloned() {
                    Some(schema) => schema,
                    None => {
                        self.handle_internal_processor_error_for_acks(
                            domain,
                            ModelKind::Reingestor,
                            reingestor,
                            error_policies,
                            batch.acks.iter(),
                            format!(
                                "stream '{}' schema is not instantiated in domain '{}'",
                                from_relay.as_str(),
                                domain.as_str()
                            ),
                        );
                        return None;
                    }
                };
                (
                    input_schema,
                    execution.materialized_stream_specs.clone(),
                    execution.lookups.clone(),
                    execution.udfs.clone(),
                    execution
                        .relay_branchings
                        .get(from_relay)
                        .cloned()
                        .unwrap_or_default(),
                    execution
                        .relay_branching_schemas
                        .get(from_relay)
                        .cloned()
                        .flatten(),
                )
            };
            match compile_expression_filter_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: &ModelName::from(reingestor),
                },
                Some(from_where),
                RuntimeVmSchema {
                    schema: batch.arrow_schema(),
                    sensitivity: input_schema.vm_sensitivity(),
                },
                false,
                MessageErrorOperation::SourceWhere,
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
                    udfs: Some(&udfs),
                },
            ) {
                Ok(program) => *compiled_from_where = program,
                Err(error) => {
                    self.handle_internal_processor_error_for_acks(
                        domain,
                        ModelKind::Reingestor,
                        reingestor,
                        error_policies,
                        batch.acks.iter(),
                        format!("FROM WHERE compile failed: {}", error),
                    );
                    return None;
                }
            }
        }

        let Some(program) = compiled_from_where.clone() else {
            return Some(batch);
        };
        let plan = match plan_filter_map_messages(
            "reingestor",
            reingestor,
            "FROM WHERE",
            &program,
            batch,
            execution_now,
            materialized_values,
        )
        .await
        {
            Ok(plan) => plan,
            Err(error) => {
                self.handle_internal_processor_error_for_acks(
                    domain,
                    ModelKind::Reingestor,
                    reingestor,
                    error_policies,
                    error.acks.iter(),
                    error.reason,
                );
                return None;
            }
        };
        self.handle_planned_message_errors(
            domain,
            ModelKind::Reingestor,
            reingestor,
            error_policies,
            plan.message_errors,
        )
        .await;
        plan.batch
    }

    pub(in crate::runtime) fn spawn_reingestor_task(
        &self,
        domain: &DomainName,
        shutdown_tx: &watch::Sender<bool>,
        branched_entrypoint_senders: &HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
        reingestor: CreateReingestor,
        from_relay: RelayName,
        receiver: RelayRuntimeFanIn,
    ) -> Result<JoinHandle<()>, RuntimeError> {
        let input_collect_policy = Self::parse_runtime_node_input_collect_policy(
            domain,
            "reingestor",
            &reingestor.name,
            reingestor.from.collect_policy.as_ref(),
        )?;
        let mut task_output_routes = RelayProcessorOutputsNode {
            routes: reingestor
                .output_routes
                .routes
                .iter()
                .map(|output| {
                    let flush_policy = output
                        .flush_policy
                        .as_ref()
                        .map(|policy| {
                            Self::parse_runtime_node_flush_policy(
                                domain,
                                "reingestor output",
                                &output.relay,
                                policy,
                            )
                        })
                        .transpose()?;
                    Ok(RelayProcessorOutputNode {
                        relay: output.relay.clone(),
                        construction: output.construction.clone(),
                        branch: output.branch.clone(),
                        flush_policy,
                        message_error_policy: output.message_error_policy.clone(),
                        pending: Vec::new(),
                        flush_timer: BranchBufferTimer::default(),
                        compiled_program: None,
                        compiled_branch_program: None,
                    })
                })
                .collect::<Result<Vec<_>, RuntimeError>>()?,
        };
        let mut task_branched_senders = HashMap::default();
        for output in reingestor.output_routes.outputs() {
            let Some(sender) = branched_entrypoint_senders.get(&output.relay).cloned() else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "missing reingestor branched entrypoint for relay '{}'",
                        output.relay.as_str()
                    ),
                });
            };
            task_branched_senders.insert(output.relay.clone(), sender);
        }
        let task_domain = domain.clone();
        let task_reingestor = reingestor.name.clone();
        let task_from_relay = from_relay;
        let task_from_where = reingestor
            .from
            .where_clauses()
            .iter()
            .find(|source_filter| source_filter.relay == task_from_relay)
            .map(|source_filter| source_filter.where_clause.clone());
        let task_materialized_state = reingestor.materialized_state.clone();
        let task_mode = reingestor.mode;
        let task_error_policies = internal_processor_error_policies(GeneralErrorPolicy::Log);
        let quiesce_counters = self.node_quiesce_counters(
            domain,
            NodeRef::new(ModelKind::Reingestor, &reingestor.name),
        );
        let runtime = self.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let force_flush = self.force_flush_participant(domain, quiesce_counters.clone());

        Ok(tokio::spawn(async move {
            let domain_clock = match runtime.bind_domain_clock(&task_domain) {
                Ok(clock) => clock,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "reingestor '{}' in domain '{}' could not bind its clock: {error}",
                        task_reingestor.as_str(),
                        task_domain.as_str(),
                    ));
                    return;
                }
            };
            let mut task_output_buffers = ReingestorOutputBuffers::new(
                task_output_routes.routes.len(),
                quiesce_counters.clone(),
            );
            let output_flush_context = ReingestorOutputFlushContext {
                domain: &task_domain,
                reingestor: &task_reingestor,
                mode: task_mode,
                error_policies: &task_error_policies,
                branched_senders: &task_branched_senders,
                domain_clock: &domain_clock,
            };
            let interaction_input =
                RelayInteractionInput::new(task_from_relay.clone(), receiver, input_collect_policy)
                    .with_domain_clock(domain_clock.clone());
            let mut interaction = RelayInteraction::new(
                vec![interaction_input],
                shutdown_rx,
                Some(force_flush),
                Some(quiesce_counters.clone()),
            )
            .verified(
                "the registry validated this input, and a non-empty input list builds an \
                 interaction",
            );
            let mut compiled_from_where = None;
            loop {
                tokio::task::consume_budget().await;
                let output_deadlines = task_output_buffers.deadlines();
                let has_output_deadlines = !output_deadlines.is_empty();
                let work = tokio::select! {
                    result = wait_for_branch_buffer_deadlines(&domain_clock, output_deadlines),
                        if has_output_deadlines =>
                    {
                        if let Err(error) = result {
                            runtime.events().report_error(format!(
                                "reingestor '{}' in domain '{}' could not wait for an output \
                                 flush deadline: {error}",
                                task_reingestor.as_str(),
                                task_domain.as_str(),
                            ));
                            break;
                        }
                        runtime
                            .flush_reingestor_outputs(
                                output_flush_context,
                                &task_output_routes,
                                &mut task_output_buffers,
                                ReingestorOutputFlush::Due,
                            )
                            .await;
                        continue;
                    }
                    work = interaction.next(None) => work,
                };
                let work = match work {
                    Ok(work) => work,
                    Err(error) => {
                        let reason = format!(
                            "reingestor '{}' relay interaction failed: {error}",
                            task_reingestor.as_str()
                        );
                        runtime.handle_internal_processor_error_for_acks(
                            &task_domain,
                            ModelKind::Reingestor,
                            &task_reingestor,
                            &task_error_policies,
                            error.acks(),
                            reason,
                        );
                        continue;
                    }
                };
                let (event, mut work) = work.into_parts();
                match event {
                    RelayInteractionEvent::Stopped(reason) => {
                        debug!(
                            domain = task_domain.as_str(),
                            reingestor = task_reingestor.as_str(),
                            ?reason,
                            "reingestor relay interaction stopped"
                        );
                        break;
                    }
                    RelayInteractionEvent::Wake => {
                        runtime
                            .flush_reingestor_outputs(
                                output_flush_context,
                                &task_output_routes,
                                &mut task_output_buffers,
                                ReingestorOutputFlush::Due,
                            )
                            .await;
                    }
                    RelayInteractionEvent::ForceFlush(completion) => {
                        runtime
                            .flush_reingestor_outputs(
                                output_flush_context,
                                &task_output_routes,
                                &mut task_output_buffers,
                                ReingestorOutputFlush::All,
                            )
                            .await;
                        completion.complete();
                    }
                    RelayInteractionEvent::Command(command) => match command {},
                    RelayInteractionEvent::Batch {
                        relay: input_relay,
                        batch,
                    } => {
                        debug_assert_eq!(input_relay, task_from_relay);
                        let delivery_observation = batch.delivery_observation(current_timestamp());
                        let physical_node_id =
                            runtime.inner.remote_dispatch.local_node_id.read().clone();
                        runtime
                            .inner
                            .metrics
                            .observe_global_node_received(NodeBatchObservation {
                                domain: &task_domain,
                                kind: ModelKind::Reingestor,
                                node: &ModelName::from(&task_reingestor),
                                relay: &task_from_relay,
                                physical_node_id: physical_node_id.as_ref(),
                                messages: batch.message_count(),
                                bytes: batch.estimated_bytes(),
                                domain_timestamp: delivery_observation.domain_timestamp,
                            });
                        runtime.mark_branch_aggregated_metrics_updated(
                            &task_domain,
                            ModelKind::Reingestor,
                            &task_reingestor,
                        );
                        for seconds in delivery_observation.latency_seconds {
                            runtime
                                .inner
                                .metrics
                                .observe_global_delivery_latency_at_domain_time(
                                    NodeLatencyObservation {
                                        domain: &task_domain,
                                        kind: ModelKind::Reingestor,
                                        node: &ModelName::from(&task_reingestor),
                                        relay: &task_from_relay,
                                        physical_node_id: physical_node_id.as_ref(),
                                        seconds,
                                        domain_timestamp: delivery_observation.domain_timestamp,
                                    },
                                );
                        }
                        let dependency_error_acks = batch.acks.clone();
                        let wait_for_required_state = !interaction.is_terminal_drain();
                        let batch = match runtime
                            .resolve_materialized_dependencies_for_batch(
                                &task_domain,
                                &task_from_relay,
                                &task_materialized_state,
                                batch,
                                MaterializedBatchWaitContext {
                                    shutdown_rx: interaction.shutdown_receiver(),
                                    wait_for_required_state,
                                    quiesce_work: work.as_mut(),
                                },
                            )
                            .await
                        {
                            Ok(Some(resolved)) => resolved,
                            Ok(None) => continue,
                            Err(error) => {
                                runtime.handle_internal_processor_error_for_acks(
                                    &task_domain,
                                    ModelKind::Reingestor,
                                    &task_reingestor,
                                    &task_error_policies,
                                    dependency_error_acks.iter(),
                                    format!(
                                        "reingestor '{}' failed to resolve materialized \
                                         dependencies: {error}",
                                        task_reingestor.as_str()
                                    ),
                                );
                                continue;
                            }
                        };
                        let (batch, materialized_values, execution_now) = batch;
                        runtime
                            .dispatch_reingestor_outputs(
                                ReingestorDispatchContext {
                                    domain: &task_domain,
                                    reingestor: &task_reingestor,
                                    from_relay: &task_from_relay,
                                    from_where: task_from_where.as_ref(),
                                    mode: task_mode,
                                    error_policies: &task_error_policies,
                                    branched_senders: &task_branched_senders,
                                    domain_clock: &domain_clock,
                                    execution_now,
                                },
                                &mut compiled_from_where,
                                &mut task_output_routes,
                                &mut task_output_buffers,
                                batch,
                                &materialized_values,
                            )
                            .await;
                    }
                }
            }
            runtime
                .flush_reingestor_outputs(
                    output_flush_context,
                    &task_output_routes,
                    &mut task_output_buffers,
                    ReingestorOutputFlush::All,
                )
                .await;
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc as StdArc, atomic::Ordering};

    use ahash::{HashMap, HashSet};
    use arc_swap::ArcSwapOption;
    use nervix_models::{
        AckMode, CreateReingestor, DomainSchedule, ErrorPolicies, ModelKind, NodeRef, ParseAsType,
        ProcessorInputs, ProcessorOutputs, ReingestorName, RelayName, Timestamp,
    };
    use tokio::{
        sync::{Mutex, mpsc, watch},
        time::{Duration, timeout},
    };
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime::branch_runtime::{
            BranchExecutionDispatchContext, BranchExecutionRuntime, IngestorRouteTask,
        },
        runtime_ack::{AckOutcome, AckSet},
        runtime_schema::{RuntimeValue, test_runtime_row},
    };
    #[tokio::test]
    async fn reingestor_branched_entrypoint_splits_precomputed_keys_with_arrow_filters() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let root_relay = named("tenant_orders");
        let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(
            TWO_ITEM_TEST_CHANNEL_CAPACITY,
        ));
        let mut fan_in =
            RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
        let services = Arc::new(RelayBoundaryServices::new(fanout, 1, 0, Vec::new(), None));
        let registry = RelayRegistry::new();
        let owner_task = runtime.spawn_relay_owner_task(
            &domain,
            &root_relay,
            registry.clone(),
            services.clone(),
            RelayRetention::default(),
        );
        let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::U32)]);
        let template = BranchInstanceTemplate {
            source_kind: ModelKind::Reingestor,
            source: named("tenant_partition"),
            root_relay: root_relay.clone(),
            branch: None,
            branch_ttl: None,
            branch_max_instances: None,
            error_policies: ErrorPolicies::handled_by_log(),
            relays: [(
                root_relay.clone(),
                RelayProcessorRelayTemplate {
                    registry,
                    services: services.clone(),
                },
            )]
            .into_iter()
            .collect(),
            materialized_streams: HashSet::default(),
            processors: HashMap::default(),
        };
        let inputs = [
            RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", "acme"),
                test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    ("value".to_string(), RuntimeValue::U32(1)),
                ]),
                AckSet::empty(),
            )
            .expect("acme batch should build"),
            RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", "beta"),
                test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("beta".to_string()),
                    ),
                    ("value".to_string(), RuntimeValue::U32(2)),
                ]),
                AckSet::empty(),
            )
            .expect("beta batch should build"),
            RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", "acme"),
                test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    ("value".to_string(), RuntimeValue::U32(3)),
                ]),
                AckSet::empty(),
            )
            .expect("second acme batch should build"),
        ];
        let route_runtime = IngestorRouteRuntime::new(
            runtime,
            domain,
            named("tenant_partition"),
            StdArc::new(ArcSwapOption::from(None)),
            IngestorRouteTemplate {
                branch: template,
                ack_boundary: BranchInstanceAckBoundary::Reingestor(AckMode::Attached),
                flush_policy: RuntimeFlushPolicy::Immediate,
            },
            Duration::from_secs(30),
        );
        let expected_message_count = inputs.len();
        for input in inputs {
            route_runtime
                .sender()
                .send(input)
                .await
                .expect("reingestor route should accept input");
        }

        let outputs = timeout(Duration::from_secs(1), async {
            let mut outputs = Vec::new();
            let mut received_message_count = 0;
            while received_message_count < expected_message_count {
                let output = fan_in
                    .recv()
                    .await
                    .expect("runtime consumer should remain open");
                received_message_count += output.batch.batch().num_rows();
                outputs.push(output);
            }
            outputs
        })
        .await
        .expect("all output rows should arrive");

        let mut output_rows = outputs
            .iter()
            .flat_map(|output| {
                (0..output.batch.batch().num_rows()).map(|row| {
                    (
                        key_label(&output.key).to_string(),
                        output
                            .batch
                            .row_to_json_string(row)
                            .expect("output row should serialize"),
                    )
                })
            })
            .collect::<Vec<_>>();
        output_rows.sort();
        assert_eq!(
            output_rows,
            vec![
                (
                    r#"{"tenant":"acme"}"#.to_string(),
                    r#"{"tenant":"acme","value":1}"#.to_string(),
                ),
                (
                    r#"{"tenant":"acme"}"#.to_string(),
                    r#"{"tenant":"acme","value":3}"#.to_string(),
                ),
                (
                    r#"{"tenant":"beta"}"#.to_string(),
                    r#"{"tenant":"beta","value":2}"#.to_string(),
                ),
            ]
        );
        route_runtime.shutdown().await;
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }

    #[tokio::test]
    async fn reingestor_branched_entrypoint_reuses_existing_branches() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let root_relay = named("tenant_orders");
        let services = Arc::new(RelayBoundaryServices::new(
            RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(1)),
            0,
            0,
            Vec::new(),
            None,
        ));
        let registry = RelayRegistry::new();
        let owner_task = runtime.spawn_relay_owner_task(
            &domain,
            &root_relay,
            registry.clone(),
            services.clone(),
            RelayRetention::default(),
        );
        let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::U32)]);
        let template = BranchInstanceTemplate {
            source_kind: ModelKind::Reingestor,
            source: named("tenant_partition"),
            root_relay: root_relay.clone(),
            branch: None,
            branch_ttl: None,
            branch_max_instances: None,
            error_policies: ErrorPolicies::handled_by_log(),
            relays: [(
                root_relay.clone(),
                RelayProcessorRelayTemplate {
                    registry,
                    services: services.clone(),
                },
            )]
            .into_iter()
            .collect(),
            materialized_streams: HashSet::default(),
            processors: HashMap::default(),
        };
        let graph = StdArc::new(ArcSwapOption::from(None));
        let mut instances =
            BranchInstanceRegistry::<Option<BranchKey>, Mutex<BranchRuntime>>::new();
        let (branch_sender, _) = mpsc::channel(1);
        let route_task = IngestorRouteTask {
            runtime_handle: runtime.clone(),
            domain: domain.clone(),
            ingestor: named("tenant_partition"),
            template: IngestorRouteTemplate {
                branch: template.clone(),
                ack_boundary: BranchInstanceAckBoundary::Reingestor(AckMode::Detached),
                flush_policy: RuntimeFlushPolicy::Immediate,
            },
            branch_sender,
            pending: HashMap::default(),
        };

        for round in 0..3 {
            let mut prepared = Vec::new();
            for index in 0..64 {
                let input = RelayRecordBatch::single(
                    schema.clone(),
                    string_branch_key("tenant", &format!("tenant-{index}")),
                    test_runtime_row([
                        (
                            "tenant".to_string(),
                            RuntimeValue::String(format!("tenant-{index}")),
                        ),
                        ("value".to_string(), RuntimeValue::U32(round * 64 + index)),
                    ]),
                    AckSet::empty(),
                )
                .expect("single-branch batch should build");
                prepared.extend(route_task.prepare_input(input).await);
            }
            BranchExecutionRuntime::dispatch_prepared_inputs(
                BranchExecutionDispatchContext {
                    runtime_handle: &runtime,
                    domain: &domain,
                    ingestor: &named("tenant_partition"),
                    graph: &graph,
                    template: &template,
                    now: Timestamp::from_unix_nanos(1_000_000_000 + i64::from(round)),
                },
                &mut instances,
                prepared,
            )
            .await;

            assert_eq!(instances.len(), 64);
        }
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }

    #[tokio::test]
    async fn reingestor_propagates_attached_ack_into_branched_entrypoint() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let relay = named("tenant_orders");
        let output_registry = RelayRegistry::new();
        let output_services = test_relay_boundary_services();
        let owner_task = runtime.spawn_relay_owner_task(
            &domain,
            &relay,
            output_registry.clone(),
            output_services.clone(),
            RelayRetention::default(),
        );
        let mut output_subscription = output_services.subscription_receiver();
        let schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("user_id", ParseAsType::U32),
        ]);
        let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
        let (execution_shutdown, _) = watch::channel(false);
        runtime.inner.executions.insert(
            domain.clone(),
            DomainExecution {
                schedule: DomainSchedule::new(domain.clone(), Vec::new(), Vec::new()),
                passive_only: false,
                start_version: 0,
                domain_clock: test_domain_clock(&domain),
                shutdown: execution_shutdown,
                graph: StdArc::new(ArcSwapOption::empty()),
                relay_registries: HashMap::default(),
                relay_schemas: [
                    (named("orders"), schema.clone()),
                    (named("tenant_orders"), schema.clone()),
                ]
                .into_iter()
                .collect(),
                relay_services: HashMap::default(),
                relay_branchings: [(relay.clone(), vec![named("tenant")])]
                    .into_iter()
                    .collect(),
                relay_branching_schemas: [(relay.clone(), Some(branch_schema))]
                    .into_iter()
                    .collect(),
                materialized_stream_specs: HashMap::default(),
                materialized_stream_owner_nodes: HashMap::default(),
                branched_ingestors: HashMap::default(),
                branched_entrypoints: HashMap::default(),
                codecs: HashMap::default(),
                signaling_protocols: HashMap::default(),
                lookups: HashMap::default(),
                udfs: nervix_roto::UdfExecutor::default(),
                endpoint_routes: HashMap::default(),
                node_tasks: HashMap::default(),
                emitter_tasks: HashMap::default(),
                generator_tasks: HashMap::default(),
                reingestor_tasks: HashMap::default(),
                placement_tasks: HashMap::default(),
                relay_state_tasks: HashMap::default(),
                relay_owner_tasks: HashMap::default(),
                clients: HashMap::default(),
                tasks: Vec::new(),
            },
        );
        let branched_runtime = IngestorRouteRuntime::new(
            runtime.clone(),
            domain.clone(),
            named("tenant_partition"),
            StdArc::new(ArcSwapOption::from(None)),
            IngestorRouteTemplate {
                branch: BranchInstanceTemplate {
                    source_kind: ModelKind::Reingestor,
                    source: named("tenant_partition"),
                    root_relay: relay.clone(),
                    branch: None,
                    branch_ttl: Some(Duration::from_secs(30)),
                    branch_max_instances: None,
                    error_policies: ErrorPolicies::handled_by_log(),
                    relays: [(
                        relay.clone(),
                        RelayProcessorRelayTemplate {
                            registry: output_registry.clone(),
                            services: output_services.clone(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    materialized_streams: HashSet::default(),
                    processors: HashMap::default(),
                },
                ack_boundary: BranchInstanceAckBoundary::Reingestor(AckMode::Attached),
                flush_policy: RuntimeFlushPolicy::Immediate,
            },
            Duration::from_secs(30),
        );
        assert_eq!(
            branched_runtime.sender().max_capacity(),
            STUPID_CHANNEL_CAPACITY_REMOVE_ME.get()
        );
        let (shutdown_tx, _) = watch::channel(false);
        let broadcast = RelayBroadcast::with_capacity(STUPID_CHANNEL_CAPACITY_REMOVE_ME);
        let fan_in = RelayRuntimeFanIn::new(broadcast.new_receiver());
        let mut branched_entrypoint_senders = HashMap::default();
        branched_entrypoint_senders.insert(relay, branched_runtime.sender());
        let task = runtime
            .spawn_reingestor_task(
                &domain,
                &shutdown_tx,
                &branched_entrypoint_senders,
                CreateReingestor {
                    name: named("tenant_partition"),
                    from: ProcessorInputs::single(named("orders")),
                    output_routes: with_inherit_all(ProcessorOutputs::single(named(
                        "tenant_orders",
                    )))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "500ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    })
                    .with_branch(branched_by("tenant_orders", &["tenant"])),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                },
                named("orders"),
                fan_in,
            )
            .expect("reingestor task should spawn");
        let (acme_acks, acme_completion) = AckSet::root();
        let (beta_acks, beta_completion) = AckSet::root();
        let mut acme_completion = Box::pin(acme_completion.wait());
        let mut beta_completion = Box::pin(beta_completion.wait());
        let output_counters = runtime.node_quiesce_counters(
            &domain,
            NodeRef::new(
                ModelKind::Reingestor,
                named::<ReingestorName>("tenant_partition"),
            ),
        );
        let input_batch = |tenant: &str, user_id, acks| {
            RelayRecordBatch::single(
                schema.clone(),
                None,
                test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String(tenant.to_string()),
                    ),
                    ("user_id".to_string(), RuntimeValue::U32(user_id)),
                ]),
                acks,
            )
            .expect("input batch should build")
        };
        broadcast
            .broadcast(input_batch("acme", 42, acme_acks.attached()))
            .await
            .expect("acme message should broadcast");
        acme_acks.ack_success();
        timeout(Duration::from_secs(1), async {
            while output_counters.output_buffers.load(Ordering::Acquire) != 1 {
                tokio::task::consume_budget().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("acme output should enter its branch buffer");

        sleep(Duration::from_millis(200)).await;
        broadcast
            .broadcast(input_batch("beta", 7, beta_acks.attached()))
            .await
            .expect("beta message should broadcast");
        beta_acks.ack_success();
        timeout(Duration::from_secs(1), async {
            while output_counters.output_buffers.load(Ordering::Acquire) != 2 {
                tokio::task::consume_budget().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("beta output should enter its branch buffer");

        assert!(
            timeout(Duration::from_millis(1), &mut beta_completion)
                .await
                .is_err(),
            "attached beta ACK must remain pending with the buffered output"
        );

        let first_output_batch = timeout(Duration::from_secs(1), output_subscription.recv())
            .await
            .expect("first output subscription should receive")
            .expect("output subscription should stay open");
        assert_eq!(
            row_value(
                &first_output_batch
                    .runtime_row(0)
                    .expect("first output should contain an Arrow row"),
                "tenant",
            ),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            timeout(Duration::from_secs(1), &mut acme_completion)
                .await
                .expect("acme ack completion should resolve after output dispatch"),
            AckOutcome::Ack
        );
        assert!(
            timeout(Duration::from_millis(100), output_subscription.recv())
                .await
                .is_err(),
            "beta branch must retain its independent flush deadline"
        );
        assert!(
            timeout(Duration::from_millis(1), &mut beta_completion)
                .await
                .is_err(),
            "beta ACK must remain pending until the beta branch flushes"
        );
        let second_output_batch = timeout(Duration::from_secs(1), output_subscription.recv())
            .await
            .expect("second output subscription should receive")
            .expect("output subscription should stay open");
        assert_eq!(
            row_value(
                &second_output_batch
                    .runtime_row(0)
                    .expect("second output should contain an Arrow row"),
                "tenant",
            ),
            Some(RuntimeValue::String("beta".to_string()))
        );
        assert_eq!(
            timeout(Duration::from_secs(1), &mut beta_completion)
                .await
                .expect("beta ack completion should resolve after output dispatch"),
            AckOutcome::Ack
        );

        let _ = shutdown_tx.send(true);
        let _ = task.await;
        branched_runtime.shutdown().await;
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }

    #[tokio::test]
    async fn reingestor_force_and_shutdown_flush_buffered_routes() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let input_relay = named::<RelayName>("orders");
        let output_relay = named::<RelayName>("tenant_orders");
        let reingestor = named::<ReingestorName>("tenant_partition");
        let schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("user_id", ParseAsType::U32),
        ]);
        let (execution_shutdown, _) = watch::channel(false);
        runtime.inner.executions.insert(
            domain.clone(),
            DomainExecution {
                schedule: DomainSchedule::new(domain.clone(), Vec::new(), Vec::new()),
                passive_only: false,
                start_version: 0,
                domain_clock: test_domain_clock(&domain),
                shutdown: execution_shutdown,
                graph: StdArc::new(ArcSwapOption::empty()),
                relay_registries: HashMap::default(),
                relay_schemas: [
                    (input_relay.clone(), schema.clone()),
                    (output_relay.clone(), schema.clone()),
                ]
                .into_iter()
                .collect(),
                relay_services: HashMap::default(),
                relay_branchings: HashMap::default(),
                relay_branching_schemas: HashMap::default(),
                materialized_stream_specs: HashMap::default(),
                materialized_stream_owner_nodes: HashMap::default(),
                branched_ingestors: HashMap::default(),
                branched_entrypoints: HashMap::default(),
                codecs: HashMap::default(),
                signaling_protocols: HashMap::default(),
                lookups: HashMap::default(),
                udfs: nervix_roto::UdfExecutor::default(),
                endpoint_routes: HashMap::default(),
                node_tasks: HashMap::default(),
                emitter_tasks: HashMap::default(),
                generator_tasks: HashMap::default(),
                reingestor_tasks: HashMap::default(),
                placement_tasks: HashMap::default(),
                relay_state_tasks: HashMap::default(),
                relay_owner_tasks: HashMap::default(),
                clients: HashMap::default(),
                tasks: Vec::new(),
            },
        );
        let (shutdown_tx, _) = watch::channel(false);
        let broadcast = RelayBroadcast::with_capacity(nonzero_capacity(4));
        let fan_in = RelayRuntimeFanIn::new(broadcast.new_receiver());
        let (output_tx, mut output_rx) = mpsc::channel(4);
        let task = runtime
            .spawn_reingestor_task(
                &domain,
                &shutdown_tx,
                &[(output_relay.clone(), output_tx)].into_iter().collect(),
                CreateReingestor {
                    name: reingestor.clone(),
                    from: ProcessorInputs::single(input_relay.clone()),
                    output_routes: with_inherit_all(ProcessorOutputs::single(output_relay.clone()))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "10s".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                },
                input_relay,
                fan_in,
            )
            .expect("reingestor task should spawn");
        let input_batch = |user_id, acks| {
            RelayRecordBatch::single(
                schema.clone(),
                None,
                test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    ("user_id".to_string(), RuntimeValue::U32(user_id)),
                ]),
                acks,
            )
            .expect("input batch should build")
        };

        let (first_acks, first_completion) = AckSet::root();
        let mut first_completion = Box::pin(first_completion.wait());
        broadcast
            .broadcast(input_batch(1, first_acks.attached()))
            .await
            .expect("first input should broadcast");
        first_acks.ack_success();
        assert!(
            timeout(Duration::from_millis(20), output_rx.recv())
                .await
                .is_err(),
            "long-cadence reingestor output must remain buffered"
        );
        assert!(
            timeout(Duration::from_millis(1), &mut first_completion)
                .await
                .is_err(),
            "force-flush input ACK must remain pending with buffered output"
        );

        runtime.force_flush_domain(&domain);
        let forced = timeout(Duration::from_secs(1), output_rx.recv())
            .await
            .expect("force flush should publish buffered output")
            .expect("reingestor output should remain open");
        assert_eq!(
            row_value(
                &forced
                    .runtime_row(0)
                    .expect("forced output should contain an Arrow row"),
                "user_id",
            ),
            Some(RuntimeValue::U32(1))
        );
        forced.ack_success();
        assert_eq!(
            timeout(Duration::from_secs(1), &mut first_completion)
                .await
                .expect("force-flushed ACK should resolve"),
            AckOutcome::Ack
        );
        timeout(Duration::from_secs(1), async {
            loop {
                tokio::task::consume_budget().await;
                let pending = runtime
                    .inner
                    .force_flush_by_domain
                    .get(&domain)
                    .map(|force_flush| force_flush.pending())
                    .unwrap_or_default();
                if pending == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("force-flush generation should complete after output publication");

        let (second_acks, second_completion) = AckSet::root();
        let mut second_completion = Box::pin(second_completion.wait());
        broadcast
            .broadcast(input_batch(2, second_acks.attached()))
            .await
            .expect("second input should broadcast");
        second_acks.ack_success();
        assert!(
            timeout(Duration::from_millis(20), &mut second_completion)
                .await
                .is_err(),
            "shutdown input ACK must remain pending while its output is buffered"
        );
        shutdown_tx
            .send(true)
            .expect("reingestor shutdown receiver should remain open");
        timeout(Duration::from_secs(1), task)
            .await
            .expect("reingestor should stop after draining input")
            .expect("reingestor task should not panic");
        let stopped = timeout(Duration::from_secs(1), output_rx.recv())
            .await
            .expect("shutdown should publish buffered output")
            .expect("reingestor output should remain open");
        assert_eq!(
            row_value(
                &stopped
                    .runtime_row(0)
                    .expect("shutdown output should contain an Arrow row"),
                "user_id",
            ),
            Some(RuntimeValue::U32(2))
        );
        stopped.ack_success();
        assert_eq!(
            timeout(Duration::from_secs(1), &mut second_completion)
                .await
                .expect("shutdown-flushed ACK should resolve"),
            AckOutcome::Ack
        );
        assert_eq!(
            runtime
                .node_quiesce_counters(&domain, NodeRef::new(ModelKind::Reingestor, &reingestor))
                .output_buffers
                .load(Ordering::Acquire),
            0,
            "reingestor output gauge must be cleared when the task exits"
        );
    }
}
