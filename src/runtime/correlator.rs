use super::*;

pub(super) fn compile_correlator_where_program(
    processor: &ModelName,
    correlate_where: &nervix_models::Expression,
    left_relays: &[RelayName],
    left_schema: StdArc<arrow_schema::Schema>,
    right_relays: &[RelayName],
    right_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledCorrelatorWhereProgram, String> {
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(correlate_where.clone()),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new(
            "__invalid_correlator_bare_read",
            "__invalid_correlator_target",
        ),
    )
    .map_err(|reason| {
        format!(
            "correlator '{}' CORRELATE WHERE is invalid: {}",
            processor.as_str(),
            reason
        )
    })?;
    if left_relays.is_empty() || right_relays.is_empty() {
        return Err(format!(
            "correlator '{}' requires both LEFT and RIGHT inputs",
            processor.as_str()
        ));
    }
    let bindings = vec![
        VmCompileBinding::writable("left", left_schema.clone()),
        VmCompileBinding::readonly("right", right_schema.clone()),
    ];
    let program = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        left_schema.clone(),
        VmSchemaSensitivity::default(),
        bindings,
        runtime_udf_compile_options(udfs, VmCompileOptions::default()),
    )
    .map_err(|error| {
        format!(
            "correlator '{}' CORRELATE WHERE compile failed: {}",
            processor.as_str(),
            error.message
        )
    })?;
    Ok(CompiledCorrelatorWhereProgram {
        program: Arc::new(program),
    })
}

pub(super) struct CorrelatorOutputCompileContext<'a> {
    pub(super) processor: &'a ModelName,
    pub(super) left_schema: StdArc<arrow_schema::Schema>,
    pub(super) left_sensitivity: VmSchemaSensitivity,
    pub(super) right_schema: StdArc<arrow_schema::Schema>,
    pub(super) right_sensitivity: VmSchemaSensitivity,
    pub(super) output_relay: &'a RelayName,
    pub(super) output_schema: StdArc<arrow_schema::Schema>,
    pub(super) output_sensitivity: VmSchemaSensitivity,
    pub(super) construction: &'a RouteConstruction,
    pub(super) runtime: RuntimeVmCompileContext<'a>,
}

impl CorrelatorOutputCompileContext<'_> {
    pub(super) fn compile(self) -> Result<CompiledCorrelatorOutputProgram, String> {
        let parsed = lower_route_construction(
            self.construction,
            SemanticNamespaces::new("__invalid_correlator_bare_read", "output"),
        )?;
        if !parsed.inner.invoke.is_empty() || parsed.inner.set.is_empty() {
            return Err(format!(
                "correlator '{}' TO output '{}' must contain SET assignments and may contain WHERE",
                self.processor.as_str(),
                self.output_relay.as_str()
            ));
        }
        let error_sites = compiled_message_error_sites(
            &parsed,
            &vec![MessageErrorOperation::Set; parsed.inner.set.len()],
            Some(MessageErrorOperation::RouteWhere),
        )?;
        let original_parsed = parsed.clone();
        let mut bindings = vec![
            VmCompileBinding::readonly("left", self.left_schema.clone())
                .with_sensitivity(self.left_sensitivity),
            VmCompileBinding::readonly("right", self.right_schema.clone())
                .with_sensitivity(self.right_sensitivity),
            VmCompileBinding::writeonly("output", self.output_schema.clone())
                .with_sensitivity(self.output_sensitivity.clone()),
        ];
        if let Some(binding) = self.runtime.branch_binding() {
            bindings.push(binding);
        }
        let local_namespaces = HashSet::from_iter([
            "left".to_string(),
            "right".to_string(),
            "output".to_string(),
            BRANCH_NAMESPACE.to_string(),
        ]);
        let (materialized_bindings, materialized_interest) =
            referenced_materialized_stream_bindings(
                &original_parsed,
                &local_namespaces,
                self.runtime.available_materialized_streams,
                self.runtime.current_branching,
            )?;
        bindings.extend(materialized_bindings);
        let (parsed, pending_lookup_calls) =
            rewrite_lookup_hash_map_program(&parsed, self.runtime.available_lookups)?;
        let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
            pending_lookup_calls,
            "output",
            &bindings,
            self.runtime.udfs,
        )?;
        if let Some(lookup_binding) = lookup_binding {
            bindings.push(lookup_binding);
        }
        let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            self.output_schema.clone(),
            self.output_sensitivity.clone(),
            bindings,
            self.runtime.compile_options(VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                ..VmCompileOptions::default()
            }),
        )
        .map_err(|error| {
            format!(
                "correlator '{}' TO output '{}' compile failed: {}",
                self.processor.as_str(),
                self.output_relay.as_str(),
                error.message
            )
        })?;
        Ok(CompiledCorrelatorOutputProgram {
            program: CompiledProgramWithMaterializedInterest {
                compiled: Arc::new(compiled),
                output_sensitivity: self.output_sensitivity,
                materialized_interest,
                output_namespace_input: OutputNamespaceInput::Uninitialized,
                lookup_hash_maps,
                error_sites,
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CorrelatorSide {
    Left,
    Right,
}

#[cfg(test)]
pub(super) static CORRELATOR_WHERE_VM_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(super) static CORRELATOR_OUTPUT_VM_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
pub(super) struct CorrelatorMaterializedState {
    pub(super) left: Arc<HashMap<String, RuntimeValue>>,
    pub(super) right: Arc<HashMap<String, RuntimeValue>>,
}

impl CorrelatorMaterializedState {
    pub(super) fn value(&self, name: &str) -> Option<&RuntimeValue> {
        self.right.get(name).or_else(|| self.left.get(name))
    }

    pub(super) fn snapshot(&self) -> HashMap<String, RuntimeValue> {
        if Arc::ptr_eq(&self.left, &self.right) {
            return self.right.as_ref().clone();
        }
        let mut snapshot = self.left.as_ref().clone();
        snapshot.extend(
            self.right
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        );
        snapshot
    }
}

pub(super) struct CorrelatorMatchedBatch {
    pub(super) carrier: Arc<RuntimeRecordBatch>,
    pub(super) keys: Vec<Option<BranchKey>>,
    pub(super) metadata: Vec<RuntimeRecordMetadata>,
    pub(super) materialized_state: Vec<CorrelatorMaterializedState>,
}

impl CorrelatorMatchedBatch {
    pub(super) fn from_correlations(
        correlations: &[(CorrelatorPendingMessage, CorrelatorPendingMessage)],
        programs: &[&CompiledCorrelatorOutputProgram],
    ) -> Result<Self, String> {
        if correlations.is_empty() {
            return Err("cannot batch zero correlator matches".to_string());
        }
        if correlations
            .iter()
            .any(|(left, right)| left.message.key != right.message.key)
        {
            return Err("correlator match cannot combine different branch keys".to_string());
        }
        let left_rows = correlations
            .iter()
            .map(|(left, _)| &left.message.record)
            .collect::<Vec<_>>();
        let right_rows = correlations
            .iter()
            .map(|(_, right)| &right.message.record)
            .collect::<Vec<_>>();
        let left = RuntimeRecordBatch::from_rows(
            left_rows[0].batch().schema(),
            left_rows.iter().copied(),
        )?;
        let right = RuntimeRecordBatch::from_rows(
            right_rows[0].batch().schema(),
            right_rows.iter().copied(),
        )?;
        let materialized_state = correlations
            .iter()
            .map(|(left, right)| CorrelatorMaterializedState {
                left: left.materialized_state.clone(),
                right: right.materialized_state.clone(),
            })
            .collect::<Vec<_>>();
        let materialized_fields = Self::materialized_fields(programs)?;
        let carrier = Arc::new(correlator_input_batch(
            &left,
            &right,
            &materialized_fields,
            &materialized_state,
        )?);
        Ok(Self {
            carrier,
            keys: correlations
                .iter()
                .map(|(left, _)| left.message.key.clone())
                .collect(),
            metadata: correlations
                .iter()
                .map(|(left, right)| {
                    correlator_output_metadata(
                        left.message.record.metadata(),
                        right.message.record.metadata(),
                    )
                })
                .collect(),
            materialized_state,
        })
    }

    pub(super) fn materialized_fields(
        programs: &[&CompiledCorrelatorOutputProgram],
    ) -> Result<Vec<StdArc<arrow_schema::Field>>, String> {
        let mut fields = BTreeMap::<String, StdArc<arrow_schema::Field>>::new();
        for program in programs {
            let schemas = std::iter::once(&program.program.compiled.input_schema).chain(
                program
                    .program
                    .lookup_hash_maps
                    .iter()
                    .map(|call| &call.key_program.input_schema),
            );
            for schema in schemas {
                for field in schema
                    .fields()
                    .iter()
                    .filter(|field| field.name().starts_with("relay_state."))
                {
                    if let Some(existing) = fields.get(field.name())
                        && existing.as_ref() != field.as_ref()
                    {
                        return Err(format!(
                            "correlator materialized input '{}' has conflicting Arrow fields",
                            field.name()
                        ));
                    }
                    fields.insert(field.name().clone(), field.clone());
                }
            }
        }
        Ok(fields.into_values().collect())
    }

    pub(super) fn row_count(&self) -> usize {
        self.carrier.batch().num_rows()
    }

    pub(super) fn source_message(&self, row: usize, acks: AckSet) -> Result<RelayMessage, String> {
        let metadata = self.metadata.get(row).cloned().ok_or_else(|| {
            format!(
                "correlator output row {row} is outside {} metadata rows",
                self.metadata.len()
            )
        })?;
        let key = self.keys.get(row).cloned().ok_or_else(|| {
            format!(
                "correlator output row {row} is outside {} branch keys",
                self.keys.len()
            )
        })?;
        Ok(RelayMessage {
            key,
            record: RuntimeRow::new(self.carrier.clone(), row, metadata)?,
            acks,
        })
    }
}

pub(super) fn take_correlator_opposite_pending(
    state: &mut CorrelatorBranchState,
    incoming_side: CorrelatorSide,
) -> Vec<CorrelatorPendingMessage> {
    match incoming_side {
        CorrelatorSide::Left => std::mem::take(&mut state.pending_right),
        CorrelatorSide::Right => std::mem::take(&mut state.pending_left),
    }
}

pub(super) fn restore_correlator_opposite_pending(
    state: &mut CorrelatorBranchState,
    incoming_side: CorrelatorSide,
    mut pending: Vec<CorrelatorPendingMessage>,
) {
    match incoming_side {
        CorrelatorSide::Left => {
            pending.extend(std::mem::take(&mut state.pending_right));
            state.pending_right = pending;
        }
        CorrelatorSide::Right => {
            pending.extend(std::mem::take(&mut state.pending_left));
            state.pending_left = pending;
        }
    }
}

pub(super) fn store_correlator_unmatched_incoming(
    state: &mut CorrelatorBranchState,
    incoming_side: CorrelatorSide,
    incoming: CorrelatorPendingMessage,
    mut opposite_pending: Vec<CorrelatorPendingMessage>,
) {
    match incoming_side {
        CorrelatorSide::Left => {
            opposite_pending.extend(std::mem::take(&mut state.pending_right));
            state.pending_right = opposite_pending;
            state.pending_left.push(incoming);
        }
        CorrelatorSide::Right => {
            opposite_pending.extend(std::mem::take(&mut state.pending_left));
            state.pending_left = opposite_pending;
            state.pending_right.push(incoming);
        }
    }
}

pub(super) async fn correlate_incoming_message(
    processor: &ModelName,
    program: &CompiledCorrelatorWhereProgram,
    incoming_side: CorrelatorSide,
    match_policy: CorrelatorMatchPolicy,
    state: &mut CorrelatorBranchState,
    incoming: CorrelatorPendingMessage,
    execution_now: Timestamp,
) -> Result<Option<(CorrelatorPendingMessage, CorrelatorPendingMessage)>, (String, Vec<AckSet>)> {
    let opposite_pending = take_correlator_opposite_pending(state, incoming_side);
    if opposite_pending.is_empty() {
        store_correlator_unmatched_incoming(state, incoming_side, incoming, opposite_pending);
        return Ok(None);
    }
    let evaluated = evaluate_correlator_where_matches(
        processor,
        program,
        incoming_side,
        &incoming,
        &opposite_pending,
        execution_now,
    )
    .await?;

    let mut matching = Vec::new();
    let mut remaining = Vec::new();
    for (pending, matched) in opposite_pending.into_iter().zip(evaluated) {
        if matched {
            matching.push(pending);
        } else {
            remaining.push(pending);
        }
    }

    if matching.is_empty() {
        store_correlator_unmatched_incoming(state, incoming_side, incoming, remaining);
        return Ok(None);
    }

    let selected_index = match match_policy {
        CorrelatorMatchPolicy::Earliest => 0,
        CorrelatorMatchPolicy::Latest => matching.len() - 1,
    };
    let selected = matching.remove(selected_index);
    for duplicate in matching {
        duplicate.message.acks.ack_success();
    }
    restore_correlator_opposite_pending(state, incoming_side, remaining);

    Ok(Some(match incoming_side {
        CorrelatorSide::Left => (incoming, selected),
        CorrelatorSide::Right => (selected, incoming),
    }))
}

pub(super) async fn evaluate_correlator_where_matches(
    processor: &ModelName,
    program: &CompiledCorrelatorWhereProgram,
    incoming_side: CorrelatorSide,
    incoming: &CorrelatorPendingMessage,
    candidates: &[CorrelatorPendingMessage],
    execution_now: Timestamp,
) -> Result<Vec<bool>, (String, Vec<AckSet>)> {
    let error_acks = || {
        vec![AckSet::merged(
            std::iter::once(incoming.message.acks.attached()).chain(
                candidates
                    .iter()
                    .map(|candidate| candidate.message.acks.attached()),
            ),
        )]
    };
    let incoming_rows =
        std::iter::repeat_n(&incoming.message.record, candidates.len()).collect::<Vec<_>>();
    let candidate_rows = candidates
        .iter()
        .map(|candidate| &candidate.message.record)
        .collect::<Vec<_>>();
    let (left_rows, right_rows) = match incoming_side {
        CorrelatorSide::Left => (&incoming_rows, &candidate_rows),
        CorrelatorSide::Right => (&candidate_rows, &incoming_rows),
    };
    let Some(first_left) = left_rows.first() else {
        return Ok(Vec::new());
    };
    let left =
        RuntimeRecordBatch::from_rows(first_left.batch().schema(), left_rows.iter().copied())
            .map_err(|error| {
                (
                    format!(
                        "correlator '{}' failed to build batched LEFT CORRELATE WHERE input: {}",
                        processor.as_str(),
                        error
                    ),
                    error_acks(),
                )
            })?;
    let Some(first_right) = right_rows.first() else {
        return Ok(Vec::new());
    };
    let right =
        RuntimeRecordBatch::from_rows(first_right.batch().schema(), right_rows.iter().copied())
            .map_err(|error| {
                (
                    format!(
                        "correlator '{}' failed to build batched RIGHT CORRELATE WHERE input: {}",
                        processor.as_str(),
                        error
                    ),
                    error_acks(),
                )
            })?;
    let keys = match incoming_side {
        CorrelatorSide::Left => vec![incoming.message.key.clone(); candidates.len()],
        CorrelatorSide::Right => candidates
            .iter()
            .map(|candidate| candidate.message.key.clone())
            .collect(),
    };
    let side_inputs = HashMap::default();
    let lookup_columns = HashMap::default();
    let input = project_vm_input_batch(
        &program.program.input_schema,
        &VmInputProjectionSources {
            carrier: &left,
            namespace_batches: &[("left", &left), ("right", &right)],
            strict_namespaces: &["left", "right"],
            keys: &keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: None,
        },
        None,
    )
    .map_err(|error| {
        (
            format!(
                "correlator '{}' failed to project CORRELATE WHERE input batch: {}",
                processor.as_str(),
                error
            ),
            error_acks(),
        )
    })?;
    #[cfg(test)]
    CORRELATOR_WHERE_VM_EXECUTIONS.fetch_add(1, Ordering::Relaxed);
    let result = execute_program_with_selection_in_context(
        &program.program,
        &input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| {
        (
            format!(
                "correlator '{}' failed to evaluate CORRELATE WHERE: {}",
                processor.as_str(),
                error
            ),
            error_acks(),
        )
    })?;
    let mut matching = vec![false; candidates.len()];
    for row in result.selected_rows.iter() {
        let Some(matched) = matching.get_mut(row) else {
            return Err((
                format!(
                    "correlator '{}' CORRELATE WHERE selected row {} outside its {} candidate \
                     pairs",
                    processor.as_str(),
                    row,
                    candidates.len()
                ),
                error_acks(),
            ));
        };
        *matched = true;
    }
    Ok(matching)
}

pub(super) fn correlator_input_batch(
    left: &RuntimeRecordBatch,
    right: &RuntimeRecordBatch,
    materialized_fields: &[StdArc<arrow_schema::Field>],
    materialized_state: &[CorrelatorMaterializedState],
) -> Result<RuntimeRecordBatch, String> {
    let row_count = left.batch().num_rows();
    if right.batch().num_rows() != row_count || materialized_state.len() != row_count {
        return Err(format!(
            "correlator input has {row_count} left rows, {} right rows, and {} materialized-state \
             rows",
            right.batch().num_rows(),
            materialized_state.len()
        ));
    }
    let mut fields = Vec::with_capacity(
        left.schema().fields().len() + right.schema().fields().len() + materialized_fields.len(),
    );
    let mut columns = Vec::with_capacity(fields.capacity());
    for (namespace, batch) in [("left", left), ("right", right)] {
        for (index, field) in batch.schema().fields().iter().enumerate() {
            fields.push(StdArc::new(arrow_schema::Field::new(
                format!("{namespace}.{}", field.name()),
                field.data_type().clone(),
                field.is_nullable(),
            )));
            columns.push(batch.batch().column(index).clone());
        }
    }
    for field in materialized_fields {
        let column = runtime_values_input_column(
            materialized_state
                .iter()
                .map(|state| state.value(field.name())),
            row_count,
            field,
        )?;
        fields.push(field.clone());
        columns.push(column.to_array_ref());
    }
    let schema = StdArc::new(arrow_schema::Schema::new(fields));
    let batch = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
    } else {
        RecordBatch::try_new(schema.clone(), columns)
    }
    .map_err(|error| error.to_string())?;
    RuntimeRecordBatch::from_record_batch(schema, batch)
}

pub(super) fn correlator_output_metadata(
    left: &RuntimeRecordMetadata,
    right: &RuntimeRecordMetadata,
) -> RuntimeRecordMetadata {
    RuntimeRecordMetadata::from_ingested_at_watermarks(
        left.ingested_at_low_watermark()
            .min(right.ingested_at_low_watermark()),
        left.ingested_at_high_watermark()
            .max(right.ingested_at_high_watermark()),
    )
}

pub(super) type CorrelatorOutputOutcome = Result<Option<RelayMessage>, Box<PlannedMessageError>>;

pub(super) fn correlator_output_batch_errors(
    processor: &ModelName,
    matched: &CorrelatorMatchedBatch,
    acks: Vec<AckSet>,
    code: MessageErrorCode,
    reason: &str,
    operation: MessageErrorOperation,
) -> Vec<CorrelatorOutputOutcome> {
    acks.into_iter()
        .enumerate()
        .map(|(row, acks)| {
            let source = matched.source_message(row, acks).verified(
                "the correlator output batch is built row-aligned with the ACKs it was given",
            );
            Err(Box::new(planned_structured_message_error(
                source,
                structured_message_error(
                    code,
                    format!("correlator '{}' {reason}", processor.as_str()),
                    operation,
                    None,
                    std::iter::empty(),
                ),
                None,
                matched.materialized_state[row].snapshot(),
            )))
        })
        .collect()
}

pub(super) async fn evaluate_correlator_output_batch(
    processor: &ModelName,
    program: &CompiledCorrelatorOutputProgram,
    matched: &CorrelatorMatchedBatch,
    acks: Vec<AckSet>,
    execution_now: Timestamp,
) -> Result<Vec<CorrelatorOutputOutcome>, (String, Vec<AckSet>)> {
    let row_count = matched.row_count();
    if matched.keys.len() != row_count
        || matched.metadata.len() != row_count
        || matched.materialized_state.len() != row_count
        || acks.len() != row_count
    {
        return Err((
            format!(
                "correlator output has {row_count} Arrow rows, {} branch keys, {} metadata rows, \
                 {} materialized-state rows, and {} ACK sets",
                matched.keys.len(),
                matched.metadata.len(),
                matched.materialized_state.len(),
                acks.len()
            ),
            acks,
        ));
    }
    let side_inputs = HashMap::default();
    let lookup_columns = match compute_lookup_hash_map_columns(
        &program.program,
        &FilterMapBatchInputs {
            carrier: &matched.carrier,
            namespace_batches: &[],
            keys: &matched.keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
        },
        execution_now,
        None,
    )
    .await
    {
        Ok(columns) => columns,
        Err(error) => {
            return Ok(correlator_output_batch_errors(
                processor,
                matched,
                acks,
                MessageErrorCode::Evaluation,
                &format!("failed to prepare TO output lookup inputs: {error}"),
                MessageErrorOperation::Set,
            ));
        }
    };
    let uninitialized = VmUninitializedInput {
        fields: program
            .program
            .compiled
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("output."))
            .map(|field| field.name().clone())
            .collect(),
    };
    let input = match project_vm_input_batch(
        &program.program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: &matched.carrier,
            namespace_batches: &[],
            strict_namespaces: &["left", "right"],
            keys: &matched.keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized),
        },
        None,
    ) {
        Ok(input) => input,
        Err(error) => {
            return Ok(correlator_output_batch_errors(
                processor,
                matched,
                acks,
                MessageErrorCode::Internal,
                &format!("failed to build TO output input batch: {error}"),
                MessageErrorOperation::Set,
            ));
        }
    };
    #[cfg(test)]
    CORRELATOR_OUTPUT_VM_EXECUTIONS.fetch_add(1, Ordering::Relaxed);
    let result = match execute_program_with_selection_in_context(
        &program.program.compiled,
        &input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return Ok(correlator_output_batch_errors(
                processor,
                matched,
                acks,
                MessageErrorCode::Internal,
                &format!("failed to evaluate TO output: {error}"),
                MessageErrorOperation::Set,
            ));
        }
    };
    if result.selected_rows.len() != result.batch.row_count() {
        return Ok(correlator_output_batch_errors(
            processor,
            matched,
            acks,
            MessageErrorCode::Internal,
            &format!(
                "TO output produced {} rows for {} selected correlations",
                result.batch.row_count(),
                result.selected_rows.len()
            ),
            MessageErrorOperation::Finalize,
        ));
    }
    let mut seen = vec![false; row_count];
    for input_row in result.selected_rows.iter() {
        let Some(selected) = seen.get_mut(input_row) else {
            return Ok(correlator_output_batch_errors(
                processor,
                matched,
                acks,
                MessageErrorCode::Internal,
                &format!("TO output selected row {input_row} outside its {row_count} correlations"),
                MessageErrorOperation::Finalize,
            ));
        };
        if *selected {
            return Ok(correlator_output_batch_errors(
                processor,
                matched,
                acks,
                MessageErrorCode::Internal,
                &format!("TO output selected correlation row {input_row} more than once"),
                MessageErrorOperation::Finalize,
            ));
        }
        *selected = true;
    }
    let mut pending_acks = acks.into_iter().map(Some).collect::<Vec<_>>();
    let mut outcomes = (0..row_count).map(|_| None).collect::<Vec<_>>();
    /// One correlation that produced output: the row of the VM output batch it landed on, the
    /// input row it correlates, and the message that carries that input row's ACKs.
    struct SuccessfulCorrelation {
        output_row: usize,
        input_row: usize,
        source: RelayMessage,
    }

    let mut successful = Vec::<SuccessfulCorrelation>::new();
    for (output_row, input_row) in result.selected_rows.iter().enumerate() {
        let acks = pending_acks[input_row]
            .take()
            .verified("the selection lists each input row at most once, so its ACK is taken once");
        let source = matched.source_message(input_row, acks).verified(
            "the correlator output batch is built row-aligned with the ACKs it was given",
        );
        if let Some(side_error) = result.batch.errors().row(output_row).first() {
            outcomes[input_row] = Some(Err(Box::new(planned_structured_message_error(
                source,
                program.program.structured_side_error(
                    format!(
                        "correlator '{}' TO output side error {}: {} at {}",
                        processor.as_str(),
                        side_error.code.as_str(),
                        side_error.message,
                        side_error.span
                    ),
                    side_error.span,
                    MessageErrorOperation::Set,
                ),
                captured_partial_output(&result.batch, output_row),
                matched.materialized_state[input_row].snapshot(),
            ))));
            continue;
        }
        let invalid_fields = invalid_output_fields(&result.batch, output_row);
        if !invalid_fields.is_empty() {
            outcomes[input_row] = Some(Err(Box::new(planned_structured_message_error(
                source,
                structured_message_error(
                    MessageErrorCode::Validation,
                    format!(
                        "correlator '{}' failed to finalize TO output row",
                        processor.as_str()
                    ),
                    MessageErrorOperation::Finalize,
                    None,
                    invalid_fields,
                ),
                captured_partial_output(&result.batch, output_row),
                matched.materialized_state[input_row].snapshot(),
            ))));
            continue;
        }
        successful.push(SuccessfulCorrelation {
            output_row,
            input_row,
            source,
        });
    }
    for (input_row, acks) in pending_acks.into_iter().enumerate() {
        if let Some(acks) = acks {
            acks.ack_success();
            outcomes[input_row] = Some(Ok(None));
        }
    }

    if !successful.is_empty() {
        let output_rows = successful
            .iter()
            .map(|correlation| correlation.output_row)
            .collect::<Vec<_>>();
        match vm_typed_batch_selected_rows_to_runtime_batch(&result.batch, &output_rows) {
            Ok(output) => {
                let output = Arc::new(output);
                for (output_row, correlation) in successful.into_iter().enumerate() {
                    let input_row = correlation.input_row;
                    match RuntimeRow::new(
                        output.clone(),
                        output_row,
                        matched.metadata[input_row].clone(),
                    ) {
                        Ok(record) => {
                            let RelayMessage { key, acks, .. } = correlation.source;
                            outcomes[input_row] =
                                Some(Ok(Some(RelayMessage { key, record, acks })));
                        }
                        Err(error) => {
                            outcomes[input_row] =
                                Some(Err(Box::new(planned_structured_message_error(
                                    correlation.source,
                                    structured_message_error(
                                        MessageErrorCode::Internal,
                                        error,
                                        MessageErrorOperation::Finalize,
                                        None,
                                        std::iter::empty(),
                                    ),
                                    None,
                                    matched.materialized_state[input_row].snapshot(),
                                ))));
                        }
                    }
                }
            }
            Err(error) => {
                for correlation in successful {
                    let output_row = correlation.output_row;
                    let input_row = correlation.input_row;
                    outcomes[input_row] = Some(Err(Box::new(planned_structured_message_error(
                        correlation.source,
                        structured_message_error(
                            MessageErrorCode::Validation,
                            format!(
                                "correlator '{}' failed to finalize TO output row: {error}",
                                processor.as_str()
                            ),
                            MessageErrorOperation::Finalize,
                            None,
                            invalid_output_fields(&result.batch, output_row),
                        ),
                        captured_partial_output(&result.batch, output_row),
                        matched.materialized_state[input_row].snapshot(),
                    ))));
                }
            }
        }
    }

    Ok(outcomes
        .into_iter()
        .map(|outcome| outcome.verified("the loops above assign an outcome to every input row"))
        .collect())
}

pub(super) struct CorrelatorOutputContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
}

pub(super) async fn enqueue_correlator_output(
    context: CorrelatorOutputContext<'_>,
    output_index: usize,
    messages: Vec<RelayMessage>,
    execution_now: Timestamp,
) {
    let CorrelatorOutputContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        output_routes,
    } = context;
    if messages.is_empty() {
        return;
    }
    let Some(output) = output_routes.routes.get_mut(output_index) else {
        for message in messages {
            message.acks.no_ack(format!(
                "correlator '{}' has no output destination at index {}",
                processor.as_str(),
                output_index
            ));
        }
        return;
    };
    let output_relay = output.relay.clone();
    let output_schema =
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, &output_relay) {
            Ok(schema) => schema,
            Err(error) => {
                let policy = output.message_error_policy.clone();
                for message in messages {
                    branch
                        .runtime
                        .handle_message_error_with_policy(
                            &branch.domain,
                            node_kind,
                            processor,
                            &policy,
                            message,
                            MessageErrorFailure::new(
                                Some(&output_relay),
                                error.to_string(),
                                MessageErrorOperation::Finalize,
                            ),
                        )
                        .await;
                }
                return;
            }
        };
    let batch = match build_stream_record_batch_preserving_acks(output_schema, messages) {
        Ok(batch) => batch,
        Err((error, acks)) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                acks.iter(),
                format!(
                    "correlator '{}' failed to build output batch: {}",
                    processor.as_str(),
                    error
                ),
            );
            return;
        }
    };
    if !output.enqueue(batch, execution_now) {
        return;
    }
    let pending = output.take_pending();
    let pending_acks = pending
        .iter()
        .flat_map(|batch| batch.acks.iter().cloned())
        .collect::<Vec<_>>();
    let forwarded = match RelayRecordBatch::concat(pending) {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                pending_acks.iter(),
                format!(
                    "correlator '{}' failed to concatenate output for relay '{}': {}",
                    processor.as_str(),
                    output_relay.as_str(),
                    error
                ),
            );
            return;
        }
    };
    if branch
        .dispatch_output(
            graph,
            output,
            ModelKind::Correlator,
            &ModelName::from(&RelayName::from(processor)),
            &forwarded,
        )
        .await
        .is_ok()
    {
        for ack in &forwarded.acks {
            ack.ack_success();
        }
    } else {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            format!(
                "correlator '{}' failed to forward output to relay '{}'",
                processor.as_str(),
                output_relay.as_str()
            ),
        );
    }
}

pub(super) async fn handle_correlator_timeout_action(
    graph: &SharedActiveGraph,
    branch: &mut BranchRuntime,
    node_kind: ModelKind,
    processor: &ModelName,
    error_policies: &ErrorPolicies,
    action: &CorrelationTimeoutAction,
    message: RelayMessage,
) {
    match action {
        CorrelationTimeoutAction::Drop => {
            message.acks.ack_success();
        }
        CorrelationTimeoutAction::SendTo { relay } => {
            let output = RelayProcessorOutputNode {
                relay: relay.clone(),
                construction: RouteConstruction {
                    inherit: Some(nervix_models::Inheritance::All),
                    ..RouteConstruction::default()
                },
                branch: None,
                flush_policy: None,
                message_error_policy: error_policies.message.clone(),
                pending: Vec::new(),
                next_flush: None,
                compiled_program: None,
                compiled_branch_program: None,
            };
            let output_schema =
                match relay_schema_for_runtime(&branch.runtime, &branch.domain, relay) {
                    Ok(schema) => schema,
                    Err(error) => {
                        branch
                            .runtime
                            .handle_message_error(
                                &branch.domain,
                                node_kind,
                                processor,
                                error_policies,
                                message,
                                MessageErrorFailure::publish(None, error.to_string()),
                            )
                            .await;
                        return;
                    }
                };
            let batch = match RelayRecordBatch::from_messages(output_schema, vec![message]) {
                Ok(batch) => batch,
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        std::iter::empty::<&AckSet>(),
                        format!(
                            "correlator '{}' failed to build timeout batch: {}",
                            processor.as_str(),
                            error
                        ),
                    );
                    return;
                }
            };
            if branch
                .dispatch_output(
                    graph,
                    &output,
                    ModelKind::Correlator,
                    &ModelName::from(&RelayName::from(processor)),
                    &batch,
                )
                .await
                .is_ok()
            {
                for ack in batch.acks.iter() {
                    ack.ack_success();
                }
            } else {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    batch.acks.iter(),
                    format!(
                        "correlator '{}' failed to forward timeout message",
                        processor.as_str()
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use ahash::HashMap;
    use nervix_models::{FieldName, ParseAsType};
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime_ack::AckSet,
        runtime_schema::{
            RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow, RuntimeValue, test_runtime_row,
        },
    };
    #[test]
    fn correlator_runtime_rows_use_only_left_and_right_scopes() {
        let left = test_runtime_row([
            ("id".to_string(), RuntimeValue::U32(1)),
            (
                "relay_state.profiles.status".to_string(),
                RuntimeValue::String("active".to_string()),
            ),
        ]);
        let right = test_runtime_row([("id".to_string(), RuntimeValue::U32(2))]);
        let left = RuntimeRecordBatch::from_rows(left.batch().schema(), std::iter::once(&left))
            .expect("left batch should build");
        let right = RuntimeRecordBatch::from_rows(right.batch().schema(), std::iter::once(&right))
            .expect("right batch should build");
        let materialized_state = [CorrelatorMaterializedState {
            left: Arc::new(HashMap::default()),
            right: Arc::new(HashMap::default()),
        }];
        let combined = correlator_input_batch(&left, &right, &[], &materialized_state)
            .and_then(|batch| batch.runtime_row(0, RuntimeRecordMetadata::test()))
            .expect("correlator inputs should form one Arrow row");

        assert_eq!(row_value(&combined, "left.id"), Some(RuntimeValue::U32(1)));
        assert_eq!(row_value(&combined, "right.id"), Some(RuntimeValue::U32(2)));
        assert_eq!(
            row_value(&combined, "left.relay_state.profiles.status"),
            Some(RuntimeValue::String("active".to_string()))
        );
        assert_eq!(row_value(&combined, "relay_state.profiles.status"), None);
        assert_eq!(row_value(&combined, "id"), None);
    }

    #[tokio::test]
    async fn correlator_where_matches_pending_candidates_in_one_vm_execution() {
        let left_schema = test_schema(&[("id", ParseAsType::U32), ("marker", ParseAsType::I64)]);
        let right_schema = test_schema(&[("id", ParseAsType::U32)]);
        let processor = named("join_profiles");
        let program = compile_correlator_where_program(
            &processor,
            &expression("left.id = right.id"),
            &[named("left_profiles")],
            left_schema.arrow_schema(),
            &[named("right_profiles")],
            right_schema.arrow_schema(),
            None,
        )
        .expect("correlator WHERE should compile");
        let now = current_timestamp();
        let pending = |id, marker| CorrelatorPendingMessage {
            received_at: now,
            message: RelayMessage {
                key: None,
                record: test_runtime_row([
                    ("id".to_string(), RuntimeValue::U32(id)),
                    ("marker".to_string(), RuntimeValue::I64(marker)),
                ]),
                acks: AckSet::empty(),
            },
            materialized_state: Arc::new(HashMap::default()),
        };
        let mut state = CorrelatorBranchState {
            pending_left: vec![pending(7, 1), pending(8, 2), pending(7, 3)],
            pending_right: Vec::new(),
        };
        let incoming = CorrelatorPendingMessage {
            received_at: now,
            message: RelayMessage {
                key: None,
                record: test_runtime_row([("id".to_string(), RuntimeValue::U32(7))]),
                acks: AckSet::empty(),
            },
            materialized_state: Arc::new(HashMap::default()),
        };
        CORRELATOR_WHERE_VM_EXECUTIONS.store(0, Ordering::Relaxed);

        let (matched_left, _matched_right) = correlate_incoming_message(
            &processor,
            &program,
            CorrelatorSide::Right,
            nervix_models::CorrelatorMatchPolicy::Latest,
            &mut state,
            incoming,
            now,
        )
        .await
        .expect("batched WHERE evaluation should succeed")
        .expect("matching candidates should produce a correlation");

        assert_eq!(
            CORRELATOR_WHERE_VM_EXECUTIONS.load(Ordering::Relaxed),
            1,
            "all candidate pairs must share one WHERE VM execution"
        );
        assert_eq!(
            row_value(&matched_left.message.record, "marker"),
            Some(RuntimeValue::I64(3))
        );
        assert_eq!(state.pending_left.len(), 1);
        assert_eq!(
            row_value(&state.pending_left[0].message.record, "marker"),
            Some(RuntimeValue::I64(2))
        );

        let incoming_left = pending(7, 4);
        let right_candidate = |id| CorrelatorPendingMessage {
            received_at: now,
            message: RelayMessage {
                key: None,
                record: test_runtime_row([("id".to_string(), RuntimeValue::U32(id))]),
                acks: AckSet::empty(),
            },
            materialized_state: Arc::new(HashMap::default()),
        };
        let right_candidates = vec![right_candidate(7), right_candidate(8), right_candidate(7)];
        CORRELATOR_WHERE_VM_EXECUTIONS.store(0, Ordering::Relaxed);

        let matching = evaluate_correlator_where_matches(
            &processor,
            &program,
            CorrelatorSide::Left,
            &incoming_left,
            &right_candidates,
            now,
        )
        .await
        .expect("left-side arrival should evaluate its right-side candidates");

        assert_eq!(matching, vec![true, false, true]);
        assert_eq!(
            CORRELATOR_WHERE_VM_EXECUTIONS.load(Ordering::Relaxed),
            1,
            "a left-side arrival must also batch all candidate pairs"
        );
    }

    #[tokio::test]
    async fn correlator_output_evaluates_all_matched_pairs_once_per_route() {
        let left_schema = test_schema(&[("id", ParseAsType::U32)]);
        let right_schema = test_schema(&[("score", ParseAsType::I64)]);
        let output_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("status", ParseAsType::String),
            ("score", ParseAsType::I64),
        ]);
        let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
        let state_schema = test_schema(&[("status", ParseAsType::String)]);
        let branch = named::<FieldName>("by_tenant");
        let materialized_specs = HashMap::from_iter([(
            named("profiles"),
            RuntimeMaterializedRelaySpec::new(
                state_schema.arrow_schema(),
                VmSchemaSensitivity::default(),
                vec![branch.clone()],
            ),
        )]);
        let program = CorrelatorOutputCompileContext {
            processor: &named("join_profiles"),
            left_schema: left_schema.arrow_schema(),
            left_sensitivity: VmSchemaSensitivity::default(),
            right_schema: right_schema.arrow_schema(),
            right_sensitivity: VmSchemaSensitivity::default(),
            output_relay: &named("joined_profiles"),
            output_schema: output_schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
            construction: &construction(
                "SET tenant = branch.tenant, status = relay_state.profiles.status, score = \
                 right.score WHERE relay_state.profiles.status != \"blocked\"",
            ),
            runtime: RuntimeVmCompileContext {
                available_materialized_streams: &materialized_specs,
                available_lookups: &HashMap::default(),
                current_branching: std::slice::from_ref(&branch),
                current_branch_schema: Some(&branch_schema),
                current_branch_sensitivity: None,
                udfs: None,
            },
        }
        .compile()
        .expect("correlator output should compile");
        let left_batch = Arc::new(
            left_schema
                .batch_from_test_rows([
                    [("id".to_string(), RuntimeValue::U32(7))],
                    [("id".to_string(), RuntimeValue::U32(8))],
                ])
                .expect("left rows should build"),
        );
        let right_batch = Arc::new(
            right_schema
                .batch_from_test_rows([
                    [("score".to_string(), RuntimeValue::I64(41))],
                    [("score".to_string(), RuntimeValue::I64(42))],
                ])
                .expect("right rows should build"),
        );
        let now = current_timestamp();
        let key = string_branch_key("tenant", "acme");
        let correlation = |row, status: &str| {
            let state = Arc::new(HashMap::from_iter([(
                "relay_state.profiles.status".to_string(),
                RuntimeValue::String(status.to_string()),
            )]));
            (
                CorrelatorPendingMessage {
                    received_at: now,
                    message: RelayMessage {
                        key: key.clone(),
                        record: RuntimeRow::new(
                            left_batch.clone(),
                            row,
                            RuntimeRecordMetadata::test(),
                        )
                        .expect("left row should exist"),
                        acks: AckSet::empty(),
                    },
                    materialized_state: Arc::new(HashMap::default()),
                },
                CorrelatorPendingMessage {
                    received_at: now,
                    message: RelayMessage {
                        key: key.clone(),
                        record: RuntimeRow::new(
                            right_batch.clone(),
                            row,
                            RuntimeRecordMetadata::test(),
                        )
                        .expect("right row should exist"),
                        acks: AckSet::empty(),
                    },
                    materialized_state: state,
                },
            )
        };
        let correlations = vec![correlation(0, "active"), correlation(1, "paused")];
        let matched = CorrelatorMatchedBatch::from_correlations(&correlations, &[&program])
            .expect("matched pairs should form one Arrow batch");
        CORRELATOR_OUTPUT_VM_EXECUTIONS.store(0, Ordering::Relaxed);

        let outcomes = evaluate_correlator_output_batch(
            &named("join_profiles"),
            &program,
            &matched,
            vec![AckSet::empty(), AckSet::empty()],
            now,
        )
        .await
        .expect("batched correlator output should evaluate");
        let messages = outcomes
            .into_iter()
            .map(|outcome| match outcome {
                Ok(Some(message)) => message,
                Ok(None) => panic!("route WHERE must retain every eligible pair"),
                Err(error) => panic!("correlator output should succeed: {}", error.error.message),
            })
            .collect::<Vec<_>>();

        assert_eq!(
            CORRELATOR_OUTPUT_VM_EXECUTIONS.load(Ordering::Relaxed),
            1,
            "all matched pairs for one route must share one output VM execution"
        );
        assert_eq!(messages.len(), 2);
        assert_eq!(
            row_value(&messages[0].record, "status"),
            Some(RuntimeValue::String("active".to_string()))
        );
        assert_eq!(
            row_value(&messages[1].record, "status"),
            Some(RuntimeValue::String("paused".to_string()))
        );
        assert_eq!(
            row_value(&messages[0].record, "score"),
            Some(RuntimeValue::I64(41))
        );
        assert_eq!(
            row_value(&messages[1].record, "score"),
            Some(RuntimeValue::I64(42))
        );
        let output_batch = messages[0].record.batch().clone();
        let relay_batch = build_stream_record_batch_preserving_acks(output_schema, messages)
            .expect("batched correlator output should form one relay batch");
        assert!(
            Arc::ptr_eq(&relay_batch.batch, &output_batch),
            "relay batching must preserve the correlator's shared output allocation"
        );
    }
}
