use super::*;

pub(crate) async fn execute_filter_map_on_record(
    subscription: &SubscriptionName,
    filter_map: &CompiledProgramWithMaterializedInterest,
    record: RuntimeRow,
    branch_key: Option<&BranchKey>,
    filter_map_metadata: Option<&IngestFilterMapMetadata>,
    side_inputs: &HashMap<String, RuntimeValue>,
    execution_now: Timestamp,
) -> Result<Option<RuntimeRow>, String> {
    let keys = vec![branch_key.cloned()];
    let metadata = vec![record.metadata().clone()];
    let carrier = record.one_row_batch();
    let outcome = evaluate_filter_map_on_batch(
        "subscription",
        subscription,
        filter_map,
        FilterMapOutcomeInputs {
            carrier: &carrier,
            record_metadata: &metadata,
            keys: &keys,
            filter_map_metadata,
            side_inputs,
        },
        execution_now,
    )
    .await?
    .into_iter()
    .next()
    .verified("this call passes a single record, and filter-map answers one outcome per record");
    match outcome {
        SingleRecordFilterMapOutcome::Filtered => Ok(None),
        SingleRecordFilterMapOutcome::Output(record) => Ok(Some(record)),
        SingleRecordFilterMapOutcome::MessageError { error, .. } => {
            Err(format!("FILTER-MAP message error: {}", error.message))
        }
    }
}

/// Runs one filter-map program over a whole group of records in a single VM execution
/// and returns one outcome per input record, in input row order.
///
/// The columnar VM already filters many rows at once: `ExecutionResult::selected_rows`
/// carries the input row index behind every surviving output row. Walking that mapping
/// is what keeps message-error attribution and ack identity tied to the record each
/// outcome came from, so a group never has to be evaluated a row at a time.
pub(super) struct FilterMapOutcomeInputs<'a> {
    pub(super) carrier: &'a RuntimeRecordBatch,
    pub(super) record_metadata: &'a [RuntimeRecordMetadata],
    pub(super) keys: &'a [Option<BranchKey>],
    pub(super) filter_map_metadata: Option<&'a IngestFilterMapMetadata>,
    pub(super) side_inputs: &'a HashMap<String, RuntimeValue>,
}

pub(super) async fn evaluate_filter_map_on_batch(
    processor_kind: &str,
    processor: impl Into<ModelName>,
    filter_map: &CompiledProgramWithMaterializedInterest,
    inputs: FilterMapOutcomeInputs<'_>,
    execution_now: Timestamp,
) -> Result<Vec<SingleRecordFilterMapOutcome>, String> {
    let processor = processor.into();
    let FilterMapOutcomeInputs {
        carrier,
        record_metadata,
        keys,
        filter_map_metadata,
        side_inputs,
    } = inputs;
    let row_count = carrier.batch().num_rows();
    if row_count == 0 {
        return Ok(Vec::new());
    }
    if record_metadata.len() != row_count {
        return Err(format!(
            "FILTER-MAP received {} runtime metadata rows for {row_count} records",
            record_metadata.len()
        ));
    }
    if keys.len() != row_count {
        return Err(format!(
            "FILTER-MAP received {} branch keys for {row_count} records",
            keys.len()
        ));
    }
    if let Some(metadata) = filter_map_metadata
        && metadata.len() != row_count
    {
        return Err(format!(
            "FILTER-MAP received {} ingest metadata rows for {row_count} records",
            metadata.len()
        ));
    }
    let executed = execute_filter_map_program_on_batch(
        processor_kind,
        processor,
        filter_map,
        FilterMapBatchInputs {
            carrier,
            namespace_batches: &[],
            keys,
            side_inputs,
            ingest_metadata: filter_map_metadata,
        },
        execution_now,
        (0..row_count).map(|_| AckSet::empty()).collect(),
        None,
    )
    .await
    .map_err(|error| error.reason)?;
    if executed.selected_rows.len() != executed.batch.row_count() {
        return Err(format!(
            "FILTER-MAP produced {} rows for {} selected rows",
            executed.batch.row_count(),
            executed.selected_rows.len()
        ));
    }
    // Rows the program filtered out never appear in `selected_rows`, so starting every
    // record at `Filtered` and overwriting the survivors keeps the result row-aligned
    // with the input without a second pass over the predicate.
    let mut outcomes = (0..row_count)
        .map(|_| SingleRecordFilterMapOutcome::Filtered)
        .collect::<Vec<_>>();
    let state_snapshot = relay_state_snapshot_from_side_inputs(side_inputs);
    let mut successful_output_rows = Vec::new();
    let mut successful_input_rows = Vec::new();
    for (output_row, input_row) in executed.selected_rows.iter().enumerate() {
        let (Some(slot), Some(metadata)) = (
            outcomes.get_mut(input_row),
            record_metadata.get(input_row).cloned(),
        ) else {
            return Err(format!(
                "FILTER-MAP selected row {input_row} outside its {row_count}-record input"
            ));
        };
        if let Some(side_error) = executed.batch.errors().row(output_row).first() {
            *slot = SingleRecordFilterMapOutcome::MessageError {
                error: filter_map.structured_side_error(
                    format!(
                        "FILTER-MAP side error {}: {} at {}",
                        side_error.code.as_str(),
                        side_error.message,
                        side_error.span
                    ),
                    side_error.span,
                    MessageErrorOperation::Set,
                ),
                partial_output: captured_partial_output(&executed.batch, output_row),
                materialized_state: state_snapshot.clone(),
            };
            continue;
        }
        let _ = metadata;
        successful_output_rows.push(output_row);
        successful_input_rows.push(input_row);
    }
    if !successful_output_rows.is_empty() {
        let output_batch = Arc::new(vm_typed_batch_selected_rows_to_runtime_batch(
            &executed.batch,
            &successful_output_rows,
        )?);
        for (output_row, input_row) in successful_input_rows.into_iter().enumerate() {
            outcomes[input_row] = SingleRecordFilterMapOutcome::Output(RuntimeRow::new(
                output_batch.clone(),
                output_row,
                record_metadata[input_row].clone(),
            )?);
        }
    }
    Ok(outcomes)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct InferencerFilterMapTensors<'a> {
    pub(super) output_schema: &'a [InferencerTensorDeclaration],
}

impl InferencerFilterMapTensors<'_> {
    pub(super) fn output_arrow_schema(&self) -> StdArc<arrow_schema::Schema> {
        StdArc::new(arrow_schema::Schema::new(
            self.output_schema
                .iter()
                .map(|declaration| {
                    arrow_schema::Field::new(
                        &declaration.tensor,
                        crate::runtime_schema::arrow_data_type(&declaration.schema.message_type()),
                        false,
                    )
                })
                .collect::<Vec<_>>(),
        ))
    }
}

pub(super) fn expression_reads_sensitive_source(
    expression: &nervix_models::Expression,
    sensitivity: &VmSchemaSensitivity,
) -> bool {
    match expression {
        nervix_models::Expression::Literal(_) => false,
        nervix_models::Expression::Field(reference) => {
            matches!(
                reference.scope,
                nervix_models::FieldScope::Bare | nervix_models::FieldScope::Input
            ) && sensitivity.is_sensitive(reference.field.as_str())
        }
        nervix_models::Expression::Unary { expression, .. }
        | nervix_models::Expression::Cast { expression, .. } => {
            expression_reads_sensitive_source(expression, sensitivity)
        }
        nervix_models::Expression::Binary { left, right, .. } => {
            expression_reads_sensitive_source(left, sensitivity)
                || expression_reads_sensitive_source(right, sensitivity)
        }
        nervix_models::Expression::Call {
            function,
            arguments,
        } => {
            !function.as_str().eq_ignore_ascii_case("leak_sensitive")
                && arguments
                    .iter()
                    .any(|argument| expression_reads_sensitive_source(argument, sensitivity))
        }
        nervix_models::Expression::UdfCall { arguments, .. } => arguments
            .iter()
            .any(|argument| expression_reads_sensitive_source(argument, sensitivity)),
        nervix_models::Expression::Array(items) => items
            .iter()
            .any(|item| expression_reads_sensitive_source(item, sensitivity)),
        nervix_models::Expression::If {
            condition,
            then_result,
            else_result,
        } => {
            expression_reads_sensitive_source(condition, sensitivity)
                || expression_reads_sensitive_source(then_result, sensitivity)
                || expression_reads_sensitive_source(else_result, sensitivity)
        }
        nervix_models::Expression::Case {
            operand,
            branches,
            else_result,
        } => {
            operand
                .as_deref()
                .is_some_and(|operand| expression_reads_sensitive_source(operand, sensitivity))
                || branches.iter().any(|branch| {
                    expression_reads_sensitive_source(&branch.when, sensitivity)
                        || expression_reads_sensitive_source(&branch.result, sensitivity)
                })
                || else_result.as_deref().is_some_and(|else_result| {
                    expression_reads_sensitive_source(else_result, sensitivity)
                })
        }
    }
}

pub(super) async fn plan_filter_map_messages(
    processor_kind: &str,
    processor: impl Into<ModelName>,
    program_label: &str,
    program: &CompiledProgramWithMaterializedInterest,
    mut batch: RelayRecordBatch,
    execution_now: Timestamp,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> Result<FilterMapPlan, PlannedGeneralError> {
    let processor = processor.into();
    let lookup_columns = match compute_lookup_hash_map_columns(
        program,
        &FilterMapBatchInputs {
            carrier: &batch.batch,
            namespace_batches: &[],
            keys: &batch.keys,
            side_inputs,
            ingest_metadata: None,
        },
        execution_now,
        None,
    )
    .await
    {
        Ok(columns) => columns,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks: batch.acks,
                reason: format!(
                    "{} '{}' failed to prepare LOOKUP_HASH_MAP inputs: {}",
                    processor_kind,
                    processor.as_str(),
                    error
                ),
            });
        }
    };
    let uninitialized = VmUninitializedInput {
        fields: program
            .compiled
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("output."))
            .map(|field| field.name().clone())
            .collect(),
    };
    let vm_batch = match project_vm_input_batch(
        &program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: &batch.batch,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &batch.keys,
            side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized),
        },
        None,
    ) {
        Ok(vm_batch) => vm_batch,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks: batch.acks,
                reason: format!(
                    "{} '{}' failed to prepare {} input batch: {}",
                    processor_kind,
                    processor.as_str(),
                    program_label,
                    error
                ),
            });
        }
    };
    let key = batch.key.clone();
    let keys = batch.keys.clone();
    let metadata = batch.metadata.clone();
    let mut acks = std::mem::take(&mut batch.acks);
    let state_snapshot = relay_state_snapshot_from_side_inputs(side_inputs);
    let result = match execute_program_with_selection_in_context(
        &program.compiled,
        &vm_batch,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks,
                reason: format!(
                    "{} '{}' {} execution failed: {}",
                    processor_kind,
                    processor.as_str(),
                    program_label,
                    error
                ),
            });
        }
    };

    let mut selected_rows = vec![false; acks.len()];
    for row in result.selected_rows.iter() {
        if row < selected_rows.len() {
            selected_rows[row] = true;
        }
    }
    for (row, selected) in selected_rows.iter().enumerate() {
        if !selected {
            acks[row].ack_success();
        }
    }

    let mut success_output_rows = Vec::new();
    let mut success_input_rows = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, input_row) in result.selected_rows.iter().enumerate() {
        if let Some(side_error) = result.batch.errors().row(output_row).first() {
            let partial_output = if program.captures_partial_output() {
                Some(vm_partial_output_row_to_runtime_batch(
                    &result.batch,
                    output_row,
                ))
            } else {
                None
            };
            let partial_output_failure = partial_output
                .as_ref()
                .and_then(|partial_output| partial_output.as_ref().err());
            let reason = format!(
                "{} '{}' {} side error {}: {} at {}",
                processor_kind,
                processor.as_str(),
                program_label,
                side_error.code.as_str(),
                side_error.message,
                side_error.span
            );
            let reason = if let Some(partial_output_failure) = partial_output_failure {
                format!("{reason}; failed to capture partial output: {partial_output_failure}")
            } else {
                reason
            };
            let record = batch
                .runtime_row(input_row)
                .map_err(|error| PlannedGeneralError {
                    acks: acks.clone(),
                    reason: format!(
                        "{} '{}' failed to materialize {} error input row: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        error
                    ),
                })?;
            message_errors.push(planned_structured_message_error(
                RelayMessage {
                    key: keys[input_row].clone(),
                    record,
                    acks: std::mem::take(&mut acks[input_row]),
                },
                program.structured_side_error(
                    reason,
                    side_error.span,
                    operation_for_filter_label(program_label),
                ),
                partial_output.and_then(Result::ok),
                state_snapshot.clone(),
            ));
            continue;
        }
        let invalid_fields = invalid_output_fields(&result.batch, output_row);
        if !invalid_fields.is_empty() {
            let record =
                batch
                    .runtime_row(input_row)
                    .map_err(|decode_error| PlannedGeneralError {
                        acks: acks.clone(),
                        reason: format!(
                            "{} '{}' failed to materialize {} error input row: {}",
                            processor_kind,
                            processor.as_str(),
                            program_label,
                            decode_error
                        ),
                    })?;
            message_errors.push(planned_structured_message_error(
                RelayMessage {
                    key: keys[input_row].clone(),
                    record,
                    acks: std::mem::take(&mut acks[input_row]),
                },
                structured_message_error(
                    MessageErrorCode::Evaluation,
                    format!(
                        "{} '{}' failed to materialize {} output row: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        "required output fields are uninitialized or null"
                    ),
                    operation_for_filter_label(program_label),
                    None,
                    invalid_fields,
                ),
                None,
                state_snapshot.clone(),
            ));
            continue;
        }
        success_output_rows.push(output_row);
        success_input_rows.push(input_row);
    }

    let batch = if success_output_rows.is_empty() {
        None
    } else {
        let output_batch =
            vm_typed_batch_selected_rows_to_runtime_batch(&result.batch, &success_output_rows)
                .map_err(|error| PlannedGeneralError {
                    acks: acks.clone(),
                    reason: format!(
                        "{} '{}' failed to materialize successful {} rows: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        error
                    ),
                })?;
        let output_metadata = success_input_rows
            .iter()
            .map(|input_row| metadata[*input_row].clone())
            .collect::<Vec<_>>();
        let output_acks = success_input_rows
            .iter()
            .map(|input_row| std::mem::take(&mut acks[*input_row]))
            .collect::<Vec<_>>();
        let error_acks = output_acks.clone();
        Some(
            RelayRecordBatch::from_filtered_parts(key, output_batch, output_metadata, output_acks)
                .map_err(|error| PlannedGeneralError {
                    acks: error_acks,
                    reason: format!(
                        "{} '{}' failed to build {} output batch: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        error
                    ),
                })?,
        )
    };

    Ok(FilterMapPlan {
        batch,
        message_errors,
    })
}

pub(super) struct EmitterFilterMapPlan {
    pub(super) batch: Option<RelayRecordBatch>,
    pub(super) headers: Option<Vec<EmitterHeaders>>,
    pub(super) source_rows: Vec<usize>,
    pub(super) message_errors: Vec<PlannedMessageError>,
}

pub(super) async fn plan_emitter_filter_map_batch(
    emitter: &EmitterName,
    program: &CompiledEmitterFilterMapProgram,
    mut input: RelayRecordBatch,
    execution_now: Timestamp,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> Result<EmitterFilterMapPlan, PlannedGeneralError> {
    let acks = std::mem::take(&mut input.acks);
    let body_result = execute_filter_map_program_on_batch(
        "emitter",
        emitter,
        &program.body,
        FilterMapBatchInputs {
            carrier: &input.batch,
            namespace_batches: &[],
            keys: &input.keys,
            side_inputs,
            ingest_metadata: None,
        },
        execution_now,
        acks,
        None,
    )
    .await?;
    let mut acks = body_result.acks;
    let state_snapshot = relay_state_snapshot_from_side_inputs(side_inputs);

    let mut selected_rows = vec![false; acks.len()];
    for row in body_result.selected_rows.iter() {
        if row < selected_rows.len() {
            selected_rows[row] = true;
        }
    }
    for (row, selected) in selected_rows.iter().enumerate() {
        if !selected {
            acks[row].ack_success();
        }
    }

    let mut successful_output_rows = Vec::new();
    let mut successful_input_rows = Vec::new();
    let mut headers = (!body_result.invocations.is_empty()).then(Vec::new);
    let mut message_errors = Vec::new();
    for (output_row, input_row) in body_result.selected_rows.iter().enumerate() {
        let source_record = |context: &str| {
            input
                .runtime_row(input_row)
                .map_err(|error| PlannedGeneralError {
                    acks: acks.clone(),
                    reason: format!(
                        "emitter '{}' failed to materialize {context} input row: {error}",
                        emitter.as_str()
                    ),
                })
        };
        if let Some(side_error) = body_result.batch.errors().row(output_row).first() {
            let source_record = source_record("FILTER-MAP error")?;
            let partial_output = program
                .codec_route
                .then(|| captured_partial_output(&body_result.batch, output_row))
                .flatten();
            let reason = format!(
                "emitter '{}' FILTER-MAP side error {}: {} at {}",
                emitter.as_str(),
                side_error.code.as_str(),
                side_error.message,
                side_error.span
            );
            message_errors.push(planned_structured_message_error(
                RelayMessage {
                    key: input.keys[input_row].clone(),
                    record: source_record,
                    acks: std::mem::take(&mut acks[input_row]),
                },
                program.body.structured_side_error(
                    reason,
                    side_error.span,
                    if program.codec_route {
                        MessageErrorOperation::Set
                    } else {
                        MessageErrorOperation::Values
                    },
                ),
                partial_output,
                state_snapshot.clone(),
            ));
            continue;
        }
        let message_headers =
            match emitter_headers_from_invocations(&body_result.invocations, output_row) {
                Ok(headers) => headers,
                Err(error) => {
                    let source_record = source_record("FILTER-MAP header error")?;
                    let partial_output = program
                        .codec_route
                        .then(|| captured_partial_output(&body_result.batch, output_row))
                        .flatten();
                    message_errors.push(planned_structured_message_error(
                        RelayMessage {
                            key: input.keys[input_row].clone(),
                            record: source_record,
                            acks: std::mem::take(&mut acks[input_row]),
                        },
                        structured_message_error(
                            MessageErrorCode::Evaluation,
                            format!(
                                "emitter '{}' failed to materialize FILTER-MAP headers: {}",
                                emitter.as_str(),
                                error
                            ),
                            MessageErrorOperation::Invoke,
                            None,
                            std::iter::empty(),
                        ),
                        partial_output,
                        state_snapshot.clone(),
                    ));
                    continue;
                }
            };
        let invalid_fields = invalid_output_fields(&body_result.batch, output_row);
        if !invalid_fields.is_empty() {
            let source_record = source_record("FILTER-MAP validation error")?;
            let partial_output = program
                .codec_route
                .then(|| captured_partial_output(&body_result.batch, output_row))
                .flatten();
            message_errors.push(planned_structured_message_error(
                RelayMessage {
                    key: input.keys[input_row].clone(),
                    record: source_record,
                    acks: std::mem::take(&mut acks[input_row]),
                },
                structured_message_error(
                    MessageErrorCode::Validation,
                    format!(
                        "emitter '{}' FILTER-MAP output row has uninitialized required fields",
                        emitter.as_str()
                    ),
                    if program.codec_route {
                        MessageErrorOperation::Finalize
                    } else {
                        MessageErrorOperation::Values
                    },
                    None,
                    invalid_fields,
                ),
                partial_output,
                state_snapshot.clone(),
            ));
            continue;
        }
        successful_output_rows.push(output_row);
        successful_input_rows.push(input_row);
        if let Some(headers) = &mut headers {
            headers.push(message_headers);
        }
    }

    let batch = if successful_output_rows.is_empty() {
        None
    } else {
        let output_batch = vm_typed_batch_selected_rows_to_runtime_batch(
            &body_result.batch,
            &successful_output_rows,
        )
        .map_err(|error| PlannedGeneralError {
            acks: acks.clone(),
            reason: format!(
                "emitter '{}' failed to finalize FILTER-MAP output batch: {error}",
                emitter.as_str()
            ),
        })?;
        let metadata = successful_input_rows
            .iter()
            .map(|input_row| input.metadata[*input_row].clone())
            .collect::<Vec<_>>();
        let output_acks = successful_input_rows
            .iter()
            .map(|input_row| std::mem::take(&mut acks[*input_row]))
            .collect::<Vec<_>>();
        let error_acks = output_acks.clone();
        Some(
            RelayRecordBatch::from_filtered_parts(
                input.key.clone(),
                output_batch,
                metadata,
                output_acks,
            )
            .map_err(|error| PlannedGeneralError {
                acks: error_acks,
                reason: format!(
                    "emitter '{}' failed to build FILTER-MAP output batch: {error}",
                    emitter.as_str()
                ),
            })?,
        )
    };

    Ok(EmitterFilterMapPlan {
        batch,
        headers,
        source_rows: successful_input_rows,
        message_errors,
    })
}

pub(in crate::runtime) async fn evaluate_sqs_fifo_group_program(
    emitter: &EmitterName,
    program: &CompiledProgramWithMaterializedInterest,
    batch: &RelayRecordBatch,
    execution_now: Timestamp,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> Result<Vec<Result<Option<String>, String>>, PlannedGeneralError> {
    let row_count = batch.batch.batch().num_rows();
    let result = execute_filter_map_program_on_batch(
        "emitter",
        emitter,
        program,
        FilterMapBatchInputs {
            carrier: &batch.batch,
            namespace_batches: &[],
            keys: &batch.keys,
            side_inputs,
            ingest_metadata: None,
        },
        execution_now,
        batch.acks.clone(),
        None,
    )
    .await?;
    let mut groups = (0..row_count)
        .map(|_| Err("SQS FIFO GROUP expression omitted its input row".to_string()))
        .collect::<Vec<_>>();
    for (output_row, input_row) in result.selected_rows.iter().enumerate() {
        if input_row >= row_count {
            return Err(PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "emitter '{}' SQS FIFO GROUP expression referenced missing input row \
                     {input_row}",
                    emitter.as_str()
                ),
            });
        }
        if let Some(side_error) = result.batch.errors().row(output_row).first() {
            groups[input_row] = Err(format!(
                "SQS FIFO GROUP expression failed with {} at {}",
                side_error.code.as_str(),
                side_error.span
            ));
            continue;
        }
        groups[input_row] = match vm_output_value(&result.batch, output_row, "fifo_group") {
            Ok(Some(RuntimeValue::String(value))) => Ok(Some(value)),
            Ok(Some(value)) => Err(format!(
                "SQS FIFO GROUP expression produced {}, expected STRING",
                runtime_value_type_name(&value)
            )),
            Ok(None) => Err("SQS FIFO GROUP expression produced NULL".to_string()),
            Err(reason) => Err(reason),
        };
    }
    Ok(groups)
}

pub(super) struct ExecutedFilterMap {
    pub(super) batch: VmTypedBatch,
    pub(super) selected_rows: nervix_vm::RowSelection,
    pub(super) invocations: Vec<nervix_vm::FunctionInvocation>,
    pub(super) acks: Vec<AckSet>,
}

pub(super) struct VmUninitializedInput {
    pub(super) fields: HashSet<String>,
}

impl VmUninitializedInput {
    pub(super) fn contains(&self, field: &arrow_schema::Field) -> bool {
        self.fields.contains(field.name())
    }
}

pub(super) async fn execute_prepared_filter_map(
    processor_kind: &str,
    processor: impl Into<ModelName>,
    program: &CompiledProgramWithMaterializedInterest,
    vm_batch: VmTypedBatch,
    execution_now: Timestamp,
    acks: Vec<AckSet>,
    injector: Option<Arc<Box<dyn VmFunctionInjector>>>,
) -> Result<ExecutedFilterMap, PlannedGeneralError> {
    let processor = processor.into();
    let result = match execute_program_with_selection_in_context(
        &program.compiled,
        &vm_batch,
        &VmExecutionContext {
            now: execution_now,
            injector,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks,
                reason: format!(
                    "{} '{}' FILTER-MAP execution failed: {}",
                    processor_kind,
                    processor.as_str(),
                    error
                ),
            });
        }
    };
    Ok(ExecutedFilterMap {
        batch: result.batch,
        selected_rows: result.selected_rows,
        invocations: result.invocations,
        acks,
    })
}

pub(super) struct FilterMapBatchInputs<'a> {
    pub(super) carrier: &'a RuntimeRecordBatch,
    pub(super) namespace_batches: &'a [(&'a str, &'a RuntimeRecordBatch)],
    pub(super) keys: &'a [Option<BranchKey>],
    pub(super) side_inputs: &'a HashMap<String, RuntimeValue>,
    pub(super) ingest_metadata: Option<&'a IngestFilterMapMetadata>,
}

pub(super) async fn execute_filter_map_program_on_batch(
    processor_kind: &str,
    processor: impl Into<ModelName>,
    program: &CompiledProgramWithMaterializedInterest,
    inputs: FilterMapBatchInputs<'_>,
    execution_now: Timestamp,
    acks: Vec<AckSet>,
    mut shared: Option<&mut SharedBatchColumns>,
) -> Result<ExecutedFilterMap, PlannedGeneralError> {
    let processor = processor.into();
    let lookup_columns = match compute_lookup_hash_map_columns(
        program,
        &inputs,
        execution_now,
        shared.as_mut().map(|shared| &mut shared.lookups),
    )
    .await
    {
        Ok(columns) => columns,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks,
                reason: format!(
                    "{} '{}' failed to prepare LOOKUP_HASH_MAP inputs: {}",
                    processor_kind,
                    processor.as_str(),
                    error
                ),
            });
        }
    };
    let uninitialized_fields = match program.output_namespace_input {
        OutputNamespaceInput::Uninitialized => program
            .compiled
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("output."))
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>(),
        OutputNamespaceInput::Finalized => HashSet::default(),
    };
    let uninitialized = (!uninitialized_fields.is_empty()).then_some(VmUninitializedInput {
        fields: uninitialized_fields,
    });
    let vm_batch = match project_vm_input_batch(
        &program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: inputs.carrier,
            namespace_batches: inputs.namespace_batches,
            strict_namespaces: &[],
            keys: inputs.keys,
            side_inputs: inputs.side_inputs,
            ingest_metadata: inputs.ingest_metadata,
            lookup_columns: &lookup_columns,
            uninitialized: uninitialized.as_ref(),
        },
        shared.as_mut().map(|shared| &mut shared.inputs),
    ) {
        Ok(vm_batch) => vm_batch,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks,
                reason: format!(
                    "{} '{}' failed to prepare FILTER-MAP input batch: {}",
                    processor_kind,
                    processor.as_str(),
                    error
                ),
            });
        }
    };
    execute_prepared_filter_map(
        processor_kind,
        processor,
        program,
        vm_batch,
        execution_now,
        acks,
        inputs.ingest_metadata.map(|metadata| {
            IngestHeaderFunctionInjector::from_metadata(
                Some(metadata),
                inputs.carrier.batch().num_rows(),
            )
        }),
    )
    .await
}

pub(super) async fn evaluate_output_branch_program(
    node: impl Into<ModelName>,
    program: &CompiledBranchProgram,
    input: &RuntimeRecordBatch,
    output: &RuntimeRecordBatch,
    keys: &[Option<BranchKey>],
    side_inputs: &HashMap<String, RuntimeValue>,
    execution_now: Timestamp,
) -> Result<Vec<Result<Option<BranchKey>, String>>, String> {
    let node = node.into();
    let row_count = output.batch().num_rows();
    if input.batch().num_rows() != row_count || keys.len() != row_count {
        return Err(format!(
            "branch construction for '{}' received {} input rows, {} output rows, and {} keys",
            node.as_str(),
            input.batch().num_rows(),
            row_count,
            keys.len()
        ));
    }
    let namespace_batches = [("input", input), ("output", output), ("message", output)];
    let lookup_columns = compute_lookup_hash_map_columns(
        &program.program,
        &FilterMapBatchInputs {
            carrier: output,
            namespace_batches: &namespace_batches,
            keys,
            side_inputs,
            ingest_metadata: None,
        },
        execution_now,
        None,
    )
    .await?;
    let uninitialized = VmUninitializedInput {
        fields: program
            .program
            .compiled
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("branch."))
            .map(|field| field.name().clone())
            .collect(),
    };
    let vm_input = project_vm_input_batch(
        &program.program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: output,
            namespace_batches: &namespace_batches,
            strict_namespaces: &["input", "output", "message"],
            keys,
            side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized),
        },
        None,
    )?;
    let result = execute_program_with_selection_in_context(
        &program.program.compiled,
        &vm_input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| {
        format!(
            "branch construction VM for '{}' failed: {}",
            node.as_str(),
            error
        )
    })?;
    let mut outcomes = (0..row_count)
        .map(|_| Err("branch construction VM did not preserve the input row".to_string()))
        .collect::<Vec<_>>();
    for (output_row, input_row) in result.selected_rows.iter().enumerate() {
        if input_row >= outcomes.len() {
            return Err(format!(
                "branch construction VM for '{}' selected unknown row {}",
                node.as_str(),
                input_row
            ));
        }
        if let Some(error) = result.batch.errors().row(output_row).first() {
            outcomes[input_row] = Err(format!(
                "branch SET failed with {}: {} at {}",
                error.code.as_str(),
                error.message,
                error.span
            ));
            continue;
        }
        let mut fields = Vec::with_capacity(result.batch.schema().fields().len());
        for (column_index, field) in result.batch.schema().fields().iter().enumerate() {
            let array = result.batch.column(column_index).to_array_ref();
            let value = runtime_value_from_arrow_array(
                array.as_ref(),
                &parse_as_type_from_arrow(field.data_type()).map_err(|error| error.to_string())?,
                false,
                output_row,
                field.name(),
            )?
            .ok_or_else(|| format!("branch field '{}' is null", field.name()))?;
            let name = FieldName::parse(field.name()).map_err(|error| {
                format!(
                    "compiled branch field '{}' is invalid: {}",
                    field.name(),
                    error
                )
            })?;
            fields.push((name, value));
        }
        outcomes[input_row] = BranchKey::from_fields(fields).map(Some);
    }
    Ok(outcomes)
}

pub(super) fn emitter_headers_from_invocations(
    invocations: &[nervix_vm::FunctionInvocation],
    row: usize,
) -> Result<EmitterHeaders, String> {
    let mut headers = Vec::new();
    for invocation in invocations {
        if invocation.function != FunctionName::WriteHeader {
            return Err(format!(
                "unsupported invocation '{}'",
                invocation.function.as_str()
            ));
        }
        let [VmTypedArray::Utf8(names), VmTypedArray::Utf8(values)] =
            invocation.arguments.as_slice()
        else {
            return Err("write_header arguments must both be STRING".to_string());
        };
        if row >= names.len() || row >= values.len() {
            return Err(format!(
                "write_header result does not contain output row {row}"
            ));
        }
        if names.is_null(row) || values.is_null(row) {
            return Err("write_header arguments cannot be NULL".to_string());
        }
        headers.push((names.value(row).to_string(), values.value(row).to_string()));
    }
    Ok(headers)
}

macro_rules! append_filter_map_numeric_list_value {
    ($builder:expr, $value:expr, $field:expr, $pattern:path) => {{
        match $value {
            $pattern(value) => {
                $builder.append_value(*value);
                Ok(())
            }
            value => Err(format!(
                "FILTER-MAP input field '{}' expected {:?}, got {}",
                $field.name(),
                $field.data_type(),
                runtime_value_type_name(value)
            )),
        }
    }};
}

macro_rules! define_filter_map_numeric_list_appender {
    ($fn_name:ident, $builder:ty, $pattern:path) => {
        fn $fn_name(
            builder: &mut $builder,
            value: &RuntimeValue,
            field: &arrow_schema::Field,
        ) -> Result<(), String> {
            append_filter_map_numeric_list_value!(builder, value, field, $pattern)
        }
    };
}

define_filter_map_numeric_list_appender!(append_filter_map_u8, UInt8Builder, RuntimeValue::U8);

define_filter_map_numeric_list_appender!(append_filter_map_i8, Int8Builder, RuntimeValue::I8);

define_filter_map_numeric_list_appender!(append_filter_map_u16, UInt16Builder, RuntimeValue::U16);

define_filter_map_numeric_list_appender!(append_filter_map_i16, Int16Builder, RuntimeValue::I16);

define_filter_map_numeric_list_appender!(append_filter_map_u32, UInt32Builder, RuntimeValue::U32);

define_filter_map_numeric_list_appender!(append_filter_map_i32, Int32Builder, RuntimeValue::I32);

define_filter_map_numeric_list_appender!(append_filter_map_u64, UInt64Builder, RuntimeValue::U64);

define_filter_map_numeric_list_appender!(append_filter_map_i64, Int64Builder, RuntimeValue::I64);

pub(super) fn append_filter_map_f32(
    builder: &mut Float32Builder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::F32(value) => {
            builder.append_value(value.into_inner());
            Ok(())
        }
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

pub(super) fn append_filter_map_f64(
    builder: &mut Float64Builder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::F64(value) => {
            builder.append_value(value.into_inner());
            Ok(())
        }
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

pub(super) fn append_filter_map_bool(
    builder: &mut BooleanBuilder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::Bool(value) => {
            builder.append_value(*value);
            Ok(())
        }
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

pub(super) fn append_filter_map_string(
    builder: &mut StringBuilder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::String(value) => {
            builder.append_value(value);
            Ok(())
        }
        RuntimeValue::Datetime(value) => {
            builder.append_value(value.to_rfc3339());
            Ok(())
        }
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

pub(super) fn append_filter_map_datetime(
    builder: &mut TimestampNanosecondBuilder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::Datetime(value) => match value.timestamp_nanos_opt() {
            Some(value) => {
                builder.append_value(value);
                Ok(())
            }
            None => Err(format!(
                "FILTER-MAP input field '{}' datetime is out of nanosecond range",
                field.name()
            )),
        },
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

pub(super) fn append_filter_map_nested_value(
    builder: &mut dyn ArrayBuilder,
    data_type: &ArrowDataType,
    value: Option<&RuntimeValue>,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    macro_rules! append_primitive {
        ($builder:ty, $append:ident) => {{
            let builder = builder
                .as_any_mut()
                .downcast_mut::<$builder>()
                .ok_or_else(|| {
                    format!(
                        "FILTER-MAP input field '{}' has an incompatible Arrow builder",
                        field.name()
                    )
                })?;
            if let Some(value) = value {
                $append(builder, value, field)?;
            } else {
                builder.append_null();
            }
            Ok(())
        }};
    }

    match data_type {
        ArrowDataType::UInt8 => append_primitive!(UInt8Builder, append_filter_map_u8),
        ArrowDataType::Int8 => append_primitive!(Int8Builder, append_filter_map_i8),
        ArrowDataType::UInt16 => append_primitive!(UInt16Builder, append_filter_map_u16),
        ArrowDataType::Int16 => append_primitive!(Int16Builder, append_filter_map_i16),
        ArrowDataType::UInt32 => append_primitive!(UInt32Builder, append_filter_map_u32),
        ArrowDataType::Int32 => append_primitive!(Int32Builder, append_filter_map_i32),
        ArrowDataType::UInt64 => append_primitive!(UInt64Builder, append_filter_map_u64),
        ArrowDataType::Int64 => append_primitive!(Int64Builder, append_filter_map_i64),
        ArrowDataType::Float32 => append_primitive!(Float32Builder, append_filter_map_f32),
        ArrowDataType::Float64 => append_primitive!(Float64Builder, append_filter_map_f64),
        ArrowDataType::Boolean => append_primitive!(BooleanBuilder, append_filter_map_bool),
        ArrowDataType::Utf8 => append_primitive!(StringBuilder, append_filter_map_string),
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
            if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" =>
        {
            append_primitive!(TimestampNanosecondBuilder, append_filter_map_datetime)
        }
        ArrowDataType::List(element) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| {
                    format!(
                        "FILTER-MAP input field '{}' has an incompatible list builder",
                        field.name()
                    )
                })?;
            let values = match value {
                Some(RuntimeValue::Vec(values)) => Some(values),
                None => None,
                Some(value) => {
                    return Err(format!(
                        "FILTER-MAP input field '{}' expected VEC, got {}",
                        field.name(),
                        runtime_value_type_name(value)
                    ));
                }
            };
            if let Some(values) = values {
                for value in values {
                    append_filter_map_nested_value(
                        builder.values().as_mut(),
                        element.data_type(),
                        Some(value),
                        field,
                    )?;
                }
            }
            builder.append(values.is_some());
            Ok(())
        }
        ArrowDataType::FixedSizeList(element, len) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| {
                    format!(
                        "FILTER-MAP input field '{}' has an incompatible fixed-list builder",
                        field.name()
                    )
                })?;
            let expected = usize::try_from(*len).map_err(|_| {
                format!(
                    "FILTER-MAP input field '{}' has invalid array length",
                    field.name()
                )
            })?;
            let values = match value {
                Some(RuntimeValue::Array(values)) if values.len() == expected => Some(values),
                Some(RuntimeValue::Array(values)) => {
                    return Err(format!(
                        "FILTER-MAP input field '{}' expected array length {expected}, got {}",
                        field.name(),
                        values.len()
                    ));
                }
                None => None,
                Some(value) => {
                    return Err(format!(
                        "FILTER-MAP input field '{}' expected ARRAY, got {}",
                        field.name(),
                        runtime_value_type_name(value)
                    ));
                }
            };
            for index in 0..expected {
                append_filter_map_nested_value(
                    builder.values().as_mut(),
                    element.data_type(),
                    values.map(|values| &values[index]),
                    field,
                )?;
            }
            builder.append(values.is_some());
            Ok(())
        }
        data_type => Err(format!(
            "FILTER-MAP input field '{}' has unsupported nested type {data_type:?}",
            field.name()
        )),
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use nervix_models::{
        AckMode, CreateEmitter, CreateSchema, EmitSink, EmitterPublishingMode, ErrorPolicies,
        FieldPath, MessageErrorCode, MessageErrorOperation, ModelName, ParseAsType,
        ProcessorInputs, RetryPolicy, SqsFifoGroup, Timestamp,
    };
    use nonzero_ext::nonzero;
    use ordered_float::OrderedFloat;
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime_ack::AckSet,
        runtime_schema::{
            RuntimeRecordBatch, RuntimeRow, RuntimeValue, compile_schema, test_runtime_row,
        },
    };
    #[tokio::test]
    async fn filter_map_can_read_branch_namespace() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("amount", ParseAsType::I64),
            ("branch_tenant", ParseAsType::String),
        ]);
        let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
        let program = compile_processor_output_filter_map_program(
            RuntimeCompileTarget {
                domain: &domain("default"),
                identifier: &named("project_notifications"),
            },
            &[named("notifications")],
            &named("projected_notifications"),
            &construction(
                "INHERIT ALL SET branch_tenant = branch.tenant, amount = amount + 1 WHERE \
                 branch.tenant = output.tenant",
            ),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: input_schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            None,
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[named("tenant")],
                current_branch_schema: Some(&branch_schema),
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("filter-map should compile")
        .expect("program should exist");

        let (acks, _completion) = AckSet::root();
        let batch = RelayRecordBatch::from_messages(
            input_schema,
            vec![RelayMessage {
                key: string_branch_key("tenant", "acme"),
                record: test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    ("amount".to_string(), RuntimeValue::I64(7)),
                    (
                        "branch_tenant".to_string(),
                        RuntimeValue::String("".to_string()),
                    ),
                ]),
                acks,
            }],
        )
        .expect("batch should build");

        let plan = plan_filter_map_messages(
            "deduplicator",
            &named::<ModelName>("project_notifications"),
            "FILTER-MAP",
            &program,
            batch,
            current_timestamp(),
            &HashMap::default(),
        )
        .await
        .expect("filter-map planning should succeed");

        assert!(
            plan.message_errors.is_empty(),
            "projection should not produce message errors: {:?}",
            plan.message_errors
                .iter()
                .map(|error| error.error.message.as_str())
                .collect::<Vec<_>>()
        );

        let messages = plan
            .batch
            .expect("filter-map should produce a batch")
            .try_into_messages()
            .expect("filter-map batch should convert to messages");

        assert_eq!(messages.len(), 1);
        assert_eq!(
            row_value(&messages[0].record, "branch_tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            row_value(&messages[0].record, "amount"),
            Some(RuntimeValue::I64(8))
        );
    }

    #[tokio::test]
    async fn projection_can_read_branch_namespace() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("active", ParseAsType::Bool),
            ("amount", ParseAsType::I64),
        ]);
        let output_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("amount", ParseAsType::I64),
            ("branch_tenant", ParseAsType::String),
        ]);
        let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
        let program = compile_processor_output_filter_map_program(
            RuntimeCompileTarget {
                domain: &domain("default"),
                identifier: &named("project_notifications"),
            },
            &[named("notifications")],
            &named("projected_notifications"),
            &construction(
                "INHERIT tenant, amount SET branch_tenant = branch.tenant, amount = amount + 1 \
                 WHERE branch.tenant = output.tenant",
            ),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: output_schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            None,
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[named("tenant")],
                current_branch_schema: Some(&branch_schema),
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("filter-map should compile")
        .expect("program should exist");

        let (acks, _completion) = AckSet::root();
        let batch = RelayRecordBatch::from_messages(
            input_schema,
            vec![RelayMessage {
                key: string_branch_key("tenant", "acme"),
                record: test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    ("active".to_string(), RuntimeValue::Bool(true)),
                    ("amount".to_string(), RuntimeValue::I64(7)),
                ]),
                acks,
            }],
        )
        .expect("batch should build");

        let plan = plan_filter_map_messages(
            "processor",
            &named::<ModelName>("project_notifications"),
            "FILTER-MAP",
            &program,
            batch,
            current_timestamp(),
            &HashMap::default(),
        )
        .await
        .expect("filter-map planning should succeed");

        assert!(
            plan.message_errors.is_empty(),
            "projection should not produce message errors: {:?}",
            plan.message_errors
                .iter()
                .map(|error| error.error.message.as_str())
                .collect::<Vec<_>>()
        );

        let messages = plan
            .batch
            .expect("filter-map should produce a batch")
            .try_into_messages()
            .expect("filter-map batch should convert to messages");

        assert_eq!(messages.len(), 1);
        assert_eq!(
            row_value(&messages[0].record, "branch_tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            row_value(&messages[0].record, "amount"),
            Some(RuntimeValue::I64(8))
        );
        assert_eq!(
            messages[0].key.as_ref(),
            string_branch_key("tenant", "acme").as_ref()
        );
    }

    #[tokio::test]
    async fn inherit_all_preserves_fixed_size_array_values_through_the_vm() {
        let schema = test_schema(&[(
            "vector",
            ParseAsType::Array {
                element: Box::new(ParseAsType::F32),
                len: nonzero!(2u32),
            },
        )]);
        let program = compile_processor_output_filter_map_program(
            RuntimeCompileTarget {
                domain: &domain("default"),
                identifier: &named("copy_vectors"),
            },
            &[named("vectors")],
            &named("copied_vectors"),
            &construction("INHERIT ALL"),
            RuntimeVmSchemaPair {
                input: schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            None,
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("array inheritance should compile")
        .expect("INHERIT ALL should produce a VM program");
        let expected = RuntimeValue::Array(vec![
            RuntimeValue::F32(1.25.into()),
            RuntimeValue::F32((-2.5).into()),
        ]);
        let batch = RelayRecordBatch::from_messages(
            schema,
            vec![RelayMessage {
                key: None,
                record: test_runtime_row([("vector".to_string(), expected.clone())]),
                acks: AckSet::empty(),
            }],
        )
        .expect("array input batch should build");

        let plan = plan_filter_map_messages(
            "junction",
            &named::<ModelName>("copy_vectors"),
            "FILTER-MAP",
            &program,
            batch,
            current_timestamp(),
            &HashMap::default(),
        )
        .await
        .expect("array inheritance should execute");

        assert!(plan.message_errors.is_empty());
        let output = plan.batch.expect("array output batch should exist");
        let record = output
            .runtime_row(0)
            .expect("array output row should materialize at the test boundary");
        assert_eq!(row_value(&record, "vector"), Some(expected));
    }

    #[tokio::test]
    async fn ordered_set_error_reports_operation_index_and_previous_partial_value() {
        let input_schema = test_schema(&[
            ("amount", ParseAsType::I64),
            ("denominator", ParseAsType::I64),
        ]);
        let output_schema = test_schema(&[("amount", ParseAsType::I64)]);
        let program = compile_processor_output_filter_map_program(
            RuntimeCompileTarget {
                domain: &domain("default"),
                identifier: &named("calculate_amount"),
            },
            &[named("amounts")],
            &named("calculated_amounts"),
            &construction("SET amount = input.amount, amount = amount / input.denominator"),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: output_schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            None,
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("ordered SET should compile")
        .expect("ordered SET should produce a program");
        let batch = RelayRecordBatch::from_messages(
            input_schema,
            vec![
                RelayMessage {
                    key: None,
                    record: test_runtime_row([
                        ("amount".to_string(), RuntimeValue::I64(7)),
                        ("denominator".to_string(), RuntimeValue::I64(0)),
                    ]),
                    acks: AckSet::empty(),
                },
                RelayMessage {
                    key: None,
                    record: test_runtime_row([
                        ("amount".to_string(), RuntimeValue::I64(10)),
                        ("denominator".to_string(), RuntimeValue::I64(2)),
                    ]),
                    acks: AckSet::empty(),
                },
            ],
        )
        .expect("input batch should build");

        let plan = plan_filter_map_messages(
            "junction",
            &named::<ModelName>("calculate_amount"),
            "FILTER-MAP",
            &program,
            batch,
            current_timestamp(),
            &HashMap::default(),
        )
        .await
        .expect("a side error is a planned message error");

        let successful = plan
            .batch
            .as_ref()
            .expect("the successful row should remain in the output batch");
        assert_eq!(successful.message_count(), 1);
        let successful_record = successful
            .runtime_row(0)
            .expect("successful output row should materialize at the test boundary");
        assert_eq!(
            row_value(&successful_record, "amount"),
            Some(RuntimeValue::I64(5))
        );
        let [error] = plan.message_errors.as_slice() else {
            panic!("expected exactly one planned message error");
        };
        assert_eq!(error.error.code, MessageErrorCode::Evaluation);
        assert_eq!(error.error.operation, MessageErrorOperation::Set);
        assert_eq!(error.error.operation_index, Some(1));
        assert_eq!(
            error
                .error
                .fields
                .iter()
                .map(FieldPath::as_str)
                .collect::<Vec<_>>(),
            vec!["input.denominator", "output.amount"]
        );
        assert_eq!(
            error.partial_output.as_ref().and_then(|output| output
                .value(0, "amount")
                .expect("partial output is readable")),
            Some(RuntimeValue::I64(7)),
            "partial output: {:?}; error: {}",
            error.partial_output,
            error.error.message
        );
    }

    #[test]
    fn filter_map_rejects_missing_branch_key() {
        let schema = test_schema(&[("tenant", ParseAsType::String)]);
        let branch_schema = test_schema(&[("region", ParseAsType::String)]).arrow_schema();
        let error = compile_processor_output_filter_map_program(
            RuntimeCompileTarget {
                domain: &domain("default"),
                identifier: &named("project_notifications"),
            },
            &[named("notifications")],
            &named("projected_notifications"),
            &construction("INHERIT ALL WHERE branch.tenant = output.tenant"),
            RuntimeVmSchemaPair {
                input: schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            None,
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[named("region")],
                current_branch_schema: Some(&branch_schema),
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect_err("branch namespace must reject missing keys");
        let error = error.to_string();

        assert!(
            error.contains("branch.tenant") || error.contains("tenant"),
            "expected missing branch key error, got {error}"
        );
    }

    #[tokio::test]
    async fn emitter_invocations_run_after_set_for_selected_rows_and_append_headers() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("raw", ParseAsType::String),
            ("active", ParseAsType::Bool),
        ]);
        let output_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("normalized", ParseAsType::String),
        ]);
        let emitter = CreateEmitter {
            name: named("kafka_notifications"),
            from: ProcessorInputs::single(named("notifications")),
            encode_using_codec: Some(named("notification_codec")),
            sink: Box::new(EmitSink::Kafka {
                client: named("kafka_main"),
                topic: named("notifications_out"),
            }),
            flush_policy: FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode: EmitterPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            construction: construction(
                "INHERIT tenant SET normalized = lower(input.raw) WHERE input.active INVOKE \
                 write_header(lower(\"TENANT\"),
                 input.tenant), write_header(\"route\", output.normalized), \
                 write_header(\"route\", \"second\")",
            ),
            materialized_state: Vec::new(),
        };
        let program = compile_emitter_filter_map_program(
            &domain("default"),
            &emitter,
            input_schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            output_schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("emitter filter-map must compile")
        .expect("program must exist");
        let mut unsupported_emitter = emitter.clone();
        *unsupported_emitter.sink = EmitSink::ZeroMq {
            client: named("zeromq_main"),
        };
        let error = compile_emitter_filter_map_program(
            &domain("default"),
            &unsupported_emitter,
            input_schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            output_schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect_err("ZeroMQ emitters must reject write_header");
        assert!(error.to_string().contains("ZEROMQ emitters do not support"));
        let messages = [true, false]
            .into_iter()
            .map(|active| {
                let (acks, _completion) = AckSet::root();
                RelayMessage {
                    key: None,
                    record: test_runtime_row([
                        (
                            "tenant".to_string(),
                            RuntimeValue::String("acme".to_string()),
                        ),
                        (
                            "raw".to_string(),
                            RuntimeValue::String("FAST-LANE".to_string()),
                        ),
                        ("active".to_string(), RuntimeValue::Bool(active)),
                    ]),
                    acks,
                }
            })
            .collect::<Vec<_>>();
        let batch =
            RelayRecordBatch::from_messages(input_schema, messages).expect("batch must build");

        let plan = plan_emitter_filter_map_batch(
            &emitter.name,
            &program,
            batch,
            Timestamp::from_unix_nanos(1),
            &HashMap::default(),
        )
        .await
        .expect("emitter filter-map must execute");

        let output = plan
            .batch
            .expect("selected emitter output must remain a batch");
        assert_eq!(output.batch.batch().num_rows(), 1);
        let output_record = output
            .runtime_row(0)
            .expect("test may inspect the selected output row");
        assert_eq!(plan.source_rows, vec![0]);
        assert_eq!(
            row_value(&output_record, "normalized"),
            Some(RuntimeValue::String("fast-lane".to_string()))
        );
        assert_eq!(
            plan.headers,
            Some(vec![vec![
                ("tenant".to_string(), "acme".to_string()),
                ("route".to_string(), "fast-lane".to_string()),
                ("route".to_string(), "second".to_string()),
            ]])
        );
    }

    #[tokio::test]
    async fn sqs_fifo_group_expression_evaluates_per_source_row_in_order() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("region", ParseAsType::String),
        ]);
        let emitter = CreateEmitter {
            name: named("sqs_notifications"),
            from: ProcessorInputs::single(named("notifications")),
            encode_using_codec: Some(named("notification_codec")),
            sink: Box::new(EmitSink::Sqs {
                client: named("sqs_main"),
                queue: "notifications.fifo".to_string(),
                fifo_group: Some(SqsFifoGroup::Expression(expression(
                    "concat(input.tenant, '-', input.region)",
                ))),
            }),
            flush_policy: FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode: EmitterPublishingMode::SqsBatch {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            construction: construction("INHERIT ALL"),
            materialized_state: Vec::new(),
        };
        let program = compile_sqs_fifo_group_program(
            &domain("default"),
            &emitter,
            input_schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("SQS FIFO group expression must compile")
        .expect("expression mode must produce a program");
        let messages = [("acme", "us"), ("globex", "eu"), ("acme", "ap")]
            .into_iter()
            .map(|(tenant, region)| {
                let (acks, _completion) = AckSet::root();
                RelayMessage {
                    key: None,
                    record: test_runtime_row([
                        (
                            "tenant".to_string(),
                            RuntimeValue::String(tenant.to_string()),
                        ),
                        (
                            "region".to_string(),
                            RuntimeValue::String(region.to_string()),
                        ),
                    ]),
                    acks,
                }
            })
            .collect::<Vec<_>>();
        let batch = RelayRecordBatch::from_messages(input_schema, messages)
            .expect("SQS source batch must build");

        let groups = evaluate_sqs_fifo_group_program(
            &emitter.name,
            &program,
            &batch,
            Timestamp::from_unix_nanos(1),
            &HashMap::default(),
        )
        .await
        .expect("SQS FIFO group expression must execute");

        assert_eq!(
            groups,
            vec![
                Ok(Some("acme-us".to_string())),
                Ok(Some("globex-eu".to_string())),
                Ok(Some("acme-ap".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn filter_map_on_runtime_row_evaluates_only_selected_arrow_row() {
        let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::U32)]);
        let where_clause = expression("input.value = (3 AS U32)");
        let program = compile_session_filter_map_program(
            &domain("default"),
            named::<ModelName>("selected_row_subscription"),
            Some(&where_clause),
            schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("subscription filter must compile")
        .expect("WHERE clause must produce a program");
        let rows = [
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("value".to_string(), RuntimeValue::U32(1)),
            ]),
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("value".to_string(), RuntimeValue::U32(3)),
            ]),
        ];
        let row_batches = rows
            .iter()
            .map(RuntimeRow::one_row_batch)
            .collect::<Vec<_>>();
        let batch = RuntimeRecordBatch::concat(&row_batches.iter().collect::<Vec<_>>())
            .expect("multi-row Arrow batch should build");
        let selected = RuntimeRow::new(Arc::new(batch), 1, rows[1].metadata().clone())
            .expect("second Arrow row should be addressable");

        let output = execute_filter_map_for_test(
            &program,
            selected,
            None,
            None,
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("selected row filter-map must execute")
        .expect("second Arrow row must pass the filter");

        assert_eq!(row_value(&output, "value"), Some(RuntimeValue::U32(3)));
    }

    #[tokio::test]
    async fn filter_map_internal_types_roundtrip_matches_http_logic_fixture() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("active", ParseAsType::Bool),
            ("u8", ParseAsType::U8),
            ("i8", ParseAsType::I8),
            ("u16", ParseAsType::U16),
            ("i16", ParseAsType::I16),
            ("u32", ParseAsType::U32),
            ("i32", ParseAsType::I32),
            ("u64", ParseAsType::U64),
            ("i64", ParseAsType::I64),
            ("f32", ParseAsType::F32),
            ("f64", ParseAsType::F64),
            ("occurred_at", ParseAsType::Datetime),
            ("raw", ParseAsType::String),
        ]);
        let output_schema = Arc::new(compile_schema(&CreateSchema {
            name: named("logic_output"),
            fields: vec![
                nervix_models::SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("u8_next"),
                    ty: ParseAsType::U8,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("i8_abs"),
                    ty: ParseAsType::I8,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("u16_keep"),
                    ty: ParseAsType::U16,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("i16_prev"),
                    ty: ParseAsType::I16,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("u32_same"),
                    ty: ParseAsType::U32,
                    optional: true,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("i32_neg"),
                    ty: ParseAsType::I32,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("u64_next"),
                    ty: ParseAsType::U64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("i64_keep"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("f32_next"),
                    ty: ParseAsType::F32,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("f64_keep"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("bool_copy"),
                    ty: ParseAsType::Bool,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("occurred_text"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("occurred_copy"),
                    ty: ParseAsType::Datetime,
                    optional: false,
                    sensitive: false,
                },
            ],
        }));
        let program = compile_ingestor_filter_map_program(
            &domain("default"),
            named::<ModelName>("logic_ingestor"),
            IngestMetadataKind::Headers,
            true,
            &construction(
                "INHERIT tenant SET u8_next = input.u8 + (1 AS U8), i8_abs = abs(input.i8), \
                 u16_keep = coalesce(input.u16, (0 AS U16)), i16_prev = input.i16 - (1 AS I16), \
                 u32_same = coalesce(nullif(input.u32, (999 AS U32)), (0 AS U32)), i32_neg = \
                 -input.i32, u64_next = input.u64 + (2 AS U64), i64_keep = input.i64, f32_next = \
                 input.f32 + (1.5 AS F32), f64_keep = input.f64, bool_copy = input.active, \
                 occurred_text = input.occurred_at AS STRING, occurred_copy = (input.occurred_at \
                 AS STRING) AS DATETIME WHERE input.active AND input.occurred_at > \
                 ('2026-04-07T00:00:00Z' AS DATETIME)",
            ),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: output_schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("filter-map must compile")
        .expect("program must exist");

        let record = test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("active".to_string(), RuntimeValue::Bool(true)),
            ("u8".to_string(), RuntimeValue::U8(5)),
            ("i8".to_string(), RuntimeValue::I8(-7)),
            ("u16".to_string(), RuntimeValue::U16(9)),
            ("i16".to_string(), RuntimeValue::I16(12)),
            ("u32".to_string(), RuntimeValue::U32(42)),
            ("i32".to_string(), RuntimeValue::I32(-11)),
            ("u64".to_string(), RuntimeValue::U64(100)),
            ("i64".to_string(), RuntimeValue::I64(-64)),
            ("f32".to_string(), RuntimeValue::F32(OrderedFloat(2.5))),
            ("f64".to_string(), RuntimeValue::F64(OrderedFloat(7.25))),
            (
                "occurred_at".to_string(),
                RuntimeValue::Datetime(
                    chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z")
                        .expect("valid timestamp"),
                ),
            ),
            (
                "raw".to_string(),
                RuntimeValue::String("ignored".to_string()),
            ),
        ]);

        let output = execute_filter_map_for_test(
            &program,
            record,
            None,
            None,
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("filter-map must execute")
        .expect("record must not be filtered out");

        assert_eq!(
            row_value(&output, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(row_value(&output, "u8_next"), Some(RuntimeValue::U8(6)));
        assert_eq!(row_value(&output, "i8_abs"), Some(RuntimeValue::I8(7)));
        assert_eq!(row_value(&output, "u16_keep"), Some(RuntimeValue::U16(9)));
        assert_eq!(row_value(&output, "i16_prev"), Some(RuntimeValue::I16(11)));
        assert_eq!(row_value(&output, "u32_same"), Some(RuntimeValue::U32(42)));
        assert_eq!(row_value(&output, "i32_neg"), Some(RuntimeValue::I32(11)));
        assert_eq!(row_value(&output, "u64_next"), Some(RuntimeValue::U64(102)));
        assert_eq!(row_value(&output, "i64_keep"), Some(RuntimeValue::I64(-64)));
        assert_eq!(
            row_value(&output, "f32_next"),
            Some(RuntimeValue::F32(OrderedFloat(4.0)))
        );
        assert_eq!(
            row_value(&output, "f64_keep"),
            Some(RuntimeValue::F64(OrderedFloat(7.25)))
        );
        assert_eq!(
            row_value(&output, "bool_copy"),
            Some(RuntimeValue::Bool(true))
        );
        assert_eq!(
            row_value(&output, "occurred_text"),
            Some(RuntimeValue::String(
                "2026-04-07T12:34:56+00:00".to_string()
            ))
        );
        assert_eq!(
            row_value(&output, "occurred_copy"),
            Some(RuntimeValue::Datetime(
                chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z")
                    .expect("valid timestamp"),
            ))
        );
    }

    #[tokio::test]
    async fn large_vm_batches_preserve_results_through_public_vm_api() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("sequence", ParseAsType::U32),
            ("payload", ParseAsType::String),
        ]);
        let program = compile_reorderer_program(
            &named("order_notifications"),
            &[named("incoming_notifications")],
            &[expression("input.sequence")],
            input_schema.arrow_schema(),
            None,
        )
        .expect("reorderer key program should compile");
        let records = (0..=VM_SPAWN_BLOCKING_ROW_THRESHOLD)
            .map(|sequence| {
                test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    (
                        "sequence".to_string(),
                        RuntimeValue::U32(
                            u32::try_from(sequence)
                                .assured("the VM blocking threshold fits a u32 test field"),
                        ),
                    ),
                    (
                        "payload".to_string(),
                        RuntimeValue::String(format!("payload-{sequence}")),
                    ),
                ])
            })
            .collect::<Vec<_>>();
        let input = vm_input_from_test_rows(&records, &program.program.input_schema)
            .expect("VM input batch should build");

        let output = execute_program_with_selection_in_context(
            &program.program,
            &input,
            &VmExecutionContext {
                now: Timestamp::from_unix_nanos(1),
                injector: None,
            },
        )
        .await
        .expect("large VM batch should execute");

        assert_eq!(
            output.batch.row_count(),
            VM_SPAWN_BLOCKING_ROW_THRESHOLD + 1
        );
    }
}
