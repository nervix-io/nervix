use super::*;

pub(super) struct WasmOutputContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) input_relays: &'a [RelayName],
    pub(super) input_schema: &'a Arc<CompiledSchema>,
    pub(super) output_schemas: &'a [(RelayName, Arc<CompiledSchema>)],
    pub(super) key: &'a Option<BranchKey>,
    pub(super) dispatch_error: &'static str,
}

pub(super) struct WasmDecodedOutputBatch {
    pub(super) batch: RelayRecordBatch,
    pub(super) uninitialized_columns: HashSet<usize>,
}

impl WasmDecodedOutputBatch {
    pub(super) fn materialize_uninitialized_for_relay(&mut self) -> Result<(), String> {
        let schema = self.batch.arrow_schema();
        for column_index in &self.uninitialized_columns {
            let field = schema.fields().get(*column_index).ok_or_else(|| {
                format!("uninitialized output column {column_index} is outside the relay schema")
            })?;
            if !field.is_nullable() {
                return Err(format!(
                    "required relay field '{}' remains uninitialized",
                    field.name()
                ));
            }
        }
        self.uninitialized_columns.clear();
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct WasmMaterializedOutput {
    pub(super) output_route_index: usize,
    pub(super) schema: Arc<CompiledSchema>,
    pub(super) batch: RuntimeRecordBatch,
    pub(super) acks: WasmAckSidecar,
    pub(super) uninitialized_columns: HashSet<usize>,
}

#[derive(Debug, Error)]
pub(super) enum WasmOutputError {
    #[error("expected an output envelope at callback index {envelope_index}")]
    UnexpectedEnvelopeKind { envelope_index: usize },
    #[error("WASM output group at callback index {envelope_index} has no routed outputs")]
    EmptyOutputGroup { envelope_index: usize },
    #[error("unknown WASM output relay '{output_relay}'")]
    UnknownOutputRelay { output_relay: String },
    #[error(
        "WASM output relay '{output_relay}' has {actual} columns, but its destination schema has \
         {expected} fields"
    )]
    RoutedOutputColumnCountMismatch {
        output_relay: String,
        expected: usize,
        actual: usize,
    },
    #[error("WASM output group has invalid generated Arrow IPC: {reason}")]
    InvalidGeneratedArrowIpc { reason: String },
    #[error("WASM generated Arrow IPC has {actual} record batches instead of exactly one")]
    GeneratedRecordBatchCount { actual: usize },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} ('{field_name}') references \
         generated column {column_index}, but the generated pool has {generated_column_count} \
         columns"
    )]
    GeneratedColumnOutOfRange {
        output_relay: String,
        field_index: usize,
        field_name: String,
        column_index: u32,
        generated_column_count: usize,
    },
    #[error("WASM generated column {column_index} is not referenced by any routed output")]
    UnreferencedGeneratedColumn { column_index: usize },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} ('{field_name}') references \
         incompatible generated column {column_index}: expected {expected}, actual {actual}"
    )]
    GeneratedColumnTypeMismatch {
        output_relay: String,
        field_index: usize,
        field_name: String,
        column_index: u32,
        expected: String,
        actual: String,
    },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} ('{field_name}') references \
         generated column {column_index} with {actual} rows, but the routed output has {expected} \
         rows"
    )]
    GeneratedColumnRowCountMismatch {
        output_relay: String,
        field_index: usize,
        field_name: String,
        column_index: u32,
        expected: usize,
        actual: usize,
    },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} references input column \
         {column_index}, but the input schema has {input_column_count} fields"
    )]
    InputColumnOutOfRange {
        output_relay: String,
        field_index: usize,
        column_index: u32,
        input_column_count: usize,
    },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} references incompatible input \
         column {column_index}: expected {expected}, actual {actual}"
    )]
    InputColumnTypeMismatch {
        output_relay: String,
        field_index: usize,
        column_index: u32,
        expected: String,
        actual: String,
    },
    #[error("WASM output relay '{output_relay}' row {row_index} is missing a source token")]
    MissingSourceToken {
        output_relay: String,
        row_index: usize,
    },
    #[error(
        "WASM output relay '{output_relay}' row {row_index} references unknown source token \
         {token}"
    )]
    UnknownSourceToken {
        output_relay: String,
        row_index: usize,
        token: u64,
    },
    #[error(
        "WASM output relay '{output_relay}' row {row_index} source token {token} is absent from \
         row lineage"
    )]
    SourceTokenNotCarried {
        output_relay: String,
        row_index: usize,
        token: u64,
    },
    #[error("invalid WASM token decision for token {token}: {reason}")]
    InvalidTokenDecision { token: u64, reason: String },
    #[error("failed to build WASM output batch for relay '{output_relay}': {reason}")]
    OutputBatchBuild {
        output_relay: String,
        reason: String,
    },
}

pub(super) struct WasmOutputValidator<'a> {
    pub(super) ack_map: &'a WasmAckMap,
    pub(super) input_schema: &'a Arc<CompiledSchema>,
    pub(super) output_schemas: &'a [(RelayName, Arc<CompiledSchema>)],
    pub(super) output_routes: &'a RelayProcessorOutputsNode,
}

impl WasmOutputValidator<'_> {
    pub(super) fn validate(
        &self,
        outputs: Vec<WasmEnvelope>,
    ) -> Result<Vec<WasmMaterializedOutput>, WasmOutputError> {
        self.validate_token_decisions(&outputs)?;
        let mut materialized = Vec::new();
        for (envelope_index, output) in outputs.into_iter().enumerate() {
            materialized.extend(self.materialize_group(envelope_index, output)?);
        }
        Ok(materialized)
    }

    pub(super) fn validate_token_decisions(
        &self,
        outputs: &[WasmEnvelope],
    ) -> Result<(), WasmOutputError> {
        let mut carried_tokens = HashSet::<u64>::default();
        let mut terminal_tokens = HashSet::<u64>::default();
        for (envelope_index, output) in outputs.iter().enumerate() {
            let WasmEnvelope::Output { outputs, .. } = output else {
                return Err(WasmOutputError::UnexpectedEnvelopeKind { envelope_index });
            };
            if outputs.is_empty() {
                return Err(WasmOutputError::EmptyOutputGroup { envelope_index });
            }
            for output in outputs {
                for (row_index, row) in output.acks.rows.iter().enumerate() {
                    if let Some(source_token) = row.source_token
                        && !self.ack_map.contains_key(&source_token.0)
                    {
                        return Err(WasmOutputError::UnknownSourceToken {
                            output_relay: output.output_relay.clone(),
                            row_index,
                            token: source_token.0,
                        });
                    }
                    let mut row_tokens = HashSet::<u64>::default();
                    for token in &row.tokens {
                        if !self.ack_map.contains_key(&token.0) {
                            return Err(WasmOutputError::InvalidTokenDecision {
                                token: token.0,
                                reason: "carried token is unknown to this branch instance"
                                    .to_string(),
                            });
                        }
                        if !row_tokens.insert(token.0) {
                            return Err(WasmOutputError::InvalidTokenDecision {
                                token: token.0,
                                reason: "token occurs more than once in one output row".to_string(),
                            });
                        }
                        carried_tokens.insert(token.0);
                    }
                }
                for token_set in &output.acks.acked {
                    self.validate_terminal_set(token_set, &mut terminal_tokens, "ACK")?;
                }
                for token_set in &output.acks.nacked {
                    self.validate_terminal_tokens(&token_set.tokens, &mut terminal_tokens, "NACK")?;
                }
                for token_set in &output.acks.message_errors {
                    self.validate_terminal_tokens(
                        &token_set.tokens,
                        &mut terminal_tokens,
                        "message error",
                    )?;
                }
            }
        }
        if let Some(token) = carried_tokens.intersection(&terminal_tokens).next() {
            return Err(WasmOutputError::InvalidTokenDecision {
                token: *token,
                reason: "token is both carried and terminally completed in one callback"
                    .to_string(),
            });
        }
        Ok(())
    }

    pub(super) fn validate_terminal_set(
        &self,
        token_set: &WasmAckTokenSet,
        terminal_tokens: &mut HashSet<u64>,
        decision: &str,
    ) -> Result<(), WasmOutputError> {
        self.validate_terminal_tokens(&token_set.tokens, terminal_tokens, decision)
    }

    pub(super) fn validate_terminal_tokens(
        &self,
        tokens: &[WasmAckToken],
        terminal_tokens: &mut HashSet<u64>,
        decision: &str,
    ) -> Result<(), WasmOutputError> {
        for token in tokens {
            if !self.ack_map.contains_key(&token.0) {
                return Err(WasmOutputError::InvalidTokenDecision {
                    token: token.0,
                    reason: format!("terminal {decision} token is unknown to this branch instance"),
                });
            }
            if !terminal_tokens.insert(token.0) {
                return Err(WasmOutputError::InvalidTokenDecision {
                    token: token.0,
                    reason: "token receives more than one terminal decision in one callback"
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    pub(super) fn materialize_group(
        &self,
        envelope_index: usize,
        output: WasmEnvelope,
    ) -> Result<Vec<WasmMaterializedOutput>, WasmOutputError> {
        let WasmEnvelope::Output {
            generated_arrow_ipc_batch,
            outputs,
        } = output
        else {
            return Err(WasmOutputError::UnexpectedEnvelopeKind { envelope_index });
        };
        if outputs.is_empty() {
            return Err(WasmOutputError::EmptyOutputGroup { envelope_index });
        }
        let generated_batch = self.decode_generated_batch(&generated_arrow_ipc_batch)?;
        let generated_column_count = match generated_batch.as_ref() {
            Some(generated_batch) => generated_batch.num_columns(),
            None => 0,
        };
        let mut referenced_generated_columns = vec![false; generated_column_count];
        let mut materialized = Vec::with_capacity(outputs.len());
        for output in outputs {
            materialized.push(self.materialize_routed_output(
                output,
                generated_batch.as_ref(),
                &mut referenced_generated_columns,
            )?);
        }
        if let Some(column_index) = referenced_generated_columns
            .iter()
            .position(|referenced| !referenced)
        {
            return Err(WasmOutputError::UnreferencedGeneratedColumn { column_index });
        }
        Ok(materialized)
    }

    pub(super) fn materialize_routed_output(
        &self,
        output: WasmRoutedOutput,
        generated_batch: Option<&RecordBatch>,
        referenced_generated_columns: &mut [bool],
    ) -> Result<WasmMaterializedOutput, WasmOutputError> {
        let WasmRoutedOutput {
            output_relay,
            columns,
            acks,
        } = output;
        let output_identifier =
            RelayName::parse(&output_relay).map_err(|_| WasmOutputError::UnknownOutputRelay {
                output_relay: output_relay.clone(),
            })?;
        let Some(schema) = wasm_output_schema(self.output_schemas, &output_identifier) else {
            return Err(WasmOutputError::UnknownOutputRelay { output_relay });
        };
        let Some(output_route_index) = self
            .output_routes
            .routes
            .iter()
            .position(|route| route.relay == output_identifier)
        else {
            return Err(WasmOutputError::UnknownOutputRelay { output_relay });
        };
        let destination_schema = schema.arrow_schema();
        let destination_fields = destination_schema.fields();
        if columns.len() != destination_fields.len() {
            return Err(WasmOutputError::RoutedOutputColumnCountMismatch {
                output_relay,
                expected: destination_fields.len(),
                actual: columns.len(),
            });
        }
        let has_input_columns = columns.iter().any(WasmOutputColumnRef::is_input);
        self.validate_source_tokens(&output_relay, &acks.rows, has_input_columns)?;
        let mut uninitialized_columns = HashSet::default();
        let arrays = columns
            .into_iter()
            .zip(destination_fields)
            .enumerate()
            .map(|(field_index, (column, destination_field))| match column {
                WasmOutputColumnRef::Generated { column_index } => {
                    let generated_column_count = match generated_batch {
                        Some(generated_batch) => generated_batch.num_columns(),
                        None => 0,
                    };
                    let generated_index = column_index.arch_into();
                    let Some(generated_batch) = generated_batch else {
                        return Err(WasmOutputError::GeneratedColumnOutOfRange {
                            output_relay: output_relay.clone(),
                            field_index,
                            field_name: destination_field.name().to_string(),
                            column_index,
                            generated_column_count,
                        });
                    };
                    let generated_schema = generated_batch.schema();
                    let Some(generated_field) = generated_schema.fields().get(generated_index)
                    else {
                        return Err(WasmOutputError::GeneratedColumnOutOfRange {
                            output_relay: output_relay.clone(),
                            field_index,
                            field_name: destination_field.name().to_string(),
                            column_index,
                            generated_column_count,
                        });
                    };
                    let expected_generated_field = destination_field.as_ref().clone().with_name("");
                    if generated_field.as_ref() != &expected_generated_field {
                        return Err(WasmOutputError::GeneratedColumnTypeMismatch {
                            output_relay: output_relay.clone(),
                            field_index,
                            field_name: destination_field.name().to_string(),
                            column_index,
                            expected: format!("{expected_generated_field:?}"),
                            actual: format!("{generated_field:?}"),
                        });
                    }
                    if generated_batch.num_rows() != acks.rows.len() {
                        return Err(WasmOutputError::GeneratedColumnRowCountMismatch {
                            output_relay: output_relay.clone(),
                            field_index,
                            field_name: destination_field.name().to_string(),
                            column_index,
                            expected: acks.rows.len(),
                            actual: generated_batch.num_rows(),
                        });
                    }
                    referenced_generated_columns[generated_index] = true;
                    Ok(generated_batch.column(generated_index).clone())
                }
                WasmOutputColumnRef::Input { column_index } => self.materialize_input_column(
                    &output_relay,
                    field_index,
                    destination_field,
                    column_index,
                    &acks.rows,
                ),
                WasmOutputColumnRef::Uninitialized => {
                    uninitialized_columns.insert(field_index);
                    Ok(new_null_array(
                        destination_field.data_type(),
                        acks.rows.len(),
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let record_batch =
            RecordBatch::try_new(destination_schema.clone(), arrays).map_err(|error| {
                WasmOutputError::OutputBatchBuild {
                    output_relay: output_relay.clone(),
                    reason: error.to_string(),
                }
            })?;
        let batch = RuntimeRecordBatch::from_record_batch(destination_schema, record_batch)
            .map_err(|reason| WasmOutputError::OutputBatchBuild {
                output_relay: output_relay.clone(),
                reason,
            })?;
        Ok(WasmMaterializedOutput {
            output_route_index,
            schema: Arc::clone(schema),
            batch,
            acks,
            uninitialized_columns,
        })
    }

    pub(super) fn decode_generated_batch(
        &self,
        ipc: &[u8],
    ) -> Result<Option<RecordBatch>, WasmOutputError> {
        if ipc.is_empty() {
            return Ok(None);
        }
        let invalid = |reason: String| WasmOutputError::InvalidGeneratedArrowIpc { reason };
        let mut cursor = std::io::Cursor::new(ipc);
        let (actual_schema, mut batches) = {
            let reader = StreamReader::try_new(&mut cursor, None)
                .map_err(|error| invalid(error.to_string()))?;
            let actual_schema = reader.schema();
            let batches = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| invalid(error.to_string()))?;
            (actual_schema, batches)
        };
        let consumed = cursor.position().arch_into();
        if consumed != ipc.len() {
            return Err(invalid(format!(
                "IPC stream has {} trailing bytes",
                ipc.len()
                    .checked_sub(consumed)
                    .verified("the reader consumed a prefix of this same buffer")
            )));
        }
        if batches.len() != 1 {
            return Err(WasmOutputError::GeneratedRecordBatchCount {
                actual: batches.len(),
            });
        }
        if actual_schema.fields().is_empty() {
            return Err(invalid(
                "encoded zero-column Arrow streams are not valid empty generated pools".to_string(),
            ));
        }
        if let Some(field_index) = actual_schema
            .fields()
            .iter()
            .position(|field| !field.name().is_empty())
        {
            return Err(invalid(format!(
                "generated field {field_index} has non-empty name '{}'",
                actual_schema.field(field_index).name()
            )));
        }
        Ok(batches.pop())
    }

    pub(super) fn validate_source_tokens(
        &self,
        output_relay: &str,
        rows: &[WasmOutputRow],
        required: bool,
    ) -> Result<(), WasmOutputError> {
        for (row_index, row) in rows.iter().enumerate() {
            let Some(source_token) = row.source_token else {
                if required {
                    return Err(WasmOutputError::MissingSourceToken {
                        output_relay: output_relay.to_string(),
                        row_index,
                    });
                }
                continue;
            };
            if !self.ack_map.contains_key(&source_token.0) {
                return Err(WasmOutputError::UnknownSourceToken {
                    output_relay: output_relay.to_string(),
                    row_index,
                    token: source_token.0,
                });
            }
            // Bounded by the ack tokens the guest attached to this one row, so a per-row set
            // would allocate more than the walk it replaces.
            if !row.tokens.contains(&source_token) {
                return Err(WasmOutputError::SourceTokenNotCarried {
                    output_relay: output_relay.to_string(),
                    row_index,
                    token: source_token.0,
                });
            }
        }
        Ok(())
    }

    pub(super) fn materialize_input_column(
        &self,
        output_relay: &str,
        field_index: usize,
        destination_field: &StdArc<arrow_schema::Field>,
        column_index: u32,
        rows: &[WasmOutputRow],
    ) -> Result<ArrayRef, WasmOutputError> {
        let input_index = column_index.arch_into();
        let input_schema = self.input_schema.arrow_schema();
        let Some(source_field) = input_schema.fields().get(input_index) else {
            return Err(WasmOutputError::InputColumnOutOfRange {
                output_relay: output_relay.to_string(),
                field_index,
                column_index,
                input_column_count: input_schema.fields().len(),
            });
        };
        if source_field.data_type() != destination_field.data_type()
            || source_field.is_nullable() != destination_field.is_nullable()
        {
            return Err(WasmOutputError::InputColumnTypeMismatch {
                output_relay: output_relay.to_string(),
                field_index,
                column_index,
                expected: format!("{destination_field:?}"),
                actual: format!("{source_field:?}"),
            });
        }
        if rows.is_empty() {
            return Ok(new_empty_array(destination_field.data_type()));
        }
        let sources = rows
            .iter()
            .enumerate()
            .map(|(row_index, row)| {
                let source_token =
                    row.source_token
                        .ok_or_else(|| WasmOutputError::MissingSourceToken {
                            output_relay: output_relay.to_string(),
                            row_index,
                        })?;
                self.ack_map.get(&source_token.0).ok_or_else(|| {
                    WasmOutputError::UnknownSourceToken {
                        output_relay: output_relay.to_string(),
                        row_index,
                        token: source_token.0,
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let first = sources[0];
        let one_batch = sources
            .iter()
            .all(|source| Arc::ptr_eq(&first.input_batch, &source.input_batch));
        if one_batch {
            let array = first.input_batch.batch().column(input_index);
            let identity = sources.len() == first.input_batch.batch().num_rows()
                && sources
                    .iter()
                    .enumerate()
                    .all(|(row, source)| source.input_row == row);
            if identity {
                return Ok(array.clone());
            }
            let start = first.input_row;
            let contiguous = sources
                .iter()
                .enumerate()
                .all(|(offset, source)| Some(source.input_row) == start.checked_add(offset));
            if contiguous {
                return Ok(array.slice(start, sources.len()));
            }
            let indices = UInt64Array::from_iter_values(
                sources.iter().map(|source| source.input_row.arch_into()),
            );
            return take_arrow_array(array.as_ref(), &indices, None).map_err(|error| {
                WasmOutputError::OutputBatchBuild {
                    output_relay: output_relay.to_string(),
                    reason: error.to_string(),
                }
            });
        }
        let slices = sources
            .iter()
            .map(|source| {
                source
                    .input_batch
                    .batch()
                    .column(input_index)
                    .slice(source.input_row, 1)
            })
            .collect::<Vec<_>>();
        let arrays = slices
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        concat_arrow_arrays(&arrays).map_err(|error| WasmOutputError::OutputBatchBuild {
            output_relay: output_relay.to_string(),
            reason: error.to_string(),
        })
    }
}

pub(super) async fn dispatch_wasm_output_envelopes(
    context: WasmOutputContext<'_>,
    outputs: Vec<WasmEnvelope>,
    ack_map: &mut WasmAckMap,
) -> Result<(), String> {
    let WasmOutputContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        output_routes,
        input_relays,
        input_schema,
        output_schemas,
        key,
        dispatch_error,
    } = context;
    let validated_outputs = match (WasmOutputValidator {
        ack_map,
        input_schema,
        output_schemas,
        output_routes,
    })
    .validate(outputs)
    {
        Ok(outputs) => outputs,
        Err(error) => {
            let reason = format!(
                "wasm processor '{}' produced invalid output: {}",
                processor.as_str(),
                error
            );
            branch.runtime.handle_general_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                ack_map.values().map(|context| &context.acks),
                reason,
            );
            ack_map.clear();
            return Ok(());
        }
    };
    let mut token_use_counts = wasm_output_token_use_counts(&validated_outputs);
    for output in validated_outputs {
        let output_route = &output_routes.routes[output.output_route_index];
        let message_error_relay = output_route.relay.clone();
        let message_error_policy = output_route.message_error_policy.clone();
        apply_wasm_sidecar_terminal_decisions(
            WasmSidecarTerminalContext {
                branch,
                node_kind,
                processor,
                error_policies,
                message_error_relay: &message_error_relay,
                message_error_policy: &message_error_policy,
            },
            ack_map,
            &output.acks,
        )
        .await;
        let output_route = &mut output_routes.routes[output.output_route_index];
        let output_batch = relay_batch_from_wasm_output(
            key,
            output.schema,
            output.batch,
            output.acks.rows,
            output.uninitialized_columns,
            ack_map,
            &mut token_use_counts,
        )?;
        if output_batch.batch.message_count() == 0 {
            continue;
        }
        if let Some(acks) = dispatch_wasm_output_route(
            WasmRouteDispatchContext {
                graph,
                branch,
                node_kind,
                processor,
                error_policies,
                input_relays,
                dispatch_error,
            },
            output_batch,
            output_route,
        )
        .await
        {
            for ack in acks {
                ack.ack_success();
            }
        } else {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                ack_map.values().map(|context| &context.acks),
                format!("wasm processor '{}' {}", processor.as_str(), dispatch_error),
            );
        }
    }
    Ok(())
}

pub(super) struct WasmRouteDispatchContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) input_relays: &'a [RelayName],
    pub(super) dispatch_error: &'static str,
}

pub(super) async fn dispatch_wasm_output_route(
    context: WasmRouteDispatchContext<'_>,
    mut decoded: WasmDecodedOutputBatch,
    output: &mut RelayProcessorOutputNode,
) -> Option<Vec<AckSet>> {
    if output.compiled_program.is_none() {
        let Some(primary_input_relay) = context.input_relays.first() else {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "wasm processor '{}' has no input relays",
                        context.processor.as_str()
                    ),
                );
            return None;
        };
        let materialized_stream_specs = materialized_stream_specs_for_graph(
            &context.branch.runtime,
            &context.branch.domain,
            context.graph,
        );
        let mut current_branching = Vec::new();
        if let Some(execution) = context
            .branch
            .runtime
            .inner
            .executions
            .get(&context.branch.domain)
            && let Some(branching) = execution.relay_branchings.get(primary_input_relay)
        {
            current_branching = branching.clone();
        }
        let current_branch_schema = relay_branch_schema_for_runtime(
            &context.branch.runtime,
            &context.branch.domain,
            primary_input_relay,
        );
        let available_lookups = match context
            .branch
            .runtime
            .inner
            .executions
            .get(&context.branch.domain)
        {
            Some(execution) => execution.lookups.clone(),
            None => HashMap::default(),
        };
        let udfs = context
            .branch
            .runtime
            .inner
            .executions
            .get(&context.branch.domain)
            .map(|execution| execution.udfs.clone());
        let output_schema = match relay_schema_for_runtime(
            &context.branch.runtime,
            &context.branch.domain,
            &output.relay,
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
                        decoded.batch.acks.iter(),
                        error,
                    );
                return None;
            }
        };
        match compile_wasm_output_filter_map_program(
            &context.branch.domain,
            context.processor,
            &output.construction,
            output_schema.arrow_schema(),
            output_schema.vm_sensitivity(),
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized_stream_specs,
                available_lookups: &available_lookups,
                current_branching: &current_branching,
                current_branch_schema: current_branch_schema.as_ref(),
                current_branch_sensitivity: None,
                udfs: udfs.as_ref(),
            },
        ) {
            Ok(program) => output.compiled_program = program,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        decoded.batch.acks.iter(),
                        error.to_string(),
                    );
                return None;
            }
        }
    }

    let Some(program) = output.compiled_program.as_ref() else {
        if let Err(error) = decoded.materialize_uninitialized_for_relay() {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "wasm processor '{}' failed to materialize output for relay '{}': {}",
                        context.processor.as_str(),
                        output.relay.as_str(),
                        error
                    ),
                );
            return None;
        }
        let dispatched_acks = decoded.batch.acks.to_vec();
        if context
            .branch
            .dispatch_output(
                context.graph,
                output,
                ModelKind::WasmProcessor,
                context.processor,
                &decoded.batch,
            )
            .await
            .is_ok()
        {
            return Some(dispatched_acks);
        }
        context
            .branch
            .runtime
            .handle_internal_processor_error_for_acks(
                &context.branch.domain,
                context.node_kind,
                context.processor,
                context.error_policies,
                decoded.batch.acks.iter(),
                format!(
                    "wasm processor '{}' {} to relay '{}'",
                    context.processor.as_str(),
                    context.dispatch_error,
                    output.relay.as_str()
                ),
            );
        return None;
    };
    let execution_now = context
        .branch
        .runtime
        .current_stream_expiration_time(&context.branch.domain)
        .ok()
        .flatten()
        .unwrap_or_else(current_timestamp);
    let owner_nodes = match context
        .branch
        .runtime
        .inner
        .executions
        .get(&context.branch.domain)
    {
        Some(execution) => execution.materialized_stream_owner_nodes.clone(),
        None => HashMap::default(),
    };
    let side_inputs = match context
        .branch
        .runtime
        .load_materialized_side_inputs(
            &context.branch.domain,
            &decoded.batch.key,
            &program.materialized_interest,
            &owner_nodes,
        )
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
                    decoded.batch.acks.iter(),
                    format!(
                        "{} '{}' failed to load materialized side inputs: {}",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let output_arrow_schema = decoded.batch.arrow_schema();
    let uninitialized_input = VmUninitializedInput {
        fields: decoded
            .uninitialized_columns
            .iter()
            .filter_map(|column_index| output_arrow_schema.fields().get(*column_index))
            .map(|field| format!("generated.{}", field.name()))
            .collect(),
    };
    let lookup_columns = match compute_lookup_hash_map_columns(
        program,
        &FilterMapBatchInputs {
            carrier: &decoded.batch.batch,
            namespace_batches: &[],
            keys: &decoded.batch.keys,
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
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "{} '{}' failed to prepare LOOKUP_HASH_MAP columns: {}",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let vm_input = match project_vm_input_batch(
        &program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: &decoded.batch.batch,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &decoded.batch.keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized_input),
        },
        None,
    ) {
        Ok(input) => input,
        Err(error) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "{} '{}' failed to project WASM output into FILTER-MAP input: {}",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let executed = match execute_program_with_selection_in_context(
        &program.compiled,
        &vm_input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    {
        Ok(executed) => executed,
        Err(error) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "{} '{}' FILTER-MAP execution failed: {}",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let mut success_output_rows = Vec::new();
    let mut success_input_rows = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, input_row) in executed.selected_rows.iter().enumerate() {
        if let Some(side_error) = executed.batch.errors().row(output_row).first() {
            let partial_output = captured_partial_output(&executed.batch, output_row);
            let record = match decoded.batch.runtime_row(input_row) {
                Ok(record) => record,
                Err(error) => {
                    context
                        .branch
                        .runtime
                        .handle_internal_processor_error_for_acks(
                            &context.branch.domain,
                            context.node_kind,
                            context.processor,
                            context.error_policies,
                            decoded.batch.acks.iter(),
                            format!(
                                "{} '{}' failed to address WASM input row {}: {}",
                                context.node_kind.as_str(),
                                context.processor.as_str(),
                                input_row,
                                error
                            ),
                        );
                    return None;
                }
            };
            message_errors.push(PendingProcessorOutputMessageError {
                row: input_row,
                key: decoded.batch.keys[input_row].clone(),
                record,
                error: program.structured_side_error(
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
                materialized_state: relay_state_snapshot_from_side_inputs(&side_inputs),
            });
            continue;
        }
        success_output_rows.push(output_row);
        success_input_rows.push(input_row);
    }
    let mut delivery_counts = vec![0usize; decoded.batch.acks.len()];
    for row in &success_input_rows {
        delivery_counts[*row] += 1;
    }
    for error in &message_errors {
        delivery_counts[error.row] += 1;
    }
    let mut ack_queues = Vec::with_capacity(decoded.batch.acks.len());
    for (row, ack) in decoded.batch.acks.into_iter().enumerate() {
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
    let mut planned_errors = Vec::new();
    for error in message_errors {
        let Some(acks) = ack_queues[error.row].pop_front() else {
            continue;
        };
        planned_errors.push(PlannedMessageError {
            message: RelayMessage {
                key: error.key,
                record: error.record,
                acks,
            },
            error: error.error,
            partial_output: error.partial_output,
            materialized_state: error.materialized_state,
        });
    }
    context
        .branch
        .runtime
        .handle_planned_message_errors_with_policy(
            &context.branch.domain,
            context.node_kind,
            context.processor,
            Some(&output.relay),
            &output.message_error_policy,
            planned_errors,
        )
        .await;
    if success_output_rows.is_empty() {
        return Some(Vec::new());
    }
    let output_batch = match vm_typed_batch_selected_rows_to_runtime_batch(
        &executed.batch,
        &success_output_rows,
    ) {
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
                    ack_queues.iter().flatten(),
                    format!(
                        "{} '{}' failed to materialize successful FILTER-MAP rows: {}",
                        context.node_kind.as_str(),
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let metadata = success_input_rows
        .iter()
        .map(|input_row| decoded.batch.metadata[*input_row].clone())
        .collect::<Vec<_>>();
    let mut batch_acks = Vec::with_capacity(success_input_rows.len());
    for row in &success_input_rows {
        let Some(acks) = ack_queues[*row].pop_front() else {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    batch_acks.iter(),
                    "WASM processor output batch ack count does not match selected row count"
                        .to_string(),
                );
            return None;
        };
        batch_acks.push(acks);
    }
    let forwarded = match RelayRecordBatch::from_filtered_parts(
        decoded.batch.key.clone(),
        output_batch,
        metadata,
        batch_acks,
    ) {
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
                    ack_queues.iter().flatten(),
                    error,
                );
            return None;
        }
    };
    let dispatched_acks = forwarded.acks.to_vec();
    if context
        .branch
        .dispatch_output(
            context.graph,
            output,
            ModelKind::WasmProcessor,
            context.processor,
            &forwarded,
        )
        .await
        .is_ok()
    {
        Some(dispatched_acks)
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
                    "wasm processor '{}' {} to relay '{}'",
                    context.processor.as_str(),
                    context.dispatch_error,
                    output.relay.as_str()
                ),
            );
        None
    }
}

pub(super) fn wasm_output_schema<'a>(
    output_schemas: &'a [(RelayName, Arc<CompiledSchema>)],
    output_relay: &RelayName,
) -> Option<&'a Arc<CompiledSchema>> {
    output_schemas
        .iter()
        .find_map(|(relay, schema)| (relay == output_relay).then_some(schema))
}

pub(super) fn wasm_output_token_use_counts(
    outputs: &[WasmMaterializedOutput],
) -> HashMap<u64, usize> {
    let mut token_use_counts = HashMap::<u64, usize>::default();
    for output in outputs {
        for row in &output.acks.rows {
            for token in &row.tokens {
                *token_use_counts.entry(token.0).or_default() += 1;
            }
        }
    }
    token_use_counts
}

pub(super) struct WasmSidecarTerminalContext<'a> {
    pub(super) branch: &'a BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) message_error_relay: &'a RelayName,
    pub(super) message_error_policy: &'a MessageErrorPolicy,
}

pub(super) async fn apply_wasm_sidecar_terminal_decisions(
    context: WasmSidecarTerminalContext<'_>,
    ack_map: &mut WasmAckMap,
    sidecar: &WasmAckSidecar,
) {
    let WasmSidecarTerminalContext {
        branch,
        node_kind,
        processor,
        error_policies,
        message_error_relay,
        message_error_policy,
    } = context;
    for message_error in &sidecar.message_errors {
        for token in &message_error.tokens {
            let context = ack_map.remove(&token.0).verified(
                "the guest output was validated against this ACK map above, and invalid output \
                 returned early",
            );
            let record = match context
                .input_batch
                .runtime_row(context.input_row, context.metadata.clone())
            {
                Ok(record) => record,
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        std::iter::once(&context.acks),
                        format!(
                            "wasm processor '{}' failed to materialize message-error input row: {}",
                            processor.as_str(),
                            error
                        ),
                    );
                    continue;
                }
            };
            branch
                .runtime
                .handle_message_error_with_policy(
                    &branch.domain,
                    node_kind,
                    processor,
                    message_error_policy,
                    RelayMessage {
                        key: branch.key.clone(),
                        record,
                        acks: context.acks,
                    },
                    MessageErrorFailure::new(
                        Some(message_error_relay),
                        message_error.reason.clone(),
                        MessageErrorOperation::Wasm,
                    ),
                )
                .await;
        }
    }
    for acked in &sidecar.acked {
        for token in &acked.tokens {
            let context = ack_map.remove(&token.0).verified(
                "the guest output was validated against this ACK map above, and invalid output \
                 returned early",
            );
            context.acks.ack_success();
        }
    }
    for nacked in &sidecar.nacked {
        for token in &nacked.tokens {
            let context = ack_map.remove(&token.0).verified(
                "the guest output was validated against this ACK map above, and invalid output \
                 returned early",
            );
            context.acks.no_ack(nacked.reason.clone());
        }
    }
}

pub(super) async fn persist_wasm_guest_state(
    runtime: &Runtime,
    processor: &ModelName,
    replicated_state: &ReplicatedWasmProcessorState,
    instance: &mut Option<Box<nervix_wasm::WasmBranchInstance>>,
) -> Result<(), String> {
    persist_wasm_guest_state_with_failure_mode(
        runtime,
        processor,
        replicated_state,
        instance,
        WasmStateSaveFailureMode::InvalidateInstance,
    )
    .await
    .map_err(|error| error.to_string())
}

pub(super) async fn checkpoint_wasm_guest_state(
    runtime: &Runtime,
    processor: &ModelName,
    replicated_state: &ReplicatedWasmProcessorState,
    instance: &mut Option<Box<nervix_wasm::WasmBranchInstance>>,
) -> OwnershipHandoffResult<()> {
    persist_wasm_guest_state_with_failure_mode(
        runtime,
        processor,
        replicated_state,
        instance,
        WasmStateSaveFailureMode::RetainInstance,
    )
    .await
}

#[derive(Clone, Copy)]
enum WasmStateSaveFailureMode {
    InvalidateInstance,
    RetainInstance,
}

async fn persist_wasm_guest_state_with_failure_mode(
    runtime: &Runtime,
    processor: &ModelName,
    replicated_state: &ReplicatedWasmProcessorState,
    instance: &mut Option<Box<nervix_wasm::WasmBranchInstance>>,
    failure_mode: WasmStateSaveFailureMode,
) -> OwnershipHandoffResult<()> {
    let save_result = match instance.as_mut() {
        Some(instance) => instance.save_state().await,
        None => {
            return Err(OwnershipHandoffError::checkpoint(format!(
                "wasm processor '{}' instance is unavailable while saving guest state",
                processor.as_str()
            )));
        }
    };
    let guest_state = match save_result {
        Ok(guest_state) => guest_state,
        Err(error) => {
            let resource_limit_exceeded = error.is_resource_limit_exceeded();
            let reason = format!(
                "wasm processor '{}' failed to save guest state: {}",
                processor.as_str(),
                error
            );
            if resource_limit_exceeded
                && let WasmStateSaveFailureMode::InvalidateInstance = failure_mode
            {
                *instance = None;
            }
            return Err(OwnershipHandoffError::checkpoint(reason));
        }
    };
    let (lsm, payload) = replicated_state
        .replace_guest_state(guest_state)
        .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?;
    runtime
        .persist_wasm_processor_snapshot(replicated_state, lsm, &payload)
        .await
        .map_err(OwnershipHandoffError::checkpoint)
}

pub(super) fn relay_batch_from_wasm_output(
    key: &Option<BranchKey>,
    schema: Arc<CompiledSchema>,
    batch: RuntimeRecordBatch,
    rows: Vec<WasmOutputRow>,
    uninitialized_columns: HashSet<usize>,
    ack_map: &mut WasmAckMap,
    token_use_counts: &mut HashMap<u64, usize>,
) -> Result<WasmDecodedOutputBatch, String> {
    let mut metadata = Vec::with_capacity(rows.len());
    let mut acks = Vec::with_capacity(rows.len());
    for row in rows {
        let source_context = row
            .source_token
            .and_then(|source_token| ack_map.get(&source_token.0));
        metadata.push(match source_context {
            Some(context) => context.metadata.clone(),
            None => {
                let now = current_timestamp();
                RuntimeRecordMetadata::from_ingested_at_watermarks(now, now)
            }
        });
        let mut row_ack_sets = Vec::with_capacity(row.tokens.len());
        for token in row.tokens {
            let remaining_uses = token_use_counts.get_mut(&token.0).verified(
                "the use counts were built from this same validated output, which the ACK map \
                 still backs",
            );
            if *remaining_uses > 1 {
                *remaining_uses -= 1;
                let context = ack_map.get(&token.0).verified(
                    "the use counts were built from this same validated output, which the ACK map \
                     still backs",
                );
                row_ack_sets.push(context.acks.attached());
            } else {
                let context = ack_map.remove(&token.0).verified(
                    "the use counts were built from this same validated output, which the ACK map \
                     still backs",
                );
                row_ack_sets.push(context.acks);
            }
        }
        acks.push(AckSet::merged(row_ack_sets));
    }
    if batch.schema().as_ref() != schema.arrow_schema().as_ref() {
        return Err("WASM output Arrow schema does not match its relay schema".to_string());
    }
    RelayRecordBatch::from_filtered_parts(key.clone(), batch, metadata, acks).map(|batch| {
        WasmDecodedOutputBatch {
            batch,
            uninitialized_columns,
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use ahash::HashSet;
    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::Schema as ArrowSchema;
    use nervix_models::ParseAsType;
    use nervix_wasm::{
        WasmAckSidecar, WasmAckToken, WasmAckTokenSet, WasmEnvelope, WasmOutputColumnRef,
        WasmOutputRow, WasmRoutedOutput,
    };
    use tokio::time::{Duration, timeout};
    use triomphe::Arc;

    use super::*;
    use crate::runtime_ack::{AckOutcome, AckSet};

    #[tokio::test]
    async fn wasm_zero_row_output_builds_exact_empty_destination_columns() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (_, ack_map) = wasm_input_for_values(&schema, &[10]).await;
        let outputs = validate_wasm_test_outputs(
            &schema,
            &schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                Vec::new(),
            )],
        )
        .expect("zero-row output must build");

        assert_eq!(outputs[0].batch.batch().num_rows(), 0);
        assert_eq!(outputs[0].batch.batch().num_columns(), 1);
        assert_eq!(outputs[0].batch.batch().schema(), schema.arrow_schema());
    }

    #[test]
    fn wasm_uninitialized_column_uses_destination_type_and_ack_row_count() {
        let input_schema = test_schema(&[("input", ParseAsType::I32)]);
        let output_schema = test_optional_schema(&[OptionalTestField {
            name: "value",
            ty: ParseAsType::I64,
            optional: true,
        }]);
        let rows = vec![
            WasmOutputRow {
                tokens: Vec::new(),
                source_token: None,
            },
            WasmOutputRow {
                tokens: Vec::new(),
                source_token: None,
            },
        ];

        let outputs = validate_wasm_test_outputs(
            &input_schema,
            &output_schema,
            &WasmAckMap::default(),
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::uninitialized()],
                rows,
            )],
        )
        .expect("uninitialized output must pass host validation");

        assert_eq!(outputs[0].batch.batch().num_rows(), 2);
        assert_eq!(
            outputs[0].batch.batch().column(0).data_type(),
            &arrow_schema::DataType::Int64
        );
        assert_eq!(outputs[0].batch.batch().column(0).null_count(), 2);
        assert!(outputs[0].uninitialized_columns.contains(&0));
    }

    #[tokio::test]
    async fn wasm_mixed_input_and_generated_columns_match_destination_schema() {
        let input_schema = test_schema(&[("value", ParseAsType::I32)]);
        let output_schema =
            test_schema(&[("value", ParseAsType::I32), ("bucket", ParseAsType::String)]);
        let (input, ack_map) = wasm_input_for_values(&input_schema, &[2, 4]).await;
        let field = output_schema.arrow_schema().field(1).clone();
        let ipc = wasm_guest_column(field, StdArc::new(StringArray::from(vec!["EVEN", "EVEN"])));
        let outputs = validate_wasm_test_outputs(
            &input_schema,
            &output_schema,
            &ack_map,
            vec![wasm_test_generated_output(
                ipc,
                vec![
                    WasmOutputColumnRef::input(0),
                    WasmOutputColumnRef::generated(0),
                ],
                wasm_input_acks(&input).rows.clone(),
            )],
        )
        .expect("mixed output must materialize");

        assert_eq!(
            outputs[0].batch.batch().schema(),
            output_schema.arrow_schema()
        );
        assert_eq!(outputs[0].batch.batch().num_rows(), 2);
    }

    #[tokio::test]
    async fn wasm_shared_generated_column_reuses_one_array_across_routes_and_fields() {
        let input_schema = test_schema(&[("value", ParseAsType::I32)]);
        let enriched_schema = test_schema(&[
            ("value", ParseAsType::I32),
            ("bucket", ParseAsType::String),
            ("bucket_copy", ParseAsType::String),
        ]);
        let audit_schema = test_schema(&[
            ("value", ParseAsType::I32),
            ("classification", ParseAsType::String),
        ]);
        let (input, ack_map) = wasm_input_for_values(&input_schema, &[2, 4]).await;
        let rows = wasm_input_acks(&input).rows.clone();
        let generated_arrow_ipc_batch = wasm_guest_column(
            enriched_schema.arrow_schema().field(1).clone(),
            StdArc::new(StringArray::from(vec!["EVEN", "EVEN"])),
        );
        let output = WasmEnvelope::output(
            generated_arrow_ipc_batch,
            vec![
                WasmRoutedOutput::new(
                    "enriched",
                    vec![
                        WasmOutputColumnRef::input(0),
                        WasmOutputColumnRef::generated(0),
                        WasmOutputColumnRef::generated(0),
                    ],
                    WasmAckSidecar {
                        rows: rows.clone(),
                        ..WasmAckSidecar::default()
                    },
                ),
                WasmRoutedOutput::new(
                    "audit",
                    vec![
                        WasmOutputColumnRef::input(0),
                        WasmOutputColumnRef::generated(0),
                    ],
                    WasmAckSidecar {
                        rows,
                        ..WasmAckSidecar::default()
                    },
                ),
            ],
        );
        let outputs = validate_wasm_test_output_groups(
            &input_schema,
            vec![("enriched", enriched_schema), ("audit", audit_schema)],
            &ack_map,
            vec![output],
        )
        .expect("shared generated output must materialize");

        let first = outputs[0].batch.batch().column(1);
        assert!(StdArc::ptr_eq(first, outputs[0].batch.batch().column(2)));
        assert!(StdArc::ptr_eq(first, outputs[1].batch.batch().column(1)));
        let input_values = outputs[0]
            .batch
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("input reference must remain I32");
        assert_eq!(input_values.values().as_ref(), &[2, 4]);
    }

    #[test]
    fn wasm_generated_pool_rejects_out_of_range_and_unreferenced_columns() {
        let input_schema = test_schema(&[("input", ParseAsType::I32)]);
        let output_schema = test_schema(&[("generated", ParseAsType::String)]);
        let field = output_schema.arrow_schema().field(0).clone();
        let one_column =
            wasm_guest_column(field.clone(), StdArc::new(StringArray::from(vec!["value"])));
        let rows = vec![WasmOutputRow {
            tokens: Vec::new(),
            source_token: None,
        }];
        let out_of_range = validate_wasm_test_outputs(
            &input_schema,
            &output_schema,
            &WasmAckMap::default(),
            vec![wasm_test_generated_output(
                one_column,
                vec![WasmOutputColumnRef::generated(1)],
                rows.clone(),
            )],
        )
        .expect_err("out-of-range generated column must fail");
        assert!(matches!(
            out_of_range,
            WasmOutputError::GeneratedColumnOutOfRange {
                column_index: 1,
                ..
            }
        ));

        let two_columns = wasm_generated_pool(
            vec![field.clone(), field],
            vec![
                StdArc::new(StringArray::from(vec!["used"])),
                StdArc::new(StringArray::from(vec!["unused"])),
            ],
        );
        let unreferenced = validate_wasm_test_outputs(
            &input_schema,
            &output_schema,
            &WasmAckMap::default(),
            vec![wasm_test_generated_output(
                two_columns,
                vec![WasmOutputColumnRef::generated(0)],
                rows,
            )],
        )
        .expect_err("unreferenced generated column must fail");
        assert!(matches!(
            unreferenced,
            WasmOutputError::UnreferencedGeneratedColumn { column_index: 1 }
        ));
    }

    #[test]
    fn wasm_generated_pool_rejects_route_shape_type_and_row_mismatches() {
        let input_schema = test_schema(&[("input", ParseAsType::I32)]);
        let output_schema = test_schema(&[("generated", ParseAsType::String)]);
        let field = output_schema.arrow_schema().field(0).clone();
        let generated = wasm_guest_column(
            field.clone(),
            StdArc::new(StringArray::from(vec!["first", "second"])),
        );
        let one_row = vec![WasmOutputRow {
            tokens: Vec::new(),
            source_token: None,
        }];
        let row_count = validate_wasm_test_outputs(
            &input_schema,
            &output_schema,
            &WasmAckMap::default(),
            vec![wasm_test_generated_output(
                generated,
                vec![WasmOutputColumnRef::generated(0)],
                one_row.clone(),
            )],
        )
        .expect_err("generated row count must match every referencing route");
        assert!(matches!(
            row_count,
            WasmOutputError::GeneratedColumnRowCountMismatch {
                expected: 1,
                actual: 2,
                ..
            }
        ));

        let nullable_output_schema = test_optional_schema(&[OptionalTestField {
            name: "generated",
            ty: ParseAsType::String,
            optional: true,
        }]);
        let nullability = validate_wasm_test_outputs(
            &input_schema,
            &nullable_output_schema,
            &WasmAckMap::default(),
            vec![wasm_test_generated_output(
                wasm_guest_column(field, StdArc::new(StringArray::from(vec!["value"]))),
                vec![WasmOutputColumnRef::generated(0)],
                one_row.clone(),
            )],
        )
        .expect_err("generated nullability must match the destination");
        assert!(matches!(
            nullability,
            WasmOutputError::GeneratedColumnTypeMismatch { .. }
        ));

        let column_count = validate_wasm_test_outputs(
            &input_schema,
            &output_schema,
            &WasmAckMap::default(),
            vec![wasm_test_output(Vec::new(), one_row)],
        )
        .expect_err("routed output column count must match the destination");
        assert!(matches!(
            column_count,
            WasmOutputError::RoutedOutputColumnCountMismatch {
                expected: 1,
                actual: 0,
                ..
            }
        ));

        let empty_group = validate_wasm_test_outputs(
            &input_schema,
            &output_schema,
            &WasmAckMap::default(),
            vec![WasmEnvelope::output(Vec::new(), Vec::new())],
        )
        .expect_err("empty output group must fail");
        assert!(matches!(
            empty_group,
            WasmOutputError::EmptyOutputGroup { .. }
        ));
    }

    #[test]
    fn wasm_shared_generated_column_requires_each_route_to_use_the_pool_row_layout() {
        let input_schema = test_schema(&[("input", ParseAsType::I32)]);
        let first_schema = test_schema(&[("first", ParseAsType::String)]);
        let second_schema = test_schema(&[("second", ParseAsType::String)]);
        let generated = wasm_guest_column(
            first_schema.arrow_schema().field(0).clone(),
            StdArc::new(StringArray::from(vec!["one", "two"])),
        );
        let row = WasmOutputRow {
            tokens: Vec::new(),
            source_token: None,
        };
        let output = WasmEnvelope::output(
            generated,
            vec![
                WasmRoutedOutput::new(
                    "first",
                    vec![WasmOutputColumnRef::generated(0)],
                    WasmAckSidecar {
                        rows: vec![row.clone(), row.clone()],
                        ..WasmAckSidecar::default()
                    },
                ),
                WasmRoutedOutput::new(
                    "second",
                    vec![WasmOutputColumnRef::generated(0)],
                    WasmAckSidecar {
                        rows: vec![row],
                        ..WasmAckSidecar::default()
                    },
                ),
            ],
        );
        let error = validate_wasm_test_output_groups(
            &input_schema,
            vec![("first", first_schema), ("second", second_schema)],
            &WasmAckMap::default(),
            vec![output],
        )
        .expect_err("one pool cannot serve routes with different row counts");

        assert!(matches!(
            error,
            WasmOutputError::GeneratedColumnRowCountMismatch {
                output_relay,
                expected: 1,
                actual: 2,
                ..
            } if output_relay == "second"
        ));
    }

    #[tokio::test]
    async fn wasm_routed_output_fanout_waits_for_every_downstream_ack() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (input, mut ack_map) = wasm_input_for_values(&schema, &[2]).await;
        let (root_acks, completion) = AckSet::root();
        ack_map.get_mut(&1).expect("token must exist").acks = root_acks;
        let row = wasm_input_acks(&input).rows[0].clone();
        let output = WasmEnvelope::output(
            Vec::new(),
            vec![
                WasmRoutedOutput::new(
                    "first",
                    vec![WasmOutputColumnRef::input(0)],
                    WasmAckSidecar {
                        rows: vec![row.clone()],
                        ..WasmAckSidecar::default()
                    },
                ),
                WasmRoutedOutput::new(
                    "second",
                    vec![WasmOutputColumnRef::input(0)],
                    WasmAckSidecar {
                        rows: vec![row],
                        ..WasmAckSidecar::default()
                    },
                ),
            ],
        );
        let mut outputs = validate_wasm_test_output_groups(
            &schema,
            vec![
                ("first", Arc::clone(&schema)),
                ("second", Arc::clone(&schema)),
            ],
            &ack_map,
            vec![output],
        )
        .expect("fanout output must validate");
        let mut token_use_counts = wasm_output_token_use_counts(&outputs);

        let first = outputs.remove(0);
        let first = relay_batch_from_wasm_output(
            &None,
            first.schema,
            first.batch,
            first.acks.rows,
            HashSet::default(),
            &mut ack_map,
            &mut token_use_counts,
        )
        .expect("first routed batch must build");
        let completion_task = tokio::spawn(completion.wait());
        first.batch.acks[0].ack_success();
        tokio::task::yield_now().await;
        assert!(
            !completion_task.is_finished(),
            "the first downstream ACK must not complete the fanned-out input"
        );

        let second = outputs.remove(0);
        let second = relay_batch_from_wasm_output(
            &None,
            second.schema,
            second.batch,
            second.acks.rows,
            HashSet::default(),
            &mut ack_map,
            &mut token_use_counts,
        )
        .expect("second routed batch must build");
        second.batch.acks[0].ack_success();
        let outcome = timeout(Duration::from_millis(50), completion_task)
            .await
            .expect("the final downstream ACK must complete the input")
            .expect("ACK completion task must not panic");
        assert_eq!(outcome, AckOutcome::Ack);
    }

    #[test]
    fn wasm_guest_generated_rows_do_not_require_source_tokens() {
        let input_schema = test_schema(&[("input_value", ParseAsType::I32)]);
        let output_schema = test_schema(&[("value", ParseAsType::I32)]);
        let ipc = wasm_guest_column(
            output_schema.arrow_schema().field(0).clone(),
            StdArc::new(Int32Array::from(vec![42])),
        );
        let outputs = validate_wasm_test_outputs(
            &input_schema,
            &output_schema,
            &WasmAckMap::default(),
            vec![wasm_test_generated_output(
                ipc,
                vec![WasmOutputColumnRef::generated(0)],
                vec![WasmOutputRow {
                    tokens: Vec::new(),
                    source_token: None,
                }],
            )],
        )
        .expect("fully generated output rows may omit a source token");

        assert_eq!(outputs[0].batch.batch().num_rows(), 1);
    }

    #[tokio::test]
    async fn wasm_generated_arrow_contract_rejects_invalid_stream_shapes_and_schema() {
        let input_schema = test_schema(&[("value", ParseAsType::I32)]);
        let output_schema = test_schema(&[("value", ParseAsType::I32)]);
        let (input, ack_map) = wasm_input_for_values(&input_schema, &[2]).await;
        let rows = wasm_input_acks(&input).rows.clone();
        let destination_field = output_schema.arrow_schema().field(0).clone();
        let validate_ipc = |ipc| {
            validate_wasm_test_outputs(
                &input_schema,
                &output_schema,
                &ack_map,
                vec![wasm_test_generated_output(
                    ipc,
                    vec![WasmOutputColumnRef::generated(0)],
                    rows.clone(),
                )],
            )
        };

        let empty =
            validate_ipc(Vec::new()).expect_err("generated reference without a pool must fail");
        assert!(matches!(
            empty,
            WasmOutputError::GeneratedColumnOutOfRange { .. }
        ));

        let empty_schema = StdArc::new(ArrowSchema::empty());
        let zero_fields = wasm_guest_stream(
            empty_schema.clone(),
            &[RecordBatch::new_empty(empty_schema)],
        );
        let zero_fields = validate_ipc(zero_fields).expect_err("zero fields must fail");
        assert!(matches!(
            zero_fields,
            WasmOutputError::InvalidGeneratedArrowIpc { .. }
        ));

        let named_field_schema = StdArc::new(ArrowSchema::new(vec![destination_field.clone()]));
        let named_field_batch = RecordBatch::try_new(
            named_field_schema.clone(),
            vec![StdArc::new(Int32Array::from(vec![2]))],
        )
        .expect("named field batch must build");
        let named_field = validate_ipc(wasm_guest_stream(named_field_schema, &[named_field_batch]))
            .expect_err("generated field names must be empty");
        assert!(matches!(
            named_field,
            WasmOutputError::InvalidGeneratedArrowIpc { .. }
        ));

        let one_field_schema = StdArc::new(ArrowSchema::new(vec![
            destination_field.clone().with_name(""),
        ]));
        let no_batches = validate_ipc(wasm_guest_stream(one_field_schema.clone(), &[]))
            .expect_err("missing guest record batch must fail");
        assert!(matches!(
            no_batches,
            WasmOutputError::GeneratedRecordBatchCount { actual: 0 }
        ));
        let one_batch = RecordBatch::try_new(
            one_field_schema.clone(),
            vec![StdArc::new(Int32Array::from(vec![2]))],
        )
        .expect("one-field batch must build");
        let multiple_batches = validate_ipc(wasm_guest_stream(
            one_field_schema,
            &[one_batch.clone(), one_batch],
        ))
        .expect_err("multiple guest batches must fail");
        assert!(matches!(
            multiple_batches,
            WasmOutputError::GeneratedRecordBatchCount { actual: 2 }
        ));

        let mismatched_field = validate_ipc(wasm_guest_column(
            arrow_schema::Field::new("ignored", arrow_schema::DataType::Utf8, false),
            StdArc::new(StringArray::from(vec!["wrong type"])),
        ))
        .expect_err("guest field mismatch must fail");
        assert!(matches!(
            mismatched_field,
            WasmOutputError::GeneratedColumnTypeMismatch { .. }
        ));

        let row_count = validate_ipc(wasm_guest_column(
            destination_field.clone(),
            StdArc::new(Int32Array::from(vec![2, 4])),
        ))
        .expect_err("guest row-count mismatch must fail");
        assert!(matches!(
            row_count,
            WasmOutputError::GeneratedColumnRowCountMismatch { .. }
        ));

        let mut trailing_ipc =
            wasm_guest_column(destination_field, StdArc::new(Int32Array::from(vec![2])));
        trailing_ipc.push(0);
        let trailing = validate_ipc(trailing_ipc).expect_err("trailing guest IPC must fail");
        assert!(matches!(
            trailing,
            WasmOutputError::InvalidGeneratedArrowIpc { .. }
        ));
    }

    #[tokio::test]
    async fn wasm_input_reference_validation_rejects_invalid_mapping_and_source_tokens() {
        let input_schema = test_schema(&[("value", ParseAsType::I32)]);
        let renamed_schema = test_schema(&[("renamed_value", ParseAsType::I32)]);
        let string_schema = test_schema(&[("value", ParseAsType::String)]);
        let nullable_schema = test_optional_schema(&[OptionalTestField {
            name: "value",
            ty: ParseAsType::I32,
            optional: true,
        }]);
        let (input, ack_map) = wasm_input_for_values(&input_schema, &[10]).await;
        let rows = wasm_input_acks(&input).rows.clone();

        validate_wasm_test_outputs(
            &input_schema,
            &renamed_schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                rows.clone(),
            )],
        )
        .expect("explicit input references may rename fields");

        let out_of_range = validate_wasm_test_outputs(
            &input_schema,
            &input_schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 9 }],
                rows.clone(),
            )],
        )
        .expect_err("out-of-range input column must fail");
        assert!(matches!(
            out_of_range,
            WasmOutputError::InputColumnOutOfRange { .. }
        ));

        let type_mismatch = validate_wasm_test_outputs(
            &input_schema,
            &string_schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                rows.clone(),
            )],
        )
        .expect_err("input type mismatch must fail");
        assert!(matches!(
            type_mismatch,
            WasmOutputError::InputColumnTypeMismatch { .. }
        ));

        let nullability_mismatch = validate_wasm_test_outputs(
            &input_schema,
            &nullable_schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                rows.clone(),
            )],
        )
        .expect_err("input nullability mismatch must fail");
        assert!(matches!(
            nullability_mismatch,
            WasmOutputError::InputColumnTypeMismatch { .. }
        ));

        let mut missing_source = rows.clone();
        missing_source[0].source_token = None;
        let missing = validate_wasm_test_outputs(
            &input_schema,
            &input_schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                missing_source,
            )],
        )
        .expect_err("missing source token must fail");
        assert!(matches!(
            missing,
            WasmOutputError::MissingSourceToken { .. }
        ));

        let mut unknown_source = rows.clone();
        unknown_source[0].tokens = vec![WasmAckToken(99)];
        unknown_source[0].source_token = Some(WasmAckToken(99));
        let unknown = validate_wasm_test_outputs(
            &input_schema,
            &input_schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                unknown_source,
            )],
        )
        .expect_err("unknown source token must fail");
        assert!(matches!(
            unknown,
            WasmOutputError::UnknownSourceToken { .. }
        ));

        let mut not_carried = rows;
        not_carried[0].tokens.clear();
        let not_carried = validate_wasm_test_outputs(
            &input_schema,
            &input_schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                not_carried,
            )],
        )
        .expect_err("source token outside lineage must fail");
        assert!(matches!(
            not_carried,
            WasmOutputError::SourceTokenNotCarried { .. }
        ));
    }

    #[tokio::test]
    async fn wasm_callback_rejects_tokens_that_are_both_carried_and_terminal() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (input, ack_map) = wasm_input_for_values(&schema, &[10]).await;
        let output = WasmEnvelope::output(
            Vec::new(),
            vec![WasmRoutedOutput::new(
                "output",
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                WasmAckSidecar {
                    rows: wasm_input_acks(&input).rows.clone(),
                    acked: vec![WasmAckTokenSet {
                        tokens: vec![WasmAckToken(1)],
                    }],
                    nacked: Vec::new(),
                    message_errors: Vec::new(),
                },
            )],
        );

        let error = validate_wasm_test_outputs(&schema, &schema, &ack_map, vec![output])
            .expect_err("one token cannot be carried and terminally completed");
        assert!(matches!(
            error,
            WasmOutputError::InvalidTokenDecision { token: 1, .. }
        ));

        let duplicate_terminal = WasmEnvelope::output(
            Vec::new(),
            vec![
                WasmRoutedOutput::new(
                    "output",
                    vec![WasmOutputColumnRef::Input { column_index: 0 }],
                    WasmAckSidecar {
                        rows: Vec::new(),
                        acked: vec![WasmAckTokenSet {
                            tokens: vec![WasmAckToken(1)],
                        }],
                        nacked: Vec::new(),
                        message_errors: Vec::new(),
                    },
                ),
                WasmRoutedOutput::new(
                    "output",
                    vec![WasmOutputColumnRef::Input { column_index: 0 }],
                    WasmAckSidecar {
                        rows: Vec::new(),
                        acked: Vec::new(),
                        nacked: vec![nervix_wasm::WasmNackSet {
                            tokens: vec![WasmAckToken(1)],
                            reason: "rejected".to_string(),
                        }],
                        message_errors: Vec::new(),
                    },
                ),
            ],
        );
        let error =
            validate_wasm_test_outputs(&schema, &schema, &ack_map, vec![duplicate_terminal])
                .expect_err("one token cannot receive multiple terminal decisions");
        assert!(matches!(
            error,
            WasmOutputError::InvalidTokenDecision { token: 1, .. }
        ));
    }

    #[tokio::test]
    async fn wasm_reference_to_terminally_removed_or_other_branch_token_is_rejected() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (input, _) = wasm_input_for_values(&schema, &[10]).await;
        let empty_ack_map = WasmAckMap::default();

        let error = validate_wasm_test_outputs(
            &schema,
            &schema,
            &empty_ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                wasm_input_acks(&input).rows.clone(),
            )],
        )
        .expect_err("a token outside the current live branch map must fail");
        assert!(matches!(
            error,
            WasmOutputError::UnknownSourceToken { token: 1, .. }
        ));
    }

    #[tokio::test]
    async fn wasm_callback_validation_is_all_or_nothing_for_terminal_decisions() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (input, mut ack_map) = wasm_input_for_values(&schema, &[10]).await;
        let (acks, completion) = AckSet::root();
        ack_map.get_mut(&1).expect("token must exist").acks = acks;
        let output_group = WasmEnvelope::output(
            Vec::new(),
            vec![
                WasmRoutedOutput::new(
                    "output",
                    vec![WasmOutputColumnRef::Input { column_index: 0 }],
                    WasmAckSidecar {
                        rows: Vec::new(),
                        acked: vec![WasmAckTokenSet {
                            tokens: vec![WasmAckToken(1)],
                        }],
                        nacked: Vec::new(),
                        message_errors: Vec::new(),
                    },
                ),
                WasmRoutedOutput::new(
                    "output",
                    Vec::new(),
                    WasmAckSidecar {
                        rows: wasm_input_acks(&input).rows.clone(),
                        ..WasmAckSidecar::default()
                    },
                ),
            ],
        );

        validate_wasm_test_outputs(&schema, &schema, &ack_map, vec![output_group])
            .expect_err("later malformed output must reject the whole callback");
        assert!(
            timeout(Duration::from_millis(50), completion.wait())
                .await
                .is_err(),
            "validation must not apply an earlier terminal ACK"
        );
    }
}
