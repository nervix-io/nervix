//! Even-row filter reference guest built on the `nervix-wasm-sdk` crate.
//!
//! Rows are filtered by their global row ordinal: even ordinals are
//! preserved with their complete input row sidecars, odd ordinals are dropped
//! into the `acked` sidecar. Batches accumulate until every second batch or a
//! guest-requested one-second domain-clock timeout flushes the pending batch.
//! Sentinel first values exercise the error paths: `-100` routes a message
//! error, `-200` reports a global error, and `-300` latches guest error
//! state.

use std::{sync::Arc, time::Duration};

use arrow_array::{Array, ArrayRef, Int32Array, StringArray};
use arrow_schema::ArrowError;
use nervix_wasm_sdk::{
    AckSidecar, AckTokenSet, BranchContext, GuestContext, GuestError, InputBatch, MessageErrorSet,
    OutputColumnRef, OutputEnvelope, Processor, ProcessorField, ProcessorSchema, ProcessorType,
    TimeoutHandle,
};

const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);
const FLUSH_EVERY_BATCHES: u64 = 2;
const STATE_HEADER_BYTES: usize = 24;

struct EvenRowFilter {
    processed_batches: u64,
    processed_rows: u64,
    pending_start_row: u64,
    pending: Option<InputBatch>,
}

impl EvenRowFilter {
    fn flush_pending(&mut self, ctx: &mut GuestContext<'_>) -> Result<(), GuestError> {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let output = filter_even_rows(
            ctx.branch().input_schema(),
            ctx.branch().output_schemas(),
            &pending,
            self.pending_start_row,
        )?;
        ctx.emit(output)?;
        self.pending_start_row = self.processed_rows;
        Ok(())
    }
}

impl Processor for EvenRowFilter {
    fn create(_branch: &BranchContext) -> Result<Self, GuestError> {
        Ok(Self {
            processed_batches: 0,
            processed_rows: 0,
            pending_start_row: 0,
            pending: None,
        })
    }

    fn process_batch(
        &mut self,
        ctx: &mut GuestContext<'_>,
        input: InputBatch,
    ) -> Result<(), GuestError> {
        self.processed_batches = self.processed_batches.saturating_add(1);
        ctx.domain_time();
        ctx.request_timeout(FLUSH_TIMEOUT)?;
        match first_i32_value(&input)? {
            Some(-300) => return Err(GuestError::failed("guest error state for value -300")),
            Some(-200) => {
                ctx.report_global_error("guest global error for value -200");
                return Ok(());
            }
            Some(-100) => {
                let output_schema = ctx
                    .branch()
                    .output_schemas()
                    .first()
                    .ok_or(GuestError::NotInitialized)?;
                let output = message_error_envelope(
                    ctx.branch().input_schema(),
                    output_schema,
                    input.acks(),
                    "guest message error for value -100".to_string(),
                )?;
                ctx.emit(output)?;
                return Ok(());
            }
            _ => {}
        }
        self.flush_pending(ctx)?;
        self.pending_start_row = self.processed_rows;
        self.processed_rows = self.processed_rows.saturating_add(input.row_count());
        self.pending = Some(input);
        if self.processed_batches.is_multiple_of(FLUSH_EVERY_BATCHES) {
            self.flush_pending(ctx)?;
        }
        Ok(())
    }

    fn on_timeout(
        &mut self,
        ctx: &mut GuestContext<'_>,
        _handle: TimeoutHandle,
    ) -> Result<(), GuestError> {
        self.flush_pending(ctx)
    }

    fn save_state(&self) -> Vec<u8> {
        let pending = self
            .pending
            .as_ref()
            .map(InputBatch::envelope_bytes)
            .unwrap_or_default();
        let mut state = Vec::with_capacity(STATE_HEADER_BYTES + pending.len());
        state.extend_from_slice(&self.processed_batches.to_le_bytes());
        state.extend_from_slice(&self.processed_rows.to_le_bytes());
        state.extend_from_slice(&self.pending_start_row.to_le_bytes());
        state.extend_from_slice(pending);
        state
    }

    fn restore(_branch: &BranchContext, state: &[u8]) -> Result<Self, GuestError> {
        if state.len() < STATE_HEADER_BYTES {
            return Err(GuestError::InvalidSize);
        }
        let (header, pending) = state.split_at(STATE_HEADER_BYTES);
        let counters = header
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("chunks are eight bytes")))
            .collect::<Vec<_>>();
        let pending = if pending.is_empty() {
            None
        } else {
            Some(InputBatch::from_envelope_bytes(pending.to_vec())?)
        };
        Ok(Self {
            processed_batches: counters[0],
            processed_rows: counters[1],
            pending_start_row: counters[2],
            pending,
        })
    }
}

nervix_wasm_sdk::export_processor!(EvenRowFilter);

fn int32_input_error() -> GuestError {
    GuestError::ArrowIpc(ArrowError::SchemaError(
        "input column 0 is not a non-null I32 column".to_string(),
    ))
}

fn first_i32_value(input: &InputBatch) -> Result<Option<i32>, GuestError> {
    for batch in input.batches() {
        if batch.num_rows() == 0 {
            continue;
        }
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(int32_input_error)?;
        if values.is_valid(0) {
            return Ok(Some(values.value(0)));
        }
    }
    Ok(None)
}

fn filter_even_rows(
    input_schema: &ProcessorSchema,
    output_schemas: &[ProcessorSchema],
    pending: &InputBatch,
    start_row: u64,
) -> Result<OutputEnvelope, GuestError> {
    if output_schemas.is_empty() {
        return Err(GuestError::NotInitialized);
    }
    let acks = pending.acks();
    let mut selected_values = Vec::new();
    let mut selected_rows = Vec::new();
    let mut acked = acks.acked.clone();
    let mut next_row = start_row;
    let mut input_row = 0_usize;
    for batch in pending.batches() {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(int32_input_error)?;
        for row in 0..values.len() {
            next_row = next_row.saturating_add(1);
            if next_row.is_multiple_of(2) && values.is_valid(row) {
                selected_values.push(values.value(row));
                selected_rows.push(acks.rows.get(input_row).cloned().unwrap_or_default());
            } else if let Some(ack) = acks.rows.get(input_row) {
                acked.push(AckTokenSet {
                    tokens: ack.tokens.clone(),
                });
            }
            input_row += 1;
        }
    }

    let generated_fields = generated_fields(input_schema, output_schemas);
    let mut output = OutputEnvelope::new();
    for field in &generated_fields {
        output.add_generated_column(generated_column(field, &selected_values)?, field.optional);
    }
    for (index, output_schema) in output_schemas.iter().enumerate() {
        let columns = output_columns(input_schema, output_schema, &generated_fields)?;
        output.add_route(
            output_schema.name.clone(),
            columns,
            AckSidecar {
                rows: selected_rows.clone(),
                acked: if index == 0 {
                    acked.clone()
                } else {
                    Vec::new()
                },
                nacked: if index == 0 {
                    acks.nacked.clone()
                } else {
                    Vec::new()
                },
                message_errors: if index == 0 {
                    acks.message_errors.clone()
                } else {
                    Vec::new()
                },
            },
        );
    }
    Ok(output)
}

fn message_error_envelope(
    input_schema: &ProcessorSchema,
    output_schema: &ProcessorSchema,
    acks: &AckSidecar,
    reason: String,
) -> Result<OutputEnvelope, GuestError> {
    let tokens = acks
        .rows
        .first()
        .map(|row| row.tokens.clone())
        .unwrap_or_default();
    let generated_fields = generated_fields(input_schema, std::slice::from_ref(output_schema));
    let mut output = OutputEnvelope::new();
    for field in &generated_fields {
        output.add_generated_column(generated_column(field, &[])?, field.optional);
    }
    let columns = output_columns(input_schema, output_schema, &generated_fields)?;
    output.add_route(
        output_schema.name.clone(),
        columns,
        AckSidecar {
            rows: Vec::new(),
            acked: acks.acked.clone(),
            nacked: acks.nacked.clone(),
            message_errors: vec![MessageErrorSet { tokens, reason }],
        },
    );
    Ok(output)
}

/// Required destination fields with no identical input field, deduplicated by
/// type and nullability so identically typed destinations share one generated
/// column. Optional destinations stay uninitialized instead.
fn generated_fields(
    input_schema: &ProcessorSchema,
    output_schemas: &[ProcessorSchema],
) -> Vec<ProcessorField> {
    let mut generated = Vec::<ProcessorField>::new();
    for destination in output_schemas.iter().flat_map(|schema| &schema.fields) {
        let is_input = input_schema.fields.iter().any(|source| {
            source.name == destination.name
                && source.ty == destination.ty
                && source.optional == destination.optional
        });
        if !is_input
            && !destination.optional
            && !generated
                .iter()
                .any(|field| field.ty == destination.ty && field.optional == destination.optional)
        {
            generated.push(destination.clone());
        }
    }
    generated
}

fn output_columns(
    input_schema: &ProcessorSchema,
    output_schema: &ProcessorSchema,
    generated_fields: &[ProcessorField],
) -> Result<Vec<OutputColumnRef>, GuestError> {
    output_schema
        .fields
        .iter()
        .map(|destination| {
            if let Some(column_index) = input_schema.fields.iter().position(|source| {
                source.name == destination.name
                    && source.ty == destination.ty
                    && source.optional == destination.optional
            }) {
                return Ok(OutputColumnRef::Input {
                    column_index: u32::try_from(column_index)
                        .map_err(|_| GuestError::InvalidSize)?,
                });
            }
            if destination.optional {
                return Ok(OutputColumnRef::Uninitialized);
            }
            let column_index = generated_fields
                .iter()
                .position(|field| {
                    field.ty == destination.ty && field.optional == destination.optional
                })
                .ok_or_else(|| {
                    GuestError::failed(format!(
                        "no generated column source for required destination field '{}'",
                        destination.name
                    ))
                })?;
            Ok(OutputColumnRef::Generated {
                column_index: u32::try_from(column_index).map_err(|_| GuestError::InvalidSize)?,
            })
        })
        .collect()
}

fn generated_column(
    field: &ProcessorField,
    selected_values: &[i32],
) -> Result<ArrayRef, GuestError> {
    match &field.ty {
        ProcessorType::I32 if field.name == "value" => {
            Ok(Arc::new(Int32Array::from(selected_values.to_vec())))
        }
        ProcessorType::String => {
            let values = selected_values.iter().map(|_| "EVEN").collect::<Vec<_>>();
            Ok(Arc::new(StringArray::from(values)))
        }
        _ => Err(GuestError::failed(format!(
            "no generated value source for required field '{}'",
            field.name
        ))),
    }
}
