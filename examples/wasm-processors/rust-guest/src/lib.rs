//! Even-row filter reference guest built on the `nervix-wasm-sdk` crate.
//!
//! Rows are filtered by their global row ordinal: even ordinals are
//! preserved with their complete input row sidecars, odd ordinals are dropped
//! into the `acked` sidecar. Batches accumulate until every second batch or a
//! guest-requested one-second domain-clock timeout flushes the pending batch.
//!
//! The processed row count is the guest's durable computation state: a
//! recreated instance numbers the rows that follow from it, and a state
//! inspection sees it described as the one-row `progress` section. When the
//! input carries a `DURATION` field named `wait`, the guest also sums the wait
//! of every row it accepts, saves the total after the row ordinal, returns it
//! for each preserved row in a required `DURATION` destination field named
//! `total_wait`, and describes it as the one-row `waits` section. The batch the
//! guest still buffers belongs to the live instance together with its ACK
//! tokens, so a quiesce flush releases it and it is never saved or described.
//!
//! Sentinel first values exercise the error paths: `-100` routes a message
//! error, `-200` reports a global error, `-300` latches guest error state,
//! `-400` is filtered like any other value but leaves the guest unable to
//! serialize its state, and `-500` asks the host for a new state lifetime.

use std::{sync::Arc, time::Duration};

use arrow_array::{Array, ArrayRef, DurationNanosecondArray, Int32Array, StringArray, UInt64Array};
use arrow_schema::ArrowError;
use error_stack::{Report, Result};
use nervix_wasm_sdk::{
    AckSidecar, AckTokenSet, BranchContext, GuestContext, GuestError, InputBatch, MessageErrorSet,
    OutputColumnRef, OutputEnvelope, Processor, ProcessorField, ProcessorSchema, ProcessorType,
    StateDescription, StateResetRequestAnswer, TimeoutHandle,
};

const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);
const FLUSH_EVERY_BATCHES: u64 = 2;
const UNSERIALIZABLE_STATE_VALUE: i32 = -400;
const REQUEST_STATE_RESET_VALUE: i32 = -500;
/// The input field whose lengths of time the guest totals.
const WAIT_FIELD: &str = "wait";
/// The destination field that receives the running total of the waits.
const TOTAL_WAIT_FIELD: &str = "total_wait";
const SAVED_STATE_REJECTION: &str =
    "saved state must be an 8-byte row ordinal, optionally followed by an 8-byte total wait";

struct EvenRowFilter {
    /// Rows this branch has accepted. Saved and restored, it numbers the rows that follow.
    processed_rows: u64,
    /// The sum of the `wait` of every row this branch has accepted, in nanoseconds, when the
    /// branch's input carries a `DURATION` field named `wait`. Saved and restored after the row
    /// ordinal.
    total_wait: Option<i64>,
    /// Batches this instance has received, which drives the every-second-batch flush.
    processed_batches: u64,
    /// The accepted batch whose keep or drop decisions are not emitted yet.
    pending: Option<PendingBatch>,
    /// The sentinel value that left this instance unable to serialize its state.
    unserializable_by: Option<i32>,
}

/// An accepted batch the guest still buffers, and the ordinal and total wait of the row before
/// its first row.
struct PendingBatch {
    input: InputBatch,
    start_row: u64,
    start_total_wait: Option<i64>,
}

/// The rows a flush keeps: each one's `value` and, when the branch totals its waits, the total
/// wait after it.
struct SelectedRows {
    values: Vec<i32>,
    total_waits: Option<Vec<i64>>,
}

impl SelectedRows {
    /// No rows, as a message-error envelope routes: every generated column is empty.
    fn none() -> Self {
        Self {
            values: Vec::new(),
            total_waits: Some(Vec::new()),
        }
    }
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
        )?;
        ctx.emit(output)
    }
}

impl Processor for EvenRowFilter {
    fn create(branch: &BranchContext) -> Result<Self, GuestError> {
        let total_wait = if wait_column_index(branch.input_schema()).is_some() {
            Some(0)
        } else {
            None
        };
        Ok(Self {
            processed_rows: 0,
            total_wait,
            processed_batches: 0,
            pending: None,
            unserializable_by: None,
        })
    }

    fn process_batch(
        &mut self,
        ctx: &mut GuestContext<'_>,
        input: InputBatch,
    ) -> Result<(), GuestError> {
        self.processed_batches = self
            .processed_batches
            .checked_add(1)
            .expect("a guest cannot process 2^64 batches in the lifetime of an instance");
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
                    .ok_or_else(|| Report::new(GuestError::NotInitialized))?;
                let output = message_error_envelope(
                    ctx.branch().input_schema(),
                    output_schema,
                    input.acks(),
                    "guest message error for value -100".to_string(),
                )?;
                ctx.emit(output)?;
                return Ok(());
            }
            Some(REQUEST_STATE_RESET_VALUE) => {
                // The row ordinal this guest counts is unusable, so it asks for the whole state
                // lifetime rather than clearing the field. The host replaces it after this
                // callback returns: the batch still buffered here and the one that asked are both
                // left to their sources, and the instance that asked runs nothing more.
                let answer = ctx.request_state_reset();
                if answer == StateResetRequestAnswer::Refused {
                    return Err(GuestError::failed(
                        "host refused a state reset request from process_batch",
                    ));
                }
                return Ok(());
            }
            Some(UNSERIALIZABLE_STATE_VALUE) => {
                self.unserializable_by = Some(UNSERIALIZABLE_STATE_VALUE);
            }
            _ => {}
        }
        self.flush_pending(ctx)?;
        let start_row = self.processed_rows;
        self.processed_rows = self
            .processed_rows
            .checked_add(input.row_count())
            .expect("a guest cannot process 2^64 rows in the lifetime of an instance");
        let start_total_wait = self.total_wait;
        if let Some(total) = self.total_wait {
            let wait_column = wait_column_index(ctx.branch().input_schema())
                .ok_or_else(|| Report::new(GuestError::NotInitialized))?;
            self.total_wait = Some(add_waits(total, &input, wait_column)?);
        }
        self.pending = Some(PendingBatch {
            input,
            start_row,
            start_total_wait,
        });
        if self.unserializable_by.is_some()
            || self.processed_batches.is_multiple_of(FLUSH_EVERY_BATCHES)
        {
            return self.flush_pending(ctx);
        }
        ctx.request_timeout(FLUSH_TIMEOUT)?;
        Ok(())
    }

    fn on_timeout(
        &mut self,
        ctx: &mut GuestContext<'_>,
        _handle: TimeoutHandle,
    ) -> Result<(), GuestError> {
        self.flush_pending(ctx)
    }

    fn flush(&mut self, ctx: &mut GuestContext<'_>) -> Result<(), GuestError> {
        self.flush_pending(ctx)
    }

    fn save_state(&self) -> Result<Vec<u8>, GuestError> {
        if let Some(value) = self.unserializable_by {
            return Err(GuestError::failed(format!(
                "guest cannot serialize its state for value {value}"
            )));
        }
        let mut state = self.processed_rows.to_le_bytes().to_vec();
        if let Some(total) = self.total_wait {
            state.extend_from_slice(&total.to_le_bytes());
        }
        Ok(state)
    }

    fn restore(branch: &BranchContext, state: &[u8]) -> Result<Self, GuestError> {
        let (ordinal, saved_total_wait) = match state.len() {
            8 => (state, None),
            16 => {
                let (ordinal, total) = state.split_at(8);
                let total = <[u8; 8]>::try_from(total)
                    .expect("the second half of sixteen bytes is eight bytes");
                (ordinal, Some(i64::from_le_bytes(total)))
            }
            _ => return Err(GuestError::failed(SAVED_STATE_REJECTION)),
        };
        let processed_rows = <[u8; 8]>::try_from(ordinal)
            .expect("the row ordinal is the first eight bytes of the saved state");
        // A branch whose input has no wait field keeps no total; one whose input gained it starts
        // from no time at all.
        let total_wait = if wait_column_index(branch.input_schema()).is_some() {
            Some(saved_total_wait.unwrap_or(0))
        } else {
            None
        };
        Ok(Self {
            processed_rows: u64::from_le_bytes(processed_rows),
            total_wait,
            processed_batches: 0,
            pending: None,
            unserializable_by: None,
        })
    }

    fn describe_state(&self) -> Result<StateDescription, GuestError> {
        let mut description = StateDescription::new();
        description.add_section(
            "progress",
            vec![ProcessorField {
                name: "processed_rows".to_string(),
                ty: ProcessorType::U64,
                optional: false,
            }],
            vec![Arc::new(UInt64Array::from(vec![self.processed_rows]))],
        )?;
        if let Some(total) = self.total_wait {
            description.add_section(
                "waits",
                vec![ProcessorField {
                    name: TOTAL_WAIT_FIELD.to_string(),
                    ty: ProcessorType::Duration,
                    optional: false,
                }],
                vec![Arc::new(DurationNanosecondArray::from(vec![total]))],
            )?;
        }
        Ok(description)
    }
}

nervix_wasm_sdk::export_processor!(EvenRowFilter);

fn int32_input_error() -> Report<GuestError> {
    Report::new(GuestError::ArrowIpc(ArrowError::SchemaError(
        "input column 0 is not a non-null I32 column".to_string(),
    )))
}

/// The position of the input's `DURATION` field named `wait`, when it has one.
fn wait_column_index(input_schema: &ProcessorSchema) -> Option<usize> {
    input_schema
        .fields
        .iter()
        .position(|field| field.name == WAIT_FIELD && field.ty == ProcessorType::Duration)
}

fn wait_input_error() -> Report<GuestError> {
    Report::new(GuestError::ArrowIpc(ArrowError::SchemaError(format!(
        "input field '{WAIT_FIELD}' is not a DURATION column"
    ))))
}

fn total_wait_overflow() -> Report<GuestError> {
    GuestError::failed("the total wait overflows 64 bits of nanoseconds")
}

/// The waits of one row's column, which the input's schema declares a `DURATION`.
fn wait_values(
    batch: &arrow_array::RecordBatch,
    wait_column: usize,
) -> Result<&DurationNanosecondArray, GuestError> {
    batch
        .column(wait_column)
        .as_any()
        .downcast_ref::<DurationNanosecondArray>()
        .ok_or_else(wait_input_error)
}

/// `total` plus the wait of every row of `input`; a row without a wait adds nothing.
fn add_waits(total: i64, input: &InputBatch, wait_column: usize) -> Result<i64, GuestError> {
    let mut total = total;
    for batch in input.batches() {
        let waits = wait_values(batch, wait_column)?;
        for wait in waits.iter().flatten() {
            total = total.checked_add(wait).ok_or_else(total_wait_overflow)?;
        }
    }
    Ok(total)
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
    pending: &PendingBatch,
) -> Result<OutputEnvelope, GuestError> {
    if output_schemas.is_empty() {
        return Err(Report::new(GuestError::NotInitialized));
    }
    let acks = pending.input.acks();
    let wait_column = wait_column_index(input_schema);
    let mut selected = SelectedRows {
        values: Vec::new(),
        total_waits: pending.start_total_wait.map(|_| Vec::new()),
    };
    let mut running_total_wait = pending.start_total_wait;
    let mut selected_rows = Vec::new();
    let mut acked = acks.acked.clone();
    let mut next_row = pending.start_row;
    let mut input_row = 0_usize;
    for batch in pending.input.batches() {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(int32_input_error)?;
        let waits = match (running_total_wait, wait_column) {
            (Some(_), Some(wait_column)) => Some(wait_values(batch, wait_column)?),
            _ => None,
        };
        for row in 0..values.len() {
            next_row = next_row
                .checked_add(1)
                .expect("the rows counted here belong to one batch held in memory");
            if let (Some(total), Some(waits)) = (running_total_wait, waits)
                && waits.is_valid(row)
            {
                running_total_wait = Some(
                    total
                        .checked_add(waits.value(row))
                        .ok_or_else(total_wait_overflow)?,
                );
            }
            if next_row.is_multiple_of(2) && values.is_valid(row) {
                selected.values.push(values.value(row));
                if let (Some(totals), Some(total)) =
                    (selected.total_waits.as_mut(), running_total_wait)
                {
                    totals.push(total);
                }
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
        output.add_generated_column(generated_column(field, &selected)?, field.optional);
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
        output.add_generated_column(
            generated_column(field, &SelectedRows::none())?,
            field.optional,
        );
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
                        .map_err(|_| Report::new(GuestError::InvalidSize))?,
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
                column_index: u32::try_from(column_index)
                    .map_err(|_| Report::new(GuestError::InvalidSize))?,
            })
        })
        .collect()
}

fn generated_column(
    field: &ProcessorField,
    selected: &SelectedRows,
) -> Result<ArrayRef, GuestError> {
    match (&field.ty, &selected.total_waits) {
        (ProcessorType::I32, _) if field.name == "value" => {
            Ok(Arc::new(Int32Array::from(selected.values.clone())))
        }
        (ProcessorType::String, _) => {
            let values = selected.values.iter().map(|_| "EVEN").collect::<Vec<_>>();
            Ok(Arc::new(StringArray::from(values)))
        }
        (ProcessorType::Duration, Some(total_waits)) if field.name == TOTAL_WAIT_FIELD => {
            Ok(Arc::new(DurationNanosecondArray::from(total_waits.clone())))
        }
        _ => Err(GuestError::failed(format!(
            "no generated value source for required field '{}'",
            field.name
        ))),
    }
}
