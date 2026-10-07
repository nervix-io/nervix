//! The Arrow IPC body a batch travels as, and the only way to produce or consume one.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Encoding a batch into one shared immutable body and decoding a body back into a
//!   batch, both off the async workers and both charged to the budget of the carriage they travel
//!   under before they allocate.
//! - **Depends on.** The executor that admits and charges the work, and Arrow's IPC codec.
//! - **Must not know.** Who sends the body, how many destinations it has, or what happens to the
//!   batch afterwards.
//!
//! There is deliberately no synchronous alternative beside these entry points. A body is encoded
//! once and shared: every destination and every retry sends the same allocation, charged once, and
//! reserves its own outstanding-delivery bytes separately.

use std::{
    io::Cursor,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
};

use arch_into::ArchInto as _;
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_ipc::{MessageHeader, reader::StreamReader, writer::StreamWriter};
use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use nervix_execution::{
    BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass, Reservation,
};
use nervix_primitives::sync::StdArc;
use thiserror::Error;

use super::{CompiledSchema, RuntimeRecordBatch, batch_payload_bytes};

/// Why a relay body could not be produced or consumed.
#[derive(Debug, Error)]
pub enum ArrowBodyError {
    #[error("the node has no relay capacity for this body")]
    Admission,
    #[error("the relay body could not be admitted for execution")]
    Execution,
    #[error("the relay body was cancelled before it finished")]
    Cancelled,
    #[error("a relay body of {size} bytes exceeds the {limit} byte limit")]
    BodyTooLarge { size: u64, limit: u64 },
    #[error("a decoded relay batch of {size} bytes exceeds the {limit} byte limit")]
    DecodedTooLarge { size: u64, limit: u64 },
    #[error("a relay body of {sections} Arrow sections exceeds the {limit} this operation accepts")]
    TooManySections { sections: usize, limit: usize },
    #[error("the relay body carried no Arrow section")]
    NoSection,
    #[error("failed to encode the relay body: {reason}")]
    Encode { reason: String },
    #[error("failed to decode the relay body: {reason}")]
    Decode { reason: String },
}

impl ArrowBodyError {
    fn encoding(error: impl ToString) -> Report<Self> {
        Report::new(Self::Encode {
            reason: error.to_string(),
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the typed Arrow decoding failure conversion"
        )
    )]
    fn decoding(error: impl ToString) -> Report<Self> {
        Report::new(Self::Decode {
            reason: error.to_string(),
        })
    }
}

/// Which budget and which limits one Arrow body is produced and consumed under. A body's carriage
/// is a property of the path it travels, so a relay batch and a sealed snapshot section never
/// share a ceiling or a memory class by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrowBodyCarriage {
    /// One relay batch on the data plane.
    Relay,
    /// One Arrow section of a sealed runtime snapshot on the bulk path.
    SnapshotSection,
}

impl ArrowBodyCarriage {
    const fn memory_class(self) -> MemoryClass {
        match self {
            Self::Relay => MemoryClass::Relay,
            Self::SnapshotSection => MemoryClass::Bulk,
        }
    }

    const fn cpu_class(self) -> CpuClass {
        match self {
            Self::Relay => CpuClass::Data,
            Self::SnapshotSection => CpuClass::Bulk,
        }
    }

    fn encoded_limit(self, executor: &Executor) -> u64 {
        match self {
            Self::Relay => executor.limits().relay_encoded_bytes.as_u64(),
            Self::SnapshotSection => executor.limits().snapshot_section_bytes.as_u64(),
        }
    }

    fn decoded_limit(self, executor: &Executor) -> u64 {
        match self {
            Self::Relay => executor.limits().relay_decoded_bytes.as_u64(),
            Self::SnapshotSection => executor.limits().snapshot_section_bytes.as_u64(),
        }
    }
}

/// What one caller accepts from an untrusted body, checked before the bytes it describes are
/// turned into columns.
struct BodyContract {
    /// The exact schema the body must declare, when the caller already knows it.
    schema: Option<StdArc<ArrowSchema>>,
    /// How many Arrow sections the body may carry.
    max_sections: NonZeroUsize,
    /// The budget and ceilings this body travels under.
    carriage: ArrowBodyCarriage,
}

impl RuntimeRecordBatch {
    /// Encode this batch into one shared Arrow IPC body.
    ///
    /// The work runs on the executor's data workers, never on an async worker, and it writes into
    /// a buffer that fails at the configured relay body limit rather than growing past it. The
    /// returned handle carries the single charge that backs the allocation: cloning it for another
    /// destination costs one refcount and no second copy of the bytes.
    pub async fn encode_arrow_ipc(
        &self,
        executor: &Executor,
    ) -> Result<ChargedBytes, Report<ArrowBodyError>> {
        self.encode_body(executor, ArrowBodyCarriage::Relay).await
    }

    /// Encode this batch as one Arrow section of a sealed runtime snapshot, charged to the bulk
    /// budget and bounded by the section limit rather than by the relay body limit.
    pub async fn encode_arrow_snapshot_section(
        &self,
        executor: &Executor,
    ) -> Result<ChargedBytes, Report<ArrowBodyError>> {
        self.encode_body(executor, ArrowBodyCarriage::SnapshotSection)
            .await
    }

    /// Admit projection and its encoded result together, so neither allocation waits for a
    /// second grant while retaining the first. The projection and IPC writer run as one bounded
    /// CPU job; only the encoded allocation survives that job.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the admitted bounded row projection")
    )]
    pub(crate) async fn encode_arrow_snapshot_projection<E: error_stack::Context>(
        executor: &Executor,
        projection_bytes: u64,
        encoded_estimate: u64,
        project: impl FnOnce() -> error_stack::Result<Self, E> + Send + 'static,
    ) -> Result<ChargedBytes, Report<ArrowBodyError>> {
        let limit = executor.limits().snapshot_section_bytes.as_u64();
        let encoded_bytes = encoded_estimate.min(limit);
        let bytes = projection_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| Report::new(ArrowBodyError::Admission))?;
        let reservation = executor
            .try_reserve(MemoryClass::Bulk, bytes.max(1))
            .change_context(ArrowBodyError::Admission)?;
        executor
            .run_cpu(CpuClass::Bulk, reservation, move |charge, cancellation| {
                let (_projection_charge, encoded_charge) = charge
                    .split(projection_bytes)
                    .change_context(ArrowBodyError::Admission)?;
                cancellation
                    .check()
                    .change_context(ArrowBodyError::Cancelled)?;
                let batch = project().map_err(ArrowBodyError::encoding)?;
                cancellation
                    .check()
                    .change_context(ArrowBodyError::Cancelled)?;
                encode_admitted_body(batch.batch, encoded_charge, limit)
            })
            .await
            .change_context(ArrowBodyError::Execution)?
    }

    async fn encode_body(
        &self,
        executor: &Executor,
        carriage: ArrowBodyCarriage,
    ) -> Result<ChargedBytes, Report<ArrowBodyError>> {
        let limit = carriage.encoded_limit(executor);
        // Charge what the columns already occupy, bounded by the body limit, before the writer
        // allocates anything. It grows from there only while the carriage's class can back it.
        let estimate: u64 = self.batch.get_array_memory_size().arch_into();
        let reservation = executor
            .reserve(carriage.memory_class(), estimate.min(limit))
            .await
            .change_context(ArrowBodyError::Admission)?;
        let batch = self.batch.clone();
        executor
            .run_cpu(
                carriage.cpu_class(),
                reservation,
                move |charge, cancellation| {
                    cancellation
                        .check()
                        .change_context(ArrowBodyError::Cancelled)?;
                    encode_admitted_body(batch, charge, limit)
                },
            )
            .await
            .change_context(ArrowBodyError::Execution)?
    }

    /// Decode a body whose schema the caller does not know in advance, concatenating the sections
    /// it carries within the configured decoded limit.
    pub async fn decode_arrow_ipc(
        executor: &Executor,
        body: ChargedBytes,
    ) -> Result<Self, Report<ArrowBodyError>> {
        decode_body(
            executor,
            body,
            BodyContract {
                schema: None,
                max_sections: NonZeroUsize::MAX,
                carriage: ArrowBodyCarriage::Relay,
            },
        )
        .await
    }

    /// Decode one Arrow section of a sealed runtime snapshot, which must carry exactly one section
    /// of `expected_schema`. Charged to the bulk budget and bounded by the section limit rather
    /// than by the relay body limit.
    pub async fn decode_arrow_snapshot_section(
        executor: &Executor,
        expected_schema: StdArc<ArrowSchema>,
        body: ChargedBytes,
    ) -> Result<Self, Report<ArrowBodyError>> {
        decode_body(
            executor,
            body,
            BodyContract {
                schema: Some(expected_schema),
                max_sections: NonZeroUsize::MIN,
                carriage: ArrowBodyCarriage::SnapshotSection,
            },
        )
        .await
    }

    /// Rebuild one batch from the sections an IPC stream carried. An empty stream still declares a
    /// schema, so it becomes an empty batch of that shape rather than an error.
    fn from_decoded_sections(
        schema: StdArc<ArrowSchema>,
        batches: Vec<RecordBatch>,
    ) -> Result<Self, Report<ArrowBodyError>> {
        if batches.is_empty() {
            let batch = RecordBatch::try_new_with_options(
                schema,
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(0)),
            )
            .map_err(ArrowBodyError::decoding)?;
            return Ok(Self { batch });
        }
        let sections = batches
            .into_iter()
            .map(|batch| Self { batch })
            .collect::<Vec<_>>();
        let refs = sections.iter().collect::<Vec<_>>();
        Self::concat(&refs).map_err(ArrowBodyError::decoding)
    }
}

/// The IPC writer shared by ordinary bodies and admitted snapshot projections. Its callers have
/// already entered a charged CPU job and checked cancellation.
fn encode_admitted_body(
    batch: RecordBatch,
    charge: Reservation,
    limit: u64,
) -> Result<ChargedBytes, Report<ArrowBodyError>> {
    let buffer = BudgetedBuffer::with_limit(charge, limit);
    let mut writer =
        StreamWriter::try_new(buffer, batch.schema_ref()).map_err(ArrowBodyError::encoding)?;
    writer.write(&batch).map_err(ArrowBodyError::encoding)?;
    writer.finish().map_err(ArrowBodyError::encoding)?;
    let buffer = writer.into_inner().map_err(ArrowBodyError::encoding)?;
    Ok(ChargedBytes::from_buffer(buffer))
}

impl CompiledSchema {
    /// Decode one relay body that must carry exactly one Arrow section of this schema. A body that
    /// declares another schema, no section, or more than one is refused before its columns reach a
    /// relay.
    pub async fn decode_arrow_body(
        &self,
        executor: &Executor,
        body: ChargedBytes,
    ) -> Result<RuntimeRecordBatch, Report<ArrowBodyError>> {
        let decoded = decode_body(
            executor,
            body,
            BodyContract {
                schema: Some(StdArc::clone(&self.arrow_schema)),
                max_sections: NonZeroUsize::MIN,
                carriage: ArrowBodyCarriage::Relay,
            },
        )
        .await?;
        if decoded.batch.num_columns() != self.fields.len() {
            return Err(ArrowBodyError::decoding(format!(
                "arrow batch column count {} does not match schema field count {}",
                decoded.batch.num_columns(),
                self.fields.len()
            )));
        }
        Ok(decoded)
    }
}

/// Check the IPC framing and the sizes Arrow trusts before its reader allocates or slices. The
/// FlatBuffers verifier checks table offsets, but it does not constrain their semantic lengths.
fn validate_ipc_messages(bytes: &[u8], decoded_limit: u64) -> Result<(), Report<ArrowBodyError>> {
    let invalid = |reason| ArrowBodyError::decoding(reason);
    let mut offset = 0_usize;
    let mut expanded = 0_u64;
    let mut saw_schema = false;
    while offset < bytes.len() {
        let prefix_end = offset
            .checked_add(4)
            .ok_or_else(|| invalid("Arrow IPC metadata length overflowed"))?;
        let prefix: [u8; 4] = bytes
            .get(offset..prefix_end)
            .ok_or_else(|| invalid("truncated Arrow IPC metadata length"))?
            .try_into()
            .map_err(|_| invalid("invalid Arrow IPC metadata length"))?;
        offset = prefix_end;
        let length = if prefix == [0xff; 4] {
            let end = offset
                .checked_add(4)
                .ok_or_else(|| invalid("Arrow IPC metadata length overflowed"))?;
            let bytes: [u8; 4] = bytes
                .get(offset..end)
                .ok_or_else(|| invalid("truncated Arrow IPC metadata length"))?
                .try_into()
                .map_err(|_| invalid("invalid Arrow IPC metadata length"))?;
            offset = end;
            i32::from_le_bytes(bytes)
        } else {
            i32::from_le_bytes(prefix)
        };
        if length == 0 {
            break;
        }
        let length =
            usize::try_from(length).map_err(|_| invalid("negative Arrow IPC metadata length"))?;
        let metadata_end = offset
            .checked_add(length)
            .ok_or_else(|| invalid("Arrow IPC metadata length overflowed"))?;
        let metadata = bytes
            .get(offset..metadata_end)
            .ok_or_else(|| invalid("truncated Arrow IPC metadata"))?;
        let message = arrow_ipc::root_as_message(metadata).map_err(ArrowBodyError::decoding)?;
        offset = metadata_end;
        let body_length = usize::try_from(message.bodyLength())
            .map_err(|_| invalid("negative or unaddressable Arrow IPC body length"))?;
        let body_end = offset
            .checked_add(body_length)
            .ok_or_else(|| invalid("Arrow IPC body length overflowed"))?;
        let body = bytes
            .get(offset..body_end)
            .ok_or_else(|| invalid("truncated Arrow IPC body"))?;
        if !saw_schema && message.header_type() != MessageHeader::Schema {
            return Err(invalid("Arrow IPC stream does not begin with a schema"));
        }
        match message.header_type() {
            MessageHeader::Schema => {
                saw_schema = true;
            }
            MessageHeader::RecordBatch => {
                let batch = message
                    .header_as_record_batch()
                    .ok_or_else(|| invalid("invalid Arrow IPC record batch header"))?;
                validate_ipc_batch(batch, body, decoded_limit, &mut expanded)?;
            }
            MessageHeader::DictionaryBatch => {
                let dictionary = message
                    .header_as_dictionary_batch()
                    .ok_or_else(|| invalid("invalid Arrow IPC dictionary header"))?;
                let batch = dictionary
                    .data()
                    .ok_or_else(|| invalid("invalid Arrow IPC dictionary record batch"))?;
                validate_ipc_batch(batch, body, decoded_limit, &mut expanded)?;
            }
            _ => {}
        }
        offset = body_end;
    }
    Ok(())
}

fn validate_ipc_batch(
    batch: arrow_ipc::RecordBatch<'_>,
    body: &[u8],
    limit: u64,
    expanded: &mut u64,
) -> Result<(), Report<ArrowBodyError>> {
    let invalid = |reason| ArrowBodyError::decoding(reason);
    let count = |value| {
        let count = u64::try_from(value).map_err(|_| invalid("negative Arrow IPC count"))?;
        if count > limit {
            return Err(invalid("Arrow IPC count exceeds the decoded body limit"));
        }
        Ok(count)
    };
    count(batch.length())?;
    let nodes = batch
        .nodes()
        .ok_or_else(|| invalid("Arrow IPC record batch has no field nodes"))?;
    for node in nodes {
        let length = count(node.length())?;
        if count(node.null_count())? > length {
            return Err(invalid("Arrow IPC null count exceeds its field length"));
        }
    }
    let buffers = batch
        .buffers()
        .ok_or_else(|| invalid("Arrow IPC record batch has no buffers"))?;
    if let Some(counts) = batch.variadicBufferCounts() {
        for variadic in counts {
            if usize::try_from(variadic)
                .ok()
                .is_none_or(|value| value > buffers.len())
            {
                return Err(invalid("Arrow IPC variadic buffer count is invalid"));
            }
        }
    }
    for buffer in buffers {
        let start = usize::try_from(buffer.offset())
            .map_err(|_| invalid("negative or unaddressable Arrow IPC buffer offset"))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| invalid("negative or unaddressable Arrow IPC buffer length"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| invalid("Arrow IPC buffer length overflowed"))?;
        let contents = body
            .get(start..end)
            .ok_or_else(|| invalid("Arrow IPC buffer exceeds its message body"))?;
        let decoded = if batch.compression().is_some() && !contents.is_empty() {
            let declared: [u8; 8] = contents
                .get(..8)
                .ok_or_else(|| invalid("truncated Arrow IPC compression length"))?
                .try_into()
                .map_err(|_| invalid("invalid Arrow IPC compression length"))?;
            match i64::from_le_bytes(declared) {
                -1 => u64::try_from(contents.len() - 8)
                    .map_err(|_| invalid("unaddressable Arrow IPC buffer length"))?,
                value => u64::try_from(value)
                    .map_err(|_| invalid("negative Arrow IPC compression length"))?,
            }
        } else {
            u64::try_from(length).map_err(|_| invalid("unaddressable Arrow IPC buffer length"))?
        };
        *expanded = expanded
            .checked_add(decoded)
            .ok_or_else(|| invalid("Arrow IPC decoded buffer length overflowed"))?;
        if *expanded > limit {
            return Err(invalid("Arrow IPC buffers exceed the decoded body limit"));
        }
    }
    Ok(())
}

async fn decode_body(
    executor: &Executor,
    body: ChargedBytes,
    contract: BodyContract,
) -> Result<RuntimeRecordBatch, Report<ArrowBodyError>> {
    let encoded_limit = contract.carriage.encoded_limit(executor);
    let encoded: u64 = body.len().arch_into();
    // The encoded length is known before anything is decoded, so an oversized body is refused here
    // rather than after its buffers have been allocated.
    if encoded > encoded_limit {
        return Err(Report::new(ArrowBodyError::BodyTooLarge {
            size: encoded,
            limit: encoded_limit,
        }));
    }
    let decoded_limit = contract.carriage.decoded_limit(executor);
    // Charge the data the decoder will produce and the scratch its conversion overlaps with, both
    // before it allocates either. Uncompressed Arrow decodes to about the size it was encoded at,
    // so one encoded size covers the decoded columns and a second covers the overlap. The charge
    // scales with this body rather than with the largest one the class would accept: a kilobyte of
    // relay traffic must not hold the whole scratch allowance.
    let charge = encoded
        .checked_mul(2)
        .unwrap_or(decoded_limit)
        .min(decoded_limit);
    let reservation = executor
        .reserve(contract.carriage.memory_class(), charge)
        .await
        .change_context(ArrowBodyError::Admission)?;
    executor
        .run_cpu(
            contract.carriage.cpu_class(),
            reservation,
            move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(ArrowBodyError::Cancelled)?;
                validate_ipc_messages(body.as_ref(), decoded_limit)?;
                let mut reader = catch_unwind(AssertUnwindSafe(|| {
                    StreamReader::try_new(Cursor::new(body.as_ref()), None)
                }))
                .map_err(|_| ArrowBodyError::decoding("invalid Arrow IPC schema"))?
                .map_err(ArrowBodyError::decoding)?;
                let schema = reader.schema();
                if let Some(expected) = &contract.schema
                    && schema.as_ref() != expected.as_ref()
                {
                    return Err(ArrowBodyError::decoding(
                        "arrow ipc schema does not match the compiled schema",
                    ));
                }
                let mut batches = Vec::new();
                let mut decoded = 0_u64;
                loop {
                    cancellation
                        .check()
                        .change_context(ArrowBodyError::Cancelled)?;
                    let next = catch_unwind(AssertUnwindSafe(|| reader.next()))
                        .map_err(|_| ArrowBodyError::decoding("invalid Arrow IPC record batch"))?;
                    let Some(next) = next else {
                        break;
                    };
                    let batch = next.map_err(ArrowBodyError::decoding)?;
                    if batches.len() >= contract.max_sections.get() {
                        return Err(Report::new(ArrowBodyError::TooManySections {
                            sections: batches
                                .len()
                                .checked_add(1)
                                .unwrap_or(contract.max_sections.get()),
                            limit: contract.max_sections.get(),
                        }));
                    }
                    let section = batch_payload_bytes(&batch);
                    decoded = decoded.checked_add(section).ok_or_else(|| {
                        Report::new(ArrowBodyError::DecodedTooLarge {
                            size: u64::MAX,
                            limit: decoded_limit,
                        })
                    })?;
                    // Each section is measured as it lands, so a body whose declared buffers expand
                    // past the limit stops at the section that crossed it.
                    if decoded > decoded_limit {
                        return Err(Report::new(ArrowBodyError::DecodedTooLarge {
                            size: decoded,
                            limit: decoded_limit,
                        }));
                    }
                    batches.push(batch);
                }
                if batches.is_empty() && contract.schema.is_some() {
                    return Err(Report::new(ArrowBodyError::NoSection));
                }
                catch_unwind(AssertUnwindSafe(|| {
                    RuntimeRecordBatch::from_decoded_sections(schema, batches)
                }))
                .map_err(|_| ArrowBodyError::decoding("invalid Arrow IPC sections"))?
            },
        )
        .await
        .change_context(ArrowBodyError::Execution)?
}

#[cfg(test)]
mod projection_tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[nervix_primitives::test]
    async fn snapshot_projection_refuses_before_conversion_when_bulk_memory_is_busy() {
        let config = nervix_execution::ExecutionConfig::default();
        let bulk_bytes = config.budgets.bulk.as_u64();
        let executor = Executor::new(config).assured("the default executor is valid");
        let _busy = executor
            .try_reserve(MemoryClass::Bulk, bulk_bytes)
            .assured("the test holds the bulk budget");
        let result = RuntimeRecordBatch::encode_arrow_snapshot_projection::<
            super::super::RuntimeSchemaError,
        >(&executor, 1024, 1024, || {
            panic!("a refused projection must not allocate or convert rows")
        })
        .await;
        assert!(matches!(
            result
                .expect_err("the occupied budget refuses projection")
                .current_context(),
            ArrowBodyError::Admission
        ));
    }
}

#[cfg(test)]
mod malformed_ipc_tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    #[nervix_primitives::test]
    async fn a_declared_ipc_body_must_fit_before_arrow_allocates_it() {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let message = arrow_ipc::Message::create(
            &mut builder,
            &arrow_ipc::MessageArgs {
                bodyLength: i64::MAX,
                ..Default::default()
            },
        );
        builder.finish(message, None);
        let metadata = builder.finished_data();
        let mut bytes = u32::try_from(metadata.len())
            .assured("bounded metadata length fits")
            .to_le_bytes()
            .to_vec();
        bytes.extend_from_slice(metadata);
        let executor = Executor::default();
        let reservation = executor
            .try_reserve(
                MemoryClass::Relay,
                u64::try_from(bytes.len()).assured("bounded body"),
            )
            .assured("the bounded body is admitted");
        let body = ChargedBytes::from_owned(bytes, reservation);
        let error = RuntimeRecordBatch::decode_arrow_ipc(&executor, body)
            .await
            .expect_err("an impossible IPC body length is refused before allocation");
        assert!(matches!(
            error.current_context(),
            ArrowBodyError::Decode { .. }
        ));
    }

    #[test]
    fn an_ipc_buffer_must_fit_its_message_body() {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let nodes = builder.create_vector(&[arrow_ipc::FieldNode::new(1, 0)]);
        let buffers = builder.create_vector(&[arrow_ipc::Buffer::new(64, i64::MAX)]);
        let batch = arrow_ipc::RecordBatch::create(
            &mut builder,
            &arrow_ipc::RecordBatchArgs {
                length: 1,
                nodes: Some(nodes),
                buffers: Some(buffers),
                ..Default::default()
            },
        );
        let message = arrow_ipc::Message::create(
            &mut builder,
            &arrow_ipc::MessageArgs {
                header_type: MessageHeader::RecordBatch,
                header: Some(batch.as_union_value()),
                bodyLength: 128,
                ..Default::default()
            },
        );
        builder.finish(message, None);
        let message = arrow_ipc::root_as_message(builder.finished_data())
            .assured("a bounded record batch metadata table verifies");
        let batch = message
            .header_as_record_batch()
            .assured("the message carries its record batch");
        let error = validate_ipc_batch(batch, &[0; 128], 1024, &mut 0)
            .expect_err("a declared buffer cannot escape its message body");
        assert!(matches!(
            error.current_context(),
            ArrowBodyError::Decode { .. }
        ));
    }
}
