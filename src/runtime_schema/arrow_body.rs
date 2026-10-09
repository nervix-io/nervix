//! The Arrow IPC body a batch travels as, and the only way to produce or consume one.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Encoding a batch into one shared immutable body and decoding a body back into a
//!   batch, both off the async workers and both charged to the budget of the carriage they travel
//!   under before they allocate.
//! - **Depends on.** The executor that admits and charges the work, the IPC stream scan that checks
//!   a body and opens Arrow's reader over it, and Arrow's IPC writer.
//! - **Must not know.** Who sends the body, how many destinations it has, or what happens to the
//!   batch afterwards.
//!
//! There is deliberately no synchronous alternative beside these entry points. A body is encoded
//! once and shared: every destination and every retry sends the same allocation, charged once, and
//! reserves its own outstanding-delivery bytes separately.

use std::num::NonZeroUsize;

use arch_into::ArchInto as _;
use arrow_array::{RecordBatch, RecordBatchOptions, new_empty_array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use nervix_execution::{
    BudgetedBuffer, ChargedBytes, CpuClass, ExecutionError, Executor, MemoryClass, Reservation,
};
use nervix_primitives::sync::StdArc;
use thiserror::Error;

use super::{
    CompiledSchema, RuntimeRecordBatch, batch_payload_bytes,
    ipc_stream::{IpcFramingDefect, IpcMessages, IpcStreamError},
};

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
    #[error("the relay body's Arrow stream is misframed: {defect}")]
    Framing { defect: IpcFramingDefect },
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

    /// A body the scan refused, with what the scan found beneath: misframed, naming what its
    /// framing gets wrong, or one that does not decode for what its stream declares.
    fn refused(refusal: Report<IpcStreamError>) -> Report<Self> {
        let context = match refusal.current_context() {
            IpcStreamError::Framing { defect } => Self::Framing {
                defect: defect.clone(),
            },
            declared => Self::Decode {
                reason: declared.to_string(),
            },
        };
        refusal.change_context(context)
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
            let columns = schema
                .fields()
                .iter()
                .map(|field| new_empty_array(field.data_type()))
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new_with_options(
                schema,
                columns,
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
    let decoded = executor
        .run_cpu(
            contract.carriage.cpu_class(),
            reservation,
            move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(ArrowBodyError::Cancelled)?;
                // Arrow's reader sizes what it allocates from the lengths a stream declares, and
                // panics on a stream that declares what it does not expect. The stream is
                // therefore scanned first, and its sections counted, before any column is read.
                let scanned = IpcMessages::new(body.as_ref())
                    .scan(None)
                    .map_err(ArrowBodyError::refused)?;
                if scanned.record_batches > contract.max_sections.get() {
                    return Err(Report::new(ArrowBodyError::TooManySections {
                        sections: scanned.record_batches,
                        limit: contract.max_sections.get(),
                    }));
                }
                if scanned.record_batches == 0 && contract.schema.is_some() {
                    return Err(Report::new(ArrowBodyError::NoSection));
                }
                let mut reader = scanned.reader().map_err(ArrowBodyError::decoding)?;
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
                for next in reader.by_ref() {
                    cancellation
                        .check()
                        .change_context(ArrowBodyError::Cancelled)?;
                    let batch = next.map_err(ArrowBodyError::decoding)?;
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
                RuntimeRecordBatch::from_decoded_sections(schema, batches)
            },
        )
        .await;
    match decoded {
        Ok(batch) => batch,
        Err(error) => {
            let failure = match error.current_context() {
                ExecutionError::QueueFull { .. } | ExecutionError::PoolClosed { .. } => {
                    ArrowBodyError::Execution
                }
                // Arrow's reader panics only on a stream it failed to reject, so the body is
                // what is wrong, and decoding it again would panic again.
                ExecutionError::JobPanicked { .. } => ArrowBodyError::Decode {
                    reason: "the stream could not be decoded".to_string(),
                },
            };
            Err(error.change_context(failure))
        }
    }
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
mod framing_tests {
    use std::ops::Range;

    use arrow_array::{ArrayRef, Int32Array, Int64Array};
    use arrow_schema::{DataType, Field};
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::{super::ipc_stream::CONTINUATION_MARKER, *};

    /// The length prefix of a message in the current stream format: the continuation marker and
    /// the metadata length.
    const PREFIX_BYTES: usize = 8;

    /// One sealed section of three rows, and the batch it was sealed from.
    struct SealedSection {
        schema: StdArc<ArrowSchema>,
        batch: RuntimeRecordBatch,
        bytes: Vec<u8>,
    }

    impl SealedSection {
        /// A wide and a narrow column. Nothing else in the record batch message is twelve bytes
        /// long, so a test finds the narrow column's buffer length by its value.
        async fn sealed(executor: &Executor) -> Self {
            let fields = vec![
                Field::new("wide", DataType::Int64, false),
                Field::new("narrow", DataType::Int32, false),
            ];
            let wide: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2, 3]));
            let narrow: ArrayRef = StdArc::new(Int32Array::from(vec![4, 5, 6]));
            Self::of(executor, fields, vec![wide, narrow]).await
        }

        async fn of(executor: &Executor, fields: Vec<Field>, columns: Vec<ArrayRef>) -> Self {
            let schema = StdArc::new(ArrowSchema::new(fields));
            let batch = RecordBatch::try_new(StdArc::clone(&schema), columns)
                .assured("every column has its field's type and three rows");
            let batch = RuntimeRecordBatch::from_record_batch(StdArc::clone(&schema), batch)
                .assured("the batch has the schema it was built with");
            let section = batch
                .encode_arrow_snapshot_section(executor)
                .await
                .assured("a three-row section seals");
            Self {
                schema,
                bytes: section.as_ref().to_vec(),
                batch,
            }
        }

        /// Opens `bytes` as a section of this schema.
        async fn open(
            &self,
            executor: &Executor,
            bytes: Vec<u8>,
        ) -> Result<RuntimeRecordBatch, Report<ArrowBodyError>> {
            let section = executor
                .charge_owned(MemoryClass::Bulk, bytes)
                .await
                .assured("the bulk budget holds one small section");
            RuntimeRecordBatch::decode_arrow_snapshot_section(
                executor,
                StdArc::clone(&self.schema),
                section,
            )
            .await
        }

        /// Where the metadata of the record batch message lies, and where in it the message
        /// declares its body length.
        fn record_batch(&self) -> RecordBatchMessage {
            let mut offset = 0_usize;
            loop {
                let prefix_end = offset
                    .checked_add(PREFIX_BYTES)
                    .assured("a sealed test section is far below the address width");
                let prefix = &self.bytes[offset..prefix_end];
                assert_eq!(prefix[..4], CONTINUATION_MARKER);
                let length: [u8; 4] = prefix[4..]
                    .try_into()
                    .assured("the prefix holds four length bytes behind its marker");
                let length = usize::try_from(i32::from_le_bytes(length))
                    .assured("a sealed message declares a positive metadata length");
                let metadata_end = prefix_end
                    .checked_add(length)
                    .assured("a sealed test section is far below the address width");
                let metadata = prefix_end..metadata_end;
                let message = arrow_ipc::root_as_message(&self.bytes[metadata.clone()])
                    .assured("a sealed message is an Arrow message");
                let body_length = message.bodyLength();
                if message.header_type() == arrow_ipc::MessageHeader::RecordBatch {
                    // A table's vtable holds each field's offset from the table.
                    let field = message._tab.vtable().get(arrow_ipc::Message::VT_BODYLENGTH);
                    let table = prefix_end
                        .checked_add(message._tab.loc())
                        .assured("a position inside the section is far below the address width");
                    let body_length_field = table
                        .checked_add(usize::from(field))
                        .assured("a position inside the section is far below the address width");
                    return RecordBatchMessage {
                        metadata,
                        body_length_field,
                    };
                }
                let body_bytes = usize::try_from(body_length)
                    .assured("a sealed message declares a body length within the section");
                offset = metadata_end
                    .checked_add(body_bytes)
                    .assured("a sealed test section is far below the address width");
            }
        }

        /// The position of the one little-endian 64-bit field of `metadata` that holds `value`.
        fn field_holding(&self, metadata: &Range<usize>, value: i64) -> usize {
            let pattern = value.to_le_bytes();
            let mut found = Vec::new();
            for (index, window) in self.bytes[metadata.clone()].windows(8).enumerate() {
                if window == pattern {
                    found.push(index);
                }
            }
            assert_eq!(
                found.len(),
                1,
                "exactly one field of the message holds {value}"
            );
            metadata
                .start
                .checked_add(found[0])
                .assured("a position inside the section is far below the address width")
        }
    }

    struct RecordBatchMessage {
        metadata: Range<usize>,
        /// The position of the little-endian 64-bit body length the message declares.
        body_length_field: usize,
    }

    /// The framing defect `result` was refused for.
    fn framing_defect(
        result: Result<RuntimeRecordBatch, Report<ArrowBodyError>>,
    ) -> IpcFramingDefect {
        let report = result.expect_err("a misframed section does not open");
        match report.current_context() {
            ArrowBodyError::Framing { defect } => defect.clone(),
            other => panic!("the section is refused as misframed: {other:?}"),
        }
    }

    #[nervix_primitives::test]
    async fn a_sealed_section_opens_with_every_row() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        let opened = section
            .open(&executor, section.bytes.clone())
            .await
            .assured("an undamaged section opens");
        assert_eq!(opened.batch(), section.batch.batch());
    }

    /// The reader would otherwise allocate the declared body, 256 TiB here, before it found the
    /// stream too short to fill it.
    #[nervix_primitives::test]
    async fn a_message_body_longer_than_the_stream_is_refused_before_it_is_allocated() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        let message = section.record_batch();
        let mut damaged = section.bytes.clone();
        // Bit 48 of the little-endian body length.
        let high_byte = message
            .body_length_field
            .checked_add(6)
            .assured("a position inside the section is far below the address width");
        damaged[high_byte] ^= 1;
        let declared = arrow_ipc::root_as_message(&damaged[message.metadata.clone()])
            .assured("the damaged metadata is still an Arrow message")
            .bodyLength();
        assert!(
            declared > 1 << 48,
            "the message declares {declared} body bytes"
        );

        let result = section.open(&executor, damaged).await;

        assert_eq!(framing_defect(result), IpcFramingDefect::Truncated);
    }

    /// The reader would otherwise zero the declared metadata, 2 GiB here, before it found the
    /// stream too short to fill it.
    #[nervix_primitives::test]
    async fn message_metadata_longer_than_the_stream_is_refused_before_it_is_allocated() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        let mut damaged = section.bytes.clone();
        damaged[4..PREFIX_BYTES].copy_from_slice(&i32::MAX.to_le_bytes());

        let result = section.open(&executor, damaged).await;

        assert_eq!(framing_defect(result), IpcFramingDefect::Truncated);
    }

    #[nervix_primitives::test]
    async fn a_negative_metadata_length_is_refused() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        let mut damaged = section.bytes.clone();
        damaged[4..PREFIX_BYTES].copy_from_slice(&i32::MIN.to_le_bytes());

        let result = section.open(&executor, damaged).await;

        assert_eq!(framing_defect(result), IpcFramingDefect::MetadataLength);
    }

    #[nervix_primitives::test]
    async fn a_stream_cut_inside_a_length_prefix_is_refused() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        let mut damaged = section.bytes.clone();
        damaged.truncate(6);

        let result = section.open(&executor, damaged).await;

        assert_eq!(framing_defect(result), IpcFramingDefect::Truncated);
    }

    #[nervix_primitives::test]
    async fn a_message_with_a_damaged_continuation_marker_is_refused() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        let mut damaged = section.bytes.clone();
        damaged[0] = 0xfe;

        let result = section.open(&executor, damaged).await;

        assert_eq!(framing_defect(result), IpcFramingDefect::Continuation);
    }

    #[nervix_primitives::test]
    async fn a_stream_without_its_end_of_stream_marker_is_refused() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        let mut damaged = section.bytes.clone();
        let marker = damaged
            .len()
            .checked_sub(PREFIX_BYTES)
            .assured("a sealed section ends with its end-of-stream marker");
        assert_eq!(damaged[marker..][..4], CONTINUATION_MARKER);
        damaged.truncate(marker);

        let result = section.open(&executor, damaged).await;

        assert_eq!(framing_defect(result), IpcFramingDefect::Truncated);
    }

    #[nervix_primitives::test]
    async fn bytes_behind_the_end_of_stream_marker_are_refused() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        let mut damaged = section.bytes.clone();
        damaged.push(0);

        let result = section.open(&executor, damaged).await;

        assert_eq!(framing_defect(result), IpcFramingDefect::TrailingBytes);
    }

    /// The reader would otherwise panic on a worker when it sliced the buffer out of the body.
    #[nervix_primitives::test]
    async fn a_column_buffer_outside_its_message_body_is_refused_without_a_worker_panic() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        let message = section.record_batch();
        // The narrow column's three 32-bit values are the only 12 bytes the message declares.
        let field = section.field_holding(&message.metadata, 12);
        let mut damaged = section.bytes.clone();
        // Bit 40 of the little-endian buffer length.
        let high_byte = field
            .checked_add(5)
            .assured("a position inside the section is far below the address width");
        damaged[high_byte] ^= 1;
        let longest = arrow_ipc::root_as_message(&damaged[message.metadata.clone()])
            .assured("the damaged metadata is still an Arrow message")
            .header_as_record_batch()
            .assured("the damaged message is still a record batch")
            .buffers()
            .assured("a record batch of two columns declares buffers")
            .iter()
            .map(|buffer| buffer.length())
            .max()
            .assured("a record batch of two columns declares buffers");
        assert!(longest > 1 << 40, "a buffer declares {longest} bytes");

        let result = section.open(&executor, damaged).await;

        assert_eq!(framing_defect(result), IpcFramingDefect::Buffer);
    }

    /// Whatever single bit of a sealed section is damaged, the section either opens or is refused
    /// for what it holds. No such damage reaches the reader as a length it would allocate, which
    /// aborts the process when the node cannot make the allocation, or as a declaration it would
    /// panic on, as its schema conversion does on a field type the verifier admits: the scan
    /// refuses both before the reader reads the stream.
    #[nervix_primitives::test]
    async fn every_single_damaged_bit_opens_or_is_refused_typed() {
        let executor = Executor::default();
        let section = SealedSection::sealed(&executor).await;
        for position in 0..section.bytes.len() {
            nervix_primitives::task::consume_budget().await;
            for bit in 0..8_u8 {
                let mut damaged = section.bytes.clone();
                damaged[position] ^= 1_u8 << bit;

                let result = section.open(&executor, damaged).await;

                let Err(report) = result else {
                    continue;
                };
                // What the scan or the reader rejects for what the stream declares is a decode
                // failure, and what the framing rejects names its defect.
                assert!(
                    matches!(
                        report.current_context(),
                        ArrowBodyError::Decode { .. }
                            | ArrowBodyError::Framing { .. }
                            | ArrowBodyError::NoSection
                            | ArrowBodyError::TooManySections { .. }
                    ),
                    "bit {bit} of byte {position}: {report:?}"
                );
            }
        }
    }
}
