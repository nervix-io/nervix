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

use std::{io::Cursor, num::NonZeroUsize, sync::Arc as StdArc};

use arch_into::ArchInto as _;
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use nervix_execution::{BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass};
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
                    let buffer = BudgetedBuffer::with_limit(charge, limit);
                    let mut writer = StreamWriter::try_new(buffer, batch.schema_ref())
                        .map_err(ArrowBodyError::encoding)?;
                    writer.write(&batch).map_err(ArrowBodyError::encoding)?;
                    writer.finish().map_err(ArrowBodyError::encoding)?;
                    let buffer = writer.into_inner().map_err(ArrowBodyError::encoding)?;
                    Ok(ChargedBytes::from_buffer(buffer))
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
    executor
        .run_cpu(
            contract.carriage.cpu_class(),
            reservation,
            move |_charge, cancellation| {
            cancellation
                .check()
                .change_context(ArrowBodyError::Cancelled)?;
            let mut reader = StreamReader::try_new(Cursor::new(body.as_ref()), None)
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
            for next in reader.by_ref() {
                cancellation
                    .check()
                    .change_context(ArrowBodyError::Cancelled)?;
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
            RuntimeRecordBatch::from_decoded_sections(schema, batches)
            },
        )
        .await
        .change_context(ArrowBodyError::Execution)?
}
