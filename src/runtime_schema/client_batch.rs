//! The one Arrow IPC form a client producer submits a batch in, and the only way to accept one.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Checking the framing of a submitted stream before any column is allocated, its exact
//!   schema, its row and byte limits, and decoding its one record batch off the async workers
//!   under the relay budget.
//! - **Depends on.** The executor that admits and charges the work, Arrow's IPC codec, and the
//!   vocabulary's batch defects.
//! - **Must not know.** Producers, sessions, ingestors, or what happens to the batch afterwards.
//!
//! A client batch is one uncompressed Arrow IPC stream: its schema message, exactly one record
//! batch message, and the end-of-stream marker, every message opened by the continuation marker.
//! The schema is the ingestor's own, field for field: names, order, types and nullability, with
//! no schema or field metadata. Nothing is inferred, cast or reordered.

use std::{
    fmt,
    io::Cursor,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc as StdArc,
};

use arch_into::ArchInto as _;
use arrow_ipc::{MessageHeader, reader::StreamReader, root_as_message};
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};
use bytes::Bytes;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{CpuClass, ExecutionError, Executor, MemoryClass};
use nervix_models::ClientBatchDefect;
use thiserror::Error;

use super::{CompiledSchema, RuntimeRecordBatch, batch_payload_bytes};

/// The marker that opens every message of a canonical Arrow IPC stream.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// The width of the continuation marker and of the metadata length that follows it.
const FRAME_WORD: usize = 4;

/// What one client batch may carry, as its producer's grant decides.
#[derive(Debug, Clone, Copy)]
pub struct ClientBatchLimits {
    pub max_bytes: NonZeroU64,
    pub max_rows: NonZeroUsize,
}

/// Why a submitted body did not become one batch of its ingestor's schema.
#[derive(Debug, Clone, Error)]
pub enum ClientBatchError {
    #[error("the node has no capacity to validate the batch now")]
    Busy,
    #[error("a batch of {size} bytes exceeds the {limit} byte limit")]
    TooLarge { size: u64, limit: u64 },
    #[error("the batch is not a canonical Arrow IPC stream: {reason}")]
    Malformed { reason: String },
    #[error("the stream carries a {kind} message, which a client batch never does")]
    UnexpectedMessage { kind: &'static str },
    #[error("the record batch is compressed, and a client batch is uncompressed")]
    Compressed,
    #[error("the batch's schema differs from the ingestor's: {difference}")]
    SchemaMismatch { difference: ClientSchemaDifference },
    #[error("the stream carries {batches} record batches, and a client batch is exactly one")]
    NotOneBatch { batches: usize },
    #[error("the batch has {rows} rows, more than the {limit} one batch may carry")]
    TooManyRows { rows: u64, limit: usize },
    #[error("the batch's columns are invalid: {reason}")]
    InvalidData { reason: String },
}

impl ClientBatchError {
    /// The defect this names in the submitted batch, or `None` when the node, not the batch, is
    /// why it was not accepted.
    pub fn defect(&self) -> Option<ClientBatchDefect> {
        match self {
            Self::Busy => None,
            Self::TooLarge { .. } => Some(ClientBatchDefect::TooLarge),
            Self::Malformed { .. } => Some(ClientBatchDefect::Malformed),
            Self::UnexpectedMessage { .. } => Some(ClientBatchDefect::UnexpectedMessage),
            Self::Compressed => Some(ClientBatchDefect::Compressed),
            Self::SchemaMismatch { .. } => Some(ClientBatchDefect::SchemaMismatch),
            Self::NotOneBatch { .. } => Some(ClientBatchDefect::NotOneBatch),
            Self::TooManyRows { .. } => Some(ClientBatchDefect::TooManyRows),
            Self::InvalidData { .. } => Some(ClientBatchDefect::InvalidData),
        }
    }

    fn malformed(reason: impl ToString) -> Report<Self> {
        Report::new(Self::Malformed {
            reason: reason.to_string(),
        })
    }

    fn invalid_data(reason: impl ToString) -> Report<Self> {
        Report::new(Self::InvalidData {
            reason: reason.to_string(),
        })
    }
}

/// The first way a submitted schema differs from the ingestor's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientSchemaDifference {
    FieldCount {
        expected: usize,
        found: usize,
    },
    FieldName {
        index: usize,
        expected: String,
        found: String,
    },
    FieldType {
        field: String,
        expected: ArrowDataType,
        found: ArrowDataType,
    },
    FieldNullability {
        field: String,
        expected_nullable: bool,
    },
    Metadata,
}

impl ClientSchemaDifference {
    /// The first difference between the ingestor's schema and a submitted one, or `None` when
    /// they are the same schema.
    fn between(expected: &ArrowSchema, found: &ArrowSchema) -> Option<Self> {
        if expected == found {
            return None;
        }
        let expected_fields = expected.fields();
        let found_fields = found.fields();
        if expected_fields.len() != found_fields.len() {
            return Some(Self::FieldCount {
                expected: expected_fields.len(),
                found: found_fields.len(),
            });
        }
        for (index, (expected, found)) in expected_fields.iter().zip(found_fields).enumerate() {
            if expected.name() != found.name() {
                return Some(Self::FieldName {
                    index,
                    expected: expected.name().clone(),
                    found: found.name().clone(),
                });
            }
            if expected.data_type() != found.data_type() {
                return Some(Self::FieldType {
                    field: expected.name().clone(),
                    expected: expected.data_type().clone(),
                    found: found.data_type().clone(),
                });
            }
            if expected.is_nullable() != found.is_nullable() {
                return Some(Self::FieldNullability {
                    field: expected.name().clone(),
                    expected_nullable: expected.is_nullable(),
                });
            }
        }
        // Names, types and nullability agree everywhere, so what remains is metadata the
        // ingestor's schema does not carry.
        Some(Self::Metadata)
    }
}

impl fmt::Display for ClientSchemaDifference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldCount { expected, found } => write!(
                formatter,
                "it has {found} fields where the ingestor's schema has {expected}"
            ),
            Self::FieldName {
                index,
                expected,
                found,
            } => write!(
                formatter,
                "field {index} is '{found}' where the ingestor's schema has '{expected}'"
            ),
            Self::FieldType {
                field,
                expected,
                found,
            } => write!(
                formatter,
                "field '{field}' is {found} where the ingestor's schema has {expected}"
            ),
            Self::FieldNullability {
                field,
                expected_nullable: true,
            } => write!(
                formatter,
                "field '{field}' is not nullable where the ingestor's schema declares it optional"
            ),
            Self::FieldNullability {
                field,
                expected_nullable: false,
            } => write!(
                formatter,
                "field '{field}' is nullable where the ingestor's schema declares it required"
            ),
            Self::Metadata => write!(
                formatter,
                "it carries schema or field metadata, which the ingestor's schema does not"
            ),
        }
    }
}

/// The messages of an Arrow IPC stream, read one at a time without decoding their bodies, so a
/// client batch's framing is checked before any column is allocated.
struct IpcMessages<'a> {
    body: &'a [u8],
    offset: usize,
}

/// The messages one scanned stream carried.
struct ScannedStream {
    record_batches: usize,
}

impl<'a> IpcMessages<'a> {
    fn new(body: &'a [u8]) -> Self {
        Self { body, offset: 0 }
    }

    /// Checks that the stream is one schema message, record batch messages within `max_rows`
    /// and without compression, and the end-of-stream marker ending the body.
    fn scan(mut self, max_rows: NonZeroUsize) -> Result<ScannedStream, Report<ClientBatchError>> {
        let mut schema_seen = false;
        let mut record_batches = 0_usize;
        loop {
            let Some(message) = self.next_message()? else {
                break;
            };
            let header = message.header_type();
            if header == MessageHeader::Schema && !schema_seen {
                schema_seen = true;
                continue;
            }
            if !schema_seen {
                return Err(ClientBatchError::malformed(
                    "the stream does not open with its schema message",
                ));
            }
            if header != MessageHeader::RecordBatch {
                let kind = header.variant_name().unwrap_or("unknown");
                return Err(Report::new(ClientBatchError::UnexpectedMessage { kind }));
            }
            let batch = message.header_as_record_batch().verified(
                "the message verifier admits a union type only beside its value, and the type is \
                 a record batch, checked above",
            );
            if batch.compression().is_some() {
                return Err(Report::new(ClientBatchError::Compressed));
            }
            let Ok(rows) = u64::try_from(batch.length()) else {
                return Err(ClientBatchError::malformed(
                    "a record batch declares a negative row count",
                ));
            };
            let limit: u64 = max_rows.get().arch_into();
            if rows > limit {
                return Err(Report::new(ClientBatchError::TooManyRows {
                    rows,
                    limit: max_rows.get(),
                }));
            }
            record_batches = record_batches
                .checked_add(1)
                .assured("every counted message occupies bytes of a body that fits in memory");
        }
        if !schema_seen {
            return Err(ClientBatchError::malformed(
                "the stream ends before its schema message",
            ));
        }
        Ok(ScannedStream { record_batches })
    }

    /// The next message's header, or `None` at the end-of-stream marker, which must end the body.
    fn next_message(&mut self) -> Result<Option<arrow_ipc::Message<'a>>, Report<ClientBatchError>> {
        let marker = self.take(FRAME_WORD)?;
        if marker != CONTINUATION_MARKER {
            return Err(ClientBatchError::malformed(
                "a message does not open with the continuation marker",
            ));
        }
        let length_bytes = self.take(FRAME_WORD)?;
        let length = i32::from_le_bytes(
            length_bytes
                .try_into()
                .verified("take returned exactly the four bytes it was asked for"),
        );
        if length == 0 {
            if self.offset != self.body.len() {
                return Err(ClientBatchError::malformed(
                    "bytes follow the end-of-stream marker",
                ));
            }
            return Ok(None);
        }
        let Ok(length) = usize::try_from(length) else {
            return Err(ClientBatchError::malformed(
                "a message declares a negative metadata length",
            ));
        };
        let metadata = self.take(length)?;
        let message = root_as_message(metadata).map_err(ClientBatchError::malformed)?;
        let Ok(body_length) = usize::try_from(message.bodyLength()) else {
            return Err(ClientBatchError::malformed(
                "a message declares a body length outside this body",
            ));
        };
        self.take(body_length)?;
        Ok(Some(message))
    }

    /// The next `length` bytes of the body.
    fn take(&mut self, length: usize) -> Result<&'a [u8], Report<ClientBatchError>> {
        let body: &'a [u8] = self.body;
        let Some(end) = self.offset.checked_add(length) else {
            return Err(ClientBatchError::malformed(
                "the stream ends inside a message",
            ));
        };
        let Some(bytes) = body.get(self.offset..end) else {
            return Err(ClientBatchError::malformed(
                "the stream ends inside a message",
            ));
        };
        self.offset = end;
        Ok(bytes)
    }
}

impl CompiledSchema {
    /// Decode one batch a client producer submitted. It must be exactly one uncompressed record
    /// batch of this schema within `limits`; anything else is refused with the defect it has,
    /// checked before the columns it describes are allocated wherever the framing allows.
    ///
    /// The decode runs on the executor's data workers under a relay charge taken without
    /// waiting, so a node without the capacity refuses the batch as busy rather than stalling its
    /// ingestor's admission behind it.
    pub async fn decode_client_batch(
        &self,
        executor: &Executor,
        body: Bytes,
        limits: ClientBatchLimits,
    ) -> Result<RuntimeRecordBatch, Report<ClientBatchError>> {
        let encoded: u64 = body.len().arch_into();
        if encoded > limits.max_bytes.get() {
            return Err(Report::new(ClientBatchError::TooLarge {
                size: encoded,
                limit: limits.max_bytes.get(),
            }));
        }
        let decoded_limit = executor.limits().relay_decoded_bytes.as_u64();
        // Uncompressed Arrow decodes to about the size it was encoded at, so one encoded size
        // covers the decoded columns and a second covers the scratch the conversion overlaps with.
        let charge = encoded
            .checked_mul(2)
            .assured("a batch within its producer's byte grant is far below half of u64::MAX")
            .min(decoded_limit);
        let reservation = executor
            .try_reserve(MemoryClass::Relay, charge)
            .change_context(ClientBatchError::Busy)?;
        let expected = StdArc::clone(&self.arrow_schema);
        let decoded = executor
            .run_cpu(CpuClass::Data, reservation, move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(ClientBatchError::Busy)?;
                RuntimeRecordBatch::from_client_body(
                    &body,
                    &expected,
                    limits.max_rows,
                    decoded_limit,
                )
            })
            .await;
        match decoded {
            Ok(batch) => batch,
            Err(error) => {
                let failure = match error.current_context() {
                    ExecutionError::QueueFull { .. } | ExecutionError::PoolClosed { .. } => {
                        ClientBatchError::Busy
                    }
                    // Arrow decoding panics only on input it failed to reject, so the batch is
                    // what is wrong, and retrying it would panic again.
                    ExecutionError::JobPanicked { .. } => ClientBatchError::Malformed {
                        reason: "the stream could not be decoded".to_string(),
                    },
                };
                Err(error.change_context(failure))
            }
        }
    }
}

impl RuntimeRecordBatch {
    /// Checks a client body's framing, then decodes its one record batch against `expected`.
    fn from_client_body(
        body: &[u8],
        expected: &ArrowSchema,
        max_rows: NonZeroUsize,
        decoded_limit: u64,
    ) -> Result<Self, Report<ClientBatchError>> {
        let scanned = IpcMessages::new(body).scan(max_rows)?;
        let mut reader =
            StreamReader::try_new(Cursor::new(body), None).map_err(ClientBatchError::malformed)?;
        let schema = reader.schema();
        if let Some(difference) = ClientSchemaDifference::between(expected, &schema) {
            return Err(Report::new(ClientBatchError::SchemaMismatch { difference }));
        }
        if scanned.record_batches != 1 {
            return Err(Report::new(ClientBatchError::NotOneBatch {
                batches: scanned.record_batches,
            }));
        }
        let Some(batch) = reader.next() else {
            return Err(Report::new(ClientBatchError::NotOneBatch { batches: 0 }));
        };
        let batch = batch.map_err(ClientBatchError::invalid_data)?;
        let decoded = batch_payload_bytes(&batch);
        if decoded > decoded_limit {
            return Err(Report::new(ClientBatchError::TooLarge {
                size: decoded,
                limit: decoded_limit,
            }));
        }
        Ok(Self { batch })
    }
}

#[cfg(test)]
#[path = "client_batch_tests.rs"]
mod tests;
