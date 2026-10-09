//! The one Arrow IPC form a client producer submits a batch in, and the only way to accept one.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** A submitted stream's exact schema, its row and byte limits, the defect each refusal
//!   names, and decoding its one record batch off the async workers under the relay budget.
//! - **Depends on.** The executor that admits and charges the work, the IPC stream scan that checks
//!   the stream before any column is allocated and opens Arrow's reader over it, and the
//!   vocabulary's batch defects.
//! - **Must not know.** Producers, sessions, ingestors, or what happens to the batch afterwards.
//!
//! A client batch is one uncompressed Arrow IPC stream: its schema message, exactly one record
//! batch message, and the end-of-stream marker, every message opened by the continuation marker.
//! The schema is the ingestor's own, field for field: names, order, types and nullability, with
//! no schema or field metadata. Nothing is inferred, cast or reordered.

use std::{
    fmt,
    num::{NonZeroU64, NonZeroUsize},
};

use arch_into::ArchInto as _;
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};
use bytes::Bytes;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_execution::{CpuClass, ExecutionError, Executor, MemoryClass};
use nervix_models::ClientBatchDefect;
use nervix_primitives::sync::StdArc;
use thiserror::Error;

use super::{
    CompiledSchema, RuntimeRecordBatch, batch_payload_bytes,
    ipc_stream::{IpcMessages, IpcStreamError, UnsupportedFieldKind},
};

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

    /// The defect a stream the scan refused has, with what the scan found beneath. A field type
    /// Nervix does not carry is a schema that differs from the ingestor's, a record batch at odds
    /// with the schema its stream declares is invalid data, and anything else the scan refuses is
    /// a stream that is not canonical.
    fn framing(refusal: Report<IpcStreamError>) -> Report<Self> {
        let defect = match refusal.current_context() {
            IpcStreamError::UnexpectedMessage { kind } => Self::UnexpectedMessage { kind },
            IpcStreamError::Compressed => Self::Compressed,
            IpcStreamError::TooManyRows { rows, limit } => Self::TooManyRows {
                rows: *rows,
                limit: *limit,
            },
            IpcStreamError::UnsupportedField { field, kind } => Self::SchemaMismatch {
                difference: ClientSchemaDifference::UnsupportedFieldType {
                    index: *field,
                    kind: *kind,
                },
            },
            other if other.is_of_record_batch_shape() => Self::InvalidData {
                reason: other.to_string(),
            },
            other => Self::Malformed {
                reason: other.to_string(),
            },
        };
        refusal.change_context(defect)
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
    /// The field at `index`, or a field it nests, is of a type no Nervix type is carried as, so no
    /// ingestor's schema declares it.
    UnsupportedFieldType {
        index: usize,
        kind: UnsupportedFieldKind,
    },
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
            Self::UnsupportedFieldType { index, kind } => write!(
                formatter,
                "field {index} declares {kind}, which no Nervix type is carried as"
            ),
        }
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
        let scanned = IpcMessages::new(body)
            .scan(Some(max_rows))
            .map_err(ClientBatchError::framing)?;
        let mut reader = scanned.reader().map_err(ClientBatchError::malformed)?;
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
#[path = "client_batch_properties.rs"]
mod properties;

#[cfg(test)]
#[path = "client_batch_tests.rs"]
mod tests;
