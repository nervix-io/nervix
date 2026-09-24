pub(in crate::runtime) type RelayDispatchResult = Result<(), Box<RelayRecordBatch>>;

use std::sync::Arc as StdArc;

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::Timestamp;
use triomphe::Arc;

use super::BranchKey;
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{
        CompiledSchema, RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow, RuntimeSchemaError,
    },
};

#[derive(Debug, Clone)]
pub(crate) struct RelayMessage {
    pub(crate) key: Option<BranchKey>,
    pub(crate) record: RuntimeRow,
    pub(crate) acks: AckSet,
}

#[derive(Debug, Clone)]
pub(crate) struct RelayRecordBatch {
    pub(super) key: Option<BranchKey>,
    pub(super) keys: Vec<Option<BranchKey>>,
    pub(super) batch: Arc<RuntimeRecordBatch>,
    pub(super) metadata: Vec<RuntimeRecordMetadata>,
    pub(super) acks: Vec<AckSet>,
}

pub(super) struct RelayDeliveryObservation {
    pub(super) domain_timestamp: Option<Timestamp>,
    pub(super) latency_seconds: Vec<f64>,
}

#[derive(Debug, Clone, Copy, strum::Display)]
pub(crate) enum RelayRecordBatchOperation {
    #[strum(serialize = "address a relay batch row")]
    AddressRow,
    #[strum(serialize = "build a relay batch from messages")]
    BuildFromMessages,
    #[strum(serialize = "validate a runtime batch against its relay schema")]
    ValidateRuntimeBatch,
    #[strum(serialize = "select relay batch rows")]
    SelectRows,
    #[strum(serialize = "reorder relay batch rows")]
    ReorderRows,
    #[strum(serialize = "materialize relay messages")]
    MaterializeMessages,
    #[strum(serialize = "build a relay batch while preserving acknowledgements")]
    BuildPreservingAcks,
    #[strum(serialize = "concatenate relay batches")]
    Concatenate,
}

#[derive(Debug, Clone, Copy, strum::Display)]
pub(crate) enum RelayRecordBatchSidecar {
    #[strum(serialize = "metadata")]
    Metadata,
    #[strum(serialize = "branch key")]
    BranchKey,
    #[strum(serialize = "ACK")]
    Ack,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RelayRecordBatchError {
    #[error("relay batch row {row} is outside metadata with {metadata_rows} rows")]
    MetadataRowOutOfBounds { row: usize, metadata_rows: usize },
    #[error("a relay batch must contain at least one message")]
    EmptyMessages,
    #[error("a relay batch cannot mix branch keys")]
    MixedBranchKeys,
    #[error(
        "relay batch {sidecar} sidecar has {found} rows for an Arrow batch with {expected} rows"
    )]
    SidecarRowCount {
        sidecar: RelayRecordBatchSidecar,
        expected: usize,
        found: usize,
    },
    #[error("failed to {operation}")]
    RuntimeSchema {
        operation: RelayRecordBatchOperation,
    },
    #[error("relay batch row {row} is outside a batch with {batch_rows} rows")]
    SelectedRowOutOfBounds { row: usize, batch_rows: usize },
    #[error(
        "relay batch selected rows are not strictly increasing at {previous} followed by {next}"
    )]
    SelectedRowsNotIncreasing { previous: usize, next: usize },
    #[error(
        "cannot reorder relay batch with {arrow_rows} Arrow rows, {metadata_rows} metadata rows, \
         {branch_keys} branch keys, and {ack_sets} ACK sets"
    )]
    SidecarCount {
        arrow_rows: usize,
        metadata_rows: usize,
        branch_keys: usize,
        ack_sets: usize,
    },
    #[error("relay batch reorder has {order_rows} rows for a {batch_rows}-row batch")]
    ReorderRowCount {
        order_rows: usize,
        batch_rows: usize,
    },
    #[error("relay batch reorder row {row} is outside {batch_rows} rows")]
    ReorderRowOutOfBounds { row: usize, batch_rows: usize },
    #[error("relay batch reorder contains row {row} more than once")]
    DuplicateReorderRow { row: usize },
}

#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub(super) struct RelayRecordBatchReorderFailure {
    pub(super) error: Report<RelayRecordBatchError>,
    pub(super) batch: RelayRecordBatch,
}

#[derive(Debug)]
pub(crate) struct RelayRecordBatchFailure<T> {
    pub(crate) error: Report<RelayRecordBatchError>,
    pub(crate) preserved: T,
}

impl<T> RelayRecordBatchFailure<T> {
    fn new(error: Report<RelayRecordBatchError>, preserved: T) -> Self {
        Self { error, preserved }
    }
}

/// A relay batch taken apart into the Arrow batch and the per-row sidecars that travel with it.
/// The branch keys come along so a caller that concatenates batches keeps every row's key, which
/// is why this is not simply the batch plus metadata.
pub(super) struct UnkeyedRelayBatchParts {
    pub(super) batch: Arc<RuntimeRecordBatch>,
    pub(super) metadata: Vec<RuntimeRecordMetadata>,
    pub(super) keys: Vec<Option<BranchKey>>,
    pub(super) acks: Vec<AckSet>,
}

impl RelayRecordBatch {
    /// The Arrow rows the batch carries.
    pub(crate) fn record_batch(&self) -> &arrow_array::RecordBatch {
        self.batch.batch()
    }

    /// The concrete branch of every row, in row order.
    pub(crate) fn branch_keys(&self) -> &[Option<BranchKey>] {
        &self.keys
    }

    /// Addresses one row of the batch with its metadata, without copying its values.
    pub(crate) fn runtime_row(
        &self,
        row: usize,
    ) -> error_stack::Result<RuntimeRow, RelayRecordBatchError> {
        if self.metadata.get(row).is_none() {
            return Err(Report::new(RelayRecordBatchError::MetadataRowOutOfBounds {
                row,
                metadata_rows: self.metadata.len(),
            }));
        }
        RuntimeRow::new(self.batch.clone(), row, self.metadata[row].clone()).change_context(
            RelayRecordBatchError::RuntimeSchema {
                operation: RelayRecordBatchOperation::AddressRow,
            },
        )
    }

    pub(super) fn single(
        schema: Arc<CompiledSchema>,
        key: Option<BranchKey>,
        record: RuntimeRow,
        acks: AckSet,
    ) -> error_stack::Result<Self, RelayRecordBatchError> {
        Self::from_messages(schema, vec![RelayMessage { key, record, acks }])
    }

    pub(super) fn from_messages(
        schema: Arc<CompiledSchema>,
        messages: Vec<RelayMessage>,
    ) -> error_stack::Result<Self, RelayRecordBatchError> {
        let Some(first) = messages.first() else {
            return Err(Report::new(RelayRecordBatchError::EmptyMessages));
        };
        let key = first.key.clone();
        if messages.iter().any(|message| message.key != key) {
            return Err(Report::new(RelayRecordBatchError::MixedBranchKeys));
        }
        let keys = vec![key.clone(); messages.len()];
        let metadata = messages
            .iter()
            .map(|message| message.record.metadata().clone())
            .collect::<Vec<_>>();
        let (records, acks): (Vec<_>, Vec<_>) = messages
            .into_iter()
            .map(|message| (message.record, message.acks))
            .unzip();
        let batch = RuntimeRecordBatch::shared_from_rows(schema.arrow_schema(), &records)
            .change_context(RelayRecordBatchError::RuntimeSchema {
                operation: RelayRecordBatchOperation::BuildFromMessages,
            })?;
        Ok(Self {
            key,
            keys,
            batch,
            metadata,
            acks,
        })
    }

    pub(super) fn from_runtime_batch(
        schema: Arc<CompiledSchema>,
        key: Option<BranchKey>,
        batch: RuntimeRecordBatch,
        metadata: Vec<RuntimeRecordMetadata>,
        acks: Vec<AckSet>,
    ) -> error_stack::Result<Self, RelayRecordBatchError> {
        let row_count = batch.batch().num_rows();
        if row_count != acks.len() {
            return Err(Report::new(RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Ack,
                expected: row_count,
                found: acks.len(),
            }));
        }
        if row_count != metadata.len() {
            return Err(Report::new(RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Metadata,
                expected: row_count,
                found: metadata.len(),
            }));
        }
        if batch.schema().as_ref() != schema.arrow_schema().as_ref() {
            return Err(Report::new(RuntimeSchemaError::SchemaMismatch {
                expected: schema.arrow_schema(),
                found: batch.schema(),
            })
            .change_context(RelayRecordBatchError::RuntimeSchema {
                operation: RelayRecordBatchOperation::ValidateRuntimeBatch,
            }));
        }
        let keys = vec![key.clone(); row_count];
        Ok(Self {
            key,
            keys,
            batch: Arc::new(batch),
            metadata,
            acks,
        })
    }

    pub(super) fn from_filtered_parts(
        key: Option<BranchKey>,
        batch: RuntimeRecordBatch,
        metadata: Vec<RuntimeRecordMetadata>,
        acks: Vec<AckSet>,
    ) -> error_stack::Result<Self, RelayRecordBatchError> {
        let row_count = batch.batch().num_rows();
        if metadata.len() != row_count {
            return Err(Report::new(RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Metadata,
                expected: row_count,
                found: metadata.len(),
            }));
        }
        if acks.len() != row_count {
            return Err(Report::new(RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Ack,
                expected: row_count,
                found: acks.len(),
            }));
        }
        let keys = vec![key.clone(); row_count];
        Ok(Self {
            key,
            keys,
            batch: Arc::new(batch),
            metadata,
            acks,
        })
    }

    pub(super) fn take(self, rows: &[usize]) -> Result<Self, RelayRecordBatchFailure<Vec<AckSet>>> {
        let row_count = self.batch.batch().num_rows();
        if self.metadata.len() != row_count
            || self.keys.len() != row_count
            || self.acks.len() != row_count
        {
            return Err(RelayRecordBatchFailure::new(
                Report::new(RelayRecordBatchError::SidecarCount {
                    arrow_rows: row_count,
                    metadata_rows: self.metadata.len(),
                    branch_keys: self.keys.len(),
                    ack_sets: self.acks.len(),
                }),
                self.acks,
            ));
        }
        if rows.len() == row_count && rows.iter().copied().eq(0..row_count) {
            return Ok(self);
        }
        if let Some(row) = rows.iter().find(|row| **row >= row_count) {
            return Err(RelayRecordBatchFailure::new(
                Report::new(RelayRecordBatchError::SelectedRowOutOfBounds {
                    row: *row,
                    batch_rows: row_count,
                }),
                self.acks,
            ));
        }
        if let Some(pair) = rows.windows(2).find(|pair| pair[0] >= pair[1]) {
            return Err(RelayRecordBatchFailure::new(
                Report::new(RelayRecordBatchError::SelectedRowsNotIncreasing {
                    previous: pair[0],
                    next: pair[1],
                }),
                self.acks,
            ));
        }
        let Self {
            key,
            keys,
            batch,
            metadata,
            acks,
        } = self;
        let batch = match batch.take(rows) {
            Ok(batch) => batch,
            Err(error) => {
                return Err(RelayRecordBatchFailure::new(
                    error.change_context(RelayRecordBatchError::RuntimeSchema {
                        operation: RelayRecordBatchOperation::SelectRows,
                    }),
                    acks,
                ));
            }
        };
        fn select<T>(values: Vec<T>, rows: &[usize]) -> Vec<T> {
            let mut selected = Vec::with_capacity(rows.len());
            let mut rows = rows.iter().copied();
            let mut next = rows.next();
            for (row, value) in values.into_iter().enumerate() {
                if next == Some(row) {
                    selected.push(value);
                    next = rows.next();
                }
            }
            debug_assert!(
                next.is_none(),
                "selected rows were validated against the batch"
            );
            selected
        }
        Ok(Self {
            key,
            keys: select(keys, rows),
            batch: Arc::new(batch),
            metadata: select(metadata, rows),
            acks: select(acks, rows),
        })
    }

    pub(super) fn into_unkeyed_parts(self) -> UnkeyedRelayBatchParts {
        UnkeyedRelayBatchParts {
            batch: self.batch,
            metadata: self.metadata,
            keys: self.keys,
            acks: self.acks,
        }
    }

    pub(super) fn into_reordered(
        self,
        row_order: &[usize],
    ) -> Result<Self, Box<RelayRecordBatchReorderFailure>> {
        let row_count = self.batch.batch().num_rows();
        if self.metadata.len() != row_count
            || self.keys.len() != row_count
            || self.acks.len() != row_count
        {
            return Err(Box::new(RelayRecordBatchReorderFailure {
                error: Report::new(RelayRecordBatchError::SidecarCount {
                    arrow_rows: row_count,
                    metadata_rows: self.metadata.len(),
                    branch_keys: self.keys.len(),
                    ack_sets: self.acks.len(),
                }),
                batch: self,
            }));
        }
        if row_order.len() != row_count {
            return Err(Box::new(RelayRecordBatchReorderFailure {
                error: Report::new(RelayRecordBatchError::ReorderRowCount {
                    order_rows: row_order.len(),
                    batch_rows: row_count,
                }),
                batch: self,
            }));
        }
        let mut seen = vec![false; row_count];
        for &row in row_order {
            let Some(was_seen) = seen.get_mut(row) else {
                return Err(Box::new(RelayRecordBatchReorderFailure {
                    error: Report::new(RelayRecordBatchError::ReorderRowOutOfBounds {
                        row,
                        batch_rows: row_count,
                    }),
                    batch: self,
                }));
            };
            if *was_seen {
                return Err(Box::new(RelayRecordBatchReorderFailure {
                    error: Report::new(RelayRecordBatchError::DuplicateReorderRow { row }),
                    batch: self,
                }));
            }
            *was_seen = true;
        }
        if row_order.iter().copied().eq(0..row_count) {
            return Ok(self);
        }
        let reordered_batch = match self.batch.take(row_order) {
            Ok(batch) => batch,
            Err(error) => {
                return Err(Box::new(RelayRecordBatchReorderFailure {
                    error: error.change_context(RelayRecordBatchError::RuntimeSchema {
                        operation: RelayRecordBatchOperation::ReorderRows,
                    }),
                    batch: self,
                }));
            }
        };
        let Self {
            key,
            keys,
            metadata,
            acks,
            ..
        } = self;
        Ok(Self {
            key,
            keys: reorder_owned_values(keys, row_order),
            batch: Arc::new(reordered_batch),
            metadata: reorder_owned_values(metadata, row_order),
            acks: reorder_owned_values(acks, row_order),
        })
    }

    pub(crate) fn try_into_messages(
        self,
    ) -> Result<Vec<RelayMessage>, Box<RelayRecordBatchFailure<Self>>> {
        let row_count = self.batch.batch().num_rows();
        if row_count != self.acks.len() {
            return Err(Box::new(RelayRecordBatchFailure::new(
                Report::new(RelayRecordBatchError::SidecarRowCount {
                    sidecar: RelayRecordBatchSidecar::Ack,
                    expected: row_count,
                    found: self.acks.len(),
                }),
                self,
            )));
        }
        if row_count != self.metadata.len() {
            return Err(Box::new(RelayRecordBatchFailure::new(
                Report::new(RelayRecordBatchError::SidecarRowCount {
                    sidecar: RelayRecordBatchSidecar::Metadata,
                    expected: row_count,
                    found: self.metadata.len(),
                }),
                self,
            )));
        }
        if row_count != self.keys.len() {
            return Err(Box::new(RelayRecordBatchFailure::new(
                Report::new(RelayRecordBatchError::SidecarRowCount {
                    sidecar: RelayRecordBatchSidecar::BranchKey,
                    expected: row_count,
                    found: self.keys.len(),
                }),
                self,
            )));
        }
        let rows = match (0..row_count)
            .zip(self.metadata.iter().cloned())
            .map(|(row, metadata)| RuntimeRow::new(self.batch.clone(), row, metadata))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(rows) => rows,
            Err(error) => {
                return Err(Box::new(RelayRecordBatchFailure::new(
                    error.change_context(RelayRecordBatchError::RuntimeSchema {
                        operation: RelayRecordBatchOperation::MaterializeMessages,
                    }),
                    self,
                )));
            }
        };
        let Self { keys, acks, .. } = self;
        let mut messages = Vec::with_capacity(row_count);
        for ((record, acks), key) in rows.into_iter().zip(acks).zip(keys) {
            messages.push(RelayMessage { key, record, acks });
        }
        Ok(messages)
    }

    pub(super) fn concat(batches: Vec<Self>) -> error_stack::Result<Self, RelayRecordBatchError> {
        match Self::concat_preserving(batches) {
            Ok(batch) => Ok(batch),
            Err(failure) => Err(failure.error),
        }
    }

    pub(super) fn concat_preserving(
        batches: Vec<Self>,
    ) -> Result<Self, Box<RelayRecordBatchFailure<Vec<Self>>>> {
        let Some(first) = batches.first() else {
            let error = Report::new(RuntimeSchemaError::EmptyConcatenation).change_context(
                RelayRecordBatchError::RuntimeSchema {
                    operation: RelayRecordBatchOperation::Concatenate,
                },
            );
            return Err(Box::new(RelayRecordBatchFailure {
                error,
                preserved: batches,
            }));
        };

        let key = first.key.clone();
        if batches.len() == 1 {
            return Ok(batches
                .into_iter()
                .next()
                .verified("the length was just checked to be one"));
        }

        let concatenated = {
            let runtime_batches = batches
                .iter()
                .map(|batch| batch.batch.as_ref())
                .collect::<Vec<_>>();
            match RuntimeRecordBatch::concat(&runtime_batches) {
                Ok(batch) => batch,
                Err(error) => {
                    return Err(Box::new(RelayRecordBatchFailure {
                        error: error.change_context(RelayRecordBatchError::RuntimeSchema {
                            operation: RelayRecordBatchOperation::Concatenate,
                        }),
                        preserved: batches,
                    }));
                }
            }
        };

        let total_metadata = batches
            .iter()
            .map(|batch| batch.metadata.len())
            .sum::<usize>();
        let total_acks = batches.iter().map(|batch| batch.acks.len()).sum::<usize>();
        let total_keys = batches.iter().map(|batch| batch.keys.len()).sum::<usize>();
        let mut metadata = Vec::with_capacity(total_metadata);
        let mut acks = Vec::with_capacity(total_acks);
        let mut keys = Vec::with_capacity(total_keys);
        for batch in batches {
            metadata.extend(batch.metadata);
            acks.extend(batch.acks);
            keys.extend(batch.keys);
        }

        Ok(Self {
            key,
            keys,
            batch: Arc::new(concatenated),
            metadata,
            acks,
        })
    }

    pub(super) fn detached(&self) -> Self {
        Self {
            key: self.key.clone(),
            keys: self.keys.clone(),
            batch: self.batch.clone(),
            metadata: self.metadata.clone(),
            acks: vec![AckSet::empty(); self.acks.len()],
        }
    }

    pub(super) fn attached(&self) -> Self {
        Self {
            key: self.key.clone(),
            keys: self.keys.clone(),
            batch: self.batch.clone(),
            metadata: self.metadata.clone(),
            acks: self.acks.iter().map(AckSet::attached).collect::<Vec<_>>(),
        }
    }

    pub(super) fn attached_for_receivers(&self, receivers: usize) -> Self {
        Self {
            key: self.key.clone(),
            keys: self.keys.clone(),
            batch: self.batch.clone(),
            metadata: self.metadata.clone(),
            acks: self
                .acks
                .iter()
                .map(|acks| acks.attached_for_receivers(receivers))
                .collect::<Vec<_>>(),
        }
    }

    pub(super) fn into_attached_fanout(self, output_count: usize) -> Vec<Self> {
        if output_count == 0 {
            self.ack_success();
            return Vec::new();
        }
        let mut batches = Vec::with_capacity(output_count);
        batches.push(self);
        for _ in 1..output_count {
            let attached = batches[0].attached();
            batches.push(attached);
        }
        batches
    }

    pub(super) fn message_count(&self) -> u64 {
        self.batch.batch().num_rows().arch_into()
    }

    pub(super) fn arrow_schema(&self) -> StdArc<arrow_schema::Schema> {
        self.batch.schema()
    }

    pub(super) fn estimated_bytes(&self) -> u64 {
        self.batch.estimated_bytes()
    }

    pub(super) fn ack_success(&self) {
        for ack in &self.acks {
            ack.ack_success();
        }
    }

    pub(super) fn merged_acks(&self) -> AckSet {
        AckSet::merged(self.acks.iter().cloned())
    }

    pub(super) fn delivery_observation(&self, now: Timestamp) -> RelayDeliveryObservation {
        delivery_observation_from_timestamps(
            now,
            self.metadata
                .iter()
                .map(RuntimeRecordMetadata::ingested_at_high_watermark),
        )
    }

    pub(super) fn domain_timestamp(&self) -> Option<Timestamp> {
        self.metadata
            .iter()
            .map(|metadata| metadata.ingested_at_high_watermark())
            .max()
    }
}

fn reorder_owned_values<T>(values: Vec<T>, row_order: &[usize]) -> Vec<T> {
    let mut values = values.into_iter().map(Some).collect::<Vec<_>>();
    row_order
        .iter()
        .map(|row| {
            values[*row]
                .take()
                .verified("the row order is a permutation, so each row is taken exactly once")
        })
        .collect()
}

fn delivery_observation_from_timestamps(
    now: Timestamp,
    timestamps: impl Iterator<Item = Timestamp>,
) -> RelayDeliveryObservation {
    let mut domain_timestamp: Option<Timestamp> = None;
    let mut latency_seconds = Vec::with_capacity(timestamps.size_hint().0);
    for timestamp in timestamps {
        domain_timestamp = Some(match domain_timestamp {
            Some(current) => current.max(timestamp),
            None => timestamp,
        });
        if let Ok(duration) = now
            .into_datetime()
            .signed_duration_since(timestamp.into_datetime())
            .to_std()
        {
            latency_seconds.push(duration.as_secs_f64());
        }
    }
    RelayDeliveryObservation {
        domain_timestamp,
        latency_seconds,
    }
}

pub(super) fn build_stream_record_batch_preserving_acks(
    schema: Arc<CompiledSchema>,
    messages: Vec<RelayMessage>,
) -> Result<RelayRecordBatch, RelayRecordBatchFailure<Vec<AckSet>>> {
    let Some(first) = messages.first() else {
        return Err(RelayRecordBatchFailure::new(
            Report::new(RelayRecordBatchError::EmptyMessages),
            Vec::new(),
        ));
    };
    let key = first.key.clone();
    let mut records = Vec::with_capacity(messages.len());
    let mut metadata = Vec::with_capacity(messages.len());
    let mut acks = Vec::with_capacity(messages.len());
    for message in messages {
        let RelayMessage {
            key: message_key,
            record,
            acks: message_acks,
        } = message;
        if message_key != key {
            let mut pending_acks = acks;
            pending_acks.push(message_acks);
            return Err(RelayRecordBatchFailure::new(
                Report::new(RelayRecordBatchError::MixedBranchKeys),
                pending_acks,
            ));
        }
        metadata.push(record.metadata().clone());
        records.push(record);
        acks.push(message_acks);
    }
    let batch = match RuntimeRecordBatch::shared_from_rows(schema.arrow_schema(), &records) {
        Ok(batch) => batch,
        Err(error) => {
            return Err(RelayRecordBatchFailure::new(
                error.change_context(RelayRecordBatchError::RuntimeSchema {
                    operation: RelayRecordBatchOperation::BuildPreservingAcks,
                }),
                acks,
            ));
        }
    };
    let keys = vec![key.clone(); records.len()];
    Ok(RelayRecordBatch {
        key,
        keys,
        batch,
        metadata,
        acks,
    })
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, sync::Arc as StdArc};

    use meticulous::ResultExt as _;
    use nervix_models::{
        CreateSchema, FieldName, ModelName, ParseAsType, SchemaField, SchemaName, Timestamp,
    };
    use triomphe::Arc;

    use super::{
        RelayMessage, RelayRecordBatch, RelayRecordBatchError, RelayRecordBatchOperation,
        RelayRecordBatchSidecar, build_stream_record_batch_preserving_acks,
        delivery_observation_from_timestamps,
    };
    use crate::{
        runtime::test_fixtures::string_branch_key,
        runtime_ack::AckSet,
        runtime_schema::{
            CompiledSchema, RuntimeRecordMetadata, RuntimeRow, RuntimeValue, compile_schema,
        },
    };

    fn test_schema() -> Arc<CompiledSchema> {
        schema_with_field("relay_batch_test", "value")
    }

    fn schema_with_field(schema: &str, field: &str) -> Arc<CompiledSchema> {
        Arc::new(compile_schema(&CreateSchema {
            name: SchemaName::from(&ModelName::parse(schema).expect("valid schema name")),
            fields: vec![SchemaField {
                name: FieldName::parse(field).expect("valid field name"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }))
    }

    fn test_rows(schema: &Arc<CompiledSchema>, values: &[i64]) -> Vec<RuntimeRow> {
        let mut builder = schema.batch_builder(values.len());
        for value in values {
            builder
                .append(Some(&RuntimeValue::I64(*value)))
                .expect("value must match schema");
            builder.finish_row().expect("row must be complete");
        }
        let batch = Arc::new(builder.finish().expect("batch must build"));
        values
            .iter()
            .enumerate()
            .map(|(row, _)| {
                RuntimeRow::new(
                    batch.clone(),
                    row,
                    RuntimeRecordMetadata::from_ingested_at_watermarks(
                        Timestamp::from_unix_nanos(
                            i64::try_from(row)
                                .assured("the test fixture allocates fewer than i64::MAX rows"),
                        ),
                        Timestamp::from_unix_nanos(
                            i64::try_from(row)
                                .assured("the test fixture allocates fewer than i64::MAX rows"),
                        ),
                    ),
                )
                .expect("row must exist")
            })
            .collect()
    }

    fn test_batch(schema: &Arc<CompiledSchema>, values: &[i64]) -> RelayRecordBatch {
        let messages = test_rows(schema, values)
            .into_iter()
            .map(|record| RelayMessage {
                key: None,
                record,
                acks: AckSet::empty(),
            })
            .collect();
        RelayRecordBatch::from_messages(schema.clone(), messages)
            .expect("relay batch fixture must be valid")
    }

    #[test]
    fn runtime_rows_share_the_relay_batch_allocation() {
        let schema = test_schema();
        let messages = test_rows(&schema, &[10, 20])
            .into_iter()
            .map(|record| RelayMessage {
                key: None,
                record,
                acks: AckSet::empty(),
            })
            .collect();
        let batch = RelayRecordBatch::from_messages(schema, messages)
            .expect("relay batch must build from shared rows");

        let first = batch.runtime_row(0).expect("first row should exist");
        let second = batch.runtime_row(1).expect("second row should exist");

        assert!(
            Arc::ptr_eq(first.batch(), second.batch()),
            "row views from one relay batch must retain the same batch allocation"
        );

        let missing = batch
            .runtime_row(2)
            .expect_err("a row view cannot address metadata beyond the batch");
        assert!(matches!(
            missing.current_context(),
            RelayRecordBatchError::MetadataRowOutOfBounds {
                row: 2,
                metadata_rows: 2,
            }
        ));
    }

    #[test]
    fn message_batching_reuses_an_identity_arrow_batch() {
        let schema = test_schema();
        let rows = test_rows(&schema, &[10, 20, 30]);
        let input_column = rows[0].batch().batch().column(0).clone();
        let messages = rows
            .into_iter()
            .map(|record| RelayMessage {
                key: None,
                record,
                acks: AckSet::empty(),
            })
            .collect();

        let batch = RelayRecordBatch::from_messages(schema, messages)
            .expect("relay batch must build from shared rows");

        assert!(StdArc::ptr_eq(&input_column, batch.batch.batch().column(0)));
        assert_eq!(
            batch.batch.value(0, "value").expect("readable value"),
            Some(RuntimeValue::I64(10))
        );
        assert_eq!(
            batch.batch.value(2, "value").expect("readable value"),
            Some(RuntimeValue::I64(30))
        );
    }

    #[test]
    fn relay_batch_take_has_identity_and_sparse_paths() {
        let schema = test_schema();
        let rows = test_rows(&schema, &[10, 20, 30]);
        let messages = rows
            .into_iter()
            .map(|record| RelayMessage {
                key: None,
                record,
                acks: AckSet::empty(),
            })
            .collect();
        let batch = RelayRecordBatch::from_messages(schema, messages)
            .expect("relay batch must build from shared rows");
        let input_column = batch.batch.batch().column(0).clone();

        let identity = batch.clone().take(&[0, 1, 2]).expect("identity take");
        assert!(StdArc::ptr_eq(
            &input_column,
            identity.batch.batch().column(0)
        ));

        let sparse = batch.take(&[0, 2]).expect("sparse take");
        assert_eq!(sparse.message_count(), 2);
        assert_eq!(
            sparse.batch.value(0, "value").expect("readable value"),
            Some(RuntimeValue::I64(10))
        );
        assert_eq!(
            sparse.batch.value(1, "value").expect("readable value"),
            Some(RuntimeValue::I64(30))
        );
    }

    #[test]
    fn relay_batch_construction_reports_typed_structural_errors() {
        let schema = test_schema();
        let empty = RelayRecordBatch::from_messages(schema.clone(), Vec::new())
            .expect_err("an empty relay batch must be rejected");
        assert!(matches!(
            empty.current_context(),
            RelayRecordBatchError::EmptyMessages
        ));

        let mut rows = test_rows(&schema, &[10, 20]).into_iter();
        let mixed = RelayRecordBatch::from_messages(
            schema.clone(),
            vec![
                RelayMessage {
                    key: None,
                    record: rows.next().expect("first fixture row"),
                    acks: AckSet::empty(),
                },
                RelayMessage {
                    key: string_branch_key("tenant", "acme"),
                    record: rows.next().expect("second fixture row"),
                    acks: AckSet::empty(),
                },
            ],
        )
        .expect_err("mixed branch keys must be rejected");
        assert!(matches!(
            mixed.current_context(),
            RelayRecordBatchError::MixedBranchKeys
        ));

        let valid = test_batch(&schema, &[10]);
        let missing_acks = RelayRecordBatch::from_runtime_batch(
            schema.clone(),
            None,
            valid.batch.as_ref().clone(),
            valid.metadata.clone(),
            Vec::new(),
        )
        .expect_err("one Arrow row requires one ACK sidecar");
        assert!(matches!(
            missing_acks.current_context(),
            RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Ack,
                expected: 1,
                found: 0,
            }
        ));

        let missing_metadata = RelayRecordBatch::from_runtime_batch(
            schema.clone(),
            None,
            valid.batch.as_ref().clone(),
            Vec::new(),
            vec![AckSet::empty()],
        )
        .expect_err("one Arrow row requires one metadata sidecar");
        assert!(matches!(
            missing_metadata.current_context(),
            RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Metadata,
                expected: 1,
                found: 0,
            }
        ));

        let other_schema = schema_with_field("other_relay_batch", "other_value");
        let wrong_schema = RelayRecordBatch::from_runtime_batch(
            other_schema,
            None,
            valid.batch.as_ref().clone(),
            valid.metadata.clone(),
            valid.acks.clone(),
        )
        .expect_err("the runtime batch must match the compiled relay schema");
        assert!(matches!(
            wrong_schema.current_context(),
            RelayRecordBatchError::RuntimeSchema {
                operation: RelayRecordBatchOperation::ValidateRuntimeBatch,
            }
        ));
        assert!(wrong_schema.contains::<crate::runtime_schema::RuntimeSchemaError>());
    }

    #[test]
    fn filtered_relay_batch_requires_row_aligned_sidecars() {
        let schema = test_schema();
        let valid = test_batch(&schema, &[10]);

        let missing_metadata = RelayRecordBatch::from_filtered_parts(
            None,
            valid.batch.as_ref().clone(),
            Vec::new(),
            vec![AckSet::empty()],
        )
        .expect_err("filtered metadata must remain row aligned");
        assert!(matches!(
            missing_metadata.current_context(),
            RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Metadata,
                expected: 1,
                found: 0,
            }
        ));

        let missing_acks = RelayRecordBatch::from_filtered_parts(
            None,
            valid.batch.as_ref().clone(),
            valid.metadata.clone(),
            Vec::new(),
        )
        .expect_err("filtered ACKs must remain row aligned");
        assert!(matches!(
            missing_acks.current_context(),
            RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Ack,
                expected: 1,
                found: 0,
            }
        ));
    }

    #[test]
    fn relay_batch_take_preserves_acks_for_every_rejected_selection() {
        let schema = test_schema();

        let mut malformed = test_batch(&schema, &[10, 20]);
        malformed.keys.pop();
        let malformed_failure = malformed
            .take(&[0])
            .expect_err("misaligned sidecars must be rejected before selection");
        assert_eq!(malformed_failure.preserved.len(), 2);
        assert!(matches!(
            malformed_failure.error.current_context(),
            RelayRecordBatchError::SidecarCount {
                arrow_rows: 2,
                metadata_rows: 2,
                branch_keys: 1,
                ack_sets: 2,
            }
        ));

        let out_of_bounds = test_batch(&schema, &[10, 20])
            .take(&[2])
            .expect_err("selection indices must address an existing row");
        assert_eq!(out_of_bounds.preserved.len(), 2);
        assert!(matches!(
            out_of_bounds.error.current_context(),
            RelayRecordBatchError::SelectedRowOutOfBounds {
                row: 2,
                batch_rows: 2,
            }
        ));

        let unordered = test_batch(&schema, &[10, 20])
            .take(&[1, 0])
            .expect_err("selected rows must be strictly increasing");
        assert_eq!(unordered.preserved.len(), 2);
        assert!(matches!(
            unordered.error.current_context(),
            RelayRecordBatchError::SelectedRowsNotIncreasing {
                previous: 1,
                next: 0,
            }
        ));
    }

    #[test]
    fn relay_batch_reordering_reports_indices_and_returns_the_batch() {
        let schema = test_schema();

        let mut malformed = test_batch(&schema, &[10, 20]);
        malformed.metadata.pop();
        let malformed_failure = malformed
            .into_reordered(&[1, 0])
            .expect_err("reordering requires aligned sidecars");
        assert_eq!(malformed_failure.batch.message_count(), 2);
        assert!(matches!(
            malformed_failure.error.current_context(),
            RelayRecordBatchError::SidecarCount {
                arrow_rows: 2,
                metadata_rows: 1,
                branch_keys: 2,
                ack_sets: 2,
            }
        ));

        let wrong_length = test_batch(&schema, &[10, 20])
            .into_reordered(&[0])
            .expect_err("a reorder must name every row");
        assert_eq!(wrong_length.batch.message_count(), 2);
        assert!(matches!(
            wrong_length.error.current_context(),
            RelayRecordBatchError::ReorderRowCount {
                order_rows: 1,
                batch_rows: 2,
            }
        ));

        let out_of_bounds = test_batch(&schema, &[10, 20])
            .into_reordered(&[0, 2])
            .expect_err("a reorder index must address an existing row");
        assert_eq!(out_of_bounds.batch.message_count(), 2);
        assert!(matches!(
            out_of_bounds.error.current_context(),
            RelayRecordBatchError::ReorderRowOutOfBounds {
                row: 2,
                batch_rows: 2,
            }
        ));

        let duplicate = test_batch(&schema, &[10, 20])
            .into_reordered(&[0, 0])
            .expect_err("a reorder must be a permutation");
        assert_eq!(duplicate.batch.message_count(), 2);
        assert!(matches!(
            duplicate.error.current_context(),
            RelayRecordBatchError::DuplicateReorderRow { row: 0 }
        ));

        let reordered = test_batch(&schema, &[10, 20])
            .into_reordered(&[1, 0])
            .expect("a valid permutation must reorder the batch");
        assert_eq!(
            reordered.batch.value(0, "value").expect("readable value"),
            Some(RuntimeValue::I64(20))
        );
        assert_eq!(
            reordered.batch.value(1, "value").expect("readable value"),
            Some(RuntimeValue::I64(10))
        );
    }

    #[test]
    fn relay_batch_message_materialization_returns_misaligned_batches() {
        let schema = test_schema();

        let mut missing_acks = test_batch(&schema, &[10]);
        missing_acks.acks.clear();
        let ack_failure = missing_acks
            .try_into_messages()
            .expect_err("message materialization requires one ACK set per row");
        assert_eq!(ack_failure.preserved.message_count(), 1);
        assert!(matches!(
            ack_failure.error.current_context(),
            RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Ack,
                expected: 1,
                found: 0,
            }
        ));

        let mut missing_metadata = test_batch(&schema, &[10]);
        missing_metadata.metadata.clear();
        let metadata_failure = missing_metadata
            .try_into_messages()
            .expect_err("message materialization requires one metadata row per Arrow row");
        assert_eq!(metadata_failure.preserved.message_count(), 1);
        assert!(matches!(
            metadata_failure.error.current_context(),
            RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::Metadata,
                expected: 1,
                found: 0,
            }
        ));

        let mut missing_keys = test_batch(&schema, &[10]);
        missing_keys.keys.clear();
        let key_failure = missing_keys
            .try_into_messages()
            .expect_err("message materialization requires one branch key per row");
        assert_eq!(key_failure.preserved.message_count(), 1);
        assert!(matches!(
            key_failure.error.current_context(),
            RelayRecordBatchError::SidecarRowCount {
                sidecar: RelayRecordBatchSidecar::BranchKey,
                expected: 1,
                found: 0,
            }
        ));
    }

    #[test]
    fn relay_batch_concatenation_preserves_every_rejected_batch() {
        let empty = RelayRecordBatch::concat(Vec::new())
            .expect_err("concatenating no relay batches must fail");
        assert!(matches!(
            empty.current_context(),
            RelayRecordBatchError::RuntimeSchema {
                operation: RelayRecordBatchOperation::Concatenate,
            }
        ));
        assert!(empty.contains::<crate::runtime_schema::RuntimeSchemaError>());

        let first_schema = test_schema();
        let second_schema = schema_with_field("other_relay_batch", "other_value");
        let batches = vec![
            test_batch(&first_schema, &[10]),
            test_batch(&second_schema, &[20]),
        ];
        let mismatch = RelayRecordBatch::concat_preserving(batches)
            .expect_err("relay batches with different Arrow schemas cannot concatenate");
        assert_eq!(mismatch.preserved.len(), 2);
        assert!(matches!(
            mismatch.error.current_context(),
            RelayRecordBatchError::RuntimeSchema {
                operation: RelayRecordBatchOperation::Concatenate,
            }
        ));
        assert!(
            mismatch
                .error
                .contains::<crate::runtime_schema::RuntimeSchemaError>()
        );
    }

    #[test]
    fn preserving_batch_builder_returns_every_pending_ack_set() {
        let schema = test_schema();
        let empty = build_stream_record_batch_preserving_acks(schema.clone(), Vec::new())
            .expect_err("building from no messages must fail");
        assert!(empty.preserved.is_empty());
        assert!(matches!(
            empty.error.current_context(),
            RelayRecordBatchError::EmptyMessages
        ));

        let mut rows = test_rows(&schema, &[10, 20]).into_iter();
        let mixed = build_stream_record_batch_preserving_acks(
            schema.clone(),
            vec![
                RelayMessage {
                    key: None,
                    record: rows.next().expect("first fixture row"),
                    acks: AckSet::empty(),
                },
                RelayMessage {
                    key: string_branch_key("tenant", "acme"),
                    record: rows.next().expect("second fixture row"),
                    acks: AckSet::empty(),
                },
            ],
        )
        .expect_err("mixed branch keys must return the ACKs collected so far");
        assert_eq!(mixed.preserved.len(), 2);
        assert!(matches!(
            mixed.error.current_context(),
            RelayRecordBatchError::MixedBranchKeys
        ));

        let other_schema = schema_with_field("other_relay_batch", "other_value");
        let wrong_schema = build_stream_record_batch_preserving_acks(
            other_schema,
            test_rows(&schema, &[10])
                .into_iter()
                .map(|record| RelayMessage {
                    key: None,
                    record,
                    acks: AckSet::empty(),
                })
                .collect(),
        )
        .expect_err("schema construction failure must preserve the ACK set");
        assert_eq!(wrong_schema.preserved.len(), 1);
        assert!(matches!(
            wrong_schema.error.current_context(),
            RelayRecordBatchError::RuntimeSchema {
                operation: RelayRecordBatchOperation::BuildPreservingAcks,
            }
        ));
        assert!(
            wrong_schema
                .error
                .contains::<crate::runtime_schema::RuntimeSchemaError>()
        );
    }

    #[test]
    fn delivery_observation_visits_each_timestamp_once() {
        let visited = Cell::new(0);
        let timestamps = [
            Timestamp::from_unix_nanos(2_000_000_000),
            Timestamp::from_unix_nanos(4_000_000_000),
            Timestamp::from_unix_nanos(1_000_000_000),
            Timestamp::from_unix_nanos(3_000_000_000),
        ];

        let observation = delivery_observation_from_timestamps(
            Timestamp::from_unix_nanos(5_000_000_000),
            timestamps
                .into_iter()
                .inspect(|_| visited.set(visited.get() + 1)),
        );

        assert_eq!(visited.get(), timestamps.len());
        assert_eq!(
            observation.domain_timestamp,
            Some(Timestamp::from_unix_nanos(4_000_000_000))
        );
        assert_eq!(observation.latency_seconds, [3.0, 1.0, 4.0, 2.0]);
    }
}
