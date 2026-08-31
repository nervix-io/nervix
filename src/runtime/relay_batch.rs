use std::sync::Arc as StdArc;

use nervix_models::Timestamp;
use triomphe::Arc;

use super::BranchKey;
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{CompiledSchema, RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow},
};

#[derive(Debug, Clone)]
pub struct RelayMessage {
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

#[derive(Debug, thiserror::Error)]
pub(super) enum RelayRecordBatchReorderError {
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
    RowCount {
        order_rows: usize,
        batch_rows: usize,
    },
    #[error("relay batch reorder row {row} is outside {batch_rows} rows")]
    RowOutOfBounds { row: usize, batch_rows: usize },
    #[error("relay batch reorder contains row {row} more than once")]
    DuplicateRow { row: usize },
    #[error("Arrow batch reorder failed: {reason}")]
    Arrow { reason: String },
}

#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub(super) struct RelayRecordBatchReorderFailure {
    #[source]
    pub(super) error: RelayRecordBatchReorderError,
    pub(super) batch: RelayRecordBatch,
}

type UnkeyedRelayBatchParts = (
    Arc<RuntimeRecordBatch>,
    Vec<RuntimeRecordMetadata>,
    Vec<Option<BranchKey>>,
    Vec<AckSet>,
);

impl RelayRecordBatch {
    pub(super) fn runtime_row(&self, row: usize) -> Result<RuntimeRow, String> {
        if self.metadata.get(row).is_none() {
            return Err(format!(
                "stream batch row {row} is outside metadata with {} rows",
                self.metadata.len()
            ));
        }
        RuntimeRow::new(self.batch.clone(), row, self.metadata[row].clone())
    }

    pub(super) fn single(
        schema: Arc<CompiledSchema>,
        key: Option<BranchKey>,
        record: RuntimeRow,
        acks: AckSet,
    ) -> Result<Self, String> {
        Self::from_messages(schema, vec![RelayMessage { key, record, acks }])
    }

    pub(super) fn from_messages(
        schema: Arc<CompiledSchema>,
        messages: Vec<RelayMessage>,
    ) -> Result<Self, String> {
        let Some(first) = messages.first() else {
            return Err("stream batch must contain at least one message".to_string());
        };
        let key = first.key.clone();
        if messages.iter().any(|message| message.key != key) {
            return Err("stream batch cannot mix different branch keys".to_string());
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
        if records
            .iter()
            .any(|record| record.batch().schema().as_ref() != schema.arrow_schema().as_ref())
        {
            return Err("stream message row schema does not match relay schema".to_string());
        }
        let batch = RuntimeRecordBatch::shared_from_rows(schema.arrow_schema(), &records)?;
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
    ) -> Result<Self, String> {
        let row_count = batch.batch().num_rows();
        if row_count != acks.len() {
            return Err(format!(
                "stream batch ack count {} does not match row count {}",
                acks.len(),
                row_count
            ));
        }
        if row_count != metadata.len() {
            return Err(format!(
                "stream batch metadata count {} does not match row count {}",
                metadata.len(),
                row_count
            ));
        }
        if batch.schema().as_ref() != schema.arrow_schema().as_ref() {
            return Err("stream batch schema does not match compiled schema".to_string());
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
    ) -> Result<Self, String> {
        let row_count = batch.batch().num_rows();
        if metadata.len() != row_count {
            return Err(format!(
                "filtered metadata count {} does not match row count {}",
                metadata.len(),
                row_count
            ));
        }
        if acks.len() != row_count {
            return Err(format!(
                "filtered ack count {} does not match row count {}",
                acks.len(),
                row_count
            ));
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

    pub(super) fn take(self, rows: &[usize]) -> Result<Self, (String, Vec<AckSet>)> {
        let row_count = self.batch.batch().num_rows();
        if self.metadata.len() != row_count
            || self.keys.len() != row_count
            || self.acks.len() != row_count
        {
            return Err((
                format!(
                    "stream batch sidecar lengths ({}, {}, {}) do not match row count {row_count}",
                    self.metadata.len(),
                    self.keys.len(),
                    self.acks.len()
                ),
                self.acks,
            ));
        }
        if rows.len() == row_count && rows.iter().copied().eq(0..row_count) {
            return Ok(self);
        }
        if let Some(row) = rows.iter().find(|row| **row >= row_count) {
            return Err((
                format!("stream batch row {row} is outside batch with {row_count} rows"),
                self.acks,
            ));
        }
        if rows.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err((
                "stream batch selected rows must be strictly increasing".to_string(),
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
            Err(error) => return Err((error, acks)),
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
        (self.batch, self.metadata, self.keys, self.acks)
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
                error: RelayRecordBatchReorderError::SidecarCount {
                    arrow_rows: row_count,
                    metadata_rows: self.metadata.len(),
                    branch_keys: self.keys.len(),
                    ack_sets: self.acks.len(),
                },
                batch: self,
            }));
        }
        if row_order.len() != row_count {
            return Err(Box::new(RelayRecordBatchReorderFailure {
                error: RelayRecordBatchReorderError::RowCount {
                    order_rows: row_order.len(),
                    batch_rows: row_count,
                },
                batch: self,
            }));
        }
        let mut seen = vec![false; row_count];
        for &row in row_order {
            let Some(was_seen) = seen.get_mut(row) else {
                return Err(Box::new(RelayRecordBatchReorderFailure {
                    error: RelayRecordBatchReorderError::RowOutOfBounds {
                        row,
                        batch_rows: row_count,
                    },
                    batch: self,
                }));
            };
            if *was_seen {
                return Err(Box::new(RelayRecordBatchReorderFailure {
                    error: RelayRecordBatchReorderError::DuplicateRow { row },
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
            Err(reason) => {
                return Err(Box::new(RelayRecordBatchReorderFailure {
                    error: RelayRecordBatchReorderError::Arrow { reason },
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

    pub(crate) fn try_into_messages(self) -> Result<Vec<RelayMessage>, Box<(String, Self)>> {
        let row_count = self.batch.batch().num_rows();
        if row_count != self.acks.len() {
            return Err(Box::new((
                format!(
                    "stream batch ack count {} does not match row count {}",
                    self.acks.len(),
                    row_count
                ),
                self,
            )));
        }
        if row_count != self.metadata.len() {
            return Err(Box::new((
                format!(
                    "stream batch metadata count {} does not match row count {}",
                    self.metadata.len(),
                    row_count
                ),
                self,
            )));
        }
        if row_count != self.keys.len() {
            return Err(Box::new((
                format!(
                    "stream batch branch key count {} does not match row count {}",
                    self.keys.len(),
                    row_count
                ),
                self,
            )));
        }
        let rows = match (0..row_count)
            .zip(self.metadata.iter().cloned())
            .map(|(row, metadata)| RuntimeRow::new(self.batch.clone(), row, metadata))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(rows) => rows,
            Err(error) => return Err(Box::new((error, self))),
        };
        let Self { keys, acks, .. } = self;
        let mut messages = Vec::with_capacity(row_count);
        for ((record, acks), key) in rows.into_iter().zip(acks).zip(keys) {
            messages.push(RelayMessage { key, record, acks });
        }
        Ok(messages)
    }

    pub(super) fn concat(batches: Vec<Self>) -> Result<Self, String> {
        Self::concat_preserving(batches).map_err(|error| {
            let (reason, _batches) = *error;
            reason
        })
    }

    pub(super) fn concat_preserving(batches: Vec<Self>) -> Result<Self, Box<(String, Vec<Self>)>> {
        let Some(first) = batches.first() else {
            return Err(Box::new((
                "cannot concat zero relay batches".to_string(),
                batches,
            )));
        };

        let key = first.key.clone();
        if batches.len() == 1 {
            return Ok(batches.into_iter().next().expect("single batch must exist"));
        }

        let concatenated = {
            let runtime_batches = batches
                .iter()
                .map(|batch| batch.batch.as_ref())
                .collect::<Vec<_>>();
            match RuntimeRecordBatch::concat(&runtime_batches) {
                Ok(batch) => batch,
                Err(error) => return Err(Box::new((error, batches))),
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
        u64::try_from(self.batch.batch().num_rows()).unwrap_or(u64::MAX)
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
                .expect("validated relay batch reorder must contain each row once")
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
        domain_timestamp =
            Some(domain_timestamp.map_or(timestamp, |current| current.max(timestamp)));
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
) -> Result<RelayRecordBatch, (String, Vec<AckSet>)> {
    let Some(first) = messages.first() else {
        return Err((
            "cannot build relay batch from zero messages".to_string(),
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
            return Err((
                "stream batch cannot mix different branch keys".to_string(),
                pending_acks,
            ));
        }
        metadata.push(record.metadata().clone());
        records.push(record);
        acks.push(message_acks);
    }
    if records
        .iter()
        .any(|record| record.batch().schema().as_ref() != schema.arrow_schema().as_ref())
    {
        return Err((
            "stream message row schema does not match relay schema".to_string(),
            acks,
        ));
    }
    let batch = match RuntimeRecordBatch::shared_from_rows(schema.arrow_schema(), &records) {
        Ok(batch) => batch,
        Err(error) => return Err((error, acks)),
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

    use nervix_models::{CreateSchema, Identifier, ParseAsType, SchemaField, Timestamp};
    use triomphe::Arc;

    use super::{RelayMessage, RelayRecordBatch, delivery_observation_from_timestamps};
    use crate::{
        runtime_ack::AckSet,
        runtime_schema::{
            CompiledSchema, RuntimeRecordMetadata, RuntimeRow, RuntimeValue, compile_schema,
        },
    };

    fn test_schema() -> Arc<CompiledSchema> {
        Arc::new(compile_schema(&CreateSchema {
            name: Identifier::parse("relay_batch_test").expect("valid schema name"),
            fields: vec![SchemaField {
                name: Identifier::parse("value").expect("valid field name"),
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
                            i64::try_from(row).expect("test row must fit i64"),
                        ),
                        Timestamp::from_unix_nanos(
                            i64::try_from(row).expect("test row must fit i64"),
                        ),
                    ),
                )
                .expect("row must exist")
            })
            .collect()
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
            batch.batch.value(0, "value"),
            Ok(Some(RuntimeValue::I64(10)))
        );
        assert_eq!(
            batch.batch.value(2, "value"),
            Ok(Some(RuntimeValue::I64(30)))
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
            sparse.batch.value(0, "value"),
            Ok(Some(RuntimeValue::I64(10)))
        );
        assert_eq!(
            sparse.batch.value(1, "value"),
            Ok(Some(RuntimeValue::I64(30)))
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
