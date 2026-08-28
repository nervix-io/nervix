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
    pub(super) batch: RuntimeRecordBatch,
    pub(super) metadata: Vec<RuntimeRecordMetadata>,
    pub(super) acks: Vec<AckSet>,
}

pub(super) struct RelayDeliveryObservation {
    pub(super) domain_timestamp: Option<Timestamp>,
    pub(super) latency_seconds: Vec<f64>,
}

type UnkeyedRelayBatchParts = (
    RuntimeRecordBatch,
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
        RuntimeRow::new(
            Arc::new(self.batch.clone()),
            row,
            self.metadata[row].clone(),
        )
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
        let (batches, acks): (Vec<_>, Vec<_>) = messages
            .into_iter()
            .map(|message| (message.record.one_row_batch(), message.acks))
            .unzip();
        if batches
            .iter()
            .any(|batch| batch.schema().as_ref() != schema.arrow_schema().as_ref())
        {
            return Err("stream message row schema does not match relay schema".to_string());
        }
        let batch_refs = batches.iter().collect::<Vec<_>>();
        let batch = RuntimeRecordBatch::concat(&batch_refs)?;
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
            batch,
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
            batch,
            metadata,
            acks,
        })
    }

    pub(super) fn into_unkeyed_parts(self) -> UnkeyedRelayBatchParts {
        (self.batch, self.metadata, self.keys, self.acks)
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
        let batch = Arc::new(self.batch.clone());
        let rows = match (0..row_count)
            .zip(self.metadata.iter().cloned())
            .map(|(row, metadata)| RuntimeRow::new(batch.clone(), row, metadata))
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
            let runtime_batches = batches.iter().map(|batch| &batch.batch).collect::<Vec<_>>();
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
            batch: concatenated,
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
        self.batch
            .batch()
            .columns()
            .iter()
            .map(|column| {
                column
                    .to_data()
                    .get_slice_memory_size()
                    .ok()
                    .and_then(|bytes| u64::try_from(bytes).ok())
                    .unwrap_or(u64::MAX)
            })
            .fold(0_u64, u64::saturating_add)
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
    let mut batches = Vec::with_capacity(messages.len());
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
        batches.push(record.one_row_batch());
        acks.push(message_acks);
    }
    if batches
        .iter()
        .any(|batch| batch.schema().as_ref() != schema.arrow_schema().as_ref())
    {
        return Err((
            "stream message row schema does not match relay schema".to_string(),
            acks,
        ));
    }
    let batch_refs = batches.iter().collect::<Vec<_>>();
    let batch = match RuntimeRecordBatch::concat(&batch_refs) {
        Ok(batch) => batch,
        Err(error) => return Err((error, acks)),
    };
    let keys = vec![key.clone(); batches.len()];
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
    use std::cell::Cell;

    use nervix_models::Timestamp;

    use super::delivery_observation_from_timestamps;

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
