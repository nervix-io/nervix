use std::sync::Arc;

use nervix_models::Timestamp;

use super::BranchKey;
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{CompiledSchema, RuntimeRecord, RuntimeRecordBatch, RuntimeRecordMetadata},
};

#[derive(Debug, Clone)]
pub struct RelayMessage {
    pub(crate) key: Option<BranchKey>,
    pub(crate) record: RuntimeRecord,
    pub(crate) acks: AckSet,
}

#[derive(Debug, Clone)]
pub(crate) struct RelayRecordBatch {
    pub(super) key: Option<BranchKey>,
    pub(super) keys: Vec<Option<BranchKey>>,
    pub(super) batch: RuntimeRecordBatch,
    pub(super) records: Vec<RuntimeRecord>,
    pub(super) metadata: Vec<RuntimeRecordMetadata>,
    pub(super) acks: Vec<AckSet>,
}

impl RelayRecordBatch {
    pub(super) fn single(
        schema: Arc<CompiledSchema>,
        key: Option<BranchKey>,
        record: RuntimeRecord,
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
        let records = messages
            .iter()
            .map(|message| message.record.clone())
            .collect::<Vec<_>>();
        let metadata = messages
            .iter()
            .map(|message| message.record.metadata().clone())
            .collect::<Vec<_>>();
        let acks = messages
            .into_iter()
            .map(|message| message.acks)
            .collect::<Vec<_>>();
        let batch = schema.arrow_batch_from_records(&records)?;
        Ok(Self {
            key,
            keys,
            batch,
            records,
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
        let decoded_records = schema.decoded_records_from_arrow_batch(&batch)?;
        if decoded_records.len() != acks.len() {
            return Err(format!(
                "stream batch ack count {} does not match row count {}",
                acks.len(),
                decoded_records.len()
            ));
        }
        if decoded_records.len() != metadata.len() {
            return Err(format!(
                "stream batch metadata count {} does not match row count {}",
                metadata.len(),
                decoded_records.len()
            ));
        }
        let keys = vec![key.clone(); decoded_records.len()];
        let records = decoded_records
            .into_iter()
            .zip(metadata.iter().cloned())
            .map(|(record, metadata)| record.into_runtime_record(metadata))
            .collect();
        Ok(Self {
            key,
            keys,
            batch,
            records,
            metadata,
            acks,
        })
    }

    pub(super) fn from_filtered_parts(
        key: Option<BranchKey>,
        batch: RuntimeRecordBatch,
        records: Vec<RuntimeRecord>,
        metadata: Vec<RuntimeRecordMetadata>,
        acks: Vec<AckSet>,
    ) -> Result<Self, String> {
        let row_count = batch.batch().num_rows();
        if records.len() != row_count {
            return Err(format!(
                "filtered record count {} does not match row count {}",
                records.len(),
                row_count
            ));
        }
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
            records,
            metadata,
            acks,
        })
    }

    pub(super) fn into_unkeyed_parts(
        self,
    ) -> (
        RuntimeRecordBatch,
        Vec<RuntimeRecord>,
        Vec<RuntimeRecordMetadata>,
        Vec<Option<BranchKey>>,
        Vec<AckSet>,
    ) {
        (
            self.batch,
            self.records,
            self.metadata,
            self.keys,
            self.acks,
        )
    }

    pub(crate) fn try_into_messages(self) -> Result<Vec<RelayMessage>, Box<(String, Self)>> {
        if self.records.len() != self.acks.len() {
            return Err(Box::new((
                format!(
                    "stream batch ack count {} does not match row count {}",
                    self.acks.len(),
                    self.records.len()
                ),
                self,
            )));
        }
        if self.records.len() != self.metadata.len() {
            return Err(Box::new((
                format!(
                    "stream batch metadata count {} does not match row count {}",
                    self.metadata.len(),
                    self.records.len()
                ),
                self,
            )));
        }
        if self.records.len() != self.keys.len() {
            return Err(Box::new((
                format!(
                    "stream batch branch key count {} does not match row count {}",
                    self.keys.len(),
                    self.records.len()
                ),
                self,
            )));
        }
        Ok(self
            .records
            .into_iter()
            .zip(self.acks)
            .zip(self.keys)
            .map(|((record, acks), key)| RelayMessage { key, record, acks })
            .collect())
    }

    pub(super) fn concat(batches: Vec<Self>) -> Result<Self, String> {
        let Some(first) = batches.first() else {
            return Err("cannot concat zero relay batches".to_string());
        };

        let key = first.key.clone();

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().expect("single batch must exist"));
        }

        let concatenated = {
            let runtime_batches = batches.iter().map(|batch| &batch.batch).collect::<Vec<_>>();
            RuntimeRecordBatch::concat(&runtime_batches)?
        };

        let total_metadata = batches
            .iter()
            .map(|batch| batch.metadata.len())
            .sum::<usize>();
        let total_records = batches
            .iter()
            .map(|batch| batch.records.len())
            .sum::<usize>();
        let total_acks = batches.iter().map(|batch| batch.acks.len()).sum::<usize>();
        let total_keys = batches.iter().map(|batch| batch.keys.len()).sum::<usize>();
        let mut records = Vec::with_capacity(total_records);
        let mut metadata = Vec::with_capacity(total_metadata);
        let mut acks = Vec::with_capacity(total_acks);
        let mut keys = Vec::with_capacity(total_keys);
        for batch in batches {
            records.extend(batch.records);
            metadata.extend(batch.metadata);
            acks.extend(batch.acks);
            keys.extend(batch.keys);
        }

        Ok(Self {
            key,
            keys,
            batch: concatenated,
            records,
            metadata,
            acks,
        })
    }

    pub(super) fn detached(&self) -> Self {
        Self {
            key: self.key.clone(),
            keys: self.keys.clone(),
            batch: self.batch.clone(),
            records: self.records.clone(),
            metadata: self.metadata.clone(),
            acks: vec![AckSet::empty(); self.acks.len()],
        }
    }

    pub(super) fn attached(&self) -> Self {
        Self {
            key: self.key.clone(),
            keys: self.keys.clone(),
            batch: self.batch.clone(),
            records: self.records.clone(),
            metadata: self.metadata.clone(),
            acks: self.acks.iter().map(AckSet::attached).collect::<Vec<_>>(),
        }
    }

    pub(super) fn message_count(&self) -> u64 {
        u64::try_from(self.batch.batch().num_rows()).unwrap_or(u64::MAX)
    }

    pub(super) fn arrow_schema(&self) -> Arc<arrow_schema::Schema> {
        self.batch.schema()
    }

    pub(super) fn estimated_bytes(&self) -> u64 {
        self.batch
            .batch()
            .columns()
            .iter()
            .map(|column| u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX))
            .sum()
    }

    pub(super) fn ack_success(&self) {
        for ack in &self.acks {
            ack.ack_success();
        }
    }

    pub(super) fn merged_acks(&self) -> AckSet {
        AckSet::merged(self.acks.iter().cloned())
    }

    pub(super) fn delivery_latency_seconds(&self, now: Timestamp) -> Vec<f64> {
        self.metadata
            .iter()
            .filter_map(|metadata| {
                now.into_datetime()
                    .signed_duration_since(metadata.ingested_at_high_watermark().into_datetime())
                    .to_std()
                    .ok()
                    .map(|duration| duration.as_secs_f64())
            })
            .collect()
    }

    pub(super) fn domain_timestamp(&self) -> Option<Timestamp> {
        self.metadata
            .iter()
            .map(|metadata| metadata.ingested_at_high_watermark())
            .max()
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
        records.push(record);
        acks.push(message_acks);
    }
    let batch = match schema.arrow_batch_from_records(&records) {
        Ok(batch) => batch,
        Err(error) => return Err((error, acks)),
    };
    let metadata = records
        .iter()
        .map(|record| record.metadata().clone())
        .collect();
    let keys = vec![key.clone(); records.len()];
    Ok(RelayRecordBatch {
        key,
        keys,
        batch,
        records,
        metadata,
        acks,
    })
}
