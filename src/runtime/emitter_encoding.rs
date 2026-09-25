//! Encoding an emitter's rows into the records a record sink publishes.
//!
//! Layer: data plane.
//! - **Owns.** Encoding every row a batch still holds with the emitter's codec, holding each
//!   encoding to the emitter's `BATCH ... MAX SIZE` when it declares one, pairing each record with
//!   its key, headers and ordering group, rejecting the rows that cannot be encoded or do not fit,
//!   and handing the records to the record sink in one write.
//! - **Depends on.** The emitter's compiled codec, its buffered batches, and the connector
//!   contract's record sink and record value type.
//! - **Must not know.** Which external system receives the records, or when the emitter publishes
//!   them.

use async_trait::async_trait;
use nervix_connector::{RecordSink, SinkLifecycle, SinkRecord, SinkRecordPosition};
use nervix_models::PayloadSizeLimit;

use super::*;
use crate::runtime_schema::{BoundedRowEncoding, CompiledCodecBatchEncoder, PayloadLimitExceeded};

/// What encoding one pending row produced.
#[derive(Debug)]
enum RowPayload {
    Encoded(Vec<u8>),
    Failed(Report<CodecError>),
    /// The row's encoding would have exceeded the emitter's `MAX SIZE`, so it was abandoned.
    Oversize(PayloadLimitExceeded),
}

impl RowPayload {
    /// Encodes `row_index`, held to `limit` when the emitter declares one.
    fn encode(
        encoder: &CompiledCodecBatchEncoder<'_>,
        row_index: usize,
        limit: Option<PayloadSizeLimit>,
    ) -> Self {
        let Some(limit) = limit else {
            let mut payload = Vec::new();
            return match encoder.encode_row_into(row_index, &mut payload) {
                Ok(()) => Self::Encoded(payload),
                Err(error) => Self::Failed(error),
            };
        };
        match encoder.encode_row_within(row_index, limit) {
            Ok(BoundedRowEncoding::Encoded(payload)) => Self::Encoded(payload),
            Ok(BoundedRowEncoding::Oversize(exceeded)) => Self::Oversize(exceeded),
            Err(error) => Self::Failed(error),
        }
    }
}

#[derive(Debug)]
struct PendingRowPayload {
    row_index: usize,
    payload: RowPayload,
}

#[derive(Debug)]
struct EncodedBrokerRecord {
    batch_index: usize,
    row_index: usize,
    key: Option<String>,
    payload: Vec<u8>,
    headers: EmitterHeaders,
    message_group: Result<Option<String>, SqsMessageGroupError>,
    execution_now: Timestamp,
}

impl EncodedBrokerRecord {
    const fn position(&self) -> SinkRecordPosition {
        SinkRecordPosition {
            batch_index: self.batch_index,
            row_index: self.row_index,
        }
    }
}

/// A record sink and the codec the host encodes its records with.
pub(super) struct EncodedRecordSink {
    pub(super) sink: Box<dyn RecordSink>,
    pub(super) codec: Arc<CompiledCodec>,
    /// The emitter's `BATCH ... MAX SIZE`. Every encoded payload is held to it, so a record whose
    /// encoding exceeds it never reaches the sink.
    pub(super) payload_limit: Option<PayloadSizeLimit>,
}

#[async_trait]
impl EmitterSink for EncodedRecordSink {
    fn lifecycle(&self) -> &dyn SinkLifecycle {
        &*self.sink
    }

    fn lifecycle_mut(&mut self) -> &mut dyn SinkLifecycle {
        &mut *self.sink
    }

    /// Encodes every row the batches still hold and publishes them in one write.
    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()> {
        let encoded =
            encode_broker_records(self.codec.clone(), self.payload_limit, context, batches).await?;
        let records = sink_records(context, batches, encoded).await?;
        let outcome = self.sink.publish(records).await;
        finish_record_sink_publish(context, batches, outcome, DeliveredAcknowledgements::Host).await
    }
}

async fn encode_pending_broker_payloads(
    codec: Arc<CompiledCodec>,
    payload_limit: Option<PayloadSizeLimit>,
    context: &EmitterSinkContext,
    batch: &EmitterPublishBatch,
    pending_rows: Vec<usize>,
) -> EmitterRuntimeResult<Vec<PendingRowPayload>> {
    if codec.requires_blocking_encode() {
        let arrow_batch = batch.batch.batch.clone();
        let codec_name = codec.name.as_str().to_string();
        return tokio::task::spawn_blocking(move || {
            let encoder = codec.batch_encoder(&arrow_batch)?;
            Ok::<_, CodecError>(
                pending_rows
                    .into_iter()
                    .map(|row_index| PendingRowPayload {
                        row_index,
                        payload: RowPayload::encode(&encoder, row_index, payload_limit),
                    })
                    .collect(),
            )
        })
        .await
        .map_err(|error| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "emitter '{}' blocking codec task for '{}' failed: {error}",
                context.emitter.as_str(),
                codec_name
            ))
        })?
        .map_err(|error| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "emitter '{}' failed to initialize columnar encoding: {error}",
                context.emitter.as_str()
            ))
        });
    }

    let encoder = codec.batch_encoder(&batch.batch.batch).map_err(|error| {
        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
            "emitter '{}' failed to initialize columnar encoding: {error}",
            context.emitter.as_str()
        ))
    })?;
    Ok(pending_rows
        .into_iter()
        .map(|row_index| PendingRowPayload {
            row_index,
            payload: RowPayload::encode(&encoder, row_index, payload_limit),
        })
        .collect())
}

async fn encode_broker_records(
    codec: Arc<CompiledCodec>,
    payload_limit: Option<PayloadSizeLimit>,
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
) -> EmitterRuntimeResult<Vec<EncodedBrokerRecord>> {
    let mut encoded = Vec::new();
    let mut rejected = Vec::new();
    for (batch_index, batch) in batches.iter().enumerate() {
        tokio::task::consume_budget().await;
        let row_count = batch.batch.batch.batch().num_rows();
        let pending_rows = (0..row_count)
            .filter(|row_index| !batch.is_delivered(*row_index))
            .collect::<Vec<_>>();
        let batch_acks = batch.merged_acks();
        let payloads = await_emitter_confirmation(
            &batch_acks,
            encode_pending_broker_payloads(
                codec.clone(),
                payload_limit,
                context,
                batch,
                pending_rows,
            ),
        )
        .await?;

        for PendingRowPayload { row_index, payload } in payloads {
            tokio::task::consume_budget().await;
            let key = batch
                .batch
                .keys
                .get(row_index)
                .ok_or_else(|| {
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "emitter batch row {row_index} has no branch key entry"
                    ))
                })?
                .as_ref()
                .map(|key| key.as_str().to_string());
            let headers = batch.headers_for_row(row_index).cloned().ok_or_else(|| {
                Report::new(EmitterRuntimeError::EncodeBatch)
                    .attach_printable(format!("emitter batch row {row_index} has no header entry"))
            })?;
            let message_group = batch
                .sqs_message_groups
                .get(row_index)
                .cloned()
                .ok_or_else(|| {
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "emitter batch row {row_index} has no SQS FIFO group entry"
                    ))
                })?;
            let position = SinkRecordPosition {
                batch_index,
                row_index,
            };
            let payload = match payload {
                RowPayload::Encoded(payload) => payload,
                RowPayload::Failed(error) => {
                    rejected.push(RejectedEmitterRecord {
                        position,
                        reason: format!(
                            "emitter '{}' failed to encode record: {error}",
                            context.emitter.as_str()
                        ),
                        structured_error: None,
                    });
                    continue;
                }
                RowPayload::Oversize(exceeded) => {
                    let message = format!("emitter '{}' {exceeded}", context.emitter.as_str());
                    rejected.push(RejectedEmitterRecord {
                        position,
                        reason: message.clone(),
                        structured_error: Some(structured_message_error(
                            batch.execution_now,
                            MessageErrorCode::Validation,
                            message,
                            MessageErrorOperation::Encode,
                            None,
                            std::iter::empty(),
                        )),
                    });
                    continue;
                }
            };
            encoded.push(EncodedBrokerRecord {
                batch_index,
                row_index,
                key,
                payload,
                headers,
                message_group,
                execution_now: batch.execution_now,
            });
        }
    }
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Encode).await?;
    Ok(encoded)
}

/// The records a sink publishes, once every record whose ordering group could not be evaluated
/// has been rejected here.
///
/// A sink that orders records per group receives the group already evaluated for its record, so
/// the expression that produced it, and the reason a record has none, both stay with the host.
async fn sink_records(
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
    encoded: Vec<EncodedBrokerRecord>,
) -> EmitterRuntimeResult<Vec<SinkRecord>> {
    let mut records = Vec::with_capacity(encoded.len());
    let mut rejected = Vec::new();
    for record in encoded {
        tokio::task::consume_budget().await;
        let position = record.position();
        let message_group = match record.message_group {
            Ok(message_group) => message_group,
            Err(error) => {
                rejected.push(RejectedEmitterRecord {
                    position,
                    reason: error.to_string(),
                    structured_error: None,
                });
                continue;
            }
        };
        let sink_record = SinkRecord::new(
            position,
            record.key,
            record.payload,
            record.headers,
            record.execution_now,
        );
        records.push(match message_group {
            Some(message_group) => sink_record.with_message_group(message_group),
            None => sink_record,
        });
    }
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Publish).await?;
    Ok(records)
}
