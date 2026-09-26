//! Encoding an emitter's rows into the records a record sink publishes.
//!
//! Layer: data plane.
//! - **Owns.** Encoding every row a batch still holds with the emitter's codec — one record per row,
//!   or, when the emitter declares `BATCH`, one record per packed batch payload — pairing each
//!   record with its key, headers and ordering group, rejecting the rows that cannot be encoded or
//!   do not fit, handing the records to the record sink in one write, and applying the sink's
//!   outcome for a batch record to every member it carries.
//! - **Depends on.** The emitter's compiled codec, its buffered batches, the batch packing, and the
//!   connector contract's record sink and record value type.
//! - **Must not know.** Which external system receives the records, or when the emitter publishes
//!   them.

use std::collections::BTreeMap;

use async_trait::async_trait;
use nervix_connector::{
    PerRecordOutcome, RecordSink, RejectedSinkRecord, SinkLifecycle, SinkRecord, SinkRecordPosition,
};
use nervix_models::EmitterBatchPolicy;

use super::{
    emitter_batch_packing::{
        BatchEnvelope, BatchPayload, BufferedBatchPacking, PackedOutcome, PackingRow,
        pack_buffered_batch,
    },
    *,
};
use crate::runtime_schema::{BatchContainerError, CompiledCodecBatchEncoder};

#[derive(Debug)]
struct PendingRowPayload {
    row_index: usize,
    payload: Result<Vec<u8>, Report<CodecError>>,
}

impl PendingRowPayload {
    fn encode(encoder: &CompiledCodecBatchEncoder<'_>, row_index: usize) -> Self {
        let mut payload = Vec::new();
        let encoded = encoder.encode_row_into(row_index, &mut payload);
        Self {
            row_index,
            payload: encoded.map(|()| payload),
        }
    }
}

#[derive(Debug)]
struct EncodedBrokerRecord {
    batch_index: usize,
    row_index: usize,
    key: Option<String>,
    payload: Vec<u8>,
    headers: EmitterHeaders,
    message_group: Result<Option<String>, OrderingGroupError>,
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
    /// The emitter's `BATCH` clause. With it, each record the sink receives is one batch payload
    /// holding several rows; without it, each record is one row.
    pub(super) batch: Option<EmitterBatchPolicy>,
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
        if let Some(policy) = self.batch {
            let packed = pack_batch_records(self.codec.clone(), policy, context, batches).await?;
            let outcome = self.sink.publish(packed.records).await;
            let outcome = packed.membership.apply_to_members(outcome);
            return finish_record_sink_publish(
                context,
                batches,
                outcome,
                DeliveredAcknowledgements::Host,
            )
            .await;
        }
        let encoded = encode_broker_records(self.codec.clone(), context, batches).await?;
        let records = sink_records(context, batches, encoded).await?;
        let outcome = self.sink.publish(records).await;
        finish_record_sink_publish(context, batches, outcome, DeliveredAcknowledgements::Host).await
    }
}

async fn encode_pending_broker_payloads(
    codec: Arc<CompiledCodec>,
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
                    .map(|row_index| PendingRowPayload::encode(&encoder, row_index))
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
        .map(|row_index| PendingRowPayload::encode(&encoder, row_index))
        .collect())
}

async fn encode_broker_records(
    codec: Arc<CompiledCodec>,
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
            encode_pending_broker_payloads(codec.clone(), context, batch, pending_rows),
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
            let message_group = batch.ordering_group(row_index)?;
            let position = SinkRecordPosition {
                batch_index,
                row_index,
            };
            let payload = match payload {
                Ok(payload) => payload,
                Err(error) => {
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

/// The records a batching emitter publishes, one per batch payload, and the rows each carries.
struct PackedBatchRecords {
    records: Vec<SinkRecord>,
    membership: BatchMembership,
}

/// The rows every batch record carries, keyed by the position the record is published under.
///
/// A batch record is published under its first member's position, so a sink that answers for the
/// record answers for exactly one entry here.
#[derive(Debug, Default)]
struct BatchMembership {
    members: BTreeMap<SinkRecordPosition, Vec<SinkRecordPosition>>,
}

impl BatchMembership {
    /// Applies the sink's outcome for each batch record to every member it carries.
    ///
    /// One confirmation delivers every member, and one rejection rejects every member with the
    /// same error, so its reference shows that they failed together.
    fn apply_to_members(&self, outcome: PerRecordOutcome) -> PerRecordOutcome {
        let outcome = outcome.into_parts();
        let mut applied = PerRecordOutcome::with_capacity(outcome.delivered.len());
        for position in outcome.delivered {
            for member in self.members_of(position) {
                applied.deliver(member);
            }
        }
        for rejected in outcome.rejected {
            for member in self.members_of(rejected.position) {
                applied.reject(RejectedSinkRecord {
                    position: member,
                    error: rejected.error.clone(),
                });
            }
        }
        if let Some(error) = outcome.infrastructure_error {
            applied.fail(error);
        }
        applied
    }

    /// The members of the batch record published under `position`. A position the host never
    /// published a batch under answers for itself, so an outcome is never silently dropped.
    fn members_of(&self, position: SinkRecordPosition) -> Vec<SinkRecordPosition> {
        match self.members.get(&position) {
            Some(members) => members.clone(),
            None => vec![position],
        }
    }
}

/// Packs the rows every batch still holds into batch payloads and pairs each payload with the key,
/// headers and ordering group its members share.
///
/// Rows whose ordering group could not be evaluated are rejected before packing, as they are when
/// the emitter publishes one record per row. Rows that cannot be members, rows that alone exceed
/// `MAX SIZE`, and the rows of a candidate whose container failed are rejected here as well.
async fn pack_batch_records(
    codec: Arc<CompiledCodec>,
    policy: EmitterBatchPolicy,
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
) -> EmitterRuntimeResult<PackedBatchRecords> {
    let mut records = Vec::new();
    let mut membership = BatchMembership::default();
    let mut unpublishable = Vec::new();
    let mut rejected = Vec::new();
    for (batch_index, batch) in batches.iter().enumerate() {
        tokio::task::consume_budget().await;
        let mut rows = Vec::new();
        for row_index in batch.pending_record_rows() {
            let envelope = match batch_envelope(batch, row_index)? {
                Ok(envelope) => envelope,
                Err(error) => {
                    unpublishable.push(RejectedEmitterRecord {
                        position: SinkRecordPosition {
                            batch_index,
                            row_index,
                        },
                        reason: error.to_string(),
                        structured_error: None,
                    });
                    continue;
                }
            };
            rows.push(PackingRow {
                row_index,
                envelope,
            });
        }
        if rows.is_empty() {
            continue;
        }
        let batch_acks = batch.merged_acks();
        let packing = await_emitter_confirmation(
            &batch_acks,
            pack_pending_rows(codec.clone(), policy, context, batch, rows),
        )
        .await?;
        if packing.subdivisions > 0 {
            tracing::debug!(
                emitter = %context.emitter,
                subdivisions = packing.subdivisions,
                "re-encoded batch candidates that reached MAX SIZE"
            );
        }
        for outcome in packing.outcomes {
            tokio::task::consume_budget().await;
            match outcome {
                PackedOutcome::Payload(BatchPayload {
                    rows,
                    envelope,
                    payload,
                }) => {
                    let members = rows
                        .into_iter()
                        .map(|row_index| SinkRecordPosition {
                            batch_index,
                            row_index,
                        })
                        .collect::<Vec<_>>();
                    let position = *members
                        .first()
                        .assured("a packed payload always carries at least one member");
                    let record = SinkRecord::new(
                        position,
                        envelope.key,
                        payload,
                        envelope.headers,
                        batch.execution_now,
                    );
                    records.push(match envelope.message_group {
                        Some(message_group) => record.with_message_group(message_group),
                        None => record,
                    });
                    membership.members.insert(position, members);
                }
                PackedOutcome::MemberFailed { row_index, error } => {
                    rejected.push(RejectedEmitterRecord {
                        position: SinkRecordPosition {
                            batch_index,
                            row_index,
                        },
                        reason: format!(
                            "emitter '{}' failed to encode record: {error}",
                            context.emitter.as_str()
                        ),
                        structured_error: None,
                    });
                }
                PackedOutcome::Oversize {
                    row_index,
                    exceeded,
                } => {
                    let message = format!("emitter '{}' {exceeded}", context.emitter.as_str());
                    rejected.push(RejectedEmitterRecord {
                        position: SinkRecordPosition {
                            batch_index,
                            row_index,
                        },
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
                }
                PackedOutcome::ContainerFailed { rows, error } => {
                    let shared = container_failure(context, &codec, batch, rows.len(), error);
                    for row_index in rows {
                        rejected.push(RejectedEmitterRecord {
                            position: SinkRecordPosition {
                                batch_index,
                                row_index,
                            },
                            reason: shared.message.clone(),
                            structured_error: Some(shared.clone()),
                        });
                    }
                }
            }
        }
    }
    finish_rejected_records(
        context,
        batches,
        unpublishable,
        MessageErrorOperation::Publish,
    )
    .await?;
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Encode).await?;
    Ok(PackedBatchRecords {
        records,
        membership,
    })
}

/// The one error every member of a candidate whose container failed is rejected with.
///
/// It names the emitter, the codec, the cause and the member count, and never a payload value.
fn container_failure(
    context: &EmitterSinkContext,
    codec: &CompiledCodec,
    batch: &EmitterPublishBatch,
    member_count: usize,
    error: BatchContainerError,
) -> StructuredMessageError {
    let code = match error {
        BatchContainerError::NoOutput
        | BatchContainerError::MultipleOutputs
        | BatchContainerError::Evaluation => MessageErrorCode::Evaluation,
        BatchContainerError::Unwritable { .. } => MessageErrorCode::Validation,
    };
    let noun = if member_count == 1 {
        "message"
    } else {
        "messages"
    };
    structured_message_error(
        batch.execution_now,
        code,
        format!(
            "emitter '{}' codec '{}' {error} for a batch of {member_count} {noun}",
            context.emitter.as_str(),
            codec.name.as_str(),
        ),
        MessageErrorOperation::Encode,
        None,
        std::iter::empty(),
    )
}

/// The key, headers and ordering group row `row_index` would be published under, or why its
/// ordering group could not be evaluated.
fn batch_envelope(
    batch: &EmitterPublishBatch,
    row_index: usize,
) -> EmitterRuntimeResult<Result<BatchEnvelope, OrderingGroupError>> {
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
    let message_group = match batch.ordering_group(row_index)? {
        Ok(message_group) => message_group,
        Err(failure) => return Ok(Err(failure)),
    };
    Ok(Ok(BatchEnvelope {
        key,
        headers,
        message_group,
    }))
}

/// Packs `rows` of `batch`, off the reactor when the codec's transformations require it.
async fn pack_pending_rows(
    codec: Arc<CompiledCodec>,
    policy: EmitterBatchPolicy,
    context: &EmitterSinkContext,
    batch: &EmitterPublishBatch,
    rows: Vec<PackingRow>,
) -> EmitterRuntimeResult<BufferedBatchPacking> {
    let initialization_failed = |error: Report<CodecError>| {
        error
            .change_context(EmitterRuntimeError::EncodeBatch)
            .attach_printable(format!(
                "emitter '{}' failed to initialize columnar encoding",
                context.emitter.as_str()
            ))
    };
    if !codec.requires_blocking_encode() {
        return pack_buffered_batch(&codec, &batch.batch.batch, rows, policy)
            .map_err(initialization_failed);
    }
    let arrow_batch = batch.batch.batch.clone();
    let codec_name = codec.name.as_str().to_string();
    tokio::task::spawn_blocking(move || pack_buffered_batch(&codec, &arrow_batch, rows, policy))
        .await
        .map_err(|error| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "emitter '{}' blocking codec task for '{}' failed: {error}",
                context.emitter.as_str(),
                codec_name
            ))
        })?
        .map_err(initialization_failed)
}

#[cfg(test)]
mod tests {
    use nervix_connector::SinkPublishError;

    use super::*;

    fn position(batch_index: usize, row_index: usize) -> SinkRecordPosition {
        SinkRecordPosition {
            batch_index,
            row_index,
        }
    }

    fn membership() -> BatchMembership {
        let mut membership = BatchMembership::default();
        membership.members.insert(
            position(0, 0),
            vec![position(0, 0), position(0, 1), position(0, 3)],
        );
        membership
            .members
            .insert(position(1, 2), vec![position(1, 2), position(1, 4)]);
        membership
    }

    #[test]
    fn a_confirmed_batch_record_delivers_every_member() {
        let mut outcome = PerRecordOutcome::with_capacity(1);
        outcome.deliver(position(1, 2));

        let applied = membership().apply_to_members(outcome).into_parts();

        assert_eq!(applied.delivered, vec![position(1, 2), position(1, 4)]);
        assert!(applied.rejected.is_empty());
        assert!(applied.infrastructure_error.is_none());
    }

    #[test]
    fn a_rejected_batch_record_rejects_every_member_with_one_reference() {
        let mut outcome = PerRecordOutcome::with_capacity(0);
        outcome.reject(RejectedSinkRecord::external(
            position(0, 0),
            Timestamp::from_unix_nanos(1),
            "refused".to_string(),
        ));

        let applied = membership().apply_to_members(outcome).into_parts();

        let rejected = applied
            .rejected
            .iter()
            .map(|rejected| rejected.position)
            .collect::<Vec<_>>();
        assert_eq!(
            rejected,
            vec![position(0, 0), position(0, 1), position(0, 3)]
        );
        let references = applied
            .rejected
            .iter()
            .map(|rejected| rejected.error.reference)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(references.len(), 1);
    }

    #[test]
    fn an_infrastructure_failure_and_unknown_positions_pass_through() {
        let mut outcome = PerRecordOutcome::with_capacity(1);
        outcome.deliver(position(2, 0));
        outcome.fail(Report::new(SinkPublishError::Publish { sink: "test" }));

        let applied = membership().apply_to_members(outcome).into_parts();

        assert_eq!(applied.delivered, vec![position(2, 0)]);
        assert!(applied.infrastructure_error.is_some());
    }
}
