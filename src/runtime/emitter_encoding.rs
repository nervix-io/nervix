//! Encoding an emitter's rows into the records a record sink publishes.
//!
//! Layer: data plane.
//! - **Owns.** Encoding every row a batch still holds with the emitter's codec — one record per row,
//!   or, when the emitter declares `BATCH`, one batch payload per packed candidate — pairing each
//!   record with its key, headers and ordering group, rejecting the rows that cannot be encoded or
//!   do not fit, and handing one write to the record sink: the batch payloads earlier attempts
//!   retained, unchanged, followed by the ones packed now.
//! - **Depends on.** The emitter's compiled codec, its buffered batches, the batch packing, the
//!   record identities and retained payloads a write is answered through, and the connector
//!   contract's record sink and record value type.
//! - **Must not know.** Which external system receives the records, or when the emitter publishes
//!   them.

use async_trait::async_trait;
use nervix_connector::{RecordSink, SinkLifecycle, SinkRecord, SinkRecordPosition};
use nervix_models::EmitterBatchPolicy;

use super::{
    emitter_batch_packing::{
        BatchEnvelope, BatchPayload, BufferedBatchPacking, PackedOutcome, PackingCarrier,
        PackingRow, pack_buffered_batches,
    },
    *,
};
use crate::runtime_schema::{BatchContainerError, CompiledCodecBatchEncoder};

/// The encoding of one pending row, or why the codec could not encode it.
#[derive(Debug)]
pub(super) struct PendingRowPayload {
    pub(super) row_index: usize,
    pub(super) payload: Result<Vec<u8>, Report<CodecError>>,
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
    ///
    /// With `BATCH`, the write carries every batch payload the emitter retains: the ones earlier
    /// attempts prepared and the sink left unanswered, written again exactly as they were first
    /// written, and the ones packed now from rows no payload carries yet.
    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        publication: EmitterPublication<'_>,
    ) -> EmitterRuntimeResult<()> {
        let EmitterPublication {
            batches,
            payloads: prepared,
            ..
        } = publication;
        let Some(policy) = self.batch else {
            return self.publish_rows(context, batches).await;
        };
        let packed = pack_batch_records(self.codec.clone(), policy, context, batches).await?;
        for payload in packed {
            prepared.retain(payload, batches)?;
        }
        if prepared.is_empty() {
            return Ok(());
        }
        let PreparedWrite { records, payloads } = prepared.next_write();
        let record_count = records.len();
        let outcome = self.sink.publish(records).await;
        let outcome = context.received_outcome(record_count, outcome);
        prepared
            .answers(batches, payloads, outcome)?
            .apply(context, batches, DeliveredAcknowledgements::Host)
            .await
    }
}

impl EncodedRecordSink {
    /// Encodes one record for every row the batches still hold and publishes them in one write.
    async fn publish_rows(
        &mut self,
        context: &EmitterSinkContext,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()> {
        let encoded = encode_broker_records(self.codec.clone(), context, batches).await?;
        let RowWrite { records, rows } = sink_records(context, batches, encoded).await?;
        if records.is_empty() {
            return Ok(());
        }
        let record_count = records.len();
        let outcome = self.sink.publish(records).await;
        let outcome = context.received_outcome(record_count, outcome);
        rows.answers(outcome)?
            .apply(context, batches, DeliveredAcknowledgements::Host)
            .await
    }
}

/// Encodes each of `pending_rows` of `batch` with the emitter's codec, off the reactor when the
/// codec's transformations require it.
pub(super) async fn encode_pending_broker_payloads(
    codec: Arc<CompiledCodec>,
    context: &EmitterSinkContext,
    batch: &EmitterPublishBatch,
    pending_rows: Vec<usize>,
) -> EmitterRuntimeResult<Vec<PendingRowPayload>> {
    if codec.requires_blocking_encode() {
        let arrow_batch = batch.relay_batch().batch.clone();
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

    let encoder = codec
        .batch_encoder(&batch.relay_batch().batch)
        .map_err(|error| {
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
        let pending_rows = batch.pending_record_rows();
        let batch_acks = batch.merged_acks();
        let payloads = await_emitter_confirmation(
            &batch_acks,
            encode_pending_broker_payloads(codec.clone(), context, batch, pending_rows),
        )
        .await?;

        for PendingRowPayload { row_index, payload } in payloads {
            tokio::task::consume_budget().await;
            let key = batch
                .relay_batch()
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
                execution_now: batch.execution_now(),
            });
        }
    }
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Encode).await?;
    Ok(encoded)
}

/// The records one write hands a sink when each record carries one row, and the row each carries.
struct RowWrite {
    records: Vec<SinkRecord>,
    rows: RowRecords,
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
) -> EmitterRuntimeResult<RowWrite> {
    let mut records = Vec::with_capacity(encoded.len());
    let mut rows = RowRecords::with_capacity(encoded.len());
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
            rows.record(position),
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
    Ok(RowWrite { records, rows })
}

/// Packs the rows no payload carries yet across successive Arrow carriers, and pairs each payload
/// with the key, headers and ordering group its members share.
///
/// Rows whose ordering group could not be evaluated are rejected before packing, as they are when
/// the emitter publishes one record per row. Rows that cannot be members, rows that alone exceed
/// `MAX SIZE`, and the rows of a candidate whose container failed are rejected here as well, so
/// every row packing sees ends either rejected or a member of one returned payload.
async fn pack_batch_records(
    codec: Arc<CompiledCodec>,
    policy: EmitterBatchPolicy,
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
) -> EmitterRuntimeResult<Vec<PreparedPayload<EncodedPayload>>> {
    let mut payloads = Vec::new();
    let mut unpublishable = Vec::new();
    let mut rejected = Vec::new();
    let mut carriers = Vec::with_capacity(batches.len());
    for (batch_index, batch) in batches.iter().enumerate() {
        tokio::task::consume_budget().await;
        let mut rows = Vec::new();
        for row in batch.rows_to_pack() {
            // A payload packed now never spans the rows a retained payload carries, so the two
            // cannot interleave their members.
            let RowToPack::Pending(row_index) = row else {
                rows.push(PackingRow::Seal);
                continue;
            };
            let position = SinkRecordPosition {
                batch_index,
                row_index,
            };
            let envelope = match batch_envelope(batch, row_index)? {
                Ok(envelope) => envelope,
                Err(error) => {
                    unpublishable.push(RejectedEmitterRecord {
                        position,
                        reason: error.to_string(),
                        structured_error: None,
                    });
                    rows.push(PackingRow::Seal);
                    continue;
                }
            };
            rows.push(PackingRow::Ready { position, envelope });
        }
        if rows.is_empty() {
            continue;
        }
        carriers.push(PackingCarrier {
            source_relay: batch.source_relay().clone(),
            branch_key: batch.relay_batch().key.clone(),
            batch: batch.relay_batch().batch.clone(),
            rows,
        });
    }
    let acks = AckSet::merged(batches.iter().map(EmitterPublishBatch::merged_acks));
    let packing = await_emitter_confirmation(
        &acks,
        pack_pending_rows(codec.clone(), policy, context, carriers),
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
                let first = *rows
                    .first()
                    .assured("a packed payload always carries at least one member");
                let batch = batches
                    .get(first.batch_index)
                    .assured("packing positions refer to the source batches it received");
                payloads.push(PreparedPayload {
                    members: rows,
                    occurred_at: batch.execution_now(),
                    content: EncodedPayload { envelope, payload },
                });
            }
            PackedOutcome::MemberFailed { position, error } => {
                rejected.push(RejectedEmitterRecord {
                    position,
                    reason: format!(
                        "emitter '{}' failed to encode record: {error}",
                        context.emitter.as_str()
                    ),
                    structured_error: None,
                });
            }
            PackedOutcome::Oversize { position, exceeded } => {
                let batch = batches
                    .get(position.batch_index)
                    .assured("packing positions refer to the source batches it received");
                let message = format!("emitter '{}' {exceeded}", context.emitter.as_str());
                rejected.push(RejectedEmitterRecord {
                    position,
                    reason: message.clone(),
                    structured_error: Some(structured_message_error(
                        batch.execution_now(),
                        MessageErrorCode::Validation,
                        message,
                        MessageErrorOperation::Encode,
                        None,
                        std::iter::empty(),
                    )),
                });
            }
            PackedOutcome::ContainerFailed { rows, error } => {
                let first = rows
                    .first()
                    .assured("a failed container held at least one selected row");
                let batch = batches
                    .get(first.batch_index)
                    .assured("packing positions refer to the source batches it received");
                let shared = container_failure(context, &codec, batch, rows.len(), error);
                for position in rows {
                    let batch = batches
                        .get(position.batch_index)
                        .assured("packing positions refer to the source batches it received");
                    let mut member_error = shared.clone();
                    member_error.occurred_at = batch.execution_now();
                    rejected.push(RejectedEmitterRecord {
                        position,
                        reason: shared.message.clone(),
                        structured_error: Some(member_error),
                    });
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
    Ok(payloads)
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
        batch.execution_now(),
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
        .relay_batch()
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

/// Packs selected rows of all released carriers, off the reactor when transformations require it.
async fn pack_pending_rows(
    codec: Arc<CompiledCodec>,
    policy: EmitterBatchPolicy,
    context: &EmitterSinkContext,
    carriers: Vec<PackingCarrier>,
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
        return pack_buffered_batches(&codec, carriers, policy).map_err(initialization_failed);
    }
    let codec_name = codec.name.as_str().to_string();
    tokio::task::spawn_blocking(move || pack_buffered_batches(&codec, carriers, policy))
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
    use std::collections::VecDeque;

    use nervix_connector::{PerRecordOutcome, SinkPublishError, SinkRecordId};
    use nervix_models::{
        BatchMessageLimit, CodecWireFormat, CreateCodec, CreateWireSchema, JsonType,
        ResolvedCodecWireFormat, WireSchemaField,
    };
    use parking_lot::Mutex;

    use super::*;
    use crate::{
        runtime::test_fixtures::{input_batch_with, input_schema, named, sink_context},
        runtime_schema::compile_codec,
    };

    /// How the scripted sink answers one write.
    enum Answer {
        /// The sink fails before it answers for any record, as a stalled broker does.
        Stall,
        /// The sink confirms every record of the write.
        ConfirmAll,
    }

    /// A record sink that keeps every payload it was handed and answers each write from a script.
    struct ScriptedSink {
        writes: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
        answers: VecDeque<Answer>,
    }

    impl SinkLifecycle for ScriptedSink {}

    #[async_trait]
    impl RecordSink for ScriptedSink {
        async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
            self.writes.lock().push(
                records
                    .iter()
                    .map(|record| record.payload.clone())
                    .collect(),
            );
            let mut outcome = PerRecordOutcome::with_capacity(records.len());
            let answer = self
                .answers
                .pop_front()
                .expect("the test scripts an answer for every write it makes");
            match answer {
                Answer::Stall => {
                    outcome.fail(Report::new(SinkPublishError::Publish { sink: "scripted" }));
                }
                Answer::ConfirmAll => {
                    for record in &records {
                        outcome.deliver(record.id);
                    }
                }
            }
            outcome
        }
    }

    fn json_codec() -> Arc<CompiledCodec> {
        let wire = CreateWireSchema {
            name: named("input_wire"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: named("value"),
                ty: JsonType::Integer,
                optional: false,
            }],
        };
        let model = CreateCodec {
            name: named("input_codec"),
            wire_format: CodecWireFormat::Json {
                wire_schema: wire.name.clone(),
            },
            schema: named("emitter_input"),
            encoding_rules: Vec::new(),
        };
        compile_codec(&model, input_schema(), ResolvedCodecWireFormat::Json(&wire))
            .expect("the test codec and Arrow schema both define one required integer field")
    }

    fn two_rows(first: i64, second: i64) -> EmitterPublishBatch {
        let messages = [first, second]
            .into_iter()
            .map(|value| RelayMessage {
                key: None,
                record: test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]),
                acks: AckSet::empty(),
            })
            .collect();
        EmitterPublishBatch::from_batch(
            RelayRecordBatch::from_messages(input_schema(), messages)
                .expect("the test rows match the emitter input schema"),
            Timestamp::from_unix_nanos(100),
        )
    }

    fn one_row(value: i64) -> EmitterPublishBatch {
        EmitterPublishBatch::from_batch(
            input_batch_with(value, 0, AckSet::empty()),
            Timestamp::from_unix_nanos(100),
        )
    }

    #[tokio::test]
    async fn a_retry_writes_the_retained_payloads_unchanged_before_packing_new_rows() {
        let context = sink_context();
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut sink = EncodedRecordSink {
            sink: Box::new(ScriptedSink {
                writes: writes.clone(),
                answers: VecDeque::from([Answer::Stall, Answer::ConfirmAll]),
            }),
            codec: json_codec(),
            batch: Some(EmitterBatchPolicy {
                max_messages: BatchMessageLimit::try_from(2_u32)
                    .expect("two is a positive message limit"),
                max_size: "1KiB".parse().expect("1KiB is a positive byte limit"),
            }),
        };
        let mut batches = vec![two_rows(1, 2), one_row(3)];
        let mut prepared = PreparedPayloads::default();

        let stalled = sink
            .publish_batches(
                &context,
                EmitterPublication {
                    batches: &mut batches,
                    payloads: &mut prepared,
                    requests: &mut PreparedPayloads::default(),
                },
            )
            .await
            .expect_err("a stalled write leaves its payloads unresolved");
        assert!(emitter_publish_error_is_retryable(&stalled));
        assert!(
            batches
                .iter()
                .all(|batch| batch.pending_record_rows().is_empty())
        );

        // A batch buffered after the stalled write is packed on its own: the retained payload of
        // row 3 is written as it was, not regrouped with row 4.
        batches.push(one_row(4));
        sink.publish_batches(
            &context,
            EmitterPublication {
                batches: &mut batches,
                payloads: &mut prepared,
                requests: &mut PreparedPayloads::default(),
            },
        )
        .await
        .expect("the retry is confirmed");

        assert_eq!(
            *writes.lock(),
            vec![
                vec![
                    br#"[{"value":1},{"value":2}]"#.to_vec(),
                    br#"[{"value":3}]"#.to_vec()
                ],
                vec![
                    br#"[{"value":1},{"value":2}]"#.to_vec(),
                    br#"[{"value":3}]"#.to_vec(),
                    br#"[{"value":4}]"#.to_vec(),
                ],
            ]
        );
        assert!(prepared.is_empty());
        assert!(
            batches
                .iter()
                .all(|batch| batch.resolved_rows().iter().all(|resolved| *resolved))
        );
    }

    #[tokio::test]
    async fn one_record_per_row_is_encoded_again_after_a_stalled_write() {
        let context = sink_context();
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut sink = EncodedRecordSink {
            sink: Box::new(ScriptedSink {
                writes: writes.clone(),
                answers: VecDeque::from([Answer::Stall, Answer::ConfirmAll]),
            }),
            codec: json_codec(),
            batch: None,
        };
        let mut batches = vec![two_rows(1, 2)];
        let mut prepared = PreparedPayloads::default();

        let stalled = sink
            .publish_batches(
                &context,
                EmitterPublication {
                    batches: &mut batches,
                    payloads: &mut prepared,
                    requests: &mut PreparedPayloads::default(),
                },
            )
            .await
            .expect_err("a stalled write leaves its rows unresolved");
        assert!(emitter_publish_error_is_retryable(&stalled));
        assert_eq!(batches[0].pending_record_rows(), vec![0, 1]);
        sink.publish_batches(
            &context,
            EmitterPublication {
                batches: &mut batches,
                payloads: &mut prepared,
                requests: &mut PreparedPayloads::default(),
            },
        )
        .await
        .expect("the retry is confirmed");

        let expected = vec![br#"{"value":1}"#.to_vec(), br#"{"value":2}"#.to_vec()];
        assert_eq!(*writes.lock(), vec![expected.clone(), expected]);
        assert!(prepared.is_empty());
        assert_eq!(batches[0].resolved_rows(), vec![true, true]);
    }
}
