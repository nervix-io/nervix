//! The records one write hands a sink, and what the sink's answers for them resolve.
//!
//! Layer: data plane.
//! - **Owns.** The identity a sink answers for each record of a write under and the source rows
//!   each record carries: one row, every member of a batch payload, or the one row of a prepared
//!   HTTP request, which the emitter retains verbatim — bytes, envelope or request fields, and
//!   members — until the sink answers for it. Checking a sink's answers against the write they
//!   answer, and applying them: a delivered record delivers every row it carries, a rejected one
//!   rejects each of them with the one error the sink gave it, and a payload the sink left
//!   unanswered stays retained for the next attempt.
//! - **Depends on.** The emitter's buffered batches and the state of their rows, the request fields
//!   an HTTP emitter admitted a row with, the connector contract's record identity, written values
//!   and outcome, and the node's message-error delivery.
//! - **Must not know.** How rows are encoded or packed into payloads, how request fields were
//!   evaluated, which connector writes them, or when the emitter retries.

use std::collections::BTreeMap;

use nervix_connector::{
    PerRecordOutcome, RejectedSinkRecord, SinkHttpRequest, SinkRecord, SinkRecordId,
    SinkRecordPosition,
};

use super::{emitter_batch_packing::BatchEnvelope, *};

/// The guarantee every lookup of an answered record relies on.
const CHECKED_RECORD: &str =
    "CheckedAnswers::check admits only records this write handed over, each answered once";

/// A record sink's answers for one write, checked against the records the write handed over.
struct CheckedAnswers {
    delivered: Vec<SinkRecordId>,
    rejected: Vec<RejectedSinkRecord<SinkRecordId>>,
    /// Why the write left records unresolved: the failure the sink reported, or, when it reported
    /// none, how many records it did not answer for. Nothing says an unanswered record was not
    /// written, so either way the attempt failed in a way the emitter retries.
    unresolved: Option<Report<EmitterRuntimeError>>,
}

impl CheckedAnswers {
    /// Checks that `outcome` answers only for the `records` the write handed over, and for each
    /// of them at most once.
    fn check(
        outcome: PerRecordOutcome<SinkRecordId>,
        records: usize,
    ) -> EmitterRuntimeResult<Self> {
        let parts = outcome.into_parts();
        let mut answered = vec![false; records];
        for record in &parts.delivered {
            Self::record_answer(&mut answered, *record)?;
        }
        for rejected in &parts.rejected {
            Self::record_answer(&mut answered, rejected.id)?;
        }
        let answered_records = parts
            .delivered
            .len()
            .checked_add(parts.rejected.len())
            .assured("both counts total answers this node already holds in memory");
        let unresolved = match parts.infrastructure_error {
            Some(error) => Some(sink_publish_failure(error)),
            None if answered_records < records => {
                let unanswered = records
                    .checked_sub(answered_records)
                    .verified("the guard above admits fewer answers than records");
                Some(Report::new(EmitterRuntimeError::UnansweredSinkRecords {
                    unanswered,
                    records,
                }))
            }
            None => None,
        };
        Ok(Self {
            delivered: parts.delivered,
            rejected: parts.rejected,
            unresolved,
        })
    }

    fn record_answer(answered: &mut [bool], record: SinkRecordId) -> EmitterRuntimeResult<()> {
        let records = answered.len();
        let Some(slot) = answered.get_mut(record.index()) else {
            return Err(Report::new(EmitterRuntimeError::UnknownSinkRecord {
                record: record.index(),
                records,
            }));
        };
        if *slot {
            return Err(Report::new(EmitterRuntimeError::SinkRecordAnsweredTwice {
                record: record.index(),
            }));
        }
        *slot = true;
        Ok(())
    }
}

/// What one write resolved, by the source row each answer applies to.
#[derive(Debug)]
pub(super) struct RowAnswers {
    delivered: Vec<SinkRecordPosition>,
    rejected: Vec<RejectedEmitterRecord>,
    /// Why the write left rows unresolved, which the emitter retries.
    unresolved: Option<Report<EmitterRuntimeError>>,
}

impl From<PerRecordOutcome<SinkRecordPosition>> for RowAnswers {
    /// A row sink's answers, which name every row by its source position already.
    fn from(outcome: PerRecordOutcome<SinkRecordPosition>) -> Self {
        let parts = outcome.into_parts();
        let mut rejected = Vec::with_capacity(parts.rejected.len());
        for RejectedSinkRecord { id, error } in parts.rejected {
            rejected.push(RejectedEmitterRecord {
                position: id,
                reason: String::new(),
                structured_error: Some(error),
            });
        }
        Self {
            delivered: parts.delivered,
            rejected,
            unresolved: parts.infrastructure_error.map(sink_publish_failure),
        }
    }
}

impl RowAnswers {
    /// Applies these answers to the rows they name: `acknowledgements` says who acknowledges a
    /// delivered row, and a rejected row follows the emitter's message error policy. The write's
    /// failure, when it has one, is returned once both are applied.
    pub(super) async fn apply(
        self,
        context: &EmitterSinkContext,
        batches: &mut [EmitterPublishBatch],
        acknowledgements: DeliveredAcknowledgements,
    ) -> EmitterRuntimeResult<()> {
        for SinkRecordPosition {
            batch_index,
            row_index,
        } in self.delivered
        {
            let batch = batches.get_mut(batch_index).ok_or_else(|| {
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "sink confirmation references missing emitter batch {batch_index}"
                ))
            })?;
            batch.mark_delivered(row_index, acknowledgements)?;
        }
        finish_rejected_records(
            context,
            batches,
            self.rejected,
            MessageErrorOperation::Publish,
        )
        .await?;
        match self.unresolved {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// The source row each record of a write carries, for a write whose records carry one row each.
///
/// An emitter without `BATCH` publishes one record per row and retains nothing: the next attempt
/// encodes a row the sink left unanswered again, exactly as it encoded it the first time.
#[derive(Debug, Default)]
pub(super) struct RowRecords {
    rows: Vec<SinkRecordPosition>,
}

impl RowRecords {
    pub(super) fn with_capacity(records: usize) -> Self {
        Self {
            rows: Vec::with_capacity(records),
        }
    }

    /// The identity the record carrying `row` is handed over under.
    pub(super) fn record(&mut self, row: SinkRecordPosition) -> SinkRecordId {
        let record = SinkRecordId::new(self.rows.len());
        self.rows.push(row);
        record
    }

    /// The sink's answers for this write, by the row each record carries.
    pub(super) fn answers(
        &self,
        outcome: PerRecordOutcome<SinkRecordId>,
    ) -> EmitterRuntimeResult<RowAnswers> {
        let checked = CheckedAnswers::check(outcome, self.rows.len())?;
        let mut delivered = Vec::with_capacity(checked.delivered.len());
        for record in checked.delivered {
            delivered.push(self.row(record));
        }
        let mut rejected = Vec::with_capacity(checked.rejected.len());
        for RejectedSinkRecord { id, error } in checked.rejected {
            rejected.push(RejectedEmitterRecord {
                position: self.row(id),
                reason: String::new(),
                structured_error: Some(error),
            });
        }
        Ok(RowAnswers {
            delivered,
            rejected,
            unresolved: checked.unresolved,
        })
    }

    fn row(&self, record: SinkRecordId) -> SinkRecordPosition {
        *self.rows.get(record.index()).verified(CHECKED_RECORD)
    }
}

/// What a prepared payload hands its sink on every attempt, prepared once and written unchanged.
pub(super) trait PreparedContent {
    /// The value one write hands the sink for the payload.
    type Written;

    /// The value the write hands the sink for this payload under `record`. The sink takes its own
    /// copy, so the payload stays retained until an answer resolves it.
    fn written(&self, record: SinkRecordId, occurred_at: Timestamp) -> Self::Written;
}

/// A batch payload for a record sink: the envelope every member shares and exactly the bytes the
/// codec produced.
#[derive(Debug)]
pub(super) struct EncodedPayload {
    /// The key, headers and ordering group every member shares.
    pub(super) envelope: BatchEnvelope,
    /// Exactly the bytes the codec produced. Every attempt writes these bytes, so a duplicate a
    /// retry produces is the payload the destination may already hold.
    pub(super) payload: Vec<u8>,
}

impl PreparedContent for EncodedPayload {
    type Written = SinkRecord;

    fn written(&self, record: SinkRecordId, occurred_at: Timestamp) -> SinkRecord {
        let sink_record = SinkRecord::new(
            record,
            self.envelope.key.clone(),
            self.payload.clone(),
            self.envelope.headers.clone(),
            occurred_at,
        );
        match &self.envelope.message_group {
            Some(message_group) => sink_record.with_message_group(message_group.clone()),
            None => sink_record,
        }
    }
}

/// An HTTP request for an HTTP sink: the request fields its one record was admitted with, and its
/// body.
#[derive(Debug)]
pub(super) struct PreparedHttpRequest {
    pub(super) fields: HttpRequestFields,
    /// Exactly the bytes the codec produced, or nothing for an emitter declared `WITHOUT BODY`.
    /// Every attempt sends these bytes, so the codec never runs again for a retry.
    pub(super) body: Option<Vec<u8>>,
}

impl PreparedContent for PreparedHttpRequest {
    type Written = SinkHttpRequest;

    fn written(&self, record: SinkRecordId, occurred_at: Timestamp) -> SinkHttpRequest {
        SinkHttpRequest {
            id: record,
            method: self.fields.method.clone(),
            target: self.fields.target.clone(),
            headers: self.fields.headers.clone(),
            body: self.body.clone(),
            occurred_at,
        }
    }
}

/// A payload prepared for a sink, retained with everything it is written with until the sink
/// answers for it.
#[derive(Debug)]
pub(super) struct PreparedPayload<Content> {
    /// The source rows the payload carries, in packing order. A payload always carries one.
    pub(super) members: Vec<SinkRecordPosition>,
    /// The execution time of the batch the first member came from, which the sink reports the
    /// payload's rejection with. Each member's own replaces it when the rejection is routed.
    pub(super) occurred_at: Timestamp,
    /// What every attempt hands the sink for the payload.
    pub(super) content: Content,
}

impl<Content: PreparedContent> PreparedPayload<Content> {
    fn first_member(&self) -> SinkRecordPosition {
        *self
            .members
            .first()
            .assured("a payload is prepared only from a candidate with at least one member")
    }

    /// The value one write hands the sink for this payload.
    fn written(&self, record: SinkRecordId) -> Content::Written {
        self.content.written(record, self.occurred_at)
    }
}

/// The payloads prepared for a sink and not yet answered for, in packing order.
///
/// Payloads are keyed by their first member. No two payloads share a member, and every member of a
/// payload follows the members of the payloads packed before it, so their first members order them
/// the way they were packed. A retry therefore writes the retained payloads in their own order and
/// before any payload packed after them.
#[derive(Debug)]
pub(super) struct PreparedPayloads<Content> {
    retained: BTreeMap<SinkRecordPosition, PreparedPayload<Content>>,
}

impl<Content> Default for PreparedPayloads<Content> {
    fn default() -> Self {
        Self {
            retained: BTreeMap::new(),
        }
    }
}

/// The values one write hands a sink for the retained payloads, and which payload each carries.
pub(super) struct PreparedWrite<Written> {
    pub(super) records: Vec<Written>,
    pub(super) payloads: WrittenPayloads,
}

/// The payload each record of one write carries, by the record's identity.
pub(super) struct WrittenPayloads {
    first_members: Vec<SinkRecordPosition>,
}

impl<Content: PreparedContent> PreparedPayloads<Content> {
    pub(super) fn is_empty(&self) -> bool {
        self.retained.is_empty()
    }

    pub(super) fn clear(&mut self) {
        self.retained.clear();
    }

    /// Retains `payload` and marks every row it carries as prepared, so no later attempt packs a
    /// member again while the payload waits for its answer.
    pub(super) fn retain(
        &mut self,
        payload: PreparedPayload<Content>,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()> {
        for member in &payload.members {
            let batch = batches.get_mut(member.batch_index).ok_or_else(|| {
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "a prepared payload references missing emitter batch {}",
                    member.batch_index
                ))
            })?;
            batch.mark_prepared(member.row_index)?;
        }
        self.retained.insert(payload.first_member(), payload);
        Ok(())
    }

    /// One record for every retained payload, in packing order, each carrying what its payload
    /// was prepared with.
    pub(super) fn next_write(&self) -> PreparedWrite<Content::Written> {
        let mut records = Vec::with_capacity(self.retained.len());
        let mut first_members = Vec::with_capacity(self.retained.len());
        for (first_member, payload) in &self.retained {
            records.push(payload.written(SinkRecordId::new(records.len())));
            first_members.push(*first_member);
        }
        PreparedWrite {
            records,
            payloads: WrittenPayloads { first_members },
        }
    }

    /// The sink's answers for one write, applied to the members of the payloads they name.
    ///
    /// A confirmed payload delivers every member here, in the same step that stops retaining it. A
    /// rejected payload rejects every member with the error the sink gave it, so its one reference
    /// shows that they failed together, while each member keeps its own execution time, branch and
    /// acknowledgement; its members wait for their message errors as pending rows, so a delivery
    /// the stop deadline cuts short leaves them for the next attempt rather than for a payload that
    /// is gone. A payload the sink left unanswered stays retained with its bytes and members, and
    /// the answers carry the write's failure.
    pub(super) fn answers(
        &mut self,
        batches: &mut [EmitterPublishBatch],
        payloads: WrittenPayloads,
        outcome: PerRecordOutcome<SinkRecordId>,
    ) -> EmitterRuntimeResult<RowAnswers> {
        let checked = CheckedAnswers::check(outcome, payloads.first_members.len())?;
        for record in checked.delivered {
            let payload = self.take(&payloads, record);
            for member in payload.members {
                let batch = batches.get_mut(member.batch_index).ok_or_else(|| {
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "sink confirmation references missing emitter batch {}",
                        member.batch_index
                    ))
                })?;
                batch.mark_delivered(member.row_index, DeliveredAcknowledgements::Host)?;
            }
        }
        let mut rejected = Vec::new();
        for RejectedSinkRecord { id, error } in checked.rejected {
            let payload = self.take(&payloads, id);
            for member in payload.members {
                let batch = batches.get_mut(member.batch_index).ok_or_else(|| {
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "sink rejection references missing emitter batch {}",
                        member.batch_index
                    ))
                })?;
                batch.release_prepared(member.row_index)?;
                let mut member_error = error.clone();
                member_error.occurred_at = batch.execution_now();
                rejected.push(RejectedEmitterRecord {
                    position: member,
                    reason: String::new(),
                    structured_error: Some(member_error),
                });
            }
        }
        Ok(RowAnswers {
            delivered: Vec::new(),
            rejected,
            unresolved: checked.unresolved,
        })
    }

    /// Takes the payload `record` carried out of the retained ones, now that the sink answered for
    /// it.
    fn take(
        &mut self,
        payloads: &WrittenPayloads,
        record: SinkRecordId,
    ) -> PreparedPayload<Content> {
        let first_member = payloads
            .first_members
            .get(record.index())
            .verified(CHECKED_RECORD);
        self.retained.remove(first_member).verified(
            "the write was built from the retained payloads, and an answered payload leaves them \
             only here, once",
        )
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "emitter_record_writes_shuttle_tests.rs"]
mod shuttle_tests;

#[cfg(test)]
mod tests {
    use nervix_connector::SinkPublishError;

    use super::*;
    use crate::runtime::test_fixtures::{input_schema, sink_context};

    /// One buffered batch whose rows each carry an acknowledgement root of their own, and the
    /// completions of those roots in row order.
    fn batch(values: &[i64], execution_now: i64) -> (EmitterPublishBatch, Vec<AckCompletion>) {
        let mut messages = Vec::with_capacity(values.len());
        let mut completions = Vec::with_capacity(values.len());
        for value in values {
            let (acks, completion) = AckSet::root();
            messages.push(RelayMessage {
                key: None,
                record: test_runtime_row([("value".to_string(), RuntimeValue::I64(*value))]),
                acks,
            });
            completions.push(completion);
        }
        let batch = RelayRecordBatch::from_messages(input_schema(), messages)
            .expect("the test rows match the emitter input schema");
        (
            EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(execution_now)),
            completions,
        )
    }

    fn position(batch_index: usize, row_index: usize) -> SinkRecordPosition {
        SinkRecordPosition {
            batch_index,
            row_index,
        }
    }

    fn payload(members: &[SinkRecordPosition], bytes: &str) -> PreparedPayload<EncodedPayload> {
        PreparedPayload {
            members: members.to_vec(),
            occurred_at: Timestamp::from_unix_nanos(1),
            content: EncodedPayload {
                envelope: BatchEnvelope {
                    key: Some("key".to_string()),
                    headers: vec![("source".to_string(), "a".to_string())],
                    message_group: Some("group".to_string()),
                },
                payload: bytes.as_bytes().to_vec(),
            },
        }
    }

    fn written(write: &PreparedWrite<SinkRecord>) -> Vec<(usize, Vec<u8>)> {
        write
            .records
            .iter()
            .map(|record| (record.id.index(), record.payload.clone()))
            .collect()
    }

    #[tokio::test]
    async fn a_confirmed_payload_delivers_every_member_and_an_unanswered_one_is_written_unchanged()
    {
        let (first, first_completions) = batch(&[1, 2, 3], 10);
        let (second, second_completions) = batch(&[4, 5], 20);
        let mut batches = vec![first, second];
        let mut prepared = PreparedPayloads::default();
        for retained in [
            payload(&[position(0, 0), position(0, 1)], "[1,2]"),
            payload(&[position(0, 2), position(1, 0)], "[3,4]"),
            payload(&[position(1, 1)], "[5]"),
        ] {
            prepared
                .retain(retained, &mut batches)
                .expect("every member is a pending row of a buffered batch");
        }
        assert!(
            batches
                .iter()
                .all(|batch| batch.pending_record_rows().is_empty())
        );

        let write = prepared.next_write();
        assert_eq!(
            written(&write),
            vec![
                (0, b"[1,2]".to_vec()),
                (1, b"[3,4]".to_vec()),
                (2, b"[5]".to_vec()),
            ]
        );
        let first_record = write
            .records
            .first()
            .expect("the write carries three records");
        assert_eq!(first_record.key.as_deref(), Some("key"));
        assert_eq!(first_record.message_group.as_deref(), Some("group"));
        assert_eq!(
            first_record.headers,
            vec![("source".to_string(), "a".to_string())]
        );
        let mut outcome = PerRecordOutcome::with_capacity(1);
        outcome.deliver(SinkRecordId::new(0));
        outcome.fail(Report::new(SinkPublishError::Publish { sink: "test" }));

        let answers = prepared
            .answers(&mut batches, write.payloads, outcome)
            .expect("the answers name records of this write");
        assert!(answers.rejected.is_empty());
        let unresolved = answers
            .unresolved
            .expect("the sink failed before answering for every record");
        assert!(emitter_publish_error_is_retryable(&unresolved));
        let [first_row, second_row, third_row] = first_completions
            .try_into()
            .expect("the first batch has three rows");
        assert_eq!(first_row.wait().await, AckOutcome::Ack);
        assert_eq!(second_row.wait().await, AckOutcome::Ack);
        assert_eq!(batches[0].resolved_rows(), vec![true, true, false]);
        assert_eq!(batches[1].resolved_rows(), vec![false, false]);

        let retry = prepared.next_write();
        assert_eq!(
            written(&retry),
            vec![(0, b"[3,4]".to_vec()), (1, b"[5]".to_vec())],
            "a retry writes exactly the unanswered payloads, in their order"
        );
        let mut outcome = PerRecordOutcome::with_capacity(2);
        outcome.deliver(SinkRecordId::new(1));
        outcome.deliver(SinkRecordId::new(0));
        let answers = prepared
            .answers(&mut batches, retry.payloads, outcome)
            .expect("the answers name records of this write");
        assert!(answers.unresolved.is_none());
        assert!(prepared.is_empty());
        assert_eq!(third_row.wait().await, AckOutcome::Ack);
        for completion in second_completions {
            assert_eq!(completion.wait().await, AckOutcome::Ack);
        }
    }

    #[tokio::test]
    async fn a_rejected_payload_rejects_every_member_with_one_reference_and_its_own_time() {
        let (first, first_completions) = batch(&[1], 10);
        let (second, second_completions) = batch(&[2], 20);
        let mut batches = vec![first, second];
        let mut prepared = PreparedPayloads::default();
        prepared
            .retain(
                payload(&[position(0, 0), position(1, 0)], "[1,2]"),
                &mut batches,
            )
            .expect("both members are pending rows of buffered batches");
        let write = prepared.next_write();
        let mut outcome = PerRecordOutcome::with_capacity(0);
        outcome.reject(RejectedSinkRecord::external(
            SinkRecordId::new(0),
            Timestamp::from_unix_nanos(1),
            "refused".to_string(),
        ));

        let answers = prepared
            .answers(&mut batches, write.payloads, outcome)
            .expect("the answers name records of this write");

        assert!(prepared.is_empty());
        assert!(answers.unresolved.is_none());
        let rejected = answers
            .rejected
            .iter()
            .map(|rejected| {
                let error = rejected
                    .structured_error
                    .as_ref()
                    .expect("a sink rejection carries its structured error");
                (rejected.position, error.reference, error.occurred_at)
            })
            .collect::<Vec<_>>();
        let reference = rejected[0].1;
        assert_eq!(
            rejected,
            vec![
                (position(0, 0), reference, Timestamp::from_unix_nanos(10)),
                (position(1, 0), reference, Timestamp::from_unix_nanos(20)),
            ]
        );
        assert_eq!(
            batches[0].pending_record_rows(),
            vec![0],
            "a member waits for its message error as a pending row"
        );

        answers
            .apply(
                &sink_context(),
                &mut batches,
                DeliveredAcknowledgements::Host,
            )
            .await
            .expect("delivering both message errors succeeds");
        assert!(
            batches
                .iter()
                .all(|batch| batch.resolved_rows() == vec![true])
        );
        for completion in first_completions.into_iter().chain(second_completions) {
            assert_eq!(
                completion.wait().await,
                AckOutcome::NoAck("refused".to_string())
            );
        }
    }

    #[test]
    fn retained_payloads_are_written_in_packing_order() {
        let (first, _first_completions) = batch(&[1], 10);
        let (second, _second_completions) = batch(&[2], 20);
        let mut batches = vec![first, second];
        let mut prepared = PreparedPayloads::default();
        prepared
            .retain(payload(&[position(1, 0)], "later"), &mut batches)
            .expect("the member is a pending row");
        prepared
            .retain(payload(&[position(0, 0)], "earlier"), &mut batches)
            .expect("the member is a pending row");

        assert_eq!(
            written(&prepared.next_write()),
            vec![(0, b"earlier".to_vec()), (1, b"later".to_vec())]
        );
    }

    #[test]
    fn a_row_is_carried_by_one_payload_at_a_time() {
        let (first, _completions) = batch(&[1, 2], 10);
        let mut batches = vec![first];
        let mut prepared = PreparedPayloads::default();
        prepared
            .retain(payload(&[position(0, 0)], "first"), &mut batches)
            .expect("the member is a pending row");

        let error = prepared
            .retain(
                payload(&[position(0, 1), position(0, 0)], "second"),
                &mut batches,
            )
            .expect_err("a prepared row cannot join a second payload");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::RowAlreadyPrepared { row: 0 }
        );
        let missing = prepared
            .retain(payload(&[position(1, 0)], "missing"), &mut batches)
            .expect_err("a member must be a row of a buffered batch");
        assert_eq!(*missing.current_context(), EmitterRuntimeError::EncodeBatch);
    }

    #[test]
    fn answers_for_records_outside_the_write_or_given_twice_break_the_contract() {
        let (first, _completions) = batch(&[1, 2], 10);
        let mut batches = vec![first];
        let mut prepared = PreparedPayloads::default();
        prepared
            .retain(payload(&[position(0, 0)], "first"), &mut batches)
            .expect("the member is a pending row");
        prepared
            .retain(payload(&[position(0, 1)], "second"), &mut batches)
            .expect("the member is a pending row");

        let mut unknown = PerRecordOutcome::with_capacity(1);
        unknown.deliver(SinkRecordId::new(5));
        let error = prepared
            .answers(&mut batches, prepared.next_write().payloads, unknown)
            .expect_err("record 5 is not part of a two-record write");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::UnknownSinkRecord {
                record: 5,
                records: 2,
            }
        );

        let mut twice = PerRecordOutcome::with_capacity(1);
        twice.deliver(SinkRecordId::new(0));
        twice.reject(RejectedSinkRecord::external(
            SinkRecordId::new(0),
            Timestamp::from_unix_nanos(1),
            "refused".to_string(),
        ));
        let error = prepared
            .answers(&mut batches, prepared.next_write().payloads, twice)
            .expect_err("one record has one answer");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::SinkRecordAnsweredTwice { record: 0 }
        );
        assert!(!emitter_publish_error_is_retryable(&error));
        assert_eq!(
            prepared.next_write().records.len(),
            2,
            "an outcome that breaks the contract resolves nothing"
        );
        assert_eq!(batches[0].resolved_rows(), vec![false, false]);
    }

    #[test]
    fn a_write_left_partly_unanswered_without_a_failure_is_retried() {
        let (first, _completions) = batch(&[1, 2], 10);
        let mut batches = vec![first];
        let mut prepared = PreparedPayloads::default();
        prepared
            .retain(payload(&[position(0, 0)], "first"), &mut batches)
            .expect("the member is a pending row");
        prepared
            .retain(payload(&[position(0, 1)], "second"), &mut batches)
            .expect("the member is a pending row");
        let mut outcome = PerRecordOutcome::with_capacity(1);
        outcome.deliver(SinkRecordId::new(0));

        let answers = prepared
            .answers(&mut batches, prepared.next_write().payloads, outcome)
            .expect("the answer names a record of this write");

        let unresolved = answers
            .unresolved
            .expect("an unanswered record leaves the write unresolved");
        assert_eq!(
            *unresolved.current_context(),
            EmitterRuntimeError::UnansweredSinkRecords {
                unanswered: 1,
                records: 2,
            }
        );
        assert!(emitter_publish_error_is_retryable(&unresolved));
        assert_eq!(
            written(&prepared.next_write()),
            vec![(0, b"second".to_vec())]
        );
    }

    #[test]
    fn row_records_answer_for_the_row_each_record_carries() {
        let mut rows = RowRecords::with_capacity(2);
        let rejected_record = rows.record(position(0, 3));
        let delivered_record = rows.record(position(2, 1));
        let mut outcome = PerRecordOutcome::with_capacity(1);
        outcome.deliver(delivered_record);
        outcome.reject(RejectedSinkRecord::external(
            rejected_record,
            Timestamp::from_unix_nanos(1),
            "refused".to_string(),
        ));

        let answers = rows
            .answers(outcome)
            .expect("both answers name records of this write");

        assert_eq!(answers.delivered, vec![position(2, 1)]);
        assert_eq!(
            answers
                .rejected
                .iter()
                .map(|rejected| rejected.position)
                .collect::<Vec<_>>(),
            vec![position(0, 3)]
        );
        assert!(answers.unresolved.is_none());
    }
}
