//! An emitter's `GIVE UP AFTER` running out while an attempt is in flight, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariant that giving up never fails a row the destination confirmed: the rows
//!   of a payload the emitter confirmed during the attempt deliver, the rows of the payload it left
//!   unanswered are given up together and fail once, and every source acknowledgement completes
//!   exactly once, however the attempt, the connector's answers and the acknowledgement observers
//!   interleave.
//! - **Depends on.** The emitter's buffer and its row states, its retained batch payloads and the
//!   answers applied to them, the give-up owner, acknowledgement sets, the fake record emitter and
//!   the server Shuttle runner.
//! - **Must not know.** Which connector writes a payload, how rows are encoded or packed, or how
//!   the emitter task schedules its attempts.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_connector::{EmitterRecordPosition, RecordEmitter as _, RecordEnvelope};
use nervix_connector_fake::{FakeEmitter, PublishAnswer, Records};
use nervix_model_harness::shuttle::check_interleavings;

use super::*;
use crate::runtime::{
    emitter_batch_packing::BatchEnvelope,
    emitter_buffer::EmitterBufferedMessages,
    emitter_record_writes::{EncodedPayload, PreparedPayload},
    test_fixtures::input_schema,
};

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";

/// The payload the modeled destination confirms: the first record of the write.
const CONFIRMED_PAYLOAD: &[u8] = b"[0,1]";
/// The payload the modeled destination never answers for: the write fails after the first record.
const UNANSWERED_PAYLOAD: &[u8] = b"[2,3]";

/// One buffered batch whose rows hold `acks`, one set per row.
fn one_batch(acks: Vec<AckSet>) -> EmitterPublishBatch {
    let mut messages = Vec::with_capacity(acks.len());
    for (value, acks) in (0_i64..).zip(acks) {
        messages.push(RelayMessage {
            key: None,
            record: test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]),
            acks,
        });
    }
    let batch = RelayRecordBatch::from_messages(input_schema(), messages)
        .assured("every modeled row matches the emitter input schema");
    EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(1))
}

fn payload(rows: &[usize], bytes: &[u8]) -> PreparedPayload<EncodedPayload> {
    let mut members = Vec::with_capacity(rows.len());
    for row in rows {
        members.push(EmitterRecordPosition {
            batch_index: 0,
            row_index: *row,
        });
    }
    PreparedPayload {
        members,
        occurred_at: Timestamp::from_unix_nanos(1),
        content: EncodedPayload {
            envelope: BatchEnvelope {
                destination: None,
                headers: EmitterHeaders::Text(Vec::new()),
                message_group: None,
                clauses: Some(Arc::new(RecordEnvelope::default())),
            },
            payload: bytes.to_vec(),
        },
    }
}

/// A batch of four rows sits in two retained payloads. An earlier attempt failed without an answer,
/// which started the batch's give-up clock with a zero limit, so the limit runs out while the next
/// attempt is in flight. That attempt's destination confirms the first payload and leaves the second
/// unanswered. The give-up takes only the unanswered rows: the confirmed ones are delivered, and each
/// source acknowledgement completes once, the confirmed ones successfully and the given-up ones with
/// the give-up's failure.
fn an_attempt_in_flight_when_the_limit_runs_out_never_fails_a_confirmed_row() {
    shuttle::future::block_on(async move {
        let mut shares = Vec::with_capacity(4);
        let mut observers = Vec::with_capacity(4);
        for _ in 0..4 {
            let (share, completion) = AckSet::root();
            shares.push(share);
            observers.push(nervix_primitives::task::spawn(async move {
                completion.wait().await
            }));
        }

        let emitter = nervix_primitives::task::spawn(async move {
            let mut buffer = EmitterBatchBuffer::reporting_to(
                Arc::new(EmitterBufferedMessages::default()),
                RuntimeFlushPolicy::Immediate {
                    minimum: nervix_models::DataPlaneSettings::default().immediate_flush_minimum(),
                },
            );
            buffer
                .retain_without_cadence(one_batch(shares))
                .assured("the modeled buffer has a flush policy");
            let EmitterPublication {
                batches, payloads, ..
            } = buffer.publication_mut();
            payloads
                .retain(payload(&[0, 1], CONFIRMED_PAYLOAD), batches)
                .assured("both members are pending rows of the buffered batch");
            payloads
                .retain(payload(&[2, 3], UNANSWERED_PAYLOAD), batches)
                .assured("both members are pending rows of the buffered batch");

            let mut give_up = EmitterGiveUp::new(Some(Duration::ZERO));
            give_up.record_failure(
                &mut buffer,
                &Report::new(EmitterRuntimeError::PublishBatch)
                    .attach_printable("the destination answered nothing"),
            );

            let mut connector = FakeEmitter::<Records>::new([PublishAnswer::DeliverFirstThenFail]);
            let write = buffer.publication_mut().payloads.next_write();
            nervix_primitives::task::yield_now().await;
            let outcome = connector.publish(write.records).await;
            let EmitterPublication {
                batches, payloads, ..
            } = buffer.publication_mut();
            let answers = payloads.answers(batches, write.payloads, outcome);
            assert!(answers.is_ok(), "the answers name records of the write");
            answers.discarded(
                "the write's failure only repeats the one the give-up owner recorded, and the \
                 rows it left unresolved are checked below",
            );
            assert_eq!(
                batches
                    .first()
                    .assured("the buffer holds its one batch")
                    .resolved_rows(),
                vec![true, true, false, false],
                "the write confirmed the first payload and left the second unanswered"
            );
            nervix_primitives::task::yield_now().await;

            give_up.collect(&mut buffer).await;
            let given_up = give_up
                .take_given_up()
                .assured("the zero limit has run out with rows unanswered");
            assert!(
                buffer.is_empty(),
                "the one batch leaves the buffer once given up"
            );
            assert!(given_up.staged.is_empty());
            let mut batches = given_up.batches;
            let batch = batches
                .pop()
                .assured("the given-up work holds the buffer's one batch");
            assert!(batches.is_empty());
            let resolved = batch.resolved_rows();
            assert_eq!(
                resolved,
                vec![true, true, false, false],
                "the confirmed rows are delivered and only the unanswered ones are given up"
            );
            // The emitter's message-error policy fails every unresolved row once.
            for (acks, resolved) in batch.relay_batch().acks.iter().zip(resolved) {
                if !resolved {
                    acks.no_ack(given_up.message.clone());
                }
            }
            given_up.message
        });

        let message = emitter.await.assured(MODEL_TASK_JOINS);
        assert!(
            message.starts_with("gave up after 0s of retries: the destination answered nothing"),
            "{message}"
        );
        for (row, observer) in observers.into_iter().enumerate() {
            let outcome = observer.await.assured(MODEL_TASK_JOINS);
            let expected = if row < 2 {
                AckOutcome::Ack
            } else {
                AckOutcome::NoAck(message.clone())
            };
            assert_eq!(outcome, expected, "row {row}");
        }
    });
}

#[test]
fn shuttle_an_attempt_in_flight_when_the_limit_runs_out_never_fails_a_confirmed_row() {
    check_interleavings(an_attempt_in_flight_when_the_limit_runs_out_never_fails_a_confirmed_row);
}
