//! A batching emitter's retained payloads and the acknowledgements their members hold, explored
//! under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants the members of a batch payload are held to while the sink's answers,
//!   a retry, a cancelled attempt, terminal shutdown, a sibling emitter and a drain race one
//!   another: every member resolves once, no source acknowledgement completes before every
//!   attached emitter confirmed it, a retry writes a retained payload with exactly the bytes it
//!   was first written with, and a drain never reads the emitter empty while one of its members
//!   is unresolved.
//! - **Depends on.** The emitter's buffer, its buffered batches and their row states, its retained
//!   batch payloads and the answers applied to them, acknowledgement sets, and the server Shuttle
//!   runner.
//! - **Must not know.** Which connector writes a payload, how rows are encoded or packed, or how the
//!   emitter task schedules its attempts.

// The standard library's atomics are not Shuttle scheduling points, so each record below changes in
// the same scheduling step as the operation it records.
use std::sync::{
    Arc as StdArc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::SinkPublishError;
use nervix_recovery::NoReceiver as _;

use super::*;
use crate::{
    runtime::test_fixtures::input_schema, runtime_ack::AckProgress,
    shuttle_test::check_interleavings,
};

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";
const MODEL_ROWS: &str = "every modeled position names a row of the one modeled batch";

/// The payload the modeled sink confirms whenever it is written.
const CONFIRMED_PAYLOAD: &[u8] = b"[0,1]";
/// The payload the modeled sink rejects whenever it is written.
const REJECTED_PAYLOAD: &[u8] = b"[2,3]";

fn position(row_index: usize) -> SinkRecordPosition {
    SinkRecordPosition {
        batch_index: 0,
        row_index,
    }
}

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
        members.push(position(*row));
    }
    PreparedPayload {
        members,
        occurred_at: Timestamp::from_unix_nanos(1),
        content: EncodedPayload {
            envelope: BatchEnvelope {
                key: None,
                headers: Vec::new(),
                message_group: None,
            },
            payload: bytes.to_vec(),
        },
    }
}

/// The answers of a sink that stalled before it answered for any record of the write.
fn stalled() -> PerRecordOutcome<SinkRecordId> {
    let mut outcome = PerRecordOutcome::with_capacity(0);
    outcome.fail(Report::new(SinkPublishError::Publish { sink: "modeled" }));
    outcome
}

/// The answers of the modeled sink, which confirms one payload and rejects the other by what the
/// record carries.
fn answered(records: &[SinkRecord]) -> PerRecordOutcome<SinkRecordId> {
    let mut outcome = PerRecordOutcome::with_capacity(records.len());
    for record in records {
        if record.payload == CONFIRMED_PAYLOAD {
            outcome.deliver(record.id);
        } else {
            outcome.reject(record.rejected("refused".to_string()));
        }
    }
    outcome
}

/// How the sibling emitter that shares every source message ends.
#[derive(Clone, Copy)]
enum SiblingEnd {
    Confirmed,
    Failed,
}

/// Two source messages fan out to a batching emitter and a sibling emitter. The batching emitter
/// writes one payload of both, learns nothing, and writes it again, while the sibling resolves its
/// own shares. Each source acknowledgement completes once: successfully only after both emitters
/// confirmed it, and the retry writes exactly the bytes of the first attempt.
fn fanned_in_members_resolve_once(sibling_end: SiblingEnd) {
    shuttle::future::block_on(async move {
        let emitter_confirmed = StdArc::new(AtomicBool::new(false));
        let sibling_confirmed = StdArc::new(AtomicBool::new(false));
        let mut sibling_shares = Vec::with_capacity(2);
        let mut emitter_shares = Vec::with_capacity(2);
        let mut observers = Vec::with_capacity(2);
        for _ in 0..2 {
            let (root, completion) = AckSet::root();
            emitter_shares.push(root.attached());
            sibling_shares.push(root);
            let emitter_confirmed = emitter_confirmed.clone();
            let sibling_confirmed = sibling_confirmed.clone();
            observers.push(tokio::spawn(async move {
                let outcome = completion.wait().await;
                if outcome == AckOutcome::Ack {
                    assert!(
                        emitter_confirmed.load(Ordering::SeqCst),
                        "a source acknowledgement completed before the batching emitter's sink \
                         confirmed the payload carrying it"
                    );
                    assert!(
                        sibling_confirmed.load(Ordering::SeqCst),
                        "a source acknowledgement completed before the sibling emitter confirmed \
                         it"
                    );
                }
                outcome
            }));
        }

        let emitter = tokio::spawn(async move {
            let mut batches = vec![one_batch(emitter_shares)];
            let mut prepared = PreparedPayloads::default();
            prepared
                .retain(payload(&[0, 1], CONFIRMED_PAYLOAD), &mut batches)
                .assured("both members are pending rows of the modeled batch");
            let first = prepared.next_write();
            let first_bytes = first
                .records
                .first()
                .assured("one retained payload makes one record")
                .payload
                .clone();
            tokio::task::yield_now().await;
            let answers = prepared
                .answers(&mut batches, first.payloads, stalled())
                .assured("a stalled write answers for no record");
            assert!(
                answers.unresolved.is_some(),
                "a write the sink never answered must stay unresolved"
            );
            tokio::task::yield_now().await;

            let retry = prepared.next_write();
            let retried = retry
                .records
                .iter()
                .map(|record| record.payload.clone())
                .collect::<Vec<_>>();
            assert_eq!(
                retried,
                vec![first_bytes],
                "a retry must write the retained payload exactly as it was first written"
            );
            emitter_confirmed.store(true, Ordering::SeqCst);
            let mut confirmed = PerRecordOutcome::with_capacity(1);
            confirmed.deliver(SinkRecordId::new(0));
            let answers = prepared
                .answers(&mut batches, retry.payloads, confirmed)
                .assured("the confirmation names the one record of the retry");
            assert!(answers.unresolved.is_none());
            assert!(prepared.is_empty());
        });
        let sibling = tokio::spawn(async move {
            tokio::task::yield_now().await;
            match sibling_end {
                SiblingEnd::Confirmed => {
                    sibling_confirmed.store(true, Ordering::SeqCst);
                    for share in sibling_shares {
                        share.ack_success();
                    }
                }
                SiblingEnd::Failed => {
                    for share in sibling_shares {
                        share.no_ack("the sibling emitter failed");
                    }
                }
            }
        });

        emitter.await.assured(MODEL_TASK_JOINS);
        sibling.await.assured(MODEL_TASK_JOINS);
        for observer in observers {
            let outcome = observer.await.assured(MODEL_TASK_JOINS);
            let expected = match sibling_end {
                SiblingEnd::Confirmed => AckOutcome::Ack,
                SiblingEnd::Failed => AckOutcome::NoAck("the sibling emitter failed".to_string()),
            };
            assert_eq!(outcome, expected);
        }
    });
}

fn fanned_in_members_confirmed_by_both_emitters_resolve_once() {
    fanned_in_members_resolve_once(SiblingEnd::Confirmed);
}

fn fanned_in_members_a_sibling_failed_resolve_once() {
    fanned_in_members_resolve_once(SiblingEnd::Failed);
}

#[test]
fn shuttle_a_retried_payload_acknowledges_each_fanned_in_member_once_after_every_emitter() {
    check_interleavings(fanned_in_members_confirmed_by_both_emitters_resolve_once);
}

#[test]
fn shuttle_a_sibling_failure_resolves_each_fanned_in_member_once_despite_a_retry() {
    check_interleavings(fanned_in_members_a_sibling_failed_resolve_once);
}

/// What the attempts of the cancellation model did, recorded as they did it.
struct AttemptRecord {
    /// Message errors delivered for each row.
    deliveries: [AtomicUsize; 4],
    /// Every payload a write carried, in the order the writes carried them.
    written: parking_lot::Mutex<Vec<Vec<u8>>>,
    /// Every payload the sink answered for.
    answered: parking_lot::Mutex<Vec<Vec<u8>>>,
}

impl AttemptRecord {
    fn new() -> Self {
        Self {
            deliveries: [const { AtomicUsize::new(0) }; 4],
            written: parking_lot::Mutex::new(Vec::new()),
            answered: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

/// Delivers the message error of `row`, the way `RowAnswers::apply` does: the row resolves once
/// the delivery completes, and a delivery cut short leaves it pending.
async fn deliver_message_error(
    batches: &mut [EmitterPublishBatch],
    row: usize,
    record: &AttemptRecord,
) {
    let batch = batches.first_mut().assured(MODEL_ROWS);
    let acks = batch
        .relay_batch()
        .acks
        .get(row)
        .cloned()
        .assured(MODEL_ROWS);
    let deliveries = record.deliveries.get(row).assured(MODEL_ROWS);
    batch
        .mark_rejected_after_delivery(row, async move {
            tokio::task::yield_now().await;
            deliveries.fetch_add(1, Ordering::SeqCst);
            acks.no_ack("refused");
        })
        .await
        .assured(MODEL_ROWS);
}

/// One attempt: write every retained payload, apply the sink's answers, and deliver the message
/// errors of the members it rejected.
async fn attempt(
    batches: &mut [EmitterPublishBatch],
    prepared: &mut PreparedPayloads<EncodedPayload>,
    record: &AttemptRecord,
) {
    let write = prepared.next_write();
    record
        .written
        .lock()
        .extend(write.records.iter().map(|record| record.payload.clone()));
    tokio::task::yield_now().await;
    let outcome = answered(&write.records);
    let answers = prepared
        .answers(batches, write.payloads, outcome)
        .assured("the modeled sink answers once for each record of the write");
    record
        .answered
        .lock()
        .extend(write.records.into_iter().map(|record| record.payload));
    for rejected in answers.rejected {
        deliver_message_error(batches, rejected.position.row_index, record).await;
    }
}

/// An attempt that the emitter's stop deadline cuts short at any point — while the sink writes,
/// after its answers, or between two message errors — leaves every member to resolve exactly once
/// in the attempt after it. That attempt writes again only the payloads nobody answered for, with
/// their first bytes, and delivers only the message errors the first attempt did not.
fn a_cancelled_attempt_leaves_each_member_to_resolve_once() {
    shuttle::future::block_on(async {
        let source_released = StdArc::new(AtomicBool::new(false));
        let mut source_shares = Vec::with_capacity(4);
        let mut emitter_shares = Vec::with_capacity(4);
        let mut observers = Vec::with_capacity(4);
        for row in 0..4 {
            let (root, completion) = AckSet::root();
            emitter_shares.push(root.attached());
            source_shares.push(root);
            let source_released = source_released.clone();
            observers.push(tokio::spawn(async move {
                let outcome = completion.wait().await;
                if row < 2 {
                    assert!(
                        source_released.load(Ordering::SeqCst),
                        "a delivered member completed its source acknowledgement alone, so the \
                         emitter resolved its share more than once"
                    );
                }
                outcome
            }));
        }
        let record = StdArc::new(AttemptRecord::new());
        let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();

        let attempt_record = record.clone();
        let emitter = tokio::spawn(async move {
            let record = attempt_record;
            let mut batches = vec![one_batch(emitter_shares)];
            let mut prepared = PreparedPayloads::default();
            for (rows, bytes) in [(&[0, 1], CONFIRMED_PAYLOAD), (&[2, 3], REJECTED_PAYLOAD)] {
                prepared
                    .retain(payload(rows, bytes), &mut batches)
                    .assured("every member is a pending row of the modeled batch");
            }
            tokio::select! {
                biased;
                () = attempt(&mut batches, &mut prepared, &record) => {}
                _ = cancelled => {}
            }
            let first_answered = record.answered.lock().clone();
            let first_written = record.written.lock().len();

            attempt(&mut batches, &mut prepared, &record).await;
            // A rejected member whose message error the first attempt did not deliver is pending
            // again, and the next attempt packs it anew; the modeled sink rejects it once more.
            let leftover = batches.first().assured(MODEL_ROWS).pending_record_rows();
            for row in leftover {
                deliver_message_error(&mut batches, row, &record).await;
            }

            let retried = record
                .written
                .lock()
                .get(first_written..)
                .assured("the writes of the first attempt precede the ones after it")
                .to_vec();
            for bytes in &retried {
                assert!(
                    bytes == CONFIRMED_PAYLOAD || bytes == REJECTED_PAYLOAD,
                    "a retry wrote bytes no payload was prepared with: {bytes:?}"
                );
                assert!(
                    !first_answered.contains(bytes),
                    "a retry wrote a payload the sink had already answered for: {bytes:?}"
                );
            }
            assert!(prepared.is_empty());
            assert_eq!(
                batches.first().assured(MODEL_ROWS).resolved_rows(),
                vec![true; 4],
                "every member must be resolved once the retry finished"
            );
        });
        let canceller = tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel
                .send(())
                .means_peer_left("the modeled attempt finished before its stop deadline");
        });

        emitter.await.assured(MODEL_TASK_JOINS);
        canceller.await.assured(MODEL_TASK_JOINS);
        for (row, deliveries) in record.deliveries.iter().enumerate() {
            let expected = usize::from(row >= 2);
            assert_eq!(
                deliveries.load(Ordering::SeqCst),
                expected,
                "row {row} must have exactly {expected} message errors delivered"
            );
        }
        source_released.store(true, Ordering::SeqCst);
        for share in source_shares {
            share.ack_success();
        }
        for (row, observer) in observers.into_iter().enumerate() {
            let outcome = observer.await.assured(MODEL_TASK_JOINS);
            let expected = match row {
                0 | 1 => AckOutcome::Ack,
                _ => AckOutcome::NoAck("refused".to_string()),
            };
            assert_eq!(outcome, expected, "row {row} resolved to the wrong outcome");
        }
    });
}

#[test]
fn shuttle_a_cancelled_attempt_leaves_each_member_to_resolve_once() {
    check_interleavings(a_cancelled_attempt_leaves_each_member_to_resolve_once);
}

/// Terminal teardown can arrive after a request entered its connector await. The emitter's
/// shutdown signal must end that await, drop its prepared request, and leave its attached source
/// share unconfirmed. The source may then redeliver after restart.
fn terminal_shutdown_drops_an_unanswered_request_without_source_ack() {
    shuttle::future::block_on(async {
        let (source, completion) = AckSet::root();
        let emitter_share = source.attached();
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let (entered_attempt, attempt_started) = tokio::sync::oneshot::channel();

        let emitter = tokio::spawn(async move {
            let mut batches = vec![one_batch(vec![emitter_share])];
            let mut prepared = PreparedPayloads::default();
            prepared
                .retain(payload(&[0], b"/events/1"), &mut batches)
                .assured("the prepared request has one pending source member");
            tokio::select! {
                biased;
                _ = crate::runtime::emitter_publishing::wait_for_emitter_work_cancel(&mut shutdown_rx) => {}
                _ = async {
                    entered_attempt.send(()).means_peer_left(
                        "the modeled connector attempt has not been canceled yet"
                    );
                    std::future::pending::<()>().await;
                } => unreachable!("the modeled connector never answers"),
            }
            drop(prepared);
            drop(batches);
        });

        attempt_started
            .await
            .assured("the modeled emitter enters its connector await before shutdown");
        source.ack_success();
        drop(source);
        shutdown.send_replace(true);
        emitter.await.assured(MODEL_TASK_JOINS);
        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck("ack completion sender dropped".to_string()),
            "terminal cancellation cannot confirm an unanswered attached request"
        );
    });
}

#[test]
fn shuttle_terminal_shutdown_leaves_an_unanswered_request_unacknowledged() {
    check_interleavings(terminal_shutdown_drops_an_unanswered_request_without_source_ack);
}

/// A drain reads the emitter's buffered count while the emitter's first write of a payload stalls
/// and a force flush writes it again. The drain never reads the emitter empty while a member of the
/// retained payload is unresolved: the members resolve before the buffer lets its batch go.
fn a_drain_never_finds_the_emitter_empty_while_a_member_is_retained() {
    shuttle::future::block_on(async {
        let reported = Arc::new(AtomicUsize::new(0));
        let buffered = Arc::new(EmitterBufferedMessages::new(reported.clone()));
        let mut shares = Vec::with_capacity(2);
        let mut completions = Vec::with_capacity(2);
        for _ in 0..2 {
            let (share, completion) = AckSet::root();
            shares.push(share);
            completions.push(completion);
        }
        let mut buffer = EmitterBatchBuffer::reporting_to(buffered, RuntimeFlushPolicy::Immediate);
        buffer
            .retain_without_cadence(one_batch(shares))
            .assured("the modeled buffer has a flush policy");
        let (request_flush, flush_requested) = tokio::sync::oneshot::channel::<()>();

        let emitter = tokio::spawn(async move {
            let EmitterPublication {
                batches,
                payloads: prepared,
                ..
            } = buffer.publication_mut();
            prepared
                .retain(payload(&[0, 1], CONFIRMED_PAYLOAD), batches)
                .assured("both members are pending rows of the buffered batch");
            let first = prepared.next_write();
            tokio::task::yield_now().await;
            let answers = prepared
                .answers(batches, first.payloads, stalled())
                .assured("a stalled write answers for no record");
            assert!(answers.unresolved.is_some());

            flush_requested
                .await
                .assured("the force flush task requests exactly one flush");
            let EmitterPublication {
                batches,
                payloads: prepared,
                ..
            } = buffer.publication_mut();
            let retry = prepared.next_write();
            tokio::task::yield_now().await;
            let mut confirmed = PerRecordOutcome::with_capacity(1);
            confirmed.deliver(SinkRecordId::new(0));
            let answers = prepared
                .answers(batches, retry.payloads, confirmed)
                .assured("the confirmation names the one record of the flush");
            assert!(answers.unresolved.is_none());
            // A publish that resolved everything lets the buffer go, as the flush does.
            buffer.clear();
        });
        let force_flush = tokio::spawn(async move {
            tokio::task::yield_now().await;
            request_flush
                .send(())
                .assured("the emitter waits for the force flush before it finishes");
        });
        // The drain spins until it reads the emitter empty, so it is spawned after the tasks it
        // waits for: the depth-first search runs the lowest-numbered runnable task first.
        let drain = tokio::spawn(async move {
            let mut completions = completions;
            while reported.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
            for completion in &mut completions {
                let progress = completion.wait_for_progress().now_or_never();
                assert!(
                    matches!(progress, Some(AckProgress::Complete(AckOutcome::Ack))),
                    "a drain read the emitter empty while a member of its retained payload was \
                     unresolved: {progress:?}"
                );
            }
        });

        emitter.await.assured(MODEL_TASK_JOINS);
        force_flush.await.assured(MODEL_TASK_JOINS);
        drain.await.assured(MODEL_TASK_JOINS);
    });
}

#[test]
fn shuttle_a_drain_never_finds_the_emitter_empty_while_a_member_is_retained() {
    check_interleavings(a_drain_never_finds_the_emitter_empty_while_a_member_is_retained);
}
