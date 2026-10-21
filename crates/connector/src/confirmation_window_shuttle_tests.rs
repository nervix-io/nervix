//! The confirmation window explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants the window is held to while its broker answers records concurrently
//!   with the publish: a failure settles every answer given before it, a full window settles each
//!   record exactly once whatever order its answers arrive in, and an oldest answer that fails or
//!   misses its deadline ends the publish without losing an answer already given.
//! - **Depends on.** The confirmation window, the scripted verdicts of its ordinary tests, oneshot
//!   channels as the broker's answers, the model harness's Shuttle runner, and the task labels and
//!   timeout triggers of Shuttle's Tokio.
//! - **Must not know.** Any broker driver or the host's retry.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        oneshot,
    },
    task::JoinHandle,
    time::{clear_triggers, trigger_timeouts},
};
use nervix_recovery::Discarded as _;

use super::{
    BrokerAnswer, ConfirmationWindow, ConfirmationWindowError,
    tests::{
        DriverRefusedRecords, FailedAnswerOf, ScriptedVerdict, driver_refused, occurred_at,
        settings,
    },
};
use crate::sink::{PerRecordOutcomeParts, SinkPublishError, SinkPublishResult, SinkRecordId};

const CHECK_TASK_JOINS: &str =
    "a check task that panics fails the execution before its join returns";

/// The `ACK TIMEOUT` of every checked window. Shuttle does not model elapsed time, so a deadline
/// passes only when a check triggers the publishing task's timeouts.
const TIMEOUT: Duration = Duration::from_secs(1);

/// The answer a broker task gives through a oneshot channel. It counts toward the answers its
/// window holds until the window drops it.
struct ChannelAnswer {
    receiver: oneshot::Receiver<BrokerAnswer>,
    held: Arc<AtomicUsize>,
}

impl Future for ChannelAnswer {
    type Output = BrokerAnswer;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<BrokerAnswer> {
        let answer = self.get_mut();
        match Pin::new(&mut answer.receiver).poll(context) {
            Poll::Ready(received) => Poll::Ready(
                received.assured("every broker task sends its record's answer before it ends"),
            ),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ChannelAnswer {
    fn drop(&mut self) {
        self.held.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Gives `answer` through `sender`, then records in `given` that the window can read it.
async fn give(sender: oneshot::Sender<BrokerAnswer>, answer: BrokerAnswer, given: Arc<AtomicBool>) {
    match sender.send(answer) {
        Ok(()) => given.store(true, Ordering::SeqCst),
        unread @ Err(_) => unread.discarded(
            "the publish already ended and dropped the answer's receiver, so its record stays \
             unresolved for the host's retry",
        ),
    }
}

/// One record a check handed over, its verdict, and the flag its broker task sets once the window
/// can read the record's answer.
struct Handed {
    record: SinkRecordId,
    verdict: ScriptedVerdict,
    given: Arc<AtomicBool>,
}

/// A publish under check: its window, the records it handed over, and the broker tasks that answer
/// them concurrently with it.
struct CheckedPublish {
    window: ConfirmationWindow<ChannelAnswer>,
    held: Arc<AtomicUsize>,
    handed: Vec<Handed>,
    brokers: Vec<JoinHandle<()>>,
}

/// A record a checked publish handed over, and whether its answer had been given when the publish
/// began to end.
struct EndedRecord {
    record: SinkRecordId,
    verdict: ScriptedVerdict,
    given_before_the_end: bool,
}

/// How often an outcome settled one record: as delivered and as rejected.
#[derive(Debug, Default, Clone, Copy)]
struct Settlements {
    delivered: usize,
    rejected: usize,
}

/// How a checked publish ended, and which answers had been given when it began to end.
struct Settled {
    outcome: PerRecordOutcomeParts<SinkRecordId>,
    records: Vec<EndedRecord>,
}

impl CheckedPublish {
    fn new(max_in_flight: usize, records: usize) -> Self {
        Self {
            window: ConfirmationWindow::new(
                super::tests::SINK,
                settings(max_in_flight, TIMEOUT),
                records,
            ),
            held: Arc::new(AtomicUsize::new(0)),
            handed: Vec::with_capacity(records),
            brokers: Vec::with_capacity(records),
        }
    }

    /// Hands the next record over. A broker task of its own gives the record's answer, `verdict`,
    /// concurrently with the rest of the publish.
    async fn hand_over(&mut self, verdict: ScriptedVerdict) -> SinkPublishResult<()> {
        let record = SinkRecordId::new(self.handed.len());
        let (sender, receiver) = oneshot::channel();
        let given = Arc::new(AtomicBool::new(false));
        self.held.fetch_add(1, Ordering::SeqCst);
        let answer = ChannelAnswer {
            receiver,
            held: self.held.clone(),
        };
        let broker =
            nervix_primitives::task::spawn(give(sender, verdict.answer(record), given.clone()));
        self.brokers.push(broker);
        self.handed.push(Handed {
            record,
            verdict,
            given,
        });
        self.window.admit(record, occurred_at(record), answer).await
    }

    /// How many answers the window holds.
    fn held(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }

    /// Every record handed over so far, with whether its broker task has given its answer by now.
    fn ending_records(&self) -> Vec<EndedRecord> {
        let mut records = Vec::with_capacity(self.handed.len());
        for handed in &self.handed {
            records.push(EndedRecord {
                record: handed.record,
                verdict: handed.verdict,
                given_before_the_end: handed.given.load(Ordering::SeqCst),
            });
        }
        records
    }

    /// Ends the publish with `failure` and waits for every broker task.
    async fn end_with(self, failure: Report<SinkPublishError>) -> Settled {
        let records = self.ending_records();
        let outcome = self.window.fail(failure);
        Self::settled(outcome.into_parts(), records, self.brokers).await
    }

    /// Ends the publish once every record was handed over and waits for every broker task.
    async fn finish(self) -> Settled {
        let records = self.ending_records();
        let outcome = self.window.finish().await;
        Self::settled(outcome.into_parts(), records, self.brokers).await
    }

    async fn settled(
        outcome: PerRecordOutcomeParts<SinkRecordId>,
        records: Vec<EndedRecord>,
        brokers: Vec<JoinHandle<()>>,
    ) -> Settled {
        for broker in brokers {
            broker.await.assured(CHECK_TASK_JOINS);
        }
        Settled { outcome, records }
    }
}

impl Settled {
    /// Checks that every record was settled at most once and as its verdict says, and that every
    /// record whose answer was given before the publish began to end was settled.
    fn assert_given_answers_settled(&self) {
        let settlements = self.settlements();
        for ended in &self.records {
            let Settlements {
                delivered,
                rejected,
            } = match settlements.get(&ended.record) {
                Some(settled) => *settled,
                None => Settlements::default(),
            };
            match ended.verdict {
                ScriptedVerdict::Confirmed => {
                    assert_eq!(rejected, 0, "a confirmed record is never rejected");
                    if ended.given_before_the_end {
                        assert_eq!(
                            delivered,
                            1,
                            "record {} was confirmed before the publish ended and must be \
                             delivered once",
                            ended.record.index()
                        );
                    } else {
                        assert!(delivered <= 1, "a record is delivered at most once");
                    }
                }
                ScriptedVerdict::Rejected => {
                    assert_eq!(delivered, 0, "a refused record is never delivered");
                    if ended.given_before_the_end {
                        assert_eq!(
                            rejected,
                            1,
                            "record {} was refused before the publish ended and must be rejected \
                             once",
                            ended.record.index()
                        );
                    } else {
                        assert!(rejected <= 1, "a record is rejected at most once");
                    }
                }
                ScriptedVerdict::Failed => assert_eq!(
                    delivered + rejected,
                    0,
                    "a record whose answer failed stays unresolved"
                ),
            }
        }
        for rejection in &self.outcome.rejected {
            assert_eq!(
                rejection.error.message,
                ScriptedVerdict::rejection(rejection.id)
            );
            assert_eq!(rejection.error.occurred_at, occurred_at(rejection.id));
        }
    }

    /// How often the outcome settled each record it settled.
    fn settlements(&self) -> BTreeMap<SinkRecordId, Settlements> {
        let mut settlements = BTreeMap::<SinkRecordId, Settlements>::new();
        for delivered in &self.outcome.delivered {
            settlements.entry(*delivered).or_default().delivered += 1;
        }
        for rejection in &self.outcome.rejected {
            settlements.entry(rejection.id).or_default().rejected += 1;
        }
        settlements
    }

    fn failure(&self) -> &Report<SinkPublishError> {
        self.outcome
            .infrastructure_error
            .as_ref()
            .verified("the check ended this publish with a failure")
    }
}

fn a_failure_settles_every_answer_given_before_it() {
    shuttle::future::block_on(async {
        let mut publish = CheckedPublish::new(5, 4);
        for verdict in [
            ScriptedVerdict::Confirmed,
            ScriptedVerdict::Rejected,
            ScriptedVerdict::Failed,
            ScriptedVerdict::Confirmed,
        ] {
            publish
                .hand_over(verdict)
                .await
                .assured("a window of five never waits while it takes four records");
        }

        // The driver refuses the next record for a reason that is not the record's own.
        let settled = publish.end_with(driver_refused()).await;

        settled.assert_given_answers_settled();
        assert!(
            settled.failure().contains::<DriverRefusedRecords>(),
            "the driver's failure ends the publish"
        );
    });
}

/// A failure ends a publish without dropping an answer its broker gave before it, whichever answers
/// raced the failure.
#[test]
fn shuttle_a_failure_settles_every_answer_its_broker_gave_before_it() {
    check_interleavings(a_failure_settles_every_answer_given_before_it);
}

fn answers_racing_a_full_window_settle_each_record_exactly_once() {
    shuttle::future::block_on(async {
        let mut publish = CheckedPublish::new(2, 5);
        for verdict in [
            ScriptedVerdict::Confirmed,
            ScriptedVerdict::Rejected,
            ScriptedVerdict::Confirmed,
            ScriptedVerdict::Rejected,
            ScriptedVerdict::Confirmed,
        ] {
            publish.hand_over(verdict).await.assured(
                "a deadline passes only when a check triggers it, and every answer is a verdict",
            );
            assert!(
                publish.held() < 2,
                "a full window settles its oldest record before it takes another"
            );
        }

        let settled = publish.finish().await;

        assert!(settled.outcome.infrastructure_error.is_none());
        let mut delivered = settled.outcome.delivered.clone();
        delivered.sort();
        assert_eq!(
            delivered,
            [0, 2, 4].map(SinkRecordId::new),
            "every confirmed record is delivered exactly once"
        );
        let mut rejected = Vec::new();
        for rejection in &settled.outcome.rejected {
            rejected.push(rejection.id);
        }
        rejected.sort();
        assert_eq!(
            rejected,
            [1, 3].map(SinkRecordId::new),
            "every refused record is rejected exactly once"
        );
        settled.assert_given_answers_settled();
    });
}

/// A window of two settles each of five records exactly once, as its answer says, whatever order
/// the answers arrive in.
#[test]
fn shuttle_answers_racing_a_full_window_settle_each_record_exactly_once() {
    check_interleavings(answers_racing_a_full_window_settle_each_record_exactly_once);
}

fn driver_backpressure_settles_below_max_in_flight() {
    shuttle::future::block_on(async {
        let mut publish = CheckedPublish::new(8, 5);
        assert!(
            publish
                .window
                .settle_next()
                .await
                .assured("an empty window waits for nothing")
                .is_none()
        );
        for verdict in [
            ScriptedVerdict::Confirmed,
            ScriptedVerdict::Rejected,
            ScriptedVerdict::Confirmed,
            ScriptedVerdict::Rejected,
            ScriptedVerdict::Confirmed,
        ] {
            nervix_primitives::task::consume_budget().await;
            if publish.held() >= 2 {
                let settled = publish.window.settle_next().await.assured(
                    "every answer is a verdict and only an explicit trigger expires a deadline",
                );
                assert!(settled.is_some());
                assert_eq!(publish.held(), 1);
            }
            publish
                .hand_over(verdict)
                .await
                .assured("MAX is larger than the driver's queue");
            assert!(publish.held() <= 2);
        }
        let settled = publish.finish().await;
        assert!(settled.outcome.infrastructure_error.is_none());
        let mut delivered = settled.outcome.delivered.clone();
        delivered.sort();
        assert_eq!(delivered, [0, 2, 4].map(SinkRecordId::new));
        let mut rejected: Vec<_> = settled
            .outcome
            .rejected
            .iter()
            .map(|record| record.id)
            .collect();
        rejected.sort();
        assert_eq!(rejected, [1, 3].map(SinkRecordId::new));
        settled.assert_given_answers_settled();
    });
}

/// A driver's capacity below MAX requests an oldest answer without ending the publish or
/// settling any record twice, including answers that race the request.
#[test]
fn shuttle_driver_backpressure_settles_below_max_in_flight() {
    check_interleavings(driver_backpressure_settles_below_max_in_flight);
}

/// Marks the publishing task, whose window deadlines a check's clock expires.
#[derive(Debug, Clone, Copy)]
struct PublishDeadlines;

fn an_expired_oldest_settles_the_answers_given_before_it() {
    // Timeout triggers are thread-local rather than per execution, so every execution starts
    // without the trigger an earlier one registered.
    clear_triggers();
    shuttle::future::block_on(async {
        shuttle::current::set_label_for_task(shuttle::current::me(), PublishDeadlines);
        // The clock expires every deadline of the publish at a point Shuttle chooses: before, among
        // or after the broker's answers.
        let clock = nervix_primitives::task::spawn(async {
            trigger_timeouts(|labels| labels.get::<PublishDeadlines>().is_some());
        });
        let mut publish = CheckedPublish::new(2, 3);
        let mut stopped = None;
        for verdict in [
            ScriptedVerdict::Confirmed,
            ScriptedVerdict::Rejected,
            ScriptedVerdict::Confirmed,
        ] {
            if let Err(failure) = publish.hand_over(verdict).await {
                stopped = Some(failure);
                break;
            }
        }

        let settled = match stopped {
            Some(failure) => publish.end_with(failure).await,
            None => publish.finish().await,
        };
        clock.await.assured(CHECK_TASK_JOINS);

        settled.assert_given_answers_settled();
        if let Some(failure) = &settled.outcome.infrastructure_error {
            assert_eq!(
                failure.downcast_ref::<ConfirmationWindowError>(),
                Some(&ConfirmationWindowError::AnswerTimedOut { timeout: TIMEOUT }),
                "only an expired deadline ends this publish"
            );
        }
    });
}

/// An expired oldest deadline ends a publish without dropping an answer given before it, the
/// oldest's own answer included.
#[test]
fn shuttle_an_expired_oldest_settles_the_answers_given_before_it() {
    check_interleavings(an_expired_oldest_settles_the_answers_given_before_it);
}

fn a_failed_oldest_answer_settles_the_younger_answers_given_before_it() {
    shuttle::future::block_on(async {
        let mut publish = CheckedPublish::new(2, 2);
        publish
            .hand_over(ScriptedVerdict::Failed)
            .await
            .assured("a window of two takes its first record without waiting");
        let admitted = publish.hand_over(ScriptedVerdict::Confirmed).await;
        let Err(failure) = admitted else {
            panic!("a full window settled a failed oldest answer as a verdict");
        };
        assert_eq!(
            failure.downcast_ref::<FailedAnswerOf>(),
            Some(&FailedAnswerOf(SinkRecordId::new(0))),
            "the oldest record's failed answer ends the publish"
        );

        let settled = publish.end_with(failure).await;

        settled.assert_given_answers_settled();
    });
}

/// A failed oldest answer ends the publish with its own failure, and a younger answer given before
/// the end is still settled.
#[test]
fn shuttle_a_failed_oldest_answer_settles_the_younger_answers_given_before_it() {
    check_interleavings(a_failed_oldest_answer_settles_the_younger_answers_given_before_it);
}
