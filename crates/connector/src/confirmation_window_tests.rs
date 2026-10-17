//! The confirmation window against the window rule its owner documents.
//!
//! Layer: test harness.
//! - **Owns.** A scripted broker that answers each record at a chosen instant of a paused clock,
//!   the reference publish the documented window rule gives a script, and the Bolero property and
//!   focused tests that hold the window to it.
//! - **Depends on.** The confirmation window, the sink contract's outcome types, and a runtime on a
//!   paused clock.
//! - **Must not know.** Any broker driver or the host's retry.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    cell::Cell,
    collections::VecDeque,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use error_stack::{AttachmentKind, FrameKind, Report};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{MessageErrorCode, MessageErrorOperation, Timestamp};
use nervix_primitives::{
    sync::oneshot,
    time::{Instant, Sleep, sleep, sleep_until},
};

use super::{BrokerAnswer, BrokerVerdict, ConfirmationWindow, ConfirmationWindowError};
use crate::sink::{
    AckConfirmation, PerRecordOutcomeParts, RejectedSinkRecord, SinkPublishError,
    SinkPublishResult, SinkRecordId,
};

/// The sink the scripted publishes report their failures for.
pub(super) const SINK: &str = "scripted";

/// What the driver reports for a record it refused for good as it was handed over.
const REFUSED_AT_HAND_OVER: &str = "the scripted driver refused the record as it was handed over";

/// What the scripted broker decides about one record it was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScriptedVerdict {
    Confirmed,
    Rejected,
    /// The answer fails, so the record's outcome is unknown.
    Failed,
}

/// Names the record whose scripted answer failed, in the failure that answer reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FailedAnswerOf(pub(super) SinkRecordId);

/// Marks the failure of a scripted driver that could take no more records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DriverRefusedRecords;

impl ScriptedVerdict {
    /// The answer the scripted broker gives `record`.
    pub(super) fn answer(self, record: SinkRecordId) -> BrokerAnswer {
        match self {
            Self::Confirmed => Ok(BrokerVerdict::Confirmed),
            Self::Rejected => Ok(BrokerVerdict::Rejected {
                message: Self::rejection(record),
            }),
            Self::Failed => Err(Report::new(SinkPublishError::Publish { sink: SINK })
                .attach(FailedAnswerOf(record))),
        }
    }

    /// The message the scripted broker refuses `record` with.
    pub(super) fn rejection(record: SinkRecordId) -> String {
        format!("the scripted broker refused record {}", record.index())
    }
}

/// A record a publish rejected, and the message its rejection reports.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RejectedRecord {
    record: SinkRecordId,
    message: String,
}

impl RejectedRecord {
    /// `record` as the scripted broker refuses it.
    fn by_broker(record: SinkRecordId) -> Self {
        Self {
            record,
            message: ScriptedVerdict::rejection(record),
        }
    }

    /// `record` as the scripted driver refuses it at hand-over.
    fn at_hand_over(record: SinkRecordId) -> Self {
        Self {
            record,
            message: REFUSED_AT_HAND_OVER.to_owned(),
        }
    }
}

/// When the scripted publish says `record` occurred, distinct for every record so a settled
/// rejection shows it kept its own record's time.
pub(super) fn occurred_at(record: SinkRecordId) -> Timestamp {
    let nanos = i64::try_from(record.index()).assured("a scripted publish has few records");
    Timestamp::from_unix_nanos(nanos)
}

/// The failure of a scripted driver that could take no more records.
pub(super) fn driver_refused() -> Report<SinkPublishError> {
    Report::new(SinkPublishError::Publish { sink: SINK }).attach(DriverRefusedRecords)
}

/// The settings of a window that awaits at most `max_in_flight` answers, each for `timeout`.
pub(super) fn settings(max_in_flight: usize, timeout: Duration) -> AckConfirmation {
    AckConfirmation {
        max_in_flight: NonZeroUsize::new(max_in_flight)
            .assured("every scripted MAX is at least one"),
        timeout,
    }
}

/// A runtime whose clock moves only when every task waits, straight to the next timer.
fn paused_runtime() -> nervix_primitives::runtime::Runtime {
    nervix_primitives::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .assured("a current-thread runtime with a paused clock opens")
}

/// The answer for one record, which the scripted broker gives once the clock reaches `given_at`,
/// or never.
struct ScriptedAnswer<'a> {
    record: SinkRecordId,
    verdict: ScriptedVerdict,
    given_at: Option<Instant>,
    /// Wakes a publish that waits for this answer when it is given.
    wake: Option<Pin<Box<Sleep>>>,
    /// How many answers the window holds; this one counts until the window drops it.
    held: &'a Cell<usize>,
}

impl<'a> ScriptedAnswer<'a> {
    fn new(
        record: SinkRecordId,
        verdict: ScriptedVerdict,
        given_at: Option<Instant>,
        held: &'a Cell<usize>,
    ) -> Self {
        held.set(held.get() + 1);
        Self {
            record,
            verdict,
            given_at,
            wake: None,
            held,
        }
    }
}

impl Future for ScriptedAnswer<'_> {
    type Output = BrokerAnswer;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<BrokerAnswer> {
        let answer = self.get_mut();
        let Some(given_at) = answer.given_at else {
            return Poll::Pending;
        };
        // The clock decides whether the answer was given, so an answer due at the instant of a
        // deadline is given whichever of the two timers fires first.
        if Instant::now() >= given_at {
            return Poll::Ready(answer.verdict.answer(answer.record));
        }
        let wake = answer
            .wake
            .get_or_insert_with(|| Box::pin(sleep_until(given_at)));
        match wake.as_mut().poll(context) {
            Poll::Ready(()) => Poll::Ready(answer.verdict.answer(answer.record)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ScriptedAnswer<'_> {
    fn drop(&mut self) {
        self.held.set(self.held.get() - 1);
    }
}

/// How the driver and the broker treat one scripted record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Treatment {
    /// The driver refuses the record for good as it is handed over.
    Refused,
    /// The driver takes the record, and the broker answers `verdict` the given milliseconds after
    /// the hand-over, or never.
    Taken {
        verdict: ScriptedVerdict,
        answer_after: Option<u64>,
    },
}

/// One record of a generated publish.
#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
struct ScriptedRecord {
    /// The sink's own work before it hands this record over: up to nine milliseconds.
    pause: u8,
    /// Selects how the driver and the broker treat the record; see [`ScriptedRecord::treatment`].
    treatment: u8,
    /// Places the answer within the record's deadline or past it.
    millis: u8,
}

impl ScriptedRecord {
    fn pause(self) -> Duration {
        Duration::from_millis(u64::from(self.pause % 10))
    }

    /// One record in sixteen is refused at hand-over. A taken record's answer fails one time in
    /// eight and refuses the record one time in eight. One taken record in eight is never
    /// answered, one is answered at or after its deadline, and the rest within it, at its
    /// deadline included.
    fn treatment(self, timeout_millis: u64) -> Treatment {
        if self.treatment.is_multiple_of(16) {
            return Treatment::Refused;
        }
        let verdict = match (self.treatment / 16) % 8 {
            0 => ScriptedVerdict::Failed,
            1 => ScriptedVerdict::Rejected,
            _ => ScriptedVerdict::Confirmed,
        };
        let millis = u64::from(self.millis);
        let answer_after = match self.treatment % 8 {
            0 => None,
            1 => Some(timeout_millis + millis % 20),
            _ => Some(millis % (timeout_millis + 1)),
        };
        Treatment::Taken {
            verdict,
            answer_after,
        }
    }
}

/// A generated publish: the window's settings, the records the sink hands over in order, and the
/// record the driver fails at, if any.
#[derive(Debug, bolero::TypeGenerator)]
struct WindowCase {
    /// `MAX`: one to six records await answers at once.
    max_in_flight: u8,
    /// `ACK TIMEOUT`: one to forty milliseconds.
    timeout: u8,
    #[generator(bolero::generator::produce_with::<Vec<ScriptedRecord>>().len(0_usize..=24))]
    records: Vec<ScriptedRecord>,
    /// Selects the record whose hand-over the driver fails for a reason not the record's own,
    /// which ends the publish.
    driver_fails_at: Option<u8>,
}

/// The failure that ends a publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedFailure {
    /// The driver could take no more records.
    DriverRefused,
    /// The answer for the oldest record failed.
    AnswerFailed(SinkRecordId),
    /// The answer for the oldest record did not arrive by its deadline.
    TimedOut,
}

/// A record the reference publish handed over, with its answer and deadline in milliseconds since
/// the publish began.
#[derive(Debug, Clone, Copy)]
struct ReferenceRecord {
    record: SinkRecordId,
    verdict: ScriptedVerdict,
    given_at: Option<u64>,
    deadline: u64,
}

impl ReferenceRecord {
    /// Whether the broker gave this record's answer by `instant`.
    fn given_by(&self, instant: u64) -> bool {
        match self.given_at {
            Some(given_at) => given_at <= instant,
            None => false,
        }
    }
}

/// The publish the documented window rule gives a case.
struct ReferencePublish {
    /// When the publish ends, in milliseconds since it began.
    end: u64,
    /// Every record the driver took, in hand-over order.
    taken: Vec<ReferenceRecord>,
    /// Every record the driver refused as it was handed over.
    refused: Vec<SinkRecordId>,
    failure: Option<ExpectedFailure>,
}

impl ReferencePublish {
    /// The records the publish delivers: exactly those whose broker confirmed them before it
    /// ended. No other record is delivered, and none of them is left unresolved.
    fn delivered(&self) -> Vec<SinkRecordId> {
        let mut delivered = Vec::new();
        for taken in &self.taken {
            if taken.verdict == ScriptedVerdict::Confirmed && taken.given_by(self.end) {
                delivered.push(taken.record);
            }
        }
        delivered
    }

    /// The records the publish rejects, with the message each is rejected with: the ones refused at
    /// hand-over, and the ones the broker refused before the publish ended.
    fn rejected(&self) -> Vec<RejectedRecord> {
        let mut rejected = Vec::new();
        for record in &self.refused {
            rejected.push(RejectedRecord::at_hand_over(*record));
        }
        for taken in &self.taken {
            if taken.verdict == ScriptedVerdict::Rejected && taken.given_by(self.end) {
                rejected.push(RejectedRecord::by_broker(taken.record));
            }
        }
        rejected.sort();
        rejected
    }
}

/// What the window reported for a scripted publish, and how long the publish took.
struct ScriptedPublish {
    took: Duration,
    outcome: PerRecordOutcomeParts<SinkRecordId>,
}

impl WindowCase {
    fn max_in_flight(&self) -> usize {
        usize::from(self.max_in_flight % 6) + 1
    }

    fn timeout_millis(&self) -> u64 {
        u64::from(self.timeout % 40) + 1
    }

    /// The position of the record whose hand-over the driver fails.
    fn driver_fails_at(&self) -> Option<usize> {
        let selector = usize::from(self.driver_fails_at?);
        if self.records.is_empty() {
            return None;
        }
        Some(selector % self.records.len())
    }

    /// The publish the documented window rule gives this case, computed in whole milliseconds: a
    /// record's deadline is the timeout after its hand-over; a window holding `MAX` records waits
    /// for the oldest answer until the oldest's deadline before the next hand-over; a publish that
    /// handed every record over waits for each answer still awaited, oldest first; and the first
    /// failure, a missed deadline or a failed oldest answer or the driver's refusal, ends it.
    fn reference(&self) -> ReferencePublish {
        let max_in_flight = self.max_in_flight();
        let timeout = self.timeout_millis();
        let mut now = 0;
        let mut in_flight = VecDeque::new();
        let mut taken = Vec::new();
        let mut refused = Vec::new();
        for (index, record) in self.records.iter().enumerate() {
            now += u64::from(record.pause % 10);
            let id = SinkRecordId::new(index);
            if self.driver_fails_at() == Some(index) {
                return ReferencePublish {
                    end: now,
                    taken,
                    refused,
                    failure: Some(ExpectedFailure::DriverRefused),
                };
            }
            match record.treatment(timeout) {
                Treatment::Refused => refused.push(id),
                Treatment::Taken {
                    verdict,
                    answer_after,
                } => {
                    let given_at = answer_after.map(|after| now + after);
                    let handed_over = ReferenceRecord {
                        record: id,
                        verdict,
                        given_at,
                        deadline: now + timeout,
                    };
                    taken.push(handed_over);
                    in_flight.push_back(handed_over);
                    if in_flight.len() >= max_in_flight
                        && let Some(failure) = Self::await_oldest(&mut in_flight, &mut now)
                    {
                        return ReferencePublish {
                            end: now,
                            taken,
                            refused,
                            failure: Some(failure),
                        };
                    }
                }
            }
        }
        while !in_flight.is_empty() {
            if let Some(failure) = Self::await_oldest(&mut in_flight, &mut now) {
                return ReferencePublish {
                    end: now,
                    taken,
                    refused,
                    failure: Some(failure),
                };
            }
        }
        ReferencePublish {
            end: now,
            taken,
            refused,
            failure: None,
        }
    }

    /// The reference wait for the oldest answer: given by now, it is taken at once; given by the
    /// oldest's deadline, the wait ends when it is given; otherwise the wait ends at the deadline,
    /// or at once when the deadline has passed.
    fn await_oldest(
        in_flight: &mut VecDeque<ReferenceRecord>,
        now: &mut u64,
    ) -> Option<ExpectedFailure> {
        let oldest = *in_flight
            .front()
            .verified("the reference waits only while a record is in flight");
        let wait_until = (*now).max(oldest.deadline);
        if !oldest.given_by(wait_until) {
            *now = wait_until;
            return Some(ExpectedFailure::TimedOut);
        }
        let given_at = oldest
            .given_at
            .verified("a record given by an instant has an answer");
        *now = (*now).max(given_at);
        in_flight.pop_front();
        match oldest.verdict {
            ScriptedVerdict::Failed => Some(ExpectedFailure::AnswerFailed(oldest.record)),
            ScriptedVerdict::Confirmed | ScriptedVerdict::Rejected => None,
        }
    }

    /// Hands every record over to `window` in order, and returns the failure that ends the publish
    /// before every record was handed over, if one does.
    async fn hand_over<'a>(
        &self,
        window: &mut ConfirmationWindow<ScriptedAnswer<'a>>,
        held: &'a Cell<usize>,
    ) -> Option<Report<SinkPublishError>> {
        let timeout = self.timeout_millis();
        for (index, record) in self.records.iter().enumerate() {
            nervix_primitives::task::consume_budget().await;
            sleep(record.pause()).await;
            let id = SinkRecordId::new(index);
            if self.driver_fails_at() == Some(index) {
                return Some(driver_refused());
            }
            match record.treatment(timeout) {
                Treatment::Refused => window.reject(RejectedSinkRecord::external(
                    id,
                    occurred_at(id),
                    REFUSED_AT_HAND_OVER.to_owned(),
                )),
                Treatment::Taken {
                    verdict,
                    answer_after,
                } => {
                    let given_at =
                        answer_after.map(|after| Instant::now() + Duration::from_millis(after));
                    let answer = ScriptedAnswer::new(id, verdict, given_at, held);
                    if let Err(failure) = window.admit(id, occurred_at(id), answer).await {
                        return Some(failure);
                    }
                    assert!(
                        held.get() < self.max_in_flight(),
                        "a window took record {} while {} answers were awaited, at MAX {}",
                        index,
                        held.get(),
                        self.max_in_flight()
                    );
                }
            }
        }
        None
    }

    /// Publishes this case through a window on a paused clock.
    fn publish(&self) -> ScriptedPublish {
        paused_runtime().block_on(async {
            let began = Instant::now();
            let held = Cell::new(0);
            let mut window = ConfirmationWindow::new(
                SINK,
                settings(
                    self.max_in_flight(),
                    Duration::from_millis(self.timeout_millis()),
                ),
                self.records.len(),
            );
            let outcome = match self.hand_over(&mut window, &held).await {
                Some(failure) => window.fail(failure),
                None => window.finish().await,
            };
            assert_eq!(held.get(), 0, "an ended publish holds no answer");
            ScriptedPublish {
                took: began.elapsed(),
                outcome: outcome.into_parts(),
            }
        })
    }
}

/// The records `outcome` rejected, with the message each is rejected with, after checking that
/// each rejection is the external publish error of its own record.
fn rejected_records(outcome: &PerRecordOutcomeParts<SinkRecordId>) -> Vec<RejectedRecord> {
    let mut rejected = Vec::new();
    for rejection in &outcome.rejected {
        assert_eq!(rejection.error.code, MessageErrorCode::External);
        assert_eq!(rejection.error.operation, MessageErrorOperation::Publish);
        assert_eq!(
            rejection.error.occurred_at,
            occurred_at(rejection.id),
            "a rejection keeps its own record's time"
        );
        rejected.push(RejectedRecord {
            record: rejection.id,
            message: rejection.error.message.clone(),
        });
    }
    rejected.sort();
    rejected
}

/// Checks that `failure` is the failure `expected` names.
fn assert_failure(
    failure: &Report<SinkPublishError>,
    expected: ExpectedFailure,
    timeout: Duration,
) {
    assert_eq!(
        failure.current_context(),
        &SinkPublishError::Publish { sink: SINK }
    );
    match expected {
        ExpectedFailure::DriverRefused => assert!(
            failure.contains::<DriverRefusedRecords>(),
            "the driver's own failure ends the publish: {failure:?}"
        ),
        ExpectedFailure::AnswerFailed(record) => assert_eq!(
            failure.downcast_ref::<FailedAnswerOf>(),
            Some(&FailedAnswerOf(record)),
            "the oldest record's failed answer ends the publish: {failure:?}"
        ),
        ExpectedFailure::TimedOut => assert_eq!(
            failure.downcast_ref::<ConfirmationWindowError>(),
            Some(&ConfirmationWindowError::AnswerTimedOut { timeout }),
            "the oldest record's missed deadline ends the publish: {failure:?}"
        ),
    }
}

/// The first printable attachment of `failure`: the description an emitter shows as its transient
/// error.
fn printable_description(failure: &Report<SinkPublishError>) -> Option<String> {
    for frame in failure.frames() {
        if let FrameKind::Attachment(AttachmentKind::Printable(attachment)) = frame.kind() {
            return Some(attachment.to_string());
        }
    }
    None
}

#[test]
fn bolero_a_publish_settles_every_answer_given_before_it_ends() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .with_type::<WindowCase>()
        .for_each(|case| {
            let reference = case.reference();
            let published = case.publish();
            assert_eq!(
                published.took,
                Duration::from_millis(reference.end),
                "the publish ends when the window rule ends it"
            );

            // No record whose broker confirmed it before the publish ended is left unresolved, and
            // no other record is delivered.
            let mut delivered = published.outcome.delivered.clone();
            delivered.sort();
            assert_eq!(delivered, reference.delivered());
            assert_eq!(rejected_records(&published.outcome), reference.rejected());

            match (&published.outcome.infrastructure_error, reference.failure) {
                (None, None) => {}
                (Some(failure), Some(expected)) => assert_failure(
                    failure,
                    expected,
                    Duration::from_millis(case.timeout_millis()),
                ),
                (failure, expected) => {
                    panic!("the publish ended with {failure:?}, the window rule with {expected:?}")
                }
            }
        });
}

/// One record taken in a focused test, answered after `answer_after` or never.
struct Taken {
    verdict: ScriptedVerdict,
    answer_after: Option<Duration>,
}

/// Hands `records` over one after another, without a pause between them, and returns the failure
/// that ends the publish before every record was handed over, if one does.
async fn hand_over_all<'a>(
    window: &mut ConfirmationWindow<ScriptedAnswer<'a>>,
    records: &[Taken],
    held: &'a Cell<usize>,
) -> SinkPublishResult<()> {
    for (index, taken) in records.iter().enumerate() {
        nervix_primitives::task::consume_budget().await;
        let record = SinkRecordId::new(index);
        let given_at = taken.answer_after.map(|after| Instant::now() + after);
        let answer = ScriptedAnswer::new(record, taken.verdict, given_at, held);
        window.admit(record, occurred_at(record), answer).await?;
    }
    Ok(())
}

fn sorted_delivered(outcome: &PerRecordOutcomeParts<SinkRecordId>) -> Vec<SinkRecordId> {
    let mut delivered = outcome.delivered.clone();
    delivered.sort();
    delivered
}

fn record_ids(indices: &[usize]) -> Vec<SinkRecordId> {
    indices.iter().copied().map(SinkRecordId::new).collect()
}

/// The defect the window removes: a driver that fails while answers are awaited no longer drops
/// the answers the broker already gave, so the host's retry never resends a confirmed record.
#[nervix_primitives::test(start_paused = true)]
async fn a_driver_failure_settles_every_answer_the_broker_already_gave() {
    let held = Cell::new(0);
    let mut window = ConfirmationWindow::new(SINK, settings(5, Duration::from_millis(50)), 5);
    let records = [
        Taken {
            verdict: ScriptedVerdict::Confirmed,
            answer_after: Some(Duration::from_millis(2)),
        },
        Taken {
            verdict: ScriptedVerdict::Rejected,
            answer_after: Some(Duration::from_millis(1)),
        },
        Taken {
            verdict: ScriptedVerdict::Failed,
            answer_after: Some(Duration::from_millis(1)),
        },
        Taken {
            verdict: ScriptedVerdict::Confirmed,
            answer_after: Some(Duration::from_millis(30)),
        },
        Taken {
            verdict: ScriptedVerdict::Confirmed,
            answer_after: Some(Duration::ZERO),
        },
    ];
    hand_over_all(&mut window, &records, &held)
        .await
        .verified("a window of five never waits while it takes five records");
    sleep(Duration::from_millis(5)).await;

    let outcome = window.fail(driver_refused()).into_parts();

    assert_eq!(sorted_delivered(&outcome), record_ids(&[0, 4]));
    assert_eq!(
        rejected_records(&outcome),
        vec![RejectedRecord::by_broker(SinkRecordId::new(1))]
    );
    let failure = outcome
        .infrastructure_error
        .verified("the driver's failure ends the publish");
    assert_failure(
        &failure,
        ExpectedFailure::DriverRefused,
        Duration::from_millis(50),
    );
    assert_eq!(held.get(), 0);
}

/// More given answers than Tokio's cooperative budget lets one task poll read.
const ANSWERS_BEYOND_ONE_BUDGET: usize = 300;

/// A window larger than the publish, so taking its records never waits for an answer.
const WINDOW_LARGER_THAN_THE_PUBLISH: usize = 512;

const _: () = assert!(WINDOW_LARGER_THAN_THE_PUBLISH > ANSWERS_BEYOND_ONE_BUDGET);

/// An answer a driver gives through a runtime channel, as broker drivers answer their publishes.
type ChannelAnswer = Pin<Box<dyn Future<Output = BrokerAnswer> + Send>>;

/// The answer `receiver` carries, which its sender gave before the window reads it.
fn channel_answer(receiver: oneshot::Receiver<BrokerVerdict>) -> ChannelAnswer {
    Box::pin(async move {
        let verdict = receiver
            .await
            .assured("each sender gives its answer before the publish fails");
        Ok(verdict)
    })
}

/// A failure settles every answer a driver already gave through a runtime channel, even when there
/// are more of them than one cooperative budget lets a task read: the runtime never refuses the
/// window an answer that has arrived, so the host's retry never resends a confirmed record.
#[nervix_primitives::test]
async fn a_failure_settles_more_given_answers_than_one_cooperative_budget() {
    let mut window = ConfirmationWindow::new(
        SINK,
        settings(WINDOW_LARGER_THAN_THE_PUBLISH, Duration::from_secs(30)),
        ANSWERS_BEYOND_ONE_BUDGET,
    );
    for index in 0..ANSWERS_BEYOND_ONE_BUDGET {
        nervix_primitives::task::consume_budget().await;
        let record = SinkRecordId::new(index);
        let (give, receiver) = oneshot::channel();
        give.send(BrokerVerdict::Confirmed)
            .assured("the window holds the receiver as the record's answer");
        window
            .admit(record, occurred_at(record), channel_answer(receiver))
            .await
            .verified("a window larger than the publish never waits while it takes the records");
    }

    let outcome = window.fail(driver_refused()).into_parts();

    assert_eq!(
        outcome.delivered.len(),
        ANSWERS_BEYOND_ONE_BUDGET,
        "every answer the driver gave is settled"
    );
    let every_record: Vec<usize> = (0..ANSWERS_BEYOND_ONE_BUDGET).collect();
    assert_eq!(sorted_delivered(&outcome), record_ids(&every_record));
    assert!(outcome.rejected.is_empty());
}

/// A full window waits for its oldest answer until the oldest's own deadline, not a full timeout
/// from the wait, and the missed deadline still settles the answers given before it.
#[nervix_primitives::test(start_paused = true)]
async fn a_full_window_waits_for_the_oldest_until_its_own_deadline() {
    let held = Cell::new(0);
    let began = Instant::now();
    let timeout = Duration::from_millis(10);
    let mut window = ConfirmationWindow::new(SINK, settings(2, timeout), 2);
    let oldest = ScriptedAnswer::new(
        SinkRecordId::new(0),
        ScriptedVerdict::Confirmed,
        None,
        &held,
    );
    window
        .admit(
            SinkRecordId::new(0),
            occurred_at(SinkRecordId::new(0)),
            oldest,
        )
        .await
        .verified("a window of two takes its first record without waiting");
    sleep(Duration::from_millis(4)).await;
    let younger = ScriptedAnswer::new(
        SinkRecordId::new(1),
        ScriptedVerdict::Confirmed,
        Some(Instant::now() + Duration::from_millis(2)),
        &held,
    );

    let admitted = window
        .admit(
            SinkRecordId::new(1),
            occurred_at(SinkRecordId::new(1)),
            younger,
        )
        .await;
    let Err(failure) = admitted else {
        panic!("a full window took a record although its oldest answer never arrives");
    };

    assert_eq!(
        began.elapsed(),
        timeout,
        "the wait ends at the oldest's deadline"
    );
    assert_failure(&failure, ExpectedFailure::TimedOut, timeout);
    assert_eq!(
        format!("{failure:#}"),
        "failed to publish through scripted sink: the broker did not answer within ACK TIMEOUT \
         10ms",
        "the rendered chain names the sink, then the deadline it missed"
    );
    assert_eq!(
        printable_description(&failure).as_deref(),
        Some("the broker did not answer within ACK TIMEOUT 10ms"),
        "the window describes the deadline it missed for the emitter's transient error"
    );
    let outcome = window.fail(failure).into_parts();
    assert_eq!(sorted_delivered(&outcome), record_ids(&[1]));
    assert!(outcome.rejected.is_empty());
}

/// An answer the broker gives at the very instant of its deadline is an answer, not a timeout.
#[nervix_primitives::test(start_paused = true)]
async fn an_answer_given_at_its_deadline_is_settled() {
    let held = Cell::new(0);
    let began = Instant::now();
    let timeout = Duration::from_millis(10);
    let mut window = ConfirmationWindow::new(SINK, settings(1, timeout), 1);
    let records = [Taken {
        verdict: ScriptedVerdict::Confirmed,
        answer_after: Some(timeout),
    }];
    hand_over_all(&mut window, &records, &held)
        .await
        .verified("the answer arrives by the deadline");

    let outcome = window.finish().await.into_parts();

    assert_eq!(began.elapsed(), timeout);
    assert_eq!(sorted_delivered(&outcome), record_ids(&[0]));
    assert!(outcome.infrastructure_error.is_none());
}

/// `finish` waits for every answer still awaited, oldest first, whatever order they arrive in.
#[nervix_primitives::test(start_paused = true)]
async fn finish_settles_every_answer_whatever_order_it_arrives_in() {
    let held = Cell::new(0);
    let began = Instant::now();
    let mut window = ConfirmationWindow::new(SINK, settings(3, Duration::from_millis(20)), 3);
    let records = [
        Taken {
            verdict: ScriptedVerdict::Confirmed,
            answer_after: Some(Duration::from_millis(7)),
        },
        Taken {
            verdict: ScriptedVerdict::Rejected,
            answer_after: Some(Duration::from_millis(2)),
        },
        Taken {
            verdict: ScriptedVerdict::Confirmed,
            answer_after: Some(Duration::from_millis(5)),
        },
    ];
    hand_over_all(&mut window, &records, &held)
        .await
        .verified("a window of three never waits while it takes three records");

    let outcome = window.finish().await.into_parts();

    assert_eq!(began.elapsed(), Duration::from_millis(7));
    assert_eq!(sorted_delivered(&outcome), record_ids(&[0, 2]));
    assert_eq!(
        rejected_records(&outcome),
        vec![RejectedRecord::by_broker(SinkRecordId::new(1))]
    );
    assert!(outcome.infrastructure_error.is_none());
    assert_eq!(held.get(), 0);
}

/// A failed oldest answer ends the publish with that failure and leaves its record unresolved.
#[nervix_primitives::test(start_paused = true)]
async fn a_failed_oldest_answer_ends_the_publish_and_leaves_its_record_unresolved() {
    let held = Cell::new(0);
    let mut window = ConfirmationWindow::new(SINK, settings(1, Duration::from_millis(20)), 2);
    let records = [
        Taken {
            verdict: ScriptedVerdict::Failed,
            answer_after: Some(Duration::from_millis(3)),
        },
        Taken {
            verdict: ScriptedVerdict::Confirmed,
            answer_after: Some(Duration::ZERO),
        },
    ];

    let handed_over = hand_over_all(&mut window, &records, &held).await;
    let Err(failure) = handed_over else {
        panic!("a window of one took its second record although the first answer failed");
    };
    let outcome = window.fail(failure).into_parts();

    assert!(outcome.delivered.is_empty());
    assert!(outcome.rejected.is_empty());
    let failure = outcome
        .infrastructure_error
        .verified("the failed answer ends the publish");
    assert_failure(
        &failure,
        ExpectedFailure::AnswerFailed(SinkRecordId::new(0)),
        Duration::from_millis(20),
    );
}

/// A deadline the monotonic clock cannot represent ends the publish rather than waiting forever,
/// and the record it was computed for stays unresolved.
#[nervix_primitives::test(start_paused = true)]
async fn a_deadline_past_the_clock_range_ends_the_publish() {
    let held = Cell::new(0);
    let mut window = ConfirmationWindow::new(SINK, settings(4, Duration::MAX), 1);
    let answer = ScriptedAnswer::new(
        SinkRecordId::new(0),
        ScriptedVerdict::Confirmed,
        Some(Instant::now()),
        &held,
    );

    let admitted = window
        .admit(
            SinkRecordId::new(0),
            occurred_at(SinkRecordId::new(0)),
            answer,
        )
        .await;
    let Err(failure) = admitted else {
        panic!("a window took a record whose deadline the monotonic clock cannot represent");
    };

    assert_eq!(
        failure.downcast_ref::<ConfirmationWindowError>(),
        Some(&ConfirmationWindowError::DeadlineOutOfRange {
            timeout: Duration::MAX
        })
    );
    assert_eq!(
        printable_description(&failure),
        Some(
            ConfirmationWindowError::DeadlineOutOfRange {
                timeout: Duration::MAX
            }
            .to_string()
        ),
        "the window describes the deadline it has no clock for"
    );
    assert_eq!(
        held.get(),
        0,
        "the window took no answer it has no deadline for"
    );
    let outcome = window.fail(failure).into_parts();
    assert!(outcome.delivered.is_empty());
    assert!(outcome.infrastructure_error.is_some());
}
