//! The confirmation window a brokered sink publishes through in `MODE ACK`.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The records a confirmed publish handed its broker and still awaits answers for,
//!   oldest first and at most the declared `MAX` at once; each record's deadline, `ACK TIMEOUT`
//!   after its own hand-over; the wait for the oldest answer until that deadline; and the publish
//!   outcome the answers settle, including every answer a failure finds already given.
//! - **Depends on.** The sink contract's outcome, record identity and failure types, the
//!   boundary's monotonic clock and deadline-bounded wait, and the futures through which a
//!   connector's driver answers.
//! - **Must not know.** Any broker driver or how a connector classifies its driver's answers, the
//!   host's retry, buffer or backoff, or the source acknowledgements a record carries.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::Timestamp;
use nervix_primitives::time::{Instant, timeout_at};
use nervix_recovery::Discarded as _;
use thiserror::Error;

use crate::sink::{
    AckConfirmation, PerRecordOutcome, RejectedSinkRecord, SinkPublishError, SinkPublishResult,
    SinkRecordId,
};

/// Why a failed answer is dropped while a failure settles the window: the record's outcome is
/// unknown, so it stays unresolved for the host's retry, and the publish already reports the
/// failure that ends it.
const UNKNOWN_OUTCOME: &str = "the record stays unresolved for the host's retry, and the publish \
                               reports the failure that ended it";

/// What a broker decided about one record it was handed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerVerdict {
    /// The broker confirmed the record.
    Confirmed,
    /// The broker refused the record for good, so sending it again cannot succeed. `message` is
    /// what the record's message error reports.
    Rejected { message: String },
}

/// What the answer for one record resolves to: the broker's verdict on it, or the failure that
/// left its outcome unknown, because the connection, the client or the broker failed rather than
/// the record.
pub type BrokerAnswer = Result<BrokerVerdict, Report<SinkPublishError>>;

/// Why a confirmed publish stopped waiting for its broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ConfirmationWindowError {
    /// The oldest record's answer did not arrive within `ACK TIMEOUT` of its hand-over. The broker
    /// may still have taken the record, so its outcome is unknown.
    #[error(
        "the broker did not answer within ACK TIMEOUT {}",
        humantime::format_duration(*.timeout)
    )]
    AnswerTimedOut { timeout: Duration },
    /// `ACK TIMEOUT` after a record's hand-over is past the monotonic clock's range, so there is no
    /// deadline to wait for the record's answer until.
    #[error(
        "ACK TIMEOUT {} after a record's hand-over is past the monotonic clock's range",
        humantime::format_duration(*.timeout)
    )]
    DeadlineOutOfRange { timeout: Duration },
}

/// The records a confirmed publish handed its broker and awaits answers for, and the outcome
/// their answers settle.
///
/// A brokered sink in `MODE ACK` hands its records to the driver one after another and receives,
/// for each, a future that resolves to the broker's [`BrokerAnswer`] for it. The window takes each
/// record with that future, oldest first. A record's deadline is `ACK TIMEOUT` after its own
/// hand-over. While the window holds `MAX` records it waits for the oldest answer, until the
/// oldest's deadline and no longer, before the sink hands over another record, so at most `MAX`
/// records await their answers at once. `ACK SEQUENTIAL` is a window of one.
///
/// A publish ends in one of two ways. Both consume the window, which holds the publish's outcome,
/// so the outcome cannot be reported without them:
///
/// - [`ConfirmationWindow::finish`], once every record was handed over, waits for each answer
///   still awaited, oldest first and each until its own deadline.
/// - [`ConfirmationWindow::fail`] ends the publish with a failure: the driver could not take a
///   record for a reason that is not the record's own, the oldest answer failed or missed its
///   deadline, or a deadline is past the clock's range.
///
/// A failure ends the publish at once but never discards an answer the broker already gave. Every
/// given answer still in the window is settled first, the oldest's included: a confirmed record is
/// delivered and a refused one rejected, so the host's retry never sends either again. A record
/// whose answer has not arrived, or whose answer itself failed, stays unresolved: its outcome is
/// unknown, and the host's retry sends it again, which is the at-least-once delivery `MODE ACK`
/// promises.
#[must_use = "a confirmed publish reports its outcome only through `finish` or `fail`"]
pub struct ConfirmationWindow<A> {
    /// The sink a failure of the window is reported for.
    sink: &'static str,
    settings: AckConfirmation,
    /// The records awaiting their answers, oldest first.
    in_flight: VecDeque<InFlightRecord<A>>,
    outcome: PerRecordOutcome<SinkRecordId>,
}

/// One record the broker was handed, and the future its answer arrives through.
struct InFlightRecord<A> {
    record: SinkRecordId,
    occurred_at: Timestamp,
    /// `ACK TIMEOUT` after the record's hand-over.
    deadline: Instant,
    answer: A,
}

impl<A> InFlightRecord<A>
where
    A: Future<Output = BrokerAnswer> + Unpin,
{
    /// The answer the broker already gave for this record, read without waiting for one.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "polls the future through which the connector's broker driver answers for \
                      this record; its implementation is the driver's"
        )
    )]
    fn given_answer(&mut self) -> Option<BrokerAnswer> {
        let mut context = Context::from_waker(Waker::noop());
        match Pin::new(&mut self.answer).poll(&mut context) {
            Poll::Ready(answer) => Some(answer),
            Poll::Pending => None,
        }
    }

    /// Records the broker's verdict on this record in `outcome`.
    fn settle(self, verdict: BrokerVerdict, outcome: &mut PerRecordOutcome<SinkRecordId>) {
        match verdict {
            BrokerVerdict::Confirmed => outcome.deliver(self.record),
            BrokerVerdict::Rejected { message } => outcome.reject(RejectedSinkRecord::external(
                self.record,
                self.occurred_at,
                message,
            )),
        }
    }
}

impl<A> ConfirmationWindow<A>
where
    A: Future<Output = BrokerAnswer> + Unpin,
{
    /// An empty window for a publish of `records` records through `sink`, which awaits answers for
    /// at most `settings.max_in_flight` of them at once and for each until `settings.timeout` after
    /// its hand-over.
    pub fn new(sink: &'static str, settings: AckConfirmation, records: usize) -> Self {
        let in_flight = settings.max_in_flight.get().min(records);
        Self {
            sink,
            settings,
            in_flight: VecDeque::with_capacity(in_flight),
            outcome: PerRecordOutcome::with_capacity(records),
        }
    }

    /// Records a record the driver refused for good as it was handed over, so it never awaited an
    /// answer.
    pub fn reject(&mut self, rejected: RejectedSinkRecord<SinkRecordId>) {
        self.outcome.reject(rejected);
    }

    /// Takes `record`, which the broker was just handed, with the future its answer arrives
    /// through.
    ///
    /// The record's deadline is `ACK TIMEOUT` from now. When the window then holds `MAX` records,
    /// this waits for the oldest answer until the oldest's deadline and settles it.
    ///
    /// An error ends the publish, which the caller ends with [`ConfirmationWindow::fail`] and that
    /// error: the oldest answer failed, so the oldest record left the window unresolved; the oldest
    /// answer did not arrive by its deadline; or the new record's deadline is past the clock's
    /// range, so the new record was never taken and stays unresolved.
    pub async fn admit(
        &mut self,
        record: SinkRecordId,
        occurred_at: Timestamp,
        answer: A,
    ) -> SinkPublishResult<()> {
        let timeout = self.settings.timeout;
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return Err(self.stopped(ConfirmationWindowError::DeadlineOutOfRange { timeout }));
        };
        self.in_flight.push_back(InFlightRecord {
            record,
            occurred_at,
            deadline,
            answer,
        });
        if self.in_flight.len() < self.settings.max_in_flight.get() {
            return Ok(());
        }
        self.settle_oldest().await
    }

    /// Ends a publish that handed every record over: waits for each answer still awaited, oldest
    /// first and each until its own deadline.
    ///
    /// When an answer fails or misses its deadline, the publish ends with that failure as
    /// [`ConfirmationWindow::fail`] ends it.
    pub async fn finish(mut self) -> PerRecordOutcome<SinkRecordId> {
        while !self.in_flight.is_empty() {
            nervix_primitives::task::consume_budget().await;
            if let Err(failure) = self.settle_oldest().await {
                return self.fail(failure);
            }
        }
        self.outcome
    }

    /// Ends the publish with `failure`, settling every answer the broker already gave.
    ///
    /// Each record still in the window whose answer has arrived is settled, the oldest's included:
    /// a confirmed record is delivered and a refused one rejected, so the host's retry never sends
    /// either again. A record without an answer, or whose answer failed, stays unresolved for the
    /// retry.
    pub fn fail(self, failure: Report<SinkPublishError>) -> PerRecordOutcome<SinkRecordId> {
        let Self {
            in_flight,
            mut outcome,
            ..
        } = self;
        for mut awaited in in_flight {
            let Some(answer) = awaited.given_answer() else {
                continue;
            };
            match answer {
                Ok(verdict) => awaited.settle(verdict, &mut outcome),
                failed @ Err(_) => failed.discarded(UNKNOWN_OUTCOME),
            }
        }
        outcome.fail(failure);
        outcome
    }

    /// Waits for the oldest answer until the oldest's deadline, and settles it.
    ///
    /// A failed answer takes the oldest record out of the window unresolved and is returned as the
    /// failure that ends the publish. A missed deadline leaves the oldest record in the window, so
    /// the failure that ends the publish still settles an answer that arrives after the deadline
    /// and before the window is settled.
    async fn settle_oldest(&mut self) -> SinkPublishResult<()> {
        let oldest = self
            .in_flight
            .front_mut()
            .verified("every caller checks that the window holds a record before waiting for it");
        let deadline = oldest.deadline;
        let Ok(answer) = timeout_at(deadline, &mut oldest.answer).await else {
            let timeout = self.settings.timeout;
            return Err(self.stopped(ConfirmationWindowError::AnswerTimedOut { timeout }));
        };
        let oldest = self
            .in_flight
            .pop_front()
            .verified("the oldest record was read from the front of this window above");
        let verdict = answer?;
        oldest.settle(verdict, &mut self.outcome);
        Ok(())
    }

    /// The failure that ends a publish whose window stopped waiting for the broker.
    fn stopped(&self, error: ConfirmationWindowError) -> Report<SinkPublishError> {
        Report::new(error).change_context(SinkPublishError::Publish { sink: self.sink })
    }
}

#[cfg(test)]
#[path = "confirmation_window_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "confirmation_window_shuttle_tests.rs"]
mod shuttle_tests;
