//! Physical retry and acknowledgement liveness for the work an emitter holds.
//!
//! Layer: data plane.
//! - **Owns.** The monotonic deadline of an emitter's next attempt, the keepalive of the
//!   acknowledgements it holds between publishes, and how long each retry waits.
//! - **Depends on.** The emitter's declared backoff, the physical-time capability, the node's
//!   installed remote acknowledgement timing, the acknowledgements of its buffered and staged work,
//!   and its transient-error status.
//! - **Must not know.** What the emitter publishes, which connector it publishes through, or the
//!   domain clock its flush cadence reads.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::{
    EmitterRecordAcknowledgements, EmitterRetryDelay,
    physical_time::{PhysicalDeadline, PhysicalDeadlineCapability},
};
use nervix_models::ExternalErrorCode;

use super::{emitter_buffer::GiveUpClock, *};

/// Why an emitter's acknowledgement beat always fits the monotonic clock.
const ACK_ALIVE_BEAT_FITS: &str = "the installed alive interval is a validated cluster parameter \
                                   of at most u32::MAX seconds, which the monotonic clock's \
                                   64-bit seconds add to any instant it reports";

/// One physical beat for buffered or connector-staged work. It retains no ACK handles and
/// admits each input's obligations once, so hot input neither postpones the beat nor repeatedly
/// copies the growing buffer's acknowledgements. A retry owns its own beat while active.
///
/// Each beat waits the committed `REMOTE_ACK_ALIVE_INTERVAL` it reads as it is armed, so a
/// committed change reaches the emitter's next beat.
pub(super) struct EmitterBufferedKeepalive {
    /// The installed remote acknowledgement timing the emitter task kept from its start.
    timing: SharedRemoteAckTiming,
    next: Option<PhysicalDeadline>,
}

impl EmitterBufferedKeepalive {
    pub(super) fn new(timing: SharedRemoteAckTiming) -> Self {
        Self { timing, next: None }
    }

    pub(super) fn include_batch(&mut self, batch: &EmitterPublishBatch) {
        if self.next.is_none() && batch.relay_batch().acks.iter().any(|acks| !acks.is_empty()) {
            self.arm();
        }
    }

    fn arm(&mut self) {
        self.next = Some(
            PhysicalDeadlineCapability::operational()
                .after(self.timing.ack_alive_interval())
                .assured(ACK_ALIVE_BEAT_FITS),
        );
    }

    pub(super) fn wake(&self, ordinary: RuntimeWake, retry_active: bool) -> RuntimeWake {
        match (retry_active, self.next) {
            (false, Some(next)) => ordinary.with_physical(next),
            _ => ordinary,
        }
    }

    pub(super) fn take_due(&mut self) -> bool {
        if self
            .next
            .is_some_and(|next| PhysicalDeadlineCapability::operational().is_reached(next))
        {
            self.arm();
            true
        } else {
            false
        }
    }

    pub(super) fn refresh(&mut self, acks: &EmitterAcknowledgements) {
        if acks.is_empty() {
            self.clear();
        } else if self.next.is_none() {
            self.arm();
        }
    }

    pub(super) fn clear(&mut self) {
        self.next = None;
    }
}

/// One deferral of an emitter's publish attempt: how long the emitter waits, which
/// acknowledgements it keeps alive meanwhile, and what the operator is told about the wait.
pub(super) struct EmitterRetryDeferral<'a> {
    pub(super) cause: EmitterRetryCause,
    pub(super) wait: Duration,
    pub(super) acks: EmitterAcknowledgements,
    pub(super) waiting_for_stall_clear: bool,
    pub(super) reason: Option<&'a str>,
    /// What the operator is told the emitter retries: an outage, a commit or a misconfiguration.
    pub(super) kind: EmitterRetryKind,
    /// When the emitter gives up the oldest unit it retries, which the operator is told too.
    pub(super) give_up_at: Option<PhysicalDeadline>,
}

#[derive(Default)]
pub(super) struct EmitterAcknowledgements {
    pub(super) runtime: AckSet,
    pub(super) connector: Option<EmitterRecordAcknowledgements>,
}

impl EmitterAcknowledgements {
    fn is_empty(&self) -> bool {
        self.runtime.is_empty()
            && self
                .connector
                .as_ref()
                .is_none_or(EmitterRecordAcknowledgements::is_empty)
    }

    fn extend(&mut self, further: Self) {
        self.runtime = AckSet::merged([std::mem::take(&mut self.runtime), further.runtime]);
        if further.connector.is_some() {
            self.connector = further.connector;
        }
    }
}

impl From<AckSet> for EmitterAcknowledgements {
    fn from(runtime: AckSet) -> Self {
        Self {
            runtime,
            connector: None,
        }
    }
}

impl AcknowledgementKeepalive for EmitterAcknowledgements {
    fn keep_alive(&self) {
        self.runtime.ack_alive();
        if let Some(acks) = &self.connector {
            acks.keep_alive();
        }
    }
}

/// The emitter's physical retry state: when the next publish attempt is allowed, and how often
/// the acknowledgements it is holding are kept alive until then.
///
/// Both deadlines are monotonic. Retry backoff measures real unavailability of an external system
/// and an acknowledgement keepalive measures a real upstream liveness expectation, so neither is
/// scaled by the domain's pace. While a retry is scheduled it replaces the emitter's cadence wake,
/// which stays armed and unchanged underneath it. Each keepalive beat waits the committed
/// `REMOTE_ACK_ALIVE_INTERVAL` it reads as it is armed.
pub(super) struct EmitterRetrySchedule {
    /// The installed remote acknowledgement timing the emitter task kept from its start.
    timing: SharedRemoteAckTiming,
    retry_at: Option<PhysicalDeadline>,
    ack_alive_at: Option<PhysicalDeadline>,
    acks: EmitterAcknowledgements,
    waiting_for_stall_clear: bool,
}

impl EmitterRetrySchedule {
    pub(super) fn new(timing: SharedRemoteAckTiming) -> Self {
        Self {
            timing,
            retry_at: None,
            ack_alive_at: None,
            acks: EmitterAcknowledgements::default(),
            waiting_for_stall_clear: false,
        }
    }

    pub(super) fn is_active(&self) -> bool {
        self.retry_at.is_some()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the admitted retry converts its retained acknowledgement \
                                   obligation through the caller-supplied Into contract")
    )]
    fn schedule(
        &mut self,
        delay: Duration,
        acks: impl Into<EmitterAcknowledgements>,
        waiting_for_stall_clear: bool,
    ) -> EmitterRuntimeResult<()> {
        let acks = acks.into();
        let retry_at = PhysicalDeadlineCapability::operational()
            .after(delay)
            .change_context(EmitterRuntimeError::RetryTiming)?;
        self.retry_at = Some(retry_at);
        if !acks.is_empty() {
            self.acks = acks;
        }
        self.ack_alive_at = (!self.acks.is_empty()).then(|| self.next_ack_alive_at(retry_at));
        self.waiting_for_stall_clear = waiting_for_stall_clear;
        Ok(())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the admitted retry combines its retained acknowledgement \
                                   obligations through their Into contract")
    )]
    pub(super) fn include_acks(&mut self, acks: impl Into<EmitterAcknowledgements>) {
        let acks = acks.into();
        if acks.is_empty() {
            return;
        }
        self.acks.extend(acks);
        if let Some(retry_at) = self.retry_at
            && self.ack_alive_at.is_none()
        {
            self.ack_alive_at = Some(self.next_ack_alive_at(retry_at));
        }
    }

    /// Branch retirement replaces the held-work view without moving the physical retry or an
    /// already armed keepalive beat. Connector-staged work is included by the caller.
    pub(super) fn replace_acks(&mut self, acks: EmitterAcknowledgements) {
        self.acks = acks;
        if self.acks.is_empty() {
            self.ack_alive_at = None;
        } else if let Some(retry_at) = self.retry_at
            && self.ack_alive_at.is_none()
        {
            self.ack_alive_at = Some(self.next_ack_alive_at(retry_at));
        }
    }

    fn next_ack_alive_at(&self, retry_at: PhysicalDeadline) -> PhysicalDeadline {
        let keepalive = PhysicalDeadlineCapability::operational()
            .after(self.timing.ack_alive_interval())
            .assured(ACK_ALIVE_BEAT_FITS);
        // The last keepalive of a wait lands on the retry itself rather than after it.
        keepalive.min(retry_at)
    }

    /// The wake the emitter asks for: the retry and keepalive deadlines while a retry is
    /// scheduled, and otherwise the emitter's ordinary flush cadence.
    pub(super) fn wake(&self, ordinary: RuntimeWake) -> RuntimeWake {
        let Some(retry_at) = self.retry_at else {
            return ordinary;
        };
        let wake = RuntimeWake::never().with_physical(retry_at);
        match self.ack_alive_at {
            Some(ack_alive_at) => wake.with_physical(ack_alive_at),
            None => wake,
        }
    }

    pub(super) fn retry_is_due(&mut self) -> bool {
        let Some(retry_at) = self.retry_at else {
            return true;
        };
        let physical_time = PhysicalDeadlineCapability::operational();
        if physical_time.is_reached(retry_at) {
            self.retry_at = None;
            self.ack_alive_at = None;
            return true;
        }
        if self
            .ack_alive_at
            .is_some_and(|ack_alive_at| physical_time.is_reached(ack_alive_at))
        {
            self.acks.keep_alive();
            self.ack_alive_at = Some(self.next_ack_alive_at(retry_at));
        }
        false
    }

    pub(super) fn release_if_stall_cleared(&mut self, stalled: bool) -> bool {
        if !self.waiting_for_stall_clear || stalled {
            return false;
        }
        self.retry_at = None;
        self.ack_alive_at = None;
        self.waiting_for_stall_clear = false;
        true
    }

    /// Defers the next publish attempt and records the transient error that explains the wait.
    ///
    /// Constructing the monotonic deadline is the only fallible part. A server-supplied wait the
    /// monotonic clock cannot represent never reaches here, because it contributes nothing to the
    /// wait. A configured one leaves no retry scheduled, so the emitter falls back to its unchanged
    /// flush cadence and the failure is recorded as the emitter's transient error instead of
    /// disappearing.
    pub(super) fn defer(
        &mut self,
        context: &EmitterConnectorContext,
        deferral: EmitterRetryDeferral<'_>,
    ) {
        let EmitterRetryDeferral {
            cause,
            wait,
            acks,
            waiting_for_stall_clear,
            reason,
            kind,
            give_up_at,
        } = deferral;
        match self.schedule(wait, acks, waiting_for_stall_clear) {
            Ok(()) => {
                context.interactions.record_retry(cause, wait);
                if let Some(reason) = reason {
                    context.record_retry(reason, wait, kind, give_up_at);
                }
            }
            Err(error) => {
                context.record_failure(emitter_error_message(&error));
            }
        }
    }

    pub(super) fn clear(&mut self) {
        self.retry_at = None;
        self.ack_alive_at = None;
        self.acks = EmitterAcknowledgements::default();
        self.waiting_for_stall_clear = false;
    }
}

/// An emitter's `GIVE UP AFTER`: how long it keeps retrying a unit of work its destination did not
/// take, why its latest attempt failed, and the work it gave up during a publish call, which the
/// emitter task delivers once the call returns.
///
/// The limit is measured on the physical clock from the first failed attempt that held a unit,
/// across reconnects. Only a failure to reach or use the destination starts it: an encoding the
/// node's bounded execution could not take yet judged nothing about the destination. Each buffered
/// batch keeps its own clock in the buffer, and the rows a connector staged for its commit keep
/// theirs with the connector state.
pub(super) struct EmitterGiveUp {
    limit: Option<Duration>,
    last_failure: Option<EmitterGiveUpCause>,
    given_up: Vec<EmitterPublishBatch>,
    given_up_staged: Vec<EmitterRecordAcknowledgements>,
}

/// Why the emitter's latest attempt failed, as the message errors of the rows it gives up name it.
#[derive(Debug, Clone)]
struct EmitterGiveUpCause {
    reason: String,
    external_code: Option<ExternalErrorCode>,
}

impl EmitterGiveUpCause {
    /// The cause `error` names: its rendered chain, and the outside system's own code when the
    /// connector attached one to the failure.
    fn of(error: &Report<EmitterRuntimeError>) -> Self {
        let mut external_code = None;
        for frame in error.frames() {
            if let Some(code) = frame.downcast_ref::<ExternalErrorCode>() {
                external_code = Some(code.clone());
                break;
            }
        }
        Self {
            reason: emitter_error_message(error),
            external_code,
        }
    }
}

/// Everything an emitter gave up at once, and what the failure of each of its rows says.
pub(super) struct GivenUpWork {
    /// The buffered batches whose unresolved rows fail, each with an `unavailable` error.
    pub(super) batches: Vec<EmitterPublishBatch>,
    /// The acknowledgements of the rows a connector staged for a commit the emitter gave up. The
    /// host no longer holds those rows, so the commit's failure names no row of them.
    pub(super) staged: Vec<EmitterRecordAcknowledgements>,
    /// The message every failure of this work carries.
    pub(super) message: String,
    /// The outside system's code for the latest failed attempt, when its connector gave one.
    pub(super) external_code: Option<ExternalErrorCode>,
}

/// The deadline that marks the current instant, which a give-up clock is compared against.
fn give_up_now() -> PhysicalDeadline {
    PhysicalDeadlineCapability::operational()
        .after(Duration::ZERO)
        .assured("the current instant is within the monotonic clock range it was read from")
}

impl EmitterGiveUp {
    pub(super) fn new(limit: Option<Duration>) -> Self {
        Self {
            limit,
            last_failure: None,
            given_up: Vec::new(),
            given_up_staged: Vec::new(),
        }
    }

    /// The clock an attempt failing now starts: the limit from now, or a clock that never ends when
    /// the limit reaches past every instant the monotonic clock reports. Absent without a limit.
    pub(super) fn started_clock(&self) -> Option<GiveUpClock> {
        let limit = self.limit?;
        let clock = match PhysicalDeadlineCapability::operational().after(limit) {
            Ok(deadline) => GiveUpClock::Until(deadline),
            Err(_) => GiveUpClock::Unreachable,
        };
        Some(clock)
    }

    /// Whether `clock` has run out by now.
    pub(super) fn has_run_out(clock: GiveUpClock) -> bool {
        match clock {
            GiveUpClock::Until(deadline) => {
                PhysicalDeadlineCapability::operational().is_reached(deadline)
            }
            GiveUpClock::RanOut => true,
            GiveUpClock::NotStarted | GiveUpClock::Unreachable => false,
        }
    }

    /// Records an attempt that failed while `buffer` held its batches: their clocks start unless
    /// an earlier failed attempt already started them.
    pub(super) fn record_failure(
        &mut self,
        buffer: &mut EmitterBatchBuffer,
        error: &Report<EmitterRuntimeError>,
    ) {
        if !error.current_context().fails_the_destination() {
            return;
        }
        self.last_failure = Some(EmitterGiveUpCause::of(error));
        if let Some(clock) = self.started_clock() {
            buffer.start_give_up_clocks(clock);
        }
    }

    /// Records a commit of staged rows the destination failed, which its connector state's own
    /// clock counts.
    pub(super) fn record_commit_failure(&mut self, error: &Report<EmitterRuntimeError>) {
        self.last_failure = Some(EmitterGiveUpCause::of(error));
    }

    /// Records that the emitter holds `buffer`'s batches because it has no connector to publish
    /// them through, for `reason`. Not reaching the destination fails every batch it holds.
    pub(super) fn record_unavailable(&mut self, buffer: &mut EmitterBatchBuffer, reason: &str) {
        self.last_failure = Some(EmitterGiveUpCause {
            reason: reason.to_owned(),
            external_code: None,
        });
        if let Some(clock) = self.started_clock() {
            buffer.start_give_up_clocks(clock);
        }
    }

    /// Moves every batch `buffer` has retried for as long as the limit allows into the work this
    /// emitter gave up.
    pub(super) async fn collect(&mut self, buffer: &mut EmitterBatchBuffer) {
        if self.limit.is_none() {
            return;
        }
        let given_up = buffer.take_given_up(give_up_now()).await;
        self.given_up.extend(given_up);
    }

    /// Records the acknowledgements of the staged rows a connector dropped because the emitter gave
    /// their commit up.
    pub(super) fn collect_staged(&mut self, acks: Option<EmitterRecordAcknowledgements>) {
        if let Some(acks) = acks {
            self.given_up_staged.push(acks);
        }
    }

    /// Waits until the give-up `deadline`, or for ever without one. A retry's wait races this, so
    /// it ends no later than the next give-up.
    pub(super) async fn until_run_out(deadline: Option<PhysicalDeadline>) {
        match deadline {
            Some(deadline) => {
                PhysicalDeadlineCapability::operational()
                    .wait_until(deadline)
                    .await;
            }
            None => std::future::pending::<()>().await,
        }
    }

    /// The physical deadline at which `buffer` gives its next batch up.
    pub(super) fn next_deadline(&self, buffer: &EmitterBatchBuffer) -> Option<PhysicalDeadline> {
        match self.limit {
            Some(_) => buffer.next_give_up(),
            None => None,
        }
    }

    /// Takes everything this emitter gave up since it was last asked, absent when it gave nothing
    /// up.
    pub(super) fn take_given_up(&mut self) -> Option<GivenUpWork> {
        if self.given_up.is_empty() && self.given_up_staged.is_empty() {
            return None;
        }
        let limit = self
            .limit
            .verified("only an emitter that declares GIVE UP AFTER gives work up");
        let cause = self.last_failure.clone().verified(
            "a give-up clock starts only once a failed attempt or a missing connector recorded \
             its cause",
        );
        Some(GivenUpWork {
            batches: std::mem::take(&mut self.given_up),
            staged: std::mem::take(&mut self.given_up_staged),
            message: format!(
                "gave up after {} of retries: {}",
                humantime::format_duration(limit),
                cause.reason
            ),
            external_code: cause.external_code,
        })
    }
}

/// How long the emitter asked this emitter to wait, which bounds its next attempt from below.
///
/// A delay whose end the monotonic clock cannot represent asks for an attempt that could never be
/// scheduled, so it contributes nothing and the declared backoff alone decides the wait.
pub(super) fn emitter_minimum_retry_delay(error: &Report<EmitterRuntimeError>) -> Duration {
    let Some(attachment) = error.downcast_ref::<EmitterRetryDelay>() else {
        return Duration::ZERO;
    };
    let delay = attachment.0;
    if PhysicalDeadlineCapability::operational()
        .after(delay)
        .is_err()
    {
        return Duration::ZERO;
    }
    delay
}

pub(super) fn emitter_retry_delay(
    backoff: &mut RuntimeReconnectBackoff,
    error: &Report<EmitterRuntimeError>,
) -> Duration {
    backoff
        .take_next_delay()
        .max(emitter_minimum_retry_delay(error))
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "emitter_retry_shuttle_tests.rs"]
mod shuttle_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_connector::{EmitterRecordId, EmitterWriteError, ParsedRetryPolicy, RecordEnvelope};

    use super::*;
    use crate::runtime::{
        emitter_batch_packing::BatchEnvelope,
        emitter_buffer::EmitterPublication,
        emitter_publishing::emitter_publish_failure,
        emitter_record_writes::{EncodedPayload, PreparedPayload},
        test_fixtures::{emitter_context, input_batch, input_batch_with},
    };

    /// A buffer holding `batches` one-row batches, as an emitter's buffer holds what it received.
    fn give_up_buffer(batches: usize) -> EmitterBatchBuffer {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Immediate {
            minimum: nervix_models::DataPlaneSettings::default().immediate_flush_minimum(),
        });
        for _ in 0..batches {
            buffer
                .retain_without_cadence(one_row_batch())
                .assured("the test buffer has a flush policy");
        }
        buffer
    }

    fn one_row_batch() -> EmitterPublishBatch {
        EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(0))
    }

    /// An attempt the destination failed, as a connector's failed publish reaches the emitter task.
    fn destination_failure() -> Report<EmitterRuntimeError> {
        Report::new(EmitterRuntimeError::PublishBatch)
            .attach_printable("HTTP request returned status 503 Service Unavailable")
    }

    /// A payload carrying the first row of each batch `batches` names.
    fn payload_over(batches: &[usize]) -> PreparedPayload<EncodedPayload> {
        let mut members = Vec::with_capacity(batches.len());
        for batch_index in batches {
            members.push(nervix_connector::EmitterRecordPosition {
                batch_index: *batch_index,
                row_index: 0,
            });
        }
        PreparedPayload {
            members,
            occurred_at: Timestamp::from_unix_nanos(0),
            content: EncodedPayload {
                envelope: BatchEnvelope {
                    destination: None,
                    headers: EmitterHeaders::Text(Vec::new()),
                    message_group: None,
                    clauses: Some(Arc::new(RecordEnvelope::default())),
                },
                payload: b"[1,1]".to_vec(),
            },
        }
    }

    #[nervix_primitives::test]
    async fn a_zero_limit_gives_up_every_held_batch_at_the_first_failed_attempt() {
        let mut buffer = give_up_buffer(2);
        let mut give_up = EmitterGiveUp::new(Some(Duration::ZERO));

        give_up.record_failure(&mut buffer, &destination_failure());
        give_up.collect(&mut buffer).await;

        let given_up = give_up
            .take_given_up()
            .assured("both batches were held when the attempt failed with no time to retry");
        assert_eq!(given_up.batches.len(), 2);
        assert!(given_up.staged.is_empty());
        assert!(buffer.is_empty(), "every given-up batch leaves the buffer");
        assert_eq!(
            given_up.message,
            "gave up after 0s of retries: HTTP request returned status 503 Service Unavailable"
        );
        assert!(
            give_up.take_given_up().is_none(),
            "given-up work is handed over once"
        );
    }

    #[nervix_primitives::test]
    async fn an_emitter_without_a_limit_never_gives_a_batch_up() {
        let mut buffer = give_up_buffer(1);
        let mut give_up = EmitterGiveUp::new(None);

        give_up.record_failure(&mut buffer, &destination_failure());
        give_up.collect(&mut buffer).await;

        assert!(give_up.take_given_up().is_none());
        assert_eq!(buffer.pending().len(), 1);
        assert!(give_up.next_deadline(&buffer).is_none());
    }

    #[nervix_primitives::test]
    async fn an_encoding_the_node_refused_starts_no_clock() {
        let mut buffer = give_up_buffer(1);
        let mut give_up = EmitterGiveUp::new(Some(Duration::ZERO));

        give_up.record_failure(
            &mut buffer,
            &Report::new(EmitterRuntimeError::EncodingRefused),
        );
        give_up.collect(&mut buffer).await;

        assert!(
            give_up.take_given_up().is_none(),
            "a refusal of the node's own bounded execution judged nothing about the destination"
        );
        assert_eq!(buffer.pending().len(), 1);
        assert!(buffer.next_give_up().is_none());
    }

    /// A batch received after an earlier attempt failed starts its clock at the first failed
    /// attempt that holds it, so each batch gives up its own limit after its own first failure.
    #[nervix_primitives::test(start_paused = true)]
    async fn each_held_batch_gives_up_its_limit_after_its_own_first_failed_attempt() {
        let limit = Duration::from_secs(10);
        let mut buffer = give_up_buffer(1);
        let mut give_up = EmitterGiveUp::new(Some(limit));
        give_up.record_failure(&mut buffer, &destination_failure());
        nervix_primitives::time::advance(Duration::from_secs(5)).await;
        buffer
            .retain_without_cadence(one_row_batch())
            .assured("the test buffer has a flush policy");
        give_up.record_failure(&mut buffer, &destination_failure());

        nervix_primitives::time::advance(Duration::from_secs(5)).await;
        give_up.collect(&mut buffer).await;
        let first = give_up
            .take_given_up()
            .assured("the first batch has been retried for its whole limit");
        assert_eq!(first.batches.len(), 1);
        assert_eq!(
            buffer.pending().len(),
            1,
            "the later batch keeps the clock its own first failed attempt started"
        );
        assert!(give_up.next_deadline(&buffer).is_some());

        nervix_primitives::time::advance(Duration::from_secs(5)).await;
        give_up.collect(&mut buffer).await;
        let second = give_up
            .take_given_up()
            .assured("the later batch has now been retried for its whole limit");
        assert_eq!(second.batches.len(), 1);
        assert!(buffer.is_empty());
    }

    /// A payload the emitter already wrote is retried whole, so a batch whose rows it carries
    /// leaves the buffer with the batch whose limit passed first, though its own clock still runs.
    #[nervix_primitives::test(start_paused = true)]
    async fn a_shared_retained_payload_gives_up_every_batch_it_carries_together() {
        let limit = Duration::from_secs(10);
        let mut buffer = give_up_buffer(1);
        let mut give_up = EmitterGiveUp::new(Some(limit));
        give_up.record_failure(&mut buffer, &destination_failure());
        nervix_primitives::time::advance(Duration::from_secs(5)).await;
        buffer
            .retain_without_cadence(one_row_batch())
            .assured("the test buffer has a flush policy");
        let EmitterPublication {
            batches, payloads, ..
        } = buffer.publication_mut();
        payloads
            .retain(payload_over(&[0, 1]), batches)
            .assured("both members are pending rows of the buffered batches");
        give_up.record_failure(&mut buffer, &destination_failure());

        nervix_primitives::time::advance(Duration::from_secs(5)).await;
        give_up.collect(&mut buffer).await;

        let given_up = give_up
            .take_given_up()
            .assured("the first batch has been retried for its whole limit");
        assert_eq!(
            given_up.batches.len(),
            2,
            "the batch sharing the retained payload leaves with it"
        );
        assert!(buffer.is_empty());
        assert!(
            buffer.publication_mut().payloads.is_empty(),
            "the payload the given-up batches carried is no longer retained"
        );
    }

    /// A destination's own delivery owner that gave a payload up after measuring the limit itself
    /// makes the emitter give up the payload's batches at once, whatever their clocks say.
    #[nervix_primitives::test]
    async fn a_batch_its_destination_gave_up_is_given_up_at_once() {
        let mut buffer = give_up_buffer(1);
        let mut give_up = EmitterGiveUp::new(Some(Duration::from_secs(3600)));
        buffer
            .publication_mut()
            .batches
            .first_mut()
            .assured("the test buffer holds one batch")
            .give_up_now();

        give_up.record_failure(&mut buffer, &destination_failure());
        give_up.collect(&mut buffer).await;

        let given_up = give_up
            .take_given_up()
            .assured("the destination already gave the batch up");
        assert_eq!(given_up.batches.len(), 1);
        assert_eq!(
            given_up.message,
            "gave up after 1h of retries: HTTP request returned status 503 Service Unavailable"
        );
    }

    /// A payload the destination's own delivery owner gave up marks every batch it carries, so the
    /// emitter gives them up at once however long their own clocks still run, and a record the
    /// write never held names nothing to give up.
    #[nervix_primitives::test]
    async fn a_written_payload_its_destination_gave_up_gives_up_every_batch_it_carries() {
        let mut buffer = give_up_buffer(2);
        let EmitterPublication {
            batches, payloads, ..
        } = buffer.publication_mut();
        payloads
            .retain(payload_over(&[0, 1]), batches)
            .assured("both members are pending rows of the buffered batches");
        let write = payloads.next_write();
        let unknown = payloads.give_up_written(batches, &write.payloads, EmitterRecordId::new(1));
        assert!(
            unknown.is_err(),
            "the write held one record, so a second one names nothing"
        );
        payloads
            .give_up_written(batches, &write.payloads, EmitterRecordId::new(0))
            .assured("the write's one record carries the retained payload");
        let mut give_up = EmitterGiveUp::new(Some(Duration::from_secs(3600)));

        give_up.record_failure(&mut buffer, &destination_failure());
        give_up.collect(&mut buffer).await;

        let given_up = give_up
            .take_given_up()
            .assured("the destination gave up the payload both batches share");
        assert_eq!(given_up.batches.len(), 2);
        assert!(buffer.is_empty());
    }

    #[nervix_primitives::test]
    async fn buffered_keepalive_preserves_its_beat_and_refreshes_only_surviving_obligations() {
        let mut keepalive = EmitterBufferedKeepalive::new(SharedRemoteAckTiming::default());
        keepalive.include_batch(&EmitterPublishBatch::from_batch(
            input_batch(),
            Timestamp::from_unix_nanos(0),
        ));
        assert!(keepalive.next.is_none());

        let (runtime, mut completion) = AckSet::root();
        let first = EmitterPublishBatch::from_batch(
            input_batch_with(1, 0, runtime.clone()),
            Timestamp::from_unix_nanos(0),
        );
        keepalive.include_batch(&first);
        let beat = keepalive.next;
        assert!(beat.is_some());
        keepalive.include_batch(&first);
        assert_eq!(keepalive.next, beat, "hot input cannot postpone a held ACK");

        let (staged, mut staged_completion) = AckSet::root();
        let held = EmitterAcknowledgements {
            runtime,
            connector: Some(EmitterRecordAcknowledgements::new(staged)),
        };
        keepalive.refresh(&held);
        assert_eq!(keepalive.next, beat);
        keepalive.next = Some(
            PhysicalDeadlineCapability::operational()
                .after(Duration::ZERO)
                .assured("an immediate fixture beat fits the physical clock"),
        );
        let ordinary = keepalive.wake(RuntimeWake::never(), false);
        assert!(
            ordinary
                .is_reached()
                .assured("a physical beat needs no domain clock")
        );
        assert!(
            !keepalive
                .wake(RuntimeWake::never(), true)
                .is_reached()
                .assured("a retry owns its own physical beat")
        );
        assert!(keepalive.take_due());
        held.keep_alive();
        assert_eq!(completion.wait_for_progress().await, AckProgress::Alive);
        assert_eq!(
            staged_completion.wait_for_progress().await,
            AckProgress::Alive
        );
        assert!(!keepalive.take_due());

        keepalive.refresh(&EmitterAcknowledgements::default());
        assert!(keepalive.next.is_none());
        assert!(!keepalive.take_due());
    }

    #[nervix_primitives::test]
    async fn retiring_the_last_branch_ack_keeps_the_physical_retry_deadline() {
        let (acks, completion) = AckSet::root();
        let mut retry = EmitterRetrySchedule::new(SharedRemoteAckTiming::default());
        retry
            .schedule(Duration::from_secs(60), acks.clone(), false)
            .assured("a one-minute fixture retry fits the physical clock");
        let deadline = retry.retry_at;
        assert!(retry.ack_alive_at.is_some());
        acks.no_ack("emitter source branch retired");
        retry.replace_acks(EmitterAcknowledgements::default());
        assert!(retry.is_active());
        assert_eq!(retry.retry_at, deadline);
        assert!(retry.ack_alive_at.is_none());
        assert!(retry.acks.is_empty());
        assert!(!retry.retry_is_due());
        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck("emitter source branch retired".to_string())
        );
    }

    #[nervix_primitives::test]
    async fn emitter_backoff_resets_to_the_declared_initial_delay() {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
        };
        let mut backoff = RuntimeReconnectBackoff::from_policy(
            policy,
            nervix_connector::RetryBackoffSettings::default(),
        );
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        assert_eq!(backoff.next_delay(), Duration::from_millis(1));
        assert!(backoff.wait(&mut shutdown_rx).await);
        assert_eq!(backoff.next_delay(), Duration::from_millis(2));
        assert!(backoff.wait(&mut shutdown_rx).await);
        assert_eq!(backoff.next_delay(), Duration::from_millis(4));
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(1));
    }

    #[test]
    fn emitter_retry_delay_honors_the_server_minimum() {
        let mut backoff = RuntimeReconnectBackoff::from_policy(
            ParsedRetryPolicy {
                backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(100),
            },
            nervix_connector::RetryBackoffSettings::default(),
        );
        let error = emitter_publish_failure(
            Report::new(EmitterWriteError::Publish { connector: "test" })
                .attach(EmitterRetryDelay(Duration::from_secs(2))),
        );

        assert_eq!(
            emitter_retry_delay(&mut backoff, &error),
            Duration::from_secs(2)
        );
        assert_eq!(backoff.next_delay(), Duration::from_millis(20));
    }

    #[test]
    fn a_server_delay_the_monotonic_clock_cannot_represent_leaves_the_declared_backoff() {
        let mut backoff = RuntimeReconnectBackoff::from_policy(
            ParsedRetryPolicy {
                backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(100),
            },
            nervix_connector::RetryBackoffSettings::default(),
        );
        let error = emitter_publish_failure(
            Report::new(EmitterWriteError::Publish { connector: "test" })
                .attach(EmitterRetryDelay(Duration::MAX)),
        );

        assert_eq!(
            emitter_retry_delay(&mut backoff, &error),
            Duration::from_millis(10)
        );
        assert_eq!(backoff.next_delay(), Duration::from_millis(20));
    }

    #[test]
    fn retry_schedule_preserves_its_deadline_until_a_retry_attempt() {
        let mut retry = EmitterRetrySchedule::new(SharedRemoteAckTiming::default());
        retry
            .schedule(Duration::from_secs(10), AckSet::empty(), false)
            .expect("the fixture backoff fits the monotonic clock range");
        let retry_at = retry.retry_at.expect("retry must have a deadline");

        assert!(!retry.retry_is_due());
        assert!(!retry.retry_is_due());
        assert_eq!(retry.retry_at, Some(retry_at));
    }

    #[test]
    fn an_active_retry_replaces_the_ordinary_cadence_wake() {
        let context = emitter_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("batch must buffer");
        let cadence = buffer
            .deadline()
            .expect("the push arms the logical cadence");
        let mut retry = EmitterRetrySchedule::new(SharedRemoteAckTiming::default());

        let idle = retry.wake(RuntimeWake::never().with_buffer(&context.clock, cadence.clone()));
        assert!(
            !idle
                .is_reached()
                .expect("the fixture clock stays installed"),
            "an idle retry leaves the ordinary cadence wake in place"
        );

        retry
            .schedule(Duration::ZERO, AckSet::empty(), false)
            .expect("the fixture backoff fits the monotonic clock range");
        let deferred = retry.wake(RuntimeWake::never().with_buffer(&context.clock, cadence));
        assert!(
            deferred
                .is_reached()
                .expect("a monotonic retry deadline needs no clock read"),
            "an active retry replaces the cadence wake with its own due deadline"
        );
        assert!(
            buffer.deadline().is_some(),
            "the cadence the retry replaced stays armed underneath it"
        );
    }

    #[test]
    fn retry_schedule_releases_a_stall_as_soon_as_the_fault_clears() {
        let mut retry = EmitterRetrySchedule::new(SharedRemoteAckTiming::default());
        retry
            .schedule(Duration::from_secs(30), AckSet::empty(), true)
            .expect("the fixture backoff fits the monotonic clock range");

        assert!(!retry.release_if_stall_cleared(true));
        assert!(retry.is_active());
        assert!(retry.release_if_stall_cleared(false));
        assert!(!retry.is_active());
    }

    #[nervix_primitives::test]
    async fn retry_schedule_heartbeats_acks_added_by_a_force_drain() {
        let (existing, mut existing_completion) = AckSet::root();
        let (force_drained, mut force_completion) = AckSet::root();
        let (emitter_retained, mut emitter_completion) = AckSet::root();
        let mut retry = EmitterRetrySchedule::new(SharedRemoteAckTiming::default());
        retry
            .schedule(Duration::from_secs(30), existing, false)
            .expect("the fixture backoff fits the monotonic clock range");
        retry.include_acks(EmitterAcknowledgements {
            runtime: force_drained,
            connector: Some(EmitterRecordAcknowledgements::new(emitter_retained)),
        });
        retry.ack_alive_at = Some(
            PhysicalDeadlineCapability::operational()
                .after(Duration::ZERO)
                .expect("an immediate keepalive fits the monotonic clock range"),
        );

        assert!(!retry.retry_is_due());

        assert_eq!(
            existing_completion.wait_for_progress().await,
            AckProgress::Alive
        );
        assert_eq!(
            force_completion.wait_for_progress().await,
            AckProgress::Alive
        );
        assert_eq!(
            emitter_completion.wait_for_progress().await,
            AckProgress::Alive
        );
    }
}
