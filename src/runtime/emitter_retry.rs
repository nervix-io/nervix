//! When an emitter tries again after a failed publish or an unavailable sink.
//!
//! Layer: data plane.
//! - **Owns.** The monotonic deadline of an emitter's next attempt, the keepalive of the
//!   acknowledgements it holds until then, and how long each retry waits.
//! - **Depends on.** The emitter's declared backoff, the physical-time capability, the
//!   acknowledgements of its buffered and staged work, and its transient-error status.
//! - **Must not know.** What the emitter publishes, which connector it publishes through, or the
//!   domain clock its flush cadence reads.

use error_stack::ResultExt as _;
use nervix_connector::{
    SinkAcknowledgements, SinkRetryDelay,
    physical_time::{PhysicalDeadline, PhysicalDeadlineCapability},
};

use super::*;

pub(super) const RETRY_ACK_ALIVE_EACH: Duration = Duration::from_millis(100);

/// One deferral of an emitter's publish attempt: how long the emitter waits, which
/// acknowledgements it keeps alive meanwhile, and what the operator is told about the wait.
pub(super) struct EmitterRetryDeferral<'a> {
    pub(super) wait: Duration,
    pub(super) acks: EmitterAcknowledgements,
    pub(super) waiting_for_stall_clear: bool,
    pub(super) reason: Option<&'a str>,
}

#[derive(Default)]
pub(super) struct EmitterAcknowledgements {
    pub(super) runtime: AckSet,
    pub(super) sink: Option<SinkAcknowledgements>,
}

impl EmitterAcknowledgements {
    fn is_empty(&self) -> bool {
        self.runtime.is_empty()
            && self
                .sink
                .as_ref()
                .is_none_or(SinkAcknowledgements::is_empty)
    }

    fn extend(&mut self, further: Self) {
        self.runtime = AckSet::merged([std::mem::take(&mut self.runtime), further.runtime]);
        if further.sink.is_some() {
            self.sink = further.sink;
        }
    }
}

impl From<AckSet> for EmitterAcknowledgements {
    fn from(runtime: AckSet) -> Self {
        Self {
            runtime,
            sink: None,
        }
    }
}

impl AcknowledgementKeepalive for EmitterAcknowledgements {
    fn keep_alive(&self) {
        self.runtime.ack_alive();
        if let Some(acks) = &self.sink {
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
/// which stays armed and unchanged underneath it.
#[derive(Default)]
pub(super) struct EmitterRetrySchedule {
    retry_at: Option<PhysicalDeadline>,
    ack_alive_at: Option<PhysicalDeadline>,
    acks: EmitterAcknowledgements,
    waiting_for_stall_clear: bool,
}

impl EmitterRetrySchedule {
    pub(super) fn is_active(&self) -> bool {
        self.retry_at.is_some()
    }

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
        self.ack_alive_at = (!self.acks.is_empty()).then(|| Self::next_ack_alive_at(retry_at));
        self.waiting_for_stall_clear = waiting_for_stall_clear;
        Ok(())
    }

    pub(super) fn include_acks(&mut self, acks: impl Into<EmitterAcknowledgements>) {
        let acks = acks.into();
        if acks.is_empty() {
            return;
        }
        self.acks.extend(acks);
        if let Some(retry_at) = self.retry_at
            && self.ack_alive_at.is_none()
        {
            self.ack_alive_at = Some(Self::next_ack_alive_at(retry_at));
        }
    }

    fn next_ack_alive_at(retry_at: PhysicalDeadline) -> PhysicalDeadline {
        let keepalive = PhysicalDeadlineCapability::operational()
            .after(RETRY_ACK_ALIVE_EACH)
            .assured("the acknowledgement keepalive interval is a fixed hundred milliseconds");
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
            self.ack_alive_at = Some(Self::next_ack_alive_at(retry_at));
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
    /// Constructing the monotonic deadline is the only fallible part. A configured or
    /// server-supplied wait the monotonic clock cannot represent leaves no retry scheduled, so the
    /// emitter falls back to its unchanged flush cadence and the failure is recorded as the
    /// emitter's transient error instead of disappearing.
    pub(super) fn defer(
        &mut self,
        context: &EmitterSinkContext,
        deferral: EmitterRetryDeferral<'_>,
    ) {
        let EmitterRetryDeferral {
            wait,
            acks,
            waiting_for_stall_clear,
            reason,
        } = deferral;
        match self.schedule(wait, acks, waiting_for_stall_clear) {
            Ok(()) => {
                if let Some(reason) = reason {
                    context.runtime.record_emitter_transient_error_with_backoff(
                        &context.domain,
                        &context.emitter,
                        reason,
                        wait,
                    );
                }
            }
            Err(error) => {
                context.runtime.record_emitter_transient_error(
                    &context.domain,
                    &context.emitter,
                    emitter_error_message(&error),
                );
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

/// How long the sink asked this emitter to wait, which bounds its next attempt from below.
fn emitter_minimum_retry_delay(error: &Report<EmitterRuntimeError>) -> Duration {
    match error.downcast_ref::<SinkRetryDelay>() {
        Some(attachment) => attachment.0,
        None => Duration::ZERO,
    }
}

pub(super) fn emitter_retry_delay(
    backoff: &mut RuntimeReconnectBackoff,
    error: &Report<EmitterRuntimeError>,
) -> Duration {
    backoff
        .take_next_delay()
        .max(emitter_minimum_retry_delay(error))
}

#[cfg(test)]
mod tests {
    use nervix_connector::{ParsedRetryPolicy, SinkPublishError};

    use super::*;
    use crate::runtime::{
        emitter_publishing::sink_publish_failure,
        test_fixtures::{input_batch, sink_context},
    };

    #[tokio::test]
    async fn emitter_backoff_resets_to_the_declared_initial_delay() {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
        };
        let mut backoff = RuntimeReconnectBackoff::from_policy(policy);
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
        let mut backoff = RuntimeReconnectBackoff::from_policy(ParsedRetryPolicy {
            backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
        });
        let error = sink_publish_failure(
            Report::new(SinkPublishError::Publish { sink: "test" })
                .attach(SinkRetryDelay(Duration::from_secs(2))),
        );

        assert_eq!(
            emitter_retry_delay(&mut backoff, &error),
            Duration::from_secs(2)
        );
        assert_eq!(backoff.next_delay(), Duration::from_millis(20));
    }

    #[test]
    fn retry_schedule_preserves_its_deadline_until_a_retry_attempt() {
        let mut retry = EmitterRetrySchedule::default();
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
        let context = sink_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
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
        let mut retry = EmitterRetrySchedule::default();

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
        let mut retry = EmitterRetrySchedule::default();
        retry
            .schedule(Duration::from_secs(30), AckSet::empty(), true)
            .expect("the fixture backoff fits the monotonic clock range");

        assert!(!retry.release_if_stall_cleared(true));
        assert!(retry.is_active());
        assert!(retry.release_if_stall_cleared(false));
        assert!(!retry.is_active());
    }

    #[tokio::test]
    async fn retry_schedule_heartbeats_acks_added_by_a_force_drain() {
        let (existing, mut existing_completion) = AckSet::root();
        let (force_drained, mut force_completion) = AckSet::root();
        let (sink_retained, mut sink_completion) = AckSet::root();
        let mut retry = EmitterRetrySchedule::default();
        retry
            .schedule(Duration::from_secs(30), existing, false)
            .expect("the fixture backoff fits the monotonic clock range");
        retry.include_acks(EmitterAcknowledgements {
            runtime: force_drained,
            sink: Some(SinkAcknowledgements::new(sink_retained)),
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
            sink_completion.wait_for_progress().await,
            AckProgress::Alive
        );
    }
}
