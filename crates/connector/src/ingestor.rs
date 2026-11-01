//! Ingestor operations and host services shared by connector families.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The typed ingestor plan, broker and paced-polling connector traits, ingestor messages,
//!   acknowledgement policies, lifecycle operations, typed ingestor failures, the cancellation
//!   through which the host ends a broker read in flight, and the opaque host handle through which
//!   an ingestor delivers messages and reports lifecycle state.
//! - **Depends on.** Typed ingest metadata, the vocabulary's positions and timestamps,
//!   `error-stack`, and the primitive boundary's monotonic clock and cancellation token.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, ACK-tree
//!   implementations, metrics implementations, or any connector driver.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{
    fmt::Debug,
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use async_trait::async_trait;
use error_stack::Report;
use nervix_models::{PositionClass, Timestamp};
use nervix_primitives::{sync::CancellationToken, time::Instant};
use thiserror::Error;

use crate::{
    IngestMessageHeaders, IngestMetadataRow, ParsedRetryPolicy, RetainedIngestHeaders,
    RetainedIngestMetadata,
};

/// The acknowledgement behavior an ingestor transport supports for the selected plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestorAcknowledgementSupport {
    None,
    Sequential,
    Parallel,
}

/// Capabilities resolved from the ingestor vocabulary before a connector starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestorCapabilities {
    reads_headers: bool,
    supports_quiesce: bool,
    instances: NonZeroU64,
    acknowledgement: IngestorAcknowledgementSupport,
    position_class: PositionClass,
}

impl IngestorCapabilities {
    pub fn new(
        reads_headers: bool,
        supports_quiesce: bool,
        instances: NonZeroU64,
        acknowledgement: IngestorAcknowledgementSupport,
        position_class: PositionClass,
    ) -> Self {
        Self {
            reads_headers,
            supports_quiesce,
            instances,
            acknowledgement,
            position_class,
        }
    }

    pub fn reads_headers(self) -> bool {
        self.reads_headers
    }

    pub fn supports_quiesce(self) -> bool {
        self.supports_quiesce
    }

    pub fn instances(self) -> NonZeroU64 {
        self.instances
    }

    pub fn acknowledgement(self) -> IngestorAcknowledgementSupport {
        self.acknowledgement
    }

    /// What the ingestor's positions mean once the connection that delivered them is gone, as its
    /// entity declares.
    ///
    /// The host reads it to decide, once, what to do with an acknowledgement or a rejection a
    /// broker ingestor could not complete:
    ///
    /// - [`PositionClass::Replayable`]: the host suspends and resumes the ingestor, then retries the
    ///   same rejection before it reads another batch. The retried rejection never depends on a
    ///   poll, because the ingestor can seek to its positions on any connection.
    /// - [`PositionClass::BrokerRedelivered`]: the host suspends and resumes the ingestor, then
    ///   discards the pending positions and counts them as transport redeliveries. The broker
    ///   redelivers every delivery the lost connection did not acknowledge, so nothing is retried
    ///   against a connection that no longer exists. An acknowledgement that was in flight when
    ///   the connection died is treated the same way, so delivery is at least once.
    /// - [`PositionClass::None`]: nothing is pending, so the host keeps no position and reads
    ///   again after its retry delay.
    pub fn position_class(self) -> PositionClass {
        self.position_class
    }
}

/// Host-owned policy for grouping messages and waiting on their acknowledgement trees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestorAckPolicy {
    None,
    Sequential {
        timeout: Duration,
        retry: ParsedRetryPolicy,
    },
    Parallel {
        max_in_flight: NonZeroUsize,
        batch_timeout: Duration,
        timeout: Duration,
        retry: ParsedRetryPolicy,
    },
}

impl IngestorAckPolicy {
    pub fn support(self) -> IngestorAcknowledgementSupport {
        match self {
            Self::None => IngestorAcknowledgementSupport::None,
            Self::Sequential { .. } => IngestorAcknowledgementSupport::Sequential,
            Self::Parallel { .. } => IngestorAcknowledgementSupport::Parallel,
        }
    }
}

/// The complete host and connector plan for one ingestor node.
pub struct IngestorPlan<P> {
    pub connector: P,
    pub capabilities: IngestorCapabilities,
    pub acknowledgement: IngestorAckPolicy,
}

/// How many messages the host asks a connector to poll together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestorBatchRequest {
    pub max_messages: NonZeroUsize,
    pub batch_timeout: Option<Duration>,
}

/// One ingestor poll result.
pub enum IngestorBatch<M> {
    Messages(Vec<M>),
    /// The read ended early because the host cancelled it. `gathered` holds every message the
    /// ingestor had already taken from its transport for this batch, possibly none, and the host
    /// takes them in before it acts on what made it cancel.
    Interrupted {
        gathered: Vec<M>,
    },
    /// The read took `payloads` payloads from its transport that exceed the size bound of the
    /// ingestor's client, and dropped each of them alone, leaving the transport's next payload
    /// intact. The host counts them and reads again at once.
    Oversized {
        payloads: NonZeroUsize,
    },
    ResumeRequired,
    Closed,
}

/// The host's request that a read in flight end and return what it already took.
///
/// The host hands one to each [`BrokerIngestorConnector::next_batch`] and cancels it when a quiesce
/// change or a stop needs its loop back while the read waits. It never drops a read in flight:
/// after cancelling it keeps awaiting the read, takes in what the read returns, and only then acts
/// on the change or the stop.
#[derive(Debug, Default)]
pub struct IngestorReadCancellation {
    token: CancellationToken,
}

impl IngestorReadCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    /// Asks the read to end. Only the host that started the read cancels it.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Resolves once the host has cancelled the read.
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    /// Waits for `read` unless the host cancels first, which returns `None`.
    ///
    /// A read that is ready completes even after the host cancelled, so a message its transport
    /// already delivered is taken rather than left behind. A read still waiting when the host
    /// cancels is dropped, and anything it holds is dropped with it, so an ingestor passes only a read
    /// that holds nothing it took from its transport while it waits.
    pub async fn unless_cancelled<F>(&self, read: F) -> Option<F::Output>
    where
        F: Future,
    {
        nervix_primitives::select! {
            biased;
            output = read => Some(output),
            () = self.token.cancelled() => None,
        }
    }
}

/// A ingestor message whose transport values remain owned by its connector.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait IngestorMessage: Send {
    type Position: Clone + Debug + Send + Sync + 'static;

    fn payload(&self) -> &[u8];
    fn position(&self) -> &Self::Position;
    fn headers(&self) -> &dyn IngestMessageHeaders;
    fn metadata(&self) -> IngestMetadataRow<'_>;
}

/// Whether a resumed ingestor is ready to poll or is waiting for an external assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestorResume {
    Ready,
    Waiting { retry_after: Duration },
}

/// Why a connector could not complete an ingestor operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum IngestorError {
    #[error("failed to open {connector} ingestor")]
    Open { connector: &'static str },
    #[error("failed to read from {connector} ingestor")]
    Read { connector: &'static str },
    #[error("failed to acknowledge {connector} ingestor positions")]
    Acknowledge { connector: &'static str },
    #[error("failed to reject {connector} ingestor positions")]
    Reject { connector: &'static str },
    #[error("failed to suspend {connector} ingestor")]
    Suspend { connector: &'static str },
    #[error("failed to resume {connector} ingestor")]
    Resume { connector: &'static str },
    #[error("failed to close {connector} ingestor")]
    Close { connector: &'static str },
}

pub type IngestorResult<T> = Result<T, Report<IngestorError>>;

/// Lifecycle operations common to every ingestor family.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait IngestorConnector: Send + Sized + 'static {
    /// The vocabulary entity implemented by this concrete connector.
    const KIND: nervix_models::IngestorEntityKind;

    type Plan: Send + Sync;

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "the host installs one driver before admitting ingestor work"
        )
    )]
    async fn open(plan: &Self::Plan, instance_index: u64) -> IngestorResult<Self>;

    fn needs_resume(&mut self) -> bool {
        false
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        Ok(())
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        Ok(IngestorResume::Ready)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "the host ends the exact ingestor lifetime after intake has stopped"
        )
    )]
    async fn close(&mut self) -> IngestorResult<()> {
        Ok(())
    }
}

/// The transport boundary at which an ingestor can settle graph outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestorSettlement {
    /// Positions share a commit or acknowledgement boundary, so the host settles their batch.
    Batch,
    /// Every position has its own acknowledgement and rejection, independent of its siblings.
    Individual,
}

/// Broker operations the host drives between ingestor lifecycle transitions.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait BrokerIngestorConnector: IngestorConnector {
    type Message: IngestorMessage<Position = Self::Position>;
    type Position: Clone + Debug + Send + Sync + 'static;

    /// Whether one graph outcome settles a batch or only its own position.
    const SETTLEMENT: IngestorSettlement = IngestorSettlement::Batch;

    /// Reads the next batch, up to `request.max_messages`.
    ///
    /// The host awaits every read to completion, so a read never loses what it took from its
    /// transport to a dropped future. Once the host cancels `cancellation`, the read ends as soon
    /// as it can and returns [`IngestorBatch::Interrupted`] with every message it had already taken.
    /// A ingestor that gathers several messages observes the cancellation between its transport
    /// reads, and a read it cannot interrupt without losing what that read returns completes
    /// within its own bounded wait first.
    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>>;

    /// Settles `positions` as processed on the transport.
    ///
    /// Returns once the transport has confirmed them as far as the ingestor's entity declares in its
    /// [`AcknowledgementConfirmation`](nervix_models::AcknowledgementConfirmation): the broker
    /// answered the acknowledgement or the commit, or, where the protocol has no answer, the
    /// acknowledgement was written to the connection. A ingestor that returns before that declares
    /// so rather than reporting a confirmation it did not get. A confirmation that fails returns
    /// [`IngestorError::Acknowledge`]: the host shows it as the ingestor's transient status and
    /// rejects the positions, which the ingestor's [`PositionClass`] then handles.
    ///
    /// Until this returns the host reports no progress for the positions: drains and ownership
    /// handoffs keep waiting for them, and the ingestor's transient status stays as it was.
    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()>;

    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()>;
}

/// One owned message returned by a paced poll: its payload, its transport headers and the values
/// of the metadata fields its entity declares, in declared order.
pub struct IngestorPollMessage {
    pub payload: Vec<u8>,
    pub headers: RetainedIngestHeaders,
    pub metadata: RetainedIngestMetadata,
}

/// The result of one host-scheduled ingestor poll.
///
/// `failures` contains individual records a connector could not materialize while allowing the
/// other messages from the same external response to continue through intake.
/// `dropped_header_values` counts header values the connector skipped because they were not
/// valid UTF-8, without discarding their message or treating the header as a record failure.
pub struct IngestorPoll {
    pub messages: Vec<IngestorPollMessage>,
    pub failures: Vec<Report<IngestorError>>,
    pub dropped_header_values: usize,
    pub observed_at: Timestamp,
}

/// A ingestor whose transport operation is scheduled by a host-owned domain cadence.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait PacedIngestorConnector: IngestorConnector {
    async fn poll(&mut self, scheduled_at: Timestamp) -> IngestorResult<IngestorPoll>;
}

/// One borrowed ingestor message crossing into host-owned decoding and dispatch.
pub struct IngestorIntakeMessage<'a> {
    pub payload: &'a [u8],
    pub metadata: IngestMetadataRow<'a>,
}

/// Why host-owned ingestor intake could not accept a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum IngestorIntakeError {
    #[error("failed to retain an ingestor batch during quiesce")]
    Retain,
    #[error("failed to dispatch an ingestor batch")]
    Dispatch,
    #[error("failed to flush an ingestor batch")]
    Flush,
}

pub type IngestorIntakeResult<T> = Result<T, Report<IngestorIntakeError>>;

/// The terminal result of one host-owned acknowledgement tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestorAcknowledgementOutcome {
    Ack,
    NoAck(String),
    Shutdown,
}

/// The host implementation behind an opaque batch acknowledgement.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait IngestorBatchAcknowledgementServices: Send + 'static {
    /// Waits until every message of the batch has been acknowledged through the graph, until one
    /// of them failed, or until the host stops. `timeout` bounds how long the acknowledgement of
    /// one message may stay silent.
    async fn wait(&mut self, timeout: Duration) -> IngestorAcknowledgementOutcome;
}

/// The acknowledgement the host returns for one acknowledged intake batch.
///
/// The host counts the batch's work as outstanding, which drains and ownership handoffs wait for,
/// until this handle drops, also once its wait has returned. The loop therefore keeps it until the
/// ingestor has confirmed or rejected the batch's positions on its transport.
pub struct IngestorBatchAcknowledgement {
    services: Box<dyn IngestorBatchAcknowledgementServices>,
}

impl IngestorBatchAcknowledgement {
    pub fn new(services: impl IngestorBatchAcknowledgementServices) -> Self {
        Self {
            services: Box::new(services),
        }
    }

    pub async fn wait(&mut self, timeout: Duration) -> IngestorAcknowledgementOutcome {
        self.services.wait(timeout).await
    }
}

/// Every runtime service exposed through one ingestor host handle.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait IngestorHostServices: Send + 'static {
    /// Takes in a batch whose positions nothing downstream acknowledges.
    async fn intake_unacknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()>;

    /// Takes in a batch whose positions the ingestor settles once every message has been
    /// acknowledged through the graph, and returns the acknowledgement the loop waits on.
    async fn intake_acknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<IngestorBatchAcknowledgement>;

    async fn flush(&mut self) -> IngestorIntakeResult<()>;
    async fn replay_buffered(&mut self) -> IngestorIntakeResult<bool>;
    fn next_flush(&self) -> Option<Instant>;
    fn should_suspend_intake(&self) -> bool;
    async fn wait_for_quiesce_change(&mut self);
    async fn wait_until_not_suspended(&mut self);
    async fn wait_until_active(&mut self) -> bool;
    fn mark_ready(&self);
    fn mark_unready(&self);
    fn record_transient_error(&self, reason: String, retry_after: Duration);
    fn clear_transient_error(&self);
    fn report_error(&self, message: String);
    fn handle_ack_failure(&self, reason: String);
    /// Counts positions the host discarded after a reconnect because the broker redelivers them.
    fn record_transport_redeliveries(&self, positions: usize);
    /// Counts payloads the ingestor dropped because they exceed the size bound of its client.
    fn record_oversized_payloads(&self, payloads: NonZeroUsize);
    /// Reports that the ingestor stopped before its transport confirmed `positions` it had read,
    /// which the transport presents again by its own redelivery contract.
    fn record_unconfirmed_stop(&self, positions: usize);
}

/// Task-local host services an ingestor loop drives without exposing a runtime type.
pub struct IngestorHost {
    services: Box<dyn IngestorHostServices>,
}

impl IngestorHost {
    pub fn new(services: impl IngestorHostServices) -> Self {
        Self {
            services: Box::new(services),
        }
    }

    pub async fn intake_unacknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()> {
        self.services.intake_unacknowledged(messages).await
    }

    pub async fn intake_acknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<IngestorBatchAcknowledgement> {
        self.services.intake_acknowledged(messages).await
    }

    pub async fn flush(&mut self) -> IngestorIntakeResult<()> {
        self.services.flush().await
    }

    pub async fn replay_buffered(&mut self) -> IngestorIntakeResult<bool> {
        self.services.replay_buffered().await
    }

    pub fn next_flush(&self) -> Option<Instant> {
        self.services.next_flush()
    }

    pub fn should_suspend_intake(&self) -> bool {
        self.services.should_suspend_intake()
    }

    pub async fn wait_for_quiesce_change(&mut self) {
        self.services.wait_for_quiesce_change().await;
    }

    pub async fn wait_until_not_suspended(&mut self) {
        self.services.wait_until_not_suspended().await;
    }

    pub async fn wait_until_active(&mut self) -> bool {
        self.services.wait_until_active().await
    }

    pub fn mark_ready(&self) {
        self.services.mark_ready();
    }

    pub fn mark_unready(&self) {
        self.services.mark_unready();
    }

    pub fn record_transient_error(&self, reason: String, retry_after: Duration) {
        self.services.record_transient_error(reason, retry_after);
    }

    pub fn clear_transient_error(&self) {
        self.services.clear_transient_error();
    }

    pub fn report_error(&self, message: String) {
        self.services.report_error(message);
    }

    pub fn handle_ack_failure(&self, reason: String) {
        self.services.handle_ack_failure(reason);
    }

    pub fn record_transport_redeliveries(&self, positions: usize) {
        self.services.record_transport_redeliveries(positions);
    }

    pub fn record_oversized_payloads(&self, payloads: NonZeroUsize) {
        self.services.record_oversized_payloads(payloads);
    }

    pub fn record_unconfirmed_stop(&self, positions: usize) {
        self.services.record_unconfirmed_stop(positions);
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{
        future::{self, poll_fn},
        pin::pin,
        task::Poll,
    };

    use super::*;

    #[nervix_primitives::test]
    async fn a_read_that_is_ready_completes_after_the_host_cancelled() {
        let cancellation = IngestorReadCancellation::new();
        cancellation.cancel();

        let output = cancellation.unless_cancelled(future::ready(7)).await;

        assert_eq!(output, Some(7));
        assert!(cancellation.is_cancelled());
    }

    #[nervix_primitives::test]
    async fn a_read_still_waiting_when_the_host_cancels_ends_without_output() {
        let cancellation = IngestorReadCancellation::new();
        let mut waiting = pin!(cancellation.unless_cancelled(future::pending::<u8>()));

        let first = poll_fn(|context| Poll::Ready(waiting.as_mut().poll(context))).await;
        assert!(
            first.is_pending(),
            "a read nobody cancelled keeps waiting for its transport"
        );
        assert!(!cancellation.is_cancelled());
        cancellation.cancel();

        assert_eq!(waiting.await, None);
    }
}
