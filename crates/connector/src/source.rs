//! Source operations and host services shared by connector families.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The typed source plan, broker and paced-polling connector traits, source messages,
//!   acknowledgement policies, lifecycle operations, typed source failures, and the opaque host
//!   handle through which a source delivers messages and reports lifecycle state.
//! - **Depends on.** Typed ingest metadata and timestamps, `error-stack`, and Tokio's monotonic
//!   clock.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, ACK-tree
//!   implementations, metrics implementations, or any connector driver.

use std::{
    fmt::Debug,
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use async_trait::async_trait;
use error_stack::Report;
use nervix_models::Timestamp;
use thiserror::Error;
use tokio::time::Instant;

use crate::{IngestMessageHeaders, IngestMetadataRow, ParsedRetryPolicy, RetainedIngestHeaders};

/// The metadata namespace a source message makes available to ingest expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMetadataScope {
    Headers,
    Kafka,
    Syslog,
}

/// The acknowledgement behavior a source transport supports for the selected plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceAcknowledgementSupport {
    None,
    Sequential,
    Parallel,
}

/// Capabilities resolved from the source vocabulary before a connector starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceCapabilities {
    reads_headers: bool,
    metadata_scope: SourceMetadataScope,
    supports_quiesce: bool,
    instances: NonZeroU64,
    acknowledgement: SourceAcknowledgementSupport,
}

impl SourceCapabilities {
    pub fn new(
        reads_headers: bool,
        metadata_scope: SourceMetadataScope,
        supports_quiesce: bool,
        instances: NonZeroU64,
        acknowledgement: SourceAcknowledgementSupport,
    ) -> Self {
        Self {
            reads_headers,
            metadata_scope,
            supports_quiesce,
            instances,
            acknowledgement,
        }
    }

    pub fn reads_headers(self) -> bool {
        self.reads_headers
    }

    pub fn metadata_scope(self) -> SourceMetadataScope {
        self.metadata_scope
    }

    pub fn supports_quiesce(self) -> bool {
        self.supports_quiesce
    }

    pub fn instances(self) -> NonZeroU64 {
        self.instances
    }

    pub fn acknowledgement(self) -> SourceAcknowledgementSupport {
        self.acknowledgement
    }
}

/// Host-owned policy for grouping messages and waiting on their acknowledgement trees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceAckPolicy {
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

impl SourceAckPolicy {
    pub fn support(self) -> SourceAcknowledgementSupport {
        match self {
            Self::None => SourceAcknowledgementSupport::None,
            Self::Sequential { .. } => SourceAcknowledgementSupport::Sequential,
            Self::Parallel { .. } => SourceAcknowledgementSupport::Parallel,
        }
    }

    pub fn retry(self) -> ParsedRetryPolicy {
        match self {
            Self::None => ParsedRetryPolicy {
                backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            },
            Self::Sequential { retry, .. } | Self::Parallel { retry, .. } => retry,
        }
    }
}

/// The complete host and connector plan for one source node.
pub struct SourcePlan<P> {
    pub connector: P,
    pub capabilities: SourceCapabilities,
    pub acknowledgement: SourceAckPolicy,
}

/// How many messages the host asks a connector to poll together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceBatchRequest {
    pub max_messages: NonZeroUsize,
    pub batch_timeout: Option<Duration>,
}

/// One source poll result.
pub enum SourceBatch<M> {
    Messages(Vec<M>),
    ResumeRequired,
    Closed,
}

/// A source message whose transport values remain owned by its connector.
pub trait SourceMessage: Send {
    type Position: Clone + Debug + Send + Sync + 'static;

    fn payload(&self) -> &[u8];
    fn position(&self) -> &Self::Position;
    fn headers(&self) -> &dyn IngestMessageHeaders;
    fn metadata(&self) -> IngestMetadataRow<'_>;
}

/// Whether a resumed source is ready to poll or is waiting for an external assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceResume {
    Ready,
    Waiting { retry_after: Duration },
}

/// Why a connector could not complete a source operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SourceError {
    #[error("failed to open {connector} source")]
    Open { connector: &'static str },
    #[error("failed to read from {connector} source")]
    Read { connector: &'static str },
    #[error("failed to acknowledge {connector} source positions")]
    Acknowledge { connector: &'static str },
    #[error("failed to reject {connector} source positions")]
    Reject { connector: &'static str },
    #[error("failed to suspend {connector} source")]
    Suspend { connector: &'static str },
    #[error("failed to resume {connector} source")]
    Resume { connector: &'static str },
    #[error("failed to close {connector} source")]
    Close { connector: &'static str },
}

pub type SourceResult<T> = Result<T, Report<SourceError>>;

/// Lifecycle operations common to every source family.
#[async_trait]
pub trait SourceConnector: Send + Sized + 'static {
    type Plan: Send + Sync;

    async fn open(plan: &Self::Plan, instance_index: u64) -> SourceResult<Self>;

    fn needs_resume(&mut self) -> bool {
        false
    }

    async fn suspend(&mut self) -> SourceResult<()> {
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        Ok(())
    }
}

/// Broker operations the host drives between source lifecycle transitions.
#[async_trait]
pub trait BrokerSourceConnector: SourceConnector {
    type Message: SourceMessage<Position = Self::Position>;
    type Position: Clone + Debug + Send + Sync + 'static;

    async fn next_batch(
        &mut self,
        request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>>;

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()>;

    async fn reject(&mut self, positions: &[Self::Position]) -> SourceResult<()>;
}

/// One owned message returned by a paced poll.
pub struct SourcePollMessage {
    pub payload: Vec<u8>,
    pub headers: RetainedIngestHeaders,
}

/// The result of one host-scheduled source poll.
///
/// `failures` contains individual records a connector could not materialize while allowing the
/// other messages from the same external response to continue through intake.
pub struct SourcePoll {
    pub messages: Vec<SourcePollMessage>,
    pub failures: Vec<Report<SourceError>>,
    pub observed_at: Timestamp,
}

/// A source whose transport operation is scheduled by a host-owned domain cadence.
#[async_trait]
pub trait PacedSourceConnector: SourceConnector {
    async fn poll(&mut self, scheduled_at: Timestamp) -> SourceResult<SourcePoll>;
}

/// Whether the host should attach one ACK root to every accepted source message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceIntakeMode {
    Unacknowledged,
    Acknowledged,
}

/// One borrowed source message crossing into host-owned decoding and dispatch.
pub struct SourceIntakeMessage<'a> {
    pub payload: &'a [u8],
    pub metadata: IngestMetadataRow<'a>,
}

/// One source batch crossing the connector boundary once.
pub struct SourceIntakeBatch<'a> {
    pub messages: Vec<SourceIntakeMessage<'a>>,
    pub mode: SourceIntakeMode,
}

/// Why host-owned source intake could not accept a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SourceIntakeError {
    #[error("failed to decode a source payload")]
    Decode,
    #[error("failed to dispatch a source batch")]
    Dispatch,
    #[error("failed to flush a source batch")]
    Flush,
}

pub type SourceIntakeResult<T> = Result<T, Report<SourceIntakeError>>;

/// The terminal result of one host-owned acknowledgement tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceAcknowledgementOutcome {
    Ack,
    NoAck(String),
    Shutdown,
}

/// The host implementation behind an opaque source acknowledgement.
#[async_trait]
pub trait SourceAcknowledgementServices: Send + 'static {
    async fn wait(self: Box<Self>, timeout: Duration) -> SourceAcknowledgementOutcome;
}

/// One acknowledgement the host returns for one accepted source message.
pub struct SourceAcknowledgement {
    services: Box<dyn SourceAcknowledgementServices>,
}

impl SourceAcknowledgement {
    pub fn new(services: impl SourceAcknowledgementServices) -> Self {
        Self {
            services: Box::new(services),
        }
    }

    pub async fn wait(self, timeout: Duration) -> SourceAcknowledgementOutcome {
        self.services.wait(timeout).await
    }
}

/// Acknowledgements returned in the same order as an acknowledged intake batch.
pub struct SourceIntakeOutcome {
    pub acknowledgements: Vec<SourceAcknowledgement>,
}

/// Every runtime service exposed through one source host handle.
#[async_trait]
pub trait SourceHostServices: Send + 'static {
    async fn intake(
        &mut self,
        batch: SourceIntakeBatch<'_>,
    ) -> SourceIntakeResult<SourceIntakeOutcome>;

    async fn flush(&mut self) -> SourceIntakeResult<()>;
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
}

/// Task-local host services a source loop drives without exposing a runtime type.
pub struct SourceHost {
    services: Box<dyn SourceHostServices>,
}

impl SourceHost {
    pub fn new(services: impl SourceHostServices) -> Self {
        Self {
            services: Box::new(services),
        }
    }

    pub async fn intake(
        &mut self,
        batch: SourceIntakeBatch<'_>,
    ) -> SourceIntakeResult<SourceIntakeOutcome> {
        self.services.intake(batch).await
    }

    pub async fn flush(&mut self) -> SourceIntakeResult<()> {
        self.services.flush().await
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
}
