//! Sink operations and host services shared by every connector that emits records.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The record and mapped-row sink traits, their lifecycle hooks, typed start and
//!   publish failures, per-record outcomes, and the opaque handles through which a sink reports to
//!   its host or keeps host-owned acknowledgements alive.
//! - **Depends on.** Arrow batches, vocabulary values, `error-stack`, Tokio's monotonic instant,
//!   and trait-object support.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, error-policy
//!   implementations, or any connector driver.

use std::{num::NonZeroUsize, ops::Range, path::PathBuf, time::Duration};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use error_stack::Report;
use nervix_models::{MessageErrorCode, MessageErrorOperation, StructuredMessageError, Timestamp};
use thiserror::Error;
use tokio::time::Instant;
use triomphe::Arc;

use crate::physical_time::PhysicalDeadline;

/// How many publishes may await confirmation at once, and how long each one may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckConfirmation {
    pub max_in_flight: NonZeroUsize,
    pub timeout: Duration,
}

/// How a broker sink learns that a record was accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerPublishingMode {
    NoAck,
    Ack(AckConfirmation),
}

/// Where one record sits in a sink write: its source batch and row within that batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkRecordPosition {
    pub batch_index: usize,
    pub row_index: usize,
}

/// One codec-encoded record ready for a connector to write.
///
/// A record carries no runtime acknowledgement. The host retains acknowledgements by position and
/// applies the returned outcome after the connector's one batch-level virtual call completes.
#[derive(Debug)]
pub struct SinkRecord {
    pub position: SinkRecordPosition,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub occurred_at: Timestamp,
}

impl SinkRecord {
    pub fn new(
        position: SinkRecordPosition,
        key: Option<String>,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
        occurred_at: Timestamp,
    ) -> Self {
        Self {
            position,
            key,
            payload,
            headers,
            occurred_at,
        }
    }

    pub fn rejected(&self, message: String) -> RejectedSinkRecord {
        RejectedSinkRecord::external(self.position, self.occurred_at, message)
    }
}

/// One record the connector definitively rejected, with the message error the host must deliver.
#[derive(Debug)]
pub struct RejectedSinkRecord {
    pub position: SinkRecordPosition,
    pub error: StructuredMessageError,
}

impl RejectedSinkRecord {
    pub fn external(position: SinkRecordPosition, occurred_at: Timestamp, message: String) -> Self {
        Self {
            position,
            error: StructuredMessageError {
                reference: uuid::Uuid::now_v7(),
                code: MessageErrorCode::External,
                message,
                operation: MessageErrorOperation::Publish,
                operation_index: None,
                fields: Default::default(),
                occurred_at,
            },
        }
    }
}

/// The result of one connector write, classified per record where a definitive outcome exists.
pub struct PerRecordOutcome {
    delivered: Vec<SinkRecordPosition>,
    rejected: Vec<RejectedSinkRecord>,
    infrastructure_error: Option<Report<SinkPublishError>>,
}

/// The named parts of a [`PerRecordOutcome`] consumed by the host.
pub struct PerRecordOutcomeParts {
    pub delivered: Vec<SinkRecordPosition>,
    pub rejected: Vec<RejectedSinkRecord>,
    pub infrastructure_error: Option<Report<SinkPublishError>>,
}

impl PerRecordOutcome {
    pub fn empty() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            delivered: Vec::with_capacity(capacity),
            rejected: Vec::new(),
            infrastructure_error: None,
        }
    }

    pub fn deliver(&mut self, position: SinkRecordPosition) {
        self.delivered.push(position);
    }

    pub fn reject(&mut self, rejected: RejectedSinkRecord) {
        self.rejected.push(rejected);
    }

    pub fn fail(&mut self, error: Report<SinkPublishError>) {
        self.infrastructure_error = Some(error);
    }

    pub fn into_parts(self) -> PerRecordOutcomeParts {
        PerRecordOutcomeParts {
            delivered: self.delivered,
            rejected: self.rejected,
            infrastructure_error: self.infrastructure_error,
        }
    }
}

/// A host-projected Arrow batch and the rows one row sink must write from it.
pub struct MappedSinkRows<'a> {
    pub batch_index: usize,
    pub batch: &'a RecordBatch,
    pub target_columns: &'a [String],
    pub selected_rows: &'a [usize],
    /// Ranges into `selected_rows` that respect the emitter's declared maximum batch size.
    pub selected_row_chunks: &'a [Range<usize>],
}

/// A sink-owned deadline the host includes in the task's next wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkDeadline {
    Domain(Timestamp),
    Physical(PhysicalDeadline),
}

/// Why a connector could not initialize its sink client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SinkStartError {
    #[error("invalid {sink} sink configuration")]
    InvalidConfiguration { sink: &'static str },
    #[error("failed to initialize {sink} sink")]
    Initialize { sink: &'static str },
}

pub type SinkStartResult<T> = Result<T, Report<SinkStartError>>;

/// Why a connector could not complete a sink operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SinkPublishError {
    #[error("{sink} sink is not initialized")]
    NotInitialized { sink: &'static str },
    #[error("failed to publish through {sink} sink")]
    Publish { sink: &'static str },
    #[error("failed to finish {sink} sink")]
    Finish { sink: &'static str },
}

pub type SinkPublishResult<T> = Result<T, Report<SinkPublishError>>;

/// Lifecycle policy shared by record and row sinks.
#[async_trait]
pub trait SinkLifecycle: Send {
    async fn finish(&mut self, _deadline: Instant) -> SinkPublishResult<()> {
        Ok(())
    }

    fn keeps_client_on_publish_failure(&self) -> bool {
        false
    }

    fn commit_deadline(&self) -> Option<SinkDeadline> {
        None
    }

    fn pending_acks(&self) -> Option<SinkAcknowledgements> {
        None
    }
}

/// A connector that writes codec-encoded records.
#[async_trait]
pub trait RecordSink: SinkLifecycle {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome;
}

/// A connector that encodes values directly from host-projected Arrow columns.
#[async_trait]
pub trait RowSink: SinkLifecycle {
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome;
}

/// Operations the host owns for acknowledgements retained by a sink.
pub trait SinkAcknowledgementServices: Send + Sync + 'static {
    fn acknowledge(&self);
    fn keep_alive(&self);
    fn reject(&self, reason: String);
    fn is_empty(&self) -> bool;
}

struct SinkAcknowledgementsInner {
    services: Box<dyn SinkAcknowledgementServices>,
}

/// An opaque handle to acknowledgements whose concrete representation remains in the host.
#[derive(Clone)]
pub struct SinkAcknowledgements {
    inner: Arc<SinkAcknowledgementsInner>,
}

impl SinkAcknowledgements {
    pub fn new(services: impl SinkAcknowledgementServices) -> Self {
        Self {
            inner: Arc::new(SinkAcknowledgementsInner {
                services: Box::new(services),
            }),
        }
    }

    pub fn acknowledge(&self) {
        self.inner.services.acknowledge();
    }

    pub fn keep_alive(&self) {
        self.inner.services.keep_alive();
    }

    pub fn reject(&self, reason: String) {
        self.inner.services.reject(reason);
    }

    pub fn is_empty(&self) -> bool {
        self.inner.services.is_empty()
    }
}

/// Transient-error status a connector may update while it retains a live client.
pub trait SinkTransientErrorStatus: Send + Sync + 'static {
    fn record_transient_error(&self, reason: String, retry_after: Duration);
    fn clear_transient_error(&self);
}

/// Runtime event reporting available to a connector background task.
pub trait SinkEventReporter: Send + Sync + 'static {
    fn report_error(&self, message: String);
}

/// The directory in which a connector may stage local files before external publication.
pub trait SinkStagingDirectory: Send + Sync + 'static {
    fn staging_directory(&self) -> PathBuf;
}

/// Node-wide general-error handling for acknowledgements a connector retained.
pub trait SinkGeneralErrorHandler: Send + Sync + 'static {
    fn handle_general_error(&self, acks: &SinkAcknowledgements, reason: String);
}

/// Every service exposed through one sink host handle.
pub trait SinkHostServices:
    SinkTransientErrorStatus
    + SinkEventReporter
    + SinkStagingDirectory
    + SinkGeneralErrorHandler
    + Send
    + Sync
    + 'static
{
}

impl<T> SinkHostServices for T where
    T: SinkTransientErrorStatus
        + SinkEventReporter
        + SinkStagingDirectory
        + SinkGeneralErrorHandler
        + Send
        + Sync
        + 'static
{
}

struct SinkHostInner {
    services: Box<dyn SinkHostServices>,
}

/// The task-scoped host services a connector may retain without learning a runtime type.
#[derive(Clone)]
pub struct SinkHost {
    inner: Arc<SinkHostInner>,
}

impl SinkHost {
    pub fn new(services: impl SinkHostServices) -> Self {
        Self {
            inner: Arc::new(SinkHostInner {
                services: Box::new(services),
            }),
        }
    }

    pub fn record_transient_error(&self, reason: String, retry_after: Duration) {
        self.inner
            .services
            .record_transient_error(reason, retry_after);
    }

    pub fn clear_transient_error(&self) {
        self.inner.services.clear_transient_error();
    }

    pub fn report_error(&self, message: String) {
        self.inner.services.report_error(message);
    }

    pub fn staging_directory(&self) -> PathBuf {
        self.inner.services.staging_directory()
    }

    pub fn handle_general_error(&self, acks: &SinkAcknowledgements, reason: String) {
        self.inner.services.handle_general_error(acks, reason);
    }
}
