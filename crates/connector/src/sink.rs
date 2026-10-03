//! Sink operations and host services shared by every connector that emits records.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The record, mapped-row, row request and HTTP request sink traits, their lifecycle
//!   hooks, typed start and publish failures, the carriers one mapped-row write covers, the
//!   identities a sink answers for — a record or request the host assigned, or a mapped row's source
//!   position — the requests a row request sink prepares from mapped rows, the outcome it answers
//!   with, and the opaque handles through which a sink reports to its host or keeps host-owned
//!   acknowledgements alive.
//! - **Depends on.** Arrow batches, vocabulary values, `error-stack`, Tokio's monotonic instant,
//!   and trait-object support.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, error-policy
//!   implementations, or any connector driver.

use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_execution::Executor;
use nervix_models::{
    FieldPath, HttpApplicationHeaders, HttpMethod, HttpTarget, MessageErrorCode,
    MessageErrorOperation, StructuredMessageError, Timestamp,
};
use nervix_primitives::{sync::Arc, time::Instant};
use thiserror::Error;

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

/// Where one source row sits in the host's buffered batches: its batch and its row within it.
///
/// A row sink answers for each mapped row under its position. Positions order by batch and then by
/// row, which is the order the host hands rows over in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SinkRecordPosition {
    pub batch_index: usize,
    pub row_index: usize,
}

/// The identity of one record or prepared request in a write, which the connector answers for.
///
/// One record is one external payload: one source record, or with the emitter's `BATCH` clause
/// every member of one batch. The host assigns the identity and keeps which source rows the record
/// carries, so a connector answers for the record and never learns its members or their
/// acknowledgements. A prepared HTTP or row request is handed over under an identity the same way.
/// Identities order the way the host hands records over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SinkRecordId(usize);

impl SinkRecordId {
    /// The identity of the record at `index` in the write the host hands over.
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    /// Where the record sits in the write it belongs to.
    pub const fn index(self) -> usize {
        self.0
    }
}

/// One codec-encoded record ready for a connector to write.
///
/// A record carries no runtime acknowledgement. The host retains the acknowledgements of the
/// source rows a record carries and applies the connector's answer for the record to each of them
/// after the connector's one batch-level virtual call completes.
#[derive(Debug)]
pub struct SinkRecord {
    pub id: SinkRecordId,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
    /// The group a service that delivers records in order per group writes this one under, as the
    /// host evaluated it for this record. Absent where the emitter declares no group.
    pub message_group: Option<String>,
    pub occurred_at: Timestamp,
}

impl SinkRecord {
    pub fn new(
        id: SinkRecordId,
        key: Option<String>,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
        occurred_at: Timestamp,
    ) -> Self {
        Self {
            id,
            key,
            payload,
            headers,
            message_group: None,
            occurred_at,
        }
    }

    /// This record written under the ordering group the host evaluated for it.
    pub fn with_message_group(mut self, message_group: String) -> Self {
        self.message_group = Some(message_group);
        self
    }

    pub fn rejected(&self, message: String) -> RejectedSinkRecord<SinkRecordId> {
        RejectedSinkRecord::external(self.id, self.occurred_at, message)
    }
}

/// One prepared HTTP request ready for a connector to send.
///
/// The host evaluated and validated every request field once, when it admitted the record, and
/// keeps them with the body until the connector answers for the request, so every attempt sends the
/// request the first attempt sent. A request carries no runtime acknowledgement: the host retains
/// the acknowledgement of the one source record the request carries.
#[derive(Debug)]
pub struct SinkHttpRequest {
    pub id: SinkRecordId,
    pub method: HttpMethod,
    pub target: HttpTarget,
    pub headers: HttpApplicationHeaders,
    /// Exactly the bytes the codec produced, or nothing for an emitter declared `WITHOUT BODY`.
    pub body: Option<Vec<u8>>,
    /// When the host admitted the record, which a rejection of the request is reported with.
    pub occurred_at: Timestamp,
}

impl SinkHttpRequest {
    pub fn rejected(&self, message: String) -> RejectedSinkRecord<SinkRecordId> {
        RejectedSinkRecord::external(self.id, self.occurred_at, message)
    }
}

/// One request a row request sink prepared from mapped rows, handed back to it on every attempt
/// until it answers for the request.
///
/// The host retains the bytes and the source rows the request carries from the moment the sink
/// prepared it, so every attempt, through whichever connector the emitter holds by then, sends the
/// bytes the first attempt sent. A request carries no runtime acknowledgement: the host retains the
/// acknowledgements of the source rows the request carries.
#[derive(Debug)]
pub struct SinkRowRequest {
    pub id: SinkRecordId,
    /// Exactly the bytes the sink prepared.
    pub body: Vec<u8>,
    /// When the host evaluated the mapping the request was prepared from, which a rejection of the
    /// request is reported with.
    pub occurred_at: Timestamp,
}

impl SinkRowRequest {
    pub fn rejected(&self, message: String) -> RejectedSinkRecord<SinkRecordId> {
        RejectedSinkRecord::external(self.id, self.occurred_at, message)
    }
}

/// One request a row request sink prepared, and the source rows it carries.
#[derive(Debug)]
pub struct PreparedRowRequest {
    /// The source rows the request carries, in the order it carries them.
    pub members: Vec<SinkRecordPosition>,
    /// Exactly the bytes every attempt sends.
    pub body: Vec<u8>,
}

/// What a row request sink prepared from one batch of mapped rows.
///
/// Every selected row is a member of exactly one request or refused, and every member of a request
/// follows the members of the requests before it in source order. The host checks both before it
/// retains a request.
#[derive(Debug, Default)]
pub struct RowRequestPreparation {
    /// The requests that carry the rows the sink accepted, in the order it sends them.
    pub requests: Vec<PreparedRowRequest>,
    /// The rows the sink refused, each with the message error the host delivers for it.
    pub rejected: Vec<RejectedSinkRecord<SinkRecordPosition>>,
}

/// One record the connector definitively rejected, with the message error the host must deliver.
///
/// `Id` is what the connector answers for: a [`SinkRecordId`] from a record sink, or a
/// [`SinkRecordPosition`] from a row sink.
#[derive(Debug)]
pub struct RejectedSinkRecord<Id> {
    pub id: Id,
    pub error: StructuredMessageError,
}

impl<Id> RejectedSinkRecord<Id> {
    /// A record the external system itself refused.
    pub fn external(id: Id, occurred_at: Timestamp, message: String) -> Self {
        Self {
            id,
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

    /// A record whose own request measures more than the emitter's `MAX SIZE`, which the connector
    /// found by measuring the record alone. It never reaches the destination.
    pub fn oversize(id: Id, occurred_at: Timestamp, message: String) -> Self {
        Self {
            id,
            error: StructuredMessageError {
                reference: uuid::Uuid::now_v7(),
                code: MessageErrorCode::Validation,
                message,
                operation: MessageErrorOperation::Encode,
                operation_index: None,
                fields: Default::default(),
                occurred_at,
            },
        }
    }

    /// A record whose mapped values the connector's own validation refused, naming the fields that
    /// carry them.
    pub fn invalid(
        id: Id,
        occurred_at: Timestamp,
        message: String,
        fields: impl IntoIterator<Item = FieldPath>,
    ) -> Self {
        Self {
            id,
            error: StructuredMessageError {
                reference: uuid::Uuid::now_v7(),
                code: MessageErrorCode::Validation,
                message,
                operation: MessageErrorOperation::Values,
                operation_index: None,
                fields: fields.into_iter().collect(),
                occurred_at,
            },
        }
    }
}

/// The result of one connector write, classified per record where a definitive outcome exists.
///
/// `Id` is what the connector answers for: a [`SinkRecordId`] from a record or request sink, whose
/// host applies the answer to every source row the record or request carries, or a
/// [`SinkRecordPosition`] from a row sink, which answers for each mapped row. A record the connector
/// neither delivered nor rejected stays unresolved, and only an infrastructure failure explains why
/// the write left it so.
pub struct PerRecordOutcome<Id> {
    delivered: Vec<Id>,
    rejected: Vec<RejectedSinkRecord<Id>>,
    infrastructure_error: Option<Report<SinkPublishError>>,
}

/// The named parts of a [`PerRecordOutcome`] consumed by the host.
pub struct PerRecordOutcomeParts<Id> {
    pub delivered: Vec<Id>,
    pub rejected: Vec<RejectedSinkRecord<Id>>,
    pub infrastructure_error: Option<Report<SinkPublishError>>,
}

impl<Id> PerRecordOutcome<Id> {
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

    pub fn deliver(&mut self, id: Id) {
        self.delivered.push(id);
    }

    pub fn reject(&mut self, rejected: RejectedSinkRecord<Id>) {
        self.rejected.push(rejected);
    }

    pub fn fail(&mut self, error: Report<SinkPublishError>) {
        self.infrastructure_error = Some(error);
    }

    /// Whether this write already failed for a reason the host must retry, which a sink writing
    /// several chunks reads before it starts the next one.
    pub fn has_infrastructure_error(&self) -> bool {
        self.infrastructure_error.is_some()
    }

    pub fn into_parts(self) -> PerRecordOutcomeParts<Id> {
        PerRecordOutcomeParts {
            delivered: self.delivered,
            rejected: self.rejected,
            infrastructure_error: self.infrastructure_error,
        }
    }
}

/// One host-projected Arrow carrier of a mapped-row write and the rows a row sink writes, or a row
/// request sink prepares requests, from it.
///
/// `batch.column(i)` holds the values mapped to the write's `target_columns[i]`, so a sink reads
/// its columns by position and never resolves a name a mapping may have used twice.
pub struct MappedSinkCarrier<'a> {
    pub batch_index: usize,
    pub batch: &'a RecordBatch,
    pub selected_rows: &'a [usize],
    /// When the host evaluated the mapping, which a rejected row is reported with and a sink whose
    /// commit cadence is a domain duration measures that cadence from.
    pub occurred_at: Timestamp,
    /// The acknowledgements of `selected_rows`, handed over to a sink that declares
    /// [`SinkLifecycle::retains_acknowledgements`]. The host builds them for no other sink, so a
    /// sink that acknowledges on the publish boundary receives none.
    pub acknowledgements: Option<SinkAcknowledgements>,
}

/// One mapped-row write: successive host-projected Arrow carriers of one source relay and concrete
/// branch, in the order the host released them.
///
/// A write never spans source relays or branches, so every row it carries may travel in one
/// request. Each carrier keeps its own mapped columns, execution time and acknowledgements. A row
/// sink is handed every run of carriers; a row request sink is handed one carrier per preparation,
/// because the host checks and retains what it prepared batch by batch.
pub struct MappedSinkRows<'a> {
    pub target_columns: &'a [String],
    pub carriers: Vec<MappedSinkCarrier<'a>>,
}

/// One row a write carries: the carrier that holds it and its row there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappedSinkMember {
    pub carrier: usize,
    pub row: usize,
}

/// The guarantee every lookup of a member's carrier relies on.
const MEMBER_CARRIER: &str = "a member names a carrier of the write whose members it came from";

impl MappedSinkRows<'_> {
    /// Every row this write carries, in the order the host packs them: carrier by carrier, and
    /// each carrier's selected rows in their order.
    pub fn members(&self) -> Vec<MappedSinkMember> {
        let mut members = Vec::with_capacity(self.member_count());
        for (carrier, mapped) in self.carriers.iter().enumerate() {
            for row in mapped.selected_rows {
                members.push(MappedSinkMember { carrier, row: *row });
            }
        }
        members
    }

    /// How many rows this write carries.
    pub fn member_count(&self) -> usize {
        let mut count = 0_usize;
        for carrier in &self.carriers {
            count = count
                .checked_add(carrier.selected_rows.len())
                .assured("the rows of one write are held in memory");
        }
        count
    }

    /// Where `member` sits in the host's buffered batches, which the sink answers for it under.
    pub fn position(&self, member: MappedSinkMember) -> SinkRecordPosition {
        let carrier = self.carriers.get(member.carrier).assured(MEMBER_CARRIER);
        SinkRecordPosition {
            batch_index: carrier.batch_index,
            row_index: member.row,
        }
    }

    /// When the host evaluated the mapping of the carrier that holds `member`.
    pub fn occurred_at(&self, member: MappedSinkMember) -> Timestamp {
        self.carriers
            .get(member.carrier)
            .assured(MEMBER_CARRIER)
            .occurred_at
    }
}

/// What one commit published, for the host's output metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkCommitReport {
    pub messages: u64,
    pub bytes: u64,
    /// The latest domain time among the rows this commit published.
    pub domain_timestamp: Timestamp,
}

/// A sink-owned deadline the host includes in the task's next wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkDeadline {
    Domain(Timestamp),
    Physical(PhysicalDeadline),
}

/// Why a connector could not initialize its sink client.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SinkStartError {
    #[error("invalid {sink} sink configuration")]
    InvalidConfiguration { sink: &'static str },
    #[error("failed to initialize {sink} sink")]
    Initialize { sink: &'static str },
    /// Nervix creates nothing in an external system, so an entity a sink writes to has to exist
    /// before it starts. A connector reports the entity it looked for rather than creating one.
    #[error("{kind} '{name}' does not exist")]
    MissingExternalEntity {
        sink: &'static str,
        kind: &'static str,
        name: String,
    },
}

pub type SinkStartResult<T> = Result<T, Report<SinkStartError>>;

/// Why a connector could not complete a sink operation.
///
/// Every variant but [`SinkPublishError::Misconfigured`] describes a condition the host retries on
/// its declared backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SinkPublishError {
    #[error("{sink} sink is not initialized")]
    NotInitialized { sink: &'static str },
    #[error("failed to publish through {sink} sink")]
    Publish { sink: &'static str },
    #[error("failed to finish {sink} sink")]
    Finish { sink: &'static str },
    #[error("failed to commit {sink} sink")]
    Commit { sink: &'static str },
    #[error("{sink} sink cannot accept this write as it is configured")]
    Misconfigured { sink: &'static str },
}

pub type SinkPublishResult<T> = Result<T, Report<SinkPublishError>>;

/// How long the external system asked the host to wait before its next attempt.
///
/// A connector attaches this to a publish failure when the receiver stated a delay of its own, such
/// as an OTLP `retry_info` or an HTTP `Retry-After`. The host waits at least this long, and never
/// less than its own backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkRetryDelay(pub Duration);

/// Lifecycle policy shared by record and row sinks.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait SinkLifecycle: Send {
    async fn finish(&mut self, _deadline: Instant) -> SinkPublishResult<()> {
        Ok(())
    }

    fn keeps_client_on_publish_failure(&self) -> bool {
        false
    }

    /// When what this sink staged has to be published.
    ///
    /// The host includes the deadline in the emitter task's next wake and asks for the commit once
    /// the deadline is reached. A sink that has nothing staged declares none.
    fn commit_deadline(&self) -> Option<SinkDeadline> {
        None
    }

    fn pending_acks(&self) -> Option<SinkAcknowledgements> {
        None
    }

    /// Whether this sink takes the acknowledgements of the rows it accepts and resolves them
    /// itself once its commit succeeds.
    ///
    /// The host merges a batch's acknowledgements only for a sink that answers `true`, so a sink
    /// that acknowledges on the publish boundary never pays for a handover it would not read.
    fn retains_acknowledgements(&self) -> bool {
        false
    }

    /// How many messages this sink holds that the host's own buffer no longer counts.
    ///
    /// A drain reads this total together with the emitter buffer, so staged work keeps the node
    /// busy until its commit publishes it.
    fn staged_messages(&self) -> u64 {
        0
    }

    /// Publishes everything this sink staged and resolves the acknowledgements it retained.
    ///
    /// The host calls this when [`SinkLifecycle::commit_deadline`] is reached and once more for a
    /// drain, which commits whatever is staged without waiting for that deadline. A sink that
    /// writes each batch as it arrives stages nothing and has nothing to commit.
    async fn commit(&mut self) -> SinkPublishResult<Option<SinkCommitReport>> {
        Ok(None)
    }
}

/// A connector that writes codec-encoded records.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait RecordSink: SinkLifecycle {
    /// Writes `records` and answers for each of them by its identity.
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId>;
}

/// A connector that sends one prepared HTTP request for each record.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait HttpRequestSink: SinkLifecycle {
    /// Sends `requests` in the order they are handed over and answers for each of them by its
    /// identity. A request the connector leaves unanswered stays with the host, which sends it
    /// again unchanged.
    async fn publish(&mut self, requests: Vec<SinkHttpRequest>) -> PerRecordOutcome<SinkRecordId>;
}

/// A connector that encodes values directly from host-projected Arrow columns.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait RowSink: SinkLifecycle {
    /// Writes the selected rows and answers for each of them by its source position.
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome<SinkRecordPosition>;
}

/// A connector that prepares requests from host-projected Arrow columns once, and sends each of them
/// unchanged until it answers for it.
///
/// The host retains every prepared request with the source rows it carries, beside the batches they
/// came from, so an attempt that follows an unknown outcome, such as a lost response or a timeout,
/// sends exactly what the first attempt sent, even through a connector the host reopened in between.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait RowRequestSink: SinkLifecycle {
    /// Prepares the requests that carry the selected rows, and refuses the rows it cannot carry. A
    /// failure prepares nothing, and the host prepares the same rows again on its next attempt.
    async fn prepare(
        &mut self,
        rows: MappedSinkRows<'_>,
    ) -> SinkPublishResult<RowRequestPreparation>;

    /// Sends `requests` in the order they are handed over and answers for each of them by its
    /// identity. A request the connector leaves unanswered stays with the host, which hands it over
    /// again unchanged.
    async fn publish(&mut self, requests: Vec<SinkRowRequest>) -> PerRecordOutcome<SinkRecordId>;
}

/// Operations the host owns for acknowledgements retained by a sink.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
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
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait SinkTransientErrorStatus: Send + Sync + 'static {
    fn record_transient_error(&self, reason: String, retry_after: Duration);
    fn clear_transient_error(&self);
}

/// Runtime event reporting available to a connector background task.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait SinkEventReporter: Send + Sync + 'static {
    fn report_error(&self, message: String);
}

/// The directory in which a connector may stage local files before external publication.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait SinkStagingDirectory: Send + Sync + 'static {
    fn staging_directory(&self) -> PathBuf;
}

/// Node-wide general-error handling for acknowledgements a connector retained.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait SinkGeneralErrorHandler: Send + Sync + 'static {
    fn handle_general_error(&self, acks: &SinkAcknowledgements, reason: String);
}

/// The node's bounded executor, through which a connector admits the synchronous filesystem work
/// it does itself, such as writing and reading the files it stages.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait SinkBoundedExecution: Send + Sync + 'static {
    fn executor(&self) -> Executor;
}

/// Every service exposed through one sink host handle.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait SinkHostServices:
    SinkTransientErrorStatus
    + SinkEventReporter
    + SinkStagingDirectory
    + SinkGeneralErrorHandler
    + SinkBoundedExecution
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
        + SinkBoundedExecution
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

    /// The node's bounded executor. A connector keeps the handle it is given for as long as it
    /// stages work through it.
    pub fn executor(&self) -> Executor {
        self.inner.services.executor()
    }

    pub fn handle_general_error(&self, acks: &SinkAcknowledgements, reason: String) {
        self.inner.services.handle_general_error(acks, reason);
    }
}
