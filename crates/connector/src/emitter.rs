//! Emitter operations and host services shared by every connector that emits records.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The record, mapped-row, row request and HTTP request emitter traits, their lifecycle
//!   hooks, typed start and publish failures, the carriers one mapped-row write covers, the
//!   identities an emitter answers for — a record or request the host assigned, or a mapped row's ingestor
//!   position — the requests a row request emitter prepares from mapped rows, the outcome it answers
//!   with, and the opaque handles through which an emitter reports to its host or keeps host-owned
//!   acknowledgements alive.
//! - **Depends on.** Arrow batches, vocabulary values, `error-stack`, Tokio's monotonic instant,
//!   and trait-object support.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, error-policy
//!   implementations, or any connector driver.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use arch_into::ArchInto as _;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_execution::Executor;
use nervix_models::{
    FieldPath, HttpApplicationHeaders, HttpMethod, HttpTarget, MessageErrorCode,
    MessageErrorOperation, PropertyType, StructuredMessageError, Timestamp,
};
use nervix_primitives::{sync::Arc, time::Instant};
use thiserror::Error;

/// How many publishes may await confirmation at once, and how long each one may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckConfirmation {
    pub max_in_flight: NonZeroUsize,
    pub timeout: Duration,
}

/// How a broker emitter learns that a record was accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerPublishingMode {
    NoAck,
    Ack(AckConfirmation),
}

/// Where one source row sits in the host's buffered batches: its batch and its row within it.
///
/// A row emitter answers for each mapped row under its position. Positions order by batch and then by
/// row, which is the order the host hands rows over in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EmitterRecordPosition {
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
pub struct EmitterRecordId(usize);

impl EmitterRecordId {
    /// The identity of the record at `index` in the write the host hands over.
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    /// Where the record sits in the write it belongs to.
    pub const fn index(self) -> usize {
        self.0
    }
}

/// What one record carries beside its payload, as its emitter's envelope clauses and properties
/// set it.
///
/// A flag clause the statement writes is set for every record, and a value clause holds what the
/// host evaluated for this record when it admitted it. The host retains the envelope with the
/// payload, so every attempt to publish the record carries the envelope the first attempt carried.
/// The registry refuses a clause or property the emitter's entity does not declare, so a connector
/// receives only what its entity declares, and everything else is unset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordEnvelope {
    /// `PERSISTENT`.
    pub persistent: bool,
    /// `MANDATORY`.
    pub mandatory: bool,
    /// `RETAIN`.
    pub retain: bool,
    /// `TIMESTAMP <expression>`.
    pub timestamp: Option<Timestamp>,
    /// `DELIVER AFTER <duration>`.
    pub deliver_after: Option<Duration>,
    /// `DELAY <duration>`.
    pub delay: Option<Duration>,
    /// `REPLY TO <expression>`.
    pub reply_to: Option<String>,
    /// `MESSAGE ID <expression>`.
    pub message_id: Option<String>,
    /// `DEDUPLICATION ID <expression>`.
    pub deduplication_id: Option<String>,
    /// The properties the statement sets, in the order it writes them.
    pub properties: Vec<RecordProperty>,
}

impl RecordEnvelope {
    /// The bytes the envelope's values hold beyond its fixed fields, which a host counts with the
    /// record that carries them.
    pub fn held_bytes(&self) -> u64 {
        let mut bytes = 0_u64;
        let texts = [&self.reply_to, &self.message_id, &self.deduplication_id];
        for text in texts.into_iter().flatten() {
            let length: u64 = text.len().arch_into();
            bytes = bytes.checked_add(length).assured(HELD_IN_MEMORY);
        }
        for property in &self.properties {
            bytes = bytes
                .checked_add(property.value.held_bytes())
                .assured(HELD_IN_MEMORY);
        }
        bytes
    }
}

/// The bound every byte count of an envelope relies on: each term counts bytes of a value this
/// node already holds in memory.
const HELD_IN_MEMORY: &str = "every term counts bytes of a value this node already holds in memory";

/// One property a record carries, under the name its entity declares for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordProperty {
    pub name: &'static str,
    pub value: PropertyValue,
}

/// The value of one property, or of one typed header, of exactly the type its entity declares for
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyValue {
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    Bool(bool),
    String(String),
    Datetime(Timestamp),
    Bytes(Vec<u8>),
}

impl PropertyValue {
    /// The exact type of the value.
    pub fn ty(&self) -> PropertyType {
        match self {
            Self::U8(_) => PropertyType::U8,
            Self::I8(_) => PropertyType::I8,
            Self::U16(_) => PropertyType::U16,
            Self::I16(_) => PropertyType::I16,
            Self::U32(_) => PropertyType::U32,
            Self::I32(_) => PropertyType::I32,
            Self::U64(_) => PropertyType::U64,
            Self::I64(_) => PropertyType::I64,
            Self::Bool(_) => PropertyType::Bool,
            Self::String(_) => PropertyType::String,
            Self::Datetime(_) => PropertyType::Datetime,
            Self::Bytes(_) => PropertyType::Bytes,
        }
    }

    /// The bytes the value holds beyond its fixed size.
    pub fn held_bytes(&self) -> u64 {
        match self {
            Self::String(text) => text.len().arch_into(),
            Self::Bytes(bytes) => bytes.len().arch_into(),
            Self::U8(_)
            | Self::I8(_)
            | Self::U16(_)
            | Self::I16(_)
            | Self::U32(_)
            | Self::I32(_)
            | Self::U64(_)
            | Self::I64(_)
            | Self::Bool(_)
            | Self::Datetime(_) => 0,
        }
    }
}

/// One `write_header` write of a record whose entity declares typed header values: the name, and
/// a value of exactly one of the types the entity declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedRecordHeader {
    pub name: String,
    pub value: PropertyValue,
}

impl TypedRecordHeader {
    /// The bytes the header holds beyond its fixed size, which a host counts with the record that
    /// carries it.
    pub fn held_bytes(&self) -> u64 {
        let name: u64 = self.name.len().arch_into();
        name.checked_add(self.value.held_bytes())
            .assured(HELD_IN_MEMORY)
    }
}

/// One codec-encoded record ready for a connector to write.
///
/// A record carries no runtime acknowledgement. The host retains the acknowledgements of the
/// source rows a record carries and applies the connector's answer for the record to each of them
/// after the connector's one batch-level virtual call completes.
///
/// A record's `write_header` writes arrive in the one form its entity's header model declares:
/// `headers` holds them where the entity declares text values, and `typed_headers` where it
/// declares typed ones, so a connector reads the field its declaration names and finds the other
/// empty.
#[derive(Debug)]
pub struct EmitterRecord {
    pub id: EmitterRecordId,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    /// The writes in written order, each value a `STRING`, where the entity declares text values.
    pub headers: Vec<(String, String)>,
    /// The writes in written order, each value of exactly one of the types the entity declares,
    /// where the entity declares typed values.
    pub typed_headers: Vec<TypedRecordHeader>,
    /// The group a service that delivers records in order per group writes this one under, as the
    /// host evaluated it for this record. Absent where the emitter declares no group.
    pub message_group: Option<String>,
    /// What the record carries beside its payload, as the emitter's envelope clauses and
    /// properties set it. Empty where the emitter writes none.
    pub envelope: RecordEnvelope,
    pub occurred_at: Timestamp,
}

impl EmitterRecord {
    pub fn new(
        id: EmitterRecordId,
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
            typed_headers: Vec::new(),
            message_group: None,
            envelope: RecordEnvelope::default(),
            occurred_at,
        }
    }

    /// This record carrying the typed header writes the host evaluated for it, in place of text
    /// ones.
    pub fn with_typed_headers(mut self, typed_headers: Vec<TypedRecordHeader>) -> Self {
        self.typed_headers = typed_headers;
        self
    }

    /// This record written under the ordering group the host evaluated for it.
    pub fn with_message_group(mut self, message_group: String) -> Self {
        self.message_group = Some(message_group);
        self
    }

    /// This record carrying the envelope the host evaluated for it.
    pub fn with_envelope(mut self, envelope: RecordEnvelope) -> Self {
        self.envelope = envelope;
        self
    }

    pub fn rejected(&self, message: String) -> RejectedEmitterMember<EmitterRecordId> {
        RejectedEmitterMember::external(self.id, self.occurred_at, message)
    }
}

/// One prepared HTTP request ready for a connector to send.
///
/// The host evaluated and validated every request field once, when it admitted the record, and
/// keeps them with the body until the connector answers for the request, so every attempt sends the
/// request the first attempt sent. A request carries no runtime acknowledgement: the host retains
/// the acknowledgement of the one source record the request carries.
#[derive(Debug)]
pub struct EmitterHttpRequest {
    pub id: EmitterRecordId,
    pub method: HttpMethod,
    pub target: HttpTarget,
    pub headers: HttpApplicationHeaders,
    /// Exactly the bytes the codec produced, or nothing for an emitter declared `WITHOUT BODY`.
    pub body: Option<Vec<u8>>,
    /// When the host admitted the record, which a rejection of the request is reported with.
    pub occurred_at: Timestamp,
}

impl EmitterHttpRequest {
    pub fn rejected(&self, message: String) -> RejectedEmitterMember<EmitterRecordId> {
        RejectedEmitterMember::external(self.id, self.occurred_at, message)
    }
}

/// One request a row request emitter prepared from mapped rows, handed back to it on every attempt
/// until it answers for the request.
///
/// The host retains the bytes and the source rows the request carries from the moment the emitter
/// prepared it, so every attempt, through whichever connector the emitter holds by then, sends the
/// bytes the first attempt sent. A request carries no runtime acknowledgement: the host retains the
/// acknowledgements of the source rows the request carries.
#[derive(Debug)]
pub struct EmitterRowRequest {
    pub id: EmitterRecordId,
    /// Exactly the bytes the emitter prepared.
    pub body: Vec<u8>,
    /// When the host evaluated the mapping the request was prepared from, which a rejection of the
    /// request is reported with.
    pub occurred_at: Timestamp,
}

impl EmitterRowRequest {
    pub fn rejected(&self, message: String) -> RejectedEmitterMember<EmitterRecordId> {
        RejectedEmitterMember::external(self.id, self.occurred_at, message)
    }
}

/// One request a row request emitter prepared, and the source rows it carries.
#[derive(Debug)]
pub struct PreparedRowRequest {
    /// The source rows the request carries, in the order it carries them.
    pub members: Vec<EmitterRecordPosition>,
    /// Exactly the bytes every attempt sends.
    pub body: Vec<u8>,
}

/// What a row request emitter prepared from one batch of mapped rows.
///
/// Every selected row is a member of exactly one request or refused, and every member of a request
/// follows the members of the requests before it in source order. The host checks both before it
/// retains a request.
#[derive(Debug, Default)]
pub struct RowRequestPreparation {
    /// The requests that carry the rows the emitter accepted, in the order it sends them.
    pub requests: Vec<PreparedRowRequest>,
    /// The rows the emitter refused, each with the message error the host delivers for it.
    pub rejected: Vec<RejectedEmitterMember<EmitterRecordPosition>>,
}

/// One record the connector definitively rejected, with the message error the host must deliver.
///
/// `Id` is what the connector answers for: a [`EmitterRecordId`] from a record emitter, or a
/// [`EmitterRecordPosition`] from a row emitter.
#[derive(Debug)]
pub struct RejectedEmitterMember<Id> {
    pub id: Id,
    pub error: StructuredMessageError,
}

impl<Id> RejectedEmitterMember<Id> {
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
/// `Id` is what the connector answers for: a [`EmitterRecordId`] from a record or request emitter, whose
/// host applies the answer to every source row the record or request carries, or a
/// [`EmitterRecordPosition`] from a row emitter, which answers for each mapped row. A record the connector
/// neither delivered nor rejected stays unresolved, and only an infrastructure failure explains why
/// the write left it so.
pub struct PerRecordOutcome<Id> {
    delivered: Vec<Id>,
    rejected: Vec<RejectedEmitterMember<Id>>,
    infrastructure_error: Option<Report<EmitterWriteError>>,
}

/// The named parts of a [`PerRecordOutcome`] consumed by the host.
pub struct PerRecordOutcomeParts<Id> {
    pub delivered: Vec<Id>,
    pub rejected: Vec<RejectedEmitterMember<Id>>,
    pub infrastructure_error: Option<Report<EmitterWriteError>>,
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

    pub fn reject(&mut self, rejected: RejectedEmitterMember<Id>) {
        self.rejected.push(rejected);
    }

    pub fn fail(&mut self, error: Report<EmitterWriteError>) {
        self.infrastructure_error = Some(error);
    }

    /// Whether this write already failed for a reason the host must retry, which an emitter writing
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

/// One host-projected Arrow carrier of a mapped-row write and the rows a row emitter writes, or a row
/// request emitter prepares requests, from it.
///
/// `batch.column(i)` holds the values mapped to the write's `target_columns[i]`, so an emitter reads
/// its columns by position and never resolves a name a mapping may have used twice.
pub struct MappedEmitterCarrier<'a> {
    pub batch_index: usize,
    pub batch: &'a RecordBatch,
    pub selected_rows: &'a [usize],
    /// When the host evaluated the mapping, which a rejected row is reported with and a commit
    /// report names as the domain time of the rows it published.
    pub occurred_at: Timestamp,
    /// The acknowledgements of `selected_rows`, handed over to an emitter that declares
    /// [`EmitterLifecycle::retains_acknowledgements`]. The host builds them for no other emitter, so a
    /// emitter that acknowledges on the publish boundary receives none.
    pub acknowledgements: Option<EmitterRecordAcknowledgements>,
}

/// One mapped-row write: successive host-projected Arrow carriers of one source relay and concrete
/// branch, in the order the host released them.
///
/// A write never spans source relays or branches, so every row it carries may travel in one
/// request. Each carrier keeps its own mapped columns, execution time and acknowledgements. A row
/// emitter is handed every run of carriers; a row request emitter is handed one carrier per preparation,
/// because the host checks and retains what it prepared batch by batch.
pub struct MappedEmitterRows<'a> {
    pub target_columns: &'a [String],
    pub carriers: Vec<MappedEmitterCarrier<'a>>,
}

/// One row a write carries: the carrier that holds it and its row there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappedEmitterMember {
    pub carrier: usize,
    pub row: usize,
}

/// The guarantee every lookup of a member's carrier relies on.
const MEMBER_CARRIER: &str = "a member names a carrier of the write whose members it came from";

impl MappedEmitterRows<'_> {
    /// Every row this write carries, in the order the host packs them: carrier by carrier, and
    /// each carrier's selected rows in their order.
    pub fn members(&self) -> Vec<MappedEmitterMember> {
        let mut members = Vec::with_capacity(self.member_count());
        for (carrier, mapped) in self.carriers.iter().enumerate() {
            for row in mapped.selected_rows {
                members.push(MappedEmitterMember { carrier, row: *row });
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

    /// Where `member` sits in the host's buffered batches, which the emitter answers for it under.
    pub fn position(&self, member: MappedEmitterMember) -> EmitterRecordPosition {
        let carrier = self.carriers.get(member.carrier).assured(MEMBER_CARRIER);
        EmitterRecordPosition {
            batch_index: carrier.batch_index,
            row_index: member.row,
        }
    }

    /// When the host evaluated the mapping of the carrier that holds `member`.
    pub fn occurred_at(&self, member: MappedEmitterMember) -> Timestamp {
        self.carriers
            .get(member.carrier)
            .assured(MEMBER_CARRIER)
            .occurred_at
    }
}

/// What one commit published, for the host's output metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitterCommitReport {
    pub messages: u64,
    pub bytes: u64,
    /// The latest domain time among the rows this commit published.
    pub domain_timestamp: Timestamp,
}

/// What an emitter that stages its writes holds for its next commit.
///
/// The emitter reports what it staged and the host owns the cadence: it measures
/// [`EmitterStagedCommit::Cadence`] on the emitter's domain clock from the first cadence check that
/// finds the rows staged, which follows the write that staged them. Rows the emitter's buffer held
/// before releasing them therefore wait the whole cadence after they are staged, not after the
/// domain time their batch was accepted at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterStagedCommit {
    /// Staged rows whose commit is due once this domain duration has passed since they were staged.
    Cadence(Duration),
    /// Staged rows that reached the emitter's maximum commit size, so their commit is due now.
    SizeReached,
}

/// Why a connector could not initialize its emitter client.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EmitterOpenError {
    #[error("invalid {connector} emitter configuration")]
    InvalidConfiguration { connector: &'static str },
    #[error("failed to initialize {connector} emitter")]
    Initialize { connector: &'static str },
    /// Nervix creates nothing in an external system, so an entity an emitter writes to has to exist
    /// before it starts. A connector reports the entity it looked for rather than creating one.
    #[error("{kind} '{name}' does not exist")]
    MissingExternalEntity {
        connector: &'static str,
        kind: &'static str,
        name: String,
    },
}

pub type EmitterStartResult<T> = Result<T, Report<EmitterOpenError>>;

/// Why a connector could not complete an emitter operation.
///
/// Every variant but [`EmitterWriteError::Misconfigured`] describes a condition the host retries on
/// its declared backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum EmitterWriteError {
    #[error("{connector} emitter is not initialized")]
    NotInitialized { connector: &'static str },
    #[error("failed to publish through {connector} emitter")]
    Publish { connector: &'static str },
    #[error("failed to commit {connector} emitter")]
    Commit { connector: &'static str },
    #[error("{connector} emitter cannot accept this write as it is configured")]
    Misconfigured { connector: &'static str },
}

pub type EmitterWriteResult<T> = Result<T, Report<EmitterWriteError>>;

/// What finishing an emitter did with the records its transport client still held.
///
/// A publish reports a record delivered at the completion point its mode declares. Where that point
/// is the client taking the record, the client can still hold delivered records when the emitter
/// stops, so the host finishes every emitter once the emitter will publish nothing more through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterFinish {
    /// The client holds no record once a publish returns, so there was nothing to send.
    NothingHeld,
    /// The client held records and sent every one of them before the deadline.
    Flushed,
}

/// Why finishing an emitter did not send every record its transport client still held.
///
/// The host retries neither: a stop that asked for the finish fails, and terminal teardown records
/// what the client still held as lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum EmitterFinishError {
    /// The deadline passed while the client still held `records` records it had not sent.
    #[error(
        "{connector} emitter's transport client still held {records} records when its finish \
         deadline passed"
    )]
    Unsent {
        connector: &'static str,
        records: u64,
    },
    /// The client failed while it sent the records it held.
    #[error("{connector} emitter's transport client failed while it sent the records it held")]
    Flush { connector: &'static str },
}

pub type EmitterFinishResult = Result<EmitterFinish, Report<EmitterFinishError>>;

/// How long the external system asked the host to wait before its next attempt.
///
/// A connector attaches this to a publish failure when the receiver stated a delay of its own, such
/// as an OTLP `retry_info` or an HTTP `Retry-After`. The host waits at least this long, and never
/// less than its own backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitterRetryDelay(pub Duration);

/// Lifecycle policy shared by record and row emitters.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait EmitterLifecycle: Send {
    /// The vocabulary entity implemented by this concrete connector.
    fn kind(&self) -> nervix_models::EmitterEntityKind;

    /// Sends what the transport client still holds of the records a publish reported delivered, by
    /// `deadline`, and answers with what that did.
    ///
    /// The host calls this once the emitter will publish nothing more through this emitter: after a
    /// stop drained the emitter's buffer, and after terminal teardown ended the emitter task,
    /// possibly in the middle of a publish. Every emitter answers for what its own client holds, so
    /// this hook has no default.
    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult;

    fn keeps_client_on_publish_failure(&self) -> bool {
        false
    }

    /// What this emitter holds for its next commit, absent while it has nothing staged.
    ///
    /// The host reads it after every write its cadence releases. It arms the cadence this names at
    /// the first such check that finds rows staged, includes that deadline in the emitter task's
    /// next wake, and asks for the commit once the deadline is reached or the emitter reports its size
    /// boundary. A emitter that writes each batch as it arrives stages nothing.
    fn staged_commit(&self) -> Option<EmitterStagedCommit> {
        None
    }

    fn pending_acks(&self) -> Option<EmitterRecordAcknowledgements> {
        None
    }

    /// Whether this emitter takes the acknowledgements of the rows it accepts and resolves them
    /// itself once its commit succeeds.
    ///
    /// The host merges a batch's acknowledgements only for an emitter that answers `true`, so an emitter
    /// that acknowledges on the publish boundary never pays for a handover it would not read.
    fn retains_acknowledgements(&self) -> bool {
        false
    }

    /// How many messages this emitter holds that the host's own buffer no longer counts.
    ///
    /// A drain reads this total together with the emitter buffer, so staged work keeps the node
    /// busy until its commit publishes it.
    fn staged_messages(&self) -> u64 {
        0
    }

    /// Publishes everything this emitter staged and resolves the acknowledgements it retained.
    ///
    /// The host calls this when the commit [`EmitterLifecycle::staged_commit`] describes is due, and
    /// for a retry or a drain, which commit whatever is staged without waiting for the cadence or
    /// reading the domain clock. A emitter that writes each batch as it arrives stages nothing and has
    /// nothing to commit.
    async fn commit(&mut self) -> EmitterWriteResult<Option<EmitterCommitReport>> {
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
pub trait RecordEmitter: EmitterLifecycle {
    /// Prepares the external bytes of one newly encoded payload. The host retains the result
    /// until delivery or rejection, and never prepares a retained payload again on a retry or
    /// through a reopened connector. Most connectors send the codec's bytes unchanged.
    ///
    /// A refusal is the payload's definitive message error, shared by all its source members.
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "a connector's definitive payload refusal is a fixed public message outcome"
        )
    )]
    fn prepare_payload(
        &mut self,
        payload: Vec<u8>,
        _occurred_at: Timestamp,
    ) -> Result<Vec<u8>, StructuredMessageError> {
        Ok(payload)
    }

    /// Writes `records` and answers for each of them by its identity.
    /// Payloads are the retained bytes returned by `prepare_payload`.
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId>;
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
pub trait HttpRequestEmitter: EmitterLifecycle {
    /// Sends `requests` in the order they are handed over and answers for each of them by its
    /// identity. A request the connector leaves unanswered stays with the host, which sends it
    /// again unchanged.
    async fn publish(
        &mut self,
        requests: Vec<EmitterHttpRequest>,
    ) -> PerRecordOutcome<EmitterRecordId>;
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
pub trait RowEmitter: EmitterLifecycle {
    /// Writes the selected rows and answers for each of them by its ingestor position.
    async fn publish(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> PerRecordOutcome<EmitterRecordPosition>;
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
pub trait RowRequestEmitter: EmitterLifecycle {
    /// Prepares the requests that carry the selected rows, and refuses the rows it cannot carry. A
    /// failure prepares nothing, and the host prepares the same rows again on its next attempt.
    async fn prepare(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> EmitterWriteResult<RowRequestPreparation>;

    /// Sends `requests` in the order they are handed over and answers for each of them by its
    /// identity. A request the connector leaves unanswered stays with the host, which hands it over
    /// again unchanged.
    async fn publish(
        &mut self,
        requests: Vec<EmitterRowRequest>,
    ) -> PerRecordOutcome<EmitterRecordId>;
}

/// Operations the host owns for acknowledgements retained by an emitter.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait EmitterRecordAcknowledgementServices: Send + Sync + 'static {
    fn acknowledge(&self);
    fn keep_alive(&self);
    fn reject(&self, reason: String);
    fn is_empty(&self) -> bool;
}

struct EmitterRecordAcknowledgementsInner {
    services: Box<dyn EmitterRecordAcknowledgementServices>,
}

/// An opaque handle to acknowledgements whose concrete representation remains in the host.
#[derive(Clone)]
pub struct EmitterRecordAcknowledgements {
    inner: Arc<EmitterRecordAcknowledgementsInner>,
}

impl EmitterRecordAcknowledgements {
    pub fn new(services: impl EmitterRecordAcknowledgementServices) -> Self {
        Self {
            inner: Arc::new(EmitterRecordAcknowledgementsInner {
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
pub trait EmitterTransientErrorStatus: Send + Sync + 'static {
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
pub trait EmitterEventReporter: Send + Sync + 'static {
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
pub trait EmitterStagingDirectory: Send + Sync + 'static {
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
pub trait EmitterGeneralErrorHandler: Send + Sync + 'static {
    fn handle_general_error(&self, acks: &EmitterRecordAcknowledgements, reason: String);
}

/// The node's bounded executor, through which a connector admits the synchronous work it does
/// itself, such as writing and reading the files it stages or destroying a native client whose
/// destruction waits.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait EmitterBoundedExecution: Send + Sync + 'static {
    fn executor(&self) -> Executor;
}

/// Every service exposed through one emitter host handle.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "connector host drives this contract for admitted records, polls, commits or \
                  acknowledgements"
    )
)]
pub trait EmitterHostServices:
    EmitterTransientErrorStatus
    + EmitterEventReporter
    + EmitterStagingDirectory
    + EmitterGeneralErrorHandler
    + EmitterBoundedExecution
    + Send
    + Sync
    + 'static
{
}

impl<T> EmitterHostServices for T where
    T: EmitterTransientErrorStatus
        + EmitterEventReporter
        + EmitterStagingDirectory
        + EmitterGeneralErrorHandler
        + EmitterBoundedExecution
        + Send
        + Sync
        + 'static
{
}

struct EmitterHostInner {
    services: Box<dyn EmitterHostServices>,
}

/// The task-scoped host services a connector may retain without learning a runtime type.
#[derive(Clone)]
pub struct EmitterHost {
    inner: Arc<EmitterHostInner>,
}

impl EmitterHost {
    pub fn new(services: impl EmitterHostServices) -> Self {
        Self {
            inner: Arc::new(EmitterHostInner {
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

    pub fn handle_general_error(&self, acks: &EmitterRecordAcknowledgements, reason: String) {
        self.inner.services.handle_general_error(acks, reason);
    }
}

#[cfg(test)]
#[path = "emitter_tests.rs"]
mod tests;
