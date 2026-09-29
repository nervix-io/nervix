//! Producers: typed batches an application submits to a client ingestor, and their outcomes.
//!
//! Layer: edges.
//!
//! - **Owns.** Opening a producer on the session's current exchange, the credit the server granted
//!   it and the order batches wait for it in, every submission from the moment it waits for credit
//!   until the application observes its terminal outcome, retrying a batch the server refused only
//!   temporarily, the producer's admission state and end, and closing it.
//! - **Depends on.** The exchange's request identities and reply routing, the wire contract's
//!   producer operations, and the client producer vocabulary.
//! - **Must not know.** How the server admits a batch, which node executes the ingestor, or Arrow,
//!   apart from encoding a record batch as the canonical stream under the `arrow` feature.
//!
//! A producer keeps its desired endpoint across session exchanges. Each attachment and every
//! submission attempt remains bound to the exchange that carried it. Restoration only installs a
//! fresh attachment after its generation and endpoint contract match the producer's first open;
//! it never resends a submission whose outcome is missing.
//!
//! A submission keeps its credit until the application observes its outcome through
//! [`Producer::send`] or [`Producer::rejoin`], or releases it. Cancelling a wait therefore never
//! loses an outcome or the batch: [`Producer::pending_submissions`] lists it, and a producer whose
//! application stops reading outcomes stops being granted credit for new batches.

use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU64,
    sync::{Arc as StdArc, Weak},
    time::Duration,
};

use ahash::HashMap;
use arch_into::ArchInto as _;
use bytes::Bytes;
use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_client_wire::{
    ClientMessage, ClientRequest, CloseIngestorRequest, OpenIngestorDisposition,
    OpenIngestorRequest, ProducerAdmissionChanged, ProducerEnded, ProducerId, ReplyBody, RequestId,
    SubmissionOutcome as WireSubmissionOutcome, SubmitBatchRequest,
};
use nervix_models::{
    CLIENT_PRODUCER_SESSION_BYTES, ClientOutcomeUncertainty, ClientProcessingFailure,
    ClientProducerAdmission, ClientProducerDescription, ClientProducerEndReason,
    ClientProducerLimits, ClientSubmissionOutcome, ClientSubmissionRefusal, DomainName,
    IngestorName, SchemaField,
};
use nervix_primitives::sync::{blocking::Mutex as SyncMutex, oneshot, watch};
use nervix_recovery::Discarded as _;
use thiserror::Error;
use triomphe::Arc;

use crate::{
    client::{Client, RecoveryMode, SessionRecovery},
    error::{ClientError, RequestKind},
    exchange::{ExchangeRequests, SESSION_LIMITS},
};

mod slots;

use slots::SubmissionSlots;

/// The identity of one submission within its producer. It stays the same when a batch the server
/// refused temporarily is sent again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubmissionId(NonZeroU64);

impl SubmissionId {
    pub const fn get(self) -> NonZeroU64 {
        self.0
    }
}

impl fmt::Display for SubmissionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Why no terminal result could be established for a submitted batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubmissionUncertainty {
    /// The server reports that the ingestor's execution stopped, or the producer ended, while the
    /// batch's acknowledgement was unresolved.
    Interrupted,
    /// The server reports that the node executing the ingestor, or the connection to it, was lost.
    OwnerLost,
    /// The session that carried the batch ended before its outcome arrived.
    SessionLost,
}

impl From<ClientOutcomeUncertainty> for SubmissionUncertainty {
    fn from(uncertainty: ClientOutcomeUncertainty) -> Self {
        match uncertainty {
            ClientOutcomeUncertainty::Interrupted => Self::Interrupted,
            ClientOutcomeUncertainty::OwnerLost => Self::OwnerLost,
        }
    }
}

/// The terminal outcome of one submitted batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerOutcome {
    /// No row of the batch entered the graph. A batch refused only temporarily is sent again
    /// before this is reported, so a refusal reported here is final for this attempt.
    NotAdmitted {
        refusal: ClientSubmissionRefusal,
        message: String,
    },
    /// The batch's source acknowledgement resolved successfully under the graph's policies.
    Completed,
    /// The batch was admitted and its source acknowledgement failed. Some of its effects may have
    /// happened; replaying it is the application's choice.
    ProcessingFailed {
        failure: ClientProcessingFailure,
        message: String,
    },
    /// The batch may have been admitted and processed. Replaying it may duplicate its effects.
    OutcomeUnknown {
        cause: SubmissionUncertainty,
        message: String,
    },
}

impl ProducerOutcome {
    fn from_wire(outcome: WireSubmissionOutcome) -> Self {
        let WireSubmissionOutcome { outcome, message } = outcome;
        match outcome {
            ClientSubmissionOutcome::NotAdmitted(refusal) => Self::NotAdmitted { refusal, message },
            ClientSubmissionOutcome::Completed => Self::Completed,
            ClientSubmissionOutcome::ProcessingFailed(failure) => {
                Self::ProcessingFailed { failure, message }
            }
            ClientSubmissionOutcome::OutcomeUnknown(cause) => Self::OutcomeUnknown {
                cause: cause.into(),
                message,
            },
        }
    }

    /// A batch that never left the client.
    fn not_sent(message: String) -> Self {
        Self::NotAdmitted {
            refusal: ClientSubmissionRefusal::ProducerEnded,
            message,
        }
    }
}

/// How a producer ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerEnd {
    /// The application closed it.
    Closed,
    /// The server ended it.
    Ended {
        reason: ClientProducerEndReason,
        message: String,
    },
    /// The session that held it ended.
    SessionLost,
    /// The original endpoint contract or domain generation no longer exists. The application
    /// must open a new producer and inspect its contract before sending again.
    ReopenRequired(ProducerReopenReason),
}

/// Why restoration cannot keep the contract of an existing producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerReopenReason {
    DomainStopped,
    EndpointRemoved,
    SchemaChanged,
    ContractChanged,
    GenerationChanged,
    ProtocolViolated,
    Refused(nervix_models::ClientProducerRefusal),
}

/// The connectivity of a producer handle. An interrupted handle remains desired until closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerConnection {
    Active,
    Interrupted,
    Restoring,
    ReopenRequired,
    Closed,
}

/// Why a producer operation failed. None of these sends anything to the server.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProducerError {
    #[error("the producer ended: {0:?}")]
    Ended(ProducerEnd),
    #[error("a batch of {size} bytes exceeds the {limit} bytes one submission may carry")]
    BatchTooLarge { size: usize, limit: u64 },
    #[error("a batch is empty")]
    EmptyBatch,
    #[error("the producer holds no submission {0}")]
    UnknownSubmission(SubmissionId),
    #[error("the producer's session could not be restored before the retry deadline")]
    SessionUnavailable,
    #[cfg(feature = "arrow")]
    #[error("the batch's schema differs from the producer's")]
    SchemaMismatch,
    #[cfg(feature = "arrow")]
    #[error("the batch has {rows} rows, more than the {limit} one batch may carry")]
    TooManyRows { rows: usize, limit: u32 },
    #[cfg(feature = "arrow")]
    #[error("the batch could not be encoded as an Arrow IPC stream")]
    Encode,
}

/// One batch, as the canonical Arrow IPC stream the server accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerBatch {
    ipc: Bytes,
}

impl ProducerBatch {
    /// A batch already written as one canonical Arrow IPC stream: its schema message, one record
    /// batch message and the end-of-stream marker, uncompressed and without dictionaries.
    pub fn from_arrow_ipc(ipc: Bytes) -> Self {
        Self { ipc }
    }

    pub fn len(&self) -> usize {
        self.ipc.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ipc.is_empty()
    }
}

/// A submission the producer holds, and its outcome once it has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSubmission {
    pub id: SubmissionId,
    /// `None` while the batch waits to be sent or for its outcome.
    pub outcome: Option<ProducerOutcome>,
}

/// The events of producers the server opened on one exchange: admission changes and ends.
#[derive(Clone, Default)]
pub(crate) struct ProducerRegistry {
    state: Arc<SyncMutex<ProducerRegistryState>>,
}

#[derive(Default)]
struct ProducerRegistryState {
    producers: HashMap<ProducerKey, RegisteredProducer>,
    /// Handles are weak so dropping a client application handle releases its desired entry.
    desired: BTreeMap<usize, Weak<ProducerInner>>,
    current: HashMap<ProducerKey, Weak<ProducerInner>>,
}

/// A producer of one exchange. Request identities restart with every exchange, so the exchange is
/// part of the key; the registered producer keeps that exchange's generation alive, so its address
/// names it for as long as the entry exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ProducerKey {
    exchange: usize,
    producer: ProducerId,
}

impl ProducerKey {
    fn new(generation: &Arc<()>, producer: ProducerId) -> Self {
        Self {
            exchange: Arc::as_ptr(generation).addr(),
            producer,
        }
    }
}

struct RegisteredProducer {
    _generation: Arc<()>,
    signals: Arc<ProducerSignals>,
}

/// What the exchange reader tells one producer.
pub(crate) struct ProducerSignals {
    admission: watch::Sender<ClientProducerAdmission>,
    end: watch::Sender<Option<ProducerEnd>>,
}

impl ProducerRegistry {
    /// Registers the producer an open reply announced, before the reply reaches its waiter, so no
    /// event the server sent after the reply can arrive before the producer is registered.
    pub(crate) fn opened(
        &self,
        generation: &Arc<()>,
        producer: ProducerId,
        admission: ClientProducerAdmission,
    ) {
        let (admission, _) = watch::channel(admission);
        let (end, _) = watch::channel(None);
        let registered = RegisteredProducer {
            _generation: generation.clone(),
            signals: Arc::new(ProducerSignals { admission, end }),
        };
        self.state
            .lock()
            .producers
            .insert(ProducerKey::new(generation, producer), registered)
            .discarded("a request identity opens at most one producer on its exchange");
    }

    fn signals(&self, generation: &Arc<()>, producer: ProducerId) -> Option<Arc<ProducerSignals>> {
        let state = self.state.lock();
        let registered = state
            .producers
            .get(&ProducerKey::new(generation, producer))?;
        Some(registered.signals.clone())
    }

    fn bind(&self, handle: &StdArc<ProducerInner>, attachment: &ProducerAttachment) {
        let key = ProducerKey::new(&attachment.generation, attachment.id);
        let mut state = self.state.lock();
        state
            .desired
            .insert(StdArc::as_ptr(handle).addr(), StdArc::downgrade(handle));
        state.current.insert(key, StdArc::downgrade(handle));
    }

    fn unbind(&self, handle: &ProducerInner, attachment: Option<&ProducerAttachment>) {
        let mut state = self.state.lock();
        state.desired.remove(&std::ptr::from_ref(handle).addr());
        if let Some(attachment) = attachment {
            state
                .current
                .remove(&ProducerKey::new(&attachment.generation, attachment.id));
        }
    }

    pub(crate) fn restorable(&self) -> Vec<StdArc<ProducerInner>> {
        let mut state = self.state.lock();
        state.desired.retain(|_, handle| handle.strong_count() > 0);
        state.desired.values().filter_map(Weak::upgrade).collect()
    }

    pub(crate) fn admission(&self, generation: &Arc<()>, changed: ProducerAdmissionChanged) {
        let Some(signals) = self.signals(generation, changed.producer) else {
            return;
        };
        signals.admission.send_replace(changed.admission);
    }

    pub(crate) fn ended(&self, generation: &Arc<()>, ended: ProducerEnded) {
        let key = ProducerKey::new(generation, ended.producer);
        let (removed, handle) = {
            let mut state = self.state.lock();
            let removed = state.producers.remove(&key);
            let handle = state
                .current
                .remove(&key)
                .and_then(|handle| handle.upgrade());
            (removed, handle)
        };
        let Some(removed) = removed else {
            return;
        };
        let terminal = ProducerEnd::Ended {
            reason: ended.reason,
            message: ended.message,
        };
        removed.signals.end.send_replace(Some(terminal.clone()));
        if let Some(handle) = handle {
            handle.attachment_ended(generation, ended.producer, terminal);
        }
    }

    /// Stops following a producer the application closed.
    fn closed(&self, generation: &Arc<()>, producer: ProducerId) {
        let key = ProducerKey::new(generation, producer);
        let removed = {
            let mut state = self.state.lock();
            state.current.remove(&key);
            state.producers.remove(&key)
        };
        if let Some(removed) = removed {
            removed.signals.end.send_replace(Some(ProducerEnd::Closed));
        }
    }

    /// Ends every producer of an exchange that ended.
    pub(crate) fn exchange_ended(&self, generation: &Arc<()>) {
        let exchange = Arc::as_ptr(generation).addr();
        let mut ended = Vec::new();
        let desired = {
            let mut state = self.state.lock();
            let keys = state
                .producers
                .keys()
                .filter(|key| key.exchange == exchange)
                .copied()
                .collect::<Vec<_>>();
            for key in keys {
                state.current.remove(&key);
                if let Some(removed) = state.producers.remove(&key) {
                    ended.push(removed);
                }
            }
            state.desired.retain(|_, handle| handle.strong_count() > 0);
            state
                .desired
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        for removed in ended {
            removed
                .signals
                .end
                .send_replace(Some(ProducerEnd::SessionLost));
        }
        for handle in desired {
            handle.exchange_ended(generation);
        }
    }
}

/// A producer attached to a client ingestor. Dropping it closes it without waiting.
pub struct Producer {
    inner: StdArc<ProducerInner>,
    closed: bool,
}

pub(crate) struct ProducerInner {
    initial_id: ProducerId,
    domain: DomainName,
    ingestor: IngestorName,
    request: OpenIngestorRequest,
    pinned: ClientProducerDescription,
    client: Client,
    registry: ProducerRegistry,
    /// Submission identities and credit survive an attachment replacement. Old attempts resolve
    /// through their own exchange before they can release a slot for a new attempt.
    slots: SubmissionSlots,
    lifecycle: ProducerLifecycle,
}

/// The desired producer's selected primitive owner of attachment and close transitions.
struct ProducerLifecycle {
    phase: SyncMutex<ProducerPhase>,
    changed: watch::Sender<()>,
}

struct ProducerAttachment {
    id: ProducerId,
    description: ClientProducerDescription,
    exchange: Arc<ExchangeRequests>,
    generation: Arc<()>,
    signals: Arc<ProducerSignals>,
}

enum ProducerPhase {
    Active(Arc<ProducerAttachment>),
    Interrupted,
    Restoring(Arc<()>),
    ReopenRequired(ProducerReopenReason),
    Closed,
}

/// What became of one attempt to send a batch.
enum Attempt {
    Answered(ProducerOutcome),
    /// The exchange ended before the batch was sent.
    NotSent,
    /// The exchange ended after the batch was sent and before its outcome arrived.
    Lost,
}

impl Client {
    /// Attaches a producer to a client ingestor of `domain`.
    ///
    /// The producer is bound to `domain`; a later `USE` does not move it. Its wire attachment is
    /// bound to one exchange, while the handle restores that attachment after a session loss if
    /// the domain generation and endpoint contract still match. `expected_fields` must be exactly
    /// the ingestor's input schema, including each field's optionality and sensitivity. A refusal
    /// is [`ClientError::ProducerRefused`] and leaves nothing attached. An open interrupted by a
    /// lost exchange is sent again on the next one because that exchange ended its attachments.
    pub async fn open_ingestor(
        &self,
        domain: DomainName,
        ingestor: IngestorName,
        expected_fields: Vec<SchemaField>,
        limits: ClientProducerLimits,
    ) -> error_stack::Result<Producer, ClientError> {
        let request = OpenIngestorRequest {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
            expected_fields,
            limits,
        };
        let opened = tokio::time::timeout(self.inner.connector.retry_timeout(), async {
            for _ in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
                nervix_primitives::task::consume_budget().await;
                let attempt = self.open_on_current_exchange(request.clone()).await;
                let report = match attempt {
                    Ok(producer) => return Ok(producer),
                    Err(report) => report,
                };
                if !report.current_context().retryable_session_failure() {
                    return Err(report);
                }
                match self.recover_session(RecoveryMode::IfClosed).await? {
                    SessionRecovery::Ready => {}
                    SessionRecovery::Unavailable => return Err(report),
                }
            }
            // Only a session that closed again on the last attempt leaves the loop.
            Err(Report::new(ClientError::SessionClosed))
        })
        .await;
        match opened {
            Ok(result) => result,
            Err(_) => Err(Report::new(ClientError::RetryDeadline)),
        }
    }

    /// Sends one open on the current exchange and waits for its answer.
    async fn open_on_current_exchange(
        &self,
        request: OpenIngestorRequest,
    ) -> error_stack::Result<Producer, ClientError> {
        let (exchange, generation) = {
            let exchange = self.inner.exchange.lock().await;
            (exchange.requests(), exchange.generation.clone())
        };
        let registry = self.inner.events.sinks.producers.clone();
        let client = self.clone();
        let (answer, answered) = oneshot::channel();
        // The open runs in a task of its own, so a caller that stops waiting leaves behind a task
        // that closes a producer the server opens anyway.
        nervix_primitives::task::spawn(async move {
            let sent = request_on_exchange(
                &exchange,
                ClientRequest::OpenIngestor(request.clone()),
                RequestKind::OpenIngestor,
            )
            .await;
            let opened = match sent {
                Ok(sent) => {
                    ProducerInner::opened(sent, request, client, exchange, generation, registry)
                }
                Err(error) => Err(error),
            };
            if let Err(Ok(producer)) = answer.send(opened) {
                drop(producer);
            }
        });
        let Ok(opened) = answered.await else {
            return Err(Report::new(ClientError::SessionClosed));
        };
        opened
    }
}

/// A reply and the identity of the request it answers.
pub(crate) struct Answered {
    pub(crate) request_id: RequestId,
    pub(crate) body: ReplyBody,
}

/// Sends one request on `exchange` and waits for its reply without a deadline of its own, and
/// without closing the exchange when the caller stops waiting.
pub(crate) async fn request_on_exchange(
    exchange: &ExchangeRequests,
    request: ClientRequest,
    kind: RequestKind,
) -> error_stack::Result<Answered, ClientError> {
    let Some(mut registered) = exchange.register() else {
        return Err(Report::new(exchange.pending.lock().failure()));
    };
    let request_id = registered.request_id;
    let message = ClientMessage {
        request_id,
        request,
    };
    let frame = message.encode(&SESSION_LIMITS).map_err(|report| {
        Report::new(ClientError::EncodeRequest {
            request: kind,
            source: report.current_context().clone(),
        })
    })?;
    if exchange.frames.send(frame).await.is_err() {
        return Err(Report::new(exchange.pending.lock().failure()));
    }
    match registered.receive().await {
        Some(body) => Ok(Answered { request_id, body }),
        None => Err(Report::new(ClientError::RequestInterrupted {
            request: kind,
        })),
    }
}

/// Releases one wire attachment even when the application stopped awaiting its close. A silent
/// server cannot retain the caller's cleanup task without bound; expiring the request also marks
/// the exchange unusable so a later operation installs a replacement session.
async fn close_attachment(
    attachment: Arc<ProducerAttachment>,
    deadline: Duration,
) -> error_stack::Result<(), ClientError> {
    let request = ClientRequest::CloseIngestor(CloseIngestorRequest {
        producer: attachment.id,
    });
    let answered = tokio::time::timeout(
        deadline,
        request_on_exchange(&attachment.exchange, request, RequestKind::CloseIngestor),
    )
    .await;
    let answered = match answered {
        Ok(answered) => answered?,
        Err(_) => {
            attachment.exchange.pending.lock().close();
            return Err(Report::new(ClientError::RequestDeadline {
                request: RequestKind::CloseIngestor,
            }));
        }
    };
    match answered.body {
        ReplyBody::CloseIngestor(_) => Ok(()),
        other => Err(Report::new(ClientError::unexpected_reply(
            RequestKind::CloseIngestor,
            other,
        ))),
    }
}

impl ProducerLifecycle {
    fn new(phase: ProducerPhase) -> Self {
        let (changed, _) = watch::channel(());
        Self {
            phase: SyncMutex::new(phase),
            changed,
        }
    }

    fn connection(&self) -> ProducerConnection {
        match &*self.phase.lock() {
            ProducerPhase::Active(_) => ProducerConnection::Active,
            ProducerPhase::Interrupted => ProducerConnection::Interrupted,
            ProducerPhase::Restoring(_) => ProducerConnection::Restoring,
            ProducerPhase::ReopenRequired(_) => ProducerConnection::ReopenRequired,
            ProducerPhase::Closed => ProducerConnection::Closed,
        }
    }

    fn end(&self) -> Option<ProducerEnd> {
        match &*self.phase.lock() {
            ProducerPhase::ReopenRequired(reason) => {
                Some(ProducerEnd::ReopenRequired(reason.clone()))
            }
            ProducerPhase::Closed => Some(ProducerEnd::Closed),
            _ => None,
        }
    }

    fn current(&self) -> Option<Arc<ProducerAttachment>> {
        match &*self.phase.lock() {
            ProducerPhase::Active(attachment) => Some(attachment.clone()),
            _ => None,
        }
    }

    fn exchange_ended(&self, generation: &Arc<()>) {
        let mut phase = self.phase.lock();
        let matches = match &*phase {
            ProducerPhase::Active(attachment) => Arc::ptr_eq(&attachment.generation, generation),
            ProducerPhase::Restoring(restoring) => Arc::ptr_eq(restoring, generation),
            _ => false,
        };
        if matches {
            *phase = ProducerPhase::Interrupted;
            drop(phase);
            self.changed.send_replace(());
        }
    }

    fn begin_restore(&self, generation: &Arc<()>) -> bool {
        let mut phase = self.phase.lock();
        if !matches!(&*phase, ProducerPhase::Interrupted) {
            return false;
        }
        *phase = ProducerPhase::Restoring(generation.clone());
        drop(phase);
        self.changed.send_replace(());
        true
    }

    fn close(&self) -> Option<Arc<ProducerAttachment>> {
        let mut phase = self.phase.lock();
        let current = match &*phase {
            ProducerPhase::Active(attachment) => Some(attachment.clone()),
            _ => None,
        };
        *phase = ProducerPhase::Closed;
        drop(phase);
        self.changed.send_replace(());
        current
    }
}

impl ProducerInner {
    /// The producer an open reply announced, or the error the reply stands for.
    fn opened(
        sent: Answered,
        request: OpenIngestorRequest,
        client: Client,
        exchange: Arc<ExchangeRequests>,
        generation: Arc<()>,
        registry: ProducerRegistry,
    ) -> error_stack::Result<Producer, ClientError> {
        let deadline = client.inner.connector.request_timeout();
        let attachment =
            ProducerAttachment::opened(sent, &request, exchange, generation, &registry, deadline)?;
        let description = attachment.description.clone();
        if description.fields != request.expected_fields {
            ProducerAttachment::discard(attachment, registry, deadline);
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::OpenIngestor,
            }));
        }
        let batches: usize = description.grant.batches.get().arch_into();
        let bytes = usize::try_from(description.grant.bytes.get())
            .verified("the granted bytes are within the session budget, checked at decode");
        let inner = StdArc::new(Self {
            initial_id: attachment.id,
            domain: request.domain.clone(),
            ingestor: request.ingestor.clone(),
            request,
            pinned: description,
            client,
            registry: registry.clone(),
            slots: SubmissionSlots::new(batches, bytes),
            lifecycle: ProducerLifecycle::new(ProducerPhase::Active(attachment.clone())),
        });
        registry.bind(&inner, &attachment);
        if attachment.signals.end.borrow().is_some() {
            inner.exchange_ended(&attachment.generation);
        }
        Ok(Producer {
            inner,
            closed: false,
        })
    }

    fn connection(&self) -> ProducerConnection {
        self.lifecycle.connection()
    }

    fn end(&self) -> Option<ProducerEnd> {
        self.lifecycle.end()
    }

    fn current(&self) -> Option<Arc<ProducerAttachment>> {
        self.lifecycle.current()
    }

    pub(crate) fn exchange_ended(&self, generation: &Arc<()>) {
        self.lifecycle.exchange_ended(generation);
    }

    fn attachment_ended(&self, generation: &Arc<()>, id: ProducerId, end: ProducerEnd) {
        let mut phase = self.lifecycle.phase.lock();
        let ProducerPhase::Active(attachment) = &*phase else {
            return;
        };
        if !Arc::ptr_eq(&attachment.generation, generation) || attachment.id != id {
            return;
        }
        let next = match end {
            ProducerEnd::Ended {
                reason: ClientProducerEndReason::EndpointChanged,
                ..
            } => ProducerPhase::ReopenRequired(ProducerReopenReason::ContractChanged),
            ProducerEnd::Ended {
                reason: ClientProducerEndReason::EndpointRemoved,
                ..
            } => ProducerPhase::ReopenRequired(ProducerReopenReason::EndpointRemoved),
            ProducerEnd::Ended {
                reason: ClientProducerEndReason::DomainStopped,
                ..
            } => ProducerPhase::ReopenRequired(ProducerReopenReason::DomainStopped),
            ProducerEnd::Ended {
                reason: ClientProducerEndReason::ProtocolViolated,
                ..
            } => ProducerPhase::ReopenRequired(ProducerReopenReason::ProtocolViolated),
            _ => ProducerPhase::Interrupted,
        };
        *phase = next;
        drop(phase);
        self.lifecycle.changed.send_replace(());
    }

    pub(crate) fn begin_restore(&self, generation: &Arc<()>) -> Option<OpenIngestorRequest> {
        self.lifecycle
            .begin_restore(generation)
            .then(|| self.request.clone())
    }

    pub(crate) fn is_restoring(&self, generation: &Arc<()>) -> bool {
        let phase = self.lifecycle.phase.lock();
        matches!(&*phase, ProducerPhase::Restoring(current) if Arc::ptr_eq(current, generation))
    }

    pub(crate) fn watch(&self) -> watch::Receiver<()> {
        self.lifecycle.changed.subscribe()
    }

    pub(crate) fn restore_request(&self) -> OpenIngestorRequest {
        self.request.clone()
    }

    pub(crate) fn restoration_failed(&self, generation: &Arc<()>) {
        let mut phase = self.lifecycle.phase.lock();
        if !matches!(&*phase, ProducerPhase::Restoring(current) if Arc::ptr_eq(current, generation))
        {
            return;
        }
        *phase = ProducerPhase::ReopenRequired(ProducerReopenReason::ProtocolViolated);
        drop(phase);
        self.lifecycle.changed.send_replace(());
    }

    pub(crate) fn restoration_refused(
        &self,
        generation: &Arc<()>,
        refusal: nervix_models::ClientProducerRefusal,
    ) -> bool {
        let mut phase = self.lifecycle.phase.lock();
        if !matches!(&*phase, ProducerPhase::Restoring(current) if Arc::ptr_eq(current, generation))
        {
            return false;
        }
        let terminal = match refusal {
            nervix_models::ClientProducerRefusal::DomainNotFound
            | nervix_models::ClientProducerRefusal::DomainStopped => {
                Some(ProducerReopenReason::DomainStopped)
            }
            nervix_models::ClientProducerRefusal::IngestorNotFound
            | nervix_models::ClientProducerRefusal::NotClientIngestor => {
                Some(ProducerReopenReason::EndpointRemoved)
            }
            nervix_models::ClientProducerRefusal::SchemaMismatch => {
                Some(ProducerReopenReason::SchemaChanged)
            }
            nervix_models::ClientProducerRefusal::InvalidLimits
            | nervix_models::ClientProducerRefusal::InTransaction => {
                Some(ProducerReopenReason::Refused(refusal))
            }
            nervix_models::ClientProducerRefusal::EndpointUnavailable
            | nervix_models::ClientProducerRefusal::TooManyProducers
            | nervix_models::ClientProducerRefusal::SessionCapacityExhausted
            | nervix_models::ClientProducerRefusal::NodeCapacityExhausted => None,
        };
        if let Some(reason) = terminal {
            *phase = ProducerPhase::ReopenRequired(reason);
            drop(phase);
            self.lifecycle.changed.send_replace(());
            return false;
        }
        true
    }

    pub(crate) async fn restored(
        self: &StdArc<Self>,
        sent: Answered,
        exchange: Arc<ExchangeRequests>,
        generation: Arc<()>,
    ) -> error_stack::Result<bool, ClientError> {
        let attachment = ProducerAttachment::opened(
            sent,
            &self.request,
            exchange,
            generation.clone(),
            &self.registry,
            self.client.inner.connector.request_timeout(),
        )?;
        let description = &attachment.description;
        let reason = if description.generation != self.pinned.generation {
            Some(ProducerReopenReason::GenerationChanged)
        } else if description.fields != self.pinned.fields {
            Some(ProducerReopenReason::SchemaChanged)
        } else if description.contract != self.pinned.contract
            || description.policy != self.pinned.policy
            || description.grant != self.pinned.grant
        {
            Some(ProducerReopenReason::ContractChanged)
        } else {
            None
        };
        let valid = {
            let mut phase = self.lifecycle.phase.lock();
            let valid = matches!(&*phase, ProducerPhase::Restoring(current) if Arc::ptr_eq(current, &generation));
            if valid {
                if let Some(reason) = reason.clone() {
                    *phase = ProducerPhase::ReopenRequired(reason);
                } else {
                    *phase = ProducerPhase::Active(attachment.clone());
                    // Keep the phase lock while publishing the desired binding. A concurrent
                    // close removes it only after this insertion, so a late open cannot leave
                    // a desired entry behind after the handle was closed.
                    self.registry.bind(self, &attachment);
                }
            }
            valid
        };
        if valid && reason.is_none() {
            if let Some(ended) = attachment.end() {
                self.attachment_ended(&generation, attachment.id, ended);
            }
            self.lifecycle.changed.send_replace(());
            return Ok(true);
        }
        self.lifecycle.changed.send_replace(());
        close_attachment(
            attachment.clone(),
            self.client.inner.connector.request_timeout(),
        )
        .await
        .discarded("a restoration that lost its handle releases its new attachment");
        self.registry.closed(&attachment.generation, attachment.id);
        Ok(false)
    }

    async fn attachment(&self) -> error_stack::Result<Arc<ProducerAttachment>, ProducerError> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut changed = self.lifecycle.changed.subscribe();
            if let Some(attachment) = self.current() {
                if let Some(end) = attachment.end() {
                    self.attachment_ended(&attachment.generation, attachment.id, end);
                    continue;
                }
                return Ok(attachment);
            }
            if let Some(end) = self.end() {
                return Err(Report::new(ProducerError::Ended(end)));
            }
            let recovered = self.client.recover_session(RecoveryMode::IfClosed).await;
            if !matches!(recovered, Ok(SessionRecovery::Ready)) {
                return Err(Report::new(ProducerError::SessionUnavailable));
            }
            self.client.restore_interrupted_endpoints().await;
            if let Some(attachment) = self.current() {
                return Ok(attachment);
            }
            changed
                .changed()
                .await
                .discarded("the producer still holds its restoration notifier");
        }
    }

    fn stop(&self) -> Option<Arc<ProducerAttachment>> {
        let current = self.lifecycle.close();
        self.registry.unbind(self, current.as_deref());
        if let Some(attachment) = &current {
            self.registry.closed(&attachment.generation, attachment.id);
        }
        self.slots.close();
        current
    }
}

impl ProducerAttachment {
    fn opened(
        sent: Answered,
        request: &OpenIngestorRequest,
        exchange: Arc<ExchangeRequests>,
        generation: Arc<()>,
        registry: &ProducerRegistry,
        deadline: Duration,
    ) -> error_stack::Result<Arc<Self>, ClientError> {
        let Answered { request_id, body } = sent;
        let ReplyBody::OpenIngestor(outcome) = body else {
            return Err(Report::new(ClientError::unexpected_reply(
                RequestKind::OpenIngestor,
                body,
            )));
        };
        let opened = match outcome.disposition {
            OpenIngestorDisposition::Opened(opened) => opened,
            OpenIngestorDisposition::Refused(refusal) => {
                return Err(Report::new(ClientError::ProducerRefused {
                    refusal,
                    message: outcome.message,
                }));
            }
        };
        let id = ProducerId::opened_by(request_id);
        let Some(signals) = registry.signals(&generation, id) else {
            // The exchange ended between the reply and this read, which ended the producer too.
            return Err(Report::new(ClientError::SessionClosed));
        };
        let attachment = Arc::new(Self {
            id,
            description: opened.description,
            exchange,
            generation,
            signals,
        });
        // An inconsistent reply may still represent a server attachment. Release that attachment
        // before returning the protocol error, including when this was a cancelled initial open.
        if opened.domain != request.domain
            || opened.ingestor != request.ingestor
            || attachment.description.grant.bytes.get() > CLIENT_PRODUCER_SESSION_BYTES
            || attachment.description.grant.batches > request.limits.batches
            || attachment.description.grant.bytes > request.limits.bytes
        {
            Self::discard(attachment, registry.clone(), deadline);
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::OpenIngestor,
            }));
        }
        Ok(attachment)
    }

    fn discard(attachment: Arc<Self>, registry: ProducerRegistry, deadline: Duration) {
        nervix_primitives::task::spawn(async move {
            close_attachment(attachment.clone(), deadline)
                .await
                .discarded("an invalid producer open still releases its attachment");
            registry.closed(&attachment.generation, attachment.id);
        });
    }

    fn end(&self) -> Option<ProducerEnd> {
        self.signals.end.borrow().clone()
    }

    /// Waits until the producer's batches are admitted, or until it ends.
    async fn admitting(&self) -> Result<(), ProducerEnd> {
        let mut admission = self.signals.admission.subscribe();
        let mut end = self.signals.end.subscribe();
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(ended) = end.borrow_and_update().clone() {
                return Err(ended);
            }
            if let ClientProducerAdmission::Open = *admission.borrow_and_update() {
                return Ok(());
            }
            nervix_primitives::select! {
                changed = admission.changed() => changed.assured("the producer holds the sender of its admission"),
                changed = end.changed() => changed.assured("the producer holds the sender of its end"),
            }
        }
    }

    /// Sends one attempt of a batch and waits for its outcome.
    async fn attempt(&self, batch: &Bytes) -> Attempt {
        let Some(mut registered) = self.exchange.register() else {
            return Attempt::NotSent;
        };
        let message = ClientMessage {
            request_id: registered.request_id,
            request: ClientRequest::SubmitBatch(SubmitBatchRequest {
                producer: self.id,
                batch: batch.clone(),
            }),
        };
        let frame = match message.encode(&SESSION_LIMITS) {
            Ok(frame) => frame,
            Err(error) => {
                return Attempt::Answered(ProducerOutcome::not_sent(format!(
                    "the batch does not fit a session frame: {}",
                    error.current_context()
                )));
            }
        };
        if self.exchange.frames.send(frame).await.is_err() {
            return Attempt::NotSent;
        }
        match registered.receive().await {
            Some(ReplyBody::Submission(outcome)) => {
                Attempt::Answered(ProducerOutcome::from_wire(outcome))
            }
            Some(ReplyBody::Rejected(rejected)) => {
                Attempt::Answered(ProducerOutcome::not_sent(rejected.message))
            }
            Some(_) => Attempt::Answered(ProducerOutcome::not_sent(
                "the server answered the batch with a reply of another kind".to_string(),
            )),
            None => Attempt::Lost,
        }
    }

    /// Sends a batch until it has an outcome that is not a temporary refusal, waiting for
    /// admission before every attempt and for the declared backoff after a temporary refusal.
    async fn deliver(&self, batch: Bytes) -> ProducerOutcome {
        let policy = self.description.policy;
        let mut backoff = policy.retry_backoff;
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Err(ended) = self.admitting().await {
                return ProducerOutcome::not_sent(format!("the producer ended: {ended:?}"));
            }
            let outcome = match self.attempt(&batch).await {
                Attempt::Answered(outcome) => outcome,
                Attempt::NotSent => {
                    return ProducerOutcome::not_sent(
                        "the session ended before the batch was sent".to_string(),
                    );
                }
                Attempt::Lost => {
                    return ProducerOutcome::OutcomeUnknown {
                        cause: SubmissionUncertainty::SessionLost,
                        message: "the session ended before the batch's outcome arrived".to_string(),
                    };
                }
            };
            let ProducerOutcome::NotAdmitted { refusal, .. } = &outcome else {
                return outcome;
            };
            if !refusal.is_temporary() {
                return outcome;
            }
            let mut end = self.signals.end.subscribe();
            if end.borrow_and_update().is_some() {
                return outcome;
            }
            nervix_primitives::select! {
                () = tokio::time::sleep(backoff) => {}
                _ = end.changed() => return outcome,
            }
            backoff = next_backoff(backoff, policy.retry_max_backoff);
        }
    }
}

/// The delay after `current` that the retry policy allows: twice as long, up to `maximum`.
fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    match current.checked_mul(2) {
        Some(doubled) => doubled.min(maximum),
        None => maximum,
    }
}

impl Producer {
    /// The producer's identity within its session.
    pub fn id(&self) -> ProducerId {
        match self.inner.current() {
            Some(attachment) => attachment.id,
            None => self.inner.initial_id,
        }
    }

    pub fn domain(&self) -> &DomainName {
        &self.inner.domain
    }

    pub fn ingestor(&self) -> &IngestorName {
        &self.inner.ingestor
    }

    /// The original open description. Its schema, domain generation, endpoint contract, policy,
    /// and grant are pinned across restoration. Its attachment and admission are snapshots of the
    /// first open; use [`Producer::id`] and [`Producer::admission`] for their current values.
    pub fn description(&self) -> &ClientProducerDescription {
        &self.inner.pinned
    }

    /// Whether the handle has an attachment, is waiting for one, or needs an explicit new open.
    pub fn connection(&self) -> ProducerConnection {
        self.inner.connection()
    }

    /// Whether the producer's batches are admitted right now.
    pub fn admission(&self) -> ClientProducerAdmission {
        let Some(attachment) = self.inner.current() else {
            return ClientProducerAdmission::Suspended;
        };
        *attachment.signals.admission.borrow()
    }

    /// How the producer ended, or `None` while it is open.
    pub fn end(&self) -> Option<ProducerEnd> {
        self.inner.end()
    }

    /// Waits for credit, submits one batch, and returns its terminal outcome.
    ///
    /// A batch the server refuses only temporarily, because admission is suspended or the node is
    /// busy, is sent again after the declared backoff. Cancelling the wait leaves the submission
    /// with the producer, which [`Producer::pending_submissions`] lists and
    /// [`Producer::rejoin`] resumes.
    pub async fn send(
        &self,
        batch: ProducerBatch,
    ) -> error_stack::Result<ProducerOutcome, ProducerError> {
        let id = self.submit(batch).await?;
        self.rejoin(id).await
    }

    /// Waits for credit and submits one batch, returning its identity once the producer holds it.
    /// Its outcome is observed through [`Producer::rejoin`].
    pub async fn submit(
        &self,
        batch: ProducerBatch,
    ) -> error_stack::Result<SubmissionId, ProducerError> {
        let ProducerBatch { ipc } = batch;
        if ipc.is_empty() {
            return Err(Report::new(ProducerError::EmptyBatch));
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            let attachment = self.inner.attachment().await?;
            let limit = attachment.description.grant.max_batch_bytes.get();
            let size: u64 = ipc.len().arch_into();
            if size > limit {
                return Err(Report::new(ProducerError::BatchTooLarge {
                    size: ipc.len(),
                    limit,
                }));
            }
            let bytes = u32::try_from(ipc.len()).assured(
                "a batch within the granted bytes, which one session's budget bounds, fits u32",
            );
            if let Some(ended) = attachment.end() {
                self.inner
                    .attachment_ended(&attachment.generation, attachment.id, ended);
                continue;
            }
            let mut end = attachment.signals.end.subscribe();
            let credit = nervix_primitives::select! {
                acquired = self.inner.slots.credit(bytes) => {
                    let Some(credit) = acquired else {
                        let ended = attachment.end().unwrap_or(ProducerEnd::Closed);
                        return Err(Report::new(ProducerError::Ended(ended)));
                    };
                    Some(credit)
                }
                changed = end.changed() => {
                    changed.assured("the producer holds the sender of its end");
                    let ended = attachment.end().unwrap_or(ProducerEnd::Closed);
                    self.inner.attachment_ended(&attachment.generation, attachment.id, ended);
                    None
                }
            };
            let Some(credit) = credit else {
                continue;
            };
            let id = self.inner.slots.hold();
            let slots = self.inner.clone();
            nervix_primitives::task::spawn(async move {
                let outcome = attachment.deliver(ipc).await;
                slots.slots.resolve(id, outcome, credit);
            });
            return Ok(id);
        }
    }

    /// Waits for a submission's terminal outcome and takes it, which returns its credit.
    pub async fn rejoin(
        &self,
        id: SubmissionId,
    ) -> error_stack::Result<ProducerOutcome, ProducerError> {
        self.inner.slots.rejoin(id).await
    }

    /// Every submission the producer holds, in submission order, with the outcome of each that has
    /// one.
    pub fn pending_submissions(&self) -> Vec<PendingSubmission> {
        self.inner.slots.pending()
    }

    /// Lets go of a submission. A resolved one returns its outcome and its credit now; an unresolved
    /// one returns its credit once its outcome arrives, and nobody observes that outcome.
    pub fn release(
        &self,
        id: SubmissionId,
    ) -> error_stack::Result<Option<ProducerOutcome>, ProducerError> {
        self.inner.slots.release(id)
    }

    /// Stops admission for the producer and waits until the server released it. Every batch the
    /// producer sent has its outcome by then.
    pub async fn close(mut self) -> error_stack::Result<(), ClientError> {
        self.closed = true;
        let Some(attachment) = self.inner.stop() else {
            return Ok(());
        };
        let deadline = self.inner.client.inner.connector.request_timeout();
        let (answer, answered) = oneshot::channel();
        nervix_primitives::task::spawn(async move {
            let result = close_attachment(attachment, deadline).await;
            answer
                .send(result)
                .discarded("a cancelled close still releases its attachment");
        });
        match answered.await {
            Ok(result) => result,
            Err(_) => Err(Report::new(ClientError::SessionClosed)),
        }
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let Some(attachment) = self.inner.stop() else {
            return;
        };
        let Ok(runtime) = nervix_primitives::runtime::Handle::try_current() else {
            return;
        };
        let deadline = self.inner.client.inner.connector.request_timeout();
        runtime.spawn(async move {
            // A producer dropped by its application is released without anyone waiting for it.
            close_attachment(attachment, deadline)
                .await
                .discarded("a dropped producer's close has nobody left to tell");
        });
    }
}

#[cfg(feature = "arrow")]
mod arrow_batch {
    //! Encoding a record batch as the canonical stream a producer submits.

    use arrow_array::RecordBatch;
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::Schema;
    use bytes::Bytes;
    use error_stack::Report;
    use nervix_models::SchemaField;

    use super::{Producer, ProducerBatch, ProducerError};

    impl ProducerBatch {
        /// Writes `batch` as one canonical Arrow IPC stream: its schema, the batch and the
        /// end-of-stream marker, uncompressed.
        pub fn from_record_batch(batch: &RecordBatch) -> error_stack::Result<Self, ProducerError> {
            let mut writer = StreamWriter::try_new(Vec::new(), batch.schema_ref())
                .map_err(|error| Report::new(ProducerError::Encode).attach_printable(error))?;
            writer
                .write(batch)
                .map_err(|error| Report::new(ProducerError::Encode).attach_printable(error))?;
            writer
                .finish()
                .map_err(|error| Report::new(ProducerError::Encode).attach_printable(error))?;
            let bytes = writer
                .into_inner()
                .map_err(|error| Report::new(ProducerError::Encode).attach_printable(error))?;
            Ok(Self {
                ipc: Bytes::from(bytes),
            })
        }
    }

    impl Producer {
        /// The Arrow schema every batch of this producer carries: the ingestor's input schema in
        /// declared order, without metadata.
        pub fn arrow_schema(&self) -> Schema {
            SchemaField::arrow_schema(&self.description().fields)
        }

        /// Checks `batch` against the producer's schema and row limit and writes it as the
        /// canonical stream, without sending it.
        pub fn batch(
            &self,
            batch: &RecordBatch,
        ) -> error_stack::Result<ProducerBatch, ProducerError> {
            let expected = self.arrow_schema();
            if batch.schema_ref().as_ref() != &expected {
                return Err(Report::new(ProducerError::SchemaMismatch));
            }
            let limit = self.description().grant.max_batch_rows.get();
            let rows = batch.num_rows();
            let within = match u32::try_from(rows) {
                Ok(rows) => rows <= limit,
                Err(_) => false,
            };
            if !within {
                return Err(Report::new(ProducerError::TooManyRows { rows, limit }));
            }
            ProducerBatch::from_record_batch(batch)
        }
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use super::*;
    use crate::shuttle_test::check_random_and_pct;

    #[test]
    fn shuttle_close_fences_a_producer_restore_started_on_the_same_exchange() {
        check_random_and_pct(|| {
            shuttle::future::block_on(async {
                let lifecycle = StdArc::new(ProducerLifecycle::new(ProducerPhase::Interrupted));
                let generation = Arc::new(());
                let restoring = {
                    let lifecycle = lifecycle.clone();
                    let generation = generation.clone();
                    nervix_primitives::task::spawn(
                        async move { lifecycle.begin_restore(&generation) },
                    )
                };
                let closing = {
                    let lifecycle = lifecycle.clone();
                    nervix_primitives::task::spawn(async move { lifecycle.close() })
                };
                assert!(restoring.await.is_ok());
                assert!(matches!(closing.await, Ok(None)));
                assert_eq!(lifecycle.connection(), ProducerConnection::Closed);
                assert!(!lifecycle.begin_restore(&Arc::new(())));
            });
        });
    }

    #[test]
    fn shuttle_close_fences_a_producer_restore_interrupted_by_another_loss() {
        check_random_and_pct(|| {
            shuttle::future::block_on(async {
                let generation = Arc::new(());
                let lifecycle = StdArc::new(ProducerLifecycle::new(ProducerPhase::Restoring(
                    generation.clone(),
                )));
                let ending = {
                    let lifecycle = lifecycle.clone();
                    let generation = generation.clone();
                    nervix_primitives::task::spawn(async move {
                        lifecycle.exchange_ended(&generation);
                    })
                };
                let closing = {
                    let lifecycle = lifecycle.clone();
                    nervix_primitives::task::spawn(async move { lifecycle.close() })
                };
                assert!(ending.await.is_ok());
                assert!(matches!(closing.await, Ok(None)));
                assert_eq!(lifecycle.connection(), ProducerConnection::Closed);
                assert!(!lifecycle.begin_restore(&Arc::new(())));
            });
        });
    }
}

#[cfg(test)]
#[path = "producer_tests.rs"]
mod tests;
