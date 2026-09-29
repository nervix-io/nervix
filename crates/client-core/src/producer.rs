//! Producers: typed batches an application submits to a client ingestor, and their outcomes.
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
//! A producer belongs to the exchange that opened it. When that exchange ends, every batch it
//! sent without an outcome is reported as of unknown outcome, every batch still waiting to be sent
//! is reported as not admitted, and the producer ends: this release restores no producer across a
//! reconnect, so the application opens another one.
//!
//! A submission keeps its credit until the application observes its outcome through
//! [`Producer::send`] or [`Producer::rejoin`], or releases it. Cancelling a wait therefore never
//! loses an outcome or the batch: [`Producer::pending_submissions`] lists it, and a producer whose
//! application stops reading outcomes stops being granted credit for new batches.

use std::{fmt, num::NonZeroU64, time::Duration};

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
    producers: Arc<SyncMutex<HashMap<ProducerKey, RegisteredProducer>>>,
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
        self.producers
            .lock()
            .insert(ProducerKey::new(generation, producer), registered)
            .discarded("a request identity opens at most one producer on its exchange");
    }

    fn signals(&self, generation: &Arc<()>, producer: ProducerId) -> Option<Arc<ProducerSignals>> {
        let producers = self.producers.lock();
        let registered = producers.get(&ProducerKey::new(generation, producer))?;
        Some(registered.signals.clone())
    }

    pub(crate) fn admission(&self, generation: &Arc<()>, changed: ProducerAdmissionChanged) {
        let Some(signals) = self.signals(generation, changed.producer) else {
            return;
        };
        signals.admission.send_replace(changed.admission);
    }

    pub(crate) fn ended(&self, generation: &Arc<()>, ended: ProducerEnded) {
        let removed = self
            .producers
            .lock()
            .remove(&ProducerKey::new(generation, ended.producer));
        let Some(removed) = removed else {
            return;
        };
        removed.signals.end.send_replace(Some(ProducerEnd::Ended {
            reason: ended.reason,
            message: ended.message,
        }));
    }

    /// Stops following a producer the application closed.
    fn closed(&self, generation: &Arc<()>, producer: ProducerId) {
        let removed = self
            .producers
            .lock()
            .remove(&ProducerKey::new(generation, producer));
        if let Some(removed) = removed {
            removed.signals.end.send_replace(Some(ProducerEnd::Closed));
        }
    }

    /// Ends every producer of an exchange that ended.
    pub(crate) fn exchange_ended(&self, generation: &Arc<()>) {
        let exchange = Arc::as_ptr(generation).addr();
        let mut ended = Vec::new();
        {
            let mut producers = self.producers.lock();
            let keys = producers
                .keys()
                .filter(|key| key.exchange == exchange)
                .copied()
                .collect::<Vec<_>>();
            for key in keys {
                if let Some(removed) = producers.remove(&key) {
                    ended.push(removed);
                }
            }
        }
        for removed in ended {
            removed
                .signals
                .end
                .send_replace(Some(ProducerEnd::SessionLost));
        }
    }
}

/// A producer attached to a client ingestor. Dropping it closes it without waiting.
pub struct Producer {
    inner: Arc<ProducerInner>,
    closed: bool,
}

struct ProducerInner {
    id: ProducerId,
    domain: DomainName,
    ingestor: IngestorName,
    description: ClientProducerDescription,
    exchange: Arc<ExchangeRequests>,
    generation: Arc<()>,
    registry: ProducerRegistry,
    signals: Arc<ProducerSignals>,
    /// Every submission from the moment the producer takes it until its outcome is taken, and
    /// the credit each holds.
    slots: SubmissionSlots,
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
    /// The producer is bound to `domain` and to this session's current exchange: a later `USE`
    /// does not move it, and it ends with the exchange. `expected_fields` must be exactly the
    /// ingestor's input schema, including each field's optionality and sensitivity. A refusal is
    /// [`ClientError::ProducerRefused`], and leaves nothing attached. An exchange that was lost is
    /// reopened first, and an open the lost exchange interrupted is sent again on the next one: a
    /// producer that exchange may have attached ended with it.
    pub async fn open_ingestor(
        &self,
        domain: DomainName,
        ingestor: IngestorName,
        expected_fields: Vec<SchemaField>,
        limits: ClientProducerLimits,
    ) -> error_stack::Result<Producer, ClientError> {
        let request = ClientRequest::OpenIngestor(OpenIngestorRequest {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
            expected_fields,
            limits,
        });
        let opened = tokio::time::timeout(self.inner.connector.retry_timeout(), async {
            for _ in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
                nervix_primitives::task::consume_budget().await;
                let attempt = self
                    .open_on_current_exchange(request.clone(), &domain, &ingestor)
                    .await;
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
        request: ClientRequest,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> error_stack::Result<Producer, ClientError> {
        let (exchange, generation) = {
            let exchange = self.inner.exchange.lock().await;
            (exchange.requests(), exchange.generation.clone())
        };
        let registry = self.inner.events.sinks.producers.clone();
        let domain = domain.clone();
        let ingestor = ingestor.clone();
        let (answer, answered) = oneshot::channel();
        // The open runs in a task of its own, so a caller that stops waiting leaves behind a task
        // that closes a producer the server opens anyway.
        nervix_primitives::task::spawn(async move {
            let sent = request_on_exchange(&exchange, request, RequestKind::OpenIngestor).await;
            let opened = match sent {
                Ok(sent) => {
                    ProducerInner::opened(sent, domain, ingestor, exchange, generation, registry)
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
struct Answered {
    request_id: RequestId,
    body: ReplyBody,
}

/// Sends one request on `exchange` and waits for its reply without a deadline of its own, and
/// without closing the exchange when the caller stops waiting.
async fn request_on_exchange(
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

impl ProducerInner {
    /// The producer an open reply announced, or the error the reply stands for.
    fn opened(
        sent: Answered,
        domain: DomainName,
        ingestor: IngestorName,
        exchange: Arc<ExchangeRequests>,
        generation: Arc<()>,
        registry: ProducerRegistry,
    ) -> error_stack::Result<Producer, ClientError> {
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
        let description = opened.description;
        let batches: usize = description.grant.batches.get().arch_into();
        // A grant beyond what one producer may ask for is not an answer to this open.
        if description.grant.bytes.get() > CLIENT_PRODUCER_SESSION_BYTES {
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::OpenIngestor,
            }));
        }
        let bytes = usize::try_from(description.grant.bytes.get())
            .verified("the granted bytes are within the session budget, checked above");
        let inner = ProducerInner {
            id,
            domain,
            ingestor,
            slots: SubmissionSlots::new(batches, bytes),
            description,
            exchange,
            generation,
            registry,
            signals,
        };
        Ok(Producer {
            inner: Arc::new(inner),
            closed: false,
        })
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
        self.inner.id
    }

    pub fn domain(&self) -> &DomainName {
        &self.inner.domain
    }

    pub fn ingestor(&self) -> &IngestorName {
        &self.inner.ingestor
    }

    /// Everything the open established: the schema, the domain generation, the endpoint contract,
    /// the attachment, the policy and the grant.
    pub fn description(&self) -> &ClientProducerDescription {
        &self.inner.description
    }

    /// Whether the producer's batches are admitted right now.
    pub fn admission(&self) -> ClientProducerAdmission {
        *self.inner.signals.admission.borrow()
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
        let limit = self.inner.description.grant.max_batch_bytes.get();
        let size: u64 = ipc.len().arch_into();
        if size > limit {
            return Err(Report::new(ProducerError::BatchTooLarge {
                size: ipc.len(),
                limit,
            }));
        }
        if let Some(ended) = self.inner.end() {
            return Err(Report::new(ProducerError::Ended(ended)));
        }
        let bytes = u32::try_from(ipc.len()).assured(
            "a batch within the granted bytes, which one session's budget bounds, fits u32",
        );
        let credit = {
            let mut end = self.inner.signals.end.subscribe();
            nervix_primitives::select! {
                acquired = self.inner.slots.credit(bytes) => {
                    let Some(credit) = acquired else {
                        let ended = self.inner.end().unwrap_or(ProducerEnd::Closed);
                        return Err(Report::new(ProducerError::Ended(ended)));
                    };
                    credit
                }
                changed = end.changed() => {
                    changed.assured("the producer holds the sender of its end");
                    let ended = self.inner.end().unwrap_or(ProducerEnd::Closed);
                    return Err(Report::new(ProducerError::Ended(ended)));
                }
            }
        };
        let id = self.inner.slots.hold();
        let inner = self.inner.clone();
        nervix_primitives::task::spawn(async move {
            let outcome = inner.deliver(ipc).await;
            inner.slots.resolve(id, outcome, credit);
        });
        Ok(id)
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
        let request = ClientRequest::CloseIngestor(CloseIngestorRequest {
            producer: self.inner.id,
        });
        let answered =
            request_on_exchange(&self.inner.exchange, request, RequestKind::CloseIngestor).await?;
        self.inner.stop();
        match answered.body {
            ReplyBody::CloseIngestor(_) => Ok(()),
            other => Err(Report::new(ClientError::unexpected_reply(
                RequestKind::CloseIngestor,
                other,
            ))),
        }
    }
}

impl ProducerInner {
    /// Ends the producer on the client: credit waits stop, and the registry stops following it.
    fn stop(&self) {
        self.registry.closed(&self.generation, self.id);
        self.slots.close();
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        self.inner.stop();
        let Ok(runtime) = nervix_primitives::runtime::Handle::try_current() else {
            return;
        };
        let inner = self.inner.clone();
        runtime.spawn(async move {
            let request = ClientRequest::CloseIngestor(CloseIngestorRequest { producer: inner.id });
            // A producer dropped by its application is released without anyone waiting for it.
            request_on_exchange(&inner.exchange, request, RequestKind::CloseIngestor)
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

#[cfg(test)]
#[path = "producer_tests.rs"]
mod tests;
