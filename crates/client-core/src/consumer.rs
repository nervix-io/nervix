//! Application consumers of native client emitter output.
//!
//! Layer: edges.
//! - **Owns.** Desired emitter consumers across session exchanges, reading Arrow deliveries,
//!   keeping a read whose caller stopped waiting for the next read of the same attachment,
//!   settling each attempt only through the attachment that delivered it, and closing attachments.
//! - **Depends on.** The session request exchange and typed consumer wire contract.
//! - **Must not know.** Which node executes the emitter or how its output is produced.
//!
//! A read the server answers with a batch assigns that attempt to the consumer. A caller that
//! stops waiting for a read therefore parks it with its consumer, and the next read of the same
//! attachment takes it over instead of asking for another batch, so a cancelled wait never strands
//! an assigned attempt until its ACK timeout.

use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

use bytes::Bytes;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_client_wire::{
    ClientRequest, CloseEmitterRequest, ConsumerId, EmitterBatchDecision, EmitterCloseDisposition,
    EmitterOpenRefusal, EmitterOpened, EmitterSettlement, OpenEmitterDisposition,
    OpenEmitterRequest, ReadEmitterBatchRequest, ReadEmitterDisposition, ReplyBody,
    SettleEmitterBatchRequest,
};
use nervix_models::{
    ClientConsumerLimits, DomainName, EmitterName, RelayName, SchemaField, Timestamp,
};
use nervix_primitives::sync::{
    Arc, StdArc, StdWeak,
    atomic::{AtomicBool, Ordering},
    blocking::Mutex as SyncMutex,
    oneshot, watch,
};
use nervix_recovery::Discarded as _;
use uuid::Uuid;

use crate::{
    client::{Client, RecoveryMode, SessionRecovery},
    error::{ClientError, RequestKind},
    exchange::{ExchangeRequests, PendingRequest},
    producer::{Answered, request_on_exchange, send_on_exchange},
};

struct ConsumerAttachment {
    id: ConsumerId,
    #[cfg(feature = "arrow")]
    description: EmitterOpened,
    exchange: Arc<ExchangeRequests>,
    generation: Arc<()>,
    closed: AtomicBool,
}

pub(crate) struct ConsumerHandle {
    client: Client,
    request: OpenEmitterRequest,
    pinned: EmitterOpened,
    registry: DesiredConsumers,
    initial_id: ConsumerId,
    lifecycle: ConsumerLifecycle,
    /// Reads whose callers stopped waiting before their replies arrived, oldest first. Each is
    /// taken over by a later read of its attachment; the rest end with their attachment.
    parked: SyncMutex<VecDeque<ParkedRead>>,
}

/// A batch read sent on one attachment, whose reply nobody waits for right now.
struct ParkedRead {
    attachment: Arc<ConsumerAttachment>,
    request: PendingRequest,
}

/// The wait for one read's reply. Dropping it before the reply arrives parks the read with its
/// consumer.
struct WaitingRead<'a> {
    handle: &'a ConsumerHandle,
    read: Option<ParkedRead>,
}

/// The selected primitive owner of a desired consumer's attachment and close fence. It is kept
/// independent of the transport so its state transitions can be checked under Shuttle.
struct ConsumerLifecycle {
    phase: SyncMutex<ConsumerPhase>,
    changed: watch::Sender<()>,
    /// The first read after an attachment gap reports it before any restored delivery.
    interruption_pending: AtomicBool,
}

enum ConsumerPhase {
    Active(Arc<ConsumerAttachment>),
    Interrupted,
    Restoring(Arc<()>),
    ReopenRequired(ConsumerReopenReason),
    Closed,
}

/// Why a consumer must explicitly open a new endpoint instead of following its former one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerReopenReason {
    DomainStopped,
    EndpointRemoved,
    SchemaChanged,
    ContractChanged,
    GenerationChanged,
    Refused(EmitterOpenRefusal),
    ProtocolViolated,
}

/// Connectivity of a consumer handle, independently of deliveries already given to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerConnection {
    Active,
    Interrupted,
    Restoring,
    ReopenRequired,
    Closed,
}

#[derive(Clone, Default)]
pub(crate) struct DesiredConsumers {
    desired: Arc<SyncMutex<BTreeMap<usize, StdWeak<ConsumerHandle>>>>,
}

/// One competing application consumer attached to an emitter on a single session exchange.
pub struct EmitterConsumer {
    inner: StdArc<ConsumerHandle>,
}

/// One Arrow batch attempt. A retry or timeout may deliver the same identity again with a fresh
/// reference; only the current reference can settle it.
pub struct EmitterDelivery {
    inner: Arc<ConsumerAttachment>,
    pub identity: Uuid,
    pub reference: Uuid,
    pub source_relay: RelayName,
    pub branch_fingerprint: Option<[u8; 32]>,
    pub batch: Bytes,
    pub members: u32,
    pub execution_now: Timestamp,
}

impl Client {
    /// Opens a competing consumer with an exact output schema. A lost exchange is restored before
    /// retrying the open. The returned consumer follows an unchanged endpoint across exchanges.
    pub async fn subscribe_emitter(
        &self,
        domain: DomainName,
        emitter: EmitterName,
        expected_fields: Vec<SchemaField>,
        limits: ClientConsumerLimits,
    ) -> error_stack::Result<EmitterConsumer, ClientError> {
        let request = OpenEmitterRequest {
            domain,
            emitter,
            expected_fields,
            limits,
        };
        let opened =
            nervix_primitives::time::timeout(self.inner.connector.retry_timeout(), async {
                for _ in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
                    nervix_primitives::task::consume_budget().await;
                    let (exchange, generation) = {
                        let exchange = self.inner.exchange.lock().await;
                        (exchange.requests(), exchange.generation.clone())
                    };
                    let (answer, answered) = oneshot::channel();
                    let request = request.clone();
                    let client = self.clone();
                    nervix_primitives::task::spawn(async move {
                        let result = async {
                            let reply = request_on_exchange(
                                &exchange,
                                ClientRequest::OpenEmitter(request.clone()),
                                RequestKind::OpenEmitter,
                            )
                            .await?;
                            let ReplyBody::OpenEmitter(outcome) = reply.body else {
                                return Err(Report::new(ClientError::unexpected_reply(
                                    RequestKind::OpenEmitter,
                                    reply.body,
                                )));
                            };
                            let description = match outcome.disposition {
                                OpenEmitterDisposition::Opened(opened) => *opened,
                                OpenEmitterDisposition::Refused(refusal) => {
                                    return Err(Report::new(ClientError::ConsumerRefused {
                                        refusal,
                                        message: outcome.message,
                                    }));
                                }
                            };
                            let id = ConsumerId::opened_by(reply.request_id);
                            if description.domain != request.domain
                                || description.emitter != request.emitter
                                || description.fields != request.expected_fields
                                || description.granted.batches > request.limits.batches
                                || description.granted.bytes > request.limits.bytes
                            {
                                close_consumer(
                                    exchange.clone(),
                                    id,
                                    client.inner.connector.request_timeout(),
                                )
                                .await
                                .discarded(
                                    "an invalid consumer open still releases its attachment",
                                );
                                return Err(Report::new(ClientError::UnexpectedReply {
                                    request: RequestKind::OpenEmitter,
                                }));
                            }
                            let attachment = Arc::new(ConsumerAttachment {
                                id,
                                #[cfg(feature = "arrow")]
                                description: description.clone(),
                                exchange,
                                generation,
                                closed: AtomicBool::new(false),
                            });
                            let registry = client.inner.events.sinks.consumers.clone();
                            let inner = StdArc::new(ConsumerHandle {
                                client,
                                request,
                                pinned: description,
                                registry: registry.clone(),
                                initial_id: id,
                                lifecycle: ConsumerLifecycle::new(ConsumerPhase::Active(
                                    attachment.clone(),
                                )),
                                parked: SyncMutex::new(VecDeque::new()),
                            });
                            registry.register(&inner);
                            if !attachment.exchange.pending.lock().is_open() {
                                inner.exchange_ended(&attachment.generation);
                            }
                            Ok(EmitterConsumer { inner })
                        }
                        .await;
                        if let Err(Ok(consumer)) = answer.send(result) {
                            drop(consumer);
                        }
                    });
                    let attempt = answered
                        .await
                        .unwrap_or_else(|_| Err(Report::new(ClientError::SessionClosed)));
                    let report = match attempt {
                        Ok(consumer) => return Ok(consumer),
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
                Err(Report::new(ClientError::SessionClosed))
            })
            .await;
        opened.unwrap_or_else(|_| Err(Report::new(ClientError::RetryDeadline)))
    }
}

impl DesiredConsumers {
    fn register(&self, handle: &StdArc<ConsumerHandle>) {
        self.desired
            .lock()
            .insert(StdArc::as_ptr(handle).addr(), StdArc::downgrade(handle));
    }

    fn unregister(&self, handle: &ConsumerHandle) {
        self.desired
            .lock()
            .remove(&std::ptr::from_ref(handle).addr());
    }

    pub(crate) fn restorable(&self) -> Vec<StdArc<ConsumerHandle>> {
        let mut desired = self.desired.lock();
        desired.retain(|_, handle| handle.strong_count() > 0);
        desired.values().filter_map(StdWeak::upgrade).collect()
    }

    pub(crate) fn exchange_ended(&self, generation: &Arc<()>) {
        for handle in self.restorable() {
            handle.exchange_ended(generation);
        }
    }
}

async fn close_consumer(
    exchange: Arc<ExchangeRequests>,
    id: ConsumerId,
    deadline: Duration,
) -> error_stack::Result<EmitterCloseDisposition, ClientError> {
    let request = ClientRequest::CloseEmitter(CloseEmitterRequest { consumer: id });
    let reply = nervix_primitives::time::timeout(
        deadline,
        request_on_exchange(&exchange, request, RequestKind::CloseEmitter),
    )
    .await;
    let reply = match reply {
        Ok(reply) => reply,
        Err(_) => {
            exchange.pending.lock().close();
            return Err(Report::new(ClientError::RequestDeadline {
                request: RequestKind::CloseEmitter,
            }));
        }
    }?;
    let ReplyBody::CloseEmitter(outcome) = reply.body else {
        return Err(Report::new(ClientError::unexpected_reply(
            RequestKind::CloseEmitter,
            reply.body,
        )));
    };
    Ok(outcome.disposition)
}

impl ConsumerLifecycle {
    fn new(phase: ConsumerPhase) -> Self {
        let (changed, _) = watch::channel(());
        Self {
            phase: SyncMutex::new(phase),
            changed,
            interruption_pending: AtomicBool::new(false),
        }
    }

    fn connection(&self) -> ConsumerConnection {
        match &*self.phase.lock() {
            ConsumerPhase::Active(_) => ConsumerConnection::Active,
            ConsumerPhase::Interrupted => ConsumerConnection::Interrupted,
            ConsumerPhase::Restoring(_) => ConsumerConnection::Restoring,
            ConsumerPhase::ReopenRequired(_) => ConsumerConnection::ReopenRequired,
            ConsumerPhase::Closed => ConsumerConnection::Closed,
        }
    }

    fn current(&self) -> Option<Arc<ConsumerAttachment>> {
        match &*self.phase.lock() {
            ConsumerPhase::Active(attachment) => Some(attachment.clone()),
            _ => None,
        }
    }

    fn reopen_reason(&self) -> Option<ConsumerReopenReason> {
        match &*self.phase.lock() {
            ConsumerPhase::ReopenRequired(reason) => Some(reason.clone()),
            _ => None,
        }
    }

    fn exchange_ended(&self, generation: &Arc<()>) {
        let mut phase = self.phase.lock();
        let matches = match &*phase {
            ConsumerPhase::Active(attachment) => Arc::ptr_eq(&attachment.generation, generation),
            ConsumerPhase::Restoring(current) => Arc::ptr_eq(current, generation),
            _ => false,
        };
        if matches {
            if let ConsumerPhase::Active(attachment) = &*phase {
                attachment.closed.store(true, Ordering::Release);
            }
            *phase = ConsumerPhase::Interrupted;
            self.interruption_pending.store(true, Ordering::Release);
            drop(phase);
            self.changed.send_replace(());
        }
    }

    fn begin_restore(&self, generation: &Arc<()>) -> bool {
        let mut phase = self.phase.lock();
        if !matches!(&*phase, ConsumerPhase::Interrupted) {
            return false;
        }
        *phase = ConsumerPhase::Restoring(generation.clone());
        drop(phase);
        self.changed.send_replace(());
        true
    }

    fn close(&self) -> Option<Arc<ConsumerAttachment>> {
        let mut phase = self.phase.lock();
        let attachment = match &*phase {
            ConsumerPhase::Active(attachment) => Some(attachment.clone()),
            _ => None,
        };
        *phase = ConsumerPhase::Closed;
        drop(phase);
        if let Some(attachment) = &attachment {
            attachment.closed.store(true, Ordering::Release);
        }
        self.changed.send_replace(());
        attachment
    }
}

impl ConsumerHandle {
    fn connection(&self) -> ConsumerConnection {
        self.lifecycle.connection()
    }

    fn current(&self) -> Option<Arc<ConsumerAttachment>> {
        self.lifecycle.current()
    }

    pub(crate) fn exchange_ended(&self, generation: &Arc<()>) {
        self.lifecycle.exchange_ended(generation);
    }

    pub(crate) fn begin_restore(&self, generation: &Arc<()>) -> Option<OpenEmitterRequest> {
        self.lifecycle
            .begin_restore(generation)
            .then(|| self.request.clone())
    }

    pub(crate) fn is_restoring(&self, generation: &Arc<()>) -> bool {
        matches!(&*self.lifecycle.phase.lock(), ConsumerPhase::Restoring(current) if Arc::ptr_eq(current, generation))
    }

    pub(crate) fn watch(&self) -> watch::Receiver<()> {
        self.lifecycle.changed.subscribe()
    }

    pub(crate) fn restore_request(&self) -> OpenEmitterRequest {
        self.request.clone()
    }

    pub(crate) fn restoration_failed(&self, generation: &Arc<()>) {
        let mut phase = self.lifecycle.phase.lock();
        if !matches!(&*phase, ConsumerPhase::Restoring(current) if Arc::ptr_eq(current, generation))
        {
            return;
        }
        *phase = ConsumerPhase::ReopenRequired(ConsumerReopenReason::ProtocolViolated);
        drop(phase);
        self.lifecycle.changed.send_replace(());
    }

    pub(crate) fn restoration_refused(
        &self,
        generation: &Arc<()>,
        refusal: EmitterOpenRefusal,
    ) -> bool {
        let mut phase = self.lifecycle.phase.lock();
        if !matches!(&*phase, ConsumerPhase::Restoring(current) if Arc::ptr_eq(current, generation))
        {
            return false;
        }
        let terminal = match refusal {
            EmitterOpenRefusal::DomainNotFound | EmitterOpenRefusal::DomainStopped => {
                Some(ConsumerReopenReason::DomainStopped)
            }
            EmitterOpenRefusal::EmitterNotFound | EmitterOpenRefusal::NotClientEmitter => {
                Some(ConsumerReopenReason::EndpointRemoved)
            }
            EmitterOpenRefusal::SchemaMismatch => Some(ConsumerReopenReason::SchemaChanged),
            EmitterOpenRefusal::InvalidLimits | EmitterOpenRefusal::InTransaction => {
                Some(ConsumerReopenReason::Refused(refusal))
            }
            EmitterOpenRefusal::EndpointUnavailable
            | EmitterOpenRefusal::TooManyConsumers
            | EmitterOpenRefusal::SessionCapacityExhausted
            | EmitterOpenRefusal::NodeCapacityExhausted => None,
        };
        if let Some(reason) = terminal {
            *phase = ConsumerPhase::ReopenRequired(reason);
            drop(phase);
            self.lifecycle.changed.send_replace(());
            return false;
        }
        true
    }

    pub(crate) async fn restored(
        self: &StdArc<Self>,
        id: ConsumerId,
        description: EmitterOpened,
        exchange: Arc<ExchangeRequests>,
        generation: Arc<()>,
    ) {
        let reason = if description.domain != self.request.domain
            || description.emitter != self.request.emitter
        {
            Some(ConsumerReopenReason::ProtocolViolated)
        } else if description.generation != self.pinned.generation {
            Some(ConsumerReopenReason::GenerationChanged)
        } else if description.fields != self.pinned.fields {
            Some(ConsumerReopenReason::SchemaChanged)
        } else if description.contract != self.pinned.contract
            || description.window != self.pinned.window
            || description.ack_timeout != self.pinned.ack_timeout
            || description.retry_backoff != self.pinned.retry_backoff
            || description.retry_max_backoff != self.pinned.retry_max_backoff
            || description.granted != self.pinned.granted
            || description.max_batch_bytes != self.pinned.max_batch_bytes
            || description.max_batch_rows != self.pinned.max_batch_rows
        {
            Some(ConsumerReopenReason::ContractChanged)
        } else {
            None
        };
        let attachment = Arc::new(ConsumerAttachment {
            id,
            #[cfg(feature = "arrow")]
            description,
            exchange,
            generation: generation.clone(),
            closed: AtomicBool::new(false),
        });
        let installed = {
            let mut phase = self.lifecycle.phase.lock();
            let valid = matches!(&*phase, ConsumerPhase::Restoring(current) if Arc::ptr_eq(current, &generation));
            if valid {
                *phase = match reason {
                    Some(reason) => ConsumerPhase::ReopenRequired(reason),
                    None => ConsumerPhase::Active(attachment.clone()),
                };
            }
            valid && matches!(&*phase, ConsumerPhase::Active(_))
        };
        self.lifecycle.changed.send_replace(());
        if !installed {
            attachment.closed.store(true, Ordering::Release);
            close_consumer(
                attachment.exchange.clone(),
                id,
                self.client.inner.connector.request_timeout(),
            )
            .await
            .discarded("a refused or late consumer restoration releases its new attachment");
        }
    }

    async fn attachment(&self) -> error_stack::Result<Arc<ConsumerAttachment>, ClientError> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut changed = self.lifecycle.changed.subscribe();
            {
                let phase = self.lifecycle.phase.lock();
                match &*phase {
                    ConsumerPhase::Active(attachment) => return Ok(attachment.clone()),
                    ConsumerPhase::Closed => return Err(Report::new(ClientError::SessionClosed)),
                    ConsumerPhase::ReopenRequired(reason) => {
                        return Err(Report::new(ClientError::ConsumerReopenRequired(
                            reason.clone(),
                        )));
                    }
                    _ => {}
                }
            }
            if !matches!(
                self.client.recover_session(RecoveryMode::IfClosed).await,
                Ok(SessionRecovery::Ready)
            ) {
                return Err(Report::new(ClientError::ConsumerSessionUnavailable));
            }
            self.client.restore_interrupted_endpoints().await;
            if self.current().is_some() {
                continue;
            }
            changed
                .changed()
                .await
                .discarded("the consumer still holds its restoration notifier");
        }
    }

    fn stop(&self) -> Option<Arc<ConsumerAttachment>> {
        let attachment = self.lifecycle.close();
        self.registry.unregister(self);
        // A closed consumer reads nothing again; the attempts its parked reads may hold are
        // revoked with its attachment.
        let parked = std::mem::take(&mut *self.parked.lock());
        drop(parked);
        attachment
    }

    /// Waits for the reply of a batch read on `attachment`: one a caller stopped waiting for
    /// earlier, or else a new one.
    async fn read(
        &self,
        attachment: &Arc<ConsumerAttachment>,
    ) -> error_stack::Result<Answered, ClientError> {
        let request = match self.adopt(attachment) {
            Some(request) => request,
            None => {
                let read = ClientRequest::ReadEmitterBatch(ReadEmitterBatchRequest {
                    consumer: attachment.id,
                });
                send_on_exchange(&attachment.exchange, read, RequestKind::ReadEmitterBatch).await?
            }
        };
        let waiting = WaitingRead {
            handle: self,
            read: Some(ParkedRead {
                attachment: attachment.clone(),
                request,
            }),
        };
        waiting.reply().await
    }

    /// Takes over the oldest parked read of `attachment`. Parked reads of an earlier attachment
    /// ended with it, and are dropped on the way.
    fn adopt(&self, attachment: &Arc<ConsumerAttachment>) -> Option<PendingRequest> {
        let mut stale = Vec::new();
        let adopted = {
            let mut parked = self.parked.lock();
            loop {
                let Some(read) = parked.pop_front() else {
                    break None;
                };
                if Arc::ptr_eq(&read.attachment, attachment) {
                    break Some(read.request);
                }
                stale.push(read);
            }
        };
        drop(stale);
        adopted
    }

    /// Keeps a read whose caller stopped waiting for a later read of its attachment. A read of an
    /// attachment that already ended has no reply left to take over.
    fn park(&self, read: ParkedRead) {
        if read.attachment.closed.load(Ordering::Acquire) {
            return;
        }
        self.parked.lock().push_back(read);
    }
}

impl WaitingRead<'_> {
    async fn reply(mut self) -> error_stack::Result<Answered, ClientError> {
        let read = self
            .read
            .as_mut()
            .verified("a waiting read holds its request until its reply arrives");
        let request_id = read.request.request_id;
        let body = read.request.receive().await;
        // The reply arrived, or the exchange ended without one: either way nothing is left to
        // take over.
        self.read = None;
        match body {
            Some(body) => Ok(Answered { request_id, body }),
            None => Err(Report::new(ClientError::RequestInterrupted {
                request: RequestKind::ReadEmitterBatch,
            })),
        }
    }
}

impl Drop for WaitingRead<'_> {
    fn drop(&mut self) {
        if let Some(read) = self.read.take() {
            self.handle.park(read);
        }
    }
}

impl EmitterConsumer {
    pub fn description(&self) -> &EmitterOpened {
        &self.inner.pinned
    }

    pub fn id(&self) -> ConsumerId {
        match self.inner.current() {
            Some(attachment) => attachment.id,
            None => self.inner.initial_id,
        }
    }

    pub fn connection(&self) -> ConsumerConnection {
        self.inner.connection()
    }

    /// Why the consumer needs an explicit new open, once it does.
    pub fn reopen_reason(&self) -> Option<ConsumerReopenReason> {
        self.inner.lifecycle.reopen_reason()
    }

    /// Reads one attempt. An attachment gap is reported once before any restored delivery.
    ///
    /// A caller that stops waiting leaves the read with the consumer, and the next read of the
    /// same attachment receives its reply, so no attempt the server assigned is stranded.
    pub async fn next_batch(&self) -> error_stack::Result<Option<EmitterDelivery>, ClientError> {
        if self
            .inner
            .lifecycle
            .interruption_pending
            .swap(false, Ordering::AcqRel)
        {
            return Err(Report::new(ClientError::ConsumerInterrupted));
        }
        if self.inner.connection() == ConsumerConnection::Closed {
            return Ok(None);
        }
        let attachment = self.inner.attachment().await?;
        if self
            .inner
            .lifecycle
            .interruption_pending
            .swap(false, Ordering::AcqRel)
        {
            return Err(Report::new(ClientError::ConsumerInterrupted));
        }
        let reply = self.inner.read(&attachment).await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(report) if report.current_context().retryable_session_failure() => {
                self.inner.exchange_ended(&attachment.generation);
                self.inner
                    .lifecycle
                    .interruption_pending
                    .store(false, Ordering::Release);
                return Err(Report::new(ClientError::ConsumerInterrupted));
            }
            Err(report) => return Err(report),
        };
        let ReplyBody::ReadEmitterBatch(outcome) = reply.body else {
            return Err(Report::new(ClientError::unexpected_reply(
                RequestKind::ReadEmitterBatch,
                reply.body,
            )));
        };
        if attachment.closed.load(Ordering::Acquire)
            || self
                .inner
                .current()
                .is_none_or(|current| !Arc::ptr_eq(&current, &attachment))
        {
            self.inner
                .lifecycle
                .interruption_pending
                .store(false, Ordering::Release);
            return Err(Report::new(ClientError::ConsumerInterrupted));
        }
        match outcome.disposition {
            ReadEmitterDisposition::Ended => {
                // The wire end does not distinguish relocation from a removed endpoint. A fresh
                // open checks the pinned contract and generation, and its refusal supplies the
                // terminal reason when the endpoint really disappeared.
                self.inner.exchange_ended(&attachment.generation);
                self.inner
                    .lifecycle
                    .interruption_pending
                    .store(false, Ordering::Release);
                Err(Report::new(ClientError::ConsumerInterrupted))
            }
            ReadEmitterDisposition::Batch(batch) => Ok(Some(EmitterDelivery {
                inner: attachment,
                identity: batch.identity,
                reference: batch.reference,
                source_relay: batch.source_relay,
                branch_fingerprint: batch.branch_fingerprint,
                batch: batch.batch,
                members: batch.members,
                execution_now: batch.execution_now,
            })),
        }
    }

    /// Closes the desired consumer and releases its current attachment.
    ///
    /// Only the first close releases the attachment; a later one, or one while the consumer waits
    /// to be restored, finds nothing attached and returns at once. A caller that stops waiting
    /// leaves the release running.
    pub async fn close(&self) -> error_stack::Result<EmitterCloseDisposition, ClientError> {
        let Some(attachment) = self.inner.stop() else {
            return Ok(EmitterCloseDisposition::Closed);
        };
        let deadline = self.inner.client.inner.connector.request_timeout();
        let (answer, answered) = oneshot::channel();
        nervix_primitives::task::spawn(async move {
            let closed = close_consumer(attachment.exchange.clone(), attachment.id, deadline).await;
            answer
                .send(closed)
                .discarded("a cancelled close still releases its attachment");
        });
        match answered.await {
            Ok(closed) => closed,
            Err(_) => Err(Report::new(ClientError::SessionClosed)),
        }
    }
}

impl Drop for EmitterConsumer {
    fn drop(&mut self) {
        let Some(attachment) = self.inner.stop() else {
            return;
        };
        let deadline = self.inner.client.inner.connector.request_timeout();
        if let Ok(runtime) = nervix_primitives::runtime::Handle::try_current() {
            runtime.spawn(async move {
                close_consumer(attachment.exchange.clone(), attachment.id, deadline)
                    .await
                    .discarded("consumer Drop cannot report a close error to its former caller");
            });
        }
    }
}

impl EmitterDelivery {
    pub async fn ack(&self) -> error_stack::Result<EmitterSettlement, ClientError> {
        self.settle(EmitterBatchDecision::Ack).await
    }

    pub async fn retry(&self) -> error_stack::Result<EmitterSettlement, ClientError> {
        self.settle(EmitterBatchDecision::Retry).await
    }

    pub async fn reject(
        &self,
        reason: impl Into<String>,
    ) -> error_stack::Result<EmitterSettlement, ClientError> {
        self.settle(EmitterBatchDecision::Reject(reason.into()))
            .await
    }

    async fn settle(
        &self,
        decision: EmitterBatchDecision,
    ) -> error_stack::Result<EmitterSettlement, ClientError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Report::new(ClientError::DeliveryReferenceExpired {
                reference: self.reference,
            }));
        }
        let request = ClientRequest::SettleEmitterBatch(SettleEmitterBatchRequest {
            consumer: self.inner.id,
            reference: self.reference,
            decision,
        });
        let reply = request_on_exchange(
            &self.inner.exchange,
            request,
            RequestKind::SettleEmitterBatch,
        )
        .await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(report) if report.current_context().retryable_session_failure() => {
                return Err(Report::new(ClientError::SettlementUnknown {
                    reference: self.reference,
                }));
            }
            Err(report) => return Err(report),
        };
        let ReplyBody::SettleEmitterBatch(outcome) = reply.body else {
            return Err(Report::new(ClientError::unexpected_reply(
                RequestKind::SettleEmitterBatch,
                reply.body,
            )));
        };
        Ok(outcome.disposition)
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use nervix_model_harness::shuttle::check_random_and_pct;

    use super::*;

    #[test]
    fn shuttle_close_fences_a_consumer_restore_started_on_the_same_exchange() {
        check_random_and_pct(|| {
            shuttle::future::block_on(async {
                let lifecycle = StdArc::new(ConsumerLifecycle::new(ConsumerPhase::Interrupted));
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
                assert_eq!(lifecycle.connection(), ConsumerConnection::Closed);
                assert!(!lifecycle.begin_restore(&Arc::new(())));
            });
        });
    }

    #[test]
    fn shuttle_close_fences_a_consumer_restore_interrupted_by_another_loss() {
        check_random_and_pct(|| {
            shuttle::future::block_on(async {
                let generation = Arc::new(());
                let lifecycle = StdArc::new(ConsumerLifecycle::new(ConsumerPhase::Restoring(
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
                assert_eq!(lifecycle.connection(), ConsumerConnection::Closed);
                assert!(!lifecycle.begin_restore(&Arc::new(())));
            });
        });
    }
}

#[cfg(feature = "arrow")]
impl EmitterDelivery {
    /// Decodes the canonical Arrow stream and checks every field against the emitter's declared
    /// output schema before yielding it to the application.
    pub fn record_batch(&self) -> error_stack::Result<arrow_array::RecordBatch, ClientError> {
        use arrow_ipc::reader::StreamReader;
        let expected = SchemaField::arrow_schema(&self.inner.description.fields);
        let mut reader =
            StreamReader::try_new(std::io::Cursor::new(&self.batch), None).map_err(|_| {
                Report::new(ClientError::UnexpectedReply {
                    request: RequestKind::ReadEmitterBatch,
                })
            })?;
        if reader.schema().as_ref() != &expected {
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::ReadEmitterBatch,
            }));
        }
        let Some(batch) = reader.next() else {
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::ReadEmitterBatch,
            }));
        };
        let batch = batch.map_err(|_| {
            Report::new(ClientError::UnexpectedReply {
                request: RequestKind::ReadEmitterBatch,
            })
        })?;
        if reader.next().is_some()
            || batch.num_rows()
                != meticulous::ResultExt::assured(
                    usize::try_from(self.members),
                    "a u32 row count fits in supported client targets",
                )
        {
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::ReadEmitterBatch,
            }));
        }
        Ok(batch)
    }
}
