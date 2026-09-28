//! The producers one session holds open.
//!
//! Layer: edges.
//!
//! - **Owns.** Opening a producer for the session and answering the open, the session's producer
//!   count and byte budget together with the node's, each producer's credit, handing its batches
//!   to its endpoint without the receive loop waiting on them, answering every batch with its
//!   terminal outcome, reporting the producer's admission changes and end, and closing it.
//! - **Depends on.** The client producer router, the committed schedule and domain states, the
//!   session's control lane and in-flight registry, and the client wire contract.
//! - **Must not know.** How a batch is decoded or admitted, an endpoint's window, the interconnect,
//!   or how a transport carries frames.
//!
//! Each open producer has one pump task. It owns the producer's route and the batches the producer
//! has outstanding, and it is the only writer of their replies, so a batch's outcome, the producer's
//! admission changes and its end reach the client in the order the endpoint produced them. The
//! receive loop only checks a batch against the producer's credit and hands it to the pump.
//!
//! A batch holds one of the producer's granted batches and its bytes from the moment the receive
//! loop takes it until the pump returns them, which it does before it queues the batch's reply. A
//! client that sends a batch only once an earlier reply has freed room for it therefore never
//! exceeds its credit. A batch beyond the credit is refused, and the producer is closed and ended as
//! a protocol violation; the batches it already submitted still receive their outcomes.

use std::{collections::BTreeMap, num::NonZeroU64};

use ahash::HashMap;
use arch_into::ArchInto as _;
use bytes::Bytes;
use meticulous::OptionExt as _;
use nervix_client_wire::{
    CloseIngestorDisposition, CloseIngestorOutcome, CloseIngestorRequest, OpenIngestorDisposition,
    OpenIngestorOutcome, OpenIngestorRequest, ProducerAdmissionChanged, ProducerEnded, ProducerId,
    ProducerOpened, ReplyBody, RequestId, SubmissionOutcome, SubmitBatchRequest,
};
use nervix_models::{
    CLIENT_PRODUCER_SESSION_BYTES, ClientOutcomeUncertainty, ClientProducerEndReason,
    ClientProducerGrant, ClientProducerRefusal, ClientSubmissionOutcome, ClientSubmissionRefusal,
    DomainStatus, IngestorInput, MAX_CLIENT_PRODUCERS_PER_SESSION,
};
use nervix_recovery::Discarded as _;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::debug;
use triomphe::Arc;

use super::{InFlightKind, QueuedReply, SessionShared};
use crate::{
    application::client_producers::{OpenedRoute, ProducerOpen, ProducerRoute},
    runtime::{
        ClientProducerEvent, ClientProducerEvents, ClientProducerReservation, ClientSubmissionId,
    },
};

/// The producers of one session, shared by its receive loop, its ordered lane and each producer's
/// pump.
#[derive(Default)]
pub(super) struct SessionProducers {
    state: Mutex<ProducersState>,
}

#[derive(Default)]
struct ProducersState {
    /// Producers that accept batches and closes, by the request that opened them.
    open: HashMap<ProducerId, OpenProducer>,
    /// Producers attached for the session, open or still answering what they hold after a close
    /// or an end, and the bytes they reserved. Both count against the session's limits until the
    /// producer's pump has answered everything.
    held_producers: usize,
    held_bytes: u64,
}

/// What the receive loop needs of an open producer.
struct OpenProducer {
    credit: Arc<ProducerCredit>,
    commands: mpsc::UnboundedSender<PumpCommand>,
}

/// What the receive loop hands a producer's pump.
enum PumpCommand {
    Submit {
        submission: RequestId,
        batch: Bytes,
    },
    /// A batch beyond the producer's credit, which ends the producer.
    Violation {
        submission: RequestId,
    },
    Close {
        request: RequestId,
    },
}

/// The credit one producer was granted and what its outstanding batches hold of it.
pub(super) struct ProducerCredit {
    grant: ClientProducerGrant,
    held: Mutex<HeldCredit>,
}

/// The batches and bytes a producer's outstanding batches hold of its credit.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct HeldCredit {
    pub(super) batches: u32,
    pub(super) bytes: u64,
}

impl ProducerCredit {
    pub(super) fn new(grant: ClientProducerGrant) -> Self {
        Self {
            grant,
            held: Mutex::new(HeldCredit::default()),
        }
    }

    /// Takes one batch of `bytes` from the credit, or `false` when that would exceed it.
    pub(super) fn take(&self, bytes: u64) -> bool {
        let mut held = self.held.lock();
        let Some(batches) = held.batches.checked_add(1) else {
            return false;
        };
        let Some(bytes) = held.bytes.checked_add(bytes) else {
            return false;
        };
        if batches > self.grant.batches.get() || bytes > self.grant.bytes.get() {
            return false;
        }
        *held = HeldCredit { batches, bytes };
        true
    }

    /// Returns one batch of `bytes` that `take` gave out.
    pub(super) fn release(&self, bytes: u64) {
        let mut held = self.held.lock();
        held.batches = held
            .batches
            .checked_sub(1)
            .verified("a batch is released once, after `take` counted it");
        held.bytes = held
            .bytes
            .checked_sub(bytes)
            .verified("a batch releases exactly the bytes `take` counted for it");
    }

    #[cfg(test)]
    pub(super) fn held(&self) -> HeldCredit {
        *self.held.lock()
    }
}

/// How the receive loop routed a batch.
enum SubmissionRouting {
    /// The producer's pump holds the batch and answers it.
    Routed,
    /// No producer took it; the receive loop answers it with this refusal.
    Refused(ClientSubmissionRefusal),
}

impl SessionProducers {
    /// Reserves room for one more producer of `bytes`, or the refusal the session's limits give.
    fn reserve(&self, bytes: u64) -> Result<(), ClientProducerRefusal> {
        let mut state = self.state.lock();
        if state.held_producers >= MAX_CLIENT_PRODUCERS_PER_SESSION {
            return Err(ClientProducerRefusal::TooManyProducers);
        }
        let Some(held_bytes) = state.held_bytes.checked_add(bytes) else {
            return Err(ClientProducerRefusal::SessionCapacityExhausted);
        };
        if held_bytes > CLIENT_PRODUCER_SESSION_BYTES {
            return Err(ClientProducerRefusal::SessionCapacityExhausted);
        }
        state.held_producers = state
            .held_producers
            .checked_add(1)
            .verified("the count is below the session limit, checked above");
        state.held_bytes = held_bytes;
        Ok(())
    }

    /// Returns the room a producer reserved.
    fn release(&self, bytes: u64) {
        let mut state = self.state.lock();
        state.held_producers = state
            .held_producers
            .checked_sub(1)
            .verified("a producer releases the room it reserved once");
        state.held_bytes = state
            .held_bytes
            .checked_sub(bytes)
            .verified("a producer releases exactly the bytes it reserved");
    }

    fn insert(&self, producer: ProducerId, open: OpenProducer) {
        self.state.lock().open.insert(producer, open).discarded(
            "a producer is keyed by the request that opened it, which the session never reuses",
        );
    }

    fn remove(&self, producer: ProducerId) {
        self.state.lock().open.remove(&producer);
    }

    /// Takes the batch's credit and hands it to its producer's pump. A batch beyond the credit
    /// ends the producer, whose pump answers the batch.
    fn route_submission(
        &self,
        producer: ProducerId,
        submission: RequestId,
        batch: Bytes,
    ) -> SubmissionRouting {
        let bytes: u64 = batch.len().arch_into();
        let mut state = self.state.lock();
        let Some(open) = state.open.get(&producer) else {
            return SubmissionRouting::Refused(ClientSubmissionRefusal::ProducerEnded);
        };
        if open.credit.take(bytes) {
            let command = PumpCommand::Submit { submission, batch };
            if open.commands.send(command).is_ok() {
                return SubmissionRouting::Routed;
            }
            // The pump stopped, which only happens once the session ended.
            open.credit.release(bytes);
            return SubmissionRouting::Refused(ClientSubmissionRefusal::ProducerEnded);
        }
        let open = state
            .open
            .remove(&producer)
            .verified("the producer was found above under the same lock");
        if open
            .commands
            .send(PumpCommand::Violation { submission })
            .is_ok()
        {
            return SubmissionRouting::Routed;
        }
        SubmissionRouting::Refused(ClientSubmissionRefusal::CreditExceeded)
    }

    /// Hands a close to the producer's pump, which answers it once the producer is released.
    /// `false` means the session holds no open producer by that identity.
    fn route_close(&self, producer: ProducerId, request: RequestId) -> bool {
        let Some(open) = self.state.lock().open.remove(&producer) else {
            return false;
        };
        open.commands.send(PumpCommand::Close { request }).is_ok()
    }
}

/// Why an open was refused, with the message the client reads.
struct OpenRefusal {
    refusal: ClientProducerRefusal,
    message: String,
}

impl OpenRefusal {
    fn new(refusal: ClientProducerRefusal, message: String) -> Self {
        Self { refusal, message }
    }
}

/// The room one producer holds of its session's limits and its node's budget, returned when the
/// producer's pump has answered everything.
struct ProducerHold {
    shared: Arc<SessionShared>,
    bytes: u64,
    _node: ClientProducerReservation,
}

impl Drop for ProducerHold {
    fn drop(&mut self) {
        self.shared.producers.release(self.bytes);
    }
}

impl SessionProducers {
    /// Opens a producer for `shared`'s session and answers the request. Serves on the ordered
    /// lane.
    pub(super) async fn open(
        &self,
        shared: &Arc<SessionShared>,
        request_id: RequestId,
        open: OpenIngestorRequest,
        in_transaction: bool,
    ) {
        let producer = ProducerId::opened_by(request_id);
        let domain = open.domain.clone();
        let ingestor = open.ingestor.clone();
        let opened = self.attach(shared, open, in_transaction).await;
        let (route, hold) = match opened {
            Ok(attached) => attached,
            Err(OpenRefusal { refusal, message }) => {
                debug!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    refusal = refusal.as_ref(),
                    "a producer open was refused"
                );
                let outcome = OpenIngestorOutcome {
                    disposition: OpenIngestorDisposition::Refused(refusal),
                    message,
                };
                shared
                    .finish_with(request_id, ReplyBody::OpenIngestor(outcome))
                    .await;
                return;
            }
        };
        let OpenedRoute {
            description,
            route,
            events,
        } = route;
        let credit = Arc::new(ProducerCredit::new(description.grant));
        let (commands, received) = mpsc::unbounded_channel();
        // The producer accepts batches before its reply is queued: a client sends the first one
        // only after reading that reply, which the transport takes after it is queued.
        self.insert(
            producer,
            OpenProducer {
                credit: credit.clone(),
                commands,
            },
        );
        let message = format!(
            "producer {producer} attached to ingestor '{}' of domain '{}'",
            ingestor.as_str(),
            domain.as_str()
        );
        let outcome = OpenIngestorOutcome {
            disposition: OpenIngestorDisposition::Opened(Box::new(ProducerOpened {
                domain,
                ingestor,
                description,
            })),
            message,
        };
        let queued = shared
            .finish_with(request_id, ReplyBody::OpenIngestor(outcome))
            .await;
        let QueuedReply::Reply = queued else {
            // A producer whose reply was not queued was never announced, so it is let go before it
            // takes anything; dropping its route detaches it.
            self.remove(producer);
            debug!(%producer, "a producer open was not announced and was released");
            return;
        };
        let pump = ProducerPump {
            shared: shared.clone(),
            producer,
            route: Some(route),
            events,
            commands: received,
            commands_open: true,
            admission_open: true,
            credit,
            outstanding: BTreeMap::new(),
            closers: Vec::new(),
            violated: false,
            _hold: hold,
        };
        shared.service.inner.service_tasks.spawn(pump.run());
    }

    /// Validates an open against the session, the committed graph and the budgets, and attaches
    /// the producer through the node that executes its ingestor.
    async fn attach(
        &self,
        shared: &Arc<SessionShared>,
        open: OpenIngestorRequest,
        in_transaction: bool,
    ) -> Result<(OpenedRoute, ProducerHold), OpenRefusal> {
        let OpenIngestorRequest {
            domain,
            ingestor,
            expected_fields,
            limits,
        } = open;
        if in_transaction {
            return Err(OpenRefusal::new(
                ClientProducerRefusal::InTransaction,
                "producers belong to the session and cannot be opened inside a transaction"
                    .to_string(),
            ));
        }
        if !limits.is_within_bounds() {
            return Err(OpenRefusal::new(
                ClientProducerRefusal::InvalidLimits,
                format!(
                    "a producer may ask for at most {} batches and {} bytes, not {} and {}",
                    nervix_models::MAX_CLIENT_PRODUCER_BATCHES,
                    CLIENT_PRODUCER_SESSION_BYTES,
                    limits.batches,
                    limits.bytes
                ),
            ));
        }
        let service = &shared.service;
        let Some(state) = service.inner.consensus.current_domain(&domain).await else {
            return Err(OpenRefusal::new(
                ClientProducerRefusal::DomainNotFound,
                format!("domain '{}' does not exist", domain.as_str()),
            ));
        };
        if let DomainStatus::Stopped = state.status {
            return Err(OpenRefusal::new(
                ClientProducerRefusal::DomainStopped,
                format!("domain '{}' is not running", domain.as_str()),
            ));
        }
        let target = service
            .ingestor_target_from_schedule(&domain, &ingestor)
            .await;
        let Ok(Some((model, scheduled))) = target else {
            return Err(OpenRefusal::new(
                ClientProducerRefusal::IngestorNotFound,
                format!(
                    "ingestor '{}' does not exist in domain '{}'",
                    ingestor.as_str(),
                    domain.as_str()
                ),
            ));
        };
        if let IngestorInput::Transport(_) = &model.input {
            return Err(OpenRefusal::new(
                ClientProducerRefusal::NotClientIngestor,
                format!(
                    "ingestor '{}' reads {}, not client batches",
                    ingestor.as_str(),
                    model.input.source_label()
                ),
            ));
        }
        let Some(owner) = scheduled.execution_node().cloned() else {
            return Err(OpenRefusal::new(
                ClientProducerRefusal::EndpointUnavailable,
                format!(
                    "ingestor '{}' is not scheduled on any node",
                    ingestor.as_str()
                ),
            ));
        };
        let bytes = limits.bytes.get();
        if let Err(refusal) = self.reserve(bytes) {
            let message = match refusal {
                ClientProducerRefusal::TooManyProducers => format!(
                    "this session already holds {MAX_CLIENT_PRODUCERS_PER_SESSION} producers"
                ),
                _ => format!(
                    "the session's {CLIENT_PRODUCER_SESSION_BYTES} byte producer budget cannot \
                     hold {bytes} more bytes"
                ),
            };
            return Err(OpenRefusal::new(refusal, message));
        }
        let Some(node) = service
            .inner
            .runtime
            .client_producer_budget()
            .try_reserve(limits.bytes)
        else {
            self.release(bytes);
            return Err(OpenRefusal::new(
                ClientProducerRefusal::NodeCapacityExhausted,
                format!("the serving node's producer budget cannot hold {bytes} more bytes"),
            ));
        };
        let hold = ProducerHold {
            shared: shared.clone(),
            bytes,
            _node: node,
        };
        let max_batch_bytes = shared.limits().max_submitted_batch_bytes();
        let max_batch_bytes: u64 = max_batch_bytes.arch_into();
        let max_batch_bytes = NonZeroU64::new(max_batch_bytes)
            .assured("a frame limit leaves room for a batch beside the submission envelope");
        let request = ProducerOpen {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
            expected_fields,
            limits,
            max_batch_bytes,
        };
        let opened = service.inner.client_producers.open(&owner, request).await;
        match opened {
            Ok(route) => Ok((route, hold)),
            Err(refusal) => {
                let message = match refusal {
                    ClientProducerRefusal::SchemaMismatch => format!(
                        "the expected fields are not exactly the input schema of ingestor '{}'",
                        ingestor.as_str()
                    ),
                    ClientProducerRefusal::EndpointUnavailable => format!(
                        "ingestor '{}' is not running on node '{owner}' right now",
                        ingestor.as_str()
                    ),
                    ClientProducerRefusal::NodeCapacityExhausted => format!(
                        "the producer budget of node '{owner}' cannot hold {bytes} more bytes"
                    ),
                    other => format!("ingestor '{}': {}", ingestor.as_str(), other.as_ref()),
                };
                Err(OpenRefusal::new(refusal, message))
            }
        }
    }
}

impl SessionShared {
    /// Takes one batch from the client. It never waits for the batch's outcome, which the
    /// producer's pump sends.
    pub(super) async fn submit_batch(&self, request_id: RequestId, submit: SubmitBatchRequest) {
        if let Err(rejection) = self.register(request_id, InFlightKind::Submission) {
            self.reject(request_id, rejection).await;
            return;
        }
        let SubmitBatchRequest { producer, batch } = submit;
        let routing = self.producers.route_submission(producer, request_id, batch);
        let SubmissionRouting::Refused(refusal) = routing else {
            return;
        };
        let message = match refusal {
            ClientSubmissionRefusal::CreditExceeded => {
                format!("the batch exceeds the credit granted to producer {producer}")
            }
            _ => format!("this session holds no open producer {producer}"),
        };
        let outcome = SubmissionOutcome {
            outcome: ClientSubmissionOutcome::NotAdmitted(refusal),
            message,
        };
        self.finish_with(request_id, ReplyBody::Submission(outcome))
            .await;
    }

    /// Closes a producer. Its pump answers once every batch it submitted has its outcome.
    pub(super) async fn close_producer(&self, request_id: RequestId, close: CloseIngestorRequest) {
        if let Err(rejection) = self.register(request_id, InFlightKind::Request) {
            self.reject(request_id, rejection).await;
            return;
        }
        if self.producers.route_close(close.producer, request_id) {
            return;
        }
        let outcome = CloseIngestorOutcome {
            disposition: CloseIngestorDisposition::NotOpen,
            message: format!("this session holds no open producer {}", close.producer),
        };
        self.finish_with(request_id, ReplyBody::CloseIngestor(outcome))
            .await;
    }
}

/// The task that answers one producer: every batch it submitted, its admission changes, its close
/// and its end.
struct ProducerPump {
    shared: Arc<SessionShared>,
    producer: ProducerId,
    /// `None` once the producer asked to close or broke its credit; the endpoint then answers
    /// what it holds and closes the events.
    route: Option<ProducerRoute>,
    events: ClientProducerEvents,
    commands: mpsc::UnboundedReceiver<PumpCommand>,
    commands_open: bool,
    admission_open: bool,
    credit: Arc<ProducerCredit>,
    /// The batches handed to the endpoint and not answered yet, with their bytes.
    outstanding: BTreeMap<RequestId, u64>,
    /// Closes to answer once the producer is released.
    closers: Vec<RequestId>,
    /// Whether the producer broke its credit, which its end reports.
    violated: bool,
    _hold: ProducerHold,
}

/// How a producer's pump stops.
enum PumpEnd {
    /// The endpoint ended the producer.
    Ended(ClientProducerEndReason),
    /// The endpoint released the producer after a close, or after a protocol violation closed it.
    Released,
}

impl ProducerPump {
    async fn run(mut self) {
        let end = loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                biased;
                () = self.shared.ended() => {
                    // The session is gone: dropping the route detaches the producer, and admitted
                    // work continues in the graph with nobody left to answer.
                    return;
                }
                event = self.events.outcomes.recv() => match event {
                    Some(ClientProducerEvent::Outcome {
                        submission,
                        outcome,
                        detail,
                    }) => {
                        let submission = RequestId::new(submission.get());
                        self.answer(submission, outcome, detail).await;
                    }
                    Some(ClientProducerEvent::Ended(reason)) => break PumpEnd::Ended(reason),
                    None => break PumpEnd::Released,
                },
                command = self.commands.recv(), if self.commands_open => match command {
                    Some(command) => self.command(command).await,
                    None => self.commands_open = false,
                },
                changed = self.events.admission.changed(), if self.admission_open => {
                    if changed.is_err() {
                        self.admission_open = false;
                        continue;
                    }
                    let admission = *self.events.admission.borrow_and_update();
                    let frame = ProducerAdmissionChanged {
                        producer: self.producer,
                        admission,
                    }
                    .encode(self.shared.limits());
                    match frame {
                        Ok(frame) => {
                            if self.shared.send_frame(frame).await.is_err() {
                                return;
                            }
                        }
                        Err(error) => debug!(error = %error, "an admission change does not fit a frame"),
                    }
                }
            }
        };
        self.finish(end).await;
    }

    async fn command(&mut self, command: PumpCommand) {
        match command {
            PumpCommand::Submit { submission, batch } => {
                let bytes: u64 = batch.len().arch_into();
                let Some(route) = &self.route else {
                    // A batch the receive loop routed before the close that follows it in the
                    // queue: the close already stopped admission, so it is not admitted.
                    self.credit.release(bytes);
                    self.refuse(submission, ClientSubmissionRefusal::ProducerEnded)
                        .await;
                    return;
                };
                self.outstanding.insert(submission, bytes);
                route.submit(ClientSubmissionId::new(submission.get()), batch);
            }
            PumpCommand::Violation { submission } => {
                self.violated = true;
                self.refuse(submission, ClientSubmissionRefusal::CreditExceeded)
                    .await;
                if let Some(route) = self.route.take() {
                    route.close();
                }
            }
            PumpCommand::Close { request } => {
                self.closers.push(request);
                if let Some(route) = self.route.take() {
                    route.close();
                }
            }
        }
    }

    /// Answers a batch the endpoint answered, returning its credit first.
    async fn answer(
        &mut self,
        submission: RequestId,
        outcome: ClientSubmissionOutcome,
        detail: Option<String>,
    ) {
        let Some(bytes) = self.outstanding.remove(&submission) else {
            return;
        };
        self.credit.release(bytes);
        let outcome = SubmissionOutcome {
            outcome,
            message: detail.unwrap_or_default(),
        };
        self.shared
            .finish_with(submission, ReplyBody::Submission(outcome))
            .await;
    }

    /// Answers a batch that never reached the endpoint.
    async fn refuse(&self, submission: RequestId, refusal: ClientSubmissionRefusal) {
        let message = match refusal {
            ClientSubmissionRefusal::CreditExceeded => format!(
                "the batch exceeds the credit granted to producer {}, which ends it",
                self.producer
            ),
            _ => format!("producer {} is closing", self.producer),
        };
        let outcome = SubmissionOutcome {
            outcome: ClientSubmissionOutcome::NotAdmitted(refusal),
            message,
        };
        self.shared
            .finish_with(submission, ReplyBody::Submission(outcome))
            .await;
    }

    /// Answers everything the producer still holds and reports how it ended.
    async fn finish(mut self, end: PumpEnd) {
        self.shared.producers.remove(self.producer);
        self.route = None;
        self.commands.close();
        // Batches the receive loop routed before the producer left the session's table never
        // reached the endpoint.
        while let Ok(command) = self.commands.try_recv() {
            match command {
                PumpCommand::Submit { submission, batch } => {
                    let bytes: u64 = batch.len().arch_into();
                    self.credit.release(bytes);
                    self.refuse(submission, ClientSubmissionRefusal::ProducerEnded)
                        .await;
                }
                PumpCommand::Violation { submission } => {
                    self.violated = true;
                    self.refuse(submission, ClientSubmissionRefusal::CreditExceeded)
                        .await;
                }
                PumpCommand::Close { request } => self.closers.push(request),
            }
        }
        let (reason, cause) = match end {
            PumpEnd::Ended(reason) => {
                let cause = match reason {
                    ClientProducerEndReason::OwnerLost => ClientOutcomeUncertainty::OwnerLost,
                    _ => ClientOutcomeUncertainty::Interrupted,
                };
                (Some(reason), cause)
            }
            PumpEnd::Released if self.violated => (
                Some(ClientProducerEndReason::ProtocolViolated),
                ClientOutcomeUncertainty::Interrupted,
            ),
            PumpEnd::Released if self.closers.is_empty() => {
                // The endpoint let the producer go without a close or an end, which only a lost
                // owner does.
                (
                    Some(ClientProducerEndReason::OwnerLost),
                    ClientOutcomeUncertainty::OwnerLost,
                )
            }
            PumpEnd::Released => (None, ClientOutcomeUncertainty::Interrupted),
        };
        let outstanding = std::mem::take(&mut self.outstanding);
        for (submission, bytes) in outstanding {
            tokio::task::consume_budget().await;
            self.credit.release(bytes);
            let outcome = SubmissionOutcome {
                outcome: ClientSubmissionOutcome::OutcomeUnknown(cause),
                message: format!(
                    "producer {} ended before the batch's outcome was established",
                    self.producer
                ),
            };
            self.shared
                .finish_with(submission, ReplyBody::Submission(outcome))
                .await;
        }
        let closers = std::mem::take(&mut self.closers);
        for request in closers {
            let outcome = CloseIngestorOutcome {
                disposition: CloseIngestorDisposition::Closed,
                message: format!("producer {} closed", self.producer),
            };
            self.shared
                .finish_with(request, ReplyBody::CloseIngestor(outcome))
                .await;
        }
        let Some(reason) = reason else {
            debug!(producer = %self.producer, "a producer closed");
            return;
        };
        debug!(
            producer = %self.producer,
            reason = reason.as_ref(),
            "a producer ended"
        );
        let ended = ProducerEnded {
            producer: self.producer,
            reason,
            message: format!("producer {} ended: {}", self.producer, reason.as_ref()),
        };
        match ended.encode(self.shared.limits()) {
            Ok(frame) => {
                if self.shared.send_frame(frame).await.is_err() {
                    debug!(producer = %self.producer, "the session ended before a producer's end");
                }
            }
            Err(error) => debug!(error = %error, "a producer end does not fit a frame"),
        }
    }
}
