//! One client session, whatever transport carries its frames.
//!
//! Layer: edges.
//!
//! - **Owns.** Correlating every reply with the request it answers, running the requests that
//!   change a session in the order they were written while completion, domain, inspection and
//!   cancellation requests proceed beside them, deciding each cancellation against its request's
//!   admission, refusing what a session cannot serve with typed rejections, encoding replies and
//!   transferring the ones larger than a frame, and the unsolicited events a session receives.
//! - **Depends on.** The client wire contract, the command pipeline and the control-plane use
//!   cases behind it, and the execution classes large replies are encoded under.
//! - **Must not know.** How a transport frames, authenticates or closes a session.
//!
//! A request is served on one of two lanes. Commands, attachments and subscription changes all
//! change the session, so they run one at a time in the order the client wrote them. Everything
//! else only reads it, from the view the ordered lane last published, and runs beside them, so a
//! long command never delays a completion, an inspection, a domain request or a cancellation.
//!
//! Every request has exactly one terminal reply. Whoever takes the request's in-flight entry owes
//! it: the lane that served it, or a cancellation. A cancellation decides against admission
//! atomically: before admission the request never begins any effect, and after admission its
//! effects continue and only the wait for them ends. When the session ends, no reply follows for a
//! request still in flight, and a request not yet admitted never begins.

pub(in crate::application) mod admission;
mod events;
pub(in crate::application) mod grpc;
mod outcome;
mod upload;
pub(in crate::application) mod websocket;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use error_stack::Report;
use futures_util::{Stream, StreamExt as _};
use nervix_client_wire::{
    AttachTransactionRequest, CancelOutcome, CancelRequest, CancelState, CancellationStage,
    ClientFrame, ClientMessage, ClientRequest, CommandRequest, DomainList, DomainSelection,
    EncodedFrame, InspectTransactionRequest, InspectionOutcome, Reply, ReplyBody, ReplyDelivery,
    RequestCancelled, RequestId, RequestRejected, RequestRejection, SelectDomainRequest,
    ServerFrame, SessionEndReason, SessionEnding, SessionLimits, SubscribeDisposition,
    SubscribeOutcome, SubscribeRequest, SubscriptionType, SuggestRequest, UnsubscribeDisposition,
    UnsubscribeOutcome, UnsubscribeRequest, VerifiedFrame, WireDecodeError, WireEncodeError,
};
use nervix_execution::{AdmissionError, CpuClass, ExecutionError, MemoryClass};
use nervix_models::{
    CreateSubscription, DeleteSubscription, DomainName, TransactionInspectionRequest, UserName,
};
use nervix_nspl::{
    client_statement::{ClientStatement, ParsedClientStatement, parse_client_statement_sources},
    schema::ParseFromSourceError,
};
use nervix_recovery::{Discarded as _, NoReceiver as _};
use parking_lot::{Mutex, RwLock};
use tokio::{
    sync::{mpsc, watch},
    task::AbortHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use triomphe::Arc;

use self::{
    admission::{CancelledBeforeAdmission, CancelledStage, RequestAdmission},
    outcome::{attach_outcome, command_outcome, leader_redirect, wire_diagnostics},
};
use super::{
    command_result::CommandResult,
    model_mutation::command_error,
    session_service::{SessionServiceImpl, error_response},
    subscription::{
        OpenedSubscription, SessionDelivery, SessionOutbound, SessionSubscriptions, SessionView,
    },
    transaction::TransactionInspectionOutcome,
};

/// How many requests one session may have in flight. A request beyond it is refused rather than
/// queued, so a client flooding one session cannot grow what the server holds for it. It is also
/// what bounds the queue of ordered requests waiting for the lane.
const MAX_IN_FLIGHT_REQUESTS: usize = 64;

/// How many frames a session queues for its transport before whatever produces them waits. A frame
/// is at most the session frame limit, so this bounds the bytes a session holds for a client that
/// reads slowly; a transfer's parts wait for room one at a time.
pub(in crate::application) const SESSION_OUTBOUND_CAPACITY: usize = 16;

/// The transport a session arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum SessionTransport {
    /// A native client over gRPC, served by any node.
    Grpc,
    /// The web console over its WebSocket, served only by the leader.
    Console,
}

/// What a transport hands its session.
pub(in crate::application) enum InboundFrame {
    Frame(VerifiedFrame<ClientFrame>),
    /// The client ended its side of the session cleanly.
    Closed,
    /// The transport failed. The session ends without a clean close.
    Failed,
}

/// A request that changes the session, served in the order the client wrote it.
enum OrderedRequest {
    Command(CommandRequest),
    Attach(AttachTransactionRequest),
    Subscribe(SubscribeRequest),
    Unsubscribe(UnsubscribeRequest),
}

/// A request that only reads the session, served beside the ordered lane.
enum ConcurrentRequest {
    Suggest(SuggestRequest),
    ListDomains,
    SelectDomain(SelectDomainRequest),
    Inspect(InspectTransactionRequest),
}

/// The lane a request is served on.
enum RoutedRequest {
    Ordered(OrderedRequest),
    Concurrent(ConcurrentRequest),
}

/// An ordered request and the admission decision it is served under.
struct OrderedWork {
    request_id: RequestId,
    admission: Arc<RequestAdmission>,
    request: OrderedRequest,
}

/// One request of the session still owed its terminal reply.
struct InFlight {
    admission: Arc<RequestAdmission>,
    /// The task serving a request that runs beside the ordered lane. An ordered request has none.
    task: Option<AbortHandle>,
}

/// The entry a cancellation reads of the request it targets.
struct CancelTarget {
    admission: Arc<RequestAdmission>,
    task: Option<AbortHandle>,
}

/// Why a reply could not be encoded.
enum ReplyEncodingFailure {
    /// The reply does not fit the session limits.
    Encoding(Report<WireEncodeError>),
    /// The bulk memory a large reply is encoded under could not take the job.
    Memory(Report<AdmissionError>),
    /// The bulk workers could not take the job.
    Workers(Report<ExecutionError>),
}

impl ReplyEncodingFailure {
    /// The rejection that answers the request instead.
    fn rejection(self) -> RequestRejected {
        match self {
            Self::Encoding(error) => RequestRejected {
                rejection: RequestRejection::ReplyTooLarge,
                field: None,
                message: error.current_context().to_string(),
            },
            Self::Memory(error) => RequestRejected {
                rejection: RequestRejection::ServerBusy,
                field: None,
                message: format!(
                    "the server had no memory left to prepare the reply: {}",
                    error.current_context()
                ),
            },
            Self::Workers(error) => RequestRejected {
                rejection: RequestRejection::ServerBusy,
                field: None,
                message: format!(
                    "the server had no worker left to prepare the reply: {}",
                    error.current_context()
                ),
            },
        }
    }
}

/// Everything the tasks of one session share.
pub(super) struct SessionShared {
    service: SessionServiceImpl,
    transport: SessionTransport,
    delivery: SessionDelivery,
    in_flight: Mutex<BTreeMap<RequestId, InFlight>>,
    /// The session as the ordered lane last left it, for the requests that run beside it.
    view: RwLock<SessionView>,
    /// The domain whose observations the session receives.
    selection: watch::Sender<Option<DomainName>>,
    /// Cancelled when the session ends, whatever ended it.
    ended: CancellationToken,
}

impl SessionShared {
    fn limits(&self) -> &SessionLimits {
        &self.delivery.limits
    }

    /// Queues one frame for the client. An error means the transport stopped taking frames.
    async fn send_frame(
        &self,
        frame: EncodedFrame<ServerFrame>,
    ) -> Result<(), mpsc::error::SendError<EncodedFrame<ServerFrame>>> {
        self.delivery.outbound.send(frame).await
    }

    /// Takes the request's in-flight entry. `true` means the caller owes it its terminal reply.
    fn finish(&self, request_id: RequestId) -> bool {
        self.in_flight.lock().remove(&request_id).is_some()
    }

    /// Sends the terminal reply of a request whose entry the caller still has to take. `false`
    /// means another path already answered it, or the session ended.
    async fn finish_with(&self, request_id: RequestId, body: ReplyBody) -> bool {
        if !self.finish(request_id) {
            return false;
        }
        self.reply(request_id, body).await;
        true
    }

    /// Encodes and queues one reply, splitting it into transfer parts when it is larger than a
    /// frame. A reply that cannot be encoded is replaced by a typed rejection, never truncated.
    async fn reply(&self, request_id: RequestId, body: ReplyBody) {
        let reply = Reply { request_id, body };
        let delivery = match self.encode_reply(reply).await {
            Ok(delivery) => delivery,
            Err(failure) => {
                let rejection = failure.rejection();
                debug!(
                    %request_id,
                    message = rejection.message,
                    "a session reply was replaced by a rejection"
                );
                let rejected = Reply {
                    request_id,
                    body: ReplyBody::Rejected(rejection),
                };
                match rejected.encode(self.limits()) {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        warn!(%request_id, error = %error, "a session rejection does not fit a frame");
                        return;
                    }
                }
            }
        };
        match delivery {
            ReplyDelivery::Frame(frame) => {
                self.send_frame(frame)
                    .await
                    .means_peer_left("the session's client");
            }
            ReplyDelivery::Transfer(parts) => {
                for part in parts {
                    tokio::task::consume_budget().await;
                    if self.send_frame(part).await.is_err() {
                        debug!(%request_id, "the session's client left during a reply transfer");
                        return;
                    }
                }
            }
        }
    }

    /// Encodes a reply. A reply carrying an impact report can be large, so it is encoded on the
    /// bulk workers under the bulk memory class; every other reply is small and encoded in place.
    async fn encode_reply(&self, reply: Reply) -> Result<ReplyDelivery, ReplyEncodingFailure> {
        let carries_report = match &reply.body {
            ReplyBody::Inspection(InspectionOutcome::Inspected(_)) => true,
            ReplyBody::Command(outcome) => outcome.inspection.is_some(),
            _ => false,
        };
        if !carries_report {
            return reply
                .encode(self.limits())
                .map_err(ReplyEncodingFailure::Encoding);
        }
        let executor = self.service.inner.runtime.executor();
        // The charge admits the job onto the bulk workers. The reply it produces is bounded by the
        // session transfer limit, which the encoder enforces as the reply grows.
        let chunk_bytes = executor.limits().bulk_chunk_bytes.as_u64();
        let reservation = executor
            .reserve(MemoryClass::Bulk, chunk_bytes)
            .await
            .map_err(ReplyEncodingFailure::Memory)?;
        let limits = *self.limits();
        let encoded = executor
            .run_cpu(
                CpuClass::Bulk,
                reservation,
                move |_charge, _cancellation| reply.encode(&limits),
            )
            .await
            .map_err(ReplyEncodingFailure::Workers)?;
        encoded.map_err(ReplyEncodingFailure::Encoding)
    }

    /// Answers a request that was not served.
    async fn reject(&self, request_id: RequestId, rejection: RequestRejected) {
        self.reply(request_id, ReplyBody::Rejected(rejection)).await;
    }

    /// Tells the client why the session ends, and ends it.
    async fn end(&self, reason: SessionEndReason) {
        let ending = SessionEnding { reason }.encode(self.limits());
        match ending {
            Ok(frame) => {
                self.send_frame(frame)
                    .await
                    .means_peer_left("the session's client");
            }
            Err(error) => warn!(error = %error, "a session ending does not fit a frame"),
        }
        self.ended.cancel();
    }

    fn publish_view(&self, subscriptions: &SessionSubscriptions) {
        *self.view.write() = subscriptions.view();
    }

    /// Registers a request that is to be served, unless the session is at its limit or the
    /// request's identity is already in flight.
    fn register(&self, request_id: RequestId) -> Result<Arc<RequestAdmission>, RequestRejected> {
        let mut in_flight = self.in_flight.lock();
        if in_flight.contains_key(&request_id) {
            return Err(RequestRejected {
                rejection: RequestRejection::DuplicateRequestId,
                field: Some("ClientMessage.request_id".to_string()),
                message: format!("request {request_id} is already in flight in this session"),
            });
        }
        if in_flight.len() >= MAX_IN_FLIGHT_REQUESTS {
            return Err(RequestRejected {
                rejection: RequestRejection::TooManyRequestsInFlight,
                field: None,
                message: format!(
                    "the session already has {MAX_IN_FLIGHT_REQUESTS} requests in flight; send \
                     this one again once an earlier one is answered"
                ),
            });
        }
        let admission = Arc::new(RequestAdmission::default());
        in_flight.insert(
            request_id,
            InFlight {
                admission: admission.clone(),
                task: None,
            },
        );
        Ok(admission)
    }

    /// Records the task serving a request that runs beside the ordered lane, so a cancellation
    /// can stop it. A request answered before this runs has no entry left to record it in.
    fn attach_task(&self, request_id: RequestId, task: AbortHandle) {
        if let Some(entry) = self.in_flight.lock().get_mut(&request_id) {
            entry.task = Some(task);
        }
    }

    fn cancel_target(&self, target: RequestId) -> Option<CancelTarget> {
        let in_flight = self.in_flight.lock();
        let entry = in_flight.get(&target)?;
        Some(CancelTarget {
            admission: entry.admission.clone(),
            task: entry.task.clone(),
        })
    }

    /// Serves a cancel request. The cancel itself is answered first; the target's own terminal
    /// reply follows it, unless the target was answered in the meantime.
    async fn cancel(&self, request_id: RequestId, cancel: CancelRequest) {
        if self.in_flight.lock().contains_key(&request_id) {
            let rejection = RequestRejected {
                rejection: RequestRejection::DuplicateRequestId,
                field: Some("ClientMessage.request_id".to_string()),
                message: format!("request {request_id} is already in flight in this session"),
            };
            self.reject(request_id, rejection).await;
            return;
        }
        let target = cancel.target;
        let Some(entry) = self.cancel_target(target) else {
            let outcome = CancelOutcome {
                target,
                state: CancelState::NotInFlight,
            };
            self.reply(request_id, ReplyBody::Cancel(outcome)).await;
            return;
        };
        let stage = entry.admission.cancel();
        let outcome = CancelOutcome {
            target,
            state: CancelState::Requested,
        };
        self.reply(request_id, ReplyBody::Cancel(outcome)).await;
        let stage = match stage {
            CancelledStage::BeforeAdmission => {
                if let Some(task) = entry.task {
                    task.abort();
                }
                CancellationStage::BeforeAdmission
            }
            CancelledStage::AfterAdmission => CancellationStage::AfterAdmission,
        };
        let cancelled = ReplyBody::Cancelled(RequestCancelled { stage });
        self.finish_with(target, cancelled).await;
    }

    /// Ends every request still in flight when the session ends. A request not yet admitted is
    /// decided cancelled, so it never begins an effect after its client is gone; an admitted one
    /// runs to its end without a reply.
    fn abandon_in_flight(&self) {
        let abandoned = std::mem::take(&mut *self.in_flight.lock());
        for entry in abandoned.into_values() {
            let stage = entry.admission.cancel();
            if let CancelledStage::BeforeAdmission = stage
                && let Some(task) = entry.task
            {
                task.abort();
            }
        }
    }
}

/// How a request that cannot be decoded is refused.
fn decode_rejection(error: &Report<WireDecodeError>) -> RequestRejected {
    let context = error.current_context();
    let (rejection, field) = match context {
        WireDecodeError::UnknownUnionVariant {
            field: "ClientMessage.request",
            ..
        } => (
            RequestRejection::UnsupportedRequest,
            "ClientMessage.request",
        ),
        WireDecodeError::UnknownEnumValue { field, .. } => {
            (RequestRejection::UnsupportedValue, *field)
        }
        WireDecodeError::UnknownUnionVariant { field, .. }
        | WireDecodeError::MissingField { field }
        | WireDecodeError::ZeroValue { field }
        | WireDecodeError::EmptyCollection { field }
        | WireDecodeError::TooManyEntries { field, .. }
        | WireDecodeError::StringTooLong { field, .. }
        | WireDecodeError::OutOfRange { field, .. }
        | WireDecodeError::InvalidValue { field, .. }
        | WireDecodeError::NonCanonicalSet { field } => (RequestRejection::InvalidRequest, *field),
        WireDecodeError::UnexpectedMessage { .. } => {
            (RequestRejection::InvalidRequest, "ClientMessage")
        }
    };
    RequestRejected {
        rejection,
        field: Some(field.to_string()),
        message: context.to_string(),
    }
}

impl SessionServiceImpl {
    /// Serves one session until its client leaves, its transport fails, or the node ends it.
    ///
    /// The transport hands its frames in through `inbound` and receives every frame the session
    /// sends through `outbound`. When this returns, the session's subscriptions are stopped, its
    /// transaction binding is released or cleanly closed, and nothing sends on `outbound` any
    /// more that matters to the client.
    pub(in crate::application) async fn run_session<S>(
        &self,
        user: UserName,
        transport: SessionTransport,
        limits: SessionLimits,
        mut inbound: S,
        outbound: SessionOutbound,
    ) where
        S: Stream<Item = InboundFrame> + Unpin + Send,
    {
        let subscriptions = SessionSubscriptions::for_user(user);
        let (selection, _) = watch::channel(None);
        let shared = Arc::new(SessionShared {
            service: self.clone(),
            transport,
            delivery: SessionDelivery { outbound, limits },
            in_flight: Mutex::new(BTreeMap::new()),
            view: RwLock::new(subscriptions.view()),
            selection,
            ended: CancellationToken::new(),
        });
        // Every queued request is registered in flight first, so the queue holds at most
        // `MAX_IN_FLIGHT_REQUESTS` requests even though the channel itself is unbounded.
        let (ordered_tx, ordered_rx) = mpsc::unbounded_channel();
        let lane = self.inner.service_tasks.spawn(run_ordered_lane(
            shared.clone(),
            ordered_rx,
            subscriptions,
        ));
        let events = self
            .inner
            .service_tasks
            .spawn(events::run_session_events(shared.clone()));

        let mut closed_cleanly = false;
        loop {
            tokio::task::consume_budget().await;
            let item = tokio::select! {
                biased;
                _ = shared.ended.cancelled() => break,
                _ = self.inner.admission_shutdown.cancelled() => {
                    shared.end(SessionEndReason::ServerShuttingDown).await;
                    break;
                }
                item = inbound.next() => item,
            };
            match item {
                Some(InboundFrame::Frame(frame)) => {
                    let accepted = accept_frame(&shared, &ordered_tx, frame).await;
                    if !accepted {
                        break;
                    }
                }
                Some(InboundFrame::Closed) | None => {
                    closed_cleanly = shared.in_flight.lock().is_empty();
                    break;
                }
                Some(InboundFrame::Failed) => break,
            }
        }

        shared.ended.cancel();
        shared.abandon_in_flight();
        drop(ordered_tx);
        events.abort();
        let finished = lane.await;
        let mut subscriptions = match finished {
            Ok(Some(subscriptions)) => subscriptions,
            // The lane ends without its state only when shutdown cancelled it at the deadline or
            // it panicked; either way the session has nothing left to clean up.
            Ok(None) => return,
            Err(error) => {
                warn!(error = %error, "a session's ordered lane failed");
                return;
            }
        };
        subscriptions.stop_all(self).await;
        if closed_cleanly {
            self.clean_close_transaction(&mut subscriptions).await;
        } else {
            self.release_session_transaction_binding(&mut subscriptions);
        }
    }
}

/// Takes one frame from the client. `false` ends the session.
async fn accept_frame(
    shared: &Arc<SessionShared>,
    ordered: &mpsc::UnboundedSender<OrderedWork>,
    frame: VerifiedFrame<ClientFrame>,
) -> bool {
    let message = match ClientMessage::decode(&frame) {
        Ok(message) => message,
        Err(error) => {
            let Some(request_id) = frame.request_id() else {
                // Without a request identity there is no request to refuse, so the frame breaks
                // the protocol itself.
                let reason = SessionEndReason::ProtocolViolated {
                    message: format!("a request without a valid identity: {error}"),
                };
                shared.end(reason).await;
                return false;
            };
            shared.reject(request_id, decode_rejection(&error)).await;
            return true;
        }
    };
    let ClientMessage {
        request_id,
        request,
    } = message;
    let routed = match request {
        ClientRequest::Cancel(cancel) => {
            shared.cancel(request_id, cancel).await;
            return true;
        }
        ClientRequest::Command(command) => RoutedRequest::Ordered(OrderedRequest::Command(command)),
        ClientRequest::AttachTransaction(attach) => {
            RoutedRequest::Ordered(OrderedRequest::Attach(attach))
        }
        ClientRequest::Subscribe(subscribe) => {
            RoutedRequest::Ordered(OrderedRequest::Subscribe(subscribe))
        }
        ClientRequest::Unsubscribe(unsubscribe) => {
            RoutedRequest::Ordered(OrderedRequest::Unsubscribe(unsubscribe))
        }
        ClientRequest::Suggest(suggest) => {
            RoutedRequest::Concurrent(ConcurrentRequest::Suggest(suggest))
        }
        ClientRequest::ListDomains => RoutedRequest::Concurrent(ConcurrentRequest::ListDomains),
        ClientRequest::SelectDomain(select) => {
            RoutedRequest::Concurrent(ConcurrentRequest::SelectDomain(select))
        }
        ClientRequest::InspectTransaction(inspect) => {
            RoutedRequest::Concurrent(ConcurrentRequest::Inspect(inspect))
        }
    };
    let admission = match shared.register(request_id) {
        Ok(admission) => admission,
        Err(rejection) => {
            shared.reject(request_id, rejection).await;
            return true;
        }
    };
    match routed {
        RoutedRequest::Ordered(request) => {
            let work = OrderedWork {
                request_id,
                admission,
                request,
            };
            if ordered.send(work).is_err() {
                // The lane stops before the session only when shutdown cancelled it at the
                // deadline, which leaves nothing to serve the session's changes.
                debug!("a session's ordered lane stopped before the session");
                return false;
            }
        }
        RoutedRequest::Concurrent(request) => {
            let task = shared.service.inner.service_tasks.spawn(serve_concurrent(
                shared.clone(),
                request_id,
                request,
            ));
            shared.attach_task(request_id, task.abort_handle());
        }
    }
    true
}

/// Serves a request that only reads the session, beside the ordered lane.
async fn serve_concurrent(
    shared: Arc<SessionShared>,
    request_id: RequestId,
    request: ConcurrentRequest,
) {
    let service = &shared.service;
    let body = match request {
        ConcurrentRequest::Suggest(suggest) => {
            let view = shared.view.read().clone();
            let outcome = service.process_suggest(suggest, &view).await;
            ReplyBody::Suggest(outcome)
        }
        ConcurrentRequest::ListDomains => {
            let domains = service.domain_infos().await;
            ReplyBody::DomainList(DomainList { domains })
        }
        ConcurrentRequest::SelectDomain(select) => {
            let selection = select_domain(&shared, select).await;
            ReplyBody::DomainSelection(selection)
        }
        ConcurrentRequest::Inspect(inspect) => {
            let outcome = inspect_transaction(&shared, inspect).await;
            ReplyBody::Inspection(outcome)
        }
    };
    shared.finish_with(request_id, body).await;
}

/// Selects the domain whose observations the session receives.
async fn select_domain(shared: &SessionShared, select: SelectDomainRequest) -> DomainSelection {
    let domain = select.domain;
    let existing = shared.service.inner.consensus.current_domain(&domain).await;
    if existing.is_none() {
        return DomainSelection::NotFound(domain);
    }
    shared.selection.send_replace(Some(domain.clone()));
    DomainSelection::Selected(domain)
}

async fn inspect_transaction(
    shared: &SessionShared,
    inspect: InspectTransactionRequest,
) -> InspectionOutcome {
    let view = shared.view.read().clone();
    let request = TransactionInspectionRequest {
        target: inspect.target,
        operation: inspect.operation,
    };
    let outcome = shared
        .service
        .inspect_transaction(&request, view.inspecting())
        .await;
    match outcome {
        TransactionInspectionOutcome::Inspected(inspection) => {
            InspectionOutcome::Inspected(inspection)
        }
        TransactionInspectionOutcome::Rejected { rejection, message } => {
            InspectionOutcome::Rejected { rejection, message }
        }
        TransactionInspectionOutcome::NotLeader { leader } => {
            let redirect = shared.service.redirect_to_leader(leader).await;
            InspectionOutcome::NotLeader(leader_redirect(redirect))
        }
    }
}

/// Serves the session's ordered requests one at a time, in the order they were written, and hands
/// back the session's state once the session stops sending and the last request is served.
async fn run_ordered_lane(
    shared: Arc<SessionShared>,
    mut work: mpsc::UnboundedReceiver<OrderedWork>,
    mut subscriptions: SessionSubscriptions,
) -> SessionSubscriptions {
    while let Some(item) = work.recv().await {
        tokio::task::consume_budget().await;
        // A request cancelled while it waited was already answered by its cancellation.
        if item.admission.is_cancelled() {
            continue;
        }
        serve_ordered(&shared, item, &mut subscriptions).await;
        shared.publish_view(&subscriptions);
    }
    subscriptions
}

async fn serve_ordered(
    shared: &SessionShared,
    item: OrderedWork,
    subscriptions: &mut SessionSubscriptions,
) {
    let OrderedWork {
        request_id,
        admission,
        request,
    } = item;
    match request {
        OrderedRequest::Command(command) => {
            serve_command(shared, request_id, &admission, command, subscriptions).await;
        }
        OrderedRequest::Attach(attach) => {
            if admission.admit().is_err() {
                return;
            }
            let attachment = shared
                .service
                .attach_transaction(attach.transaction_id, subscriptions)
                .await;
            let body = ReplyBody::Attach(attach_outcome(attachment));
            shared.finish_with(request_id, body).await;
        }
        OrderedRequest::Subscribe(subscribe) => {
            if admission.admit().is_err() {
                return;
            }
            serve_subscribe(shared, request_id, subscribe, subscriptions).await;
        }
        OrderedRequest::Unsubscribe(unsubscribe) => {
            if admission.admit().is_err() {
                return;
            }
            let deleted = if subscriptions.transaction_active() {
                Err(Box::new(subscription_in_transaction()))
            } else {
                let delete = DeleteSubscription {
                    name: unsubscribe.subscription,
                };
                shared
                    .service
                    .delete_subscription(delete, subscriptions)
                    .await
            };
            let outcome = match deleted {
                Ok(deleted) => UnsubscribeOutcome {
                    disposition: UnsubscribeDisposition::Deleted(deleted.handle),
                    message: deleted.message,
                    diagnostics: Vec::new(),
                },
                Err(result) => UnsubscribeOutcome {
                    disposition: UnsubscribeDisposition::Failed,
                    message: result.message,
                    diagnostics: wire_diagnostics(result.diagnostics),
                },
            };
            shared
                .finish_with(request_id, ReplyBody::Unsubscribe(outcome))
                .await;
        }
    }
}

/// Serves one command. A cancellation that wins before the command's admission stops the wait
/// for it at once, and the command never begins an effect.
async fn serve_command(
    shared: &SessionShared,
    request_id: RequestId,
    admission: &RequestAdmission,
    command: CommandRequest,
    subscriptions: &mut SessionSubscriptions,
) {
    let execution_reference = command.execution_reference.clone();
    let processing = shared
        .service
        .process_command(command, subscriptions, admission);
    let processed = tokio::select! {
        biased;
        processed = processing => processed,
        _ = admission.cancelled_before_admission() => Err(CancelledBeforeAdmission),
    };
    let Ok(response) = processed else {
        return;
    };
    let outcome = command_outcome(execution_reference, response);
    shared
        .finish_with(request_id, ReplyBody::Command(Box::new(outcome)))
        .await;
}

async fn serve_subscribe(
    shared: &SessionShared,
    request_id: RequestId,
    subscribe: SubscribeRequest,
    subscriptions: &mut SessionSubscriptions,
) {
    let opened = open_subscription(shared, subscribe, subscriptions).await;
    let OpenedSubscription {
        opened,
        message,
        release,
    } = match opened {
        Ok(opened) => opened,
        Err(result) => {
            let outcome = SubscribeOutcome {
                disposition: SubscribeDisposition::Failed,
                message: result.message,
                diagnostics: wire_diagnostics(result.diagnostics),
            };
            shared
                .finish_with(request_id, ReplyBody::Subscribe(outcome))
                .await;
            return;
        }
    };
    let outcome = SubscribeOutcome {
        disposition: SubscribeDisposition::Opened(Box::new(opened)),
        message,
        diagnostics: Vec::new(),
    };
    // The rows follow the reply that announces their schema. A subscription whose reply was not
    // sent was never announced, so it stays gated and delivers nothing until the session ends.
    let announced = shared
        .finish_with(request_id, ReplyBody::Subscribe(outcome))
        .await;
    if announced {
        release
            .send(())
            .discarded("a subscription whose task already ended has no rows to release");
    }
}

/// Opens the subscription a subscribe request describes: exactly one `CREATE SUBSCRIPTION`
/// statement in an existing domain, delivering typed rows.
async fn open_subscription(
    shared: &SessionShared,
    subscribe: SubscribeRequest,
    subscriptions: &mut SessionSubscriptions,
) -> Result<OpenedSubscription, Box<CommandResult>> {
    let SubscribeRequest {
        domain,
        statement,
        subscription_type,
    } = subscribe;
    // Rows are the only subscription type a session delivers.
    let SubscriptionType::Row = subscription_type;
    if subscriptions.transaction_active() {
        return Err(Box::new(subscription_in_transaction()));
    }
    let statements = match parse_client_statement_sources(&statement) {
        Ok(statements) => statements,
        Err(ParseFromSourceError::Lex { diagnostics, .. }) => {
            return Err(Box::new(error_response("lex error", &diagnostics)));
        }
        Err(ParseFromSourceError::Parse { diagnostics, .. }) => {
            return Err(Box::new(error_response("parse error", &diagnostics)));
        }
    };
    let Some(subscription) = single_create_subscription(statements) else {
        return Err(Box::new(command_error(
            "a subscribe request carries exactly one CREATE SUBSCRIPTION statement".to_string(),
        )));
    };
    let existing = shared.service.inner.consensus.current_domain(&domain).await;
    if existing.is_none() {
        return Err(Box::new(command_error(format!(
            "domain '{}' does not exist",
            domain.as_str()
        ))));
    }
    shared
        .service
        .create_subscription(&domain, subscription, &shared.delivery, subscriptions)
        .await
}

/// The refusal of a subscription change while the session holds an active transaction. A
/// subscription belongs to the session, not to the transaction, so it is sent separately.
fn subscription_in_transaction() -> CommandResult {
    command_error(
        "session-scoped and client-local statements cannot be queued in a transaction".to_string(),
    )
}

/// The subscription of a statement list that holds exactly one `CREATE SUBSCRIPTION`.
fn single_create_subscription(
    statements: Vec<ParsedClientStatement>,
) -> Option<CreateSubscription> {
    let mut statements = statements.into_iter();
    let first = statements.next()?;
    if statements.next().is_some() {
        return None;
    }
    match first.statement {
        ClientStatement::CreateSubscription(subscription) => Some(subscription),
        _ => None,
    }
}
