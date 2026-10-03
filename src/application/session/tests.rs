//! Session engine tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that a reply larger than a frame arrives whole as transfer parts, that a
//!   reply larger than the transfer limit is refused whole, that a cancellation of a request that
//!   is not in flight says so, that registration refuses a duplicate or excess request rather
//!   than queueing it while a submitted batch stays outside the in-flight limit, that a subscription statement the parser rejects is refused with the
//!   stage and the diagnostic located in that statement, and that a domain clock attachment
//!   delivers its changes between its replies and ends when its domain leaves the node. An attach
//!   is answered only once its node has installed the committed domains, and the wait for them
//!   ends with the session.
//! - **Depends on.** The session engine and the session test fixtures.
//! - **Must not know.** Production ownership beyond the parent module under test.

use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    time::Duration,
};

use arch_into::ArchInto as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    AttachDomainClockRequest, CancelRequest, ClientFrame, ClientMessage, ClientRequest,
    CommandDisposition as WireCommandDisposition, CommandRequest, DetachDomainClockRequest,
    DomainClockAttachDisposition, DomainClockAttachmentEndReason, DomainClockDetachDisposition,
    EmitterOpenRefusal, LeaderRedirect as WireLeaderRedirect, OpenEmitterDisposition,
    OpenEmitterRequest, OpenIngestorDisposition, OpenIngestorRequest, ReplyBody, RequestId,
    RequestRejection, ServerEvent, ServerFrame, ServerMessage, SessionLimitSettings, SessionLimits,
    SubscribeDisposition, SubscribeRequest, SubscriptionType, TransferAssembly, VerifiedFrame,
};
use nervix_models::{
    ClientConsumerLimits, ClientProducerLimits, ClientProducerRefusal, DomainClockObservation,
    DomainClockObservedState, DomainClockState, DomainConfig, DomainName, DomainPace,
    DomainStartPoint, DomainState, DomainStatus, DomainTimeRate, PacedDomainClock, ParseAsType,
    PlacementPolicy, SchemaField, Timestamp, TransactionPosition, UserName,
};
use nervix_primitives::{
    stream::wrappers::UnboundedReceiverStream,
    sync::{CancellationToken, mpsc},
    task::JoinHandle,
};

use super::{
    InFlightKind, InFlightRequests, InboundFrame, MAX_IN_FLIGHT_REQUESTS, SessionShared,
    SessionTransport, outbound, producers::SessionProducers,
};
use crate::application::{
    command_result::CommandDisposition,
    session_service::SessionServiceImpl,
    subscription::{SessionDelivery, SessionSubscriptions},
    test_fixtures::{TestService, build_test_service, named, test_command_request},
};

const REPLY_TIMEOUT: Duration = Duration::from_secs(30);

fn request_id(id: u64) -> RequestId {
    RequestId::new(NonZeroU64::new(id).assured("test request identities are non-zero"))
}

/// Limits with the smallest frame a session accepts, `transfer_bytes` for a whole reply, and
/// strings as long as the whole reply may be.
fn small_limits(transfer_bytes: usize) -> SessionLimits {
    let defaults = SessionLimits::DEFAULT;
    let size = |value: usize| NonZeroUsize::new(value).assured("test limits are non-zero");
    SessionLimits::try_from(SessionLimitSettings {
        frame_bytes: size(1024),
        transfer_bytes: size(transfer_bytes),
        nesting_depth: size(defaults.nesting_depth()),
        collection_entries: size(defaults.collection_entries()),
        string_bytes: size(transfer_bytes.min(defaults.string_bytes())),
    })
    .assured("the test limits pass their checks")
}

/// One session served by the engine, driven by the frames a test sends it.
struct SessionUnderTest {
    inbound: mpsc::UnboundedSender<InboundFrame>,
    outbound: outbound::SessionFrames,
    limits: SessionLimits,
    task: JoinHandle<()>,
}

impl SessionUnderTest {
    fn start(service: &SessionServiceImpl, limits: SessionLimits) -> Self {
        let (inbound, inbound_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound) = outbound::channel(CancellationToken::new());
        let service = service.clone();
        let task = nervix_primitives::task::spawn(async move {
            service
                .run_session(
                    named::<UserName>("default"),
                    SessionTransport::Grpc,
                    limits,
                    UnboundedReceiverStream::new(inbound_rx),
                    outbound_tx,
                )
                .await;
        });
        Self {
            inbound,
            outbound,
            limits,
            task,
        }
    }

    fn send(&self, message: &ClientMessage) {
        let frame = message
            .encode(&self.limits)
            .assured("test requests fit a frame");
        let frame = VerifiedFrame::<ClientFrame>::verify(frame.into_bytes(), &self.limits)
            .assured("an encoded request verifies");
        self.inbound
            .send(InboundFrame::Frame(frame))
            .assured("the session reads until the test closes it");
    }

    fn command(&self, id: u64, query: &str, position: Option<usize>) {
        let request = CommandRequest {
            expected_transaction_position: position.map(TransactionPosition::new),
            ..test_command_request(query, "default")
        };
        self.send(&ClientMessage {
            request_id: request_id(id),
            request: ClientRequest::Command(request),
        });
    }

    /// The terminal reply to `request`, and how many frames carried it.
    async fn reply(&mut self, request: RequestId) -> (ReplyBody, usize) {
        let mut assembly = None;
        let mut frames = 0_usize;
        loop {
            nervix_primitives::task::consume_budget().await;
            let frame = nervix_primitives::time::timeout(REPLY_TIMEOUT, self.outbound.next())
                .await
                .assured("the session answers within the deadline")
                .assured("the session sends until the test closes it");
            let frame = VerifiedFrame::<ServerFrame>::verify(frame.into_bytes(), &self.limits)
                .assured("the session sends verified frames");
            let message = ServerMessage::decode(&frame).assured("a server frame decodes");
            match message {
                ServerMessage::Reply(reply) if reply.request_id == request => {
                    frames = frames.checked_add(1).assured("a test counts few frames");
                    return (reply.body, frames);
                }
                ServerMessage::TransferPart(part) if part.request_id() == request => {
                    frames = frames.checked_add(1).assured("a test counts few frames");
                    let reassembled = assembly
                        .get_or_insert_with(|| TransferAssembly::new(request, &self.limits));
                    reassembled
                        .append(&part)
                        .assured("the parts of one reply arrive in order");
                    if reassembled.is_complete() {
                        let reply = assembly
                            .take()
                            .verified("the assembly was just appended to")
                            .finish()
                            .assured("a complete transfer is one reply");
                        return (reply.body, frames);
                    }
                }
                ServerMessage::Reply(_)
                | ServerMessage::TransferPart(_)
                | ServerMessage::Event(_) => {}
            }
        }
    }

    async fn close(self) {
        self.inbound
            .send(InboundFrame::Closed)
            .assured("the session reads until the test closes it");
        drop(self.outbound);
        nervix_primitives::time::timeout(REPLY_TIMEOUT, self.task)
            .await
            .assured("the session ends once its client closes")
            .assured("the session does not panic");
    }
}

/// A transaction with two queued schemas, then a JSON description of it as request 4.
fn describe_two_operation_transaction(session: &SessionUnderTest) {
    session.command(1, "BEGIN;", None);
    session.command(
        2,
        "CREATE SCHEMA transferred_first ( user_id U32, label STRING );",
        Some(0),
    );
    session.command(
        3,
        "CREATE SCHEMA transferred_second ( order_id U32, note STRING );",
        Some(1),
    );
    session.command(4, "DESCRIBE TRANSACTION FORMAT JSON;", Some(2));
}

#[nervix_primitives::test]
async fn endpoint_opens_remain_retryable_until_committed_state_is_admitted() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut session = SessionUnderTest::start(&service, SessionLimits::DEFAULT);
    let fields = vec![SchemaField {
        name: named("value"),
        ty: ParseAsType::F64,
        optional: false,
        sensitive: false,
    }];
    let batches = NonZeroU32::new(1).assured("one is non-zero");
    let bytes = NonZeroU64::new(1024).assured("1024 is non-zero");
    let producer = ClientRequest::OpenIngestor(OpenIngestorRequest {
        domain: named("default"),
        ingestor: named("input"),
        expected_fields: fields.clone(),
        limits: ClientProducerLimits { batches, bytes },
    });
    let consumer = ClientRequest::OpenEmitter(OpenEmitterRequest {
        domain: named("default"),
        emitter: named("output"),
        expected_fields: fields,
        limits: ClientConsumerLimits { batches, bytes },
    });
    assert!(!service.inner.runtime_admission.is_admitted());
    for (id, request) in [(1, producer.clone()), (2, consumer.clone())] {
        session.send(&ClientMessage {
            request_id: request_id(id),
            request,
        });
        let (body, _) = session.reply(request_id(id)).await;
        match body {
            ReplyBody::OpenIngestor(outcome) => assert_eq!(
                outcome.disposition,
                OpenIngestorDisposition::Refused(ClientProducerRefusal::EndpointUnavailable)
            ),
            ReplyBody::OpenEmitter(outcome) => assert_eq!(
                outcome.disposition,
                OpenEmitterDisposition::Refused(EmitterOpenRefusal::EndpointUnavailable)
            ),
            outcome => panic!("expected a typed endpoint refusal, received {outcome:?}"),
        }
    }
    let admitted = service
        .inner
        .runtime_admission
        .runtime_state(&service.inner.consensus, &CancellationToken::new())
        .await;
    assert!(admitted.is_some());
    assert!(service.inner.runtime_admission.is_admitted());
    for (id, request) in [(3, producer), (4, consumer)] {
        session.send(&ClientMessage {
            request_id: request_id(id),
            request,
        });
        let (body, _) = session.reply(request_id(id)).await;
        match body {
            ReplyBody::OpenIngestor(outcome) => assert_eq!(
                outcome.disposition,
                OpenIngestorDisposition::Refused(ClientProducerRefusal::DomainStopped)
            ),
            ReplyBody::OpenEmitter(outcome) => assert_eq!(
                outcome.disposition,
                OpenEmitterDisposition::Refused(EmitterOpenRefusal::DomainStopped)
            ),
            outcome => panic!("expected a committed domain refusal, received {outcome:?}"),
        }
    }
    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_reply_larger_than_a_frame_arrives_whole_in_transfer_parts() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let limits = small_limits(SessionLimits::DEFAULT.transfer_bytes());
    let mut session = SessionUnderTest::start(&service, limits);
    describe_two_operation_transaction(&session);

    let (body, frames) = session.reply(request_id(4)).await;

    let ReplyBody::Command(outcome) = body else {
        panic!("DESCRIBE TRANSACTION is answered with a command outcome, found {body:?}");
    };
    assert!(frames > 1, "a reply above one frame travels in parts");
    let inspection = outcome
        .inspection
        .verified("a description carries its typed inspection");
    assert_eq!(
        inspection.transaction.accepted_operations(),
        TransactionPosition::new(2)
    );
    let document: serde_json::Value =
        serde_json::from_str(&outcome.message).assured("FORMAT JSON prints one JSON document");
    assert_eq!(document["transaction"]["state"], "OPEN");

    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_reply_larger_than_the_transfer_limit_is_refused_whole() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut session = SessionUnderTest::start(&service, small_limits(1024));
    describe_two_operation_transaction(&session);

    let (body, frames) = session.reply(request_id(4)).await;

    let ReplyBody::Rejected(rejected) = body else {
        panic!("an oversized reply is refused, found {body:?}");
    };
    assert_eq!(frames, 1);
    assert_eq!(rejected.rejection, RequestRejection::ReplyTooLarge);

    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn cancelling_a_request_that_is_not_in_flight_says_so() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut session = SessionUnderTest::start(&service, SessionLimits::DEFAULT);
    session.command(1, "SHOW CLUSTER STATUS;", None);
    let (answered, _) = session.reply(request_id(1)).await;
    assert!(matches!(answered, ReplyBody::Command(_)));

    session.send(&ClientMessage {
        request_id: request_id(2),
        request: ClientRequest::Cancel(CancelRequest {
            target: request_id(1),
        }),
    });
    let (body, _) = session.reply(request_id(2)).await;

    let ReplyBody::Cancel(outcome) = body else {
        panic!("a cancel request is answered with its outcome, found {body:?}");
    };
    assert_eq!(outcome.target, request_id(1));
    assert_eq!(outcome.state, nervix_client_wire::CancelState::NotInFlight);

    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn registration_refuses_a_duplicate_and_every_request_beyond_the_limit() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (outbound, _frames) = outbound::channel(CancellationToken::new());
    let subscriptions = SessionSubscriptions::for_user(named::<UserName>("default"));
    let (selection, _) = nervix_primitives::sync::watch::channel(None);
    let shared = SessionShared {
        service: service.clone(),
        transport: SessionTransport::Grpc,
        delivery: SessionDelivery {
            outbound,
            limits: SessionLimits::DEFAULT,
        },
        in_flight: nervix_primitives::sync::blocking::Mutex::new(InFlightRequests::default()),
        view: nervix_primitives::sync::blocking::RwLock::new(subscriptions.view()),
        selection,
        producers: SessionProducers::default(),
        consumers: super::consumers::SessionConsumers::default(),
    };

    shared
        .register(request_id(1), InFlightKind::Request)
        .assured("the first request registers");
    let Err(duplicate) = shared.register(request_id(1), InFlightKind::Request) else {
        panic!("a request identity already in flight is refused");
    };
    assert_eq!(duplicate.rejection, RequestRejection::DuplicateRequestId);

    for id in 2..=MAX_IN_FLIGHT_REQUESTS {
        let id = u64::try_from(id).assured("the in-flight limit fits in u64");
        shared
            .register(request_id(id), InFlightKind::Request)
            .assured("requests up to the limit register");
    }
    let beyond = u64::try_from(MAX_IN_FLIGHT_REQUESTS)
        .assured("the in-flight limit fits in u64")
        .checked_add(1)
        .assured("one past the limit fits in u64");
    let Err(refused) = shared.register(request_id(beyond), InFlightKind::Request) else {
        panic!("a request beyond the in-flight limit is refused");
    };
    assert_eq!(refused.rejection, RequestRejection::TooManyRequestsInFlight);

    // A submitted batch is bounded by its producer's credit, not by the in-flight limit, and a
    // duplicate identity is refused for it as for any request.
    let submission = beyond
        .checked_add(1)
        .assured("two past the limit fits in u64");
    shared
        .register(request_id(submission), InFlightKind::Submission)
        .assured("a submission registers while requests fill the limit");
    let Err(duplicate) = shared.register(request_id(submission), InFlightKind::Submission) else {
        panic!("a submission identity already in flight is refused");
    };
    assert_eq!(duplicate.rejection, RequestRejection::DuplicateRequestId);
    assert!(shared.finish(request_id(submission)));
    let Err(refused) = shared.register(request_id(beyond), InFlightKind::Request) else {
        panic!("an answered submission frees no place a request counts");
    };
    assert_eq!(refused.rejection, RequestRejection::TooManyRequestsInFlight);

    assert!(shared.finish(request_id(1)));
    shared
        .register(request_id(beyond), InFlightKind::Request)
        .assured("an answered request frees its place");

    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_redirect_while_no_leader_is_known_names_none() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;

    let refused = service
        .not_leader_response("CREATE SCHEMA orphan ( id I64 );", None)
        .await;

    let CommandDisposition::NotLeader(redirect) = &refused.disposition else {
        panic!("a command that needs the leader is redirected, found {refused:?}");
    };
    assert_eq!(redirect.leader, None);
    assert_eq!(
        super::outcome::leader_redirect(redirect.clone()),
        WireLeaderRedirect { leader: None }
    );
    let [diagnostic] = refused.diagnostics.as_slice() else {
        panic!(
            "a redirect carries one diagnostic, found {:?}",
            refused.diagnostics
        );
    };
    assert_eq!(
        diagnostic.message,
        "retry this command on the current leader"
    );
    assert_eq!(
        diagnostic.span,
        Some(0.."CREATE SCHEMA orphan ( id I64 );".len())
    );

    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_subscription_statement_the_parser_rejects_is_refused_at_its_rejected_token() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut session = SessionUnderTest::start(&service, SessionLimits::DEFAULT);
    let statement = "CREATE SUBSCRIPTION watch TO 42;";
    session.send(&ClientMessage {
        request_id: request_id(1),
        request: ClientRequest::Subscribe(SubscribeRequest {
            domain: named::<DomainName>("default"),
            statement: statement.to_string(),
            subscription_type: SubscriptionType::Row,
        }),
    });
    let (body, _) = session.reply(request_id(1)).await;

    let ReplyBody::Subscribe(outcome) = body else {
        panic!("a subscribe request is answered with its outcome, found {body:?}");
    };
    assert!(matches!(outcome.disposition, SubscribeDisposition::Failed));
    assert_eq!(outcome.message, "parse error");
    let [diagnostic] = outcome.diagnostics.as_slice() else {
        panic!(
            "a rejected statement carries one diagnostic, found {:?}",
            outcome.diagnostics
        );
    };
    let span = diagnostic
        .span
        .assured("a parse diagnostic locates the token it rejected");
    let start: usize = span.start().arch_into();
    let end: usize = span.end().arch_into();
    assert_eq!(statement.get(start..end), Some("42"));

    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

/// A message of a session about a domain clock: the reply to a request, or a clock frame.
#[derive(Debug)]
enum ClockMessage {
    Reply(RequestId, ReplyBody),
    Frame(ServerEvent),
}

impl SessionUnderTest {
    fn attach_clock(&self, id: u64, domain: &str) {
        self.send(&ClientMessage {
            request_id: request_id(id),
            request: ClientRequest::AttachDomainClock(AttachDomainClockRequest {
                domain: named::<DomainName>(domain),
            }),
        });
    }

    fn detach_clock(&self, id: u64, domain: &str) {
        self.send(&ClientMessage {
            request_id: request_id(id),
            request: ClientRequest::DetachDomainClock(DetachDomainClockRequest {
                domain: named::<DomainName>(domain),
            }),
        });
    }

    /// The next reply or clock frame the session sends, skipping every other event.
    async fn next_clock_message(&mut self) -> ClockMessage {
        loop {
            nervix_primitives::task::consume_budget().await;
            let frame = nervix_primitives::time::timeout(REPLY_TIMEOUT, self.outbound.next())
                .await
                .assured("the session sends within the deadline")
                .assured("the session sends until the test closes it");
            let frame = VerifiedFrame::<ServerFrame>::verify(frame.into_bytes(), &self.limits)
                .assured("the session sends verified frames");
            match ServerMessage::decode(&frame).assured("a server frame decodes") {
                ServerMessage::Reply(reply) => {
                    return ClockMessage::Reply(reply.request_id, reply.body);
                }
                ServerMessage::Event(
                    event @ (ServerEvent::DomainClockObserved(_)
                    | ServerEvent::DomainClockAttachmentEnded(_)),
                ) => return ClockMessage::Frame(event),
                ServerMessage::Event(_) | ServerMessage::TransferPart(_) => {}
            }
        }
    }

    /// The reply to `id`, which must be the next reply or clock frame the session sends.
    async fn next_clock_reply(&mut self, id: u64) -> ReplyBody {
        match self.next_clock_message().await {
            ClockMessage::Reply(request, body) if request == request_id(id) => body,
            other => panic!("request {id} is answered before any other clock message: {other:?}"),
        }
    }

    /// The clock frame the session sends next, before any reply.
    async fn next_clock_frame(&mut self) -> ServerEvent {
        match self.next_clock_message().await {
            ClockMessage::Frame(event) => event,
            other => panic!("a clock frame comes next: {other:?}"),
        }
    }

    /// Whether the session sends no reply and no clock frame for [`UNANSWERED_WINDOW`].
    async fn sends_no_clock_message(&mut self) -> bool {
        let message =
            nervix_primitives::time::timeout(UNANSWERED_WINDOW, self.next_clock_message()).await;
        message.is_err()
    }
}

/// How long a test watches for an answer that must not arrive at all while its precondition holds,
/// such as the answer to an attach before its node installed the committed domains. A longer
/// window only strengthens the assertion.
const UNANSWERED_WINDOW: Duration = Duration::from_millis(500);

fn clocked_domain(start_version: u64, status: DomainStatus) -> DomainState {
    let clock = match status {
        DomainStatus::Stopped => None,
        DomainStatus::Running | DomainStatus::Paused => Some(DomainClockState::new(
            Timestamp::from_unix_nanos(5),
            Timestamp::from_unix_nanos(1_000),
            DomainTimeRate::ONE,
        )),
    };
    DomainState {
        id: named("clocked"),
        config: DomainConfig {
            pace: DomainPace::Paced {
                period: "1s".parse().assured("one second is a valid period"),
                skew: "10ms".parse().assured("ten milliseconds is a valid skew"),
            },
            placement: PlacementPolicy::Neutral,
        },
        status,
        start_version,
        last_start: DomainStartPoint::Resume,
        clock,
    }
}

fn clocked_observation(state: &DomainState) -> DomainClockObservation {
    let observed = match (&state.status, &state.clock, state.config.pace) {
        (DomainStatus::Stopped, _, _) => DomainClockObservedState::Stopped,
        (_, Some(mapping), DomainPace::Paced { period, skew }) => {
            DomainClockObservedState::Paced(PacedDomainClock {
                period,
                skew,
                mapping: mapping.clone(),
            })
        }
        (_, _, _) => DomainClockObservedState::Uninstalled,
    };
    DomainClockObservation {
        generation: state.start_version,
        state: observed,
    }
}

fn install(service: &SessionServiceImpl, states: &[DomainState]) {
    let domains = states
        .iter()
        .map(|state| (state.id.clone(), state.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    service.inner.runtime.sync_domains(&domains);
}

#[nervix_primitives::test]
async fn a_session_follows_a_domain_clock_until_it_detaches_or_the_domain_leaves_the_node() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    let running = clocked_domain(1, DomainStatus::Running);
    install(&service, std::slice::from_ref(&running));
    let mut session = SessionUnderTest::start(&service, SessionLimits::DEFAULT);

    session.attach_clock(1, "clocked");
    let ReplyBody::DomainClockAttach(outcome) = session.next_clock_reply(1).await else {
        panic!("an attach request is answered with its outcome");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockAttachDisposition::Attached {
            domain: named("clocked"),
            clock: clocked_observation(&running),
        }
    );
    session.attach_clock(2, "clocked");
    let ReplyBody::DomainClockAttach(outcome) = session.next_clock_reply(2).await else {
        panic!("an attach request is answered with its outcome");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockAttachDisposition::AlreadyAttached(named("clocked"))
    );
    session.attach_clock(3, "elsewhere");
    let ReplyBody::DomainClockAttach(outcome) = session.next_clock_reply(3).await else {
        panic!("an attach request is answered with its outcome");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockAttachDisposition::DomainNotFound(named("elsewhere"))
    );

    let stopped = clocked_domain(1, DomainStatus::Stopped);
    install(&service, std::slice::from_ref(&stopped));
    let ServerEvent::DomainClockObserved(observed) = session.next_clock_frame().await else {
        panic!("stopping the domain delivers its stopped clock");
    };
    assert_eq!(observed.domain, named::<DomainName>("clocked"));
    assert_eq!(observed.clock, clocked_observation(&stopped));

    session.detach_clock(4, "clocked");
    let ReplyBody::DomainClockDetach(outcome) = session.next_clock_reply(4).await else {
        panic!("a detach request is answered with its outcome");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockDetachDisposition::Detached(named("clocked"))
    );
    let restarted = clocked_domain(2, DomainStatus::Running);
    install(&service, std::slice::from_ref(&restarted));
    session.attach_clock(5, "clocked");
    let ReplyBody::DomainClockAttach(outcome) = session.next_clock_reply(5).await else {
        panic!("nothing about a detached clock precedes the next reply");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockAttachDisposition::Attached {
            domain: named("clocked"),
            clock: clocked_observation(&restarted),
        }
    );

    install(&service, &[]);
    let ServerEvent::DomainClockAttachmentEnded(ended) = session.next_clock_frame().await else {
        panic!("removing the domain ends the attachment");
    };
    assert_eq!(ended.domain, named::<DomainName>("clocked"));
    assert_eq!(ended.reason, DomainClockAttachmentEndReason::DomainRemoved);
    session.detach_clock(6, "clocked");
    let ReplyBody::DomainClockDetach(outcome) = session.next_clock_reply(6).await else {
        panic!("a detach request is answered with its outcome");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockDetachDisposition::NotAttached(named("clocked"))
    );

    install(&service, std::slice::from_ref(&restarted));
    session.attach_clock(7, "clocked");
    let ReplyBody::DomainClockAttach(outcome) = session.next_clock_reply(7).await else {
        panic!("an attach request is answered with its outcome");
    };
    assert!(matches!(
        outcome.disposition,
        DomainClockAttachDisposition::Attached { .. }
    ));
    install(&service, &[]);
    assert!(matches!(
        session.next_clock_frame().await,
        ServerEvent::DomainClockAttachmentEnded(_)
    ));
    install(&service, std::slice::from_ref(&restarted));
    session.attach_clock(8, "clocked");
    let ReplyBody::DomainClockAttach(outcome) = session.next_clock_reply(8).await else {
        panic!("a clock the server ended can be attached again");
    };
    assert!(matches!(
        outcome.disposition,
        DomainClockAttachDisposition::Attached { .. }
    ));

    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn an_attach_answers_once_its_node_has_installed_the_committed_domains() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    let mut session = SessionUnderTest::start(&service, SessionLimits::DEFAULT);

    session.attach_clock(1, "clocked");
    assert!(
        session.sends_no_clock_message().await,
        "a node that has installed no committed domains cannot tell whether one exists, so it \
         does not answer"
    );
    let running = clocked_domain(1, DomainStatus::Running);
    install(&service, std::slice::from_ref(&running));
    let ReplyBody::DomainClockAttach(outcome) = session.next_clock_reply(1).await else {
        panic!("an attach request is answered with its outcome");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockAttachDisposition::Attached {
            domain: named("clocked"),
            clock: clocked_observation(&running),
        }
    );

    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn an_attach_waiting_for_the_committed_domains_ends_with_its_session() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    let mut session = SessionUnderTest::start(&service, SessionLimits::DEFAULT);

    session.attach_clock(1, "clocked");
    assert!(
        session.sends_no_clock_message().await,
        "a node that has installed no committed domains does not answer an attach"
    );
    // The session ends while its ordered lane waits in the attach, and the lane stops with it.
    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_session_holding_a_transaction_refuses_domain_clock_requests() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    install(&service, &[clocked_domain(1, DomainStatus::Running)]);
    let mut session = SessionUnderTest::start(&service, SessionLimits::DEFAULT);
    session.command(1, "BEGIN;", None);
    let (begun, _) = session.reply(request_id(1)).await;
    assert!(matches!(begun, ReplyBody::Command(_)));

    session.attach_clock(2, "clocked");
    let (body, _) = session.reply(request_id(2)).await;
    let ReplyBody::DomainClockAttach(outcome) = body else {
        panic!("an attach request is answered with its outcome, found {body:?}");
    };
    assert_eq!(outcome.disposition, DomainClockAttachDisposition::Failed);
    assert_eq!(outcome.message, super::SESSION_LOCAL_IN_TRANSACTION);
    session.detach_clock(3, "clocked");
    let (body, _) = session.reply(request_id(3)).await;
    let ReplyBody::DomainClockDetach(outcome) = body else {
        panic!("a detach request is answered with its outcome, found {body:?}");
    };
    assert_eq!(outcome.disposition, DomainClockDetachDisposition::Failed);
    assert_eq!(outcome.message, super::SESSION_LOCAL_IN_TRANSACTION);

    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_domain_clock_statement_sent_as_a_command_is_refused_in_favour_of_its_request() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut session = SessionUnderTest::start(&service, SessionLimits::DEFAULT);

    session.command(1, "ATTACH DOMAIN CLOCK;", None);
    let (body, _) = session.reply(request_id(1)).await;
    let ReplyBody::Command(outcome) = body else {
        panic!("a command is answered with its outcome, found {body:?}");
    };
    assert_eq!(outcome.disposition, WireCommandDisposition::Failed);
    assert_eq!(
        outcome.message,
        "ATTACH DOMAIN CLOCK is a session-local command; send an attach domain clock request"
    );

    session.command(2, "DETACH DOMAIN CLOCK;", None);
    let (body, _) = session.reply(request_id(2)).await;
    let ReplyBody::Command(outcome) = body else {
        panic!("a command is answered with its outcome, found {body:?}");
    };
    assert_eq!(outcome.disposition, WireCommandDisposition::Failed);
    assert_eq!(
        outcome.message,
        "DETACH DOMAIN CLOCK is a session-local command; send a detach domain clock request"
    );

    session.close().await;
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}
