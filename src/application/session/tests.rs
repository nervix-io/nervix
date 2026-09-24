//! Session engine tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that a reply larger than a frame arrives whole as transfer parts, that a
//!   reply larger than the transfer limit is refused whole, that a cancellation of a request that
//!   is not in flight says so, and that registration refuses a duplicate or excess request rather
//!   than queueing it.
//! - **Depends on.** The session engine and the session test fixtures.
//! - **Must not know.** Production ownership beyond the parent module under test.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    CancelRequest, ClientFrame, ClientMessage, ClientRequest, CommandRequest, EncodedFrame,
    LeaderRedirect as WireLeaderRedirect, ReplyBody, RequestId, RequestRejection, ServerFrame,
    ServerMessage, SessionLimitSettings, SessionLimits, TransferAssembly, VerifiedFrame,
};
use nervix_models::{TransactionPosition, UserName};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{
    InboundFrame, MAX_IN_FLIGHT_REQUESTS, SESSION_OUTBOUND_CAPACITY, SessionShared,
    SessionTransport,
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
    outbound: mpsc::Receiver<EncodedFrame<ServerFrame>>,
    limits: SessionLimits,
    task: JoinHandle<()>,
}

impl SessionUnderTest {
    fn start(service: &SessionServiceImpl, limits: SessionLimits) -> Self {
        let (inbound, inbound_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound) = mpsc::channel(SESSION_OUTBOUND_CAPACITY);
        let service = service.clone();
        let task = tokio::spawn(async move {
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
            tokio::task::consume_budget().await;
            let frame = tokio::time::timeout(REPLY_TIMEOUT, self.outbound.recv())
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
        tokio::time::timeout(REPLY_TIMEOUT, self.task)
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
async fn registration_refuses_a_duplicate_and_every_request_beyond_the_limit() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (outbound, _frames) = mpsc::channel(SESSION_OUTBOUND_CAPACITY);
    let subscriptions = SessionSubscriptions::for_user(named::<UserName>("default"));
    let (selection, _) = tokio::sync::watch::channel(None);
    let shared = SessionShared {
        service: service.clone(),
        transport: SessionTransport::Grpc,
        delivery: SessionDelivery {
            outbound,
            limits: SessionLimits::DEFAULT,
        },
        in_flight: parking_lot::Mutex::new(std::collections::BTreeMap::new()),
        view: parking_lot::RwLock::new(subscriptions.view()),
        selection,
        ended: tokio_util::sync::CancellationToken::new(),
    };

    shared
        .register(request_id(1))
        .assured("the first request registers");
    let Err(duplicate) = shared.register(request_id(1)) else {
        panic!("a request identity already in flight is refused");
    };
    assert_eq!(duplicate.rejection, RequestRejection::DuplicateRequestId);

    for id in 2..=MAX_IN_FLIGHT_REQUESTS {
        let id = u64::try_from(id).assured("the in-flight limit fits in u64");
        shared
            .register(request_id(id))
            .assured("requests up to the limit register");
    }
    let beyond = u64::try_from(MAX_IN_FLIGHT_REQUESTS)
        .assured("the in-flight limit fits in u64")
        .checked_add(1)
        .assured("one past the limit fits in u64");
    let Err(refused) = shared.register(request_id(beyond)) else {
        panic!("a request beyond the in-flight limit is refused");
    };
    assert_eq!(refused.rejection, RequestRejection::TooManyRequestsInFlight);

    assert!(shared.finish(request_id(1)));
    shared
        .register(request_id(beyond))
        .assured("an answered request frees its place");

    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[tokio::test]
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
