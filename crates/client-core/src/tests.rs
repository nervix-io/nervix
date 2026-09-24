//! Unit tests of the client session: the dispatcher that routes an exchange's frames, the
//! transaction state a session keeps, and the statements the client serves itself or turns into
//! requests.
//!
//! Dispatcher tests hand real frames, encoded by the wire contract, to an exchange's reader.
//! Loopback tests play the server by hand: they read the frames a client sends and complete the
//! waiters those requests registered. The `session` tests run the client against an in-process
//! gRPC server.

// A Shuttle build replaces the client's synchronization with models that only run inside a
// Shuttle test, so the tests over a real connection drive the production build only.
#[cfg(not(feature = "shuttle"))]
mod session;

use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    AttachDisposition, AttachOutcome, ClientFrame, ClientMessage, ClientRequest,
    CommandDisposition, Diagnostic, DomainInfo, DomainList, DomainsObserved, EncodedFrame,
    LeaderEndpoints, LeaderRedirect, Leadership, LeadershipObserved, NoticeLevel, OutcomeOrigin,
    Reply, ReplyBody, ReplyDelivery, RequestId, RequestRejected, RequestRejection, RowSchema,
    ServerFrame, ServerNotice, SessionEndReason, SessionEnding, SessionLimitSettings,
    SessionLimits, SourceSpan, SubscribeDisposition, SubscribeOutcome, SubscriptionEndReason,
    SubscriptionEnded, SubscriptionHandle, SubscriptionOpened, SubscriptionRowsEncoder,
    SubscriptionType, UnknownOutcomeCause, UnsubscribeDisposition, UnsubscribeOutcome,
    VerifiedFrame, WireDecodeError,
};
use nervix_models::{
    ClusterNodeName, CommandExecutionReference, DomainName, DomainPace, DomainStatus, FieldName,
    ImpactPlanningBasis, ImpactReportCompleteness, ParseAsType, RelayName, SchemaField,
    SubscriptionName, TransactionImpactReport, TransactionInspection, TransactionLifecycle,
    TransactionOperationNumber, TransactionPosition, TransactionPreviewIdentity, TransactionStatus,
};
use tokio::sync::{Mutex, mpsc, watch};
use tonic::transport::Channel;
use triomphe::Arc;
use url::Url;

use crate::{
    Client, ClientError, CommandOutcome, ConnectOptions, RequestKind, ServerEvent,
    SubscriptionEvent, SubscriptionRequest, TlsRequirement,
    connection::{GrpcConnector, ServerDirectory},
    exchange::{
        EventSinks, Exchange, ExchangeReader, ExchangeRequests, PendingReplies, ReaderFlow,
        RegisteredRequest, SESSION_LIMITS, SessionEvents,
    },
    outcome::Routing,
    split_query_statements,
    upload::{expand_user_path, upload_status_is_retryable},
};

const DEADLINE: Duration = Duration::from_secs(10);

fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the test domain is an accepted literal")
}

fn request_id(id: u64) -> RequestId {
    RequestId::new(NonZeroU64::new(id).assured("test request identities are non-zero"))
}

fn reference(value: &str) -> CommandExecutionReference {
    CommandExecutionReference::parse(value).assured("the test reference is an accepted literal")
}

fn open_transaction(id: &str, accepted_operations: usize) -> TransactionStatus {
    TransactionStatus::new(
        id.to_string(),
        domain("tenant"),
        TransactionLifecycle::Open,
        TransactionPosition::new(accepted_operations),
        0,
    )
    .assured("an open transaction with nothing applied is consistent")
}

fn test_preview(transaction_id: &str, position: usize) -> TransactionPreviewIdentity {
    TransactionPreviewIdentity {
        transaction_id: transaction_id.to_string(),
        position: TransactionPosition::new(position),
        planning_basis: ImpactPlanningBasis::new([7; 32]),
    }
}

fn empty_report() -> TransactionImpactReport {
    TransactionImpactReport::new(
        domain("tenant"),
        TransactionPosition::new(0),
        ImpactPlanningBasis::new([7; 32]),
        ImpactReportCompleteness::Complete,
        Vec::new(),
        Vec::new(),
    )
    .assured("an empty report numbers no operation and so needs no execution step")
}

fn wire_outcome(
    execution_reference: &str,
    disposition: CommandDisposition,
    message: &str,
) -> nervix_client_wire::CommandOutcome {
    nervix_client_wire::CommandOutcome {
        execution_reference: reference(execution_reference),
        origin: OutcomeOrigin::Executed,
        disposition,
        message: message.to_string(),
        diagnostics: Vec::new(),
        statements: Vec::new(),
        transaction: None,
        transaction_admission: None,
        inspection: None,
    }
}

fn completed() -> CommandDisposition {
    CommandDisposition::Completed {
        already_existed: false,
    }
}

fn command_reply(disposition: CommandDisposition, message: &str) -> ReplyBody {
    ReplyBody::Command(Box::new(wire_outcome("command-1", disposition, message)))
}

fn domain_list_reply() -> ReplyBody {
    ReplyBody::DomainList(DomainList {
        domains: vec![DomainInfo {
            domain: domain("orders"),
            status: DomainStatus::Running,
            pace: DomainPace::Unpaced,
        }],
    })
}

fn subscription(name: &str, generation: u64) -> SubscriptionHandle {
    SubscriptionHandle {
        name: SubscriptionName::parse(name).assured("the test subscription name is valid"),
        generation: NonZeroU64::new(generation).assured("test generations are non-zero"),
    }
}

fn field(name: &str, ty: ParseAsType) -> SchemaField {
    SchemaField {
        name: FieldName::parse(name).assured("the test field name is valid"),
        ty,
        optional: false,
        sensitive: false,
    }
}

fn orders_schema() -> RowSchema {
    RowSchema {
        fields: vec![
            field("name", ParseAsType::String),
            field("id", ParseAsType::U64),
        ],
        branch: None,
    }
}

fn opened_reply(handle: SubscriptionHandle) -> ReplyBody {
    ReplyBody::Subscribe(SubscribeOutcome {
        disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
            subscription: handle,
            domain: domain("tenant"),
            relay: RelayName::parse("orders").assured("the test relay name is valid"),
            subscription_type: SubscriptionType::Row,
            schema: orders_schema(),
        })),
        message: "subscription opened".to_string(),
        diagnostics: Vec::new(),
    })
}

fn verified(frame: EncodedFrame<ServerFrame>) -> VerifiedFrame<ServerFrame> {
    frame
        .verify(&SESSION_LIMITS)
        .assured("a frame the wire contract encoded verifies")
}

fn reply_frame(id: RequestId, body: ReplyBody) -> VerifiedFrame<ServerFrame> {
    let delivery = Reply {
        request_id: id,
        body,
    }
    .encode(&SESSION_LIMITS)
    .assured("a test reply fits the transfer limit");
    let ReplyDelivery::Frame(frame) = delivery else {
        panic!("a small test reply fits one frame");
    };
    verified(frame)
}

fn notice_frame(message: &str) -> VerifiedFrame<ServerFrame> {
    verified(
        ServerNotice {
            level: NoticeLevel::Info,
            message: message.to_string(),
        }
        .encode(&SESSION_LIMITS)
        .assured("a test notice fits a frame"),
    )
}

fn rows_frame(handle: SubscriptionHandle, rows: &[(u64, &str)]) -> VerifiedFrame<ServerFrame> {
    let mut batch = SubscriptionRowsEncoder::unbranched(handle, &SESSION_LIMITS)
        .assured("an unbranched batch starts within the limits");
    for (id, name) in rows {
        batch
            .push_row(|cells| {
                cells.push_string(name)?;
                cells.push_u64(*id)
            })
            .assured("a test row fits the limits");
    }
    verified(batch.finish().assured("a test batch finishes"))
}

fn ended_frame(handle: SubscriptionHandle) -> VerifiedFrame<ServerFrame> {
    verified(
        SubscriptionEnded {
            subscription: handle,
            reason: SubscriptionEndReason::RelayChanged,
            message: "relay 'orders' was redefined".to_string(),
        }
        .encode(&SESSION_LIMITS)
        .assured("a test end fits a frame"),
    )
}

/// An exchange with no server behind it: its frames go nowhere, and its reader has finished.
fn detached_exchange(
    frames: mpsc::Sender<EncodedFrame<ClientFrame>>,
    pending: Arc<Mutex<PendingReplies>>,
) -> Exchange {
    Exchange {
        requests: Arc::new(ExchangeRequests {
            frames,
            pending,
            channel: Channel::from_static("http://127.0.0.1:9").connect_lazy(),
        }),
        reader: tokio::spawn(std::future::ready(())),
    }
}

fn client_on(exchange: Exchange, session_domain: Option<DomainName>) -> Client {
    let connector = GrpcConnector::new(ConnectOptions::default())
        .assured("options without credentials build no authorization metadata");
    Client::assemble(
        exchange,
        SessionEvents::new(),
        connector,
        session_domain,
        ServerDirectory::default(),
    )
}

/// A client whose exchange lost its transport: every request finds nothing to send it on.
fn test_client(domain_name: &str) -> Client {
    let (frames, outbound) = mpsc::channel(1);
    drop(outbound);
    let pending = Arc::new(Mutex::new(PendingReplies::new()));
    client_on(
        detached_exchange(frames, pending),
        Some(domain(domain_name)),
    )
}

/// A client whose exchange the test serves by hand.
struct Loopback {
    client: Client,
    requests: mpsc::Receiver<EncodedFrame<ClientFrame>>,
    pending: Arc<Mutex<PendingReplies>>,
}

impl Loopback {
    fn new(session_domain: Option<DomainName>) -> Self {
        let (frames, requests) = mpsc::channel(8);
        let pending = Arc::new(Mutex::new(PendingReplies::new()));
        let client = client_on(detached_exchange(frames, pending.clone()), session_domain);
        Self {
            client,
            requests,
            pending,
        }
    }

    /// The next request the client sends, as the server decodes it.
    async fn next_request(&mut self) -> ClientMessage {
        let frame = tokio::time::timeout(DEADLINE, self.requests.recv())
            .await
            .assured("the client sends its request within the deadline")
            .assured("the client keeps its exchange open");
        let frame = frame
            .verify(&SESSION_LIMITS)
            .assured("a request the client encoded verifies");
        ClientMessage::decode(&frame).assured("a request the client encoded decodes")
    }

    /// Answers a request the way the exchange's reader would.
    async fn answer(&self, request: RequestId, body: ReplyBody) {
        let waiter = self
            .pending
            .lock()
            .await
            .take(request)
            .assured("the request waits for its reply");
        waiter
            .send(body)
            .assured("the client waits for the reply to its request");
    }
}

/// An exchange reader over a fresh registry, with event queues of `capacity` entries.
struct ReaderFixture {
    reader: ExchangeReader,
    pending: Arc<Mutex<PendingReplies>>,
    notice_sink: mpsc::Sender<ServerEvent>,
    subscription_events: mpsc::Receiver<SubscriptionEvent>,
    notices: mpsc::Receiver<ServerEvent>,
    leadership: watch::Receiver<Option<Leadership>>,
    domains: watch::Receiver<Option<Vec<DomainInfo>>>,
}

fn reader_fixture(capacity: usize) -> ReaderFixture {
    let (subscriptions, subscription_events) = mpsc::channel(capacity);
    let (notices, server_notices) = mpsc::channel(capacity);
    let (leadership, observed_leadership) = watch::channel(None);
    let (domains, observed_domains) = watch::channel(None);
    let pending = Arc::new(Mutex::new(PendingReplies::new()));
    let reader = ExchangeReader::new(
        pending.clone(),
        EventSinks {
            subscriptions,
            notices: notices.clone(),
            leadership,
            domains,
        },
    );
    ReaderFixture {
        reader,
        pending,
        notice_sink: notices,
        subscription_events,
        notices: server_notices,
        leadership: observed_leadership,
        domains: observed_domains,
    }
}

async fn register(pending: &Mutex<PendingReplies>) -> RegisteredRequest {
    pending
        .lock()
        .await
        .register()
        .assured("an open exchange registers requests")
}

#[test]
fn statement_splitting_returns_exact_source_slices() {
    let query = "USE prod; LIST DOMAINS;";

    assert_eq!(
        split_query_statements(query).assured("the literal statement batch is valid current NSPL"),
        ["USE prod;", "LIST DOMAINS;"]
    );
}

#[tokio::test]
async fn a_commit_fences_against_the_basis_its_own_transaction_reported() {
    let client = test_client("tenant");
    client
        .adopt_transaction_status(open_transaction("tx-1", 1))
        .await;
    *client.inner.commit_basis.lock().await = Some(test_preview("tx-1", 1));

    let expectation = client.transaction_expectation().await;

    assert_eq!(expectation.position, Some(TransactionPosition::new(1)));
    assert_eq!(expectation.preview, Some(test_preview("tx-1", 1)));
}

#[tokio::test]
async fn a_basis_read_for_another_transaction_never_fences_this_one() {
    let client = test_client("tenant");
    client
        .adopt_transaction_status(open_transaction("tx-2", 1))
        .await;
    *client.inner.commit_basis.lock().await = Some(test_preview("tx-1", 4));

    let expectation = client.transaction_expectation().await;

    assert_eq!(expectation.position, Some(TransactionPosition::new(1)));
    assert_eq!(
        expectation.preview, None,
        "a basis naming another transaction cannot decide this transaction's commit"
    );
}

#[tokio::test]
async fn a_refused_commit_leaves_the_current_basis_for_the_next_attempt() {
    let client = test_client("tenant");
    client
        .adopt_transaction_status(open_transaction("tx-1", 1))
        .await;
    let outcome = CommandOutcome::from(wire_outcome(
        "commit-1",
        CommandDisposition::PreviewStale {
            expected: test_preview("tx-1", 1),
            current: test_preview("tx-1", 2),
        },
        "stale",
    ));

    client.record_commit_basis(&outcome).await;

    assert_eq!(
        client.transaction_expectation().await.preview,
        Some(test_preview("tx-1", 2))
    );
}

#[tokio::test]
async fn an_inspected_report_arrives_typed_beside_the_callers_own_binding() {
    let mut fixture = reader_fixture(4);
    let request = register(&fixture.pending).await;
    let inspected = TransactionStatus::new(
        "tx-inspected".to_string(),
        domain("tenant"),
        TransactionLifecycle::Open,
        TransactionPosition::new(0),
        0,
    )
    .assured("an empty open transaction is consistent");
    let mut described = wire_outcome("describe-1", completed(), "described");
    described.transaction = Some(open_transaction("tx-bound", 0));
    described.inspection = Some(Box::new(TransactionInspection {
        transaction: inspected,
        operation: None,
        report: empty_report(),
    }));

    let flow = fixture
        .reader
        .route(reply_frame(
            request.request_id,
            ReplyBody::Command(Box::new(described)),
        ))
        .await;

    assert_eq!(flow, ReaderFlow::Continue);
    let Ok(ReplyBody::Command(outcome)) = request.reply.await else {
        panic!("the waiter receives the command outcome");
    };
    let outcome = CommandOutcome::from(*outcome);
    let inspection = outcome
        .inspection
        .as_ref()
        .verified("the reply carried an inspection");
    assert_eq!(inspection.transaction.transaction_id(), "tx-inspected");
    assert_eq!(
        inspection.transaction.lifecycle(),
        &TransactionLifecycle::Open
    );
    assert_eq!(inspection.operation, None);
    assert_eq!(inspection.report, empty_report());
    let binding = outcome
        .transaction
        .as_ref()
        .verified("the reply reports the session's own binding");
    assert_eq!(
        binding.transaction_id(),
        "tx-bound",
        "the inspected transaction does not replace the session's own binding"
    );
}

#[tokio::test]
async fn a_failed_inspected_transaction_names_its_failing_operation() {
    let mut fixture = reader_fixture(4);
    let request = register(&fixture.pending).await;
    let failing_operation = TransactionOperationNumber::new(
        NonZeroUsize::new(1).assured("the first operation number is non-zero"),
    );
    let failed = TransactionStatus::new(
        "tx-inspected".to_string(),
        domain("tenant"),
        TransactionLifecycle::Failed {
            failing_operation,
            error: "domain start refused".to_string(),
        },
        TransactionPosition::new(1),
        0,
    )
    .assured("a failed transaction that applied nothing is consistent");
    let mut described = wire_outcome("describe-1", completed(), "described");
    described.inspection = Some(Box::new(TransactionInspection {
        transaction: failed,
        operation: None,
        report: empty_report(),
    }));

    fixture
        .reader
        .route(reply_frame(
            request.request_id,
            ReplyBody::Command(Box::new(described)),
        ))
        .await;

    let Ok(ReplyBody::Command(outcome)) = request.reply.await else {
        panic!("the waiter receives the command outcome");
    };
    let inspection = outcome
        .inspection
        .as_ref()
        .verified("the reply carried an inspection");
    let TransactionLifecycle::Failed {
        failing_operation,
        error,
    } = inspection.transaction.lifecycle()
    else {
        panic!("a failed status decodes as a failure");
    };
    assert_eq!(failing_operation.get(), 1);
    assert_eq!(error, "domain start refused");
}

#[tokio::test]
async fn a_frame_the_contract_does_not_describe_ends_the_exchange() {
    let defaults = SessionLimits::DEFAULT;
    let size = |value: usize| NonZeroUsize::new(value).assured("a non-zero test limit");
    let short_strings = SessionLimits::try_from(SessionLimitSettings {
        frame_bytes: size(defaults.frame_bytes()),
        transfer_bytes: size(defaults.transfer_bytes()),
        nesting_depth: size(defaults.nesting_depth()),
        collection_entries: size(defaults.collection_entries()),
        string_bytes: size(8),
    })
    .assured("the test limits pass their checks");
    let frame = notice_frame("a notice longer than eight bytes").into_bytes();
    let undecodable = VerifiedFrame::<ServerFrame>::verify(frame, &short_strings)
        .assured("verification does not hold strings to the string limit");
    let decoded = nervix_client_wire::ServerMessage::decode(&undecodable);
    assert!(
        matches!(
            decoded.as_ref().map_err(|report| report.current_context()),
            Err(WireDecodeError::StringTooLong { .. })
        ),
        "the frame breaks the limits its receiver decodes it under"
    );

    let fixture = reader_fixture(4);
    let request = register(&fixture.pending).await;
    let frames = tokio_stream::iter([Ok(undecodable), Ok(notice_frame("never routed"))]);
    fixture.reader.run(frames).await;

    assert!(
        request.reply.await.is_err(),
        "the waiter observes the closed session"
    );
    assert!(
        fixture.pending.lock().await.register().is_none(),
        "an ended exchange takes no further requests"
    );
    let mut notices = fixture.notices;
    assert!(
        notices.try_recv().is_err(),
        "no frame after the violation is routed"
    );
}

#[tokio::test]
async fn connect_rejects_plain_server_when_tls_is_required() {
    let connector = GrpcConnector::new(ConnectOptions {
        tls_requirement: Some(TlsRequirement::Required),
        ca_certificate_pem: None,
        username: None,
        password: None,
    })
    .assured("options without credentials build no authorization metadata");
    let server = Url::parse("http://127.0.0.1:47391").assured("a literal loopback URL");
    let error = connector
        .connect(&server)
        .await
        .expect_err("plain server should be rejected when tls is required");
    assert!(matches!(error, ClientError::TlsRequired));
}

#[tokio::test]
async fn response_reordering_cannot_take_another_requests_waiter() {
    let mut fixture = reader_fixture(4);
    let domains_request = register(&fixture.pending).await;
    let command_request = register(&fixture.pending).await;

    assert_eq!(
        fixture
            .reader
            .route(reply_frame(
                command_request.request_id,
                command_reply(completed(), "executed"),
            ))
            .await,
        ReaderFlow::Continue
    );
    assert_eq!(
        fixture
            .reader
            .route(reply_frame(domains_request.request_id, domain_list_reply()))
            .await,
        ReaderFlow::Continue
    );

    let Ok(ReplyBody::Command(command)) = command_request.reply.await else {
        panic!("the command response was discarded by the domain-list waiter");
    };
    let Ok(ReplyBody::DomainList(domains)) = domains_request.reply.await else {
        panic!("the domain-list response was discarded by the command waiter");
    };
    assert_eq!(command.message, "executed");
    assert_eq!(domains.domains.len(), 1);
    assert_eq!(domains.domains[0].domain.as_str(), "orders");
}

#[tokio::test]
#[ignore = "CLIENT-WIRE-11 separates bounded event delivery from command replies"]
async fn saturated_event_consumer_cannot_block_a_command_reply() {
    let ReaderFixture {
        mut reader,
        pending,
        notice_sink,
        notices: _undrained_notices,
        ..
    } = reader_fixture(1);
    let command_request = register(&pending).await;
    let command_id = command_request.request_id;
    notice_sink
        .send(ServerEvent {
            level: NoticeLevel::Info,
            message: "undrained".to_string(),
        })
        .await
        .assured("the receiver remains alive and its one slot starts empty");

    let delivery = tokio::spawn(async move {
        if reader.route(notice_frame("also undrained")).await == ReaderFlow::End {
            return;
        }
        reader
            .route(reply_frame(
                command_id,
                command_reply(completed(), "executed"),
            ))
            .await;
    });

    let received = tokio::time::timeout(Duration::from_millis(50), command_request.reply).await;
    delivery.abort();
    let Ok(Ok(ReplyBody::Command(command))) = received else {
        panic!("the command response stalled behind an undrained event consumer");
    };
    assert_eq!(command.message, "executed");
}

#[tokio::test]
async fn request_identities_start_at_one_and_are_never_reused() {
    let mut pending = PendingReplies::new();
    let first = pending
        .register()
        .assured("an open exchange registers requests");
    let second = pending
        .register()
        .assured("an open exchange registers requests");
    assert_eq!(first.request_id, request_id(1));
    assert_eq!(second.request_id, request_id(2));

    assert!(pending.take(second.request_id).is_some());
    let third = pending
        .register()
        .assured("an open exchange registers requests");
    assert_eq!(
        third.request_id,
        request_id(3),
        "an identity whose request completed is not handed out again"
    );
}

#[tokio::test]
async fn an_exchange_that_used_every_identity_takes_no_further_requests() {
    let mut pending = PendingReplies::Open {
        next_request_id: Some(RequestId::new(NonZeroU64::MAX)),
        waiters: ahash::HashMap::default(),
    };
    let last = pending
        .register()
        .assured("the last identity is still free");
    assert_eq!(last.request_id, RequestId::new(NonZeroU64::MAX));
    assert!(pending.register().is_none());
}

#[tokio::test]
async fn closing_an_exchange_drops_every_waiter_once() {
    let mut pending = PendingReplies::new();
    let first = pending
        .register()
        .assured("an open exchange registers requests");
    let second = pending
        .register()
        .assured("an open exchange registers requests");

    pending.close();

    assert!(first.reply.await.is_err(), "the first waiter is dropped");
    assert!(second.reply.await.is_err(), "the second waiter is dropped");
    assert!(pending.register().is_none());
    assert!(pending.take(first.request_id).is_none());
}

#[tokio::test]
async fn a_reply_no_request_waits_for_is_dropped() {
    let mut fixture = reader_fixture(4);
    let mut waiting = register(&fixture.pending).await;

    let flow = fixture
        .reader
        .route(reply_frame(
            request_id(7),
            command_reply(completed(), "executed"),
        ))
        .await;

    assert_eq!(flow, ReaderFlow::Continue);
    assert!(
        waiting.reply.try_recv().is_err(),
        "a reply naming another identity leaves the waiting request untouched"
    );
    assert!(
        fixture
            .pending
            .lock()
            .await
            .take(waiting.request_id)
            .is_some()
    );
}

#[tokio::test]
async fn a_transferred_reply_completes_its_waiter() {
    let defaults = SessionLimits::DEFAULT;
    let size = |value: usize| NonZeroUsize::new(value).assured("a non-zero test limit");
    let small_frames = SessionLimits::try_from(SessionLimitSettings {
        frame_bytes: size(2048),
        transfer_bytes: size(defaults.transfer_bytes()),
        nesting_depth: size(defaults.nesting_depth()),
        collection_entries: size(defaults.collection_entries()),
        string_bytes: size(defaults.string_bytes()),
    })
    .assured("the test limits pass their checks");
    let mut fixture = reader_fixture(4);
    let request = register(&fixture.pending).await;
    let message = "x".repeat(10_000);
    let delivery = Reply {
        request_id: request.request_id,
        body: command_reply(completed(), &message),
    }
    .encode(&small_frames)
    .assured("the reply fits the transfer limit");
    let ReplyDelivery::Transfer(parts) = delivery else {
        panic!("a reply five times the frame limit is transferred in parts");
    };
    assert!(parts.len() > 1);

    for part in parts {
        assert_eq!(
            fixture.reader.route(verified(part)).await,
            ReaderFlow::Continue
        );
    }

    let Ok(ReplyBody::Command(outcome)) = request.reply.await else {
        panic!("the reassembled reply completes the waiter");
    };
    assert_eq!(outcome.message, message);
}

#[tokio::test]
async fn subscription_rows_render_against_the_schema_their_subscription_announced() {
    let mut fixture = reader_fixture(8);
    let request = register(&fixture.pending).await;
    let live = subscription("live", 1);

    fixture
        .reader
        .route(reply_frame(request.request_id, opened_reply(live.clone())))
        .await;
    fixture
        .reader
        .route(rows_frame(live.clone(), &[(1, "a"), (2, "b")]))
        .await;
    fixture
        .reader
        .route(rows_frame(subscription("live", 2), &[(3, "stale")]))
        .await;
    fixture.reader.route(ended_frame(live.clone())).await;
    fixture
        .reader
        .route(rows_frame(live.clone(), &[(4, "after the end")]))
        .await;

    let Ok(ReplyBody::Subscribe(opened)) = request.reply.await else {
        panic!("the subscribe reply completes its waiter");
    };
    assert!(matches!(
        opened.disposition,
        SubscribeDisposition::Opened(_)
    ));
    let Ok(SubscriptionEvent::Rows(rows)) = fixture.subscription_events.try_recv() else {
        panic!("the rows of the opened subscription are delivered");
    };
    assert_eq!(rows.relay.as_str(), "orders");
    assert_eq!(rows.rows.subscription(), &live);
    assert_eq!(
        rows.display_lines()
            .assured("the rows follow the announced schema"),
        ["{\"id\":1,\"name\":\"a\"}", "{\"id\":2,\"name\":\"b\"}"]
    );
    let Ok(SubscriptionEvent::Ended(ended)) = fixture.subscription_events.try_recv() else {
        panic!("the end of the subscription is delivered, and the stale generation's rows are not");
    };
    assert_eq!(ended.subscription, live);
    assert!(
        fixture.subscription_events.try_recv().is_err(),
        "rows of a subscription that ended are dropped"
    );
}

#[tokio::test]
async fn an_unsubscribe_reply_stops_the_subscription_rows() {
    let mut fixture = reader_fixture(8);
    let subscribe = register(&fixture.pending).await;
    let unsubscribe = register(&fixture.pending).await;
    let live = subscription("live", 1);

    fixture
        .reader
        .route(reply_frame(
            subscribe.request_id,
            opened_reply(live.clone()),
        ))
        .await;
    fixture
        .reader
        .route(reply_frame(
            unsubscribe.request_id,
            ReplyBody::Unsubscribe(UnsubscribeOutcome {
                disposition: UnsubscribeDisposition::Deleted(live.clone()),
                message: "subscription deleted".to_string(),
                diagnostics: Vec::new(),
            }),
        ))
        .await;
    fixture.reader.route(rows_frame(live, &[(1, "late")])).await;

    assert!(unsubscribe.reply.await.is_ok());
    assert!(
        fixture.subscription_events.try_recv().is_err(),
        "rows of a deleted subscription are dropped"
    );
}

#[tokio::test]
async fn observations_keep_only_their_latest_value() {
    let mut fixture = reader_fixture(4);
    let leader = |name: &str| {
        LeadershipObserved {
            leadership: Leadership::ServingNode(
                ClusterNodeName::parse(name).assured("the test node name is valid"),
            ),
        }
        .encode(&SESSION_LIMITS)
        .assured("a leadership observation fits a frame")
    };
    let domains = |names: &[&str]| {
        DomainsObserved {
            domains: names
                .iter()
                .map(|name| DomainInfo {
                    domain: domain(name),
                    status: DomainStatus::Running,
                    pace: DomainPace::Unpaced,
                })
                .collect(),
        }
        .encode(&SESSION_LIMITS)
        .assured("a domain observation fits a frame")
    };

    for frame in [
        leader("node-1"),
        domains(&["orders"]),
        leader("node-2"),
        domains(&["orders", "billing"]),
    ] {
        assert_eq!(
            fixture.reader.route(verified(frame)).await,
            ReaderFlow::Continue
        );
    }

    assert_eq!(
        Option::clone(&fixture.leadership.borrow()),
        Some(Leadership::ServingNode(
            ClusterNodeName::parse("node-2").assured("the test node name is valid")
        ))
    );
    assert!(
        fixture
            .domains
            .has_changed()
            .assured("the domain sink is alive")
    );
    let latest = Option::clone(&fixture.domains.borrow_and_update())
        .verified("the reader published a domain list");
    assert_eq!(latest.len(), 2);
}

#[tokio::test]
async fn the_client_reads_the_latest_observations_of_its_exchange() {
    let client = test_client("tenant");
    let pending = Arc::new(Mutex::new(PendingReplies::new()));
    let mut reader = ExchangeReader::new(pending, client.inner.events.sinks.clone());
    assert_eq!(client.leadership(), None);

    let observed = LeadershipObserved {
        leadership: Leadership::Unknown,
    }
    .encode(&SESSION_LIMITS)
    .assured("a leadership observation fits a frame");
    reader.route(verified(observed)).await;
    for names in [vec!["orders"], vec!["orders", "billing"]] {
        let observed = DomainsObserved {
            domains: names
                .iter()
                .map(|name| DomainInfo {
                    domain: domain(name),
                    status: DomainStatus::Stopped,
                    pace: DomainPace::Unpaced,
                })
                .collect(),
        }
        .encode(&SESSION_LIMITS)
        .assured("a domain observation fits a frame");
        reader.route(verified(observed)).await;
    }

    assert_eq!(client.leadership(), Some(Leadership::Unknown));
    let domains = tokio::time::timeout(DEADLINE, client.next_domain_list())
        .await
        .assured("an observed list is returned at once")
        .assured("the client's domain sink is alive");
    assert_eq!(
        domains.len(),
        2,
        "a caller that reads late gets the latest list, not every list in between"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.next_domain_list())
            .await
            .is_err(),
        "the next call waits for the next observation"
    );
}

#[tokio::test]
async fn a_session_ending_frame_closes_the_waiters_of_its_exchange() {
    let fixture = reader_fixture(4);
    let request = register(&fixture.pending).await;
    let ending = SessionEnding {
        reason: SessionEndReason::ServerShuttingDown,
    }
    .encode(&SESSION_LIMITS)
    .assured("a session ending fits a frame");
    let frames = tokio_stream::iter([
        Ok(verified(ending)),
        Ok(reply_frame(
            request.request_id,
            command_reply(completed(), "too late"),
        )),
    ]);

    fixture.reader.run(frames).await;

    assert!(
        request.reply.await.is_err(),
        "no reply follows a session ending"
    );
}

#[test]
fn subscription_query_is_rendered() {
    let request = SubscriptionRequest::new("live_orders", "orders");
    assert_eq!(
        request.to_query(),
        "CREATE SUBSCRIPTION live_orders TO orders;"
    );

    let sampled = SubscriptionRequest::new("sampled_orders", "orders")
        .dropping()
        .with_batch_sample_rate("0.1")
        .with_where_clause(
            nervix_nspl::parse_expression("input.tenant = \"acme\"")
                .expect("valid subscription expression"),
        );
    assert_eq!(
        sampled.to_query(),
        "CREATE SUBSCRIPTION sampled_orders TO orders DROPPING BATCH SAMPLE RATE 0.1 WHERE \
         input.tenant = 'acme';"
    );

    assert_eq!(
        nervix_nspl::subscribe::delete_subscription_query("sampled_orders"),
        "DELETE SUBSCRIPTION sampled_orders;"
    );
}

#[tokio::test]
async fn client_domain_can_be_updated() {
    let client = test_client("tenant_a");
    assert_eq!(client.domain().await, Some(domain("tenant_a")));
    client.set_domain(Some(domain("tenant_b"))).await;
    assert_eq!(client.domain().await, Some(domain("tenant_b")));
    client.set_domain(None).await;
    assert_eq!(client.domain().await, None);
}

#[test]
fn wire_outcomes_convert_into_public_outcomes() {
    let leader = LeaderEndpoints {
        node: ClusterNodeName::parse("node-2").assured("the test node name is valid"),
        grpc_uri: Some(Url::parse("http://127.0.0.1:47393").assured("a literal URL")),
        web_console_uri: None,
    };
    let mut refused = wire_outcome(
        "command-1",
        CommandDisposition::NotLeader(LeaderRedirect {
            leader: Some(leader.clone()),
        }),
        "parse failed",
    );
    refused.diagnostics = vec![Diagnostic {
        message: "bad token".to_string(),
        span: Some(SourceSpan::new(3, 7).assured("the span starts before it ends")),
    }];
    refused.transaction = Some(open_transaction("tx-1", 2));

    let outcome = CommandOutcome::from(refused);

    assert!(!outcome.succeeded());
    assert!(!outcome.already_existed());
    assert_eq!(outcome.execution_reference, Some(reference("command-1")));
    assert_eq!(outcome.origin, Some(OutcomeOrigin::Executed));
    assert_eq!(
        outcome.disposition,
        CommandDisposition::NotLeader(LeaderRedirect {
            leader: Some(leader)
        })
    );
    assert_eq!(outcome.message, "parse failed");
    assert_eq!(outcome.diagnostics.len(), 1);
    assert_eq!(outcome.transaction, Some(open_transaction("tx-1", 2)));
    assert_eq!(outcome.subscription, None);
    assert_eq!(outcome.resource_upload, None);

    let existing = CommandOutcome::from(wire_outcome(
        "command-2",
        CommandDisposition::Completed {
            already_existed: true,
        },
        "exists",
    ));
    assert!(existing.succeeded());
    assert!(existing.already_existed());

    let server = ServerEvent::from(ServerNotice {
        level: NoticeLevel::Warning,
        message: "watch out".to_string(),
    });
    assert_eq!(
        server,
        ServerEvent {
            level: NoticeLevel::Warning,
            message: "watch out".to_string(),
        }
    );
}

#[test]
fn an_attach_to_a_finished_transaction_reports_its_final_status_without_holding_it() {
    let committed = TransactionStatus::new(
        "tx-1".to_string(),
        domain("tenant"),
        TransactionLifecycle::Committed,
        TransactionPosition::new(1),
        1,
    )
    .assured("a committed transaction that applied everything is consistent");

    let outcome = CommandOutcome::from(AttachOutcome {
        disposition: AttachDisposition::AlreadyFinished(committed.clone()),
        message: "transaction 'tx-1' finished with outcome COMMITTED".to_string(),
        diagnostics: Vec::new(),
    });

    assert!(!outcome.succeeded());
    assert_eq!(outcome.transaction, Some(committed));
    assert_eq!(
        outcome.message,
        "transaction 'tx-1' finished with outcome COMMITTED"
    );
}

#[test]
fn reconnect_candidates_prefer_non_current_servers() {
    let url = |raw: &str| Url::parse(raw).assured("a literal URL");
    let mut servers = ServerDirectory::connected_to(Some(url("http://node-1")));
    servers.connected(&url("http://node-2"));
    servers.remember(&url("http://node-3"));
    servers.remember(&url("http://node-1"));

    assert_eq!(
        servers.reconnect_candidates(),
        vec![
            url("http://node-1"),
            url("http://node-3"),
            url("http://node-2"),
        ]
    );
}

#[test]
fn replies_ask_for_the_routing_their_disposition_needs() {
    let uri = Url::parse("http://node-2").assured("a literal URL");
    let node = ClusterNodeName::parse("node-2").assured("the test node name is valid");
    let routing_of = |disposition: CommandDisposition| {
        let outcome = CommandOutcome::from(wire_outcome("command-1", disposition, "routed"));
        match outcome.routing() {
            Routing::Complete => "complete".to_string(),
            Routing::Redirect(leader) => format!("redirect {leader}"),
            Routing::AwaitElection => "await election".to_string(),
            Routing::Detached => "detached".to_string(),
            Routing::AwaitOutcome => "await outcome".to_string(),
        }
    };

    assert_eq!(
        routing_of(CommandDisposition::NotLeader(LeaderRedirect {
            leader: None
        })),
        "await election"
    );
    assert_eq!(
        routing_of(CommandDisposition::NotLeader(LeaderRedirect {
            leader: Some(LeaderEndpoints {
                node: node.clone(),
                grpc_uri: None,
                web_console_uri: None,
            }),
        })),
        "await election",
        "a leader that advertises no session URI cannot be followed"
    );
    assert_eq!(
        routing_of(CommandDisposition::NotLeader(LeaderRedirect {
            leader: Some(LeaderEndpoints {
                node,
                grpc_uri: Some(uri.clone()),
                web_console_uri: None,
            }),
        })),
        format!("redirect {uri}")
    );
    assert_eq!(
        routing_of(CommandDisposition::TransactionDetached {
            transaction_id: "tx-1".to_string(),
        }),
        "detached"
    );
    assert_eq!(
        routing_of(CommandDisposition::OutcomeUnknown(
            UnknownOutcomeCause::LeadershipLost
        )),
        "await outcome"
    );
    assert_eq!(routing_of(CommandDisposition::Failed), "complete");
    assert_eq!(
        routing_of(CommandDisposition::ExecutionReferenceExpired),
        "complete"
    );
}

#[tokio::test]
async fn execute_returns_session_closed_when_request_channel_is_closed() {
    let client = test_client("tenant_a");
    let error = client
        .execute("SHOW CLUSTER STATUS;")
        .await
        .expect_err("must fail");
    assert!(matches!(error, ClientError::SessionClosed));
}

#[tokio::test]
async fn execute_rejects_mixed_client_local_multi_statement_request() {
    let client = test_client("default");
    let outcome = client
        .execute("USE prod; CREATE DOMAIN prod;")
        .await
        .assured("a client-local multi-statement rejection does not use the network");

    assert!(!outcome.succeeded());
    assert_eq!(
        outcome.message,
        "client-local commands must be executed separately"
    );
}

#[tokio::test]
async fn execute_rejects_subscription_statements_in_a_batch() {
    let client = test_client("default");
    let outcome = client
        .execute("CREATE SUBSCRIPTION live TO orders; SHOW CLUSTER STATUS;")
        .await
        .assured("a batched subscription rejection does not use the network");

    assert!(!outcome.succeeded());
    assert_eq!(
        outcome.message,
        "subscription commands must be executed separately"
    );
}

#[tokio::test]
async fn execute_rejects_client_local_command_during_transaction() {
    let client = test_client("default");
    *client.inner.transaction.lock().await = Some(open_transaction("tx-1", 0));

    let outcome = client
        .execute("USE prod;")
        .await
        .assured("a client-local transaction rejection does not use the network");

    assert!(!outcome.succeeded());
    assert_eq!(
        outcome.message,
        "client-local commands are not allowed while a transaction is active"
    );
}

#[tokio::test]
async fn use_domain_is_served_by_the_client() {
    let client = test_client("default");
    let outcome = client
        .execute("USE prod;")
        .await
        .assured("USE does not use the network");

    assert!(outcome.succeeded());
    assert_eq!(outcome.message, "using domain 'prod'");
    assert_eq!(outcome.execution_reference, None);
    assert_eq!(client.domain().await, Some(domain("prod")));
}

#[tokio::test]
async fn a_subscription_needs_a_selected_domain() {
    let (frames, outbound) = mpsc::channel(1);
    drop(outbound);
    let pending = Arc::new(Mutex::new(PendingReplies::new()));
    let client = client_on(detached_exchange(frames, pending), None);

    let error = client
        .subscribe(&SubscriptionRequest::new("live", "orders"))
        .await
        .expect_err("a subscription is refused without a domain");

    assert!(matches!(error, ClientError::NoActiveDomain));
}

#[tokio::test]
async fn a_create_subscription_statement_is_sent_as_a_subscribe_request() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let execution = tokio::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });

    let request = loopback.next_request().await;
    let ClientRequest::Subscribe(subscribe) = request.request else {
        panic!("a CREATE SUBSCRIPTION statement is sent as a subscribe request");
    };
    assert_eq!(subscribe.domain, domain("tenant"));
    assert_eq!(subscribe.statement, "CREATE SUBSCRIPTION live TO orders;");
    assert_eq!(subscribe.subscription_type, SubscriptionType::Row);
    loopback
        .answer(request.request_id, opened_reply(subscription("live", 1)))
        .await;

    let outcome = execution
        .await
        .assured("the subscribe task completes")
        .assured("the subscription opens");
    assert!(outcome.succeeded());
    assert_eq!(outcome.execution_reference, None);
    assert_eq!(outcome.message, "subscription opened");
    let opened = outcome
        .subscription
        .verified("an opened subscription is reported");
    assert_eq!(opened.subscription, subscription("live", 1));
    assert_eq!(opened.schema, orders_schema());
}

#[tokio::test]
async fn subscription_diagnostics_address_the_query_the_caller_passed() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let execution = tokio::spawn(async move {
        client
            .execute("  // live orders\n  CREATE SUBSCRIPTION live TO order;")
            .await
    });

    let request = loopback.next_request().await;
    let ClientRequest::Subscribe(subscribe) = request.request else {
        panic!("a CREATE SUBSCRIPTION statement is sent as a subscribe request");
    };
    assert_eq!(subscribe.statement, "CREATE SUBSCRIPTION live TO order;");
    // `order` spans bytes 28..33 of the statement the request carried.
    loopback
        .answer(
            request.request_id,
            ReplyBody::Subscribe(SubscribeOutcome {
                disposition: SubscribeDisposition::Failed,
                message: "relay 'order' does not exist".to_string(),
                diagnostics: vec![
                    Diagnostic {
                        message: "unknown relay".to_string(),
                        span: Some(SourceSpan::new(28, 33).assured("the span is ordered")),
                    },
                    Diagnostic {
                        message: "no location".to_string(),
                        span: None,
                    },
                ],
            }),
        )
        .await;

    let outcome = execution
        .await
        .assured("the subscribe task completes")
        .assured("the reply arrives");
    assert!(!outcome.succeeded());
    let query = "  // live orders\n  CREATE SUBSCRIPTION live TO order;";
    let span = outcome.diagnostics[0]
        .span
        .verified("the located diagnostic keeps a location");
    let start = usize::try_from(span.start()).assured("a u32 offset fits usize");
    let end = usize::try_from(span.end()).assured("a u32 offset fits usize");
    assert_eq!(&query[start..end], "order");
    assert_eq!(outcome.diagnostics[1].span, None);
}

#[tokio::test]
async fn a_delete_subscription_statement_is_sent_as_an_unsubscribe_request() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let execution = tokio::spawn(async move { client.unsubscribe("live").await });

    let request = loopback.next_request().await;
    let ClientRequest::Unsubscribe(unsubscribe) = request.request else {
        panic!("a DELETE SUBSCRIPTION statement is sent as an unsubscribe request");
    };
    assert_eq!(unsubscribe.subscription.as_str(), "live");
    loopback
        .answer(
            request.request_id,
            ReplyBody::Unsubscribe(UnsubscribeOutcome {
                disposition: UnsubscribeDisposition::Failed,
                message: "subscription 'live' does not exist".to_string(),
                diagnostics: Vec::new(),
            }),
        )
        .await;

    let outcome = execution
        .await
        .assured("the unsubscribe task completes")
        .assured("the reply arrives");
    assert!(!outcome.succeeded());
    assert_eq!(outcome.message, "subscription 'live' does not exist");
}

#[tokio::test]
async fn a_command_carries_the_expectation_of_the_attached_transaction() {
    let mut loopback = Loopback::new(Some(domain("default")));
    loopback
        .client
        .adopt_transaction_status(open_transaction("tx-1", 2))
        .await;
    *loopback.client.inner.commit_basis.lock().await = Some(test_preview("tx-1", 2));
    let client = loopback.client.clone();
    let execution = tokio::spawn(async move { client.execute("COMMIT;").await });

    let request = loopback.next_request().await;
    let ClientRequest::Command(command) = request.request else {
        panic!("COMMIT is sent as a command");
    };
    assert_eq!(command.query, "COMMIT;");
    assert_eq!(
        command.domain,
        Some(domain("tenant")),
        "the session follows the domain of its transaction"
    );
    assert_eq!(
        command.expected_transaction_position,
        Some(TransactionPosition::new(2))
    );
    assert_eq!(command.expected_preview, Some(test_preview("tx-1", 2)));
    loopback
        .answer(
            request.request_id,
            ReplyBody::Command(Box::new(wire_outcome(
                command.execution_reference.as_str(),
                completed(),
                "committed",
            ))),
        )
        .await;

    let outcome = execution
        .await
        .assured("the command task completes")
        .assured("the command completes");
    assert!(outcome.succeeded());
    assert_eq!(
        outcome.execution_reference,
        Some(command.execution_reference)
    );
}

#[tokio::test]
async fn an_unknown_outcome_is_recovered_with_the_same_execution_reference() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let execution = tokio::spawn(async move { client.execute("CREATE DOMAIN orders;").await });

    let first = loopback.next_request().await;
    let ClientRequest::Command(first_command) = first.request else {
        panic!("the statement is sent as a command");
    };
    loopback
        .answer(
            first.request_id,
            ReplyBody::Command(Box::new(wire_outcome(
                first_command.execution_reference.as_str(),
                CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::StillApplying),
                "still applying",
            ))),
        )
        .await;
    let second = loopback.next_request().await;
    let ClientRequest::Command(second_command) = second.request else {
        panic!("the retry is sent as a command");
    };
    assert_ne!(second.request_id, first.request_id);
    assert_eq!(
        second_command.execution_reference, first_command.execution_reference,
        "a retry repeats the execution reference so the admitted command is recovered"
    );
    let mut recovered = wire_outcome(
        second_command.execution_reference.as_str(),
        completed(),
        "domain created",
    );
    recovered.origin = OutcomeOrigin::Recovered;
    loopback
        .answer(second.request_id, ReplyBody::Command(Box::new(recovered)))
        .await;

    let outcome = execution
        .await
        .assured("the command task completes")
        .assured("the command completes");
    assert!(outcome.succeeded());
    assert_eq!(outcome.origin, Some(OutcomeOrigin::Recovered));
}

#[tokio::test]
async fn a_detached_transaction_is_attached_again_before_the_command_is_retried() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    loopback
        .client
        .adopt_transaction_status(open_transaction("tx-1", 1))
        .await;
    let client = loopback.client.clone();
    let execution = tokio::spawn(async move { client.execute("CREATE DOMAIN orders;").await });

    let first = loopback.next_request().await;
    let ClientRequest::Command(first_command) = first.request else {
        panic!("the statement is sent as a command");
    };
    loopback
        .answer(
            first.request_id,
            ReplyBody::Command(Box::new(wire_outcome(
                first_command.execution_reference.as_str(),
                CommandDisposition::TransactionDetached {
                    transaction_id: "tx-1".to_string(),
                },
                "detached",
            ))),
        )
        .await;
    let attach = loopback.next_request().await;
    let ClientRequest::AttachTransaction(attach_request) = attach.request else {
        panic!("the session attaches its transaction again");
    };
    assert_eq!(attach_request.transaction_id, "tx-1");
    loopback
        .answer(
            attach.request_id,
            ReplyBody::Attach(AttachOutcome {
                disposition: AttachDisposition::Attached(open_transaction("tx-1", 1)),
                message: "attached".to_string(),
                diagnostics: Vec::new(),
            }),
        )
        .await;
    let retry = loopback.next_request().await;
    let ClientRequest::Command(retry_command) = retry.request else {
        panic!("the command is sent again");
    };
    assert_eq!(
        retry_command.execution_reference,
        first_command.execution_reference
    );
    loopback
        .answer(
            retry.request_id,
            ReplyBody::Command(Box::new(wire_outcome(
                retry_command.execution_reference.as_str(),
                completed(),
                "queued",
            ))),
        )
        .await;

    let outcome = execution
        .await
        .assured("the command task completes")
        .assured("the command completes");
    assert!(outcome.succeeded());
}

#[tokio::test]
async fn a_rejected_request_surfaces_as_a_typed_error() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let listing = tokio::spawn(async move { client.list_domains().await });

    let request = loopback.next_request().await;
    assert!(matches!(request.request, ClientRequest::ListDomains));
    loopback
        .answer(
            request.request_id,
            ReplyBody::Rejected(RequestRejected {
                rejection: RequestRejection::TooManyRequestsInFlight,
                field: None,
                message: "too many requests in flight".to_string(),
            }),
        )
        .await;

    let error = listing
        .await
        .assured("the listing task completes")
        .expect_err("a rejected request fails");
    let ClientError::RequestRejected {
        request,
        rejection,
        field,
        message,
    } = error
    else {
        panic!("the rejection is reported as such, not as {error:?}");
    };
    assert_eq!(request, RequestKind::ListDomains);
    assert_eq!(rejection, RequestRejection::TooManyRequestsInFlight);
    assert_eq!(field, None);
    assert_eq!(message, "too many requests in flight");
}

#[tokio::test]
async fn list_domains_is_served_from_a_domain_list_request() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let execution = tokio::spawn(async move { client.execute("LIST DOMAINS;").await });

    let request = loopback.next_request().await;
    assert!(matches!(request.request, ClientRequest::ListDomains));
    loopback
        .answer(request.request_id, domain_list_reply())
        .await;

    let outcome = execution
        .await
        .assured("the listing task completes")
        .assured("the listing completes");
    assert!(outcome.succeeded());
    assert_eq!(
        outcome.message,
        "domains:\norders pace=UNPACED status=RUNNING"
    );
}

#[tokio::test]
async fn next_event_calls_return_session_closed_when_channels_are_closed() {
    let client = test_client("tenant_a");
    client.inner.events.subscriptions.lock().await.close();
    client.inner.events.notices.lock().await.close();
    let subscription_error = client
        .next_subscription()
        .await
        .expect_err("must fail once channel is closed");
    assert!(matches!(subscription_error, ClientError::SessionClosed));

    let server_error = client
        .next_server_event()
        .await
        .expect_err("must fail once channel is closed");
    assert!(matches!(server_error, ClientError::SessionClosed));
}

#[cfg(feature = "autocomplete")]
#[tokio::test]
async fn suggest_returns_session_closed_when_request_channel_is_closed() {
    let client = test_client("tenant_a");
    let error = client.suggest("CREATE ", 7).await.expect_err("must fail");
    assert!(matches!(error, ClientError::SessionClosed));
}

#[cfg(feature = "autocomplete")]
#[tokio::test]
async fn suggest_refuses_a_cursor_inside_a_character() {
    let client = test_client("tenant_a");
    let error = client.suggest("é", 1).await.expect_err("must fail");
    assert!(matches!(
        error,
        ClientError::InvalidCursor {
            cursor: 1,
            length: 2
        }
    ));
}

#[test]
fn expand_user_path_resolves_home_prefix() {
    let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
        return;
    };
    assert_eq!(expand_user_path(Path::new("~")), home);
    assert_eq!(expand_user_path(Path::new("~/proto")), home.join("proto"));
    assert_eq!(
        expand_user_path(Path::new("/tmp/proto")),
        PathBuf::from("/tmp/proto")
    );
}

#[test]
fn upload_retries_transport_statuses_that_can_hide_an_installed_outcome() {
    for code in [
        tonic::Code::Cancelled,
        tonic::Code::Unknown,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Unavailable,
    ] {
        assert!(upload_status_is_retryable(&tonic::Status::new(
            code, "lost"
        )));
    }
    assert!(!upload_status_is_retryable(
        &tonic::Status::invalid_argument("permanent",)
    ));
}
