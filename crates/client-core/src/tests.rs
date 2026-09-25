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
    UploadDisposition, UploadFailure, UploadReply, VerifiedFrame, WireDecodeError,
};
use nervix_models::{
    ClusterNodeName, CommandExecutionReference, DomainName, DomainPace, DomainStatus, FieldName,
    ImpactPlanningBasis, ImpactReportCompleteness, ParseAsType, RelayName, ResourceDescription,
    ResourceName, ResourceUploadIdentity, SchemaField, SubscriptionName, TransactionImpactReport,
    TransactionInspection, TransactionLifecycle, TransactionOperationNumber, TransactionPosition,
    TransactionPreviewIdentity, TransactionStatus,
};
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use tonic::{Status, transport::Channel};
use triomphe::Arc;
use url::Url;

use crate::{
    Client, ClientError, CommandOutcome, ConnectOptions, RequestKind, ResourceUploadOutcome,
    ServerEvent, SubscriptionEvent, SubscriptionRequest, TlsRequirement,
    connection::{GrpcConnector, ServerDirectory},
    exchange::{
        EventQueue, EventQueueError, EventSinks, Exchange, ExchangeReader, ExchangeRequests,
        PendingReplies, ReaderFlow, RegisteredRequest, SERVER_NOTICE_BYTES, SESSION_LIMITS,
        SUBSCRIPTION_EVENT_BYTES, SessionEvents,
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

async fn cache_test_preview(client: &Client, transaction_id: &str, position: usize) {
    let preview = test_preview(transaction_id, position);
    let key = (preview.transaction_id.clone(), preview.position);
    client.inner.previews.lock().await.insert(key, preview);
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
        wasm_state: None,
        resource: None,
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
    let sinks = SessionEvents::new().sinks;
    let generation = sinks.begin_generation();
    Exchange {
        requests: Arc::new(ExchangeRequests {
            frames,
            pending,
            channel: Channel::from_static("http://127.0.0.1:9").connect_lazy(),
        }),
        reader: tokio::spawn(std::future::ready(())),
        sinks,
        generation,
    }
}

fn client_on(exchange: Exchange, session_domain: Option<DomainName>) -> Client {
    let connector = GrpcConnector::new(ConnectOptions::default())
        .assured("options without credentials build no authorization metadata");
    let sinks = exchange.sinks.clone();
    let events = SessionEvents {
        leadership: sinks.leadership.subscribe(),
        domains: tokio::sync::Mutex::new(sinks.domains.subscribe()),
        sinks,
    };
    Client::assemble(
        exchange,
        events,
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
            .take(request)
            .assured("the request waits for its reply");
        waiter
            .send(body)
            .assured("the client waits for the reply to its request");
    }

    async fn replace_exchange(&mut self) {
        let (frames, requests) = mpsc::channel(8);
        let pending = Arc::new(Mutex::new(PendingReplies::new()));
        let mut replacement = detached_exchange(frames, pending.clone());
        let sinks = self.client.inner.events.sinks.clone();
        replacement.generation = sinks.begin_generation();
        replacement.sinks = sinks;
        let mut current = self.client.inner.exchange.lock().await;
        let previous = std::mem::replace(&mut *current, replacement);
        drop(current);
        previous.close().await;
        self.requests = requests;
        self.pending = pending;
        let current = self.client.inner.exchange.lock().await;
        self.client
            .restore_subscriptions(current.generation.clone(), current.requests());
    }
}

/// An exchange reader over a fresh registry, with event queues of `capacity` entries.
struct ReaderFixture {
    reader: ExchangeReader,
    pending: Arc<Mutex<PendingReplies>>,
    subscription_events: EventQueue<SubscriptionEvent>,
    notices: EventQueue<ServerEvent>,
    leadership: watch::Receiver<Option<Leadership>>,
    domains: watch::Receiver<Option<Vec<DomainInfo>>>,
}

fn reader_fixture(capacity: usize) -> ReaderFixture {
    let subscriptions = EventQueue::new(capacity, SUBSCRIPTION_EVENT_BYTES);
    let notices = EventQueue::new(capacity, SERVER_NOTICE_BYTES);
    let (leadership, observed_leadership) = watch::channel(None);
    let (domains, observed_domains) = watch::channel(None);
    let pending = Arc::new(Mutex::new(PendingReplies::new()));
    let sinks = EventSinks {
        subscriptions: subscriptions.clone(),
        desired: crate::subscriptions::DesiredSubscriptions::new(),
        notices: notices.clone(),
        leadership,
        domains,
    };
    let generation = sinks.begin_generation();
    let reader = ExchangeReader::new(pending.clone(), sinks, generation);
    ReaderFixture {
        reader,
        pending,
        subscription_events: subscriptions,
        notices,
        leadership: observed_leadership,
        domains: observed_domains,
    }
}

async fn register(pending: &Mutex<PendingReplies>) -> RegisteredRequest {
    pending
        .lock()
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

#[test]
fn statement_splitting_reports_invalid_source_with_context() {
    let error = split_query_statements("CREATE SCHEMA ???;")
        .expect_err("an invalid schema declaration cannot be split");
    assert!(
        error
            .current_context()
            .to_string()
            .contains("failed to parse")
    );
}

#[tokio::test]
async fn a_commit_fences_against_the_basis_its_own_transaction_reported() {
    let client = test_client("tenant");
    client
        .adopt_transaction_status(open_transaction("tx-1", 1))
        .await;
    cache_test_preview(&client, "tx-1", 1).await;

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
    cache_test_preview(&client, "tx-1", 4).await;

    let expectation = client.transaction_expectation().await;

    assert_eq!(expectation.position, Some(TransactionPosition::new(1)));
    assert_eq!(
        expectation.preview, None,
        "a basis naming another transaction cannot decide this transaction's commit"
    );
}

#[tokio::test]
async fn a_refused_commit_does_not_adopt_an_unreviewed_basis() {
    let client = test_client("tenant");
    client
        .adopt_transaction_status(open_transaction("tx-1", 1))
        .await;
    cache_test_preview(&client, "tx-1", 1).await;
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
        Some(test_preview("tx-1", 1))
    );
}

#[tokio::test]
async fn inspection_refreshes_only_the_attached_transaction_at_its_queue_position() {
    let client = test_client("tenant");
    client
        .adopt_transaction_status(open_transaction("tx-bound", 0))
        .await;
    let mut described = wire_outcome("describe-1", completed(), "described");
    described.transaction = Some(open_transaction("tx-bound", 0));
    described.inspection = Some(Box::new(TransactionInspection {
        transaction: open_transaction("tx-other", 0),
        operation: None,
        report: empty_report(),
    }));
    client
        .record_commit_basis(&CommandOutcome::from(described.clone()))
        .await;
    assert_eq!(client.transaction_expectation().await.preview, None);
    let other_key = ("tx-other".to_string(), TransactionPosition::new(0));
    assert_eq!(
        client.inner.previews.lock().await.get(&other_key),
        Some(&test_preview("tx-other", 0))
    );

    described.inspection = Some(Box::new(TransactionInspection {
        transaction: open_transaction("tx-bound", 0),
        operation: None,
        report: empty_report(),
    }));
    client
        .record_commit_basis(&CommandOutcome::from(described))
        .await;
    assert_eq!(
        client.transaction_expectation().await.preview,
        Some(test_preview("tx-bound", 0))
    );
}

#[tokio::test]
async fn an_older_inspection_cannot_replace_a_newer_queue_preview() {
    let client = test_client("tenant");
    client
        .adopt_transaction_status(open_transaction("tx-bound", 1))
        .await;
    cache_test_preview(&client, "tx-bound", 1).await;
    let mut described = wire_outcome("describe-1", completed(), "described");
    described.inspection = Some(Box::new(TransactionInspection {
        transaction: open_transaction("tx-bound", 0),
        operation: None,
        report: empty_report(),
    }));

    client
        .record_commit_basis(&CommandOutcome::from(described))
        .await;

    assert_eq!(
        client.transaction_expectation().await.preview,
        Some(test_preview("tx-bound", 1))
    );
    assert_eq!(client.inner.previews.lock().await.len(), 1);
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
        fixture.pending.lock().register().is_none(),
        "an ended exchange takes no further requests"
    );
    assert!(
        fixture.notices.try_next().is_none(),
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
        ..ConnectOptions::default()
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
async fn saturated_event_consumer_cannot_block_a_command_reply() {
    let ReaderFixture {
        mut reader,
        pending,
        notices: _undrained_notices,
        ..
    } = reader_fixture(1);
    let command_request = register(&pending).await;
    let command_id = command_request.request_id;
    reader.route(notice_frame("undrained")).await;

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
async fn overflowing_a_notice_stream_is_reported_without_losing_replies() {
    let mut fixture = reader_fixture(1);
    let request = register(&fixture.pending).await;
    fixture.reader.route(notice_frame("first")).await;
    fixture.reader.route(notice_frame("second")).await;
    fixture
        .reader
        .route(reply_frame(
            request.request_id,
            command_reply(completed(), "complete"),
        ))
        .await;

    assert!(matches!(
        fixture.notices.next().await,
        Err(error) if *error.current_context() == EventQueueError::Overflow
    ));
    assert!(matches!(request.reply.await, Ok(ReplyBody::Command(_))));
}

#[tokio::test]
async fn overflowing_subscription_rows_cannot_delay_a_command_reply() {
    let mut fixture = reader_fixture(1);
    let subscribe = register(&fixture.pending).await;
    let command = register(&fixture.pending).await;
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
        .route(rows_frame(live.clone(), &[(1, "first")]))
        .await;
    fixture
        .reader
        .route(rows_frame(live, &[(2, "second")]))
        .await;
    fixture
        .reader
        .route(reply_frame(
            command.request_id,
            command_reply(completed(), "complete"),
        ))
        .await;

    assert!(matches!(
        fixture.subscription_events.next().await,
        Err(error) if *error.current_context() == EventQueueError::Overflow
    ));
    assert!(matches!(command.reply.await, Ok(ReplyBody::Command(_))));
}

#[test]
fn one_subscription_overflow_preserves_other_subscription_events() {
    let queue = EventQueue::for_subscriptions();
    let generation = Arc::new(());
    queue.begin(&generation);
    let full = subscription("full", 1);
    let healthy = subscription("healthy", 1);
    for _ in 0..32 {
        queue.push(
            &generation,
            SubscriptionEvent::DeliveryLost(nervix_client_wire::SubscriptionDeliveryLost {
                subscription: full.clone(),
                dropped_rows: NonZeroU64::MIN,
            }),
            1,
        );
    }
    queue.push(
        &generation,
        SubscriptionEvent::DeliveryLost(nervix_client_wire::SubscriptionDeliveryLost {
            subscription: healthy.clone(),
            dropped_rows: NonZeroU64::MIN,
        }),
        1,
    );
    queue.push(
        &generation,
        SubscriptionEvent::DeliveryLost(nervix_client_wire::SubscriptionDeliveryLost {
            subscription: full.clone(),
            dropped_rows: NonZeroU64::MIN,
        }),
        1,
    );
    let Some(SubscriptionEvent::ConsumerOverflow(overflowed)) = queue.try_next() else {
        panic!("the full subscription reports terminal consumer overflow");
    };
    assert_eq!(overflowed, full);
    let Some(SubscriptionEvent::DeliveryLost(delivered)) = queue.try_next() else {
        panic!("the other subscription retains its event");
    };
    assert_eq!(delivered.subscription, healthy);
    assert!(queue.try_next().is_none());
}

#[tokio::test]
async fn event_queue_counts_retained_bytes_as_well_as_records() {
    let notices = EventQueue::new(10, 128);
    let subscriptions = EventQueue::new(10, 128);
    let (leadership, _) = watch::channel(None);
    let (domains, _) = watch::channel(None);
    let sinks = EventSinks {
        subscriptions,
        desired: crate::subscriptions::DesiredSubscriptions::new(),
        notices: notices.clone(),
        leadership,
        domains,
    };
    let generation = sinks.begin_generation();
    notices.push(
        &generation,
        ServerEvent {
            level: NoticeLevel::Info,
            message: "oversized".to_string(),
        },
        129,
    );

    assert!(matches!(
        notices.next().await,
        Err(error) if *error.current_context() == EventQueueError::Overflow
    ));
}

#[tokio::test]
async fn closing_a_failed_exchange_preserves_its_grpc_status_for_waiters() {
    let fixture = reader_fixture(1);
    let (frames, _) = mpsc::channel(1);
    let exchange = ExchangeRequests {
        frames,
        pending: fixture.pending.clone(),
        channel: Channel::from_static("http://127.0.0.1:9").connect_lazy(),
    };
    let mut request = exchange.register().assured("the exchange is open");
    fixture
        .reader
        .run(tokio_stream::iter([Err(Status::unauthenticated(
            "credentials were rejected",
        ))]))
        .await;
    assert!(request.receive().await.is_none());
    let ClientError::Transport(status) = exchange.pending.lock().failure() else {
        panic!("the transport status must reach its waiter");
    };
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert_eq!(status.message(), "credentials were rejected");
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
async fn connection_options_reject_unbounded_seeds_and_deadlines_before_connecting() {
    let endpoint = Url::parse("http://127.0.0.1:9").assured("the test endpoint is a URL");
    let too_many_seeds = ConnectOptions {
        seed_servers: vec![endpoint.clone(); 33],
        ..ConnectOptions::default()
    };
    let Err(error) = Client::connect_with_options(endpoint.as_str(), None, too_many_seeds).await
    else {
        panic!("too many seeds are rejected before connecting");
    };
    assert!(matches!(
        error,
        ClientError::TooManySeedServers { count: 33 }
    ));

    let invalid_deadline = ConnectOptions {
        request_timeout: Duration::ZERO,
        ..ConnectOptions::default()
    };
    let Err(error) = Client::connect_with_options(endpoint.as_str(), None, invalid_deadline).await
    else {
        panic!("a zero request deadline is rejected before connecting");
    };
    assert!(matches!(
        error,
        ClientError::InvalidDeadline {
            field: "request_timeout"
        }
    ));

    let insecure_seed = ConnectOptions {
        seed_servers: vec![endpoint],
        ..ConnectOptions::default()
    };
    let Err(error) = Client::connect_with_options("https://127.0.0.1:9", None, insecure_seed).await
    else {
        panic!("a TLS session rejects a plaintext recovery seed");
    };
    assert!(matches!(error, ClientError::TlsRequired));
}

#[tokio::test]
async fn a_prepared_execution_keeps_its_identity_domain_and_upload_identity() {
    let client = test_client("tenant");
    let execution = client
        .prepare_execution("CREATE SCHEMA record (id I64);")
        .await;
    assert_eq!(execution.domain(), Some(&domain("tenant")));
    assert!(!execution.reference().as_str().is_empty());
    assert!(!execution.upload_identity().as_str().is_empty());
    assert!(execution.can_have_admitted_command());
    let invalid = client.prepare_execution("???").await;
    assert!(invalid.can_have_admitted_command());
    let local = client.prepare_execution("USE prod;").await;
    assert!(!local.can_have_admitted_command());
}

#[test]
fn transport_failure_classification_preserves_uncertainty_and_authentication() {
    let retryable = ClientError::Transport(Box::new(Status::unavailable("connection lost")));
    assert!(retryable.retryable_session_failure());
    assert!(retryable.can_hide_admitted_work());
    assert!(retryable.can_hide_installed_upload());
    let rejected = ClientError::Transport(Box::new(Status::unauthenticated("bad credentials")));
    assert!(!rejected.retryable_session_failure());
    assert!(!rejected.can_hide_admitted_work());
    let lost_upload =
        ClientError::UploadResource(Box::new(Status::deadline_exceeded("lost reply")));
    assert!(lost_upload.can_hide_installed_upload());
    let rejected_upload =
        ClientError::UploadResource(Box::new(Status::invalid_argument("bad upload")));
    assert!(!rejected_upload.can_hide_installed_upload());
    assert!(ClientError::RetryDeadline.can_hide_admitted_work());
    assert!(
        ClientError::RequestInterrupted {
            request: RequestKind::Command
        }
        .retryable_session_failure()
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
    assert!(fixture.pending.lock().take(waiting.request_id).is_some());
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
    let Some(SubscriptionEvent::Rows(rows)) = fixture.subscription_events.try_next() else {
        panic!("the rows of the opened subscription are delivered");
    };
    assert_eq!(rows.relay.as_str(), "orders");
    assert_eq!(rows.rows.subscription(), &live);
    assert_eq!(
        rows.display_lines()
            .assured("the rows follow the announced schema"),
        ["{\"id\":1,\"name\":\"a\"}", "{\"id\":2,\"name\":\"b\"}"]
    );
    let Some(SubscriptionEvent::Ended(ended)) = fixture.subscription_events.try_next() else {
        panic!("the end of the subscription is delivered, and the stale generation's rows are not");
    };
    assert_eq!(ended.subscription, live);
    assert!(
        fixture.subscription_events.try_next().is_none(),
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
        fixture.subscription_events.try_next().is_none(),
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
    let sinks = client.inner.events.sinks.clone();
    let generation = sinks.begin_generation();
    let mut reader = ExchangeReader::new(pending, sinks, generation);
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
    let description = ResourceDescription {
        resource: ResourceName::parse("bundle").assured("the test resource name is valid"),
        latest_version: None,
        versions: Vec::new(),
        usages: Vec::new(),
    };
    refused.resource = Some(Box::new(description.clone()));

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
    assert_eq!(outcome.resource.as_deref(), Some(&description));

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
fn upload_replies_keep_an_absent_version_absent() {
    let identity =
        ResourceUploadIdentity::parse("upload-1").assured("the test upload identity is valid");
    let reply = |disposition: UploadDisposition| UploadReply {
        request_id: None,
        disposition,
        message: "upload answered".to_string(),
        diagnostics: Vec::new(),
    };
    let upload_of = |disposition: UploadDisposition| {
        CommandOutcome::from_upload(reply(disposition), identity.clone())
    };

    let refused = upload_of(UploadDisposition::Failed {
        upload_identity: Some(identity.clone()),
        failure: UploadFailure::QuotaExceeded,
        assigned_version: None,
    });
    assert_eq!(refused.disposition, CommandDisposition::Failed);
    assert_eq!(
        refused.resource_upload,
        Some(ResourceUploadOutcome {
            identity: identity.clone(),
            version: None,
            origin: None,
            failure: Some(UploadFailure::QuotaExceeded),
        })
    );

    let failed = upload_of(UploadDisposition::Failed {
        upload_identity: Some(identity.clone()),
        failure: UploadFailure::InstallationFailed,
        assigned_version: NonZeroU64::new(3),
    });
    assert_eq!(
        failed.resource_upload,
        Some(ResourceUploadOutcome {
            identity: identity.clone(),
            version: NonZeroU64::new(3),
            origin: None,
            failure: Some(UploadFailure::InstallationFailed),
        })
    );

    let redirected = upload_of(UploadDisposition::NotLeader(LeaderRedirect {
        leader: None,
    }));
    assert_eq!(
        redirected.disposition,
        CommandDisposition::NotLeader(LeaderRedirect { leader: None })
    );
    assert_eq!(redirected.routing(), Routing::AwaitElection);
    assert_eq!(
        redirected.resource_upload,
        Some(ResourceUploadOutcome {
            identity,
            version: None,
            origin: None,
            failure: None,
        })
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
    let generation = loopback
        .client
        .inner
        .exchange
        .lock()
        .await
        .generation
        .clone();
    loopback
        .client
        .inner
        .events
        .sinks
        .close_generation(&generation);
    let interrupted = loopback
        .client
        .next_subscription()
        .await
        .assured("the session loss reports the acknowledged subscription's gap");
    assert!(matches!(interrupted, SubscriptionEvent::Interrupted(_)));
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
    assert_eq!(
        loopback.client.subscription_lifecycle(
            &SubscriptionName::parse("live").assured("the test name is valid")
        ),
        None
    );
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
async fn deleting_while_creation_is_in_flight_drains_its_late_success_before_name_reuse() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let creating = tokio::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let create_request = loopback.next_request().await;
    assert!(matches!(
        create_request.request,
        ClientRequest::Subscribe(_)
    ));
    creating.abort();

    let client = loopback.client.clone();
    let deleting = tokio::spawn(async move { client.unsubscribe("live").await });
    tokio::task::yield_now().await;
    assert_eq!(
        loopback
            .client
            .subscription_lifecycle(&SubscriptionName::parse("live").assured("test name is valid")),
        Some(crate::SubscriptionLifecycle::Closing)
    );
    loopback
        .answer(
            create_request.request_id,
            opened_reply(subscription("live", 1)),
        )
        .await;
    let delete_request = loopback.next_request().await;
    assert!(matches!(
        delete_request.request,
        ClientRequest::Unsubscribe(_)
    ));
    loopback
        .answer(
            delete_request.request_id,
            ReplyBody::Unsubscribe(UnsubscribeOutcome {
                disposition: UnsubscribeDisposition::Deleted(subscription("live", 1)),
                message: "subscription deleted".to_string(),
                diagnostics: Vec::new(),
            }),
        )
        .await;
    let deletion = deleting
        .await
        .assured("the deletion task completes")
        .assured("the late success is cleaned up");
    assert!(deletion.succeeded());
    assert_eq!(
        loopback
            .client
            .subscription_lifecycle(&SubscriptionName::parse("live").assured("test name is valid")),
        None
    );

    let client = loopback.client.clone();
    let recreating = tokio::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let recreate_request = loopback.next_request().await;
    assert!(matches!(
        recreate_request.request,
        ClientRequest::Subscribe(_)
    ));
    loopback
        .answer(
            recreate_request.request_id,
            opened_reply(subscription("live", 2)),
        )
        .await;
    let recreated = recreating
        .await
        .assured("the replacement task completes")
        .assured("the name can be reused after deletion");
    assert!(recreated.succeeded());
}

#[tokio::test]
async fn an_acknowledged_subscription_retries_restoration_on_the_replacement_exchange() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let creating = tokio::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let initial = loopback.next_request().await;
    loopback
        .answer(initial.request_id, opened_reply(subscription("live", 1)))
        .await;
    creating
        .await
        .assured("the create task completes")
        .assured("the initial subscription opens");

    loopback.replace_exchange().await;
    let interrupted = loopback
        .client
        .next_subscription()
        .await
        .assured("the old exchange reports its delivery gap");
    assert!(matches!(interrupted, SubscriptionEvent::Interrupted(_)));
    let first_restore = loopback.next_request().await;
    let ClientRequest::Subscribe(request) = first_restore.request else {
        panic!("restoration sends a typed subscribe request");
    };
    assert_eq!(request.domain, domain("tenant"));
    assert_eq!(request.statement, "CREATE SUBSCRIPTION live TO orders;");
    loopback
        .answer(
            first_restore.request_id,
            ReplyBody::Subscribe(SubscribeOutcome {
                disposition: SubscribeDisposition::Failed,
                message: "relay is starting".to_string(),
                diagnostics: Vec::new(),
            }),
        )
        .await;

    let second_restore = loopback.next_request().await;
    assert!(matches!(
        second_restore.request,
        ClientRequest::Subscribe(_)
    ));
    loopback
        .answer(
            second_restore.request_id,
            opened_reply(subscription("live", 2)),
        )
        .await;
    let mut changed = loopback.client.inner.events.sinks.desired.watch();
    tokio::time::timeout(DEADLINE, async {
        loop {
            tokio::task::consume_budget().await;
            if let Some(crate::SubscriptionLifecycle::Active(handle)) =
                loopback.client.subscription_lifecycle(
                    &SubscriptionName::parse("live").assured("the test name is valid"),
                )
            {
                assert_eq!(handle, subscription("live", 2));
                break;
            }
            changed
                .changed()
                .await
                .assured("the registry remains alive while the client is held");
        }
    })
    .await
    .assured("the replacement subscription becomes active within the deadline");
}

#[tokio::test]
async fn a_session_lost_before_create_acknowledgement_does_not_restore_the_request() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let creating = tokio::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let request = loopback.next_request().await;
    assert!(matches!(request.request, ClientRequest::Subscribe(_)));
    let generation = loopback
        .client
        .inner
        .exchange
        .lock()
        .await
        .generation
        .clone();
    loopback.pending.lock().close();
    loopback
        .client
        .inner
        .events
        .sinks
        .close_generation(&generation);
    assert!(creating.await.assured("the create task completes").is_err());
    let name = SubscriptionName::parse("live").assured("the test name is valid");
    assert_eq!(loopback.client.subscription_lifecycle(&name), None);

    loopback.replace_exchange().await;
    assert_eq!(loopback.client.subscription_lifecycle(&name), None);
    assert!(loopback.requests.try_recv().is_err());
}

#[tokio::test]
async fn a_session_lost_during_unsubscribe_releases_the_name_on_the_next_exchange() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let creating = tokio::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let request = loopback.next_request().await;
    loopback
        .answer(request.request_id, opened_reply(subscription("live", 1)))
        .await;
    creating
        .await
        .assured("the create task completes")
        .assured("the first subscription opens");

    let client = loopback.client.clone();
    let deleting = tokio::spawn(async move { client.unsubscribe("live").await });
    let request = loopback.next_request().await;
    assert!(matches!(request.request, ClientRequest::Unsubscribe(_)));
    let generation = loopback
        .client
        .inner
        .exchange
        .lock()
        .await
        .generation
        .clone();
    loopback.pending.lock().close();
    loopback
        .client
        .inner
        .events
        .sinks
        .close_generation(&generation);
    assert!(deleting.await.assured("the delete task completes").is_err());
    let name = SubscriptionName::parse("live").assured("the test name is valid");
    assert_eq!(loopback.client.subscription_lifecycle(&name), None);

    loopback.replace_exchange().await;
    let client = loopback.client.clone();
    let recreating = tokio::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let request = loopback.next_request().await;
    assert!(matches!(request.request, ClientRequest::Subscribe(_)));
    loopback
        .answer(request.request_id, opened_reply(subscription("live", 2)))
        .await;
    assert!(
        recreating
            .await
            .assured("the replacement task completes")
            .assured("the name can be reused")
            .succeeded()
    );
}

#[tokio::test]
async fn cancelling_an_in_flight_restore_cleans_up_its_late_success() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let creating = tokio::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let request = loopback.next_request().await;
    loopback
        .answer(request.request_id, opened_reply(subscription("live", 1)))
        .await;
    creating
        .await
        .assured("the initial create task completes")
        .assured("the initial subscription opens");
    loopback.replace_exchange().await;
    let interrupted = loopback
        .client
        .next_subscription()
        .await
        .assured("the lost session reports a gap");
    assert!(matches!(interrupted, SubscriptionEvent::Interrupted(_)));
    let restore = loopback.next_request().await;
    assert!(matches!(restore.request, ClientRequest::Subscribe(_)));

    let client = loopback.client.clone();
    let deleting = tokio::spawn(async move { client.unsubscribe("live").await });
    tokio::task::yield_now().await;
    let name = SubscriptionName::parse("live").assured("the test name is valid");
    assert_eq!(
        loopback.client.subscription_lifecycle(&name),
        Some(crate::SubscriptionLifecycle::Closing)
    );
    loopback
        .answer(restore.request_id, opened_reply(subscription("live", 2)))
        .await;
    let deletion = loopback.next_request().await;
    assert!(matches!(deletion.request, ClientRequest::Unsubscribe(_)));
    loopback
        .answer(
            deletion.request_id,
            ReplyBody::Unsubscribe(UnsubscribeOutcome {
                disposition: UnsubscribeDisposition::Deleted(subscription("live", 2)),
                message: "subscription deleted".to_string(),
                diagnostics: Vec::new(),
            }),
        )
        .await;
    assert!(
        deleting
            .await
            .assured("the delete task completes")
            .assured("the late restore was deleted")
            .succeeded()
    );
    assert_eq!(loopback.client.subscription_lifecycle(&name), None);
}

#[tokio::test]
async fn a_command_carries_the_expectation_of_the_attached_transaction() {
    let mut loopback = Loopback::new(Some(domain("default")));
    loopback
        .client
        .adopt_transaction_status(open_transaction("tx-1", 2))
        .await;
    cache_test_preview(&loopback.client, "tx-1", 2).await;
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
async fn concurrent_commands_capture_transaction_position_in_send_order() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    loopback
        .client
        .adopt_transaction_status(open_transaction("tx-1", 0))
        .await;
    let first_client = loopback.client.clone();
    let first_task =
        tokio::spawn(async move { first_client.execute("SHOW CLUSTER STATUS;").await });
    let first = loopback.next_request().await;
    let ClientRequest::Command(first_command) = first.request else {
        panic!("the first request is a command");
    };
    assert_eq!(
        first_command.expected_transaction_position,
        Some(TransactionPosition::new(0))
    );

    let second_client = loopback.client.clone();
    let second_task =
        tokio::spawn(async move { second_client.execute("SHOW CLUSTER STATUS;").await });
    let mut first_outcome = wire_outcome(
        first_command.execution_reference.as_str(),
        completed(),
        "first complete",
    );
    first_outcome.transaction = Some(open_transaction("tx-1", 1));
    loopback
        .answer(
            first.request_id,
            ReplyBody::Command(Box::new(first_outcome)),
        )
        .await;
    first_task
        .await
        .assured("the first task completes")
        .assured("the first command succeeds");
    let second = loopback.next_request().await;
    let ClientRequest::Command(second_command) = second.request else {
        panic!("the second request is a command");
    };
    assert_eq!(
        second_command.expected_transaction_position,
        Some(TransactionPosition::new(1)),
    );
    loopback
        .answer(
            second.request_id,
            ReplyBody::Command(Box::new(wire_outcome(
                second_command.execution_reference.as_str(),
                completed(),
                "second complete",
            ))),
        )
        .await;
    second_task
        .await
        .assured("the second task completes")
        .assured("the second command succeeds");
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
    loopback.client.set_domain(Some(domain("changed"))).await;
    let second = loopback.next_request().await;
    let ClientRequest::Command(second_command) = second.request else {
        panic!("the retry is sent as a command");
    };
    assert_ne!(second.request_id, first.request_id);
    assert_eq!(
        second_command.execution_reference, first_command.execution_reference,
        "a retry repeats the execution reference so the admitted command is recovered"
    );
    assert_eq!(second_command.domain, first_command.domain);
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
async fn a_command_reply_for_another_execution_cannot_claim_success() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let execution = tokio::spawn(async move { client.execute("SHOW CLUSTER STATUS;").await });
    let request = loopback.next_request().await;
    loopback
        .answer(
            request.request_id,
            command_reply(completed(), "wrong execution"),
        )
        .await;
    let result = execution.await.assured("the command task completes");
    assert!(
        result.is_err(),
        "a completed reply with another durable identity is not proof for this command"
    );
}

#[tokio::test]
async fn cancelling_a_command_releases_its_pending_reply() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let execution = client.prepare_execution("SHOW CLUSTER STATUS;").await;
    let waiter = tokio::spawn(async move { client.execute_prepared(&execution).await });
    let request = loopback.next_request().await;

    waiter.abort();
    waiter.await.expect_err("the command waiter was cancelled");
    assert!(
        loopback.pending.lock().take(request.request_id).is_none(),
        "a cancelled waiter cannot occupy the registry until a reply or disconnect"
    );
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
    client.inner.events.sinks.subscriptions.close_current();
    client.inner.events.sinks.notices.close_current();
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
