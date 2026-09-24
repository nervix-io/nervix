//! Steps that drive the session protocol directly.
//!
//! Layer: test harness.
//! - **Owns.** Requests a scenario names and answers by name, cancellations, altered and malformed
//!   frames, upload streams shaped the way the protocol does not allow, and the typed dispositions
//!   and transport statuses a session reports.
//! - **Depends on.** The harness's own session client and the scenario cluster.
//! - **Must not know.** How the server correlates, admits or cancels a request.

use cucumber::{then, when};
use nervix_client_wire::{
    CancelState, CancellationStage, ClientMessage, ClientRequest, CommandDisposition,
    CommandRequest, DomainList, InspectTransactionRequest, InspectionOutcome, ReplyBody, RequestId,
    RequestRejection, SessionEndReason, SuggestRequest, UnknownOutcomeCause, UploadDisposition,
};
use nervix_models::{
    CommandExecutionReference, ResourceUploadIdentity, TransactionInspectionRejection,
    TransactionInspectionTarget,
};

use super::*;
use crate::common::raw_session::{open_raw_session, open_session_as};

/// How long a scenario waits for the server to end a session it broke.
const SESSION_END_TIMEOUT: Duration = Duration::from_secs(30);

fn active_session(world: &mut ScenarioWorld) -> &mut TestSession {
    world
        .active_session
        .as_mut()
        .expect("an active session must exist")
}

fn named_request(world: &ScenarioWorld, name: &str) -> RequestId {
    *world
        .session_requests
        .get(name)
        .unwrap_or_else(|| panic!("request '{name}' was never sent"))
}

async fn reply_to_named(world: &mut ScenarioWorld, name: &str) -> ReplyBody {
    let request_id = named_request(world, name);
    active_session(world)
        .reply_to(request_id)
        .await
        .unwrap_or_else(|error| panic!("request '{name}' was not answered: {error}"))
}

fn execution_reference(raw: &str) -> CommandExecutionReference {
    CommandExecutionReference::parse(raw).expect("scenario execution references are valid")
}

#[when(expr = "the active session sends request {string} with this NSPL command")]
async fn when_active_session_sends_named_command(
    world: &mut ScenarioWorld,
    name: String,
    #[step] step: &Step,
) {
    let query = expand_placeholders(world, docstring(step));
    let reference = crate::common::raw_session::fresh_execution_reference();
    let request_id = active_session(world)
        .send_command_request_with_reference(&query, &reference)
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{name}': {error}"));
    world.session_requests.insert(name, request_id);
}

#[when(
    expr = "the active session sends request {string} with execution reference {string} and this \
            NSPL command"
)]
async fn when_active_session_sends_named_referenced_command(
    world: &mut ScenarioWorld,
    name: String,
    execution_reference: String,
    #[step] step: &Step,
) {
    let query = expand_placeholders(world, docstring(step));
    let reference = command_execution_reference(world, &execution_reference);
    let request_id = active_session(world)
        .send_command_request_with_reference(&query, &reference)
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{name}': {error}"));
    world.session_requests.insert(name, request_id);
}

#[when(expr = "the active session sends request {string} listing domains")]
async fn when_active_session_lists_domains(world: &mut ScenarioWorld, name: String) {
    let (request_id, _) = active_session(world)
        .send_request(ClientRequest::ListDomains)
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{name}': {error}"));
    world.session_requests.insert(name, request_id);
}

#[then(expr = "request {string} lists domain {string}")]
async fn then_request_lists_domain(world: &mut ScenarioWorld, name: String, domain: String) {
    let domain = expand_placeholders(world, &domain);
    let body = reply_to_named(world, &name).await;
    let ReplyBody::DomainList(DomainList { domains }) = body else {
        panic!("request '{name}' was answered with {body:?}");
    };
    assert!(
        domains.iter().any(|info| info.domain.as_str() == domain),
        "request '{name}' did not list domain '{domain}': {domains:?}"
    );
}

#[when(expr = "the active session sends request {string} completing {string} at byte {int}")]
async fn when_active_session_requests_completions(
    world: &mut ScenarioWorld,
    name: String,
    input: String,
    cursor: usize,
) {
    let session = active_session(world);
    let domain = session.domain().cloned();
    let request = SuggestRequest::new(input, cursor, domain)
        .unwrap_or_else(|error| panic!("request '{name}' has an invalid cursor: {error}"));
    let (request_id, _) = session
        .send_request(ClientRequest::Suggest(request))
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{name}': {error}"));
    world.session_requests.insert(name, request_id);
}

#[then(expr = "request {string} suggests {string}")]
async fn then_request_suggests(world: &mut ScenarioWorld, name: String, expected: String) {
    let body = reply_to_named(world, &name).await;
    let ReplyBody::Suggest(outcome) = body else {
        panic!("request '{name}' was answered with {body:?}");
    };
    assert!(
        outcome
            .suggestions
            .iter()
            .any(|suggestion| suggestion.value == expected),
        "request '{name}' did not suggest '{expected}': {:?}",
        outcome.suggestions
    );
}

#[when(expr = "the active session cancels request {string}")]
async fn when_active_session_cancels_request(world: &mut ScenarioWorld, name: String) {
    let target = named_request(world, &name);
    let session = active_session(world);
    let cancel = session
        .cancel(target)
        .await
        .unwrap_or_else(|error| panic!("failed to cancel request '{name}': {error}"));
    let body = session
        .reply_to(cancel)
        .await
        .unwrap_or_else(|error| panic!("the cancellation of '{name}' was not answered: {error}"));
    let ReplyBody::Cancel(outcome) = body else {
        panic!("the cancellation of '{name}' was answered with {body:?}");
    };
    assert_eq!(outcome.target, target);
    assert_eq!(
        outcome.state,
        CancelState::Requested,
        "request '{name}' must still be in flight when it is cancelled"
    );
}

async fn assert_cancelled(world: &mut ScenarioWorld, name: &str, expected: CancellationStage) {
    let body = reply_to_named(world, name).await;
    let ReplyBody::Cancelled(cancelled) = body else {
        panic!("request '{name}' was answered with {body:?} instead of its cancellation");
    };
    assert_eq!(
        cancelled.stage, expected,
        "request '{name}' was cancelled at the wrong stage"
    );
}

#[then(expr = "request {string} is cancelled before admission")]
async fn then_request_is_cancelled_before_admission(world: &mut ScenarioWorld, name: String) {
    assert_cancelled(world, &name, CancellationStage::BeforeAdmission).await;
}

#[then(expr = "request {string} is cancelled after admission")]
async fn then_request_is_cancelled_after_admission(world: &mut ScenarioWorld, name: String) {
    assert_cancelled(world, &name, CancellationStage::AfterAdmission).await;
}

/// A command request carrying `reference`, for a scenario that alters its encoding.
fn command_message(
    request_id: RequestId,
    reference: &str,
    domain: Option<nervix_models::DomainName>,
) -> ClientMessage {
    ClientMessage {
        request_id,
        request: ClientRequest::Command(CommandRequest {
            query: "SHOW CLUSTER STATUS;".to_string(),
            domain,
            execution_reference: execution_reference(reference),
            expected_transaction_position: None,
            expected_preview: None,
        }),
    }
}

#[when(expr = "the active session sends request {string} with execution reference text {string}")]
async fn when_active_session_sends_malformed_reference(
    world: &mut ScenarioWorld,
    name: String,
    reference: String,
) {
    let session = active_session(world);
    let request_id = session.reserve_request_id();
    let domain = session.domain().cloned();
    // Two valid references of the text's length tell the reference's bytes apart from the rest of
    // the frame, which the text then replaces.
    let original = command_message(request_id, &"a".repeat(reference.len()), domain.clone());
    let variant = command_message(request_id, &"b".repeat(reference.len()), domain);
    session
        .send_altered(&original, &variant, reference.as_bytes())
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{name}': {error}"));
    world.session_requests.insert(name, request_id);
}

#[when(
    expr = "the active session sends request {string} completing {string} at byte {int} inside a \
            character"
)]
async fn when_active_session_sends_split_cursor(
    world: &mut ScenarioWorld,
    name: String,
    input: String,
    cursor: usize,
) {
    assert!(
        !input.is_char_boundary(cursor),
        "byte {cursor} of {input:?} must fall inside a character"
    );
    let before = (0..cursor)
        .rev()
        .find(|offset| input.is_char_boundary(*offset))
        .expect("offset zero is a character boundary");
    let after = (cursor..=input.len())
        .find(|offset| input.is_char_boundary(*offset))
        .expect("the end of the input is a character boundary");
    let session = active_session(world);
    let request_id = session.reserve_request_id();
    let domain = session.domain().cloned();
    let suggestion = |offset: usize| ClientMessage {
        request_id,
        request: ClientRequest::Suggest(
            SuggestRequest::new(input.clone(), offset, domain.clone())
                .expect("a character boundary is a valid cursor"),
        ),
    };
    let original = suggestion(before);
    let variant = suggestion(after);
    let cursor_byte = u8::try_from(cursor).expect("a scenario cursor fits one byte");
    session
        .send_altered(&original, &variant, &[cursor_byte])
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{name}': {error}"));
    world.session_requests.insert(name, request_id);
}

#[then(expr = "request {string} is rejected as an invalid request naming {string}")]
async fn then_request_is_rejected_naming(world: &mut ScenarioWorld, name: String, field: String) {
    let body = reply_to_named(world, &name).await;
    let ReplyBody::Rejected(rejected) = body else {
        panic!("request '{name}' was answered with {body:?} instead of a rejection");
    };
    assert_eq!(rejected.rejection, RequestRejection::InvalidRequest);
    assert_eq!(rejected.field.as_deref(), Some(field.as_str()));
}

#[when("the active session sends a request whose identity is zero")]
async fn when_active_session_sends_zero_identity(world: &mut ScenarioWorld) {
    let session = active_session(world);
    let domain = session.domain().cloned();
    let first = RequestId::new(std::num::NonZeroU64::MIN);
    let second = RequestId::new(
        std::num::NonZeroU64::MIN
            .checked_add(1)
            .expect("two is a non-zero identity"),
    );
    let original = command_message(first, "zero-identity", domain.clone());
    let variant = command_message(second, "zero-identity", domain);
    session
        .send_altered(&original, &variant, &[0])
        .await
        .unwrap_or_else(|error| panic!("failed to send the zero-identity request: {error}"));
}

#[then("the active session is ended for violating the protocol")]
async fn then_active_session_is_ended_for_violating_protocol(world: &mut ScenarioWorld) {
    let session = active_session(world);
    let status = session
        .wait_until_ended(SESSION_END_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("the session did not end: {error}"));
    let ending = session.ending().cloned();
    assert!(
        matches!(ending, Some(SessionEndReason::ProtocolViolated { .. })),
        "the session must end for violating the protocol, ended with {ending:?} and {status:?}"
    );
}

#[when(expr = "the active session sends {int} bytes that are not a frame")]
async fn when_active_session_sends_garbage(world: &mut ScenarioWorld, length: usize) {
    let garbage = bytes::Bytes::from(vec![0x5a_u8; length]);
    active_session(world)
        .send_raw_frame(garbage)
        .await
        .unwrap_or_else(|error| panic!("failed to send bytes that are not a frame: {error}"));
}

#[when("the active session sends a message larger than the frame limit")]
async fn when_active_session_sends_oversized_message(world: &mut ScenarioWorld) {
    let limit = nervix_client_wire::SessionLimits::DEFAULT.frame_bytes();
    let length = limit
        .checked_add(1)
        .expect("the frame limit is far below usize::MAX");
    when_active_session_sends_garbage(world, length).await;
}

#[then(expr = "the active session ends with status {string}")]
async fn then_active_session_ends_with_status(world: &mut ScenarioWorld, expected: String) {
    let status = active_session(world)
        .wait_until_ended(SESSION_END_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("the session did not end: {error}"));
    assert_eq!(
        format!("{:?}", status.code()),
        expected,
        "the session ended with {status:?}"
    );
}

#[then(
    expr = "a session opened on the leader node with a wrong password is refused with status \
            {string}"
)]
async fn then_wrong_password_session_is_refused(world: &mut ScenarioWorld, expected: String) {
    let leader = current_leader_node(world).await;
    let server = world
        .cluster()
        .grpc_uri(&leader)
        .unwrap_or_else(|error| panic!("the leader has no gRPC address: {error}"));
    let authorization =
        crate::common::cluster::test_basic_authorization_for_password("not-the-password");
    let opened = open_session_as(&server, &world.domain, &authorization)
        .await
        .unwrap_or_else(|error| panic!("the session could not be attempted: {error}"));
    let Err(status) = opened else {
        panic!("a session with a wrong password must be refused");
    };
    assert_eq!(
        format!("{:?}", status.code()),
        expected,
        "refused with {status:?}"
    );
}

#[then("the background command request reports an unknown outcome because leadership was lost")]
async fn then_background_command_request_reports_unknown_outcome(world: &mut ScenarioWorld) {
    let task = world
        .background_command_result
        .take()
        .expect("a background command request must be active");
    let result = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .unwrap_or_else(|error| panic!("background command request did not finish: {error}"))
        .unwrap_or_else(|error| panic!("background command request task failed: {error}"))
        .unwrap_or_else(|error| panic!("background command request transport failed: {error}"));
    assert_eq!(
        result.disposition,
        CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::LeadershipLost),
        "leadership lost after durable admission leaves the outcome unknown: {result:?}"
    );
}

/// Waits until `node_id` refuses a command that needs the leader with a redirect naming `leader`
/// and none of its endpoints. A node without quorum keeps naming the leader it last followed, and
/// once discovery no longer reaches that leader the redirect must not guess an address for it.
#[then(
    expr = "within {string} node {string} redirects commands to leader {string} without an \
            endpoint"
)]
async fn then_node_redirects_without_an_endpoint(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    leader: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let leader = expand_placeholders(world, &leader);
    let server = world
        .cluster()
        .grpc_uri(&node_id)
        .unwrap_or_else(|error| panic!("node '{node_id}' has no gRPC address: {error}"));
    let deadline = Instant::now() + duration;
    let mut last = None;
    loop {
        tokio::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "node '{node_id}' never redirected to '{leader}' without an endpoint within \
             {duration:?}; last: {last:?}"
        );
        let session = open_raw_session(&server, &world.domain).await;
        let mut session = match session {
            Ok(session) => session,
            Err(error) => {
                last = Some(format!("session failed: {error}"));
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let result = session
            .run_command_result("CREATE SCHEMA unreachable_leader ( id I64 );")
            .await;
        match result {
            Ok(result) => {
                if let CommandDisposition::NotLeader(redirect) = &result.disposition
                    && let Some(named) = &redirect.leader
                    && named.node.as_str() == leader
                    && named.grpc_uri.is_none()
                    && named.web_console_uri.is_none()
                {
                    return;
                }
                last = Some(format!("{result:?}"));
            }
            Err(error) => last = Some(format!("request failed: {error}")),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Opens the console WebSocket on the leader, sends one message that is not a session frame, and
/// waits for the close that ends the connection.
#[then(
    expr = "a console WebSocket on the leader node that sends {string} is closed with code {int}"
)]
async fn then_console_websocket_closes_with_code(
    world: &mut ScenarioWorld,
    message_kind: String,
    expected: u16,
) {
    use futures_util::{SinkExt as _, StreamExt as _};
    use tokio_tungstenite::tungstenite::Message;

    let leader = current_leader_node(world).await;
    let console = world
        .cluster()
        .web_console_url(&leader)
        .unwrap_or_else(|error| panic!("the leader has no console address: {error}"));
    let mut url = url::Url::parse(&console).expect("the console address is a URL");
    url.set_scheme("ws")
        .unwrap_or_else(|()| panic!("a console address can use the ws scheme"));
    url.set_path("/console/ws");
    let (mut socket, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap_or_else(|error| panic!("the console WebSocket did not open: {error}"));
    let message = match message_kind.as_str() {
        "text" => Message::Text("not a session frame".to_string()),
        "garbage" => Message::Binary(vec![0x5a; 12]),
        other => panic!("unknown console message kind '{other}'"),
    };
    socket
        .send(message)
        .await
        .unwrap_or_else(|error| panic!("the console WebSocket refused the message: {error}"));
    let deadline = Instant::now() + SESSION_END_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let received = tokio::time::timeout(remaining, socket.next())
            .await
            .unwrap_or_else(|_| panic!("the console WebSocket was not closed in time"));
        match received {
            Some(Ok(Message::Close(Some(frame)))) => {
                assert_eq!(u16::from(frame.code), expected, "closed with {frame:?}");
                return;
            }
            Some(Ok(Message::Close(None))) => panic!("the console closed without a close code"),
            Some(Ok(_)) => {}
            Some(Err(error)) => panic!("the console WebSocket failed before closing: {error}"),
            None => panic!("the console WebSocket ended without a close frame"),
        }
    }
}

#[when(expr = "the active session sends request {string} inspecting its attached transaction")]
async fn when_active_session_inspects_attached_transaction(
    world: &mut ScenarioWorld,
    name: String,
) {
    let request = InspectTransactionRequest {
        target: TransactionInspectionTarget::Attached,
        operation: None,
    };
    let (request_id, _) = active_session(world)
        .send_request(ClientRequest::InspectTransaction(request))
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{name}': {error}"));
    world.session_requests.insert(name, request_id);
}

#[then(expr = "request {string} is refused because no transaction is attached")]
async fn then_request_is_refused_without_attached_transaction(
    world: &mut ScenarioWorld,
    name: String,
) {
    let body = reply_to_named(world, &name).await;
    let ReplyBody::Inspection(InspectionOutcome::Rejected { rejection, .. }) = body else {
        panic!("request '{name}' was answered with {body:?}");
    };
    assert_eq!(
        rejection,
        TransactionInspectionRejection::NoAttachedTransaction
    );
}

/// Waits for the background command request to end and asserts that its session ended before
/// answering it, as a session does for a request it had not admitted when its node stopped.
#[then("the background command request ends without an answer")]
async fn then_background_command_request_ends_without_an_answer(world: &mut ScenarioWorld) {
    let task = world
        .background_command_result
        .take()
        .expect("a background command request must be active");
    let result = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .unwrap_or_else(|error| panic!("background command request did not finish: {error}"))
        .unwrap_or_else(|error| panic!("background command request task failed: {error}"));
    if let Ok(outcome) = result {
        panic!("the background command request was answered: {outcome:?}");
    }
}

/// The frames of an upload stream shaped the way a scenario names it.
fn shaped_upload_parts(shape: &str) -> Vec<TestUploadPart> {
    let declaring_one_byte = || TestUploadPart::Start {
        declared_bytes: NonZeroU64::MIN,
    };
    match shape {
        "is empty" => Vec::new(),
        "begins with a chunk" => vec![TestUploadPart::Chunk(vec![0])],
        "carries a second start" => vec![declaring_one_byte(), declaring_one_byte()],
        "carries more bytes than it declares" => {
            vec![declaring_one_byte(), TestUploadPart::Chunk(vec![0, 0])]
        }
        "declares more bytes than an archive may hold" => vec![TestUploadPart::Start {
            declared_bytes: NonZeroU64::MAX,
        }],
        other => panic!("unsupported upload shape '{other}'"),
    }
}

#[when(
    regex = r#"^an upload of resource "([^"]+)" with identity "([^"]+)" that (is empty|begins with a chunk|carries a second start|carries more bytes than it declares|declares more bytes than an archive may hold) is sent to the leader node$"#
)]
async fn when_shaped_upload_is_sent(
    world: &mut ScenarioWorld,
    resource: String,
    identity: String,
    shape: String,
) {
    let resource = expand_placeholders(world, &resource);
    let identity = expand_placeholders(world, &identity);
    let leader = current_leader_node(world).await;
    let upload = TestUpload {
        domain: &world.domain,
        resource: &resource,
        identity: &identity,
        parts: shaped_upload_parts(&shape),
    };
    let reply = world
        .cluster()
        .send_shaped_resource_upload(&leader, upload)
        .await
        .unwrap_or_else(|error| panic!("the upload that {shape} was not answered: {error}"));
    world.last_command_error = Some(reply.message.clone());
    world.last_upload_reply = Some(reply);
}

/// The failure of the last shaped upload, which a stream the protocol does not allow always
/// ends with, and which never assigns a version.
fn last_upload_refusal(world: &ScenarioWorld, expected: &str) -> Option<ResourceUploadIdentity> {
    let reply = world
        .last_upload_reply
        .as_ref()
        .expect("a shaped upload must have been sent");
    let UploadDisposition::Failed {
        upload_identity,
        failure,
        assigned_version,
    } = &reply.disposition
    else {
        panic!("the upload was not refused: {reply:?}");
    };
    assert_eq!(
        format!("{failure:?}"),
        expected,
        "the upload was refused for another reason: {}",
        reply.message
    );
    assert_eq!(
        *assigned_version, None,
        "a refused stream assigns no version"
    );
    upload_identity.clone()
}

#[then(expr = "the last upload is refused as {string} for identity {string}")]
async fn then_last_upload_is_refused_for_identity(
    world: &mut ScenarioWorld,
    failure: String,
    identity: String,
) {
    let identity = expand_placeholders(world, &identity);
    let Some(refused) = last_upload_refusal(world, &failure) else {
        panic!("the refusal names no upload identity");
    };
    assert_eq!(refused.as_str(), identity);
}

#[then(expr = "the last upload is refused as {string} before it names an identity")]
async fn then_last_upload_is_refused_before_it_names_an_identity(
    world: &mut ScenarioWorld,
    failure: String,
) {
    let refused = last_upload_refusal(world, &failure);
    assert_eq!(
        refused, None,
        "a stream without a valid start names no identity"
    );
    let reply = world
        .last_upload_reply
        .as_ref()
        .verified("last_upload_refusal above found the reply");
    assert_eq!(reply.request_id, None);
}
